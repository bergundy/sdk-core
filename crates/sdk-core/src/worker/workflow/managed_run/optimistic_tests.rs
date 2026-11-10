//! Replay of histories containing optimistic start batches.

use super::WorkflowManager;
use crate::{
    replay::{DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    telemetry::metrics::MetricsContext,
    test_help::test_worker_cfg,
    worker::workflow::{RunBasics, WFCommand, WFCommandVariant, WFMachinesError},
};
use std::time::{Duration, SystemTime};
use temporalio_common::protos::{
    coresdk::{
        AsJsonPayloadExt, IntoPayloadsExt,
        activity_result::activity_resolution,
        workflow_activation::{WorkflowActivation, workflow_activation_job::Variant},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
            ScheduleActivity, StartTimer,
        },
    },
    temporal::api::{
        common::v1::{ActivityType, Payload},
        enums::v1::EventType,
        history::v1::{
            ActivityTaskCompletedEventAttributes, ActivityTaskScheduledEventAttributes,
            ActivityTaskStartedEventAttributes, OptimisticResolution, OptimisticStartInfo,
            TimerFiredEventAttributes, TimerStartedEventAttributes,
        },
    },
};

fn manager(history: &TestHistoryBuilder) -> WorkflowManager {
    WorkflowManager::new(RunBasics {
        worker_config: test_worker_cfg().build().unwrap().into(),
        optimistic_env: None,
        workflow_id: "optimistic".to_owned(),
        workflow_type: DEFAULT_WORKFLOW_TYPE.to_owned(),
        run_id: history.get_orig_run_id().to_owned(),
        history: history.get_full_history_info().unwrap().into(),
        metrics: MetricsContext::no_op(),
        capabilities: &Default::default(),
        sdk_name: "test",
        sdk_version: "test",
    })
}

fn result(seq: u32) -> Payload {
    format!("result-{seq}").as_json_payload().unwrap()
}

fn schedule(seq: u32) -> WFCommand {
    schedule_with(seq, ActivityCancellationType::TryCancel)
}

fn schedule_with(seq: u32, cancellation_type: ActivityCancellationType) -> WFCommand {
    WFCommand::new(WFCommandVariant::AddActivity(ScheduleActivity {
        seq,
        activity_id: format!("act-{seq}"),
        activity_type: DEFAULT_ACTIVITY_TYPE.to_owned(),
        task_queue: "q".to_owned(),
        start_to_close_timeout: Some(Duration::from_secs(10).try_into().unwrap()),
        cancellation_type: cancellation_type as i32,
        speculative: true,
        ..Default::default()
    }))
}

fn timer(seq: u32) -> WFCommand {
    WFCommand::new(WFCommandVariant::AddTimer(StartTimer {
        seq,
        start_to_fire_timeout: Some(Duration::from_secs(3).try_into().unwrap()),
        speculative: true,
    }))
}

fn complete() -> WFCommand {
    WFCommand::new(WFCommandVariant::CompleteWorkflow(
        CompleteWorkflowExecution::default(),
    ))
}

fn optimistic_resolution(at: SystemTime) -> Option<OptimisticResolution> {
    Some(OptimisticResolution {
        workflow_time: Some(at.into()),
    })
}

trait OptimisticHistoryExt {
    fn add_optimistic_scheduled(&mut self, seq: u32) -> i64;
    fn add_activity_resolution(
        &mut self,
        scheduled_event_id: i64,
        seq: u32,
        observed_at: Option<SystemTime>,
    );
    fn add_optimistic_timer_started(&mut self, seq: u32) -> i64;
    fn add_optimistic_timer_fired(&mut self, started_event_id: i64, seq: u32, at: SystemTime);
}

impl OptimisticHistoryExt for TestHistoryBuilder {
    fn add_optimistic_scheduled(&mut self, seq: u32) -> i64 {
        self.add(ActivityTaskScheduledEventAttributes {
            activity_id: format!("act-{seq}"),
            activity_type: Some(ActivityType {
                name: DEFAULT_ACTIVITY_TYPE.to_owned(),
            }),
            optimistic_start: Some(OptimisticStartInfo {
                start_time: Some(SystemTime::now().into()),
            }),
            ..Default::default()
        })
    }

    /// Adds the started and completed events of an activity, flagged as an optimistic resolution
    /// if `observed_at` is set.
    fn add_activity_resolution(
        &mut self,
        scheduled_event_id: i64,
        seq: u32,
        observed_at: Option<SystemTime>,
    ) {
        let started_event_id = self.add(ActivityTaskStartedEventAttributes {
            scheduled_event_id,
            optimistic_resolution: observed_at.is_some(),
            ..Default::default()
        });
        self.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id,
            started_event_id,
            result: vec![result(seq)].into_payloads(),
            optimistic_resolution: observed_at.and_then(optimistic_resolution),
            ..Default::default()
        });
    }

    fn add_optimistic_timer_started(&mut self, seq: u32) -> i64 {
        self.add(TimerStartedEventAttributes {
            timer_id: seq.to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(3).try_into().unwrap()),
            optimistic_start: Some(OptimisticStartInfo {
                start_time: Some(SystemTime::now().into()),
            }),
            ..Default::default()
        })
    }

    fn add_optimistic_timer_fired(&mut self, started_event_id: i64, seq: u32, at: SystemTime) {
        self.add(TimerFiredEventAttributes {
            timer_id: seq.to_string(),
            started_event_id,
            optimistic_resolution: optimistic_resolution(at),
        });
    }
}

/// Adds a full workflow task, returning its started event's time, which optimistic workflow times
/// are relative to.
fn add_full_wf_task_returning_start(t: &mut TestHistoryBuilder) -> SystemTime {
    t.add_workflow_task_scheduled_and_started();
    let started = t
        .last_event()
        .unwrap()
        .event_time
        .unwrap()
        .try_into()
        .unwrap();
    t.add_workflow_task_completed();
    started
}

fn job_names(activation: &WorkflowActivation) -> Vec<String> {
    activation
        .jobs
        .iter()
        .map(|job| match job.variant.as_ref().unwrap() {
            Variant::InitializeWorkflow(_) => "init".to_owned(),
            Variant::SignalWorkflow(_) => "signal".to_owned(),
            Variant::FireTimer(t) => format!("timer-{}", t.seq),
            Variant::ResolveActivity(r) => {
                match r.result.as_ref().unwrap().status.as_ref().unwrap() {
                    activity_resolution::Status::Completed(c) => {
                        assert_eq!(c.result, Some(result(r.seq)));
                        format!("act-{}", r.seq)
                    }
                    activity_resolution::Status::Cancelled(_) => format!("act-{}-cancelled", r.seq),
                    other => panic!("unexpected activity resolution {other:?}"),
                }
            }
            unexpected => panic!("unexpected job: {unexpected:?}"),
        })
        .collect()
}

struct Observed {
    jobs: Vec<String>,
    timestamp: Option<SystemTime>,
    is_replaying: bool,
    history_length: u32,
}

/// Replays the history, returning what each activation looked like.
fn replay(
    history: &TestHistoryBuilder,
    mut respond: impl FnMut(&[String]) -> Vec<WFCommand>,
) -> Result<Vec<Observed>, WFMachinesError> {
    let mut replay = manager(history);
    let mut activations = vec![];
    loop {
        let activation = replay.get_next_activation()?;
        if activation.jobs.is_empty() {
            return Ok(activations);
        }
        let jobs = job_names(&activation);
        replay.push_commands_and_iterate(respond(&jobs))?;
        activations.push(Observed {
            jobs,
            timestamp: activation.timestamp.map(|t| t.try_into().unwrap()),
            is_replaying: activation.is_replaying,
            history_length: activation.history_length,
        });
    }
}

/// The workflow from the design's example: `wait([a1, a2])`, then `sleep(3)`, then `a3`, all in
/// a speculative block.
fn example_workflow(jobs: &[String]) -> Vec<WFCommand> {
    let mut commands = vec![];
    for job in jobs {
        match job.as_str() {
            "init" => commands.extend([schedule(1), schedule(2)]),
            "act-1" => commands.push(timer(3)),
            "timer-3" => commands.push(schedule(4)),
            "act-4" => commands.push(complete()),
            _ => {}
        }
    }
    commands
}

/// Builds the design's example history: WFT 3 checkpoints `a2` and `a1` resolving out of order,
/// then a timer, then hands off `a3`, which completes after the checkpoint.
fn example_history(signal_after_batch: bool, finish: bool) -> (TestHistoryBuilder, SystemTime) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let wft_started = add_full_wf_task_returning_start(&mut t);
    let a1 = t.add_optimistic_scheduled(1);
    let a2 = t.add_optimistic_scheduled(2);
    assert_eq!((a1, a2), (5, 6));
    t.add_activity_resolution(a2, 2, Some(wft_started + Duration::from_millis(200)));
    t.add_activity_resolution(a1, 1, Some(wft_started + Duration::from_millis(350)));
    let timer_started = t.add_optimistic_timer_started(3);
    assert_eq!(timer_started, 11);
    t.add_optimistic_timer_fired(timer_started, 3, wft_started + Duration::from_millis(3351));
    let a3 = t.add_optimistic_scheduled(4);
    assert_eq!(a3, 13);
    if signal_after_batch {
        t.add_we_signaled("signal", vec![]);
    }
    t.add_activity_resolution(a3, 4, None);
    t.add_workflow_task_scheduled_and_started();
    if finish {
        t.add_workflow_task_completed();
        t.add_workflow_execution_completed();
    }
    (t, wft_started)
}

#[test]
fn replays_design_example() {
    let (history, wft_started) = example_history(false, true);
    let activations = replay(&history, example_workflow).unwrap();
    let jobs: Vec<_> = activations.iter().map(|a| a.jobs.clone()).collect();
    assert_eq!(
        jobs,
        [
            vec!["init"],
            vec!["act-2"],
            vec!["act-1"],
            vec!["timer-3"],
            vec!["act-4"],
        ]
    );
    let times: Vec<_> = activations[1..4]
        .iter()
        .map(|a| a.timestamp.unwrap().duration_since(wft_started).unwrap())
        .collect();
    assert_eq!(
        times,
        [
            Duration::from_millis(200),
            Duration::from_millis(350),
            Duration::from_millis(3351)
        ]
    );
    // History length stays that of the workflow task throughout the window
    assert!(activations[..4].iter().all(|a| a.history_length == 3));
    assert!(activations.iter().all(|a| a.is_replaying));
}

#[test]
fn signal_after_batch_waits_for_next_task_and_ends_replay() {
    let (history, _) = example_history(true, false);
    let activations = replay(&history, example_workflow).unwrap();
    let jobs: Vec<_> = activations.iter().map(|a| a.jobs.clone()).collect();
    assert_eq!(
        jobs,
        [
            vec!["init"],
            vec!["act-2"],
            vec!["act-1"],
            vec!["timer-3"],
            vec!["signal", "act-4"],
        ]
    );
    // The batch's own resolutions don't end replay, the signal does.
    let replaying: Vec<_> = activations.iter().map(|a| a.is_replaying).collect();
    assert_eq!(replaying, [true, true, true, true, false]);
}

#[test]
fn extra_command_before_resolution_is_nondeterministic() {
    let (history, _) = example_history(false, true);
    let err = replay(&history, |jobs| {
        let mut commands = example_workflow(jobs);
        // The workflow now schedules something else when a2 resolves, so a1's resolution would be
        // observed after 3 commands instead of 2.
        if jobs.iter().any(|j| j == "act-2") {
            commands.push(schedule(9));
        }
        commands
    })
    .err()
    .expect("replay must fail");
    assert_matches!(err, WFMachinesError::Nondeterminism(msg) if msg.contains("after 2 commands"));
}

#[test]
fn missing_command_before_resolution_is_nondeterministic() {
    let (history, _) = example_history(false, true);
    let err = replay(&history, |jobs| {
        if jobs.iter().any(|j| j == "init") {
            vec![schedule(1)]
        } else {
            example_workflow(jobs)
        }
    })
    .err()
    .expect("replay must fail");
    assert_matches!(err, WFMachinesError::Nondeterminism(_));
}

#[test]
fn resolution_of_unstarted_activity_is_nondeterministic() {
    let (history, _) = example_history(false, true);
    // Same command count, but the workflow scheduled a different activity at position 2, so the
    // resolution recorded for the activity at event 6 has no machine.
    let err = replay(&history, |jobs| {
        if jobs.iter().any(|j| j == "init") {
            let mut second = schedule(2);
            if let WFCommandVariant::AddActivity(a) = &mut second.variant {
                a.activity_id = "other".to_owned();
            }
            vec![schedule(1), second]
        } else {
            example_workflow(jobs)
        }
    })
    .err()
    .expect("replay must fail");
    assert_matches!(err, WFMachinesError::Nondeterminism(_));
}

/// The same workflow code replays against history recorded without speculation.
#[test]
fn replays_history_without_speculation() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let a1 = t.add_activity_task_scheduled("act-1");
    let a2 = t.add_activity_task_scheduled("act-2");
    t.add_activity_resolution(a2, 2, None);
    t.add_full_wf_task();
    t.add_activity_resolution(a1, 1, None);
    t.add_full_wf_task();
    let timer_started = t.add_timer_started("3".to_owned());
    t.add_timer_fired(timer_started, "3".to_owned());
    t.add_full_wf_task();
    let a3 = t.add_activity_task_scheduled("act-4");
    t.add_activity_resolution(a3, 4, None);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let activations = replay(&t, example_workflow).unwrap();
    let jobs: Vec<_> = activations.iter().map(|a| a.jobs.clone()).collect();
    assert_eq!(
        jobs,
        [
            vec!["init"],
            vec!["act-2"],
            vec!["act-1"],
            vec!["timer-3"],
            vec!["act-4"],
        ]
    );
}

/// The workflow cancels an optimistic activity (TryCancel) when an optimistic timer fires. The
/// cancel request references the predicted scheduled event id, and the schedule command stays in
/// the log.
#[test]
fn replays_cancel_of_optimistic_activity() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let wft_started = add_full_wf_task_returning_start(&mut t);
    let a1 = t.add_optimistic_scheduled(1);
    let timer_started = t.add_optimistic_timer_started(2);
    t.add_optimistic_timer_fired(timer_started, 2, wft_started + Duration::from_secs(3));
    t.add_activity_task_cancel_requested(a1);
    t.add_workflow_execution_completed();
    let activations = replay(&t, |jobs| match jobs {
        [j] if j == "init" => vec![schedule(1), timer(2)],
        [j] if j == "timer-2" => vec![WFCommand::new(WFCommandVariant::RequestCancelActivity(
            RequestCancelActivity { seq: 1 },
        ))],
        [j] if j == "act-1-cancelled" => vec![complete()],
        other => panic!("unexpected jobs {other:?}"),
    })
    .unwrap();
    let jobs: Vec<_> = activations.iter().map(|a| a.jobs.clone()).collect();
    assert_eq!(
        jobs,
        [vec!["init"], vec!["timer-2"], vec!["act-1-cancelled"]]
    );
}

/// A WaitCancellationCompleted activity resolves as cancelled inside the window.
#[test]
fn replays_observed_cancellation() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let wft_started = add_full_wf_task_returning_start(&mut t);
    let a1 = t.add_optimistic_scheduled(1);
    let timer_started = t.add_optimistic_timer_started(2);
    t.add_optimistic_timer_fired(timer_started, 2, wft_started + Duration::from_secs(3));
    t.add_activity_task_cancel_requested(a1);
    let started = t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id: a1,
        optimistic_resolution: true,
        ..Default::default()
    });
    t.add(
        temporalio_common::protos::temporal::api::history::v1::ActivityTaskCanceledEventAttributes {
            scheduled_event_id: a1,
            started_event_id: started,
            optimistic_resolution: optimistic_resolution(
                wft_started + Duration::from_millis(3100),
            ),
            ..Default::default()
        },
    );
    t.add_workflow_execution_completed();
    let activations = replay(&t, |jobs| match jobs {
        [j] if j == "init" => vec![
            schedule_with(1, ActivityCancellationType::WaitCancellationCompleted),
            timer(2),
        ],
        [j] if j == "timer-2" => vec![WFCommand::new(WFCommandVariant::RequestCancelActivity(
            RequestCancelActivity { seq: 1 },
        ))],
        [j] if j == "act-1-cancelled" => vec![complete()],
        other => panic!("unexpected jobs {other:?}"),
    })
    .unwrap();
    let jobs: Vec<_> = activations.iter().map(|a| a.jobs.clone()).collect();
    assert_eq!(
        jobs,
        [vec!["init"], vec!["timer-2"], vec!["act-1-cancelled"]]
    );
}
