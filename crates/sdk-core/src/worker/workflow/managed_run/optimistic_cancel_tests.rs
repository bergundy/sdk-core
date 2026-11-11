//! Cancelling optimistic activities and timers. A live run drives speculation windows against a
//! fake environment, a simulated server turns each checkpoint into history the way the server
//! does (validating references to predicted event ids), the cached run continues with the next
//! workflow task, and the full history is replayed from scratch. Both must see the same
//! activations.

use super::WorkflowManager;
use crate::{
    replay::{DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    telemetry::metrics::MetricsContext,
    test_help::test_worker_cfg,
    worker::workflow::{
        LocalResolution, RunBasics, WFCommand, WFCommandVariant,
        optimistic::{
            FallbackReason, OptimisticActivityStart, OptimisticEnv, OptimisticResult,
            OptimisticResultKind,
        },
    },
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use temporalio_common::protos::{
    TaskToken,
    coresdk::{
        AsJsonPayloadExt, IntoPayloadsExt,
        activity_result::{
            Cancellation, Success, activity_execution_result::Status, activity_resolution,
        },
        workflow_activation::workflow_activation_job::Variant,
        workflow_commands::{
            ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
            RequestCancelActivity, ScheduleActivity, StartTimer,
        },
    },
    temporal::api::{
        command::v1::{
            Command, command::Attributes, resolve_optimistic_activity_command_attributes::Outcome,
        },
        common::v1::{ActivityType, Payload},
        enums::v1::EventType,
        failure::v1::{CanceledFailureInfo, Failure, failure::FailureInfo},
        history::v1::{
            ActivityTaskCancelRequestedEventAttributes, ActivityTaskCanceledEventAttributes,
            ActivityTaskCompletedEventAttributes, ActivityTaskScheduledEventAttributes,
            ActivityTaskStartedEventAttributes, OptimisticResolution, OptimisticStartInfo,
            TimerCanceledEventAttributes, TimerFiredEventAttributes, TimerStartedEventAttributes,
        },
    },
};

#[derive(Debug, Clone, PartialEq)]
enum EnvEvent {
    StartActivity(u32),
    CancelActivity(u32),
    StartTimer(u32),
    CancelTimer(u32),
    Observed(u32),
    WindowClosed,
    WindowDropped,
}

/// Optimistic environment with a manual clock that records what Core asks of it.
struct FakeEnv {
    now: Mutex<Instant>,
    events: Mutex<Vec<EnvEvent>>,
    generation: Mutex<u64>,
}

impl FakeEnv {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            now: Mutex::new(Instant::now()),
            events: Default::default(),
            generation: Default::default(),
        })
    }
    fn advance(&self, ms: u64) {
        *self.now.lock() += Duration::from_millis(ms);
    }
    fn take_events(&self) -> Vec<EnvEvent> {
        std::mem::take(&mut *self.events.lock())
    }
}

fn token(seq: u32) -> TaskToken {
    TaskToken::new_optimistic_activity_token(seq.to_string().into_bytes())
}

impl OptimisticEnv for FakeEnv {
    fn enabled(&self) -> bool {
        true
    }
    fn now(&self) -> Instant {
        *self.now.lock()
    }
    fn reserve_activity_slots(&self, _: usize) -> Result<(), FallbackReason> {
        Ok(())
    }
    fn release_reserved_slots(&self) {}
    fn start_activity(&self, start: OptimisticActivityStart) {
        *self.generation.lock() = start.generation;
        self.events.lock().push(EnvEvent::StartActivity(start.seq));
    }
    fn cancel_activity(&self, _: &str, _: u64, seq: u32) {
        self.events.lock().push(EnvEvent::CancelActivity(seq));
    }
    fn start_timer(&self, _: &str, generation: u64, seq: u32, _: Duration) {
        *self.generation.lock() = generation;
        self.events.lock().push(EnvEvent::StartTimer(seq));
    }
    fn cancel_timer(&self, _: &str, _: u64, seq: u32) {
        self.events.lock().push(EnvEvent::CancelTimer(seq));
    }
    fn set_deadline(&self, _: &str, _: u64, _: Instant) {}
    fn activity_outcome_observed(&self, token: &TaskToken) {
        let bytes = token.clone().into_inner();
        let seq = String::from_utf8(bytes[b"optimistic_act_".len()..].to_vec()).unwrap();
        self.events
            .lock()
            .push(EnvEvent::Observed(seq.parse().unwrap()));
    }
    fn window_closed(&self, _: &str, _: u64) {
        self.events.lock().push(EnvEvent::WindowClosed);
    }
    fn window_dropped(&self, _: &str) -> Vec<String> {
        self.events.lock().push(EnvEvent::WindowDropped);
        vec![]
    }
}

/// What the test workflow does in response to an activation.
#[derive(Default)]
struct Response {
    commands: Vec<WFCommand>,
    exited: bool,
    active: bool,
}

fn in_block(commands: Vec<WFCommand>) -> Response {
    Response {
        commands,
        active: true,
        ..Default::default()
    }
}

fn after_block(commands: Vec<WFCommand>) -> Response {
    Response {
        commands,
        exited: true,
        ..Default::default()
    }
}

type Workflow = Rc<dyn Fn(&[String]) -> Response>;

fn result(seq: u32) -> Payload {
    format!("result-{seq}").as_json_payload().unwrap()
}

fn activity(seq: u32, cancellation_type: ActivityCancellationType) -> WFCommand {
    WFCommand::new(WFCommandVariant::AddActivity(ScheduleActivity {
        seq,
        activity_id: format!("act-{seq}"),
        activity_type: DEFAULT_ACTIVITY_TYPE.to_owned(),
        start_to_close_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        cancellation_type: cancellation_type as i32,
        speculative: true,
        ..Default::default()
    }))
}

fn spec(seq: u32) -> WFCommand {
    activity(seq, ActivityCancellationType::TryCancel)
}

fn other_queue(seq: u32) -> WFCommand {
    let mut cmd = spec(seq);
    if let WFCommandVariant::AddActivity(a) = &mut cmd.variant {
        a.task_queue = "elsewhere".to_owned();
    }
    cmd
}

fn normal(seq: u32) -> WFCommand {
    let mut cmd = spec(seq);
    if let WFCommandVariant::AddActivity(a) = &mut cmd.variant {
        a.speculative = false;
    }
    cmd
}

fn timer(seq: u32, speculative: bool) -> WFCommand {
    WFCommand::new(WFCommandVariant::AddTimer(StartTimer {
        seq,
        start_to_fire_timeout: Some(Duration::from_secs(1).try_into().unwrap()),
        speculative,
    }))
}

fn cancel(seq: u32) -> WFCommand {
    WFCommand::new(WFCommandVariant::RequestCancelActivity(
        RequestCancelActivity { seq },
    ))
}

fn cancel_timer(seq: u32) -> WFCommand {
    WFCommand::new(WFCommandVariant::CancelTimer(CancelTimer { seq }))
}

fn complete() -> WFCommand {
    WFCommand::new(WFCommandVariant::CompleteWorkflow(
        CompleteWorkflowExecution::default(),
    ))
}

fn job_names(
    jobs: &[temporalio_common::protos::coresdk::workflow_activation::WorkflowActivationJob],
) -> Vec<String> {
    jobs.iter()
        .map(|job| match job.variant.as_ref().unwrap() {
            Variant::InitializeWorkflow(_) => "init".to_owned(),
            Variant::SignalWorkflow(s) => format!("signal-{}", s.signal_name),
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

/// A compact description of a sent command, with its predicted ids and offsets.
fn describe(c: &Command) -> String {
    let ms = |d: &Option<prost_types::Duration>| {
        Duration::try_from(d.unwrap_or_default())
            .unwrap()
            .as_millis()
    };
    match c.attributes.as_ref().unwrap() {
        Attributes::ScheduleActivityTaskCommandAttributes(a) => match &a.optimistic_start {
            Some(o) => format!("schedule {} @{}", a.activity_id, ms(&o.start_offset)),
            None => format!("schedule {}", a.activity_id),
        },
        Attributes::StartTimerCommandAttributes(a) => match &a.optimistic_start {
            Some(o) => format!("timer {} @{}", a.timer_id, ms(&o.start_offset)),
            None => format!("timer {}", a.timer_id),
        },
        Attributes::RequestCancelActivityTaskCommandAttributes(a) => {
            format!("cancel {}", a.scheduled_event_id)
        }
        Attributes::CancelTimerCommandAttributes(a) => format!("cancel-timer {}", a.timer_id),
        Attributes::ResolveOptimisticActivityCommandAttributes(a) => format!(
            "resolve {} {} @{}",
            a.scheduled_event_id,
            match a.outcome.as_ref().unwrap() {
                Outcome::Completed(_) => "completed",
                Outcome::Canceled(_) => "canceled",
            },
            ms(&a.resolve_offset)
        ),
        Attributes::FireOptimisticTimerCommandAttributes(a) => {
            format!("fire {} @{}", a.timer_id, ms(&a.fire_offset))
        }
        Attributes::CompleteWorkflowExecutionCommandAttributes(_) => "complete".to_owned(),
        other => panic!("unexpected command {other:?}"),
    }
}

#[derive(Debug)]
struct ServerActivity {
    activity_id: String,
    started: bool,
    closed: bool,
    cancel_requested: bool,
}

/// Writes batches into history the way the server does, rejecting what the server rejects.
#[derive(Default)]
struct Server {
    activities: HashMap<i64, ServerActivity>,
    timers: HashMap<String, i64>,
}

impl Server {
    /// Records a workflow task completion. Returns the ids of the optimistic activities handed off.
    fn record(
        &mut self,
        t: &mut TestHistoryBuilder,
        commands: &[Command],
        wft_started: SystemTime,
    ) -> Vec<String> {
        t.add_workflow_task_completed();
        let mut batch = HashSet::new();
        let mut fired = HashSet::new();
        let mut buffered = vec![];
        for c in commands {
            match c.attributes.as_ref().unwrap() {
                Attributes::ScheduleActivityTaskCommandAttributes(a) => {
                    let id = t.add(ActivityTaskScheduledEventAttributes {
                        activity_id: a.activity_id.clone(),
                        activity_type: Some(ActivityType {
                            name: DEFAULT_ACTIVITY_TYPE.to_owned(),
                        }),
                        optimistic_start: a.optimistic_start.as_ref().map(|_| {
                            OptimisticStartInfo {
                                start_time: Some(wft_started.into()),
                            }
                        }),
                        ..Default::default()
                    });
                    if a.optimistic_start.is_some() {
                        batch.insert(id);
                    }
                    self.activities.insert(
                        id,
                        ServerActivity {
                            activity_id: a.activity_id.clone(),
                            started: a.optimistic_start.is_some(),
                            closed: false,
                            cancel_requested: false,
                        },
                    );
                }
                Attributes::StartTimerCommandAttributes(a) => {
                    let id = t.add(TimerStartedEventAttributes {
                        timer_id: a.timer_id.clone(),
                        start_to_fire_timeout: a.start_to_fire_timeout,
                        optimistic_start: a.optimistic_start.as_ref().map(|_| {
                            OptimisticStartInfo {
                                start_time: Some(wft_started.into()),
                            }
                        }),
                        ..Default::default()
                    });
                    self.timers.insert(a.timer_id.clone(), id);
                }
                Attributes::RequestCancelActivityTaskCommandAttributes(a) => {
                    let act = self
                        .activities
                        .get_mut(&a.scheduled_event_id)
                        .unwrap_or_else(|| panic!("cancel of unknown activity {a:?}"));
                    assert!(!act.closed, "cancel after resolution of {a:?}");
                    assert!(!act.cancel_requested, "duplicate cancel of {a:?}");
                    act.cancel_requested = true;
                    t.add(ActivityTaskCancelRequestedEventAttributes {
                        scheduled_event_id: a.scheduled_event_id,
                        ..Default::default()
                    });
                    if !act.started {
                        act.closed = true;
                        buffered.push(a.scheduled_event_id);
                    }
                }
                Attributes::CancelTimerCommandAttributes(a) => {
                    let started_event_id = self.timers[&a.timer_id];
                    t.add(TimerCanceledEventAttributes {
                        timer_id: a.timer_id.clone(),
                        started_event_id,
                        ..Default::default()
                    });
                }
                Attributes::ResolveOptimisticActivityCommandAttributes(a) => {
                    assert!(
                        batch.contains(&a.scheduled_event_id),
                        "resolution of an activity not started optimistically in this batch: {a:?}"
                    );
                    let act = self.activities.get_mut(&a.scheduled_event_id).unwrap();
                    assert!(!act.closed, "activity resolved twice: {a:?}");
                    act.closed = true;
                    let workflow_time = Some(OptimisticResolution {
                        workflow_time: Some(
                            (wft_started
                                + Duration::try_from(a.resolve_offset.unwrap_or_default())
                                    .unwrap())
                            .into(),
                        ),
                    });
                    let started = t.add(ActivityTaskStartedEventAttributes {
                        scheduled_event_id: a.scheduled_event_id,
                        optimistic_resolution: true,
                        ..Default::default()
                    });
                    match a.outcome.as_ref().unwrap() {
                        Outcome::Completed(c) => t.add(ActivityTaskCompletedEventAttributes {
                            scheduled_event_id: a.scheduled_event_id,
                            started_event_id: started,
                            result: c.result.clone(),
                            optimistic_resolution: workflow_time,
                            ..Default::default()
                        }),
                        Outcome::Canceled(c) => {
                            assert!(act.cancel_requested, "canceled without a cancel request");
                            t.add(ActivityTaskCanceledEventAttributes {
                                scheduled_event_id: a.scheduled_event_id,
                                started_event_id: started,
                                details: c.details.clone(),
                                optimistic_resolution: workflow_time,
                                ..Default::default()
                            })
                        }
                    };
                }
                Attributes::FireOptimisticTimerCommandAttributes(a) => {
                    assert!(fired.insert(a.timer_id.clone()), "timer fired twice");
                    t.add(TimerFiredEventAttributes {
                        timer_id: a.timer_id.clone(),
                        started_event_id: self.timers[&a.timer_id],
                        optimistic_resolution: Some(OptimisticResolution {
                            workflow_time: Some(
                                (wft_started
                                    + Duration::try_from(a.fire_offset.unwrap_or_default())
                                        .unwrap())
                                .into(),
                            ),
                        }),
                    });
                }
                Attributes::CompleteWorkflowExecutionCommandAttributes(_) => {
                    t.add_workflow_execution_completed();
                }
                other => panic!("unexpected command {other:?}"),
            }
        }
        for id in buffered {
            t.add(ActivityTaskCanceledEventAttributes {
                scheduled_event_id: id,
                ..Default::default()
            });
        }
        batch
            .into_iter()
            .filter(|id| !self.activities[id].closed)
            .map(|id| self.activities[&id].activity_id.clone())
            .collect()
    }

    /// A handed off activity reports its outcome through the normal RPC.
    fn complete_activity(&mut self, t: &mut TestHistoryBuilder, activity_id: &str, seq: u32) {
        let (id, act) = self
            .activities
            .iter_mut()
            .find(|(_, a)| a.activity_id == activity_id)
            .unwrap();
        assert!(!act.closed);
        act.closed = true;
        let id = *id;
        let started = t.add(ActivityTaskStartedEventAttributes {
            scheduled_event_id: id,
            ..Default::default()
        });
        t.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id: id,
            started_event_id: started,
            result: vec![result(seq)].into_payloads(),
            ..Default::default()
        });
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Observed {
    jobs: Vec<String>,
    timestamp: SystemTime,
}

/// A cached run executing speculation windows.
struct Live {
    wfm: WorkflowManager,
    env: Arc<FakeEnv>,
    history: TestHistoryBuilder,
    server: Server,
    workflow: Workflow,
    wft_number: usize,
    wft_started: SystemTime,
    observed: Vec<Observed>,
    /// Set once lang was given an activation it hasn't responded to.
    outstanding: Option<Vec<String>>,
}

fn manager(history: &TestHistoryBuilder, env: Option<Arc<dyn OptimisticEnv>>) -> WorkflowManager {
    WorkflowManager::new(RunBasics {
        worker_config: test_worker_cfg().build().unwrap().into(),
        optimistic_env: env,
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

impl Live {
    fn new(workflow: impl Fn(&[String]) -> Response + 'static) -> Self {
        let workflow: Workflow = Rc::new(workflow);
        let mut history = TestHistoryBuilder::default();
        history.add_by_type(EventType::WorkflowExecutionStarted);
        history.add_workflow_task_scheduled_and_started();
        let wft_started = history
            .last_event()
            .unwrap()
            .event_time
            .unwrap()
            .try_into()
            .unwrap();
        let env = FakeEnv::new();
        let mut wfm = manager(&history, Some(env.clone()));
        wfm.machines.set_wft_received_at(env.now());
        Self {
            wfm,
            env,
            history,
            server: Default::default(),
            workflow,
            wft_number: 1,
            wft_started,
            observed: vec![],
            outstanding: None,
        }
    }

    /// Gives lang the next activation without responding yet.
    fn activate(&mut self) -> Vec<String> {
        assert!(self.outstanding.is_none());
        let act = self.wfm.get_next_activation().unwrap();
        assert!(!act.jobs.is_empty(), "no activation to deliver");
        let jobs = job_names(&act.jobs);
        if !act.is_replaying {
            self.observed.push(Observed {
                jobs: jobs.clone(),
                timestamp: act.timestamp.unwrap().try_into().unwrap(),
            });
        }
        self.outstanding = Some(jobs.clone());
        jobs
    }

    /// Lang responds to the outstanding activation, as the workflow decides.
    fn respond(&mut self) {
        let jobs = self.outstanding.take().expect("no activation outstanding");
        let resp = (self.workflow)(&jobs);
        self.wfm
            .machines
            .set_speculative_block_state(resp.exited, resp.active);
        self.wfm.push_commands_and_iterate(resp.commands).unwrap();
    }

    fn step(&mut self) -> Vec<String> {
        let jobs = self.activate();
        self.respond();
        jobs
    }

    fn advance(&self, ms: u64) {
        self.env.advance(ms);
    }

    fn generation(&self) -> u64 {
        *self.env.generation.lock()
    }

    fn outcome(&mut self, seq: u32, status: Status) {
        let generation = self.generation();
        self.wfm
            .notify_of_local_result(LocalResolution::Optimistic(OptimisticResult {
                generation,
                kind: OptimisticResultKind::Activity {
                    seq,
                    token: token(seq),
                    status: Box::new(status),
                },
            }))
            .unwrap();
    }

    fn activity_completes(&mut self, seq: u32) {
        self.outcome(
            seq,
            Status::Completed(Success {
                result: Some(result(seq)),
            }),
        );
    }

    fn activity_cancelled(&mut self, seq: u32) {
        self.outcome(
            seq,
            Status::Cancelled(Cancellation {
                failure: Some(Failure {
                    failure_info: Some(FailureInfo::CanceledFailureInfo(
                        CanceledFailureInfo::default(),
                    )),
                    ..Default::default()
                }),
            }),
        );
    }

    fn timer_fires(&mut self, seq: u32) {
        let generation = self.generation();
        self.wfm
            .notify_of_local_result(LocalResolution::Optimistic(OptimisticResult {
                generation,
                kind: OptimisticResultKind::TimerFired { seq },
            }))
            .unwrap();
    }

    fn deadline(&mut self) {
        let generation = self.generation();
        self.wfm
            .notify_of_local_result(LocalResolution::Optimistic(OptimisticResult {
                generation,
                kind: OptimisticResultKind::Deadline,
            }))
            .unwrap();
    }

    /// Whether Core would send the workflow task completion now.
    fn completion_due(&self) -> bool {
        self.outstanding.is_none()
            && !self.wfm.machines.has_pending_jobs()
            && !self.wfm.machines.optimistic_window_wants_to_stay_open()
    }

    /// Sends the completion and has the server record it. Returns the commands sent and the ids
    /// of the activities handed off.
    fn send_completion(&mut self) -> (Vec<String>, Vec<String>) {
        assert!(self.completion_due(), "the completion isn't due yet");
        let commands: Vec<_> = self.wfm.machines.get_commands().collect();
        self.wfm.wft_completion_reported();
        let handed_off = self
            .server
            .record(&mut self.history, &commands, self.wft_started);
        (commands.iter().map(describe).collect(), handed_off)
    }

    /// The server hands the cached run its next workflow task, after the given events.
    fn next_wft(&mut self, before: impl FnOnce(&mut TestHistoryBuilder, &mut Server)) {
        before(&mut self.history, &mut self.server);
        self.history.add_workflow_task_scheduled_and_started();
        self.wft_started = self
            .history
            .last_event()
            .unwrap()
            .event_time
            .unwrap()
            .try_into()
            .unwrap();
        self.wft_number += 1;
        self.wfm.machines.set_wft_received_at(self.env.now());
        let act = self
            .wfm
            .new_work_from_server(
                self.history.get_one_wft(self.wft_number).unwrap().into(),
                vec![],
            )
            .unwrap();
        let jobs = job_names(&act.jobs);
        self.observed.push(Observed {
            jobs: jobs.clone(),
            timestamp: act.timestamp.unwrap().try_into().unwrap(),
        });
        self.outstanding = Some(jobs);
    }

    /// Replays the full history from scratch, without a cache, and checks it observes exactly
    /// what the cached run did.
    fn replay_matches(&self) {
        let mut replay = manager(&self.history, None);
        let mut activations = vec![];
        loop {
            let act = replay.get_next_activation().unwrap();
            if act.jobs.is_empty() {
                break;
            }
            let jobs = job_names(&act.jobs);
            activations.push(Observed {
                jobs: jobs.clone(),
                timestamp: act.timestamp.unwrap().try_into().unwrap(),
            });
            replay
                .push_commands_and_iterate((self.workflow)(&jobs).commands)
                .unwrap();
        }
        assert_eq!(activations, self.observed);
    }
}

fn jobs(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

// ------------------------------------------------------------------------------------------------
// Scheduling and cancelling in the same activation
// ------------------------------------------------------------------------------------------------

fn same_activation_workflow(
    jobs: &[String],
    cancellation_type: ActivityCancellationType,
) -> Response {
    match jobs {
        [j] if j == "init" => in_block(vec![
            spec(1),
            activity(2, cancellation_type),
            cancel(2),
            spec(3),
            timer(4, true),
        ]),
        [j] if j == "act-2-cancelled" => in_block(vec![]),
        [j] if j == "act-1" => in_block(vec![]),
        [j] if j == "act-3" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    }
}

/// A command scheduled and cancelled in the same activation is dropped, as it is outside of a
/// speculative block: it's never dispatched and isn't part of the log, so the commands around
/// it get the event ids they will actually be recorded with.
#[rstest::rstest]
#[case::try_cancel(ActivityCancellationType::TryCancel)]
#[case::wait_cancellation_completed(ActivityCancellationType::WaitCancellationCompleted)]
#[case::abandon(ActivityCancellationType::Abandon)]
fn schedule_and_cancel_in_same_activation_drops_activity(
    #[case] cancellation_type: ActivityCancellationType,
) {
    let mut live = Live::new(move |j| same_activation_workflow(j, cancellation_type));
    live.step();
    assert_eq!(
        live.env.take_events(),
        [
            EnvEvent::StartActivity(1),
            EnvEvent::StartActivity(3),
            EnvEvent::StartTimer(4)
        ]
    );
    // Cancelling an activity that never left Core resolves it right away, whatever the type
    assert_eq!(live.step(), jobs(&["act-2-cancelled"]));
    live.advance(10);
    live.activity_completes(1);
    assert_eq!(live.step(), jobs(&["act-1"]));
    live.advance(10);
    live.activity_completes(3);
    assert_eq!(live.step(), jobs(&["act-3"]));
    let (log, handed_off) = live.send_completion();
    assert_eq!(
        log,
        [
            "schedule act-1 @0",
            "schedule act-3 @0",
            "timer 4 @0",
            "resolve 5 completed @10",
            "resolve 6 completed @20",
            "complete"
        ]
    );
    assert!(handed_off.is_empty());
    live.replay_matches();
}

fn same_activation_timer_workflow(jobs: &[String]) -> Response {
    match jobs {
        [j] if j == "init" => in_block(vec![spec(1), timer(2, true), cancel_timer(2), spec(3)]),
        [j] if j == "act-1" => in_block(vec![]),
        [j] if j == "act-3" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    }
}

#[test]
fn start_and_cancel_timer_in_same_activation_drops_timer() {
    let mut live = Live::new(same_activation_timer_workflow);
    live.step();
    assert_eq!(
        live.env.take_events(),
        [EnvEvent::StartActivity(1), EnvEvent::StartActivity(3)]
    );
    live.advance(5);
    live.activity_completes(1);
    assert_eq!(live.step(), jobs(&["act-1"]));
    live.activity_completes(3);
    assert_eq!(live.step(), jobs(&["act-3"]));
    let (log, _) = live.send_completion();
    assert_eq!(
        log,
        [
            "schedule act-1 @0",
            "schedule act-3 @0",
            "resolve 5 completed @5",
            "resolve 6 completed @5",
            "complete"
        ]
    );
    live.replay_matches();
}

fn signal(name: &str) -> impl FnOnce(&mut TestHistoryBuilder, &mut Server) {
    let name = name.to_owned();
    move |t, _| t.add_we_signaled(&name, vec![])
}

// ------------------------------------------------------------------------------------------------
// Cancelling once the outcome is known locally
// ------------------------------------------------------------------------------------------------

/// The workflow cancels an optimistic activity whose outcome Core already has, but hasn't
/// delivered yet, or gets the outcome right after cancelling. With TryCancel the workflow already
/// observed the cancellation, so the outcome isn't resolved in the log and goes through the
/// handoff. With WaitCancellationCompleted the workflow observes the outcome, a completion after
/// the cancel request.
#[rstest::rstest]
fn cancel_with_outcome_known_locally(
    #[values(
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::WaitCancellationCompleted
    )]
    cancellation_type: ActivityCancellationType,
    #[values(true, false)] outcome_before_cancel: bool,
) {
    let mut live = Live::new(move |jobs| match jobs {
        [j] if j == "init" => in_block(vec![activity(1, cancellation_type), spec(2)]),
        [j] if j == "act-2" => in_block(vec![cancel(1)]),
        [j] if j == "act-1-cancelled" => in_block(vec![]),
        [j] if j == "act-1" || j == "signal-done" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    live.advance(10);
    live.activity_completes(2);
    assert_eq!(live.activate(), jobs(&["act-2"]));
    live.advance(10);
    if outcome_before_cancel {
        // Arrives while lang works on the activation, so it's applied after the cancel
        live.activity_completes(1);
    }
    live.respond();
    if !outcome_before_cancel {
        live.activity_completes(1);
    }
    let mut events = live.env.take_events();
    let observed_1 = events.last() == Some(&EnvEvent::Observed(1));
    if observed_1 {
        events.pop();
    }
    assert_eq!(
        events,
        [
            EnvEvent::StartActivity(1),
            EnvEvent::StartActivity(2),
            EnvEvent::Observed(2),
            EnvEvent::CancelActivity(1)
        ]
    );
    assert_eq!(
        observed_1,
        cancellation_type == ActivityCancellationType::WaitCancellationCompleted
    );
    let base = [
        "schedule act-1 @0",
        "schedule act-2 @0",
        "resolve 6 completed @10",
        "cancel 5",
    ];
    match cancellation_type {
        ActivityCancellationType::TryCancel => {
            assert_eq!(live.step(), jobs(&["act-1-cancelled"]));
            let (log, handed_off) = live.send_completion();
            assert_eq!(log, base);
            assert_eq!(handed_off, ["act-1"]);
            assert!(!live.env.take_events().contains(&EnvEvent::Observed(1)));
            live.next_wft(|t, server| {
                server.complete_activity(t, "act-1", 1);
                t.add_we_signaled("done", vec![]);
            });
            live.respond();
            let (log, _) = live.send_completion();
            assert_eq!(log, ["complete"]);
        }
        _ => {
            assert_eq!(live.step(), jobs(&["act-1"]));
            let (log, handed_off) = live.send_completion();
            assert_eq!(
                log,
                [&base[..], &["resolve 5 completed @20", "complete"]].concat()
            );
            assert!(handed_off.is_empty());
        }
    }
    live.replay_matches();
}

// ------------------------------------------------------------------------------------------------
// Timers
// ------------------------------------------------------------------------------------------------

/// The local timer fires, and the firing is waiting to be delivered, when the workflow cancels
/// the timer. The firing must not be delivered or logged.
#[rstest::rstest]
fn cancel_timer_racing_its_firing(#[values(true, false)] firing_deferred: bool) {
    let mut live = Live::new(|jobs| match jobs {
        [j] if j == "init" => in_block(vec![timer(1, true), timer(2, true)]),
        [j] if j == "timer-2" => in_block(vec![cancel_timer(1)]),
        [j] if j == "signal-done" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    live.advance(10);
    live.timer_fires(2);
    if firing_deferred {
        // Fires while lang works on the activation
        assert_eq!(live.activate(), jobs(&["timer-2"]));
        live.timer_fires(1);
        live.respond();
    } else {
        // Fires while timer 2's firing waits to be delivered
        live.timer_fires(1);
        assert_eq!(live.step(), jobs(&["timer-2"]));
    }
    assert!(live.env.take_events().contains(&EnvEvent::CancelTimer(1)));
    let (log, _) = live.send_completion();
    assert_eq!(
        log,
        ["timer 1 @0", "timer 2 @0", "fire 2 @10", "cancel-timer 1"]
    );
    live.next_wft(signal("done"));
    live.respond();
    live.send_completion();
    live.replay_matches();
}

/// Cancelling an activity and a timer together, as a cancellation scope would.
#[test]
fn cancel_activity_and_timer_together() {
    let mut live = Live::new(|jobs| match jobs {
        [j] if j == "init" => in_block(vec![spec(1), timer(2, true), timer(3, true)]),
        [j] if j == "timer-3" => in_block(vec![cancel(1), cancel_timer(2)]),
        [j] if j == "act-1-cancelled" => in_block(vec![]),
        [j] if j == "signal-done" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    live.advance(10);
    live.timer_fires(3);
    assert_eq!(live.step(), jobs(&["timer-3"]));
    assert_eq!(live.step(), jobs(&["act-1-cancelled"]));
    let events = live.env.take_events();
    assert!(events.contains(&EnvEvent::CancelActivity(1)));
    assert!(events.contains(&EnvEvent::CancelTimer(2)));
    let (log, handed_off) = live.send_completion();
    assert_eq!(
        log,
        [
            "schedule act-1 @0",
            "timer 2 @0",
            "timer 3 @0",
            "fire 3 @10",
            "cancel 5",
            "cancel-timer 2"
        ]
    );
    assert_eq!(handed_off, ["act-1"]);
    live.next_wft(|t, server| {
        server.complete_activity(t, "act-1", 1);
        t.add_we_signaled("done", vec![]);
    });
    live.respond();
    live.send_completion();
    live.replay_matches();
}

// ------------------------------------------------------------------------------------------------
// Cancelling around checkpoints
// ------------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Trigger {
    BlockExit,
    NonDeferrable,
    Deadline,
}

/// The activation that cancels an optimistic activity also ends the window.
#[rstest::rstest]
fn cancel_in_checkpointing_activation(
    #[values(Trigger::BlockExit, Trigger::NonDeferrable, Trigger::Deadline)] trigger: Trigger,
) {
    let mut live = Live::new(move |jobs| {
        let respond = |commands| match trigger {
            Trigger::BlockExit => after_block(commands),
            _ => in_block(commands),
        };
        match jobs {
            [j] if j == "init" => in_block(vec![spec(1), spec(2), timer(3, true)]),
            [j] if j == "timer-3" => match trigger {
                Trigger::NonDeferrable => respond(vec![cancel(1), timer(4, false)]),
                _ => respond(vec![cancel(1)]),
            },
            [j] if j == "act-1-cancelled" => respond(vec![]),
            [j] if j == "act-2" => after_block(vec![complete()]),
            other => panic!("unexpected jobs {other:?}"),
        }
    });
    live.step();
    live.advance(10);
    live.timer_fires(3);
    assert_eq!(live.activate(), jobs(&["timer-3"]));
    if let Trigger::Deadline = trigger {
        live.deadline();
    }
    live.respond();
    assert_eq!(live.step(), jobs(&["act-1-cancelled"]));
    let (log, mut handed_off) = live.send_completion();
    let mut expected = vec![
        "schedule act-1 @0",
        "schedule act-2 @0",
        "timer 3 @0",
        "fire 3 @10",
        "cancel 5",
    ];
    if let Trigger::NonDeferrable = trigger {
        expected.push("timer 4");
    }
    assert_eq!(log, expected);
    handed_off.sort();
    assert_eq!(handed_off, ["act-1", "act-2"]);
    live.next_wft(|t, server| {
        server.complete_activity(t, "act-2", 2);
        server.complete_activity(t, "act-1", 1);
    });
    live.respond();
    live.send_completion();
    live.replay_matches();
}

/// After earlier optimistic starts, an activation's speculative command can't run optimistically
/// and goes out normally, ending the window, along with a cancel of an optimistic activity.
#[test]
fn cancel_in_activation_falling_back_to_normal_commands() {
    let mut live = Live::new(|jobs| match jobs {
        [j] if j == "init" => in_block(vec![spec(1), timer(2, true)]),
        [j] if j == "timer-2" => in_block(vec![other_queue(3), cancel(1)]),
        [j] if j == "act-1-cancelled" => in_block(vec![]),
        [j] if j == "act-3" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    live.advance(10);
    live.timer_fires(2);
    assert_eq!(live.step(), jobs(&["timer-2"]));
    assert_eq!(live.step(), jobs(&["act-1-cancelled"]));
    let events = live.env.take_events();
    assert!(!events.contains(&EnvEvent::StartActivity(3)));
    assert!(events.contains(&EnvEvent::CancelActivity(1)));
    let (log, handed_off) = live.send_completion();
    assert_eq!(
        log,
        [
            "schedule act-1 @0",
            "timer 2 @0",
            "fire 2 @10",
            "schedule act-3",
            "cancel 5"
        ]
    );
    assert_eq!(handed_off, ["act-1"]);
    live.next_wft(|t, server| {
        server.complete_activity(t, "act-3", 3);
        server.complete_activity(t, "act-1", 1);
    });
    live.respond();
    live.send_completion();
    live.replay_matches();
}

/// Once handed off, an optimistic activity is cancelled like any other: a normal cancel request
/// in a later workflow task, which reaches the local execution through the server.
#[test]
fn cancel_after_handoff_is_a_normal_cancel() {
    let mut live = Live::new(|jobs| match jobs {
        [j] if j == "init" => in_block(vec![spec(1), timer(2, true)]),
        [j] if j == "timer-2" => after_block(vec![]),
        [j] if j == "signal-cancel" => Response {
            commands: vec![cancel(1)],
            ..Default::default()
        },
        [j] if j == "act-1-cancelled" => Response {
            commands: vec![complete()],
            ..Default::default()
        },
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    live.advance(10);
    live.timer_fires(2);
    assert_eq!(live.step(), jobs(&["timer-2"]));
    let (log, handed_off) = live.send_completion();
    assert_eq!(log, ["schedule act-1 @0", "timer 2 @0", "fire 2 @10"]);
    assert_eq!(handed_off, ["act-1"]);
    live.env.take_events();
    live.next_wft(signal("cancel"));
    live.respond();
    assert_eq!(live.step(), jobs(&["act-1-cancelled"]));
    assert!(
        !live
            .env
            .take_events()
            .contains(&EnvEvent::CancelActivity(1))
    );
    let (log, _) = live.send_completion();
    assert_eq!(log, ["cancel 5", "complete"]);
    live.replay_matches();
}

/// An abandoned optimistic activity isn't cancelled locally and its outcome isn't logged. It
/// completes through the handoff, and its late events are ignored.
#[test]
fn abandon_hands_off_and_ignores_late_events() {
    let mut live = Live::new(|jobs| match jobs {
        [j] if j == "init" => in_block(vec![
            activity(1, ActivityCancellationType::Abandon),
            timer(2, true),
        ]),
        [j] if j == "timer-2" => in_block(vec![cancel(1)]),
        [j] if j == "act-1-cancelled" => in_block(vec![]),
        [j] if j == "signal-done" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    live.advance(10);
    live.timer_fires(2);
    assert_eq!(live.step(), jobs(&["timer-2"]));
    live.activity_completes(1);
    assert_eq!(live.step(), jobs(&["act-1-cancelled"]));
    let events = live.env.take_events();
    assert!(!events.contains(&EnvEvent::CancelActivity(1)));
    assert!(!events.contains(&EnvEvent::Observed(1)));
    let (log, handed_off) = live.send_completion();
    assert_eq!(log, ["schedule act-1 @0", "timer 2 @0", "fire 2 @10"]);
    assert_eq!(handed_off, ["act-1"]);
    live.next_wft(|t, server| {
        server.complete_activity(t, "act-1", 1);
        t.add_we_signaled("done", vec![]);
    });
    live.respond();
    live.send_completion();
    live.replay_matches();
}

/// One activation cancels both an optimistic activity and a normal one scheduled in an earlier
/// workflow task. The normal cancel ends the window, and the server writes the normal activity's
/// cancellation after the batch, which doesn't move any predicted id.
#[test]
fn mixed_optimistic_and_normal_cancels() {
    let mut live = Live::new(|jobs| match jobs {
        [j] if j == "init" => Response {
            commands: vec![normal(1)],
            ..Default::default()
        },
        [j] if j == "signal-go" => in_block(vec![spec(2), timer(3, true)]),
        [j] if j == "timer-3" => in_block(vec![cancel(2), cancel(1)]),
        [a, b] if a == "act-2-cancelled" && b == "act-1-cancelled" => in_block(vec![]),
        [j] if j == "signal-done" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    let (log, _) = live.send_completion();
    assert_eq!(log, ["schedule act-1"]);
    live.next_wft(signal("go"));
    live.respond();
    live.advance(10);
    live.timer_fires(3);
    assert_eq!(live.step(), jobs(&["timer-3"]));
    assert_eq!(live.step(), jobs(&["act-2-cancelled", "act-1-cancelled"]));
    let (log, handed_off) = live.send_completion();
    assert_eq!(
        log,
        [
            "schedule act-2 @0",
            "timer 3 @0",
            "fire 3 @10",
            "cancel 10",
            "cancel 5"
        ]
    );
    assert_eq!(handed_off, ["act-2"]);
    live.next_wft(|t, server| {
        server.complete_activity(t, "act-2", 2);
        t.add_we_signaled("done", vec![]);
    });
    live.respond();
    live.send_completion();
    live.replay_matches();
}

/// Lang reports an optimistic activity cancelled after the workflow asked to cancel it. Only a
/// workflow waiting for the cancellation observes it, as a canceled resolution after the cancel
/// request.
#[rstest::rstest]
fn cancelled_outcome_after_cancel_request(
    #[values(
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::WaitCancellationCompleted
    )]
    cancellation_type: ActivityCancellationType,
) {
    let mut live = Live::new(move |jobs| match jobs {
        [j] if j == "init" => in_block(vec![activity(1, cancellation_type), timer(2, true)]),
        [j] if j == "timer-2" => in_block(vec![cancel(1)]),
        [j] if j == "act-1-cancelled" => match cancellation_type {
            ActivityCancellationType::TryCancel => in_block(vec![]),
            _ => after_block(vec![complete()]),
        },
        [j] if j == "signal-done" => after_block(vec![complete()]),
        other => panic!("unexpected jobs {other:?}"),
    });
    live.step();
    live.advance(10);
    live.timer_fires(2);
    assert_eq!(live.step(), jobs(&["timer-2"]));
    live.advance(10);
    live.activity_cancelled(1);
    assert_eq!(live.step(), jobs(&["act-1-cancelled"]));
    let (log, handed_off) = live.send_completion();
    let base = ["schedule act-1 @0", "timer 2 @0", "fire 2 @10", "cancel 5"];
    match cancellation_type {
        ActivityCancellationType::TryCancel => {
            assert_eq!(log, base);
            assert_eq!(handed_off, ["act-1"]);
            live.next_wft(|t, server| {
                server.complete_activity(t, "act-1", 1);
                t.add_we_signaled("done", vec![]);
            });
            live.respond();
            live.send_completion();
        }
        _ => {
            assert_eq!(
                log,
                [&base[..], &["resolve 5 canceled @20", "complete"]].concat()
            );
            assert!(handed_off.is_empty());
        }
    }
    live.replay_matches();
}
