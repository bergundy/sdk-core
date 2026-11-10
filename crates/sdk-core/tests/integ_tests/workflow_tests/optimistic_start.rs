//! Optimistic start (speculative blocks) against a server with the `optimistic_start` namespace
//! capability, driving Core directly the way a lang SDK would.

use crate::common::{CoreWfStarter, init_core_replay_preloaded};
use std::time::{Duration, SystemTime};
use temporalio_client::{NamespacedClient, grpc::WorkflowService};
use temporalio_common::protos::{
    TaskToken,
    coresdk::{
        ActivityHeartbeat, ActivityTaskCompletion, AsJsonPayloadExt,
        activity_result::{ActivityExecutionResult, activity_resolution},
        activity_task::{ActivityTask, activity_task},
        workflow_activation::{WorkflowActivation, workflow_activation_job},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, ScheduleActivity, StartTimer,
            workflow_command,
        },
        workflow_completion::{self, WorkflowActivationCompletion, workflow_activation_completion},
    },
    temporal::api::{
        common::v1::{Payload, RetryPolicy, WorkflowExecution},
        enums::v1::EventType,
        history::v1::{HistoryEvent, history_event::Attributes},
        workflowservice::v1::{DescribeWorkflowExecutionRequest, SignalWorkflowExecutionRequest},
    },
};
use temporalio_sdk_core::{
    PollError, Worker as CoreWorker, replay::HistoryForReplay,
    test_help::drain_pollers_and_shutdown,
};
use tonic::IntoRequest;

/// What the test workflow does in response to an activation.
#[derive(Default)]
struct Response {
    commands: Vec<workflow_command::Variant>,
    /// A speculative block exited during the activation
    exited: bool,
    /// A speculative block is still active at the end of the activation
    active: bool,
    /// The workflow completes with this activation
    done: bool,
}

fn job_names(act: &WorkflowActivation) -> Vec<String> {
    act.jobs
        .iter()
        .map(|j| match j.variant.as_ref().unwrap() {
            workflow_activation_job::Variant::InitializeWorkflow(_) => "init".to_owned(),
            workflow_activation_job::Variant::SignalWorkflow(s) => {
                format!("signal-{}", s.signal_name)
            }
            workflow_activation_job::Variant::FireTimer(t) => format!("timer-{}", t.seq),
            workflow_activation_job::Variant::ResolveActivity(r) => {
                match r.result.as_ref().unwrap().status.as_ref().unwrap() {
                    activity_resolution::Status::Completed(c) => {
                        assert_eq!(c.result, Some(result(r.seq)));
                        format!("act-{}", r.seq)
                    }
                    activity_resolution::Status::Failed(_) => format!("act-{}-failed", r.seq),
                    activity_resolution::Status::Cancelled(_) => {
                        format!("act-{}-cancelled", r.seq)
                    }
                    other => panic!("unexpected resolution {other:?}"),
                }
            }
            other => panic!("unexpected job {other:?}"),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
struct Observed {
    jobs: Vec<String>,
    /// Workflow time relative to the first activation's
    elapsed: Duration,
}

/// Runs the workflow's activations until it completes, returning those that weren't replays.
async fn run_workflow(
    core: &CoreWorker,
    respond: &mut impl FnMut(&[String]) -> Response,
) -> Vec<Observed> {
    let mut observed = vec![];
    let mut start: Option<SystemTime> = None;
    loop {
        let act = core.poll_workflow_activation().await.unwrap();
        if act.is_only_eviction() {
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
                .await
                .unwrap();
            continue;
        }
        let jobs = job_names(&act);
        let now: SystemTime = act.timestamp.unwrap().try_into().unwrap();
        let start = *start.get_or_insert(now);
        let resp = respond(&jobs);
        core.complete_workflow_activation(completion(&act.run_id, &resp))
            .await
            .unwrap();
        if !act.is_replaying {
            observed.push(Observed {
                jobs,
                elapsed: now.duration_since(start).unwrap(),
            });
        }
        if resp.done && !act.is_replaying {
            return observed;
        }
    }
}

/// Replays a workflow's history with Core's replayer, returning every activation.
async fn replay(
    task_queue: &str,
    events: Vec<HistoryEvent>,
    mut respond: impl FnMut(&[String]) -> Response,
) -> Vec<Observed> {
    let core = init_core_replay_preloaded(
        task_queue,
        [HistoryForReplay::new(events, task_queue.to_owned())],
    );
    let mut observed = vec![];
    let mut start: Option<SystemTime> = None;
    loop {
        match core.poll_workflow_activation().await {
            Err(PollError::ShutDown) => break,
            Ok(act) if act.is_only_eviction() => {
                core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
                    .await
                    .unwrap();
            }
            Ok(act) => {
                assert!(act.is_replaying);
                let jobs = job_names(&act);
                let now: SystemTime = act.timestamp.unwrap().try_into().unwrap();
                let start = *start.get_or_insert(now);
                let resp = respond(&jobs);
                core.complete_workflow_activation(completion(&act.run_id, &resp))
                    .await
                    .unwrap();
                observed.push(Observed {
                    jobs,
                    elapsed: now.duration_since(start).unwrap(),
                });
            }
            Err(e) => panic!("replay failed: {e:?}"),
        }
    }
    core.shutdown().await;
    observed
}

fn completion(run_id: &str, resp: &Response) -> WorkflowActivationCompletion {
    let mut success = workflow_completion::Success::from_variants(resp.commands.clone());
    success.speculative_block_exited = resp.exited;
    success.speculative_block_active = resp.active;
    WorkflowActivationCompletion {
        run_id: run_id.to_owned(),
        status: Some(workflow_activation_completion::Status::Successful(success)),
        ..Default::default()
    }
}

fn result(seq: u32) -> Payload {
    format!("result-{seq}").as_json_payload().unwrap()
}

fn speculative_activity(task_queue: &str, seq: u32) -> workflow_command::Variant {
    ScheduleActivity {
        seq,
        activity_id: format!("act-{seq}"),
        activity_type: "optimistic_act".to_owned(),
        task_queue: task_queue.to_owned(),
        start_to_close_timeout: Some(Duration::from_secs(30).try_into().unwrap()),
        cancellation_type: ActivityCancellationType::TryCancel as i32,
        arguments: vec![format!("input-{seq}").as_json_payload().unwrap()],
        speculative: true,
        ..Default::default()
    }
    .into()
}

fn speculative_timer(seq: u32, duration: Duration) -> workflow_command::Variant {
    StartTimer {
        seq,
        start_to_fire_timeout: Some(duration.try_into().unwrap()),
        speculative: true,
    }
    .into()
}

fn complete_workflow() -> Response {
    Response {
        commands: vec![CompleteWorkflowExecution::default().into()],
        done: true,
        ..Default::default()
    }
}

fn in_block(commands: Vec<workflow_command::Variant>) -> Response {
    Response {
        commands,
        active: true,
        ..Default::default()
    }
}

async fn poll_start(core: &CoreWorker) -> (TaskToken, String, u32) {
    let task: ActivityTask = core.poll_activity_task().await.unwrap();
    let Some(activity_task::Variant::Start(start)) = task.variant else {
        panic!("expected an activity start, got {task:?}");
    };
    (task.task_token.into(), start.activity_id, start.attempt)
}

async fn complete_activity(core: &CoreWorker, token: &TaskToken, seq: u32) {
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.clone().into_inner(),
        result: Some(ActivityExecutionResult::ok(result(seq))),
    })
    .await
    .unwrap();
}

/// A compact description of an event, with the optimistic flags it carries.
fn describe(e: &HistoryEvent) -> String {
    let ty = format!("{:?}", e.event_type());
    match e.attributes.as_ref().unwrap() {
        Attributes::ActivityTaskScheduledEventAttributes(a) => {
            format!(
                "{ty} {}{}",
                a.activity_id,
                flag(a.optimistic_start.is_some())
            )
        }
        Attributes::ActivityTaskStartedEventAttributes(a) => {
            format!(
                "{ty} {}{}",
                a.scheduled_event_id,
                flag(a.optimistic_resolution)
            )
        }
        Attributes::ActivityTaskCompletedEventAttributes(a) => format!(
            "{ty} {}{}",
            a.scheduled_event_id,
            flag(a.optimistic_resolution.is_some())
        ),
        Attributes::TimerStartedEventAttributes(a) => {
            format!("{ty} {}{}", a.timer_id, flag(a.optimistic_start.is_some()))
        }
        Attributes::TimerFiredEventAttributes(a) => format!(
            "{ty} {}{}",
            a.timer_id,
            flag(a.optimistic_resolution.is_some())
        ),
        _ => ty,
    }
}

fn flag(set: bool) -> &'static str {
    if set { " optimistic" } else { "" }
}

/// Workflow times recorded on resolution events, relative to the started event they belong to.
fn resolution_offsets(events: &[HistoryEvent]) -> Vec<Duration> {
    let mut wft_started = None;
    let mut out = vec![];
    for e in events {
        if e.event_type() == EventType::WorkflowTaskStarted {
            wft_started = e.event_time;
        }
        let resolution = match e.attributes.as_ref() {
            Some(Attributes::ActivityTaskCompletedEventAttributes(a)) => {
                a.optimistic_resolution.as_ref()
            }
            Some(Attributes::TimerFiredEventAttributes(a)) => a.optimistic_resolution.as_ref(),
            _ => None,
        };
        if let Some(r) = resolution {
            let at: SystemTime = r.workflow_time.unwrap().try_into().unwrap();
            let started: SystemTime = wft_started.unwrap().try_into().unwrap();
            out.push(at.duration_since(started).unwrap());
        }
    }
    out
}

/// The design's example: two activities resolving out of order, a timer, and an activity that is
/// still running when the block exits. Runs without a workflow cache, so the task after the
/// checkpoint replays the batch.
#[tokio::test]
async fn optimistic_start_example_flow() {
    let mut starter = CoreWfStarter::new("optimistic_start_example_flow");
    let core = starter.get_core_worker().await;
    core.validate().await.unwrap();
    starter.start_wf().await;
    let tq = starter.get_task_queue().to_owned();

    let workflow = {
        let tq = tq.clone();
        move |jobs: &[String]| -> Response {
            match jobs {
                [j] if j == "init" => in_block(vec![
                    speculative_activity(&tq, 1),
                    speculative_activity(&tq, 2),
                ]),
                [j] if j == "act-2" => in_block(vec![]),
                [j] if j == "act-1" => {
                    in_block(vec![speculative_timer(3, Duration::from_millis(200))])
                }
                [j] if j == "timer-3" => Response {
                    commands: vec![speculative_activity(&tq, 4)],
                    exited: true,
                    ..Default::default()
                },
                [j] if j == "act-4" => complete_workflow(),
                other => panic!("unexpected jobs {other:?}"),
            }
        }
    };

    let activities = async {
        let (t1, id1, _) = poll_start(&core).await;
        let (t2, id2, _) = poll_start(&core).await;
        assert_eq!((id1.as_str(), id2.as_str()), ("act-1", "act-2"));
        assert!(t1.is_optimistic_activity_task() && t2.is_optimistic_activity_task());
        // Returns once the workflow observed the outcome
        complete_activity(&core, &t2, 2).await;
        complete_activity(&core, &t1, 1).await;
        let (t4, id4, _) = poll_start(&core).await;
        assert_eq!(id4, "act-4");
        // Reported through RespondActivityTaskCompleted after the handoff
        complete_activity(&core, &t4, 4).await;
    };
    let mut wf = workflow.clone();
    let (observed, _) = tokio::join!(run_workflow(&core, &mut wf), activities);
    let jobs: Vec<_> = observed.iter().map(|o| o.jobs.clone()).collect();
    assert_eq!(
        jobs,
        [
            vec!["init"],
            vec!["act-2"],
            vec!["act-1"],
            vec!["timer-3"],
            vec!["act-4"]
        ]
    );
    assert!(observed[3].elapsed - observed[2].elapsed >= Duration::from_millis(200));

    let events = starter.get_history().await.events;
    let described: Vec<_> = events.iter().map(describe).collect();
    assert_eq!(
        described[3..13],
        [
            "WorkflowTaskCompleted",
            "ActivityTaskScheduled act-1 optimistic",
            "ActivityTaskScheduled act-2 optimistic",
            "ActivityTaskStarted 6 optimistic",
            "ActivityTaskCompleted 6 optimistic",
            "ActivityTaskStarted 5 optimistic",
            "ActivityTaskCompleted 5 optimistic",
            "TimerStarted 3 optimistic",
            "TimerFired 3 optimistic",
            "ActivityTaskScheduled act-4 optimistic",
        ],
        "{described:#?}"
    );
    assert!(described.contains(&"ActivityTaskStarted 13".to_owned()));
    assert!(described.contains(&"ActivityTaskCompleted 13".to_owned()));
    let offsets = resolution_offsets(&events);
    assert_eq!(offsets.len(), 3);
    assert!(offsets.is_sorted(), "{offsets:?}");
    assert!(offsets[2] - offsets[1] >= Duration::from_millis(200));

    let replayed = replay(&tq, events, workflow).await;
    assert_eq!(replayed, observed);
    drain_pollers_and_shutdown(&core).await;
}

/// An activity still running at the 80% deadline is handed off, carrying its latest heartbeat,
/// and completes through the normal RPC with the server's token.
#[tokio::test]
async fn optimistic_start_long_activity_handed_off() {
    let mut starter = CoreWfStarter::new("optimistic_start_long_activity");
    starter.workflow_options.task_timeout = Some(Duration::from_secs(2));
    starter.set_core_cfg_mutator(|c| c.max_cached_workflows = 10);
    let core = starter.get_core_worker().await;
    core.validate().await.unwrap();
    let run_id = starter.start_wf().await;
    let client = starter.get_core_client().await;
    let tq = starter.get_task_queue().to_owned();

    let workflow = {
        let tq = tq.clone();
        move |jobs: &[String]| -> Response {
            match jobs {
                [j] if j == "init" => in_block(vec![speculative_activity(&tq, 1)]),
                [j] if j == "act-1" => complete_workflow(),
                other => panic!("unexpected jobs {other:?}"),
            }
        }
    };
    let activities = async {
        let (token, _, _) = poll_start(&core).await;
        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: token.clone().into_inner(),
            details: vec!["progress".as_json_payload().unwrap()],
        });
        // Wait for the checkpoint at 80% of the 2s task timeout
        let pending = loop {
            let desc = client
                .connection()
                .clone()
                .describe_workflow_execution(
                    DescribeWorkflowExecutionRequest {
                        namespace: client.namespace(),
                        execution: Some(WorkflowExecution {
                            workflow_id: tq.clone(),
                            run_id: run_id.clone(),
                        }),
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();
            if let Some(p) = desc.pending_activities.into_iter().next() {
                break p;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(pending.activity_id, "act-1");
        assert_eq!(
            pending.heartbeat_details.unwrap().payloads,
            vec!["progress".as_json_payload().unwrap()]
        );
        complete_activity(&core, &token, 1).await;
    };
    let mut wf = workflow.clone();
    let (observed, _) = tokio::join!(run_workflow(&core, &mut wf), activities);
    let jobs: Vec<_> = observed.iter().map(|o| o.jobs.clone()).collect();
    assert_eq!(jobs, [vec!["init"], vec!["act-1"]]);

    let events = starter.get_history().await.events;
    let described: Vec<_> = events.iter().map(describe).collect();
    assert_eq!(described[4], "ActivityTaskScheduled act-1 optimistic");
    assert!(described.contains(&"ActivityTaskStarted 5".to_owned()));
    assert!(described.contains(&"ActivityTaskCompleted 5".to_owned()));
    assert!(!described.iter().any(|d| d == "WorkflowTaskTimedOut"));

    let replayed = replay(&tq, events, workflow).await;
    assert_eq!(
        replayed.iter().map(|o| &o.jobs).collect::<Vec<_>>(),
        jobs.iter().collect::<Vec<_>>()
    );
    drain_pollers_and_shutdown(&core).await;
}

/// A failed optimistic attempt checkpoints the window and is reported through the normal RPC, so
/// the server's retry policy schedules the next attempt, which runs as a normal activity.
#[tokio::test]
async fn optimistic_start_failure_is_retried_by_server() {
    let mut starter = CoreWfStarter::new("optimistic_start_failure_retried");
    starter.set_core_cfg_mutator(|c| c.max_cached_workflows = 10);
    let core = starter.get_core_worker().await;
    core.validate().await.unwrap();
    starter.start_wf().await;
    let tq = starter.get_task_queue().to_owned();

    let workflow = {
        let tq = tq.clone();
        move |jobs: &[String]| -> Response {
            match jobs {
                [j] if j == "init" => {
                    let mut cmd = speculative_activity(&tq, 1);
                    if let workflow_command::Variant::ScheduleActivity(a) = &mut cmd {
                        a.retry_policy = Some(RetryPolicy {
                            initial_interval: Some(Duration::from_millis(100).try_into().unwrap()),
                            backoff_coefficient: 1.0,
                            maximum_attempts: 2,
                            ..Default::default()
                        });
                    }
                    in_block(vec![cmd])
                }
                [j] if j == "act-1" => complete_workflow(),
                other => panic!("unexpected jobs {other:?}"),
            }
        }
    };
    let activities = async {
        let (token, _, attempt) = poll_start(&core).await;
        assert!(token.is_optimistic_activity_task());
        assert_eq!(attempt, 1);
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: token.into_inner(),
            result: Some(ActivityExecutionResult::fail("first attempt fails".into())),
        })
        .await
        .unwrap();
        let (token, id, attempt) = poll_start(&core).await;
        assert!(!token.is_optimistic_activity_task());
        assert_eq!((id.as_str(), attempt), ("act-1", 2));
        complete_activity(&core, &token, 1).await;
    };
    let mut wf = workflow.clone();
    let (observed, _) = tokio::join!(run_workflow(&core, &mut wf), activities);
    let jobs: Vec<_> = observed.iter().map(|o| o.jobs.clone()).collect();
    assert_eq!(jobs, [vec!["init"], vec!["act-1"]]);

    let events = starter.get_history().await.events;
    let started = events
        .iter()
        .find_map(|e| match e.attributes.as_ref() {
            Some(Attributes::ActivityTaskStartedEventAttributes(a)) => Some(a),
            _ => None,
        })
        .unwrap();
    assert_eq!(started.attempt, 2);
    assert!(!started.optimistic_resolution);
    assert!(started.last_failure.is_some());

    let replayed = replay(&tq, events, workflow).await;
    assert_eq!(
        replayed.iter().map(|o| &o.jobs).collect::<Vec<_>>(),
        jobs.iter().collect::<Vec<_>>()
    );
    drain_pollers_and_shutdown(&core).await;
}

/// A timer runs locally and moves workflow time forward, and its firing is recorded in the
/// same batch that completes the workflow.
#[tokio::test]
async fn optimistic_start_local_timer() {
    let mut starter = CoreWfStarter::new("optimistic_start_local_timer");
    let core = starter.get_core_worker().await;
    core.validate().await.unwrap();
    starter.start_wf().await;
    let tq = starter.get_task_queue().to_owned();

    let workflow = |jobs: &[String]| -> Response {
        match jobs {
            [j] if j == "init" => in_block(vec![speculative_timer(1, Duration::from_millis(300))]),
            [j] if j == "timer-1" => complete_workflow(),
            other => panic!("unexpected jobs {other:?}"),
        }
    };
    let mut wf = workflow;
    let observed = run_workflow(&core, &mut wf).await;
    assert_eq!(observed.len(), 2);
    assert!(observed[1].elapsed >= Duration::from_millis(300));

    let events = starter.get_history().await.events;
    let described: Vec<_> = events.iter().map(describe).collect();
    assert_eq!(
        described[3..],
        [
            "WorkflowTaskCompleted",
            "TimerStarted 1 optimistic",
            "TimerFired 1 optimistic",
            "WorkflowExecutionCompleted",
        ]
    );
    let replayed = replay(&tq, events, workflow).await;
    assert_eq!(replayed, observed);
    drain_pollers_and_shutdown(&core).await;
}

/// A signal arriving during a window is delivered after the checkpoint, in an activation of its
/// own, and replay groups it the same way.
#[tokio::test]
async fn optimistic_start_signal_during_window() {
    let mut starter = CoreWfStarter::new("optimistic_start_signal");
    starter.set_core_cfg_mutator(|c| c.max_cached_workflows = 10);
    let core = starter.get_core_worker().await;
    core.validate().await.unwrap();
    let run_id = starter.start_wf().await;
    let client = starter.get_core_client().await;
    let tq = starter.get_task_queue().to_owned();

    let workflow = |jobs: &[String]| -> Response {
        match jobs {
            [j] if j == "init" => in_block(vec![speculative_timer(1, Duration::from_secs(1))]),
            [j] if j == "timer-1" => Response {
                exited: true,
                ..Default::default()
            },
            [j] if j == "signal-sig" => complete_workflow(),
            other => panic!("unexpected jobs {other:?}"),
        }
    };
    let signaller = async {
        // The timer keeps the window open for a second, during which the signal arrives
        tokio::time::sleep(Duration::from_millis(300)).await;
        client
            .connection()
            .clone()
            .signal_workflow_execution(
                SignalWorkflowExecutionRequest {
                    namespace: client.namespace(),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: tq.clone(),
                        run_id: run_id.clone(),
                    }),
                    signal_name: "sig".to_owned(),
                    identity: "test".to_owned(),
                    request_id: uuid::Uuid::new_v4().to_string(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();
    };
    let mut wf = workflow;
    let (observed, _) = tokio::join!(run_workflow(&core, &mut wf), signaller);
    let jobs: Vec<_> = observed.iter().map(|o| o.jobs.clone()).collect();
    assert_eq!(jobs, [vec!["init"], vec!["timer-1"], vec!["signal-sig"]]);

    let events = starter.get_history().await.events;
    let described: Vec<_> = events.iter().map(describe).collect();
    let fired = described
        .iter()
        .position(|d| d == "TimerFired 1 optimistic")
        .unwrap();
    let signaled = described
        .iter()
        .position(|d| d == "WorkflowExecutionSignaled")
        .unwrap();
    assert!(signaled > fired, "{described:#?}");

    let replayed = replay(&tq, events, workflow).await;
    assert_eq!(
        replayed.iter().map(|o| &o.jobs).collect::<Vec<_>>(),
        jobs.iter().collect::<Vec<_>>()
    );
    drain_pollers_and_shutdown(&core).await;
}
