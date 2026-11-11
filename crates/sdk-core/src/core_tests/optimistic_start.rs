//! Speculation windows (optimistic start) driven through a worker with mocked server calls.

use crate::{
    ActivityHeartbeat, PollError, Worker,
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    test_help::{
        CounterRecordingMeter, MockPollCfg, MocksHolder, build_mock_pollers,
        drain_pollers_and_shutdown, mock_worker, schedule_local_activity_cmd,
    },
    worker::client::{
        MockWorkerClient, WorkflowTaskCompletion, mocks::mock_worker_client,
        wft_completion_command_pages,
    },
};
use futures_util::FutureExt;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_common::protos::{
    TaskToken,
    coresdk::{
        ActivityTaskCompletion, AsJsonPayloadExt, IntoPayloadsExt,
        activity_result::{ActivityExecutionResult, activity_resolution},
        activity_task::{ActivityCancelReason, ActivityTask, Cancel, activity_task},
        workflow_activation::{WorkflowActivation, workflow_activation_job},
        workflow_commands::{
            ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
            RequestCancelActivity, ScheduleActivity, StartTimer, workflow_command,
        },
        workflow_completion::{self, WorkflowActivationCompletion, workflow_activation_completion},
    },
    temporal::api::{
        command::v1::{
            Command, command::Attributes,
            resolve_optimistic_activity_command_attributes::Outcome as ResolveOutcome,
        },
        common::v1::Payload,
        namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
        workflowservice::v1::{
            DescribeNamespaceResponse, PollActivityTaskQueueResponse,
            RecordActivityTaskHeartbeatResponse, RespondActivityTaskCanceledResponse,
            RespondActivityTaskCompletedResponse, RespondActivityTaskFailedResponse,
            RespondWorkflowTaskCompletedResponse,
        },
    },
};
use tokio::{
    sync::mpsc::{UnboundedReceiver, unbounded_channel},
    task::JoinHandle,
};

type CompletionResponder =
    Box<dyn FnMut(&WorkflowTaskCompletion) -> RespondWorkflowTaskCompletedResponse + Send>;
type FallibleCompletionResponder = Box<
    dyn FnMut(
            &WorkflowTaskCompletion,
        ) -> Result<RespondWorkflowTaskCompletedResponse, tonic::Status>
        + Send,
>;

/// A worker for a single workflow whose first workflow task is handed out, with the namespace
/// supporting optimistic start. WFT completions are sent to the returned channel.
async fn optimistic_worker(
    t: TestHistoryBuilder,
    client: MockWorkerClient,
    mut respond: CompletionResponder,
) -> (Arc<Worker>, UnboundedReceiver<WorkflowTaskCompletion>) {
    optimistic_worker_with(
        t,
        client,
        Box::new(move |wtc| Ok(respond(wtc))),
        |_| {},
        optimistic_capabilities(),
    )
    .await
}

/// Like [optimistic_worker], also recording the worker's metrics.
async fn optimistic_worker_metered(
    t: TestHistoryBuilder,
    client: MockWorkerClient,
    mut respond: CompletionResponder,
) -> (
    Arc<Worker>,
    UnboundedReceiver<WorkflowTaskCompletion>,
    Arc<CounterRecordingMeter>,
) {
    let meter = Arc::new(CounterRecordingMeter::default());
    let m = meter.clone();
    let (core, rx) = optimistic_worker_with(
        t,
        client,
        Box::new(move |wtc| Ok(respond(wtc))),
        move |mock| mock.set_temporal_meter(m.into_temporal_meter()),
        optimistic_capabilities(),
    )
    .await;
    (core, rx, meter)
}

fn optimistic_capabilities() -> Capabilities {
    Capabilities {
        optimistic_start: true,
        ..Default::default()
    }
}

async fn optimistic_worker_with(
    t: TestHistoryBuilder,
    mut client: MockWorkerClient,
    mut respond: FallibleCompletionResponder,
    setup: impl FnOnce(&mut MocksHolder),
    capabilities: Capabilities,
) -> (Arc<Worker>, UnboundedReceiver<WorkflowTaskCompletion>) {
    client.expect_describe_namespace().returning(move || {
        Ok(DescribeNamespaceResponse {
            namespace_info: Some(NamespaceInfo {
                capabilities: Some(capabilities),
                ..Default::default()
            }),
            ..Default::default()
        })
    });
    let (tx, rx) = unbounded_channel();
    let mut mh = MockPollCfg::from_resp_batches("fake_wf_id", t, [1], client);
    mh.activity_responses = Some(vec![]);
    mh.completion_mock_fn = Some(Box::new(move |wtc| {
        let resp = respond(wtc);
        let _ = tx.send(wtc.clone());
        resp
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    setup(&mut mock);
    let core = mock_worker(mock);
    core.validate().await.unwrap();
    (Arc::new(core), rx)
}

/// Lang always keeps a workflow activation poll outstanding, which is what lets Core complete a
/// workflow task on its own, ex: when a window reaches its deadline.
fn idle_workflow_poller(core: Arc<Worker>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match core.poll_workflow_activation().await {
                Ok(a) if a.is_only_eviction() => core
                    .complete_workflow_activation(WorkflowActivationCompletion::empty(a.run_id))
                    .await
                    .unwrap(),
                Ok(a) => panic!("Unexpected activation {a:?}"),
                Err(PollError::ShutDown) => break,
                Err(e) => panic!("Unexpected poll error {e:?}"),
            }
        }
    })
}

async fn shutdown_with_poller(core: Arc<Worker>, poller: JoinHandle<()>) {
    core.initiate_shutdown();
    assert_matches!(
        core.poll_activity_task().await.unwrap_err(),
        PollError::ShutDown
    );
    poller.await.unwrap();
    core.shutdown().await;
}

fn history(wft_timeout: Duration) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(wft_timeout);
    t.add_workflow_task_scheduled_and_started();
    t
}

fn speculative_activity(core: &Worker, seq: u32) -> workflow_command::Variant {
    speculative_activity_with(core, seq, ActivityCancellationType::TryCancel)
}

fn speculative_activity_with(
    core: &Worker,
    seq: u32,
    cancellation_type: ActivityCancellationType,
) -> workflow_command::Variant {
    ScheduleActivity {
        seq,
        activity_id: format!("act-{seq}"),
        activity_type: "act".to_owned(),
        task_queue: core.get_config().task_queue.clone(),
        start_to_close_timeout: Some(Duration::from_secs(10).try_into().unwrap()),
        cancellation_type: cancellation_type as i32,
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

fn complete_workflow() -> workflow_command::Variant {
    CompleteWorkflowExecution::default().into()
}

fn completion(
    run_id: &str,
    commands: Vec<workflow_command::Variant>,
    block_exited: bool,
    block_active: bool,
) -> WorkflowActivationCompletion {
    let mut success = workflow_completion::Success::from_variants(commands);
    success.speculative_block_exited = block_exited;
    success.speculative_block_active = block_active;
    WorkflowActivationCompletion {
        run_id: run_id.to_owned(),
        status: Some(workflow_activation_completion::Status::Successful(success)),
        ..Default::default()
    }
}

fn result(seq: u32) -> Payload {
    format!("result-{seq}").as_json_payload().unwrap()
}

async fn poll_start(core: &Worker) -> (TaskToken, String) {
    let task: ActivityTask = core.poll_activity_task().await.unwrap();
    let Some(activity_task::Variant::Start(start)) = task.variant else {
        panic!("expected an activity start, got {task:?}");
    };
    let token: TaskToken = task.task_token.into();
    assert!(token.is_optimistic_activity_task());
    (token, start.activity_id)
}

async fn complete_activity(core: &Worker, token: &TaskToken, seq: u32) {
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.clone().into_inner(),
        result: Some(ActivityExecutionResult::ok(result(seq))),
    })
    .await
    .unwrap();
}

fn resolved_seq(act: &WorkflowActivation) -> Vec<String> {
    act.jobs
        .iter()
        .map(|j| match j.variant.as_ref().unwrap() {
            workflow_activation_job::Variant::ResolveActivity(r) => {
                match r.result.as_ref().unwrap().status.as_ref().unwrap() {
                    activity_resolution::Status::Completed(c) => {
                        assert_eq!(c.result, Some(result(r.seq)));
                        format!("act-{}", r.seq)
                    }
                    other => format!("act-{}-{other:?}", r.seq),
                }
            }
            workflow_activation_job::Variant::FireTimer(t) => format!("timer-{}", t.seq),
            other => format!("{other:?}"),
        })
        .collect()
}

/// A compact description of the commands sent at a checkpoint.
fn describe(commands: &[Command]) -> Vec<String> {
    commands
        .iter()
        .map(|c| match c.attributes.as_ref().unwrap() {
            Attributes::ScheduleActivityTaskCommandAttributes(a) => format!(
                "schedule {}{}",
                a.activity_id,
                if a.optimistic_start.is_some() {
                    " optimistic"
                } else {
                    ""
                }
            ),
            Attributes::StartTimerCommandAttributes(a) => format!(
                "timer {}{}",
                a.timer_id,
                if a.optimistic_start.is_some() {
                    " optimistic"
                } else {
                    ""
                }
            ),
            Attributes::ResolveOptimisticActivityCommandAttributes(a) => {
                match a.outcome.as_ref().unwrap() {
                    ResolveOutcome::Completed(_) => {
                        format!("resolve {} completed", a.scheduled_event_id)
                    }
                    ResolveOutcome::Canceled(_) => {
                        format!("resolve {} canceled", a.scheduled_event_id)
                    }
                }
            }
            Attributes::FireOptimisticTimerCommandAttributes(a) => format!("fire {}", a.timer_id),
            Attributes::RequestCancelActivityTaskCommandAttributes(a) => {
                format!("cancel {}", a.scheduled_event_id)
            }
            Attributes::CompleteWorkflowExecutionCommandAttributes(_) => "complete".to_owned(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Offsets of every optimistic entry in the log, which must never decrease.
fn offsets(commands: &[Command]) -> Vec<Duration> {
    commands
        .iter()
        .filter_map(|c| match c.attributes.as_ref().unwrap() {
            Attributes::ScheduleActivityTaskCommandAttributes(a) => {
                a.optimistic_start.as_ref().and_then(|o| o.start_offset)
            }
            Attributes::StartTimerCommandAttributes(a) => {
                a.optimistic_start.as_ref().and_then(|o| o.start_offset)
            }
            Attributes::ResolveOptimisticActivityCommandAttributes(a) => a.resolve_offset,
            Attributes::FireOptimisticTimerCommandAttributes(a) => a.fire_offset,
            _ => None,
        })
        .map(|d| d.try_into().unwrap())
        .collect()
}

fn handed_off(activity_id: &str, token: &[u8]) -> PollActivityTaskQueueResponse {
    PollActivityTaskQueueResponse {
        task_token: token.to_vec(),
        activity_id: activity_id.to_owned(),
        ..Default::default()
    }
}

/// The design's example: two activities resolving out of order, a timer, and an activity still
/// running when the window checkpoints, which is then completed through the normal RPC.
#[tokio::test]
async fn window_records_ordered_log_and_hands_off() {
    let mut client = mock_worker_client();
    client
        .expect_complete_activity_task()
        .withf(|token, res| {
            *token == TaskToken::from(b"real-4".to_vec())
                && res.as_ref().map(|p| p.payloads.clone()) == Some(vec![result(4)])
        })
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-4", b"real-4")],
            ..Default::default()
        }),
    )
    .await;

    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![
            speculative_activity(&core, 1),
            speculative_activity(&core, 2),
        ],
        false,
        true,
    ))
    .await
    .unwrap();

    let (token1, id1) = poll_start(&core).await;
    let (token2, id2) = poll_start(&core).await;
    assert_eq!((id1.as_str(), id2.as_str()), ("act-1", "act-2"));

    complete_activity(&core, &token2, 2).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["act-2"]);
    // a1's outcome can only be observed once lang responds to a2's activation
    let complete_a1 = complete_activity(&core, &token1, 1).boxed();
    let (_, act) = tokio::join!(complete_a1, async {
        core.complete_workflow_activation(completion(&run_id, vec![], false, true))
            .await
            .unwrap();
        core.poll_workflow_activation().await.unwrap()
    });
    assert_eq!(resolved_seq(&act), ["act-1"]);
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_timer(3, Duration::from_millis(50))],
        false,
        true,
    ))
    .await
    .unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-3"]);
    // The block exits with a3 still running, which checkpoints the window
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 4)],
        true,
        false,
    ))
    .await
    .unwrap();

    let wtc = completions.recv().await.unwrap();
    assert_eq!(
        describe(&wtc.commands),
        [
            "schedule act-1 optimistic",
            "schedule act-2 optimistic",
            "resolve 6 completed",
            "resolve 5 completed",
            "timer 3 optimistic",
            "fire 3",
            "schedule act-4 optimistic",
        ]
    );
    let offsets = offsets(&wtc.commands);
    assert!(offsets.is_sorted(), "{offsets:?}");
    assert!(offsets[5] - offsets[4] >= Duration::from_millis(50));
    assert!(!wtc.force_create_new_workflow_task);

    // a3 belongs to the server now and reports its outcome there
    let (token4, _) = poll_start(&core).await;
    complete_activity(&core, &token4, 4).await;

    drain_pollers_and_shutdown(&core).await;
}

/// Tasks for optimistic activities carry the timeouts the server fills in when it schedules an
/// activity, as on tasks it dispatches, since lang may rely on them.
#[tokio::test]
async fn optimistic_activity_tasks_have_server_timeouts() {
    let mut client = mock_worker_client();
    client
        .expect_complete_activity_task()
        .times(2)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let (core, _completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![
                handed_off("act-1", b"real-1"),
                handed_off("act-2", b"real-2"),
            ],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    let mut schedule_to_close_only = speculative_activity(&core, 2);
    if let workflow_command::Variant::ScheduleActivity(a) = &mut schedule_to_close_only {
        a.start_to_close_timeout = None;
        a.schedule_to_close_timeout = Some(Duration::from_secs(5).try_into().unwrap());
        a.heartbeat_timeout = Some(Duration::from_secs(30).try_into().unwrap());
    }
    // The block exits with both activities still running, so they're handed off
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 1), schedule_to_close_only],
        true,
        false,
    ))
    .await
    .unwrap();

    let secs = |seconds| Some(prost_types::Duration { seconds, nanos: 0 });
    // The workflow has no run timeout, so there's no schedule-to-close timeout unless set
    for (seq, schedule_to_close, start_to_close, heartbeat) in [(1, 0, 10, 0), (2, 5, 5, 5)] {
        let task = core.poll_activity_task().await.unwrap();
        let Some(activity_task::Variant::Start(start)) = task.variant else {
            panic!("expected an activity start, got {task:?}");
        };
        assert_eq!(start.activity_id, format!("act-{seq}"));
        assert_eq!(
            (
                start.schedule_to_close_timeout,
                start.start_to_close_timeout,
                start.heartbeat_timeout
            ),
            (
                secs(schedule_to_close),
                secs(start_to_close),
                secs(heartbeat)
            )
        );
        complete_activity(&core, &task.task_token.into(), seq).await;
    }

    drain_pollers_and_shutdown(&core).await;
}

/// A local activity scheduled while the window is open checkpoints the window.
#[tokio::test]
async fn non_deferrable_command_checkpoints() {
    let (core, mut completions, meter) = optimistic_worker_metered(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| RespondWorkflowTaskCompletedResponse::default()),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![
            speculative_activity(&core, 1),
            speculative_timer(2, Duration::from_millis(10)),
        ],
        false,
        true,
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-2"]);
    core.complete_workflow_activation(completion(&run_id, vec![complete_workflow()], false, true))
        .await
        .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(
        describe(&wtc.commands),
        [
            "schedule act-1 optimistic",
            "timer 2 optimistic",
            "fire 2",
            "complete"
        ]
    );
    // The server doesn't take over activities of a closed workflow, so the local execution is
    // cancelled and its outcome dropped.
    let (token, _) = poll_start(&core).await;
    let cancel = core.poll_activity_task().await.unwrap();
    assert_eq!(cancel.task_token, token.clone().into_inner());
    assert_matches!(cancel.variant, Some(activity_task::Variant::Cancel(_)));
    complete_activity(&core, &token, 1).await;

    drain_pollers_and_shutdown(&core).await;
    assert_eq!(
        meter.counter_total(
            "speculation_window_checkpoint",
            &[("checkpoint_reason", "workflow_completion")]
        ),
        1,
        "speculation_window_checkpoint"
    );
    assert_eq!(
        meter.counter_total("optimistic_activity_dropped", &[("activity_type", "act")]),
        1,
        "optimistic_activity_dropped"
    );
}

/// Activities still running at the deadline are handed off, with heartbeats held locally until
/// then and forwarded with the server's token afterwards.
#[tokio::test]
async fn deadline_checkpoints_and_heartbeats_follow_handoff() {
    let heartbeats = Arc::new(Mutex::new(vec![]));
    let hb = heartbeats.clone();
    let mut client = mock_worker_client();
    client
        .expect_record_activity_heartbeat()
        .returning(move |token, details| {
            hb.lock().unwrap().push((token, details));
            Ok(RecordActivityTaskHeartbeatResponse::default())
        });
    client
        .expect_complete_activity_task()
        .withf(|token, _| *token == TaskToken::from(b"real-1".to_vec()))
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let (core, mut completions, meter) = optimistic_worker_metered(
        history(Duration::from_secs(1)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 1)],
        false,
        true,
    ))
    .await
    .unwrap();
    let poller = idle_workflow_poller(core.clone());
    let (token, _) = poll_start(&core).await;
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: token.clone().into_inner(),
        details: vec!["before".as_json_payload().unwrap()],
    });

    let wtc = completions.recv().await.unwrap();
    assert_eq!(describe(&wtc.commands), ["schedule act-1 optimistic"]);
    let Some(Attributes::ScheduleActivityTaskCommandAttributes(attrs)) =
        wtc.commands[0].attributes.as_ref()
    else {
        unreachable!()
    };
    assert_eq!(
        attrs
            .optimistic_start
            .as_ref()
            .unwrap()
            .heartbeat_details
            .as_ref()
            .unwrap()
            .payloads,
        vec!["before".as_json_payload().unwrap()]
    );
    assert!(heartbeats.lock().unwrap().is_empty());

    // The handoff is processed right after the completion is sent
    let wait_for_handoff = async {
        loop {
            core.record_activity_heartbeat(ActivityHeartbeat {
                task_token: token.clone().into_inner(),
                details: vec!["after".as_json_payload().unwrap()],
            });
            if !heartbeats.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait_for_handoff)
        .await
        .unwrap();
    assert_eq!(
        heartbeats.lock().unwrap()[0].0,
        TaskToken::from(b"real-1".to_vec())
    );
    complete_activity(&core, &token, 1).await;

    shutdown_with_poller(core, poller).await;
    assert_eq!(
        meter.counter_total(
            "speculation_window_checkpoint",
            &[("checkpoint_reason", "deadline")]
        ),
        1,
        "speculation_window_checkpoint"
    );
    assert_eq!(
        meter.counter_total("optimistic_activity_handed_off", &[]),
        1,
        "optimistic_activity_handed_off"
    );
}

/// A failed optimistic activity checkpoints the window and is reported through the normal RPC,
/// so the server applies its retry policy.
#[tokio::test]
async fn failure_is_handed_off() {
    let mut client = mock_worker_client();
    client
        .expect_fail_activity_task()
        .withf(|token, _, _, _| *token == TaskToken::from(b"real-1".to_vec()))
        .times(1)
        .returning(|_, _, _, _| Ok(RespondActivityTaskFailedResponse::default()));
    let (core, mut completions, meter) = optimistic_worker_metered(
        history(Duration::from_secs(10)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 1)],
        false,
        true,
    ))
    .await
    .unwrap();
    let poller = idle_workflow_poller(core.clone());
    let (token, _) = poll_start(&core).await;
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.into_inner(),
        result: Some(ActivityExecutionResult::fail("boom".into())),
    })
    .await
    .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(describe(&wtc.commands), ["schedule act-1 optimistic"]);

    shutdown_with_poller(core, poller).await;
    assert_eq!(
        meter.counter_total(
            "speculation_window_checkpoint",
            &[("checkpoint_reason", "activity_failed")]
        ),
        1,
        "speculation_window_checkpoint"
    );
}

/// Async completion isn't possible with a made up token, so the attempt fails and the failure
/// goes through the normal handoff.
#[tokio::test]
async fn async_completion_fails_the_attempt() {
    let mut client = mock_worker_client();
    client
        .expect_fail_activity_task()
        .withf(|token, _, failure, _| {
            *token == TaskToken::from(b"real-1".to_vec())
                && failure
                    .as_ref()
                    .is_some_and(|f| f.message.contains("can't complete asynchronously"))
        })
        .times(1)
        .returning(|_, _, _, _| Ok(RespondActivityTaskFailedResponse::default()));
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 1)],
        false,
        true,
    ))
    .await
    .unwrap();
    let poller = idle_workflow_poller(core.clone());
    let (token, _) = poll_start(&core).await;
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.into_inner(),
        result: Some(ActivityExecutionResult::will_complete_async()),
    })
    .await
    .unwrap();
    completions.recv().await.unwrap();

    shutdown_with_poller(core, poller).await;
}

/// A speculative command that isn't on the workflow's task queue can't run optimistically, so
/// nothing is started and the commands are sent normally.
#[tokio::test]
async fn ineligible_command_disables_window() {
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| RespondWorkflowTaskCompletedResponse::default()),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    let mut other_queue = speculative_activity(&core, 2);
    if let workflow_command::Variant::ScheduleActivity(a) = &mut other_queue {
        a.task_queue = "elsewhere".to_owned();
    }
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 1), other_queue],
        false,
        true,
    ))
    .await
    .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(
        describe(&wtc.commands),
        ["schedule act-1", "schedule act-2"]
    );
    drain_pollers_and_shutdown(&core).await;
}

/// History records the window with the same event ids Core predicted, so replaying the
/// checkpoint's next task applies the resolutions without new jobs.
#[tokio::test]
async fn next_task_applies_batch_without_jobs() {
    let mut t = history(Duration::from_secs(10));
    let wft_started: std::time::SystemTime = t
        .last_event()
        .unwrap()
        .event_time
        .unwrap()
        .try_into()
        .unwrap();
    t.add_workflow_task_completed();
    let a1 = t.add(
        temporalio_common::protos::temporal::api::history::v1::ActivityTaskScheduledEventAttributes {
            activity_id: "act-1".to_owned(),
            activity_type: Some(temporalio_common::protos::temporal::api::common::v1::ActivityType {
                name: "act".to_owned(),
            }),
            optimistic_start: Some(
                temporalio_common::protos::temporal::api::history::v1::OptimisticStartInfo::default(),
            ),
            ..Default::default()
        },
    );
    let started = t.add(
        temporalio_common::protos::temporal::api::history::v1::ActivityTaskStartedEventAttributes {
            scheduled_event_id: a1,
            optimistic_resolution: true,
            ..Default::default()
        },
    );
    t.add(
        temporalio_common::protos::temporal::api::history::v1::ActivityTaskCompletedEventAttributes {
            scheduled_event_id: a1,
            started_event_id: started,
            result: vec![result(1)].into_payloads(),
            optimistic_resolution: Some(
                temporalio_common::protos::temporal::api::history::v1::OptimisticResolution {
                    workflow_time: Some((wft_started + Duration::from_millis(5)).into()),
                },
            ),
            ..Default::default()
        },
    );
    t.add_we_signaled("sig", vec![]);
    t.add_workflow_task_scheduled_and_started();
    assert_eq!(t.get_full_history_info().unwrap().wf_task_count(), 2);

    let mut client = mock_worker_client();
    client.expect_describe_namespace().returning(|| {
        Ok(DescribeNamespaceResponse {
            namespace_info: Some(NamespaceInfo {
                capabilities: Some(Capabilities {
                    optimistic_start: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    });
    let mut mh = MockPollCfg::from_resp_batches("fake_wf_id", t, [1, 2], client);
    mh.activity_responses = Some(vec![]);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    let core = mock_worker(mock);
    core.validate().await.unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 1)],
        false,
        true,
    ))
    .await
    .unwrap();
    let (token, _) = poll_start(&core).await;
    complete_activity(&core, &token, 1).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["act-1"]);
    core.complete_workflow_activation(completion(&run_id, vec![], true, false))
        .await
        .unwrap();
    // The next task only delivers the signal
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [j] if matches!(j.variant, Some(workflow_activation_job::Variant::SignalWorkflow(_)))
    );
    assert!(!act.is_replaying);
    core.complete_workflow_activation(completion(&run_id, vec![complete_workflow()], false, false))
        .await
        .unwrap();
    drain_pollers_and_shutdown(&core).await;
}

fn cancel_activity(seq: u32) -> workflow_command::Variant {
    RequestCancelActivity { seq }.into()
}

fn cancel_timer(seq: u32) -> workflow_command::Variant {
    CancelTimer { seq }.into()
}

/// A TryCancel cancellation of an optimistic activity references its predicted scheduled event
/// id and resolves the workflow immediately. The activity's own cancellation isn't observed, so it
/// is reported to the server after the handoff.
#[tokio::test]
async fn try_cancel_is_logged_and_reported_after_handoff() {
    let mut client = mock_worker_client();
    client
        .expect_cancel_activity_task()
        .withf(|token, _| *token == TaskToken::from(b"real-1".to_vec()))
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCanceledResponse::default()));
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![
            speculative_activity(&core, 1),
            speculative_timer(2, Duration::from_millis(10)),
        ],
        false,
        true,
    ))
    .await
    .unwrap();
    let (token, _) = poll_start(&core).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-2"]);
    core.complete_workflow_activation(completion(&run_id, vec![cancel_activity(1)], false, true))
        .await
        .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(resolved_seq(&act).as_slice(), [r] if r.contains("Cancelled"));
    core.complete_workflow_activation(completion(&run_id, vec![], false, true))
        .await
        .unwrap();
    // Nothing the workflow waits on is in flight anymore, so the window checkpoints
    let wtc = completions.recv().await.unwrap();
    assert_eq!(
        describe(&wtc.commands),
        [
            "schedule act-1 optimistic",
            "timer 2 optimistic",
            "fire 2",
            "cancel 5"
        ]
    );
    let cancel = core.poll_activity_task().await.unwrap();
    assert_eq!(cancel.task_token, token.clone().into_inner());
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.into_inner(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();

    drain_pollers_and_shutdown(&core).await;
}

/// A workflow waiting for the cancellation of an optimistic activity observes it inside the
/// window, recorded as a canceled resolution after the cancel request.
#[tokio::test]
async fn wait_cancellation_completed_resolves_in_window() {
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| RespondWorkflowTaskCompletedResponse::default()),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![
            speculative_activity_with(
                &core,
                1,
                ActivityCancellationType::WaitCancellationCompleted,
            ),
            speculative_timer(2, Duration::from_millis(10)),
        ],
        false,
        true,
    ))
    .await
    .unwrap();
    let (token, _) = poll_start(&core).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-2"]);
    core.complete_workflow_activation(completion(&run_id, vec![cancel_activity(1)], false, true))
        .await
        .unwrap();
    let cancel = core.poll_activity_task().await.unwrap();
    assert_matches!(cancel.variant, Some(activity_task::Variant::Cancel(_)));
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.into_inner(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(resolved_seq(&act).as_slice(), [r] if r.contains("Cancelled"));
    core.complete_workflow_activation(completion(&run_id, vec![complete_workflow()], true, false))
        .await
        .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(
        describe(&wtc.commands),
        [
            "schedule act-1 optimistic",
            "timer 2 optimistic",
            "fire 2",
            "cancel 5",
            "resolve 5 canceled",
            "complete"
        ]
    );
    drain_pollers_and_shutdown(&core).await;
}

/// A batch too large for one request goes out as a paginated completion when the namespace
/// supports it, with its commands in the same order.
#[tokio::test]
async fn large_batch_is_paginated() {
    let big = |seq: u32| Payload {
        data: vec![b'0' + seq as u8; 1536 * 1024],
        ..Default::default()
    };
    let (core, mut completions) = optimistic_worker_with(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| Ok(RespondWorkflowTaskCompletedResponse::default())),
        |_| {},
        Capabilities {
            optimistic_start: true,
            workflow_task_completion_pagination: true,
            ..Default::default()
        },
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    let commands = (1..=3)
        .map(|seq| {
            let mut cmd = speculative_activity(&core, seq);
            if let workflow_command::Variant::ScheduleActivity(a) = &mut cmd {
                a.arguments = vec![big(seq)];
            }
            cmd
        })
        .collect();
    core.complete_workflow_activation(completion(&run_id, commands, false, true))
        .await
        .unwrap();
    let mut tokens = vec![];
    for _ in 1..=3 {
        tokens.push(poll_start(&core).await.0);
    }
    for (ix, token) in tokens.into_iter().enumerate() {
        let seq = ix as u32 + 1;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: token.into_inner(),
            result: Some(ActivityExecutionResult::ok(big(seq + 3))),
        })
        .await
        .unwrap();
        let act = core.poll_workflow_activation().await.unwrap();
        assert_eq!(act.jobs.len(), 1);
        let cmds = if seq == 3 {
            vec![complete_workflow()]
        } else {
            vec![]
        };
        core.complete_workflow_activation(completion(&run_id, cmds, seq == 3, seq != 3))
            .await
            .unwrap();
    }
    let wtc = completions.recv().await.unwrap();
    assert!(wtc.pagination_enabled);
    assert_eq!(
        describe(&wtc.commands),
        [
            "schedule act-1 optimistic",
            "schedule act-2 optimistic",
            "schedule act-3 optimistic",
            "resolve 5 completed",
            "resolve 6 completed",
            "resolve 7 completed",
            "complete"
        ]
    );
    let pages = wft_completion_command_pages(wtc.commands.clone());
    assert!(
        pages.len() > 2,
        "expected several pages, got {}",
        pages.len()
    );
    assert_eq!(pages.concat(), wtc.commands);
    drain_pollers_and_shutdown(&core).await;
}

/// When the server rejects the checkpoint, local executions are cancelled, their outcomes are
/// dropped, and the batch is never sent again.
#[tokio::test]
async fn rejected_checkpoint_drops_window() {
    let meter = Arc::new(CounterRecordingMeter::default());
    let m = meter.clone();
    let (core, mut completions) = optimistic_worker_with(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| Err(tonic::Status::invalid_argument("UnhandledCommand"))),
        move |mock| mock.set_temporal_meter(m.into_temporal_meter()),
        optimistic_capabilities(),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![
            speculative_activity(&core, 1),
            speculative_timer(2, Duration::from_millis(10)),
        ],
        false,
        true,
    ))
    .await
    .unwrap();
    let (token, _) = poll_start(&core).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-2"]);
    core.complete_workflow_activation(completion(&run_id, vec![complete_workflow()], false, true))
        .await
        .unwrap();
    completions.recv().await.unwrap();
    let cancel = core.poll_activity_task().await.unwrap();
    assert_eq!(cancel.task_token, token.clone().into_inner());
    // No activity RPC is expected for the discarded outcome
    complete_activity(&core, &token, 1).await;
    drain_pollers_and_shutdown(&core).await;
    assert!(completions.try_recv().is_err());
    assert_eq!(
        meter.counter_total("optimistic_activity_dropped", &[]),
        1,
        "optimistic_activity_dropped"
    );
}

/// Exiting a block and entering the next in the same activation keeps the window going.
#[tokio::test]
async fn sequential_blocks_share_window() {
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| RespondWorkflowTaskCompletedResponse::default()),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 1)],
        false,
        true,
    ))
    .await
    .unwrap();
    let (token, _) = poll_start(&core).await;
    complete_activity(&core, &token, 1).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["act-1"]);
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_activity(&core, 2)],
        true,
        true,
    ))
    .await
    .unwrap();
    let (token, _) = poll_start(&core).await;
    complete_activity(&core, &token, 2).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["act-2"]);
    core.complete_workflow_activation(completion(&run_id, vec![complete_workflow()], true, false))
        .await
        .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(
        describe(&wtc.commands),
        [
            "schedule act-1 optimistic",
            "resolve 5 completed",
            "schedule act-2 optimistic",
            "resolve 8 completed",
            "complete"
        ]
    );
    drain_pollers_and_shutdown(&core).await;
}

/// A local activity scheduled while a window is open checkpoints it, requesting a new workflow
/// task like a local activity heartbeat does.
#[tokio::test]
async fn local_activity_checkpoints_window() {
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| RespondWorkflowTaskCompletedResponse::default()),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![speculative_timer(1, Duration::from_millis(10))],
        false,
        true,
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-1"]);
    core.complete_workflow_activation(completion(
        &run_id,
        vec![schedule_local_activity_cmd(
            2,
            "la-2",
            ActivityCancellationType::TryCancel,
            Duration::from_secs(10),
        )],
        true,
        false,
    ))
    .await
    .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(describe(&wtc.commands), ["timer 1 optimistic", "fire 1"]);
    assert!(wtc.force_create_new_workflow_task);
    let la = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: la.task_token,
        result: Some(ActivityExecutionResult::ok(result(2))),
    })
    .await
    .unwrap();
    drain_pollers_and_shutdown(&core).await;
}

/// Metrics follow a window from its optimistic starts to the checkpoint and handoff.
#[tokio::test]
async fn metrics_follow_window_lifecycle() {
    let mut client = mock_worker_client();
    client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let (core, mut completions, meter) = optimistic_worker_metered(
        history(Duration::from_secs(10)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![
            speculative_activity(&core, 1),
            speculative_activity(&core, 2),
            speculative_timer(3, Duration::from_millis(300)),
            speculative_timer(4, Duration::from_secs(10)),
        ],
        false,
        true,
    ))
    .await
    .unwrap();
    let (token1, _) = poll_start(&core).await;
    let (token2, _) = poll_start(&core).await;
    complete_activity(&core, &token2, 2).await;
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["act-2"]);
    core.complete_workflow_activation(completion(&run_id, vec![], false, true))
        .await
        .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-3"]);
    core.complete_workflow_activation(completion(&run_id, vec![cancel_timer(4)], true, false))
        .await
        .unwrap();
    completions.recv().await.unwrap();
    complete_activity(&core, &token1, 1).await;
    drain_pollers_and_shutdown(&core).await;

    let wf = ("workflow_type", DEFAULT_WORKFLOW_TYPE);
    let act_type = ("activity_type", "act");
    for (name, labels, expected) in [
        ("optimistic_activity_started", vec![wf, act_type], 2),
        (
            "optimistic_activity_observed",
            vec![wf, act_type, ("outcome", "completed")],
            1,
        ),
        ("optimistic_activity_handed_off", vec![act_type], 1),
        ("optimistic_activity_dropped", vec![], 0),
        ("optimistic_timer_started", vec![wf], 2),
        ("optimistic_timer_fired", vec![wf], 1),
        ("optimistic_timer_cancelled", vec![wf], 1),
        ("optimistic_timer_handed_off", vec![wf], 0),
        ("optimistic_start_fallback", vec![], 0),
        (
            "speculation_window_checkpoint",
            vec![wf, ("checkpoint_reason", "block_exit")],
            1,
        ),
    ] {
        assert_eq!(meter.counter_total(name, &labels), expected, "{name}");
    }
    assert_eq!(
        meter.durations("speculation_window_duration", &[wf]).len(),
        1
    );
}

/// Speculative commands that can't start optimistically are counted with the reason.
#[tokio::test]
async fn metrics_count_fallbacks() {
    let (core, mut completions, meter) = optimistic_worker_metered(
        history(Duration::from_secs(10)),
        mock_worker_client(),
        Box::new(|_| RespondWorkflowTaskCompletedResponse::default()),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let mut other_queue = speculative_activity(&core, 2);
    if let workflow_command::Variant::ScheduleActivity(a) = &mut other_queue {
        a.task_queue = "elsewhere".to_owned();
    }
    core.complete_workflow_activation(completion(
        &act.run_id,
        vec![
            speculative_activity(&core, 1),
            other_queue,
            speculative_timer(3, Duration::from_secs(1)),
        ],
        false,
        true,
    ))
    .await
    .unwrap();
    completions.recv().await.unwrap();
    drain_pollers_and_shutdown(&core).await;
    assert_eq!(
        meter.counter_total(
            "optimistic_start_fallback",
            &[("fallback_reason", "task_queue")]
        ),
        3
    );
    assert_eq!(meter.counter_total("optimistic_activity_started", &[]), 0);
    assert_eq!(meter.counter_total("speculation_window_checkpoint", &[]), 0);
}

fn activity_with_timeouts(
    core: &Worker,
    seq: u32,
    start_to_close: Duration,
    heartbeat: Option<Duration>,
) -> workflow_command::Variant {
    let mut cmd = speculative_activity(core, seq);
    if let workflow_command::Variant::ScheduleActivity(a) = &mut cmd {
        a.start_to_close_timeout = Some(start_to_close.try_into().unwrap());
        a.heartbeat_timeout = heartbeat.map(|h| h.try_into().unwrap());
    }
    cmd
}

/// A handed off activity is cancelled locally when the server would time it out: its
/// start-to-close timeout counts from the optimistic start, and its heartbeat timeout from the
/// handoff.
#[rstest::rstest]
#[case::start_to_close(Duration::from_secs(10), Duration::from_millis(700), None)]
#[case::heartbeat(
    Duration::from_secs(1),
    Duration::from_secs(10),
    Some(Duration::from_millis(300))
)]
#[tokio::test]
async fn handoff_arms_local_timeouts(
    #[case] wft_timeout: Duration,
    #[case] start_to_close: Duration,
    #[case] heartbeat: Option<Duration>,
) {
    let (core, mut completions) = optimistic_worker_with(
        history(wft_timeout),
        mock_worker_client(),
        Box::new(|_| {
            Ok(RespondWorkflowTaskCompletedResponse {
                optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
                ..Default::default()
            })
        }),
        |mock| mock.worker_cfg(|wc| wc.local_timeout_buffer_for_activities = Duration::ZERO),
        optimistic_capabilities(),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(completion(
        &act.run_id,
        vec![activity_with_timeouts(&core, 1, start_to_close, heartbeat)],
        false,
        true,
    ))
    .await
    .unwrap();
    let poller = idle_workflow_poller(core.clone());
    let (token, _) = poll_start(&core).await;
    let started = std::time::Instant::now();
    // The local timeout only starts with the handoff, at the checkpoint
    let (wtc, cancel) = tokio::join!(completions.recv(), core.poll_activity_task());
    let checkpointed = describe(&wtc.unwrap().commands);
    let cancel = cancel.unwrap();
    assert_eq!(checkpointed, ["schedule act-1 optimistic"]);
    assert_eq!(cancel.task_token, token.clone().into_inner());
    assert_matches!(
        cancel.variant,
        Some(activity_task::Variant::Cancel(Cancel { reason, .. }))
            if reason == ActivityCancelReason::TimedOut as i32
    );
    let elapsed = started.elapsed();
    match heartbeat {
        Some(hb) => assert!(elapsed >= wft_timeout.mul_f32(0.8) + hb, "{elapsed:?}"),
        None => assert!(elapsed >= start_to_close, "{elapsed:?}"),
    }
    // The server already timed it out, so the outcome isn't reported
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.into_inner(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    shutdown_with_poller(core, poller).await;
}

#[derive(Clone, Copy, Debug)]
enum ServerCancel {
    HeartbeatCancelRequested,
    HeartbeatNotFound,
    WorkerCommand,
}

/// Cancellations the server sends for a handed off activity name it by the server's token, and
/// must reach the local execution, which only knows the made up one.
#[rstest::rstest]
#[tokio::test]
async fn server_cancel_reaches_handed_off_activity(
    #[values(
        ServerCancel::HeartbeatCancelRequested,
        ServerCancel::HeartbeatNotFound,
        ServerCancel::WorkerCommand
    )]
    how: ServerCancel,
) {
    let real = TaskToken::from(b"real-1".to_vec());
    let mut client = mock_worker_client();
    let real_for_hb = real.clone();
    client
        .expect_record_activity_heartbeat()
        .withf(move |token, _| *token == real_for_hb)
        .returning(move |_, _| match how {
            ServerCancel::HeartbeatNotFound => Err(tonic::Status::not_found("gone")),
            ServerCancel::HeartbeatCancelRequested => Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
                ..Default::default()
            }),
            ServerCancel::WorkerCommand => Ok(RecordActivityTaskHeartbeatResponse::default()),
        });
    let real_for_cancel = real.clone();
    client
        .expect_cancel_activity_task()
        .withf(move |token, _| *token == real_for_cancel)
        .times(match how {
            ServerCancel::HeartbeatNotFound => 0,
            _ => 1,
        })
        .returning(|_, _| Ok(RespondActivityTaskCanceledResponse::default()));
    let (core, mut completions) = optimistic_worker(
        history(Duration::from_secs(1)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(completion(
        &act.run_id,
        vec![speculative_activity(&core, 1)],
        false,
        true,
    ))
    .await
    .unwrap();
    let poller = idle_workflow_poller(core.clone());
    let (token, _) = poll_start(&core).await;
    completions.recv().await.unwrap();
    let trigger = async {
        loop {
            match how {
                ServerCancel::WorkerCommand => {
                    if core.cancel_activity_from_server(real.clone()) {
                        break;
                    }
                }
                _ => core.record_activity_heartbeat(ActivityHeartbeat {
                    task_token: token.clone().into_inner(),
                    details: vec![],
                }),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let cancel = tokio::select! {
        cancel = core.poll_activity_task() => cancel.unwrap(),
        _ = trigger => core.poll_activity_task().await.unwrap(),
    };
    assert_eq!(cancel.task_token, token.clone().into_inner());
    assert_matches!(cancel.variant, Some(activity_task::Variant::Cancel(_)));
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.into_inner(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    shutdown_with_poller(core, poller).await;
}

/// When the checkpoint also closes the workflow, the server still takes over running optimistic
/// activities. Nothing can observe them anymore, so Core cancels their local executions, but keeps
/// their tokens mapped: heartbeats and worker control commands still reach them, and their
/// outcome is reported with the server's token, which is expected to be gone by then.
#[rstest::rstest]
#[tokio::test]
async fn closing_checkpoint_hands_off_and_cancels(
    #[values(true, false)] polled_before_checkpoint: bool,
) {
    let real = TaskToken::from(b"real-1".to_vec());
    let mut client = mock_worker_client();
    let r = real.clone();
    client
        .expect_record_activity_heartbeat()
        .withf(move |token, _| *token == r)
        .times(1..)
        .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()));
    let r = real.clone();
    client
        .expect_cancel_activity_task()
        .withf(move |token, _| *token == r)
        .times(1)
        .returning(|_, _| Err(tonic::Status::not_found("workflow closed")));
    let (core, mut completions, meter) = optimistic_worker_metered(
        history(Duration::from_secs(10)),
        client,
        Box::new(|_| RespondWorkflowTaskCompletedResponse {
            optimistic_activity_tasks: vec![handed_off("act-1", b"real-1")],
            ..Default::default()
        }),
    )
    .await;
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(completion(
        &run_id,
        vec![
            speculative_activity(&core, 1),
            speculative_timer(2, Duration::from_millis(10)),
        ],
        false,
        true,
    ))
    .await
    .unwrap();
    let early_token = if polled_before_checkpoint {
        Some(poll_start(&core).await.0)
    } else {
        None
    };
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_seq(&act), ["timer-2"]);
    core.complete_workflow_activation(completion(&run_id, vec![complete_workflow()], true, false))
        .await
        .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(
        describe(&wtc.commands),
        [
            "schedule act-1 optimistic",
            "timer 2 optimistic",
            "fire 2",
            "complete"
        ]
    );
    let token = match early_token {
        Some(t) => t,
        None => poll_start(&core).await.0,
    };
    let cancel = core.poll_activity_task().await.unwrap();
    assert_eq!(cancel.task_token, token.clone().into_inner());
    assert_matches!(cancel.variant, Some(activity_task::Variant::Cancel(_)));
    // The mapping is kept: the server's token still finds the local execution
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: token.clone().into_inner(),
        details: vec![],
    });
    assert!(core.cancel_activity_from_server(real.clone()));
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token.into_inner(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    drain_pollers_and_shutdown(&core).await;
    assert_eq!(
        meter.counter_total("optimistic_activity_handed_off", &[]),
        1
    );
    assert_eq!(meter.counter_total("optimistic_activity_dropped", &[]), 0);
}
