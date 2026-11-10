//! Speculation windows (optimistic start) driven through a worker with mocked server calls.

use crate::{
    ActivityHeartbeat, PollError, Worker,
    replay::TestHistoryBuilder,
    test_help::{MockPollCfg, build_mock_pollers, drain_pollers_and_shutdown, mock_worker},
    worker::client::{MockWorkerClient, WorkflowTaskCompletion, mocks::mock_worker_client},
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
        activity_task::{ActivityTask, activity_task},
        workflow_activation::{WorkflowActivation, workflow_activation_job},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, ScheduleActivity, StartTimer,
            workflow_command,
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
            RecordActivityTaskHeartbeatResponse, RespondActivityTaskCompletedResponse,
            RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};
use tokio::{
    sync::mpsc::{UnboundedReceiver, unbounded_channel},
    task::JoinHandle,
};

type CompletionResponder =
    Box<dyn FnMut(&WorkflowTaskCompletion) -> RespondWorkflowTaskCompletedResponse + Send>;

/// A worker for a single workflow whose first workflow task is handed out, with the namespace
/// supporting optimistic start. WFT completions are sent to the returned channel.
async fn optimistic_worker(
    t: TestHistoryBuilder,
    mut client: MockWorkerClient,
    mut respond: CompletionResponder,
) -> (Arc<Worker>, UnboundedReceiver<WorkflowTaskCompletion>) {
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
    let (tx, rx) = unbounded_channel();
    let mut mh = MockPollCfg::from_resp_batches("fake_wf_id", t, [1], client);
    mh.activity_responses = Some(vec![]);
    mh.completion_mock_fn = Some(Box::new(move |wtc| {
        let resp = respond(wtc);
        let _ = tx.send(wtc.clone());
        Ok(resp)
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
    });
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

/// A local activity scheduled while the window is open checkpoints the window.
#[tokio::test]
async fn non_deferrable_command_checkpoints() {
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
        result: Some(ActivityExecutionResult::fail("boom".into())),
    })
    .await
    .unwrap();
    let wtc = completions.recv().await.unwrap();
    assert_eq!(describe(&wtc.commands), ["schedule act-1 optimistic"]);

    shutdown_with_poller(core, poller).await;
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
            optimistic_start: true,
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
