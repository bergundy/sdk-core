//! Tracks activities started optimistically inside speculation windows, from their local dispatch
//! until the workflow observed their outcome, they were handed off to the server, or their window
//! was discarded. Shared by the workflow and activity sides of the worker.

use crate::worker::workflow::{
    LocalResolution,
    optimistic::{OptimisticResult, OptimisticResultKind},
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use temporalio_client::worker::CancelActivityCallback;
use temporalio_common::protos::{
    TaskToken,
    coresdk::activity_result::activity_execution_result,
    temporal::api::{common::v1::Payloads, workflowservice::v1::PollActivityTaskQueueResponse},
};
use tokio::sync::watch;

type Forwarder = Box<dyn Fn(String, LocalResolution) + Send + Sync>;
type HandoffCallback = Box<dyn Fn(&TaskToken, Instant) + Send + Sync>;

/// What happens to an optimistic activity's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Fate {
    /// The activity belongs to an open window. Its outcome goes to the workflow.
    InWindow,
    /// The workflow observed the outcome, which is part of the checkpointed log.
    Observed,
    /// The server owns the activity now. Heartbeats and the outcome use this token.
    HandedOff(TaskToken),
    /// The window was discarded, along with anything the activity does.
    Dropped,
}

/// Where a heartbeat for an optimistic activity should go.
#[derive(Debug)]
pub(crate) enum HeartbeatRoute {
    /// Kept locally until the activity is handed off.
    Hold,
    Forward(TaskToken),
    Discard,
}

/// Activities whose fate a checkpoint decided, by activity type.
#[derive(Debug, Default)]
pub(crate) struct Handoff {
    pub(crate) handed_off: Vec<String>,
    pub(crate) dropped: Vec<String>,
}

struct Entry {
    run_id: String,
    generation: u64,
    seq: u32,
    activity_id: String,
    activity_type: String,
    /// When the activity started, which the server backdates its scheduled and started times to.
    started_at: Instant,
    /// Set if the checkpoint that handed the activity off also closed the workflow.
    workflow_closed: bool,
    fate: watch::Sender<Fate>,
    heartbeat_details: Option<Vec<temporalio_common::protos::temporal::api::common::v1::Payload>>,
}

#[derive(Default)]
struct Inner {
    by_token: HashMap<TaskToken, Entry>,
    synthetic_by_real: HashMap<TaskToken, TaskToken>,
}

#[derive(Default)]
pub(crate) struct OptimisticActivities {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
    forward: OnceLock<Forwarder>,
    cancel: OnceLock<CancelActivityCallback>,
    on_handoff: OnceLock<HandoffCallback>,
}

impl OptimisticActivities {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Sets how outcomes reach the workflow that started the activity.
    pub(crate) fn set_forwarder(
        &self,
        f: impl Fn(String, LocalResolution) + Send + Sync + 'static,
    ) {
        let _ = self.forward.set(Box::new(f));
    }

    /// Sets what to do for an activity once the server took it over, given its token and when it
    /// started.
    pub(crate) fn set_handoff_callback(
        &self,
        f: impl Fn(&TaskToken, Instant) + Send + Sync + 'static,
    ) {
        let _ = self.on_handoff.set(Box::new(f));
    }

    /// When a handed off activity started, if it was handed off.
    pub(crate) fn handed_off_start(&self, token: &TaskToken) -> Option<Instant> {
        self.inner
            .lock()
            .by_token
            .get(token)
            .filter(|e| matches!(*e.fate.borrow(), Fate::HandedOff(_)))
            .map(|e| e.started_at)
    }

    /// Sets how local executions are asked to stop.
    pub(crate) fn set_cancel_callback(&self, cb: CancelActivityCallback) {
        let _ = self.cancel.set(cb);
    }

    /// Registers a new optimistic activity and returns the token lang will know it by.
    pub(crate) fn register(
        &self,
        run_id: &str,
        generation: u64,
        seq: u32,
        task: &PollActivityTaskQueueResponse,
    ) -> TaskToken {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token = TaskToken::new_optimistic_activity_token(
            format!("{run_id}:{generation}:{seq}:{id}").into_bytes(),
        );
        self.inner.lock().by_token.insert(
            token.clone(),
            Entry {
                run_id: run_id.to_owned(),
                generation,
                seq,
                activity_id: task.activity_id.clone(),
                activity_type: task.activity_type.clone().unwrap_or_default().name,
                started_at: Instant::now(),
                workflow_closed: false,
                fate: watch::Sender::new(Fate::InWindow),
                heartbeat_details: None,
            },
        );
        token
    }

    /// Returns a receiver for the activity's fate, if it is tracked.
    pub(crate) fn subscribe(&self, token: &TaskToken) -> Option<watch::Receiver<Fate>> {
        self.inner
            .lock()
            .by_token
            .get(token)
            .map(|e| e.fate.subscribe())
    }

    /// Sends an outcome lang reported to the workflow, while the activity is in its window.
    pub(crate) fn forward_outcome(
        &self,
        token: &TaskToken,
        status: activity_execution_result::Status,
    ) {
        let (run_id, generation, seq) = {
            let inner = self.inner.lock();
            let Some(e) = inner.by_token.get(token) else {
                return;
            };
            if *e.fate.borrow() != Fate::InWindow {
                return;
            }
            (e.run_id.clone(), e.generation, e.seq)
        };
        if let Some(f) = self.forward.get() {
            f(
                run_id,
                LocalResolution::Optimistic(OptimisticResult {
                    generation,
                    kind: OptimisticResultKind::Activity {
                        seq,
                        token: token.clone(),
                        status: Box::new(status),
                    },
                }),
            );
        }
    }

    pub(crate) fn route_heartbeat(
        &self,
        token: &TaskToken,
        details: &[temporalio_common::protos::temporal::api::common::v1::Payload],
    ) -> HeartbeatRoute {
        let mut inner = self.inner.lock();
        let Some(e) = inner.by_token.get_mut(token) else {
            return HeartbeatRoute::Discard;
        };
        let fate = e.fate.borrow().clone();
        match fate {
            Fate::InWindow => {
                e.heartbeat_details = Some(details.to_vec());
                HeartbeatRoute::Hold
            }
            Fate::HandedOff(real) => HeartbeatRoute::Forward(real),
            Fate::Observed | Fate::Dropped => HeartbeatRoute::Discard,
        }
    }

    /// The latest heartbeat details of a still running optimistic activity, sent along when it is
    /// handed off.
    pub(crate) fn heartbeat_details(&self, run_id: &str, activity_id: &str) -> Option<Payloads> {
        self.inner
            .lock()
            .by_token
            .values()
            .find(|e| {
                e.run_id == run_id
                    && e.activity_id == activity_id
                    && *e.fate.borrow() == Fate::InWindow
            })
            .and_then(|e| e.heartbeat_details.clone())
            .map(|payloads| Payloads { payloads })
    }

    pub(crate) fn observed(&self, token: &TaskToken) {
        if let Some(e) = self.inner.lock().by_token.get(token) {
            e.fate.send_replace(Fate::Observed);
        }
    }

    /// Hands still running activities of a checkpointed window over to the server, matching them
    /// to the tasks the server returned by activity id. Activities the server didn't return are
    /// discarded. If the checkpoint closed the workflow, nothing can observe the handed off
    /// activities anymore, so their local executions are cancelled too, while their tokens stay
    /// mapped so the outcome and anything the server sends about them still find their way.
    pub(crate) fn handoff(
        &self,
        run_id: &str,
        tasks: &[PollActivityTaskQueueResponse],
        workflow_closed: bool,
    ) -> Handoff {
        let mut to_cancel = vec![];
        let mut handoff = Handoff::default();
        let mut handed_off_tokens = vec![];
        {
            let mut inner = self.inner.lock();
            let mut real_tokens = vec![];
            for (token, e) in inner.by_token.iter_mut() {
                if e.run_id != run_id || *e.fate.borrow() != Fate::InWindow {
                    continue;
                }
                match tasks.iter().find(|t| t.activity_id == e.activity_id) {
                    Some(task) => {
                        let real: TaskToken = task.task_token.clone().into();
                        debug!(activity_id = %e.activity_id, synthetic = %token, real = %real,
                               "Handed off optimistic activity");
                        e.workflow_closed = workflow_closed;
                        e.fate.send_replace(Fate::HandedOff(real.clone()));
                        real_tokens.push((real, token.clone()));
                        handoff.handed_off.push(e.activity_type.clone());
                        handed_off_tokens.push((token.clone(), e.started_at));
                        if workflow_closed {
                            to_cancel.push(token.clone());
                        }
                    }
                    None => {
                        debug!(activity_id = %e.activity_id,
                               "Server didn't take over optimistic activity, discarding it");
                        e.fate.send_replace(Fate::Dropped);
                        to_cancel.push(token.clone());
                        handoff.dropped.push(e.activity_type.clone());
                    }
                }
            }
            inner.synthetic_by_real.extend(real_tokens);
        }
        if let Some(on_handoff) = self.on_handoff.get() {
            for (token, started_at) in handed_off_tokens {
                on_handoff(&token, started_at);
            }
        }
        self.cancel_all(to_cancel);
        handoff
    }

    /// Discards a run's activities whose window was discarded or never accepted by the server.
    /// Returns their activity types.
    pub(crate) fn drop_run(&self, run_id: &str) -> Vec<String> {
        let (to_cancel, dropped): (Vec<_>, Vec<_>) = {
            let inner = self.inner.lock();
            inner
                .by_token
                .iter()
                .filter(|(_, e)| e.run_id == run_id && *e.fate.borrow() == Fate::InWindow)
                .map(|(token, e)| {
                    e.fate.send_replace(Fate::Dropped);
                    (token.clone(), e.activity_type.clone())
                })
                .unzip()
        };
        self.cancel_all(to_cancel);
        dropped
    }

    /// Asks lang to cancel a local execution on the workflow's request.
    pub(crate) fn cancel(&self, run_id: &str, generation: u64, seq: u32) {
        let token = self
            .inner
            .lock()
            .by_token
            .iter()
            .find(|(_, e)| e.run_id == run_id && e.generation == generation && e.seq == seq)
            .map(|(t, _)| t.clone());
        if let Some(token) = token {
            self.cancel_all([token]);
        }
    }

    fn cancel_all(&self, tokens: impl IntoIterator<Item = TaskToken>) {
        if let Some(cancel) = self.cancel.get() {
            for token in tokens {
                cancel(token);
            }
        }
    }

    /// True if the local execution of an optimistic activity must be cancelled as soon as lang
    /// receives it, because its window was discarded or its workflow closed before then.
    pub(crate) fn cancel_on_dispatch(&self, token: &TaskToken) -> bool {
        self.inner
            .lock()
            .by_token
            .get(token)
            .is_some_and(|e| match *e.fate.borrow() {
                Fate::Dropped => true,
                Fate::HandedOff(_) => e.workflow_closed,
                Fate::InWindow | Fate::Observed => false,
            })
    }

    /// True if the activity was handed off by a checkpoint that also closed its workflow.
    pub(crate) fn handed_off_with_closed_workflow(&self, token: &TaskToken) -> bool {
        self.inner
            .lock()
            .by_token
            .get(token)
            .is_some_and(|e| e.workflow_closed)
    }

    /// Returns the token lang knows a handed off activity by.
    pub(crate) fn synthetic_for(&self, real: &TaskToken) -> Option<TaskToken> {
        self.inner.lock().synthetic_by_real.get(real).cloned()
    }

    /// Forgets an activity once lang finished it.
    pub(crate) fn remove(&self, token: &TaskToken) {
        let mut inner = self.inner.lock();
        if let Some(e) = inner.by_token.remove(token)
            && let Fate::HandedOff(real) = e.fate.borrow().clone()
        {
            inner.synthetic_by_real.remove(&real);
        }
    }
}
