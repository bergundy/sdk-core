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
};
use temporalio_client::worker::CancelActivityCallback;
use temporalio_common::protos::{
    TaskToken,
    coresdk::activity_result::activity_execution_result,
    temporal::api::{common::v1::Payloads, workflowservice::v1::PollActivityTaskQueueResponse},
};
use tokio::sync::watch;

type Forwarder = Box<dyn Fn(String, LocalResolution) + Send + Sync>;

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

struct Entry {
    run_id: String,
    generation: u64,
    seq: u32,
    activity_id: String,
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
                        status,
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
    /// to the tasks the server returned by activity id. Activities the server didn't return (ex:
    /// because the workflow closed) are discarded.
    pub(crate) fn handoff(&self, run_id: &str, tasks: &[PollActivityTaskQueueResponse]) {
        let mut to_cancel = vec![];
        {
            let mut inner = self.inner.lock();
            let mut real_tokens = vec![];
            for (token, e) in inner.by_token.iter() {
                if e.run_id != run_id || *e.fate.borrow() != Fate::InWindow {
                    continue;
                }
                match tasks.iter().find(|t| t.activity_id == e.activity_id) {
                    Some(task) => {
                        let real: TaskToken = task.task_token.clone().into();
                        debug!(activity_id = %e.activity_id, synthetic = %token, real = %real,
                               "Handed off optimistic activity");
                        e.fate.send_replace(Fate::HandedOff(real.clone()));
                        real_tokens.push((real, token.clone()));
                    }
                    None => {
                        debug!(activity_id = %e.activity_id,
                               "Server didn't take over optimistic activity, discarding it");
                        e.fate.send_replace(Fate::Dropped);
                        to_cancel.push(token.clone());
                    }
                }
            }
            inner.synthetic_by_real.extend(real_tokens);
        }
        self.cancel_all(to_cancel);
    }

    /// Discards a run's activities whose window was discarded or never accepted by the server.
    pub(crate) fn drop_run(&self, run_id: &str) {
        let to_cancel: Vec<_> = {
            let inner = self.inner.lock();
            inner
                .by_token
                .iter()
                .filter(|(_, e)| e.run_id == run_id && *e.fate.borrow() == Fate::InWindow)
                .map(|(token, e)| {
                    e.fate.send_replace(Fate::Dropped);
                    token.clone()
                })
                .collect()
        };
        self.cancel_all(to_cancel);
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

    /// True if the activity's window was discarded, ex: before lang even received the task.
    pub(crate) fn is_dropped(&self, token: &TaskToken) -> bool {
        self.inner
            .lock()
            .by_token
            .get(token)
            .is_some_and(|e| *e.fate.borrow() == Fate::Dropped)
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
