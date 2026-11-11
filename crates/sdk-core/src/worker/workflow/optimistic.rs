//! Types connecting the workflow machines' speculation windows (optimistic start) to the parts
//! of the worker that run optimistic activities and timers locally.

use crate::{
    abstractions::{TrackedOwnedMeteredSemPermit, dbg_panic},
    pollers::TrackedPermittedTqResp,
    worker::{
        ActivitySlotKind, NamespaceCapabilities,
        activities::ActivitiesFromWFTsHandle,
        optimistic_activities::OptimisticActivities,
        workflow::{
            LocalResolution, LocalResolutionMsg,
            workflow_stream::{LocalInput, LocalInputs},
        },
    },
};
use futures_util::future::{AbortHandle, abortable};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    TaskToken, coresdk::activity_result::activity_execution_result,
    temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::Span;

/// What workflow machines need from the worker to run commands optimistically. All calls are
/// made from the workflow processing thread and must not block.
pub(crate) trait OptimisticEnv: Send + Sync {
    /// Whether commands may be started optimistically at all, ex: the namespace has the
    /// capability and this worker runs activities.
    fn enabled(&self) -> bool;
    fn now(&self) -> Instant;
    /// Reserves `n` activity slots for optimistic starts. Reserves nothing and returns false if
    /// they are not all available.
    fn reserve_activity_slots(&self, n: usize) -> bool;
    /// Releases slots reserved by [Self::reserve_activity_slots] that weren't used.
    fn release_reserved_slots(&self);
    /// Dispatches an activity to the local activity worker using a reserved slot. The task token
    /// is assigned by the environment.
    fn start_activity(&self, start: OptimisticActivityStart);
    /// Asks lang to cancel the local execution of an optimistic activity, on the workflow's
    /// request.
    fn cancel_activity(&self, run_id: &str, generation: u64, seq: u32);
    fn start_timer(&self, run_id: &str, generation: u64, seq: u32, duration: Duration);
    fn cancel_timer(&self, run_id: &str, generation: u64, seq: u32);
    /// Arms (or re-arms) the time by which the window must checkpoint.
    fn set_deadline(&self, run_id: &str, generation: u64, deadline: Instant);
    /// The workflow observed the outcome of the activity with this token, so it is recorded in
    /// the log and must not be reported to the server separately.
    fn activity_outcome_observed(&self, token: &TaskToken);
    /// The window is being checkpointed. Local timers stop, while activities keep running and
    /// await their handoff.
    fn window_closed(&self, run_id: &str, generation: u64);
    /// The window's work is discarded, ex: because its workflow task failed. Local executions
    /// are cancelled and their outcomes dropped.
    fn window_dropped(&self, run_id: &str);
}

/// An activity started optimistically, to be dispatched to the local activity worker.
#[derive(Debug)]
pub(crate) struct OptimisticActivityStart {
    pub(crate) run_id: String,
    pub(crate) generation: u64,
    pub(crate) seq: u32,
    /// The task as it will be given to lang, minus the task token.
    pub(crate) task: PollActivityTaskQueueResponse,
}

/// Something that happened to optimistic work, reported back to the run that started it.
#[derive(Debug)]
pub(crate) struct OptimisticResult {
    /// The window the work was started in. Results for older windows are ignored.
    pub(crate) generation: u64,
    pub(crate) kind: OptimisticResultKind,
}

#[derive(Debug)]
pub(crate) enum OptimisticResultKind {
    /// Lang finished executing an optimistic activity.
    Activity {
        seq: u32,
        token: TaskToken,
        status: Box<activity_execution_result::Status>,
    },
    TimerFired {
        seq: u32,
    },
    /// The window reached the time by which it must checkpoint.
    Deadline,
}

/// The worker's [OptimisticEnv]: dispatches optimistic activities through the activity task
/// stream like eager activities, and runs timers and window deadlines on the workflow
/// processing thread's runtime.
pub(crate) struct OptimisticRuntime {
    capabilities: Arc<NamespaceCapabilities>,
    activities: Option<ActivitiesFromWFTsHandle>,
    registry: Arc<OptimisticActivities>,
    local_tx: UnboundedSender<LocalInput>,
    reserved: Mutex<Vec<TrackedOwnedMeteredSemPermit<ActivitySlotKind>>>,
    timers: Mutex<HashMap<(String, u32), AbortHandle>>,
    deadlines: Mutex<HashMap<String, AbortHandle>>,
}

impl OptimisticRuntime {
    pub(super) fn new(
        capabilities: Arc<NamespaceCapabilities>,
        activities: Option<ActivitiesFromWFTsHandle>,
        registry: Arc<OptimisticActivities>,
        local_tx: UnboundedSender<LocalInput>,
    ) -> Self {
        Self {
            capabilities,
            activities,
            registry,
            local_tx,
            reserved: Default::default(),
            timers: Default::default(),
            deadlines: Default::default(),
        }
    }

    fn send_later(
        &self,
        run_id: &str,
        at: Instant,
        result: OptimisticResult,
    ) -> Option<AbortHandle> {
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            dbg_panic!("Optimistic timers must be started from within a runtime");
            return None;
        };
        let tx = self.local_tx.clone();
        let run_id = run_id.to_owned();
        let span = Span::current();
        let (fut, handle) = abortable(async move {
            tokio::time::sleep_until(at.into()).await;
            let _ = tx.send(LocalInput {
                input: LocalInputs::LocalResolution(LocalResolutionMsg {
                    run_id,
                    res: LocalResolution::Optimistic(result),
                }),
                span,
            });
        });
        rt.spawn(fut);
        Some(handle)
    }

    fn stop_timers(&self, run_id: &str) {
        self.timers.lock().retain(|(r, _), h| {
            if r == run_id {
                h.abort();
                false
            } else {
                true
            }
        });
        if let Some(h) = self.deadlines.lock().remove(run_id) {
            h.abort();
        }
    }
}

impl OptimisticEnv for OptimisticRuntime {
    fn enabled(&self) -> bool {
        self.capabilities.optimistic_start()
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn reserve_activity_slots(&self, n: usize) -> bool {
        let Some(handle) = self.activities.as_ref() else {
            return false;
        };
        let mut permits = Vec::with_capacity(n);
        for _ in 0..n {
            match handle.reserve_slot() {
                Some(p) => permits.push(p),
                None => return false,
            }
        }
        self.reserved.lock().extend(permits);
        true
    }

    fn release_reserved_slots(&self) {
        self.reserved.lock().clear();
    }

    fn start_activity(&self, start: OptimisticActivityStart) {
        let (Some(handle), Some(permit)) = (self.activities.as_ref(), self.reserved.lock().pop())
        else {
            dbg_panic!("Optimistic activity started without a reserved slot");
            return;
        };
        let mut task = start.task;
        task.task_token = self
            .registry
            .register(&start.run_id, start.generation, start.seq, &task)
            .into_inner();
        debug!(run_id = %start.run_id, activity_id = %task.activity_id,
               "Starting optimistic activity");
        handle.add_tasks([TrackedPermittedTqResp { permit, resp: task }]);
    }

    fn cancel_activity(&self, run_id: &str, generation: u64, seq: u32) {
        self.registry.cancel(run_id, generation, seq);
    }

    fn start_timer(&self, run_id: &str, generation: u64, seq: u32, duration: Duration) {
        if let Some(h) = self.send_later(
            run_id,
            Instant::now() + duration,
            OptimisticResult {
                generation,
                kind: OptimisticResultKind::TimerFired { seq },
            },
        ) && let Some(old) = self.timers.lock().insert((run_id.to_owned(), seq), h)
        {
            old.abort();
        }
    }

    fn cancel_timer(&self, run_id: &str, _generation: u64, seq: u32) {
        if let Some(h) = self.timers.lock().remove(&(run_id.to_owned(), seq)) {
            h.abort();
        }
    }

    fn set_deadline(&self, run_id: &str, generation: u64, deadline: Instant) {
        if let Some(h) = self.send_later(
            run_id,
            deadline,
            OptimisticResult {
                generation,
                kind: OptimisticResultKind::Deadline,
            },
        ) && let Some(old) = self.deadlines.lock().insert(run_id.to_owned(), h)
        {
            old.abort();
        }
    }

    fn activity_outcome_observed(&self, token: &TaskToken) {
        self.registry.observed(token);
    }

    fn window_closed(&self, run_id: &str, _generation: u64) {
        self.stop_timers(run_id);
    }

    fn window_dropped(&self, run_id: &str) {
        self.stop_timers(run_id);
        self.registry.drop_run(run_id);
    }
}
