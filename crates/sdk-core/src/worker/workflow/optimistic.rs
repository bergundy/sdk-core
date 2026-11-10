//! Types connecting the workflow machines' speculation windows (optimistic start) to the parts
//! of the worker that run optimistic activities and timers locally.

use std::time::{Duration, Instant};
use temporalio_common::protos::{
    TaskToken, coresdk::activity_result::activity_execution_result,
    temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
};

/// What workflow machines need from the worker to run commands optimistically. All calls are
/// made from the workflow processing thread and must not block.
pub(crate) trait OptimisticEnv: Send + Sync {
    /// Whether commands may be started optimistically at all, ex: the namespace has the
    /// capability and this worker runs activities.
    fn enabled(&self) -> bool;
    /// Maximum total size of payloads a window may accumulate in its log.
    fn payload_budget(&self) -> usize;
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
        status: activity_execution_result::Status,
    },
    TimerFired {
        seq: u32,
    },
    /// The window reached the time by which it must checkpoint.
    Deadline,
}
