//! Speculation windows (optimistic start). See `optimistic-start.md` in the proposals repository
//! for the design.
//!
//! During original execution, speculative activities and timers may be started locally inside a
//! window. Their resolutions are delivered to the workflow one per activation and recorded in the
//! command list (the optimistic log) at the position the workflow observed them, using predicted
//! event ids. During replay, the same resolutions are peeked from the next workflow task sequence
//! and delivered at the same positions. In both cases the events recording them are applied later
//! to the already resolved machines, without producing jobs.

use super::{CommandAndMachine, MachineKey, WorkflowMachines};
use crate::{
    abstractions::dbg_panic,
    protosext::HistoryEventExt,
    worker::workflow::{
        CommandID, WFMachinesError, fatal,
        machines::{
            HistEventData, Machines, TemporalStateMachine,
            activity_state_machine::OptimisticActivityOutcome, workflow_machines::MachineResponse,
        },
        nondeterminism,
        optimistic::{
            OptimisticActivityStart, OptimisticEnv, OptimisticResult, OptimisticResultKind,
        },
    },
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use temporalio_common::protos::{
    TaskToken, VERSION_SEARCH_ATTR_KEY,
    coresdk::activity_result::{Cancellation, Success, activity_execution_result},
    temporal::api::{
        command::v1::{
            Command as ProtoCommand, FireOptimisticTimerCommandAttributes, OptimisticStart,
            ResolveOptimisticActivityCommandAttributes, ScheduleActivityTaskCommandAttributes,
            command::Attributes as ProtoCmdAttrs,
            resolve_optimistic_activity_command_attributes as resolve_attrs,
        },
        common::v1::{ActivityType, Payloads, WorkflowExecution, WorkflowType},
        enums::v1::CommandType,
        failure::v1::{CanceledFailureInfo, Failure, failure::FailureInfo},
        history::v1::{ActivityTaskCanceledEventAttributes, HistoryEvent, history_event},
        workflowservice::v1::PollActivityTaskQueueResponse,
    },
};

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// Same fraction of the workflow task timeout local activities heartbeat at.
const WINDOW_DEADLINE_FRACTION: f32 = crate::worker::workflow::WFT_HEARTBEAT_TIMEOUT_FRACTION;
/// New optimistic commands are only started if at least this fraction of the workflow task
/// timeout remains before the window must checkpoint, since anything still running at that point
/// costs a handoff.
const MIN_REMAINING_WINDOW_FRACTION: f32 = 0.1;

#[derive(Default)]
pub(super) struct OptimisticData {
    pub(super) env: Option<Arc<dyn OptimisticEnv>>,
    /// When the current workflow task was received. Optimistic offsets are measured from it.
    pub(super) wft_received_at: Option<Instant>,
    /// Resolutions delivered during the current workflow task, in order.
    delivered: Vec<DeliveredResolution>,
    /// Activities started optimistically during the current workflow task, by predicted
    /// scheduled event id.
    activities: HashMap<i64, MachineKey>,
    /// Timers started optimistically during the current workflow task, by seq.
    timers: HashMap<u32, MachineKey>,
    /// Every machine in `activities` and `timers`.
    machines: HashSet<MachineKey>,
    window: Option<Window>,
    next_generation: u64,
    /// Reported by lang with the most recent activation completion.
    block_exited: bool,
    block_active: bool,
}

struct DeliveredResolution {
    /// How many commands precede the resolution in the command list.
    after_commands: usize,
    event_count: i64,
    /// Only present during original execution, since commands aren't sent while replaying.
    command: Option<ProtoCommand>,
}

struct Window {
    generation: u64,
    checkpoint: Option<CheckpointReason>,
    /// Outcomes that arrived but haven't been delivered yet, in arrival order.
    queued: VecDeque<QueuedOutcome>,
    deadline: Instant,
}

struct QueuedOutcome {
    machine: MachineKey,
    outcome: OptimisticOutcome,
    token: Option<TaskToken>,
}

#[derive(Debug, Clone)]
enum OptimisticOutcome {
    Activity(OptimisticActivityOutcome),
    TimerFired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointReason {
    BlockExit,
    NonDeferrableCommand,
    LocalActivity,
    Deadline,
    UnobservedOutcome,
    QueryResponse,
    Eviction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OptimisticKey {
    Activity(i64),
    Timer(String),
}

/// A resolution found in the next workflow task sequence while replaying.
struct PeekedResolution {
    key: OptimisticKey,
    commands_before: usize,
    workflow_time: Option<SystemTime>,
    outcome: OptimisticOutcome,
}

impl OptimisticData {
    pub(super) fn new(env: Option<Arc<dyn OptimisticEnv>>) -> Self {
        Self {
            env,
            ..Default::default()
        }
    }
}

impl WorkflowMachines {
    pub(crate) fn set_wft_received_at(&mut self, at: Instant) {
        self.optimistic.wft_received_at = Some(at);
    }

    /// Records the speculative block state lang reported with the activation being completed.
    pub(crate) fn set_speculative_block_state(&mut self, exited: bool, active: bool) {
        self.optimistic.block_exited = exited;
        self.optimistic.block_active = active;
    }

    pub(crate) fn optimistic_window_open(&self) -> bool {
        self.optimistic.window.is_some()
    }

    /// True if the window should keep the workflow task open, because the workflow may still
    /// observe the outcome of optimistic work and nothing asked for a checkpoint.
    pub(crate) fn optimistic_window_wants_to_stay_open(&self) -> bool {
        match &self.optimistic.window {
            Some(w) if w.checkpoint.is_none() => {
                !w.queued.is_empty()
                    || self
                        .optimistic
                        .machines
                        .iter()
                        .any(|mk| match self.machine(*mk) {
                            Machines::ActivityMachine(m) => m.awaiting_optimistic_outcome(),
                            Machines::TimerMachine(m) => m.awaiting_optimistic_fire(),
                            _ => false,
                        })
            }
            _ => false,
        }
    }

    /// True if a window is open but nothing keeps it open, so the workflow task should be
    /// completed (checkpointed) even though lang has nothing new to say.
    pub(crate) fn optimistic_checkpoint_ready(&self) -> bool {
        self.optimistic_window_open()
            && !self.has_pending_jobs()
            && !self.optimistic_window_wants_to_stay_open()
    }

    pub(crate) fn request_optimistic_checkpoint(&mut self, reason: CheckpointReason) {
        if let Some(w) = self.optimistic.window.as_mut()
            && w.checkpoint.is_none()
        {
            debug!(?reason, run_id = %self.run_id, "Checkpointing speculation window");
            w.checkpoint = Some(reason);
        }
    }

    /// Must be called once the completion carrying the current window's log was reported.
    pub(crate) fn optimistic_window_reported(&mut self) {
        if let Some(w) = self.optimistic.window.take()
            && let Some(env) = self.optimistic.env.as_ref()
        {
            env.window_closed(&self.run_id, w.generation);
        }
    }

    /// Discards the current window, ex: because the workflow task failed or the run is being
    /// evicted. Its log is never sent.
    pub(crate) fn drop_optimistic_window(&mut self) {
        if self.optimistic.window.take().is_some()
            && let Some(env) = self.optimistic.env.as_ref()
        {
            env.window_dropped(&self.run_id);
        }
    }

    /// Resets per workflow task state. Called when a workflow task starts.
    pub(super) fn optimistic_wft_started(&mut self) {
        if self.optimistic.window.is_some() {
            dbg_panic!("A speculation window survived into a new workflow task");
            self.drop_optimistic_window();
        }
        self.optimistic.delivered.clear();
        self.optimistic.activities.clear();
        self.optimistic.timers.clear();
        self.optimistic.machines.clear();
    }

    /// Returns the commands to send, with the resolutions observed in this workflow task placed
    /// where they were observed.
    pub(super) fn interleave_optimistic_resolutions(
        &self,
        commands: impl Iterator<Item = (usize, ProtoCommand)>,
    ) -> Vec<ProtoCommand> {
        let mut resolutions = self
            .optimistic
            .delivered
            .iter()
            .filter_map(|d| d.command.as_ref().map(|c| (d.after_commands, c)))
            .peekable();
        let mut out = vec![];
        for (ix, cmd) in commands {
            while let Some((_, res)) = resolutions.next_if(|(after, _)| *after <= ix) {
                out.push(res.clone());
            }
            out.push(cmd);
        }
        out.extend(resolutions.map(|(_, c)| c.clone()));
        out
    }

    /// The event id the command at `position` in the command list will be recorded with,
    /// assuming every command before it produces exactly one event, which holds for everything
    /// that may precede an optimistic command.
    fn predicted_event_id(&self, position: usize) -> i64 {
        let resolution_events: i64 = self
            .optimistic
            .delivered
            .iter()
            .filter(|d| d.after_commands <= position)
            .map(|d| d.event_count)
            .sum();
        self.current_started_event_id + 2 + position as i64 + resolution_events
    }

    /// Whether a command can be part of an optimistic log without ending the window.
    fn command_is_deferrable(&self, c: &CommandAndMachine) -> bool {
        match c.command.attributes.as_ref() {
            Some(
                ProtoCmdAttrs::ScheduleActivityTaskCommandAttributes(_)
                | ProtoCmdAttrs::RequestCancelActivityTaskCommandAttributes(_)
                | ProtoCmdAttrs::StartTimerCommandAttributes(_)
                | ProtoCmdAttrs::CancelTimerCommandAttributes(_),
            ) => self.optimistic.machines.contains(&c.machine),
            Some(ProtoCmdAttrs::RecordMarkerCommandAttributes(_)) => {
                matches!(self.machine(c.machine), Machines::PatchMachine(_))
            }
            Some(
                ProtoCmdAttrs::UpsertWorkflowSearchAttributesCommandAttributes(_)
                | ProtoCmdAttrs::ModifyWorkflowPropertiesCommandAttributes(_),
            ) => true,
            _ => false,
        }
    }

    /// Patch markers, and the search attribute upserts Core adds with them, may be skipped by
    /// replay on either side: a deprecated patch call may have no marker in history, and a
    /// deprecated marker in history may have no call. They're left out when lining up a window's
    /// commands with the events recording them, which can't otherwise tell the cases apart. See
    /// [is_patch_event] for the history side.
    fn is_patch_command(&self, c: &CommandAndMachine) -> bool {
        match (c.command.attributes.as_ref(), self.machine(c.machine)) {
            (_, Machines::PatchMachine(_)) => true,
            (Some(ProtoCmdAttrs::UpsertWorkflowSearchAttributesCommandAttributes(a)), _) => a
                .search_attributes
                .as_ref()
                .is_some_and(|sa| sa.indexed_fields.contains_key(VERSION_SEARCH_ATTR_KEY)),
            _ => false,
        }
    }

    fn is_speculative_candidate(&self, c: &CommandAndMachine) -> bool {
        match (c.command.attributes.as_ref(), self.machine(c.machine)) {
            (
                Some(ProtoCmdAttrs::ScheduleActivityTaskCommandAttributes(_)),
                Machines::ActivityMachine(m),
            ) => m.attrs().speculative && m.optimistic_scheduled_event_id().is_none(),
            (Some(ProtoCmdAttrs::StartTimerCommandAttributes(_)), Machines::TimerMachine(m)) => {
                m.attrs().speculative && !m.is_optimistic()
            }
            _ => false,
        }
    }

    fn has_pending_non_optimistic_work(&self) -> bool {
        self.all_machines.iter().any(|(k, m)| {
            !self.optimistic.machines.contains(&k)
                && !m.is_final_state()
                && matches!(
                    m,
                    Machines::ActivityMachine(_)
                        | Machines::TimerMachine(_)
                        | Machines::ChildWorkflowMachine(_)
                        | Machines::NexusOperationMachine(_)
                )
        })
    }

    /// Runs after each activation's commands have been prepared. `first_new` is the index of the
    /// first command this activation added.
    pub(super) fn optimistic_after_activation(&mut self, first_new: usize) -> Result<()> {
        if self.replaying {
            self.mark_optimistic_from_history(first_new);
            return self.deliver_peeked_resolution();
        }
        self.start_optimistic_commands(first_new)?;
        self.check_checkpoint_triggers();
        self.optimistic.block_exited = false;
        self.deliver_queued_optimistic_outcome()
    }

    // ---------------------------------------------------------------------------------------
    // Replay
    // ---------------------------------------------------------------------------------------

    fn mark_optimistic_from_history(&mut self, first_new: usize) {
        let peeked = self
            .last_history_from_server
            .peek_next_wft_sequence(self.current_started_event_id);
        // Commands are recorded in order, so the n-th command lines up with the n-th command
        // event. Patch markers are left out on both sides, since replay may skip them on either.
        let earlier_commands = self
            .commands
            .iter()
            .take(first_new)
            .filter(|c| !self.is_patch_command(c))
            .count();
        let mut command_events = peeked
            .iter()
            .filter(|e| e.is_command_event() && !is_patch_event(e))
            .skip(earlier_commands);
        let mut marks = vec![];
        for c in self.commands.iter().skip(first_new) {
            if self.is_patch_command(c) {
                continue;
            }
            let Some(event) = command_events.next() else {
                break;
            };
            match (c.command.attributes.as_ref(), event.attributes.as_ref()) {
                (
                    Some(ProtoCmdAttrs::ScheduleActivityTaskCommandAttributes(cmd)),
                    Some(history_event::Attributes::ActivityTaskScheduledEventAttributes(ev)),
                ) if ev.optimistic_start.is_some() && ev.activity_id == cmd.activity_id => {
                    marks.push((c.machine, Some(event.event_id)));
                }
                (
                    Some(ProtoCmdAttrs::StartTimerCommandAttributes(cmd)),
                    Some(history_event::Attributes::TimerStartedEventAttributes(ev)),
                ) if ev.optimistic_start.is_some() && ev.timer_id == cmd.timer_id => {
                    marks.push((c.machine, None));
                }
                _ => {}
            }
        }
        for (mk, predicted) in marks {
            self.mark_optimistic(mk, predicted);
        }
    }

    fn mark_optimistic(&mut self, mk: MachineKey, predicted: Option<i64>) {
        match self.machine_mut(mk) {
            Machines::ActivityMachine(m) => {
                let predicted = predicted.expect("Activities always have a predicted event id");
                m.set_optimistic(predicted);
                self.optimistic.activities.insert(predicted, mk);
            }
            Machines::TimerMachine(m) => {
                m.set_optimistic();
                let seq = m.seq();
                self.optimistic.timers.insert(seq, mk);
            }
            _ => return,
        }
        self.optimistic.machines.insert(mk);
    }

    /// Delivers the next resolution recorded in history, if the workflow is idle and produced
    /// exactly as many commands as were recorded before it.
    fn deliver_peeked_resolution(&mut self) -> Result<()> {
        if self.has_pending_jobs() {
            return Ok(());
        }
        let Some(next) = peek_resolutions(
            self.last_history_from_server
                .peek_next_wft_sequence(self.current_started_event_id),
        )
        .into_iter()
        .nth(self.optimistic.delivered.len()) else {
            return Ok(());
        };
        let produced = self
            .commands
            .iter()
            .filter(|c| !self.is_patch_command(c))
            .count();
        if produced != next.commands_before {
            return Err(nondeterminism!(
                "History recorded an optimistic resolution of {:?} after {} commands, but the \
                 workflow produced {} commands before it would have been observed",
                next.key,
                next.commands_before,
                produced
            ));
        }
        let mk = match &next.key {
            OptimisticKey::Activity(scheduled_event_id) => {
                self.optimistic.activities.get(scheduled_event_id).copied()
            }
            OptimisticKey::Timer(timer_id) => timer_id
                .parse::<u32>()
                .ok()
                .and_then(|seq| self.optimistic.timers.get(&seq).copied()),
        }
        .ok_or_else(|| {
            nondeterminism!(
                "History recorded an optimistic resolution of {:?}, but the workflow didn't start \
                 that command optimistically",
                next.key
            )
        })?;
        let event_count = match next.outcome {
            OptimisticOutcome::Activity(_) => 2,
            OptimisticOutcome::TimerFired => 1,
        };
        let resps = self.resolve_machine_optimistically(mk, next.outcome)?;
        if let Some(t) = next.workflow_time {
            self.set_current_time(t);
        }
        self.process_machine_responses(mk, resps)?;
        self.optimistic.delivered.push(DeliveredResolution {
            after_commands: self.commands.len(),
            event_count,
            command: None,
        });
        Ok(())
    }

    fn resolve_machine_optimistically(
        &mut self,
        mk: MachineKey,
        outcome: OptimisticOutcome,
    ) -> Result<Vec<MachineResponse>> {
        match (self.machine_mut(mk), outcome) {
            (Machines::ActivityMachine(m), OptimisticOutcome::Activity(o)) => {
                m.resolve_optimistically(o)
            }
            (Machines::TimerMachine(m), OptimisticOutcome::TimerFired) => m.fire_optimistically(),
            (m, o) => Err(nondeterminism!(
                "Optimistic resolution {o:?} does not match machine {}",
                m.name()
            )),
        }
    }

    /// Applies an event recording an outcome the workflow already observed. The machine must
    /// have been resolved, and the event must not produce another job.
    pub(super) fn apply_optimistic_resolution_event(
        &mut self,
        mk: MachineKey,
        event_dat: HistEventData,
    ) -> Result<()> {
        if !self.machine(mk).resolved_optimistically() {
            return Err(nondeterminism!(
                "History contains an optimistic resolution the workflow didn't observe: {}",
                event_dat.event
            ));
        }
        let resps = self
            .machine_mut(mk)
            .handle_event(event_dat)?
            .into_iter()
            .filter(|r| !matches!(r, MachineResponse::PushWFJob(_)))
            .collect();
        self.process_machine_responses(mk, resps)
    }

    // ---------------------------------------------------------------------------------------
    // Original execution
    // ---------------------------------------------------------------------------------------

    fn start_optimistic_commands(&mut self, first_new: usize) -> Result<()> {
        let Some(env) = self.optimistic.env.clone() else {
            return Ok(());
        };
        let candidates: Vec<usize> = (first_new..self.commands.len())
            .filter(|ix| self.is_speculative_candidate(&self.commands[*ix]))
            .collect();
        if candidates.is_empty() || !env.enabled() {
            return Ok(());
        }
        let blocked = self
            .optimistic
            .window
            .as_ref()
            .is_some_and(|w| w.checkpoint.is_some())
            || self.local_activity_data.outstanding_la_count() > 0
            || !self.message_outbox.is_empty()
            || self.workflow_is_finished()
            || self
                .commands
                .iter()
                .enumerate()
                .any(|(ix, c)| !candidates.contains(&ix) && !self.command_is_deferrable(c));
        if blocked {
            return Ok(());
        }
        let (Some(received), Some(wft_timeout)) = (
            self.optimistic.wft_received_at,
            self.get_started_info()
                .and_then(|i| i.workflow_task_timeout),
        ) else {
            return Ok(());
        };
        let now = env.now();
        let window_deadline = self
            .optimistic
            .window
            .as_ref()
            .map(|w| w.deadline)
            .unwrap_or_else(|| received + wft_timeout.mul_f32(WINDOW_DEADLINE_FRACTION));
        if window_deadline <= now + wft_timeout.mul_f32(MIN_REMAINING_WINDOW_FRACTION) {
            return Ok(());
        }

        let mut deadline = window_deadline;
        let mut num_activities = 0;
        for ix in &candidates {
            let Some(ProtoCmdAttrs::ScheduleActivityTaskCommandAttributes(attrs)) =
                self.commands[*ix].command.attributes.as_ref()
            else {
                continue;
            };
            let same_queue = attrs
                .task_queue
                .as_ref()
                .is_none_or(|tq| tq.name.is_empty() || tq.name == self.worker_config.task_queue);
            if !same_queue {
                return Ok(());
            }
            if let Some(d) = activity_handoff_deadline(
                now,
                attrs.start_to_close_timeout,
                attrs.schedule_to_close_timeout,
            ) {
                if d <= now {
                    return Ok(());
                }
                deadline = deadline.min(d);
            }
            num_activities += 1;
        }
        if num_activities > 0 && !env.reserve_activity_slots(num_activities) {
            return Ok(());
        }

        let generation = match &self.optimistic.window {
            Some(w) => w.generation,
            None => {
                self.optimistic.next_generation += 1;
                let generation = self.optimistic.next_generation;
                debug!(run_id = %self.run_id, generation, "Opening speculation window");
                self.optimistic.window = Some(Window {
                    generation,
                    checkpoint: None,
                    queued: Default::default(),
                    deadline: window_deadline,
                });
                generation
            }
        };
        let start_offset: prost_types::Duration = now
            .saturating_duration_since(received)
            .try_into()
            .unwrap_or_default();
        for ix in candidates {
            let predicted = self.predicted_event_id(ix);
            let mk = self.commands[ix].machine;
            let seq = match self.machine(mk) {
                Machines::ActivityMachine(m) => m.seq(),
                Machines::TimerMachine(m) => m.seq(),
                _ => continue,
            };
            let is_activity = matches!(self.machine(mk), Machines::ActivityMachine(_));
            self.mark_optimistic(mk, is_activity.then_some(predicted));
            let optimistic_start = Some(OptimisticStart {
                start_offset: Some(start_offset),
                heartbeat_details: None,
            });
            match self.commands[ix].command.attributes.as_mut() {
                Some(ProtoCmdAttrs::ScheduleActivityTaskCommandAttributes(attrs)) => {
                    attrs.optimistic_start = optimistic_start;
                    attrs.request_eager_execution = false;
                    let attrs = attrs.clone();
                    let task = self.optimistic_activity_task(&attrs);
                    env.start_activity(OptimisticActivityStart {
                        run_id: self.run_id.clone(),
                        generation,
                        seq,
                        task,
                    });
                }
                Some(ProtoCmdAttrs::StartTimerCommandAttributes(attrs)) => {
                    attrs.optimistic_start = optimistic_start;
                    let duration = attrs
                        .start_to_fire_timeout
                        .and_then(|d| Duration::try_from(d).ok())
                        .unwrap_or_default();
                    env.start_timer(&self.run_id, generation, seq, duration);
                }
                _ => {}
            }
        }
        env.release_reserved_slots();
        let w = self
            .optimistic
            .window
            .as_mut()
            .expect("Window was just opened");
        w.deadline = deadline;
        env.set_deadline(&self.run_id, generation, deadline);
        Ok(())
    }

    fn optimistic_activity_task(
        &self,
        attrs: &ScheduleActivityTaskCommandAttributes,
    ) -> PollActivityTaskQueueResponse {
        let now = SystemTime::now();
        let timeouts = ActivityTimeouts::normalized(
            attrs,
            self.get_started_info()
                .and_then(|i| i.workflow_run_timeout)
                .unwrap_or_default(),
        );
        PollActivityTaskQueueResponse {
            workflow_namespace: self.worker_config.namespace.clone(),
            workflow_type: Some(WorkflowType {
                name: self.workflow_type.clone(),
            }),
            workflow_execution: Some(WorkflowExecution {
                workflow_id: self.workflow_id.clone(),
                run_id: self.run_id.clone(),
            }),
            activity_type: attrs
                .activity_type
                .clone()
                .or(Some(ActivityType::default())),
            activity_id: attrs.activity_id.clone(),
            header: attrs.header.clone(),
            input: attrs.input.clone(),
            scheduled_time: Some(now.into()),
            current_attempt_scheduled_time: Some(now.into()),
            started_time: Some(now.into()),
            attempt: 1,
            schedule_to_close_timeout: Some(timeouts.schedule_to_close),
            start_to_close_timeout: Some(timeouts.start_to_close),
            heartbeat_timeout: Some(timeouts.heartbeat),
            retry_policy: attrs.retry_policy.clone(),
            priority: attrs.priority.clone(),
            ..Default::default()
        }
    }

    fn check_checkpoint_triggers(&mut self) {
        let Some(w) = self.optimistic.window.as_ref() else {
            return;
        };
        if w.checkpoint.is_some() {
            return;
        }
        if !self.message_outbox.is_empty()
            || self.commands.iter().any(|c| !self.command_is_deferrable(c))
        {
            self.request_optimistic_checkpoint(CheckpointReason::NonDeferrableCommand);
        } else if self.local_activity_data.outstanding_la_count() > 0 {
            self.request_optimistic_checkpoint(CheckpointReason::LocalActivity);
        } else if self.optimistic.block_exited
            && (!self.optimistic.block_active || self.has_pending_non_optimistic_work())
        {
            self.request_optimistic_checkpoint(CheckpointReason::BlockExit);
        }
    }

    /// Handles something that happened to optimistic work outside of the workflow. Returns true
    /// if it may have produced work for the workflow.
    pub(super) fn optimistic_local_result(&mut self, result: OptimisticResult) -> Result<bool> {
        let Some(w) = self.optimistic.window.as_ref() else {
            return Ok(false);
        };
        if w.generation != result.generation || w.checkpoint.is_some() {
            return Ok(false);
        }
        match result.kind {
            OptimisticResultKind::Deadline => {
                self.request_optimistic_checkpoint(CheckpointReason::Deadline);
                return Ok(true);
            }
            OptimisticResultKind::TimerFired { seq } => {
                let Some(mk) = self.optimistic.timers.get(&seq).copied() else {
                    return Ok(false);
                };
                if !matches!(self.machine(mk), Machines::TimerMachine(m) if m.awaiting_optimistic_fire())
                {
                    return Ok(false);
                }
                self.queue_optimistic_outcome(mk, OptimisticOutcome::TimerFired, None);
            }
            OptimisticResultKind::Activity { seq, token, status } => {
                let Some(mk) = self
                    .id_to_machine
                    .get(&CommandID::Activity(seq))
                    .copied()
                    .filter(|mk| self.optimistic.machines.contains(mk))
                else {
                    return Ok(false);
                };
                let Machines::ActivityMachine(m) = self.machine(mk) else {
                    return Ok(false);
                };
                let outcome = match *status {
                    activity_execution_result::Status::Completed(Success { result }) => {
                        Some(OptimisticActivityOutcome::Completed(
                            result.map(|p| Payloads { payloads: vec![p] }),
                        ))
                    }
                    activity_execution_result::Status::Cancelled(Cancellation { failure }) => Some(
                        OptimisticActivityOutcome::Canceled(ActivityTaskCanceledEventAttributes {
                            details: cancel_details(failure),
                            scheduled_event_id: m
                                .optimistic_scheduled_event_id()
                                .unwrap_or_default(),
                            ..Default::default()
                        }),
                    ),
                    activity_execution_result::Status::Failed(_)
                    | activity_execution_result::Status::WillCompleteAsync(_) => None,
                };
                let observable = outcome
                    .as_ref()
                    .is_some_and(|o| m.optimistic_outcome_observable(o));
                let awaited = m.awaiting_optimistic_outcome();
                match outcome {
                    Some(o) if observable => {
                        self.queue_optimistic_outcome(
                            mk,
                            OptimisticOutcome::Activity(o),
                            Some(token),
                        );
                    }
                    _ => {
                        // Outcomes the workflow can't observe here are reported to the server
                        // after the checkpoint, which applies retry policies to failures, so a
                        // workflow waiting on one needs the window to end.
                        if awaited {
                            self.request_optimistic_checkpoint(CheckpointReason::UnobservedOutcome);
                        }
                        return Ok(true);
                    }
                }
            }
        }
        self.deliver_queued_optimistic_outcome()?;
        Ok(true)
    }

    fn queue_optimistic_outcome(
        &mut self,
        machine: MachineKey,
        outcome: OptimisticOutcome,
        token: Option<TaskToken>,
    ) {
        if let Some(w) = self.optimistic.window.as_mut() {
            w.queued.push_back(QueuedOutcome {
                machine,
                outcome,
                token,
            });
        }
    }

    /// Delivers at most one queued outcome, and only into an activation of its own, so replay
    /// can reproduce which activation observed it.
    fn deliver_queued_optimistic_outcome(&mut self) -> Result<()> {
        if self.has_pending_jobs() {
            return Ok(());
        }
        let Some(env) = self.optimistic.env.clone() else {
            return Ok(());
        };
        loop {
            let Some(w) = self.optimistic.window.as_mut() else {
                return Ok(());
            };
            if w.checkpoint.is_some() {
                return Ok(());
            }
            let Some(q) = w.queued.pop_front() else {
                return Ok(());
            };
            let still_observable = match (self.machine(q.machine), &q.outcome) {
                (Machines::ActivityMachine(m), OptimisticOutcome::Activity(o)) => {
                    m.optimistic_outcome_observable(o)
                }
                (Machines::TimerMachine(m), OptimisticOutcome::TimerFired) => {
                    m.awaiting_optimistic_fire()
                }
                _ => false,
            };
            if !still_observable {
                continue;
            }
            return self.deliver_optimistic_outcome(&env, q);
        }
    }

    fn deliver_optimistic_outcome(
        &mut self,
        env: &Arc<dyn OptimisticEnv>,
        q: QueuedOutcome,
    ) -> Result<()> {
        let received = self
            .optimistic
            .wft_received_at
            .ok_or_else(|| fatal!("Speculation window open without a workflow task"))?;
        let offset: prost_types::Duration = env
            .now()
            .saturating_duration_since(received)
            .try_into()
            .unwrap_or_default();
        let after_commands = self.commands.len();
        let first_event_id = self.predicted_event_id(after_commands);
        let (command, event_count, outcome) = match q.outcome {
            OptimisticOutcome::Activity(o) => {
                let Machines::ActivityMachine(m) = self.machine(q.machine) else {
                    return Err(fatal!(
                        "Optimistic activity outcome for a non-activity machine"
                    ));
                };
                let scheduled_event_id = m
                    .optimistic_scheduled_event_id()
                    .ok_or_else(|| fatal!("Optimistic activity without a predicted event id"))?;
                let (cmd_outcome, o) = match o {
                    OptimisticActivityOutcome::Completed(result) => (
                        resolve_attrs::Outcome::Completed(resolve_attrs::Completed {
                            result: result.clone(),
                        }),
                        OptimisticActivityOutcome::Completed(result),
                    ),
                    OptimisticActivityOutcome::Canceled(mut attrs) => {
                        attrs.started_event_id = first_event_id;
                        (
                            resolve_attrs::Outcome::Canceled(resolve_attrs::Canceled {
                                details: attrs.details.clone(),
                            }),
                            OptimisticActivityOutcome::Canceled(attrs),
                        )
                    }
                };
                (
                    ProtoCommand {
                        command_type: CommandType::ResolveOptimisticActivity as i32,
                        attributes: Some(
                            ProtoCmdAttrs::ResolveOptimisticActivityCommandAttributes(
                                ResolveOptimisticActivityCommandAttributes {
                                    scheduled_event_id,
                                    outcome: Some(cmd_outcome),
                                    resolve_offset: Some(offset),
                                },
                            ),
                        ),
                        ..Default::default()
                    },
                    2,
                    OptimisticOutcome::Activity(o),
                )
            }
            OptimisticOutcome::TimerFired => {
                let Machines::TimerMachine(m) = self.machine(q.machine) else {
                    return Err(fatal!("Optimistic timer firing for a non-timer machine"));
                };
                (
                    ProtoCommand {
                        command_type: CommandType::FireOptimisticTimer as i32,
                        attributes: Some(ProtoCmdAttrs::FireOptimisticTimerCommandAttributes(
                            FireOptimisticTimerCommandAttributes {
                                timer_id: m.seq().to_string(),
                                fire_offset: Some(offset),
                            },
                        )),
                        ..Default::default()
                    },
                    1,
                    OptimisticOutcome::TimerFired,
                )
            }
        };
        let resps = self.resolve_machine_optimistically(q.machine, outcome)?;
        // The server computes the recorded workflow time from the offset as sent, so it is used
        // here too, to make replay reproduce the time exactly.
        if let (Some(start), Ok(offset)) = (self.wft_start_time, Duration::try_from(offset)) {
            self.set_current_time(start + offset);
        }
        self.process_machine_responses(q.machine, resps)?;
        self.optimistic.delivered.push(DeliveredResolution {
            after_commands,
            event_count,
            command: Some(command),
        });
        if let Some(token) = q.token {
            env.activity_outcome_observed(&token);
        }
        Ok(())
    }

    /// Lets the window's local work know that the workflow cancelled an optimistic command.
    pub(super) fn optimistic_command_cancelled(&mut self, mk: MachineKey) {
        let (Some(env), Some(w)) = (
            self.optimistic.env.as_ref(),
            self.optimistic.window.as_ref(),
        ) else {
            return;
        };
        if !self.optimistic.machines.contains(&mk) {
            return;
        }
        match self.machine(mk) {
            Machines::ActivityMachine(m) => {
                if !m.resolved_optimistically() && !m.is_abandon() {
                    env.cancel_activity(&self.run_id, w.generation, m.seq());
                }
            }
            Machines::TimerMachine(m) => {
                env.cancel_timer(&self.run_id, w.generation, m.seq());
            }
            _ => {}
        }
    }
}

/// Activity timeouts as the server fills them in when it schedules an activity
/// (`validateAndNormalizeTimeouts`), so that tasks for optimistic activities look like the ones
/// the server dispatches. Lang SDKs rely on that, ex: TypeScript requires a schedule-to-close
/// timeout on every task. Zero means no timeout.
struct ActivityTimeouts {
    schedule_to_close: prost_types::Duration,
    start_to_close: prost_types::Duration,
    heartbeat: prost_types::Duration,
}

impl ActivityTimeouts {
    fn normalized(attrs: &ScheduleActivityTaskCommandAttributes, run_timeout: Duration) -> Self {
        let get = |d: Option<prost_types::Duration>| {
            d.and_then(|d| Duration::try_from(d).ok())
                .unwrap_or_default()
        };
        let mut schedule_to_close = get(attrs.schedule_to_close_timeout);
        let mut start_to_close = get(attrs.start_to_close_timeout);
        let mut heartbeat = get(attrs.heartbeat_timeout);
        if !schedule_to_close.is_zero() {
            start_to_close = if start_to_close.is_zero() {
                schedule_to_close
            } else {
                start_to_close.min(schedule_to_close)
            };
        } else {
            schedule_to_close = run_timeout;
        }
        if !run_timeout.is_zero() {
            schedule_to_close = schedule_to_close.min(run_timeout);
            start_to_close = start_to_close.min(run_timeout);
            heartbeat = heartbeat.min(run_timeout);
        }
        heartbeat = heartbeat.min(start_to_close);
        let proto = |d: Duration| d.try_into().unwrap_or_default();
        Self {
            schedule_to_close: proto(schedule_to_close),
            start_to_close: proto(start_to_close),
            heartbeat: proto(heartbeat),
        }
    }
}

/// The time by which an activity started now must be handed off, so the server can enforce its
/// timeouts.
fn activity_handoff_deadline(
    now: Instant,
    start_to_close: Option<prost_types::Duration>,
    schedule_to_close: Option<prost_types::Duration>,
) -> Option<Instant> {
    [start_to_close, schedule_to_close]
        .into_iter()
        .flatten()
        .filter_map(|d| Duration::try_from(d).ok())
        .filter(|d| !d.is_zero())
        .min()
        .map(|d| now + d)
}

fn cancel_details(failure: Option<Failure>) -> Option<Payloads> {
    let mut f = failure?;
    loop {
        if let Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo { details, .. })) =
            f.failure_info
        {
            return details;
        }
        f = *f.cause?;
    }
}

/// Finds the resolutions the workflow observed during the workflow task that produced the given
/// sequence, in order, with the number of command events recorded before each, not counting
/// patch markers (see [WorkflowMachines::is_patch_command]).
fn peek_resolutions(events: &[HistoryEvent]) -> Vec<PeekedResolution> {
    let mut commands = 0;
    let mut out = vec![];
    for e in events {
        if e.is_command_event() {
            if !is_patch_event(e) {
                commands += 1;
            }
            continue;
        }
        if !e.is_optimistic_resolution() {
            continue;
        }
        let (key, outcome, resolution) = match &e.attributes {
            Some(history_event::Attributes::ActivityTaskCompletedEventAttributes(a)) => (
                OptimisticKey::Activity(a.scheduled_event_id),
                OptimisticActivityOutcome::Completed(a.result.clone()),
                a.optimistic_resolution.as_ref(),
            ),
            Some(history_event::Attributes::ActivityTaskCanceledEventAttributes(a)) => (
                OptimisticKey::Activity(a.scheduled_event_id),
                OptimisticActivityOutcome::Canceled(a.clone()),
                a.optimistic_resolution.as_ref(),
            ),
            Some(history_event::Attributes::TimerFiredEventAttributes(a)) => {
                out.push(PeekedResolution {
                    key: OptimisticKey::Timer(a.timer_id.clone()),
                    commands_before: commands,
                    workflow_time: a
                        .optimistic_resolution
                        .as_ref()
                        .and_then(|r| r.workflow_time)
                        .and_then(|t| t.try_into().ok()),
                    outcome: OptimisticOutcome::TimerFired,
                });
                continue;
            }
            // The started event written with an outcome carries no resolution of its own.
            _ => continue,
        };
        out.push(PeekedResolution {
            key,
            commands_before: commands,
            workflow_time: resolution
                .and_then(|r| r.workflow_time)
                .and_then(|t| t.try_into().ok()),
            outcome: OptimisticOutcome::Activity(outcome),
        });
    }
    out
}

/// The history side of [WorkflowMachines::is_patch_command].
fn is_patch_event(e: &HistoryEvent) -> bool {
    match &e.attributes {
        Some(history_event::Attributes::UpsertWorkflowSearchAttributesEventAttributes(a)) => a
            .search_attributes
            .as_ref()
            .is_some_and(|sa| sa.indexed_fields.contains_key(VERSION_SEARCH_ATTR_KEY)),
        _ => e.get_patch_marker_details().is_some(),
    }
}
