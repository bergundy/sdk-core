#![allow(clippy::large_enum_variant)]

use super::{
    EventInfo, MachineError, NewMachineWithCommand, OnEventWrapper, StateMachine, TransitionResult,
    WFMachinesAdapter, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    CommandAnnotations, ProtoCommandExt, WFMachinesError, fatal, machines::HistEventData,
    nondeterminism,
};
use std::convert::TryFrom;
use temporalio_common::protos::{
    coresdk::{
        HistoryEventId,
        workflow_activation::FireTimer,
        workflow_commands::{CancelTimer, StartTimer},
    },
    temporal::api::{
        command::v1::{Command, command},
        enums::v1::{CommandType, EventType},
        history::v1::{TimerFiredEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name TimerMachine;
    command TimerMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted(HistoryEventId), on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, shared on_cancel) --> Canceled;
    // An optimistically started timer already is in the command list sent at the checkpoint, and
    // later commands' event ids depend on it, so cancelling it must not remove the command.
    StartCommandCreated --(OptimisticCancel, shared on_optimistic_cancel)
      --> OptimisticCancelCommandCreated;
    OptimisticCancelCommandCreated --(CommandCancelTimer) --> OptimisticCancelCommandSent;
    OptimisticCancelCommandSent --(TimerStarted(HistoryEventId)) --> CancelTimerCommandSent;

    StartCommandRecorded --(TimerFired(TimerFiredEventAttributes), shared on_timer_fired) --> Fired;
    StartCommandRecorded --(Cancel, shared on_cancel) --> CancelTimerCommandCreated;

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated
        --(CommandCancelTimer, on_command_cancel_timer) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled) --> Canceled;

    // Ignore any spurious cancellations after resolution
    Canceled --(Cancel) --> Canceled;
    Fired --(Cancel) --> Fired;
    OptimisticCancelCommandCreated --(Cancel) --> OptimisticCancelCommandCreated;
    OptimisticCancelCommandSent --(Cancel) --> OptimisticCancelCommandSent;
    CancelTimerCommandSent --(Cancel) --> CancelTimerCommandSent;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum TimerMachineCommand {
    Complete,
    IssueCancelCmd(command::Attributes),
    // We don't issue activations for timer cancellations. Lang SDK is expected to cancel
    // it's own timers when user calls cancel, and they cannot be cancelled by any other
    // means.
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: StartTimer,
    cancelled_before_sent: bool,
    annotations: CommandAnnotations,
    /// Set if the timer was started optimistically inside a speculation window.
    optimistic: bool,
    /// Set once the workflow observed the optimistic timer firing before the event recording it
    /// was applied.
    resolved_optimistically: bool,
}

/// Creates a new, scheduled, timer as a [CancellableCommand]
pub(super) fn new_timer(
    attribs: StartTimer,
    annotations: CommandAnnotations,
) -> NewMachineWithCommand {
    let (timer, add_cmd) = TimerMachine::new_scheduled(attribs, annotations);
    NewMachineWithCommand {
        command: add_cmd,
        machine: timer.into(),
    }
}

impl TimerMachine {
    /// Create a new timer and immediately schedule it
    fn new_scheduled(
        attribs: StartTimer,
        annotations: CommandAnnotations,
    ) -> (Self, command::Attributes) {
        let mut s = Self::new(attribs, annotations);
        OnEventWrapper::on_event_mut(&mut s, TimerMachineEvents::Schedule)
            .expect("Scheduling timers doesn't fail");
        let cmd = s.shared_state().attrs.into();
        (s, cmd)
    }

    fn new(attribs: StartTimer, annotations: CommandAnnotations) -> Self {
        Self::from_parts(
            Created {}.into(),
            SharedState {
                attrs: attribs,
                cancelled_before_sent: false,
                annotations,
                optimistic: false,
                resolved_optimistically: false,
            },
        )
    }

    pub(super) fn cancel(
        &mut self,
        annotations: CommandAnnotations,
    ) -> Result<Vec<MachineResponse>, MachineError<WFMachinesError>> {
        self.shared_state.annotations.override_with(annotations);
        if self.shared_state.resolved_optimistically {
            return Ok(vec![]);
        }
        let event = if self.shared_state.optimistic
            && matches!(self.state(), TimerMachineState::StartCommandCreated(_))
        {
            TimerMachineEvents::OptimisticCancel
        } else {
            TimerMachineEvents::Cancel
        };
        Ok(match OnEventWrapper::on_event_mut(self, event)?.pop() {
            Some(TimerMachineCommand::IssueCancelCmd(cmd)) => {
                vec![MachineResponse::IssueNewCommand(Command::new(
                    cmd,
                    self.shared_state.annotations.clone(),
                ))]
            }
            None => vec![],
            x => panic!("Invalid cancel event response {x:?}"),
        })
    }

    pub(super) fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state().cancelled_before_sent
    }

    pub(super) fn seq(&self) -> u32 {
        self.shared_state.attrs.seq
    }

    pub(super) fn attrs(&self) -> &StartTimer {
        &self.shared_state.attrs
    }

    pub(super) fn set_optimistic(&mut self) {
        self.shared_state.optimistic = true;
    }

    pub(super) fn is_optimistic(&self) -> bool {
        self.shared_state.optimistic
    }

    pub(super) fn resolved_optimistically(&self) -> bool {
        self.shared_state.resolved_optimistically
    }

    /// True while the workflow may still observe this optimistic timer firing during the current
    /// workflow task.
    pub(super) fn awaiting_optimistic_fire(&self) -> bool {
        self.shared_state.optimistic
            && !self.shared_state.resolved_optimistically
            && matches!(self.state(), TimerMachineState::StartCommandCreated(_))
    }

    /// Fires the timer for the workflow during the workflow task that started it. The event
    /// recording the firing is applied later without producing a job.
    pub(super) fn fire_optimistically(&mut self) -> Result<Vec<MachineResponse>, WFMachinesError> {
        if !self.awaiting_optimistic_fire() {
            return Err(fatal!(
                "Timer (seq {}) can't be fired optimistically in state {}",
                self.shared_state.attrs.seq,
                self.state()
            ));
        }
        self.shared_state.resolved_optimistically = true;
        Ok(vec![
            FireTimer {
                seq: self.shared_state.attrs.seq,
            }
            .into(),
        ])
    }
}

impl TryFrom<HistEventData> for TimerMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::TimerStarted => Self::TimerStarted(e.event_id),
            EventType::TimerCanceled => Self::TimerCanceled,
            EventType::TimerFired => {
                if let Some(history_event::Attributes::TimerFiredEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::TimerFired(attrs)
                } else {
                    return Err(fatal!("Timer fired attribs were unset: {e}"));
                }
            }
            _ => {
                return Err(nondeterminism!(
                    "Timer machine does not handle this event: {e}"
                ));
            }
        })
    }
}

impl TryFrom<CommandType> for TimerMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::StartTimer => Self::CommandStartTimer,
            CommandType::CancelTimer => Self::CommandCancelTimer,
            _ => return Err(()),
        })
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> TimerMachineTransition<StartCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandCreated {}

impl CancelTimerCommandCreated {
    pub(super) fn on_command_cancel_timer(self) -> TimerMachineTransition<CancelTimerCommandSent> {
        TransitionResult::ok(vec![], CancelTimerCommandSent::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandSent {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

impl From<CancelTimerCommandSent> for Canceled {
    fn from(_: CancelTimerCommandSent) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Fired {}

#[derive(Default, Clone)]
pub(super) struct StartCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct OptimisticCancelCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct OptimisticCancelCommandSent {}

impl From<OptimisticCancelCommandCreated> for OptimisticCancelCommandSent {
    fn from(_: OptimisticCancelCommandCreated) -> Self {
        Self::default()
    }
}

impl From<OptimisticCancelCommandSent> for CancelTimerCommandSent {
    fn from(_: OptimisticCancelCommandSent) -> Self {
        Self::default()
    }
}

impl StartCommandCreated {
    pub(super) fn on_optimistic_cancel(
        self,
        dat: &mut SharedState,
    ) -> TimerMachineTransition<OptimisticCancelCommandCreated> {
        TransitionResult::ok(
            vec![TimerMachineCommand::IssueCancelCmd(
                CancelTimer { seq: dat.attrs.seq }.into(),
            )],
            OptimisticCancelCommandCreated::default(),
        )
    }

    pub(super) fn on_timer_started(
        self,
        _id: HistoryEventId,
    ) -> TimerMachineTransition<StartCommandRecorded> {
        TransitionResult::default()
    }

    pub(super) fn on_cancel(self, dat: &mut SharedState) -> TimerMachineTransition<Canceled> {
        dat.cancelled_before_sent = true;

        #[cfg(feature = "antithesis_assertions")]
        crate::antithesis::assert_sometimes!(
            true,
            "Timer cancelled before sent to server",
            ::serde_json::json!({"timer_seq": dat.attrs.seq})
        );

        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartCommandRecorded {}

impl StartCommandRecorded {
    pub(super) fn on_timer_fired(
        self,
        dat: &mut SharedState,
        attrs: TimerFiredEventAttributes,
    ) -> TimerMachineTransition<Fired> {
        if dat.attrs.seq.to_string() == attrs.timer_id {
            TransitionResult::ok(vec![TimerMachineCommand::Complete], Fired::default())
        } else {
            TransitionResult::Err(nondeterminism!(
                "Timer fired event did not have expected timer id {}, it was {}!",
                dat.attrs.seq,
                attrs.timer_id
            ))
        }
    }

    pub(super) fn on_cancel(
        self,
        dat: &mut SharedState,
    ) -> TimerMachineTransition<CancelTimerCommandCreated> {
        #[cfg(feature = "antithesis_assertions")]
        crate::antithesis::assert_sometimes!(
            true,
            "Timer cancelled after started",
            ::serde_json::json!({"timer_seq": dat.attrs.seq})
        );

        TransitionResult::ok(
            vec![TimerMachineCommand::IssueCancelCmd(
                CancelTimer { seq: dat.attrs.seq }.into(),
            )],
            CancelTimerCommandCreated::default(),
        )
    }
}

impl WFMachinesAdapter for TimerMachine {
    fn adapt_response(
        &self,
        my_command: TimerMachineCommand,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            // Fire the completion
            TimerMachineCommand::Complete => vec![
                FireTimer {
                    seq: self.shared_state.attrs.seq,
                }
                .into(),
            ],
            TimerMachineCommand::IssueCancelCmd(c) => {
                vec![MachineResponse::IssueNewCommand(Command::new(
                    c,
                    self.shared_state.annotations.clone(),
                ))]
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::mem::discriminant;

    #[test]
    fn cancels_ignored_terminal() {
        for state in [TimerMachineState::Canceled(Canceled {}), Fired {}.into()] {
            let mut s = TimerMachine::from_parts(state.clone(), Default::default());
            let cmds = s.cancel(Default::default()).unwrap();
            assert_eq!(cmds.len(), 0);
            assert_eq!(discriminant(&state), discriminant(s.state()));
        }
    }
}
