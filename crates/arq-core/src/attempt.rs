//! Attempt lifecycle. Execution, ownership, and finish are separate axes.
//!
//! A handler return does not ACK. Ownership lost during the handler stays lost.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    Claimed,
    Running,
    /// Cancel was requested. Python may still be running.
    CancelRequested,
    /// Stop was observed. The next step is finish, not another wait.
    Stopped,
    /// The handler returned. Ownership is independent of this state.
    HandlerFinished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipState {
    Held,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishState {
    NotStarted,
    InFlight,
    Confirmed,
    RejectedStale,
    Unconfirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptLifecycle {
    pub execution: ExecutionState,
    pub ownership: OwnershipState,
    pub finish: FinishState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptEvent {
    StartHandler,
    RequestCancel,
    ConfirmExecutionStopped,
    LoseOwnership,
    HandlerReturned,
    BeginFinish,
    FinishConfirmed,
    FinishRejectedStale,
    FinishUnconfirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptTransitionError {
    pub lifecycle: AttemptLifecycle,
    pub event: AttemptEvent,
}

impl std::fmt::Display for AttemptTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "illegal attempt event {:?} from execution {:?} finish {:?}",
            self.event, self.lifecycle.execution, self.lifecycle.finish
        )
    }
}

impl std::error::Error for AttemptTransitionError {}

impl AttemptLifecycle {
    pub fn claimed() -> Self {
        Self {
            execution: ExecutionState::Claimed,
            ownership: OwnershipState::Held,
            finish: FinishState::NotStarted,
        }
    }

    pub fn ownership_lost(self) -> bool {
        self.ownership == OwnershipState::Lost
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self.finish,
            FinishState::Confirmed | FinishState::RejectedStale | FinishState::Unconfirmed
        )
    }

    /// Lost ownership must not ACK, even when finish is already in flight.
    pub fn allowed_success_ack(self) -> bool {
        self.ownership == OwnershipState::Held && self.finish == FinishState::InFlight
    }

    pub fn transition(self, event: AttemptEvent) -> Result<Self, AttemptTransitionError> {
        let mut next = self;
        let ok = match event {
            AttemptEvent::StartHandler => {
                if next.execution == ExecutionState::Claimed && next.finish == FinishState::NotStarted {
                    next.execution = ExecutionState::Running;
                    true
                } else {
                    false
                }
            }
            AttemptEvent::RequestCancel => match next.execution {
                ExecutionState::Running | ExecutionState::CancelRequested
                    if next.finish == FinishState::NotStarted =>
                {
                    next.execution = ExecutionState::CancelRequested;
                    true
                }
                _ => false,
            },
            AttemptEvent::LoseOwnership => {
                if next.is_terminal() {
                    false
                } else {
                    next.ownership = OwnershipState::Lost;
                    true
                }
            }
            AttemptEvent::ConfirmExecutionStopped => match next.execution {
                ExecutionState::CancelRequested | ExecutionState::Running
                    if next.finish == FinishState::NotStarted =>
                {
                    if next.execution == ExecutionState::Running && next.ownership != OwnershipState::Lost {
                        false
                    } else {
                        next.execution = ExecutionState::Stopped;
                        true
                    }
                }
                _ => false,
            },
            AttemptEvent::HandlerReturned => match next.execution {
                ExecutionState::Running | ExecutionState::CancelRequested
                    if next.finish == FinishState::NotStarted =>
                {
                    next.execution = ExecutionState::HandlerFinished;
                    true
                }
                _ => false,
            },
            AttemptEvent::BeginFinish => match next.execution {
                ExecutionState::HandlerFinished | ExecutionState::Stopped
                    if next.finish == FinishState::NotStarted =>
                {
                    next.finish = FinishState::InFlight;
                    true
                }
                _ => false,
            },
            AttemptEvent::FinishConfirmed => {
                if matches!(
                    next.execution,
                    ExecutionState::HandlerFinished | ExecutionState::Stopped
                ) && next.finish == FinishState::InFlight
                {
                    next.finish = FinishState::Confirmed;
                    true
                } else {
                    false
                }
            }
            AttemptEvent::FinishRejectedStale => {
                if matches!(
                    next.execution,
                    ExecutionState::HandlerFinished | ExecutionState::Stopped
                ) && next.finish == FinishState::InFlight
                {
                    next.finish = FinishState::RejectedStale;
                    true
                } else {
                    false
                }
            }
            AttemptEvent::FinishUnconfirmed => {
                if matches!(
                    next.execution,
                    ExecutionState::HandlerFinished | ExecutionState::Stopped
                ) && next.finish == FinishState::InFlight
                {
                    next.finish = FinishState::Unconfirmed;
                    true
                } else {
                    false
                }
            }
        };
        if ok {
            Ok(next)
        } else {
            Err(AttemptTransitionError {
                lifecycle: self,
                event,
            })
        }
    }
}
