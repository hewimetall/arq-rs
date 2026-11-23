//! One supervisor slot. `Ready`, `Drain`, and `Reaped` are the manifest transitions.

use std::sync::Mutex;

/// `Starting → Running → Draining → Stopping → Exited`, or `Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessPhase {
    Starting,
    Running,
    Draining,
    Stopping,
    Exited,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessEvent {
    Ready,
    StartFailed,
    Drain,
    Stop,
    Reaped,
    Crash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessTransitionError {
    pub from: ProcessPhase,
    pub event: ProcessEvent,
}

impl std::fmt::Display for ProcessTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "illegal slot event {:?} from {:?}", self.event, self.from)
    }
}

impl std::error::Error for ProcessTransitionError {}

/// One slot. Copying the value does not share the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotLifecycle {
    phase: ProcessPhase,
}

impl SlotLifecycle {
    pub fn starting() -> Self {
        Self {
            phase: ProcessPhase::Starting,
        }
    }

    pub fn phase(self) -> ProcessPhase {
        self.phase
    }

    /// A second drain while `Draining` is a no-op.
    /// `Reaped` is accepted only from `Stopping` or `Failed`.
    pub fn transition(self, event: ProcessEvent) -> Result<Self, ProcessTransitionError> {
        let phase = match (self.phase, event) {
            (ProcessPhase::Starting, ProcessEvent::Ready) => ProcessPhase::Running,
            (ProcessPhase::Starting, ProcessEvent::StartFailed) => ProcessPhase::Failed,
            (ProcessPhase::Running, ProcessEvent::Drain) => ProcessPhase::Draining,
            (ProcessPhase::Running, ProcessEvent::Stop) => ProcessPhase::Stopping,
            (ProcessPhase::Running, ProcessEvent::Crash) => ProcessPhase::Failed,
            (ProcessPhase::Draining, ProcessEvent::Drain) => ProcessPhase::Draining,
            (ProcessPhase::Draining, ProcessEvent::Stop) => ProcessPhase::Stopping,
            (ProcessPhase::Draining, ProcessEvent::Crash) => ProcessPhase::Failed,
            (ProcessPhase::Stopping, ProcessEvent::Stop) => ProcessPhase::Stopping,
            (ProcessPhase::Stopping, ProcessEvent::Reaped) => ProcessPhase::Exited,
            (ProcessPhase::Failed, ProcessEvent::Reaped) => ProcessPhase::Exited,
            (from, event) => return Err(ProcessTransitionError { from, event }),
        };
        Ok(Self { phase })
    }
}

/// Single transition owner for one slot.
#[derive(Debug)]
pub struct SlotOwner {
    inner: Mutex<SlotLifecycle>,
}

impl SlotOwner {
    pub fn starting() -> Self {
        Self {
            inner: Mutex::new(SlotLifecycle::starting()),
        }
    }

    pub fn apply(&self, event: ProcessEvent) -> Result<ProcessPhase, ProcessTransitionError> {
        let mut life = self.inner.lock().expect("slot owner");
        *life = life.transition(event)?;
        Ok(life.phase())
    }

    pub fn phase(&self) -> ProcessPhase {
        self.inner.lock().expect("slot owner").phase()
    }
}
