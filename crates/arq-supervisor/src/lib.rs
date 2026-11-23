//! Parent supervisor. This crate does not initialize CPython and does not depend
//! on `arq-pybridge` or PyO3.
//!
//! Slot processes are started with `watchexec-supervisor` 6.0.0 (`start_job`).
//! The metrics helper is a separate `tokio::process::Command`.

mod busy;
mod child;
mod control;
mod fd;
mod memory;
mod metrics;
mod osutil;
mod pidfd;
mod policy;
mod registry;
mod signals;
mod spawn;
mod supervisor;

#[cfg(test)]
mod boundary;
#[cfg(test)]
mod testutil;

pub use child::{
    ok_outcome, parent_or_signal_drain, run_child_session, scripted_delivery, ChildParams, ChildReport, ChildSession,
    DispatchMode, Feed, Live, Scripts, SessionControl,
};
pub use control::{poll_kills, poll_kills_default, Binding};
pub use pidfd::{prepare_child_runtime, ParentWatch, PARENT_FD_ENV};
pub use policy::{exit_effects, exit_for_storage, max_tasks_reached};
pub use registry::{heartbeat_supervisor, hostname, register_supervisor, release_supervisor, RegistryBody, RegistryChild};
pub use signals::Shutdown;
pub use spawn::{spawn_slot, CommandSpec, SPAWN_CRATE};
pub use supervisor::{run, PauseMode, PidfdMode, PressureOverride, SupervisorExit, SupervisorSpec, Trace};

/// Names the job handle used to spawn slot processes.
pub fn supervisor_job() -> &'static str {
    std::any::type_name::<watchexec_supervisor::job::Job>()
}
