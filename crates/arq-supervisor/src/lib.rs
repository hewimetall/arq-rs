//! Parent supervisor. This crate does not depend on `arq-pybridge` or PyO3.
//!
//! Slot spawning is not implemented yet. The dependency on `watchexec-supervisor`
//! is the production spawn crate named by the manifest.

/// Names the job handle the supervisor will use to spawn children.
pub fn supervisor_job() -> &'static str {
    std::any::type_name::<watchexec_supervisor::job::Job>()
}
