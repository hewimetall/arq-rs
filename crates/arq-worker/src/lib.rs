//! Composition root for arq-rs.
//!
//! This is the only crate that depends on `arq-core`, `arq-redis`, `arq-pybridge`,
//! and `arq-supervisor`. It links `arq-pybridge` as an rlib and does not enable
//! `extension-module`, so the binary links libpython.
//!
//! The parent arms `SIGTERM` and `SIGINT`, then calls [`arq_supervisor::run`].
//! It does not initialize CPython and it does not keep a second slot machine.
//! `--factory` is forwarded to the child. The child arms restart signals, watches
//! the parent pidfd, and only then initializes Python and calls the factory.
//! The child then claims, dispatches, renews, and finishes one task at a time.

mod child;
mod cli;
mod delivery;
mod parent;

pub use child::run_child;
#[doc(hidden)]
pub use delivery::{
    fail_next_finish_transports, production_arq_metric, production_begin_finish_count, production_finish_terminal,
    production_halt_log_count,
};
pub use cli::{
    execute, package_version, prepare, redis_endpoint_label, usage, version_line, Action, ChildPlan, ParentPlan,
};
pub use parent::{run_parent, spec_from_plan};

/// `true` when this process has initialized CPython.
pub fn python_initialized() -> bool {
    unsafe { pyo3::ffi::Py_IsInitialized() != 0 }
}

/// Names the four crates linked into the binary. `extension-module` stays off.
pub fn linked_crates() -> (&'static str, bool) {
    let _config = arq_core::CONFIG_EXIT_CODE;
    let _driver = arq_redis::RedisBackend::driver();
    let _job = arq_supervisor::supervisor_job();
    (
        arq_pybridge::pyo3_linked(),
        arq_pybridge::extension_module_enabled(),
    )
}
