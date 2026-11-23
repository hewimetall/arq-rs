//! PyO3 bridge. Maturin builds `arq_rs._native` with `extension-module`.
//! `arq-worker` links this crate as an rlib and leaves that feature off.
//!
//! Constructing these types does not initialize CPython. The child calls
//! [`init_child_interpreter`] after its restart handlers are installed.
//! `arq-supervisor` does not depend on this crate.

mod child;
mod decide;
mod encode;
mod load;
mod native;
mod ping;
mod service;

pub use child::{
    add_python_path, arm_restart_signals, configured_signal_handlers, dispatch_json, init_child_interpreter,
    invoke_note_dead, load_application, ping_running_loop, record_outcome, run_child_script, saw_drain, saw_int, saw_term,
    spawn_event_loop, stop_event_loop, ArmedSignals, InitError, RunningLoop,
};
pub use decide::{
    action_for, budget_effect, dispatch_then_commit, note_dead_after_commit, observe_ping, plan_shutdown, CommitAction,
    DispatchRaw, LoopGate, PingExit, ShutdownPlan,
};
pub use ping::{begin_ping, end_ping, ping_busy, ping_loop, run_stage, wait_flag};
pub use service::{connects, describe_config, parsed_outcome};

/// JSON body for a client that has already been closed.
pub fn closed_client_reply() -> String {
    encode::error_response(&load::BridgeError::Closed)
}

/// `true` only when maturin (or another caller) builds with `extension-module`.
pub const fn extension_module_enabled() -> bool {
    cfg!(feature = "extension-module")
}

/// Links the PyO3 crate without initializing an interpreter.
pub fn pyo3_linked() -> &'static str {
    std::any::type_name::<pyo3::PyErr>()
}

/// Install `parse_config`, `redis_connects`, and `Backend` on a module object.
///
/// Maturin calls this from the cdylib entry. Tests call it after an explicit
/// interpreter init so the same rlib lines run without loading a second copy.
pub fn register_native(module: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    arq_native(module)
}

#[cfg(test)]
mod tests {
    #[test]
    fn linking_pyo3_does_not_initialize_python() {
        assert!(!super::extension_module_enabled());
        let _ = super::pyo3_linked();
        assert!(super::closed_client_reply().contains("closed"));
        assert_eq!(unsafe { pyo3::ffi::Py_IsInitialized() }, 0);
    }
}

#[pyo3::pymodule(name = "_native")]
fn arq_native(module: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    native::register(module)
}
