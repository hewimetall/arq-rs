//! Child interpreter startup. Linking this crate does not call into CPython.
//!
//! `init_child_interpreter` is explicit. `arq-supervisor` does not link this crate,
//! so constructing any type here from the supervisor is impossible.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::JoinHandle;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::decide::{note_dead_after_commit, DispatchRaw};
use crate::ping::ping_loop;

static TERM: AtomicBool = AtomicBool::new(false);
static INT: AtomicBool = AtomicBool::new(false);
static SIGNAL_HANDLERS: AtomicI32 = AtomicI32::new(-1);

enum InitState {
    Fresh,
    Ready,
}

fn init_state() -> &'static Mutex<InitState> {
    static STATE: OnceLock<Mutex<InitState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(InitState::Fresh))
}

extern "C" fn on_signal(sig: i32) {
    if sig == libc::SIGTERM {
        TERM.store(true, Ordering::SeqCst);
    } else if sig == libc::SIGINT {
        INT.store(true, Ordering::SeqCst);
    }
}

pub struct ArmedSignals {
    term: libc::sigaction,
    int_signal: libc::sigaction,
}

/// Install the process handlers that restart or drain a child. This does not initialize Python.
pub fn arm_restart_signals() -> std::io::Result<ArmedSignals> {
    let term = install(libc::SIGTERM)?;
    let int_signal = install(libc::SIGINT)?;
    Ok(ArmedSignals { term, int_signal })
}

fn install(signal: i32) -> std::io::Result<libc::sigaction> {
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = on_signal as *const () as libc::sighandler_t;
        if libc::sigemptyset(&mut action.sa_mask) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut previous: libc::sigaction = std::mem::zeroed();
        if libc::sigaction(signal, &action, &mut previous) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(previous)
    }
}

impl Drop for ArmedSignals {
    fn drop(&mut self) {
        unsafe {
            libc::sigaction(libc::SIGTERM, &self.term, std::ptr::null_mut());
            libc::sigaction(libc::SIGINT, &self.int_signal, std::ptr::null_mut());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildStartup {
    pub install_signal_handlers: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitError {
    SignalsNotArmed,
    Python,
}

impl std::fmt::Display for InitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SignalsNotArmed => write!(formatter, "restart signals must be armed before Python init"),
            Self::Python => write!(formatter, "Py_InitializeFromConfig failed"),
        }
    }
}

pub fn startup_plan(signals_armed: bool) -> Result<ChildStartup, InitError> {
    if !signals_armed {
        return Err(InitError::SignalsNotArmed);
    }
    Ok(ChildStartup { install_signal_handlers: 0 })
}

/// Initialize CPython with `install_signal_handlers = 0`. A second call is a no-op.
pub fn init_child_interpreter(armed: &ArmedSignals) -> Result<ChildStartup, InitError> {
    let _armed = armed;
    let plan = startup_plan(true)?;
    let mut state = init_state().lock().unwrap_or_else(|error| error.into_inner());
    if matches!(*state, InitState::Ready) {
        return Ok(plan);
    }
    unsafe {
        if pyo3::ffi::Py_IsInitialized() != 0 {
            *state = InitState::Ready;
            return Ok(plan);
        }
        let mut config = std::mem::MaybeUninit::<pyo3::ffi::PyConfig>::zeroed();
        pyo3::ffi::PyConfig_InitPythonConfig(config.as_mut_ptr());
        let config = config.as_mut_ptr();
        (*config).install_signal_handlers = plan.install_signal_handlers;
        (*config).parse_argv = 0;
        SIGNAL_HANDLERS.store((*config).install_signal_handlers, Ordering::SeqCst);
        let status = pyo3::ffi::Py_InitializeFromConfig(config);
        pyo3::ffi::PyConfig_Clear(config);
        if pyo3::ffi::PyStatus_Exception(status) != 0 {
            return Err(InitError::Python);
        }
        pyo3::ffi::PyEval_SaveThread();
    }
    *state = InitState::Ready;
    Ok(plan)
}

pub fn configured_signal_handlers() -> i32 {
    SIGNAL_HANDLERS.load(Ordering::SeqCst)
}

pub fn saw_term() -> bool {
    TERM.load(Ordering::SeqCst)
}

pub fn saw_int() -> bool {
    INT.load(Ordering::SeqCst)
}

pub fn python_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python")
}

pub fn add_python_path(py: Python<'_>) -> PyResult<()> {
    let root = python_root();
    let sys = py.import("sys")?;
    sys.getattr("path")?.call_method1("insert", (0, root.display().to_string()))?;
    Ok(())
}

pub fn split_app_path(path: &str) -> Result<(&str, &str), &'static str> {
    let Some((module, attribute)) = path.split_once(':') else {
        return Err("Expected application path: module:attribute");
    };
    if module.is_empty() || attribute.is_empty() {
        return Err("Expected application path: module:attribute");
    }
    Ok((module, attribute))
}

pub fn load_application<'py>(py: Python<'py>, path: &str, factory: bool) -> PyResult<Bound<'py, PyAny>> {
    let (module, attribute) = split_app_path(path).map_err(pyo3::exceptions::PyValueError::new_err)?;
    let loaded = py.import(module)?.getattr(attribute)?;
    let app = if factory { loaded.call0()? } else { loaded };
    if app.getattr("dispatch").is_err() || app.getattr("client").is_err() {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!("{path} must be an App")));
    }
    Ok(app)
}

pub struct RunningLoop {
    pub handle: JoinHandle<()>,
    pub event_loop: Py<PyAny>,
}

pub fn spawn_event_loop() -> PyResult<RunningLoop> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let handle = std::thread::Builder::new()
        .name("arq-asyncio".into())
        .spawn(move || {
            let started = Python::attach(|py| -> PyResult<Py<PyAny>> {
                let asyncio = py.import("asyncio")?;
                let event_loop = asyncio.call_method0("new_event_loop")?;
                asyncio.call_method1("set_event_loop", (&event_loop,))?;
                let owned = event_loop.clone().unbind();
                sender.send(owned).map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("loop channel closed"))?;
                event_loop.call_method0("run_forever")?;
                Ok(event_loop.unbind())
            });
            if let Err(error) = started {
                let _ = sender.send(Python::attach(|py| py.None()));
                Python::attach(|py| error.print(py));
            }
        })
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
    let event_loop = receiver
        .recv()
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Python loop thread exited before signalling"))?;
    let usable = Python::attach(|py| !event_loop.bind(py).is_none());
    if !usable {
        return Err(pyo3::exceptions::PyRuntimeError::new_err("Python loop thread failed to start"));
    }
    Ok(RunningLoop { handle, event_loop })
}

pub fn stop_event_loop(event_loop: &Py<PyAny>) -> PyResult<()> {
    Python::attach(|py| {
        let event_loop = event_loop.bind(py);
        let stop = event_loop.getattr("stop")?;
        event_loop.call_method1("call_soon_threadsafe", (stop,))?;
        Ok(())
    })
}

pub fn ping_running_loop(event_loop: &Py<PyAny>, timeout: std::time::Duration) -> bool {
    let cloned = Python::attach(|py| event_loop.clone_ref(py));
    ping_loop(cloned, timeout)
}

pub fn exception_name_is_grace(name: &str) -> bool {
    name == "CancelGraceExceeded" || name.ends_with(".CancelGraceExceeded")
}

pub fn raw_from_exception_name(name: &str) -> DispatchRaw {
    if exception_name_is_grace(name) {
        DispatchRaw::GraceExceeded
    } else {
        DispatchRaw::Raised
    }
}

fn raw_from_err(py: Python<'_>, err: PyErr) -> DispatchRaw {
    let name = err
        .get_type(py)
        .getattr("__name__")
        .ok()
        .and_then(|value| value.extract::<String>().ok())
        .unwrap_or_default();
    raw_from_exception_name(&name)
}

pub fn dispatch_json(event_loop: &Py<PyAny>, app: &Py<PyAny>, message_json: &str, timeout_secs: f64) -> PyResult<DispatchRaw> {
    Python::attach(|py| {
        let json = py.import("json")?;
        let asyncio = py.import("asyncio")?;
        let message = json.call_method1("loads", (message_json,))?;
        let coroutine = app.bind(py).call_method1("dispatch", (message,))?;
        let future = asyncio.call_method1("run_coroutine_threadsafe", (coroutine, event_loop.bind(py)))?;
        match future.call_method1("result", (timeout_secs,)) {
            Ok(value) => {
                let dumped: String = json.call_method1("dumps", (value,))?.extract()?;
                Ok(DispatchRaw::Returned(dumped))
            }
            Err(error) => Ok(raw_from_err(py, error)),
        }
    })
}

pub fn invoke_note_dead(
    app: &Py<PyAny>,
    actor: &str,
    kwargs_json: &str,
    reason: &str,
    message_id: &str,
    retries: i64,
    deliveries: i64,
    error: Option<&str>,
) -> PyResult<bool> {
    if !note_dead_after_commit(reason) {
        return Ok(false);
    }
    Python::attach(|py| {
        let json = py.import("json")?;
        let kwargs = json.call_method1("loads", (kwargs_json,))?;
        let keywords = PyDict::new(py);
        match error {
            Some(error) => keywords.set_item("error", error)?,
            None => keywords.set_item("error", py.None())?,
        }
        let coroutine = match app
            .bind(py)
            .call_method("note_dead", (actor, kwargs, reason, message_id, retries, deliveries), Some(&keywords))
        {
            Ok(coroutine) => coroutine,
            Err(error) => {
                eprintln!("note_dead was not called: {error}");
                return Ok(false);
            }
        };
        if coroutine.hasattr("__await__")? {
            match py.import("asyncio")?.call_method1("run", (coroutine,)) {
                Ok(_) => Ok(true),
                Err(error) => {
                    eprintln!("note_dead hook failed: {error}");
                    Ok(true)
                }
            }
        } else {
            Ok(true)
        }
    })
}

pub fn record_outcome(py: Python<'_>, queue: &str, actor: &str, outcome: &arq_core::ParsedOutcome) -> PyResult<()> {
    let metrics = py.import("arq_rs.metrics")?;
    match outcome {
        arq_core::ParsedOutcome::Proposal(arq_core::OutcomeProposal::Retry { .. }) => {
            metrics.call_method1("note_retry", (queue, actor))?;
        }
        arq_core::ParsedOutcome::Proposal(arq_core::OutcomeProposal::Dead { .. }) | arq_core::ParsedOutcome::Invalid(_) => {
            metrics.call_method1("reject", (queue, actor))?;
        }
        arq_core::ParsedOutcome::Proposal(arq_core::OutcomeProposal::Ok | arq_core::OutcomeProposal::Skipped) => {}
    }
    Ok(())
}

pub fn run_child_script(script: &Path) -> i32 {
    let armed = match arm_restart_signals() {
        Ok(armed) => armed,
        Err(error) => {
            eprintln!("restart signals: {error}");
            return 3;
        }
    };
    if let Err(error) = init_child_interpreter(&armed) {
        eprintln!("child interpreter: {error}");
        return 3;
    }
    let executed = Python::attach(|py| -> PyResult<()> {
        add_python_path(py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("run_name", "__main__")?;
        py.import("runpy")?.call_method("run_path", (script.display().to_string(),), Some(&kwargs))?;
        let sys = py.import("sys")?;
        sys.getattr("stdout")?.call_method0("flush")?;
        sys.getattr("stderr")?.call_method0("flush")?;
        Ok(())
    });
    match executed {
        Ok(()) => 0,
        Err(error) => {
            Python::attach(|py| error.print(py));
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_plan_refuses_an_unarmed_process() {
        assert_eq!(startup_plan(false), Err(InitError::SignalsNotArmed));
        assert_eq!(startup_plan(true).unwrap().install_signal_handlers, 0);
        assert!(InitError::Python.to_string().contains("Py_Initialize"));
        assert!(InitError::SignalsNotArmed.to_string().contains("signals"));
    }

    #[test]
    fn the_handler_records_term_and_int_without_installing_python() {
        assert_eq!(unsafe { pyo3::ffi::Py_IsInitialized() }, 0);
        on_signal(libc::SIGTERM);
        assert!(saw_term());
        on_signal(libc::SIGINT);
        assert!(saw_int());
        on_signal(0);
        assert!(python_root().ends_with("python"));
    }

    #[test]
    fn app_paths_and_grace_names() {
        assert_eq!(split_app_path("pkg.mod:create_app").unwrap(), ("pkg.mod", "create_app"));
        assert_eq!(split_app_path("module:app").unwrap(), ("module", "app"));
        assert!(split_app_path("missing").is_err());
        assert!(split_app_path(":app").is_err());
        assert!(split_app_path("module:").is_err());
        assert!(exception_name_is_grace("CancelGraceExceeded"));
        assert!(exception_name_is_grace("arq_rs.dispatch.CancelGraceExceeded"));
        assert!(!exception_name_is_grace("RuntimeError"));
        assert!(matches!(raw_from_exception_name("CancelGraceExceeded"), DispatchRaw::GraceExceeded));
        assert!(matches!(raw_from_exception_name("RuntimeError"), DispatchRaw::Raised));
    }
}
