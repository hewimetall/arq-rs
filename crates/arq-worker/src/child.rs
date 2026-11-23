//! Child process. Restart signals and the parent pidfd are armed before Python.
//! `--factory` runs in this process and must return an App.

use pyo3::prelude::*;

use arq_pybridge::{add_python_path, arm_restart_signals, init_child_interpreter, load_application};
use arq_supervisor::prepare_child_runtime;

use crate::cli::ChildPlan;
use crate::python_initialized;

pub fn run_child(plan: ChildPlan) -> i32 {
    if python_initialized() {
        eprintln!("arq-worker: restart signals must be armed before Python");
        return arq_core::CONFIG_EXIT_CODE;
    }
    let armed = match arm_restart_signals() {
        Ok(armed) => armed,
        Err(error) => {
            eprintln!("arq-worker: restart signals: {error}");
            return arq_core::CONFIG_EXIT_CODE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("arq-worker: runtime: {error}");
            return arq_core::CONFIG_EXIT_CODE;
        }
    };
    runtime.block_on(child_session(plan, armed))
}

async fn child_session(plan: ChildPlan, armed: arq_pybridge::ArmedSignals) -> i32 {
    let mut failure: Option<String> = None;
    let mut loaded_app: Option<Py<PyAny>> = None;
    let prepared = prepare_child_runtime(|watch| {
        if !watch.ready_before_python() {
            failure = Some("parent watch was not armed before Python".into());
            return;
        }
        if let Err(error) = init_child_interpreter(&armed) {
            failure = Some(error.to_string());
            return;
        }
        match Python::attach(|py| load_child_app(py, &plan)) {
            Ok(app) => loaded_app = Some(app),
            Err(error) => failure = Some(error),
        }
    })
    .await;
    let watch = match prepared {
        Ok(watch) => watch,
        Err(error) => {
            eprintln!("arq-worker: {error}");
            return arq_core::CONFIG_EXIT_CODE;
        }
    };
    if let Some(error) = failure {
        eprintln!("arq-worker: {error}");
        return arq_core::CONFIG_EXIT_CODE;
    }
    let Some(app) = loaded_app else {
        eprintln!("arq-worker: application was not loaded");
        return arq_core::CONFIG_EXIT_CODE;
    };
    crate::delivery::run(&plan, watch, app).await
}

pub fn child_should_stop(term: bool, interrupt: bool, parent_exited: bool) -> bool {
    term || interrupt || parent_exited
}

fn load_child_app(py: Python<'_>, plan: &ChildPlan) -> Result<Py<PyAny>, String> {
    add_python_path(py).map_err(|error| error.to_string())?;
    let app = load_application(py, &plan.app, plan.factory).map_err(|error| error.to_string())?;
    if app.getattr("dispatch").is_err() {
        return Err(format!("{} must be an App", plan.app));
    }
    Ok(app.unbind())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_when_signalled_or_the_parent_exits() {
        assert!(!child_should_stop(false, false, false));
        assert!(child_should_stop(true, false, false));
        assert!(child_should_stop(false, true, false));
        assert!(child_should_stop(false, false, true));
        assert!(child_should_stop(true, true, true));
    }
}
