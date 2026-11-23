//! Parent process. Stop signals are armed before Python and before slots.
//! [`arq_supervisor::run`] owns the slot machine. The factory is not called here.

use std::time::Duration;

use tokio::signal::unix::{signal, SignalKind};

use arq_supervisor::{hostname, CommandSpec, SupervisorExit, SupervisorSpec};

use crate::cli::ParentPlan;
use crate::linked_crates;
use crate::python_initialized;

pub fn run_parent(plan: ParentPlan) -> i32 {
    if python_initialized() {
        eprintln!("arq-worker: CPython must not be initialized before stop signals");
        return arq_core::CONFIG_EXIT_CODE;
    }
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
    let result = runtime.block_on(supervise(plan));
    match result {
        Ok(exit) => exit.code,
        Err(error) => {
            eprintln!("arq-worker: {error}");
            arq_core::CONFIG_EXIT_CODE
        }
    }
}

async fn supervise(plan: ParentPlan) -> Result<SupervisorExit, String> {
    // Arm SIGTERM and SIGINT before any Python initialization, then run the
    // supervisor. `run` registers its own listeners and drives the slots.
    let _terminate = signal(SignalKind::terminate()).map_err(|error| error.to_string())?;
    let _interrupt = signal(SignalKind::interrupt()).map_err(|error| error.to_string())?;
    if python_initialized() {
        return Err("CPython was initialized before the supervisor".into());
    }
    let _linked = linked_crates();
    let spec = spec_from_plan(&plan);
    arq_supervisor::run(spec)
        .await
        .map_err(|error| error.to_string())
}

pub fn spec_from_plan(plan: &ParentPlan) -> SupervisorSpec {
    let mut spec = SupervisorSpec::new(slot_command(plan));
    spec.config = plan.config;
    spec.url = plan.redis_url.clone();
    spec.queues = plan.queues.clone();
    spec.processes = plan.processes;
    spec.concurrency = plan.concurrency;
    spec.host = hostname();
    spec.pid = std::process::id();
    spec.version = crate::package_version().to_string();
    spec.shutdown = plan.shutdown.clone();
    spec.metrics = CommandSpec::new("/bin/sleep").arg("86400");
    spec.heartbeat_every = Duration::from_secs(plan.config.heartbeat_secs.max(1));
    spec.memory_tick = Duration::from_secs(1);
    spec
}

pub fn slot_command(plan: &ParentPlan) -> CommandSpec {
    let program = std::env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "arq-worker".into());
    let mut command = CommandSpec::new(program)
        .arg("--child")
        .arg("--app")
        .arg(&plan.app)
        .arg("--queues")
        .arg(plan.queues.join(","))
        .arg("--concurrency")
        .arg(plan.concurrency.to_string())
        .env("REDIS_URL", &plan.redis_url);
    if plan.factory {
        command = command.arg("--factory");
    }
    command
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::cli::{prepare, Action};
    use crate::python_initialized;
    use arq_supervisor::Shutdown;

    static LOCK: Mutex<()> = Mutex::new(());

    fn plan_with(url: &str, processes: u32, shutdown: Shutdown) -> ParentPlan {
        match prepare(
            ["--app", "module:create_app", "--factory"],
            [("REDIS_URL", "redis://redis:6379/0")],
        )
        .unwrap()
        {
            Action::Parent(mut plan) => {
                plan.redis_url = url.to_string();
                plan.processes = processes;
                plan.shutdown = shutdown;
                plan
            }
            other => panic!("expected parent, got {other:?}"),
        }
    }

    #[test]
    fn the_child_command_carries_factory_and_the_parent_does_not_start_python() {
        let plan = plan_with("redis://redis:6379/0", 2, Shutdown::new());
        let command = slot_command(&plan);
        assert!(command.program.contains("arq-worker") || !command.program.is_empty());
        assert_eq!(
            command.args,
            vec![
                "--child",
                "--app",
                "module:create_app",
                "--queues",
                "default",
                "--concurrency",
                "1",
                "--factory"
            ]
        );
        assert_eq!(
            command.env.get("REDIS_URL").map(String::as_str),
            Some("redis://redis:6379/0")
        );
        let spec = spec_from_plan(&plan);
        assert_eq!(spec.processes, 2);
        assert_eq!(spec.concurrency, 1);
        assert_eq!(spec.queues, vec!["default".to_string()]);
        assert_eq!(spec.version, crate::package_version());
        assert_eq!(spec.url, "redis://redis:6379/0");
        assert_eq!(spec.metrics.program, "/bin/sleep");
        assert_eq!(spec.slot.args.first().map(String::as_str), Some("--child"));
        let plain = plan_with("redis://redis:6379/0", 1, Shutdown::new());
        let mut without = plain.clone();
        without.factory = false;
        assert!(!slot_command(&without)
            .args
            .iter()
            .any(|arg| arg == "--factory"));
        assert!(!python_initialized());
        let (linked, extension) = crate::linked_crates();
        assert!(linked.contains("PyErr"));
        assert!(!extension);
    }

    #[test]
    fn stop_signals_are_armed_before_python_and_the_supervisor_runs() {
        let _guard = LOCK.lock().unwrap_or_else(|error| error.into_inner());
        assert!(!python_initialized());
        let shutdown = Shutdown::new();
        shutdown.request();
        // An unparsable URL makes `run` skip Redis. The stop flag is already set,
        // so the supervisor returns before it starts slots or Python.
        let code = run_parent(plan_with("not-a-url", 1, shutdown));
        assert_eq!(code, 0);
        assert!(!python_initialized());
        let shutdown = Shutdown::new();
        shutdown.request();
        let code = run_parent(plan_with("not-a-url", 0, shutdown));
        assert_eq!(code, arq_core::CONFIG_EXIT_CODE);
        assert!(!python_initialized());
    }
}
