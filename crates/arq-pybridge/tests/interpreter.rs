//! One child interpreter. Parallel tests would race `Py_Initialize`.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

use arq_pybridge::{
    add_python_path, arm_restart_signals, budget_effect, configured_signal_handlers, dispatch_json, init_child_interpreter,
    invoke_note_dead, load_application, observe_ping, parsed_outcome, ping_loop, ping_running_loop, record_outcome,
    register_native, run_child_script, saw_int, saw_term, spawn_event_loop, stop_event_loop, DispatchRaw,
};

const URL: &str = "redis://redis:6379/0";

fn ident() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let tick = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let mixed = tick.wrapping_mul(17).wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed));
    format!("00000000-0000-4000-8000-{:012x}", mixed & 0x0000_ffff_ffff_ffff)
}

fn queue_name() -> String {
    format!("q{:x}", ident().as_bytes().iter().fold(0u64, |acc, byte| acc.wrapping_mul(131).wrapping_add(*byte as u64)) & 0xffff_ffff)
}

fn message(item_id: &str) -> String {
    format!(
        r#"{{"actor_name":"process_item","queue":"default","kwargs":{{"item_id":"{item_id}"}},"headers":{{}},"message_id":"00000000-0000-4000-8000-0000000000aa","task_id":"00000000-0000-4000-8000-0000000000aa","retries":0,"deliveries":1,"time_limit_ms":40,"cancel_grace_ms":40}}"#
    )
}

fn spec(queue: &str, message: &str, operation: &str, cycle: &str) -> String {
    format!(
        r#"{{"queue":"{queue}","actor_name":"process_item","kwargs":{{"item_id":"1"}},"headers":{{"source":"bridge"}},"unique_key":null,"tag":"batch-a","delay_ms":0,"message_id":"{message}","operation_id":"{operation}","cycle_id":"{cycle}","max_retries":1,"max_deliveries":5,"time_limit_ms":1000,"min_backoff_ms":10,"max_backoff_ms":20}}"#
    )
}

fn child_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/tests/child_main.py")
}

fn run_probe(py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyModule>)> {
    add_python_path(py)?;
    let module = PyModule::new(py, "arq_rs._native")?;
    register_native(&module)?;
    module.setattr("injected", true)?;
    py.import("sys")?.getattr("modules")?.set_item("arq_rs._native", &module)?;
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/probe_child.py");
    let code = std::fs::read_to_string(&path).expect("probe script");
    let code = std::ffi::CString::new(code).expect("probe script has no interior null");
    let locals = PyDict::new(py);
    py.run(code.as_c_str(), Some(&locals), Some(&locals))?;
    let app = locals.get_item("APP")?.expect("APP").unbind();
    let sync_note = locals.get_item("SYNC")?.expect("SYNC").unbind();
    let bare = locals.get_item("BARE")?.expect("BARE").unbind();
    let boom = locals.get_item("BOOM")?.expect("BOOM").unbind();
    Ok((app, sync_note, bare, boom, module.unbind()))
}

fn text_of(value: &Bound<'_, PyAny>) -> String {
    value.extract::<String>().unwrap_or_else(|_| value.str().map(|text| text.to_string()).unwrap_or_default())
}

#[test]
fn a_real_child_interpreter_dispatches_pings_and_talks_to_redis() {
    let bin = env!("CARGO_BIN_EXE_child_probe");
    let missing = Command::new(bin).output().expect("child_probe");
    assert_eq!(missing.status.code(), Some(2), "{}", String::from_utf8_lossy(&missing.stderr));

    let script = child_script();
    let child = Command::new(bin)
        .arg(&script)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("child_probe script");
    assert_eq!(
        child.status.code(),
        Some(0),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(stdout.contains("enter-a"), "{stdout}");
    assert!(stdout.contains("handler-1"), "{stdout}");
    assert!(stdout.contains("dead-a:retries"), "{stdout}");

    assert_eq!(run_child_script(std::path::Path::new("/tmp/arq-no-such-child-script.py")), 1);
    assert_eq!(configured_signal_handlers(), 0);
    assert_eq!(run_child_script(&script), 0);
    {
        let armed = arm_restart_signals().expect("signals");
        unsafe {
            libc::raise(libc::SIGTERM);
            libc::raise(libc::SIGINT);
        }
        assert!(saw_term());
        assert!(saw_int());
        let again = init_child_interpreter(&armed).expect("second init");
        assert_eq!(again.install_signal_handlers, 0);
    }

    let started = Python::attach(run_probe).expect("probe");
    let (app, sync_note, bare, boom, native) = started;
    let running = spawn_event_loop().expect("event loop");
    let event_loop = &running.event_loop;

    Python::attach(|py| {
        let asyncio = py.import("asyncio").unwrap();
        let sleeper = asyncio.call_method1("sleep", (0.2,)).unwrap();
        asyncio.call_method1("run_coroutine_threadsafe", (sleeper, event_loop.bind(py))).unwrap();
    });
    assert!(ping_running_loop(event_loop, Duration::from_millis(500)), "asyncio.sleep must not exhaust the ping");

    let ok = dispatch_json(event_loop, &app, &message("ok"), 3.0).expect("dispatch ok");
    assert!(matches!(ok, DispatchRaw::Returned(ref text) if text.contains("\"ok\"")), "{ok:?}");
    let failed = dispatch_json(event_loop, &app, &message("boom"), 3.0).expect("dispatch boom");
    assert!(matches!(failed, DispatchRaw::Returned(ref text) if text.contains("retries")), "{failed:?}");
    let escaped = dispatch_json(event_loop, &app, &message("escape"), 3.0).expect("dispatch escape");
    assert!(matches!(escaped, DispatchRaw::Raised), "{escaped:?}");
    let grace = dispatch_json(event_loop, &app, &message("grace"), 3.0).expect("dispatch grace");
    assert!(matches!(grace, DispatchRaw::GraceExceeded), "{grace:?}");

    Python::attach(|py| {
        py.run(
            c"def hold():\n    import time\n    end = time.monotonic() + 0.8\n    while time.monotonic() < end:\n        pass\n",
            None,
            None,
        )
        .unwrap();
        let hold = py.eval(c"hold", None, None).unwrap();
        event_loop.bind(py).call_method1("call_soon_threadsafe", (hold,)).unwrap();
    });
    std::thread::sleep(Duration::from_millis(40));
    let mut watch = arq_core::PingWatch::new(Duration::from_millis(30));
    let started = Instant::now();
    assert!(!ping_running_loop(event_loop, Duration::from_millis(40)), "a loop holding the GIL must miss the ping");
    let early = observe_ping(&mut watch, false, started);
    assert!(budget_effect(early).is_none(), "one miss keeps renewing");
    assert!(!ping_running_loop(event_loop, Duration::from_millis(40)), "the loop is still stuck");
    let expired = observe_ping(&mut watch, false, started + Duration::from_millis(30));
    let effect = budget_effect(expired).expect("silence across the budget");
    assert_eq!(effect.exit_code, 2);
    assert!(!effect.on_stop && !effect.finish && !effect.flush_tracing);
    assert!(effect.stop_renew && effect.request_cancel);
    std::thread::sleep(Duration::from_millis(800));

    let dead_object = Python::attach(|py| py.None());
    assert!(!ping_loop(dead_object, Duration::from_millis(200)));

    stop_event_loop(event_loop).expect("stop");
    running.handle.join().expect("loop thread");

    let message_id = ident();
    assert!(!invoke_note_dead(&app, "process_item", "{}", "retries", &message_id, 0, 1, None).unwrap());
    assert!(invoke_note_dead(&app, "process_item", "{\"item_id\":\"1\"}", "max_deliveries", &message_id, 1, 5, Some("full")).unwrap());
    assert!(invoke_note_dead(&app, "process_item", "{\"item_id\":\"1\"}", "invalid_message", &message_id, 0, 1, None).unwrap());
    assert!(invoke_note_dead(&sync_note, "process_item", "{}", "max_deliveries", &message_id, 0, 1, None).unwrap());
    assert!(invoke_note_dead(&boom, "process_item", "{}", "max_deliveries", &message_id, 0, 1, Some("x")).unwrap());
    assert!(!invoke_note_dead(&bare, "process_item", "{}", "invalid_message", &message_id, 0, 1, None).unwrap());
    Python::attach(|py| {
        py.run(
            c"import asyncio\ndef broken():\n    raise RuntimeError('no loop')\nasyncio.new_event_loop = broken\n",
            None,
            None,
        )
        .unwrap();
    });
    assert!(spawn_event_loop().is_err());

    Python::attach(|py| {
        for body in [
            r#"{"outcome":"ok"}"#,
            r#"{"outcome":"skipped"}"#,
            r#"{"outcome":"retry","delay_ms":1}"#,
            r#"{"outcome":"dead","reason":"retries","error":null}"#,
            "not-json",
        ] {
            record_outcome(py, "default", "process_item", &parsed_outcome(body)).unwrap();
        }
        let loaded = load_application(py, "probeapp:app", false).unwrap();
        assert!(loaded.getattr("dispatch").is_ok());
        let created = load_application(py, "probeapp:create_app", true).unwrap();
        assert!(created.getattr("client").is_ok());
        assert!(load_application(py, "probeapp:broken_factory", true).is_err());
        assert!(load_application(py, "probeapp:not_app", false).is_err());
        assert!(load_application(py, "missing", false).is_err());

        let native = native.bind(py);
        let described = text_of(&native.getattr("parse_config").unwrap().call1(("",)).unwrap());
        assert!(described.contains("poll_interval_ms"), "{described}");
        let rejected = text_of(&native.getattr("parse_config").unwrap().call1((r#"{"SIMPLE_Q_LEASE_MS":"1"}"#,)).unwrap());
        assert!(rejected.contains("error"), "{rejected}");

        let bad_config = native.getattr("Backend").unwrap().call1((URL, r#"{"SIMPLE_Q_LEASE_MS":"1"}"#));
        assert!(bad_config.unwrap_err().is_instance_of::<pyo3::exceptions::PyValueError>(py));
        let closed = native.getattr("Backend").unwrap().call1(("redis://127.0.0.1:1", "{}"));
        assert!(closed.unwrap_err().is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));

        let backend = native.getattr("Backend").unwrap().call1((URL, "{}")).unwrap();
        let connects: u64 = native.getattr("redis_connects").unwrap().call0().unwrap().extract().unwrap();
        assert!(connects >= 1);

        let queue = queue_name();
        let task = ident();
        let operation = ident();
        let cycle = ident();
        let created = text_of(&backend.call_method1("submit", (spec(&queue, &task, &operation, &cycle),)).unwrap());
        assert!(created.contains("enqueued"), "{created}");
        let fetched = text_of(&backend.call_method1("get", (&queue, &task)).unwrap());
        assert!(fetched.contains("process_item"), "{fetched}");
        assert!(text_of(&backend.call_method1("get", (&queue, ident())).unwrap()).contains("null"));
        assert!(text_of(&backend.call_method1("peek", (&queue, "ready", 1, "")).unwrap()).contains("tasks"));
        assert!(text_of(&backend.call_method1("peek", (&queue, "ready", 1, "nope")).unwrap()).contains("page"));
        assert!(text_of(&backend.call_method1("dead", (&queue, 1, "")).unwrap()).contains("tasks"));
        assert!(text_of(&backend.call_method1("dead_for_tag", (&queue, "batch-a", 1, "")).unwrap()).contains("tasks"));
        assert!(text_of(&backend.call_method1("snapshot", (&queue, "")).unwrap()).contains("queues"));
        assert!(text_of(&backend.call_method1("snapshot", (&queue, "redis")).unwrap()).contains("taken_at"));
        let cancel = ident();
        let cancelled = text_of(&backend.call_method1("control", ("cancel", &queue, &task, &cancel)).unwrap());
        assert!(cancelled.contains("accepted"), "{cancelled}");
        assert!(text_of(&backend.call_method1("command", (&queue, &cancel)).unwrap()).contains("accepted"));
        let killed = text_of(&backend.call_method1("control", ("kill", &queue, &task, ident())).unwrap());
        assert!(killed.contains("already_terminal"), "{killed}");
        let settled = text_of(&backend.call_method1("control", ("settle", &queue, &task, ident())).unwrap());
        assert!(settled.contains("already_terminal") || settled.contains("not_blocked"), "{settled}");
        let requeued = text_of(&backend.call_method1("control", ("requeue", &queue, &task, ident())).unwrap());
        assert!(
            requeued.contains("requeued") || requeued.contains("not_dead") || requeued.contains("duplicate"),
            "{requeued}"
        );
        assert!(text_of(&backend.call_method1("control", ("nope", &queue, &task, ident())).unwrap()).contains("input"));
        let token = ident();
        let finish_op = ident();
        assert!(text_of(&backend.call_method1("commit", (&queue, &task, &task, &cycle, &token, &finish_op, "grace", "")).unwrap()).contains("abandoned"));
        let raised = text_of(&backend.call_method1("commit", (&queue, &task, &task, &cycle, &token, ident(), "raised", "")).unwrap());
        assert!(raised.contains("error") || raised.contains("finished") || raised.contains("rejected"), "{raised}");
        let returned = text_of(
            &backend.call_method1("commit", (&queue, &task, &task, &cycle, &token, ident(), "returned", r#"{"outcome":"ok"}"#)).unwrap(),
        );
        assert!(returned.contains("error") || returned.contains("finished") || returned.contains("rejected"), "{returned}");
        let unknown = text_of(
            &backend.call_method1("commit", (&queue, &task, &task, &cycle, &token, ident(), "other", r#"{"outcome":"maybe"}"#)).unwrap(),
        );
        assert!(unknown.contains("error") || unknown.contains("finished") || unknown.contains("rejected"), "{unknown}");
    });
}
