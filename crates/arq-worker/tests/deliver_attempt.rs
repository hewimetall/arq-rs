//! Production `AttemptLifecycle` through `run_child`.
//!
//! Each case re-executes this binary so Python initializes once and the finish
//! counters start at zero.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arq_core::{CanonicalUuid, Config, Json, MessageIdentity};
use arq_redis::{EnqueueJob, EnqueueResult, RedisBackend};
use arq_worker::{
    fail_next_finish_transports, prepare, production_begin_finish_count,
    production_finish_terminal, run_child, Action,
};

const URL: &str = "redis://redis:6379/1";

#[test]
fn attempt_lifecycle_drives_the_production_loop() {
    if std::env::var_os("ARQ_ATTEMPT_CASE").is_some() {
        run_case();
        return;
    }
    for case in ["confirmed", "lost", "retry", "rejected", "drain"] {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .arg("attempt_lifecycle_drives_the_production_loop")
            .arg("--exact")
            .arg("--test-threads=1")
            .env_remove("ARQRS_PARENT_PIDFD")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, _) in std::env::vars() {
            if key.starts_with("ARQ_") || key.starts_with("SIMPLE_Q_") || key == "REDIS_URL" {
                child.env_remove(&key);
            }
        }
        child.env("ARQ_ATTEMPT_CASE", case);
        let done = child.spawn().unwrap().wait_with_output().unwrap();
        assert!(
            done.status.success(),
            "{case} {:?}\n{}{}",
            done.status,
            String::from_utf8_lossy(&done.stdout),
            String::from_utf8_lossy(&done.stderr)
        );
    }
}

fn run_case() {
    thread::spawn(|| {
        thread::sleep(Duration::from_secs(25));
        std::process::exit(98);
    });
    match std::env::var("ARQ_ATTEMPT_CASE").unwrap().as_str() {
        "confirmed" => confirmed(),
        "lost" => lost_during_handler(),
        "retry" => transient_finish_then_success(),
        "rejected" => finish_rejected(),
        "drain" => drain_keeps_the_task(),
        other => panic!("unknown case {other}"),
    }
}

fn confirmed() {
    let fx = Fixture::new(
        "ok",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('down')
",
    );
    let task = fx.enqueue();
    let code = fx.child(&[("ARQ_MAX_TASKS_PER_CHILD", "1")]);
    assert_eq!(code, 0);
    assert_eq!(production_begin_finish_count(), 1);
    assert_eq!(production_finish_terminal(), Some("confirmed"));
    assert_eq!(fx.task_state(&task), "done");
    assert_eq!(fx.read(fx.stop()), "down");
}

fn lost_during_handler() {
    let fx = Fixture::new(
        "lost",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        from pathlib import Path
        import os
        import time
        Path(os.environ['CASE_MARKER']).write_text('ran')
        time.sleep(0.8)
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('down')
",
    );
    let task = fx.enqueue();
    steal_when(&fx.queue, &task, fx.marker(), Duration::from_millis(0));
    let code = fx.child(&[
        ("ARQ_MAX_TASKS_PER_CHILD", "1"),
        ("ARQ_RENEW_INTERVAL_MS", "50"),
        ("ARQ_RENEW_CONFIRM_MS", "1000"),
        ("ARQ_LEASE_MS", "30000"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 4);
    assert_eq!(production_begin_finish_count(), 0);
    assert_eq!(production_finish_terminal(), None);
    assert_eq!(fx.task_state(&task), "inflight");
}

fn transient_finish_then_success() {
    let fx = Fixture::new(
        "retry",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('down')
",
    );
    let task = fx.enqueue();
    fail_next_finish_transports(1);
    let code = fx.child(&[
        ("ARQ_MAX_TASKS_PER_CHILD", "1"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 0);
    assert_eq!(production_begin_finish_count(), 1);
    assert_eq!(production_finish_terminal(), Some("confirmed"));
    assert_eq!(fx.task_state(&task), "done");
}

fn finish_rejected() {
    let fx = Fixture::new(
        "reject",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        from pathlib import Path
        import os
        import time
        Path(os.environ['CASE_MARKER']).write_text(message['task_id'])
        time.sleep(0.4)
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('down')
",
    );
    let task = fx.enqueue();
    steal_when(&fx.queue, &task, fx.marker(), Duration::from_millis(50));
    let code = fx.child(&[
        ("ARQ_MAX_TASKS_PER_CHILD", "1"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 4);
    assert_eq!(production_begin_finish_count(), 1);
    assert_eq!(production_finish_terminal(), Some("rejected"));
    assert_eq!(fx.task_state(&task), "inflight");
}

fn drain_keeps_the_task() {
    let fx = Fixture::new(
        "drain",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        from pathlib import Path
        import os
        import time
        Path(os.environ['CASE_MARKER']).write_text('ran')
        time.sleep(2)
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('down')
",
    );
    let task = fx.enqueue();
    signal_when(fx.marker(), "USR1");
    let code = fx.child(&[
        ("ARQ_SHUTDOWN_TIMEOUT_MS", "200"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 0);
    assert!(!fx.stop().exists(), "drain does not run shutdown");
    assert_eq!(production_begin_finish_count(), 0);
    assert_eq!(production_finish_terminal(), None);
    assert_eq!(fx.task_state(&task), "inflight");
}

struct Fixture {
    queue: String,
    marker_path: PathBuf,
    stop_path: PathBuf,
}

impl Fixture {
    fn new(name: &str, app: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "arq-attempt-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut source = app.to_string();
        source.push_str("\napp = App()\n");
        fs::write(dir.join("appmod.py"), source).unwrap();
        let queue = format!("q{}{name}", std::process::id());
        let marker_path = dir.join("marker");
        let stop_path = dir.join("stop");
        std::env::set_var("PYTHONPATH", &dir);
        std::env::set_var("CASE_MARKER", &marker_path);
        std::env::set_var("STOP_MARKER", &stop_path);
        Self {
            queue,
            marker_path,
            stop_path,
        }
    }

    fn marker(&self) -> &Path {
        &self.marker_path
    }

    fn stop(&self) -> &Path {
        &self.stop_path
    }

    fn read(&self, path: &Path) -> String {
        fs::read_to_string(path).unwrap_or_default()
    }

    fn enqueue(&self) -> String {
        let config = Config::parse([] as [(&str, &str); 0]).unwrap();
        let backend = RedisBackend::connect(URL, config).unwrap();
        let identity = MessageIdentity::new(CanonicalUuid::from_random_v4(material()));
        let task_id = identity.task_id.to_string();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let enqueued = runtime.block_on(backend.enqueue(EnqueueJob {
            operation_id: arq_core::OperationId::from_uuid(CanonicalUuid::from_random_v4(
                material(),
            )),
            identity,
            cycle_id: arq_core::CycleId::from_uuid(CanonicalUuid::from_random_v4(material())),
            actor_name: "process_item".into(),
            queue: self.queue.clone(),
            kwargs: Json::parse(r#"{"item_id":"1"}"#).unwrap(),
            headers: Json::object(vec![]),
            tag: None,
            unique_key: None,
            delay_ms: 0,
            max_retries: 3,
            max_deliveries: 5,
            time_limit_ms: 5_000,
            min_backoff_ms: 10,
            max_backoff_ms: 20,
        }));
        match enqueued.expect("enqueue") {
            EnqueueResult::Enqueued { .. } => task_id,
            EnqueueResult::Duplicate(error) => panic!("duplicate {error:?}"),
        }
    }

    fn task_state(&self, task_id: &str) -> String {
        let config = Config::parse([] as [(&str, &str); 0]).unwrap();
        let backend = RedisBackend::connect(URL, config).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.admin(&self.queue).get(task_id))
            .expect("admin")
            .expect("task")
            .state
    }

    fn child(&self, env: &[(&str, &str)]) -> i32 {
        let mut pairs = vec![
            ("REDIS_URL", URL),
            ("ARQ_POLL_INTERVAL_MS", "40"),
            ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
        ];
        for (key, value) in env {
            if let Some(found) = pairs.iter_mut().find(|(name, _)| name == key) {
                found.1 = value;
            } else {
                pairs.push((key, value));
            }
        }
        let plan = match prepare(
            ["--child", "--app", "appmod:app", "--queues", self.queue.as_str()],
            pairs,
        )
        .unwrap()
        {
            Action::Child(plan) => plan,
            other => panic!("expected child, got {other:?}"),
        };
        run_child(plan)
    }
}

fn steal_when(queue: &str, task: &str, marker: &Path, pause: Duration) {
    let queue = queue.to_string();
    let task = task.to_string();
    let marker = marker.to_path_buf();
    thread::spawn(move || {
        for _ in 0..400 {
            if marker.exists() {
                if !pause.is_zero() {
                    thread::sleep(pause);
                }
                let status = Command::new("redis-cli")
                    .arg("-u")
                    .arg(URL)
                    .args([
                        "HSET",
                        &format!("q:{{{queue}}}:job:{task}"),
                        "delivery_token",
                        "00000000-0000-4000-8000-000000000099",
                    ])
                    .status();
                assert!(status.expect("redis-cli").success());
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
}

fn signal_when(path: &Path, signal: &str) {
    let path = path.to_path_buf();
    let signal = signal.to_string();
    thread::spawn(move || {
        for _ in 0..400 {
            if path.exists() {
                let _ = Command::new("kill")
                    .arg(format!("-{signal}"))
                    .arg(std::process::id().to_string())
                    .status();
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
}

fn material() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x21;
    bytes[1..5].copy_from_slice(&std::process::id().to_be_bytes());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    bytes[5..8].copy_from_slice(&n.to_be_bytes()[1..]);
    bytes[8..].copy_from_slice(&nanos.to_be_bytes());
    bytes
}
