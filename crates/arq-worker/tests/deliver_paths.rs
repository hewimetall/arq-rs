//! Paths the happy-path delivery test does not take.
//!
//! Each case re-executes this test in its own process. `run_child` initializes
//! CPython once per process, so the cases cannot share an address space.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arq_core::{CanonicalUuid, Config, Json, MessageIdentity};
use arq_redis::{ClaimRequest, ClaimResult, EnqueueJob, EnqueueResult, RedisBackend};
use arq_worker::{prepare, run_child, Action, ChildPlan};

const URL: &str = "redis://redis:6379/1";

#[test]
fn delivery_failure_and_drain_paths() {
    if let Ok(case) = std::env::var("ARQ_DELIVER_CASE") {
        run_case(&case);
        return;
    }
    let exe = std::env::current_exe().expect("test binary");
    let cases = [
        "dead",
        "ping",
        "grace",
        "drain",
        "down",
        "badurl",
        "startup",
        "empty",
        "finish_reject",
        "renew",
        "ok_continue",
        "shutdown_err",
    ];
    let mut pending = Vec::new();
    for case in cases {
        let mut child = Command::new(&exe);
        child
            .arg("delivery_failure_and_drain_paths")
            .arg("--exact")
            .arg("--test-threads=1")
            .env("ARQ_DELIVER_CASE", case)
            .env_remove("ARQRS_PARENT_PIDFD");
        for (key, _) in std::env::vars() {
            if key.starts_with("ARQ_") || key.starts_with("SIMPLE_Q_") || key == "REDIS_URL" {
                child.env_remove(&key);
            }
        }
        child.env("ARQ_DELIVER_CASE", case);
        pending.push((case, child.spawn().expect("re-exec")));
    }
    let mut failed = Vec::new();
    for (case, child) in pending {
        let output = child.wait_with_output().expect("wait");
        if !output.status.success() {
            failed.push(format!(
                "{case} {:?}\n{}{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n---\n"));
}

fn run_case(case: &str) {
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(20));
        std::process::exit(98);
    });
    eprintln!("case {case}");
    match case {
        "dead" => dead_letter(),
        "ping" => ping_budget(),
        "grace" => cancel_grace(),
        "drain" => drain_timeout(),
        "down" => redis_down(),
        "badurl" => bad_url(),
        "startup" => startup_fails(),
        "empty" => empty_queues(),
        "finish_reject" => finish_rejected(),
        "renew" => renew_not_held(),
        "ok_continue" => ok_then_continue(),
        "shutdown_err" => shutdown_error(),
        other => panic!("unknown case {other}"),
    }
}

fn dead_letter() {
    let fx = Fixture::new(
        "dead",
        "\
class App:
    def __init__(self):
        self.client = object()

    def startup(self):
        return None

    async def dispatch(self, message):
        raise AssertionError('dead letter is not dispatched')

    def note_dead(self, actor_name, kwargs, reason, message_id, retries, deliveries, error=None):
        from pathlib import Path
        import os
        Path(os.environ['CASE_MARKER']).write_text(f'{reason} {actor_name} {kwargs}')

    async def shutdown(self):
        from pathlib import Path
        import os
        from arq_rs.metrics import value
        queue = os.environ['QUEUE_NAME']
        rejects = value('dramatiq_message_rejects_total', queue, 'process_item')
        Path(os.environ['STOP_MARKER']).write_text(str(int(rejects)))
",
    );
    let task = fx.enqueue(1, 1, 1_000);
    fx.claim_once(&task);
    fx.redis(&[
        "ZADD",
        &format!("q:{{{}}}:inflight", fx.queue),
        "1",
        &task,
    ]);
    fx.redis(&[
        "HSET",
        &format!("q:{{{}}}:job:{task}", fx.queue),
        "lease_deadline_ms",
        "1",
        "kwargs",
        "not-json",
    ]);
    signal_when(fx.marker(), "TERM");
    let code = fx.child(&[
        ("ARQ_POLL_INTERVAL_MS", "50"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "50"),
    ]);
    assert_eq!(code, 0, "{}", fx.stderr_hint());
    assert_eq!(fx.read(fx.marker()), "max_deliveries process_item {}");
    assert_eq!(fx.read(fx.stop()), "1");
    assert_eq!(fx.task_state(&task), "dead");
}

fn ping_budget() {
    let fx = Fixture::new(
        "ping",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        import os
        import time
        from pathlib import Path
        Path(os.environ['CASE_MARKER']).write_text('ran')
        time.sleep(3)
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('stopped')
",
    );
    let task = fx.enqueue(5, 3, 5_000);
    let code = fx.child(&[
        ("ARQ_PING_BUDGET_SECS", "1"),
        ("ARQ_PING_INTERVAL_MS", "40"),
        ("ARQ_PING_TIMEOUT_MS", "30"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 2, "{}", fx.stderr_hint());
    assert_eq!(fx.read(fx.marker()), "ran");
    assert!(!fx.stop().exists());
    assert_eq!(fx.task_state(&task), "inflight");
}

fn cancel_grace() {
    let fx = Fixture::new(
        "grace",
        "\
class CancelGraceExceeded(Exception):
    pass

class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        raise CancelGraceExceeded()

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('stopped')
",
    );
    let task = fx.enqueue(5, 3, 5_000);
    let code = fx.child(&[
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
        ("ARQ_MAX_TASKS_PER_CHILD", "1"),
    ]);
    assert_eq!(code, 4, "{}", fx.stderr_hint());
    assert_eq!(fx.read(fx.stop()), "stopped");
    assert_eq!(fx.task_state(&task), "inflight");
}

fn drain_timeout() {
    let fx = Fixture::new(
        "drain",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        import os
        import time
        from pathlib import Path
        Path(os.environ['CASE_MARKER']).write_text('held')
        time.sleep(2)
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('stopped')
",
    );
    let task = fx.enqueue(5, 3, 5_000);
    signal_when(fx.marker(), "USR1");
    let code = fx.child(&[
        ("ARQ_SHUTDOWN_TIMEOUT_MS", "200"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 0, "{}", fx.stderr_hint());
    assert_eq!(fx.read(fx.marker()), "held");
    assert!(!fx.stop().exists());
    assert_eq!(fx.task_state(&task), "inflight");
}

fn redis_down() {
    let fx = Fixture::new(
        "down",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        return {'outcome': 'ok'}
",
    );
    let code = fx.child_with_url(
        "redis://127.0.0.1:1/0",
        &[
            ("ARQ_WATCH_RETRIES", "1"),
            ("ARQ_POLL_INTERVAL_MS", "20"),
            ("ARQ_LIFECYCLE_INTERVAL_MS", "20"),
        ],
    );
    assert_eq!(code, 5, "{}", fx.stderr_hint());
}

fn bad_url() {
    let fx = Fixture::new(
        "badurl",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        return {'outcome': 'ok'}
",
    );
    let config = Config::parse([] as [(&str, &str); 0]).unwrap();
    let plan = ChildPlan {
        app: "appmod:app".into(),
        factory: false,
        queues: vec![fx.queue.clone()],
        concurrency: 1,
        redis_url: "not-a-redis-url".into(),
        config,
    };
    assert_eq!(run_child(plan), 3);
}

fn startup_fails() {
    let fx = Fixture::new(
        "startup",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def startup(self):
        from pathlib import Path
        import os
        Path(os.environ['START_MARKER']).write_text('up')
        raise RuntimeError('startup failed')

    async def dispatch(self, message):
        return {'outcome': 'ok'}
",
    );
    let code = fx.child(&[("ARQ_SHUTDOWN_TIMEOUT_MS", "1000")]);
    assert_eq!(code, 3, "{}", fx.stderr_hint());
    assert_eq!(fx.read(fx.start()), "up");
}

fn empty_queues() {
    let fx = Fixture::new(
        "empty",
        "\
class App:
    def __init__(self):
        self.client = object()
        from pathlib import Path
        import os
        Path(os.environ['START_MARKER']).write_text('loaded')

    async def dispatch(self, message):
        return {'outcome': 'ok'}
",
    );
    let config = Config::parse([("ARQ_LIFECYCLE_INTERVAL_MS", "50")]).unwrap();
    std::env::set_var("PYTHONPATH", &fx.dir);
    let plan = ChildPlan {
        app: "appmod:app".into(),
        factory: false,
        queues: Vec::new(),
        concurrency: 1,
        redis_url: URL.into(),
        config,
    };
    assert_eq!(run_child(plan), 3);
    assert_eq!(fx.read(fx.start()), "loaded");
}

fn finish_rejected() {
    let fx = Fixture::new(
        "finishreject",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        import os
        import time
        from pathlib import Path
        Path(os.environ['CASE_MARKER']).write_text(message['task_id'])
        time.sleep(0.4)
        return {'outcome': 'ok'}
",
    );
    let _task = fx.enqueue(5, 3, 5_000);
    steal_token_when(&fx);
    let code = fx.child(&[("ARQ_LIFECYCLE_INTERVAL_MS", "40")]);
    assert_eq!(code, 4, "{}", fx.stderr_hint());
}

fn renew_not_held() {
    let fx = Fixture::new(
        "renew",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        import os
        import time
        from pathlib import Path
        Path(os.environ['CASE_MARKER']).write_text(message['task_id'])
        time.sleep(0.8)
        return {'outcome': 'ok'}
",
    );
    let _task = fx.enqueue(5, 3, 5_000);
    steal_token_when(&fx);
    let code = fx.child(&[
        ("ARQ_RENEW_INTERVAL_MS", "50"),
        ("ARQ_RENEW_CONFIRM_MS", "1000"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
        ("ARQ_LEASE_MS", "30000"),
    ]);
    assert_eq!(code, 4, "{}", fx.stderr_hint());
}

fn ok_then_continue() {
    let fx = Fixture::new(
        "ok",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def startup(self):
        from pathlib import Path
        import os
        Path(os.environ['START_MARKER']).write_text('up')

    async def dispatch(self, message):
        from pathlib import Path
        import os
        Path(os.environ['CASE_MARKER']).write_text(message['actor_name'])
        return {'outcome': 'ok'}

    def shutdown(self):
        from pathlib import Path
        import os
        Path(os.environ['STOP_MARKER']).write_text('down')
",
    );
    let task = fx.enqueue(5, 3, 1_000);
    fx.redis(&[
        "HSET",
        &format!("q:{{{}}}:job:{task}", fx.queue),
        "time_limit_ms",
        "0",
    ]);
    signal_after(fx.marker(), "TERM", Duration::from_millis(400));
    let code = fx.child(&[
        ("ARQ_POLL_INTERVAL_MS", "40"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 0, "{}", fx.stderr_hint());
    assert_eq!(fx.read(fx.start()), "up");
    assert_eq!(fx.read(fx.marker()), "process_item");
    assert_eq!(fx.read(fx.stop()), "down");
    assert_eq!(fx.task_state(&task), "done");
}

fn shutdown_error() {
    let fx = Fixture::new(
        "shutdownerr",
        "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        return {'outcome': 'ok'}

    def shutdown(self):
        raise RuntimeError('shutdown failed')
",
    );
    let task = fx.enqueue(5, 3, 1_000);
    let code = fx.child(&[
        ("ARQ_MAX_TASKS_PER_CHILD", "1"),
        ("ARQ_POLL_INTERVAL_MS", "40"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 0, "{}", fx.stderr_hint());
    assert_eq!(fx.task_state(&task), "done");
}

struct Fixture {
    dir: PathBuf,
    queue: String,
    marker_path: PathBuf,
    stop_path: PathBuf,
    start_path: PathBuf,
}

impl Fixture {
    fn new(name: &str, app: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "arq-deliver-{name}-{}-{}",
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
        let start_path = dir.join("start");
        std::env::set_var("PYTHONPATH", &dir);
        std::env::set_var("QUEUE_NAME", &queue);
        std::env::set_var("CASE_MARKER", &marker_path);
        std::env::set_var("STOP_MARKER", &stop_path);
        std::env::set_var("START_MARKER", &start_path);
        Self { dir, queue, marker_path, stop_path, start_path }
    }

    fn marker(&self) -> &Path {
        &self.marker_path
    }

    fn stop(&self) -> &Path {
        &self.stop_path
    }

    fn start(&self) -> &Path {
        &self.start_path
    }

    fn read(&self, path: &Path) -> String {
        fs::read_to_string(path).unwrap_or_default()
    }

    fn enqueue(&self, max_deliveries: u64, max_retries: u64, time_limit_ms: u64) -> String {
        let config = Config::parse([] as [(&str, &str); 0]).unwrap();
        let backend = RedisBackend::connect(URL, config).unwrap();
        let identity = MessageIdentity::new(CanonicalUuid::from_random_v4(material()));
        let task_id = identity.task_id.to_string();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let enqueued = runtime.block_on(backend.enqueue(EnqueueJob {
            operation_id: arq_core::OperationId::from_uuid(CanonicalUuid::from_random_v4(material())),
            identity,
            cycle_id: arq_core::CycleId::from_uuid(CanonicalUuid::from_random_v4(material())),
            actor_name: "process_item".into(),
            queue: self.queue.clone(),
            kwargs: Json::parse(r#"{"item_id":"1"}"#).unwrap(),
            headers: Json::object(vec![]),
            tag: None,
            unique_key: None,
            delay_ms: 0,
            max_retries,
            max_deliveries,
            time_limit_ms,
            min_backoff_ms: 10,
            max_backoff_ms: 20,
        }));
        match enqueued.expect("enqueue") {
            EnqueueResult::Enqueued { .. } => task_id,
            EnqueueResult::Duplicate(error) => panic!("duplicate {error:?}"),
        }
    }

    fn claim_once(&self, task_id: &str) {
        let _ = task_id;
        let config = Config::parse([] as [(&str, &str); 0]).unwrap();
        let backend = RedisBackend::connect(URL, config).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let claimed = runtime
            .block_on(backend.claim(ClaimRequest {
                operation_id: arq_core::OperationId::from_uuid(CanonicalUuid::from_random_v4(material())),
                message_id: arq_core::MessageId::from_uuid(CanonicalUuid::from_random_v4(material())),
                queue: self.queue.clone(),
                worker_id: format!("probe{}", std::process::id()),
                proposed_token: arq_core::DeliveryToken::from_uuid(CanonicalUuid::from_random_v4(material())),
                lease_ms: 30_000,
            }))
            .expect("claim");
        assert!(matches!(claimed, ClaimResult::Delivered(_)), "{claimed:?}");
    }

    fn task_state(&self, task_id: &str) -> String {
        let config = Config::parse([] as [(&str, &str); 0]).unwrap();
        let backend = RedisBackend::connect(URL, config).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime
            .block_on(backend.admin(&self.queue).get(task_id))
            .expect("admin")
            .expect("task")
            .state
    }

    fn redis(&self, args: &[&str]) {
        let status = Command::new("redis-cli").arg("-u").arg(URL).args(args).status().expect("redis-cli");
        assert!(status.success(), "redis-cli {args:?}");
    }

    fn child(&self, env: &[(&str, &str)]) -> i32 {
        self.child_with_url(URL, env)
    }

    fn child_with_url(&self, url: &str, env: &[(&str, &str)]) -> i32 {
        let mut pairs = vec![("REDIS_URL", url)];
        pairs.extend(env.iter().copied());
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

    fn stderr_hint(&self) -> String {
        String::new()
    }
}

fn signal_when(path: &Path, signal: &str) {
    let path = path.to_path_buf();
    let signal = signal.to_string();
    std::thread::spawn(move || {
        for _ in 0..200 {
            if path.exists() {
                let _ = Command::new("kill")
                    .arg(format!("-{signal}"))
                    .arg(std::process::id().to_string())
                    .status();
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

fn signal_after(path: &Path, signal: &str, pause: Duration) {
    let path = path.to_path_buf();
    let signal = signal.to_string();
    std::thread::spawn(move || {
        for _ in 0..200 {
            if path.exists() {
                std::thread::sleep(pause);
                let _ = Command::new("kill")
                    .arg(format!("-{signal}"))
                    .arg(std::process::id().to_string())
                    .status();
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

fn steal_token_when(fx: &Fixture) {
    let path = fx.marker().to_path_buf();
    let queue = fx.queue.clone();
    std::thread::spawn(move || {
        for _ in 0..200 {
            if let Ok(task) = fs::read_to_string(&path) {
                let task = task.trim();
                if !task.is_empty() {
                    let _ = Command::new("redis-cli")
                        .arg("-u")
                        .arg(URL)
                        .args([
                            "HSET",
                            &format!("q:{{{queue}}}:job:{task}"),
                            "delivery_token",
                            "00000000-0000-4000-8000-000000000099",
                        ])
                        .status();
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

fn material() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x11;
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
