//! The child process claims a Redis task, runs `dispatch`, and commits the proposal.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use arq_core::{CanonicalUuid, Config, Json, MessageIdentity};
use arq_redis::{EnqueueJob, EnqueueResult, RedisBackend};
use arq_worker::{prepare, run_child, Action};

const URL: &str = "redis://redis:6379/1";

#[test]
fn the_child_commits_a_retry_and_records_the_counter() {
    let dir = std::env::temp_dir().join(format!("arq-worker-deliver-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let queue = format!("q{}", std::process::id());
    let handler = dir.join("handler");
    let metrics = dir.join("metrics");
    fs::write(
        dir.join("appmod.py"),
        format!(
            "\
import os
from pathlib import Path

queue = os.environ['QUEUE_NAME']

class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        Path(os.environ['HANDLER_MARKER']).write_text(message['actor_name'])
        return {{'outcome': 'retry', 'delay_ms': 60000}}

    async def shutdown(self):
        from arq_rs.metrics import value
        retries = value('dramatiq_message_retries_total', queue, 'process_item')
        rejects = value('dramatiq_message_rejects_total', queue, 'process_item')
        Path(os.environ['METRIC_MARKER']).write_text(f'{{int(retries)}} {{int(rejects)}}\\n')

app = App()
"
        ),
    )
    .unwrap();

    let config = Config::parse([
        ("ARQ_POLL_INTERVAL_MS", "50"),
        ("ARQ_MAX_TASKS_PER_CHILD", "1"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "50"),
    ])
    .unwrap();
    let backend = RedisBackend::connect(URL, config).expect("redis url");
    let identity = MessageIdentity::new(CanonicalUuid::from_random_v4(material(1)));
    let task_id = identity.task_id;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let enqueued = runtime.block_on(backend.enqueue(EnqueueJob {
        operation_id: arq_core::OperationId::from_uuid(CanonicalUuid::from_random_v4(material(2))),
        identity,
        cycle_id: arq_core::CycleId::from_uuid(CanonicalUuid::from_random_v4(material(3))),
        actor_name: "process_item".into(),
        queue: queue.clone(),
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
        EnqueueResult::Enqueued { .. } => {}
        EnqueueResult::Duplicate(error) => panic!("duplicate {error:?}"),
    }
    drop(runtime);

    std::env::set_var("PYTHONPATH", &dir);
    std::env::set_var("QUEUE_NAME", &queue);
    std::env::set_var("HANDLER_MARKER", &handler);
    std::env::set_var("METRIC_MARKER", &metrics);
    let plan = match prepare(
        ["--child", "--app", "appmod:app", "--queues", queue.as_str()],
        [
            ("REDIS_URL", URL),
            ("ARQ_POLL_INTERVAL_MS", "50"),
            ("ARQ_MAX_TASKS_PER_CHILD", "1"),
            ("ARQ_LIFECYCLE_INTERVAL_MS", "50"),
        ],
    )
    .unwrap()
    {
        Action::Child(plan) => plan,
        other => panic!("expected child, got {other:?}"),
    };
    let code = run_child(plan);
    assert_eq!(code, 0, "child exit {code}");
    assert_eq!(
        fs::read_to_string(&handler).unwrap_or_default(),
        "process_item"
    );
    assert_eq!(fs::read_to_string(&metrics).unwrap_or_default(), "1 0\n");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let info = runtime
        .block_on(backend.admin(&queue).get(&task_id.to_string()))
        .expect("read task")
        .expect("task");
    assert_eq!(info.state, "ready");
    assert_eq!(info.retries, 1);
    assert_eq!(info.actor_name, "process_item");
    let _ = fs::remove_dir_all(&dir);
}

fn material(tag: u8) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = tag;
    bytes[1..5].copy_from_slice(&std::process::id().to_be_bytes());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    bytes[8..].copy_from_slice(&(nanos as u64).to_be_bytes());
    bytes
}
