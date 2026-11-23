//! Parse the JSON documents the Python package sends across the bridge.
//!
//! No interpreter and no Redis. Invalid documents become [`BridgeError`] before a socket opens.

use arq_core::{
    CanonicalUuid, Config, ConfigError, CycleId, Json, MessageIdentity, OperationId, validate_opaque, validate_queue,
};
use arq_redis::{BackendError, EnqueueJob};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeError {
    Input(String),
    Config(String),
    Transport(String),
    Page(String),
    Full { queue: String, max_ready: u64, ready_zcard: u64 },
    Conflict { operation_id: String, detail: String },
    Inconsistent { queue: String, detail: String },
    Halted { queue: String },
    Indeterminate { operation_id: String, message_id: String },
    Closed,
}

impl BridgeError {
    pub fn input(detail: impl Into<String>) -> Self {
        Self::Input(detail.into())
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Input(_) => "input",
            Self::Config(_) => "config",
            Self::Transport(_) => "transport",
            Self::Page(_) => "page",
            Self::Full { .. } => "full",
            Self::Conflict { .. } => "conflict",
            Self::Inconsistent { .. } => "inconsistent",
            Self::Halted { .. } => "halted",
            Self::Indeterminate { .. } => "indeterminate",
            Self::Closed => "closed",
        }
    }
}

impl From<BackendError> for BridgeError {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::Input(error) => Self::Input(error.to_string()),
            BackendError::Full(error) => Self::Full {
                queue: error.queue,
                max_ready: error.max_ready,
                ready_zcard: error.ready_zcard,
            },
            BackendError::Conflict(error) => Self::Conflict {
                operation_id: error.operation_id.to_string(),
                detail: error.to_string(),
            },
            BackendError::Inconsistent(error) => Self::Inconsistent { queue: error.queue, detail: error.detail },
            BackendError::Indeterminate(error) => Self::Indeterminate {
                operation_id: error.operation_id.to_string(),
                message_id: error.message_id.to_string(),
            },
            BackendError::Transaction(error) => Self::Transport(error.to_string()),
            BackendError::Halted { queue } => Self::Halted { queue },
            BackendError::Transport { detail } => Self::Transport(detail),
            BackendError::Page { detail } => Self::Page(detail),
        }
    }
}

pub fn config_from_json(text: &str) -> Result<Config, BridgeError> {
    let text = if text.trim().is_empty() { "{}" } else { text };
    let value = Json::parse(text).map_err(|_| BridgeError::Config("config must be a JSON object of strings".into()))?;
    let Json::Object(pairs) = value else {
        return Err(BridgeError::Config("config must be a JSON object of strings".into()));
    };
    let mut owned = Vec::with_capacity(pairs.len());
    for (key, value) in &pairs {
        let Json::String(raw) = value else {
            return Err(BridgeError::Config(format!("{key} must be a string")));
        };
        owned.push((key.clone(), raw.clone()));
    }
    Config::parse(owned.iter().map(|(key, value)| (key.as_str(), value.as_str())))
        .map_err(|error: ConfigError| BridgeError::Config(error.to_string()))
}

pub fn config_fields(config: &Config) -> Json {
    Json::object(vec![
        ("poll_interval_ms", num(config.poll_interval_ms)),
        ("inflight_scan_interval_ms", num(config.inflight_scan_interval_ms)),
        ("lease_ms", num(config.lease_ms)),
        ("renew_interval_ms", num(config.renew_interval_ms)),
        ("renew_confirm_ms", num(config.renew_confirm_ms)),
        ("ping_interval_ms", num(config.ping_interval_ms)),
        ("ping_timeout_ms", num(config.ping_timeout_ms)),
        ("lifecycle_interval_ms", num(config.lifecycle_interval_ms)),
        ("ping_budget_secs", num(config.ping_budget_secs)),
        ("ping_budget_explicit", Json::Bool(config.ping_budget_explicit)),
        ("shutdown_timeout_ms", num(config.shutdown_timeout_ms)),
        ("cancel_grace_ms", num(config.cancel_grace_ms)),
        ("max_deliveries", num(config.max_deliveries)),
        ("max_tasks_per_child", num(config.max_tasks_per_child)),
        ("memory_soft_ratio", Json::Number(format!("{}", config.memory_soft_ratio))),
        ("memory_hard_ratio", Json::Number(format!("{}", config.memory_hard_ratio))),
        ("child_memory_soft", opt_num(config.child_memory_soft)),
        ("child_memory_hard", opt_num(config.child_memory_hard)),
        ("heartbeat_secs", num(config.heartbeat_secs)),
        ("missed_heartbeats", num(config.missed_heartbeats)),
        ("maintenance_interval_ms", num(config.maintenance_interval_ms)),
        ("maintenance_batch", num(config.maintenance_batch)),
        ("watch_retries", num(config.watch_retries)),
        ("done_ttl_secs", num(config.done_ttl_secs)),
        ("dead_ttl_secs", num(config.dead_ttl_secs)),
        ("receipt_ttl_secs", num(config.receipt_ttl_secs)),
        ("max_ready", num(config.max_ready)),
        ("max_message_bytes", num(config.max_message_bytes)),
        ("max_headers_bytes", num(config.max_headers_bytes)),
        ("max_error_bytes", num(config.max_error_bytes)),
    ])
}

pub fn enqueue_job(text: &str) -> Result<EnqueueJob, BridgeError> {
    let pairs = object(text, "spec")?;
    let queue = req_str(&pairs, "queue")?;
    validate_queue(queue).map_err(|_| BridgeError::input("queue name must match [a-z0-9]+"))?;
    let actor_name = req_str(&pairs, "actor_name")?;
    let kwargs = match lookup(&pairs, "kwargs") {
        Some(Json::Object(items)) => Json::Object(items.clone()),
        _ => return Err(BridgeError::input("kwargs must be a JSON object")),
    };
    let headers = headers_of(lookup(&pairs, "headers"))?;
    let unique_key = opt_opaque(&pairs, "unique_key")?;
    let tag = opt_opaque(&pairs, "tag")?;
    let delay_ms = opt_u64(&pairs, "delay_ms", 0)?;
    let message = CanonicalUuid::parse(req_str(&pairs, "message_id")?)
        .map_err(|_| BridgeError::input("message_id must be a canonical lowercase UUID"))?;
    let operation_id = OperationId::parse(req_str(&pairs, "operation_id")?)
        .map_err(|_| BridgeError::input("operation_id must be a canonical lowercase UUID"))?;
    let cycle_id = CycleId::parse(req_str(&pairs, "cycle_id")?)
        .map_err(|_| BridgeError::input("cycle_id must be a canonical lowercase UUID"))?;
    let max_retries = req_u64(&pairs, "max_retries")?;
    let max_deliveries = req_u64(&pairs, "max_deliveries")?;
    if max_deliveries < 1 {
        return Err(BridgeError::input("max_deliveries must be >= 1"));
    }
    let time_limit_ms = req_u64(&pairs, "time_limit_ms")?;
    if time_limit_ms == 0 {
        return Err(BridgeError::input("time_limit_ms must be > 0"));
    }
    let min_backoff_ms = req_u64(&pairs, "min_backoff_ms")?;
    let max_backoff_ms = req_u64(&pairs, "max_backoff_ms")?;
    if min_backoff_ms == 0 || min_backoff_ms > max_backoff_ms {
        return Err(BridgeError::input("backoff requires 0 < min_backoff_ms <= max_backoff_ms"));
    }
    Ok(EnqueueJob {
        operation_id,
        identity: MessageIdentity::new(message),
        cycle_id,
        actor_name: actor_name.to_string(),
        queue: queue.to_string(),
        kwargs,
        headers,
        tag,
        unique_key,
        delay_ms,
        max_retries,
        max_deliveries,
        time_limit_ms,
        min_backoff_ms,
        max_backoff_ms,
    })
}

fn object(text: &str, name: &str) -> Result<Vec<(String, Json)>, BridgeError> {
    let value = Json::parse(text).map_err(|_| BridgeError::input(format!("{name} must be a JSON object")))?;
    match value {
        Json::Object(pairs) => Ok(pairs),
        _ => Err(BridgeError::input(format!("{name} must be a JSON object"))),
    }
}

fn lookup<'a>(pairs: &'a [(String, Json)], key: &str) -> Option<&'a Json> {
    pairs.iter().find(|(name, _)| name == key).map(|(_, value)| value)
}

fn req_str<'a>(pairs: &'a [(String, Json)], key: &str) -> Result<&'a str, BridgeError> {
    match lookup(pairs, key) {
        Some(Json::String(text)) if !text.is_empty() => Ok(text),
        _ => Err(BridgeError::input(format!("{key} must be a non-empty string"))),
    }
}

fn req_u64(pairs: &[(String, Json)], key: &str) -> Result<u64, BridgeError> {
    match lookup(pairs, key).and_then(Json::as_u64) {
        Some(value) => Ok(value),
        None => Err(BridgeError::input(format!("{key} must be a non-negative integer"))),
    }
}

fn opt_u64(pairs: &[(String, Json)], key: &str, default: u64) -> Result<u64, BridgeError> {
    match lookup(pairs, key) {
        None | Some(Json::Null) => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| BridgeError::input(format!("{key} must be a non-negative integer"))),
    }
}

fn opt_opaque(pairs: &[(String, Json)], key: &'static str) -> Result<Option<String>, BridgeError> {
    match lookup(pairs, key) {
        None | Some(Json::Null) => Ok(None),
        Some(Json::String(text)) => {
            validate_opaque(key, text).map_err(|_| BridgeError::input(format!("{key} is not an opaque string")))?;
            Ok(Some(text.clone()))
        }
        Some(_) => Err(BridgeError::input(format!("{key} must be a string or null"))),
    }
}

fn headers_of(value: Option<&Json>) -> Result<Json, BridgeError> {
    let Some(value) = value else {
        return Ok(Json::object(vec![]));
    };
    if matches!(value, Json::Null) {
        return Ok(Json::object(vec![]));
    }
    let Json::Object(pairs) = value else {
        return Err(BridgeError::input("headers must be a JSON object of strings"));
    };
    let mut owned = Vec::with_capacity(pairs.len());
    for (key, item) in pairs {
        let Json::String(text) = item else {
            return Err(BridgeError::input("headers must be a JSON object of strings"));
        };
        owned.push((key.clone(), Json::String(text.clone())));
    }
    Ok(Json::Object(owned))
}

fn num(value: u64) -> Json {
    Json::Number(value.to_string())
}

fn opt_num(value: Option<u64>) -> Json {
    match value {
        Some(value) => num(value),
        None => Json::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::{
        DuplicateTask, IndeterminateOperation, InputError, MessageId, OperationConflict, QueueFull, StorageInconsistency,
        TaskId, TransactionConflict,
    };

    fn uuid(n: u64) -> String {
        format!("00000000-0000-4000-8000-{n:012x}")
    }

    fn spec(extra: &str) -> String {
        format!(
            r#"{{"queue":"default","actor_name":"process_item","kwargs":{{"item_id":"1"}},"headers":{{}},"message_id":"{}","operation_id":"{}","cycle_id":"{}","max_retries":0,"max_deliveries":5,"time_limit_ms":1800000,"min_backoff_ms":15000,"max_backoff_ms":604800000{extra}}}"#,
            uuid(1),
            uuid(2),
            uuid(3)
        )
    }

    #[test]
    fn default_config_and_override_round_trip() {
        let config = config_from_json("").unwrap();
        assert_eq!(config.poll_interval_ms, 500);
        assert!(!config.ping_budget_explicit);
        assert!(config.child_memory_soft.is_none());
        let fields = config_fields(&config);
        assert_eq!(fields.get("poll_interval_ms").and_then(Json::as_u64), Some(500));
        assert_eq!(fields.get("child_memory_soft"), Some(&Json::Null));
        assert_eq!(fields.get("ping_budget_explicit"), Some(&Json::Bool(false)));

        let custom = config_from_json(r#"{"ARQ_LEASE_MS":"20000","ARQ_RENEW_INTERVAL_MS":"1000","ARQ_PING_BUDGET_SECS":"9","ARQ_CHILD_MEMORY_SOFT":"10","ARQ_CHILD_MEMORY_HARD":"20"}"#).unwrap();
        assert_eq!(custom.lease_ms, 20_000);
        assert!(custom.ping_budget_explicit);
        assert_eq!(custom.child_memory_soft, Some(10));
        let rendered = config_fields(&custom);
        assert_eq!(rendered.get("child_memory_hard").and_then(Json::as_u64), Some(20));
        assert_eq!(rendered.get("memory_soft_ratio").and_then(Json::as_str), None);
        assert!(matches!(rendered.get("memory_soft_ratio"), Some(Json::Number(_))));
    }

    #[test]
    fn config_rejects_shape_and_simple_q() {
        assert_eq!(config_from_json("[]").unwrap_err().kind(), "config");
        assert_eq!(config_from_json(r#"{"ARQ_LEASE_MS":1}"#).unwrap_err().kind(), "config");
        match config_from_json(r#"{"SIMPLE_Q_LEASE_MS":"1"}"#) {
            Err(BridgeError::Config(text)) => assert!(text.contains("SIMPLE_Q_"), "{text}"),
            other => panic!("{other:?}"),
        }
        assert!(config_from_json("{").is_err());
        assert!(config_from_json(r#"{"ARQ_LEASE_MS":"0"}"#).is_err());
    }

    #[test]
    fn enqueue_spec_accepts_transport_fields() {
        let text = spec(r#","unique_key":"item:1","tag":"batch-a","delay_ms":5"#)
            .replace(r#""headers":{}"#, r#""headers":{"traceparent":"00"}"#);
        let job = enqueue_job(&text).unwrap();
        assert_eq!(job.queue, "default");
        assert_eq!(job.actor_name, "process_item");
        assert_eq!(job.unique_key.as_deref(), Some("item:1"));
        assert_eq!(job.tag.as_deref(), Some("batch-a"));
        assert_eq!(job.delay_ms, 5);
        assert_eq!(job.identity.message_id.to_string(), job.identity.task_id.to_string());
        assert!(job.headers.get("traceparent").is_some());

        let bare = spec("");
        let bare = enqueue_job(&bare).unwrap();
        assert!(bare.unique_key.is_none());
        assert_eq!(bare.delay_ms, 0);
        let nulls = spec(r#","unique_key":null,"tag":null,"delay_ms":null"#)
            .replace(r#""headers":{}"#, r#""headers":null"#);
        let nulls = enqueue_job(&nulls).unwrap();
        assert!(nulls.tag.is_none());
        assert!(matches!(nulls.headers, Json::Object(pairs) if pairs.is_empty()));
    }

    #[test]
    fn enqueue_spec_rejects_each_bad_field() {
        let cases = [
            ("[]", "object"),
            ("{", "object"),
            (r#"{"queue":"Bad"}"#, "queue"),
            (&spec("").replace("process_item", ""), "actor"),
            (&spec("").replace(r#""kwargs":{"item_id":"1"}"#, r#""kwargs":[]"#), "kwargs"),
            (&spec(r#","headers":{"n":1}"#), "headers"),
            (&spec(r#","headers":["a"]"#), "headers"),
            (&spec(r#","unique_key":""#), "unique"),
            (&spec(r#","unique_key":1"#), "unique"),
            (&spec(r#","tag":"a b""#), "tag"),
            (&spec(r#","delay_ms":-1"#), "delay"),
            (&spec("").replace(&uuid(1), "NOPE"), "message"),
            (&spec("").replace(&uuid(2), "NOPE"), "operation"),
            (&spec("").replace(&uuid(3), "NOPE"), "cycle"),
            (&spec("").replace(r#""max_retries":0"#, r#""max_retries":-1"#), "retries"),
            (&spec("").replace(r#""max_deliveries":5"#, r#""max_deliveries":0"#), "deliveries"),
            (&spec("").replace(r#""time_limit_ms":1800000"#, r#""time_limit_ms":0"#), "limit"),
            (&spec("").replace(r#""min_backoff_ms":15000"#, r#""min_backoff_ms":0"#), "backoff"),
            (&spec("").replace(r#""min_backoff_ms":15000"#, r#""min_backoff_ms":99"#).replace(r#""max_backoff_ms":604800000"#, r#""max_backoff_ms":1"#), "backoff"),
        ];
        for (text, label) in cases {
            let error = enqueue_job(text).expect_err(label);
            assert_eq!(error.kind(), "input", "{label}: {error:?}");
        }
        let omitted = spec("").replace(",\"headers\":{}", "");
        assert!(enqueue_job(&omitted).is_ok(), "{omitted}");
        assert!(enqueue_job(&spec("").replace("\"headers\":{}", "\"headers\":[]")).is_err());
        assert!(enqueue_job(&spec("").replace("\"headers\":{}", "\"headers\":{\"n\":1}")).is_err());
    }

    #[test]
    fn backend_errors_keep_their_kind() {
        let id = OperationId::parse(&uuid(4)).unwrap();
        let message = MessageId::parse(&uuid(5)).unwrap();
        let task = TaskId::parse(&uuid(6)).unwrap();
        let errors = vec![
            BackendError::Input(InputError::Queue),
            BackendError::Full(QueueFull { queue: "default".into(), max_ready: 1, ready_zcard: 1 }),
            BackendError::Conflict(OperationConflict {
                operation_id: id,
                existing_op: "enqueue".into(),
                existing_request: "{}".into(),
                attempted_op: "enqueue".into(),
                attempted_request: "{\"a\":1}".into(),
            }),
            BackendError::Inconsistent(StorageInconsistency {
                queue: "default".into(),
                operation_id: Some(id),
                detail: "type".into(),
            }),
            BackendError::Indeterminate(IndeterminateOperation { operation_id: id, message_id: message }),
            BackendError::Transaction(TransactionConflict { limit: 2 }),
            BackendError::Halted { queue: "default".into() },
            BackendError::Transport { detail: "closed".into() },
            BackendError::Page { detail: "cursor".into() },
        ];
        let kinds = ["input", "full", "conflict", "inconsistent", "indeterminate", "transport", "halted", "transport", "page"];
        for (error, kind) in errors.into_iter().zip(kinds) {
            let mapped = BridgeError::from(error);
            assert_eq!(mapped.kind(), kind);
            if kind == "indeterminate" {
                match mapped {
                    BridgeError::Indeterminate { operation_id, message_id } => {
                        assert_eq!(operation_id, id.to_string());
                        assert_eq!(message_id, message.to_string());
                    }
                    other => panic!("{other:?}"),
                }
            }
        }
        let _ = task;
        let _ = DuplicateTask {
            unique_key: "item:1".into(),
            task_id: task,
            owner_task_id: task,
        };
        assert_eq!(BridgeError::Closed.kind(), "closed");
        assert_eq!(BridgeError::input("x").kind(), "input");
    }
}
