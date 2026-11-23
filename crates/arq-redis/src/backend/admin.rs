//! Read-only queue inspection. These calls do not watch, write, or clear a halt.

use super::RedisBackend;
use crate::error::BackendError;
use crate::session::{Fail, Session};
use crate::types::{PeekState, QueueDepth, Snapshot, StoredReceipt, TaskInfo, TaskPage};
use arq_core::{OperationId, QueueKeys, TaskId};

const PAGE_MAX: u64 = 200;

/// Read-only view of one queue. A halted backend still answers.
#[derive(Clone)]
pub struct QueueAdmin {
    backend: RedisBackend,
    queue: String,
}

impl RedisBackend {
    pub fn admin(&self, queue: impl Into<String>) -> QueueAdmin {
        QueueAdmin { backend: self.clone(), queue: queue.into() }
    }
}

impl QueueAdmin {
    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub async fn snapshot(&self, host: Option<&str>) -> Result<Snapshot, BackendError> {
        let backend = self.backend.clone();
        let queue = self.queue.clone();
        let host = host.map(str::to_string);
        super::block_on(move || {
            let depth = backend.depth_blocking(&queue)?;
            let taken_at_ms = backend.with_session(&queue, |session| session.time_ms())?;
            let (supervisors, orphans) = backend.read_registry(host.as_deref())?;
            Ok(Snapshot { taken_at_ms, queues: vec![depth], supervisors, orphans })
        })
        .await
    }

    pub async fn depth(&self) -> Result<QueueDepth, BackendError> {
        let backend = self.backend.clone();
        let queue = self.queue.clone();
        super::block_on(move || backend.depth_blocking(&queue)).await
    }

    pub async fn get(&self, task_id: &str) -> Result<Option<TaskInfo>, BackendError> {
        let backend = self.backend.clone();
        let queue = self.queue.clone();
        let task_id = task_id.to_string();
        super::block_on(move || backend.get_blocking(&queue, &task_id)).await
    }

    pub async fn peek(&self, state: &str, limit: u64, cursor: Option<&str>) -> Result<TaskPage, BackendError> {
        let Some(state) = PeekState::parse(state) else {
            return Err(BackendError::Page { detail: "state must be ready, scheduled, or inflight".into() });
        };
        self.page(state.as_str(), false, limit, cursor).await
    }

    pub async fn dead(&self, limit: u64, cursor: Option<&str>) -> Result<TaskPage, BackendError> {
        self.page("dead", true, limit, cursor).await
    }

    pub async fn dead_for_tag(&self, tag: &str, limit: u64, cursor: Option<&str>) -> Result<TaskPage, BackendError> {
        let filter = format!("dead_tag:{tag}");
        self.page(&filter, true, limit, cursor).await
    }

    pub async fn command(&self, operation_id: &str) -> Result<Option<StoredReceipt>, BackendError> {
        let backend = self.backend.clone();
        let queue = self.queue.clone();
        let operation_id = operation_id.to_string();
        super::block_on(move || backend.command_blocking(&queue, &operation_id)).await
    }

    async fn page(&self, filter: &str, reverse: bool, limit: u64, cursor: Option<&str>) -> Result<TaskPage, BackendError> {
        if !(1..=PAGE_MAX).contains(&limit) {
            return Err(BackendError::Page { detail: "limit must be from 1 to 200".into() });
        }
        let bound = match cursor {
            Some(cursor) => Some(decode_cursor(cursor, &self.queue, filter)?),
            None => None,
        };
        let backend = self.backend.clone();
        let queue = self.queue.clone();
        let filter = filter.to_string();
        super::block_on(move || backend.page_blocking(&queue, &filter, reverse, limit, bound)).await
    }
}

impl RedisBackend {
    fn depth_blocking(&self, queue: &str) -> Result<QueueDepth, BackendError> {
        self.with_session(queue, |session| {
            let keys = keys(queue)?;
            let now = session.time_ms()?;
            let ready_key = keys.ready();
            let ready_zcard = session.zcard(&ready_key)?;
            let ready = session.zcount(&ready_key, "-inf", &now.to_string())?;
            let scheduled = session.zcount(&ready_key, &format!("({now}"), "+inf")?;
            Ok(QueueDepth {
                queue: queue.to_string(),
                ready,
                scheduled,
                inflight: session.zcard(&keys.inflight())?,
                done: session.zcard(&keys.done())?,
                dead: session.zcard(&keys.dead())?,
                ready_zcard,
            })
        })
    }

    fn get_blocking(&self, queue: &str, task_id: &str) -> Result<Option<TaskInfo>, BackendError> {
        let task_id = TaskId::parse(task_id).map_err(|_| BackendError::Page { detail: "task_id is not a canonical uuid".into() })?;
        self.with_session(queue, |session| {
            let keys = keys(queue)?;
            let job = keys.job(&task_id);
            let kind = session.key_type(&job)?;
            if kind == "none" {
                return Ok(None);
            }
            if kind != "hash" {
                return Err(Fail::io(format!("job type {kind}")));
            }
            let fields = session.hgetall(&job)?;
            let score = score_of(session, &keys, &task_id.to_string());
            Ok(Some(info_from_fields(&task_id.to_string(), score, &fields, false)))
        })
    }

    fn command_blocking(&self, queue: &str, operation_id: &str) -> Result<Option<StoredReceipt>, BackendError> {
        let operation_id = OperationId::parse(operation_id)
            .map_err(|_| BackendError::Page { detail: "operation_id is not a canonical uuid".into() })?;
        self.with_session(queue, |session| {
            let keys = keys(queue)?;
            let Some(raw) = session.get(&keys.receipt(&operation_id))? else {
                return Ok(None);
            };
            stored_receipt(&raw).map(Some).map_err(Fail::io)
        })
    }

    fn page_blocking(
        &self,
        queue: &str,
        filter: &str,
        reverse: bool,
        limit: u64,
        cursor: Option<(u64, String)>,
    ) -> Result<TaskPage, BackendError> {
        self.with_session(queue, |session| {
            let keys = keys(queue)?;
            let now = session.time_ms()?;
            let (key, min, max) = match filter {
                "ready" => (keys.ready(), "-inf".to_string(), now.to_string()),
                "scheduled" => (keys.ready(), format!("({now}"), "+inf".to_string()),
                "inflight" => (keys.inflight(), "-inf".to_string(), "+inf".to_string()),
                "dead" => (keys.dead(), "-inf".to_string(), "+inf".to_string()),
                other => {
                    let tag = other.strip_prefix("dead_tag:").ok_or_else(|| Fail::io("unknown page filter"))?;
                    (keys.dead_tag(tag).map_err(|error| Fail::Backend(BackendError::Input(error)))?, "-inf".to_string(), "+inf".to_string())
                }
            };
            let mut offset = 0usize;
            let mut page = Vec::new();
            loop {
                let batch = session.zrange_with_scores(&key, &min, &max, reverse, offset, 128)?;
                if batch.is_empty() {
                    break;
                }
                offset += batch.len();
                for (member, score) in batch {
                    if let Some((cursor_score, cursor_id)) = &cursor {
                        let after = if reverse {
                            score < *cursor_score || (score == *cursor_score && member.as_str() < cursor_id.as_str())
                        } else {
                            score > *cursor_score || (score == *cursor_score && member.as_str() > cursor_id.as_str())
                        };
                        if !after {
                            continue;
                        }
                    }
                    page.push((member, score));
                    if page.len() == limit as usize + 1 {
                        break;
                    }
                }
                if page.len() > limit as usize || offset < 128 {
                    break;
                }
            }
            let more = page.len() > limit as usize;
            if more {
                page.pop();
            }
            let mut tasks = Vec::with_capacity(page.len());
            for (member, score) in &page {
                tasks.push(read_member(session, &keys, member, Some(*score))?);
            }
            let next_cursor = if more {
                page.last().map(|(member, score)| encode_cursor(queue, filter, *score, member))
            } else {
                None
            };
            Ok(TaskPage { tasks, next_cursor })
        })
    }
}

fn read_member(session: &mut Session, keys: &QueueKeys, member: &str, score: Option<u64>) -> Result<TaskInfo, Fail> {
    let Ok(task_id) = TaskId::parse(member) else {
        return Ok(missing(member, score));
    };
    let job = keys.job(&task_id);
    let kind = session.key_type(&job)?;
    if kind == "none" {
        return Ok(missing(member, score));
    }
    if kind != "hash" {
        let mut info = missing(member, score);
        info.state = kind;
        return Ok(info);
    }
    let fields = session.hgetall(&job)?;
    Ok(info_from_fields(member, score, &fields, false))
}

fn missing(task_id: &str, score: Option<u64>) -> TaskInfo {
    TaskInfo {
        task_id: task_id.to_string(),
        message_id: None,
        state: "missing_job".to_string(),
        deliveries: 0,
        retries: 0,
        score,
        missing_job: true,
        tag: None,
        reason: String::new(),
        cycle_id: None,
        control_op: String::new(),
        actor_name: String::new(),
        unique_key: None,
        lease_deadline_ms: None,
        started_at_ms: None,
        available_at_ms: None,
        error: String::new(),
        stop_evidence: String::new(),
    }
}

fn info_from_fields(task_id: &str, score: Option<u64>, fields: &[(String, String)], missing_job: bool) -> TaskInfo {
    let get = |name: &str| fields.iter().find(|(key, _)| key == name).map(|(_, value)| value.clone());
    TaskInfo {
        task_id: get("task_id").unwrap_or_else(|| task_id.to_string()),
        message_id: get("message_id"),
        state: get("state").unwrap_or_default(),
        deliveries: get("deliveries").and_then(|text| text.parse().ok()).unwrap_or(0),
        retries: get("retries").and_then(|text| text.parse().ok()).unwrap_or(0),
        score,
        missing_job,
        tag: get("tag").filter(|text| !text.is_empty()),
        reason: get("reason").unwrap_or_default(),
        cycle_id: get("cycle_id"),
        control_op: get("control_op").unwrap_or_default(),
        actor_name: get("actor_name").unwrap_or_default(),
        unique_key: get("unique_key").filter(|text| !text.is_empty()),
        lease_deadline_ms: get("lease_deadline_ms").and_then(|text| text.parse().ok()),
        started_at_ms: get("started_at_ms").and_then(|text| text.parse().ok()),
        available_at_ms: get("available_at_ms").and_then(|text| text.parse().ok()),
        error: get("error").unwrap_or_default(),
        stop_evidence: get("stop_evidence").unwrap_or_default(),
    }
}

fn score_of(session: &mut Session, keys: &QueueKeys, member: &str) -> Option<u64> {
    for key in [keys.ready(), keys.inflight(), keys.done(), keys.dead()] {
        if let Ok(Some(score)) = session.zscore(&key, member) {
            return Some(score);
        }
    }
    None
}

fn stored_receipt(raw: &str) -> Result<StoredReceipt, String> {
    let json = arq_core::Json::parse(raw).map_err(|error| error.to_string())?;
    let operation_id = json.get("operation_id").and_then(arq_core::Json::as_str).unwrap_or("").to_string();
    let task_id = json.get("task_id").and_then(arq_core::Json::as_str).unwrap_or("").to_string();
    let op = json.get("op").and_then(arq_core::Json::as_str).unwrap_or("").to_string();
    let status = json.get("result").and_then(|result| result.get("status")).and_then(arq_core::Json::as_str).unwrap_or("").to_string();
    let cycle_id = match json.get("cycle_id") {
        Some(arq_core::Json::String(text)) => Some(text.clone()),
        _ => None,
    };
    if operation_id.is_empty() || status.is_empty() {
        return Err("receipt is not canonical".into());
    }
    Ok(StoredReceipt { operation_id, task_id, op, status, cycle_id, raw: raw.to_string() })
}

fn keys(queue: &str) -> Result<QueueKeys, Fail> {
    QueueKeys::new(queue).map_err(|error| Fail::Backend(BackendError::Input(error)))
}

fn encode_cursor(queue: &str, filter: &str, score: u64, task_id: &str) -> String {
    format!("v1:{}:{}:{score}:{task_id}", hex(queue), hex(filter))
}

fn decode_cursor(cursor: &str, queue: &str, filter: &str) -> Result<(u64, String), BackendError> {
    let mut parts = cursor.split(':');
    let version = parts.next();
    let queue_hex = parts.next();
    let filter_hex = parts.next();
    let score = parts.next();
    let task = parts.next();
    if version != Some("v1") || parts.next().is_some() {
        return Err(BackendError::Page { detail: "cursor is not valid for this queue".into() });
    }
    let (Some(queue_hex), Some(filter_hex), Some(score), Some(task)) = (queue_hex, filter_hex, score, task) else {
        return Err(BackendError::Page { detail: "cursor is not valid for this queue".into() });
    };
    let cursor_queue = unhex(queue_hex).map_err(|_| BackendError::Page { detail: "cursor is not valid for this queue".into() })?;
    let cursor_filter = unhex(filter_hex).map_err(|_| BackendError::Page { detail: "cursor is not valid for this queue".into() })?;
    if cursor_queue != queue || cursor_filter != filter || task.is_empty() {
        return Err(BackendError::Page { detail: "cursor is not valid for this queue".into() });
    }
    let score = score.parse().map_err(|_| BackendError::Page { detail: "cursor is not valid for this queue".into() })?;
    Ok((score, task.to_string()))
}

fn hex(text: &str) -> String {
    text.bytes().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Result<String, ()> {
    if text.len() % 2 != 0 {
        return Err(());
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    let chars = text.as_bytes();
    let mut index = 0;
    while index < chars.len() {
        let pair = std::str::from_utf8(&chars[index..index + 2]).map_err(|_| ())?;
        bytes.push(u8::from_str_radix(pair, 16).map_err(|_| ())?);
        index += 2;
    }
    String::from_utf8(bytes).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_cursors_and_receipts() {
        assert!(stored_receipt("[]").is_err());
        let stored = stored_receipt(r#"{"operation_id":"op","cycle_id":4,"result":{"status":"enqueued"}}"#).unwrap();
        assert!(stored.cycle_id.is_none());
        assert_eq!(stored.status, "enqueued");
        assert!(stored_receipt(r#"{"operation_id":"op","result":{}}"#).unwrap_err().contains("canonical"));
        assert!(decode_cursor("v1", "default", "ready").is_err());
        assert!(decode_cursor("v1:0:3030:1:task", "default", "ready").is_err());
        assert!(unhex("zz").is_err());
        assert!(unhex("abc").is_err());
        assert_eq!(unhex("").unwrap(), "");
    }
}
