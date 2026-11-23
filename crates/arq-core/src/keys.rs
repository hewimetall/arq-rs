//! Queue key names. The hash tag is only the queue name.

use crate::error::InputError;
use crate::ids::{OperationId, TaskId};

pub fn validate_queue(queue: &str) -> Result<(), InputError> {
    if !queue.is_empty() && queue.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()) {
        Ok(())
    } else {
        Err(InputError::Queue)
    }
}

pub fn validate_opaque(field: &'static str, value: &str) -> Result<(), InputError> {
    if value.is_empty()
        || value.len() > 256
        || value.chars().any(|ch| ch.is_control() || ch.is_whitespace() || ch == '{' || ch == '}')
    {
        return Err(InputError::Opaque { field });
    }
    Ok(())
}

/// Kind after the hash tag: `ready`, `job`, `uniq`, `receipt`, `dead`, and so on.
pub fn key_kind(key: &str) -> Option<&str> {
    let rest = key.split_once("}:")?.1;
    rest.split(':').next()
}

pub fn is_shared_index(key: &str) -> bool {
    matches!(key_kind(key), Some("ready" | "inflight" | "done" | "dead"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueKeys {
    queue: String,
}

impl QueueKeys {
    pub fn new(queue: &str) -> Result<Self, InputError> {
        validate_queue(queue)?;
        Ok(Self { queue: queue.to_string() })
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn ready(&self) -> String {
        format!("q:{{{}}}:ready", self.queue)
    }

    pub fn inflight(&self) -> String {
        format!("q:{{{}}}:inflight", self.queue)
    }

    pub fn done(&self) -> String {
        format!("q:{{{}}}:done", self.queue)
    }

    pub fn dead(&self) -> String {
        format!("q:{{{}}}:dead", self.queue)
    }

    pub fn dead_tag(&self, tag: &str) -> Result<String, InputError> {
        validate_opaque("tag", tag)?;
        Ok(format!("q:{{{}}}:dead:tag:{tag}", self.queue))
    }

    pub fn job(&self, task_id: &TaskId) -> String {
        format!("q:{{{}}}:job:{task_id}", self.queue)
    }

    pub fn uniq(&self, unique_key: &str) -> Result<String, InputError> {
        validate_opaque("unique_key", unique_key)?;
        Ok(format!("q:{{{}}}:uniq:{unique_key}", self.queue))
    }

    pub fn receipt(&self, operation_id: &OperationId) -> String {
        format!("q:{{{}}}:receipt:{operation_id}", self.queue)
    }
}
