//! `ARQ_*` configuration. Any set `SIMPLE_Q_*` variable is a startup error.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Child exit when configuration cannot be loaded.
pub const CONFIG_EXIT_CODE: i32 = 3;

const PING_BUDGET_GRACE: Duration = Duration::from_secs(60);

const KNOWN: &[&str] = &[
    "ARQ_POLL_INTERVAL_MS",
    "ARQ_INFLIGHT_SCAN_INTERVAL_MS",
    "ARQ_LEASE_MS",
    "ARQ_RENEW_INTERVAL_MS",
    "ARQ_RENEW_CONFIRM_MS",
    "ARQ_PING_INTERVAL_MS",
    "ARQ_PING_TIMEOUT_MS",
    "ARQ_LIFECYCLE_INTERVAL_MS",
    "ARQ_PING_BUDGET_SECS",
    "ARQ_SHUTDOWN_TIMEOUT_MS",
    "ARQ_CANCEL_GRACE_MS",
    "ARQ_MAX_DELIVERIES",
    "ARQ_MAX_TASKS_PER_CHILD",
    "ARQ_MEMORY_SOFT_RATIO",
    "ARQ_MEMORY_HARD_RATIO",
    "ARQ_CHILD_MEMORY_SOFT",
    "ARQ_CHILD_MEMORY_HARD",
    "ARQ_HEARTBEAT_SECS",
    "ARQ_MISSED_HEARTBEATS",
    "ARQ_MAINTENANCE_INTERVAL_MS",
    "ARQ_MAINTENANCE_BATCH",
    "ARQ_WATCH_RETRIES",
    "ARQ_DONE_TTL_SECS",
    "ARQ_DEAD_TTL_SECS",
    "ARQ_RECEIPT_TTL_SECS",
    "ARQ_MAX_READY",
    "ARQ_MAX_MESSAGE_BYTES",
    "ARQ_MAX_HEADERS_BYTES",
    "ARQ_MAX_ERROR_BYTES",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    SimpleQSet { name: String },
    Unknown { name: String },
    Invalid {
        name: &'static str,
        value: String,
        reason: &'static str,
    },
    Inequality { rule: &'static str },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SimpleQSet { name } => write!(
                f,
                "{name} is set; the SIMPLE_Q_ prefix is rejected and is not read"
            ),
            Self::Unknown { name } => write!(f, "unknown configuration variable {name}"),
            Self::Invalid { name, value, reason } => {
                write!(f, "invalid {name}={value}: {reason}")
            }
            Self::Inequality { rule } => write!(f, "configuration inequality failed: {rule}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl ConfigError {
    pub const EXIT_CODE: i32 = CONFIG_EXIT_CODE;
}

/// Process configuration. Construct it with [`Config::parse`] or [`Config::from_env`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Config {
    pub poll_interval_ms: u64,
    pub inflight_scan_interval_ms: u64,
    pub lease_ms: u64,
    pub renew_interval_ms: u64,
    pub renew_confirm_ms: u64,
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
    pub lifecycle_interval_ms: u64,
    pub ping_budget_secs: u64,
    /// Set when `ARQ_PING_BUDGET_SECS` is present. Otherwise the budget is the task time limit plus 60s.
    pub ping_budget_explicit: bool,
    pub shutdown_timeout_ms: u64,
    pub cancel_grace_ms: u64,
    pub max_deliveries: u64,
    pub max_tasks_per_child: u64,
    pub memory_soft_ratio: f64,
    pub memory_hard_ratio: f64,
    pub child_memory_soft: Option<u64>,
    pub child_memory_hard: Option<u64>,
    pub heartbeat_secs: u64,
    pub missed_heartbeats: u64,
    pub maintenance_interval_ms: u64,
    pub maintenance_batch: u64,
    pub watch_retries: u64,
    pub done_ttl_secs: u64,
    pub dead_ttl_secs: u64,
    pub receipt_ttl_secs: u64,
    pub max_ready: u64,
    pub max_message_bytes: u64,
    pub max_headers_bytes: u64,
    pub max_error_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self::parse(std::iter::empty::<(&str, &str)>()).expect("default configuration is valid")
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::parse(std::env::vars())
    }

    pub fn parse<I, K, V>(vars: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut map = BTreeMap::<String, String>::new();
        for (key, value) in vars {
            map.insert(key.as_ref().to_string(), value.as_ref().to_string());
        }
        if let Some(name) = map.keys().find(|key| key.starts_with("SIMPLE_Q_")) {
            return Err(ConfigError::SimpleQSet { name: name.clone() });
        }
        if let Some(name) = map
            .keys()
            .find(|key| key.starts_with("ARQ_") && !KNOWN.contains(&key.as_str()))
        {
            return Err(ConfigError::Unknown { name: name.clone() });
        }

        let ping_budget_explicit = map.contains_key("ARQ_PING_BUDGET_SECS");
        let config = Self {
            poll_interval_ms: positive(&map, "ARQ_POLL_INTERVAL_MS", 500)?,
            inflight_scan_interval_ms: positive(&map, "ARQ_INFLIGHT_SCAN_INTERVAL_MS", 500)?,
            lease_ms: positive(&map, "ARQ_LEASE_MS", 15_000)?,
            renew_interval_ms: positive(&map, "ARQ_RENEW_INTERVAL_MS", 5_000)?,
            renew_confirm_ms: positive(&map, "ARQ_RENEW_CONFIRM_MS", 2_000)?,
            ping_interval_ms: positive(&map, "ARQ_PING_INTERVAL_MS", 2_000)?,
            ping_timeout_ms: positive(&map, "ARQ_PING_TIMEOUT_MS", 3_000)?,
            lifecycle_interval_ms: positive(&map, "ARQ_LIFECYCLE_INTERVAL_MS", 200)?,
            ping_budget_secs: positive(&map, "ARQ_PING_BUDGET_SECS", 1_860)?,
            ping_budget_explicit,
            shutdown_timeout_ms: positive(&map, "ARQ_SHUTDOWN_TIMEOUT_MS", 30_000)?,
            cancel_grace_ms: positive(&map, "ARQ_CANCEL_GRACE_MS", 5_000)?,
            max_deliveries: positive(&map, "ARQ_MAX_DELIVERIES", 5)?,
            max_tasks_per_child: positive(&map, "ARQ_MAX_TASKS_PER_CHILD", 500)?,
            memory_soft_ratio: ratio(&map, "ARQ_MEMORY_SOFT_RATIO", 0.80)?,
            memory_hard_ratio: ratio(&map, "ARQ_MEMORY_HARD_RATIO", 0.95)?,
            child_memory_soft: optional_positive(&map, "ARQ_CHILD_MEMORY_SOFT")?,
            child_memory_hard: optional_positive(&map, "ARQ_CHILD_MEMORY_HARD")?,
            heartbeat_secs: positive(&map, "ARQ_HEARTBEAT_SECS", 15)?,
            missed_heartbeats: positive(&map, "ARQ_MISSED_HEARTBEATS", 4)?,
            maintenance_interval_ms: positive(&map, "ARQ_MAINTENANCE_INTERVAL_MS", 60_000)?,
            maintenance_batch: positive(&map, "ARQ_MAINTENANCE_BATCH", 100)?,
            watch_retries: positive(&map, "ARQ_WATCH_RETRIES", 8)?,
            done_ttl_secs: positive(&map, "ARQ_DONE_TTL_SECS", 86_400)?,
            dead_ttl_secs: positive(&map, "ARQ_DEAD_TTL_SECS", 604_800)?,
            receipt_ttl_secs: positive(&map, "ARQ_RECEIPT_TTL_SECS", 691_200)?,
            max_ready: positive(&map, "ARQ_MAX_READY", 100_000)?,
            max_message_bytes: positive(&map, "ARQ_MAX_MESSAGE_BYTES", 1_048_576)?,
            max_headers_bytes: positive(&map, "ARQ_MAX_HEADERS_BYTES", 65_536)?,
            max_error_bytes: positive(&map, "ARQ_MAX_ERROR_BYTES", 4_096)?,
        };
        config.check_inequalities()?;
        Ok(config)
    }

    fn check_inequalities(self) -> Result<(), ConfigError> {
        let renew = self
            .renew_interval_ms
            .checked_add(self.renew_confirm_ms)
            .ok_or(ConfigError::Inequality {
                rule: "renew_interval_ms + renew_confirm_ms < lease_ms",
            })?;
        if renew >= self.lease_ms {
            return Err(ConfigError::Inequality {
                rule: "renew_interval_ms + renew_confirm_ms < lease_ms",
            });
        }
        if !(self.receipt_ttl_secs >= self.dead_ttl_secs && self.dead_ttl_secs >= self.done_ttl_secs)
        {
            return Err(ConfigError::Inequality {
                rule: "receipt_ttl >= dead_ttl >= done_ttl",
            });
        }
        if !(self.memory_soft_ratio > 0.0
            && self.memory_soft_ratio < self.memory_hard_ratio
            && self.memory_hard_ratio < 1.0)
        {
            return Err(ConfigError::Inequality {
                rule: "0 < memory_soft_ratio < memory_hard_ratio < 1",
            });
        }
        if let (Some(soft), Some(hard)) = (self.child_memory_soft, self.child_memory_hard) {
            if soft >= hard {
                return Err(ConfigError::Inequality {
                    rule: "child_memory_soft < child_memory_hard",
                });
            }
        }
        Ok(())
    }

    /// Budget for one delivery. An explicit `ARQ_PING_BUDGET_SECS` wins.
    /// Otherwise the budget is `task_time_limit` plus 60 seconds.
    pub fn effective_ping_budget(self, task_time_limit: Duration) -> Duration {
        if self.ping_budget_explicit {
            Duration::from_secs(self.ping_budget_secs)
        } else {
            task_time_limit.saturating_add(PING_BUDGET_GRACE)
        }
    }

    /// Reject kwargs JSON or header key/value bytes before any Redis call.
    pub fn check_payload(self, kwargs_bytes: usize, header_bytes: usize) -> Result<(), ConfigError> {
        if kwargs_bytes > self.max_message_bytes as usize {
            return Err(ConfigError::Invalid {
                name: "ARQ_MAX_MESSAGE_BYTES",
                value: kwargs_bytes.to_string(),
                reason: "kwargs exceed ARQ_MAX_MESSAGE_BYTES",
            });
        }
        if header_bytes > self.max_headers_bytes as usize {
            return Err(ConfigError::Invalid {
                name: "ARQ_MAX_HEADERS_BYTES",
                value: header_bytes.to_string(),
                reason: "headers exceed ARQ_MAX_HEADERS_BYTES",
            });
        }
        Ok(())
    }
}

fn positive(map: &BTreeMap<String, String>, name: &'static str, default: u64) -> Result<u64, ConfigError> {
    match map.get(name) {
        None => Ok(default),
        Some(raw) => parse_positive(name, raw),
    }
}

fn optional_positive(map: &BTreeMap<String, String>, name: &'static str) -> Result<Option<u64>, ConfigError> {
    match map.get(name) {
        None => Ok(None),
        Some(raw) => parse_positive(name, raw).map(Some),
    }
}

fn parse_positive(name: &'static str, raw: &str) -> Result<u64, ConfigError> {
    let text = raw.trim();
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ConfigError::Invalid {
            name,
            value: raw.to_string(),
            reason: "expected a positive integer",
        });
    }
    if text.len() > 1 && text.starts_with('0') {
        return Err(ConfigError::Invalid {
            name,
            value: raw.to_string(),
            reason: "expected a positive integer",
        });
    }
    let value: u64 = text.parse().map_err(|_| ConfigError::Invalid {
        name,
        value: raw.to_string(),
        reason: "integer out of range",
    })?;
    if value == 0 {
        return Err(ConfigError::Invalid {
            name,
            value: raw.to_string(),
            reason: "expected a positive integer",
        });
    }
    Ok(value)
}

fn ratio(map: &BTreeMap<String, String>, name: &'static str, default: f64) -> Result<f64, ConfigError> {
    let Some(raw) = map.get(name) else {
        return Ok(default);
    };
    let text = raw.trim();
    if text.is_empty()
        || text.eq_ignore_ascii_case("nan")
        || text.eq_ignore_ascii_case("inf")
        || text.eq_ignore_ascii_case("+inf")
        || text.eq_ignore_ascii_case("-inf")
    {
        return Err(ConfigError::Invalid {
            name,
            value: raw.to_string(),
            reason: "expected a finite ratio",
        });
    }
    let value: f64 = text.parse().map_err(|_| ConfigError::Invalid {
        name,
        value: raw.to_string(),
        reason: "expected a finite ratio",
    })?;
    if !value.is_finite() {
        return Err(ConfigError::Invalid {
            name,
            value: raw.to_string(),
            reason: "expected a finite ratio",
        });
    }
    Ok(value)
}
