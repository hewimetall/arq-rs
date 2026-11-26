//! One dedicated Redis connection. `WATCH` is not multiplexed.

use crate::error::BackendError;
use crate::resp::{self, as_string, as_u64};
use crate::types::ExecFault;
use arq_core::{classify_exec, ExecClass, ExecReply, PlannedCommand, StorageInconsistency};
use redis::{cmd, Connection, ServerError, ServerErrorKind, Value};
use std::time::Duration;

pub(crate) struct Session {
    inner: Connection,
    asking: bool,
    in_multi: bool,
}

#[derive(Debug)]
pub(crate) enum Fail {
    Backend(BackendError),
    Redirect { addr: String, ask: bool },
    Io(String),
    CrossSlot,
}

impl Fail {
    pub(crate) fn io(detail: impl Into<String>) -> Self {
        Self::Io(detail.into())
    }
}

#[derive(Debug)]
pub(crate) struct Executed {
    pub class: ExecClass,
}

impl Session {
    pub(crate) fn open(url: &str, asking: bool) -> Result<Self, Fail> {
        let client = redis::Client::open(url).map_err(|error| Fail::io(error.to_string()))?;
        let inner = client.get_connection().map_err(|error| Fail::io(error.to_string()))?;
        inner
            .set_read_timeout(Some(Duration::from_secs(15)))
            .map_err(|error| Fail::io(error.to_string()))?;
        inner
            .set_write_timeout(Some(Duration::from_secs(15)))
            .map_err(|error| Fail::io(error.to_string()))?;
        Ok(Self { inner, asking, in_multi: false })
    }

    pub(crate) fn time_ms(&mut self) -> Result<u64, Fail> {
        let value = self.call(&cmd("TIME"))?;
        let Value::Array(parts) = value else {
            return Err(Fail::io("TIME reply was not an array"));
        };
        let seconds = parts.first().and_then(as_u64).ok_or_else(|| Fail::io("TIME seconds"))?;
        let micros = parts.get(1).and_then(as_u64).ok_or_else(|| Fail::io("TIME microseconds"))?;
        seconds
            .checked_mul(1000)
            .and_then(|millis| millis.checked_add(micros / 1000))
            .ok_or_else(|| Fail::io("TIME overflow"))
    }

    pub(crate) fn watch(&mut self, keys: &[String]) -> Result<(), Fail> {
        if keys.is_empty() {
            return Err(Fail::io("WATCH without keys"));
        }
        self.ok(&cmd("WATCH").arg(keys).clone())
    }

    pub(crate) fn unwatch(&mut self) -> Result<(), Fail> {
        if self.in_multi {
            return Ok(());
        }
        match self.call(&cmd("UNWATCH")) {
            Ok(_) => Ok(()),
            Err(Fail::Io(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn discard_and_unwatch(&mut self) {
        if self.in_multi {
            let _ = self.call(&cmd("DISCARD"));
            self.in_multi = false;
        }
        let _ = self.call(&cmd("UNWATCH"));
    }

    pub(crate) fn zcard(&mut self, key: &str) -> Result<u64, Fail> {
        match self.call(&cmd("ZCARD").arg(key).clone())? {
            Value::Int(number) if number >= 0 => Ok(number as u64),
            other => Err(Fail::io(format!("ZCARD reply {other:?}"))),
        }
    }

    pub(crate) fn zcount(&mut self, key: &str, min: &str, max: &str) -> Result<u64, Fail> {
        match self.call(&cmd("ZCOUNT").arg(key).arg(min).arg(max).clone())? {
            Value::Int(number) if number >= 0 => Ok(number as u64),
            other => Err(Fail::io(format!("ZCOUNT reply {other:?}"))),
        }
    }

    pub(crate) fn set(&mut self, key: &str, value: &str) -> Result<(), Fail> {
        self.ok(&cmd("SET").arg(key).arg(value).clone())
    }

    pub(crate) fn get(&mut self, key: &str) -> Result<Option<String>, Fail> {
        match self.call(&cmd("GET").arg(key).clone())? {
            Value::Nil => Ok(None),
            value => as_string(&value).map(Some).ok_or_else(|| Fail::io("GET reply was not a string")),
        }
    }

    pub(crate) fn hgetall(&mut self, key: &str) -> Result<Vec<(String, String)>, Fail> {
        let value = self.call(&cmd("HGETALL").arg(key).clone())?;
        resp::pairs(value).map_err(Fail::io)
    }

    pub(crate) fn zscore(&mut self, key: &str, member: &str) -> Result<Option<u64>, Fail> {
        match self.call(&cmd("ZSCORE").arg(key).arg(member).clone())? {
            Value::Nil => Ok(None),
            value => as_u64(&value).map(Some).ok_or_else(|| Fail::io("ZSCORE reply")),
        }
    }

    pub(crate) fn zrange_by_score(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        limit: usize,
    ) -> Result<Vec<String>, Fail> {
        let value = self
            .call(
                &cmd("ZRANGEBYSCORE")
                    .arg(key)
                    .arg(min)
                    .arg(max)
                    .arg("LIMIT")
                    .arg(0)
                    .arg(limit)
                    .clone(),
            )?;
        match value {
            Value::Nil => Ok(Vec::new()),
            Value::Array(items) => items.iter().map(|item| as_string(item).ok_or_else(|| Fail::io("ZRANGE member"))).collect(),
            other => Err(Fail::io(format!("ZRANGEBYSCORE reply {other:?}"))),
        }
    }

    pub(crate) fn zrange_with_scores(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        reverse: bool,
        offset: usize,
        count: usize,
    ) -> Result<Vec<(String, u64)>, Fail> {
        let mut command = cmd("ZRANGE");
        command.arg(key);
        // Redis 8 BYSCORE REV treats the first bound as the high score.
        if reverse {
            command.arg(max).arg(min).arg("BYSCORE").arg("REV");
        } else {
            command.arg(min).arg(max).arg("BYSCORE");
        }
        command.arg("LIMIT").arg(offset).arg(count).arg("WITHSCORES");
        let value = self.call(&command)?;
        resp::scored_members(value).map_err(Fail::io)
    }

    pub(crate) fn scan(&mut self, pattern: &str) -> Result<Vec<String>, Fail> {
        let mut cursor = 0u64;
        let mut keys = Vec::new();
        loop {
            let value = self.call(&cmd("SCAN").arg(cursor).arg("MATCH").arg(pattern).arg("COUNT").arg(256).clone())?;
            let Value::Array(parts) = value else {
                return Err(Fail::io("SCAN reply"));
            };
            let next = parts.first().and_then(as_u64).ok_or_else(|| Fail::io("SCAN cursor"))?;
            if let Some(Value::Array(found)) = parts.get(1) {
                for key in found {
                    if let Some(text) = as_string(key) {
                        keys.push(text);
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(keys)
    }

    pub(crate) fn key_type(&mut self, key: &str) -> Result<String, Fail> {
        match self.call(&cmd("TYPE").arg(key).clone())? {
            value => as_string(&value).ok_or_else(|| Fail::io("TYPE reply")),
        }
    }

    /// `none` or the single expected Redis type. Anything else is storage inconsistency.
    pub(crate) fn expect_type(&mut self, key: &str) -> Result<(), Fail> {
        let Some(expected) = expected_type(key) else {
            return Ok(());
        };
        let actual = self.key_type(key)?;
        if actual == "none" || actual == expected {
            Ok(())
        } else {
            Err(Fail::Backend(BackendError::Inconsistent(StorageInconsistency {
                queue: queue_of(key).unwrap_or_default(),
                operation_id: None,
                detail: format!("key {key} has type {actual}, expected {expected} or none"),
            })))
        }
    }

    pub(crate) fn expect_types(&mut self, keys: &[String]) -> Result<(), Fail> {
        let mut seen = Vec::new();
        for key in keys {
            if seen.iter().any(|seen: &String| seen == key) {
                continue;
            }
            seen.push(key.clone());
            self.expect_type(key)?;
        }
        Ok(())
    }

    pub(crate) fn exec_plan(
        &mut self,
        commands: &[PlannedCommand],
        watched: &[String],
        fault: ExecFault,
        before_exec: Option<&(dyn Fn(&[String]) + Send + Sync)>,
    ) -> Result<Executed, Fail> {
        self.ok(&cmd("MULTI"))?;
        self.in_multi = true;
        for command in commands {
            if let Err(error) = self.queue(&planned(command)?) {
                self.in_multi = false;
                return self.finish_rejected_queue(error, commands.len());
            }
        }
        if let Some(hook) = before_exec {
            hook(watched);
        }
        match fault {
            ExecFault::DropBeforeExec => {
                self.discard_and_unwatch();
                return Err(Fail::io("connection closed before EXEC"));
            }
            ExecFault::LoseReply => {
                let packed = cmd("EXEC").get_packed_command();
                if let Err(error) = self.inner.send_packed_command(&packed) {
                    self.in_multi = false;
                    return Ok(Executed { class: lost_from_send(error) });
                }
                self.in_multi = false;
                return Ok(Executed { class: classify_exec(ExecReply::Lost) });
            }
            ExecFault::AbortOnce | ExecFault::AbortAlways => {
                let _ = self.queue(&cmd("HSET"));
            }
            ExecFault::None => {}
        }
        let class = self.read_exec(commands.len())?;
        self.in_multi = false;
        Ok(Executed { class })
    }

    fn finish_rejected_queue(&mut self, error: Fail, expected: usize) -> Result<Executed, Fail> {
        if matches!(error, Fail::Redirect { .. } | Fail::CrossSlot) {
            return Err(error);
        }
        match self.read_exec(expected) {
            Ok(class) => Ok(Executed { class }),
            Err(Fail::Io(_)) => Ok(Executed { class: classify_exec(ExecReply::Abort) }),
            Err(other) => Err(other),
        }
    }

    fn read_exec(&mut self, expected: usize) -> Result<ExecClass, Fail> {
        let packed = cmd("EXEC").get_packed_command();
        if let Err(error) = self.inner.send_packed_command(&packed) {
            self.in_multi = false;
            let _ = error;
            return Ok(classify_exec(ExecReply::Lost));
        }
        match self.inner.recv_response() {
            Ok(Value::Push { .. }) => self.read_exec_follow(expected),
            Ok(value) => self.classify_raw_exec(value, expected),
            Err(error) => {
                self.in_multi = false;
                Ok(lost_from_send(error))
            }
        }
    }

    fn read_exec_follow(&mut self, expected: usize) -> Result<ExecClass, Fail> {
        match self.inner.recv_response() {
            Ok(value) => self.classify_raw_exec(value, expected),
            Err(error) => Ok(lost_from_send(error)),
        }
    }

    fn classify_raw_exec(&mut self, value: Value, expected: usize) -> Result<ExecClass, Fail> {
        self.in_multi = false;
        if let Value::ServerError(error) = &value {
            if let Some(redirect) = redirect_of(error) {
                return Err(redirect);
            }
            if error.kind() == Some(ServerErrorKind::CrossSlot) {
                return Err(Fail::CrossSlot);
            }
        }
        Ok(resp::classify_exec_value(value, expected))
    }

    fn queue(&mut self, command: &redis::Cmd) -> Result<(), Fail> {
        match self.call(command)? {
            Value::SimpleString(text) if text == "QUEUED" => Ok(()),
            Value::Okay => Ok(()),
            Value::ServerError(error) => Err(server_fail(error)),
            other => Err(Fail::io(format!("expected QUEUED, got {other:?}"))),
        }
    }

    fn ok(&mut self, command: &redis::Cmd) -> Result<(), Fail> {
        match self.call(command)? {
            Value::Okay | Value::SimpleString(_) => Ok(()),
            Value::ServerError(error) => Err(server_fail(error)),
            other => Err(Fail::io(format!("expected OK, got {other:?}"))),
        }
    }

    pub(crate) fn call(&mut self, command: &redis::Cmd) -> Result<Value, Fail> {
        self.write(command)?;
        self.read()
    }

    fn write(&mut self, command: &redis::Cmd) -> Result<(), Fail> {
        if self.asking && !self.in_multi {
            let asking = cmd("ASKING");
            self.inner
                .send_packed_command(&asking.get_packed_command())
                .map_err(|error| Fail::io(error.to_string()))?;
            match self.read_raw()? {
                Value::Okay | Value::SimpleString(_) => {}
                Value::ServerError(error) => return Err(server_fail(error)),
                other => return Err(Fail::io(format!("ASKING reply {other:?}"))),
            }
        }
        self.inner
            .send_packed_command(&command.get_packed_command())
            .map_err(|error| Fail::io(error.to_string()))
    }

    fn read(&mut self) -> Result<Value, Fail> {
        let value = self.read_raw()?;
        if let Value::ServerError(error) = &value {
            if let Some(redirect) = redirect_of(error) {
                return Err(redirect);
            }
            if error.kind() == Some(ServerErrorKind::CrossSlot) {
                return Err(Fail::CrossSlot);
            }
            if is_transport_server(error) {
                return Err(Fail::io(error.to_string()));
            }
        }
        Ok(value)
    }

    fn read_raw(&mut self) -> Result<Value, Fail> {
        loop {
            match self.inner.recv_response() {
                Ok(Value::Push { .. }) => continue,
                Ok(value) => return Ok(value),
                Err(error) => return Err(Fail::io(error.to_string())),
            }
        }
    }
}

fn lost_from_send(error: redis::RedisError) -> ExecClass {
    let _ = error;
    classify_exec(ExecReply::Lost)
}

fn planned(command: &PlannedCommand) -> Result<redis::Cmd, Fail> {
    let mut out = cmd(match command {
        PlannedCommand::Hset { .. } => "HSET",
        PlannedCommand::Zadd { .. } => "ZADD",
        PlannedCommand::Zrem { .. } => "ZREM",
        PlannedCommand::Set { .. } => "SET",
        PlannedCommand::Del { .. } => "DEL",
        PlannedCommand::SetEx { .. } => "SET",
    });
    match command {
        PlannedCommand::Hset { key, fields } => {
            if fields.is_empty() {
                return Err(Fail::io("HSET without fields"));
            }
            out.arg(key);
            for (field, value) in fields {
                out.arg(field).arg(value);
            }
        }
        PlannedCommand::Zadd { key, member, score } => {
            out.arg(key).arg(*score).arg(member);
        }
        PlannedCommand::Zrem { key, member } => {
            out.arg(key).arg(member);
        }
        PlannedCommand::Set { key, value } => {
            out.arg(key).arg(value);
        }
        PlannedCommand::Del { key } => {
            out.arg(key);
        }
        PlannedCommand::SetEx { key, value, ttl_secs } => {
            out.arg(key).arg(value).arg("EX").arg(*ttl_secs);
        }
    }
    Ok(out)
}

pub(crate) fn expected_type(key: &str) -> Option<&'static str> {
    match arq_core::key_kind(key) {
        Some("job") => Some("hash"),
        Some("receipt" | "uniq") => Some("string"),
        Some("ready" | "inflight" | "done" | "dead" | "quarantine") => Some("zset"),
        Some("qreason") => Some("hash"),
        Some("halted") => Some("string"),
        _ => None,
    }
}

pub(crate) fn queue_of(key: &str) -> Option<String> {
    let rest = key.strip_prefix("q:{")?;
    let (queue, _) = rest.split_once("}:")?;
    Some(queue.to_string())
}

fn server_fail(error: ServerError) -> Fail {
    if let Some(redirect) = redirect_of(&error) {
        return redirect;
    }
    if error.kind() == Some(ServerErrorKind::CrossSlot) {
        return Fail::CrossSlot;
    }
    if error.kind() == Some(ServerErrorKind::ExecAbort) {
        return Fail::Io(error.to_string());
    }
    Fail::Io(error.to_string())
}

fn redirect_of(error: &ServerError) -> Option<Fail> {
    let ask = match error.kind() {
        Some(ServerErrorKind::Ask) => true,
        Some(ServerErrorKind::Moved) => false,
        _ => return None,
    };
    let detail = error.details().unwrap_or("");
    let addr = detail.split_whitespace().nth(1).unwrap_or("").to_string();
    if addr.is_empty() {
        return Some(Fail::io(format!("redirect without address: {error}")));
    }
    Some(Fail::Redirect { addr, ask })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::PlannedCommand;

    #[test]
    fn planned_commands_cover_every_variant_and_unknown_keys() {
        assert!(planned(&PlannedCommand::Hset { key: "k".into(), fields: vec![] }).is_err());
        assert!(planned(&PlannedCommand::Hset { key: "k".into(), fields: vec![("f".into(), "v".into())] }).is_ok());
        assert!(planned(&PlannedCommand::Zadd { key: "k".into(), member: "m".into(), score: 1 }).is_ok());
        assert!(planned(&PlannedCommand::Zrem { key: "k".into(), member: "m".into() }).is_ok());
        assert!(planned(&PlannedCommand::Set { key: "k".into(), value: "v".into() }).is_ok());
        assert!(planned(&PlannedCommand::Del { key: "k".into() }).is_ok());
        assert!(planned(&PlannedCommand::SetEx { key: "k".into(), value: "v".into(), ttl_secs: 5 }).is_ok());
        assert_eq!(expected_type("q:{default}:job:x"), Some("hash"));
        assert_eq!(expected_type("q:{default}:receipt:x"), Some("string"));
        assert_eq!(expected_type("q:{default}:uniq:item:1"), Some("string"));
        assert_eq!(expected_type("q:{default}:ready"), Some("zset"));
        assert_eq!(expected_type("q:{default}:inflight"), Some("zset"));
        assert_eq!(expected_type("q:{default}:done"), Some("zset"));
        assert_eq!(expected_type("q:{default}:dead"), Some("zset"));
        assert_eq!(expected_type("q:{default}:dead:tag:batch-a"), Some("zset"));
        assert_eq!(expected_type("q:{default}:quarantine"), Some("zset"));
        assert_eq!(expected_type("q:{default}:qreason"), Some("hash"));
        assert_eq!(expected_type("q:{default}:halted"), Some("string"));
        assert_eq!(expected_type("outside"), None);
        assert_eq!(queue_of("q:{default}:ready").as_deref(), Some("default"));
        assert!(queue_of("ready").is_none());
        assert!(queue_of("q:{default").is_none());
    }

    fn open(replies: Vec<Vec<u8>>, asking: bool) -> Session {
        let url = crate::scripted::serve(replies);
        Session::open(&url, asking).expect("scripted session")
    }

    #[test]
    fn malformed_replies_and_local_guards_are_errors() {
        use crate::scripted::{array, bulk, empty_array, error as redis_error, integer, nil, ok, queued, time_reply};

        let mut session = open(vec![integer(1)], false);
        assert!(matches!(session.time_ms(), Err(Fail::Io(detail)) if detail.contains("TIME")));
        assert!(matches!(session.watch(&[]), Err(Fail::Io(detail)) if detail.contains("WATCH")));
        session.in_multi = true;
        assert!(session.unwatch().is_ok());
        session.in_multi = false;

        let mut session = open(vec![redis_error("MOVED 5718 127.0.0.1:9")], false);
        assert!(matches!(session.unwatch().unwrap_err(), Fail::Redirect { ask: false, .. }));
        let mut session = open(vec![b"CLOSE".to_vec()], false);
        assert!(session.unwatch().is_ok());

        let mut session = open(vec![ok()], false);
        assert!(matches!(session.zcard("ready"), Err(Fail::Io(detail)) if detail.contains("ZCARD")));
        let mut session = open(vec![ok()], false);
        assert!(matches!(session.zcount("ready", "-inf", "+inf"), Err(Fail::Io(detail)) if detail.contains("ZCOUNT")));
        let mut session = open(vec![ok()], false);
        assert!(matches!(session.zrange_by_score("ready", "-inf", "+inf", 1), Err(Fail::Io(detail)) if detail.contains("ZRANGE")));
        let mut session = open(vec![redis_error("MOVED 1 127.0.0.1:9")], false);
        assert!(matches!(session.zrange_by_score("ready", "-inf", "+inf", 1), Err(Fail::Redirect { .. })));
        let mut session = open(vec![nil()], false);
        assert!(session.zrange_by_score("ready", "-inf", "+inf", 1).unwrap().is_empty());
        let mut session = open(vec![array(&[nil()])], false);
        assert!(session.zrange_by_score("ready", "-inf", "+inf", 1).is_err());

        let mut session = open(vec![ok()], false);
        assert!(session.scan("arq:*").is_err());
        let mut session = open(vec![array(&[bulk("0"), nil()])], false);
        assert!(session.scan("arq:*").unwrap().is_empty());
        let mut session = open(
            vec![array(&[bulk("1"), array(&[bulk("kept"), nil()])]), array(&[bulk("0"), empty_array()])],
            false,
        );
        assert_eq!(session.scan("arq:*").unwrap(), vec!["kept".to_string()]);

        let job = "q:{default}:job:same".to_string();
        let mut session = open(vec![bulk("none"), bulk("none")], false);
        assert!(session.expect_types(&[job.clone(), job]).is_ok());

        let mut session = open(vec![redis_error("EXECABORT nested")], false);
        assert!(matches!(session.ok(&cmd("WATCH").arg("k").clone()), Err(Fail::Io(detail)) if detail.contains("EXECABORT") || detail.contains("nested")));
        let mut session = open(vec![integer(1)], false);
        assert!(session.ok(&cmd("WATCH").arg("k").clone()).is_err());
        let mut session = open(vec![ok()], false);
        assert!(session.queue(&cmd("HSET").arg("k").arg("f").arg("v").clone()).is_ok());
        let mut session = open(vec![integer(1)], false);
        assert!(session.queue(&cmd("HSET").arg("k").arg("f").arg("v").clone()).is_err());
        let mut session = open(vec![redis_error("CROSSSLOT Keys")], false);
        assert!(matches!(session.ok(&cmd("WATCH").arg("a").arg("b").clone()).unwrap_err(), Fail::CrossSlot));
        let mut session = open(vec![redis_error("MOVED 1")], false);
        assert!(matches!(session.ok(&cmd("PING")), Err(Fail::Io(detail)) if detail.contains("redirect without address")));

        let mut session = open(vec![redis_error("ERR asking")], true);
        assert!(session.time_ms().is_err());
        let mut session = open(vec![redis_error("CROSSSLOT Keys")], true);
        assert!(matches!(session.time_ms(), Err(Fail::CrossSlot)));
        let mut session = open(vec![redis_error("MOVED 8 127.0.0.1:9")], true);
        assert!(matches!(session.time_ms(), Err(Fail::Redirect { ask: false, .. })));
        let mut session = open(vec![integer(1)], true);
        assert!(matches!(session.time_ms(), Err(Fail::Io(detail)) if detail.contains("ASKING")));
        let mut session = open(vec![ok(), time_reply()], true);
        assert_eq!(session.time_ms().unwrap(), 10_000);

        let mut session = open(vec![ok(), redis_error("nope"), redis_error("EXECABORT after")], false);
        let executed = session
            .exec_plan(
                &[PlannedCommand::Set { key: "k".into(), value: "v".into() }],
                &[],
                ExecFault::None,
                None,
            )
            .unwrap();
        assert!(matches!(executed.class, ExecClass::RetryCycle { charge_watch: false }));

        let mut session = open(vec![ok(), queued(), b"CLOSE".to_vec()], false);
        let executed = session
            .exec_plan(
                &[PlannedCommand::Set { key: "k".into(), value: "v".into() }],
                &[],
                ExecFault::None,
                None,
            )
            .unwrap();
        assert!(matches!(executed.class, ExecClass::UnknownOutcome));

        let mut session = open(vec![ok(), queued(), redis_error("MOVED 9 127.0.0.1:1")], false);
        let error = session
            .exec_plan(
                &[PlannedCommand::Set { key: "k".into(), value: "v".into() }],
                &[],
                ExecFault::None,
                None,
            )
            .unwrap_err();
        assert!(matches!(error, Fail::Redirect { .. }));

        let mut session = open(vec![ok(), queued(), redis_error("CROSSSLOT Keys")], false);
        let error = session
            .exec_plan(
                &[PlannedCommand::Set { key: "k".into(), value: "v".into() }],
                &[],
                ExecFault::None,
                None,
            )
            .unwrap_err();
        assert!(matches!(error, Fail::CrossSlot));

        let mut session = open(vec![], false);
        let error = session
            .finish_rejected_queue(Fail::Redirect { addr: "127.0.0.1:1".into(), ask: true }, 1)
            .unwrap_err();
        assert!(matches!(error, Fail::Redirect { ask: true, .. }));
        let error = session.finish_rejected_queue(Fail::CrossSlot, 1).unwrap_err();
        assert!(matches!(error, Fail::CrossSlot));

        let mut session = open(vec![redis_error("MOVED 4 127.0.0.1:9")], false);
        session.in_multi = true;
        let error = session.finish_rejected_queue(Fail::Io("queued".into()), 1).unwrap_err();
        assert!(matches!(error, Fail::Redirect { .. }));
    }
}

fn is_transport_server(error: &ServerError) -> bool {
    matches!(
        error.kind(),
        Some(
            ServerErrorKind::BusyLoading
                | ServerErrorKind::TryAgain
                | ServerErrorKind::ClusterDown
                | ServerErrorKind::MasterDown
                | ServerErrorKind::ReadOnly
        )
    )
}

pub(crate) fn inconsistency(queue: &str, operation: Option<arq_core::OperationId>, detail: impl Into<String>) -> BackendError {
    BackendError::Inconsistent(StorageInconsistency {
        queue: queue.to_string(),
        operation_id: operation,
        detail: detail.into(),
    })
}
