//! RESP readings used by the transaction cycle.

use arq_core::{classify_exec, ExecClass, ExecReply};
use redis::{ServerErrorKind, Value};

pub(crate) fn classify_exec_value(value: Value, expected: usize) -> ExecClass {
    let reply = match value {
        Value::Nil => ExecReply::WatchConflict,
        Value::Array(items) if items.is_empty() => ExecReply::WatchConflict,
        Value::Array(items) => ExecReply::Applied {
            commands: items.len(),
            expected,
            all_ok: items.iter().all(|item| !matches!(item, Value::ServerError(_))),
        },
        Value::ServerError(error) if error.kind() == Some(ServerErrorKind::ExecAbort) => ExecReply::Abort,
        _ => ExecReply::Applied { commands: 0, expected, all_ok: false },
    };
    classify_exec(reply)
}

#[cfg(test)]
pub(crate) fn lost_class() -> ExecClass {
    classify_exec(ExecReply::Lost)
}

pub(crate) fn as_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) | Value::VerbatimString { text, .. } => Some(text.clone()),
        Value::Int(number) => Some(number.to_string()),
        Value::Okay => Some("OK".to_string()),
        Value::Attribute { data, .. } => as_string(data),
        _ => None,
    }
}

pub(crate) fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Int(number) if *number >= 0 => Some(*number as u64),
        Value::BulkString(bytes) => parse_score_bytes(bytes),
        Value::SimpleString(text) => parse_score_text(text),
        Value::Double(number) if number.is_finite() && *number >= 0.0 && number.fract() == 0.0 => {
            Some(*number as u64)
        }
        Value::Attribute { data, .. } => as_u64(data),
        _ => None,
    }
}

fn parse_score_bytes(bytes: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(bytes).ok()?;
    parse_score_text(text)
}

pub(crate) fn parse_score_text(text: &str) -> Option<u64> {
    if let Ok(number) = text.parse::<u64>() {
        return Some(number);
    }
    let (whole, fraction) = text.split_once('.')?;
    if fraction.chars().all(|ch| ch == '0') {
        whole.parse().ok()
    } else {
        None
    }
}

pub(crate) fn pairs(value: Value) -> Result<Vec<(String, String)>, String> {
    let value = match value {
        Value::Attribute { data, .. } => *data,
        other => other,
    };
    match value {
        Value::Nil => Ok(Vec::new()),
        Value::Array(items) => {
            if items.len() % 2 != 0 {
                return Err("hash reply length".to_string());
            }
            let mut out = Vec::with_capacity(items.len() / 2);
            for chunk in items.chunks(2) {
                let key = as_string(&chunk[0]).ok_or_else(|| "hash field".to_string())?;
                let field = as_string(&chunk[1]).ok_or_else(|| "hash value".to_string())?;
                out.push((key, field));
            }
            Ok(out)
        }
        Value::Map(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (key, field) in items {
                let key = as_string(&key).ok_or_else(|| "hash field".to_string())?;
                let field = as_string(&field).ok_or_else(|| "hash value".to_string())?;
                out.push((key, field));
            }
            Ok(out)
        }
        _ => Err("hash reply".to_string()),
    }
}

pub(crate) fn scored_members(value: Value) -> Result<Vec<(String, u64)>, String> {
    let value = match value {
        Value::Attribute { data, .. } => *data,
        other => other,
    };
    match value {
        Value::Nil => Ok(Vec::new()),
        Value::Array(items) => {
            if items.len() % 2 != 0 {
                return Err("zset reply length".to_string());
            }
            let mut out = Vec::with_capacity(items.len() / 2);
            for chunk in items.chunks(2) {
                let member = as_string(&chunk[0]).ok_or_else(|| "zset member".to_string())?;
                let score = as_u64(&chunk[1]).ok_or_else(|| "zset score".to_string())?;
                out.push((member, score));
            }
            Ok(out)
        }
        Value::Map(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (member, score) in items {
                let member = as_string(&member).ok_or_else(|| "zset member".to_string())?;
                let score = as_u64(&score).ok_or_else(|| "zset score".to_string())?;
                out.push((member, score));
            }
            Ok(out)
        }
        _ => Err("zset reply".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::ExecClass;

    #[test]
    fn nil_and_empty_array_are_watch_conflicts() {
        assert!(matches!(
            classify_exec_value(Value::Nil, 2),
            ExecClass::RetryCycle { charge_watch: true }
        ));
        assert!(matches!(
            classify_exec_value(Value::Array(Vec::new()), 2),
            ExecClass::RetryCycle { charge_watch: true }
        ));
        assert!(matches!(lost_class(), ExecClass::UnknownOutcome));
    }

    #[test]
    fn wrong_length_and_scores_are_read() {
        let wrong = classify_exec_value(Value::Array(vec![Value::Okay, Value::Int(1)]), 3);
        assert!(matches!(wrong, ExecClass::StorageInconsistency));
        let ok = classify_exec_value(Value::Array(vec![Value::Okay, Value::Int(1)]), 2);
        assert!(matches!(ok, ExecClass::CheckPostconditions));
        let bad = classify_exec_value(Value::Okay, 1);
        assert!(matches!(bad, ExecClass::StorageInconsistency));
        assert_eq!(parse_score_text("15.0"), Some(15));
        assert_eq!(parse_score_text("15.5"), None);
        assert_eq!(as_u64(&Value::Int(-1)), None);
        assert_eq!(as_u64(&Value::Int(4)), Some(4));
        assert_eq!(as_u64(&Value::Double(8.0)), Some(8));
        assert_eq!(as_string(&Value::Okay).as_deref(), Some("OK"));
        assert_eq!(as_string(&Value::SimpleString("QUEUED".into())).as_deref(), Some("QUEUED"));
        let parsed = pairs(Value::Array(vec![
            Value::BulkString(b"state".to_vec()),
            Value::BulkString(b"ready".to_vec()),
        ]))
        .unwrap();
        assert_eq!(parsed, vec![("state".into(), "ready".into())]);
        assert!(pairs(Value::Array(vec![Value::Okay])).is_err());
        assert!(pairs(Value::Int(1)).is_err());
        assert!(scored_members(Value::Int(1)).is_err());
        assert_eq!(as_string(&Value::Int(7)).as_deref(), Some("7"));
        assert_eq!(as_string(&Value::BulkString(b"hash".to_vec())).as_deref(), Some("hash"));
        assert!(as_string(&Value::BulkString(vec![0xff])).is_none());
        assert_eq!(
            as_string(&Value::VerbatimString { format: redis::VerbatimFormat::Text, text: "ok".into() }).as_deref(),
            Some("ok")
        );
        assert_eq!(as_string(&Value::Attribute { data: Box::new(Value::Int(3)), attributes: Vec::new() }).as_deref(), Some("3"));
        assert!(as_string(&Value::Nil).is_none());
        assert_eq!(as_u64(&Value::BulkString(b"15.0".to_vec())), Some(15));
        assert_eq!(as_u64(&Value::SimpleString("9".into())), Some(9));
        assert_eq!(as_u64(&Value::Double(4.0)), Some(4));
        assert_eq!(as_u64(&Value::Double(4.5)), None);
        assert_eq!(as_u64(&Value::Double(f64::NAN)), None);
        assert_eq!(as_u64(&Value::Attribute { data: Box::new(Value::Int(6)), attributes: Vec::new() }), Some(6));
        assert!(pairs(Value::Nil).unwrap().is_empty());
        let mapped = pairs(Value::Map(vec![(
            Value::BulkString(b"state".to_vec()),
            Value::SimpleString("ready".into()),
        )]))
        .unwrap();
        assert_eq!(mapped, vec![("state".into(), "ready".into())]);
        let wrapped = pairs(Value::Attribute {
            data: Box::new(Value::Nil),
            attributes: Vec::new(),
        })
        .unwrap();
        assert!(wrapped.is_empty());
        assert!(scored_members(Value::Nil).unwrap().is_empty());
        assert!(scored_members(Value::Array(vec![Value::BulkString(b"only".to_vec())])).is_err());
        let wrapped_scores = scored_members(Value::Attribute {
            data: Box::new(Value::Array(vec![
                Value::BulkString(b"task".to_vec()),
                Value::Int(4),
            ])),
            attributes: Vec::new(),
        })
        .unwrap();
        assert_eq!(wrapped_scores, vec![("task".into(), 4)]);
        let scored = scored_members(Value::Map(vec![
            (Value::BulkString(b"task".to_vec()), Value::BulkString(b"10".to_vec())),
        ]))
        .unwrap();
        assert_eq!(scored, vec![("task".into(), 10)]);
    }
}
