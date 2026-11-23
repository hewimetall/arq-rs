#[path = "common/mod.rs"]
mod common;
use common::*;
use arq_core::*;
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

#[test]
fn defaults_match_the_table() {
    let config = Config::default();
    assert_eq!(config.poll_interval_ms, 500);
    assert_eq!(config.inflight_scan_interval_ms, 500);
    assert_eq!(config.lease_ms, 15_000);
    assert_eq!(config.renew_interval_ms, 5_000);
    assert_eq!(config.renew_confirm_ms, 2_000);
    assert_eq!(config.ping_interval_ms, 2_000);
    assert_eq!(config.ping_timeout_ms, 3_000);
    assert_eq!(config.lifecycle_interval_ms, 200);
    assert_eq!(config.ping_budget_secs, 1_860);
    assert!(!config.ping_budget_explicit);
    assert_eq!(config.shutdown_timeout_ms, 30_000);
    assert_eq!(config.cancel_grace_ms, 5_000);
    assert_eq!(config.max_deliveries, 5);
    assert_eq!(config.max_tasks_per_child, 500);
    assert_eq!(config.memory_soft_ratio, 0.80);
    assert_eq!(config.memory_hard_ratio, 0.95);
    assert_eq!(config.child_memory_soft, None);
    assert_eq!(config.child_memory_hard, None);
    assert_eq!(config.heartbeat_secs, 15);
    assert_eq!(config.missed_heartbeats, 4);
    assert_eq!(config.maintenance_interval_ms, 60_000);
    assert_eq!(config.maintenance_batch, 100);
    assert_eq!(config.watch_retries, 8);
    assert_eq!(config.done_ttl_secs, 86_400);
    assert_eq!(config.dead_ttl_secs, 604_800);
    assert_eq!(config.receipt_ttl_secs, 691_200);
    assert_eq!(config.max_ready, 100_000);
    assert_eq!(config.max_message_bytes, 1_048_576);
    assert_eq!(config.max_headers_bytes, 65_536);
    assert_eq!(config.max_error_bytes, 4_096);
    assert_eq!(config.effective_ping_budget(secs(1_800)), secs(1_860));
    assert_eq!(config.effective_ping_budget(secs(10)), secs(70));
    assert_eq!(CONFIG_EXIT_CODE, 3);
    assert_eq!(ConfigError::EXIT_CODE, 3);
    config.check_payload(10, 10).unwrap();
}

#[test]
fn every_known_variable_overrides_and_last_write_wins() {
    let pairs = [
        ("ARQ_POLL_INTERVAL_MS", "11"),
        ("ARQ_POLL_INTERVAL_MS", " 12 "),
        ("ARQ_INFLIGHT_SCAN_INTERVAL_MS", "13"),
        ("ARQ_LEASE_MS", "100"),
        ("ARQ_RENEW_INTERVAL_MS", "40"),
        ("ARQ_RENEW_CONFIRM_MS", "20"),
        ("ARQ_PING_INTERVAL_MS", "21"),
        ("ARQ_PING_TIMEOUT_MS", "22"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "23"),
        ("ARQ_PING_BUDGET_SECS", "10"),
        ("ARQ_SHUTDOWN_TIMEOUT_MS", "24"),
        ("ARQ_CANCEL_GRACE_MS", "25"),
        ("ARQ_MAX_DELIVERIES", "26"),
        ("ARQ_MAX_TASKS_PER_CHILD", "27"),
        ("ARQ_MEMORY_SOFT_RATIO", "0.2"),
        ("ARQ_MEMORY_HARD_RATIO", "0.4"),
        ("ARQ_CHILD_MEMORY_SOFT", "100"),
        ("ARQ_CHILD_MEMORY_HARD", "200"),
        ("ARQ_HEARTBEAT_SECS", "28"),
        ("ARQ_MISSED_HEARTBEATS", "29"),
        ("ARQ_MAINTENANCE_INTERVAL_MS", "30"),
        ("ARQ_MAINTENANCE_BATCH", "31"),
        ("ARQ_WATCH_RETRIES", "32"),
        ("ARQ_DONE_TTL_SECS", "33"),
        ("ARQ_DEAD_TTL_SECS", "34"),
        ("ARQ_RECEIPT_TTL_SECS", "35"),
        ("ARQ_MAX_READY", "36"),
        ("ARQ_MAX_MESSAGE_BYTES", "37"),
        ("ARQ_MAX_HEADERS_BYTES", "38"),
        ("ARQ_MAX_ERROR_BYTES", "39"),
        ("IGNORED", "nope"),
    ];
    let config = Config::parse(pairs).unwrap();
    assert_eq!(config.poll_interval_ms, 12);
    assert_eq!(config.inflight_scan_interval_ms, 13);
    assert_eq!(config.lease_ms, 100);
    assert_eq!(config.renew_interval_ms, 40);
    assert_eq!(config.renew_confirm_ms, 20);
    assert_eq!(config.ping_interval_ms, 21);
    assert_eq!(config.ping_timeout_ms, 22);
    assert_eq!(config.lifecycle_interval_ms, 23);
    assert!(config.ping_budget_explicit);
    assert_eq!(config.effective_ping_budget(secs(1_800)), Duration::from_secs(10));
    assert_eq!(config.shutdown_timeout_ms, 24);
    assert_eq!(config.cancel_grace_ms, 25);
    assert_eq!(config.max_deliveries, 26);
    assert_eq!(config.max_tasks_per_child, 27);
    assert_eq!(config.memory_soft_ratio, 0.2);
    assert_eq!(config.memory_hard_ratio, 0.4);
    assert_eq!(config.child_memory_soft, Some(100));
    assert_eq!(config.child_memory_hard, Some(200));
    assert_eq!(config.heartbeat_secs, 28);
    assert_eq!(config.missed_heartbeats, 29);
    assert_eq!(config.maintenance_interval_ms, 30);
    assert_eq!(config.maintenance_batch, 31);
    assert_eq!(config.watch_retries, 32);
    assert_eq!(config.done_ttl_secs, 33);
    assert_eq!(config.dead_ttl_secs, 34);
    assert_eq!(config.receipt_ttl_secs, 35);
    assert_eq!(config.max_ready, 36);
    assert_eq!(config.max_message_bytes, 37);
    assert_eq!(config.max_headers_bytes, 38);
    assert_eq!(config.max_error_bytes, 39);
}

#[test]
fn child_memory_may_set_only_one_side() {
    let soft = cfg_with(&[("ARQ_CHILD_MEMORY_SOFT", "10")]);
    assert_eq!(soft.child_memory_soft, Some(10));
    assert_eq!(soft.child_memory_hard, None);
    let hard = cfg_with(&[("ARQ_CHILD_MEMORY_HARD", "10")]);
    assert_eq!(hard.child_memory_hard, Some(10));
}

#[test]
fn rejects_simple_q_before_unknown_arq() {
    let err = Config::parse([("ARQ_NOT_A_KEY", "1"), ("SIMPLE_Q_URL", "")]).unwrap_err();
    match err {
        ConfigError::SimpleQSet { name } => assert_eq!(name, "SIMPLE_Q_URL"),
        other => panic!("{other:?}"),
    }
    let only = Config::parse([("SIMPLE_Q_REDIS", "redis://redis:6379/0")]).unwrap_err();
    assert!(only.to_string().contains("SIMPLE_Q_REDIS"));
    touch_error(&only);
}

#[test]
fn rejects_unknown_arq_and_bad_integers() {
    let unknown = Config::parse([("ARQ_TYPO", "1")]).unwrap_err();
    assert!(matches!(unknown, ConfigError::Unknown { .. }));
    assert!(unknown.to_string().contains("ARQ_TYPO"));
    for value in ["0", "01", "-1", "1.5", "+", "", "  ", "12a"] {
        let err = Config::parse([("ARQ_POLL_INTERVAL_MS", value)]).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { .. }), "{value}");
        assert!(err.to_string().contains("ARQ_POLL_INTERVAL_MS"));
    }
    let huge = "1".repeat(40);
    let err = Config::parse([("ARQ_POLL_INTERVAL_MS", huge.as_str())]).unwrap_err();
    assert!(err.to_string().contains("out of range"));
    let child = Config::parse([("ARQ_CHILD_MEMORY_SOFT", "0")]).unwrap_err();
    assert!(matches!(child, ConfigError::Invalid { .. }));
}

#[test]
fn rejects_bad_ratios_and_inequalities() {
    for value in ["", "nan", "NaN", "inf", "+inf", "-inf", "INF", "abc", "1e9999"] {
        let err = Config::parse([("ARQ_MEMORY_SOFT_RATIO", value)]).unwrap_err();
        assert!(err.to_string().contains("finite"), "{value} {err}");
    }
    fn fails(pairs: &[(&str, &str)], needle: &str) {
        let err = Config::parse(pairs.iter().copied()).unwrap_err();
        assert!(err.to_string().contains(needle), "{err}");
        touch_error(&err);
    }
    fails(&[("ARQ_RENEW_INTERVAL_MS", "8"), ("ARQ_RENEW_CONFIRM_MS", "7"), ("ARQ_LEASE_MS", "15")], "renew");
    fails(&[("ARQ_RENEW_INTERVAL_MS", "18446744073709551615"), ("ARQ_RENEW_CONFIRM_MS", "1"), ("ARQ_LEASE_MS", "1")], "renew");
    fails(&[("ARQ_DONE_TTL_SECS", "10"), ("ARQ_DEAD_TTL_SECS", "9"), ("ARQ_RECEIPT_TTL_SECS", "9")], "receipt_ttl");
    fails(&[("ARQ_DONE_TTL_SECS", "11"), ("ARQ_DEAD_TTL_SECS", "10"), ("ARQ_RECEIPT_TTL_SECS", "12")], "receipt_ttl");
    fails(&[("ARQ_MEMORY_SOFT_RATIO", "0"), ("ARQ_MEMORY_HARD_RATIO", "0.5")], "memory_soft");
    fails(&[("ARQ_MEMORY_SOFT_RATIO", "0.5"), ("ARQ_MEMORY_HARD_RATIO", "1")], "memory");
    fails(&[("ARQ_MEMORY_SOFT_RATIO", "0.9"), ("ARQ_MEMORY_HARD_RATIO", "0.8")], "memory");
    fails(&[("ARQ_MEMORY_SOFT_RATIO", "0.5"), ("ARQ_MEMORY_HARD_RATIO", "0.5")], "memory");
    fails(&[("ARQ_CHILD_MEMORY_SOFT", "5"), ("ARQ_CHILD_MEMORY_HARD", "5")], "child_memory");
    fails(&[("ARQ_CHILD_MEMORY_SOFT", "6"), ("ARQ_CHILD_MEMORY_HARD", "5")], "child_memory");
    let equal_ttl = cfg_with(&[
        ("ARQ_DONE_TTL_SECS", "10"),
        ("ARQ_DEAD_TTL_SECS", "10"),
        ("ARQ_RECEIPT_TTL_SECS", "10"),
        ("ARQ_LEASE_MS", "10"),
        ("ARQ_RENEW_INTERVAL_MS", "4"),
        ("ARQ_RENEW_CONFIRM_MS", "5"),
    ]);
    assert_eq!(equal_ttl.done_ttl_secs, 10);
    let payload = cfg_with(&[("ARQ_MAX_MESSAGE_BYTES", "4"), ("ARQ_MAX_HEADERS_BYTES", "3")]);
    assert!(payload.check_payload(5, 1).unwrap_err().to_string().contains("kwargs"));
    assert!(payload.check_payload(4, 4).unwrap_err().to_string().contains("headers"));
}

#[test]
fn from_env_loads_without_reading_files() {
    let loaded = Config::from_env().expect("this process has no ARQ_* or SIMPLE_Q_* variables");
    assert_eq!(loaded, Config::default());
}

struct Collect(Option<String>);

impl serde::Serializer for Collect {
    type Ok = ();
    type Error = serde::de::value::Error;
    type SerializeSeq = serde::ser::Impossible<(), Self::Error>;
    type SerializeTuple = serde::ser::Impossible<(), Self::Error>;
    type SerializeTupleStruct = serde::ser::Impossible<(), Self::Error>;
    type SerializeTupleVariant = serde::ser::Impossible<(), Self::Error>;
    type SerializeMap = serde::ser::Impossible<(), Self::Error>;
    type SerializeStruct = serde::ser::Impossible<(), Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<(), Self::Error>;

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        assert_eq!(value.len(), 36);
        assert!(value.chars().all(|ch| ch == '-' || ch.is_ascii_hexdigit()));
        assert_eq!(value, value.to_ascii_lowercase());
        Ok(())
    }

    fn serialize_bool(self, _: bool) -> Result<(), Self::Error> { panic!("bool") }
    fn serialize_i8(self, _: i8) -> Result<(), Self::Error> { panic!("i8") }
    fn serialize_i16(self, _: i16) -> Result<(), Self::Error> { panic!("i16") }
    fn serialize_i32(self, _: i32) -> Result<(), Self::Error> { panic!("i32") }
    fn serialize_i64(self, _: i64) -> Result<(), Self::Error> { panic!("i64") }
    fn serialize_i128(self, _: i128) -> Result<(), Self::Error> { panic!("i128") }
    fn serialize_u8(self, _: u8) -> Result<(), Self::Error> { panic!("u8") }
    fn serialize_u16(self, _: u16) -> Result<(), Self::Error> { panic!("u16") }
    fn serialize_u32(self, _: u32) -> Result<(), Self::Error> { panic!("u32") }
    fn serialize_u64(self, _: u64) -> Result<(), Self::Error> { panic!("u64") }
    fn serialize_u128(self, _: u128) -> Result<(), Self::Error> { panic!("u128") }
    fn serialize_f32(self, _: f32) -> Result<(), Self::Error> { panic!("f32") }
    fn serialize_f64(self, _: f64) -> Result<(), Self::Error> { panic!("f64") }
    fn serialize_char(self, _: char) -> Result<(), Self::Error> { panic!("char") }
    fn serialize_bytes(self, _: &[u8]) -> Result<(), Self::Error> { panic!("bytes") }
    fn serialize_none(self) -> Result<(), Self::Error> { panic!("none") }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<(), Self::Error> { panic!("some") }
    fn serialize_unit(self) -> Result<(), Self::Error> { panic!("unit") }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Self::Error> { panic!("us") }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Self::Error> { panic!("uv") }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> Result<(), Self::Error> { panic!("ns") }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, _: &T) -> Result<(), Self::Error> { panic!("nv") }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> { panic!("seq") }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> { panic!("tuple") }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Self::Error> { panic!("ts") }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeTupleVariant, Self::Error> { panic!("tv") }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> { panic!("map") }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Self::Error> { panic!("st") }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeStructVariant, Self::Error> { panic!("sv") }
}

fn roundtrip_id<T>(text: &str)
where
    T: for<'de> Deserialize<'de> + Serialize + fmt::Display + fmt::Debug + Copy,
{
    let de: serde::de::value::StrDeserializer<'_, serde::de::value::Error> = text.into_deserializer();
    let id = T::deserialize(de).unwrap();
    assert_eq!(id.to_string(), text);
    let _ = format!("{id:?}");
    id.serialize(Collect(None)).unwrap();
    let bad: serde::de::value::StrDeserializer<'_, serde::de::value::Error> = "NOPE".into_deserializer();
    assert!(T::deserialize(bad).is_err());
}

#[test]
fn identifiers_are_canonical_lowercase_uuids() {
    let parsed = CanonicalUuid::parse(TASK).unwrap();
    assert_eq!(parsed.to_string(), TASK);
    assert_eq!(format!("{parsed:?}"), format!("CanonicalUuid({TASK})"));
    assert_eq!(parsed.version(), 4);
    assert_eq!(parsed.as_bytes()[6] >> 4, 4);
    for version in 1..=8u8 {
        let text = format!("00000000-0000-{version}000-8000-000000000000");
        let id = CanonicalUuid::parse(&text).unwrap();
        assert_eq!(id.version(), version);
    }
    let variant = CanonicalUuid::parse("00000000-0000-4000-b000-000000000000").unwrap();
    assert_eq!(variant.to_string(), "00000000-0000-4000-b000-000000000000");
    for bad in [
        "3F1C0B2A-7E4D-4C1A-9B2E-6D5A4C3B2A10",
        "3f1c0b2a7e4d4c1a9b2e6d5a4c3b2a10",
        "3f1c0b2a-7e4d-0c1a-9b2e-6d5a4c3b2a10",
        "3f1c0b2a-7e4d-4c1a-0b2e-6d5a4c3b2a10",
        "3f1c0b2a-7e4d-4c1a-cb2e-6d5a4c3b2a10",
        "3f1c0b2a-7e4d-4c1a-9b2e-6d5a4c3b2a1",
        "3f1c0b2a-7e4d-4c1a-9b2e-6d5a4c3b2a100",
        "3f1c0b2a-7e4d-4c1a-9b2g-6d5a4c3b2a10",
        "3f1c0b2a+7e4d-4c1a-9b2e-6d5a4c3b2a10",
        "",
    ] {
        let err = CanonicalUuid::parse(bad).unwrap_err();
        assert!(err.to_string().contains(bad));
        touch_error(&err);
    }
    let mut rng = SliceBytes::new(&[0xff; 8]);
    let generated = CanonicalUuid::generate_v4(&mut rng);
    assert_eq!(generated.version(), 4);
    assert_eq!(generated.as_bytes()[8] & 0b1100_0000, 0b1000_0000);
    assert_eq!(generated.to_string().chars().all(|ch| ch == '-' || ch.is_ascii_lowercase() || ch.is_ascii_digit()), true);
    let forced = CanonicalUuid::from_random_v4([0xff; 16]);
    assert_eq!(forced.version(), 4);
    roundtrip_id::<TaskId>(TASK);
    roundtrip_id::<MessageId>(TASK);
    roundtrip_id::<OperationId>(OP);
    roundtrip_id::<CycleId>(CYCLE);
    roundtrip_id::<DeliveryToken>(TOKEN);
    let mut rng = SliceBytes::new(&[0x11; 32]);
    let made = MessageIdentity::generate(&mut rng);
    assert!(made.same_value());
    assert_eq!(TaskId::generate_v4(&mut rng).as_uuid().version(), 4);
    let mixed = MessageIdentity {
        message_id: message(),
        task_id: other(),
    };
    assert!(!mixed.same_value());
    assert_ne!(format!("{mixed:?}"), "");
}
