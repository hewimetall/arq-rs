#[path = "common/mod.rs"]
mod common;
use arq_core::*;
use common::*;

#[test]
fn canonical_json_preserves_number_lexemes_and_sorts_keys() {
    let value = Json::parse(" { \"b\" : [1, 1.0, true, false, null], \"a\" : \"x\" } ").unwrap();
    assert_eq!(value.canonical(), r#"{"a":"x","b":[1,1.0,true,false,null]}"#);
    assert_eq!(Json::parse("1").unwrap().canonical(), "1");
    assert_eq!(Json::parse("1.0").unwrap().canonical(), "1.0");
    assert_ne!(Json::parse("1").unwrap(), Json::parse("1.0").unwrap());
    let nested = Json::parse(r#"{"z":{"b":2,"a":1},"m":[{"k":"v"}]}"#).unwrap();
    assert_eq!(nested.canonical(), r#"{"m":[{"k":"v"}],"z":{"a":1,"b":2}}"#);
    assert_eq!(Json::parse("0").unwrap().as_u64(), Some(0));
    assert_eq!(Json::parse("10").unwrap().as_u64(), Some(10));
    assert_eq!(Json::parse("-0").unwrap().as_u64(), None);
    assert_eq!(Json::parse("1.0").unwrap().as_u64(), None);
    assert_eq!(Json::parse("01").ok().and_then(|v| v.as_u64()), None);
    assert!(Json::parse(&format!("{}", u128::from(u64::MAX) + 1)).unwrap().as_u64().is_none());
    assert_eq!(Json::Bool(true).as_u64(), None);
    assert_eq!(Json::Array(vec![]).get("a"), None);
    assert_eq!(Json::Number("1".into()).as_str(), None);
    assert_eq!(Json::Object(vec![]).as_array(), None);
    assert_eq!(Json::Array(vec![]).as_object_pairs(), None);
    assert_eq!(Json::parse(r#""hi""#).unwrap().get("no"), None);
    let text = Json::parse(r#""\"\\\/\b\f\n\r\t\u0041\uD83D\uDE00я""#).unwrap();
    assert_eq!(text.as_str().unwrap(), "\"\\/\u{0008}\u{000c}\n\r\tA😀я");
    assert_eq!(Json::String("\u{0001}".into()).canonical(), r#""\u0001""#);
    assert_eq!(Json::parse("[]").unwrap().canonical(), "[]");
    assert_eq!(Json::parse("{}").unwrap().canonical(), "{}");
    assert_eq!(header_bytes(&Json::parse(r#"{"item_id":"1001"}"#).unwrap()).unwrap(), "item_id".len() + "1001".len());
    assert!(header_bytes(&Json::Array(vec![])).is_err());
    assert!(header_bytes(&Json::parse(r#"{"a":1}"#).unwrap()).is_err());
}

#[test]
fn json_parser_rejects_bad_documents() {
    let cases = [
        ("", "expected value"),
        ("?", "expected value"),
        ("nullx", "bad literal"),
        ("truee", "bad literal"),
        ("1 2", "trailing data"),
        (r#"{"a":1,}"#, "expected key"),
        (r#"{"a":1, "a":2}"#, "duplicate key"),
        (r#"{"a" 1}"#, "expected colon"),
        (r#"{1}"#, "expected key"),
        (r#"{"a":1"#, "expected comma or end"),
        ("[1,]", "expected value"),
        ("[1 2]", "expected comma or end"),
        (r#""unterminated"#, "unterminated string"),
        (r#""\"#, "unterminated escape"),
        (r#""\q""#, "bad escape"),
        ("\"\n\"", "raw control in string"),
        (r#""\uDE00""#, "lone low surrogate"),
        (r#""\uD800""#, "expected low surrogate"),
        (r#""\uD800x""#, "expected low surrogate"),
        (r#""\uD800\uD800""#, "invalid low surrogate"),
        (r#""\u12"#, "short unicode escape"),
        (r#""\u12GG""#, "bad hex"),
        ("01", "leading zero"),
        ("-01", "leading zero"),
        ("-", "expected number"),
        ("1.", "expected fraction"),
        ("1e", "expected exponent"),
        ("1e+", "expected exponent"),
        ("1E-", "expected exponent"),
    ];
    for (text, message) in cases {
        let err = Json::parse(text).unwrap_err();
        assert_eq!(err.message, message, "{text}");
        assert!(err.to_string().contains(message));
        touch_error(&err);
    }
    assert_eq!(Json::parse("  -12.50e+1 ").unwrap().canonical(), "-12.50e+1");
    assert_eq!(Json::parse("0e0").unwrap().canonical(), "0e0");
}

#[test]
fn outcome_parser_accepts_only_the_four_proposals() {
    let ok = parse_outcome(&object(r#"{"outcome":"ok"}"#));
    assert_eq!(ok, ParsedOutcome::Proposal(OutcomeProposal::Ok));
    assert!(!ok.implies_retry());
    assert_eq!(ok.fault(), None);
    let skipped = parse_outcome(&object(r#"{"outcome":"skipped","delay_ms":0}"#));
    assert!(matches!(skipped, ParsedOutcome::Proposal(OutcomeProposal::Skipped)));
    let retry = parse_outcome(&object(r#"{"outcome":"retry","delay_ms":250}"#));
    assert_eq!(retry, ParsedOutcome::Proposal(OutcomeProposal::Retry { delay_ms: 250 }));
    assert!(retry.implies_retry());
    let dead = parse_outcome(&object(r#"{"outcome":"dead","reason":"boom","error":"nope"}"#));
    assert_eq!(
        dead,
        ParsedOutcome::Proposal(OutcomeProposal::Dead { reason: "boom".into(), error: Some("nope".into()) })
    );
    let dead_null = parse_outcome(&object(r#"{"outcome":"dead","reason":"boom","error":null}"#));
    assert_eq!(
        dead_null,
        ParsedOutcome::Proposal(OutcomeProposal::Dead { reason: "boom".into(), error: None })
    );
    let retry_zero = parse_outcome(&object(r#"{"outcome":"retry","delay_ms":0}"#));
    assert_eq!(retry_zero, ParsedOutcome::Proposal(OutcomeProposal::Retry { delay_ms: 0 }));
}

#[test]
fn outcome_parser_rejects_missing_unknown_shape_and_negative_delay() {
    let bad = [
        "null",
        "[]",
        "1",
        r#""ok""#,
        "{}",
        r#"{"outcome":1}"#,
        r#"{"outcome":"nope"}"#,
        r#"{"outcome":"ok","delay_ms":-1}"#,
        r#"{"outcome":"ok","delay_ms":1.5}"#,
        r#"{"outcome":"ok","delay_ms":"1"}"#,
        r#"{"outcome":"retry"}"#,
        r#"{"outcome":"dead"}"#,
        r#"{"outcome":"dead","reason":""}"#,
        r#"{"outcome":"dead","reason":1,"error":null}"#,
        r#"{"outcome":"dead","reason":"x"}"#,
        r#"{"outcome":"dead","reason":"x","error":1}"#,
        r#"{"outcome":"ok","reason":""}"#,
        r#"{"outcome":"ok","error":1}"#,
        r#"{"outcome":"ok","reason":1}"#,
    ];
    for text in bad {
        let parsed = parse_outcome(&Json::parse(text).unwrap());
        assert_eq!(parsed.fault(), Some(OutcomeFault::InvalidOutcome), "{text}");
        assert_eq!(parsed.fault().unwrap().reason(), "invalid_outcome");
        assert!(!parsed.fault().unwrap().implies_retry());
        assert!(!parsed.implies_retry());
    }
    let raised = dispatch_exception();
    assert_eq!(raised.fault(), Some(OutcomeFault::DispatchError));
    assert_eq!(raised.fault().unwrap().reason(), "dispatch_error");
    assert!(!raised.implies_retry());
}

#[test]
fn utf8_truncation_stops_on_a_code_point() {
    assert_eq!(truncate_utf8("я", 1), "");
    assert_eq!(truncate_utf8("я", 2), "я");
    assert_eq!(truncate_utf8("aяb", 3), "aя");
    assert_eq!(truncate_utf8("abc", 3), "abc");
    assert_eq!(truncate_utf8("abc", 0), "");
}
