//! JSON values and canonical encoding.
//!
//! Object keys are emitted in lexicographic order with no whitespace.
//! Number lexemes are preserved, so `1` and `1.0` stay distinct.
//!
//! Parsing rejects documents deeper than [`MAX_JSON_DEPTH`]. A top-level
//! value counts as one. The limit is checked before the recursive call, so a
//! deeply nested payload fails closed instead of growing the stack.

use std::fmt;

/// Maximum number of nested JSON values [`Json::parse`] will accept.
pub const MAX_JSON_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// Exact JSON number lexeme.
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonError {
    pub message: &'static str,
    pub position: usize,
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "json at {}: {}", self.position, self.message)
    }
}

impl std::error::Error for JsonError {}

impl Json {
    pub fn parse(text: &str) -> Result<Self, JsonError> {
        let mut parser = Parser { src: text, i: 0, depth: 0 };
        let value = parser.parse_value()?;
        parser.skip_ws();
        if parser.i != parser.src.len() {
            return Err(parser.err("trailing data"));
        }
        Ok(value)
    }

    pub fn canonical(&self) -> String {
        let mut out = String::new();
        self.write_canonical(&mut out);
        out
    }

    pub fn object(pairs: Vec<(&str, Json)>) -> Self {
        Json::Object(pairs.into_iter().map(|(key, value)| (key.to_string(), value)).collect())
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        let Json::Object(pairs) = self else {
            return None;
        };
        pairs.iter().find(|(name, _)| name == key).map(|(_, value)| value)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(text) => Some(text),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_object_pairs(&self) -> Option<&[(String, Json)]> {
        match self {
            Json::Object(pairs) => Some(pairs),
            _ => None,
        }
    }

    /// Non-negative integer lexeme with no leading zero, or `None`.
    pub fn as_u64(&self) -> Option<u64> {
        let Json::Number(lexeme) = self else {
            return None;
        };
        if lexeme.is_empty() || !lexeme.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        if lexeme.len() > 1 && lexeme.starts_with('0') {
            return None;
        }
        lexeme.parse().ok()
    }

    fn write_canonical(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(true) => out.push_str("true"),
            Json::Bool(false) => out.push_str("false"),
            Json::Number(lexeme) => out.push_str(lexeme),
            Json::String(text) => push_string(out, text),
            Json::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    item.write_canonical(out);
                }
                out.push(']');
            }
            Json::Object(pairs) => {
                let mut ordered: Vec<&(String, Json)> = pairs.iter().collect();
                ordered.sort_by(|left, right| left.0.cmp(&right.0));
                out.push('{');
                for (index, (key, value)) in ordered.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    push_string(out, key);
                    out.push(':');
                    value.write_canonical(out);
                }
                out.push('}');
            }
        }
    }
}

fn push_string(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

struct Parser<'a> {
    src: &'a str,
    i: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.src.as_bytes().get(self.i).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.i += 1;
        Some(byte)
    }

    fn err(&self, message: &'static str) -> JsonError {
        JsonError {
            message,
            position: self.i,
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.i += 1;
        }
    }

    fn parse_value(&mut self) -> Result<Json, JsonError> {
        self.skip_ws();
        if self.depth >= MAX_JSON_DEPTH {
            return Err(self.err("nesting too deep"));
        }
        self.depth += 1;
        let parsed = self.parse_contained();
        self.depth -= 1;
        parsed
    }

    fn parse_contained(&mut self) -> Result<Json, JsonError> {
        match self.peek() {
            Some(b'n') => self.parse_literal("null", Json::Null),
            Some(b't') => self.parse_literal("true", Json::Bool(true)),
            Some(b'f') => self.parse_literal("false", Json::Bool(false)),
            Some(b'"') => self.parse_string().map(Json::String),
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            Some(_) => Err(self.err("expected value")),
            None => Err(self.err("expected value")),
        }
    }

    fn parse_literal(&mut self, literal: &str, value: Json) -> Result<Json, JsonError> {
        if !self.src[self.i..].starts_with(literal) {
            return Err(self.err("expected value"));
        }
        self.i += literal.len();
        if matches!(
            self.peek(),
            Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
        ) {
            return Err(self.err("bad literal"));
        }
        Ok(value)
    }

    fn parse_object(&mut self) -> Result<Json, JsonError> {
        self.bump();
        self.skip_ws();
        let mut pairs = Vec::new();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(Json::Object(pairs));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(self.err("expected key"));
            }
            let key = self.parse_string()?;
            if pairs.iter().any(|(existing, _)| existing == &key) {
                return Err(self.err("duplicate key"));
            }
            self.skip_ws();
            if self.bump() != Some(b':') {
                return Err(self.err("expected colon"));
            }
            let value = self.parse_value()?;
            pairs.push((key, value));
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b'}') => return Ok(Json::Object(pairs)),
                _ => return Err(self.err("expected comma or end")),
            }
        }
    }

    fn parse_array(&mut self) -> Result<Json, JsonError> {
        self.bump();
        self.skip_ws();
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(Json::Array(items));
        }
        loop {
            items.push(self.parse_value()?);
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b']') => return Ok(Json::Array(items)),
                _ => return Err(self.err("expected comma or end")),
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, JsonError> {
        self.i += 1;
        let mut out = String::new();
        loop {
            let Some(ch) = self.src[self.i..].chars().next() else {
                return Err(self.err("unterminated string"));
            };
            self.i += ch.len_utf8();
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let Some(escaped) = self.src[self.i..].chars().next() else {
                        return Err(self.err("unterminated escape"));
                    };
                    self.i += escaped.len_utf8();
                    match escaped {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{0008}'),
                        'f' => out.push('\u{000c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let unit = self.hex4()?;
                            out.push(self.decode_unit(unit)?);
                        }
                        _ => return Err(self.err("bad escape")),
                    }
                }
                control if (control as u32) < 0x20 => {
                    return Err(self.err("raw control in string"));
                }
                other => out.push(other),
            }
        }
    }

    fn decode_unit(&mut self, unit: u32) -> Result<char, JsonError> {
        if (0xDC00..=0xDFFF).contains(&unit) {
            return Err(self.err("lone low surrogate"));
        }
        if (0xD800..=0xDBFF).contains(&unit) {
            if self.bump() != Some(b'\\') || self.bump() != Some(b'u') {
                return Err(self.err("expected low surrogate"));
            }
            let low = self.hex4()?;
            if !(0xDC00..=0xDFFF).contains(&low) {
                return Err(self.err("invalid low surrogate"));
            }
            let code = 0x10000 + (((unit - 0xD800) << 10) | (low - 0xDC00));
            return Ok(char::from_u32(code).expect("valid surrogate pair"));
        }
        Ok(char::from_u32(unit).expect("non-surrogate code point"))
    }

    fn hex4(&mut self) -> Result<u32, JsonError> {
        let mut value = 0u32;
        for _ in 0..4 {
            let Some(byte) = self.bump() else {
                return Err(self.err("short unicode escape"));
            };
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(self.err("bad hex")),
            };
            value = (value << 4) | u32::from(digit);
        }
        Ok(value)
    }

    fn parse_number(&mut self) -> Result<Json, JsonError> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        match self.bump() {
            Some(b'0') => {
                if matches!(self.peek(), Some(b'0'..=b'9')) {
                    return Err(self.err("leading zero"));
                }
            }
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.i += 1;
                }
            }
            _ => return Err(self.err("expected number")),
        }
        if self.peek() == Some(b'.') {
            self.i += 1;
            let mark = self.i;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.i += 1;
            }
            if self.i == mark {
                return Err(self.err("expected fraction"));
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.i += 1;
            }
            let mark = self.i;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.i += 1;
            }
            if self.i == mark {
                return Err(self.err("expected exponent"));
            }
        }
        Ok(Json::Number(self.src[start..self.i].to_string()))
    }
}
