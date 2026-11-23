//! Canonical lowercase UUIDs. Time and randomness are arguments.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// A UUID in the canonical 8-4-4-4-12 lowercase form, version 1–8, RFC 4122 variant.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CanonicalUuid {
    bytes: [u8; 16],
}

/// `task_id`, `message_id`, `operation_id`, `cycle_id`, or `delivery_token` was not canonical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidIdentifier {
    pub text: String,
}

impl fmt::Display for InvalidIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "identifier is not a canonical lowercase UUID: {}", self.text)
    }
}

impl std::error::Error for InvalidIdentifier {}

/// Fills a buffer from a caller-supplied generator. The core never reads a global RNG.
pub trait FillBytes {
    fn fill_bytes(&mut self, dest: &mut [u8]);
}

impl CanonicalUuid {
    pub fn parse(text: &str) -> Result<Self, InvalidIdentifier> {
        let bytes = text.as_bytes();
        if bytes.len() != 36 {
            return Err(InvalidIdentifier { text: text.to_string() });
        }
        let mut out = [0u8; 16];
        let mut nibble = 0usize;
        for (index, &byte) in bytes.iter().enumerate() {
            if matches!(index, 8 | 13 | 18 | 23) {
                if byte != b'-' {
                    return Err(InvalidIdentifier { text: text.to_string() });
                }
                continue;
            }
            let value = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => return Err(InvalidIdentifier { text: text.to_string() }),
            };
            if nibble % 2 == 0 {
                out[nibble / 2] = value << 4;
            } else {
                out[nibble / 2] |= value;
            }
            nibble += 1;
        }
        let version = out[6] >> 4;
        if !(1..=8).contains(&version) {
            return Err(InvalidIdentifier { text: text.to_string() });
        }
        if out[8] & 0b1100_0000 != 0b1000_0000 {
            return Err(InvalidIdentifier { text: text.to_string() });
        }
        Ok(Self { bytes: out })
    }

    /// UUID version 4. `bytes` supplies the random material; version and variant bits are set here.
    pub fn from_random_v4(mut bytes: [u8; 16]) -> Self {
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Self { bytes }
    }

    pub fn generate_v4(rng: &mut impl FillBytes) -> Self {
        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        Self::from_random_v4(bytes)
    }

    pub fn as_bytes(self) -> [u8; 16] {
        self.bytes
    }

    pub fn version(self) -> u8 {
        self.bytes[6] >> 4
    }
}

impl fmt::Display for CanonicalUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut text = [0u8; 36];
        let mut index = 0;
        for (byte_index, byte) in self.bytes.iter().enumerate() {
            if matches!(byte_index, 4 | 6 | 8 | 10) {
                text[index] = b'-';
                index += 1;
            }
            text[index] = HEX[(*byte >> 4) as usize];
            text[index + 1] = HEX[(*byte & 0x0f) as usize];
            index += 2;
        }
        let rendered = std::str::from_utf8(&text).map_err(|_| fmt::Error)?;
        f.write_str(rendered)
    }
}

impl fmt::Debug for CanonicalUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CanonicalUuid({self})")
    }
}

macro_rules! id_newtype {
    ($name:ident) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(CanonicalUuid);

        impl $name {
            pub fn parse(text: &str) -> Result<Self, InvalidIdentifier> {
                CanonicalUuid::parse(text).map(Self)
            }

            pub fn generate_v4(rng: &mut impl FillBytes) -> Self {
                Self(CanonicalUuid::generate_v4(rng))
            }

            pub fn from_uuid(id: CanonicalUuid) -> Self {
                Self(id)
            }

            pub fn as_uuid(self) -> CanonicalUuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = String::deserialize(deserializer)?;
                $name::parse(&text).map_err(serde::de::Error::custom)
            }
        }
    };
}

id_newtype!(TaskId);
id_newtype!(MessageId);
id_newtype!(OperationId);
id_newtype!(CycleId);
id_newtype!(DeliveryToken);

/// First-version identity: `task_id` and `message_id` are the same UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageIdentity {
    pub message_id: MessageId,
    pub task_id: TaskId,
}

impl MessageIdentity {
    pub fn new(id: CanonicalUuid) -> Self {
        Self {
            message_id: MessageId::from_uuid(id),
            task_id: TaskId::from_uuid(id),
        }
    }

    pub fn generate(rng: &mut impl FillBytes) -> Self {
        Self::new(CanonicalUuid::generate_v4(rng))
    }

    pub fn same_value(self) -> bool {
        self.message_id.as_uuid() == self.task_id.as_uuid()
    }
}

/// Reads bytes from a slice, then zeroes. Tests and callers own the slice.
#[derive(Debug, Clone)]
pub struct SliceBytes<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceBytes<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
}

impl FillBytes for SliceBytes<'_> {
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for slot in dest {
            *slot = if self.pos < self.bytes.len() {
                let value = self.bytes[self.pos];
                self.pos += 1;
                value
            } else {
                0
            };
        }
    }
}
