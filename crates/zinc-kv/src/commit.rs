use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};

/// Format version written into every commit file.
pub(crate) const FORMAT: u64 = 1;

/// The contents of one commit file. Carries no number: the file name does.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Commit {
    pub v: u64,
    /// Unique per write attempt, so a writer can recognise its own commit when the
    /// store's answer to the `Create` was lost or wrong.
    pub id: String,
    pub ops: Vec<Op>,
}

impl Commit {
    pub fn new(id: String, ops: Vec<Op>) -> Self {
        Commit { v: FORMAT, id, ops }
    }

    /// Parses commit `number`, checking the format version before the body so that a
    /// newer format is reported as such rather than as corruption.
    pub fn decode(number: u64, bytes: &[u8]) -> Result<Commit> {
        #[derive(Deserialize)]
        struct Header {
            v: u64,
        }
        let corrupt = |source| Error::Corrupt { number, source };
        let header: Header = serde_json::from_slice(bytes).map_err(corrupt)?;
        if header.v != FORMAT {
            return Err(Error::UnsupportedFormat {
                number,
                version: header.v,
            });
        }
        serde_json::from_slice(bytes).map_err(corrupt)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Op {
    Put {
        key: String,
        #[serde(with = "b64")]
        value: Bytes,
    },
    /// A tombstone: hides any older entry for the key.
    Delete {
        key: String,
    },
}

impl Op {
    pub fn key(&self) -> &str {
        match self {
            Op::Put { key, .. } | Op::Delete { key } => key,
        }
    }
}

mod b64 {
    use super::*;
    use std::result::Result;

    pub fn serialize<S: Serializer>(value: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(value))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        let s = String::deserialize(d)?;
        STANDARD
            .decode(s)
            .map(Bytes::from)
            .map_err(serde::de::Error::custom)
    }
}
