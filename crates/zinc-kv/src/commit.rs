use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The contents of one commit file. Carries no number: the file name does.
#[derive(Serialize, Deserialize)]
pub(crate) struct Commit {
    pub ops: Vec<Op>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
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
