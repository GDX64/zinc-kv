use std::collections::BTreeMap;

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Opaque per-key version: the epoch of the commit that last wrote the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Version(u64);

impl Version {
    pub(crate) fn from_epoch(epoch: u64) -> Self {
        Version(epoch)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub value: Bytes,
    pub version: Version,
}

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct State {
    pub epoch: u64,
    pub entries: BTreeMap<String, Stored>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Stored {
    #[serde(with = "b64")]
    pub value: Bytes,
    pub version: Version,
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
