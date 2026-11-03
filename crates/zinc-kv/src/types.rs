use bytes::Bytes;
use serde::{Deserialize, Serialize};

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
