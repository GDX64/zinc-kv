use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque per-key version: the number of the commit that last wrote the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Version(u64);

impl Version {
    pub(crate) fn from_commit(epoch: u64) -> Self {
        Version(epoch)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub value: Bytes,
    pub version: Version,
}

/// What a successful [`Tx::commit`](crate::Tx::commit) produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitResult {
    /// The new version of every key the commit wrote, e.g. a new leader's fencing epoch.
    /// `None` if the transaction wrote nothing, so no commit was made.
    pub epoch: Option<Version>,
}
