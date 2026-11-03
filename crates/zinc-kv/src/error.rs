use crate::Version;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    #[error("failed to encode commit: {0}")]
    Encode(#[from] serde_json::Error),
    /// The log is not a contiguous run of commits from 1: a commit file is missing.
    #[error("commit {number} is missing from the log")]
    MissingCommit { number: u64 },
    #[error("commit {number} is corrupt: {source}")]
    Corrupt {
        number: u64,
        source: serde_json::Error,
    },
    #[error("commit {number} has unsupported format version {version}")]
    UnsupportedFormat { number: u64, version: u64 },
    #[error("gave up after repeated write conflicts")]
    Contention,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum CasError {
    /// The key's current version did not match the expected one.
    #[error("version mismatch, current version is {current:?}")]
    Conflict { current: Option<Version> },
    #[error(transparent)]
    Other(#[from] Error),
}
