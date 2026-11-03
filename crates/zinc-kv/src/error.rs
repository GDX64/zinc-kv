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
    /// Concurrent writers kept winning: repeated lost races, or a log growing faster than
    /// the current head could be confirmed.
    #[error("gave up under contention from concurrent writers")]
    Contention,
    /// A key the transaction read was written by another commit after the transaction's
    /// snapshot. Nothing was written; retrying is up to the caller.
    #[error("key {key:?} changed after the transaction read it")]
    Conflict { key: String },
    /// The transaction read a key it had already written. The buffered value has no
    /// version until commit, and the caller already knows it.
    #[error("key {key:?} was read after being written in the same transaction")]
    ReadAfterWrite { key: String },
    /// The write may or may not have been applied: an attempt failed in a way that could
    /// still have written its commit (now or later), and that could not be settled.
    ///
    /// Reading the key afterwards is **not** conclusive: the request may still land after
    /// the read, and a landed write may already have been overwritten by another writer.
    /// Callers that need certainty should make their writes identifiable, e.g. put a
    /// unique token in the value: seeing the token proves the write landed; not seeing it
    /// proves nothing.
    #[error("write outcome unknown: {source}")]
    OutcomeUnknown { source: Box<Error> },
}

pub type Result<T> = std::result::Result<T, Error>;
