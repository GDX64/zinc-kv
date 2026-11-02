use crate::Version;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    #[error("corrupt state: {0}")]
    Codec(#[from] serde_json::Error),
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
