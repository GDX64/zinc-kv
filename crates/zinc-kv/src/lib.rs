//! A small strongly consistent key-value store on top of `object_store`.
//!
//! Phase 1: the whole database is a single object, rewritten on every commit
//! with a conditional put.

mod error;
mod state;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{
    path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
};

pub use error::{CasError, Error, Result};
pub use state::{Entry, Version};

use state::{State, Stored};

const MAX_ATTEMPTS: usize = 64;

pub struct Db {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

/// State as read from the store, plus the tag needed to conditionally overwrite it.
struct Snapshot {
    state: State,
    /// `None` if the object does not exist yet.
    tag: Option<UpdateVersion>,
}

impl Db {
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<Db> {
        Ok(Db {
            store,
            path: prefix.join("state.json"),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Entry>> {
        let snap = self.load().await?;
        Ok(snap.state.entries.get(key).map(|s| Entry {
            value: s.value.clone(),
            version: s.version,
        }))
    }

    /// Unconditional write. Returns the new version.
    pub async fn put(&self, key: &str, value: Bytes) -> Result<Version> {
        self.commit(key, None, Some(value))
            .await
            .map_err(unwrap_other)
    }

    /// Unconditional delete.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.commit(key, None, None).await.map_err(unwrap_other)?;
        Ok(())
    }

    /// Compare-and-set. `expected: None` means the key must not exist.
    /// `new: None` deletes the key. Returns the new version.
    pub async fn cas(
        &self,
        key: &str,
        expected: Option<Version>,
        new: Option<Bytes>,
    ) -> std::result::Result<Version, CasError> {
        self.commit(key, Some(expected), new).await
    }

    /// Read-modify-write loop. `expected: None` skips the precondition check.
    async fn commit(
        &self,
        key: &str,
        expected: Option<Option<Version>>,
        new: Option<Bytes>,
    ) -> std::result::Result<Version, CasError> {
        for _ in 0..MAX_ATTEMPTS {
            let Snapshot { mut state, tag } = self.load().await?;

            let current = state.entries.get(key).map(|s| s.version);
            if let Some(expected) = expected {
                if expected != current {
                    return Err(CasError::Conflict { current });
                }
            }

            state.epoch += 1;
            let version = Version::from_epoch(state.epoch);
            match &new {
                Some(value) => {
                    state.entries.insert(
                        key.to_owned(),
                        Stored {
                            value: value.clone(),
                            version,
                        },
                    );
                }
                None => {
                    state.entries.remove(key);
                }
            }

            if self.store_if_unchanged(&state, tag).await? {
                return Ok(version);
            }
        }
        Err(Error::Contention.into())
    }

    async fn load(&self) -> Result<Snapshot> {
        match self.store.get(&self.path).await {
            Ok(res) => {
                let tag = UpdateVersion {
                    e_tag: res.meta.e_tag.clone(),
                    version: res.meta.version.clone(),
                };
                let bytes = res.bytes().await?;
                Ok(Snapshot {
                    state: serde_json::from_slice(&bytes)?,
                    tag: Some(tag),
                })
            }
            Err(StoreError::NotFound { .. }) => Ok(Snapshot {
                state: State::default(),
                tag: None,
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Returns `false` if someone else wrote since we loaded.
    async fn store_if_unchanged(&self, state: &State, tag: Option<UpdateVersion>) -> Result<bool> {
        let mode = match tag {
            None => PutMode::Create,
            Some(v) => PutMode::Update(v),
        };
        let payload = Bytes::from(serde_json::to_vec(state)?);
        match self
            .store
            .put_opts(&self.path, payload.into(), mode.into())
            .await
        {
            Ok(_) => Ok(true),
            Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// Unconditional ops never produce `Conflict`.
fn unwrap_other(e: CasError) -> Error {
    match e {
        CasError::Other(e) => e,
        CasError::Conflict { .. } => unreachable!("unconditional commit cannot conflict"),
    }
}
