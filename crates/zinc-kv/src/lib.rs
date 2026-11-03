//! A small strongly consistent key-value store on top of `object_store`.
//!
//! Layout under the prefix: `log/<n>.json`, one immutable commit per file, where `n` is a
//! zero-padded sequence number. A commit wins by creating file `head + 1` with
//! `PutMode::Create`; only one writer can create a given name. The number is the commit
//! order and the version of every key the commit writes.

mod commit;
mod error;
mod types;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode};

pub use error::{CasError, Error, Result};
pub use types::{Entry, Version};

use commit::{Commit, Op};

const MAX_ATTEMPTS: usize = 64;

pub struct Db {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

/// What the newest commit mentioning a key says about it.
struct Found {
    version: Version,
    /// `None` for a tombstone.
    value: Option<Bytes>,
}

impl Found {
    fn live_version(&self) -> Option<Version> {
        self.value.as_ref().map(|_| self.version)
    }
}

impl Db {
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<Db> {
        Ok(Db { store, prefix })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Entry>> {
        let commits = self.list_commits().await?;
        let found = self.find(&commits, key).await?;
        Ok(found.and_then(|f| {
            let version = f.version;
            f.value.map(|value| Entry { value, version })
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

    /// `expected: None` skips the precondition check.
    async fn commit(
        &self,
        key: &str,
        expected: Option<Option<Version>>,
        new: Option<Bytes>,
    ) -> std::result::Result<Version, CasError> {
        let op = match new {
            Some(value) => Op::Put {
                key: key.to_owned(),
                value,
            },
            None => Op::Delete {
                key: key.to_owned(),
            },
        };
        let payload = Bytes::from(serde_json::to_vec(&Commit { ops: vec![op] }).map_err(Error::from)?);

        // Commits up to `checked` are already reflected in `current`.
        let mut checked = 0;
        let mut current = None;
        for _ in 0..MAX_ATTEMPTS {
            let commits = self.list_commits().await?;
            let head = commits.last().copied().unwrap_or(0);

            if let Some(expected) = expected {
                let unseen: Vec<u64> = commits.iter().copied().filter(|&n| n > checked).collect();
                if let Some(found) = self.find(&unseen, key).await? {
                    current = found.live_version();
                }
                checked = head;
                if expected != current {
                    return Err(CasError::Conflict { current });
                }
            }

            if self.create_commit(head + 1, payload.clone()).await? {
                return Ok(Version::from_commit(head + 1));
            }
            // Lost the race for `head + 1`: list again and retry.
        }
        Err(Error::Contention.into())
    }

    /// Newest entry for `key` among the commits `numbers` (ascending).
    async fn find(&self, numbers: &[u64], key: &str) -> Result<Option<Found>> {
        for &n in numbers.iter().rev() {
            let bytes = self.store.get(&self.commit_path(n)).await?.bytes().await?;
            let commit: Commit = serde_json::from_slice(&bytes)?;
            // A commit may mention a key more than once; the last op wins.
            if let Some(op) = commit.ops.into_iter().rev().find(|op| op.key() == key) {
                return Ok(Some(Found {
                    version: Version::from_commit(n),
                    value: match op {
                        Op::Put { value, .. } => Some(value),
                        Op::Delete { .. } => None,
                    },
                }));
            }
        }
        Ok(None)
    }

    /// Commit numbers present in the log, ascending.
    async fn list_commits(&self) -> Result<Vec<u64>> {
        let dir = self.prefix.clone().join("log");
        let listing = self.store.list_with_delimiter(Some(&dir)).await?;
        let mut numbers: Vec<u64> = listing
            .objects
            .iter()
            .filter_map(|o| o.location.filename()?.strip_suffix(".json")?.parse().ok())
            .collect();
        numbers.sort_unstable();
        Ok(numbers)
    }

    fn commit_path(&self, number: u64) -> Path {
        self.prefix
            .clone()
            .join("log")
            .join(format!("{number:020}.json"))
    }

    /// Returns `false` if another writer already created this commit number.
    async fn create_commit(&self, number: u64, payload: Bytes) -> Result<bool> {
        match self
            .store
            .put_opts(
                &self.commit_path(number),
                payload.into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(StoreError::AlreadyExists { .. }) => Ok(false),
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
