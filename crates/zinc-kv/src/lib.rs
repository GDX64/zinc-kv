//! A small strongly consistent key-value store on top of `object_store`.
//!
//! Layout under the prefix: `log/<n>.json`, one immutable commit per file, where `n` is a
//! zero-padded sequence number. A commit wins by creating file `head + 1` with
//! `PutMode::Create`; only one writer can create a given name. The number is the commit
//! order and the version of every key the commit writes.
//!
//! The log must be the contiguous run `1..=head`. Anything else (a hole, an unreadable
//! commit, an unknown format) is reported as an error rather than read around.

mod commit;
mod error;
mod types;

use std::{ops::RangeInclusive, sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::{path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode};
use rand::Rng;

pub use error::{CasError, Error, Result};
pub use types::{Entry, Version};

use commit::{Commit, Op};

const MAX_ATTEMPTS: u32 = 64;
const BACKOFF_BASE: Duration = Duration::from_millis(1);
const BACKOFF_CAP: Duration = Duration::from_millis(32);

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
        let head = self.head().await?;
        let found = self.find(1..=head, key).await?;
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

    /// Unconditional delete. Deleting a missing key still records a tombstone.
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
    ///
    /// A failed `Create`, even one reporting `AlreadyExists`, may still have written the
    /// commit (a lost response, a client retry), or may write it later (a request still in
    /// flight). So every failed number is kept as *uncertain* and retried like a lost race;
    /// once a later listing shows it taken, its `id` says whether the write was ours. A request can only ever land at the
    /// number it named, so this finds every write of ours that landed. If the attempts
    /// run out, the last store error is returned and the outcome is unknown only if a
    /// request may still land later (never on `InMemory` or `LocalFileSystem`).
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
        let id = format!("{:032x}", rand::random::<u128>());
        let payload = Bytes::from(
            serde_json::to_vec(&Commit::new(id.clone(), vec![op])).map_err(Error::from)?,
        );

        // Commits up to `checked` are already reflected in `current`.
        let mut checked = 0;
        let mut current = None;
        // Numbers whose `Create` failed and that were still free at the last listing.
        let mut uncertain: Vec<u64> = Vec::new();
        let mut last_error: Option<StoreError> = None;
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                backoff(attempt).await;
            }
            let head = self.head().await?;

            // A taken number is final: ours or another writer's. Checked before the
            // precondition, so that our own commit is never mistaken for a conflicting one.
            for &number in uncertain.iter().filter(|&&n| n <= head) {
                if self.is_ours(number, &id).await? {
                    return Ok(Version::from_commit(number));
                }
            }
            uncertain.retain(|&n| n > head);

            if let Some(expected) = expected {
                if let Some(found) = self.find(checked + 1..=head, key).await? {
                    current = found.live_version();
                }
                checked = head;
                if expected != current {
                    return Err(CasError::Conflict { current });
                }
            }

            let number = head + 1;
            match self.create_commit(number, payload.clone()).await {
                Ok(()) => return Ok(Version::from_commit(number)),
                // Usually another writer won `number` (`AlreadyExists`, or on Windows'
                // `LocalFileSystem` sometimes a plain error). But the failure may hide a
                // write of ours: a lost response, a client retry of a request that had
                // landed, or a request still in flight. The next pass settles it.
                Err(e) => {
                    if !uncertain.contains(&number) {
                        uncertain.push(number);
                    }
                    last_error = match e {
                        StoreError::AlreadyExists { .. } => None,
                        e => Some(e),
                    };
                }
            }
        }
        Err(last_error.map_or(Error::Contention, Error::from).into())
    }

    /// Newest entry for `key` among the commits `numbers`.
    async fn find(&self, numbers: RangeInclusive<u64>, key: &str) -> Result<Option<Found>> {
        for number in numbers.rev() {
            let commit = self
                .read_commit(number)
                .await?
                .ok_or(Error::MissingCommit { number })?;
            // A commit may mention a key more than once; the last op wins.
            if let Some(op) = commit.ops.into_iter().rev().find(|op| op.key() == key) {
                return Ok(Some(Found {
                    version: Version::from_commit(number),
                    value: match op {
                        Op::Put { value, .. } => Some(value),
                        Op::Delete { .. } => None,
                    },
                }));
            }
        }
        Ok(None)
    }

    /// Number of the newest commit (0 for an empty log), after checking that the log is
    /// exactly `1..=head`.
    async fn head(&self) -> Result<u64> {
        let dir = self.prefix.clone().join("log");
        let listing = self.store.list_with_delimiter(Some(&dir)).await?;
        let mut numbers: Vec<u64> = listing
            .objects
            .iter()
            .filter_map(|o| parse_commit_name(o.location.filename()?))
            .collect();
        numbers.sort_unstable();
        for (expected, &n) in (1..).zip(&numbers) {
            if n != expected {
                return Err(Error::MissingCommit { number: expected });
            }
        }
        Ok(numbers.len() as u64)
    }

    /// `None` if the commit does not exist.
    async fn read_commit(&self, number: u64) -> Result<Option<Commit>> {
        let bytes = match self.store.get(&self.commit_path(number)).await {
            Ok(result) => result.bytes().await?,
            Err(StoreError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Commit::decode(number, &bytes).map(Some)
    }

    /// Whether commit `number` exists and was written by the attempt `id`.
    async fn is_ours(&self, number: u64, id: &str) -> Result<bool> {
        Ok(self
            .read_commit(number)
            .await?
            .is_some_and(|commit| commit.id == id))
    }

    fn commit_path(&self, number: u64) -> Path {
        self.prefix
            .clone()
            .join("log")
            .join(format!("{number:020}.json"))
    }

    async fn create_commit(&self, number: u64, payload: Bytes) -> object_store::Result<()> {
        self.store
            .put_opts(
                &self.commit_path(number),
                payload.into(),
                PutMode::Create.into(),
            )
            .await
            .map(|_| ())
    }
}

/// Only `<20 digits>.json` names a commit; anything else in `log/` is ignored.
fn parse_commit_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".json")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

/// Full-jitter exponential backoff before retry `attempt` (1-based).
async fn backoff(attempt: u32) {
    let ceiling = BACKOFF_BASE
        .saturating_mul(1 << (attempt - 1).min(16))
        .min(BACKOFF_CAP);
    let micros = rand::rng().random_range(0..=ceiling.as_micros() as u64);
    tokio::time::sleep(Duration::from_micros(micros)).await;
}

/// Unconditional ops never produce `Conflict`.
fn unwrap_other(e: CasError) -> Error {
    match e {
        CasError::Other(e) => e,
        CasError::Conflict { .. } => unreachable!("unconditional commit cannot conflict"),
    }
}
