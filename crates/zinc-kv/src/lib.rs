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
mod tx;
mod types;

use std::{collections::HashSet, ops::RangeInclusive, sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::{path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode};
use rand::Rng;

pub use error::{Error, Result};
pub use tx::Tx;
pub use types::{CommitResult, Entry, Version};

use commit::{Commit, Op};

const MAX_ATTEMPTS: u32 = 64;
/// Commits a single head lookup may find past the listing before giving up.
const MAX_PROBES: u64 = 64;
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

impl Db {
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<Db> {
        Ok(Db { store, prefix })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Entry>> {
        let head = self.head().await?;
        self.get_at(head, key).await
    }

    /// Unconditional write. Returns the new version.
    pub async fn put(&self, key: &str, value: Bytes) -> Result<Version> {
        let op = Op::Put {
            key: key.to_owned(),
            value,
        };
        self.commit(vec![op], &HashSet::new(), 0).await
    }

    /// Unconditional delete. Deleting a missing key still records a tombstone.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let op = Op::Delete {
            key: key.to_owned(),
        };
        self.commit(vec![op], &HashSet::new(), 0).await?;
        Ok(())
    }

    /// Starts a transaction. No I/O until its first read.
    pub fn begin(&self) -> Tx<'_> {
        Tx::new(self)
    }

    /// `key` as of commit `head`.
    async fn get_at(&self, head: u64, key: &str) -> Result<Option<Entry>> {
        let found = self.find(1..=head, key).await?;
        Ok(found.and_then(|f| {
            let version = f.version;
            f.value.map(|value| Entry { value, version })
        }))
    }

    /// Appends one commit holding `ops`, failing with `Conflict` if a commit after
    /// `snapshot` touches any key in `reads`. An empty `reads` checks nothing.
    ///
    /// A failed `Create`, even one reporting `AlreadyExists`, may still have written the
    /// commit (a lost response, a client retry), or may write it later (a request still in
    /// flight). So every failed number is kept as *uncertain* and retried like a lost race;
    /// once the head reaches it, its `id` says whether the write was ours. A request can
    /// only ever land at the number it named, so this finds every write of ours that
    /// landed. Any error while a number is still uncertain becomes `OutcomeUnknown`.
    async fn commit(&self, ops: Vec<Op>, reads: &HashSet<String>, snapshot: u64) -> Result<Version> {
        let id = format!("{:032x}", rand::random::<u128>());
        let payload = Bytes::from(serde_json::to_vec(&Commit::new(id.clone(), ops))?);

        // Numbers whose `Create` failed and that were still free at the last head.
        let mut uncertain = Vec::new();
        match self
            .attempt_commits(reads, snapshot, &id, payload, &mut uncertain)
            .await
        {
            Err(e) if !uncertain.is_empty() => Err(Error::OutcomeUnknown {
                source: Box::new(e),
            }),
            result => result,
        }
    }

    async fn attempt_commits(
        &self,
        reads: &HashSet<String>,
        snapshot: u64,
        id: &str,
        payload: Bytes,
        uncertain: &mut Vec<u64>,
    ) -> Result<Version> {
        // Commits up to `checked` are known not to touch `reads`, and the head we append
        // after has been decoded (for all writes). Reads were made at `snapshot`, which the
        // reads decoded; blind writes have checked nothing yet.
        let mut checked = if reads.is_empty() { 0 } else { snapshot };
        let mut last_error: Option<StoreError> = None;
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                backoff(attempt).await;
            }
            let head = self.head().await?;
            // Checked before the precondition, so that our own commit is never mistaken
            // for a conflicting one.
            if let Some(number) = self.settle(uncertain, head, id).await? {
                return Ok(Version::from_commit(number));
            }

            if !reads.is_empty() {
                // Scans newest first, so this decodes the head commit too.
                if let Some(key) = self.touched(checked + 1..=head, reads).await? {
                    return Err(Error::Conflict { key });
                }
            } else if head > checked {
                // Never append after a commit we cannot read: it may be damaged, or in a
                // newer format whose rules this writer does not know. Every writer checks
                // this, so once a newer format is at the head, older writers stop.
                self.read_commit(head)
                    .await?
                    .ok_or(Error::MissingCommit { number: head })?;
            }
            checked = head;

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
        // The last attempt's failure has not been settled yet.
        let head = self.head().await?;
        if let Some(number) = self.settle(uncertain, head, id).await? {
            return Ok(Version::from_commit(number));
        }
        Err(last_error.map_or(Error::Contention, Error::from))
    }

    /// Settles the uncertain numbers that `head` has reached: a taken number is final,
    /// either ours or another writer's. Returns ours if found; leaves the rest uncertain.
    async fn settle(&self, uncertain: &mut Vec<u64>, head: u64, id: &str) -> Result<Option<u64>> {
        for &number in uncertain.iter().filter(|&&n| n <= head) {
            if self.is_ours(number, id).await? {
                return Ok(Some(number));
            }
        }
        uncertain.retain(|&n| n > head);
        Ok(None)
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

    /// A key from `keys` that one of the commits `numbers` writes, if any.
    async fn touched(
        &self,
        numbers: RangeInclusive<u64>,
        keys: &HashSet<String>,
    ) -> Result<Option<String>> {
        for number in numbers.rev() {
            let commit = self
                .read_commit(number)
                .await?
                .ok_or(Error::MissingCommit { number })?;
            if let Some(op) = commit.ops.iter().find(|op| keys.contains(op.key())) {
                return Ok(Some(op.key().to_owned()));
            }
        }
        Ok(None)
    }

    /// Number of the newest commit (0 for an empty log), after checking that the listed
    /// commits are exactly `1..=n`.
    ///
    /// A listing may lag behind completed commits. Commits are only created in order (a
    /// writer creates `n + 1` after seeing `n`), so probing `n + 1, n + 2, …` until a free
    /// number finds the true head as of that probe, which is what makes reads and
    /// `Conflict` answers current.
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
        let listed = numbers.len() as u64;
        let mut head = listed;
        while self.exists(head + 1).await? {
            head += 1;
            // Appends are outpacing us. Stopping at a head not confirmed as current would
            // give stale answers, so give up instead.
            if head - listed >= MAX_PROBES {
                return Err(Error::Contention);
            }
        }
        Ok(head)
    }

    async fn exists(&self, number: u64) -> Result<bool> {
        match self.store.head(&self.commit_path(number)).await {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
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
