//! A small strongly consistent key-value store on top of `object_store`.
//!
//! Layout under the prefix: `log/<n>.json`, one immutable commit per file, where `n` is a
//! zero-padded sequence number. A commit wins by creating file `head + 1` with
//! `PutMode::Create`; only one writer can create a given name. The number is the commit
//! order and the version of every key the commit writes.
//!
//! `checkpoints/<n>.json` holds the full live state as of commit `n`, for every multiple
//! `n` of [`CHECKPOINT_INTERVAL`] the log has reached, written in order. Reads start from
//! the newest checkpoint and replay only the commits after it.
//!
//! The log after the newest checkpoint must be a contiguous run. Anything else (a hole, an
//! unreadable commit or checkpoint, an unknown format) is reported as an error rather than
//! read around.

mod checkpoint;
mod commit;
mod error;
mod tx;
mod types;

use std::{
    collections::{BTreeMap, HashSet},
    ops::RangeInclusive,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode};
use rand::Rng;

pub use error::{Error, Result};
pub use tx::Tx;
pub use types::{CommitResult, Entry, Version};

use checkpoint::Checkpoint;
use commit::{Commit, Op};

/// Commits between checkpoints. Part of the storage format: every node must agree on it.
pub const CHECKPOINT_INTERVAL: u64 = 100;

const MAX_ATTEMPTS: u32 = 64;
/// Commits a single head lookup may find past the listing before giving up.
const MAX_PROBES: u64 = 64;
const BACKOFF_BASE: Duration = Duration::from_millis(1);
const BACKOFF_CAP: Duration = Duration::from_millis(32);

pub struct Db {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    cache: Mutex<Cache>,
}

/// What this `Db` has learned about the store. Commits and checkpoints are immutable, so
/// nothing here goes stale; it only falls behind.
#[derive(Default)]
struct Cache {
    /// Whether the newest checkpoint has been looked up since opening.
    located: bool,
    /// The newest checkpoint known; number 0 (empty) before the first.
    base: Arc<Checkpoint>,
    /// Highest commit confirmed to exist, with every commit between `base` and it.
    head: u64,
    /// Decoded commits after `base`.
    commits: BTreeMap<u64, Arc<Commit>>,
}

/// A consistent point to read at: the log up to `head`, starting from checkpoint `base`.
#[derive(Clone)]
pub(crate) struct Snapshot {
    pub(crate) head: u64,
    base: Arc<Checkpoint>,
}

/// What the newest commit mentioning a key says about it.
struct Found {
    version: Version,
    /// `None` for a tombstone.
    value: Option<Bytes>,
}

impl Db {
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<Db> {
        Ok(Db {
            store,
            prefix,
            cache: Mutex::default(),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Entry>> {
        let snapshot = self.head().await?;
        self.get_at(&snapshot, key).await
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

    /// Writes every checkpoint the log has reached that does not exist yet, in order.
    ///
    /// Writers call this on their own after a commit that reaches a checkpoint, so it is
    /// rarely needed. Safe from any node at any time: a checkpoint is a pure function of
    /// the log, so concurrent writers of one produce the same bytes.
    pub async fn checkpoint(&self) -> Result<()> {
        let snapshot = self.head().await?;
        let mut state = Checkpoint::clone(&snapshot.base);
        while state.number + CHECKPOINT_INTERVAL <= snapshot.head {
            let number = state.number + CHECKPOINT_INTERVAL;
            for n in state.number + 1..=number {
                let commit = self
                    .read_commit(n)
                    .await?
                    .ok_or(Error::MissingCommit { number: n })?;
                state.apply(n, &commit.ops);
            }
            let payload = state.encode()?;
            match self
                .store
                .put_opts(
                    &self.checkpoint_path(number),
                    payload.into(),
                    PutMode::Create.into(),
                )
                .await
            {
                // Another node wrote the same checkpoint first.
                Ok(_) | Err(StoreError::AlreadyExists { .. }) => {}
                Err(e) => return Err(e.into()),
            }
            self.advance_base(Arc::new(state.clone()));
        }
        Ok(())
    }

    /// `key` as of `snapshot`.
    async fn get_at(&self, snapshot: &Snapshot, key: &str) -> Result<Option<Entry>> {
        let base = &snapshot.base;
        match self.find(base.number + 1..=snapshot.head, key).await? {
            Some(f) => {
                let version = f.version;
                Ok(f.value.map(|value| Entry { value, version }))
            }
            None => Ok(base.entries.get(key).cloned()),
        }
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
        let version = match self
            .attempt_commits(reads, snapshot, &id, payload, &mut uncertain)
            .await
        {
            Err(e) if !uncertain.is_empty() => Err(Error::OutcomeUnknown {
                source: Box::new(e),
            }),
            result => result,
        }?;
        let base = self.cache.lock().unwrap().base.number;
        if version.number() >= base + CHECKPOINT_INTERVAL {
            // Best effort: the commit has landed either way, and the next commit past the
            // checkpoint, from any writer, tries again.
            let _ = self.checkpoint().await;
        }
        Ok(version)
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
            let current = self.head().await?;
            let head = current.head;
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
            } else if head > checked && head > current.base.number {
                // Never append after a commit we cannot read: it may be damaged, or in a
                // newer format whose rules this writer does not know. Every writer checks
                // this, so once a newer format is at the head, older writers stop. (A head
                // that is the checkpoint was decoded as the checkpoint.)
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
        let head = self.head().await?.head;
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
            if let Some(op) = commit.ops.iter().rev().find(|op| op.key() == key) {
                return Ok(Some(Found {
                    version: Version::from_commit(number),
                    value: match op {
                        Op::Put { value, .. } => Some(value.clone()),
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

    /// The current snapshot: the newest commit (0 for an empty log) and the newest
    /// checkpoint at or below it.
    ///
    /// Lists only the commits after the highest one already confirmed (or the newest
    /// checkpoint), and checks that they continue it without a hole.
    ///
    /// A listing may lag behind completed commits. Commits are only created in order (a
    /// writer creates `n + 1` after seeing `n`), so probing `n + 1, n + 2, …` until a free
    /// number finds the true head as of that probe, which is what makes reads and
    /// `Conflict` answers current.
    async fn head(&self) -> Result<Snapshot> {
        let (located, known) = {
            let cache = self.cache.lock().unwrap();
            (cache.located, cache.head)
        };
        if !located {
            self.locate_checkpoint().await?;
        }
        let base = self.cache.lock().unwrap().base.clone();
        let from = known.max(base.number);

        let dir = self.prefix.clone().join("log");
        let mut numbers: Vec<u64> = self
            .store
            .list_with_offset(Some(&dir), &self.commit_path(from))
            .try_filter_map(|o| {
                // Only direct children of `log/`.
                let child = o.location.prefix_match(&dir).is_some_and(|rest| rest.count() == 1);
                let number = o.location.filename().and_then(parse_commit_name);
                futures::future::ready(Ok(number.filter(|_| child)))
            })
            .try_collect()
            .await?;
        numbers.sort_unstable();
        for (expected, &n) in (from + 1..).zip(&numbers) {
            if n != expected {
                return Err(Error::MissingCommit { number: expected });
            }
        }
        let listed = from + numbers.len() as u64;
        let mut head = listed;
        while self.exists(&self.commit_path(head + 1)).await? {
            head += 1;
            // Appends are outpacing us. Stopping at a head not confirmed as current would
            // give stale answers, so give up instead.
            if head - listed >= MAX_PROBES {
                return Err(Error::Contention);
            }
        }
        {
            let mut cache = self.cache.lock().unwrap();
            cache.head = cache.head.max(head);
        }

        // Checkpoints are written only for commits that exist, so a newer one is possible
        // only once the log has reached it.
        let mut number = base.number;
        while number + CHECKPOINT_INTERVAL <= head
            && self
                .exists(&self.checkpoint_path(number + CHECKPOINT_INTERVAL))
                .await?
        {
            number += CHECKPOINT_INTERVAL;
        }
        let base = if number > base.number {
            self.load_checkpoint(number).await?
        } else {
            base
        };
        Ok(Snapshot { head, base })
    }

    /// Finds the newest checkpoint on first use: lists `checkpoints/`, then probes past
    /// the listing (which may lag), since checkpoints exist in order without gaps.
    async fn locate_checkpoint(&self) -> Result<()> {
        let dir = self.prefix.clone().join("checkpoints");
        let listing = self.store.list_with_delimiter(Some(&dir)).await?;
        let listed = listing
            .objects
            .iter()
            .filter_map(|o| parse_commit_name(o.location.filename()?))
            .filter(|n| n % CHECKPOINT_INTERVAL == 0)
            .max()
            .unwrap_or(0);
        let mut number = listed;
        while self
            .exists(&self.checkpoint_path(number + CHECKPOINT_INTERVAL))
            .await?
        {
            number += CHECKPOINT_INTERVAL;
            if number - listed >= MAX_PROBES * CHECKPOINT_INTERVAL {
                return Err(Error::Contention);
            }
        }
        if number > 0 {
            self.load_checkpoint(number).await?;
        }
        self.cache.lock().unwrap().located = true;
        Ok(())
    }

    /// Reads checkpoint `number` and makes it the base if it is the newest known.
    async fn load_checkpoint(&self, number: u64) -> Result<Arc<Checkpoint>> {
        let bytes = match self.store.get(&self.checkpoint_path(number)).await {
            Ok(result) => result.bytes().await?,
            Err(StoreError::NotFound { .. }) => return Err(Error::MissingCheckpoint { number }),
            Err(e) => return Err(e.into()),
        };
        let checkpoint = Arc::new(Checkpoint::decode(number, &bytes)?);
        self.advance_base(checkpoint.clone());
        Ok(checkpoint)
    }

    fn advance_base(&self, checkpoint: Arc<Checkpoint>) {
        let mut cache = self.cache.lock().unwrap();
        if checkpoint.number > cache.base.number {
            cache.commits.retain(|&n, _| n > checkpoint.number);
            cache.head = cache.head.max(checkpoint.number);
            cache.base = checkpoint;
        }
    }

    async fn exists(&self, path: &Path) -> Result<bool> {
        match self.store.head(path).await {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// `None` if the commit does not exist. Decoded commits are cached: they never change.
    async fn read_commit(&self, number: u64) -> Result<Option<Arc<Commit>>> {
        if let Some(commit) = self.cache.lock().unwrap().commits.get(&number) {
            return Ok(Some(commit.clone()));
        }
        let bytes = match self.store.get(&self.commit_path(number)).await {
            Ok(result) => result.bytes().await?,
            Err(StoreError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let commit = Arc::new(Commit::decode(number, &bytes)?);
        let mut cache = self.cache.lock().unwrap();
        if number > cache.base.number {
            cache.commits.insert(number, commit.clone());
        }
        Ok(Some(commit))
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

    fn checkpoint_path(&self, number: u64) -> Path {
        self.prefix
            .clone()
            .join("checkpoints")
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

/// Only `<20 digits>.json` names a commit or checkpoint; anything else is ignored.
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
