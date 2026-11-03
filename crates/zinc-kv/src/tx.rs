use std::collections::HashSet;

use bytes::Bytes;

use crate::{commit::Op, CommitResult, Db, Entry, Error, Result, Snapshot};

/// A transaction: reads at one snapshot, writes buffered until [`Tx::commit`].
///
/// The commit fails with [`Error::Conflict`] if any key the transaction read, found or
/// absent, or any key under a prefix it listed, was written after the snapshot. Writes to
/// other keys do not conflict. A transaction is tried once; retrying is up to the caller.
/// Dropping it discards it.
pub struct Tx<'a> {
    db: &'a Db,
    /// Fixed by the first read.
    snapshot: Option<Snapshot>,
    reads: ReadSet,
    writes: Vec<Op>,
}

/// What a transaction depends on: single keys and whole prefixes.
#[derive(Default)]
pub(crate) struct ReadSet {
    keys: HashSet<String>,
    prefixes: Vec<String>,
}

impl ReadSet {
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.prefixes.is_empty()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.keys.contains(key) || self.prefixes.iter().any(|p| key.starts_with(p.as_str()))
    }
}

impl<'a> Tx<'a> {
    pub(crate) fn new(db: &'a Db) -> Self {
        Tx {
            db,
            snapshot: None,
            reads: ReadSet::default(),
            writes: Vec::new(),
        }
    }

    /// Reads `key` at the snapshot. `entry.version` is the commit that last wrote it; if
    /// the transaction commits, that version was still current at the commit.
    ///
    /// Reading a key already written in this transaction is [`Error::ReadAfterWrite`].
    pub async fn get(&mut self, key: &str) -> Result<Option<Entry>> {
        if self.writes.iter().any(|op| op.key() == key) {
            return Err(Error::ReadAfterWrite {
                key: key.to_owned(),
            });
        }
        let snapshot = self.snapshot().await?;
        let entry = self.db.get_at(&snapshot, key).await?;
        // Only a read that succeeded is a dependency.
        self.reads.keys.insert(key.to_owned());
        Ok(entry)
    }

    /// Every live key starting with `prefix` at the snapshot, sorted, with its entry. Any
    /// later write under the prefix, including one creating a new key, is a conflict.
    ///
    /// Listing a prefix the transaction has already written under is
    /// [`Error::ReadAfterWrite`].
    pub async fn list(&mut self, prefix: &str) -> Result<Vec<(String, Entry)>> {
        if let Some(op) = self.writes.iter().find(|op| op.key().starts_with(prefix)) {
            return Err(Error::ReadAfterWrite {
                key: op.key().to_owned(),
            });
        }
        let snapshot = self.snapshot().await?;
        let entries = self.db.list_at(&snapshot, prefix).await?;
        self.reads.prefixes.push(prefix.to_owned());
        Ok(entries)
    }

    async fn snapshot(&mut self) -> Result<Snapshot> {
        match &self.snapshot {
            Some(snapshot) => Ok(snapshot.clone()),
            None => Ok(self.snapshot.insert(self.db.head().await?).clone()),
        }
    }

    pub fn put(&mut self, key: &str, value: Bytes) {
        self.writes.push(Op::Put {
            key: key.to_owned(),
            value,
        });
    }

    pub fn delete(&mut self, key: &str) {
        self.writes.push(Op::Delete {
            key: key.to_owned(),
        });
    }

    /// Writes every buffered op in one commit, unless a key read has changed since the
    /// snapshot. A transaction that wrote nothing makes no commit.
    pub async fn commit(self) -> Result<CommitResult> {
        if self.writes.is_empty() {
            return Ok(CommitResult { epoch: None });
        }
        let snapshot = self.snapshot.map_or(0, |s| s.head);
        let version = self.db.commit(self.writes, &self.reads, snapshot).await?;
        Ok(CommitResult {
            epoch: Some(version),
        })
    }
}
