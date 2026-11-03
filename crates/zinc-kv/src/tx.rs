use std::collections::HashSet;

use bytes::Bytes;

use crate::{commit::Op, CommitResult, Db, Entry, Error, Result};

/// A transaction: reads at one snapshot, writes buffered until [`Tx::commit`].
///
/// The commit fails with [`Error::Conflict`] if any key the transaction read, found or
/// absent, was written after the snapshot. Writes to other keys do not conflict. A
/// transaction is tried once; retrying is up to the caller. Dropping it discards it.
pub struct Tx<'a> {
    db: &'a Db,
    /// Head fixed by the first read.
    snapshot: Option<u64>,
    reads: HashSet<String>,
    writes: Vec<Op>,
}

impl<'a> Tx<'a> {
    pub(crate) fn new(db: &'a Db) -> Self {
        Tx {
            db,
            snapshot: None,
            reads: HashSet::new(),
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
        let snapshot = match self.snapshot {
            Some(snapshot) => snapshot,
            None => *self.snapshot.insert(self.db.head().await?),
        };
        let entry = self.db.get_at(snapshot, key).await?;
        // Only a read that succeeded is a dependency.
        self.reads.insert(key.to_owned());
        Ok(entry)
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
        let snapshot = self.snapshot.unwrap_or(0);
        let version = self.db.commit(self.writes, &self.reads, snapshot).await?;
        Ok(CommitResult {
            epoch: Some(version),
        })
    }
}
