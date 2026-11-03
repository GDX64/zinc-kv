# blob-zinc

A small, strongly consistent key-value store built on the [`object_store`](https://crates.io/crates/object_store) crate. It lets the nodes of a distributed mesh coordinate (leader election, locks, shared config) using nothing but an object store as shared infrastructure: no database server, no consensus cluster.

The store is an embedded Rust library. Every node opens a `Db` on the same bucket and prefix; reads and writes go straight to the object store.

> **Status:** early development. The commit log, transactions, checkpoints and failure handling are done and heavily tested. Old commits are never deleted yet, so storage grows with history (see [Roadmap](#roadmap)). Tested on `InMemory` and `LocalFileSystem` only; not yet against real S3.

## Usage

```rust
use std::sync::Arc;

use bytes::Bytes;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use zinc_kv::{Db, Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Any object_store backend; every node opens the same store and prefix.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(store, Path::from("mesh")).await?;

    // Plain reads and writes.
    let version = db.put("config/region", Bytes::from("eu-west-1")).await?;
    let entry = db.get("config/region").await?.unwrap();
    assert_eq!(entry.version, version);

    // Leader election. The commit fails if a key the transaction read changed.
    let mut tx = db.begin();
    if tx.get("leader").await?.is_none() {
        tx.put("leader", Bytes::from("node-a"));
        match tx.commit().await {
            // The commit's version doubles as a fencing epoch.
            Ok(result) => println!("we lead, epoch {:?}", result.epoch),
            Err(Error::Conflict { .. }) => println!("someone else got there first"),
            Err(e) => return Err(e.into()),
        }
    }

    // Read-modify-write across keys, applied together or not at all.
    let mut tx = db.begin();
    let previous = tx.get("leader").await?.unwrap();
    tx.put("leader", Bytes::from("node-b"));
    tx.put("previous-leader", previous.value);
    tx.commit().await?;

    db.delete("config/region").await?;
    Ok(())
}
```

### API

| Method | Returns | Meaning |
| --- | --- | --- |
| `Db::open(store, prefix)` | `Db` | Opens the database stored under `prefix`. Cheap; no I/O. |
| `get(key)` | `Option<Entry>` | Current value and its `Version`, or `None`. |
| `put(key, value)` | `Version` | Unconditional write. |
| `delete(key)` | `()` | Unconditional delete. |
| `begin()` | `Tx` | Starts a transaction. No I/O until its first read. |
| `tx.get(key)` | `Option<Entry>` | Reads at the transaction's snapshot, fixed by its first read. Records the key, found or absent, as a dependency. |
| `tx.put(key, value)`, `tx.delete(key)` | | Buffered until commit; no I/O. |
| `tx.commit()` | `CommitResult` | Writes every buffered op in one commit, or fails with `Error::Conflict` if a key the transaction read was written after its snapshot. Writes to other keys do not conflict. Tried once: retrying is up to the caller. `epoch` is the new version of every key written (`None` if nothing was written). |

`Version` is opaque and totally ordered: every write gets a strictly higher version than any write before it. A version read in a transaction that then commits was still current at the commit.

### Errors

Reliability comes first: the store returns a typed error rather than ever guessing.

| Error | When |
| --- | --- |
| `Error::Conflict { key }` | A key the transaction read changed after its snapshot. Nothing was written. |
| `Error::ReadAfterWrite { key }` | The transaction read a key it had already written. |
| `Error::Contention` | Other writers kept winning, or the log grew faster than the head could be confirmed. Retry later. |
| `Error::OutcomeUnknown` | A write failed in a way that may still have applied it. A later read is **not** conclusive; if you need certainty, put a unique token in the value. |
| `Error::MissingCommit`, `Corrupt`, `UnsupportedFormat` | The log is damaged or written in a newer format. Never read around. |
| `Error::Store` | The object store returned an error. |

## How it works

The database is a log of immutable, numbered commit files, in the style of Delta Lake, plus periodic checkpoints of the full state:

```
<prefix>/log/00000000000000000001.json
<prefix>/log/00000000000000000002.json
...
<prefix>/checkpoints/00000000000000000100.json
<prefix>/checkpoints/00000000000000000200.json
```

- **Writing.** A writer finds the newest commit `n`, checks that no commit after its transaction's snapshot writes a key it read, and creates file `n + 1` with `PutMode::Create` (create-if-absent). Only one writer can create a given file, so exactly one wins each number; losers check only the newer commits and retry with backoff. Losing a number to a write of an unrelated key is not a conflict.
- **Versions.** A commit's number is the version of every key it writes.
- **Reading.** Start from the newest checkpoint and scan the commits after it, newest first, until the key appears; otherwise the checkpoint answers. A delete is recorded as a tombstone.
- **Checkpoints.** Every `CHECKPOINT_INTERVAL` (100) commits, the writer that reaches the interval writes `checkpoints/<n>.json`: the full live state as of commit `n`. It is a pure function of the log, so concurrent writers of one produce the same file. A missed checkpoint is written by the next writer past it, in order. `Db::checkpoint()` does the same on demand.
- **Caching.** Commits and checkpoints never change, so each `Db` caches what it has read and lists only the commits after the newest one it knows. A read costs one listing, one `HEAD` probe and the commits since the last checkpoint, at most 100.
- **Failed writes.** Each write carries a random id. If the store reports a failure (even `AlreadyExists`), the writer later checks whether the commit at that number is its own, so a write that actually landed is never reported as failed or applied twice.

The full design, its reasoning, and the review findings that shaped it are in [PLAN.md](PLAN.md).

### Backend requirements

1. The store never loses, deletes or alters an object once written.
2. `PutMode::Create` is atomic: at most one writer creates a given name.
3. Listings contain no holes. They may lag behind recent writes; the store compensates by probing for newer commits.
4. Reads of a single object (`HEAD`/`GET`) see all completed writes.

`InMemory` and `LocalFileSystem` meet these. AWS S3 should (conditional `If-None-Match` puts, strong consistency) but is not tested here.

## Project layout

```
crates/zinc-kv/         the store
  src/lib.rs            Db and the commit protocol
  src/commit.rs         commit file format
  tests/                behaviour, failure, concurrency and model-based tests
  tests/common/         fault-injecting ObjectStore wrapper used by the tests
PLAN.md                 phased design plan
```

## Testing

```sh
cargo test --workspace
```

Every test runs on both `InMemory` and `LocalFileSystem` (files under `target/tmp/` are left behind for inspection). The suite includes:

- **Fault injection:** writes that fail before or after landing, requests that land late, lagging or failing listings, damaged commit files, competitors that win every race.
- **Concurrency:** many nodes racing on a counter, a lock, and a mixed workload.
- **Model-based:** random operation sequences checked against a `HashMap`.

The suite has been checked by mutation: disabling any safety mechanism makes at least one test fail.

## Roadmap

- **Phase 5:** cleanup of old commits after checkpointing.
- **Phase 6:** `list(prefix)` and consistent snapshots.

Details and later ideas (leases, watch) are in [PLAN.md](PLAN.md).
