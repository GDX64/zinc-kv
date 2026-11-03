# blob-zinc

A small, strongly consistent key-value store built on the [`object_store`](https://crates.io/crates/object_store) crate. It lets the nodes of a distributed mesh coordinate (leader election, locks, shared config) using nothing but an object store as shared infrastructure: no database server, no consensus cluster.

The store is an embedded Rust library. Every node opens a `Db` on the same bucket and prefix; reads and writes go straight to the object store.

> **Status:** early development. The commit log, compare-and-set and failure handling are done and heavily tested. Read cost still grows with history until checkpoints land (see [Roadmap](#roadmap)). Tested on `InMemory` and `LocalFileSystem` only; not yet against real S3.

## Usage

```rust
use std::sync::Arc;

use bytes::Bytes;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use zinc_kv::{CasError, Db};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Any object_store backend; every node opens the same store and prefix.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(store, Path::from("mesh")).await?;

    // Plain reads and writes.
    let version = db.put("config/region", Bytes::from("eu-west-1")).await?;
    let entry = db.get("config/region").await?.unwrap();
    assert_eq!(entry.version, version);

    // Leader election: `expected: None` means "only if the key does not exist".
    match db.cas("leader", None, Some(Bytes::from("node-a"))).await {
        Ok(_) => println!("we are the leader"),
        Err(CasError::Conflict { current }) => println!("someone else leads ({current:?})"),
        Err(e) => return Err(e.into()),
    }

    // Optimistic update: succeeds only if nobody changed the key since we read it.
    let current = db.get("leader").await?.unwrap();
    db.cas("leader", Some(current.version), Some(Bytes::from("node-b"))).await?;

    // Unconditional delete (`cas` with `new: None` deletes conditionally).
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
| `cas(key, expected, new)` | `Version` or `CasError::Conflict { current }` | Writes only if the key's current version equals `expected` (`None` = must not exist). `new: None` deletes. |

`Version` is opaque and totally ordered: every write gets a strictly higher version than any write before it.

### Errors

Reliability comes first: the store returns a typed error rather than ever guessing.

| Error | When |
| --- | --- |
| `CasError::Conflict { current }` | The `cas` precondition failed; `current` is the key's version at that moment. |
| `Error::Contention` | Other writers kept winning, or the log grew faster than the head could be confirmed. Retry later. |
| `Error::OutcomeUnknown` | A write failed in a way that may still have applied it. A later read is **not** conclusive; if you need certainty, put a unique token in the value. |
| `Error::MissingCommit`, `Corrupt`, `UnsupportedFormat` | The log is damaged or written in a newer format. Never read around. |
| `Error::Store` | The object store returned an error. |

## How it works

The database is a log of immutable, numbered commit files, in the style of Delta Lake:

```
<prefix>/log/00000000000000000001.json
<prefix>/log/00000000000000000002.json
...
```

- **Writing.** A writer finds the newest commit `n`, checks its `cas` precondition against it, and creates file `n + 1` with `PutMode::Create` (create-if-absent). Only one writer can create a given file, so exactly one wins each number; losers re-check against the newer commits and retry with backoff.
- **Versions.** A commit's number is the version of every key it writes.
- **Reading.** Scan commits from newest to oldest until the key appears. A delete is recorded as a tombstone.
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

- **Phase 4:** commit cache and checkpoints, so reads no longer cost time proportional to history.
- **Phase 5:** cleanup of old commits after checkpointing.
- **Phase 6:** `list(prefix)` and consistent snapshots.

Details and later ideas (multi-key transactions, leases, watch) are in [PLAN.md](PLAN.md).
