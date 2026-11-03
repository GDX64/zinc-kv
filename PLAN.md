# blob-zinc: KV store on `object_store` — phased plan

## Context
Nodes in a distributed mesh need a shared, strongly consistent place to coordinate (leases, leader election, config). Build a small embedded Rust library whose only shared dependency is an `object_store` backend. Correctness rests on a single primitive: create-if-absent (`PutMode::Create`). Workspace `c:\projects\blob-zinc` is a Cargo workspace.

Decisions: Delta-Lake-style layout (numbered, immutable commit files; compaction by checkpoints); backends S3 + LocalFileSystem + InMemory; semantics = get/put/delete + CAS on version. We go one phase at a time, smallest viable step first. Each phase is shippable and has its own tests.

History: Phase 1 used one object replaced with `PutMode::Update`; Phase 2 first split it into a manifest + segments. Both needed conditional *update*, which `LocalFileSystem` lacks (hence the test-only `zinc-fs` crate). We switched to the numbered-commit layout, which needs only `Create`, works on every backend, and leaves no orphan files.

---

## Workspace layout
Cargo workspace (`Cargo.toml` at root, shared `[workspace.dependencies]`), crates under `crates/`. `crates/zinc-kv` is the KV store; `crates/zinc-fs` is a test-only `ObjectStore` adding `PutMode::Update` to the local filesystem (no longer used by `zinc-kv`). File paths below are relative to `crates/zinc-kv/`.

## Phase 1 — Minimum: one object, CAS-safe (done, superseded by Phase 2)
Whole DB in one `state.json`, read-modify-write with `PutMode::Update`. Established the public API:
`Db::open(Arc<dyn ObjectStore>, Path)`, `get`, `put`, `delete`, `cas(key, expected: Option<Version>, new: Option<Bytes>) -> Result<Version, CasError>`. `Version` is an opaque newtype over `u64` (private field); `put` returns `Version`; `Entry { value, version }`. `expected: None` = key must not exist (lock/leader election); `new: None` deletes.

## Phase 2 — Numbered commit log (Delta style)
```
<prefix>/log/00000000000000000001.json     one immutable commit per file, 20-digit zero-padded
<prefix>/log/00000000000000000002.json
```
A commit file is `{"ops":[{"op":"put","key":..,"value":<base64>} | {"op":"delete","key":..}]}`. The file number *is* the commit order and the **version** of every key it writes.
- Commit: list `log/`, head = highest number (0 if empty); check the `cas` precondition; `PutMode::Create` of `head+1`. `AlreadyExists` → someone else won: list again, re-check the precondition against only the commits newer than the last one checked, retry with the new head (bounded). No separate manifest, no orphans.
- Read: list `log/`, scan commits newest→oldest until the key is found. A `delete` op is a tombstone: the key reads as absent and counts as "must not exist" for `cas`.
- API unchanged; Phase 1 behavior tests must still pass untouched.
- Known costs (addressed in later phases): every read lists and fetches commits (Phase 3 cache, Phase 4 checkpoints); retries have no backoff.

Tests:
- **Layout**: `tests/layout.rs`, on plain `LocalFileSystem` under `target/tmp/layout`, left on disk for inspection. Checks only that `log/` holds one file per commit and that there is no `manifest.json`/`state.json`. Does not parse contents.
- **Commit path**
  1. Stale `cas` returns `Conflict` with the correct `current` version.
  2. Delete then `cas(key, None, …)` recreates the key (tombstone counts as absent) with a higher version than before.
  3. Retry-window conflict detection: A computes its precondition, B commits to the same key before A's `Create` → A gets `Conflict`; if B changed a different key, A succeeds with the next number.
- **Failure handling**
  4. A gap or a missing commit file referenced by a listing makes `get` return an error, not `None`.
- **Concurrency**
  5. Counter and lock tests (exist) also run on `LocalFileSystem`.
  6. Mixed workload: concurrent writers on different keys plus deletes; every key ends at its last written value.
- Later: a model-based test comparing `Db` against a `HashMap` under random operations.

## Phase 3 — Reads: `list(prefix)`, snapshots, cache
- `list(prefix)` merging newest-wins; `snapshot()` pinning a head for consistent multi-reads.
- In-memory cache of commit files by number (safe since immutable); use `list_with_offset` to list only commits newer than what is cached.

## Phase 4 — Checkpoints and cleanup
- `checkpoint()`: pick N (e.g. every 100 commits), read the previous checkpoint, replay commits up to N, write `checkpoints/<N>.json` (full live state, each key with its original version, no tombstones) with `PutMode::Create`. It is a pure function of the immutable log, so any node may write it and duplicate writers produce identical bytes; it never conflicts with commits. Optional best-effort `checkpoints/_last` hint.
- Read: load newest checkpoint, replay commits after N. A reader that hits a missing commit reloads from the newest checkpoint.
- `cleanup(retention)`: delete commits ≤ N and older checkpoints **only after a long retention period**. **Hazard:** deleting `log/K` makes `Create log/K` succeed again, so a writer paused longer than the retention could fork the history. Retention must exceed any plausible writer pause; document this limit. (A mitigation to consider: writers re-list the head right before `Create` and refuse to create a number below the newest checkpoint.)

## Phase 5 — Real S3 validation
- Run the suite against MinIO/S3 (`PutMode::Create` via `If-None-Match`); document backend requirements/caveats.

## Later (not planned yet)
Multi-key atomic transactions (a commit file already holds many ops), TTL/leases, watch via polling the head.

---

## Verification (applies from Phase 1 onward)
- `cargo test`, suite on `InMemory` and `LocalFileSystem`.
- Counter test: several `Db` instances sharing one store (simulating nodes) CAS-increment one key; final value equals total increments.
- Lock test: many tasks race `cas(key, None, id)`; exactly one wins.
- Basic semantics: get-missing, put/get, delete, CAS mismatch returns current version.
