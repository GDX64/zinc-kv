# blob-zinc: KV store on `object_store` — phased plan

## Context
Nodes in a distributed mesh need a shared, strongly consistent place to coordinate (leases, leader election, config). Build a small embedded Rust library whose only shared dependency is an `object_store` backend. Correctness rests on conditional puts (`PutMode::Create`, `PutMode::Update(UpdateVersion)`). Workspace `c:\projects\blob-zinc` is empty (new crate).

Decisions: end-state layout is manifest + immutable segments; backends S3 + LocalFileSystem + InMemory; semantics = get/put/delete + CAS on version. We go one phase at a time, smallest viable step first. Each phase is shippable and has its own tests.

**First action after approval:** write this plan to `PLAN.md` in the repo root (user request), then start Phase 1 only.

---

## Workspace layout
Cargo workspace (`Cargo.toml` at root, shared `[workspace.dependencies]`), crates under `crates/`. Starts with one crate, `crates/zinc-kv` (the KV store). Later concerns (e.g. a node-facing coordination layer, CLI, or test harness) get their own crates. File paths below are relative to `crates/zinc-kv/`.

## Phase 1 — Minimum: one object, CAS-safe (smallest correct thing)
Entire DB = one object `<prefix>/state.json`: `{ epoch: u64, entries: { key: { value: base64, version: u64 } } }`.
- Read: GET, parse, keep ETag/version from `GetResult.meta`.
- Write: read-modify-write, `PutMode::Update(UpdateVersion{e_tag, version})`; first write uses `PutMode::Create`. On `Error::Precondition`/`AlreadyExists` → reload and retry (bounded, jittered).
- API: `Db::open(Arc<dyn ObjectStore>, Path)`, `get`, `put`, `delete`, `cas(key, expected: Option<u64>, new: Option<Bytes>) -> Result<u64, CasError>`. Key version = `epoch` of the commit that last wrote it; `expected: None` = must not exist (lock/leader election).
- Backends under test: `InMemory`, `LocalFileSystem`.
- Files: `Cargo.toml` (object_store, tokio, bytes, serde, serde_json, base64, thiserror, rand), `src/lib.rs`, `src/state.rs`, `src/error.rs`, `tests/`.
- **Done when**: concurrency tests pass (below). Known limit: whole state rewritten per write — fine for small coordination data.

## Phase 2 — Split into manifest + immutable segments
Replace `state.json` with `manifest.json` (`{epoch, segments:[{path, commit_epoch}]}`, the only CAS'd object) and `segments/<uuid7>.seg` (immutable batches of Put/Delete ops).
- Commit: upload segment (`PutMode::Create`), CAS manifest; on conflict re-validate CAS preconditions against only the newly added segments, retry manifest write reusing the segment.
- Read: manifest GET, scan segments newest→oldest. Tombstones for deletes.
- API unchanged; Phase 1 tests must still pass untouched.

## Phase 3 — Reads: `list(prefix)`, snapshots, segment cache
- `list(prefix)` merging newest-wins; `snapshot()` pinning a manifest for consistent multi-reads.
- In-memory cache of segments by path (safe since immutable).

## Phase 4 — Compaction and GC
- `compact()`: merge segments, drop shadowed/tombstoned entries, preserve per-entry `commit_epoch`, CAS-swap manifest; lost race → discard.
- `gc(grace)`: delete unreferenced segments older than the grace period (cleans orphans from failed commits).

## Phase 5 — Real S3 validation
- Run suite against MinIO/S3 with conditional put enabled (`S3ConditionalPut::ETagMatch`); document backend requirements/caveats.

## Later (not planned yet)
Multi-key atomic transactions (natural fit for Phase 2 segments), TTL/leases, watch via polling manifest `epoch`.

---

## Verification (applies from Phase 1 onward)
- `cargo test`, suite parametrized over `InMemory` and `LocalFileSystem` (tempdir).
- Counter test: 16 tokio tasks × 50 CAS-increments across multiple `Db` instances sharing one store (simulating nodes) → final value exactly 800.
- Lock test: many tasks race `cas(key, None, id)`; exactly one wins.
- Basic semantics: get-missing, put/get, delete, CAS mismatch returns current version.
