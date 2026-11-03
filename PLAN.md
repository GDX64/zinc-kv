# blob-zinc: KV store on `object_store` — phased plan

## Context
Nodes in a distributed mesh need a shared, strongly consistent place to coordinate (leases, leader election, config). Build a small embedded Rust library whose only shared dependency is an `object_store` backend.

Decisions: Delta-Lake-style layout (numbered, immutable commit files; compaction by checkpoints); semantics = get/put/delete + CAS on version. Tested backends: `InMemory` and `LocalFileSystem` only; we do not test against real S3. We go one phase at a time, smallest viable step first. Each phase is shippable and has its own tests.

History: Phase 1 used one object replaced with `PutMode::Update`; Phase 2 first split it into a manifest + segments. Both needed conditional *update*, which `LocalFileSystem` lacks (hence the test-only `zinc-fs` crate). We switched to the numbered-commit layout, which needs only `Create`, works on every backend, and leaves no orphan files.

### Reliability rules
Top priority: this crate must not return a wrong answer.
- Prefer an error to a guess. Anything unexpected in the log (gap, unreadable file, unknown format) is a typed error, never treated as "key absent".
- Every write outcome is determinable: a writer recognises its own commit even when the store reports a failure or `AlreadyExists` for a write that actually landed.
- Retries are bounded and backed off; giving up is a typed error.

### Backend requirements
0. **The store never fails us on data.** An object, once written, is never lost, deleted or altered by anyone but this crate. We do not defend against storage losing commits; e.g. a deleted commit hidden behind a lagging listing can neither be detected by reads nor stopped from being re-created by a writer, and that is accepted. Request-level failures (errors, timeouts, lost responses) are still expected and handled as described below. Phase 5 cleanup is the one place where *we* delete commits, so it must protect against exactly that case itself.
1. `PutMode::Create` is atomic: at most one writer creates a given name.
2. Listing returns a contiguous run of commits. A listing with a *hole* would make a `cas` skip the missing commit when checking its precondition, so the code checks contiguity and errors on a hole.
3. Reads of a single object (`HEAD`/`GET`) are read-after-write. A listing may lag (miss the newest commits): after listing, the head is extended by probing `n+1, n+2, …` until a number is free. Commits are only created in order, so the probe finds the true head as of that moment — reads and `Conflict` answers are current even with a lagging listing. (An earlier version of this plan claimed lagging listings were safe without the probe; a review showed a lagging listing could produce a `Conflict` naming a historical version.)

---

## Workspace layout
Cargo workspace (`Cargo.toml` at root, shared `[workspace.dependencies]`), crates under `crates/`. `crates/zinc-kv` is the KV store. (`crates/zinc-fs`, a test-only `ObjectStore` adding `PutMode::Update` to the local filesystem, is unused and being removed; a review also found its conditional updates race with unconditional writes.) File paths below are relative to `crates/zinc-kv/`.

## Phase 1 — Minimum: one object, CAS-safe (done, superseded by Phase 2)
Whole DB in one `state.json`, read-modify-write with `PutMode::Update`. Established the public API:
`Db::open(Arc<dyn ObjectStore>, Path)`, `get`, `put`, `delete`, `cas(key, expected: Option<Version>, new: Option<Bytes>) -> Result<Version, CasError>`. `Version` is an opaque newtype over `u64` (private field); `put` returns `Version`; `Entry { value, version }`. `expected: None` = key must not exist (lock/leader election); `new: None` deletes.

## Phase 2 — Numbered commit log, Delta style (done)
```
<prefix>/log/00000000000000000001.json     one immutable commit per file, 20-digit zero-padded
<prefix>/log/00000000000000000002.json
```
The file number *is* the commit order and the **version** of every key it writes.
- Commit: list `log/`, head = highest number (0 if empty); check the `cas` precondition; `PutMode::Create` of `head+1`. `AlreadyExists` → someone else won: list again, re-check the precondition against only the commits newer than the last one checked, retry with the new head (bounded).
- Read: list `log/`, scan commits newest→oldest until the key is found. A `delete` op is a tombstone: the key reads as absent and counts as "must not exist" for `cas`. Deleting a missing key still writes a tombstone (harmless, gives the delete a definite position in the log).
- Layout test: `tests/layout.rs` on `LocalFileSystem` under `target/tmp/layout`: the prefix holds only the `log/` directory, which holds one file per write.

## Phase 3 — Hardening (done)
Code:
- **Contiguity**: the listing must be exactly `1..=head`; otherwise `Error::MissingCommit`. A file that is listed but then not found is the same error.
- **Strict names**: only `<20 digits>.json` counts as a commit; other objects in `log/` are ignored.
- **Commit format** `{"v":1,"id":"<random 128-bit hex>","ops":[...]}`. Unknown `v` → `Error::UnsupportedFormat`; unparseable file → `Error::Corrupt`. `deny_unknown_fields`.
- **Ambiguous writes**: `id` is unique per `commit()` call and reused across its retries. Any failed `Create` (including `AlreadyExists`) may hide a write of ours — a lost response, a client retry of a request that had landed, a request still in flight. The number is kept as *uncertain* and retried like a lost race; on each pass, every uncertain number the listing now shows as taken is read, and if it carries our `id` the write succeeded. This runs before the precondition check, so a writer never reports `Conflict` against its own commit. A request can only land at the number it named, so no landed write of ours is missed or duplicated.
- **Errors are retried**: `LocalFileSystem` on Windows reports a plain error (staging-file `Access is denied`) instead of `AlreadyExists` when two writers race for the same number — found by running the concurrency tests on disk. So `Create` errors are retried like lost races. When attempts run out, uncertain numbers are settled one last time (a review found the final attempt's failure was never checked). Then: if a failed number is still free, the request might yet land → `Error::OutcomeUnknown` wrapping the cause; otherwise the last store error, or `Contention` if the last failures were lost races. Any error raised while a number is still uncertain (e.g. a failed listing) also becomes `OutcomeUnknown`.
- **Backoff**: full-jitter exponential backoff between attempts (1 ms doubling, capped at 32 ms), bounded by `MAX_ATTEMPTS`.

Test support: `tests/common/mod.rs` with a fault-injecting `ObjectStore` wrapper (pause before a commit `Create`; make the next or every `Create` fail before writing, after writing, report `AlreadyExists` after writing, or land only at the next listing; let a competitor steal every number; fail listing; make a listed file unreadable), plus helpers to run a test on both `InMemory` and `LocalFileSystem`.

The suite was checked by mutation: disabling the contiguity check, the precondition recheck, the uncertain-number check, error retries, the final settle, the forward probe, `deny_unknown_fields` on ops, or the `OutcomeUnknown` mapping each makes at least one test fail.

Tests (each on both backends unless noted):
- **Commit path** (`tests/commit_path.rs`)
  1. Stale `cas` returns `Conflict` with the correct `current` version; `current: None` after a delete.
  2. Delete then `cas(key, None, …)` recreates the key with a higher version.
  3. Retry window: A has checked its precondition; B commits to the same key before A's `Create` → A gets `Conflict` and writes nothing.
  4. Retry window, different key: B changes another key → A rechecks only the newer commit and succeeds with the next number.
  5. Unconditional `put` retries through a lost race.
  6. `cas` delete (`new: None`) with right/wrong version.
- **Failure handling** (`tests/failures.rs`, on disk where files are tampered with)
  7. A hole in the log (missing middle or first commit) makes `get`, `put` and `cas` return `MissingCommit`, never `None`; a commit that is listed but unreadable likewise.
  8. Corrupt commit file or unknown field → `Corrupt`; unknown format version → `UnsupportedFormat`.
  9. Stray objects in `log/` (`notes.txt`, `3.json`) are ignored.
  10. `Create` writes but reports an error → `put`/`cas` succeed exactly once (one commit file).
  11. `Create` writes but reports `AlreadyExists` → success, not `Conflict`.
  12. `Create` fails once without writing → retried, lands once. Fails every time → the store error is returned, the log is unchanged.
  13. `Create` fails, then the request lands before the next listing → claimed as ours: no duplicate, no `Conflict`.
  14. Listing fails → error returned.
  15. Every number is stolen by a competitor → `Contention` after a bounded time.
  - Second review: unconditional writes now decode the head commit before appending (a damaged or newer-format head → `Corrupt`/`UnsupportedFormat`, log unchanged; this is also what stops older writers once a newer format appears). Forward head probing is capped at `MAX_PROBES` past the listing → `Contention`, never an unconfirmed head. Tests: `unconditional_writes_reject_an_unreadable_head`, `continuous_appends_end_in_contention`.
  - Review findings, now regular tests: the final attempt's `Create` lands but reports a failure (error, `AlreadyExists`, delayed) → recognised as success, retry limit measured not assumed; unknown fields inside an op → `Corrupt`; a lagging listing → reads and `cas` see the current state, no false `Conflict` (`commit_path.rs`); `Create` failing at numbers taken by others → plain store error, not `OutcomeUnknown`.
- **Concurrency** (`tests/concurrency.rs`, both backends)
  16. Counter and lock tests.
  17. Mixed workload: concurrent writers on different keys plus deletes; every key ends at its last written value; versions per key strictly increase.
- **Model** (`tests/model.rs`): seeded random sequence of get/put/delete/cas (right and wrong expected versions) through several `Db`s on one store, checked against a `HashMap` model.

## Phase 4 — Read cost: cache and checkpoints
Until now every operation lists the whole log, and a `get` of a missing key (or any `cas(None)` lock attempt) fetches *every* commit. This must be bounded before anything else is built on top.
- In-memory cache of commit files by number (safe since immutable); `list_with_offset` lists only commits newer than what is cached.
- `checkpoint()`: N is always an exact multiple of a fixed `CHECKPOINT_INTERVAL` (e.g. 100), and checkpoint N is only written once checkpoint N − interval exists, so checkpoints exist in order with no gaps (Phase 5 relies on this). Read the previous checkpoint, replay commits up to N, write `checkpoints/<N>.json` (full live state, each key with its original version, no tombstones) with `PutMode::Create`. It is a pure function of the immutable log, so any node may write it and duplicate writers produce identical bytes; it never conflicts with commits.
- Read: load newest checkpoint, replay commits after N; contiguity is then checked from N+1.

## Phase 5 — Cleanup
- `cleanup(retention)`: delete commits ≤ N and older checkpoints only after a long retention period.
- **Hazard — phantom success**: deleting `log/K` makes `Create log/K` succeed again. A writer paused longer than the retention can create `log/K ≤ N`. Readers start from checkpoint N and never see it, so the writer gets `Ok(version)` ("I hold the lock") for a write nobody observes. Re-listing before `Create` does not close this (check-then-act). **Mitigation**: after a successful `Create` of K, the writer sends one `HEAD` for `checkpoints/<C>`, where C is K rounded up to the next multiple of `CHECKPOINT_INTERVAL`. If it exists, the commit may be invisible to readers, so the writer returns an error instead of success. This is sound because commit K can only have been deleted after some checkpoint N ≥ K existed, and checkpoints exist in order, so checkpoint C ≤ N exists too. It deliberately uses a single-object read, not a listing: listings may lag (requirement 3), and a lagging listing could hide exactly that checkpoint (found in review). Retention must still exceed plausible pauses; document the limit.
- Test: a paused writer, cleanup deleting its target commit, a lagging checkpoint listing; the writer must not report success.

## Phase 6 — `list(prefix)` and snapshots
- `list(prefix)` merging newest-wins; `snapshot()` pinning a head number for consistent multi-reads.

## Later (not planned yet)
- Multi-key atomic transactions (a commit file already holds many ops).
- Leases need no store support: put a deadline in the value; other nodes take over with `cas(Some(v))` once it has passed. Document the pattern.
- Watch: poll for `log/<head+1>` with a `HEAD` request rather than a listing.
- Throughput non-goal: all writes serialise on one commit sequence, roughly one store round trip per commit — fine for coordination traffic.
- S3: requirements above; not tested in this project.

---

## Verification
- `cargo test`: the suite runs on `InMemory` and `LocalFileSystem`.
- Concurrency tests are stress-run repeatedly before a phase is marked done.
