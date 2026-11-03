# Production readiness checklist

Where `zinc-kv` stands against production use. Checked items are done and tested; unchecked items are missing. Design details are in [PLAN.md](PLAN.md).

**Verdict:** not production ready. The two blockers are bounded read cost and validation on the real backend.

## Correctness: write protocol

- [x] One winner per commit number via atomic `PutMode::Create`
- [x] `cas` precondition re-checked against newer commits after a lost race
- [x] Writer recognises its own commit after a failed, `AlreadyExists`, or late-landing `Create` (no false failures, no duplicates)
- [x] Final attempt's failure settled before giving up
- [x] `Error::OutcomeUnknown` when a write may still land; docs say a later read is not conclusive
- [x] Bounded retries with jittered backoff → `Contention`
- [x] Unconditional writes refuse to append after an unreadable or newer-format head

## Correctness: reading the log

- [x] Holes in the listing → `MissingCommit`, never read around
- [x] Lagging listings compensated by probing forward; reads and `Conflict` answers are current
- [x] Head probing bounded → `Contention` under endless appends
- [x] Strict commit format: version field, unknown fields rejected (commit and op level)
- [x] Stray files in `log/` ignored

## Testing

- [x] Every test runs on `InMemory` and `LocalFileSystem`
- [x] Fault-injecting store: failed, delayed and misreported writes; failing and lagging listings; vanished and damaged files
- [x] Concurrency tests on both backends: counter, lock, mixed workload
- [x] Model-based test against a `HashMap` (sequential)
- [x] Mutation check: disabling any safety mechanism fails at least one test
- [ ] Tests against S3 / MinIO in CI
- [ ] Linearizability checker for concurrent operation histories
- [ ] Long-running soak test with many nodes

## Performance and cost

- [ ] **Blocker:** bounded read cost. Today every operation lists the whole log, and a missing key or `cas(None)` reads every commit (Phase 4: checkpoints)
- [ ] In-memory commit cache and `list_with_offset` (Phase 4)
- [ ] Measured request count and latency per operation on S3
- [ ] Estimate of request cost under polling load
- [ ] Documented throughput limit (all writes share one commit sequence)

## Storage

- [ ] Checkpoints (Phase 4)
- [ ] Cleanup of old commits (Phase 5)
- [ ] Phantom-success protection during cleanup: `HEAD` of the next checkpoint after each `Create` (designed in PLAN.md, not built)

## Backend

- [x] Backend requirements written down (README, PLAN.md)
- [x] `InMemory` and `LocalFileSystem` verified, including Windows `LocalFileSystem` race behaviour
- [ ] **Blocker:** S3 behaviour verified: conditional-put errors, consistency, how `object_store` maps them
- [ ] Other backends (GCS, Azure, R2) evaluated

## Features

- [x] `get`, `put`, `delete`, `cas` with opaque, ordered `Version`
- [ ] Lease or TTL support: a crashed lock holder holds the lock forever (pattern noted in PLAN.md only)
- [ ] `list(prefix)` and snapshots (Phase 6)
- [ ] Watch for changes (planned: poll for the next commit with `HEAD`)
- [ ] Multi-key transactions

## Operations

- [x] README with usage, guarantees and errors
- [ ] Generated API docs reviewed
- [ ] Logging and tracing
- [ ] Metrics (retries, contention, outcome-unknown count)
- [ ] Versioned release and changelog
- [ ] Format migration plan for commit format v2
