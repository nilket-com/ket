# ADR 003 Distributed Runtime Bench Results

**Status:** Implementation evidence, promotion candidate.
**Bench target:** `crates/toron/benches/distributed_runtime.rs`
**Companion plan:** `crates/toron/plans/impl/003_toron_distributed_runtime_and_data_movement.md`

This artifact records the executable ADR 003 evidence pass and the
completion audit for the distributed runtime and data-movement
contract.

## Success Criteria

ADR 003 is complete when Toron can execute bound fragments against
real worker endpoints, move shuffle data through pull-addressed
partition manifests, survive producer-worker loss by invalidating and
redispatching attempts, reject stale output, preserve deterministic
logical output under variable worker assignment, enforce the first
budget envelope, and keep distributed wire / identity / failure
surfaces architecture-guarded.

## Evidence Model

Promotion evidence is split across four layers:

1. **Architecture guards** catch forbidden surfaces: legacy morsel
   wire names, worker-side binding, peer-push shuffle, sleep-based
   backpressure, missing route identity, and task/partition failure
   conflation.
2. **Distributed unit fixtures** exercise the scheduler FSM,
   partition store, worker registry, deterministic hashing,
   canonical Arrow normalization, wire envelope, and budget tracker.
3. **Real-endpoint fixtures** spawn Toron worker processes or TCP
   workers, execute fragment plans, pull partitions by address, and
   cover producer-loss replay after process kill.
4. **Bench rows** record the distributed runtime shape and digest for
   reproducible calibration.

## Phase 0 Audit

- **P0.1 Multi-process harness:** Implemented by
  `crates/toron/src/bin/toron_fixture_worker.rs` and
  `crates/toron/tests/distributed_process_test.rs`. The fixture
  launches real worker processes via `CARGO_BIN_EXE_toron_fixture_worker`
  and terminates the producer worker with `tokio::process::Child::kill`.
- **P0.2 Real-endpoint transport:** Phase 0 selected framed TCP through
  `ToronTransport` / `TcpTransport`. QUIC remains deferred; the ADR
  allowed OS sockets when QUIC dependency cost did not buy immediate
  correctness evidence.
- **P0.3 Spill substrate:** `HybridPartitionStore` selects in-memory
  vs `EncryptedSpillPartitionStore`. Spill uses AES-256-GCM-SIV,
  per-partition nonces, spill-file cleanup, and `SpillBytes` budget
  reservations.
- **P0.4 Fixture matrix:** Implemented fixture rows cover
  multi-process execution, grouped aggregate with shuffle, producer
  process kill + replay, stale-attempt rejection, cancellation cleanup,
  budget exhaustion, deterministic digest comparison, registry
  availability, and strict stage barriers.
- **P0.5 Architecture guards:** `crates/toron/tests/architecture_test.rs`
  now carries the ADR 003 guards over `WireEnvelope`, `TaskIdentity`,
  `PartitionAddress`, no `tokio::time::sleep` in production runtime
  paths, no peer-push shuffle, no worker `Binder::bind`, no distributed
  `MorselId`, no `MorselComplete`, and task-failure taxonomy separation.
- **P0.6 Soft-gate calibration:** Defaults are
  `bytes_in_flight_per_query = 64 MiB`,
  `bytes_in_flight_per_worker_stream = 16 MiB`,
  `memory_per_task = 64 MiB`,
  `shuffle_bytes_published_per_task = 64 MiB`,
  `spill_bytes_reserved_per_query = 512 MiB`,
  `max_arrow_ipc_frame = 16 MiB`,
  `cancellation_grace = 1,000 ms`.
- **P0.7 WorkerRegistry field set:** `WorkerEntry` records
  `WorkerId`, `WorkerEndpoint`, `WorkerCapabilities`,
  `WorkerResourceState`, `WorkerLease`, and `WorkerStatus`.
  `mark_lost` bumps `LeaseEpoch`; heartbeat expiry moves
  `Live -> Suspect -> Lost`.
- **P0.8 Cleanup:** Legacy production `ToronMessage` /
  `MorselComplete` / mock sleep paths are retired. Remaining older
  compatibility tests exercise non-distributed execution surfaces.
- **P0.9 Canonical normalization:** `canonical_normalize` takes
  `SortPolicy::{None, FullRowLex}`. ADR 003 digest fixtures use
  `FullRowLex`: concat -> stable lex full-row sort -> reslice to
  65,536-row batches -> digest.

## Hard Gate Matrix

| # | Gate | Evidence |
|---:|---|---|
| 1 | Multi-process execution | `distributed_process_test.rs` spawns at least two worker processes and runs a grouped aggregate through coordinator-owned fragments. |
| 2 | Bound fragment dispatch | `fragment_plan` emits `TaskSpec { plan: ExecutionPlan }`; architecture guard rejects worker-side `Binder::bind`. |
| 3 | Identity-tagged lifecycle | `WireEnvelope` carries `EnvelopeIdentity::{Task, Partition, Worker}`; lifecycle payloads use `TaskIdentity`; heartbeat uses worker identity by design. |
| 4 | SIGKILL producer-loss keystone | `producer_process_kill_replays_attempt_and_matches_baseline_digest` kills the accepted producer host, invalidates its manifest, redispatches with a bumped `AttemptId`, and matches the baseline canonical digest. |
| 5 | Stale-output rejection | `TaskStateStore` rejects stale completions after invalidation; `PartitionStore` returns `AttemptStale`; TCP pull test maps stale partition pulls to `PartitionStoreMismatch`. |
| 6 | Pull-addressed shuffle observed | Consumer task execution materializes `ProducerPartitionRef` values and issues `PullPartition`; architecture guard rejects peer-push delivery. |
| 7 | Deterministic partition assignment | `partition_bucket` uses stable XXH3-64 over encoded key values; tests pin retry-stable and cross-run-stable partitioning plus canonical digest equality. |
| 8 | Cancellation correctness | `cancel_query` closes scheduler acceptance, partition stores reject late publishes and release reservations, and `CancelTask` round-trips over TCP. |
| 9 | Typed wire errors | `TaskFailureKind` has seven structured variants; `WorkerLost` is coordinator-synthesized only; architecture guard rejects `Error(String)` on distributed public API surfaces. |
| 10 | Backpressure without sleep | `BudgetTracker`, frame-size validation, partition-store budget tests, and architecture guard cover budget rejection without production `tokio::time::sleep`. |
| 11 | Cross-replica determinism | Multi-process digest test and producer-kill digest test compare canonical Arrow IPC digests after concat -> sort -> reslice -> digest. |
| 12 | Strict stage barriers | Scheduler dispatch waits for upstream `AcceptanceState::LiveAccepted`; architecture and scheduler tests reject consumer-before-producer dispatch. |
| 13 | `ExchangeId` manifest coverage | `ExchangeId` is allocated during fragmentation and appears in `PartitionAddress`, `PartitionManifest`, and wire payloads; scheduler rejects exchange mismatches. |
| 14 | Registry availability honored | `StaticWorkerRegistry` excludes `Lost`; scheduler worker-loss tests route replacement attempts to a different live worker. |

## Soft Gates

| Gate | Status |
|---|---|
| Throughput scales 2 -> 4 -> 8 | Not promotion-blocking. The current bench target records the distributed shape and supports `--workers=2,4,8`, but this evidence pass ran the quick two-worker row only. Full scaling is deferred to post-promotion calibration because the seed executor is still deliberately buffered and strict-barriered. |
| Per-query counters recorded | Partially met. Bench rows record worker count, row count, stage count, task count, exchange count, published shuffle bytes, result rows, total count, digest, and wall time. Pulled shuffle bytes, stale rejection count, retry count, and cancellation cleanup count exist in fixtures but are not yet emitted as unified tracing counters. |
| Evidence matrix per fixture row | Met for implemented promotion fixtures in this artifact. |
| Failure-fixture reusability | Met at the harness level: process spawning, worker selection order, producer kill, and canonical digest comparison are reusable in ADR 004 distributed join fixtures. |

## Verification Run

Commands run during this evidence pass:

```text
cargo check --workspace
cargo test --workspace
cargo bench -p toron --bench distributed_runtime --no-run
cargo bench -p toron --bench distributed_runtime -- --quick --distributed-runtime-only
rg -n "TableSchemaDescriptorV1|LocalityPolicyV1" crates -g '*.rs'
rg -n "TableSchemaDescriptorV1|LocalityPolicyV1" crates/toron/plans
```

Observed results:

```text
cargo check --workspace
  finished successfully

cargo test --workspace
  toron unit tests: 112 passed
  toron architecture tests: 17 passed
  toron distributed process tests: 2 passed
  toron Wendelon-backed tests: 5 passed
  wendelon, wendelon-client, and doctests passed

cargo bench -p toron --bench distributed_runtime --no-run
  bench target built successfully

cargo bench -p toron --bench distributed_runtime -- --quick --distributed-runtime-only
  emitted one quick distributed row; 0 failures

rg retired descriptor/locality names in Rust and Toron plans
  no matches
```

Quick distributed bench row:

| Case | Quick | Workers | Rows | Stages | Tasks | Exchanges | Shuffle Bytes Published | Result Rows | Total Count | Wall ms | Digest |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| adr003_distributed_runtime | true | 2 | 8192 | 2 | 3 | 1 | 34576 | 8192 | 8192 | 16.493 | `8cfb2d569db90a4aa2c0d5e3e775c8a2f8a2c304d864716ebb5c707169ae45d5` |

## Fixture Matrix

| Fixture | File | Evidence |
|---|---|---|
| Multi-process grouped aggregate | `crates/toron/tests/distributed_process_test.rs` | Two real worker processes selected across stages; canonical digest and total count verified. |
| Producer process kill replay | `crates/toron/tests/distributed_process_test.rs` | Accepted producer host killed; scheduler invalidates manifest and redispatches replacement attempt; digest equals baseline. |
| Shuffle over TCP pull | `crates/toron/src/lib.rs` | Consumer task pulls `ProducerPartitionRef` data from producer endpoint through `PullPartition`. |
| Stale partition pull | `crates/toron/src/lib.rs` | Stale attempt surfaces as typed partition-store mismatch instead of task failure. |
| Scheduler worker loss | `crates/toron/src/distributed/scheduler.rs` | Lost producer invalidation, consumer cascade, replacement attempt, heartbeat expiry, drained skip, and registry reroute covered. |
| Partition store budgets | `crates/toron/src/distributed/partition_store.rs` | Shuffle/spill reservations release on cleanup; over-budget publish is rejected atomically. |
| Cancellation cleanup | `crates/toron/src/distributed/task_state.rs`, `crates/toron/src/distributed/partition_store.rs`, `crates/toron/src/distributed/scheduler.rs` | Query cancellation closes acceptance, removes manifests, releases store reservations, and rejects late completion/publish. |
| Wire envelope taxonomy | `crates/toron/src/wire.rs`, `crates/toron/src/error/distributed.rs` | Twelve message kinds, route identity, protocol mismatch, frame-size budget error, and worker-emitted failure checks covered. |
| Canonical normalization | `crates/toron/src/distributed/determinism.rs` | Full-row sort before reslice removes row-order and batch-boundary nondeterminism without hiding value drift. |
| Architecture guards | `crates/toron/tests/architecture_test.rs` | G8-G15 plus ADR 002 guards pass. |

## Completion Audit

| Requirement | Evidence |
|---|---|
| Wire message split | `MessageKind` has 12 variants split across task lifecycle and data movement; legacy message wrapper retired. |
| Structured failure taxonomy | `TaskFailureKind`, `PartitionMissingReason`, `RuntimeError`, and `WireError` separate task failure, partition-store miss, protocol mismatch, and frame-size exhaustion. |
| Partition manifests | `PartitionManifest` carries producer worker and endpoint; consumers receive materialized partition refs at dispatch time. |
| Producer-loss invalidation | Scheduler preserves completed attempt audit state, opens acceptance, creates a bumped replacement attempt, removes accepted manifests, and cascades invalidation to dependent consumers. |
| Scoped task acceptance | Acceptance state is indexed by `(QueryId, StageId, TaskId)`, not bare `TaskId`. |
| Query identity | `QueryId` lives in `distributed::identity` and is backed by UUIDv7 for new queries. |
| Shuffle protocol | Worker data movement uses `PullPartition`, `PartitionData`, and `PartitionMissing`; stale attempts are typed. |
| Budget envelope | Query/task budgets are newtyped; frame, shuffle, spill, memory, and bytes-in-flight dimensions share a common tracker. |
| Registry | `StaticWorkerRegistry` carries lease, status, endpoint, capability, and load fields; lost workers are not selected. |
| Strict stage barrier | Scheduler waits on accepted upstream manifests before consumer dispatch. |
| Real endpoints | TCP transport implements the `ToronTransport` seam; process fixtures use real OS sockets. |
| Bench artifact | This file records Phase 0 pins, hard/soft gate evidence, fixture matrix, and verification commands. |

## Residual Watch Items

- Unified runtime telemetry still needs first-class tracing spans for
  pulled shuffle bytes, retry count, stale rejection count, and
  cancellation cleanup count. Fixtures observe those behaviors today,
  but the soft gate wants them as per-query counters.
- Full 2/4/8 worker scaling should be run after ADR 004 chooses the
  next operator surface; strict stage barriers and the buffered seed
  executor make throughput evidence less decision-useful before then.
- QUIC remains a transport-tier candidate for V1.x; ADR 003's
  correctness evidence is intentionally pinned to the transport trait
  and real TCP endpoints.
