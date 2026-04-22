# Workstream C PR Backlog

Date: 2026-04-22
Author Role: Lead Architect
Parent Plan: `plans/impl/workstream_C.md`
Primary ADRs:
- `plans/adr/009_io_uring_threading_model.md`
- `plans/adr/011_auto_adaptive_transport.md`
Execution Pack: `plans/impl/pr_C1_execution.md`

## Purpose

Turn Workstream C into concrete PRs that decouple the runtime hot paths, bound overload, and truth-match the transport and storage execution claims.

## Backlog Rules

1. Do not change Stellarator semantics in this backlog unless a B dependency explicitly requires a shared seam.
2. Keep executor-topology changes separate from admission-control changes where possible.
3. Any PR that claims performance benefit must land a benchmark or at least a benchmark hook in the same series.
4. Any PR that adds rejection or backpressure behavior must expose enough signal for Workstream D to observe it.
5. Keep doc truth-up as the last PR in the chain, after the implementation is real.

## Recommended Merge Order

1. `C1` Admission snapshot path
2. `C2` WAL and slab execution split
3. `C3` Blocking metadata isolation
4. `C4` QUIC admission limits
5. `C5` Post-auth tenant-aware backpressure
6. `C6` Panic and poison cleanup in touched paths
7. `C7` Runtime and docs truth-up

The first PR should be `C1` unless Workstream B is already mid-flight on a shared `data_server.rs` branch. If both start at once, land `B1` first, then `C1`.

## PR C1: Admission Snapshot Path

### Goal

Remove hot-path full `SmState` cloning from request admission and replace it with a narrow snapshot or watch-driven view of admission-critical fields.

### Primary Files

- `src/network/data_server.rs`
- `src/consensus/raft_storage.rs`
- `src/node.rs`

### Code Reality To Target

Hot `state_snapshot()` reads are currently visible in `src/network/data_server.rs` at multiple admission and handler boundaries, including around:

- lease or epoch gate checks
- write admission
- migration and locality checks

The problem is broader than `server.rs`; the backlog should focus on the `data_server.rs` call sites first.

### Allowed Changes

- define a narrow admission snapshot type
- expose it via `watch`, atomics plus immutable snapshots, or an equivalent monotonic mechanism
- replace hot `state_snapshot()` calls on the request path where only a subset of fields is needed

### Do Not Bundle

- WAL/slab topology changes
- QUIC limits
- broad control-server snapshot cleanup

### Required Tests

- microbenchmark before and after on admission path
- monotonicity test that admission snapshots do not move backward relative to apply order
- unchanged rejection behavior for stale epoch or lease-gate cases

### Merge Criteria

- no hot request-admission path clones full `SmState`
- admission-critical fields are available from a narrow read-mostly path

### Main Review Risk

This PR can expand into “replace every `state_snapshot()` in the repo.” Do not do that. Focus on the hot data-path call sites first.

## PR C2: WAL And Slab Execution Split

### Goal

Stop sharing a single `io_uring` worker between WAL durability and slab I/O.

### Primary Files

- `src/node.rs`
- `src/storage/io_uring.rs`
- `src/storage/manager.rs`
- `src/consensus/raft_storage.rs`

### Code Reality To Target

Bootstrapping currently creates one shared `io_uring` thread in `src/node.rs` and passes it to both slab storage and durable Raft storage. That is the first coupling to remove.

### Allowed Changes

- provision distinct WAL and slab execution handles
- add explicit config for day-one supported multi-path behavior
- keep automatic topology discovery out of this PR

### Do Not Bundle

- blocking metadata cleanup beyond what is strictly required
- QUIC load shedding
- pool sharding or unrelated memory tuning

### Required Tests

- WAL latency under concurrent slab load benchmark
- correctness tests for WAL append/flush with separate handles
- commodity and premium-supported profile smoke coverage

### Merge Criteria

- WAL and slab no longer share one execution worker by default
- the configuration surface makes the split visible and deployable

### Main Review Risk

It is easy to “split” the handles in names while still sharing a bottleneck deeper down. Review should verify actual separate handles and call paths.

## PR C3: Blocking Metadata Isolation

### Goal

Move or isolate blocking file-system work so it cannot stall the WAL path.

### Primary Files

- `src/storage/file_ops.rs`
- `src/storage/manager.rs`
- `src/consensus/raft_storage.rs`

### Code Reality To Target

`src/storage/file_ops.rs` still routes a large amount of metadata work through `spawn_blocking_io`, including:

- directory fsync
- rename
- create_dir_all
- remove_file
- metadata checks

That does not all need to disappear immediately, but it must stop interfering with durability-critical flow.

### Allowed Changes

- classify blocking operations by criticality
- isolate metadata work from the WAL path
- move only the necessary calls in this PR

### Do Not Bundle

- executor split redesign
- panic cleanup unrelated to moved operations

### Required Tests

- regression tests for rename and fsync-parent-dir correctness
- benchmark or targeted timing test showing WAL path is no longer coupled to known metadata operations

### Merge Criteria

- remaining blocking metadata work is no longer on the WAL critical path
- the separation is visible in code structure

### Main Review Risk

The danger is cargo-culting everything onto new threads without understanding which operations actually gate durability. Keep the classification explicit.

## PR C4: QUIC Admission Limits

### Goal

Add hard limits and early rejection behavior for QUIC connection and stream acceptance.

### Primary Files

- `src/network/server.rs`
- `src/network/quic_transport.rs`
- `src/network/core.rs`
- `src/metrics/registry.rs`

### Code Reality To Target

`src/network/server.rs` currently accepts connections and spawns per-stream tasks without meaningful admission caps. That is the immediate overload boundary to fix.

### Allowed Changes

- total connection limit
- per-peer or per-IP connection limit
- per-connection stream limit
- bounded task fan-out
- explicit rejection metrics

### Do Not Bundle

- tenant-aware fairness
- major protocol framing changes
- transport-family expansion beyond QUIC

### Required Tests

- limit enforcement tests for connection count
- limit enforcement tests for stream count
- rejection behavior tests that confirm the server sheds load instead of amplifying it

### Merge Criteria

- server-side admission is explicitly bounded
- saturation produces controlled rejection behavior

### Main Review Risk

Reviewers should make sure limits apply before per-stream work fan-out becomes the problem, not after.

## PR C5: Post-Auth Tenant-Aware Backpressure

### Goal

Prevent one authenticated tenant or noisy peer from consuming the entire dispatcher or pool capacity.

### Primary Files

- `src/network/server.rs`
- `src/network/data_server.rs`
- `src/network/pool.rs`
- `src/metrics/registry.rs`

### Allowed Changes

- tenant- or actor-keyed queueing after authentication
- token-bucket or bounded-queue enforcement
- backpressure metrics and rejection reasons

### Do Not Bundle

- broad quota model changes
- auth model redesign
- new transport framing

### Required Tests

- multi-tenant fairness tests
- noisy-neighbor isolation tests
- response or metric assertions for throttled and rejected work

### Merge Criteria

- one authenticated tenant cannot monopolize dispatcher capacity
- rejection or throttling reason is visible enough to operate

### Main Review Risk

Backpressure can silently become unbounded queueing with a prettier name. Review should insist on hard bounds.

## PR C6: Panic And Poison Cleanup In Touched Paths

### Goal

Convert recoverable runtime failures in the touched B/C hot paths from panics or poison assumptions into structured failure or degraded behavior.

### Primary Files

- `src/storage/file_ops.rs`
- `src/storage/io_uring.rs`
- `src/network/server.rs`
- `src/network/pending_finalizations.rs` only if touched by nearby runtime work

### Allowed Changes

- remove recoverable `expect` or `unwrap` in touched paths
- document the truly abort-worthy invariants that remain
- convert poison assumptions where the runtime can recover

### Do Not Bundle

- repo-wide panic cleanup
- semantic changes unrelated to the touched runtime paths

### Required Tests

- regression tests for formerly panicking recoverable paths
- explicit degraded or error-path tests where behavior changes

### Merge Criteria

- touched runtime paths no longer abort on recoverable conditions without explicit justification

### Main Review Risk

This PR can become a vague cleanup bucket. Keep it scoped only to code touched by `C1-C5` and any adjacent high-risk helpers.

## PR C7: Runtime And Docs Truth-Up

### Goal

Align comments, docs, config descriptions, and support claims with the implementation that now exists.

### Primary Files

- `src/storage/io_uring.rs`
- `plans/adr/009_io_uring_threading_model.md` only if tiny wording sync is needed
- `plans/adr/011_auto_adaptive_transport.md` only if tiny wording sync is needed
- any nearby operator or implementation docs that still over-claim

### Allowed Changes

- update comments and operator-facing wording
- document explicit configuration for premium-supported hardware
- remove or retract stale transport or execution claims

### Do Not Bundle

- new runtime behavior
- large roadmap rewrites

### Required Tests

- no code tests required if this PR is documentation-only
- if config descriptions move with code, include config parsing or validation tests as needed

### Merge Criteria

- comments and docs describe the real runtime shape rather than the aspirational one

### Main Review Risk

Do not use this PR to silently change behavior. It is a truth-up pass only.

## Hand-Off To Workstream D

Workstream D should shadow this backlog with:

- admission latency benchmark deltas for `C1`
- WAL versus slab contention metrics for `C2-C3`
- rejection and saturation metrics for `C4-C5`
- panic and degraded-path evidence for `C6`

## Recommended First PR

Start with `C1` if Workstream B is not already in `data_server.rs`.

If both workstreams begin together, start with `B1`, then land `C1` immediately after. That sequence minimizes semantic conflict while still attacking the biggest allocation hotspot early.
