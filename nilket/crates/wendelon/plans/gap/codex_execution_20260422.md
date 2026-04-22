# Wendelon Gap-Closure Execution Plan

Date: 2026-04-22
Author Role: Lead Software Architect
Source Evaluation: `plans/gap/codex_eval_20260422.md`
Peer Integration: reviewed `plans/gap/claude_execution_20260422.md` and `plans/gap/gemini_execution_20260422.md`; incorporated their gate naming discipline, the requirement to resolve design ambiguity before dependent code lands, a formal security-review checkpoint, and a full teardown-and-restore validation using only documented runbooks.

## Objective

Close the code-level and directly corroborated operational gaps identified in the Codex evaluation so Wendelon can move from:

- roughly `62/100` readiness for an internal/design-partner pilot
- roughly `48/100` readiness for a customer-facing pilot

to a credible external pilot bar with bounded operator intervention, defensible recovery behavior, and no known hot-path architecture mismatches between docs and code.

This plan assumes the target is a customer-facing design-partner pilot, not full GA.

## Hardware Strategy

Wendelon should treat hardware in two layers from the beginning:

- **Commodity default**: cheap VPS and modest single-node or small-cluster deployments are the default operating profile and the baseline cost target.
- **Premium supported**: higher-end NVMe, multi-device, high-core-count hardware is a supported deployment profile on day 1.

The important distinction is:

- day-1 support means Wendelon must run correctly on better hardware without architectural rewrites
- day-2 work means tuning, topology-aware worker placement, and benchmark-backed saturation claims on that hardware

The program must therefore avoid two failure modes:

- building only for cheap hardware and discovering later that the runtime shape does not scale onto better machines
- advertising premium-hardware performance claims before the storage and transport paths are actually ready

## Target Exit State

At the end of this program, the following must be true:

- no known correctness gap remains in the Stellarator write/finalization path
- Raft WAL durability and slab durability are no longer coupled through a single shared `io_uring` worker
- request admission no longer clones full `SmState`
- QUIC listener behavior is bounded under overload and noisy-neighbor conditions
- production key custody exists and `dev-insecure` is no longer the de facto default for serious deployments
- metrics, traces, chaos tests, and simulator coverage exist for the failure modes we are claiming to handle
- audit, node replacement, and DR are documented and tooled well enough that the pilot story is operationally honest

## Program Rules

1. No design claim remains in comments/docs if the code does not implement it.
2. Every hot-path change lands with metrics, tests, and benchmark updates in the same change series.
3. Correctness and crash recovery land before throughput tuning.
4. A customer-facing pilot is blocked until all Gate 3 items in this plan are complete.
5. No dependent implementation lands while its load-bearing semantics are still unresolved; the decision must be locked in code comments, docs, or ADR updates first.
6. No workstream claims completion without a green scorecard slice and a durable gate artifact.

## Duration And Staffing

Recommended duration: `10-12 weeks`

Recommended staffing:

- `1` architecture owner
- `1` storage/runtime lead
- `1` consensus/data-path lead
- `1` network/transport lead
- `1` observability/SRE lead
- `1` security/platform lead
- `1-2` shared engineers for tests, tooling, and integration

## Critical Path

The critical path is:

1. Stellarator correctness hardening
2. request-admission cache instead of `SmState` clone
3. storage execution topology split
4. QUIC admission control and bounded tasking
5. observability expansion
6. validation expansion

Node replacement, audit packaging, DR, and production KMS can run partly in parallel, but the pilot cannot open without them.

## Phased Plan

## Phase 0: Baseline And Gating

Duration: `1 week`

Purpose:

- establish measurement baselines
- prevent regressions while the runtime changes land
- make the remaining work visible

Deliverables:

- benchmark baselines for:
  - Raft commit latency under isolated WAL load
  - Raft commit latency under concurrent slab write load
  - write-path latency through `src/network/data_server.rs`
  - read-path admission latency
- deployment validation on:
  - one commodity profile
  - one premium multi-device/NVMe profile
- fault-injection harness for:
  - crash after data quorum, before finalization
  - local pending-intent persistence failure
  - remote replica stall / timeout
  - transport overload
- a program scorecard in CI tracking:
  - hot-path benchmark deltas
  - chaos pass/fail
  - simulator pass/fail
  - metric surface completeness
- a design-lock decision log for unresolved items on the critical path:
  - Stellarator failure semantics and fencing policy
  - transport truth-up path (`implement` vs `rename/retract`)
  - authoritative audit archive story for the pilot
  - supported node replacement contract
  - initial production root-KEK provider target

Exit criteria:

- baseline numbers are captured and checked into the repo or CI artifacts
- both the commodity and premium support profiles have at least one validated deployment path
- every phase below has an owning engineer and branch plan
- no Phase 1 workstream starts with unresolved critical-path semantics

## Phase 1: Correctness And Crash Recovery

Duration: `2-3 weeks`

This phase is the highest priority. It closes the gaps that can strand data or make pilot incidents unrecoverable.

### Workstream 1: Stellarator Write Path Hardening

Primary code areas:

- `src/network/data_server.rs`
- `src/network/pending_finalizations.rs`
- `src/storage/manager.rs`
- `tests/`

Required changes:

- make pending-finalization intent persistence mandatory for the recoverable branch
- if intent persistence fails:
  - fail the write with a retriable status before claiming success semantics
  - optionally fence or degrade the leader if local persistence is not trustworthy
- start leader-local durable append and remote fan-out concurrently
- make quorum aggregation operate over both local and remote legs
- define explicit write outcome states:
  - `DataBranchPending`
  - `DurableQuorumReached`
  - `FinalizationCommitted`
  - `FinalizationRecoverablePending`
  - `FinalizationTerminalFailure`
- make idempotency cache behavior consistent across restart and in-process retry paths

Tests required:

- crash after durable quorum, before finalization
- pending-intent store failure
- remote replica timeouts while local append succeeds
- duplicate client retry during pending finalization
- restart replay of queued finalizations

Exit criteria:

- no best-effort branch remains after pending-intent persistence failure
- local and remote durability legs are concurrent in code and verified by tests
- 1,000 injected crash/failure iterations complete without stranded quorum-durable writes

### Workstream 2: Admission Cache Instead Of Full `SmState` Clone

Primary code areas:

- `src/network/data_server.rs`
- `src/consensus/raft_storage.rs`

Required changes:

- introduce a narrow read-mostly admission snapshot derived from state-machine apply
- expose the current epoch and any other admission-critical fields through:
  - a `watch` channel
  - or atomics plus immutable snapshot structs
- remove `state_snapshot()` from read/write hot paths
- keep `state_snapshot()` as a test/debug hook only

Tests and benchmarks required:

- benchmark request admission before/after
- concurrency test proving watch/cache stays monotonic with apply order
- read/write integration tests proving stale epoch rejection behavior is unchanged

Exit criteria:

- no read or write request path clones full `SmState`
- admission latency no longer scales with state-machine size

### Workstream 3: Live-Path Panic And Poison Inventory

Primary code areas:

- `src/storage/`
- `src/network/`
- `src/consensus/`
- `Cargo.toml`

Required changes:

- inventory live-path `expect(...)`, `unwrap(...)`, and poisoned-lock assumptions
- convert recoverable cases to structured errors, fencing, or degraded mode
- keep `panic = "abort"` only for invariants that are explicitly audited as unreachable
- add a short design note documenting which invariants remain abort-worthy

Exit criteria:

- no known live-path recoverable condition still aborts the process
- inventory is written down and reviewed

Gate 1: `Correctness Locked`

- Workstreams 1-3 complete
- no external pilot traffic before this gate

## Phase 2: Storage And Transport Hot Paths

Duration: `3 weeks`

This phase closes the largest scale mismatches between design claims and code.

### Workstream 4: Storage Execution Topology Split

Primary code areas:

- `src/node.rs`
- `src/storage/io_uring.rs`
- `src/storage/file_ops.rs`
- `src/storage/core.rs`
- `src/storage/manager.rs`
- `src/metrics/registry.rs`

Required changes:

- split I/O by durability class:
  - dedicated WAL `io_uring` worker
  - dedicated slab `io_uring` workers
  - per-device slab worker mapping where hardware topology exists
- introduce explicit config for WAL vs slab queue depth and worker assignment
- ensure multi-device and higher-core-count systems are supportable through explicit configuration even before automatic topology tuning is complete
- remove `spawn_blocking` from steady-state durability hot paths where feasible
- isolate remaining blocking metadata operations so they cannot interfere with WAL latency
- remove scaffold-grade copies in checksum/hashing paths where straightforward

Metrics required:

- SQ depth
- CQ depth
- submit latency
- CQE wait latency
- WAL append latency
- WAL `fdatasync` latency
- slab append latency

Benchmarks required:

- WAL latency under slab pressure
- slab throughput under WAL pressure
- leader write latency before/after split

Exit criteria:

- WAL and slab paths do not share a single `io_uring` worker
- steady-state write durability no longer depends on `spawn_blocking`
- premium hardware can be deployed with explicit config and correct behavior even if full saturation tuning is still pending
- benchmark shows materially lower coupling between slab load and Raft latency

### Workstream 5: QUIC Admission Control And Transport Truth-Up

Primary code areas:

- `src/network/server.rs`
- `src/network/quic_transport.rs`
- `src/network/transport.rs`
- `src/network/core.rs`
- `src/network/pool.rs`
- `src/network/factory.rs`
- `src/metrics/registry.rs`

Required changes:

- add listener-level bounds:
  - max total connections
  - max connections per peer/IP
  - max streams per connection
  - bounded per-tenant request budgets after auth
  - bounded task fan-out using semaphores or explicit worker pools
- export connection and stream saturation metrics
- decide one of the following and execute it in the same milestone:
  - implement the documented QUIC tuning path and any actual GSO/io-uring integration we intend to claim
  - or rename the transport tier and docs to match the real implementation
- measure `BufPool` contention
- shard or redesign buffer pooling if contention is non-trivial under target load

Tests required:

- overload test with hostile client behavior
- many-streams-per-connection saturation test
- per-tenant fairness test
- topology-change plus migration-read stress scenario

Exit criteria:

- no unbounded per-connection and per-stream task spawning remains
- overload produces bounded rejection/backpressure rather than scheduler collapse
- transport docs no longer over-claim the implementation

Gate 2: `Scale & Survive`

- Workstreams 4-5 complete
- updated benchmark and overload results published

## Phase 3: Protocol Maturity, Observability, And Security

Duration: `2-3 weeks`

### Workstream 6: Protocol And State-Machine Maturity

Primary code areas:

- `src/consensus/raft_storage.rs`
- `src/network/traits.rs`
- `src/client/topology.rs`
- `tests/architecture_test.rs`

Required changes:

- model `SchemaUpdate` in durable state or explicitly remove/disable the placeholder path
- freeze control-ring wire contracts for currently placeholder structs
- replace the `"plane"` word-count architecture test with structural assertions:
  - one authoritative membership graph
  - one epoch source
  - no separate listener topology acting as hidden control/data split

Exit criteria:

- no acknowledged placeholder remains in a customer-facing control-ring path
- architecture tests verify structure, not vocabulary

### Workstream 7: Observability Completion

Primary code areas:

- `src/metrics/registry.rs`
- `src/metrics/`
- hot-path call sites in `src/network/`, `src/storage/`, `src/consensus/`

Required changes:

- add first-class metrics for:
  - pending finalization backlog
  - replay attempts, replay success/failure, replay latency
  - replica fan-out latency
  - quorum acquisition latency
  - active connections
  - active streams
  - request rejection/backpressure counters
  - WAL append and sync latency
  - `io_uring` queue depth and failure counts
  - lock contention or queue saturation where measurable
- add trace spans around:
  - write branch split
  - finalization commit
  - migration-read proxy path
  - transport admission rejections

Exit criteria:

- dashboards and alert proposals exist for every new critical metric
- no critical failure mode from the evaluation remains uninstrumented

### Workstream 8: Production Key Custody

Primary code areas:

- `src/crypto.rs`
- `src/config.rs`
- `examples/wendelon.toml.example`
- tests and any new provider modules

Required changes:

- add at least one production root-KEK provider
  - preferred: cloud KMS first plus a generic enterprise-compatible path
- make `dev-insecure` explicitly dev/test only
- fail fast on production startup if only insecure custody is configured in a production profile
- add rotation and restart tests around the new provider path
- run a focused security review on:
  - OID authorization boundaries
  - KMS/root-KEK custody path
  - audit evidence/archive story for the pilot

Exit criteria:

- a real production custody path exists in code
- pilot guidance no longer relies on plaintext root-KEK storage
- security review findings are closed or explicitly accepted with owner and due date

Gate 3: `Procurement Ready`

- Workstreams 6-8 complete
- this is the minimum gate for any customer-facing pilot
- security review evidence package is available for pilot approval

## Phase 4: Day-2 Operations And Readiness Validation

Duration: `2-3 weeks`

### Workstream 9: Node Replacement, Audit Packaging, And DR

Primary code and doc areas:

- `plans/adr/001_autonomous_healing_boundary.md`
- `plans/ops/audit.md`
- `plans/ops/backup_dr.md`
- `src/audit.rs`
- `src/node.rs`

Required changes:

- define the supported node replacement contract for the pilot:
  - preferred: first-party operator/sidecar
  - fallback: a tightly specified external automation contract with timing SLOs and validation drills
- make the authoritative audit archive story explicit:
  - either ship Kafka with chain fields in the default pilot build
  - or clearly define local NDJSON plus archival shipping as the compliance source of truth
- strengthen backup/DR from node-image guidance to cluster-level procedure:
  - cluster restore choreography
  - remote-tier validation during restore
  - recovery drill procedure
- add operator-facing runbooks for:
  - node replacement
  - pending-finalization backlog / replay failure
  - audit sink failure and archive verification
  - cluster restore

Exit criteria:

- pilot operators have a documented node replacement path with expected timing
- audit retention/evidence path is explicit and not contradictory
- DR drill is documented and testable
- required paging and recovery runbooks exist and are versioned with the codebase

### Workstream 10: Validation Expansion And Pilot Signoff

Primary code areas:

- `tests/chaos.rs`
- `tests/simulator_ladder.rs`
- `tests/architecture_test.rs`
- `ci/check_fips_symbol_sweep.sh`
- any new load/soak harnesses

Required changes:

- expand chaos beyond smoke catalog:
  - partition + restart
  - partition + transport overload
  - disk pressure / persistence failure
  - finalization failure windows
  - topology change + migration-read pressure
- expand simulator ladder beyond one named scenario
- add a deterministic scenario for lazy-migration proxy pressure
- keep the FIPS sweep as a regression test, but document it accurately as a symbol/string sweep
- run soak tests long enough to validate:
  - no unbounded task growth
  - stable metrics under sustained load
  - bounded replay backlog after injected failures
- execute a full teardown-and-restore of a test cluster using only the documented runbooks and automation artifacts produced by Workstream 9

Exit criteria:

- expanded chaos suite is in CI
- simulator ladder covers the core failure matrix
- pilot signoff report is generated from actual test evidence, not design intent
- full teardown-and-restore succeeds without relying on tribal knowledge or ad hoc engineer intervention

Gate 4: `Pilot Authorization`

- Workstreams 9-10 complete
- pilot signoff review can happen
- runbook-only restore and replacement evidence is attached to the review

## Recommended Ordering Inside The Repo

This is the preferred landing order:

1. observability scaffolding for new metrics
2. Stellarator pending-intent and concurrency fixes
3. admission cache refactor
4. panic/poison cleanup in the touched paths
5. storage execution topology split
6. QUIC admission bounds
7. protocol/wire cleanup
8. production KMS provider
9. audit/DR/node-replacement packaging
10. validation expansion and final docs truth-up

## Acceptance Checklist

The program is complete when all items below are true:

- no full `SmState` clone on request admission
- no best-effort pending-finalization persistence branch
- WAL and slab durability no longer share one `io_uring` worker
- transport overload is bounded by explicit admission control
- transport tier naming matches the actual implementation
- live-path panic inventory is resolved or explicitly justified
- production root-KEK provider exists
- security review has been completed for the pilot boundary
- audit evidence path is explicit and operationally supportable
- node replacement path is defined for the pilot
- DR procedure is cluster-scoped rather than node-image-only
- full teardown-and-restore passes using only documented runbooks
- chaos and simulator coverage materially exceed smoke-level

## Go/No-Go Recommendation

Do not open a customer-facing pilot until Gate 3 is complete.

Do not advertise five-nines, NVMe-saturating storage claims, or hardened bifurcated-write recovery until Gate 4 is complete and backed by fresh benchmark/chaos evidence.

An internal pilot may proceed after Gate 2 if:

- production data is not in scope
- operators are on-call and colocated with the engineering team
- `dev-insecure` custody is explicitly accepted for that environment
- no external reliability commitment is made
