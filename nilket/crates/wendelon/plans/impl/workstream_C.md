# Workstream C: Transport, Admission Control, And Runtime Stability

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS2`
Derived Backlog: `plans/impl/workstream_C_backlog.md`

## Objective

Turn the runtime into a bounded, truth-matched system whose hot-path behavior matches the architecture claims for transport and storage execution.

## Why Now

The current gap is not just "needs tuning." It is structural:

- request admission still does expensive state-shaped work
- WAL and slab durability are too coupled
- QUIC overload posture is not bounded enough
- some comments and planning language still outpromise the implementation

This workstream removes those mismatches.

## ADR Anchors

- ADR `009`
- ADR `011`
- ADR `027`
- ADR `030`

## Scope

- admission snapshot/watch path
- WAL/slab execution split
- isolation of blocking file-system work from durability-critical paths
- QUIC admission controls and fairness
- runtime panic/poison cleanup in touched paths
- transport/storage claim truth-up

Primary code areas:

- `src/node.rs`
- `src/storage/io_uring.rs`
- `src/storage/file_ops.rs`
- `src/consensus/raft_storage.rs`
- `src/network/server.rs`
- `src/network/quic_transport.rs`
- `src/network/transport.rs`
- `src/network/core.rs`
- `src/network/pool.rs`

## Out Of Scope

- write-semantics ownership
- custody/provider implementation
- audit event-model expansion
- operator runbook authoring

## Dependencies

- `A` for claim discipline and milestone gating
- `B` for stable write semantics
- `D` for metrics, overload evidence, and benchmark deltas

## Entry Criteria

- the admission-critical fields are identified
- the current `SmState` clone call sites are enumerated
- the WAL path and slab path interference points are identified
- the intended hardware profiles are documented under ADR `030`

## Implementation Slices

### C1. Admission Snapshot Path

Replace hot-path full-state cloning with a narrow admission snapshot exposed through:

- `watch`
- atomics plus immutable snapshots
- or an equivalent monotonic mechanism

The target property is admission work that does not scale with full replicated-state size.

### C2. WAL And Slab Execution Split

Implement distinct execution domains for:

- WAL append/flush
- slab I/O

on day one through explicit configuration, with a path to better hardware support without redesign.

### C3. Blocking Metadata Isolation

Inventory remaining blocking file operations and move or isolate them so they cannot stall the WAL durability path.

If some operations still require blocking threads temporarily, they must be bounded and visibly off the critical path.

### C4. QUIC Admission Limits

Add hard limits for:

- total connections
- per-peer or per-IP connections
- streams per connection
- bounded task fan-out

The server should reject or shed load predictably instead of amplifying it.

### C5. Post-Auth Fairness And Backpressure

Add fairness and backpressure after authentication so one tenant or one noisy peer cannot consume unbounded dispatcher or pool capacity.

### C6. Panic And Poison Cleanup

Audit touched runtime paths for:

- recoverable `unwrap` or `expect`
- poisoned-lock assumptions
- abort-worthy invariants that are not truly invariants

Convert recoverable cases into structured failure, degraded mode, or bounded rejection.

### C7. Documentation Truth-Up

Update runtime-facing docs and comments so:

- QUIC is the real day-one transport
- better hardware is supported by explicit configuration
- future transport ambition is not described as current support

## Tests And Benchmarks

- admission-path microbenchmark before/after snapshot rewrite
- apply-order monotonicity test for the admission snapshot
- WAL latency under concurrent slab-load benchmark
- overload and rejection behavior tests at connection and stream layers
- queue-depth and saturation metric validation
- panic/poison regression tests for touched failure paths
- benchmark runs on:
  - commodity default profile
  - premium-supported profile

## Exit Criteria

- no request hot path clones full `SmState`
- WAL and slab execution are isolated enough that slab pressure no longer directly controls WAL tail latency
- overload is bounded through explicit limits and tested rejection paths
- recoverable live-path panics in touched areas are removed or explicitly justified
- transport and execution-topology claims are aligned with the implementation

## Artifacts

- admission snapshot design note
- execution-topology note
- overload-limit matrix
- benchmark delta report
- panic/poison disposition note

## Deferred Follow-Ons

- automatic topology discovery for premium hardware
- more aggressive pool sharding only if benchmarks justify it
- future transport expansion beyond QUIC after real implementation and measurement
