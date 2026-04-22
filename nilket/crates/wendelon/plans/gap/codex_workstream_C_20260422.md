# Workstream C: Transport, Admission Control, And Runtime Stability

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS2

## Mandate

Remove the largest performance and stability mismatches between the code and the design claims. This workstream is where "high-scale" either becomes real or gets honestly re-scoped.

## Architectural Position

Truth-up comes before tuning theater.

My position is:

- if we do not implement a claimed transport/storage behavior now, we retract or rename the claim now
- WAL durability and slab durability cannot remain coupled through one `io_uring` worker
- request admission must become O(1)-ish in state size
- QUIC overload behavior must be bounded, not aspirational

## Scope

- request-admission cache/watch path
- WAL/slab `io_uring` execution split
- removal of steady-state blocking file operations from hot durability paths
- QUIC admission control
- transport claim reconciliation
- lock contention reduction where benchmarks justify change
- live-path panic and poisoned-lock cleanup in touched runtime paths

## Primary Code Areas

- `src/node.rs`
- `src/storage/io_uring.rs`
- `src/storage/file_ops.rs`
- `src/consensus/raft_storage.rs`
- `src/network/server.rs`
- `src/network/quic_transport.rs`
- `src/network/transport.rs`
- `src/network/core.rs`
- `src/network/pool.rs`

## Non-Goals

- crypto/KMS implementation
- audit schema expansion
- operator/runbook authoring

## Required Design Decisions

1. Do we implement the claimed QUIC `io_uring + GSO` path now, or do we retract the tier claim now?
2. What is the supported hardware-topology model for per-device slab workers?
3. Where do per-tenant backpressure queues live: connection layer, RPC dispatcher, or both?
4. Which `expect`/poison cases remain truly abort-worthy?

## Work Packages

### C1. Admission Path Rewrite

Replace hot-path `SmState` cloning with a narrow watch/cache of admission-critical fields.

Requirements:

- monotonic updates aligned with apply order
- no request path clones full replicated state
- benchmarked allocation reduction

### C2. Storage Execution Split

Implement separate execution domains for:

- Raft WAL durability
- slab durability
- per-device slab workers where hardware topology can be discovered

The design intent must become true in the code, not just in comments.

### C3. Hot-Path Blocking I/O Removal

Audit all current `spawn_blocking` use in steady-state durability flows.

Required outcome:

- no steady-state durability latency depends on general-purpose blocking pools
- remaining blocking operations are isolated and justified

### C4. QUIC Admission And Bounded Tasking

Add:

- max total connections
- max per peer/IP
- max streams per connection
- bounded task fan-out
- per-tenant budgets after authentication

Unbounded `tokio::spawn` on both connection and stream paths is not acceptable for a shared system.

### C5. Transport Truth-Up

Decide once:

- implement the documented path
- or rename the tier and adjust docs/benchmarks

My bias: if the real implementation is just default `quinn` today, say that plainly and stop pretending otherwise.

### C6. Contention And Abort Cleanup

- measure `BufPool` contention before rewriting it
- shard or redesign only if evidence justifies it
- convert recoverable `expect` and poisoned-lock cases to explicit errors or fencing

## Deliverables

- admission watch/cache implementation
- split storage execution model
- updated transport/admission control layer
- transport truth-up document or implementation
- panic/poison inventory note
- benchmark deltas and overload test results

## Exit Criteria

- no request path clones full `SmState`
- WAL and slab durability do not share one `io_uring` worker
- steady-state write durability no longer depends on generic blocking pools
- overload from one peer or tenant produces bounded rejection/backpressure
- transport naming and docs match real behavior

## Recommended Sequencing

1. transport truth-up decision
2. admission cache
3. storage execution split
4. QUIC admission control
5. panic/poison cleanup
6. contention optimization if the benchmarks justify it

## Risks

- admission cache becomes stale or subtly inconsistent with apply order
- per-device worker mapping is harder than the codebase currently assumes
- admission control introduces false positives under legitimate bursts

## Opinionated Constraints

- Do not keep the current transport tier name if the implementation does not earn it.
- Do not optimize the buffer pool before proving it is material.
- Do not accept one noisy peer being able to consume unbounded scheduler and heap resources.

## Open Questions

- Is per-device worker mapping mandatory for Gate 2, or is WAL/slab separation enough for the first customer-facing pilot?
- Should QUIC admission control reject early at accept/open-stream time, or queue and shed later with better attribution?
- How much platform-specific topology discovery are we willing to own in the runtime?
- Which live-path aborts are truly invariant failures, and who signs off on that list?
