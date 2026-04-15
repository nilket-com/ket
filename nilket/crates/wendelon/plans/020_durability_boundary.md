# Plan: Durability Boundary and Quorum Acknowledgement Semantics

## Summary
This plan freezes the write-path durability contract that Workstream A will
implement. The current code accepts a write once a quorum of replicas has
completed `slab.append(...)`, but the plans describe quorum durability, not
page-cache residency. This document resolves that mismatch.

The recommended decision for M1 is:

- The payload durability boundary is the sealed slab file on each replica, not
  a second payload WAL.
- A `QUORUM` write is not eligible for finalization until the payload bytes are
  durably flushed on a quorum of replicas.
- The normal client-visible `WriteStatus::OK` path remains post-finalization:
  payload durable on quorum first, then finalization committed in the Control
  Ring, then response.
- `LEADER` and `NONE` remain conservative in the first execution wave. Until a
  later plan widens the semantics, they may be accepted but must not claim a
  weaker latency/guarantee contract than the code actually provides.

## Motivation
Right now the implementation and the plan corpus make different claims:

- `plans/network/write_flow.md` and the gap assessments describe quorum of
  durable acknowledgements.
- `src/network/data_server.rs` currently treats append completion as sufficient
  to count toward quorum.
- `src/storage/core.rs` and `src/storage/io_uring.rs` already expose the flush
  primitives needed to make the stronger guarantee real.

This is not a product gap. It is a correctness gap. Until the durability
boundary is explicit and enforced, Wendelon cannot honestly claim the write
guarantees described in the plans.

## Goals
- Define exactly what "durable ack" means on the replica path.
- Define exactly what the leader may count toward quorum.
- Keep the payload durability boundary in the data path, not by inventing a new
  payload WAL.
- Preserve the current "OK after finalization" client contract while making the
  pre-finalization quorum step correct.
- Give Workstream A a falsifiable crash-test target.

## Non-Goals
- Throughput optimization beyond what is required for correctness.
- Group-commit tuning across multiple writes.
- Redesigning the Raft finalization marker flow.
- Implementing a distinct low-latency `LEADER` or fire-and-forget `NONE` path
  in the first execution wave.

## Must Settle
1. **Replica durability unit**
   The authoritative durable bytes for a replicated user write are the Arrow IPC
   bytes durably flushed in the destination slab file. Do not add a second
   payload WAL in M1.

2. **Leader quorum counting**
   The leader may only count acknowledgements that explicitly mean "payload
   durable on this replica." Append-complete but not flushed acknowledgements
   must not count toward quorum durability.

3. **Client-visible success point**
   For the current write API, `WriteStatus::OK` remains post-finalization:
   quorum durability first, Control Ring finalization commit second, response
   third. M1 hardens the existing sequence; it does not move the success point
   earlier.

4. **Weaker durability levels**
   `DurabilityLevel::QUORUM` is the only guarantee that M1 must make true.
   `LEADER` and `NONE` must not advertise guarantees that are not implemented.
   The recommended M1 behavior is conservative acceptance that still follows the
   quorum-durable path until a later plan widens the semantics explicitly.

5. **Replica response shape**
   The replica ack path should become explicit about what kind of acknowledgement
   it is carrying. The recommended shape is an `ack_level` or equivalent field
   on `ReplicaWriteRespMeta`, even if M1 only emits the durable form.

6. **Future payload WAL scope**
   If Wendelon ever introduces a second payload WAL to change write latency or
   durability tradeoffs, that is a new architecture decision and must land with
   its own ADR and implementation plan. It must not be introduced implicitly as
   an optimization inside Workstream A.

## Implementation
1. **Freeze the contract in schema comments and plan docs**
   Update the schema comments around `DurabilityLevel`, `ReplicaWriteRespMeta`,
   and the write-flow docs so "QUORUM" unambiguously means durable quorum.

2. **Expose slab flush on the replica path**
   After `slab.append(...)`, the replica path must explicitly drive the required
   flush before returning a quorum-countable ack.

3. **Make the leader count durable acks only**
   The leader's quorum race in `WendelonDataServer::write` must distinguish
   between durable and non-durable acknowledgements.

4. **Preserve the finalization sequence**
   Once durable quorum is reached, the leader continues to submit the
   `FinalizationMarker` to the Control Ring and waits for that commit before
   returning the normal `OK` response.

5. **Add crash-model verification**
   The implementation must include a test that crashes replicas between append
   and flush and proves that only writes with a quorum-durable success path are
   guaranteed to survive.

## Risks and Mitigations
- **Risk: latency regression from per-write flush**
  - **Mitigation:** correctness wins in M1. If latency is unacceptable, add a
    later follow-up plan for group commit or bounded flush batching, but do not
    weaken the correctness contract.

- **Risk: ambiguity around `LEADER` / `NONE`**
  - **Mitigation:** keep M1 conservative. Do not widen semantics until they are
    separately planned and tested.

- **Risk: schema churn on replica responses**
  - **Mitigation:** add explicit ack semantics now so later performance work does
    not require a second wire-shape change.

## Timeline
- M0: approve this contract.
- Workstream A.1: implement durable quorum acknowledgements and crash tests.
- Later follow-up, if desired: widen `LEADER` / `NONE` into distinct latency
  classes once the correctness path is stable.

## References
- `plans/gap/codex_execution_20260415.md`
- `plans/gap/codex_execution_workstream_A_20260415.md`
- `plans/network/write_flow.md`
- `src/network/data_server.rs`
- `src/schema/core.rs`
- `src/storage/core.rs`
- `src/storage/io_uring.rs`
