# ADR 007: Raft Implementation Strategy

## Status
Accepted

## Context
Wendelon's "Control Ring" relies entirely on the Raft consensus algorithm for cluster membership, epoch tracking, lease granting, and finalization marker commits. We must decide whether to implement Raft from scratch or adopt an existing Rust ecosystem crate. 
Writing a production-grade, bug-free Raft implementation (handling edge cases like asymmetric partitions, log compaction, snapshot streaming, and joint consensus for membership changes) is a multi-year endeavor that detracts from our core innovation (HashRaft + Zero-copy QUIC data ring).

## Decision
Wendelon will use the **`openraft`** crate for the Control Ring's consensus layer.

## Rationale
- **Maturity:** `openraft` is a highly mature, production-ready framework maintained by the Databend community (which aligns perfectly with our DataFusion/Arrow ecosystem).
- **Asynchronous & Pluggable:** It provides `RaftNetwork` and `RaftStorage` traits. We can implement these traits using our custom `quinn` transport and our zero-copy `bytemuck` WAL format (defined in `raft-core.md`), getting the best of both worlds: our custom transport/storage with their bulletproof state machine.
- **Joint Consensus:** It natively supports the exact joint-consensus membership changes required for our "Epoch Transitions."

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected Custom Raft:** We explicitly reject writing our own Raft from scratch. While a custom Raft could be hyper-optimized for our specific 88-byte `FinalizationMarker` payloads, the maintenance burden and risk of split-brain bugs are unacceptable.
- **API Mapping Overhead:** We must map `openraft`'s generic `AppData` and `AppError` concepts to our strict `bytemuck` structs. This will require some boilerplate mapping layers.
- **Raft Log Constraints:** `openraft` dictates the overall flow of how log entries are compacted and snapshotted. We must adapt our SnapshotHeader logic to fit within `openraft`'s `SnapshotBuilder` trait.