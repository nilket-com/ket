# ADR 008: Dependency Selection and Async Runtime Model

## Status
Accepted

## Context
Wendelon requires a specific set of high-performance crates. The choice of async runtime and I/O framework has cascading architectural consequences. For example, using `tokio-uring` forces a thread-per-core (share-nothing) architecture, which contradicts our design goal of isolating the Raft Control Ring onto a dedicated, high-priority thread pool while spreading data ring I/O across remaining cores.

## Decision
Wendelon will standardize on the following core dependency stack:
- **Async Runtime:** `tokio` (multi-threaded scheduler).
- **Consensus:** `openraft` (see ADR-007).
- **Network Transport:** `quinn` (QUIC) + `rustls`.
- **Data Serialization:** `arrow` (IPC/RecordBatch) + `bytemuck` (Zero-copy PODs).
- **Storage I/O:** `io-uring` (the low-level crate, *not* `tokio-uring`).
- **Concurrent State:** `dashmap` (for the Idempotency Cache and Lease tracking).
- **Hashing:** `blake3` (Record identity and default vnode/locality hashing).
- **Cryptography:** `ed25519-dalek` (for Transfer Tokens and Leases).

## Rationale
- **`tokio` + low-level `io-uring`:** By using the low-level `io-uring` crate instead of `tokio-uring`, we can spawn dedicated, blocking I/O threads (or use `tokio::task::spawn_blocking`) that submit batches of SQEs to the kernel. This allows us to keep the standard `tokio` multi-threaded executor for QUIC network handling and Raft heartbeats, preserving our required isolation between the Control and Data rings.
- **`bytemuck` + `arrow`:** Strictly enforces the zero-copy deserialization mandate.

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected `tokio-uring` and `glommio`:** We reject the thread-per-core architectures. While they offer slightly higher theoretical peak throughput by eliminating cross-thread synchronization, they make it impossible to give the Raft state machine global priority over all data-ring I/O.
- **Manual io_uring Management:** Because we are using the low-level `io-uring` crate within `tokio`, we must manually manage the submission (SQ) and completion (CQ) rings, memory lifetimes, and page-aligned buffers. We accept this significant engineering complexity.
