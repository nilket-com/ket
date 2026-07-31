# Brainstorm 009: Home-Rolled Raft vs. Openraft (The Zero-Copy Dilemma)

## The Premise
Wendelon's entire identity is built around being an aggressively optimized, zero-copy storage engine. However, at the core of our consensus layer, we rely on `openraft`. 

`openraft` is an incredible, production-grade library, but it is designed to be a generic framework. Generic frameworks in Rust usually rely on owned types, requiring `Clone`, `Serialize`, and `Deserialize` bounds on the application data (`AppData`). This means when a network buffer arrives, `openraft` often forces us to allocate memory and copy the bytes into owned Rust structs before they can be appended to the log or applied to the state machine.

This raises a massive architectural question: **Is `openraft` the reason Wendelon isn't truly zero-copy? Should we roll our own Raft?**

## The Temptation: Home-Rolled, io_uring-Native Raft
If we ditch `openraft` and write a bespoke consensus engine, we gain absolute control over the memory layout. 

*   **The Vision:** We could build a Raft engine where the network buffer received from the QUIC stream is passed directly to the `io_uring` submission queue for the WAL append, and the state machine simply stores memory-mapped pointers (offsets and lengths) into that WAL file. 
*   **The Win:** Absolutely zero allocations and zero copies from the NIC to the NVMe drive to the state machine. It would be the fastest, most memory-efficient Raft implementation possible on Linux.

## The `postcard` Discovery
Before we rewrite consensus from scratch, there is a potential lifeline. Wendelon uses `postcard` for serialization. 

`postcard` has zero-copy deserialization capabilities (e.g., borrowing strings or byte slices directly from the underlying network buffer via `&'a [u8]`). If we can pair this with ref-counted byte buffers (like the `bytes` crate's `Bytes` or `Arc<[u8]>`), we might be able to satisfy `openraft`'s `'static` bounds while physically avoiding deep memory copies. 

If we can thread `Bytes` through the `openraft` `AppData` traits, we might get the performance of a home-rolled zero-copy Raft while keeping the safety of a battle-tested library.

## The Reality Check (Why we keep `openraft` for now)

As tempting as a bespoke zero-copy Raft is, we have to look at the macro architecture—specifically **The Stellarator Bifurcated Write Path (ADR 003)**.

1. **Payloads Bypass Raft:** The 1GB Apache Arrow IPC batches *never enter the Raft log*. They are routed directly to the Data Ring via zero-copy QUIC (or RDMA/SPLICE).
2. **Control is Tiny:** The only thing `openraft` is actually replicating is control-plane data: topology changes, tenant key rotations, and tiny 88-byte `FinalizationMarker`s. 
3. **The Micro-Optimization Trap:** Achieving true zero-copy on a 1GB Arrow batch is load-bearing architecture. Doing a memory copy on an 88-byte `FinalizationMarker` is virtually free. If we spend 3 months writing a custom Raft to save a 100-nanosecond memory copy on control-plane metadata, we are focusing on the wrong bottleneck.

Furthermore, Raft is notoriously brutal to get right. Subtle edge cases in joint consensus, membership changes, and log truncation will cause silent data corruption that takes months to debug. As a solo developer, relying on `openraft`'s test suite for correctness is a massive leverage point.

## The Verdict

1. **Stick with `openraft` for Phase 1.** The bifurcated write path protects us from Raft's serialization overhead because the heavy data never touches Raft. 
2. **Experiment with `postcard` Zero-Copy.** We will push `postcard`'s `Bytes` borrowing mechanics to the limit to reduce allocations on the `openraft` `AppData` channel as an optimization, not a rewrite.
3. **The Eject Button.** If, post-GA, Wendelon is processing 500,000 finalizations per second on a $5 VPS and `openraft`'s internal channel cloning becomes the actual CPU bottleneck, *then* we rip it out and drop in our home-rolled, `io_uring`-native Raft. Because we isolated the state machine behind clean traits, swapping the consensus engine later is possible.