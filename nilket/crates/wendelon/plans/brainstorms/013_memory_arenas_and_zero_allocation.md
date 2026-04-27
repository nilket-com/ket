# Brainstorm 013: Memory Arenas and Zero-Allocation (The "TigerBeetle Diet")

## The Premise

Wendelon is engineered for "First-Person Shooter" latency and massive analytical throughput. We have stripped away legacy protocols (JDBC), heavy middleware (ZooKeeper), and slow codecs. However, the final boss of unpredictable latency remains: **the global memory allocator (`malloc` / `jemalloc`)**.

Dynamic memory allocation on the hot path introduces cache invalidation, thread contention, and non-deterministic latency spikes. Systems like TigerBeetle achieve their legendary predictability by allocating all memory at startup and statically assigning it. 

To achieve our performance goals without sacrificing the dynamic flexibility of an OLAP engine, Wendelon needs a formal strategy for **Memory Arenas and Zero-Allocation** on the data plane.

## 1. The Strict "No Malloc" Boundary

We must draw a hard line between the Control Plane and the Data Plane.

*   **Control Plane (Malloc OK):** Bootstrapping the node, parsing config files, accepting DDL/schema changes, and Raft leader elections. These happen rarely and are not latency-critical to the microsecond level.
*   **Data Plane (Malloc Forbidden):** Receiving a `READ_REQ` over QUIC/rucx, evaluating Vectorized PBC, projecting Arrow columns, and issuing L2 S3 Range-GETs. These paths must operate entirely within pre-allocated arenas.

## 2. Query Arenas (Bump Allocators for PBC)

The **Vectorized PBC Executor (ADR 035)** is the most dangerous source of hidden allocations. Evaluating an expression like `(age > 18) AND (status = 'ACTIVE')` requires allocating intermediate `BooleanArray` masks.

*   **The Problem:** Currently, the Arrow compute kernels allocate brand new buffers for each intermediate step and immediately drop them, trashing the allocator.
*   **The Arena Solution:** We introduce a per-query **Bump Allocator** (e.g., using a crate like `bumpalo` or a custom contiguous block). 
*   **How it Works:** When a query starts, the executor is handed an Arena. As the PBC interpreter runs `arrow::compute::gt` or `arrow::compute::and`, the resulting arrays are carved out of the Arena. 
*   **The Win:** Allocating memory is just adding an integer to a pointer (`O(1)`). Deallocating the entire query's memory is resetting that single pointer to zero (`O(1)`). Zero fragmentation, zero locking.

## 3. Network and Transport Slab Pools

To handle millions of concurrent operations, the network layer must not allocate memory per-packet or per-RPC.

*   **The Problem:** Async Rust frameworks (like `tokio`) often default to allocating `Box` or `Vec` for incoming payload buffers.
*   **The Slab Pool Solution:** Wendelon pre-allocates a massive pool of fixed-size blocks (e.g., 64 KiB chunks, aligning perfectly with ADR 020 AEAD chunking) at startup.
*   **Integration with `rucx`:** This maps perfectly to RDMA/UCX. We register this massive pre-allocated slab as a **Memory Domain** with the NIC. Incoming data is DMA'd directly into these pre-registered blocks. The `READ_REQ` logic borrows a block, processes it, and returns it to the free list.

## 4. Subjugating Apache Arrow

The biggest technical challenge is that `arrow-rs` assumes it can call the global allocator when building arrays.

*   **The Custom Allocator API:** Arrow allows overriding the default memory allocator. We must implement a custom Arrow `BufferAllocator` that is backed by our Query Arenas or Slab Pools.
*   **Zero-Copy Slicing:** When reading from L1 (NVMe) or L2 (S3), we `mmap` or DMA the Arrow IPC payloads directly into our pre-allocated buffers. Arrow `RecordBatch`es must be constructed by taking zero-copy slices (`Arc<Buffer>`) of these pre-existing allocations, never copying the data into newly allocated vectors.

## Open Questions and Next Steps

1.  **The Rust Ecosystem:** Are we building our own slab/arena allocators from scratch, or relying on crates like `bumpalo` and `slab`? (Given the "pure Rust" mandate, we likely want tight control over the exact memory layout).
2.  **Arrow Integration:** How difficult is it to thread a context-aware arena allocator into `arrow::compute` kernels without forking the `arrow-rs` crate? 
3.  **Memory Limits and Spilling:** If a query's intermediate masks exceed its assigned Arena size, do we fail the query (strict bounding), spill to NVMe, or dynamically request a new chunk from a global pool?

## Verdict

Adopting memory arenas is the final architectural step required to guarantee that Wendelon's software layers are as fast and predictable as the hardware they run on. It transforms the engine from a "fast Rust program" into a deterministic, mechanical-sympathy machine.