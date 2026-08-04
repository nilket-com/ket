# ADR 010: Tiered Serialization Strategy

## Status
Accepted

## Context
Wendelon has a strict "Zero-Copy First" mandate. However, not all data structures in a distributed system are suitable for simple pointer-casting. We need a tiered strategy that balances absolute performance on the hot path with engineering pragmatism and safety on the control path.

We evaluated three primary approaches:
1.  **`bytemuck` (POD Casting):** Zero-cost, but restricted to fixed-size, non-nested structures.
2.  **`rkyv` (Complex Zero-Copy):** Allows zero-copy access to nested/variable data, but introduces significant complexity (alignment requirements, `bytecheck` validation, and "Archived" type mapping).
3.  **`postcard` (Compact Serde):** Fast, stable, and compact serialization, but involves a copy/deserialization step into heap-allocated structs.

## Decision
Wendelon will employ a **Tiered Serialization Strategy**:

1.  **Hot Path Metadata (Labels):** Use **`bytemuck`** for `FrameHeader` and `RequestMeta`. These are 0ns pointer casts.
2.  **Hot Path Payloads (Data):** Use **`Apache Arrow IPC`**. This provides zero-copy columnar access for bulk operations.
3.  **Consensus Path (Control Ring):** Use **`postcard`** for `openraft` RPCs (e.g., `VoteRequest`, `AppendEntries`). 
4.  **On-Disk Index (Future):** Reserve **`rkyv`** for massive on-disk metadata indexes where `mmap`-ability and zero-copy random access are required for fast startup.

## Rationale
- **Pragmatism:** In the Control Ring, the CPU cost of `postcard` (microseconds) is statistically invisible compared to the costs of network round-trips and WAL fsyncs (milliseconds). 
- **Maintainability:** Using `postcard` for Raft allows us to use standard Rust enums and generics without the "Archived" type noise and alignment headaches of `rkyv`.
- **Security:** `postcard` is a safe, validated parser. `rkyv` requires an explicit (and often CPU-heavy) validation pass to prevent segfaults when reading untrusted network bytes.

## Consequences (The "Negatives")
- **Serialization Overhead:** We explicitly accept a non-zero CPU penalty for consensus-related metadata. We bet that this will never be the bottleneck for cluster performance.
- **Dependency Surface:** We are adding `serde` and `postcard` to the dependency tree. 
- **Bifurcation:** Engineers must be aware of which tier a structure belongs to. Adding a variable-length field to a `bytemuck` struct will now cause a compile-time error, forcing a move to either Arrow or a different serialization tier.
