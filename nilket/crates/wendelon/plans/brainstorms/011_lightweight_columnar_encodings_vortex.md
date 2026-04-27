# Brainstorm 011: Lightweight Columnar Encodings (The "Vortex" Playbook)

## Scope note (post-043 split)

V1.5 — *enable Arrow IPC's native `BodyCompression` (LZ4)* —
**shipped as ADR 043** (`plans/adr/043_arrow_ipc_body_compression.md`).
Per-table opt-in, layout-driven default
(`MultiRow → Lz4Frame`, `SingleRow → Uncompressed`), readers
trust the Arrow IPC FlatBuffer for format authority, single
coherent compaction (ADR 042 §13) is the convergence primitive.

This brainstorm now covers only V2 and V3: custom SIMD-optimized
encodings (FastLanes / ALP / FSST / RLE), encoding cascades, page
directories for O(1) point reads, and compute-over-compressed-
data. Those remain a multi-quarter design; pull when ADR 043
metrics — `storage_compression_ratio`,
`storage_compression_decode_micros`,
`storage_compression_uncompressed_resident_bytes` — show the
remaining bandwidth/memory headroom is worth the design surface.

**Sibling cold-tier ADR (not part of this brainstorm).**
ADR 044 (proposed) — L2 columnar fetch (sovereign pushdown) —
is a separate cold-tier ADR that uses ADR 043's per-buffer LZ4
as substrate (each Arrow column buffer decompresses
independently from its 64 KiB AEAD chunk). It implements
column-aware Range-GETs against encrypted L2 objects under
ADR 020's zero-trust posture, which is a different problem
from custom encodings. Per-column compression policy belongs
inside the V2 LCE ADR's column-aware encoding cascades, not as
an extension to ADR 043 or ADR 044.

## The Premise
ADR 042 established the physical layout for analytical tables: `[SlabRecordHeader] [Arrow IPC]`. Arrow IPC gives us zero-copy, vectorized execution in `PbcExecutor` and `AGG`, but it leaves massive amounts of NVMe bandwidth and RAM on the table because it is generally uncompressed (or naively dictionary-compressed).

Parquet solves the density problem but ruins the compute path with expensive, CPU-heavy decoding.

Recent state-of-the-art formats like **Vortex** (and academic precursors like BtrBlocks and FastLanes) prove that this is a false dichotomy. By using **Lightweight Columnar Encodings (LCE)**, we can achieve near-Parquet density on disk while decompressing at memory-bandwidth speeds (10x-100x faster than Parquet), and in some cases, evaluating predicates directly on the compressed data.

Following the Wendelon philosophy, we don't want to adopt a massive third-party format library that fights our MVCC, global index, or CSB integration. Instead, we **home-roll a custom LCE tier** transparently inside our slab records, stealing the best ideas from Vortex and Lance.

## The Ideas to Steal

### 1. SIMD-Optimized Lightweight Encodings
Instead of blindly throwing `zstd` or `lz4` at a block of bytes (which forces serial, scalar decompression), we encode columns using algorithms designed explicitly for modern SIMD instructions (AVX-512 / ARM NEON):
- **FastLanes Bit-Packing:** Pack integers into the minimum required bits per value. Unpacking 1024 integers takes a handful of SIMD cycles.
- **ALP (Adaptive Lossless floating-Point compression):** Compresses `f32`/`f64` by finding common multipliers and casting to integers, which are then bit-packed.
- **FSST / String Dictionary:** For strings, use Fast Static Symbol Table or dictionary encoding where the dictionary indices themselves are bit-packed.
- **Run-Length Encoding (RLE):** Extremely effective for low-cardinality columns (and specifically for our synthetic `__row_routing_locality__` which is often identical for an entire fragment).

### 2. Compute Over Compressed Data (COCD)
This is the holy grail for our `PbcExecutor` (ADR 035). If a column is Dictionary-encoded, and the predicate is `WHERE status = 'ACTIVE'`, the executor doesn't decompress the column. It evaluates the predicate *once* against the dictionary, finds the matching integer index, and then executes an ultra-fast integer match against the bit-packed array.
- Our PBC compiler could be extended to emit LCE-aware opcodes: `EVAL_DICT`, `EVAL_RLE`.

### 3. O(1) Point-Read Amplification (The "Lance" Idea)
ADR 042 accepts that point-reads over multi-row fragments require decoding up to `max_record_bytes` (1 MiB).
If we structure our LCE payload with an internal **Page Directory**, a point read for `record_row_index = 405` doesn't have to decompress the whole 1 MiB. It looks up the byte offset for row 405's mini-page and decompresses *only* the 64-value SIMD block containing that row. This gives us analytical scan speeds *without* punishing transactional point-lookups.

### 4. Cascading Encodings
Vortex uses cascading encodings. A column isn't just "dictionary encoded". It might be: `RLE(BitPacked(Dictionary(Strings)))`. We can build a small, Wendelon-native heuristic engine that samples a column chunk during the `pack_record_batch_by_bucket` phase (ADR 042) and selects the optimal encoding cascade before writing.

## How it Fits into Wendelon

1. **Storage-Internal:** This is purely a physical storage optimization. Toron, the network wire, and the logical data model know nothing about it.
2. **The "LCE" Protocol:** To comply with `no-project-name-in-protocols.md`, we name our internal encoding engine `LCE` (Lightweight Columnar Encoding).
3. **The Format Evolution:**
   - **V1 (ADR 042):** We ship raw Arrow IPC. It is fast, correct, and proves the architecture.
   - **V1.5 (ADR 043, shipped):** Arrow IPC native `BodyCompression` (LZ4). Per-table opt-in, descriptor-driven, single-coherent-compaction convergence.
   - **V2 (this brainstorm, future ADR):** The packer compresses the Arrow batch into a custom `LCE` payload for even denser, SIMD-optimized encoding (e.g., ALP, FastLanes). The slab reader decompresses the `LCE` payload back into Arrow IPC just-in-time for the `PbcExecutor`.
   - **V3 (this brainstorm, future ADR):** `PbcExecutor` becomes `LCE`-aware, executing predicates directly on the encoded bytes.

## Why Home-Roll?
- **Zero Bloat:** We only implement the encodings we actually need (e.g., we might skip complex nested-list encodings if our typed tables are mostly flat).
- **Crypto / FIPS Control:** If we use 3rd-party formats, encrypting them usually means encrypting the whole file. By home-rolling, we keep the LCE chunks perfectly aligned with our envelope encryption (ADR 020) and `rustls`/FIPS boundaries.
- **MVCC Synergy:** We can apply specialized encodings to our synthetic columns. `__row_hlc__` is monotonically increasing—perfect for Delta-encoding. `__row_routing_locality__` is often constant—perfect for RLE.

## Next Steps
This remains a brainstorm for V2 / V3. ADR 042 ships the multi-row invariant; ADR 043 shipped LZ4 body compression as the V1.5 step. When post-043 metrics — `storage_compression_ratio`, `storage_compression_decode_micros`, `storage_compression_uncompressed_resident_bytes` — show that LZ4 leaves meaningful bandwidth/memory headroom on real workloads, this brainstorm becomes the foundation for the LCE V2 ADR. V3 (COCD) lands with or after V2.