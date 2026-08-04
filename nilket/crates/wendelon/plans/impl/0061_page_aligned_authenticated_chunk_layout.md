# Implementation Plan: ADR 061 — Page-Aligned Authenticated Chunks with Macro-Block Read Coalescing

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR:
`plans/adr/0061_page_aligned_authenticated_chunk_layout.md`

Inherits substrate:

- `plans/adr/0020_l2_envelope_encryption_and_dek_lifecycle.md` —
  per-slab DEK + 64 KiB chunked AEAD; deterministic chunk-ordinal
  nonces; 48-byte fixed AAD layout; `slab_max_bytes ≤ 2^35` cap.
- `plans/adr/0022_l2_slab_format_footer_indexed_range_reads.md` —
  slab header / footer byte layout; `chunk_size_bytes` as a u32
  field; Open Question 4 anticipating variable-per-slab chunk size.
- `plans/adr/0044_l2_columnar_fetch.md` — chunk-aligned
  authenticated range-read primitive; `TieredMultiRangeReadPlan`
  contiguous-chunk-run coalescer; ADR 044 §7 "fetching more bytes
  must not turn a corruption signal into a successful query" rule.
- `plans/adr/0056_row_group_fetch_and_page_directory.md` —
  `RecordPageDirectory`; per-row-group plaintext byte ranges;
  three-stage planner.
- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingDirectory`, `BufferEncoding.page_end_offsets`,
  `EncodingPageDescriptor`, `EncodingWriterConfig::
  target_simd_page_values`, deterministic cascade selection.
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` — sparse
  projection-fetch planner; Phase 7 ciphertext-byte gate
  withdrawal recorded as ADR 061 substrate evidence.
- `plans/conventions/no-planning-phase-names.md`,
  `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/no-project-name-in-protocols.md`,
  `plans/conventions/capability-vs-feature-naming.md`.

## Status

Council-converged across Claude / Codex / Gemini,
implemented, and promoted to shipped. Full Phase 9 bench
evidence at
`plans/impl/0061_page_aligned_authenticated_chunk_layout_bench_results.md`
clears the shipped-status gate. Phase 0 no-directory
feasibility, Phase 5 macro-block coalescing, and the full
verification gate all passed before enabling small chunks by
default.

Implementation contract — council-converged:

- closed chunk-size set: `{4096, 8192, 16384, 32768, 65536}`
  bytes, encoded as `SLAB_ENCRYPTION_CHUNK_SIZES`;
- per-slab variable `chunk_size_bytes` carried in existing slab-
  header / footer field (no slab-format-version bump);
- writer-side pure-deterministic selector consuming
  `EncodingDirectory` + `L2ChunkLayoutConfig`, geometry-keyed
  off maximum per-page byte size in default-engaged buffers,
  with a V1 promoted floor of 16 KiB
  (`max(max_page_bytes, PROMOTED_FLOOR)`); 4 KiB and 8 KiB
  remain in the closed set as architecturally permitted and
  diagnostic values, but are not selectable by the V1 selector;
- default selection: 16 KiB for encoded slabs, 64 KiB baseline;
- buffer-start alignment to chunk boundaries via opaque pad
  bytes in IPC body slack *between* buffers (no intra-buffer
  pad — ADR 057's contiguous `page_end_offsets` cannot
  represent intra-buffer gaps);
- alignment discipline applies only to default sparse engagement
  set (`BitPacked`, `Dictionary { inner: Plain | BitPacked }`);
  other cascades share the per-slab chunk size but emit
  unpadded buffer starts;
- **macro-block coalescing test is a hard gate**: a 100 MiB row
  group + 16 KiB chunks must produce ONE `get_range` call on
  dense fetch;
- **warehouse-scale bench fixtures are mandatory**: 16M+ row
  runtime + 100M+ row sampled;
- no new fallback / reject / catalog variant; only
  `SlabFormatError::UnsupportedChunkSize` on bad
  `chunk_size_bytes`;
- no encrypted `AuthenticatedChunkDirectory` (deferred to V2);
- no `base_chunk_index` per `BufferEncoding` (not needed with
  fixed-size chunk math);
- byte-identical projected-batch invariant pinned across every
  chunk-size value;
- ADR 020 AAD + nonce layout unchanged;
- ADR 022 / 056 / 057 byte layouts unchanged.

## Goal

Make ADR 060 sparse projection fetch reduce encrypted ciphertext
bytes proportionally to selected page bytes (not chunk-floored),
while preserving warehouse-class analytics throughput on dense
paths.

The architecture realizes this by separating two layers:

```text
Layer 1: AEAD authentication boundary (atomic decrypt unit)
  - 16 KiB default; closed set {4, 8, 16, 32, 64} KiB.
  - Buffer starts align to chunk boundaries via inter-buffer
    pad bytes in IPC body slack. Pages within a buffer remain
    contiguous (no intra-buffer pad — ADR 057's prefix-end
    page_end_offsets cannot represent intra-buffer gaps).
  - Sparse fetches decrypt only the chunks containing selected
    pages.

Layer 2: Physical network read unit (transfer unit)
  - Coalesced via existing TieredMultiRangeReadPlan.
  - 1–100 MiB GETs on dense paths via contiguous-chunk
    coalescing.
  - AES-NI loops over chunks in CPU caches after each GET.
  - No new code; verification gate that scaling holds at small
    chunk size.
```

Today the writer + crypto path:

- emits all slabs with 64 KiB chunks regardless of geometry;
- the reader reads `chunk_size_bytes` from the slab footer and
  validates it equals 65536 (the constant).

ADR 061 changes:

```text
EncodingDirectory + L2ChunkLayoutConfig
  → choose_chunk_size_bytes(...) -> ChunkSizeBytes ∈ closed set
    (with V1 PROMOTED_FLOOR = 16 KiB)
  → writer walks buffers in IPC body order maintaining a
    running plaintext cursor; for each default-engaged buffer
    (BitPacked / Dictionary { Plain | BitPacked }) it inserts
    opaque pad bytes in inter-buffer slack so that buffer's
    encoded_range.start lands on a chunk boundary; non-
    engaged buffers (Plain / Lz4Frame / Delta / Fsst / Alp /
    Rle) emit at the running cursor (may be mid-chunk; ADR
    060 routes them dense). Intra-buffer page_end_offsets are
    unchanged (ADR 057's contiguous prefix-end shape preserved)
  → encrypts under per-slab DEK with chunk_size_bytes-grain chunks
  → slab carries chosen value in header[48..52] + footer[48..52]
  → reader reads chunk_size_bytes; validates ∈ closed set
  → crypto uses chunk_size_bytes for chunk arithmetic
  → ADR 060 sparse projection fetch ciphertext floor drops from
    64 KiB to chosen size (typically 16 KiB)
  → existing coalescer compresses contiguous selected chunks
    into single get_range calls; dense paths still produce
    macro-block GETs
```

## Deliverables

- `src/crypto.rs` — replace fixed 64 KiB chunk-size constant
  uses with per-slab values:
  - retain the current default as
    `SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES = 65_536`;
  - introduce
    `SLAB_ENCRYPTION_CHUNK_SIZES: [usize; 5] =
        [4096, 8192, 16384, 32768, 65536]`;
  - thread `chunk_size_bytes: ChunkSizeBytes` through every
    `plan_tiered_range_read` / `plan_tiered_multi_range_read`
    call site;
  - validate `chunk_size_bytes ∈ SLAB_ENCRYPTION_CHUNK_SIZES`
    on slab-footer parse; return
    `SlabFormatError::UnsupportedChunkSize` otherwise.
- `src/schema/units.rs` — `ChunkSizeBytes` newtype routed
  through `impl_unit!`.
- `src/storage/encoding/chunk_layout.rs` (new) —
  `L2ChunkLayoutConfig` + `choose_chunk_size_bytes(...)` +
  `estimate_pad_inflation(...)` + writer-side
  `align_buffers_to_chunks(...)`. Pure module; no I/O.
- `src/storage/encoding/directory.rs` — encoding-directory
  build path consults the chunk-layout selector; for each
  default-engaged buffer it rounds the running plaintext
  cursor up to the next chunk boundary and records that as
  `encoded_range.start` (with pad bytes filling the gap).
  Non-engaged buffers' `encoded_range.start` records the
  running cursor unchanged (may land mid-chunk). Intra-
  buffer `page_end_offsets` are unchanged from ADR 057's
  contiguous prefix-end shape in either case.
- `src/storage/encoding/cascade/codec.rs` — **no change
  required** for V1; pages remain emitted contiguously
  within a buffer.
- `src/storage/encoding/page_materialize.rs` — reader-side
  unchanged: pad bytes live in IPC body slack *between*
  buffers (not between pages); `decode_page` consumes only
  bytes within `BufferEncoding.encoded_range`, so pad bytes
  don't enter the decode path. ADR 057's `expand_page_descriptors`
  is unchanged.
- `src/storage/encoding/sparse_projection_fetch.rs` —
  `MAX_SPARSE_RANGES_PER_BUFFER` and per-buffer caps remain
  plaintext-grain; chunk size flows through transparently from
  the slab metadata.
- `src/storage/manager.rs` — passes `chunk_size_bytes` through
  every existing `crypto.plan_tiered_*_range_read(...)` call
  site (these already exist; the change is to read the value
  from the slab metadata rather than the constant).
- `src/storage/columnar.rs` — same; ADR 044 columnar fetch
  becomes chunk-size-aware.
- `tests/encoding_chunk_layout.rs` (new) — selector unit tests
  + cross-replica determinism + chunk-size matrix property
  suite.
- `tests/crypto_macro_block_coalescing.rs` (new) — **the load-
  bearing new test**: a 100 MiB plaintext range with 16 KiB
  chunks must produce ONE `get_range` call.
- `tests/architecture_test.rs` — ADR 061 lockdown guards.
- `tests/row_group_sparse_projection_fetch.rs` — extension
  cases at small chunk sizes.
- `tests/encoding_secondary_index.rs` — verify ADR 052 point-
  read SIMD-page fast path at every chunk size.
- `tests/l2_remote.rs` / `tests/l2_rehydration.rs` — 64 KiB
  baseline slabs read identically; new slabs at smaller chunk
  sizes round-trip.
- `benches/row_group_fetch.rs` — extended with chunk-size
  matrix axis + warehouse-scale fixtures (16M+ row runtime,
  100M+ row sampled).
- `plans/impl/061_*_bench_results.md` — checked-in evidence
  artifact.
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` — §"Where
  we are" amendment after Phase 9 promotion (closes the
  recorded-not-gated ciphertext-ratio follow-up).
- `plans/wip/roadmap.md` — planning-cadence update at promotion
  (not before).

## Non-goals

- No new public PBC opcodes; ADR 035 / 036 byte-identical.
- No `RecordBodyCompressionPolicy` variant.
- No new `L2FetchStrategy` variant.
- No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
  bit / IPC `custom_metadata` key / digest domain.
- No new `EncodingRejectCode` / `EncodingFallbackReason` /
  `RowGroupFallbackReason` / `RowGroupRejectCode` /
  `EncodingComputeRejectCode` / `EncodingComputeFallbackReason` /
  `PredicateAtomOp` / `EncodedRowMask` / `EncodingCascadeKind` /
  `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
  variants.
- No new fields in `EncodingWriterConfig`,
  `L2EncodingFetchConfig`, `L2RowGroupFetchConfig`,
  `EncodingComputeConfig`.
- No `chunk_size_*` operator-config field outside
  `L2ChunkLayoutConfig`.
- No per-cascade flag, no per-record activation knob.
- No Cargo feature gating chunk-size variability.
- No nightly rustc.
- No nullable / variable-width sparse path changes.
- No descriptor surface change.
- No `ObjectStore` trait change.
- No envelope `format_version` bump; ADR 022 byte layout
  unchanged.
- No retirement of the current/default 64 KiB chunk path.
- No migration sweeper for existing slabs; ADR 055 LSM
  compaction picks up smaller chunk sizes on natural recompacts.
- No Toron-visible contract change.
- No new metric label.
- No silent dense fallback after pad accounting fails; failure
  is a writer-time decline that keeps the slab at 64 KiB.
- No internal decline reasons leaking to public surface.
- No encrypted `AuthenticatedChunkDirectory` — V2 territory.
- No `base_chunk_index` per `BufferEncoding` — not needed with
  fixed-size chunk math.
- No row-group-aligned AEAD chunks — would conflate AEAD with
  network layers; coalescer handles that decoupling.
- No `ObjectStore::get_ranges` multi-range method — existing
  `get_range` + coalescer is sufficient.

## Phase 0 — Contract freeze + AES-GCM safety bound + decoupling check + no-directory feasibility

Lock the closed surface and verify the cryptographic safety
bound across the proposed chunk-size set. Confirm the existing
`TieredMultiRangeReadPlan` coalescer can handle the small-chunk
regime structurally. **Formally prove the no-directory path** —
that ADR 022's existing `chunk_size_bytes` field plus ADR 056 /
057 directories can express the V1 layout without a new
encrypted `AuthenticatedChunkDirectory`. **If Phase 0 feasibility
fails, this implementation plan stops** and the council returns
to the ADR with revised scope. V1 does not silently reshape to
include the encrypted directory under this plan; that would
violate the V1 non-goals and architectural lockdown guards.
Adopting the encrypted-directory approach would be a council
decision producing a revised ADR/impl pair, not an in-flight
phase change.

Files (read-only):

- `plans/adr/0019_cipher_selection_and_fips_posture.md`,
  `plans/adr/0020_l2_envelope_encryption_and_dek_lifecycle.md`,
  `plans/adr/0022_l2_slab_format_footer_indexed_range_reads.md`,
  `plans/adr/0044_l2_columnar_fetch.md`,
  `plans/adr/0061_page_aligned_authenticated_chunk_layout.md`.
- `src/crypto.rs` (audit `TieredMultiRangeReadPlan` and
  `plan_tiered_multi_range_read` for any structural assumption
  that scales poorly at small chunk sizes — e.g., `SmallVec`
  inline capacity, hard-coded chunk count limits).
- `src/storage/manager.rs` (audit hard-coded 65536 references).

Work:

1. Confirm ADR 020 §2 invariants hold at every chunk size:
   - per-slab DEK scope: unchanged;
   - 48-byte AAD layout: unchanged;
   - chunk-ordinal nonce: unchanged;
   - per-key data limit: at `slab_max_bytes = 2^35` and chunk
     size 4 KiB, chunk count is `2^35 / 2^12 = 2^23` chunks
     per slab. u64 chunk_index has plenty of room. AES-GCM
     data limit (2^36 bytes per key) is a per-key limit, not
     per-chunk; unaffected by chunk size.
   - AEAD-tag overhead at each chunk size:
     `4 KiB → 0.39%, 8 KiB → 0.20%, 16 KiB → 0.10%,
      32 KiB → 0.05%, 64 KiB → 0.024%`.
2. Confirm ADR 022 byte layout admits the change:
   - `slab_header[48..52]` carries `chunk_size_bytes` as u32;
   - footer mirror at `slab_footer[48..52]`;
   - reader cross-checks header vs footer;
   - ADR 022 Open Question 4 explicitly anticipates variable
     per-slab chunk size.
3. Confirm `TieredMultiRangeReadPlan.chunk_runs` data structure
   scales: `SmallVec<[ChunkRun; 8]>` inline capacity is fine
   even at small chunk size, because `chunk_runs` represents
   *coalesced* contiguous chunk index ranges, not individual
   chunks. A 100 MiB dense fetch at 16 KiB chunks = 1
   contiguous chunk run, not 6400.
4. Confirm ADR 060 sparse projection fetch's planner computes
   chunk arithmetic from the slab's `chunk_size_bytes`, not
   from a hard-coded constant.
5. Pin the closed set: `[4096, 8192, 16384, 32768, 65536]`.
6. Pin the V1 default policy:
   - `enabled = true` after Phase 9 evidence;
   - `max_pad_inflation_micros = 50_000` (5%);
   - default chunk size: 16 KiB for slabs with
     `EncodingDirectory`; 64 KiB for slabs without.
7. **No-directory feasibility check.** Formally document why
   the V1 layout does not need a new encrypted directory:
   - **Plaintext-to-chunk-index mapping** is fixed-size math:
     `chunk_index = plaintext_offset / chunk_size_bytes`. The
     reader has `chunk_size_bytes` from the slab header (already
     today); plaintext offsets come from ADR 056
     `RecordPageDirectory.row_groups[*].byte_range` and ADR 057
     `BufferEncoding.encoded_range` (no change).
   - **Inter-buffer pad bytes** are *implicit* — they live in
     the IPC body slack between
     `BufferEncoding[i].encoded_range.end` and
     `BufferEncoding[i+1].encoded_range.start`, which is already
     present in the IPC body layout (the alignment of buffers
     is a writer-time decision, recorded only in the
     `encoded_range.start` of each subsequent buffer). The
     reader's `expand_page_descriptors` and `decode_page`
     consume only bytes within `encoded_range`; pad bytes never
     enter the decode path. **No metadata change is required**
     because the pad bytes' existence is fully described by the
     gap between adjacent `encoded_range` values.
   - **No intra-buffer pad bytes.** ADR 057's
     `BufferEncoding.page_end_offsets` is a contiguous prefix-
     end array — pages within a buffer have no gap
     representation (`src/storage/encoding/page.rs:6-35`'s
     `expand_page_descriptors` computes each page's byte range
     as `end - previous_byte`; intra-buffer gaps would corrupt
     decoded values). V1 therefore aligns *buffer starts* to
     chunk boundaries, not individual page starts. Page-to-
     chunk alignment is a consequence when pages are chunk-size
     divisors (typical for the default sparse engagement set);
     non-divisor cases pay bounded boundary over-fetch but
     don't regress below the 64 KiB baseline.
   - **Per-buffer chunk index ranges** are derivable in O(1)
     from `BufferEncoding.encoded_range` + `chunk_size_bytes`.
     No `base_chunk_index` field needed.
   - **Variable chunk boundaries within a slab** are *not*
     needed at V1. ADR 044 column-fetch's per-record fraction
     at 16 KiB chunks is ~2.3% on a 1-of-50 warehouse-scale
     projection, well below ADR 044's 67%
     `max_selected_fraction_micros` threshold. See ADR
     §"ADR 044 columnar-fetch composition" for the per-
     projection-width table.
   - **Failure condition**: if any of the above proves wrong
     (e.g., readers need ciphertext-offset info that ADR 056 /
     057 directories don't carry; pad-byte placement varies
     non-deterministically across replicas; ADR 044 over-fetch
     exceeds the 67% gate at 16 KiB on representative
     warehouse fixtures), **Phase 0 fails and the implementation
     plan stops**. The council must produce a revised ADR/impl
     pair (likely adopting the encrypted-directory approach) as
     a separate decision. Phases 1–9 of this plan do not
     execute until the failure is resolved at the ADR level.

**Exit gate:** AES-GCM safety bound documented for every chunk
size in the closed set. ADR 020 / 022 / 056 / 057 byte layouts
unchanged. `TieredMultiRangeReadPlan` data structure scales to
small-chunk regime. **No-directory feasibility check committed
to bench artifact** — either passes (V1 proceeds to Phase 1) or
fails (implementation stops, council produces a revised ADR/impl
pair). The plan does not silently reshape under failure.
Pre-061 surface inventory recorded.

## Phase 1 — `ChunkSizeBytes` newtype + crypto-path threading

Add the newtype, rename the constant, thread per-slab chunk
size through every crypto path. **Behavior-neutral when chunk
size is 64 KiB.**

Files:

- `src/schema/units.rs` — add `ChunkSizeBytes` via `impl_unit!`.
- `src/crypto.rs` —
  replace the old fixed chunk-size constant with
  `SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES`; add
  `SLAB_ENCRYPTION_CHUNK_SIZES: [usize; 5]`; replace every
  internal use of the constant with a function parameter /
  slab-metadata read; add slab-footer parse validation.
- `src/storage/manager.rs` / `columnar.rs` /
  `encoding/page_materialize.rs` /
  `encoding/sparse_projection_fetch.rs` — every
  `crypto.plan_tiered_*_range_read(...)` call site already
  passes the slab's metadata; verify the chunk size flows
  through.
- `tests/architecture_test.rs` — guard that
  `SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES` is not used as the
  chunk-arithmetic value on read/decrypt/plan paths that must
  consume slab metadata.

Work:

1. Rename and add the closed-set constant.
2. Plumb `ChunkSizeBytes` through:
   - slab-header / footer parse;
   - tiered-range-read planners
     (`crypto.plan_tiered_range_read`,
      `plan_tiered_multi_range_read`);
   - manager / columnar / encoding sites.
3. Validate `chunk_size_bytes ∈ SLAB_ENCRYPTION_CHUNK_SIZES`
   at footer parse; return `SlabFormatError::UnsupportedChunkSize`
   for unknown values.
4. **Behavior preservation**: every existing test passes
   without modification.

Tests:

```sh
cargo test                       # full suite must pass unchanged
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test architecture_test
```

**Exit gate:** No behavior change. Crypto path is chunk-size-
parametric. Hard-coded constant access eliminated outside
legitimate slab-format-version validation uses.

## Phase 2 — `L2ChunkLayoutConfig` + selector

Add the writer-side policy and the pure-deterministic selector.
**Selector returns 64 KiB until Phase 4 wires it to the writer.**

Files:

- `src/storage/encoding/chunk_layout.rs` (new):

  ```rust
  pub struct L2ChunkLayoutConfig {
      pub enabled: bool,
      pub max_pad_inflation_micros: u32,
  }

  impl Default for L2ChunkLayoutConfig {
      fn default() -> Self {
          Self {
              enabled: true,                               // shipped default
              max_pad_inflation_micros: 50_000,            // 5%
          }
      }
  }

  // V1 promoted floor. 4 KiB and 8 KiB members of the closed
  // set are architecturally permitted and diagnostic values,
  // but not selectable by the V1 selector.
  pub(crate) const PROMOTED_FLOOR: ChunkSizeBytes =
      ChunkSizeBytes::new_unchecked(16_384);

  pub(crate) fn choose_chunk_size_bytes(
      encoding_directory: Option<&EncodingDirectory>,
      config:             &L2ChunkLayoutConfig,
  ) -> ChunkSizeBytes {
      const DEFAULT: ChunkSizeBytes = ChunkSizeBytes::new_unchecked(65_536);
      // ... selector body per ADR draft §V1 scope §3:
      //   if !config.enabled or directory is None: return DEFAULT
      //   max_page_bytes = max page byte size across default-
      //     engaged buffers
      //   threshold = max(max_page_bytes, PROMOTED_FLOOR.get())
      //   candidates = SLAB_ENCRYPTION_CHUNK_SIZES filter |c| c >= threshold
      //   chosen = candidates.first()  // smallest passing member
      //   if pad_inflation(chosen) > config.cap: return DEFAULT
      //   chosen
  }

  pub(crate) fn estimate_pad_inflation(...) -> u32 { ... }
  ```
- `tests/encoding_chunk_layout.rs` (new) — selector unit tests.

Work:

1. Define `L2ChunkLayoutConfig`. **Not** part of
   `EncodingWriterConfig`; sibling. Cross-replica byte-equality
   requires every replica's config agree.
2. Implement `choose_chunk_size_bytes(...)` per the selector
   pseudocode in ADR §V1 scope §3:
   - returns the 64 KiB baseline when `enabled = false` or no
     encoding directory;
   - computes
     `threshold = max(max_page_bytes_in_default_engaged_buffers,
                      PROMOTED_FLOOR)`
     where `PROMOTED_FLOOR = 16 KiB` for V1;
   - picks the **smallest** chunk size in the closed set that
     is ≥ `threshold`;
   - falls back to the 64 KiB baseline when no closed-set member is
     ≥ `threshold` or when pad inflation exceeds
     `config.max_pad_inflation_micros`.
   The PROMOTED_FLOOR is what keeps 4 KiB and 8 KiB out of V1
   selection while still permitting them in
   `SLAB_ENCRYPTION_CHUNK_SIZES` for Phase 9 diagnostic rows
   and architectural-test coverage.
3. Implement `estimate_pad_inflation(...)`: pure computation
   over the directory and the candidate chunk size. The
   estimator mirrors Phase 3's writer-side logic — it walks
   every buffer in IPC body order maintaining a running
   plaintext cursor, and counts pad bytes only at default-
   engaged buffer starts (where the writer would insert
   alignment pad). Specifically, for each buffer in IPC body
   order:
   - if the buffer's cascade is in the default sparse
     engagement set: `pad_for_this_buffer = round_up(cursor,
     chunk_size) - cursor`; advance
     `cursor = round_up(cursor, chunk_size) + encoded_range.len`;
   - otherwise: no pad; advance
     `cursor = cursor + encoded_range.len`.
   Return total pad bytes across the slab as a fraction of
   total record-body bytes in micros. The cursor-based walk
   correctly accounts for non-engaged buffers' lengths
   pushing eligible-buffer boundaries off the chunk grid; a
   simple per-buffer modulo would be wrong for any record
   that mixes engaged and non-engaged cascades.
4. **Pure-determinism property test**: same inputs → same
   output, byte-deterministic. No randomness, no time, no
   thread state.

Tests:

- selector returns 64 KiB when `enabled = false`;
- selector returns 64 KiB when no encoding directory;
- selector returns **16 KiB** on typical UInt8 / UInt16 / UInt32
  BitPacked + Dictionary fixtures (max page byte size ≤ 16 KiB,
  PROMOTED_FLOOR pulls the threshold up to 16 KiB);
- selector **does not return 4 KiB** even on UInt8 BitPacked
  (1 KiB pages) — the PROMOTED_FLOOR forbids it in V1;
- selector **does not return 8 KiB** even on UInt16 BitPacked
  (2 KiB pages) — same reason;
- selector returns **16 KiB** on UInt64 BitPacked (8 KiB pages,
  threshold = max(8 KiB, 16 KiB) = 16 KiB; smallest chunk size
  ≥ 16 KiB is 16 KiB);
- selector returns **32 KiB** on a fixture with > 16 KiB pages
  (e.g., a hypothetical 24 KiB page; threshold = max(24 KiB,
  16 KiB) = 24 KiB; smallest chunk size ≥ 24 KiB is 32 KiB);
- selector returns 64 KiB on fixture with > 64 KiB pages
  (pages too large for any small chunk);
- selector returns 64 KiB when pad inflation > config cap;
- byte-identical selector output across 100 randomized
  directory inputs;
- selector output stable across two threads on the same input.

**Exit gate:** Selector lands; pure-deterministic; returns 64
KiB on every call until Phase 4. No write-path behavior change
yet.

## Phase 3 — Buffer-alignment writer mechanics

Implement buffer-start-to-chunk alignment in the encoded-
buffer write path. **No intra-buffer padding** — ADR 057's
`page_end_offsets` is a contiguous prefix-end array
(`src/storage/encoding/page.rs:6-35`) and cannot represent
gaps between pages within a buffer; pad bytes go in inter-
buffer slack only. **Pure mechanics; no behavior change
until Phase 4 wires the selector.**

Files:

- `src/storage/encoding/chunk_layout.rs` —
  `align_buffers_to_chunks(...)` writer-side helper.
- `src/storage/encoding/directory.rs` — `BufferEncoding`
  records `encoded_range.start` for each default-engaged
  buffer at a chunk-boundary offset (after writer-inserted
  inter-buffer pad); non-engaged buffers' `encoded_range.start`
  records the running plaintext cursor unchanged. Intra-
  buffer `page_end_offsets` are
  unchanged from ADR 057's contiguous prefix-end shape.
- `tests/encoding_chunk_layout.rs` — alignment property tests.

Work:

1. After ADR 057 cascade selection finalizes per-buffer
   encoded ranges, call `align_buffers_to_chunks(...)`. The
   helper walks every buffer in IPC body order to maintain a
   correct running plaintext cursor, but **only inserts pad
   for default-engaged buffers**:
   - Initialize `cursor` at the IPC body start (or the
     position the prior IPC metadata window ends at).
   - For each buffer in IPC body order:
     - if the buffer's cascade is in the default sparse
       engagement set (`BitPacked`,
       `Dictionary { inner: Plain | BitPacked }`), compute
       `pad = round_up(cursor, chunk_size_bytes) - cursor`;
       emit `pad` opaque bytes (`0x00`) in IPC body slack
       before this buffer; record this buffer's
       `encoded_range.start = cursor + pad`;
       advance `cursor = encoded_range.start + encoded_range.len`;
     - otherwise (non-engaged cascade — `Plain`, `Lz4Frame`,
       `Delta`, `Fsst`, `Alp`, `Rle`), emit no pad; record
       this buffer's `encoded_range.start = cursor` (which
       may be mid-chunk); advance `cursor =
       encoded_range.start + encoded_range.len`. ADR 060
       routes these cascades dense, so mid-chunk starts are
       acceptable.
   - Intra-buffer `page_end_offsets` remain ADR 057's
     contiguous prefix-end array (no change to that shape).
2. **No intra-buffer pad** in any cascade. Pages within a
   buffer remain contiguous — page `i` starts where page
   `i-1` ends. The reader's `expand_page_descriptors` and
   `decode_page` paths are unchanged.
3. **Pad bytes are AEAD-authenticated** by the enclosing
   chunk's existing AEAD tag — no separate auth surface, no
   separate digest.
4. **Pad bytes are not part of any logical buffer range.**
   The reader sees them as slack between adjacent
   `encoded_range` values; `decode_page` consumes only bytes
   within `encoded_range`, so pad bytes never enter the
   decode path.
5. **Cross-replica determinism**: pad placement is a
   deterministic function of (per-buffer `encoded_range.len`,
   in IPC body order) + (per-buffer cascade kind, to decide
   eligibility) + `chunk_size_bytes`.
6. Track pad-byte total and assert it stays under
   `config.max_pad_inflation_micros` post-construction. If
   exceeded, the slab falls back to 64 KiB and re-emits the
   layout without pad (one bounded retry).
7. **Page-to-chunk alignment** within a buffer is a
   *consequence* of buffer-start alignment, not a guarantee:
   - For chunk-size-divisor pages (BitPacked UInt8 / UInt16 /
     UInt32 / UInt64 with byte-aligned bit widths; Dictionary
     plain low-card with 1-byte indices), page starts land on
     chunk boundaries naturally.
   - For non-divisor pages (sub-byte BitPacked widths,
     Dictionary indices with non-power-of-2 bit widths),
     pages straddle chunks. ADR 060 sparse projection fetch
     still benefits from the smaller chunk floor (16 KiB vs
     64 KiB) on these cases — selected pages map to 1–2 chunks
     instead of 1–2 64-KiB chunks. Phase 9 measures the
     boundary-over-fetch on representative non-divisor
     fixtures.

Tests:

- writer emits buffer-aligned chunk boundaries when chunk size
  < 64 KiB and `enabled = true`;
- writer falls back to 64 KiB when pad inflation exceeds cap
  on retry;
- pad bytes are present in IPC body slack between buffers but
  absent from any logical buffer's `encoded_range`;
- intra-buffer `page_end_offsets` are unchanged across chunk
  sizes — ADR 057 directory shape preserved exactly;
- decoder reads the slab and produces byte-identical decoded
  output regardless of chunk size or pad placement;
- cross-replica determinism: two writers given the same input
  produce identical bytes;
- chunk-size-divisor pages naturally align (BitPacked UInt8 /
  16 / 32 / 64 + Dictionary plain 1-byte indices);
- non-divisor pages straddle chunks but decode correctly.

**Exit gate:** Writer can emit buffer-aligned slabs at small
chunk sizes when hand-configured; reader produces byte-
identical output.

## Phase 4 — Wire selector into write path

Connect Phase 2's selector to Phase 3's mechanics. **First
phase where behavior changes when config is enabled.**

Files:

- `src/storage/encoding/directory.rs` — invoke
  `choose_chunk_size_bytes(...)` after `EncodingDirectory`
  finalizes; pass result to `align_buffers_to_chunks(...)` and
  thread into the encryption call.
- `src/crypto.rs` — encrypt path takes `ChunkSizeBytes`
  parameter (already plumbed in Phase 1).

Work:

1. After ADR 057 directory finalizes, the writer asks the
   selector for a chunk size.
2. If selector returns < 64 KiB, the writer applies page
   alignment (Phase 3) and encrypts with the chosen chunk size.
3. If selector returns 64 KiB, the writer follows the baseline
   path byte-identically.
4. The chosen chunk size is recorded in `slab_header[48..52]`
   and `slab_footer[48..52]` per ADR 022 §3 / §5.

Tests:

- end-to-end writer test with `enabled = true` and analytics
  geometry produces 16 KiB-chunk slab;
- end-to-end writer test with explicit `enabled = false`
  produces 64 KiB-chunk slab byte-identical to the baseline;
- mixed slab corpora (some 64 KiB baseline, some new small):
  reader produces byte-identical output.

**Exit gate:** Writer can emit small-chunk slabs in production
code paths when config is enabled.

## Phase 5 — Macro-block coalescing verification (load-bearing test, four sub-properties)

**The load-bearing architectural test.** Without this the
architecture fails: small AEAD chunks that produce small GETs
would regress every analytics workload (and would explode S3
GET request cost — see ADR §"Worked example: S3 cost on a
100 GiB analytic scan"). Four sub-properties together address
the council's "tiny crypto chunks not a license" concern. Each
sub-property is a separate test assertion; all four must pass
for ADR 061 to ship.

**The coalescer this test verifies already exists** at
`src/crypto.rs:3396 chunk_read_runs_for_plaintext_ranges`,
lines 3417–3422 (adjacent chunk-range merging), with an
existing test at line 6078
(`adr044_multi_range_coalesces_overlapping_and_adjacent_chunk_runs`).
Phase 5 verifies that this production coalescer scales to the
small-chunk regime — turning thousands of contiguous 16 KiB
chunks into single MiB-class `get_range` calls.

Files:

- `tests/crypto_macro_block_coalescing.rs` (new).
- `src/storage/manager.rs` / `src/storage/columnar.rs` —
  audit only; no changes expected if the coalescer scales
  correctly.

### Sub-property 1: dense fetch produces a single coalesced GET

1. Construct a `MockObjectStore`-backed slab with:
   - 100 MiB plaintext record body;
   - `chunk_size_bytes = 16 KiB`;
   - `EncodingDirectory` with default-engaged buffers;
   - synthetic content (deterministic).
2. Issue a dense projection fetch covering the entire record
   body.
3. Assert:
   - `MockObjectStore` observes EXACTLY ONE `get_range` call;
   - the GET range is approximately 100 MiB ciphertext (100
     MiB + AEAD tags ≈ 100.1 MiB);
   - the AES-NI decrypt path successfully verifies all 6400
     chunks (100 MiB / 16 KiB);
   - the decrypted plaintext matches the input byte-for-byte.
4. Repeat with chunk sizes {4 KiB, 8 KiB, 32 KiB, 64 KiB} —
   assert one GET in every case.

### Sub-property 2: AEAD-call count and CPU% bounded

1. On the same fixture, instrument the AEAD-decrypt path to
   count `aes_gcm_decrypt(...)` invocations.
2. Assert:
   - AEAD-call count == chunk count (linear in chunk count, no
     pathological multiplication);
   - AEAD CPU wall time < 1% of materialize wall time at 16 KiB;
   - AEAD CPU wall time < 2% at 4 KiB (the smallest chunk size
     in the closed set).
3. **This sub-property addresses the council's "tiny crypto
   chunks not a license" concern empirically.** AES-NI setup
   amortizes; tag work is data-bandwidth-bound; per-chunk
   overhead is bounded.

### Sub-property 3: clustered sparse fetch coalesces correctly

1. On the same fixture, issue a sparse projection fetch
   covering 5% of the record body in ~5 contiguous regions
   (each region ~1 MiB).
2. Assert:
   - `MockObjectStore` observes ~5 `get_range` calls (one per
     contiguous region);
   - each GET's ciphertext bytes ≈ region size + AEAD tags;
   - NOT 6400 / 20 = 320 individual GETs;
   - NOT 1 GET (i.e., the planner did not over-coalesce
     across non-contiguous regions).

### Sub-property 4: anti-clustered sparse fetch bounded by ADR 060 cap

1. On the same fixture, issue a sparse projection fetch
   covering 50 non-contiguous pages (perfectly anti-clustered).
2. Assert:
   - `MockObjectStore` observes a GET count bounded by ADR 060's
     `MAX_SPARSE_RANGES_PER_BUFFER = 8` cap (× per-buffer
     count);
   - GET count ≤ ADR 060's existing floor at the same
     selectivity (no regression);
   - if the sparse plan would exceed the cap, ADR 060's
     existing `SparseProjectionFetchDecline::PerBufferFragmentationCapExceeded`
     kicks in and the path declines to dense — **chunk size
     does not change ADR 060's decline behavior**.

Tests:

```sh
cargo test --test crypto_macro_block_coalescing
```

**Exit gate:** All four sub-properties pass at every chunk
size in the closed set. ADR 061's two-layer architecture is
empirically sound; the tiny-chunk concerns raised during
council convergence are empirically addressed.

## Phase 6 — ADR 060 sparse projection fetch composition

Verify ADR 060's sparse projection-fetch path runs correctly
at small chunk sizes. **Mostly verification.**

Files:

- `src/storage/encoding/sparse_projection_fetch.rs` — audit
  only.
- `tests/row_group_sparse_projection_fetch.rs` — extend with
  chunk-size matrix axis.

Work:

1. Audit the planner: hard-coded 65536 references must already
   be eliminated (Phase 1).
2. The decline reasons in `SparseProjectionFetchDecline` are
   unchanged; caps remain plaintext-grain.
3. Verify byte-identical projected-batch output across chunk
   sizes.
4. Verify the existing economic gate (`sparse_bytes <
   dense_bytes`) continues to operate on plaintext bytes; the
   ciphertext savings show up in `get_range_bytes` counters
   via the smaller chunk floor.
5. **The win shape on the original ADR 060 fixture**:
   16 records × 64 KiB at chunk_size_bytes = 16 KiB → each
   record body = 4 chunks. Sparse 5% selection clustered into
   1 chunk → 4× ciphertext-byte reduction. (At 4 KiB chunks,
   16× reduction.)

Tests:

- sparse projection fetch at {4, 8, 16, 32, 64} KiB chunks
  produces byte-identical projected batches;
- ciphertext-byte counter shows expected reduction at small
  chunk sizes on the original ADR 060 fixture;
- decline reasons unchanged across chunk sizes;
- existing ADR 060 property suite passes at every chunk size.

**Exit gate:** ADR 060 sparse projection fetch is chunk-size-
agnostic on output; chunk-size-sensitive on ciphertext-byte
counters.

## Phase 7 — ADR 052 / 056 / 057 / 058 / 059 regression neutrality

Verify every consumer of the chunk-aligned authenticated range-
read primitive continues to work at small chunk size.

Files (audit only):

- `src/storage/manager.rs` —
  `build_encoding_indexed_page_plan(...)`,
  `materialize_encoding_indexed_pages(...)`,
  `pick_point_row_group_fetch_strategy(...)`;
- `src/storage/columnar.rs` — ADR 044 columnar fetch path;
- `src/storage/encoding/compute.rs` — COCD evaluator paths.

Work:

1. ADR 052 secondary-index point-read SIMD-page fast path:
   at small chunk size, a point read pulls the chunk(s)
   covering the target page; SIMD-page decode unchanged.
2. ADR 056 row-group fetch: directory probe + predicate
   evaluation + projection fetch all consume the slab's chunk
   size; verify byte-identical row-group fetch plans. **Bulk-
   scan path benefits from the macro-block coalescer (Phase
   5)**: dense projection fetch produces fewer, larger GETs
   than today (because contiguous selected ranges coalesce
   the same way regardless of underlying chunk size).
3. ADR 057 page-emitting cascades: writer-side alignment is at
   page grain — encoding cascades emit identically.
4. ADR 058 / 059 COCD: predicate evaluation reads encoded
   bytes via the chunk-aligned path; verify reduced-chunk
   produces the same encoded masks.
5. ADR 044 column fetch: Phase A metadata window handling at
   small chunk size — the metadata window is now ~4× more
   chunks but coalesces into the same single GET via the
   coalescer.

Tests:

```sh
cargo test --test encoding_secondary_index
cargo test --test row_group_secondary_index
cargo test --test row_group_fetch_plan
cargo test --test row_group_materialization
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test lsm_manager_manifest
```

**Exit gate:** Every existing storage-path test passes under
the chunk-size matrix. **GET counters confirm the macro-block
coalescer holds at small chunk size.**

## Phase 8 — Architectural lockdown

Files:

- `tests/architecture_test.rs`.

Guards (per ADR draft §"Architectural lockdown"):

1. `SLAB_ENCRYPTION_CHUNK_SIZES` is a closed `[usize; 5]`
   array; no `Vec`, no operator-tunable runtime extension.
2. Closed-set values are exactly `{4096, 8192, 16384, 32768,
   65536}`.
3. `SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES = 65536`; references
   stay in default/config/test contexts, not slab read/decrypt
   chunk arithmetic.
4. `L2ChunkLayoutConfig` exists with the two pinned fields.
5. `choose_chunk_size_bytes(...)` is pure-deterministic.
6. **Macro-block coalescing test exists** at
   `tests/crypto_macro_block_coalescing.rs`.
7. No new `L2FetchStrategy` variant.
8. No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
   bit / IPC `custom_metadata` key / digest domain.
9. No new `EncodingRejectCode` / `EncodingFallbackReason` /
   `RowGroupFallbackReason` / `RowGroupRejectCode` /
   `EncodingComputeRejectCode` / `EncodingComputeFallbackReason` /
   `EncodedRowMask` / catalog variants.
10. No new public PBC opcode.
11. No `ObjectStore` trait method.
12. No descriptor field / fingerprint surface change.
13. ADR 020 §2 AAD layout unchanged (48 B).
14. ADR 022 §3 / §5 byte layouts unchanged.
15. ADR 056 `RecordPageDirectory` shape unchanged.
16. ADR 057 `EncodingDirectory` shape unchanged.
17. **No `AuthenticatedChunkDirectory` type exists** in V1.
18. **No `base_chunk_index` field** on `BufferEncoding`.
19. Cross-replica byte-equality test on writer output across
    {4 KiB, 8 KiB, 16 KiB, 32 KiB, 64 KiB} chunks for the
    same logical input.
20. No Cargo feature gating chunk-size variability.
21. No nightly rustc.
22. `ChunkSizeBytes` newtype routes through
    `src/schema/units.rs` via `impl_unit!`.
23. No version suffix on new types.
24. No planning-phase identifier on new symbols.
25. No project-name prefix.
26. Pad bytes live in IPC body slack between buffers, never
    enter any logical buffer's `encoded_range`. ADR 057's
    `expand_page_descriptors` and `decode_page` paths consume
    only `encoded_range` bytes and are unchanged.
27. Buffer-start-alignment discipline applies only to default
    sparse engagement set; non-engaged cascades share chunk
    size but not buffer-start alignment. **No intra-buffer
    pad bytes in any cascade** — ADR 057's contiguous
    `page_end_offsets` shape is unchanged.
28. `SlabFormatError::UnsupportedChunkSize` lives in slab-
    format error family, not in any `*FallbackReason` enum.
29. Architecture test rejects `_` catch-all arms on chunk-
    size dispatch.

**Exit gate:** Architecture tests prove ADR 061 did not leak
into public protocol / config / catalog surfaces.

## Phase 9 — Bench harness + evidence artifact

Files:

- `benches/row_group_fetch.rs` — extended.
- `plans/impl/061_*_bench_results.md` (new, empirical).

Bench command:

```sh
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
```

Bench matrix:

- **Chunk size:** {4 KiB, 8 KiB, 16 KiB, 32 KiB, 64 KiB}.
  64 KiB is the baseline.
- **Fixture sizes (warehouse-scale mandate):**
  - 1M-row × 16-record × 64 KiB (continuity smoke; ADR 060
    fixture);
  - **16M+ row runtime (mandatory)** with row groups sized
    to produce 1–10 MiB compressed plaintext;
  - **100M+ row sampled / modeled (mandatory)** with explicit
    byte / GET / CPU extrapolation.
- **Mask shapes:** clustered first-page, clustered 1%,
  clustered 5%, clustered 25%, random 1%, `All`, `None`.
- **Cascades:** BitPacked `UInt8` / `UInt16` / `UInt32` /
  `UInt64`, Dictionary plain low-card, Dictionary bitpacked
  low-card, RLE byte-run (default-off control), Plain (dense
  control), Delta (dense control), Lz4Frame (dense control),
  nullable BitPacked (dense control).
- **Projected-column counts:** 1 / 4 / 16 / 64.
- **Bytes-fetched counter:** instrumented via `MockObjectStore`
  GET counter.
- **AEAD CPU axis:** per-chunk encrypt / decrypt count and CPU
  time at each chunk size; verify sub-1% wall-time fraction
  even at 4 KiB chunks.
- **Pad-inflation axis:** plaintext bytes added by writer-side
  buffer-start alignment (inter-buffer slack pad only; no
  intra-buffer pad), per cascade, per chunk size.
- **GET count axis:** primary analytics-database metric.
- **GET bytes (p50, p95):** confirms macro-block coalescing on
  dense paths. Warehouse-scale dense rows must show p50 ≥ 1 MiB;
  the coalescer-emitted GET payload distribution is the empirical
  evidence that the two-layer architecture preserves analytics
  throughput.

Acceptance gates:

- **Macro-block coalescing test passes** (Phase 5 gate).
- **Sparse ciphertext-byte reduction**: ≥ 3× on warehouse-
  scale clustered 1% / 5% BitPacked + Dictionary fixtures
  vs 64 KiB baseline. Toy 1M fixture should show ≥ 1.5×
  (chunk-floor 64 KiB → 16 KiB).
- **Dense GET-count non-regression**: ≤ 0% inflation at
  small chunk sizes vs 64 KiB baseline (coalescer holds).
- **Dense p50 GET-byte size at 16 KiB chunks**: ≥ 1 MiB on
  warehouse-scale fixtures. The macro-block GET shape is
  preserved. **(Mandatory bench gate.)**
- **Dense p95 GET-byte size at 16 KiB chunks**: ≥ 4 MiB on
  warehouse-scale fixtures. Heavy-tail distribution of GET
  payloads must skew toward the macro-block end; outliers
  below 256 KiB indicate planner pathology and require
  investigation.
- **AEAD CPU non-regression**: ≤ 1% wall-time inflation at
  16 KiB; ≤ 2% at 4 KiB.
- **Writer wall-time non-regression**: ≤ 5% inflation.
- **Pad inflation**: ≤ 5% on representative fixtures.
- **Output equality**: byte-for-byte across chunk sizes.
- **Cross-replica determinism**: byte-identical.
- **Legacy 64 KiB slabs**: read identically.
- **Random (anti-clustered) survival masks**: ADR 060's
  existing economic gate (`sparse_bytes < dense_bytes`) and
  per-buffer fragmentation cap
  (`MAX_SPARSE_RANGES_PER_BUFFER = 8`) should produce one of:
  (a) sparse declines and routes the row group through the
  dense projection-fetch path (no chunk-grain savings expected
  on random masks; chunk-floor reduction does not help when
  selected pages span every chunk anyway), or (b) sparse
  engages and stays within ≤ 5% wall-time regression and ≤ 25%
  GET-count inflation vs the 64 KiB baseline. Either outcome
  is a pass; ciphertext-byte reduction proportional to chunk-
  size shrink is **not** required for random masks (clustered
  selectivity is ADR 060's primary target shape).
- **`All` / `None` survival masks**: unchanged behavior.
- **Default chunk size finalized**: Phase 9 sweep selects
  per-page-byte-size threshold for chunk-size choice; default
  starts at 16 KiB.
- **Closed-set values verified**: any chunk size with no
  fixture passing its gates is dropped from the set.

Reports per row:

- chunk size under test;
- fixture scale (rows / records / row groups);
- bulk-scan GET count + p50/p95 GET bytes + wall time;
- sparse projection-fetch ciphertext bytes + GET count + wall
  time;
- AEAD encrypt / decrypt count + CPU time;
- writer wall time;
- materialize wall time;
- pad-byte plaintext inflation;
- output equality status.

**Exit gate:** Bench artifact records pass / fail for every
gate, the finalized closed set, the finalized selector default,
and the council disposition.

## Phase 10 — Shipped-status promotion

Files:

- `plans/impl/0061_page_aligned_authenticated_chunk_layout_bench_results.md`
  (lands).
- `plans/adr/0061_page_aligned_authenticated_chunk_layout.md` —
  §"Status" amended to "Council-converged, implemented, and
  promoted to shipped" (matching the ADR 059 / 060 pattern).
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` —
  §"Where we are" amendment closing the recorded-not-gated
  ciphertext-ratio follow-up.
- `plans/adr/0044_l2_columnar_fetch.md` — references update.
- `plans/wip/roadmap.md` — §"Where we are" + §"Forward
  sequence" updates at the planning-cadence boundary.
- `tests/architecture_test.rs` — confirm Phase 8 lockdown
  guards reference the final ADR path.

Work:

1. Phase 9 bench artifact passes promotion gates.
2. ADR 061 §"Status" says "Council-converged, implemented,
   and promoted to shipped."
3. `L2ChunkLayoutConfig::enabled = true` by default.
4. Roadmap updated at the planning-cadence boundary.

**Exit gate:** ADR 061 marked shipped because code, tests,
and bench evidence landed together with recorded council
disposition.

## Verification gate

Required before promotion:

```sh
cargo test --test encoding_chunk_layout
cargo test --test crypto_macro_block_coalescing
cargo test --test row_group_sparse_projection_fetch
cargo test --test encoding_secondary_index
cargo test --test row_group_secondary_index
cargo test --test row_group_fetch_plan
cargo test --test row_group_materialization
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test architecture_test
cargo test --test metrics_smoke
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test lsm_manager_manifest
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
git diff --check
git diff --cached --check
```

Promotion requires:

- all tests above green;
- bench artifact meets Phase 9 gates;
- ADR 061 ships **without** any new operator config field
  beyond `L2ChunkLayoutConfig`;
- ADR 060 §"Where we are" amended in the same changeset;
- roadmap #17 updated.

## File-by-file expected edits

- `src/schema/units.rs` — `ChunkSizeBytes` newtype (Phase 1).
- `src/crypto.rs` — closed-set constant; per-slab chunk-size
  threading; slab-format validation (Phase 1).
- `src/storage/encoding/chunk_layout.rs` (new) —
  `L2ChunkLayoutConfig`; selector; pad-inflation estimator;
  writer-side `align_buffers_to_chunks(...)` (Phase 2, 3).
- `src/storage/encoding/directory.rs` — wire selector and pad
  placement into write path (Phase 4).
- `src/storage/encoding/cascade/codec.rs` — page emission with
  pad bytes (Phase 3).
- `src/storage/encoding/mod.rs` — re-export.
- `src/storage/manager.rs` — chunk-size threading (Phase 1, 7).
- `src/storage/columnar.rs` — chunk-size threading (Phase 1, 7).
- `src/storage/encoding/sparse_projection_fetch.rs` — audit
  only (Phase 6).
- `src/storage/encoding/page_materialize.rs` — audit only
  (Phase 6).
- `tests/encoding_chunk_layout.rs` (new) — Phase 2, 3.
- `tests/crypto_macro_block_coalescing.rs` (new) — Phase 5.
- `tests/row_group_sparse_projection_fetch.rs` — chunk-size
  matrix (Phase 6).
- `tests/encoding_secondary_index.rs` /
  `tests/row_group_secondary_index.rs` — Phase 7 verification.
- `tests/architecture_test.rs` — Phase 8 guards.
- `benches/row_group_fetch.rs` — Phase 9 matrix.
- `plans/impl/061_*_bench_results.md` (new) — Phase 9+.
- `plans/adr/0060_sparse_projection_fetch_narrowing.md` —
  Phase 10 amendment.

## Risks and mitigations

**Macro-block coalescer doesn't scale to small chunks.**
Catastrophic if true; would invalidate the architecture.
Mitigation: Phase 5 is the load-bearing test. Phase 0 audit
checks `TieredMultiRangeReadPlan` data structure; expectation
is that coalescer is already chunk-size-agnostic.

**AEAD CPU at 4 KiB chunks regresses materialize wall time.**
Mitigation: Phase 9 measures and gates ≤ 2% wall-time
inflation; closed set drops 4 KiB if it doesn't pass.

**Pad-byte inflation eats plaintext savings.** Mitigation:
config cap (5%); writer falls back to 64 KiB when inflation
exceeds cap.

**Writer wall-time inflation on small chunks.** Mitigation:
≤ 5% gate; selector won't pick a chunk size that fails the
writer-time gate.

**Cross-replica determinism breaks if selector takes a non-
deterministic input.** Mitigation: pure-deterministic selector
is a Phase 2 property test; cross-replica byte-equality is a
Phase 3 + Phase 9 fixture.

**Closed-set members don't compose with future ADRs.**
Mitigation: closed set is provisional; promotion-bumping ADRs
may extend with explicit council disposition.

**Pad bytes get parsed as page data on a corrupt slab.**
Mitigation: AEAD authenticates pad bytes via the chunk's
existing tag; corruption surfaces as AEAD-verify failure on
the same path that catches malformed chunks today. Pad bytes
are also outside `BufferEncoding.encoded_range`, so the page
materializer skips them by construction.

**ADR 052 secondary-index point-read SIMD-page fast path
breaks at small chunk size.** Mitigation: Phase 7 explicit
verification.

**ADR 020 §2 nonce-and-data safety violated.** Mitigation:
Phase 0 documents AES-GCM safety bound at every chunk size;
bound holds (chunk count is well within u64 chunk_index;
per-key data limit is unaffected).

**Operator pressure to make chunk size tunable.** Mitigation:
V1 ships closed set + `L2ChunkLayoutConfig` toggle. Promotion
to operator surface requires its own ADR.

**Internal pad-byte representation leaks to public surface.**
Mitigation: architectural guard prohibits pad-byte references
outside `chunk_layout.rs` / `directory.rs`.

**Rewrite burden for existing 64 KiB slabs.** Mitigation:
existing 64 KiB slabs continue at 64 KiB indefinitely; ADR 055 LSM
compaction picks up smaller chunk sizes on natural recompacts.

**AEAD setup overhead at small chunk sizes.** Concern raised
during convergence: AES-NI per-chunk setup may dominate at 16 KiB.
Mitigation: ADR §"Why 16 KiB AEAD chunks" provides concrete
numbers — setup is ~3% of data work; tag overhead 0.10%;
scheduler not involved post-coalesce. Phase 5 sub-property 2
verifies AEAD-call count and CPU% empirically across the chunk-
size matrix.

**ADR 044 column-fetch over-fetch with fixed-size chunks.**
Concern raised during convergence: chunks span multiple
buffers; 1-of-N column projection fetches some unrelated bytes.
Mitigation: at 16 KiB chunks on warehouse-scale records, over-
fetch is ~3%, well within ADR 044's existing 67% tolerance
(`max_selected_fraction_micros = 670_000`). Variable-buffer-
aligned chunks via encrypted directory (the V2 path) achieve
0% over-fetch but add load-bearing new surface. V1 defers this
to V2 with explicit promotion gate: post-V1 evidence that ADR
044 over-fetch is a measurable bottleneck on operational
workloads.

**Phase 0 no-directory feasibility check fails.** If the
feasibility analysis surfaces a real obstacle (e.g., a path
discovered late where the reader genuinely needs ciphertext-
offset info that ADR 056 / 057 directories don't carry, or
non-deterministic pad-byte placement across replicas, or ADR
044 over-fetch exceeding the gate on a real fixture),
**implementation stops**. The council produces a revised
ADR/impl pair as a separate decision (likely adopting the
encrypted-directory approach). This plan does not silently
reshape — that would violate V1 non-goals and architectural
lockdown guards. Phase 0 is read-only audit + math, and the
items checked are concrete (chunk-index from `chunk_size_bytes`
arithmetic; inter-buffer pad bytes implicit in `encoded_range`
gaps; no intra-buffer pad bytes; per-buffer chunk index O(1)
computable). Expectation: passes because all the necessary
information is already in ADR 022 + 056 + 057 metadata. But the
gate is loud and explicit so failure surfaces cleanly.

**Macro-block coalescer doesn't scale to small chunks.**
Catastrophic if true; would invalidate the architecture.
Mitigation: Phase 5 is the load-bearing test, with four sub-
properties. Phase 0 audit checks `TieredMultiRangeReadPlan`
data structure; expectation is that coalescer is already chunk-
size-agnostic since `chunk_runs: SmallVec<[ChunkRun; 8]>`
represents *coalesced* contiguous chunk index ranges, not
individual chunks.

**`max_pad_inflation_micros` Phase 0 starting value is wrong.**
Mitigation: Phase 9 sweep refines.

**Default chunk size of 16 KiB doesn't fit some real
workloads.** Mitigation: per-slab selector adapts to page
geometry. Pages ≤ 16 KiB (UInt8 / UInt16 / UInt32 / UInt64
BitPacked, Dictionary plain / bitpacked low-card) all select
16 KiB because the V1 PROMOTED_FLOOR pulls the threshold to
16 KiB. Fixtures with pages > 16 KiB but ≤ 32 KiB select
32 KiB. Fixtures with pages > 32 KiB but ≤ 64 KiB select
64 KiB. Fixtures with pages > 64 KiB stay at the 64 KiB
baseline (the closed set has no member large enough).

## Deferred (matches ADR)

- Operator-tunable per-slab chunk size beyond the closed set.
- Per-record chunk size (would require ADR 020 §2 nonce / AAD
  rework).
- Chunk sizes outside the closed set.
- Encrypted `AuthenticatedChunkDirectory` (V2 territory).
- `base_chunk_index` per `BufferEncoding` (not needed with
  fixed-size chunk math).
- Variable chunk size *within* a slab (V2).
- Sub-buffer chunk alignment.
- Chunk-aligned predicate-fetch.
- Adaptive / learned chunk-size selection.
- Cross-slab chunk-aligned coalescing.
- Pad-byte cryptographic discipline beyond `0x00`.
- `ObjectStore` trait change (provider-native multi-range).
- Migration sweeper for existing 64 KiB slabs.
- Per-cascade enabled flag.
- Per-record activation knob.
- Row-group sizing recommendations (ADR 056 amendment
  follow-up).
- Page-alignment for non-default cascades (RLE follow-up
  alongside its value-run format ADR; Delta / Plain /
  Lz4Frame / Fsst / Alp follow-up ADRs).

## Open implementation questions

All architectural forks closed by council convergence.
Remaining questions are bench-empirical and resolve in Phase 9:

1. **Final default chunk size**: 16 KiB is the V1 default;
   Phase 9 sweep may narrow if 32 KiB shows materially better
   AEAD-overhead/sparse-leverage tradeoff at warehouse scale.
2. **Closed-set membership**: any of `{4, 8, 16, 32, 64} KiB`
   with no passing fixture in Phase 9 may be dropped from the
   emitted set in V1 (kept as architecturally permitted; just
   not chosen by the selector).
3. **ADR 056 row-group sizing follow-up**: Phase 9 evidence on
   warehouse-scale fixtures may motivate an ADR 056 amendment
   recommending warehouse-profile row-group defaults. Out of
   scope for ADR 061; tracked as a separate follow-up.
4. **V2 promotion gate for variable buffer-aligned chunks**
   (encrypted directory approach): post-V1 operational-query
   evidence showing ADR 044 column-fetch over-fetch becomes a
   measurable bottleneck. Tracked as a separate ADR.
