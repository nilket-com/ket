# ADR 061: Page-Aligned Authenticated Chunks with Macro-Block Read Coalescing

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/061_page_aligned_authenticated_chunk_layout.md`

## Status

Council-converged across Claude / Codex / Gemini.
Implementation phases pending per the companion impl plan;
promotion to shipped follows after Phase 9 bench evidence
clears the gates. Phase 0 no-directory feasibility check and
Phase 5 macro-block coalescing test are the two load-bearing
gates that must pass before the writer enables small chunks by
default.

## Context

ADR 060 shipped sparse projection-fetch narrowing on the
plaintext side. Phase 7 ciphertext gate withdrew because ADR
020's 64 KiB AEAD chunks combined with ADR 044's chunk-aligned
range-read primitive rounded selected plaintext page bytes out
to 64 KiB ciphertext. ADR 061 closes that floor.

The council converged on a two-layer architectural model:
the **AEAD authentication boundary** (atomic decrypt unit) and
the **physical network read unit** (transfer unit) are
independent layers optimized for different forces. Earlier
iterations conflated the two — reaching for either uniformly
small chunks (regressing analytics throughput) or uniformly
large chunks (regressing ADR 060 sparse leverage and ADR 044
column-fetch). The decoupling resolves the tension: small AEAD
chunks for fine-grain decrypt leverage, large coalesced GETs
for warehouse-scale transfer economics, with ADR 044's existing
`TieredMultiRangeReadPlan` coalescer bridging the two.

### The two-layer decoupling

```text
Layer 1 — AEAD authentication boundary (atomic decrypt unit)
  - AES-GCM cannot verify any byte until the full chunk +
    16-byte tag is fetched and verified.
  - Wants to be SMALL so ADR 060 sparse decrypts have leverage.
  - 16 KiB chunks: 4 ADR 057 SIMD pages per chunk; one tag
    per chunk = 0.10% overhead; AES-NI ~3.2 µs decrypt per
    chunk in L1/L2 cache.

Layer 2 — Physical network read unit (transfer unit)
  - The L2 client's `ObjectStore::get_range` call.
  - Wants to be LARGE for analytics throughput.
  - Coalesced from contiguous AEAD chunks via ADR 044's
    existing `TieredMultiRangeReadPlan.chunk_runs` mechanism.
  - 100 MiB GETs from 6400 contiguous 16 KiB chunks; AES-NI
    loops verify in L1/L2 after the GET arrives.
```

This is exactly how TLS already composes. TLS records are 16 KiB;
TLS streams MiB-class transfers over those records without the
network ever seeing the record boundaries. Same architecture,
same forces.

### Worked example: S3 cost on a 100 GiB analytic scan

The most common confusion about this architecture is "if AEAD
chunks shrink to 16 KiB, won't every query cost a fortune in S3
GET requests?" The answer is no, because **AEAD chunks are not
network GETs**. The bridge is ADR 044's existing macro-block
coalescer, which is already in production code:

```text
src/crypto.rs:3396  fn chunk_read_runs_for_plaintext_ranges(...)
                      lines 3417–3422 explicitly merge adjacent
                      chunk ranges into a single TieredChunkRunReadPlan;
                      each plan is one ObjectStore::get_range call.

src/crypto.rs:6078  test adr044_multi_range_coalesces_overlapping_and_adjacent_chunk_runs
                      already proves the coalescer behaves correctly
                      at production scale.
```

Request-cost arithmetic at the S3 Standard rate of $0.0004 per
1000 GETs, anchoring the rejection shape:

```text
$10 in request charges = 25,000,000 GETs.

100 GiB analytic scan, NO COALESCING, 16 KiB GETs:
  6,553,600 GETs = ~$2.62 in request charges  ← REJECTION SHAPE.
                                                One provider request
                                                per AEAD chunk is
                                                what ADR 061 must
                                                empirically forbid.

100 GiB analytic scan, coalesced to 1 MiB GETs:
  102,400 GETs = ~$0.041 in request charges

100 GiB analytic scan, coalesced to 10 MiB GETs:
  10,240 GETs  = ~$0.0041 in request charges

100 GiB analytic scan, coalesced to 100 MiB GETs:
  1,024 GETs   = ~$0.00041 in request charges
```

These exclude byte-transfer charges, retrieval-tier fees,
storage, and compute. They exist to make the layering explicit:
**at 16 KiB AEAD chunks, S3 request cost on warehouse scans is
in the fractions-of-a-cent range, not dollars** — provided the
coalescer holds.

GET count is governed by **how many contiguous regions of
plaintext the planner asks for**, not by chunk count underneath.
A row group's chunks are written sequentially in the slab;
selecting that row group's bytes selects contiguous chunks; the
coalescer merges them into one `get_range` call regardless of
the chunk size below. AES-NI then loops over the chunks in CPU
L1/L2 cache after the bytes arrive — never touching the network
for individual chunks.

The cost worry applies if and only if 16 KiB AEAD chunks were
issued as 16 KiB GETs (the rejection shape above —
6.5M GETs × $0.0004/1000 = $2.62 just on request cost, before
any data transfer). ADR 061 does not propose this. It proposes
16 KiB **AEAD chunks** that become MiB-class GETs through the
existing coalescer.

**Phase 5 sub-property 1** of the impl plan is the load-bearing
test that empirically verifies the coalescer holds at 16 KiB
chunks: dense fetch over a 100 MiB body must produce exactly
ONE `get_range` call, not 6400. The test exists precisely to
remove this question from speculation and make it empirical. If
the test ever fails, the layout falls back to legacy 64 KiB and
ADR 061 stops the writer from enabling smaller chunks.

### Why 16 KiB AEAD chunks: the design-point analysis

The closed set `{4, 8, 16, 32, 64} KiB` and the 16 KiB default
emerge from four cross-cutting constraints. The council
considered alternatives at both ends of the spectrum (sub-4 KiB
chunks; 1+ MiB warehouse-ladder chunks; variable buffer-aligned
chunks via encrypted directory) and converged on the small-
fixed-chunks design after working through the numbers below.

**ADR 060 sparse projection fetch ciphertext floor** at typical
geometry (1024 SIMD-page rows × 4-byte BitPacked = 4 KiB pages,
clustered 5% selectivity = ~50 selected pages out of 1000):

```text
Chunk size  Selected ciphertext (clustered)  vs today's 64 KiB
---------   -------------------------------  -----------------
4   KiB     ~200 KiB                          16× narrower
16  KiB     ~200 KiB                           4× narrower    ← V1 default
64  KiB     ~800 KiB                           1× (today's floor)
1   MiB     several MiB                       0.06× (regression)
10  MiB     10–20 MiB                         0.005× (severe regression)
```

The whole point of ADR 061 is to break through the chunk floor
ADR 060 documented; chunks ≥ 1 MiB raise the floor instead.

**ADR 044 columnar-fetch composition** on a warehouse-scale
record (4 MiB plaintext body, 50 columns × 80 KiB per column
buffer). ADR 044's gate is `max_selected_fraction_micros =
670_000` (67%): the gate fires when *fetched bytes / record
bytes* ≤ 67%.

For a 1-of-N column projection on a default-engaged column,
fetched bytes ≈ buffer size + chunk-boundary over-fetch (when
buffer ends mid-chunk). With the V1 buffer-start-alignment
writer discipline (§V1 scope §2), default-engaged buffers'
starts land on chunk boundaries; the only over-fetch comes
from the buffer's *end* falling in the middle of a chunk,
costing at most one extra chunk (≤ chunk_size_bytes). Non-
default-engaged buffers (Plain / Lz4Frame / Delta / Fsst /
Alp / Rle) start at the running plaintext cursor (may be
mid-chunk) and may pay over-fetch at both ends — at most two
extra chunks total — but ADR 060 routes those cascades dense,
so per-record-fraction-gate analysis still uses the buffer's
own bytes and the bounded chunk tails. The numbers below cover
the default-engaged case explicitly; non-default cases pay at
most one additional chunk-tail of over-fetch (≤ 16 KiB at
V1's chunk size) and remain comfortably below ADR 044's 67%
gate at the per-record fraction.

```text
Layout                       Per-column buffer fetch       Per-record fraction (1-of-50)
------------------------     ---------------------------   ----------------------------
64 KiB fixed (today)         buffer + ≤64 KiB tail          (80+64) / 4096 ≈ 3.5%
16 KiB fixed (V1)            buffer + ≤16 KiB tail          (80+16) / 4096 ≈ 2.3%
1 MiB fixed                  buffer + ≤1024 KiB tail       (80+1024) / 4096 ≈ 27%
4 MiB fixed                  buffer + buffer rounds-up      4096 / 4096 = 100% (gate fails)
Buffer-aligned (V2 path)     buffer (no tail)               80 / 4096 ≈ 2.0%
```

At 16 KiB chunks the per-record fraction (~2.3%) is far below
ADR 044's 67% threshold — column-fetch fires comfortably. At
1 MiB chunks the fraction (~27%) is still below 67% but climbing
fast; at 4 MiB chunks the buffer-end tail exceeds the column
buffer itself and the gate fails outright on narrow projections.
The V1 default 16 KiB chunks and the V2 buffer-aligned approach
are within a percent of each other on this workload — the
remaining V2 win (~0.3 pp) doesn't justify an encrypted-
directory surface for V1.

For wider projections (e.g., 16-of-50 columns), the per-record
fraction at 16 KiB scales to (16 × 96) / 4096 ≈ 37.5% — still
under the 67% gate, with margin shrinking. ADR 044's existing
gate handles this case correctly today and continues to do so
under ADR 061.

The ADR 044 gate calculation is sensitive to **buffer-start
alignment** specifically (without it, both buffer-start and
buffer-end could land mid-chunk, doubling the over-fetch).
Buffer-start alignment (§V1 scope §2) is therefore load-bearing
not just for ADR 060 sparse leverage but for ADR 044 column-
fetch composition.

**AEAD-tag overhead** per slab:

```text
Chunk size   Overhead %
---------    ----------
4   KiB      0.39%
16  KiB      0.10%   ← V1 default, negligible
64  KiB      0.024%
1   MiB      0.0015%
```

The 16 KiB → 64 KiB difference (0.08 percentage points) is not
load-bearing. AEAD-tag economics are not the deciding constraint.

**AEAD setup CPU** is constant per chunk (~100 ns: nonce
derivation + tag verification; AES key schedule cached per-DEK).
At 16 KiB chunks: 6400 chunks × 100 ns = 640 µs across a 100 MiB
scan. AES-NI's data work at ~5 GB/s/core takes 20 ms across the
same 100 MiB. Setup is ~3% of data work. Sparse fragmentation
is bounded by ADR 060's existing `MAX_SPARSE_RANGES_PER_BUFFER
= 8` cap. Phase 5 sub-properties 2 and 4 verify these
empirically.

**Conclusion**: 16 KiB AEAD chunks give ADR 060 a 4× ciphertext
narrowing on clustered fixtures, comfortably preserve ADR 044
column-fetch (over-fetch ~3% ≪ 67% tolerance), and add
sub-percent AEAD overhead — all while the macro-block coalescer
preserves dense-scan throughput at the network layer. The
two-layer decoupling makes this design point coherent in a way
neither very-small-chunks-everywhere nor very-large-chunks-
everywhere could be.

### Constraints carry forward unchanged

ADR 003, ADR 016, ADR 019, ADR 020, ADR 022, ADR 029, ADR 030,
ADR 035, ADR 036, ADR 037, ADR 039, ADR 040, ADR 042, ADR 043,
ADR 044, ADR 052, ADR 053, ADR 054, ADR 055, ADR 056, ADR 057,
ADR 058, ADR 059, ADR 060.

## Decision

ADR 061 ships:

1. **Per-slab small AEAD chunk size from a closed set**:
   `{4, 8, 16, 32, 64} KiB`. Default 16 KiB for slabs with an
   `EncodingDirectory`; 64 KiB legacy default for slabs without.
   The set is deliberately under 64 KiB on the upper end:
   chunks at this scale give ADR 060 sparse leverage while
   AEAD-tag overhead stays under 0.4%.

2. **Buffer-start writer discipline**: each ADR 057
   `BufferEncoding.encoded_range.start` is rounded up to the
   next AEAD chunk boundary via opaque pad bytes in the IPC
   body's existing inter-buffer slack. Pages within a buffer
   stay contiguous (no intra-buffer pad — ADR 057's
   `page_end_offsets` is a contiguous prefix-end array and
   cannot represent intra-buffer gaps). Pad cost bounded ≤ 5%.

3. **Macro-block coalescing as the load-bearing test**: ADR
   044's existing `TieredMultiRangeReadPlan` coalescer must be
   empirically shown to produce single multi-MiB `get_range`
   calls when thousands of contiguous AEAD chunks are selected.
   No new coalescer code; the architecture stands or falls on
   this empirical property.

4. **Warehouse-scale bench fixtures**: 16M+ row runtime, 100M+
   row sampled / modeled with explicit byte / GET / CPU
   extrapolation.

5. **Phase 0 no-directory feasibility check**: formally prove
   that ADR 022's existing `chunk_size_bytes` field plus ADR
   056 / 057 directories can express the V1 layout without a
   new encrypted directory. This gate is loud and explicit; if
   Phase 0 fails, **implementation stops** and the council
   produces a revised ADR/impl pair as a separate decision. V1
   does not silently reshape to include an encrypted directory
   under this plan — that would violate V1's non-goals and
   architectural lockdown guards.

6. **p50/p95 GET-payload bench metric**: the bench artifact
   reports the distribution of coalesced `get_range` payload
   sizes. Warehouse-scale dense paths must show p50 ≥ 1 MiB.

7. **No new on-disk shape, no new directory, no new metadata
   surface.** ADR 022 §3's existing `chunk_size_bytes` u32
   field carries the per-slab value (Open Question 4 explicitly
   anticipated this). No `AuthenticatedChunkDirectory` —
   deferred to V2 if post-V1 measurements warrant. No
   `base_chunk_index` per buffer — not needed with fixed-size
   chunk math.

8. **No Toron contract change. No new wire frame. No new
   reject / fallback variant. No new `L2FetchStrategy` /
   `EncodedRowMask` / catalog variant. No `ObjectStore` trait
   method.**

The slab-format byte layout, AAD shape, nonce derivation, key
hierarchy, and footer HMAC scope are all unchanged. Only the
runtime value of an existing field varies.

## Architectural inextricability

Three design choices physically inseparable in V1.

**1. Small AEAD chunks + buffer-start alignment + macro-block
coalescing test ship together.** Small chunks alone without
buffer-start alignment leave both buffer starts and ends mid-
chunk, doubling ADR 044 boundary over-fetch and giving ADR 060
no aligned floor to target on chunk-size-divisor pages. Buffer-
start alignment alone (without smaller chunks; today's 64 KiB)
leaves the floor ADR 060 documented unchanged. The macro-block
coalescing test is what proves analytics throughput is preserved
at small chunk size — without it, small chunks could regress
dense scans. The three together produce fine-grain sparse
narrowing AND macro-block analytics throughput AND ADR 044
column-fetch composition. (Note: alignment is at buffer grain,
not page grain — ADR 057's contiguous `page_end_offsets`
forbids intra-buffer pad bytes.)

**2. Internal writer / crypto / reader change — no Toron-
visible contract change, no new directory, no new metadata
surface.** ADR 022 §3 already declares `chunk_size_bytes`
variable in spec; ADR 061 just exercises a wider value range.
ADR 057 page metadata + per-slab chunk size are sufficient for
the reader to compute every chunk index it needs.

**3. Closed chunk-size set deliberately small and bench-
promoted.** AEAD-tag overhead at 16 KiB is 0.10%, at 4 KiB is
0.39%, at 64 KiB is 0.024%. AES-NI throughput makes per-chunk
setup negligible at any size in the set. The closed set is
provisional — Phase 9 evidence may show that 4 KiB or 64 KiB
are unused in practice and can be dropped.

## Identity-vs-Locality framing

ADR 061 is identity-neutral and locality-neutral. Chunk size is
per-slab physical storage shape, downstream of admission and
orthogonal to row identity, locality projection, record
location, record row index, or visible row set.

Cross-replica determinism is preserved by **slab-byte
equality**: the writer's chunk-size decision is a deterministic
function of inputs every replica sees byte-identically.

There is no separate "chunk-layout plane." Page-aligned chunks
+ macro-block coalescing live in the same storage-write
surface as ADR 020 chunked AEAD, ADR 022 slab format, ADR 056
row groups, ADR 057 encoding cascades, ADR 044 column fetch,
and ADR 060 sparse projection fetch. Unified Ring rule applies.

## V1 scope

ADR 061 ships:

### 1. Closed chunk-size set + slab-format readout

`chunk_size_bytes ∈ {4096, 8192, 16384, 32768, 65536}`. The
reader:

- reads `chunk_size_bytes` from the slab's tail-window fetch;
- validates the value against the closed set; rejects unknown
  values with `SlabFormatError::UnsupportedChunkSize` (a new
  variant in the existing slab-format error path);
- threads the value through the crypto path where today
  `SLAB_ENCRYPTION_CHUNK_BYTES` is read as a constant.

`SLAB_ENCRYPTION_CHUNK_BYTES` is renamed to
`SLAB_ENCRYPTION_LEGACY_CHUNK_BYTES` and continues to equal
65536. The closed set is exposed as
`SLAB_ENCRYPTION_CHUNK_SIZES: [usize; 5]`.

### 2. Buffer-aligned writer discipline

ADR 057's `BufferEncoding.page_end_offsets` is a contiguous
prefix-end array: page `i` starts where page `i−1` ended; there
is no representation of gaps between pages within a buffer
(`src/storage/encoding/page.rs:6-35` shows `expand_page_descriptors`
computes each page's byte range as `end - previous_byte`). ADR
061 V1 must not introduce intra-buffer gaps — they are
unrepresentable without changing ADR 057's directory shape, and
ADR 061 commits to no ADR 057 shape change.

The writer therefore aligns **buffer starts** to chunk
boundaries, not individual page starts. When the writer emits
an encoded buffer (ADR 057 cascade output) for a page-emitting
cascade in the default sparse engagement set (`BitPacked`,
`Dictionary { inner: Plain | BitPacked }`):

- it picks a chunk size from the closed set per the selector
  (§3);
- it inserts **opaque pad bytes** in the IPC body slack
  *between* buffers (the existing gap between
  `BufferEncoding[i].encoded_range.end` and
  `BufferEncoding[i+1].encoded_range.start`) so that
  `BufferEncoding[i+1].encoded_range.start` lands on a
  `chunk_size_bytes` boundary within the slab plaintext stream;
- pad bytes are AEAD-authenticated by the enclosing chunk's
  existing tag and are not part of any logical buffer range;
  the reader's `expand_page_descriptors` and `decode_page` paths
  never see them because they consume only bytes within
  `encoded_range`;
- pages within a buffer remain contiguous (no intra-buffer pad).

**Page-to-chunk alignment is a consequence, not a guarantee.**
With a buffer-aligned start and chunk-size-divisor pages, page
starts naturally land on chunk boundaries. For the default
sparse engagement set at typical geometries:

```text
Cascade / page geometry              16 KiB-chunk alignment
-----------------------------------  ----------------------
BitPacked UInt8   (1024 × 1 B)       16 pages per chunk, exact
BitPacked UInt16  (1024 × 2 B)        8 pages per chunk, exact
BitPacked UInt32  (1024 × 4 B)        4 pages per chunk, exact
BitPacked UInt64  (1024 × 8 B)        2 pages per chunk, exact
Dictionary plain  (low-card 1 B idx)  16 pages per chunk, exact
Dictionary bitpacked, sub-byte idx    pages may straddle chunks
BitPacked non-power-of-2 bit width    pages may straddle chunks
```

When pages do straddle chunks (sub-byte bitpacked widths,
non-power-of-2 widths), ADR 060 sparse projection fetch still
benefits from the smaller chunk floor — selected page bytes
land in 1–2 chunks at 16 KiB granularity rather than 1–2
chunks at 64 KiB. The straddle introduces bounded boundary
over-fetch (≤ 1 chunk per straddle) but does not regress
ADR 060 below the 64 KiB baseline.

Non-default-engaged cascades (Plain / Lz4Frame / Delta / Fsst /
Alp / Rle) share the per-slab chunk size but the writer does
not insert buffer-alignment pad for them. ADR 060 routes them
dense; chunk-floor savings still apply via the smaller chunk
size, just without the buffer-start alignment.

### 3. Writer-side chunk-size selector

```text
PROMOTED_FLOOR = 16384   (16 KiB; bench-promoted V1 default;
                          smaller closed-set members are
                          diagnostic-only until Phase 9 evidence
                          promotes them)

choose_chunk_size_bytes(
    encoding_directory: Option<&EncodingDirectory>,
    config:             &L2ChunkLayoutConfig,
) -> ChunkSizeBytes:

    LEGACY = 65536  (64 KiB; legacy default for slabs without
                     encoding directories)

    if not config.enabled:
        return LEGACY
    if encoding_directory is None:
        return LEGACY              # legacy / non-encoded slab

    max_page_bytes = max page byte size in default-engaged buffers

    # Closed set with the V1 promoted floor applied; 4 KiB and
    # 8 KiB stay in CHUNK_SIZES as architecturally permitted but
    # are not selectable by the V1 selector until promoted.
    candidates = [c in CHUNK_SIZES
                    if c >= max(max_page_bytes, PROMOTED_FLOOR)]
    if candidates is empty:
        return LEGACY              # pages too large for any closed-set member
    chosen = min(candidates)

    pad_inflation = estimate_pad_inflation(directory, chosen)
    if pad_inflation > config.max_pad_inflation_micros:
        return LEGACY

    return chosen
```

The selector is **pure**: same inputs → same output, byte-
deterministic. V1 returns:

- **16 KiB** on slabs where every default-engaged buffer's
  largest page is ≤ 16 KiB (the typical case — UInt8 / UInt16
  / UInt32 / UInt64 BitPacked all have pages ≤ 8 KiB at
  `target_simd_page_values = 1024`, plus typical Dictionary
  plain / bitpacked low-card configurations).
- **32 KiB** on slabs with pages > 16 KiB but ≤ 32 KiB.
- **64 KiB** on slabs with pages > 32 KiB but ≤ 64 KiB, or
  on slabs whose largest page exceeds 64 KiB (the closed set
  has no member large enough; selector falls back to the
  legacy default).

**V1's promoted floor is 16 KiB** — 4 KiB and 8 KiB members
of the closed set remain in `SLAB_ENCRYPTION_CHUNK_SIZES` for
architectural permission and Phase 9 diagnostic rows, but the
selector does not pick them in V1 even when the largest page
would fit. Phase 9 evidence may lower the promoted floor in a
follow-up amendment.

### 4. `L2ChunkLayoutConfig` (node-local, cross-replica)

```rust
pub struct L2ChunkLayoutConfig {
    enabled: bool,
    /// Maximum permitted plaintext-pad inflation, in micros.
    /// 50_000 = 5%. Bench-tuned in Phase 9.
    max_pad_inflation_micros: u32,
}
```

`enabled = true` by default after Phase 9 evidence; legacy
behavior is `enabled = false`. **Two fields, not three.**

### 5. Macro-block coalescing as the load-bearing gate

ADR 044's existing `crypto.plan_tiered_multi_range_read(...)`
already coalesces contiguous chunks into single GETs. ADR 061
does not add coalescer code; it adds an architectural test that
empirically verifies the coalescer scales to the small-chunk
regime. The test (Phase 5 of impl plan) asserts:

```text
For chunk sizes ∈ {4, 8, 16, 32, 64} KiB:
  Given a 100 MiB plaintext record body in a slab with the
  given chunk size and an encoding directory:

  Dense projection-fetch covering the entire record body:
    - emits exactly ONE chunk_run in TieredMultiRangeReadPlan;
    - translates to exactly ONE ObjectStore::get_range call;
    - the GET range is approximately 100 MiB ciphertext;
    - AES-NI loops over chunks in CPU caches; per-chunk
      decrypt time × chunk_count < 100 ms;
    - AEAD-call count is bounded above (linear in chunk count
      but with bounded per-call overhead, < 1% of materialize
      wall time).

  Clustered sparse projection-fetch covering 5% of the record
  body in 5 contiguous regions:
    - emits ~5 chunk_runs;
    - each GET coalesces the contiguous chunks within its
      region;
    - GET count is region count, NOT chunk count.

  Anti-clustered sparse projection-fetch (50 non-contiguous
  pages):
    - bounded by ADR 060's MAX_SPARSE_RANGES_PER_BUFFER cap;
    - GET count ≤ ADR 060 floor at the same selectivity.
```

**Without all of these holding**, ADR 061 fails. **With them**,
ADR 061 wins on both axes simultaneously: ADR 060 sparse paths
see chunk floor drop from 64 KiB → 16 KiB, and analytics dense
paths preserve macro-block GETs.

This test is the empirical resolution to the council's
"tiny crypto chunks not a license" concern. If the coalescer
holds at small chunk size, the tiny-chunk concerns are moot.

### 6. Reader composability with ADR 060 / ADR 044

ADR 060 sparse projection fetch already routes selected
encoded-page byte ranges through
`crypto.plan_tiered_multi_range_read(...)`. ADR 061 changes the
chunk size that path uses; nothing else.

**ADR 044 column-fetch composition**:

- At 16 KiB chunks with the V1 buffer-start-alignment writer
  discipline, a 1-of-50 column projection on a 4 MiB warehouse-
  scale record body fetches the column's buffer (80 KiB) plus
  at most one chunk-tail (≤ 16 KiB end-of-buffer round-up).
  That's `(80 + 16) / 4096 ≈ 2.3%` of the record — well below
  ADR 044's 67% `max_selected_fraction_micros` threshold.
  Coalesces to 1 GET per column buffer.
- At 1 MiB chunks (a rejected alternative), the same projection
  fetches `(80 + 1024) / 4096 ≈ 27%` of the record. Still under
  the 67% gate but climbing; at 4 MiB chunks the fraction
  exceeds 100% (the buffer-end tail dominates the buffer
  itself) and the gate fails. Large AEAD chunks turn ADR 044
  off on narrow projections at warehouse scale.
- See §"Why 16 KiB AEAD chunks: the design-point analysis"
  for the full per-projection-width table.

The chosen 16 KiB design point with buffer-start alignment is
the only fixed-size chunk size in the closed set that preserves
ADR 044 (column-fetch fires comfortably) AND ADR 060 (sparse
fetch narrows ciphertext bytes). Buffer-aligned variable chunks
(the V2 path) achieve marginally lower over-fetch (within ~0.3
pp on the 1-of-50 case) at the cost of an encrypted-directory
surface — deferred to V2.

### 7. Warehouse-scale bench fixture mandate

The bench artifact must include:

- **A 16M+ row runtime fixture** with row groups sized to
  produce 1–10 MiB compressed plaintext.
- **A 100M+ row sampled / modeled fixture** with explicit
  byte / GET / CPU extrapolation.
- The original ADR 060 1M × 16-record × 64 KiB fixture as
  continuity smoke row only.
- **p50 / p95 coalesced GET payload size** on every dense-path
  row. Warehouse-scale dense rows must show p50 ≥ 1 MiB.

### 8. Reject-taxonomy discipline

Closed-catalog discipline applies recursively. ADR 061 adds
**zero** new variants to:

- `EncodingRejectCode`, `EncodingFallbackReason`,
  `RowGroupFallbackReason`, `RowGroupRejectCode`,
  `EncodingComputeRejectCode`,
  `EncodingComputeFallbackReason`, `EncodedRowMask`,
  `L2FetchStrategy`.

A single new path lives in the slab-format error family:
`SlabFormatError::UnsupportedChunkSize`.

### 9. Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- `SLAB_ENCRYPTION_CHUNK_SIZES` is a closed `[usize; 5]` array.
- Closed-set values are exactly `{4096, 8192, 16384, 32768,
  65536}`.
- `chunk_size_bytes` reads thread the value from the slab,
  not from a constant. Forbid uses of
  `SLAB_ENCRYPTION_LEGACY_CHUNK_BYTES` outside slab-format-
  version validation.
- `L2ChunkLayoutConfig` exists with two pinned fields.
- **Macro-block coalescing test exists** at
  `tests/crypto_macro_block_coalescing.rs`.
- **Phase 0 no-directory feasibility note** committed to the
  bench artifact.
- No new `L2FetchStrategy` variant.
- No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability` bit
  / IPC `custom_metadata` key / digest domain.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode addition.
- No new `ObjectStore` trait method.
- ADR 020 §2 AAD layout unchanged (48 B).
- ADR 022 §3 / §5 byte layouts unchanged.
- ADR 056 / 057 directory shapes unchanged.
- **No `AuthenticatedChunkDirectory` type exists** in V1.
- **No `base_chunk_index` field** on `BufferEncoding`.
- Cross-replica byte-equality across {4, 8, 16, 32, 64} KiB
  matrix.
- No Cargo feature gating chunk-size variability.
- No nightly rustc.
- `ChunkSizeBytes` newtype routes through
  `src/schema/units.rs` via `impl_unit!`.
- No version suffix on new types.
- No planning-phase identifier on new symbols.
- No project-name prefix.
- Pad bytes live in IPC body slack between adjacent buffers,
  never enter any logical buffer's `encoded_range`. ADR 057's
  `page_end_offsets` is a contiguous prefix-end array and is
  unchanged.
- Buffer-start-alignment discipline applies only to default
  sparse engagement set; non-engaged cascades share chunk size
  but emit unpadded buffer starts. **No intra-buffer pad bytes
  in any cascade.**
- `SlabFormatError::UnsupportedChunkSize` lives in slab-format
  error family, not in any `*FallbackReason` enum.
- Architecture test rejects `_` catch-all arms on chunk-size
  dispatch.

## V1 deferred (with explicit V2 promotion paths)

ADR 061 deliberately does **not** ship:

- **Variable-sized buffer-aligned chunks via encrypted
  `AuthenticatedChunkDirectory`**. **V2 promotion path:** if
  post-V1 evidence shows ADR 044 column-fetch over-fetch (~3%
  at warehouse scale) becomes a measurable bottleneck on
  operational query workloads, V2 adopts buffer-aligned chunks.
  The V2 ADR introduces the encrypted directory + footer
  locator + reader bootstrap path. V1 does not foreclose this.
- **Warehouse-ladder AEAD chunk sizes** (`{1, 4, 16, 64}` MiB
  chunks). **No V2 path** — large AEAD chunks regress ADR 060
  and ADR 044 simultaneously, with no compensating benefit the
  macro-block coalescer doesn't already provide. Operator
  surface for explicit chunk-size tuning is a separate
  decision.
- **Operator-tunable chunk size** beyond the closed set.
- **Per-record chunk size.** Per-slab is the grain.
- **Chunk sizes outside the closed set.** Smaller (sub-4 KiB):
  AEAD-tag overhead exceeds 0.4% with no compensating sparse
  leverage. Larger (>64 KiB): regresses ADR 060 sparse and
  ADR 044 columnar.
- **Forced page-to-chunk alignment within buffers.** V1
  treats page-to-chunk alignment as a *consequence* of buffer-
  start alignment, not a guarantee — chunk-size-divisor pages
  align naturally; non-divisor pages straddle chunks and pay
  bounded boundary over-fetch. Forcing alignment for non-
  divisor pages would require either intra-buffer pad bytes
  (unrepresentable in ADR 057's contiguous `page_end_offsets`)
  or a directory shape change. Deferred to V2 with the
  encrypted-directory follow-up.
- **Buffer-start alignment for non-default cascades.** Plain /
  Lz4Frame / Delta / Fsst / Alp / Rle buffers emit at the
  running plaintext cursor (may be mid-chunk). ADR 060 routes
  these cascades dense, so mid-chunk starts are acceptable.
- **Chunk-aligned predicate-fetch.** Separate ADR.
- **Adaptive / learned chunk-size selection.** V1 selector is
  pure-deterministic.
- **Cross-slab chunk-aligned coalescing.** Out of scope.
- **`ObjectStore` trait change.** Out of scope.
- **Migration sweeper for existing 64 KiB slabs.** ADR 055 LSM
  compaction picks up smaller chunk sizes on natural
  recompacts.
- **Row-group sizing recommendations.** ADR 056 governs row-
  group sizing; ADR 061 surfaces but doesn't mandate that
  warehouse profiles use 1+ MiB compressed row groups. Separate
  ADR 056 amendment if council wants.

## Council convergence path

Five iterations across Claude / Codex / Gemini converged on the
two-layer architecture documented above. The decision points
that drove convergence:

- **Iter-1**: All three drafts reached for sub-64-KiB chunks
  but didn't yet separate the AEAD and network layers.
- **Iter-2 (Gemini, the breakthrough)**: identified that the
  AEAD authentication boundary and the physical network read
  unit are independent layers optimized for different forces.
  This is the load-bearing insight; everything else flows from
  here.
- **Iter-2 (Claude, withdrawn)**: enlarged chunks to row-group
  grain (1–4 MiB) for analytics throughput. Self-withdrew at
  iter-3 once the decoupling argument made it clear that
  large AEAD chunks raise ADR 060's sparse floor instead of
  closing it; analytics throughput already lives at the
  coalesced GET layer regardless of chunk size.
- **Iter-2 (Codex)**: introduced the warehouse-scale bench
  fixture mandate (16M+ runtime, 100M+ sampled). Adopted into
  V1 promotion gates.
- **Iter-3 (Claude)**: proposed small fixed chunks
  `{4, 8, 16, 32, 64} KiB` + page alignment + macro-block
  coalescing as the load-bearing test, on top of Gemini's
  iter-2 decoupling.
- **Iter-4 (Codex)**: proposed warehouse-ladder defaults
  `{1, 4, 16, 64} MiB`. Withdrawn at iter-5 once the cost
  arithmetic and ADR 044 over-fetch numbers showed large AEAD
  chunks regress both ADR 060 and ADR 044. Codex also
  contributed two adopted gates: Phase 0 no-directory
  feasibility check + p50/p95 GET-payload bench metric.
- **Iter-4 (Gemini)**: proposed variable buffer-aligned chunks
  via encrypted `AuthenticatedChunkDirectory`. Withdrawn at
  iter-5 once the analysis showed ADR 044 over-fetch at 16 KiB
  is already within ADR 044's tolerance (~3% ≪ 67% threshold).
  Deferred to V2 with explicit promotion path: post-V1
  evidence that ADR 044 over-fetch becomes a measurable
  bottleneck.
- **Iter-5/6 (Codex + Gemini)**: full convergence. Codex
  withdrew warehouse-ladder defaults and the
  `AuthenticatedChunkDirectory`. Gemini wrote "Full Swarm
  Council Convergence" and adopted the synthesis verbatim.
  Both drafts adopted Claude's worked-example cost framing.
  Codex iter-6 contributed the "rejection shape" multi-row
  cost format adopted in §"Worked example" above.

## Rejected alternatives

- **Row-group-aligned AEAD chunks (1–4 MiB).** Conflate AEAD
  authentication boundary with physical network read unit;
  destroy ADR 060 sparse leverage by raising the chunk floor.
  Analytics throughput already lives at the coalesced GET
  layer regardless of AEAD chunk size.
- **Warehouse-ladder AEAD chunk sizes** (`{1, 4, 16, 64}` MiB).
  Regress ADR 060 (1+ MiB sparse floor) AND ADR 044 (over-
  fetch threshold exceeded → column fetch declines). The
  macro-block coalescer already provides analytics throughput
  regardless of chunk size; large AEAD chunks add nothing.
- **Variable buffer-aligned chunks via encrypted directory.**
  Strictly better on ADR 044 over-fetch, but marginal at
  warehouse scale (~3% → 0%). Adds encrypted directory +
  footer locator + bootstrap fetch + new validator. **Natural
  V2** if post-V1 evidence shows the marginal win matters.
  V1 prefers the no-directory path.
- **`base_chunk_index` per `BufferEncoding`.** Not needed;
  chunk index is computable in O(1) from existing fields
  (`BufferEncoding.encoded_range` + slab `chunk_size_bytes`).
- **Smaller chunk size as a global constant.** Forces every
  legacy slab through migration; per-slab variable is right.
- **Free-form `chunk_size_bytes`.** Operator could pick a
  size that breaks AES-GCM safety bounds. Closed set fixes this.
- **Page-padding to whole chunks.** Catastrophic plaintext
  inflation when pages are much smaller than chunks.
- **New AEAD primitive at page granularity.** Same primitive
  at different chunk size; no new cryptographic surface needed.
- **Page-level min/max stats as substitute.** Different
  optimization (predicate fetch vs projection fetch);
  composable, not substitute.
- **Provider-native multi-range API.** Coalescer at layer 2
  already produces big GETs from the existing portable
  `get_range`.
- **`L2FetchStrategy::ChunkAligned` variant.** Chunk size is
  per-slab metadata, not a fetch strategy.
- **Footer extension carrying per-page chunk maps.** Existing
  ADR 056 / 057 directories cover what the reader needs.
- **Two-profile config (warehouse vs lower-bound).** Eliminated
  by the two-layer decoupling — there is no warehouse vs
  sparse trade-off at the AEAD layer; both benefit from small
  chunks. Warehouse throughput lives at the coalesced-GET
  layer regardless of chunk size.
- **Tiny AEAD chunks without a coalescing test.** The Phase 5
  macro-block coalescing test is the load-bearing invariant
  that makes the design coherent.

## What not to do

- **Don't promote `chunk_size_bytes` to a public operator
  knob.**
- **Don't change ADR 020 AAD or nonce layout.**
- **Don't add a new `EncodedRowMask` / `EncodingRejectCode` /
  fallback variant.**
- **Don't add an encrypted directory in V1.** ADR 056 / 057 +
  slab field suffice.
- **Don't add per-buffer `base_chunk_index`.** Computable.
- **Don't fetch chunks across slabs to coalesce.**
- **Don't compose with provider-native multi-range API.**
- **Don't ship without the macro-block coalescing test.** This
  is the load-bearing architectural invariant.
- **Don't ship without warehouse-scale bench fixtures.**
- **Don't ship without the Phase 0 no-directory feasibility
  check.**
- **Don't introduce `Wendelon`-prefixed type names.**
- **Don't introduce version suffixes.**
- **Don't widen the closed chunk-size set without an ADR
  bump.**
- **Don't reach for sub-4-KiB AEAD chunks** — AEAD-tag overhead
  exceeds 0.4% with diminishing sparse leverage.
- **Don't reach for chunks > 64 KiB** — regresses ADR 060
  sparse fetch and (above ~256 KiB) regresses ADR 044 column
  fetch.
- **Don't pad pages on cascades outside the default sparse
  engagement set.**
- **Don't make chunk size a per-table descriptor field.**
- **Don't introduce planning-phase identifiers.**
- **Don't conflate AEAD chunks with network GETs.** The
  decoupling is the load-bearing architecture.
- **Don't treat warehouse throughput as a chunk-size
  question.** It's a coalescer + row-group-sizing question;
  warehouse-ladder AEAD chunks are the wrong layer.
- **Don't treat ADR 044 over-fetch as a V1 problem.** At 16
  KiB chunks and warehouse-scale records, over-fetch is ~3%,
  well within ADR 044's existing 67% tolerance. Variable
  buffer-aligned chunks are V2 territory.

## Convergence summary

```text
ADR 056 row-group fetch + page directory  (analytics-scale row groups)
  → ADR 057 per-buffer pages + cascades   (CPU-vector pages)
  → ADR 058 substrate COCD                (compute over encoded)
  → ADR 059 native COCD kernels           (encoded compute fast)
  → ADR 060 sparse projection fetch       (sub-row-group page selection,
                                           plaintext savings)
  → ADR 061 page-aligned AEAD chunks      (sub-record ciphertext savings
    + macro-block coalescing                via small AEAD chunks; analytics
                                            throughput preserved via existing
                                            coalescer producing macro-block
                                            GETs from contiguous chunks)  ← here
  → next Toron-adjacent follow-up
    (ADR 056 row-group sizing for warehouse profiles;
     ADR 061 V2 — variable-sized buffer-aligned chunks via
       encrypted directory, if post-V1 evidence shows ADR 044
       column-fetch over-fetch becomes a bottleneck;
     page-level min/max stats; predicate-fetch chunk narrowing;
     RLE value-run format; sparse predicate-column fetch;
     dense predicate-column zip with sparse projection;
     nullable sparse projection; Fsst evaluator)
  → capacity-tier compression / serverless / EC  (rest of #17 bundle)
```

## Promotion criteria

ADR 061 promoted from council-converged → shipped when:

1. `SLAB_ENCRYPTION_CHUNK_BYTES` renamed to `_LEGACY_` and
   `SLAB_ENCRYPTION_CHUNK_SIZES = [4096, 8192, 16384, 32768,
   65536]` introduced.
2. **Phase 0 no-directory feasibility check passes**: documented
   in bench artifact.
3. Writer selector lands at
   `src/storage/encoding/chunk_layout.rs`, pure-deterministic.
4. Buffer-start-alignment discipline lands in encoded-buffer
   write path; opaque pad bytes inserted only in IPC body slack
   between adjacent buffers (no intra-buffer pad — ADR 057's
   contiguous `page_end_offsets` is unchanged).
5. `L2ChunkLayoutConfig` exists; default `enabled = true`
   after Phase 9 evidence.
6. ADR 060 sparse projection fetch reads `chunk_size_bytes`
   from the slab.
7. **Macro-block coalescing test passes** with all four sub-
   properties (single dense GET, AEAD-call count bounded,
   clustered sparse coalesce, anti-clustered sparse cap).
8. Byte-identical projected-batch property suite passes under
   `cargo test` across the {4 KiB, 8 KiB, 16 KiB, 32 KiB,
   64 KiB} chunk-size matrix.
9. Bench artifact at `plans/impl/061_*_bench_results.md`
   demonstrates ciphertext-byte ≥ 3× reduction on warehouse-
   scale clustered fixtures, GET-count non-regression on dense
   paths, **p50 ≥ 1 MiB on warehouse-scale dense GET payloads**,
   wall-time non-regression on whole-record / dense-projection
   paths, pad inflation ≤ 5%, byte-identical output, cross-
   replica determinism, legacy 64 KiB slabs read identically,
   mandatory 16M+ row runtime fixture and 100M+ row sampled
   fixture.
10. ADR 020 / 022 / 044 / 056 / 057 / 058 / 059 / 060 focused
    tests stay green across the chunk-size matrix.
11. Architectural lockdown guards land per §"Architectural
    lockdown".
12. Cross-replica determinism row in the bench artifact records
    identical chunk size + identical ciphertext bytes across
    replica writes of the same logical slab.
