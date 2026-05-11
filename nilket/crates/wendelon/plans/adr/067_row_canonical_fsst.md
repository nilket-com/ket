# ADR 067: Row-Canonical FSST Framing and Strategy B Predicate Evaluator

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/067_row_canonical_fsst.md`

## Status

Shipped 2026-05-10 after five council iterations
across Claude / Codex / Gemini, one V2 frame-layout
amendment, and Phase 7 promotion-evidence sweep at
8K / 65K / 1M rows. V1 implementation and Phase 7
evidence: `plans/impl/067_row_canonical_fsst.md` and
`plans/impl/067_row_canonical_fsst_bench_results.md`.
Default-on for `EncodingCascadeKind::FsstRowSafe` and
`EncodingComputeEvaluatorKind::FsstRowSafe`.

The V2 frame-layout amendment landed pre-promotion:
the original `(N+1) × u32` encoded-offsets table was
replaced by a per-256-row `(payload_offset: u32,
exception_index: u32)` `block_offsets` pair table plus
a per-row `u8` `encoded_lengths` array (with `255`
sentinel) and an `exception_lengths` table for rows
encoding to ≥ 255 bytes. Metadata overhead dropped
from ~4 B/row to ~1.03 B/row, which is what made the
≤ 10% compression-loss guard pass on representative
URL and weak-compression warehouse fixtures.

Council waived the ADR 057 ≤ 75% writer-cost gate
(observed ~140% writer overhead for the eager
dual-encode strategy) given the transformative
read-side wins on a read-heavy warehouse workload.
Rationale and follow-up path documented in the
bench-results artifact §"Writer-cost gate waiver".

Anchored on ADR 066 V1 perf-iteration post-mortem
(`plans/impl/066_fsst_evaluator_bench_results.md`
§"V1 Perf Iteration 1") which proved Strategy A's
streaming token decode cannot beat dense Arrow's
vectorized equality scan at small/medium row counts —
the per-byte branched decode-and-compare loop is the
algorithmic ceiling, regardless of loop hygiene.

ADR 066 itself committed to this follow-up: *"Strategy B
(pre-encoded literal compare). … V2: dedicated ADR
(writer codec amendment + kernel optimization)."* This
ADR is that follow-up.

The full council-decision list (12 items, organized by
topic with attribution and rationales) lives at the
end of this ADR in §"Council decisions". The V1
contract immediately below is the operational
summary; the council-decisions section is the
archival record.

ADR 067 V1 contract:

- **Writer amendment (row-canonical)**: introduce a
  row-aligned FSST frame (`WFS2`) where the encoder
  (a) trains symbol pairs only over within-row byte
  pairs, (b) never emits a symbol pair spanning a row
  boundary, and (c) follows a deterministic canonical
  algorithm so that two encoders fed the same row
  bytes + symbol table produce byte-identical output.
- **Encoded-offsets-in-frame**: the `WFS2` frame embeds
  a per-row encoded-byte-offsets table after the symbol
  table and before the encoded payload. Reader
  identifies row N's encoded byte range by a single
  table lookup.
- **New cascade variant**: `EncodingCascade::FsstRowSafe`
  (discriminant 9, label `"fsst_row_safe"`) extends
  ADR 057's leaf cascade catalog by one. Existing
  `EncodingCascade::Fsst` buffers stay readable via
  Strategy A (ADR 066 V1 reader); new writes default
  to `FsstRowSafe` when the per-buffer actual-size
  guard clears.
- **Strategy B reader**: pre-encode each literal once
  per buffer using the same canonical algorithm as
  the writer, then per row do (a) decoded-length pre-
  filter, (b) encoded-length pre-filter, (c) memcmp on
  the row's encoded byte range against the pre-encoded
  literal. No streaming token decode on the predicate
  path.
- **Dense WFS2 decode**: full token-stream decode that
  produces the original values bytes. Required for
  evaluator-disabled reads, unsupported atoms,
  Sub-case A2 overlap paths, and as the correctness
  oracle for Strategy B.
- **Per-buffer writer size guard**: row-safe candidate
  is selected only when both (a) `wfs2_len ≤
  decoded_plaintext_len` (compression actually
  positive) and (b) `wfs2_len ≤ ceil(wfs1_len * 1.10)`
  (compression-loss budget vs. existing `Fsst`).
- **`ENCODING_DIRECTORY_FORMAT_VERSION` bump** required:
  the cascade catalog grows by one variant. Old readers
  encountering `FsstRowSafe` fail closed via the
  existing version-mismatch path before cascade
  inspection. Rollout is reader-before-writer.
- **No new public protocol surface**: no IPC
  `custom_metadata` key, no wire `MsgType`, no
  `NodeCapability` bit, no `ObjectStore` trait method,
  no Cargo feature, no operator-visible knob beyond
  `enabled_predicate_evaluators` and the existing
  `enabled_cascades`.
- **No removal of ADR 066 V1 reader**. Strategy A
  remains the path for `EncodingCascade::Fsst` buffers
  (back-compat). New buffers under `FsstRowSafe` use
  Strategy B.
- **Promotion gates**: ≥2.0× predicate wall-clock at
  clustered 5%, ≥1.5× end-to-end through ADR 065 →
  ADR 060 / 064 Case A, ≤5% dense-fallback overhead
  on declined paths, ≤10% compression-ratio loss on
  representative warehouse fixtures vs. existing
  `Fsst` cascade. Headline diagnostic (recorded, not
  gating): ≥5× CPU speedup target on representative
  `Eq` fixtures (much higher than ADR 066's 2× because
  Strategy B avoids decode entirely on the predicate
  path).

Phase 0 audit pins the existing WFS1 codec, the FSST
encoder's symbol-table selection, the manager's cascade
dispatch site, and the writer's row-boundary signaling
(currently the encoder runs over the entire values
buffer with no row-boundary awareness — Phase 0
confirms how to thread row boundaries through the
encoder cleanly). Bench evidence will land at
`plans/impl/067_row_canonical_fsst_bench_results.md`
at promotion.

## Context

ADR 066 V1 shipped a streaming-token-decode FSST
predicate evaluator (Strategy A) and ran the council's
combined Phase A + Phase B perf iteration in pursuit of
default-on enablement. The post-mortem at
`plans/impl/066_fsst_evaluator_bench_results.md`
§"V1 Perf Iteration 1" established three load-bearing
findings:

**1. The decode loop is the bottleneck.** The kernel's
per-byte branched decode (symbol / escape / literal
dispatch in `next_byte`) is the dominant cost. Memcmp
optimizations of the comparison phase (Phase B) cannot
recover the ~40× extra decode work imposed when the
short-circuit on first-byte mismatch is removed.

**2. The fixed per-buffer overhead amortizes only at
large row counts.** Kernel/dense ratio shrinks with row
count: ~1.4× slower at 8K, ~1.3× slower at 65K, ~0.83×
faster at 1M. Below 1M rows the dense path wins because
its decode loop vectorizes and its equality scan runs in
SIMD over decoded plaintext.

**3. Strategy A has no algorithmic improvement
available** within "stream encoded bytes, compare
against decoded" framing. The original `matches_one`
short-circuit on first byte mismatch is already near-
optimal for the rejection-dominant paths that 5%
selectivity workloads produce.

The only structural lever capable of beating dense at
small/medium row counts is **avoiding decode entirely
on the predicate path**. That is Strategy B: pre-encode
the literal once, then compare its encoded byte
sequence against each row's encoded byte range.

Strategy B is correctness-unsafe under the current WFS1
encoder because FSST's greedy encoding does not respect
Arrow row boundaries. A two-byte symbol can span the
boundary between row N and row N+1, contributing
decoded bytes to both. The encoded byte sequence at
row N's nominal start position therefore has no fixed
relationship to the row's plaintext. Pre-encoding
"/checkout/confirm" against the symbol table yields
some encoded byte sequence E; but the encoded bytes for
the row containing that same plaintext can differ from
E because the row's encoded form was influenced by
adjacent rows' bytes.

The fix is structural: change the writer so that
**symbols cannot span Arrow row boundaries**. Once that
holds, a row's encoded bytes are the deterministic
output of a fresh-state encoder fed only that row's
plaintext. Pre-encoded literal compare becomes
correctness-safe.

This is an ADR 057-adjacent change. ADR 057 ships the
cascade catalog and the WFS1 frame; ADR 067 extends the
catalog by one variant (`FsstRowSafe`) and ships the
new WFS2 frame format alongside the existing WFS1.
Existing FSST buffers stay readable via Strategy A.
New writes default to row-safe encoding; readers
dispatch on cascade variant.

### Five forces

**1. Strategy A's V1 ceiling is structural, not
implementation-quality.** The post-mortem proves that
loop-hygiene-level optimizations (op-hoist, MaskBuilder
batching, length-bucket allocation) move the bench
needle within ±5% — i.e. noise. The structural cost is
the per-byte symbol-table dispatch in the decode path,
which cannot be vectorized without table-in-register
tricks that don't fit a stable-Rust kernel. To beat
dense, the kernel must avoid per-row decode altogether.

**2. Pre-encoded literal compare requires deterministic
per-row encoding.** A literal "/checkout/confirm"
encoded against symbol table T produces a deterministic
byte sequence E only if the encoder runs from a fresh
state. If the encoder carries state from prior rows
(pending high-byte from the previous row's last
symbol), the literal's encoded form depends on the
state at its start position — which depends on what
preceded the row in the buffer. Strategy B is therefore
correctness-coupled to a writer guarantee that each row
encodes from a fresh state.

**3. The encoded-offsets table is the bridge.** The
reader needs to find row N's encoded byte range without
walking the encoded payload from byte 0 (which would
defeat Strategy B's economic claim — the entire point
is to avoid touching most of the encoded payload). The
writer therefore emits a per-row encoded-offsets table
alongside (or embedded in) the values buffer. Reader
indexes this table by row to find encoded ranges in
O(1).

**4. Compression cost is bounded but real.** Forbidding
cross-row symbols sacrifices compression at row
boundaries: a frequent two-byte sequence that happens
to span a row boundary cannot use a single symbol. For
long rows (URLs ~44 bytes, log lines hundreds of bytes)
the loss is ≤1 symbol per row — negligible. For short
rows (UUID 36 bytes, fixed identifiers ≤16 bytes) the
loss is more proportional but still bounded. Phase 7
bench measures this empirically; the council's
promotion gate caps acceptable compression loss at 10%.

**5. Existing WFS1 compatibility is preserved.** WFS2
is explicitly *not* backwards-compatible with old
readers — Decision §10 covers the version-mismatch
rollout — but the forward direction is additive. ADR
057's cascade catalog is closed-by-design but extends
by addition. Old `EncodingCascade::Fsst` (WFS1)
buffers stay readable via Strategy A (ADR 066 V1
reader path is unchanged), indefinitely, by new
binaries. The new `EncodingCascade::FsstRowSafe`
(WFS2) variant is dispatched via the existing
`evaluator_kind_for_cascade` site to a new evaluator
kind (`EncodingComputeEvaluatorKind::FsstRowSafe`)
that runs Strategy B. Two-step dispatch discipline
applies recursively. No data migration required; new
writes opt in.

### What this ADR does not propose

- **No retirement of the existing `EncodingCascade::Fsst`
  cascade variant.** Old buffers stay readable via ADR
  066 V1's Strategy A path indefinitely. If operators
  want to migrate, they re-encode (ADR 057 substrate
  already supports re-encoding via row-group rewrite).
- **No silent migration of existing WFS1 buffers.** Re-
  encoding is an explicit row-group rewrite operation;
  there is no implicit / on-the-fly conversion.
- **No old-binary read compatibility for WFS2.** New
  binaries read both WFS1 and WFS2; old binaries reject
  the bumped `ENCODING_DIRECTORY_FORMAT_VERSION`
  before they can inspect cascades. The compatibility
  guarantee is one-way: new readers continue reading
  old WFS1 data. Rollout is reader-before-writer.
- **No retirement of the ADR 066 V1 Strategy A reader.**
  It remains the dispatch target for `EncodingCascade::Fsst`
  cascade buffers.
- **No new `PredicateAtomOp` variant.** Strategy B
  evaluates the same closed atom set as ADR 066 V1:
  `Eq` / `Neq` / `InList`. `IsNull` / `IsNotNull` route
  via the existing ADR 059 validity-only path
  unchanged.
- **No range / `LIKE` / regex / substring on FSST.**
  The encoded form does not preserve byte-lexicographic
  order; `<` / `>` / `<=` / `>=` decline to dense
  fallback as in ADR 066. `LIKE` / regex / substring
  are deferred to a future ADR that adds those
  `PredicateAtomOp` variants first.
- **No FSST aggregate folds.** `string_min` /
  `string_max` / `count_distinct` on encoded form is a
  separate kernel proof and requires collation policy.
- **No page-grain row-safe FSST.** Buffer-grain only,
  matching ADR 066 V1. Page-grain remains a separate
  follow-up ADR coupled with ADR 057 page-emission for
  FSST.
- **No sparse projection eligibility lift.** FSST is
  variable-width; ADR 060 / 064 deferral inherits
  unchanged. Sub-case A1 (FSST predicate column not in
  output) migrates to Case A; Sub-case A2 (overlap)
  remains Case B per ADR 066's Decision §12.
- **No symbol-table caching across queries.** Per-
  buffer parse remains V1.
- **No writer-side compression-loss budget knob.** The
  writer always row-aligns; the council's 10%
  compression-loss gate is an evidence threshold, not a
  runtime tunable.
- **No new public protocol surface.** No IPC
  `custom_metadata` key, no wire `MsgType`, no
  `FRAME_FLAG_*`, no `NodeCapability` bit, no digest
  domain, no `ObjectStore` trait method, no descriptor
  field, no fingerprint shape change, no Cargo feature,
  no operator-visible knob beyond the existing
  `enabled_cascades` and `enabled_predicate_evaluators`
  (no new top-level config field for this ADR).

### Constraints carry forward unchanged

ADR 003, 016, 019, 020, 022, 029, 030, 035, 036, 037,
039, 040, 042, 043, 044, 052, 053, 054, 055, 056, 058,
059, 060, 061, 062, 063, 064, 065, 066. **ADR 057
extends by one cascade variant (`FsstRowSafe`) and one
new on-disk frame magic (`WFS2`); the existing `Fsst`
cascade and `WFS1` frame remain unchanged.** ADR 058's
evaluator catalog extends by one (`FsstRowSafe`); the
existing seven (after ADR 066) remain shipped. ADR 059
default predicate allow-list extends by `FsstRowSafe`
only if Phase 7 evidence clears the council's
promotion gates.

## Decision

ADR 067 ships a row-aligned FSST writer amendment
(`WFS2` frame, `EncodingCascade::FsstRowSafe`) and a
matching Strategy B predicate evaluator
(`EncodingComputeEvaluatorKind::FsstRowSafe`). The
amendment is a pure addition to ADR 057's cascade
catalog; it does not modify the existing `Fsst`
variant or `WFS1` frame. Strategy B avoids per-row
decode entirely on the predicate path by pre-encoding
each literal once per buffer and comparing encoded
byte ranges directly.

1. **`EncodingCascade::FsstRowSafe` is the new cascade
   variant.** Concrete discriminant numbering (per
   Phase 0 of Codex's audit): `Fsst = 7` (existing),
   `Alp = 8` (existing), **`FsstRowSafe = 9`** (new).
   Carries the same `symbol_table_byte_len: RecordBodyByteLen`
   field as `EncodingCascade::Fsst` for parallel-shape
   reasons; cascade-field disposition for both variants
   is the same out-of-scope question (separate trivial
   directory-layout follow-up ADR).

2. **`WFS2` is the new on-disk frame.** Layout:
   - 4-byte magic `b"WFS2"`.
   - 1-byte symbol_count (≤127, same encoder cap as
     WFS1 per the existing 7-bit symbol-vs-literal
     flag).
   - `2 × symbol_count` bytes of symbol-table pair
     entries (same shape as WFS1).
   - 4-byte row_count.
   - `block_offsets`: `ceil(row_count / 256) * 8` bytes.
     (Note: `block_count` is derived, not stored).
     Each entry is a 32-bit little-endian pair
     `(payload_offset, exception_index)` providing
     O(1) random access into any 256-row block.
   - `encoded_lengths`: `row_count` bytes (u8).
     Stores the encoded byte length of each row.
     A value of `255` is a sentinel indicating the
     actual length is stored in `exception_lengths`.
   - 4-byte exception_count (u32 little-endian).
   - `exception_lengths`: `exception_count * 4` bytes
     (32-bit little-endian lengths for rows ≥ 255 bytes).
   - Encoded payload: same byte semantics as WFS1
     (literal `< 0x80`, escape `0xff`, symbol `≥ 0x80`
     with high bit clearing to 7-bit symbol index).
     
     If `row_count == 0`: zero blocks, zero lengths,
     zero exceptions, empty payload.
     If `row_count % 256 != 0`: the final block
     has 1-255 lengths.
     The writer guarantees that for every row i,
     the encoded byte range `[current_offset_i,
     current_offset_i + encoded_len_i)` (where
     `current_offset_i` is the sum of all prior
     rows' encoded lengths, seedable from
     `block_offsets[i / 256]`, and `encoded_len_i`
     is `encoded_lengths[i]` if `< 255` else
     `exception_lengths[exc_idx_at_row_i]`) decodes
     independently from a fresh state into exactly
     the row's decoded bytes. **No symbol pair
     spans any row boundary.**

3. **Writer-side row-canonical encoding is the load-
   bearing contract.** The encoder accepts the values
   buffer plus the Arrow row-offsets buffer (which it
   already has access to for variable-width types) and
   produces a deterministic, byte-identical output
   per row given a fixed symbol table. The contract
   has three properties:

   - **Row boundary independence (row-safe).** At
     each row boundary in the input, the encoder
     flushes any pending symbol-state. The encoder
     never emits a symbol pair whose two decoded
     bytes span a row boundary. If the next-2-byte
     pair would cross the boundary, the encoder
     emits the single byte (literal or escape) before
     the boundary and starts fresh after. Encoded
     bytes for row N decode into row N only.

   - **Encoded row indexing (offsets table).** For
     every row, the encoder records the encoded byte
     position at the row's start in the WFS2 frame's
     encoded-offsets table. Reader can find row N's
     encoded byte range by a single table lookup.

   - **Canonical algorithm (deterministic).** For
     any row bytes R and symbol table T, the writer
     produces a single deterministic encoded byte
     sequence E. The reader's literal pre-encoder
     uses **the identical algorithm** to encode
     literals; pre-encoding the same R under the
     same T yields the same E. This is stronger than
     "row-safe": "row-safe" merely guarantees no
     cross-row symbols (necessary for correctness);
     "row-canonical" additionally guarantees
     determinism (necessary for Strategy B to use
     encoded byte equality as a proxy for decoded
     value equality). The shared algorithm is:
     (i) build the symbol table by counting adjacent
     byte pairs **within Arrow value boundaries
     only**; (ii) sort pairs by descending count then
     ascending pair key for tie-breaking; (iii) at
     each position, prefer a symbol for the next two
     bytes when the pair is in the table and both
     bytes are inside the same row; (iv) otherwise
     emit one literal byte (or escape + high byte for
     bytes ≥ 0x80). Step (i) and (ii) match WFS1's
     existing symbol-table construction; only step
     (iii)'s row-boundary check differs.

   The writer's contract is enforced by tests: a
   property test compares `pre_encode_literal(R, T)`
   against `writer_encoded_row_bytes(R, T)` for
   randomly-chosen R and T; they must always be
   byte-identical.

4. **Strategy B reader is the new evaluator kind.**
   `EncodingComputeEvaluatorKind::FsstRowSafe` (label
   `"fsst_row_safe"`) registers the new evaluator.
   Per-buffer setup:
   - Parse the WFS2 frame: extract symbol table,
     row_count, encoded-offsets table, encoded
     payload.
   - For each literal (typically 1 for `Eq`, up to N
     for `InList`):
     - Pre-encode the literal: feed its bytes through
       a fresh-state FSST encoder using this buffer's
       symbol table. Result: a deterministic encoded
       byte sequence E_literal. Cache the per-literal
       (encoded_bytes, encoded_len, decoded_len)
       triple in a small per-buffer scratch
       structure.
   Per row (seed `(current_offset, exc_idx) =
   block_offsets[0] = (0, 0)` once before the loop):
   - `decoded_len_row = decoded_offsets[i+1] - decoded_offsets[i]` (length pre-filter on
     decoded length).
   - `encoded_len_row = encoded_lengths[i] as u32`
     if `encoded_lengths[i] != 255`; else
     `encoded_len_row = exception_lengths[exc_idx]; exc_idx += 1`
     (length pre-filter on encoded length).
   - For the relevant literal(s) of matching decoded
     length: compare `encoded_len_row ==
     encoded_len_literal`; if yes, memcmp the row's
     encoded byte range
     `encoded_payload[current_offset
     .. current_offset + encoded_len_row]` against
     the literal's encoded bytes.
   - Advance unconditionally:
     `current_offset += encoded_len_row` (both
     length-matching and length-mismatching rows
     advance, to keep the running state coherent).
   - `row_matches_any_literal = …` per ADR 066
     §"Decision §3" semantics.
   - Apply op-specific inversion: bit = matches for
     `Eq` / `InList`; bit = !matches for `Neq`.
   - Fold with validity via `kernels/validity.rs`.

5. **The kernel does not decode any encoded byte on
   the predicate path.** This is the structural
   inversion of Strategy A. The encoded payload is
   untouched except for the bytes inside matching-
   length encoded ranges (and only via memcmp).
   Length-mismatch rows are skipped via the offsets
   table without touching encoded bytes at all. An
   architectural guard pins this.

6. **Length pre-filter is two-tiered.** Decoded
   length pre-filter (decoded_len_row vs
   decoded_len_literal) eliminates obvious
   mismatches in O(1) per row from the offsets
   buffer. Encoded length pre-filter (encoded_len_row
   vs encoded_len_literal) eliminates compression-
   variant mismatches in O(1) per row from the
   encoded-offsets table. Both checks fire before
   any byte comparison.

7. **Symbol-table parse is bounded and per-buffer.**
   Same shape as ADR 066 V1: parse once per buffer
   per query; no global cache; bounded scratch.
   Cross-buffer symbol-table consistency is not
   assumed (different buffers may have different
   symbol tables).

8. **Pre-encoded literal cache is bounded per
   buffer.** For each literal: encoded bytes
   occupy at most `2 × literal_len` (worst case is
   no compression — every byte becomes literal +
   escape pair). For `Eq` (one literal) the cache
   may live on the stack as a fixed-size array.
   For `InList(N)` with longer literals, the cache
   is a bounded heap allocation (one `Vec<u8>` of
   total capacity `2 × sum(literal_len)`,
   amortized over the buffer's row count). Built
   once per buffer at kernel entry; reused across
   all rows. No per-row encoding work. If a
   configured per-buffer scratch limit would be
   exceeded (corner case: very large literals or
   very large `InList`), the kernel declines the
   buffer to dense fallback rather than allocating
   unboundedly.

9. **Fail-closed semantics with explicit corruption
   boundary.** Strategy B by design does not read
   most of the encoded payload — the entire economic
   claim depends on touching only structural metadata
   plus length-matching memcmp ranges. Full payload
   token-stream validation would defeat that claim.
   The failure surface is therefore three-tiered:

   - **Structural metadata fails closed at parse
     time** via existing `EncodingComputeRejectCode`
     (`KernelDecodedLengthMismatch` /
     `EncodedValueOverflow` / `MaskLengthMismatch`).
     Triggers: bad WFS2 magic, impossible
     `symbol_count`, `row_count` mismatch with
     `BufferEncoding.decoded_value_count`,
     non-monotonic `block_offsets` (either
     `payload_offset` or `exception_index`),
     `block_offsets[0] != (0, 0)`,
     `encoded_lengths.len() != row_count`,
     `exception_count` not equal to the count of
     `255` sentinels in `encoded_lengths`, the
     final running offset not equal to the
     payload byte length, malformed Arrow decoded
     offsets.

   - **Compared ranges are validated only by
     equality with a canonical literal encoding.**
     The kernel does not interpret encoded bytes
     (no token decode), so it cannot detect a
     corrupted compared range that fails to decode
     to valid plaintext — its only check on the
     range is `memcmp` against the pre-encoded
     literal. Under valid data and the canonical
     writer contract, encoded-byte equality is a
     sound proxy for decoded-value equality.
     Property tests in the correctness suite verify
     the writer's canonical contract per buffer
     across representative fixtures (this is the
     only mechanism that catches a canonical-
     contract violation at write time); under
     production AEAD-protected data, contract
     violations cannot occur silently because the
     writer is the only source.

   - **Bytes outside compared ranges trust ADR 020
     authenticated chunks upstream.** Persistent byte
     corruption inside the encoded payload that the
     predicate path never reads is the responsibility
     of the AEAD layer, not Strategy B. The encoding
     frame is trusted only after authenticated chunks
     pass; arbitrary mid-payload corruption inside an
     authenticated chunk is structurally impossible.

   - **Dense WFS2 decode is the full-validation
     path.** Reads that don't go through Strategy B
     (evaluator-disabled, unsupported atom, Sub-case
     A2 overlap) run dense decode, which validates
     every token. The dense-decode path is the
     correctness oracle and the fail-closed path for
     reads that need full payload integrity.

   Buffer-level structural corruption does **not**
   route to ADR 065 Case B/C dense fallback for the
   evaluator path; the corruption is a data-integrity
   failure, not a routing event. Unsupported-cascade
   declines (e.g., evaluator disabled) do route
   through Case B/C as today.

   If the council later decides predicate evaluation
   must reject every invalid token anywhere in the
   payload before producing a mask, that is a
   different performance shape (full-payload scan
   per buffer) and must be benchmarked as a first-
   class alternative — it does not slip into
   Strategy B's V1 contract.

10. **`ENCODING_DIRECTORY_FORMAT_VERSION` bumps by
    one.** Adding a new cascade variant requires the
    bump. Old readers reject the bumped directory at
    version-check time, before they reach cascade
    inspection — they never see the new cascade tag
    or attempt to decode WFS2 bytes. Rollout
    discipline is therefore reader-before-writer:
    deploy new readers that understand the bumped
    version + `FsstRowSafe` + WFS2 everywhere first,
    then enable writers to emit `FsstRowSafe`.
    Existing WFS1 / `Fsst` cascade buffers remain
    readable by new binaries indefinitely; the
    compatibility guarantee is one-way.

11. **Writer default behavior + per-buffer actual-
    size guard.** New writes prefer
    `EncodingCascade::FsstRowSafe` for variable-
    width string columns when the FSST cascade
    selection criteria pass (per ADR 057's existing
    selection logic in `select.rs`) AND the per-
    buffer actual-size guard clears:

    ```text
    select FsstRowSafe iff:
      wfs2_len <= decoded_plaintext_len
      AND wfs2_len <= ceil(wfs1_len * 1.10)
    ```

    The guard is a **per-buffer runtime check**, not
    a bench-fixture promotion gate alone. The writer
    encodes both WFS1 and WFS2 candidates for FSST-
    eligible variable-width buffers and selects WFS2
    only when both conditions hold. If the encoded
    WFS2 frame exceeds the budget (typical for
    short-row workloads where the 4-bytes-per-row
    encoded-offsets overhead dominates), the writer
    falls back to the existing `Fsst` cascade for
    that buffer. The 10% threshold is a draft value
    subject to council convergence based on Phase 0
    micro-bench evidence; the structural point is
    that compression loss must be measured on the
    actual encoded candidates per buffer, not
    inferred from sample shape.

    Operators can override via `enabled_cascades`
    config (disable `FsstRowSafe` to force `Fsst`,
    or vice versa) but the default behavior is
    "prefer row-safe with the safety net."

    Encoding both candidates costs writer CPU; Phase
    7 writer bench measures this overhead. If the
    cost is prohibitive, the council can later add
    a sample-based estimate as a fast pre-check
    before encoding both, but V1 ships with the
    eager dual-encode for correctness over cost.

12. **Mask output and downstream dispatch are
    identical to ADR 066 V1.** Strategy B produces
    mask-only `PhaseBOutcome`; downstream dispatch
    splits by predicate-output overlap on the FSST
    column (Sub-case A1 → Case A; Sub-case A2 →
    Case B). The Strategy B kernel and the Strategy
    A kernel are interchangeable from the outer
    dispatch's perspective.

The slab-format byte layout, AAD shape, nonce
derivation, key hierarchy, footer HMAC scope, ADR 022
footer layout, ADR 056 directory shape, ADR 058 / 059
evaluator catalogs *(by addition only)*, ADR 060
sparse projection planner module location, ADR 061
closed chunk-size set, ADR 062 sizing profile catalog,
ADR 063 page-stats shape, ADR 064 nullable validity
helpers, ADR 065 manager three-case dispatch, and
ADR 066 reader path are all unchanged.

## Architectural inextricability

Five design choices are physically inseparable in V1.

**1. Row-aligned writer is the load-bearing guarantee.**
Strategy B's correctness depends on the writer never
emitting a symbol that spans a row boundary. Any
deviation breaks the equality of encoded byte ranges
to pre-encoded literals. The writer-side check is the
entire point of the amendment.

**2. Encoded-offsets table is the reader's index.**
Without per-row encoded byte positions, the reader
would have to walk the encoded payload to find row N
— defeating Strategy B's economic claim. The
encoded-offsets table is the bridge from row index to
encoded byte range.

**3. New cascade variant separates old from new.**
Reusing `EncodingCascade::Fsst` with a flag would
violate ADR 057's closed-catalog discipline and
require runtime branches in every cascade decode site.
A new variant is the clean addition. Old buffers stay
on `Fsst` (Strategy A); new writes use `FsstRowSafe`
(Strategy B).

**4. Per-buffer pre-encoding amortizes correctly.**
Pre-encoding a literal costs O(literal_len) symbol-
table lookups. Done once per buffer per literal,
amortized over `row_count` rows. The work is
negligible compared to the per-row work it saves.
Per-row pre-encoding (per row × per literal) would
defeat the win.

**5. The dense FSST fallback path remains the single
correctness oracle.** Strategy B's output must be
byte-identical to the dense decode-then-scan oracle
on every correctness fixture, including the
boundary-crossing-symbol fixture (which is now a
writer-correctness fixture: the writer must NOT
produce cross-row symbols, so the fixture verifies
the writer's contract) and the no-compressed-row-
slice regression fixture (carry-forward from ADR 066,
ensuring the reader does not slice the encoded buffer
incorrectly even in the row-aligned case).

## Identity-vs-Locality framing

ADR 067 is identity-neutral and locality-neutral. The
writer amendment changes *how* bytes are encoded; row
identity, record location, and locality decisions are
made upstream. The reader amendment changes *how* a
predicate is evaluated against an FSST-encoded buffer;
row-aligned mask output composes with ADR 060 / 064
sparse projection identically to ADR 066 V1.

Cross-replica determinism is preserved because (a)
FSST encoding under a fixed symbol table is greedy and
deterministic in the writer's encoder, (b) row-
boundary flushing is deterministic, (c) pre-encoding a
literal under a fixed symbol table is deterministic,
(d) length pre-filter is deterministic, (e) memcmp is
deterministic. Mask byte-equality across replicas
holds.

## V1 scope

### 0. Phase 0 audit

Before Phase 1 implementation begins, the council
audits read-only:

**Existing FSST writer:**

- `src/storage/encoding/cascade/codec.rs`
  `encode_fsst_bytes` — pin the current encoder
  structure, particularly how it handles input
  boundaries (currently treats the whole values
  buffer as one stream).
- `src/storage/encoding/select.rs` — pin the cascade
  selection logic that constructs
  `EncodingCascade::Fsst { symbol_table_byte_len: RecordBodyByteLen::new(0) }`
  (line 133-135). Confirm where row-offsets are
  available at write time.
- the writer's access to Arrow row offsets at the
  point where FSST encoding fires. If row offsets
  are not threaded through to the encoder, escalate
  to a separate threading-layer ADR first (V1 does
  not silently expand scope).

**Cascade catalog and frame:**

- `src/storage/encoding/types.rs` — current
  `EncodingCascade` variants; confirm extension by
  one variant is clean (no `#[non_exhaustive]`
  blockers, discriminant numbering, serde shape).
- `src/storage/encoding/directory.rs` — cascade
  serialization (lines 1189, 1244 currently handle
  `EncodingCascade::Fsst`); verify how to add a new
  variant cleanly.
- `ENCODING_DIRECTORY_FORMAT_VERSION` constant
  location and bump procedure.

**Reader-side dispatch:**

- `src/storage/encoding/compute/dispatch.rs` —
  current `evaluator_kind_for_cascade` (line 332
  for `Fsst → Some(EncodingComputeEvaluatorKind::Fsst)`); add `FsstRowSafe → Some(EncodingComputeEvaluatorKind::FsstRowSafe)`.
- `src/storage/encoding/compute/types.rs`
  `EncodingComputeEvaluatorKind` extends by one
  variant.
- ADR 058's pre-kernel eligibility — extends to
  admit `FsstRowSafe` cascades on the same V1
  atom × type matrix as `Fsst`.

**WFS2 frame design:**

- pin the encoded-offsets-in-frame layout (as opposed
  to a separate offsets buffer): trade-off is fewer
  buffer slots vs. larger frame parse. The draft
  proposes in-frame for self-containment.
- pin offset width (32-bit vs 64-bit). 32-bit covers
  buffers ≤ 4 GiB encoded, which dominates V1
  workloads. V2 64-bit offsets are reserved for
  unusually large buffers.

**Bench harness:**

- `benches/encoding_compute.rs` — current FSST
  fixtures (URL, log-line, free-form, identifier,
  JSON-key) become V2 fixtures unchanged. Add
  compression-ratio measurement (`Fsst` vs
  `FsstRowSafe` on the same input).
- `benches/row_group_fetch.rs` — end-to-end
  composition row for `FsstRowSafe`.

Phase 0 also pins how to verify the writer's row-
alignment contract empirically: a fixture where every
two-byte sequence frequent enough to be a symbol
straddles an Arrow row boundary; verify the writer
does NOT promote that pair to a symbol. The encoder's
greedy symbol selection runs once over the corpus to
build the table; the row-aligned encoding step then
respects boundaries.

### 1. Writer amendment

Modify `encode_fsst_bytes` (or introduce a sibling
`encode_fsst_row_safe_bytes`) to:

- Accept the input bytes plus a list of row-boundary
  byte positions in the decoded stream.
- During greedy encoding, treat each row's bytes
  independently: when looking up the next-2-byte pair
  for symbol promotion, never let the pair span a row
  boundary.
- Emit the WFS2 frame: magic + symbol count + symbol
  table + row count + block_offsets (pair of
  `(payload_offset: u32, exception_index: u32)` per
  256-row block) + encoded_lengths (u8 per row;
  `255` = sentinel) + exception_count +
  exception_lengths (u32 per exception) + encoded
  payload.

The encoder builds the symbol table by scanning the
full corpus (so high-frequency cross-row pairs still
appear in the table); the row-alignment constraint
applies only to the per-row encoding pass.

Phase 1 deliverables:

- New writer function (or amended existing function
  with a row-safe path).
- Cascade selection in `select.rs` opts into
  `FsstRowSafe` for variable-width string columns
  by default.
- Round-trip tests: encoded → WFS2 frame parse →
  decoded matches input.
- Compression-ratio measurement: encoded byte length
  vs. existing `Fsst` cascade on the same fixtures.
- Architectural guard: writer never emits a symbol
  pair spanning a row boundary (verified by frame
  inspection on representative fixtures).

### 2. WFS2 frame parser

Add `parse_fsst_row_safe_frame(buffer_bytes,
expected_byte_len) -> Result<FsstRowSafeFrame, _>` in
`src/storage/encoding/compute/kernels/fsst_row_safe.rs`
(new module sibling to `kernels/fsst.rs`).

The parser validates:

- Magic `b"WFS2"`.
- Symbol count ≤ 127.
- `block_offsets[0] == (0, 0)`,
  `block_offsets[*].payload_offset` monotonically
  non-decreasing, `block_offsets[*].exception_index`
  monotonically non-decreasing.
- `encoded_lengths.len() == row_count`.
- `exception_count` equal to the count of `255`
  sentinels in `encoded_lengths`.
- Sum of per-row encoded lengths (resolved through
  the sentinel/exception path) equals encoded
  payload byte length.
- Encoded payload byte count matches header.

Returns an opaque `FsstRowSafeFrame` carrying the
symbol table, `block_offsets` slice,
`encoded_lengths` slice, `exception_lengths`
slice, and encoded payload slice.

### 3. Per-buffer literal pre-encoding

`pre_encode_literal(symbol_table, literal_bytes) ->
Result<EncodedLiteral, _>` runs the same greedy
encoding logic as the writer but on a single literal
input from a fresh state. Returns
`(encoded_bytes: Vec<u8>, encoded_len: usize,
decoded_len: usize)`.

Cached per literal per buffer; reused across all
rows.

### 4. Strategy B kernel

`evaluate_fsst_row_safe_equality(frame, decoded_offsets,
encoded_literals, op) -> Result<EncodedRowMask, _>`
runs the per-row Strategy B body:

- Length pre-filter on decoded length.
- Length pre-filter on encoded length.
- Memcmp against pre-encoded literal.
- Op-specific inversion.
- Validity composition via `kernels/validity.rs`.

The kernel never reads encoded payload bytes outside
of length-matching memcmp ranges. An architectural
guard asserts this.

### 5. Pre-kernel eligibility extension

Extend ADR 058's pre-kernel eligibility to admit
`FsstRowSafe` cascades on the same V1 atom × type
matrix as `Fsst`:

- Atom `Eq` / `Neq` / `InList`.
- Type `Utf8` / `LargeUtf8` / `Binary` /
  `LargeBinary`.
- Literal coerces cleanly to bytes.
- Decoded offsets fetchable.
- Predicate-output overlap on FSST column → Sub-case
  A2, declines to dense (same as ADR 066 V1).

### 6. Evaluator catalog extension

Add `EncodingComputeEvaluatorKind::FsstRowSafe`
(label `"fsst_row_safe"`) to ADR 058's catalog.
Update `ALL`, `as_str`, `from_label`, config
validation, architectural guards. Update
`evaluator_kind_for_cascade`:
`EncodingCascade::FsstRowSafe → EncodingComputeEvaluatorKind::FsstRowSafe`.
Update `encoding_compute_predicate_data_type_supported_for_evaluator`
to admit `Utf8` / `LargeUtf8` / `Binary` /
`LargeBinary` for the new kind.

### 7. Format version bump

Bump `ENCODING_DIRECTORY_FORMAT_VERSION` by one.
Old readers reject the bumped directory at
version-check time, before they reach cascade
inspection — they never see the new cascade tag
or attempt to decode WFS2 bytes. Rollout
discipline is therefore reader-before-writer:
deploy new readers everywhere first, then enable
writers to emit `FsstRowSafe`. New readers handle
both `Fsst` (WFS1) and `FsstRowSafe` (WFS2)
cascades indefinitely.

### 8. Default enablement is Phase-7-evidence-gated

Same cadence as ADR 059 / 066: ADR 067 ships the
kernel implementation default-off; promotion to
default-on is gated on Phase 7 bench evidence
clearing the council's gates.

## Composability

**ADR 057 cascade catalog.** ADR 067 extends ADR 057
by one variant (`FsstRowSafe`) and one new on-disk
frame magic (`WFS2`). The existing `Fsst` cascade and
`WFS1` frame remain unchanged. ADR 057's selection
logic in `select.rs` is amended to opt into
`FsstRowSafe` by default for variable-width string
columns; operators can downgrade to `Fsst` via
configuration if needed.

**ADR 058 / 059 evaluator catalog.** ADR 067 extends
the catalog by one (`FsstRowSafe`). Existing seven
unchanged (after ADR 066). The default predicate
allow-list extends by `FsstRowSafe` if Phase 7
evidence clears the gates.

**ADR 060 / 064 sparse projection.** ADR 067 produces
the same mask-only `PhaseBOutcome` shape as ADR 066
V1. Sub-case A1 (FSST predicate column not in output)
migrates to Case A; Sub-case A2 (overlap) remains
Case B. No surface change.

**ADR 063 page stats.** `FsstRowSafe` is buffer-grain,
not page-grain. `emits_pages_in_v1()` is unchanged
for both `Fsst` and `FsstRowSafe`. Page-grain support
is a separate ADR coupled with ADR 057 page-emission
for FSST.

**ADR 065 dense predicate-zip.** ADR 067 reduces ADR
065's V1 engagement surface further: `FsstRowSafe`
buffers on Sub-case A1 migrate to Case A. `Fsst`
buffers (existing data) continue to use Strategy A
(ADR 066 V1) and migrate per ADR 066's rules. Sub-
case A2 stays on Case B for both variants.

**ADR 052 secondary-index point reads.** ADR 052's
confirm-step COCD path inherits `FsstRowSafe` kernel
coverage through ADR 058's existing dispatch. Point
reads on row-safe-FSST-keyed secondary indexes gain
the additional CPU win.

**ADR 066 V1 reader.** The Strategy A path remains the
dispatch target for `EncodingCascade::Fsst` cascade
buffers (existing data). No interaction with Strategy
B's path beyond shared evaluator-catalog and
dispatch infrastructure.

## Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- `EncodingCascade` extends by exactly one variant
  (`FsstRowSafe`); existing variants unchanged.
- `EncodingComputeEvaluatorKind` extends by exactly
  one variant (`FsstRowSafe`); existing seven
  unchanged (after ADR 066's `Fsst` addition).
- `evaluator_kind_for_cascade` maps `FsstRowSafe →
  EncodingComputeEvaluatorKind::FsstRowSafe`.
- `encoding_compute_predicate_data_type_supported_for_evaluator`
  admits `Utf8` / `LargeUtf8` / `Binary` /
  `LargeBinary` for `FsstRowSafe`.
- `ENCODING_DIRECTORY_FORMAT_VERSION` bumped by one.
- No new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` /
  `PredicateAtomOp` / `EncodedRowMask` /
  `RowGroupRejectCode` / `RowGroupFallbackReason` /
  `EncodingRejectCode` / `EncodingFallbackReason` /
  `L2FetchStrategy` / `NodeCapability` variant.
- No new IPC `custom_metadata` key.
- No new wire `MsgType` / `FRAME_FLAG_*` / digest
  domain / `ObjectStore` trait method.
- No new descriptor field / fingerprint surface
  change.
- No new public PBC opcode.
- No Cargo feature gating `FsstRowSafe`.
- No nightly rustc dependency.
- Strategy B kernel does not import the dense Arrow
  string materializer or the Strategy A FSST decoder
  (decode-on-predicate-path is forbidden by
  construction).
- Writer never emits a symbol pair spanning a row
  boundary (verified by frame inspection guard on
  representative fixtures).
- **Writer + reader pre-encoder produce byte-
  identical output** for the same row bytes under
  the same symbol table (canonical contract guard:
  property test compares
  `pre_encode_literal(R, T)` against
  `writer_encoded_row_bytes(R, T)` for randomly-
  chosen R and T; must always be byte-identical).
- **Per-buffer actual-size guard fires before
  `FsstRowSafe` selection**: writer rejects
  `FsstRowSafe` on a buffer-by-buffer basis if
  `wfs2_len > ceil(wfs1_len * 1.10)` or
  `wfs2_len > decoded_plaintext_len` (verified by
  selection guard on representative short-row and
  long-row fixtures).
- Pre-encoded literal cache is per-buffer-per-
  literal, not per-row.
- The boundary-crossing-symbol fixture is
  repurposed: it now verifies that the WFS2 writer
  correctly DOES NOT produce cross-row symbols, even
  on input where such pairs are frequent. The
  fixture stays load-bearing for writer correctness.
- The no-compressed-row-slice regression fixture
  (carry-forward from ADR 066) verifies that even
  with row-aligned framing, the reader does not
  slice the encoded buffer by Arrow decoded
  offsets. (In Strategy B, the reader uses the
  encoded-offsets table; the no-compressed-row-slice
  guard hardens against accidentally falling back to
  decoded-offset slicing.)
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte
  layouts, ADR 056 / 063 directory shapes, ADR 058 /
  059 evaluator catalogs *(by addition only)*, ADR
  060 / 064 sparse projection eligibility, ADR 061
  closed chunk-size set, ADR 062
  `RowGroupSizingProfile` catalog, ADR 065 manager
  three-case dispatch, ADR 066 V1 reader: all
  unchanged.
- Cross-replica byte-equal mask output across two
  writers with the same WFS2 fixture and same
  predicate.
- ADR 060 / 064 mask-only sparse projection byte-
  identical pre / post on shared fixtures (mask
  shape unchanged).
- ADR 065 Case B/C still reachable for `FsstRowSafe`
  predicates that decline the kernel (regression
  coverage).
- No version suffix or planning-phase identifier on
  new symbols. (`FsstRowSafeFrame`, `EncodedLiteral`,
  `parse_fsst_row_safe_frame`,
  `pre_encode_literal`, `evaluate_fsst_row_safe_equality`
  are semantic.)
- No project-name prefix.

## Bench fixture mandate

Bench artifact records (matching ADR 060 / 063 / 064
/ 065 / 066 cadence):

- representative warehouse string fixtures (carry-
  forward from ADR 066):
  - URL-shaped (high cardinality, low-entropy
    prefixes; high FSST compression).
  - log-line-shaped (medium cardinality, structured
    tokens).
  - free-form text (high cardinality, high entropy).
  - identifier-shaped (UUID, ULID — high
    cardinality, fixed width).
  - JSON-key-shaped (very low cardinality across
    keys, high cardinality across values).
  - **boundary-crossing-symbol fixture** (a frequent
    two-byte sequence spans adjacent Arrow row
    boundaries in input; tests that the writer
    correctly does NOT emit cross-row symbols).
  - **no-compressed-row-slice regression fixture**
    (carry-forward from ADR 066; verifies reader
    does not slice encoded buffer incorrectly).
  - **short-string adversarial** (≤ 16 bytes per
    row; tests writer compression loss on short
    rows; kernel-vs-dense CPU diagnostic).
  - **weak-compression adversarial** (high entropy;
    near-pass-through symbol table; tests
    Strategy B's per-row work doesn't degrade in
    pessimal compression cases).
- predicate atoms (kernel-engaged):
  - `Eq` clustered selectivity 0% / 1% / 5% / 25% /
    50% / 95%.
  - `Neq` (inverse coverage; verifies length-
    mismatch rows correctly emit survivors under
    op-specific inversion).
  - `InList` with 1 / 4 / 16 constants.
- predicate atoms (validity-only path; kernel not
  invoked but regression coverage):
  - `IsNull` / `IsNotNull`.
- decline fixtures (route to dense): `Lt`, `Gt`,
  `Lte`, `Gte`, future `LIKE`.
- column shape:
  - null-free fixtures.
  - nullable fixtures.
  - `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary`
    coverage.
  - empty-string fixtures.
- length-pre-filter Disjoint fixture (literal
  length matches no row length; expect zero
  byte-comparison work via length pre-filter;
  the `encoded_lengths` scan and `current_offset`
  advance still run for every row, but no encoded
  payload byte is read past pre-filter rejection).
- ADR 060 / 064 composability rows (Strategy B
  predicate engages kernel → mask-only
  `PhaseBOutcome` → sparse projection on
  remaining columns; expect Case A engagement).
- ADR 065 baseline rows (Strategy B predicate
  declines kernel → Case B/C zip; same fixture as
  Strategy B engagement to record byte savings via
  Case A migration).
- ADR 066 V1 (Strategy A) baseline rows on the
  same fixtures encoded under `Fsst` (existing
  data path); record kernel-vs-Strategy-B delta.
- ADR 052 confirm-step row (FsstRowSafe-keyed
  secondary-index point read; conditional on
  existing test harness ability per ADR 066's
  composability discussion).
- Corruption fixtures (malformed WFS2 frame, non-
  monotonic `block_offsets` pair entries (either
  field), `block_offsets[0] != (0, 0)`,
  `exception_count` not equal to the count of
  `255` sentinels in `encoded_lengths`, running
  offset overshoot, encoded-length pre-filter
  mismatch, decoded-length cross-check failure —
  expect fail-closed via
  `EncodingComputeRejectCode`).
- Cross-replica determinism on representative
  fixtures.
- **Compression-ratio rows**: encoded byte length
  ratio of `FsstRowSafe` vs `Fsst` on the same
  input, for each warehouse fixture. **Promotion
  gate: ≤10% compression loss.**

**Promotion gates (matching ADR 060 / 064 / 065 / 066
cadence)**:

1. **Predicate-evaluation wall-clock ≥ 2.0×.** At
   clustered 5% selectivity for `Eq` and `InList`,
   predicate evaluation wall-clock is at least 2×
   faster than dense materialize-then-PBC on
   representative warehouse fixtures encoded under
   `FsstRowSafe`. (Stricter than ADR 066's 1.5×
   gate because Strategy B's structural elimination
   of decode work should produce a larger headroom.
   Council can revisit if Phase 7 evidence
   suggests the gate is too aggressive.)
2. **End-to-end query wall-clock ≥ 1.5×** when an
   exact mask feeds fixed-width sparse projection
   through ADR 065 → ADR 060 / 064 Case A path.
   (Stricter than ADR 066's 1.2×.)
3. **Dense-fallback overhead ≤ 5%** on truly
   declined paths (range, future LIKE, ineligible
   atoms / types / layouts).
4. **Compression-ratio loss ≤ 10%** on
   representative warehouse fixtures vs. existing
   `Fsst` cascade. **New gate.** Workloads where
   the loss exceeds 10% are noted; operators can
   explicitly downgrade to `Fsst` cascade for
   those workloads.

Headline diagnostics (recorded, not gating):

- ≥ 5× CPU speedup on representative `Eq` fixtures
  (much higher target than ADR 066's 2× because
  Strategy B avoids decode entirely).
- Encoded-byte-range memcmp throughput vs. dense
  Arrow eq throughput — sanity check that memcmp
  on ~22-byte ranges achieves vectorization.
- Length-pre-filter elimination rate per fixture
  (decoded vs. encoded; both should fire before
  any byte comparison).
- Total query plaintext byte reduction across the
  dense baseline vs. Strategy B kernel path.
- WFS1 vs. WFS2 compression ratio per fixture.
- **ADR 065 Case B/C surface reduction** as
  `FsstRowSafe` Sub-case A1 predicates migrate to
  Case A — quantifies the engagement-surface
  shrinkage at the row-group boundary.
- **ADR 052 secondary-index confirm-step
  behavior** when the indexed predicate uses
  `FsstRowSafe` — point-read latency delta vs.
  `Fsst` cascade.
- Strategy A vs. Strategy B wall-clock delta on
  the same input encoded under both `Fsst` and
  `FsstRowSafe` — quantifies the Strategy B win
  over Strategy A.

Correctness gates:

- Mask byte-identical to dense oracle on every
  correctness fixture across `Eq` / `Neq` /
  `InList` × `Utf8` / `LargeUtf8` / `Binary` /
  `LargeBinary` × null-free / nullable.
- Boundary-crossing-symbol fixture: writer does
  NOT emit cross-row symbols; reader produces
  correct mask.
- No-compressed-row-slice regression fixture:
  reader does NOT slice encoded buffer by Arrow
  decoded offsets.
- Length pre-filter zero byte-comparison work on
  Disjoint-length fixtures.
- Corrupt WFS2 frames (bad magic, malformed
  `block_offsets`, mis-sized `encoded_lengths`,
  exception-count mismatch, running-offset
  overshoot) fail closed through
  `EncodingComputeRejectCode`; no partial masks.
- ADR 060 / 064 mask-only composition byte-
  identical on engagement fixtures.
- ADR 065 Case B/C reachable for `FsstRowSafe`
  predicates that decline the kernel (regression).
- Cross-replica determinism passes on
  representative fixtures.

## V1 deferred (with explicit V2 promotion paths)

ADR 067 does not ship:

- **64-bit block payload offsets.** V1 declines to
  dense for buffers exceeding 4 GiB encoded
  payload (since `block_offsets[*].payload_offset`
  is `u32`). **V2:** trivial pair-table width
  amendment if such buffers materialize.
- **Page-grain row-safe FSST.** Buffer-grain only.
  **V2:** dedicated ADR coupled with ADR 057
  page-emission.
- **Range / `Lt` / `Gt` on FSST.** Order-preserving
  FSST or speculative decode is a separate
  correctness story. **V2:** dedicated ADR.
- **`LIKE` / regex / substring on FSST.** Not in
  the current `PredicateAtomOp` catalog; deferred
  to a follow-up that adds those atoms first.
- **FSST aggregate folds.** `string_min` /
  `string_max` / `count_distinct` on encoded form.
  **V2:** dedicated ADR; may compose with ADR 040
  string-aggregate expansion.
- **Sparse projection eligibility for FSST.**
  Inherited variable-width deferral from ADR 060 /
  064.
- **Symbol-table caching across queries.**
  Per-buffer parse remains V1.
- **Operator-visible compression-loss tunable.**
  V1 always row-aligns. V2 if Phase 7 evidence
  reveals workloads where row-alignment is too
  expensive.
- **Migration path for existing `Fsst` buffers.**
  V1 leaves them on Strategy A indefinitely.
  Operators can re-encode via row-group rewrite
  if they want to gain Strategy B.
- **Removal of `EncodingCascade::Fsst` cascade
  field's `symbol_table_byte_len`.** Same out-of-
  scope follow-up as ADR 066's V1 deferred. ADR
  067 carries the same zero-population pattern
  forward for `FsstRowSafe` to keep both
  variants' shapes parallel for the eventual
  cleanup ADR.

## Rejected alternatives

- **Reuse `EncodingCascade::Fsst` with a `row_safe:
  bool` flag.** Violates ADR 057's closed-catalog
  discipline and adds runtime branches at every
  cascade dispatch site. New variant is the clean
  addition.
- **Insert per-row sentinel bytes in WFS1
  payload.** Cross-row sentinels add 1 byte per
  row of overhead AND don't help the reader find
  encoded byte ranges without parsing through
  them. The chosen `encoded_lengths` table costs
  ~1 byte per row (matching the sentinel
  approach's overhead) but supports O(1) random
  access via `block_offsets`, no payload parsing
  required.
- **Encode each row as an independent WFS1 frame.**
  Per-row symbol-table overhead is prohibitive
  for short rows (16-byte UUIDs would have
  bigger headers than payloads).
- **Split WFS2 metadata (block_offsets +
  encoded_lengths + exception_lengths) into a
  separate buffer alongside validity + decoded
  offsets + values.** Requires manager.rs to
  handle 4-buffer leaves (currently 2 or 3). In-
  frame keeps the buffer count at 3 (validity +
  decoded offsets + values) and the WFS2 frame
  self-contained. Phase 0 audit can revisit if
  frame parse cost proves prohibitive.
- **Use 64-bit block payload offsets in V1.** 32-
  bit `block_offsets[*].payload_offset` covers
  buffers ≤ 4 GiB encoded payload, which
  dominates V1 workloads. V2 reserves 64-bit for
  unusually large buffers; V1 declines those to
  dense.
- **Use a (N+1) × u32 absolute encoded-offsets
  table instead of u8 lengths + block index.**
  The Phase 7 smoke benches with the u32 layout
  failed the 10% compression-loss guard on URL
  (18.6%) and weak-compression (28.5%) fixtures
  because the 4 B/row overhead dominates short-
  row buffers. The u8-lengths + per-256-row pair
  table chosen here is ~1 B/row, drops both
  fixtures under the 10% guard, and preserves
  O(1) random access into any block. The u32-
  offsets alternative buys O(1) random access at
  the row level — unused by V1 callers (Strategy
  B is sequential, dense decode is sequential,
  ADR 064/065 operate on dense Arrow offsets).
- **Migrate existing `Fsst` buffers to
  `FsstRowSafe` automatically.** Implicit data
  migration violates ADR 057 row-group rewrite
  discipline. Operators re-encode explicitly if
  they want to migrate.
- **Remove the existing `Fsst` cascade variant
  after `FsstRowSafe` ships.** Existing on-disk
  data depends on the variant; removing it
  breaks readers. Closed-by-addition discipline
  applies recursively.
- **Promote-default `FsstRowSafe` evaluator
  before Phase 7 evidence.** Violates ADR 059's
  discipline. Same cadence as ADR 066.
- **Cache pre-encoded literals across queries.**
  Cross-query determinism + eviction story is
  its own ADR. Per-buffer-per-literal pre-
  encoding is V1.
- **Extend ADR 066 in-place to add
  `FsstRowSafe`.** ADR 066 is reader-only;
  ADR 067 is writer + reader. Different surface,
  different scope, different status field. ADR
  066 V1 ships; ADR 067 ships independently.
- **Bundle the cascade-field cleanup
  (`symbol_table_byte_len` removal) with ADR
  067.** Same out-of-scope discipline as ADR 066.
  Separate trivial follow-up ADR.

## What not to do

- **Don't add a new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` /
  `PredicateAtomOp` / `EncodedRowMask` variant.**
- **Don't change ADR 057 `Fsst` cascade byte
  layout** or existing `BufferEncoding` /
  `EncodingDirectory` shape. Add `FsstRowSafe`
  as a new variant alongside.
- **Don't skip the `ENCODING_DIRECTORY_FORMAT_VERSION`
  bump.** Adding a cascade variant requires it.
- **Don't add an FSST-specific descriptor field,
  IPC `custom_metadata` key, wire `MsgType`,
  `NodeCapability` bit, digest domain, or
  `ObjectStore` trait method.**
- **Don't introduce planning-phase identifiers,
  version suffixes, or project-name prefixes** on
  new symbols. (`FsstRowSafe` is structural, not
  a version suffix; the on-disk magic `WFS2` is
  a format-version marker which is acceptable
  for on-disk magic bytes per industry
  convention.)
- **Don't broaden the V1 atom set** without an ADR
  amendment.
- **Don't promote-default before Phase 7 evidence
  clears the council's gates.**
- **Don't bundle ADR 067 with page-grain
  FsstRowSafe, FSST aggregate folds, sparse
  predicate-column fetch, ALP evaluator, or RLE
  value-run.**
- **Don't introduce a process-wide pre-encoded-
  literal cache.**
- **Don't assume `FsstRowSafe` preserves byte-
  lexicographic order.** Same as `Fsst`; range
  predicates decline to dense.
- **Don't decode any encoded byte on the
  predicate path.** Strategy B's structural
  inversion of Strategy A is the entire point.
- **Don't slice the encoded values buffer by
  Arrow decoded offsets.** Use the WFS2
  encoded-offsets table.
- **Don't pre-encode literals per row.** Pre-
  encode once per buffer per literal.
- **Don't engage the kernel before validity +
  decoded offsets + values are all fetchable for
  the FSST leaf.** Same multi-buffer planner
  shape as ADR 066.
- **Don't migrate existing `Fsst` buffers
  implicitly to `FsstRowSafe`.** Re-encoding is
  an explicit operator action.

## Convergence summary

```text
ADR 057 per-buffer pages + cascades (Fsst cascade
  decode-only; multi-buffer values+offsets+validity
  layout for variable-width)
  → ADR 058 substrate COCD (Fsst not in evaluator
    catalog; single-buffer fixed-width planner shape)
  → ADR 059 native COCD kernels (Fsst not native)
  → ADR 060 sparse projection (Fsst not eligible;
    variable-width)
  → ADR 063 page-level predicate stats (Fsst not
    page-emitting)
  → ADR 064 nullable sparse projection (Fsst not
    eligible)
  → ADR 065 dense predicate-zip (Fsst predicates
    route Case B/C via dense fallback)
  → ADR 066 FSST predicate evaluator V1 (Strategy A
    streaming token decode; reader-only;
    structural ceiling reached at Phase B
    post-mortem)
  → ADR 067 row-safe FSST framing + Strategy B
    (this ADR; writer amendment + Strategy B
    reader; new EncodingCascade::FsstRowSafe
    variant + new WFS2 frame magic; encoded-
    offsets-in-frame; pre-encoded literal compare
    avoids decode entirely on predicate path;
    ADR 057 cascade catalog extends by one;
    ADR 066 V1 reader unchanged; ADR 060 / 064
    Case A composition unchanged; promotion
    gates stricter than ADR 066 V1 because
    Strategy B's structural elimination of
    decode produces larger headroom; ≤10%
    compression-ratio-loss gate; existing
    Fsst buffers stay readable via Strategy A
    indefinitely)
    ← here
  → next Toron-adjacent follow-ups (cascade-field
    removal directory cleanup for both Fsst and
    FsstRowSafe, page-grain FsstRowSafe, sparse
    predicate-column fetch, variable-width sparse
    projection, encoded-validity page decoding,
    all-null page-skip, range FSST, FSST
    aggregate folds, ALP evaluator, RLE value-run
    format)
  → capacity-tier compression / serverless / EC
    bundle
```

## Promotion criteria

ADR 067 promotes from council-converged → shipped
when:

1. Phase 0 audit pinned the existing FSST writer,
   the WFS1 codec, the cascade catalog extension
   site, the format version bump procedure, and
   confirmed row-aligned encoding is achievable
   without an ADR 057 amendment beyond the
   cascade-variant addition.
2. `EncodingCascade` extended by exactly one
   variant (`FsstRowSafe`); existing variants
   unchanged.
3. `EncodingComputeEvaluatorKind` extended by
   exactly one variant (`FsstRowSafe`, label
   `"fsst_row_safe"`); existing seven unchanged
   (after ADR 066 added `Fsst`).
   `evaluator_kind_for_cascade` and
   `encoding_compute_predicate_data_type_supported_for_evaluator`
   updated.
4. `ENCODING_DIRECTORY_FORMAT_VERSION` bumped by
   one.
5. Writer amendment lands: row-aligned encoding
   never emits a symbol pair spanning a row
   boundary (architectural guard via frame
   inspection on representative fixtures).
6. WFS2 frame parser correctness: round-trip
   through writer + parser produces byte-
   identical decoded output to the input.
7. Pre-encoded literal cache: per-buffer-per-
   literal; no per-row encoding work
   (architectural guard).
8. Strategy B kernel never reads encoded payload
   bytes outside of length-matching memcmp ranges
   (architectural guard).
9. Length pre-filter (decoded + encoded) verified
   to skip byte-comparison work on length-
   mismatch rows.
10. Mask output byte-identical to dense oracle on
    every correctness fixture across the closed
    atom × type × nullability × selectivity
    matrix.
11. Boundary-crossing-symbol fixture: writer
    correctly does NOT produce cross-row symbols.
12. No-compressed-row-slice regression fixture:
    reader does not slice encoded buffer by
    Arrow decoded offsets.
13. Cross-replica byte-equal mask output on
    representative fixtures.
14. Phase 7 bench evidence:
    - **Promotion gates:**
      - predicate-evaluation wall-clock ≥ 2.0×
        at clustered 5% for `Eq` / `InList` on
        representative warehouse fixtures
        encoded under `FsstRowSafe`.
      - end-to-end query wall-clock ≥ 1.5×
        through the ADR 065 → ADR 060 / 064
        Case A path.
      - dense-fallback overhead ≤ 5% on truly
        declined paths.
      - compression-ratio loss ≤ 10% vs.
        existing `Fsst` cascade.
      - cross-replica deterministic masks pass
        on representative WFS2 fixtures (Codex's
        iteration-2 addition; ADR 020 +
        canonical-encoder contract demand it).
      - writer overhead within ADR 057 writer-
        cost budget, or bench artifact
        explicitly justifies higher write cost
        with read-side wins.
      - ADR 060 / 064 mask-only composition
        byte-identical.
      - ADR 065 Case B/C → Case A migration
        recorded.
    - **Recorded diagnostics (not gating):**
      - ≥ 5× CPU speedup on representative `Eq`
        fixtures.
      - encoded-byte-range memcmp throughput
        vs. dense Arrow eq throughput.
      - per-fixture compression-ratio
        comparison `Fsst` vs `FsstRowSafe`.
15. Default-enablement decision: if Phase 7 clears
    the council's gates, `FsstRowSafe` joins ADR
    059's default predicate allow-list AND
    becomes the default cascade for variable-
    width string columns in ADR 057's selection
    logic; if not, kernel ships default-off with
    a separate Phase evidence row recording the
    gap.
16. Architectural lockdown guards land per
    §"Architectural lockdown".
17. ADR 057 §"Where we are" amended: `FsstRowSafe`
    in cascade catalog; new WFS2 frame; format
    version bumped; default cascade for variable-
    width string columns updated per Phase 7
    outcome.
18. ADR 058 §"Where we are" amended:
    `FsstRowSafe` in evaluator catalog; default-
    on per Phase 7 outcome.
19. ADR 059 §"Where we are" amended (if default
    allow-list extends to include
    `FsstRowSafe`).
20. ADR 063 §"Where we are" amended:
    `FsstRowSafe` is row-group grain in V1; not
    page-emitting.
21. ADR 065 §"Where we are" amended:
    `FsstRowSafe` Sub-case A1 predicates migrate
    to Case A; ADR 065 V1 engagement surface
    narrows further.
22. ADR 066 §"Where we are" amended: V1 Strategy
    A path remains for `Fsst` cascade buffers
    indefinitely; new writes default to
    `FsstRowSafe` with Strategy B (ADR 067).
23. ADR 020 / 022 / 056 / 057 / 058 / 059 / 060 /
    061 / 062 / 063 / 064 / 065 / 066 focused
    tests stay green at both `PointRead` and
    `Scan` profiles.

## Council decisions

Twelve load-bearing decisions reached by council
consensus across Claude / Codex / Gemini, organized
by topic. Originator credited where the contribution
shaped the final shape; "all aligned" means all three
drafts converged on the same answer.

**Format / structure:**

1. **Encoded-offsets-in-frame, not a separate
   buffer** (all aligned). Keeps the multi-buffer
   shape identical to ADR 066 (validity + decoded
   offsets + values); a 4th buffer would leak
   cascade-private structure into manager fetch
   planning.
2. **32-bit block payload offsets in V1; decline-
   to-dense for buffers >4 GiB encoded** (all
   aligned). `block_offsets[*].payload_offset` is
   `u32`. 64-bit reserved for V2 if such buffers
   materialize.
3. **New cascade variant `FsstRowSafe` (discriminant
   9, label `"fsst_row_safe"`) + new on-disk frame
   magic `WFS2`** (Codex; all aligned). Reusing
   `Fsst` with a flag would put format branching
   into every dispatch site and blur the proof
   boundary.

**Writer contract:**

4. **Row-canonical, not just row-safe** (Codex's
   insight; all aligned). Strategy B's correctness
   depends on writer + reader's literal pre-encoder
   producing byte-identical output for the same
   input under the same symbol table. "Row-safe"
   guarantees no cross-row symbols (necessary);
   "row-canonical" additionally guarantees
   determinism (sufficient).
5. **Per-buffer actual-size writer guard** (Codex;
   all aligned): `wfs2_len ≤
   ceil(wfs1_len * 1.10)` AND `wfs2_len ≤
   decoded_plaintext_len`. Per-buffer runtime
   check, not just a bench-fixture gate. Prevents
   silent compression regression on shapes the
   fixture set missed.
6. **Default cascade selection prefers
   `FsstRowSafe` when the guard clears**, falls
   back to `Fsst` otherwise (Codex's guard formula
   reconciles Gemini's "default-on" with Codex's
   "evidence-gated promotion": once Phase 7
   evidence clears the gates, the guard becomes
   the default selection logic).

**Reader contract:**

7. **Strategy B avoids decode on the predicate
   path** (all aligned). The encoded payload is
   untouched except for length-matching memcmp
   ranges. Architectural guard pins this.
8. **Fast corruption boundary** (Codex; all
   aligned). Three-tiered failure surface:
   structural metadata fails closed, compared
   encoded ranges fail closed if not valid
   canonical encodings, bytes outside compared
   ranges trust ADR 020 AEAD upstream. Full
   token-stream validation is the dense-decode
   path's job, not Strategy B's. If the council
   later requires per-query full validation,
   that's a separate ADR + benchmark.
9. **Dense WFS2 decode required and ships before
   Strategy B may engage** (Codex; all aligned).
   Dense decode is the correctness oracle.

**Scope:**

10. **Bundle writer amendment + Strategy B reader
    in one ADR** (all aligned). Per ADR 066's
    commitment, and because each half is useless
    without the other.
11. **No implicit migration of existing `Fsst`
    buffers** (all aligned). Re-encoding remains
    an explicit row-group rewrite operation; ADR
    066 V1 Strategy A reader stays the dispatch
    target for `Fsst` cascade buffers
    indefinitely.

**Promotion gates:**

12. **Stricter than ADR 066** (Claude + Gemini's
    position; Codex aligned at iteration 2): ≥2.0×
    predicate wall-clock + ≥1.5× end-to-end
    **as gates** (vs ADR 066's ≥1.5× / ≥1.2×).
    ≤5% dense-fallback overhead (carry-forward).
    ≤10% compression-ratio loss vs `Fsst` on
    representative warehouse fixtures (new gate;
    threshold may adjust based on Phase 0
    evidence). Cross-replica deterministic masks
    on representative WFS2 fixtures (Codex's
    iteration-2 addition). Writer-cost budget
    within ADR 057 budget. ≥5× CPU speedup is a
    recorded headline diagnostic, not a gate.

Council convergence is in the §"Council decisions"
list above; the companion implementation plan at
`plans/impl/067_row_canonical_fsst.md` carries the
operational shape. Phase 0 audit is the next step
under that plan; default-enablement promotion
follows Phase 7 bench evidence per ADR 059
cadence.
