# Implementation Plan: ADR 059 — Native Per-Cascade COCD Kernels

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/059_native_cocd_kernels.md`

Inherits substrate: `plans/adr/058_compute_over_encoded_data.md`
plus the substrate kernel-module anchors and dispatch
surface landed under `src/storage/encoding/compute/`.

Inherits encoding directory + cascade catalog:
`plans/adr/057_per_buffer_pages_and_encoding_cascades.md`.

Inherits ADR 058 Phase 8 substrate floor evidence:
`plans/impl/058_compute_over_encoded_data_bench_results.md`.

## Status

Council-converged, implemented, and promoted to shipped.
Full Phase 10 bench evidence at
`plans/impl/059_native_cocd_kernels_bench_results.md`
clears the promotion-to-shipped gate for the narrowed
default evaluator set. Phase 12 flips
`EncodingComputeConfig::default().enabled = true`, omits
`RlePlain` / `RleBitPacked` from the default predicate
allow-list, and relies on pre-kernel eligibility for
non-byte-aligned BitPacked, higher-cardinality Dictionary,
and cascade-grain aggregate exclusions.

Implementation contract locked at the council level:
4 native predicate kernel families (BitPacked, Rle,
Dictionary, Delta), 8 native aggregate kernels per
cascade (filtered by ADR 058's existing logical-type
restrictions), no new public PBC opcodes, no descriptor
field, no `EncodingComputeConfig` field additions
(`enabled` default may flip; existing
`enabled_predicate_evaluators` allow-list may narrow for
predicate-kind misses; cascade-grain aggregate misses
flow through the eligibility layer because
`enabled_aggregate_kernels` is keyed by aggregate kind,
not cascade), Exact / Prefilter / DenseFallback
predicate plan modes preserved per ADR 058, byte-
identical mask + aggregate-partial invariant pinned by
property suite, `EncodedRowMask` enum unchanged with
specialized algebra, central validity composition,
two-step dispatch discipline (pre-kernel eligibility
falls back to oracle; in-kernel reject is fail-closed),
auto-vectorization on stable rustc as V1 SIMD strategy
with helper-crate / `std::arch` escalation reserved
for Phase 4+ bench misses.

## Goal

Replace ADR 058's decode-then-scalar COCD hot path with
native per-cascade kernels living in
`src/storage/encoding/compute/kernels/`. Per-cascade
evaluators, per-aggregate kernels, specialized mask
algebra, and central validity composition all live
inside the existing module anchors (which today are
5–14 line label-shims). The substrate decode-then-scan
path stays present in `dispatch.rs` as the production
fallback for legal-but-not-yet-native sublanes, the
test oracle the property suite calls, and the kill-
switch target for operator-disabled evaluators.

The implementation:

- per-cascade native kernel modules grow from label-
  shims to load-bearing modules;
- `kernels/validity.rs` becomes the central 3VL helper
  every native kernel calls;
- `dispatch.rs` routes through native kernels for
  eligible sublanes via a two-step dispatch (eligibility
  check pre-parse; fail-closed reject post-parse);
- new property-suite home at
  `tests/encoding_compute_kernel_invariants.rs`;
- bench harness extension at `benches/encoding_compute.rs`
  to add the 1M-row tier and per-cascade native vs
  substrate-floor vs end-to-end-dense-PBC three-way
  comparison;
- kernel-promotion bench artifact at
  `plans/impl/059_native_cocd_kernels_bench_results.md`;
- `EncodingComputeConfig::default()` flip +
  `enabled_predicate_evaluators` allow-list narrow for
  any predicate-kind miss (Phase 12 only, conditional
  on bench; cascade-grain aggregate misses enforced by
  eligibility layer because `enabled_aggregate_kernels`
  is keyed by aggregate kind, not cascade);
- ADR 058 §"Where we are" amendment in the promotion
  commit (closes the kernel-promotion follow-up
  reservation);
- architectural lockdown.

No public PBC opcodes, no descriptor field, no new
evaluator kinds, no atom-set extension, no aggregate-
kind extension, no directory shape extension, no Cargo
feature, no nightly rustc.

## Deliverables

- `src/storage/encoding/compute/kernels/bitpacked.rs` —
  native predicate + aggregate (byte-aligned fast path
  + non-aligned packed-bit reader).
- `src/storage/encoding/compute/kernels/rle.rs` —
  native byte-run streaming predicate + aggregate.
- `src/storage/encoding/compute/kernels/dictionary.rs`
  — native match-set + index-walk predicate +
  aggregate.
- `src/storage/encoding/compute/kernels/delta.rs` —
  native prefix-sum predicate + aggregate.
- `src/storage/encoding/compute/kernels/validity.rs` —
  load-bearing 3VL helper (grow from 14-line stub).
- `src/storage/encoding/compute/mask.rs` — specialized
  `EncodedRowMask` algebra (no enum changes).
- `src/storage/encoding/compute/aggregate.rs` —
  native fold dispatch preserving all-or-nothing
  policy.
- `src/storage/encoding/compute/dispatch.rs` — route
  to native kernels via eligibility layer; retain
  decode-scalar oracle as pre-kernel production
  fallback.
- `src/storage/encoding/cascade/codec.rs` — expose
  `pub(crate)` parser helpers for ADR 057 frames
  (WDD1 / WDB2 / WDL1 / byte-run RLE) without
  changing encoded bytes.
- `tests/encoding_compute_kernel_invariants.rs` — new
  property suite (native-vs-oracle matrix).
- `benches/encoding_compute.rs` — extended fixture
  matrix (8K / 65K / 1M tiers; native vs substrate vs
  dense three-way).
- `plans/impl/059_native_cocd_kernels_bench_results.md`
  — checked-in evidence artifact (lands phase-by-phase
  as cascades clear gates).
- `tests/architecture_test.rs` — incremental Phase 11
  guards.
- `Cargo.toml` — scoped SIMD helper crate dep
  (conditional on Phase 4 escalation only).
- `src/config.rs` — `EncodingComputeConfig::default()`
  flip + `enabled_predicate_evaluators` allow-list
  narrow (Phase 12 only).
- `plans/adr/058_compute_over_encoded_data.md` —
  §"Where we are" amendment (Phase 12 only).
- `plans/wip/roadmap.md` — planning-cadence update at
  promotion (Phase 12 only).

## Non-goals

- No new public PBC opcodes;
  `src/schema/pbc.rs::Op` byte-identical post-implementation.
- No `RecordBodyCompressionPolicy::EncodedComputeKernel`
  descriptor variant.
- No new `EncodingComputeEvaluatorKind` /
  `EncodedAggregateKind` /
  `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp` /
  `EncodedRowMask` variants.
- No `EncodingComputeConfig` field additions (the
  `enabled` default may flip in Phase 12; the existing
  `enabled_predicate_evaluators` allow-list may
  narrow).
- No per-cascade `enabled_*` flag.
- No Cargo feature gating native kernels.
- No nightly rustc adoption.
- No `Fsst` / `Alp` cascade encoded evaluation.
- No nested / list / struct Arrow type predicate eval.
- No GROUP BY / approx aggregates / count_distinct /
  float / decimal / time-sum-avg / Utf8 min/max
  expansion.
- No ADR 057 directory extension.
- No ADR 057 RLE format change inside ADR 059.
- No variable-width Dictionary predicate expansion.
- No new wire `MsgType` / `FRAME_FLAG_*` /
  `NodeCapability` bit / envelope `format_version` /
  `ObjectStore` trait change.
- No new metric label.
- No SIMD helper crate dependency at Phase 0 (added
  conditionally at Phase 4 boundary if needed).
- No silent dense fallback after kernel reject.
- No retirement of the decode-scalar oracle from
  production code.

## Phase 0 — Contract freeze + ADR 058 status cleanup + SIMD lean

Lock the closed surface, clean stale ADR 058 status
text, and record the SIMD strategy lean before per-
cascade work begins.

Files:

- `plans/adr/058_compute_over_encoded_data.md`,
  `plans/impl/058_compute_over_encoded_data.md`
  (status cleanup only — separate commit from native-
  kernel code).

Work:

1. Confirm ADR 059 scope: native implementation of the
   ADR 058 catalog, no catalog expansion.
2. Resolve any stale ADR 058 / impl-plan status text
   that still says "implementation phases pending"
   after the substrate promotion in commit `ca74604`.
3. File-digest snapshot of the pre-059 public surface:
   - `EncodingComputeEvaluatorKind` variants 1..=6;
   - `EncodedAggregateKind` variants 1..=8;
   - `EncodingComputeRejectCode` variants 1..=5;
   - `EncodingComputeFallbackReason` labels;
   - `EncodingComputeConfig` fields;
   - `src/schema/pbc.rs::Op`;
   - `src/storage/encoding/types.rs` cascade catalog;
   - ADR 057 `EncodingDirectory` / `BufferEncoding`
     shape.
4. File-digest snapshot of the four kernel anchors and
   `validity.rs` (test fixtures for Phase 11
   architectural guards).
5. Snapshot `dispatch.rs` shape (333 lines today;
   routes all six kinds through `decode_buffer +
   scalar loop`). Phase 11 guard pins what shrinks vs
   what stays.
6. Lock the SIMD lean:
   - stable Rust only;
   - no nightly `std::simd`;
   - no Cargo feature gate;
   - V1: tight loops, block-unrolling, stack scratch,
     LLVM auto-vectorization;
   - escalation route on miss: small SIMD helper crate
     first (e.g., `wide`); target-feature `std::arch`
     second;
   - escalation trigger: per-cascade Phase 3/4/5/6
     bench row evidence, not Phase 0 planning.

**Exit gate:** ADR 058 status cleanup committed
separately; pre-059 file-digest snapshot pinned;
per-cascade work unblocked.

## Phase 1 — Parser + oracle infrastructure

Land parsing + oracle scaffolding before any kernel
specialization, so per-cascade work can call shared
parsers without each cascade reinventing them.

Files:

- `src/storage/encoding/cascade/codec.rs` (audit;
  expose `pub(crate)` parser helpers if duplication
  stays low).
- `src/storage/encoding/compute/dispatch.rs` (split
  `evaluate(...)`).
- `src/storage/encoding/compute/kernels/mod.rs`,
  `bitpacked.rs`, `rle.rs`, `dictionary.rs`,
  `delta.rs` (parser-helper imports added; no kernel
  logic yet).
- `tests/encoding_compute_test.rs` (parser parity).
- `tests/encoding_compute_kernel_invariants.rs` (new;
  property-suite home).

Work:

1. Split `dispatch::evaluate(...)` into:
   - `evaluate(...)` — production dispatch entry;
   - `evaluate_decode_scalar(...)` — explicit oracle
     the property suite + pre-kernel fallback both
     call.
2. Add `pub(crate)` parser helpers (or compute-private
   parsers + parity tests against `decode_buffer`):
   - bit-packed lane reader;
   - WDD1 / WDB2 fixed-width Dictionary frame parser;
   - WDL1 Delta frame parser;
   - byte-run RLE frame iterator.
3. Add internal native-eligibility enum inside
   dispatch (cascade × dtype × atom × literal-coercion
   shape → eligible / pre-kernel-fallback). Internal-
   only; architectural test pins it doesn't escape
   `src/storage/encoding/compute/`.
4. Establish
   `tests/encoding_compute_kernel_invariants.rs` as
   the native-vs-oracle property matrix home; existing
   `tests/encoding_compute_test.rs` keeps unit-
   contract focus.

Tests:

- BitPacked parser reconstructs same values as
  `decode_buffer` for bits 1, 2, 4, 7, 8, 12, 16, 31,
  32, 63, 64.
- Dictionary parser validates frame length, dictionary
  length, index width, bit-packed index width.
- Delta parser validates magic, value width, delta
  width, count, cursor exhaustion.
- RLE byte-run parser validates run length overflow
  and decoded byte length.

**Exit gate:** Parser helpers + oracle helper land. No
production dispatch behavior changes yet.

## Phase 2 — Validity composition helper

Grow `kernels/validity.rs` from a 14-line stub into the
central 3VL helper used by every native predicate +
aggregate kernel. Ships before per-cascade kernels so
they call the helper instead of re-implementing 3VL.

Files:

- `src/storage/encoding/compute/kernels/validity.rs`
- `src/storage/encoding/compute/mask.rs`
- `tests/encoding_compute_kernel_invariants.rs`

Work:

1. Helpers:
   - Arrow validity bit lookup;
   - comparison-bitmap AND validity-bitmap (auto-vec
     friendly);
   - `All` / `None` short-circuits when null-count
     metadata proves the whole row group valid /
     invalid;
   - run-aware validity composition for `Runs` masks
     when validity is run-homogeneous (no forced
     densification);
   - scalar fallback for irregular validity spans.
2. `IsNull` / `IsNotNull` atoms continue to route
   through this helper; validity-only kernels stay
   outside value kernels.
3. Property tests comparing validity composition
   against the existing scalar path for all mask
   shapes.

Tests:

- `All`, `None`, `Bitmap`, `Runs` masks compose with
  validity exactly like decode oracle.
- Tail bits masked correctly.
- All-null and no-null row-group metadata short-
  circuits stay byte-equivalent.

**Exit gate:** Every later native kernel phase calls
this helper instead of implementing local null logic.

## Phase 3 — `kernels/bitpacked.rs` native predicate

BitPacked is the simplest native target: fixed-width
encoded values, page-grain dispatch, comparison
kernels are auto-vec friendly.

Files:

- `src/storage/encoding/compute/kernels/bitpacked.rs`
- `src/storage/encoding/compute/dispatch.rs`
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`
- `benches/encoding_compute.rs`

Work:

1. Packed-domain literal routing: coerce literal to
   unsigned/signed comparable lane; subtract `base`
   once before lane walk; short-circuit out-of-range
   `Eq` (`None`) / `Neq` (`All`).
2. Byte-aligned fast path: `bits ∈ {8, 16, 32, 64}` →
   chunked little-endian lane loads (e.g.,
   `u32::from_le_bytes(chunk_4)` over fixed-size byte
   chunks); unrolled comparison loop, auto-vec
   friendly. No raw `&[u8]` → `&[uN]` slice cast — the
   codec writes little-endian packed bytes without
   alignment guarantees, so direct casts are alignment-
   unsafe (strict-alignment targets) and endian-unsafe
   (big-endian hosts).
3. Non-aligned path: bits ∈ {1..63} \ {8, 16, 32} →
   packed-bit reader unpacks 32 or 64 values into
   stack array per chunk; comparison happens against
   stack chunk; result bits flow into output `Bitmap`.
4. Emit `All` / `None` / `Bitmap` per result.
5. Validity composition outside the value kernel via
   Phase 2 helper.
6. Page-grain dispatch behavior preserved per ADR 058.

Tests:

- Every atom (`Eq`, `Neq`, `Gt`, `Gte`, `Lt`, `Lte`,
  `InList`, `IsNull`, `IsNotNull`) matches
  `evaluate_decode_scalar` byte-for-byte.
- Base offsets correct for signed and unsigned logical
  inputs.
- Out-of-range literals produce `All` / `None` where
  expected.
- Malformed packed length rejects with
  `EncodingComputeRejectCode` (no fallback after
  kernel entry).
- Validity-mask combination remains byte-identical to
  oracle.

Bench rows:

- `predicate_native_bitpacked_u8`,
  `predicate_native_bitpacked_u32`;
- `predicate_substrate_bitpacked` (substrate-floor
  regression tripwire);
- `predicate_dense_pbc_bitpacked` (gate denominator,
  end-to-end Phase B+C+D);
- selectivity sweep at 1% / 5% / 25% / 50%;
- byte-aligned vs non-byte-aligned bit-width rows.

**Exit gate:** BitPacked predicate kernel passes
property suite; bench shows ≥ 2× speedup vs end-to-end
dense PBC at low selectivity (≤ 5%) on representative
fixtures, OR documented miss with target-feature SIMD
escalation plan.

## Phase 4 — `kernels/dictionary.rs` native predicate

Match-set + index walk pattern. Most likely candidate
for SIMD-helper-crate escalation given indirect access
patterns in bit-packed index walks.

Files:

- `src/storage/encoding/compute/kernels/dictionary.rs`
- `src/storage/encoding/compute/dispatch.rs`
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`
- `benches/encoding_compute.rs`
- `Cargo.toml` (conditional dep add only on Phase 4
  escalation trigger).

Work:

1. Parse WDD1 (plain) and WDB2 (bit-packed) Dictionary
   frames via Phase 1 helpers.
2. Evaluate atom against inline dictionary value table
   once; build match set bounded by
   `max_dict_match_set_size = 4_096`.
3. Index walkers:
   - plain 1-byte / 2-byte / 4-byte indices;
   - bit-packed indices for WDB2.
4. Match-set strategies:
   - `|S| = 0` → `None`;
   - `|S| = 1` → direct equality SIMD walk;
   - `1 < |S| ≤ 4` → unrolled OR-composed equality;
   - `4 < |S| ≤ 4_096` → bounded membership bitmap
     by dictionary index (≤ 512 bytes; fits L1 twice
     over);
   - `|S| > 4_096` → pre-kernel fallback to oracle via
     existing `EncodingComputeFallbackReason` (no
     reject; match-set size is known from the
     dictionary table before the index walk, so this
     is an eligibility decision, not a post-parse
     invariant violation).
5. `Dictionary { inner: Lz4Frame }` stays
   DenseFallback per ADR 058.
6. **Phase 4 SIMD escalation gate.** If initial bench
   shows Dictionary-bitpacked-index-walk under-
   performing 2× under auto-vec, adopt small SIMD
   helper crate (first choice: `wide`) for Dictionary
   kernels and re-bench. Architectural test pins the
   dependency scope. Other kernels stay auto-vec
   unless they also miss.

Tests:

- Both inner variants match oracle for all supported
  atoms.
- Match-set cap routes before kernel entry through
  existing fallback reason; do not add a reject code.
- Malformed dictionary lengths reject after kernel
  entry.
- Lz4 inner remains DenseFallback (architectural test
  pins).

Bench rows:

- `predicate_native_dictionary_plain`;
- `predicate_native_dictionary_bitpacked`;
- match-set sweep: 1, 4, 16, 256, 1024, 4096.

**Exit gate:** Dictionary kernel passes property
suite; bench shows ≥ 2× speedup vs end-to-end dense
PBC at low selectivity on low-cardinality fixtures,
with SIMD helper crate adopted iff escalation
triggered.

## Phase 5 — `kernels/delta.rs` native predicate

Full-buffer prefix-sum walk. Settles ADR Open Question
2 (Delta default-set inclusion) on Phase 10 bench
evidence.

Files:

- `src/storage/encoding/compute/kernels/delta.rs`
- `src/storage/encoding/compute/dispatch.rs`
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`
- `benches/encoding_compute.rs`

Work:

1. Parse WDL1 frame header via Phase 1 helper.
2. Streamed prefix-sum reconstruction:
   - first value from frame;
   - signed deltas of width 1, 2, 4, 8, 16;
   - checked addition for overflow detection.
3. Per reconstructed value, evaluate atom against
   coerced literal; emit bitmap bit.
4. Monotonic-shape short-circuits: detect at delta-
   stream inspection time (all-non-negative deltas =
   monotonically non-decreasing); for range atoms on
   monotonic streams, prove whole-buffer reject /
   accept where the literal bound permits.
5. Non-monotonic: full walk + per-row test, equivalent
   in cost to substrate path; speedup comes from
   avoiding the intermediate decoded buffer.
6. Full-buffer grain preserved per ADR 058. All inner
   variants route identically.

Tests:

- Equality, range, `InList` atoms match oracle.
- Positive, zero, negative delta streams match.
- Overflow returns `EncodingComputeRejectCode`.
- All `Delta { inner: * }` variants byte-identical at
  encoding level and route identically.

Bench rows:

- `predicate_native_delta_prefix`;
- monotonic HLC-shaped range sweep (2× gate target);
- non-monotonic random-walk control row (substrate-
  floor regression check).

**Exit gate:** Delta kernel passes property suite.
Bench evidence settles ADR Open Question 2. If gate
misses substantially, the default `enabled_predicate_evaluators`
allow-list omits `Delta` (predicate-grain miss); cascade-
grain aggregate-only Delta misses route through the
eligibility layer instead (because
`enabled_aggregate_kernels` is keyed by aggregate kind,
not cascade);
promotion still flips `enabled` for the other three
cascades.

## Phase 6 — `kernels/rle.rs` native predicate (byte-run)

Settles ADR Open Question 1 (RLE byte-run viability)
on Phase 10 bench evidence. Existing ADR 057 RLE codec
is byte-run `(byte_run_len: u32, byte: u8)`; this
phase implements byte-run streaming, NOT a value-run
format change.

Files:

- `src/storage/encoding/compute/kernels/rle.rs`
- `src/storage/encoding/compute/dispatch.rs`
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`
- `benches/encoding_compute.rs`

Work:

1. Implement byte-run streaming over existing
   `(byte_run_len: u32, byte: u8)` RLE entries.
2. Reconstruct fixed-width logical values into a small
   stack buffer (no allocated decoded column buffer).
3. Detect row-aligned repeated values; emit `Runs`
   where byte-run stream proves row-run structure.
4. Emit `Bitmap` when byte runs do not align cleanly
   to row runs.
5. `Rle { inner: Lz4Frame }` stays unsupported.
6. Bench notes call out the byte-run format
   explicitly so council does not over-read RLE
   results as value-run evidence.

Tests:

- RLE byte-run streaming matches oracle for fixed-
  width values.
- Repeated-value fixtures emit semantically correct
  mask.
- Non-row-aligned byte runs fall back to `Bitmap`,
  not incorrect `Runs`.
- Malformed byte-run lengths reject after kernel
  entry.

Bench rows:

- `predicate_native_rle_plain`;
- `predicate_native_rle_bitpacked`;
- run-shaped fixture;
- cycling low-card fixture (exposed ADR 057 selector
  variance);
- row-aligned vs non-row-aligned byte-run cases.

**Exit gate:** RLE kernel passes property suite.
Bench evidence settles ADR Open Question 1. Phase 10
evidence missed substantially, so the default
`enabled_predicate_evaluators` allow-list omits both
`RlePlain` and `RleBitPacked`; a separate ADR considers
ADR 057 value-run format follow-up.

## Phase 7 — Mask algebra specialization

ADR 058 Phase 8 bench shows mask algebra costs are
measurable, not noise. Specialize `EncodedRowMask`
algebra without changing the enum.

Files:

- `src/storage/encoding/compute/mask.rs`
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`
- `benches/encoding_compute.rs`

Work:

1. Preserve `EncodedRowMask` enum exactly.
2. Specialized algebra:
   - `All` / `None` short-circuits without allocation;
   - `Bitmap AND/OR Bitmap` bytewise (auto-vec);
   - `Bitmap NOT` bytewise with tail-bit masking;
   - `Runs NOT` by per-run flag flip;
   - `Runs AND/OR Runs` by run merge;
   - `Runs ↔ Bitmap` densification only when needed,
     bounded by `max_mask_bytes_per_record`.
3. Keep `to_bool_vec()` for tests + slow paths, not
   as default algebra implementation.

Tests:

- Every algebra combination matches old bool-vector
  oracle byte-for-byte.
- Tail bits masked correctly.
- `Runs` validation still catches overlap and overflow.

Bench rows:

- mask-algebra rows before/after specialization;
- `Runs` densification threshold rows.

**Exit gate:** Mask algebra no longer dominates
native predicate cost; specialized algebra ≥ 2×
faster than `to_bool_vec()`-based path on the relevant
fixtures.

## Phase 8 — Native aggregate fold kernels (full coverage)

Native fold for **every (cascade × kind) pair ADR 058
already admits**. ADR 058's existing logical-type
restrictions filter the matrix:

- `count_star` / `count_non_null` apply to every
  cascade.
- `min` / `max` apply to fixed-width integer / time /
  Boolean and fixed-width Dictionary cascades.
- `signed_sum` / `unsigned_sum` / `signed_avg` /
  `unsigned_avg` apply to Int8/16/32/64 and
  UInt8/16/32/64.

Pairs ADR 058 *already* defers (Utf8 / Binary
`min`/`max`, `APPROX_DISTINCT`, `QUANTILE_KLL`, exact
`count_distinct`, float / decimal aggregates,
time-sum/avg, GROUP BY) inherit unchanged — they fall
back to dense via the existing all-or-nothing mixed-
request policy. ADR 059 adds zero new deferrals.

Files:

- `src/storage/encoding/compute/aggregate.rs` (native
  fold dispatch).
- `src/storage/encoding/compute/kernels/{bitpacked, rle,
  dictionary, delta}.rs` (extend with aggregate
  sections).
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`
- `benches/encoding_compute.rs`

Work:

1. Streaming value adapters reused across predicate +
   aggregate kernels.
2. Native fold targets per cascade:
   - **BitPacked:** auto-vec horizontal reduction
     (`min` / `max` / `sum` / `avg`); validity-aware
     popcount (`count_*`); sum widened per ADR 058
     overflow taxonomy.
   - **Rle (byte-run):** O(byte-runs) walk;
     reconstruct value once per byte run; weight by
     row-aligned contribution.
   - **Dictionary:** index histogram (one pass) →
     aggregate over `(dict_value × index_count)`
     pairs; `min` / `max` short-circuit by scanning
     dictionary entries directly when mask = `All`.
   - **Delta:** `count_star` from row count; `sum`
     accumulates each reconstructed value during the
     single-pass prefix-sum walk (the codec stores
     adjacent deltas, so `base × count + Σ deltas` is
     wrong; the natural implementation is running-
     value + running-sum in one pass — no closed
     form);
     `min` / `max` monotonic-aware (first / last
     value short-circuit on non-negative streams);
     full walk otherwise.
3. ADR 058 all-or-nothing mixed-request policy
   preserved.
4. ADR 040 partial-state byte-equality preserved.
5. Mask-aware: every aggregate respects input
   `EncodedRowMask`; `All` short-circuits; `None`
   returns identity; `Bitmap` / `Runs` does per-grain
   filtering.

Tests:

- Aggregate-partial bytes match dense AGG for every
  (cascade × kind) pair ADR 058 admits, on every
  native fixture.
- Tight signed and unsigned overflow rows match
  dense overflow flags.
- Mixed supported / unsupported requests fall back as
  a whole (ADR 058 all-or-nothing).
- Validity masks + predicate masks compose correctly.

Bench rows (per cascade):

- `aggregate_native_count_star`;
- `aggregate_native_count_non_null`;
- `aggregate_native_min`, `aggregate_native_max`;
- `aggregate_native_signed_sum`,
  `aggregate_native_unsigned_sum`;
- `aggregate_native_signed_avg`,
  `aggregate_native_unsigned_avg`;
- selectivity sweep at 1% / 5% / 25% / 50%;
- 65K + 1M tiers (8K aggregate rows are smoke /
  noise documentation only).

**Exit gate:** Native fold lands for every (cascade ×
kind) pair ADR 058 admits; property suite passes;
representative 2× evidence on `BitPacked` / `Rle` /
`Dictionary` aggregate fixtures, OR documented miss
with cascade-grain default-set exclusion via the
eligibility layer (because `enabled_aggregate_kernels`
is keyed by aggregate kind across all cascades, not
per-cascade — Phase 9 §6 details the eligibility-layer
gate).

## Phase 9 — Dispatch + config + metrics integration

Files:

- `src/storage/encoding/compute/dispatch.rs`
- `src/storage/manager.rs`
- `src/metrics/registry.rs`
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`
- `tests/metrics_smoke.rs`

Work:

1. Production dispatch routes through native kernels
   where eligible; legal-but-not-yet-native sublanes
   continue to route to `evaluate_decode_scalar`
   oracle pre-kernel.
2. Existing operator kill-switch sets remain
   authoritative:
   - disabled evaluator → decode/dense fallback
     before native kernel entry;
   - disabled aggregate kind → dense AGG fallback
     before native partial emission.
3. Kernel timings recorded under existing
   `storage_l2_encoding_compute_kernel_micros` labels.
   **No new metric labels.**
4. Native invariant failures call existing invariant
   metric path and return storage errors.
5. Decode-scalar oracle stays test-visible AND
   production-fallback-visible. The production path
   may stop using it for native-eligible cascades
   after property suite is green, but the oracle
   remains in the tree.

Tests:

- Kernel-disabled config avoids native parser entry.
- Corrupt native frame rejects + increments invariant
  metric.
- Metrics render with same bounded label set.

**Exit gate:** Runtime integration complete; no new
observability cardinality.

## Phase 10 — Kernel-promotion bench artifact

Files:

- `benches/encoding_compute.rs`
- `plans/impl/059_native_cocd_kernels_bench_results.md`
  (new, empirical, lands as cascades clear gates).

Bench command:

```sh
cargo bench --bench encoding_compute
cargo bench --bench encoding_compute -- --quick
```

Bench matrix:

- **Row counts:** 8K / 65K / 1M (1M mandatory).
- **Selectivity:** 1% / 5% / 25% / 50%.
- **Predicate fixtures:**
  - BitPacked UInt8 / UInt32;
  - RlePlain / RleBitPacked;
  - DictionaryPlain low-card;
  - DictionaryBitPacked low-card / higher-card;
  - Delta monotonic Int64;
  - Delta non-monotonic Int64 (control);
  - Plain / Lz4 dense fallback controls.
- **Aggregate fixtures:** every (cascade × kind) pair
  ADR 058 admits, 65K + 1M tiers (8K = smoke/noise).
- **End-to-end rows:**
  - Phase B only;
  - Phase B + C + D (gate denominator);
  - AGG after CSB direct-answer miss;
  - COCD-disabled regression row;
  - per-kernel-disabled fallback row.
- **Three-way per-cell measurement:** native (ADR
  059) vs substrate-floor (ADR 058 disabled, dense-
  micro shape) vs end-to-end ADR 057 dense PBC (gate
  denominator).

Acceptance gates (per ADR 058 §"Reader-CPU bench
gate"):

- Predicate-only Exact COCD ≥ 2× faster than end-to-
  end ADR 057 dense PBC (Phase B + C + D) on
  representative low-selectivity `BitPacked`, `Rle`,
  `Dictionary` fixtures.
- Delta ≥ 2× if included in default evaluator set.
- Native aggregate folds ≥ 2× faster than dense AGG
  on representative `BitPacked`, `Rle`, `Dictionary`.
- Dense fallback overhead ≤ 5%.
- Mask byte-equality across every predicate fixture.
- Aggregate-partial byte-equality across every
  aggregate fixture.
- Cross-replica determinism byte-identical.
- ADR 056 / 057 / 040 / 052 / 055 regression
  neutrality.
- No native row slower than ADR 058 Phase 8 substrate
  floor (dense-micro shape) on any cascade without a
  documented council disposition.

Bench artifact follows ADR 057 / 058 bench-results
shape: command, fixture matrix table, predicate CPU
table, aggregate CPU table, mask-algebra
completeness, mixed-leaf classifier, dense-fallback
overhead, substrate-floor tripwire status, council
disposition.

**Exit gate:** Bench artifact records pass/fail for
every gate. Council decides default enablement for
each cascade in the default set. ADR Open Questions
1 and 2 settle here — empirical, not prescriptive.

## Phase 11 — Architectural lockdown

Files:

- `tests/architecture_test.rs`

Guards:

1. File-digest pins on the four native kernel modules
   + `validity.rs` (post-Phase-2/3/4/5/6/8 hashes).
2. `dispatch.rs` shape: production path routes to
   native kernels for eligible sublanes; decode-scalar
   oracle remains test-visible AND production-
   fallback-visible.
3. No new variants in
   `EncodingComputeEvaluatorKind` (1..=6),
   `EncodedAggregateKind` (1..=8),
   `EncodingComputeRejectCode` (1..=5),
   `EncodingComputeFallbackReason`,
   `PredicateAtomOp`,
   `EncodedRowMask`.
4. No new fields in `EncodingComputeConfig`
   (`enabled` default may flip in Phase 12; existing
   `enabled_predicate_evaluators` allow-list may
   narrow).
5. No `simd_*` / `kernel_*` / `dispatch_*` config
   knob.
6. No public PBC opcode addition.
7. No new `NodeCapability` bit.
8. No new wire `MsgType` / `FRAME_FLAG_*`.
9. No ADR 057 `EncodingDirectory` / `BufferEncoding`
   shape change.
10. Native kernel modules contain non-placeholder
    evaluator entry points for BitPacked, Rle,
    Dictionary, Delta.
11. Dispatch has exactly one public production
    value-evaluator entry.
12. Decode-scalar oracle remains test-visible AND
    production-fallback-visible.
13. Metrics retain ADR 058 bounded label set.
14. `plans/impl/059_native_cocd_kernels_bench_results.md`
    exists before default enablement (Phase 12
    guard).
15. No Cargo feature gates native kernels.
16. Build remains on stable Rust.
17. Project-name prefix rule (`Wendelon[A-Z]`) — new
    symbols clean.
18. Newtype-unit rule — any new newtypes route
    through `src/schema/units.rs` via `impl_unit!`.
19. No version suffix on new types.
20. SIMD helper crate dependency (if Phase 4
    escalation triggered) scoped to one helper module
    only; architectural test pins the import
    boundary.

**Exit gate:** ~20 incremental architectural guards
land, each with ADR 059 §"Architectural lockdown"
cross-reference in test name / doc.

## Phase 12 — Promotion flip + ADR 058 amendment + roadmap update

Files:

- `src/config.rs` — `EncodingComputeConfig::default()`:
  flip `enabled = true`; for any predicate-evaluator
  kind that missed the gate, omit it from
  `enabled_predicate_evaluators` (the existing allow-
  list); cascade-grain aggregate misses are enforced
  by the eligibility layer added in Phase 9, not by
  config (because `enabled_aggregate_kernels` is keyed
  by aggregate kind, not cascade).
- `plans/adr/058_compute_over_encoded_data.md` —
  amend §"Where we are" cross-reference (kernel-
  promotion follow-up closes; pointer to landed
  bench artifact at ADR 059 path).
- `plans/wip/roadmap.md` — update §"Where we are"
  and §"Forward sequence" to reflect ADR 059 shipped
  (at next planning-cadence boundary).

Work:

1. Confirm Phase 10 bench artifact passes promotion
   gates on the ship-class machine for the council-
   chosen default evaluator set.
2. Flip `enabled` default + narrow
   `enabled_predicate_evaluators` allow-list (omitting
   any predicate kind that missed the gate) in a
   dedicated commit (matches ADR 056 / 057 / 058
   promotion-commit style).
3. Amend ADR 058 §"Where we are" reservation to
   point at the landed
   `plans/impl/059_native_cocd_kernels_bench_results.md`
   path; remove the speculative ADR 058 reservation
   path (no compat hedge — file never existed; per
   `plans/conventions/no-premature-backward-compat.md`).
4. Roadmap update at planning-cadence boundary.

**Exit gate:** ADR 059 promoted to shipped state.
`git log` carries one commit per phase 1–11, plus one
promotion commit at phase 12. Total: ~12–14 commits
matching ADR 057 / 058 cadence.

## Verification gate

Required before promotion:

```sh
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test architecture_test
cargo test --test metrics_smoke
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test lsm_manager_manifest
cargo bench --bench encoding_compute -- --quick
cargo bench --bench encoding_compute
```

Promotion requires:

- all tests above green;
- bench artifact meets Phase 10 gates for the
  council-chosen default evaluator set;
- `EncodingComputeConfig.enabled` flips to `true`
  only in the same changeset that records passing
  kernel-promotion evidence (any predicate kind that
  missed is omitted from
  `enabled_predicate_evaluators`; cascade-grain
  aggregate misses are gated by the Phase 9
  eligibility layer), OR remains `false` with
  explicit council disposition recorded in the bench
  artifact;
- ADR 058 §"Where we are" amended in the same
  changeset.

## File-by-file expected edits

- `src/storage/encoding/cascade/codec.rs` — expose or
  share parser helpers without changing encoded
  bytes.
- `src/storage/encoding/compute/dispatch.rs` — route
  to native kernels via eligibility layer; retain
  decode oracle as production fallback.
- `src/storage/encoding/compute/kernels/bitpacked.rs`
  — native packed predicate + aggregate (byte-
  aligned fast path + non-aligned packed-bit reader).
- `src/storage/encoding/compute/kernels/rle.rs` —
  native byte-run streaming predicate + aggregate.
- `src/storage/encoding/compute/kernels/dictionary.rs`
  — native dictionary match-set + index-walk +
  aggregate.
- `src/storage/encoding/compute/kernels/delta.rs` —
  native prefix-sum predicate + aggregate.
- `src/storage/encoding/compute/kernels/validity.rs`
  — load-bearing 3VL helper.
- `src/storage/encoding/compute/mask.rs` —
  specialized mask algebra.
- `src/storage/encoding/compute/aggregate.rs` —
  native fold dispatch preserving all-or-nothing
  policy.
- `src/storage/manager.rs` — minimal integration if
  dispatch signatures change.
- `src/metrics/registry.rs` — no new labels
  expected.
- `src/config.rs` — `EncodingComputeConfig::default()`
  flip + `enabled_predicate_evaluators` allow-list
  narrow (Phase 12 only).
- `Cargo.toml` — SIMD helper crate dep iff Phase 4
  escalation triggered.
- `tests/encoding_compute_test.rs` — parity +
  corruption tests.
- `tests/encoding_compute_kernel_invariants.rs` —
  property suite (new file).
- `tests/architecture_test.rs` — ADR 059 guards.
- `benches/encoding_compute.rs` — native kernel
  matrix + three-way comparison.
- `plans/impl/059_native_cocd_kernels_bench_results.md`
  — checked-in evidence artifact.
- `plans/adr/058_compute_over_encoded_data.md` —
  §"Where we are" amendment (Phase 12 only).

## Risks and mitigations

**Auto-vec doesn't deliver expected speedup.** Phase 4
(Dictionary) is the most-likely escalation trigger
given indirect access patterns in bit-packed index
walks. Mitigation: small SIMD helper crate adoption
scoped to the affected kernel only (architectural test
pins the import boundary). `std::arch` reserved as
second-tier escalation.

**RLE byte-run misses gate substantially.** Default
`enabled_predicate_evaluators` omits both `RlePlain`
and `RleBitPacked`; follow-up ADR considers ADR 057
value-run format change. ADR 059 does not block on this;
the substrate hierarchy still gains BitPacked,
Dictionary, and Delta predicate speedups.

**Delta default inclusion.** Phase 10 evidence clears
the predicate gate for Delta; it remains in the default
predicate allow-list. Non-monotonic Delta remains the
harder shape, and signed value aggregates are not part
of the default aggregate route.

**Property suite reveals byte-mismatch.** Substrate
remains the oracle; native kernel iterates until
property suite is green.

**Cross-replica byte-equality concern from SIMD.**
Integer-only aggregate scope makes this a non-risk;
integer comparisons + integer arithmetic byte-
identical across CPU vendors. Float aggregates remain
excluded per ADR 058.

**Bench-machine variance between Phase 8 substrate
floor and Phase 10 promotion measurement.** Both runs
must use the same ship-class machine for apples-to-
apples ≥ 2× claim. Bench artifact records machine
class explicitly.

**Dictionary match-set overflow on high-cardinality
fixtures.** Existing 4_096 cap; above cap routes to
the decode-scalar oracle via the existing
`EncodingComputeFallbackReason` enum (pre-kernel
fallback, not a reject — match-set size is determined
from the dictionary value table before the index walk
begins).

## Deferred (matches ADR)

- ADR 057 RLE value-run format follow-up, conditional
  on Phase 10 evidence (ADR Open Question 1).
- FSST evaluator (ADR 058 deferral).
- ALP evaluator (ADR 058 deferral).
- Page-level min/max/null stats (own ADR).
- Delta prefix-sum side table (own ADR).
- Sparse Phase-C narrowing (own ADR).
- Variable-width Dictionary predicate expansion.
- Float / decimal / time-sum-avg / Utf8 min/max
  aggregate expansion (ADR 040 + ADR 058 deferrals).
- Runtime-measured `MockObjectStore` GET counter
  (council may fold into Phase 10 if cheap).

## Open implementation questions

Mirror ADR 059 open questions:

1. **RLE byte-run native viability.** Empirical;
   settles in Phase 6 / Phase 10 bench.
2. **Delta default-set inclusion.** Empirical;
   settles in Phase 5 / Phase 10 bench.

All other questions converged at the council level
(see ADR §"Open questions" for the converged-list
record).
