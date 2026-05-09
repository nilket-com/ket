# Wendelon Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

**Update cadence.** The roadmap is rewritten at planning
sessions, not at implementation commits. The council operates on
a block-chain rhythm: plan → implement → plan → implement, and
this file is a snapshot of the most recent plan. Between
sessions the roadmap lags the code on purpose — an item can read
"implementation phases pending" while the implementation is
already merged on `main`, or "full 100k / 1M bench matrix
remains ahead" while a smoke run has already landed. That is
expected, not a discrepancy. Don't open a session by flagging
the gap as if it were news; check `git log` and the relevant
impl plan / bench-results file for actual status, and roll
roadmap edits into the next planning session along with the
forward-sequence rewrite.

## Where we are

ADR 065 (Dense Predicate-Zip with Sparse Projection) is
implemented and promoted to shipped with bench evidence at
`plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`.
Council-converged across Claude / Codex / Gemini after six
iterations. Phase 0 read-only audit completed against
`src/storage/manager.rs`,
`src/storage/encoding/sparse_projection_fetch.rs`, and
`src/storage/row_group.rs`; engagement surface confirmed
non-trivial (the
`SparseProjectionFetchDecline::DensePredicateBatchPresent`
gate at `sparse_projection_fetch.rs:120-122` is what ADR
065 lifts; the
`SparseProjectionFetchInput.dense_predicate_batches_present`
field at line 91 is what ADR 065 removes). Lifts ADR 060 /
063 / 064's mask-only Phase B contract for sparse
projection: when predicate evaluation populates
`predicate_projection_by_group` *and* produces an exact
survival mask, sparse projection engages for the remaining
projection requirement. **Manager-side three-case
dispatch** distinguishes Case A (mask-only), Case B
(predicate/output-overlap zip), and Case C (no-overlap
dense-fallback). The Case B/C discriminator is the
derived `predicate_output_overlap` set
(`predicate_fetched_requirement.predicate_columns ∩
requirement.output_columns`), not
`predicate_fetched_requirement.is_empty()` — the latter
is non-empty whenever predicate fetch happened
regardless of output overlap.
`predicate_fetched_requirement` retains its current role
of excluding already-fetched columns from projection
fetch. **V1 engagement surface narrows to the scalar
dense-decode branch** at `manager.rs:9931` — the only
production site that populates
`predicate_projection_by_group`. The encoding-compute
paths (ADR 058 / 059 native predicates, ADR 063 Maybe-
page dense COCD) emit mask-only `PhaseBOutcome` and
engage Case A in V1; multiplicative ADR 063 + ADR 065
zip composition is V2 work. The sparse projection
planner stays metadata-only; manager owns the decoded
predicate batches.
**Page-local zip** slices the dense predicate batch to the
selected page row span, applies the same page-local
survival-mask slice to both sides via
`filtered_predicate_page_batch` (in
`page_materialize.rs`), and assembles in
`ColumnFetchRequirement` order via
`zip_predicate_and_projection_columns` (in
`row_group.rs`). `materialize_sparse_projection_pages`
splits into a per-page lower helper +
`SparseProjectionPageBatch` tagged batches plus the
existing concat wrapper. ADR 064's `rebase_validity_bitmap`
is reused for nullable sparse-projected columns. Variable-
width allowed on dense predicate side (already decoded);
sparse projection side stays ADR 064 fixed-width nullable.
**Predicate bytes are sunk** in the projection economic
gate (`sparse_projection_plaintext_bytes <
dense_projection_plaintext_bytes`); bench artifact records
total-query bytes on a separate axis. **Promotion passed on
three axes**: clustered 5% representative rows clear ≥ 1.5×
remaining-projection plaintext bytes, ≥ 1.25× total-query
plaintext bytes, and ≥ 1.2× wall-clock improvement; ≥ 3×
remaining-projection reduction recorded as headline
diagnostic. Synthetic always-dense-fallback bench fixture
remains the drift-resistant regression baseline across
future native-evaluator widening (FSST / ALP / RLE-value-
run / float). **Architectural
inextricability**: mask must be exact before zip engages;
predicate and projection filtered with same page-local
mask; dense predicate fetch is not sparse predicate fetch;
sparse zip output bypasses
`apply_phase_b_masks_to_projected_row_groups` (already
page-filtered); page-local assembly invariant (no filler
reconstruction). V1 does not add planner-level private
decline variants for Maybe-mask survivors or predicate-batch
page-span / row-count invariants; manager-side checks fail
closed via existing storage / Arrow errors after sparse fetch
begins. **Naming reconciliation per
`plans/conventions/no-planning-phase-names.md`**: ADR 065
uses semantic names throughout; production already has
semantic locals; residual phase-named identifiers
(`PhaseBOutcome`,
`apply_phase_b_masks_to_projected_row_groups`,
`zip_phase_b_and_phase_c_projected_row_group`,
`phase_c_requirement` parameter) slated for the broader
convention sweep flagged by ADR 060 — ADR 065 doesn't
bundle. Closed surface: no new on-disk shape, no new
`L2FetchStrategy` / `EncodedRowMask` / fallback / reject /
catalog variant; no IPC custom_metadata key, no
`ENCODING_DIRECTORY_FORMAT_VERSION` bump, no wire /
descriptor / PBC / capability / `ObjectStore` trait change;
no Cargo feature, no metric label, no operator knob. ADR
022 / ADR 056 / ADR 057 / ADR 063 directory shapes
unchanged. ADR 060 module location unchanged (extended in
place). See
`plans/adr/065_dense_predicate_zip_with_sparse_projection.md`
and
`plans/impl/065_dense_predicate_zip_with_sparse_projection.md`.

ADR 064 (Nullable Sparse Projection) is implemented and
promoted to shipped with bench evidence at
`plans/impl/064_nullable_sparse_projection_bench_results.md`.
Council-converged across Claude / Codex / Gemini after six
iterations. Lifts ADR 060's
`SparseProjectionFetchDecline::NullableProjectedColumn`
decline for fixed-width projection columns: the planner
fetches and re-bases raw Arrow validity bitmap slices
alongside selected value pages; the page materializer
assembles nullable Arrow arrays without using dense
row-group buffers. **V1 cut is raw byte-addressable validity
only** — validity buffers with `EncodingDirectory` entries
decline to dense behind a private planner reason; encoded-
validity page decoding is a follow-up ADR. Phase 0 was a
fork-resolution gate (raw-only proceeds; mixed declines on
encoded; encoded-only revises pre-code) and confirmed raw
validity is a non-no-op V1 engagement path against current
writer output. **V1 type and cascade catalog pinned to
ADR 060's current materializer** (not ADR 063's page-stat
catalog). Eligibility joint across value and validity
buffers; row groups with `column_null_counts == 0` for
nullable Arrow fields stay on the existing all-valid sparse
path. Page-local validity rebase
(`rebase_validity_bitmap` in `page_materialize.rs`) handles
byte-aligned and non-byte-aligned page starts. Mask-first
ordering preserved (`None` / `All` unchanged). ADR 065 later
lifts the dense-predicate-batch decline for the scalar dense-
decode branch while keeping ADR 064's nullable projection
catalog unchanged.
**Composability with ADR 063 multiplicative**: ADR 063
narrows predicate bytes, ADR 064 narrows projection bytes;
the bench artifact records ExactMatch-driven and Maybe-driven
projection pages as separate rows so the asymmetry
(predicate bytes may be zero while ADR 064 still fetches
selected projection value + validity bytes) is visible.
**Headline bench**: clustered 5% nullable sparse projection
passed across BitPacked and Dictionary with 5.36×–19.98×
plaintext-byte reduction and 1.50×–5.45× wall-clock speedup,
substantially exceeding the 1.5× plaintext-byte / 1.2×
wall-clock promotion gates and the 3× headline diagnostic.
All-null page-skip optimization deferred to V2 with explicit
architectural guard. Closed surface: no new
`L2FetchStrategy` / `EncodedRowMask` / fallback / reject /
catalog variant; no new IPC custom_metadata key, no
`ENCODING_DIRECTORY_FORMAT_VERSION` bump, no new wire /
descriptor / PBC / capability / `ObjectStore` trait change;
no Cargo feature, nightly rustc dependency, public PBC
opcode, metric label, or operator-visible nullable engagement
knob. ADR 022 / ADR 056 / ADR 057 directory shapes
unchanged. ADR 060 module location unchanged (extended in
place). See
`plans/adr/064_nullable_sparse_projection.md` and
`plans/impl/064_nullable_sparse_projection.md`.

ADR 063 (Page-Level Predicate Stats + Predicate-Fetch
Narrowing) is implemented and promoted to shipped with bench
evidence at
`plans/impl/063_page_level_predicate_stats_bench_results.md`.
It amends ADR 057 with an additive
`BufferEncoding.page_stats: Vec<EncodingPageStats>` field and
an `ENCODING_DIRECTORY_FORMAT_VERSION` bump (no compat
decoder per `no-premature-backward-compat.md`). Adds an
internal predicate-page fetch planner inside
`L2FetchStrategy::RowGroup`'s Phase B that narrows encoded
predicate fetch to surviving pages before survival-mask
construction. **Three-state classification**: `Disjoint`
(stats prove false → `EncodedRowMask::None` slice),
`ExactMatch` (stats prove true for every row, with
null-position-conservative correctness → `EncodedRowMask::All`
slice), `Maybe` (fetch + COCD evaluate). For value
comparisons, `ExactMatch` requires `null_count == 0`; for
null predicates, `IS NULL` requires
`null_count == page_value_count` and `IS NOT NULL` requires
`null_count == 0`. **Cascade eligibility = intersection of ADR 059's default
predicate allow-list AND `EncodingCascade::emits_pages_in_v1()`**:
the V1 intersection is `BitPacked`, `DictionaryPlain`,
`DictionaryBitPacked`. `Delta` is allow-listed but does not
emit pages in V1 (would require an ADR 057 amendment); RLE
byte-run is page-emitting but not allow-listed (Phase 10
evidence missed the gate); both carry empty `page_stats`.
**Type catalog = fixed-width subset of ADR 056's
`default_stats_supported_types`**: Int8/16/32/64,
UInt8/16/32/64, Timestamp(ms/us/ns), Date32/64, Boolean.
Float and `Time` types are out of scope (not in ADR 056
support). UTF-8 / LargeUtf8 / Binary / LargeBinary continue
to receive ADR 056 row-group stats but no page stats in V1. **Shared
`CanonicalScalar`**: row-group `CanonicalScalar` helpers
move to `src/storage/stats.rs`; row-group + page stats use
the same encoding path (single-source architectural-test
guard). **Predicate-page planner**: lives at
`src/storage/encoding/predicate_page_fetch.rs` (sibling to
ADR 060's `sparse_projection_fetch.rs`); production read
path consults it before issuing dense predicate fetch;
declines fall through to existing dense path; engaged narrowed
plan returns mask-only `PhaseBOutcome` with no dense predicate
batches. **Phase 0 size-model + profile-emission gate is the sole
V1 budget discipline**: pre-code modeling on representative
fixtures must show (1) combined directory bytes fit ADR
044's 64 KiB initial-probe envelope with page-stats fraction
≤ 4% of record body, AND (2) `PointRead`-profile win/cost
ratio justifies emission (≥ 1.5× modeled clustered-5%
reduction). Shipped quick evidence keeps both profiles in
scope: clustered 5% predicate bytes drop `9,153 -> 144` for
`Scan` (63.56x) and `1,145 -> 144` for `PointRead` (7.95x),
with combined metadata inside the 64 KiB probe envelope. The
profile-emission scope is fixed in Phase 0 and not revisited
at promotion. V1
does **not** add a runtime per-record budget fallback,
profile-keyed runtime omission, partial per-column
heuristic, operator knob, metric label, or public fallback
variant — Wendelon V1 house style is gate-driven (matching
ADR 062 deterministic profile emission, ADR 060 plan-time
decline). **Writer eligibility ≠ reader narrowing**: a
buffer may carry stats and still see its predicate-page
planner decline at plan time (additional COCD/predicate/
economic gates); that is not a writer misconfiguration.
**Composability with ADR 060**: page-stat-narrowed
predicate fetch becomes an additional upstream source of
exact masks for ADR 060's sparse projection planner. **Closed
surface**: no new on-disk shape (one additive directory
field), no new IPC custom_metadata key, no new wire /
descriptor / PBC / capability / `ObjectStore` trait change;
no new `RowGroupRejectCode` / `RowGroupFallbackReason` /
`EncodingRejectCode` / `EncodingFallbackReason` /
`EncodingComputeRejectCode` /
`EncodingComputeFallbackReason` / `L2FetchStrategy` /
`EncodedRowMask` / `EncodingCascadeKind` /
`EncodingComputeEvaluatorKind` / `EncodedAggregateKind` /
`PredicateAtomOp` variant. ADR 022 / ADR 056 directory
shapes unchanged. **`enabled = false` writer default**
carried forward from ADR 056 / 062. See
`plans/adr/063_page_level_predicate_stats.md` and
`plans/impl/063_page_level_predicate_stats.md`.

ADR 062 (Row-Group Sizing Profile) is implemented and
promoted to shipped with bench evidence at
`plans/impl/062_row_group_sizing_profile_bench_results.md`.
Amends ADR 056 with a closed two-variant
`RowGroupSizingProfile { PointRead, Scan }` catalog and
deployment-wide profile selection. **`PointRead`**
preserves ADR 056 V1 defaults byte-identically (4K-row
target, 8K-row max, 8K-row paging-rows gate, 512 KiB
paging-bytes gate); tuned for ADR 052 secondary-index
point reads and OLTP-shape access. **`Scan`** lifts
sizing for analytical scan deployments: 65,536-row
target, 131,072-row max (= 2 × target slack headroom
matching ADR 056 convention), 65,536-row paging-rows
gate (= target, enables one-entry directories), 1 MiB
paging-bytes gate. Tuned for ADR 061 macro-block
coalescer economics on warehouse-scale records.
**Deployment-wide selection** via
`RowGroupWriterConfig.profile`; operator-declared in
node-local config, replicated identically across
deployment replicas. Per-record geometry-keyed selection
deferred to V2 with explicit promotion gate (operational
evidence of mixed-shape deployments). **Operator config
escape hatch**: `profile = "point_read"` (default)
accepts numeric overrides for non-breaking migration;
`profile = "scan"` strictly ignores numeric overrides
with a config-load warning. **One-entry `Scan`
directory accepted**: when a `Scan`-profile record at
exactly the target row count produces 1 row group, the
writer emits a one-entry `RecordPageDirectory` rather
than today's `batches.len() <= 1` single-batch
fallback; preserves ADR 057 / 060 leverage on boundary
records. `PointRead` profile retains today's
single-batch fallback unchanged. **Closed surface**: no
new on-disk shape, no new directory, no new metadata
surface, no new wire / descriptor / PBC / capability /
`ObjectStore` trait change; no new
`RowGroupRejectCode` / `RowGroupFallbackReason` /
`L2FetchStrategy` / `EncodedRowMask` variant. ADR 022 /
ADR 056 directory shapes unchanged. **`enabled = false`
default** — ADR 062 is sizing, not rollout; flipping
default-on is a separate operational ADR. **Naming
rationale**: `PointRead` / `Scan` describe what the
profile optimizes for (access pattern), not the
deployment hardware envelope (which is ADR 030
territory — `Commodity / Premium / Hardened`
describes hardware support envelopes, orthogonal to ADR
062's access-pattern shape). See
`plans/adr/062_row_group_sizing_profile.md` and
`plans/impl/062_row_group_sizing_profile.md`.

ADR 061 (Page-Aligned Authenticated Chunks with
Macro-Block Read Coalescing) council-converged
across Claude / Codex / Gemini after five council
iterations, implemented, and promoted to shipped with
bench evidence at
`plans/impl/061_page_aligned_authenticated_chunk_layout_bench_results.md`.
Closes ADR 060's recorded-not-gated ciphertext
follow-up. **Two-layer decoupling**: AEAD
authentication boundary and physical network read
unit are independent layers optimized for
different forces. **Layer 1 — AEAD chunks**:
small per-slab `chunk_size_bytes` from a closed
set `{4, 8, 16, 32, 64} KiB`; default 16 KiB for
slabs with an `EncodingDirectory`, 64 KiB baseline
elsewhere. **Buffer starts** align to chunk
boundaries via opaque pad bytes in IPC body slack
*between* adjacent buffers (no intra-buffer
pad — ADR 057's contiguous `page_end_offsets` is
a prefix-end array and cannot represent intra-
buffer gaps; the directory shape is unchanged).
Pad cost ≤ 5%; pad bytes AEAD-authenticated by
the enclosing chunk's existing tag, never enter
the page-decode path. **Layer 2 — network GETs**:
ADR 044's existing `TieredMultiRangeReadPlan`
coalescer merges contiguous AEAD chunks into
multi-MiB `get_range` calls; warehouse-scale
dense paths produce p50 ≥ 1 MiB GETs even with
16 KiB chunks underneath. **Closed surface**: no
new on-disk shape, no new directory, no new
metadata surface, no new wire / descriptor /
PBC / capability / `ObjectStore` trait change.
ADR 022 §3's existing `chunk_size_bytes` u32
field carries the per-slab value — Open Question
4 explicitly anticipated this. **No-directory
discipline**: Phase 0 formally proves that ADR
022 `chunk_size_bytes` + ADR 056 / 057
directories suffice; if Phase 0 fails,
implementation stops and the council produces a
revised ADR/impl pair as a separate decision —
no silent reshape under this plan. **ADR 060
ciphertext floor**: drops from 64 KiB → 16 KiB
on default geometry, yielding 4× ciphertext
narrowing on clustered fixtures; analytics
dense throughput preserved by the macro-block
coalescer (verified by load-bearing Phase 5
four-sub-property test: single dense GET, AEAD
CPU bounded < 1%, clustered sparse coalesce,
anti-clustered sparse cap). **Cost arithmetic
verified**: at 16 KiB AEAD chunks + macro-block
coalescing, a 100 GiB analytic scan issues
~few hundred GETs (~$0.0001 in S3 request cost),
not millions of GETs (~$2.62 in request cost
alone — the rejection shape ADR 061 forbids).
**V2 promotion path**: variable buffer-aligned
chunks via encrypted `AuthenticatedChunkDirectory`
deferred with explicit gate — post-V1 evidence
that ADR 044 column-fetch over-fetch (~3% at
warehouse scale) becomes a measurable bottleneck
on operational query workloads. **Cross-replica
determinism**: writer selector pure-deterministic
over directory + config; slab-byte equality
across replicas. **No new fallback / reject /
catalog variant**; only
`SlabFormatError::UnsupportedChunkSize` on bad
`chunk_size_bytes` (slab-format error family,
not a fallback event). See
`plans/adr/061_page_aligned_authenticated_chunk_layout.md`
and
`plans/impl/061_page_aligned_authenticated_chunk_layout.md`.

ADR 060 (Sparse Projection Fetch Narrowing)
council-converged across Claude / Codex / Gemini,
implemented, and promoted to shipped. Full Phase 7
bench evidence at
`plans/impl/060_sparse_projection_fetch_narrowing_bench_results.md`
backs the shipped scope. **Internal reader
optimization**: when predicate evaluation produces
an exact-COCD `EncodedRowMask` for a surviving row
group, the projection-fetch stage converts the
mask into a selected encoding-page set per
projected buffer and fetches only the byte ranges
spanning those pages — generalizing ADR 052's
secondary-index point-read SIMD-page fast path
from named point rows to mask survivors under
clustered selectivity. **Closed surface**: no new
`L2FetchStrategy` variant, no wire / descriptor /
PBC / capability surface, no IPC `custom_metadata`
key, no public Toron-visible hint, no ADR 056 /
057 directory shape change, no `EncodedRowMask` /
catalog variants, no new
`L2EncodingFetchConfig` / `L2RowGroupFetchConfig` /
`EncodingComputeConfig` field. **Default sparse
projection engagement set**: `BitPacked` and
`Dictionary { inner: Plain | BitPacked }`. RLE is
correctness-eligible because
`BufferEncoding.page_value_count` records rows-
per-page for byte-run RLE, but Phase 7 settles V1
RLE routing as default-off via eligibility-layer
cascade exclusion; value-run RLE remains a
separate ADR. **Per-row-group decision grain** with
cross-column page-geometry agreement; mixed sparse
/ dense projected columns within one materialize
pass deferred. **V1 engages only for exact-COCD
masks** (`decoded_predicate_batches` empty); dense
predicate-zip with sparse projection deferred.
**Fixed-width + non-null** projection only;
nullable + variable-width route to dense.
**Activation** is implicit on the existing pair of
toggles (`l2_row_group_fetch.enabled` AND
`l2_encoding_fetch.enabled`); no new flag.
**Economic gating**: strict plan-time sparse_bytes
< dense_bytes comparison over selected plaintext
projection bytes, capped by
`max_simd_pages_per_record` /
`max_ranges_per_record` / selected-byte-fraction
plus an internal `MAX_SPARSE_RANGES_PER_BUFFER`
helper-local `const` for per-buffer fragmentation.
Phase 7 revises the byte promotion gate from
ciphertext bytes to selected plaintext projection
bytes because ADR 044 chunk-aligned authenticated
range reads bound ciphertext-byte ratios around
1.0×–1.1× on the representative 1M fixtures;
ciphertext ratios are recorded, not gated, and a
page-aligned authenticated chunk-layout ADR is the
follow-up for material ciphertext narrowing.
**Two-module split**:
`src/storage/encoding/page_materialize.rs` hosts
shared encoded-page decode + assembly helpers;
`src/storage/encoding/sparse_projection_fetch.rs`
hosts the active planner + decision + economic
logic. **Internal decline discipline**: decline
reasons live entirely inside the planner module and
never become metric labels, fallback variants, or
reject codes. Existing byte / range / page metrics
absorb sparse decisions. **No-planning-phase-names
convention** applied: new identifiers use
`SparseProjectionFetch*` / `RowToPageMap` /
`ActivePageSet`, not `SparsePhaseC*`. Existing
leaks (`phase_c_fetch`,
`apply_phase_b_masks_to_projected_row_groups`,
`PhaseBOutcome`) are slated for the broader
convention sweep — ADR 060 must not extend them.
See `plans/adr/060_sparse_projection_fetch_narrowing.md`,
`plans/impl/060_sparse_projection_fetch_narrowing.md`,
and
`plans/impl/060_sparse_projection_fetch_narrowing_bench_results.md`.

ADR 059 (Native Per-Cascade COCD Kernels)
council-converged, implemented, and promoted to shipped.
Closes ADR 058's promote-to-default follow-up. Full Phase 10
bench evidence at
`plans/impl/059_native_cocd_kernels_bench_results.md`
clears the narrowed default-set promotion gate. **Native
predicate kernels** for the ADR 058 evaluator catalog
(`BitPacked`, `Rle`, `Dictionary`, `Delta`) replace the
substrate decode-then-scan body with cascade-aware
specialization that operates directly on encoded bytes.
**BitPacked**: byte-aligned `UInt8` fast path with aligned
bitmap-byte emission for the default route; legal
non-byte-aligned BitPacked sublanes remain behind pre-kernel
eligibility and fall back to the fixed-width decode-scalar
oracle unless explicitly enabled later. **Rle**: byte-run
streaming over the existing ADR 057 `(byte_run_len: u32,
byte: u8)` format landed, but Phase 10 evidence missed the
2x default-inclusion gate; `RlePlain` / `RleBitPacked` are
omitted from the default predicate allow-list. **Dictionary**:
WDD1 / WDB2 frame parse + match-set compilation + index walk;
low-cardinality Dictionary predicates clear the gate, while
higher-cardinality sublanes fall back before kernel entry.
**Delta**: WDL1 prefix-sum walk with monotonic-shape short-
circuits clears the predicate gate and remains in the default
predicate allow-list. **Native aggregate folds** are
implemented and oracle-checked, but default aggregate routing
is narrowed by cascade-grain eligibility: count kernels remain
enabled, and unsigned value aggregates default-route only for
byte-aligned BitPacked `UInt8` and plain low-cardinality
Dictionary. **Closed-catalog discipline applies recursively**:
no new `EncodingComputeEvaluatorKind` /
`EncodedAggregateKind` / `EncodingComputeRejectCode` /
`EncodingComputeFallbackReason` / `PredicateAtomOp` /
`EncodedRowMask` variants. **Two-step dispatch discipline**:
pre-kernel eligibility routes legal-but-not-promoted sublanes
to the decode-then-scalar oracle; once a native kernel begins
parsing encoded bytes, invariant failures return through ADR
058's existing reject taxonomy. **Decode-then-scalar oracle**
stays in production as the test oracle, fixed-width fallback,
and kill-switch target. **Central validity composition** in
`kernels/validity.rs`; kernel-local 3VL implementations
rejected. **Specialized `EncodedRowMask` algebra** (no enum
changes; bytewise `Bitmap` AND/OR/NOT, `Runs` merge/flip,
bounded densification). **Byte-identical mask + aggregate-
partial invariant** under `cargo test` for every (cascade ×
atom × dtype × selectivity) combination. **SIMD strategy**:
stable rustc; auto-vectorization via tight loops + block-
unrolling + stack scratch; escalation route on bench miss is
small SIMD helper crate first and target-feature `std::arch`
second — no nightly, no Cargo feature gate, no operator-
visible knob. **Default evaluator set** is encoded in two
existing surfaces: pre-kernel eligibility (cascade grain;
includes cascade-grain aggregate exclusion because
`enabled_aggregate_kernels` is keyed by aggregate kind, not
cascade) plus the existing `enabled_predicate_evaluators`
allow-list in `EncodingComputeConfig` (predicate-kind grain).
No per-cascade `enabled_*` flag, no new operator surface.
**Promotion flip landed**: `EncodingComputeConfig::default()`
now sets `enabled = true`; the default predicate allow-list
contains `BitPacked`, `DictionaryPlain`,
`DictionaryBitPacked`, and `Delta`, and omits RLE. See
`plans/adr/059_native_cocd_kernels.md` and
`plans/impl/059_native_cocd_kernels.md`.

ADR 058 (Compute Over Encoded Data, COCD)
council-converged across six iterations, implemented, and
promoted after Phase 8 substrate bench evidence. Native
per-cascade kernels (BitPacked SIMD, Rle run-grain, Dictionary
match-set + index walk, Delta prefix-sum) and the ≥ 2x CPU
promote-to-default gate are now the **ADR 059** follow-up
scope; ADR 059's narrowed native set is now enabled by default,
with RLE and non-promoted sublanes excluded by the predicate
allow-list and pre-kernel eligibility. Kernel-promotion evidence
landed at
`plans/impl/059_native_cocd_kernels_bench_results.md`.
Innermost layer of the storage hierarchy ADR 056 / ADR 057
established.
**Closed 6-variant predicate evaluator catalog**: `BitPacked`,
`RlePlain`, `RleBitPacked`, `DictionaryPlain`,
`DictionaryBitPacked`, `Delta` (full atom set, full-buffer
grain). `Plain`, `Lz4Frame`, `Fsst`, `Alp` cascades fall back
to materialize-then-evaluate. **Closed 8-kernel aggregate
catalog**: `count_star`, `count_non_null`, `min`, `max`,
`signed_sum`, `unsigned_sum`, `signed_avg`, `unsigned_avg`.
`min`/`max` target fixed-width integer / time / Boolean and
fixed-width Dictionary cascades; sum/avg are integer-only
(Int8/16/32/64 for signed, UInt8/16/32/64 for unsigned),
matching ADR 040 dense aggregate validation. Runs after
ADR 040 CSB direct-answer ordering. COCD kernels never
exceed ADR 040 dense validation; dense-supported shapes
that COCD does not yet fold (Utf8 / Binary `min`/`max`,
`APPROX_DISTINCT`, `QUANTILE_KLL`) fall back to dense AGG
via the all-or-nothing mixed-request policy. **No new public PBC opcodes** — Toron-visible PBC
contract (ADR 035 / ADR 036) byte-identical; COCD is internal
executor dispatch, not a Toron-facing opcode extension.
**Activation via node-local `EncodingComputeConfig`**
(deployment-wide for cross-replica determinism), sibling to
ADR 057's `EncodingWriterConfig` / `L2EncodingFetchConfig`;
no descriptor field, no `NodeCapability` bit. **Exact /
Prefilter / DenseFallback predicate plan modes**: Exact when
every leaf has an encoded kernel (PBC tree composes via mask
algebra; PBC executor not invoked post-Phase-B); Prefilter
when a conjunction has at least one encoded and one residual
leaf (encoded leaves drop rows; original PBC re-runs against
survivors); DenseFallback for unsupported shapes (existing
ADR 057 path). **Dual economic gates**: leaf-level
selectivity gate at 0.25 (every encoded predicate leaf
including Exact-mode requires CSB-estimated row-group
selectivity ≤ 0.25; selectivity > 0.25 routes to dense
materialize-then-evaluate, which wins by reusing one fetch as
predicate + projection input) plus secondary Prefilter
rejection gate at 0.10 (encoded leaves must reject ≥ 10% of
rows to amortize residual narrowing). Aggregate folding is
ungated by predicate selectivity. **`EncodedRowMask` tagged
enum** (`All { rows }` / `None { rows }` /
`Bitmap { rows, bytes }` / `Runs { rows, runs }`):
RLE-friendly fixtures stay compact, all/none short-circuits
remove O(N) work on common Dictionary cases, mask byte
allocation bounded by `max_mask_bytes_per_record`. **Page-
grain dispatch default** for page-emitting cascades
(`BitPacked`, `Rle { inner: BitPacked }`,
`Dictionary { inner: BitPacked }`); buffer-grain for
`Rle { inner: Plain }`, `Dictionary { inner: Plain }`;
full-buffer grain for `Delta` (no page emission in ADR 057).
**Closed 5-code `EncodingComputeRejectCode` taxonomy**
(`MaskLengthMismatch`, `EncodedValueOverflow`,
`KernelDecodedLengthMismatch`, `AggregateOverflowMismatch`,
`SelectionRangeOverflow`); authentication / parse / decode
failures route to storage errors, never silent fallback.
**Closed 9-variant `EncodingComputeFallbackReason` enum** for
planner metric labels. Bounded `storage_l2_encoding_compute_*`
metric family with `{decision, reason, kernel, cascade, kind,
aggregate, violation_kind}` enumerations. V1 defaults:
`max_dict_match_set_size = 4_096`,
`min_buffer_rows_for_encoded_eval = 256`,
`max_parallel_kernel_tasks = 4`,
`max_mask_bytes_per_record = 256 KiB`,
`max_encoded_pages_per_record = 256`,
`min_selectivity_for_encoded_eval_micros = 250_000`,
`min_prefilter_rejection_micros = 100_000`. **Reader-CPU
bench gate** as starting target: predicate-only Exact COCD
≥ 2× faster than ADR 057 dense PBC on representative
`BitPacked` / `Rle` / `Dictionary` fixtures at low
selectivity; aggregate kernels ≥ 2× faster than dense AGG on
representative `Rle` / `BitPacked` fixtures; dense-selection
overhead ≤ 5% vs. ADR 057 dense path when COCD economically
falls back. Provisional discipline applies (revisable on
Phase 8 evidence). ADR 052 secondary-index point reads gain a
COCD confirm-step for COCD-supported cascades. ADR 055 LSM
compaction, ADR 054 CDC subscription, recovery, and
rehydration are byte-neutral to COCD. **~27 architectural
lockdown guards** in `tests/architecture_test.rs`. ADR 057
directory shape unchanged — COCD reads the existing
`BufferEncoding` / `EncodingCascade` shape. No new wire
`MsgType`, no new `FRAME_FLAG_*`, no new `NodeCapability`
bit, no Cargo feature, no new digest domain, no `ObjectStore`
trait change, no envelope `format_version` bump, no ADR 022
footer extension, no descriptor surface change, no public
Toron-visible PBC opcode addition, no ADR 057 cascade catalog
extension. `Fsst` / `Alp` cascade encoded evaluation deferred
(own kernel proofs); aggregate push-into-encoded for
`approx_distinct` / KLL / HLL / `count_distinct` / float /
decimal / GROUP BY all deferred to follow-up ADRs. See
`plans/adr/058_compute_over_encoded_data.md` and
`plans/impl/058_compute_over_encoded_data.md`.

ADR 057 (Per-Buffer Pages and Encoding Cascades)
council-converged across five iterations and promoted
after implementation plus Phase 10 bench evidence. Inner layer of the
Parquet / Vortex-style storage hierarchy ADR 056
opened. **Closed 8-variant cascade catalog**: `Plain`,
`Lz4Frame`, `BitPacked`, `Rle`, `Dictionary`, `Delta`,
`Fsst`, `Alp` with one-level composition via
`Box<EncodingLeafCascade>` (`EncodingLeafCascade ∈ {Plain,
Lz4Frame, BitPacked}`); arbitrary-depth nesting
rejected. **Activation via node-local
`EncodingWriterConfig`** (deployment-wide for cross-replica
byte-equality), not via
`RecordBodyCompressionPolicy::EncodingCascade` descriptor variant;
Encoding cascades do not change logical schema or table semantics,
so descriptor churn is unwarranted.
**`EncodingDirectory` lives in IPC schema
`custom_metadata`** under literal key
`storage.encoding_directory` — sibling to ADR 056's
`storage.row_group_directory`, encrypted with the
record body, authenticated by ADR 020 chunk AEAD.
**Compact directory shape** is the council's iter
3→4 win: cascades deduped once in
`cascades: Vec<EncodingCascade>`; one `BufferEncoding`
per `(row_group_index, ipc_column_ordinal,
ipc_buffer_ordinal)` triple referencing a cascade by
`EncodingCascadeIndex`; page-grain bytes captured as
`page_end_offsets: Vec<EncodingPageByteOffset>` prefix
array per buffer — not as one serialized row per page
(per-page rows would explode metadata on wide
schemas; ADR 056 directories already use 22–26% of
ADR 044's 64 KiB initial probe).
`EncodingPageDescriptor` is transient planner-only —
expanded from prefix offsets in memory, never
serialized. **Buffer-relative byte offsets** mean Encoding
metadata is invariant under any compaction or rewrite
that shifts the buffer's absolute position but
preserves its internal encoded layout; ADR 056
locates the buffer, ADR 057 slices pages within it.
**Page emission required** for `BitPacked`, `Rle`,
`Dictionary`; other cascades carry empty
`page_end_offsets` and decode at full-buffer grain in
V1. **Phase D extension** to ADR 056's three-phase
plan (Phase A metadata → B stats prune → C projected
fetch → **D Encoding materialization**); internal to the
`L2FetchStrategy::RowGroup(plan)` leg, no fifth
strategy variant. **Custom partial-buffer
materializer** lives in `src/storage/encoding/`,
satisfying ADR 056's V1-scope guards 25–26 (the
sanctioned home for `partial_buffer_materialize` and
`mini_page_*` symbols ADR 056 explicitly kept out of
its surface). **Decode-only V1** — PBC continues to
evaluate against materialized dense Arrow batches;
compute-over-encoded-data is ADR 058 (COCD)
territory. **Closed 5-code `EncodingRejectCode`
taxonomy** (`EncodingDirectoryParseError`,
`BufferEncodingMismatch`, `PageRangeOverflow`,
`CascadeUnsupported`, `DecodedLengthMismatch`);
authentication / parse / decode failures route to
storage errors, never silent fallback. **Closed
7-variant `EncodingFallbackReason` enum** for planner
metric labels. Bounded `storage_l2_encoding_*` metric
family with `{cascade, decision, reason, kind,
violation_kind}` enumerations. V1 defaults:
`target_simd_page_values = 1_024`,
`cascade_sample_rows = 256`,
`min_buffer_bytes_for_encoding = 4_096`,
`max_simd_pages_per_record = 64`,
`max_parallel_materialize_tasks = 4`. **Writer-cost
gate ≤ 75%** as starting target on the largest legal
representative fixture, defended by mandatory
compression and read-side wins in the same bench
artifact; revisable on Phase 10 evidence per the
Provisional discipline ADR 056's ≤ 15% miss
codified. ADR 052 secondary-index point reads
accelerated via SIMD-page fast path; ADR 055 LSM
compaction recomputes cascades deterministically;
CDC, recovery, rehydration are byte-neutral. No new
wire `MsgType`, no new `FRAME_FLAG_*`, no new
`NodeCapability` bit, no Cargo feature, no new digest
domain, no `ObjectStore` trait change, no envelope
`format_version` bump, no ADR 022 footer extension,
no descriptor surface change, no public Toron-visible
PBC opcode addition, no Vortex / Lance / Parquet
external dependency. **~30 architectural lockdown
guards** in `tests/architecture_test.rs`. See
`plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
and
`plans/impl/057_per_buffer_pages_and_encoding_cascades.md`
plus `plans/impl/057_per_buffer_pages_bench_results.md`.
Implementation and Phase 10 evidence have landed:
representative writer gate passes on repeated full
bench runs after bounding multi-page variable-width
Dictionary selection; typed compression targets pass;
point-read selected bytes are ≥16x smaller than the
ADR 056 row-group-buffer baseline; materialization is
faster than ADR 056-only on all representative fixtures;
metadata stays inside the 64 KiB initial probe; and
cross-replica determinism passes. ADR 058 is now
unblocked for drafting from concrete cascade-distribution
and page-grain evidence.

ADR 056 (Row-Group Fetch and Layered Page Directory)
is the immediate predecessor and the substrate ADR
057 nests inside. Promoted after Phase 10 accepted an
empirical writer-cost gate revision. Closes the
row-axis fetch-amplification shape question for
multi-row records and lays the outer layer of a
Parquet / Vortex-style storage hierarchy. **Layered
substrate sequencing committed**: ADR 056 is the outer
row-group + per-row-group stats layer; ADR 057 is the
inner per-buffer pages + encoding-cascades layer inside
each row-group RecordBatch's encoded buffers; ADR 058 is
compute over encoded representation (COCD). The layers are
equal priority; ordering is software-development
driven — row groups establish the outer contract
under which per-buffer pages can safely nest, so they
ship first.
Row groups are Arrow IPC RecordBatches: one
qualifying `MultiRow` record body becomes one IPC
stream with one schema message followed by N dense
row-group RecordBatch messages over the same physical
schema. **Paging engages only when both thresholds
are met**: `row_count >= min_paged_record_rows`
**AND** estimated bytes `>= min_paged_record_bytes`;
fall below either, single-batch encoding stands.
**`RecordPageDirectory` lives in IPC schema
`custom_metadata`** under literal key
`storage.row_group_directory` — encrypted with the
record body, authenticated by ADR 020 chunk AEAD; not
in the ADR 022 footer (cleartext leakage), not a
synthetic Arrow column (false row semantics).
Directory carries `RowGroupDirectoryEntry` per row
group, `BufferPageDirectoryEntry` per
`(row_group_index, ipc_column_ordinal,
ipc_buffer_ordinal)` triple — the V1 per-buffer
addressing surface — and `RowGroupColumnStats` per
row group for ADR 039-supported types.
**Three-phase fetch planner** extends ADR 044's two
phases: Phase A (metadata + directory probe), Phase
B (stats prune; predicate-column fetch when stats
insufficient → derive per-group survival masks),
Phase C (projected fetch over surviving groups,
zip Phase B columns + Phase C columns by IPC
ordinal, apply per-group mask, emit dense filtered
batch — PBC is **never re-evaluated** post-Phase-B).
**Conservative PBC predicate classifier** with
asymmetric AND/OR rule: conjunction skips when *any*
branch proves disjoint (unsupported sibling branches
do not block); disjunction skips only when *every*
branch proves disjoint; LIKE / IN / BETWEEN-with-cast
keep the group at top level, collapse to Keep
without blocking siblings under conjunction.
**`L2FetchStrategy`** four-leg dispatch:
`RowGroup(RowGroupFetchPlan) | Columnar(plan) |
WholeRecord | Fallback(reason)`. Closed 3-code
`RowGroupRejectCode` taxonomy
(`PageDirectoryParseError`,
`RowGroupRangeOverflow`, `LayoutUnsupported`);
authentication / parse / decode failures route to
storage errors, never the `Fallback(...)` branch.
Bounded `storage_l2_row_group_fetch_*` metric family
with `{strategy, reason, decision, kind,
violation_kind}` enumerations. V1 defaults:
`target_row_group_rows = 4_096`,
`max_row_group_rows = 8_192`,
`min_paged_record_rows = 8_192`,
`min_paged_record_bytes = 512 KiB`,
`max_selected_row_group_fraction_micros = 250_000`
(0.25), `max_selected_byte_fraction_micros = 500_000`
(0.50), `max_ranges_per_record = 16`,
`max_parallel_range_gets = 4`. Writer-side gated by
node-local `RowGroupWriterConfig` (default off,
opt-in after Phase 10 accepted the revised empirical
writer-cost gate); reader-side automatic on
directory presence — pre-056 sealed records (single-
RecordBatch IPC, no custom_metadata key) read
transparently via ADR 044 column fetch. ADR 052
secondary-index hits map `RecordRowIndex` → row group
via the directory; identity-axis sidecar shape
unchanged. ADR 055 LSM compaction inherits via
out-of-place sidecar rebuild and per-output-slab
directory recomputation under matching writer config
— cross-replica byte-equality holds, visible-state
digest invariant. CDC, recovery, rehydration are
byte-neutral (whole IPC stream decode). No new wire
`MsgType`, no new `FRAME_FLAG_*`, no new
`NodeCapability` bit, no Cargo feature, no new digest
domain, no `ObjectStore` trait change, no envelope
`format_version` bump, no ADR 022 footer extension,
no descriptor field, no synthetic Arrow column for
the directory, no LZ4 frame block-independence
mutation, no custom partial-buffer materializer
(stock Arrow IPC reader handles row-group decode).
**27 architectural lockdown guards** in
`tests/architecture_test.rs`, including two
ADR-057-scoped guards (no `partial_buffer_materialize`
symbols and no `mini_page_*` fields in ADR 056's
surface — those land in the encoding module)
and one Phase-5 guard against post-Phase-B PBC
re-evaluation. See
`plans/adr/056_row_group_fetch_and_page_directory.md`,
`plans/impl/056_row_group_fetch_and_page_directory.md`,
and
`plans/impl/056_row_group_fetch_bench_results.md`.
Implementation and Phase 10 bench evidence have
landed. The original `<= 15%` writer-cost target was
revised to an empirical `<= 25%` gate on the largest
legal representative fixture; wide threshold-margin
fixtures are documented as opt-in writer-cost
warnings. This bench artifact is now the input shape
for ADR 057.

ADR 055 (LSM-Style Leveled Compaction Substrate) is
the immediate predecessor and the substrate ADR 056
inherits for compaction-aware writers. Per-locality
compactor; four levels (`L0..L3`); HLC-disjoint
within each level; deterministic compaction so
replicas reproduce the same logical output set;
elision floor IS ADR 048's retention floor (no
separate compaction floor); manifest-snapshot pins
as the sole MVCC mechanism (no `relocated_reads`
shim); sidecar rebuild composes via ADR 052 §9
(replace-by-new); cold-rebuild visible-domain audit
over localized manifest views as the V1 install
gate; three V1 reject codes (`ManifestRace`,
`SidecarDigestMismatch`, `VisibleDigestInvariance`).
See `plans/adr/055_lsm_leveled_compaction_substrate.md`
and `plans/impl/055_lsm_leveled_compaction_substrate.md`.
Implementation phases pending; full small / medium /
large bench evidence is the promotion-to-shipped
gate.

## Prioritization philosophy

Wendelon is being sequenced as a complete-system launch rather
than as an incrementally exposed service. That changes the
roadmap constraint. In a live product, near-term user pain,
production evidence, and delivery commitments would rightly
pull high-impact access features forward. In this pre-launch
phase, those signals inform the design, but they do not have to
dictate the order of construction.

The ordering principle for this stretch is **software
composition**. The goal is to **minimize cumulative rework
across the whole tree before launch**: choose the order that
lets later ADRs build on settled foundations instead of
revisiting earlier work because a later ADR changed a core
shape. A feature wants to come *earlier* when it settles a
foundational type or invariant (`Identity`, `NodeId`, slab
footer, descriptor fingerprint) that many later features build
on. A feature wants to come *later* when it is a consumer or
refinement of those surfaces and composes cleanly once they are
stable.

This does not make LSM compaction, vector embeddings, or
encoding refinements less important. They remain central
warehouse capabilities. The sequencing claim is narrower: they
should consume the algebraic substrate (`StateDigest`),
lifecycle layer (tombstones, retention), topology decision,
identity / descriptor cleanup, access-pattern surface
(secondary indexes), wire-compression policy, and CDC substrate
rather than force those foundations to be retrofitted
afterward.

**Toron-pivot stretch (current).** Past the ADR 055 milestone the
roadmap turns toward compute-adjacent storage primitives in
preparation for the Toron compute layer. The ordering goal in
this stretch is to land the storage hierarchy that
analytical-scan workloads (BigQuery-killer territory) need
before opening Toron-side surface. Concretely: row-group fetch
+ page directory (ADR 056), then per-buffer pages + encoding
cascades (ADR 057), then compute-over-encoded-data substrate
(ADR 058 / COCD), then native per-cascade COCD kernels
(ADR 059) that flip the substrate from correctness-first to
enabled-by-default, then sparse projection fetch narrowing
(ADR 060) that converts exact-COCD survival masks into per-
row-group selected-page fetches so projection bytes scale with
clustered selectivity rather than row-group size. CDC follow-up
ADRs and vector embeddings are deliberately deferred behind the
storage hierarchy in this stretch — not because they are
smaller, but because the Toron pivot pulls compute-adjacent
primitives ahead of OLTP-shaped access surfaces (vectors,
triggers, webhooks).

## Forward sequence (council-converged)

Decided across multiple iterations between Claude, Codex, and
Gemini. Steps 1–20 are promoted and shipped (in `plans/adr/`
with implementation merged). ADR 062 closes the row-group
sizing question opened by ADR 061; ADR 063 closes the
predicate-fetch-narrowing follow-up that ADR 062 made
load-bearing; ADR 064 closes ADR 060's nullable-projection
deferral. The forward shape continues at #21.

1. **`StateDigest` substrate** — ADR 046 promoted.
2. **Row-level tombstones on append-only** — ADR 047 promoted.
3. **Per-table retention policy** — ADR 048 promoted.
4. **Algebraic topology gate** — closed 2026-04-29 with
   Outcome B (opaque `NodeId`); see brainstorm 015 closure.
   Splits to brainstorm 018 (FROST-WAL authority) and
   brainstorm 019 (Sybil-resistance threat model), both
   off the active queue.
5. **Content-hash identity (`AppendOnlyContentHash`)** —
   ADR 049 promoted.
6. **PK-keyed row-delete via supersession** — ADR 050
   promoted.
7. **Logical / physical descriptor fingerprint split** —
   ADR 051 promoted.
8. **Secondary indexes on non-clustering columns** —
   ADR 052 promoted.
9. **Wire-side Arrow IPC body compression** — ADR 053
   promoted.
10. **CDC subscription substrate** — ADR 054 promoted.
    Substrate-only; triggers, PBC filter pushdown,
    webhooks, and continuous historical-plus-live queries
    are explicit follow-up ADRs.
11. **LSM-style leveled compaction substrate** — ADR 055
    promoted. Substrate-only; size-tiered fallback,
    subcompaction, bloom filters, cross-replica
    deduplication, manual-trigger RPC, L4+ extension,
    and direct already-tiered compaction are explicit
    follow-up ADRs (each consumes the substrate).

12. **Row-group fetch and layered page directory** —
    ADR 056 promoted.
    Outer layer of the storage hierarchy: Arrow IPC
    RecordBatch row groups + per-row-group min/max/null-
    count stats + `RecordPageDirectory` in IPC custom
    metadata; three-phase fetch planner with Phase B/C
    survival-mask handoff; asymmetric AND/OR predicate
    classifier; `L2FetchStrategy::RowGroup` four-leg
    dispatch; ADR 052 secondary-index integration; ADR
    055 LSM compaction integration; 27 architectural
    lockdown guards. Phase 10 bench evidence accepted
    the empirical writer-cost gate and opens ADR 057.
    See *Where we are*.

13. **Per-buffer pages + encoding cascades inside
    row-group buffers** — ADR 057 promoted.
    Council-converged and implemented. Inner layer of the storage
    hierarchy. Closed 8-variant cascade catalog
    (`Plain`, `Lz4Frame`, `BitPacked`, `Rle`,
    `Dictionary`, `Delta`, `Fsst`, `Alp`) with
    one-level composition via `EncodingLeafCascade`;
    `EncodingDirectory` in IPC custom_metadata under
    key `storage.encoding_directory`, sibling to
    ADR 056's row-group directory; compact directory
    shape with cascade dedup and per-buffer
    `page_end_offsets` prefix array (no per-page
    serialized rows); `EncodingPageDescriptor` is transient
    planner-only; buffer-relative byte offsets;
    page emission required for `BitPacked`, `Rle`,
    `Dictionary`; Phase D materialization step internal
    to `L2FetchStrategy::RowGroup(plan)` leg; custom
    partial-buffer materializer in `src/storage/encoding/`;
    ADR 052 secondary-index integration via SIMD-page
    fast path; ADR 055 LSM compaction recomputes
    cascades deterministically; node-local
    `EncodingWriterConfig` activation (no
    `RecordBodyCompressionPolicy::EncodingCascade` descriptor
    variant); 5-code `EncodingRejectCode` taxonomy; ~30
    architectural lockdown guards. Decode-only V1;
    compute-over-encoded-data deferred to ADR 058.
    Phase 10 bench evidence accepted: per-cascade compression
    ratios on int / float / low-card UTF-8 / mixed,
    point-read latency reduction via SIMD-page fast
    path, full-buffer materialization cost vs. ADR 056,
    writer-cost defended by compression and read-side
    wins, cross-replica determinism, mixed baseline /
    Encoding slab read throughput, secondary-index page
    fast path, and CSB bypass evidence. This opens ADR
    058. See *Where we are*.

14. **Compute over encoded data (COCD)** — ADR 058
    promoted after Phase 8 substrate bench evidence.
    Council-converged across six iterations, implemented,
    and now enabled by default for ADR 059's narrowed
    native-kernel set.
    Innermost layer of the storage hierarchy. Closed
    6-variant predicate evaluator catalog (`BitPacked`,
    `RlePlain`, `RleBitPacked`, `DictionaryPlain`,
    `DictionaryBitPacked`, `Delta`) and closed 8-kernel
    aggregate catalog (`count_star`, `count_non_null`,
    `min`, `max`, `signed_sum`, `unsigned_sum`,
    `signed_avg`, `unsigned_avg`); no new public PBC
    opcodes (Toron contract byte-identical); node-local
    `EncodingComputeConfig` activation (no descriptor
    field, no `NodeCapability` bit); Exact / Prefilter /
    DenseFallback predicate plan modes; `EncodedRowMask`
    tagged enum (`All` / `None` / `Bitmap` / `Runs`);
    page-grain dispatch default for page-emitting
    cascades; dual economic gates (0.25 leaf-level
    selectivity + 0.10 Prefilter rejection); aggregate
    folding ungated by predicate selectivity and runs
    after ADR 040 CSB direct-answer ordering; 5-code
    reject taxonomy; ADR 052 secondary-index confirm-
    step; ADR 057 directory shape unchanged; ~27
    architectural lockdown guards. `Fsst` / `Alp`
    encoded evaluation deferred. Two aggregate-deferral
    classes: dense-supported but COCD-deferred (Utf8 /
    Binary `min`/`max`, `APPROX_DISTINCT`,
    `QUANTILE_KLL` — fall back to dense AGG, no ADR 040
    surface change required) vs. not-yet-in-ADR-040
    (exact `count_distinct`, float / decimal aggregates,
    time/timestamp sum/avg, GROUP BY — each requires an
    ADR 040 expansion first). Phase 8 substrate bench
    artifact landed at
    `plans/impl/058_compute_over_encoded_data_bench_results.md`;
    native per-cascade kernels and the >= 2x CPU
    promote-to-default gate are now ADR 059 follow-up
    scope at
    `plans/impl/059_native_cocd_kernels_bench_results.md`.
    See *Where we are*.

15. **Native per-cascade COCD kernels** — ADR 059
    council-converged, implemented, and promoted to shipped.
    Closes ADR 058's promote-to-default follow-up. Full
    Phase 10 bench evidence at
    `plans/impl/059_native_cocd_kernels_bench_results.md`
    clears the narrowed default-set promotion gate. Replaces the
    substrate decode-then-scan body with native cascade-
    aware specialization for `BitPacked` (default-routed
    byte-aligned `UInt8` fast path plus non-promoted
    legal sublanes behind fallback eligibility), `Rle`
    (byte-run streaming over existing ADR 057 format,
    omitted from the default predicate allow-list after
    Phase 10 miss), `Dictionary` (WDD1/WDB2 match-set +
    index walk; low-cardinality default route; higher-
    cardinality fallback before kernel entry), and `Delta`
    (WDL1 prefix-sum walk with monotonic-shape short-
    circuits, default-routed for predicates). Native
    aggregate folds landed and are oracle-checked, with
    default aggregate routing narrowed by cascade-grain
    eligibility to count plus unsigned value aggregates
    for byte-aligned BitPacked `UInt8` and plain low-
    cardinality Dictionary. Closed-catalog discipline
    applies recursively. Two-step dispatch discipline (pre-
    kernel eligibility may fall back to oracle; in-
    kernel reject is fail-closed); decode-scalar oracle
    stays in production as test oracle + pre-kernel
    fallback + kill-switch target. Specialized
    `EncodedRowMask` algebra without enum changes;
    central validity composition in
    `kernels/validity.rs`. Byte-identical mask +
    aggregate-partial invariant under `cargo test`. SIMD
    strategy: stable rustc + auto-vectorization V1;
    small SIMD helper crate first escalation, target-
    feature `std::arch` second; no nightly, no Cargo
    feature gate, no operator-visible knob. Default
    evaluator set encoded in pre-kernel eligibility
    (cascade grain) + existing
    `enabled_predicate_evaluators` allow-list in
    `EncodingComputeConfig` (predicate-kind grain);
    cascade-grain aggregate exclusion flows through
    eligibility because `enabled_aggregate_kernels` is
    keyed by aggregate kind, not cascade. No per-
    cascade flag.
    Promotion outcome: `EncodingComputeConfig::default()`
    now sets `enabled = true`; the default predicate
    allow-list contains `BitPacked`, `DictionaryPlain`,
    `DictionaryBitPacked`, and `Delta`, while `RlePlain`
    and `RleBitPacked` remain opt-in. Dense fallback
    overhead stays within the ≤ 5% gate across 8K / 65K /
    1M tiers. No new public PBC opcodes, no new
    evaluator kinds, no descriptor field, no
    `NodeCapability` bit, no Cargo feature, no envelope
    `format_version` bump, no ADR 057 directory shape
    extension. RLE value-run format work remains a
    separate follow-up candidate. See *Where we are*.

16. **Sparse projection fetch narrowing** —
    ADR 060 council-converged across Claude /
    Codex / Gemini, implemented, and promoted to shipped.
    Composes the ADR 058 / 059 exact-COCD survival
    mask with ADR 057's `BufferEncoding` page
    metadata to fetch only the encoded pages whose
    row spans intersect surviving rows, generalizing
    ADR 052's secondary-index point-read fast path
    from named point rows to mask survivors under
    clustered selectivity. Internal reader
    dispatch — no new `L2FetchStrategy` variant, no
    wire / descriptor / PBC / capability surface,
    no new config field, no new metric label, no
    ADR 057 directory shape change. V1 default
    sparse projection engagement set: `BitPacked`,
    `Dictionary { inner: Plain | BitPacked }`.
    RLE is correctness-eligible per codec
    verification at
    `src/storage/encoding/types.rs:185` +
    `src/storage/encoding/page.rs:21`, but Phase
    7 settles default routing as off; value-run
    RLE remains a follow-up ADR. Per-row-group
    uniform decision with
    cross-column page-geometry agreement; mixed
    sparse / dense projected columns within one
    materialization pass deferred. V1 engages only
    for exact-COCD masks
    (`decoded_predicate_batches` empty); dense
    predicate-zip with sparse projection deferred.
    Fixed-width + non-null projection only;
    nullable + variable-width route to dense.
    Two-module split:
    `src/storage/encoding/page_materialize.rs`
    (passive decode + assembly helpers, shared
    with ADR 052 named-row path) +
    `src/storage/encoding/sparse_projection_fetch.rs`
    (active planner + decision + economic logic
    with internal `RowToPageMap` /
    `ActivePageSet` / `SparseProjectionFetchPlan`
    / `SparseProjectionFetchDecline` types).
    Strict plan-time sparse_bytes < dense_bytes
    comparison over selected plaintext projection
    bytes, capped by existing
    `max_simd_pages_per_record` /
    `max_ranges_per_record` / selected byte
    fraction plus an internal
    `MAX_SPARSE_RANGES_PER_BUFFER` helper-local
    `const` for per-buffer fragmentation. Internal
    decline reasons stay inside the planner
    module — never become metric labels, fallback
    variants, or reject codes. ADR 057's existing
    `EncodingRejectCode` variants cover invariant
    failures after sparse fetch begins. Shipped
    bench gate: clustered 1% / 5% masks fetch ≥ 3×
    fewer selected plaintext projection bytes,
    wall-clock ≥ 1.5× faster after multi-range RTT
    overhead, dense-fallback overhead ≤ 5%, byte-
    identical projected-batch invariant, cross-
    replica determinism, 1M tier mandatory. Phase 7
    records ciphertext-byte ratios as ADR 044
    chunk-granularity evidence, not an ADR 060 V1
    gate. Bench artifact at
    `plans/impl/060_sparse_projection_fetch_narrowing_bench_results.md`
    accepted RLE default-off and clears the revised
    promotion gate. See *Where we are*.

17. **Page-aligned authenticated chunks with macro-block
    read coalescing** —
    ADR 061 council-converged across Claude / Codex /
    Gemini after five council iterations, implemented,
    and promoted to shipped. Closes ADR 060's recorded-not-gated
    ciphertext follow-up. **Two-layer decoupling**: AEAD
    authentication boundary and physical network read unit
    are independent layers optimized for different forces.
    Layer 1 — small AEAD chunks from a closed set
    `{4, 8, 16, 32, 64} KiB`, default 16 KiB for slabs with
    an `EncodingDirectory` (V1 promoted floor; 4 KiB and
    8 KiB remain in the closed set for diagnostic rows but
    are not selectable by the V1 selector). Layer
    2 — ADR 044's existing `TieredMultiRangeReadPlan`
    coalescer merges contiguous AEAD chunks into multi-MiB
    `get_range` calls; warehouse-scale dense paths produce
    p50 ≥ 1 MiB GETs. **Buffer starts** align to chunk
    boundaries via opaque pad bytes in IPC body slack
    *between* adjacent buffers (no intra-buffer pad —
    ADR 057's contiguous `page_end_offsets` is a prefix-end
    array and cannot represent intra-buffer gaps; the
    directory shape is unchanged); pad cost ≤ 5%; pad bytes
    AEAD-authenticated, never enter the page-decode path.
    No new on-disk shape, no new directory, no new metadata
    surface, no new wire / descriptor / PBC / capability /
    `ObjectStore` trait change; ADR 022 §3's existing
    `chunk_size_bytes` field carries the per-slab value
    (Open Question 4 explicitly anticipated this). At 16 KiB
    chunks ADR 060's ciphertext floor drops 4× on clustered
    fixtures; ADR 044 column-fetch composition stays
    comfortably within its 67% selected-fraction gate
    (per-record fraction ≈ 2.3% on a 1-of-50 projection
    with buffer-start alignment). Phase 0 no-directory
    feasibility check + Phase 5 four-sub-property macro-
    block coalescing test are the two load-bearing gates;
    if Phase 0 fails, implementation stops and the council
    produces a revised ADR/impl pair as a separate decision
    (no silent reshape). Variable buffer-aligned chunks via
    encrypted `AuthenticatedChunkDirectory` deferred to V2
    with explicit promotion gate (post-V1 evidence that
    ADR 044 over-fetch becomes a measurable bottleneck on
    operational query workloads). See *Where we are*.

18. **Row-group sizing profile** —
    ADR 062 is implemented and promoted to shipped with
    bench evidence at
    `plans/impl/062_row_group_sizing_profile_bench_results.md`.
    Amends ADR 056 with a closed two-variant
    `RowGroupSizingProfile { PointRead, Scan }` catalog
    and deployment-wide profile selection. `PointRead`
    preserves ADR 056 V1 defaults byte-identically (4K /
    8K / 8K-rows / 512 KiB-bytes); tuned for OLTP-shape
    access and ADR 052 secondary-index point reads.
    `Scan` lifts sizing for analytical scan deployments
    (65K / 131K / 65K-rows / 1 MiB-bytes; max = 2 ×
    target matches ADR 056 slack-headroom convention;
    `min_paged_record_rows = target` enables one-entry
    directories). Tuned for ADR 061 macro-block coalescer
    economics on warehouse-scale records. Operator config
    exposes `profile` plus `PointRead`-only numeric
    overrides as a non-breaking-migration escape hatch;
    `Scan` + numeric overrides emits config-load warning
    and ignores numeric values. One-entry `Scan`
    directory accepted (preserves ADR 057 / 060 leverage
    on boundary records); `PointRead` retains today's
    `batches.len() <= 1` single-batch fallback unchanged.
    Per-record geometry-keyed selector deferred to V2
    with explicit promotion gate. `enabled = false` stays
    default (sizing, not rollout). No directory / wire /
    descriptor / PBC / capability / `ObjectStore` trait /
    fallback / reject / catalog variant change. Naming
    avoids ADR 030 deployment-vocabulary mismatch (ADR
    030 describes hardware support envelopes; ADR 062
    describes access-pattern shape). See *Where we are*.

19. **Page-level predicate stats + predicate-fetch
    narrowing** — ADR 063 implemented and promoted to
    shipped with bench evidence at
    `plans/impl/063_page_level_predicate_stats_bench_results.md`.
    Amends ADR 057 with an
    additive `BufferEncoding.page_stats:
    Vec<EncodingPageStats>` field and an
    `ENCODING_DIRECTORY_FORMAT_VERSION` bump (no compat
    decoder per `no-premature-backward-compat.md`). Adds an
    internal predicate-page fetch planner inside
    `L2FetchStrategy::RowGroup`'s Phase B that narrows
    encoded predicate fetch to surviving pages before
    survival-mask construction. Three-state classification
    `Disjoint` / `ExactMatch` / `Maybe` with null-position-
    conservative `ExactMatch` correctness (value comparisons
    require `null_count == 0`; null predicates use
    `null_count` directly). Cascade eligibility = intersection
    of ADR 059's default predicate allow-list AND
    `EncodingCascade::emits_pages_in_v1()` — V1 set is
    `BitPacked`, `DictionaryPlain`, `DictionaryBitPacked`;
    `Delta` is allow-listed but not page-emitting in V1, RLE
    byte-run is page-emitting but not allow-listed, both carry
    empty `page_stats`. Type catalog = fixed-width subset of
    ADR 056's `default_stats_supported_types` (Int8-64 /
    UInt8-64 / Timestamp(ms/us/ns) / Date32/64 / Boolean);
    UTF-8 / Binary / LargeUtf8 / LargeBinary remain ADR 056
    row-group-stats-only. Shared `CanonicalScalar` via
    `src/storage/stats.rs`; row-group stat byte layout
    unchanged. Predicate-page planner at
    `src/storage/encoding/predicate_page_fetch.rs` (sibling
    to ADR 060's `sparse_projection_fetch.rs`); production
    read path consults it before dense predicate fetch;
    engaged narrowed plan returns mask-only `PhaseBOutcome`,
    preserving ADR 060 sparse projection downstream. Phase 0
    size-model + profile-emission gate is the sole V1 budget
    discipline; shipped quick evidence keeps both `PointRead`
    and `Scan` in scope (`Scan` clustered 5% 63.56x,
    `PointRead` clustered 5% 7.95x) and keeps combined
    metadata inside ADR 044's 64 KiB initial-probe envelope.
    No runtime metadata-budget fallback in V1 (Wendelon V1
    house style: gate-driven, not runtime-fallback-driven).
    Closed surface: no new IPC custom_metadata key, no new
    wire / descriptor / PBC / capability / `ObjectStore`
    trait change; no new fallback / reject / catalog variant.
    `enabled = false` writer default carried forward. See
    *Where we are*.

20. **Nullable sparse projection** — ADR 064 implemented and
    promoted to shipped with bench evidence at
    `plans/impl/064_nullable_sparse_projection_bench_results.md`.
    Council-converged across Claude / Codex / Gemini after
    six iterations; Phase 0 fork-resolution audit confirmed
    raw byte-addressable validity is a non-no-op V1
    engagement path against current writer output. Lifts
    ADR 060's
    `SparseProjectionFetchDecline::NullableProjectedColumn`
    decline for fixed-width projection columns by fetching
    and re-basing Arrow validity bitmap slices alongside
    selected value pages, and by extending the page
    materializer to assemble nullable Arrow arrays without
    using dense row-group buffers. **V1 is raw byte-
    addressable validity only**: validity buffers with
    `EncodingDirectory` entries decline to dense behind a
    private planner reason; encoded-validity decode is a
    follow-up ADR. Phase 0 is a fork-resolution gate, not an
    implementation-time scope expansion: raw-only proceeds;
    mixed raw/encoded declines on encoded; encoded-only
    revises the ADR pre-code. **V1 type and cascade catalog
    pinned to ADR 060's current materializer** (not ADR 063's
    page-stat catalog). Eligibility joint across value and
    validity buffers; row groups with `column_null_counts ==
    0` for nullable Arrow fields stay on the existing
    all-valid sparse path. Page-local validity rebase
    (`rebase_validity_bitmap` helper in
    `page_materialize.rs`) handles byte-aligned and non-byte-
    aligned page starts. Mask-first ordering preserved
    (`None` / `All` unchanged). ADR 065 later lifts the
    dense-predicate-batch decline for the scalar dense-decode
    branch while leaving ADR 064's nullable projection catalog
    unchanged. **Composability
    with ADR 063 multiplicative**: ADR 063 narrows predicate
    bytes, ADR 064 narrows projection bytes; bench artifact
    records ExactMatch-driven and Maybe-driven projection
    pages as separate rows so the asymmetry (predicate bytes
    may be zero while ADR 064 still fetches selected
    projection value + validity bytes) is visible.
    All-null page-skip optimization deferred to V2 with
    explicit architectural guard. Closed surface: no new
    `L2FetchStrategy` / `EncodedRowMask` / fallback / reject
    / catalog variant; no new IPC custom_metadata key, no
    `ENCODING_DIRECTORY_FORMAT_VERSION` bump, no new wire /
    descriptor / PBC / capability / `ObjectStore` trait
    change; no Cargo feature, nightly rustc dependency,
    public PBC opcode, metric label, or operator-visible
    nullable engagement knob. ADR 022 / ADR 056 / ADR 057
    directory shapes unchanged. ADR 060 module location
    unchanged (extended in place). **Headline bench result**:
    clustered 5% nullable sparse projection passed across
    BitPacked and Dictionary with 5.36×–19.98× plaintext-byte
    reduction (value + validity combined) and 1.50×–5.45×
    wall-clock speedup, substantially exceeding the 1.5×
    plaintext-byte / 1.2× wall-clock promotion gates and the
    3× headline diagnostic. The bench artifact also records
    ADR 063 ExactMatch / Maybe composability rows, encoded-
    validity decline, no-null / all-null controls, non-byte
    page boundary coverage, variable-width decline,
    equality / null-count checks, and cross-replica
    determinism. See
    `plans/adr/064_nullable_sparse_projection.md`,
    `plans/impl/064_nullable_sparse_projection.md`, and
    `plans/impl/064_nullable_sparse_projection_bench_results.md`.

21. **Dense predicate-zip with sparse projection** — ADR 065
    implemented and promoted to shipped with bench evidence
    at
    `plans/impl/065_dense_predicate_zip_with_sparse_projection_bench_results.md`.
    Council-converged across Claude / Codex / Gemini after
    six iterations. Phase 0 read-only audit completed against
    `src/storage/manager.rs`,
    `src/storage/encoding/sparse_projection_fetch.rs`, and
    `src/storage/row_group.rs`; engagement surface confirmed
    non-trivial. Lifts ADR 060 / 063 / 064's mask-only
    Phase B contract for sparse projection: when predicate
    evaluation populates `predicate_projection_by_group` *and*
    produces an exact survival mask, sparse projection
    engages for the remaining projection requirement.
    **Manager-side three-case dispatch** on the derived
    `predicate_output_overlap` set
    (`predicate_fetched_requirement.predicate_columns ∩
    requirement.output_columns`): Case A (mask-only;
    `predicate_projection_by_group` empty; ADR 060 / 064
    path; also covers all encoding-compute paths since
    they emit mask-only `PhaseBOutcome`), Case B
    (predicate/output-overlap zip; populated +
    `predicate_output_overlap` non-empty; page-local slice
    + filter + zip), Case C (no-overlap dense-fallback;
    populated + `predicate_output_overlap` empty; skip
    zip, concatenate sparse). `predicate_fetched_requirement`
    retains its existing role of excluding already-fetched
    columns from projection fetch. The sparse projection planner
    stays metadata-only — it consumes survival masks and
    projection metadata, not Arrow `RecordBatch`es; manager
    owns the decoded predicate batches. New helpers:
    `filtered_predicate_page_batch` in
    `page_materialize.rs` (page-grain row-span slicing +
    page-local mask filtering),
    `zip_predicate_and_projection_columns` in `row_group.rs`
    (semantic adjacent to the existing dense-zip helper;
    delegates during ADR 065 lifetime),
    `SparseProjectionPageBatch` (private per-page tagged
    batch). Splits `materialize_sparse_projection_pages`
    into a per-page lower helper + the existing concat
    wrapper. **Decisive removal** of
    `SparseProjectionFetchInput.dense_predicate_batches_present`
    field (line 91) and retirement of active use of
    `SparseProjectionFetchDecline::DensePredicateBatchPresent`
    variant (line 65). Page-local zip applies the *same*
    page-local survival-mask slice to both sides; ADR 064's
    `rebase_validity_bitmap` reused for nullable sparse-
    projected columns. Variable-width allowed on dense
    predicate side (already decoded); sparse projection
    side stays ADR 064 fixed-width nullable. **Predicate
    bytes are sunk** in the projection economic gate:
    `sparse_projection_plaintext_bytes <
    dense_projection_plaintext_bytes` unchanged from ADR
    060 / 064. Bench artifact records total query bytes
    (predicate decode + projection) on a separate axis.
    **Three-axis promotion gate passed**: clustered 5% rows
    clear ≥ 1.5× remaining-projection plaintext bytes, ≥
    1.25× total-query plaintext bytes, and ≥ 1.2× wall-clock
    improvement. Synthetic always-dense-fallback bench fixture
    remains the regression-stable baseline across future
    native-evaluator widening (FSST / ALP / RLE-value-run /
    float).
    **Architectural inextricability**: mask must be exact
    before zip engages; predicate and projection filtered
    with same page-local mask; dense predicate fetch is not
    sparse predicate fetch; sparse zip output bypasses
    `apply_phase_b_masks_to_projected_row_groups` (already
    page-filtered); page-local assembly invariant (no filler
    reconstruction). V1 does not add planner-level private
    decline variants for Maybe-mask survivors or predicate-
    batch page-span / row-count invariants; manager-side
    checks fail closed via existing storage / Arrow errors
    after sparse fetch begins.
    **Naming reconciliation per
    `plans/conventions/no-planning-phase-names.md`**: ADR
    065 uses semantic names throughout; production already
    has semantic locals
    (`predicate_fetched_requirement`,
    `predicate_projection_by_group`, etc.); residual phase-
    named identifiers (`PhaseBOutcome`,
    `apply_phase_b_masks_to_projected_row_groups`,
    `zip_phase_b_and_phase_c_projected_row_group`,
    `phase_c_requirement` parameter) slated for the broader
    convention sweep flagged by ADR 060 — ADR 065 doesn't
    bundle. **Closed surface**: no new on-disk shape, no new
    `L2FetchStrategy` / `EncodedRowMask` / fallback /
    reject / catalog variant; no IPC custom_metadata key,
    no `ENCODING_DIRECTORY_FORMAT_VERSION` bump, no wire /
    descriptor / PBC / capability / `ObjectStore` trait
    change; no Cargo feature, no metric label, no operator
    knob. **V1 deferred** with explicit V2 paths: sparse
    predicate-column fetch, page-sliced PBC evaluation,
    variable-width sparse projection, native FSST / ALP /
    RLE-value-run / float evaluators (each its own ADR;
    ADR 065 is the bridge that survives their absence),
    re-evaluating Maybe survivors, allocation pooling for
    per-page Arrow slicing churn. See
    `plans/adr/065_dense_predicate_zip_with_sparse_projection.md`
    and
    `plans/impl/065_dense_predicate_zip_with_sparse_projection.md`.

22. **Capacity-tier compression + serverless / erasure-
    coded follow-ups (rest of the old #14 bundle).**
    Independent; each ships its own contract surface:
    - **zstd capacity-tier codec + `CompressionLevel`
      newtype.** Codec-catalog extension; ADR 053 / 043
      wire vs. body composition; new
      `CompressionLevel(u8)` newtype-unit. Composes
      orthogonally with the row-group / per-buffer-page
      hierarchy.
    - **Serverless PBC at L2** *(brainstorm 010.)*
      Lambda-like remote PBC executor; consumes the same
      chunk-aligned authenticated range-read substrate
      ADR 044 / 056 surfaces; composes with ADR 061's
      per-slab `chunk_size_bytes` for sub-record decrypt
      grain.
    - **L1 inline erasure coding** *(brainstorm 003.)*
      Reed-Solomon / Cauchy across L1 NVMe replicas;
      composes with the LSM-tier sweeper boundary
      (ADR 055 §"No StorageTier on the substrate").
    *Each is its own ADR; ordering opportunistic and
    independent of #13 / #14 sequencing.*

23. **CDC follow-up ADRs (consume ADR 054 substrate)** —
    deferred behind the storage hierarchy in the Toron-
    pivot stretch. Independent; each ships its own
    contract surface:
    - **PBC filter pushdown on subscriptions.** Trailing-
      section bytecode on `CDC_SUBSCRIBE_REQ`; new
      `CDC_WATERMARK` MsgType for low-selectivity streams
      (every emitted event advances the cursor at
      substrate, but watermarks become load-bearing once
      filters can drop events without advancing); filter
      reject taxonomy; budget accounting; no-match
      checkpoint advancement. Reuses ADR 036 PBC validator
      family.
    - **Continuous historical-plus-live queries.** Either
      composes externally (L2 read at HLC=T plus CDC
      subscription with cursor matching T) or introduces a
      dedicated streaming `QUERY_REQ` / `QUERY_RESP` shape.
    - **Post-commit PBC triggers.** New `TriggerCatalog`
      (mirroring ADR 052 `SecondaryIndexCatalog` shape),
      `SchemaObjectKind::Trigger`, trigger worker that runs
      PBC over CDC events and appends to descriptor-bound
      append-only sinks; sink-write idempotency; indirect
      cycle detection; pause-resume operator surface.
    - **Synchronous PBC triggers at admission.**
      Admission-time PBC hook with its own CPU budget and
      failure-domain rules; deterministic PBC requirement.
    - **Webhook worker + `__webhook_events__` system table
      (or equivalent).** Internal HTTP retry / DLQ
      semantics; webhook worker is a CDC consumer.
    - **Server-side durable cursor commit (consumer-group
      semantics).** New `CDC_CURSOR_COMMIT_REQ` / `_RESP`
      MsgType pair; substrate cursor unchanged.
    - **Exactly-once delivery contract.** Server-side
      cursor commit + per-frame ack RPC.
    - **Cross-replica cursor handoff (zero-reconnect
      failover).** Server-side durable cursor + replica
      synchronization.
    - **Multi-table / tenant-firehose subscription scope.**
      Cursor extends to carry `TableObjectId` explicitly;
      V1's two-field scope is forward-compatible.
    - **Before-image CDC payloads.** Adds `before_*`
      columns to the CDC event row schema; substrate
      cursor / digest domain unchanged.
    - **Event-history digest domain
      (`digest.cdc.event.v1`).** Adds prefix-membership
      proof when a consumer (e.g., downstream secondary-
      index replicator) needs it independent of visible
      state.
    - **Consumer-friendly higher-level `CdcEventKind`
      view.** Layered on top of substrate version-entries;
      derives insert / update / delete / resurrection
      semantics from row-axis synthetic columns.
    - **Per-table CDC opt-in / opt-out** (and per-table
      retention extension beyond source-table retention).
    *Each is its own ADR; ordering opportunistic.*

24. **Vector embeddings + vector-form identity / locality
    PBC opcodes** *(brainstorm 002.)* ANN as a first-class
    index family, composing on the secondary-index
    substrate (#8) and LSM (#11). **Set aside in the
    Toron-pivot stretch** — OLAP focus pulls
    compute-adjacent primitives ahead. Reopen after the
    storage hierarchy (ADR 056 / 057 / 058 / 059 / 060 /
    061 / 062 / 063 / 064) is in production use and Toron
    compute-layer requirements surface concrete vector
    workload signals. *Large, multi-ADR.*

25. **Refinement plumbing (opportunistic, no ordering
    constraint).** AGG follow-ups (float canonical,
    Decimal SUM/AVG, GROUP BY *(medium-large)*,
    string/binary quantiles, exact COUNT(DISTINCT),
    `COUNT(DISTINCT __row_key_id__)` shortcut, CSB
    visibility-metadata consolidation). Hilbert locality
    variant. Append-only `SingleRow` low-latency path.
    `IntraBatchIndex` widening to u64. Provider-native
    multi-range API in `ObjectStore`.
    `RecordBufferDirectory` cache.

## What not to do next

- **Don't bundle the CDC follow-up ADRs into one
  mega-ADR.** Each consumer of the ADR 054 substrate
  (filters, triggers, webhooks, continuous queries,
  durable cursor commit, multi-table firehose, before-
  images, event-history digest) has its own contract
  surface; bundling defers real decisions behind a larger
  one. Land them independently against the substrate.
- **Don't introduce a separate compaction floor.** ADR
  055 §"Architectural inextricability" pins the elision
  floor as identical to ADR 048's retention floor. A
  parallel parameter would diverge under operational
  drift and create two reachability contracts.
  Operator-tunable retention is the lever; "compact
  more aggressively than retention permits" is not a
  shape V1 supports.
- **Don't add an LSM `relocated_reads` shim.** ADR 055
  §7 commits to manifest-snapshot pins as the sole MVCC
  mechanism; cached `RecordLocation` references retire
  with the old sidecars (rebuilt out-of-place during
  compaction). Reintroducing per-row relocation rows
  for LSM would add chain-management complexity without
  improving CDC catchup or iterator safety.
- **Don't replace the cold-rebuild visible-domain audit
  with a streaming-only check.** ADR 055 §6 / §9 commit
  to localized cold rebuild over input/output slab
  views as the V1 install gate — equivalent O(touched
  bytes) cost, strictly stronger bug-catching (catches
  materialization, not just elision). Streaming
  `visible_contrib(elision_set) == identity` is a debug
  / bench tool, not the gate.
- **Don't widen `CompactionMergeKey` with locality /
  tenant / table prefix.** Compaction is per-locality
  by construction; table ownership is read from row-
  axis synthetic columns at retention-floor lookup, not
  from the merge key. The narrow `(HLC, RowKeyId,
  RecordRowIndex)` matches the data-ring slab seek key
  exactly.
- **Don't put `StorageTier` on the LSM substrate.**
  Tier moves are an orthogonal sweeper concern. Coupling
  them would force the substrate to coordinate with the
  sweeper without buying capability.
- **Don't bundle the LSM follow-up ADRs.** Size-tiered
  picker, subcompaction, bloom filters, cross-replica
  deduplication, manual-trigger RPC, L4+ extension, and
  direct already-tiered compaction each consume the ADR
  055 substrate independently. Bundling defers real
  decisions behind a larger one.
- **Don't promote stabilization watch items into roadmap
  items.** Stabilization watch — observability metrics,
  architectural-test guards, post-merge dashboards — lives
  in runbooks and tests, not as a roadmap feature.
  Decision-enabling measurement (e.g., the CDC watermark
  hold-time bench at multiple heartbeat cadences) belongs
  inside the owning ADR.
- **Don't re-open the topology gate without citing the
  rework budget recorded in brainstorm 015's closure
  status.** Speculative threat models, aesthetic
  preferences, and the desire to reuse curve machinery
  are not sufficient grounds. A re-open requires concrete
  verifier requirements or operational evidence that
  opaque-handle plus membership-bound signing forces more
  rework than a `NodeId`-shape migration.
- **Don't add a `CdcEventKind` enum to the substrate.**
  Consumer-friendly insert / update / delete / resurrection
  semantics layer on top in a future higher-level view ADR
  (#12 sub-bullet). The substrate stays storage-axis-
  aligned with the version-entry grain.
- **Don't expose `LocalityId` on the CDC cursor.** The
  identity-axis cursor is the load-bearing decision; the
  safe-emission watermark plus heartbeat contract is what
  makes global cursor monotonicity work without it.
- **Don't bundle ADR 056 / 057 / 058 into one mega-ADR.**
  The storage hierarchy is a layered substrate by
  design; each layer is a substantial decision in its
  own right: ADR 056 is row groups + stats, ADR 057 is
  per-buffer pages + encoding cascades + custom partial-
  buffer materializer, and ADR 058 is compute over
  encoded data. Bundling them defers each layer's review
  behind the next, weakens the lockdown story (ADR 056's
  scoped guards depend on a clean ADR 057 boundary), and
  loses the Parquet / Vortex precedent of evolving inner
  layers across multiple specs. Each layer ships as its
  own ADR.
- **Don't reopen ADR 058 with public PBC opcodes
  (`EVAL_DICT`, `EVAL_RLE`, `EVAL_BITPACKED_RANGE`).**
  Brainstorm 011 §2 framed COCD as opcode extensions; the
  council convergence at iter 6 rejects this uniformly.
  ADR 036 §7 ("PBC capability negotiation becomes
  mandatory before new opcodes") and §11 ("targetability
  diagnostics belong on Toron expression nodes") together
  say: new public opcodes are a Toron contract concern,
  not a storage optimization concern. Toron does not need
  to know whether Wendelon evaluated against a dictionary
  or against a materialized array — the answer is byte-
  identical. The dispatch happens internally; the opcode
  does not. Reopening requires concrete Toron-side
  evidence that the dispatch decision needs to be
  controllable from the query session, plus full ADR 036
  capability-negotiation work.
- **Don't ship ADR 058 without aggregate folding in V1.**
  Codex iter-1 deferred aggregates and iter-2 retreated
  again, but the iter-3 reasoning carried: mask
  infrastructure is the load-bearing substrate, and the
  common exact AGG operations reuse the same encoded
  scans (`count_star` is mask popcount, `count_non_null`
  is mask plus validity, `min`/`max`/`sum`/`avg` walk the
  cascade once with the mask filtering). Splitting
  predicate and aggregate substrates would force a
  near-immediate ADR 040 follow-up that reopens the same
  mask and cascade correctness questions. Council
  convergence at iter 5 / iter 6: aggregates ship with the
  predicate substrate.
- **Don't ship ADR 058 with a single economic gate or no
  gate.** The iter-3 / iter-4 oscillation (blanket gate →
  no gate → Prefilter-only gate → dual gates) settled on
  dual gates because they handle distinct regression
  cases. The leaf-level selectivity gate at 0.25 defends
  against high-selectivity Exact regressions (encoded eval
  produces a mostly-true mask while the projection fetch
  still pulls most rows; dense materialize wins by reusing
  one fetch). The Prefilter rejection gate at 0.10 defends
  against unprofitable mixed-conjunction narrowing
  (encoded leaves must drop ≥ 10% of rows to amortize
  residual narrowing). Aggregate folding has neither gate
  — the mask is the input, folding is strictly cheaper
  than materializing then folding.
- **Don't open ADR 058 with descriptor-policy
  activation.** Same physical-deployment-knob argument
  ADR 057 §"Why node-local writer config" makes: COCD
  does not change logical rows, table semantics, PBC
  programs, or Toron contracts. A descriptor field would
  make a physical deployment knob look like logical
  schema policy. Deployment-wide `EncodingComputeConfig`
  is the council-converged shape.
- **Don't extend ADR 057's directory shape for COCD.**
  COCD reads existing `BufferEncoding` /
  `EncodingCascade` metadata; no new fields, no per-page
  prefix-sum side tables for Delta range, no page-level
  min/max/null stats. Those additions are follow-up ADR
  territory and require their own bench-driven
  motivation.
- **Don't reopen ADR 058's catalog inside ADR 059.**
  Native kernel work is a physical implementation pass
  over the ADR 058 catalog; it does not add
  `EncodingComputeEvaluatorKind` /
  `EncodedAggregateKind` / `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp` /
  `EncodedRowMask` variants. Closed-catalog discipline
  applies recursively to (cascade × kind) coverage —
  ADR 059 ships native specialization for every pair
  ADR 058 already admits, with zero new deferrals.
  Hedges like "where streaming adapter exists" or
  "high-impact subset" open the partial-coverage door
  the closed-catalog principle is designed to close;
  council iter 4 rejected both.
- **Don't change ADR 057's RLE byte-run format inside
  ADR 059.** The shipped RLE codec is byte-run
  `(byte_run_len: u32, byte: u8)`, not value-run. ADR
  059's RLE kernel streams the existing byte-run format
  into a stack buffer; it does not change ADR 057's
  encoded bytes. Phase 10 evidence shows byte-run
  cannot meet the 2× gate, so the default
  `enabled_predicate_evaluators` allow-list omits both
  `RlePlain` and `RleBitPacked`; a separate ADR considers
  value-run.
  Format-change-to-rescue-gate is not a shape ADR 059
  supports.
- **Don't ship ADR 059 with a `simd_strategy` /
  `simd_width` / `kernel_dispatch_mode` config knob, a
  Cargo feature gate, or nightly rustc.** Closed-
  dispatch principle (ADR 058 §"Architectural
  inextricability" point 3). Operators don't tune SIMD
  width. V1 uses stable rustc + auto-vectorization;
  bench-driven escalation adopts a small SIMD helper
  crate (e.g., `wide`) first, target-feature
  `std::arch` second — both scoped internally to one
  helper module with architectural-test-pinned import
  boundary.
- **Don't add a per-cascade `enabled_*` flag to
  `EncodingComputeConfig`.** Default-set membership
  reuses the existing `enabled_predicate_evaluators`
  allow-list (predicate-kind grain) plus the
  eligibility layer (cascade grain, including cascade-
  grain aggregate exclusion since
  `enabled_aggregate_kernels` is keyed by aggregate
  kind, not cascade).
  A new flag would split the planner's decision matrix
  and make cross-replica byte-equality harder to argue.
- **Don't retire ADR 058's decode-scalar oracle from
  production code during ADR 059.** It stays as the
  test oracle for byte-equality property tests, the
  pre-kernel fallback for legal-but-not-yet-native
  sublanes (variable-width Dictionary, FSST, ALP, ADR-
  058-deferred atom shapes), and the kill-switch target
  for operator-disabled evaluators.
- **Don't ship ADR 057 with `Constant` or
  `FrameOfReference` cascade variants.** Iter 2 of
  the council ADR 057 process surfaced these as
  candidate variants; iter 3 rejected them as
  redundant. `Constant` is `Rle { run_count: 1, ... }`
  — same encoded bytes, one variant fewer in the
  catalog. `FrameOfReference` is subsumed by
  `BitPacked { bits, base }` where `base` is the
  Frame-of-Reference offset; the variant split was a
  category mistake. The 8-cascade catalog
  (`Plain`, `Lz4Frame`, `BitPacked`, `Rle`,
  `Dictionary`, `Delta`, `Fsst`, `Alp`) is the
  council-converged set; new variants require a
  follow-up ADR.
- **Don't open ADR 057 with descriptor-policy
  activation (`RecordBodyCompressionPolicy::EncodingCascade`).**
  The activation surface was the load-bearing fork
  across iters 1–4 of the ADR 057 council process.
  Iter 4 converged on node-local `EncodingWriterConfig`
  for principled reasons: encoding cascades do not change logical
  rows, table semantics, PBC programs, or Toron
  contracts; a descriptor field would make a physical
  deployment choice look like logical schema policy;
  cross-replica byte-equality is already enforced
  via deployment-wide config; readers auto-detect
  via directory presence. Reopening this with a
  `RecordBodyCompressionPolicy::EncodingCascade` variant requires
  a re-vote with documented evidence that the
  descriptor framing buys capability the node-local
  shape doesn't.
- **Don't add a fifth `L2FetchStrategy` variant for
  sparse projection fetch.** ADR 060 is internal
  dispatch inside the existing `RowGroup` strategy
  leg. A new top-level variant would split the
  planner's decision matrix and create path-
  dependent physical plans on the same logical
  query. `RowGroupFetchPlan.surviving_groups`
  remains row-group grain; the fact that the
  projection-fetch field carries sparse byte ranges
  is internal planner state, not a strategy shape.
- **Don't extend ADR 057's directory for ADR 060.**
  Per-page row counts already exist in
  `BufferEncoding.page_value_count`
  (`src/storage/encoding/types.rs:185`, consumed at
  `src/storage/encoding/page.rs:21`). ADR 060
  reads it. Iter-2 council framing of "byte-run RLE
  has variable rows per page" was factually wrong;
  RLE row→page mapping is deterministic from
  directory metadata. Don't reopen with new
  per-page fields.
- **Don't make BitPacked / Dictionary promotion
  depend on RLE bench result.** ADR 060 keeps RLE
  in the V1 page-materializable eligibility set
  but bench-settles default routing as
  non-blocking: pass → RLE default-on; miss →
  RLE default-off via eligibility-layer cascade
  exclusion (matching ADR 059's narrowed-default-
  set pattern). Coupling BitPacked / Dictionary
  promotion to RLE bench evidence creates risk and
  blocks the cleaner wins. RLE outcome records
  pass / miss explicitly in the bench artifact.
- **Don't add operator-visible sparse config in
  ADR 060 V1.** Existing economic caps
  (`max_simd_pages_per_record` /
  `max_ranges_per_record` / selected byte
  fraction / selected ciphertext byte fraction)
  plus the internal helper-local
  `MAX_SPARSE_RANGES_PER_BUFFER` `const` are
  sufficient. `enable_sparse_projection_fetch` /
  `min_sparse_byte_fraction` / per-cascade allow-
  list flags are explicitly rejected for V1.
  Promotion of the per-buffer fragmentation budget
  to operator config requires a revised ADR
  decision with operational pressure evidence.
- **Don't ship ADR 060 with mixed sparse / dense
  projected columns within one row-group
  materialization pass.** Per-row-group uniform
  decision with cross-column page-geometry
  agreement keeps the byte-equality oracle tight.
  Per-column mixing is a follow-up if benches show
  wins on the table; council convergence locks
  uniform-per-row-group for V1.
- **Don't surface ADR 060's internal decline
  reasons via metric labels or fallback variants.**
  `AllRowsMask`, `MissingExactCocdMask`,
  `DensePredicateBatchPresent`,
  `PageGeometryMismatchAcrossColumns`,
  `ByteEstimateNotSparse`,
  `PerBufferFragmentationCapExceeded`, and the
  rest are plan-time routing facts that live
  entirely inside
  `src/storage/encoding/sparse_projection_fetch.rs`.
  They never become new `EncodingFallbackReason` /
  `RowGroupFallbackReason` variants, never become
  metric label values, and never escape via wire /
  descriptor / capability surface. The
  architectural-test boundary pin is the load-
  bearing guard.
- **Don't ship ADR 060 V1 with dense predicate-
  zip with sparse projection.** V1 engages only
  for exact-COCD masks where
  `decoded_predicate_batches` is empty. Page-
  sliced zipping of dense predicate batches with
  sparse projected batches has its own row-
  alignment correctness story without a clean
  byte-equality oracle. Reopening this for V1
  requires a re-vote with concrete dense-
  predicate-source evidence that page-sliced zip
  buys capability the V1-deferred form doesn't —
  follow-up ADR otherwise.
- **Don't chase ciphertext-byte reduction inside
  ADR 060 by reopening the ADR 044 chunk
  contract.** ADR 060 controls selected plaintext
  projection pages and decode work. The Phase 7
  artifact records that encrypted ciphertext bytes
  are bounded by ADR 044 chunk-aligned
  authenticated range reads; material ciphertext
  narrowing requires a dedicated page-aligned
  authenticated chunk-layout ADR.
- **Don't bundle ADR 060 with the encoded-fetch
  follow-up family.** Page-level min/max/null
  stats, sparse predicate-column fetch, dense
  predicate-zip with sparse projection, nullable
  sparse projection, variable-width sparse
  projection, FSST / ALP sparse paths, RLE
  value-run format work, and mixed
  named-row + mask-driven projection composition
  each consume the ADR 060 substrate
  independently. Each ships its own contract
  surface; bundling defers each decision behind
  the next.

## Convergence summary

```text
StateDigest substrate                            (ADR 046)
  → tombstones (validate algebra @ deletion)     (ADR 047)
  → retention (validate algebra @ compaction)    (ADR 048)
  → topology gate CLOSED — Outcome B (opaque NodeId)
  → content-hash identity                        (ADR 049)
  → PK-keyed row delete via supersession         (ADR 050)
  → descriptor fingerprint split                 (ADR 051)
  → secondary indexes                            (ADR 052)
  → wire-side Arrow IPC body compression         (ADR 053)
  → CDC subscription substrate                   (ADR 054)
  → LSM-style leveled compaction substrate       (ADR 055)
  → row-group fetch + page directory             (ADR 056)
  → per-buffer pages + encoding cascades     (ADR 057)
  → compute over encoded data (COCD)             (ADR 058)
  → native per-cascade COCD kernels              (ADR 059)
  → sparse projection fetch narrowing            (ADR 060)
  → page-aligned authenticated chunks            (ADR 061)
    + macro-block read coalescing
  → row-group sizing profile                     (ADR 062)
    PointRead / Scan
  → page-level predicate stats                   (ADR 063)
    + predicate-fetch narrowing                            (shipped)
  → nullable sparse projection                   (ADR 064)
    (raw byte-addressable validity only;                   (shipped)
     ADR 060 nullable extension)
  → dense predicate-zip with sparse projection    (ADR 065)
    (scalar dense-decode branch only;                       (shipped)
     ADR 063 encoding-compute paths stay Case A)
  → capacity-tier compression / serverless / EC  (rest of old #14 bundle) ← here
  → CDC follow-up ADRs (deferred this stretch)
  → vector embeddings (set aside this stretch)
  → refinement plumbing
```

Design tip is past ADR 065 (implemented and promoted to
shipped with bench evidence). The storage hierarchy is shipped
through ADR 065 (row groups, per-buffer
pages + encoding cascades, compute over encoded data substrate,
native per-cascade kernels, sparse projection fetch narrowing,
page-aligned AEAD chunks, deployment-wide row-group sizing
profile, page-level predicate stats, predicate-fetch
narrowing, nullable sparse projection, and dense predicate-
zip with sparse projection). ADR 063 amends ADR 057 with an additive
`BufferEncoding.page_stats` field (format-version bump, no
compat decoder) and adds an internal predicate-page fetch
planner that narrows Phase B predicate fetch to surviving
pages before survival-mask construction. Three-state
classification `Disjoint` / `ExactMatch` / `Maybe` with
null-position-conservative `ExactMatch` correctness for value
comparisons. Cascade eligibility = ADR 059 default predicate
allow-list ∩ `EncodingCascade::emits_pages_in_v1()` — V1
intersection is `BitPacked` + `DictionaryPlain` +
`DictionaryBitPacked` (Delta is allow-listed but not
page-emitting; RLE byte-run is page-emitting but not
allow-listed; both excluded V1). Type catalog = fixed-width
subset of ADR 056's stats support (no floats, no `Time`).
Phase 0 size-model + profile-emission gate is the sole V1
budget discipline; shipped quick evidence keeps both
`PointRead` and `Scan` in scope, with combined metadata inside
ADR 044's 64 KiB initial-probe envelope (no runtime fallback
per Wendelon V1 gate-driven house style). No new
wire / descriptor / PBC / capability / `ObjectStore` trait /
fallback / reject / catalog variant change. ADR 056 / 057 /
058 / 059 carry Phase 10 / Phase 8 bench evidence under
`plans/impl/`; ADR 060's Phase 7 artifact records the revised
plaintext-byte gate, ciphertext-byte floor, and RLE
default-off disposition; ADR 061's Phase 9 artifact closes
the ciphertext floor and macro-block coalescing gates;
ADR 062's bench evidence pinned the `Scan` target at
65,536 rows; ADR 063's bench evidence records the predicate-
fetch narrowing gates and metadata envelope; ADR 064's bench
evidence records nullable value+validity sparse projection;
ADR 065's bench evidence records scalar dense-predicate zip
with sparse projection and the narrowed V1 boundary where ADR
063 encoding-compute paths remain Case A mask-only.
The Toron-pivot stretch closed its primary storage-hierarchy
goal with ADR 058's substrate promotion and ADR 059's narrowed
default-enabled native-kernel set; ADR 060 closed the encoded-
fetch follow-up with internal reader dispatch, no public surface
change, and a strict plan-time byte-comparison gate; ADR 061
closed the ciphertext-floor follow-up via the AEAD/network
two-layer decoupling; ADR 062 closed the warehouse throughput
follow-up that ADR 061 deferred to row-group sizing via a
deployment-wide access-pattern profile; ADR 063 closes the
predicate-fetch-narrowing follow-up that ADR 062's `Scan`
profile makes load-bearing (65K-row groups × ~64 pages per
buffer make page-grain pruning a 5–10× predicate-fetch byte
reduction on clustered low-selectivity workloads).
**The council's lean on next pickup:** ADR 065 dense
predicate-zip with sparse projection (#21) is shipped. Pick
from the remaining Toron-adjacent follow-up family (FSST
evaluator, ALP evaluator, RLE value-run format, sparse
predicate-column fetch, variable-width sparse projection,
encoded-validity page decoding, all-null page-skip — each its
own ADR); or capacity-tier compression / serverless PBC /
erasure-coding work (#22). CDC follow-ups (#23) and vector
embeddings (#24) remain deferred behind the storage hierarchy
in this stretch.
