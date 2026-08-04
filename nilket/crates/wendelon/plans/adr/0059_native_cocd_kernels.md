# ADR 059: Native Per-Cascade COCD Kernels

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/0059_native_cocd_kernels.md`

## Status

Council-converged, implemented, and promoted to shipped.
Full Phase 10 bench evidence at
`plans/impl/0059_native_cocd_kernels_bench_results.md`
clears the promotion-to-shipped gate for the narrowed
default evaluator set. Phase 12 flips
`EncodingComputeConfig::default().enabled = true`, omits
`RlePlain` / `RleBitPacked` from the default predicate
allow-list, and relies on pre-kernel eligibility for
non-byte-aligned BitPacked, higher-cardinality Dictionary,
and cascade-grain aggregate exclusions.

Closes ADR 058's promote-to-default follow-up. Opens on
top of ADR 058's Phase 8 substrate bench artifact
(`plans/impl/0058_compute_over_encoded_data_bench_results.md`),
which establishes that the V1 evaluator path runs through
ADR 057's deterministic decoder plus a scalar comparison
loop and is 1.6×–2.5× *slower* than the dense micro
baseline on the representative cascade matrix. Substrate
is correct, byte-neutral, and architecture-locked; ADR 059
makes it fast.

The council process: four iterations between Claude,
Codex, and Gemini. Load-bearing forks resolved across
iterations: SIMD strategy (`wide`-now → auto-vectorization
on stable rustc with helper-crate / `std::arch` escalation
on miss); aggregate kernel scope (high-impact subset →
hedge → full coverage wherever ADR 058 already admits);
bench artifact placement (ADR 058 reservation vs ADR 059
path); decode-scalar oracle role (`#[cfg(test)]` only →
production fallback for legal-but-not-yet-native sublanes
+ kill-switch target); RLE byte-run reality check (council
confirmed shipped ADR 057 RLE format is byte-run, not
value-run; ADR 059 streams byte runs without changing the
format). 3-of-3 convergence on every load-bearing surface
except one residual placement preference (Gemini iter 3
preferred the ADR 058 reservation path; resolved 2-of-3
to the ADR 059 path).

V1 substrate commitments:

- **Native predicate kernels** for the ADR 058 evaluator
  catalog (`BitPacked`, `Rle { inner: Plain | BitPacked }`,
  `Dictionary { inner: Plain | BitPacked }`, `Delta { .. }`),
  replacing the scalar decode-then-scan body with cascade-
  aware specialization that operates directly on encoded
  bytes without materializing a decoded value buffer;
- **Native aggregate fold** for every (cascade × kind)
  pair ADR 058 already admits — full 8 × 4 native cascade-
  set coverage filtered by ADR 058's existing logical-type
  restrictions (`min`/`max` on fixed-width int / time /
  Boolean and fixed-width Dictionary; sum/avg on
  Int8/16/32/64 and UInt8/16/32/64; `count_*` on every
  cascade);
- **Byte-identical mask + aggregate-partial invariant**:
  for every (cascade × atom × dtype × selectivity)
  combination, native kernel output equals the substrate
  decode-then-scan oracle's output byte-for-byte after
  mask shape normalization. Property suite under
  `cargo test`, not just bench;
- **Central validity composition** in
  `kernels/validity.rs`; kernel-local 3VL implementations
  rejected;
- **Specialized `EncodedRowMask` algebra** without enum
  changes (`All` / `None` short-circuits, bytewise
  `Bitmap` AND/OR/NOT, `Runs` merge/flip, bounded
  densification);
- **Two-step dispatch discipline**: pre-kernel eligibility
  may route legal-but-not-yet-native sublanes to the
  decode-then-scalar oracle; once a native kernel begins
  parsing encoded bytes, any invariant failure returns
  through ADR 058's existing `EncodingComputeRejectCode`
  taxonomy (fail-closed, no silent fallback);
- **Decode-then-scalar oracle stays in production** as
  the test oracle, the pre-kernel fallback for legal-but-
  not-yet-native sublanes, and the kill-switch target
  for operator-disabled evaluators / aggregate kinds;
- **Default evaluator set** encoded in two existing
  surfaces — pre-kernel eligibility (cascade-grain) +
  the existing `enabled_predicate_evaluators` allow-list
  in `EncodingComputeConfig` (predicate-kind grain).
  Aggregate exclusion at cascade grain flows through
  eligibility, not config (`enabled_aggregate_kernels`
  is keyed by aggregate kind, not by cascade); see
  §"Default evaluator set" for details. No new
  operator-visible flag, no per-cascade `enabled_*`
  field;
- **Promotion gate**: ≥ 2× CPU vs end-to-end ADR 057
  dense PBC on representative low-selectivity
  `BitPacked` / `Rle` / `Dictionary` fixtures
  (predicate-only); ≥ 2× vs dense AGG on representative
  aggregate fixtures; ≤ 5% dense-fallback overhead;
  mask + aggregate-partial byte-equality on every
  fixture; ADR 056 / 057 / 040 / 052 / 055 regression
  neutrality with COCD enabled and disabled; cross-
  replica determinism byte-identical; bench matrix
  tiers 8K / 65K / 1M (1M mandatory); no native row
  slower than the ADR 058 Phase 8 substrate floor
  without documented council disposition;
- **SIMD strategy**: stable rustc; auto-vectorization V1
  via tight loops, block-unrolling, and stack scratch
  buffers; escalation route on bench miss is small SIMD
  helper crate first (e.g., `wide`, scoped to the
  affected kernel only), target-feature `std::arch`
  helpers second (behind an internal `kernels::simd`
  module); no nightly rustc, no `std::simd`, no Cargo
  feature gate, no `simd_*` / `kernel_*` /
  `dispatch_*` config knob;
- **Promotion flip**: when bench passes for the council-
  chosen default evaluator set,
  `EncodingComputeConfig::default()` flips
  `enabled = true`. Any predicate-evaluator kind that
  missed the gate is omitted from the default
  `enabled_predicate_evaluators` allow-list (predicate-
  kind grain); cascade-grain aggregate misses are
  enforced by the eligibility layer, since
  `enabled_aggregate_kernels` is keyed by aggregate
  kind, not by cascade. Reader-side automatic on flag
  presence; writer-side unaffected;
- **Catalog byte-identity**: no new
  `EncodingComputeEvaluatorKind` /
  `EncodedAggregateKind` /
  `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp` /
  `EncodedRowMask` variants. Closed-catalog discipline
  applies recursively to (cascade × kind) coverage —
  ADR 059 ships native specialization for every pair
  ADR 058 already admits, with zero new deferrals.
  No new wire `MsgType`, no `FRAME_FLAG_*`, no
  `NodeCapability` bit, no Cargo feature, no new digest
  domain, no `ObjectStore` trait change, no envelope
  `format_version` bump, no ADR 022 footer extension,
  no descriptor surface change, no public Toron-visible
  PBC opcode addition, no ADR 057 cascade catalog
  extension, no ADR 057 directory shape extension.

## Context

ADR 058 landed the COCD substrate: per-row-group-buffer
encoded predicate evaluation through the planner's
Phase B; encoded global-aggregate folding routed after
ADR 040 CSB direct-answer ordering; closed 6-variant
predicate evaluator catalog; closed 8-kernel aggregate
catalog; `EncodedRowMask` tagged enum (`All` / `None` /
`Bitmap` / `Runs`); 5-code reject taxonomy; 9-variant
fallback-reason enum; bounded
`storage_l2_encoding_compute_*` metric family; ADR 052
secondary-index COCD confirm-step; ~27 architectural
lockdown guards. The substrate initially shipped disabled
by default because the V1 evaluator decoded through ADR
057's deterministic decoder and ran a scalar predicate /
fold loop with `EncodedRowMask` allocation on top — i.e.,
it paid the materialize cost ADR 058 was designed to skip,
plus the overhead of running encoded-mask bookkeeping on
top. The Phase 8 substrate result was correct, byte-neutral,
and architecture-locked; it was also slower than dense micro
on the representative cascade matrix. ADR 059's Phase 10
evidence is the promotion artifact that flips the narrowed
native set on by default.

The per-cascade kernel modules
(`src/storage/encoding/compute/kernels/{bitpacked, rle,
dictionary, delta, validity}.rs`) are 5–14 line
label-shims. The dispatch entry
(`src/storage/encoding/compute/dispatch.rs`, 333 lines)
routes all six evaluator kinds through one decode-then-
scan path. ADR 059 turns the kernel module anchors into
the home of native specialization; the unified scalar
path stays present as both the test oracle and the
pre-kernel production fallback for legal-but-not-yet-
native sublanes.

The promote-to-default gate that ADR 058 pinned
(≥ 2× CPU vs ADR 057 dense PBC on representative
`BitPacked` / `Rle` / `Dictionary` fixtures at low
selectivity, plus ≥ 2× vs dense AGG on representative
aggregate fixtures, plus ≤ 5% dense-fallback overhead)
is the load-bearing artifact ADR 059 produces. The gate
cleared for a narrowed default set: BitPacked `UInt8`,
low-cardinality Dictionary, and Delta predicate paths;
RLE and non-promoted sublanes remain behind the allow-list
and pre-kernel eligibility layers. Future Toron-adjacent
work now composes on top of an enabled-by-default COCD
substrate for that measured set.

Constraints carry forward unchanged: ADR 003, ADR 016,
ADR 020, ADR 022, ADR 029, ADR 030, ADR 035, ADR 036,
ADR 037, ADR 039, ADR 040, ADR 042, ADR 043, ADR 044,
ADR 052, ADR 053, ADR 054, ADR 055, ADR 056, ADR 057,
ADR 058.

## Architectural inextricability

Three design choices physically inseparable in V1:

**1. Native kernels + byte-identical mask invariant +
promote-to-default gate are one substrate.** Native
kernels without the mask-invariant proof would silently
break PBC correctness; the invariant proof without the
speedup gate would prove correctness on a regression. The
three ship together. The substrate decode-then-scan path
stays as the reference oracle the kernels are proven
against, and as the pre-kernel production fallback for
not-yet-native sublanes (variable-width Dictionary,
ADR 058 cascade deferrals like FSST / ALP, atom shapes
ADR 058 routes to dense, etc.).

**2. Native kernels are internal executor dispatch — ADR
058 catalog is byte-identical post-implementation.**
Closed dispatch table, no plug-in trait, no new public
PBC opcodes (Toron contract byte-identical), no new
evaluator kinds, no atom-set extension, no aggregate-
kernel catalog extension. Closed-catalog discipline
applies recursively to (cascade × kind) coverage —
ADR 059 ships native specialization for every pair
ADR 058 already admits, with zero new deferrals.
"Where streaming adapter exists" is not a tighter
constraint than ADR 058's existing logical-type filter;
adopting it would open the partial-coverage door that
the closed-catalog principle is designed to close.

**3. Promotion is deployment-wide, not per-cascade.**
`EncodingComputeConfig.enabled` flips once on Phase 10
acceptance. Default-set membership (which evaluator
kinds are eligible for native dispatch on default-
enabled deployments) is the council vote, enforced via
the existing `enabled_predicate_evaluators` allow-list in
`EncodingComputeConfig` (with cascade-grain aggregate
exclusion via the eligibility layer; see §"Default
evaluator set") rather than a new flag. If RLE
or Delta misses the gate substantially, those evaluator
kinds are omitted from the default
`enabled_predicate_evaluators` allow-list until a
follow-up ADR revises; cross-replica byte-equality holds
because per-replica dispatch decisions still produce
byte-identical mask / partial-state outputs.

## Identity-vs-Locality framing

Identity-axis, identical to ADR 058. Native kernels run at
the same `(SlabId, RecordLocation, RowGroupIndex,
IpcColumnOrdinal, IpcBufferIndex, EncodingPageIndex)`
grain as the substrate evaluators they replace.
`EncodedRowMask` and `AggregatePartial` shapes unchanged.
Cross-replica byte-equality continues to hold by
mask / partial-state byte-identity, not by cross-replica
kernel agreement (each replica picks its own internal
dispatch leg, all legs produce the same output). Unified
Ring rule applies. There is no separate "compute plane" —
native kernels run in the same storage-read surface as
ADR 057's Phase-D materializer and ADR 058's substrate
evaluators. Cross-locality compaction does not apply
(per-record substrate, downstream of admission, upstream
of ADR 055's per-locality compactor).

## V1 scope

ADR 059 ships **only** native per-cascade kernel work and
the promotion gate evidence. Catalog from ADR 058
unchanged; coverage is per-pair native specialization
where ADR 058 already admits the (cascade × kind) pair.

### 1. Four native predicate kernel families

**`BitPacked { bits, base }`** — page-grain.

- Byte-aligned fast path: `bits ∈ {8, 16, 32, 64}` reads
  the encoded little-endian bytes via chunked
  `from_le_bytes` lane loads (e.g.,
  `u32::from_le_bytes(chunk_4)` over fixed-size byte
  chunks), unrolled into block-comparison loops the auto-
  vectorizer can vectorize. No raw `&[u8]` → `&[uN]`
  slice cast: the codec writes little-endian packed
  bytes without padding, so a typed-slice cast is
  alignment-unsafe (UB on strict-alignment targets like
  aarch64) and endian-unsafe (wrong values on big-endian
  hosts). Literal coerces and subtracts `base` once
  before the lane walk.
- Non-aligned path: bits ∈ {1..63} \ {8, 16, 32} use a
  packed-bit reader that unpacks 32 or 64 values into
  a stack array per chunk; comparison happens against
  the stack chunk; result bits flow into the output
  `Bitmap`. No full-page materialization.
- Out-of-range literal short-circuits: `Eq` → `None`,
  `Neq` → `All` (subject to validity composition).
- Page-grain dispatch behavior preserved per ADR 058.

**`Rle { inner: Plain | BitPacked }`** — byte-run streaming.

The shipped ADR 057 RLE format is byte-run
`(byte_run_len: u32, byte: u8)`, not value-run. Native
kernel streams byte runs, reconstructs fixed-width
logical values into a small stack buffer (no allocated
decoded column buffer), compares each reconstructed
value against the atom, and coalesces output spans into
`Runs` only when byte runs prove a row-aligned row-run
shape; otherwise emits `Bitmap`. If existing byte-run
RLE cannot meet the 2× gate, RLE drops out of the
default evaluator set rather than driving an ADR 057
format change inside ADR 059 (see §"Default evaluator
set" mechanism below).

**`Dictionary { inner: Plain | BitPacked }`** — match-set
+ index walk. WDD1 (plain) / WDB2 (bit-packed) frames
per ADR 057.

- Step 1: parse the inline dictionary value table once;
  evaluate the atom against each entry; produce a match
  set `S` of dictionary indices, bounded by
  `max_dict_match_set_size = 4_096` (ADR 058 cap).
- Empty `S` → `None`; full `S` → `All` for matching-
  positive atoms.
- `|S| = 1` → walk the index stream for direct equality.
- `1 < |S| ≤ 4` → unrolled OR-composed equality.
- `4 < |S| ≤ 4_096` → bounded membership bitmap by
  dictionary index (≤ 512 bytes; fits L1 twice over).
- `|S| > 4_096` → pre-kernel fallback to the decode-
  scalar oracle. Match-set size is determined from the
  dictionary value table before the index walk begins,
  so the cap fires *before* the kernel commits to
  native dispatch on the per-row work; the existing
  `EncodingComputeFallbackReason` enum (e.g.,
  `UnsupportedProgram`) carries the fallback. ADR 058's
  reject taxonomy (`EncodingComputeRejectCode`) is
  reserved for invariant violations *after* parsing
  has begun (malformed dictionary frame, malformed
  index stream, decode-length mismatch); the cap is an
  eligibility decision, not a reject.
- `Dictionary { inner: Lz4Frame }` remains
  DenseFallback per ADR 058. Variable-width Dictionary
  predicate expansion remains out of scope.

**`Delta { .. }`** — full-buffer prefix-sum walk. WDL1
frame per ADR 057. All inner variants route identically
because Delta bytes / dispatch grain are inner-irrelevant
per ADR 058 §"Closed predicate evaluator catalog."

- Single-pass prefix-sum reconstruction; no
  intermediate `Vec<i64>`.
- Per reconstructed value, evaluate the atom against
  the coerced literal; emit bitmap bit.
- Monotonic-shape short-circuits derived from the delta
  stream itself (all-non-negative deltas =
  monotonically non-decreasing); no ADR 057 directory
  extension. For range atoms on monotonic streams,
  prove whole-buffer reject / accept where the literal
  bound permits.
- Non-monotonic: full walk + per-row test, equivalent
  in cost to substrate path; speedup comes from
  avoiding the intermediate decoded buffer.

### 2. Native aggregate fold kernels — full coverage

ADR 059 ships native fold for **every (cascade × kind)
pair ADR 058 already admits**. ADR 058's existing
logical-type restrictions filter the matrix:

- `count_star` / `count_non_null` apply to every
  cascade.
- `min` / `max` apply to fixed-width integer / time /
  Boolean and fixed-width Dictionary cascades.
- `signed_sum` / `unsigned_sum` / `signed_avg` /
  `unsigned_avg` apply to Int8/16/32/64 and
  UInt8/16/32/64.

Per-cascade native fold targets:

- **BitPacked:** auto-vec horizontal reduction for
  `min` / `max` / `sum` / `avg` (sum widened to avoid
  overflow per ADR 058 overflow taxonomy); validity-
  aware popcount for `count_*`.
- **Rle (byte-run):** O(byte-runs) walk; per byte run,
  reconstruct value once, weight by row-aligned
  contribution. `count_star` from total row count;
  `count_non_null` from validity-aware byte-run
  lengths; `min` / `max` / `sum` over reconstructed
  values.
- **Dictionary:** index histogram (one pass over
  indices to count occurrences per dictionary entry),
  then aggregate over `(dict_value × index_count)`
  pairs. `min` / `max` short-circuit by scanning
  dictionary entries directly when mask = `All`.
- **Delta:** `count_star` from row count; `sum`
  accumulates each reconstructed value during the
  single-pass prefix-sum walk (the codec at
  `src/storage/encoding/cascade/codec.rs` stores
  adjacent deltas where `delta_j = value_j - value_{j-1}`,
  so `base × count + Σ delta_j` is wrong — each
  `delta_j` contributes `(count − j) × delta_j` to
  the total. The natural implementation is running-
  value + running-sum in one pass; no closed-form
  optimization is needed); `min` / `max` monotonic-
  aware (first / last value short-circuit on non-
  negative streams); full walk otherwise.

Pairs ADR 058 *already* defers (Utf8 / Binary
`min`/`max`, `APPROX_DISTINCT`, `QUANTILE_KLL`, exact
`count_distinct`, float / decimal aggregates,
time-sum/avg, GROUP BY) inherit unchanged — they fall
back to dense via the existing all-or-nothing mixed-
request policy. ADR 059 adds zero new deferrals.

ADR 058 all-or-nothing mixed-request policy preserved.
ADR 040 partial-state byte-equality preserved.

### 3. Specialized mask algebra

`EncodedRowMask` enum unchanged; algebra implementations
specialize:

- `All` / `None` short-circuits without allocation;
- `Bitmap AND/OR Bitmap` bytewise (auto-vec friendly);
- `Bitmap NOT` bytewise with tail-bit masking;
- `Runs NOT` by per-run flag flip;
- `Runs AND/OR Runs` by run merge;
- `Runs ↔ Bitmap` densification only when needed,
  bounded by `max_mask_bytes_per_record`.

`to_bool_vec()` retained for tests + slow paths.

### 4. Two-step dispatch discipline

Pre-kernel eligibility check (cascade × dtype × atom ×
literal coercion shape) may route legal-but-not-yet-
native sublanes to the decode-then-scalar oracle. Once a
native kernel begins parsing encoded bytes, any invariant
failure returns through ADR 058's existing
`EncodingComputeRejectCode` taxonomy — fail-closed, no
silent fallback. Operator kill-switch sets remain
authoritative: a disabled evaluator routes to oracle
before native kernel entry; a disabled aggregate kind
routes to dense AGG before native partial emission.

### 5. Byte-identical mask + aggregate-partial invariant

Property suite under `cargo test`: for every
(cascade × atom × dtype × selectivity) combination in
the bench fixture matrix, the native kernel's
`EncodedRowMask` output equals the substrate decode-
then-scan oracle's output byte-for-byte after mask
shape normalization (`Bitmap` ↔ `Runs` conversion is a
representational choice; the *set of selected rows*
must be identical). For kernels whose native output
shape is part of the performance story (Rle's
`Runs`-when-aligned), direct byte-shape fixtures
supplement the normalized parity tests. Aggregate-
partial bytes match dense AGG exactly, including
overflow flags. Tests live in
`tests/encoding_compute_kernel_invariants.rs` (new
file); existing `tests/encoding_compute_test.rs` keeps
unit-contract focus.

### 6. Central validity bitmap composition

3VL semantics centralized in `kernels/validity.rs`. All
native kernels call this helper; kernel-local 3VL
implementations rejected by code review. Helper owns
Arrow validity bit lookup; comparison-bitmap AND
validity-bitmap (auto-vec friendly); `All` / `None`
short-circuits when null-count metadata proves the
whole row group valid or invalid; run-aware validity
composition for `Runs` masks (no forced densification
when validity is run-homogeneous); scalar fallback for
irregular validity spans. Validity atoms (`IsNull`,
`IsNotNull`) continue to route through this helper;
value kernels reject validity atoms (preserves
ADR 058).

### 7. SIMD strategy

Stable rustc only; no nightly `std::simd`; no Cargo
feature gate; no operator-visible knob. V1 native
kernels use tight loops, block-unrolling, stack scratch,
and code shapes LLVM auto-vectorizes. Escalation route
on bench miss:

- first choice: small SIMD helper crate (e.g., `wide`),
  scoped to the affected kernel only with the
  dependency import boundary pinned by an
  architectural-test guard;
- second choice: target-feature-gated `std::arch`
  helpers behind an internal `kernels::simd` module.

The most-likely escalation trigger is the Phase 4
(Dictionary) bench row, given the indirect access
pattern in bit-packed index walks; other kernels stay
auto-vec unless they also miss. Discipline: escalate at
the phase boundary that produced miss evidence, not as
Phase 0 planning.

### 8. Default evaluator set

`EncodingComputeConfig.enabled` is a deployment-wide
switch. ADR 059 does not add per-cascade config flags.

The council-chosen default evaluator set is encoded in
two existing surfaces:

- **Native eligibility** — pre-kernel dispatch decides
  whether a sublane has a native kernel and whether
  that kernel passed the gate. Excluded cascades route
  to the decode-scalar oracle before kernel entry. This
  is not silent fallback after kernel reject;
  eligibility decides before parsing begins. In-kernel
  invariant failures remain fail-closed. **Cascade-
  grain aggregate exclusion flows through this surface,
  not through config**, because `enabled_aggregate_kernels`
  is keyed by `EncodedAggregateKind` and cannot
  selectively disable (Delta × `signed_sum`) without
  also disabling (BitPacked × `signed_sum`).
- **Existing `enabled_predicate_evaluators` allow-list**
  in `EncodingComputeConfig` — `default()` populates it
  with `EncodingComputeEvaluatorKind::ALL` today; if
  RLE byte-run streaming or Delta prefix-sum misses the
  2× gate substantially, the council-chosen `default()`
  *omits* the missing evaluator kind from the allow-
  list (predicate-grain exclusion at config level).
  Operator override remains available (operators can
  remove a passing evaluator or re-add a missing one
  explicitly).

The `enabled_aggregate_kernels` allow-list (also
populated with `EncodedAggregateKind::ALL` by
`default()`) operates at aggregate-kind grain, not
cascade grain; it is the operator kill-switch for an
entire aggregate kind across all cascades. Any
cascade-specific aggregate gate miss routes through
the eligibility layer rather than the config allow-
list — the config surface stays closed (per ADR 058).

If RLE / Delta miss the gate, `enabled = true` still
flips for the gate-passing set; missing cascades route
through the substrate oracle via the appropriate
surface above.

### 9. Kernel-promotion bench artifact

At `plans/impl/0059_native_cocd_kernels_bench_results.md`.

Promotion gates:

- predicate-only Exact COCD ≥ 2× faster than end-to-end
  ADR 057 dense PBC (Phase B + C + D) on representative
  low-selectivity `BitPacked` / `Rle` / `Dictionary`
  fixtures;
- Delta range evaluation ≥ 2× on monotonic fixtures, OR
  Delta excluded from default set via §8 mechanism;
- aggregate folds ≥ 2× faster than dense AGG on
  representative `BitPacked` / `Rle` / `Dictionary`
  fixtures;
- dense fallback overhead ≤ 5%;
- mask + aggregate-partial byte-equality on every
  fixture;
- ADR 056 / 057 / 040 / 052 / 055 regression neutrality
  with COCD enabled and disabled;
- cross-replica determinism byte-identical;
- bench matrix mandatory tiers: 8K / 65K / 1M rows;
- no native row slower than the ADR 058 substrate floor
  (dense-micro shape) without documented council
  disposition.

ADR 058 Phase 8 dense-micro numbers serve as the
substrate-floor regression tripwire; the gate denominator
is end-to-end ADR 057 dense PBC, not dense-micro.

### 10. Promotion flip

When the kernel-promotion bench passes for the council-
chosen default evaluator set,
`EncodingComputeConfig::default()` flips
`enabled = true`. Any predicate-evaluator kind that
missed the gate is omitted from the default
`enabled_predicate_evaluators` allow-list; any cascade-
grain aggregate miss is enforced by the eligibility
layer (since `enabled_aggregate_kernels` is keyed by
kind, not cascade — see §8). ADR 058 §"Where
we are" amends to point at the landed bench artifact in
the same commit. Reader-side automatic on flag presence;
writer-side unaffected.

### 11. Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- file-digest pins on the four native kernel modules +
  `kernels/validity.rs`;
- `dispatch.rs` shape: production path routes to native
  kernels via eligibility layer; decode-scalar oracle
  remains test-visible AND production-fallback-visible;
- no new variants in `EncodingComputeEvaluatorKind`,
  `EncodedAggregateKind`, `EncodingComputeRejectCode`,
  `EncodingComputeFallbackReason`, `PredicateAtomOp`,
  `EncodedRowMask`;
- no new fields in `EncodingComputeConfig` (only the
  `enabled` default may flip and the existing
  `enabled_predicate_evaluators` allow-list may narrow);
- no `simd_*` / `kernel_*` / `dispatch_*` config knob;
- no Cargo feature gating native kernels;
- no nightly rustc adoption (build passes on stable);
- bench artifact presence at the promotion path;
- SIMD helper crate (if Phase 4 escalation triggered)
  scoped to one helper module with architectural-test-
  pinned import boundary.

## V1 deferred

ADR 059 deliberately does **not** ship:

- new public Toron-visible PBC opcodes;
- new evaluator kinds (`Fsst` / `Alp` encoded
  evaluation remain ADR 058 deferrals);
- new aggregate kinds, atoms, reject codes, or
  fallback reasons;
- ADR 057 directory shape extension (no per-page
  min/max stats, no per-page null counts, no Delta
  side-tables);
- ADR 057 RLE format change (byte-run → value-run is
  follow-up ADR territory if Phase 10 evidence motivates
  it);
- variable-width Dictionary predicate expansion;
- nested / list / struct Arrow type predicate
  evaluation;
- new `EncodingComputeConfig` knobs;
- per-cascade `enabled_*` flags;
- a Cargo feature gating native kernels;
- nightly rustc adoption;
- runtime CPU-feature dispatch shim;
- partial-buffer or partial-page kernels beyond ADR 058
  commitments;
- new `EncodedRowMask` variants;
- new wire `MsgType` / `FRAME_FLAG_*` /
  `NodeCapability` bit / envelope `format_version`
  bump / footer extension / descriptor change / new
  metric label / `ObjectStore` trait change.

ADR 058 deferrals stand: `approx_distinct`, KLL/HLL,
exact `count_distinct`, GROUP BY, float / decimal
aggregates, time-sum/avg, Utf8 / Binary `min`/`max`.
Sparse Phase-C narrowing, page-level min/max/null
stats, FSST evaluator, ALP evaluator each remain
follow-up ADR territory.

## Provisional discipline

Inherits ADR 056 / 057 / 058. The ≥ 2× CPU promotion
gate is starting target, revisable on Phase 10 evidence
with documented council rationale. Same revision
discipline ADR 056 applied (15% writer-cost target →
25% empirical gate after representative fixture
evidence). A Phase 10 miss does not silently weaken the
default-on decision; revision requires bench evidence
and explicit council disposition recorded in the bench
artifact.

## Bench-Settled Questions

Two empirical questions were intentionally left to Phase 10
bench evidence rather than settled by design preference.

**1. RLE byte-run native viability.** Native streaming over
the existing byte-run RLE frame did not meet the 2x gate.
`RlePlain` / `RleBitPacked` are omitted from the default
`enabled_predicate_evaluators` allow-list, and RLE value-run
format work remains a separate follow-up ADR candidate.

**2. Delta default-set inclusion.** Delta predicate kernels
cleared the 2x predicate gate across the measured tiers and
remain in the default predicate allow-list.

A residual placement preference from the council
process: Gemini iter 3 preferred the bench artifact at
`plans/impl/058_compute_over_encoded_data_kernel_bench_results.md`
(co-locating the gate evidence with ADR 058's owning
ADR). Resolved 2-of-3 to
`plans/impl/0059_native_cocd_kernels_bench_results.md`
in this canonical merge: ADR 059 produces the
experiments and owns the gates; ADR 058's reservation
was speculative, no file exists at the reserved path,
no compat hedge is needed (per
`plans/conventions/no-premature-backward-compat.md`).
ADR 058 §"Where we are" amends in the ADR 059
promotion commit to point at the landed bench file.

## What not to do

- **Don't reopen ADR 058's catalog.** No new evaluator
  kinds, atoms, aggregate kernels, reject codes, or
  fallback reasons. Closed-catalog applies recursively
  to (cascade × kind) coverage — ADR 059 ships native
  specialization for every pair ADR 058 already admits.
  "Where streaming adapter exists" or "high-impact
  subset" hedges open the partial-coverage door the
  closed-catalog principle is designed to close;
  council convergence at iter 4 rejected both.
- **Don't ship a `simd_strategy` / `simd_width` /
  `kernel_dispatch_mode` config knob.** Closed-
  dispatch principle (ADR 058 §"Architectural
  inextricability" point 3) applies recursively.
  Operators don't tune SIMD width.
- **Don't ship a Cargo feature gate** for native
  kernels. Single-build discipline.
- **Don't ship a per-cascade `enabled_*` flag.**
  Default-set membership reuses the existing
  `enabled_predicate_evaluators` allow-list in
  `EncodingComputeConfig` (predicate-kind grain) plus
  the eligibility layer (cascade grain, including
  cascade-grain aggregate misses). A new flag would
  split the planner's decision matrix and make cross-
  replica
  byte-equality harder to argue.
- **Don't ship native kernels without the byte-
  identical mask + aggregate-partial invariant
  proof.** Property suite runs on every `cargo test`.
- **Don't extend ADR 057's directory shape.** No
  per-page min/max, no per-page null counts, no
  Delta side-tables. Follow-up ADR territory with
  bench-driven motivation.
- **Don't change ADR 057's RLE byte-run format inside
  ADR 059.** If byte-run can't clear the gate, RLE
  drops out of the default set; a separate ADR
  considers value-run.
- **Don't add `Fsst` / `Alp` evaluators.** ADR 058
  deferral inherits unchanged.
- **Don't write the kernel-promotion file before
  kernels exist.** Phase 10 produces it as evidence;
  iterative bench commits as cascades clear gates.
- **Don't promote with partial coverage.** All
  cascades in the council-chosen default set clear
  gates before the flip. Per-cascade flipping splits
  the planner's decision matrix and creates path-
  dependent physical plans on the same logical query.
- **Don't widen `EncodingComputeConfig` for kernel
  selection.** Closed config surface; only `enabled`
  default may flip and existing
  `enabled_predicate_evaluators` allow-list
  may populate.
- **Don't add new metric labels.** Existing
  `storage_l2_encoding_compute_kernel_micros` with
  bounded `{decision, reason, kernel, cascade, kind,
  aggregate, violation_kind}` enumerations is
  sufficient.
- **Don't bench against synthetic best-case fixtures.**
  Bench matrix mirrors ADR 058 Phase 8 fixture set;
  new fixtures require council sign-off on parity.
- **Don't retire the decode-scalar oracle from
  production code.** Stays as pre-kernel fallback for
  legal-but-not-yet-native sublanes (variable-width
  Dictionary, FSST, ALP, ADR-058-deferred atom shapes)
  and as kill-switch target for operator-disabled
  evaluators.
- **Don't add a SIMD helper crate dependency at Phase
  0.** Auto-vec first; helper crate adds at the phase
  boundary that produces miss evidence (Phase 4
  Dictionary is the most-likely candidate).
- **Don't hedge aggregate coverage with "where
  streaming adapter exists" language.** The framing
  is firmer: "wherever ADR 058 already admits the
  expression and logical type." ADR 058's existing
  logical-type filter does the matrix narrowing;
  ADR 059 adds zero new deferrals.

## Convergence summary

```text
ADR 058 substrate (decode-then-scan, initially disabled)
  → native per-cascade kernels + promotion gate    (ADR 059)
  → enabled-by-default on the narrowed native set   ← here
  → next Toron-adjacent follow-up (sparse Phase-C narrowing,
    page-level min/max stats, FSST evaluator, RLE value-run
    format after Phase 10 miss evidence) — own ADRs
  → capacity-tier compression / serverless / EC     (rest of #15)
```

## Promotion criteria

ADR 059 promotes from design-promoted to shipped when:

1. Native kernels for all four cascades land in
   `src/storage/encoding/compute/kernels/`.
2. Byte-identical mask + aggregate-partial invariant
   property suite passes under `cargo test` on the full
   (cascade × atom × dtype) matrix.
3. Kernel-promotion bench artifact at
   `plans/impl/0059_native_cocd_kernels_bench_results.md`
   demonstrates promotion gates for the council-chosen
   default evaluator set.
4. `EncodingComputeConfig::default().enabled = true`
   flips in a dedicated commit alongside the bench
   artifact; missing predicate-evaluator kinds omitted
   from the default `enabled_predicate_evaluators`
   allow-list, cascade-grain aggregate misses enforced
   by eligibility layer.
5. ADR 058 §"Where we are" amends with pointer to the
   landed bench artifact (closes the kernel-promotion
   follow-up).
6. Architectural lockdown guards land per §11.

Those criteria landed in the ADR 059 promotion changeset;
ADR 059 is shipped/default-enabled for the narrowed default
set documented in the bench artifact.
