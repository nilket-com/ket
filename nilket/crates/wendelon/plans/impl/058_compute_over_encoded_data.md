# Implementation Plan: ADR 058 — Compute Over Encoded Data (COCD)

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/058_compute_over_encoded_data.md`

Inherits write-path bifurcation:
`plans/adr/003_stellarator_bifurcated_writes.md`.
Inherits chunked AEAD authenticator:
`plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md`.
Inherits unencrypted footer / offset index:
`plans/adr/022_l2_slab_format_footer_indexed_range_reads.md`.
Inherits PBC bytecode shape:
`plans/adr/035_vectorized_pbc_executor.md`.
Inherits PBC pushdown contract:
`plans/adr/036_pbc_pushdown_contract.md` §7 / §11.
Inherits CSB stats / selectivity estimates:
`plans/adr/039_slab_footer_column_statistics_v1.md`.
Inherits AGG short-circuit ordering and partial-state shape:
`plans/adr/040_aggregate_pushdown.md`.
Inherits multi-row record layout:
`plans/adr/042_analytical_multi_row_fragments.md`.
Inherits per-buffer LZ4_FRAME compression:
`plans/adr/043_arrow_ipc_body_compression.md`.
Inherits column-projected fetch substrate:
`plans/adr/044_l2_columnar_fetch.md`.
Inherits secondary-index hit shape:
`plans/adr/052_secondary_indexes.md` §3.
Inherits LSM slab-writer composition:
`plans/adr/055_lsm_leveled_compaction_substrate.md` §5.
Inherits row-group + page-directory substrate:
`plans/adr/056_row_group_fetch_and_page_directory.md`.
Inherits encoding cascade catalog + page directory:
`plans/adr/057_per_buffer_pages_and_encoding_cascades.md`.

## Status

Council-converged and design-promoted. Implementation
phases pending; full small / medium / large bench evidence
is the promotion-to-shipped gate.

Implementation contract locked at the council level: 6
predicate evaluators, 8 aggregate kernels, 9 predicate atoms,
node-local `EncodingComputeConfig` (no descriptor policy, no
new public PBC opcodes), Exact / Prefilter / DenseFallback
predicate plan modes, `EncodedRowMask` tagged enum, 5-code
reject taxonomy, dual economic gates (0.25 leaf selectivity +
0.10 Prefilter rejection), `Delta` full atom set at full-buffer
grain, page-grain dispatch default for page-emitting cascades.

V1 implementation commitments (pinned by the companion ADR):

- predicate + aggregate-fold COCD substrate; both consumers
  ship together;
- closed predicate-evaluator catalog: `BitPacked`,
  `RlePlain`, `RleBitPacked`, `DictionaryPlain`,
  `DictionaryBitPacked`, `Delta`. Page-grain for
  page-emitting cascades; buffer-grain for the rest;
  full-buffer grain for Delta;
- closed aggregate-kernel catalog: `count_star`,
  `count_non_null`, `min`, `max`, `signed_sum`,
  `unsigned_sum`, `signed_avg`, `unsigned_avg`. `min`/`max`
  target fixed-width integer / time / Boolean and
  fixed-width Dictionary cascades; sum/avg are integer-only
  (no time/float/decimal), matching ADR 040 dense
  aggregate validation. Mixed-request fold is all-or-
  nothing (any unsupported expression → entire request
  falls back to dense AGG);
- node-local `EncodingComputeConfig` is the activation
  surface;
- no new public PBC opcodes (`src/schema/pbc.rs::Op` byte-
  identical post-implementation);
- planner Phase-B extension with Exact / Prefilter /
  DenseFallback decomposition;
- aggregate fold runs after ADR 040 CSB direct-answer
  ordering;
- dual economic gates: 0.25 leaf-level selectivity gate
  + 0.10 Prefilter rejection gate. Aggregate folding
  ungated by predicate selectivity;
- promotion gated by Phase 8 bench acceptance.

## Goal

Land the COCD substrate as a self-contained module under
`src/storage/encoding/compute/`. Per-cascade evaluators,
per-aggregate kernels, predicate classifier, mask
infrastructure, and Phase-B / AGG dispatch all live inside
the new module; ADR 056's row-group module and ADR 057's
encoding directory / materializer modules stay unchanged.

The implementation:

- 3 newtype-units in `src/schema/units.rs`;
- `EncodingComputeConfig`, `EncodingComputeEvaluatorKind`,
  `EncodedAggregateKind`, `EncodingComputeRejectCode`,
  `EncodingComputeFallbackReason`,
  `EncodingComputePredicatePlan`, `EncodedPredicateNode`,
  `EncodedResidualMode`, `EncodingComputePlan`,
  `PredicateAtom`, `PredicateAtomOp`, `LiteralCoercion`,
  `EncodedRowMask`, `SelectionRun` in
  `src/storage/encoding/compute/mod.rs` and submodules;
- per-evaluator modules
  (`src/storage/encoding/compute/kernels/{validity,
  bitpacked,rle,dictionary,delta}.rs`);
- aggregate fold module
  (`src/storage/encoding/compute/aggregate.rs`);
- dispatch entry in `src/storage/encoding/compute/dispatch.rs`;
- predicate classifier in
  `src/storage/encoding/compute/predicate.rs`;
- mask infrastructure in
  `src/storage/encoding/compute/mask.rs`;
- planner Phase-B extension in `src/storage/columnar.rs`;
- ADR 040 AGG dispatch extension;
- ADR 052 secondary-index integration;
- bounded `storage_l2_encoding_compute_*` metric family;
- 5-code `EncodingComputeRejectCode` taxonomy;
- audit lifecycle events;
- bench matrix per ADR §"Open questions" Phase-8 list.

## Deliverables

- 3 newtype-units in `src/schema/units.rs`:
  `DictMatchSetSize`, `PredicateLeafIndex`,
  `SelectivityFractionMicros`.
- `src/storage/encoding/compute/mod.rs` (new).
- `src/storage/encoding/compute/dispatch.rs` (new) — single
  evaluator dispatch entry point.
- `src/storage/encoding/compute/predicate.rs` (new) —
  Exact / Prefilter / DenseFallback classifier.
- `src/storage/encoding/compute/mask.rs` (new) —
  `EncodedRowMask` algebra.
- `src/storage/encoding/compute/coerce.rs` (new) — literal
  coercion (per-cascade + per-atom).
- `src/storage/encoding/compute/null.rs` (new) — 3VL
  helpers.
- `src/storage/encoding/compute/kernels/validity.rs` (new).
- `src/storage/encoding/compute/kernels/bitpacked.rs` (new).
- `src/storage/encoding/compute/kernels/rle.rs` (new).
- `src/storage/encoding/compute/kernels/dictionary.rs` (new).
- `src/storage/encoding/compute/kernels/delta.rs` (new).
- `src/storage/encoding/compute/aggregate.rs` (new) — 8
  aggregate kernels.
- `src/storage/encoding/compute/planner.rs` (new) —
  row-group integration.
- `src/storage/encoding/compute/index.rs` (new) —
  secondary-index confirm-step.
- `src/storage/columnar.rs` — extend planner with Phase-B
  COCD plan.
- `src/storage/manager.rs` — wire dispatch from the fetch
  executor.
- `src/storage/aggregate.rs` — wire aggregate fold after
  CSB direct-answer.
- `src/storage/secondary_index.rs` — page-aware confirm-hit
  via encoded eval.
- `src/network/data_server.rs` — AGG ordering hook.
- `src/config.rs` — `EncodingComputeConfig`.
- `src/metrics/registry.rs` — metric family.
- `src/audit.rs` — audit events.
- `tests/architecture_test.rs` — Phase 9 guards.
- 8 new test files (`tests/encoding_compute_*.rs`).
- `benches/encoding_compute.rs` (new).
- `plans/ops/runbooks/encoding_compute.md` (new).

## Non-goals

- No new public PBC opcodes; `src/schema/pbc.rs::Op` byte-
  identical.
- No `RecordBodyCompressionPolicy::EncodedCompute`
  descriptor variant.
- No `Fsst` / `Alp` cascade encoded evaluation in V1.
- No nested / list / struct Arrow type predicate eval.
- No new envelope `format_version`.
- No ADR 022 footer extension.
- No ADR 057 directory shape extension.
- No `SlabRecordHeader` change.
- No new `MsgType` containing `Cocd` / `Encoded` /
  `Compute`.
- No new `FRAME_FLAG_*`.
- No new `NodeCapability` bit.
- No `ObjectStore` trait change.
- No AGG wire-envelope change.
- No GROUP BY.
- No approx aggregates (`approx_distinct`, KLL/HLL).
- No exact `count_distinct`.
- No float / decimal aggregates.
- No string / binary min/max from unordered Dictionary or
  FSST.
- No reserved evaluator slots, compatibility shims, or
  numeric-version Rust type suffixes.
- No project-name prefix on new symbols.
- No high-cardinality metric labels.
- No new Cargo feature or external codec dependency.
- No dense fallback after a kernel reject (fail closed).
- No sparse-output page-grain narrowing in Phase C beyond
  Prefilter (deferred follow-up).
- No Delta page side-tables for page-grain range pruning.

## Phase 0 — Contract freeze and module layout

Lock the evaluator catalog, aggregate-kernel catalog,
dispatch surface, plan modes, configs, planner integration
shape, metric label enumerations, architectural guards, and
V1 defaults before invasive storage edits begin.

**Substrate shape is immutable post-Phase-0.**

Files:

- `src/schema/units.rs`
- `src/storage/encoding/compute/mod.rs` (new)
- `src/storage/encoding/compute/dispatch.rs` (new)
- `src/storage/columnar.rs` (extension intent)
- `src/storage/aggregate.rs` (extension intent)
- `src/storage/secondary_index.rs` (extension intent)
- `src/storage/manager.rs` (dispatch wiring intent)
- `src/network/data_server.rs` (AGG ordering intent)
- `src/config.rs`
- `src/metrics/registry.rs`
- `tests/fixtures/encoding_compute/` (new)
- `tests/architecture_test.rs`

Work:

1. Audit `src/storage/columnar.rs` for the Phase-B
   integration point — where ADR 056's asymmetric AND/OR
   classifier produces leaf labels and where Phase-B
   materialization is currently triggered.

2. Audit `src/storage/encoding/types.rs`,
   `src/storage/encoding/select.rs`, and
   `src/storage/encoding/directory.rs`. Confirm
   `EncodingCascade` / `BufferEncoding` /
   `EncodingPageDescriptor` shape is sufficient for COCD
   dispatch without ADR 057 directory mutation. Record a
   file-digest snapshot for the architectural guard.

3. Audit `src/schema/pbc.rs::PredicateTreeV1` and `Op` to
   pin the precise leaf shapes COCD classifier will see.
   No additions; classifier reads existing leaves.

4. Audit `src/storage/aggregate.rs` and
   `src/network/data_server.rs` for the CSB direct-answer
   ordering hook — COCD aggregate fold runs after CSB
   direct-answer, before dense AGG materialization.

5. `EncodingComputePlan` /
   `EncodingComputePredicatePlan` live in
   `src/storage/encoding/compute/mod.rs` as internal planner
   types. V1 does not thread `EncodingComputePlan` through a
   serialized `RowGroupFetchPlan` field. Architectural guard
   ensures COCD remains internal and never adds a wire shape.

6. Freeze newtype-units:

	   ```rust
	   impl_unit!(DictMatchSetSize, u32);            // V1 cap 4_096
	   impl_unit!(PredicateLeafIndex, u16);
	   impl_unit!(SelectivityFractionMicros, u32);   // 1_000_000 = 1.0
	   ```

   Reuse `RowGroupIndex`, `IpcColumnOrdinal`,
   `IpcBufferIndex`, `BufferValueCount`, `BufferByteRange`,
   `EncodingCascadeIndex`, `EncodingPageIndex`,
   `EncodingPageCount`, `TaskCount`, `RowCount`,
   `RecordRowIndex`, `RecordBodyByteLen` from ADR 042 / 056
   / 057.

7. Freeze evaluator catalog:

   ```rust
   #[repr(u8)]
   pub enum EncodingComputeEvaluatorKind {
       BitPacked            = 1,
       RlePlain             = 2,
       RleBitPacked         = 3,
       DictionaryPlain      = 4,
       DictionaryBitPacked  = 5,
       Delta                = 6,
   }
   ```

8. Freeze aggregate-kernel catalog:

   ```rust
   #[repr(u8)]
   pub enum EncodedAggregateKind {
       CountStar      = 1,
       CountNonNull   = 2,
       Min            = 3,
       Max            = 4,
       SignedSum      = 5,
       UnsignedSum    = 6,
       SignedAvg      = 7,
       UnsignedAvg    = 8,
   }
   ```

9. Freeze predicate plan modes:

   ```rust
   pub enum EncodingComputePredicatePlan {
       Exact(EncodedPredicateNode),
       Prefilter {
           encoded: EncodedPredicateNode,
           residual_program: PbcProgramRef,
       },
       DenseFallback,
   }

   pub enum EncodedPredicateNode {
       Comparison(EncodedComparison),
       InList(EncodedInList),
       IsNull(EncodedColumnRef),
       IsNotNull(EncodedColumnRef),
       And(Vec<EncodedPredicateNode>),
       Or(Vec<EncodedPredicateNode>),
       Not(Box<EncodedPredicateNode>),
   }

   pub enum EncodedResidualMode {
       None,
       DensePbcAfterPrefilter,
       DenseAggregateAfterPrefilter,
   }
   ```

   `PbcProgramRef` is an `Arc<Program>` borrow; no copy.

10. Freeze predicate-atom shape:

    ```rust
    pub struct PredicateAtom {
        pub op: PredicateAtomOp,
        pub literal: Option<ColumnPredicateLiteralV1>,
    }

    pub enum PredicateAtomOp {
        Eq, Neq, Gt, Gte, Lt, Lte,
        InList,
        IsNull, IsNotNull,
    }
    ```

    `PredicateAtom` is **derived from**
    `PredicateTreeV1::Comparison` / `IsNull` / `IsNotNull`
    leaves at classifier time — it is not a new wire shape.

11. Freeze mask shape:

    ```rust
    pub enum EncodedRowMask {
        All { rows: RowCount },
        None { rows: RowCount },
        Bitmap { rows: RowCount, bytes: Vec<u8> },
        Runs { rows: RowCount, runs: Vec<SelectionRun> },
    }

    pub struct SelectionRun {
        pub first_row: RecordRowIndex,
        pub row_count: RowCount,
        pub selected: bool,
    }
    ```

    Mask byte allocation bounded by
    `EncodingComputeConfig.max_mask_bytes_per_record`.

12. Freeze evaluator entry signature:

    ```rust
    pub fn evaluate(
        encoded_bytes: &[u8],
        encoding: &BufferEncoding,
        cascade: &EncodingCascade,
        atom: &PredicateAtom,
        coercion: &LiteralCoercion,
    ) -> Result<EncodedRowMask, EncodingComputeRejectCode>;
    ```

    `LiteralCoercion` is a per-cascade pre-computed view of
    the predicate literal in encoded domain. Computed once
    at classifier time.

13. Freeze aggregate-fold request entry signature:

    ```rust
    pub fn try_fold_encoded_aggregate_request(
        request: &AggregateRequest,
        context: &EncodedAggregateContext,
        mask: Option<&EncodedRowMask>,
    ) -> Result<Option<Vec<AggregatePartial>>,
                EncodingComputeRejectCode>;
    ```

    `Ok(Some(partials))` — encoded path produced the same
    declared-order partial sequence as dense AGG.
    `Ok(None)` — valid economic fallback.
    `Err(...)` — hard reject; fail closed.

14. Freeze the **reject taxonomy** (5 codes):

    ```rust
    #[repr(u32)]
    pub enum EncodingComputeRejectCode {
        MaskLengthMismatch          = 1,
        EncodedValueOverflow        = 2,
        KernelDecodedLengthMismatch = 3,
        AggregateOverflowMismatch   = 4,
        SelectionRangeOverflow      = 5,
    }

    pub enum EncodingComputeFallbackReason {
        ReaderDisabled,
        DirectoryMissing,
        UnsupportedProgram,
        UnsupportedCascade,
        UnsupportedArrowLayout,
        DenseSelection,
        TooManyMaskBytes,
        TooManyPages,
        AggregateUnsupported,
    }
    ```

15. Freeze config:

    ```rust
    pub struct EncodingComputeConfig {
        pub enabled: bool,                                  // false
        pub max_dict_match_set_size: DictMatchSetSize,      // 4_096
        pub min_buffer_rows_for_encoded_eval:
            BufferValueCount,                               // 256
        pub max_parallel_kernel_tasks: TaskCount,           // 4
        pub max_mask_bytes_per_record: RecordBodyByteLen,   // 256 KiB
        pub max_encoded_pages_per_record: EncodingPageCount,
                                                            // 256
        pub min_selectivity_for_encoded_eval_micros:
            SelectivityFractionMicros,                      // 250_000
        pub min_prefilter_rejection_micros:
            SelectivityFractionMicros,                      // 100_000
        pub enabled_predicate_evaluators:
            BTreeSet<EncodingComputeEvaluatorKind>,         // all
        pub enabled_aggregate_kernels:
            BTreeSet<EncodedAggregateKind>,                 // all
    }
    ```

    `EncodingComputeConfig` is **deployment-wide** in V1.
    Dual economic gates per ADR §"Why dual economic gates":
    leaf-level selectivity gate + Prefilter rejection gate.

16. Freeze metric shapes per ADR §14.

17. Freeze fixtures:
    - PBC predicate against `BitPacked` int column at 1% /
      5% / 25% / 50% selectivity;
    - PBC predicate against `Rle { inner: Plain }` constant
      column;
    - PBC predicate against `Rle { inner: BitPacked }`
      run-length-grouped column;
    - PBC predicate against `Dictionary { inner: BitPacked }`
      with literal that exists in dict;
    - PBC predicate against `Dictionary { inner: BitPacked }`
      with literal that does not exist in dict (`None`
      mask short-circuit);
    - PBC predicate `InList` against
      `Dictionary { inner: BitPacked }` at small / medium /
      large match-set sizes (test
      `max_dict_match_set_size` cap);
    - PBC predicate range against
      `Dictionary { inner: Plain }` (variable-width →
      DenseFallback);
    - PBC predicate range against
      `Dictionary { inner: BitPacked }` (fixed-width →
      Exact);
    - PBC predicate equality / range against
      `Delta { .. }` (any inner; full-buffer grain — encoder
      ignores Delta inner per `encode_delta_bytes`);
    - PBC predicate against `Plain` int column
      (DenseFallback);
    - PBC predicate against `Lz4Frame` UTF-8 column
      (DenseFallback);
    - PBC predicate against `Fsst` UTF-8 column
      (DenseFallback);
    - PBC predicate against `Alp` float column
      (DenseFallback);
    - mixed `(A AND B)` where A encoded, B unsupported,
      rejection ≥ 10% → Prefilter (rejection-gate met);
    - mixed `(A AND B)` where A encoded, B unsupported,
      rejection < 10% → DenseFallback;
    - mixed `(A OR B)` where A encoded, B unsupported →
      DenseFallback;
    - `Not(encoded)` exact when child is exact;
    - `IsNull` / `IsNotNull` against encoded / null-heavy
      columns;
    - PBC predicate where buffer row count below
      `min_buffer_rows_for_encoded_eval` → leaf is
      residual;
    - PBC predicate with literal type coercion failure →
      leaf is residual;
    - corrupted encoded buffer (post-ADR-057-directory-
      validation): must produce reject code, never silent
      fallback;
    - record with one evaluator kind disabled in
      `enabled_predicate_evaluators` → fall back;
    - record with one aggregate kernel disabled in
      `enabled_aggregate_kernels` → fall back to dense
      AGG.
    - **Aggregate fold fixtures**:
      - `count_star` over BitPacked / Rle / Dict / Delta;
      - `count_non_null` over null-heavy buffers;
      - `min` / `max` over BitPacked / Rle / Dict
        (fixed-width) / Delta;
      - `sum` / `avg` over BitPacked / Rle / Dict
        (fixed-width) / Delta;
      - `sum` overflow detection on tight integer types;
      - approx aggregate request → fallback to dense AGG;
      - GROUP BY request → fallback (deferred);
      - float aggregate → fallback (deferred);
      - `count_star` with PBC predicate → mask popcount
        on encoded predicate result;
    - CSB direct-answer fast path → COCD bypassed.

18. Architectural test stubs (full guards in Phase 9):
    - exactly one definition each of plan / catalog /
      mask / config types;
    - newtype-unit definitions;
    - dispatch entry single-source in
      `src/storage/encoding/compute/dispatch.rs`;
    - CDC, recovery, rehydration, LSM compaction code
      paths do not import `EncodingCompute*` types
      directly;
    - no `EncodingCompute*V[0-9]` Rust type names;
    - no `Wendelon`-prefixed COCD symbols;
    - no new public PBC opcode in `src/schema/pbc.rs::Op`;
    - no PBC executor call from inside
      `src/storage/encoding/compute/`;
    - no descriptor field for COCD policy;
    - no envelope `format_version` bump; no ADR 022
      footer field addition;
    - ADR 057 directory shape file-digest snapshot
      unchanged;
    - CSB direct-answer ordering remains before COCD
      aggregate attempt.

Exit:

- Types and guard intent pinned; fixtures ready;
  evaluator + aggregate catalogs committed; module
  layout settled.

## Phase 1 — Mask infrastructure

Files:

- `src/storage/encoding/compute/mask.rs`
- `tests/encoding_compute_mask.rs` (new)

Work:

1. Implement `EncodedRowMask` + `SelectionRun` per Phase 0
   §11.

2. Mask validation:
   - row counts match row group length;
   - runs strictly increasing, cover no out-of-range rows;
   - bitmap byte length is `ceil(rows / 8)`;
   - empty / non-empty invariants route to
     `SelectionRangeOverflow` / `MaskLengthMismatch`.

3. Mask algebra (shape-specialized):
   - `mask_and(&mask_a, &mask_b)`:
     `All AND x = x`; `None AND x = None`;
     `Runs AND Runs`: walk both run streams, intersect;
     `Bitmap AND Bitmap`: SIMD bitwise AND;
     `Runs AND Bitmap`: convert Runs to Bitmap, then AND;
     lifts to `Bitmap` when `Runs` densifies past cap.
   - `mask_or(&mask_a, &mask_b)`: symmetric.
   - `mask_not(&mask)`: per-shape complement.
   - `mask_popcount(&mask) -> u64`: scalar count.

4. Mask conversion:
   - `to_arrow_boolean_array(&mask) -> BooleanArray` for
     handoff to projection materialization (Phase C zip)
     and Prefilter PBC re-run.
   - `from_arrow_boolean_array(arr) -> EncodedRowMask` for
     dense-path mask handoff.

5. Densification: a `Runs` mask whose serialized bitmap
   would exceed an internal cap converts to `Bitmap`; if
   `Bitmap` cap is also exceeded, the planner falls back to
   `DenseFallback` for that leaf.

Tests (`tests/encoding_compute_mask.rs`):

- `All AND Bitmap`, `None OR Runs`, `Not Bitmap`.
- Run-mask intersection with bitmap mask.
- Selected row count is exact across shapes.
- Conversion to `BooleanArray` byte-equivalent to expected
  row vector.
- Out-of-range run returns `SelectionRangeOverflow`.
- Densification preserves boolean equivalence.
- Mask-grain composition is associative / commutative.

Exit:

- COCD has a compact, bounded row-group-local selection
  representation.

## Phase 2 — Per-cascade predicate evaluators

Files:

- `src/storage/encoding/compute/kernels/validity.rs`
- `src/storage/encoding/compute/kernels/bitpacked.rs`
- `src/storage/encoding/compute/kernels/rle.rs`
- `src/storage/encoding/compute/kernels/dictionary.rs`
- `src/storage/encoding/compute/kernels/delta.rs`
- `src/storage/encoding/compute/coerce.rs`
- `src/storage/encoding/compute/null.rs`
- `tests/encoding_compute_evaluator.rs` (new)

Work:

1. V1 ships a correctness-first value evaluator in
   `dispatch.rs`: decode the encoded buffer with ADR 057's
   deterministic decoder, run the scalar predicate loop, and
   combine with validity when needed. Per-cascade files are
   label / module anchors in V1; native SIMD / run-grain /
   dictionary-match-set kernels are follow-up optimization
   work before enabling COCD by default.

2. **Validity helper** (`null.rs`):
   - Reads validity buffers for `IsNull` / `IsNotNull`.
   - Uses row-group null count for all-null / all-valid
     short-circuits (`All`/`None` masks).
   - Combines with predicate-result mask under 3VL.
   - `IsNull` / `IsNotNull` bypass value `evaluate(...)`;
     the value evaluator rejects those atoms with
     `KernelDecodedLengthMismatch`.

3. **Deferred specialized `BitPacked`** (`bitpacked.rs`):
   - Lift literal to packed domain (`literal - base`),
     bounds-check fits in `bits`. Out-of-range literal
     short-circuits to `All` (Neq) or `None` (Eq).
   - For range / equality / InList: SIMD unpack-and-compare
     when `bits` is non-aligned; direct compare against
     packed bytes when byte-aligned.
   - Page-grain dispatch via ADR 057 `page_end_offsets`.
   - Combines with validity helper for null handling.
   - Mask length validation at exit
     (`MaskLengthMismatch`).

4. **Deferred specialized `RlePlain` / `RleBitPacked`** (`rle.rs`):
   - Walk encoded run array; for each
     `(run_value, run_length)`, evaluate atom against
     `run_value` once.
   - Emit `SelectionRun` entries → output is `Runs` mask.
   - For `RleBitPacked`, `run_value` decoded from inner
     `BitPacked { bits, base }` once.
   - Buffer-grain for `RlePlain`; page-grain for
     `RleBitPacked`.
   - Mask length validation.

5. **Deferred specialized `DictionaryPlain` / `DictionaryBitPacked`**
   (`dictionary.rs`):
   - Step 1: `coerce::dict_match_set` produces match set
     `S` of dict indices satisfying the atom.
     - `|S| == 0` for `Eq` / `InList` → `None` mask
       short-circuit.
     - `|S| > max_dict_match_set_size` → return
       `LiteralCoercionMiss` to caller.
   - Step 2: walk index buffer, emit
     `mask[i] = (indices[i] ∈ S)`. Specialized fast paths:
     `|S| == 1` → SIMD direct equality;
     `|S| ≤ 4` → SIMD OR-compose direct equalities;
     `|S| > 4` → hash-set membership scan.
   - Variable-width Dictionary range atoms: classifier
     returns DenseFallback (architectural guard).
   - Fixed-width Dictionary range atoms: compare literal
     against dictionary value table once; resulting match
     set goes through Step 2.
   - Page-grain for `DictionaryBitPacked`; buffer-grain
     for `DictionaryPlain`.
   - Mask length validation.

6. **Deferred specialized `Delta`** (`delta.rs`):
   - Stream-reconstruct values from `base` plus `BitPacked`
     deltas; evaluate atom against each reconstructed
     value.
   - Full-buffer grain (no page emission for Delta).
   - Output is `Bitmap` mask.
   - Equality / null checks are trivial; range comparisons
     walk the prefix sum once. InList walks once with
     hash-set lookup per reconstructed value.
   - Mask length validation.

7. **Literal coercion** (`coerce.rs`):
   - `coerce::bitpacked_literal(literal, atom, bits, base)`
   - `coerce::rle_inner_literal(literal, atom, inner_cascade)`
   - `coerce::dict_match_set(literal, atom, dict_bytes,
     dict_value_count, fixed_width, cap)`
   - `coerce::delta_literal(literal, atom, base, inner_bits,
     inner_base)`
   - All deterministic; architectural guard.

8. All evaluators enforce parameter ranges and reject
   malformed cascades via `EncodedValueOverflow` /
   `KernelDecodedLengthMismatch`.

Tests (`tests/encoding_compute_evaluator.rs`):

- Round-trip per evaluator per atom: encoded eval result
  byte-equals materialize-then-evaluate result on every
  fixture.
- Empty match set short-circuits to `None` without
  scanning index buffer.
- Single-element match set takes the SIMD direct-equality
  fast path.
- BitPacked at every byte-aligned bit-width (8/16/32/64)
  and at non-aligned widths (e.g., 9, 17, 33).
- BitPacked with non-zero `base` (FOR offset).
- Dictionary with 1, 16, 256, 4096 distinct values; literal
  hits / misses / partial.
- Dictionary InList with match set sizes 1, 16, 4096, 4097
  (cap-exceeded path).
- Dictionary fixed-width range vs. variable-width range
  (latter rejects at coerce time).
- Rle with all-equal input and alternating input.
- Delta with monotonic / non-monotonic deltas; equality +
  range + null + InList at start / middle / end.
- Mask length always equals `decoded_value_count`.
- Evaluators are deterministic (cross-replica
  byte-equality of mask).
- Malformed encoded bytes return rejects, not fallbacks.

Exit:

- Five evaluators (plus validity helper) ready;
  round-trip vs. materialize-then-evaluate passes; rejects
  closed.

## Phase 3 — Predicate plan classifier (Exact/Prefilter/DenseFallback)

Files:

- `src/storage/encoding/compute/predicate.rs` (new)
- `src/storage/encoding/compute/dispatch.rs` (new)
- `tests/encoding_compute_predicate.rs` (new)
- `tests/encoding_compute_dispatch.rs` (new)

Work:

1. **Classifier** (`predicate.rs`):

   ```rust
   pub fn classify_predicate(
       tree: &PredicateTreeV1,
       row_groups: &[RowGroupContext],
       config: &EncodingComputeConfig,
       stats: &CsbRowGroupStats,
       directory: &EncodingDirectory,
   ) -> EncodingComputePredicatePlan;
   ```

   Walks the predicate tree, identifies leaves
   (`Comparison`, `IsNull`, `IsNotNull`), and per-leaf-per-
   row-group decides encoded vs. residual.

   Per-leaf rules (in order):
   1. Config disabled → residual.
   2. Cascade unsupported → residual.
   3. Buffer rows < `min_buffer_rows_for_encoded_eval` →
      residual.
   4. Atom unsupported on evaluator → residual.
   5. Literal coercion fails → residual.
   6. **CSB-estimated row-group selectivity >
      `min_selectivity_for_encoded_eval_micros` (0.25)**
      → residual. (Leaf-level selectivity gate; primary.)
   7. Else → encoded.

   Tree-level classification:
   - All leaves encoded → `Exact(EncodedPredicateNode)`.
   - Mixed conjunction with combined encoded-leaf rejection
     ≥ `min_prefilter_rejection_micros` (0.10) →
     `Prefilter { encoded, residual_program }`. (Secondary
     economic gate.)
   - Otherwise → `DenseFallback`.

   Aggregate folding is **not** gated by predicate
   selectivity — it has its own kernel-support checks and
   Phase 8 benchmark evidence / promotion-to-default gate.

   Pre-computes `LiteralCoercion` for encoded leaves;
   threads into `EncodedPredicateNode`.

   Determinism: identical config + identical CSB stats +
   identical predicate tree → identical plan.

2. **Dispatch entry** (`dispatch.rs`):

   ```rust
   pub fn dispatch_evaluate(
       node: &EncodedPredicateNode,
       context: &EncodedKernelContext,
   ) -> Result<EncodedRowMask, EncodingComputeRejectCode>;
   ```

   Single entry point — architectural guard: `dispatch.rs`
   contains exactly one `pub fn dispatch_evaluate`.

   `EncodedKernelContext` carries:
   - `RecordBufferDirectory`;
   - `EncodingDirectory`;
   - row-group index;
   - fetched encoded bytes by `(column, buffer)`;
   - Arrow field types;
   - config caps.

   Dispatch routes leaves to per-evaluator modules; AND /
   OR / Not nodes recursively dispatch and combine via
   mask algebra.

Tests (`tests/encoding_compute_predicate.rs`):

- Equality on BitPacked integer → Exact.
- Range on BitPacked integer → Exact.
- IN_LIST on Dictionary string → Exact.
- IS_NULL / IS_NOT_NULL → Exact.
- `encoded AND unsupported` with rejection ≥ 10% →
  Prefilter.
- `encoded AND unsupported` with rejection < 10% →
  DenseFallback.
- `encoded OR unsupported` → DenseFallback.
- `Not(encoded)` exact when child is exact.
- `Not(unsupported)` → DenseFallback.
- Float / Alp predicate → DenseFallback.
- FSST predicate → DenseFallback.
- Dictionary string range (variable-width) →
  DenseFallback.
- Dictionary fixed-width range → Exact.
- Determinism: same inputs → same plan.

Tests (`tests/encoding_compute_dispatch.rs`):

- Dispatch routes each evaluator kind correctly.
- Disabled evaluator → classifier short-circuits to
  residual leaf.
- Mask byte-equality: encoded eval vs. materialize-then-
  evaluate produces identical mask on every fixture.

Exit:

- Classifier + dispatch ready; routing rules exercised;
  mask correctness verified.

## Phase 4 — Aggregate fold kernels

Files:

- `src/storage/encoding/compute/aggregate.rs`
- `tests/encoding_compute_aggregate.rs` (new)

Work:

1. Aggregate-fold entry per Phase 0 §13.

2. Implement 8 kernels:

   - **`count_star`**: `mask.popcount()` if mask present,
     else `row_group.row_count`.
   - **`count_non_null`**: `(mask AND validity).popcount()`.
   - **`min` / `max`** for fixed-width:
     - `BitPacked`: scan packed bytes, track running
       min/max in packed domain, lift to literal at finish.
     - `Rle`: per-run check; one comparison per run; mask
       restricts which runs contribute.
     - `Dictionary { inner: BitPacked }` (fixed-width):
       walk bit-packed indices, look up referenced dict
       values, track running min/max.
     - `Delta`: streamed prefix-sum walk.
   - **`signed_sum` / `unsigned_sum`** for fixed-width
     integer only (Int8/16/32/64 for signed,
     UInt8/16/32/64 for unsigned, matching ADR 040 dense
     validation in `src/storage/aggregate.rs`
     `validate_expression_types`; no time/timestamp,
     no float, no decimal):
     - `BitPacked`: scan packed bytes; accumulate sum;
       overflow detection per ADR 040 partial-state.
     - `Rle`: per-run contribution
       `run_length * run_value`.
     - `Dictionary { inner: BitPacked }` (fixed-width
       integer): walk bit-packed indices, accumulate
       `dict_values[idx]`.
     - `Delta`: streamed prefix-sum walk.
   - **`signed_avg` / `unsigned_avg`**: integer-only by
     inheritance from sum; implemented as masked sum +
     masked count_non_null; same overflow semantics as
     sum.

3. Each kernel:
   - returns `Ok(Some(AggregatePartial))` on success;
   - returns `Ok(None)` on economic fallback
     (`AggregateUnsupported`,
     `UnsupportedArrowLayout`);
   - returns `Err(...)` on hard reject
     (`AggregateOverflowMismatch`,
     `KernelDecodedLengthMismatch`).

4. Overflow flag shape matches ADR 040's partial-state
   contract byte-for-byte.

5. Architectural guard: aggregate fold runs **after** ADR
   040 CSB direct-answer in `src/storage/aggregate.rs` /
   `src/network/data_server.rs`. Phase-9 lockdown
   enforces.

Tests (`tests/encoding_compute_aggregate.rs`):

- count_star / count_non_null / min / max / sum / avg over
  every supported cascade × dtype combination matches
  dense AGG.
- RLE aggregate multiplies by run length correctly.
- Dictionary fixed-width min/max/count matches dense AGG.
- Masked aggregate matches dense AGG with PBC filter.
- CSB direct-answer bypasses COCD (architectural-test
  pin).
- Approx aggregates → fallback.
- GROUP BY → fallback.
- Float aggregates → fallback.
- Unsigned/signed sum overflow flags match dense AGG
  byte-for-byte.
- Average overflow consistent with dense AGG.
- Disabled aggregate in `enabled_aggregate_kernels` →
  fallback.

Exit:

- 8 aggregate kernels match dense AGG byte-for-byte.

## Phase 5 — Phase B planner + AGG dispatch integration

Files:

- `src/storage/columnar.rs`
- `src/storage/encoding/compute/planner.rs` (new)
- `src/storage/manager.rs`
- `src/storage/aggregate.rs`
- `src/network/data_server.rs`
- `tests/encoding_compute_planner.rs` (new)

Work:

1. Keep the predicate planner internal to the row-group L2
   reader. V1 does not add a serialized or wire-visible
   `RowGroupFetchPlan::encoded_compute` field; the manager
   builds the Phase-B fetch plan from the parsed row-group +
   encoding directories and records only the realized
   `PhaseBOutcome` / fetch audit on the existing row-group
   read path.

   ```rust
   pub struct EncodingComputePlan {
       pub predicate_plan: EncodingComputePredicatePlan,
       pub residual_mode: EncodedResidualMode,
   }
   ```

   Aggregates use a private manager-side row-group fetch plan
   and fold through `try_fold_encoded_aggregate_request`;
   there is no public `EncodedAggregatePlan` type in V1.

2. Implement manager-side row-group Phase-B planning:
   - call classifier to derive
     `EncodingComputePredicatePlan`;
   - for `Exact`: emit fetch steps for encoded predicate
     buffers only;
   - for `Prefilter`: emit fetch steps for encoded
     predicate buffers + materialize steps for
     residual columns (narrowed by the encoded mask
     after Phase B);
   - for `DenseFallback`: existing ADR 057 path.

3. **Phase-B execution flow** in the manager L2 row-group
   read path:

   ```text
   for each surviving row group:
     match runtime predicate plan:
       Exact(node):
         fetch encoded predicate bytes;
         mask = dispatch_evaluate(node, context);
         if mask is None → skip row group;
         continue with mask for Phase C;
       Prefilter { encoded, residual_program }:
         fetch encoded predicate bytes;
         prefilter_mask = dispatch_evaluate(encoded, context);
         if prefilter_mask is None → skip row group;
         fetch residual predicate columns + projection;
         materialize via Phase D;
         apply prefilter_mask at materialize time;
         run residual_program (PBC) against prefiltered batch;
         use PBC mask for Phase C;
       Some(DenseFallback) | None:
         existing ADR 057 path;
   ```

4. PBC executor unchanged at the materialized-batch level.
   In Prefilter mode, the PBC executor sees a
   prefiltered batch and re-evaluates the original
   program. Architectural guard: PBC executor does not
   import `encoding::compute` types.

5. Phase C / Phase D unchanged from ADR 056 / 057. The
   final mask flows in exactly as before; how it was
   produced is internal to Phase B.

6. **AGG dispatch** in `aggregate.rs` /
   `data_server.rs`:

   ```text
   if csb_direct_answer.is_some():
     return csb_direct_answer;
   if config.enabled
      and predicate is COCD-supported
      and EVERY aggregate expression in the request is
          COCD-supported (all-or-nothing per V1):
     run encoded predicate;
     run encoded aggregate fold against mask;
     if Some(partial): return partial;
   fall through to dense AGG;
   ```

   **Mixed-request policy.** If any single aggregate
   expression is COCD-unsupported (e.g.,
   `SELECT COUNT(*), MIN(x), QUANTILE_KLL(y)` with KLL
   deferred), the whole request falls through to dense
   AGG. V1 does not splice encoded and dense partials
   per expression; partial-byte-equality with ADR 040
   requires one assembly path per request. Per-expression
   mixed support is a follow-up ADR opportunity.

   Architectural guard: CSB direct-answer ordering is
   pinned ahead of COCD aggregate attempt.

Tests (`tests/encoding_compute_planner.rs`):

- All-encoded query → Exact path; no materialization for
  predicate columns at all.
- All-materialized query → DenseFallback; Phase B
  byte-identical to ADR 057.
- Mixed leaves with rejection ≥ 10% → Prefilter path
  produces same combined mask as DenseFallback.
- Mixed leaves with rejection < 10% → DenseFallback
  (rejection-gate respected).
- Phase-B all-false short-circuit: row group dropped
  without Phase C.
- `enabled_predicate_evaluators` switch routes to
  residual.
- `Or(encoded, unsupported)` → DenseFallback.
- `Not(unsupported)` → DenseFallback.
- ADR 056/057 secondary-index page test still passes.
- AGG with CSB direct-answer → bypasses COCD.
- AGG without CSB direct-answer + COCD-supported predicate
  + COCD-supported aggregate → encoded fold matches dense
  AGG.
- AGG with COCD-unsupported aggregate → fallback.

Exit:

- Production read path exercises COCD on directory
  presence + config enabled; DenseFallback remains the
  existing ADR 057 path.

## Phase 6 — NULL handling + 3VL preservation

Files:

- `src/storage/encoding/compute/null.rs`
- `tests/encoding_compute_null.rs` (new)

Work:

1. Three-valued logic (3VL) preservation. PBC's
   materialize-then-evaluate path uses Arrow's compute
   kernels which apply NULL semantics correctly. COCD
   evaluators must reproduce this byte-for-byte.

2. NULL handling per atom:
   - `Eq` / `Neq` / range / `InList`: NULL → false in
     mask.
   - `IsNull`: NULL → true; non-NULL → false.
   - `IsNotNull`: inverse.

3. Each evaluator combines its predicate result with the
   buffer's validity bitmap. Combined mask is `valid_mask
   AND predicate_mask` for non-null-checking atoms;
   `valid_mask` (or its inverse) for null-checking atoms.

4. `compose_predicate_tree` walks the PBC predicate tree,
   replacing leaves with their computed masks, applying
   AND/OR/Not at internal nodes under 3VL semantics.

5. NULL handling for aggregate kernels: validity bitmap
   is consulted before contributing each value to
   running min/max/sum.

Tests (`tests/encoding_compute_null.rs`):

- Buffer with 25% nulls: encoded eval matches
  materialize-then-evaluate on every atom + every
  evaluator + every fixture.
- `IsNull` / `IsNotNull` produce correct masks against
  every cascade.
- `(x = 5 OR x IS NULL)` produces same mask whether
  encoded or materialized.
- All-null buffer: every comparison atom yields all-false
  mask; `IsNull` yields all-true.
- count_non_null over null-heavy buffer matches dense
  AGG.
- min/max/sum over null-heavy buffer matches dense AGG.

Exit:

- 3VL preserved across COCD path.

## Phase 7 — Secondary-index integration + LSM/CDC neutrality

Files:

- `src/storage/secondary_index.rs`
- `src/storage/encoding/compute/index.rs` (new)
- `src/storage/lsm_worker.rs` (no COCD imports)
- `src/storage/cdc.rs` (no COCD imports)
- `src/storage/recovery.rs` (no COCD imports)
- `src/storage/rehydration.rs` (no COCD imports)
- `tests/encoding_compute_secondary_index.rs` (new)
- `tests/encoding_compute_neutral.rs` (new)

Work:

1. Extend ADR 052 / 057 `fetch_indexed_rows`:
   - When index hit's record has Encoding directory AND
     hit's confirmation predicate column is COCD-supported
     AND `EncodingComputeConfig.enabled`:
     evaluate the predicate directly on the page bytes
     containing the target row.
   - On hit, fall back to ADR 057 page materializer.
   - On miss, drop the hit (false-positive case).

2. Architectural guard: LSM / CDC / recovery / rehydration
   modules do not import `EncodingCompute*` types.

3. LSM compaction byte-neutral: compaction reads via Phase
   D regardless of `EncodingComputeConfig.enabled`.

4. CDC subscription unchanged; PBC filter pushdown on
   subscriptions (ADR 054 follow-up) is a future consumer
   of COCD.

5. Recovery / rehydration unchanged.

Tests (`tests/encoding_compute_secondary_index.rs`):

- Single hit with confirmation predicate via
  `DictionaryBitPacked` evaluator.
- False-positive index hit: COCD evaluation drops hit.
- Index hit on Plain / Lz4 column: ADR 057 materializer
  (no COCD).
- COCD disabled: identical behavior to ADR 057 baseline.

Tests (`tests/encoding_compute_neutral.rs`):

- LSM compaction output byte-identical with COCD on/off.
- CDC subscription on COCD-enabled cluster emits byte-
  identical events vs. COCD-disabled.
- Recovery byte-neutral.
- Rehydrated L1 reads use Phase B + COCD automatically.

Exit:

- Secondary-index point reads use COCD confirm-step;
  LSM / CDC / recovery / rehydration byte-neutral.

## Phase 8 — Bench

Files:

- `benches/encoding_compute.rs` (new)
- `Cargo.toml`
- `plans/wip/058_compute_over_encoded_data_bench_results.md`
  during iteration; final lands at
  `plans/impl/058_compute_over_encoded_data_bench_results.md`
  post-promotion.

Bench scenarios:

1. **Per-evaluator predicate CPU** at 1% / 5% / 25% /
   50% selectivity, against fixed 65,536-row fixture.
2. **Phase-B materialize-then-evaluate baseline** — same
   fixtures, COCD disabled.
3. **Mask-grain AND/OR composition CPU** across
   `All`/`None`/`Bitmap`/`Runs` shape combinations.
4. **Dict-match-set size sweep**: 1 / 16 / 256 / 1024 /
   4096.
5. **Prefilter-rejection-gate sweep**: 5% / 10% / 25%.
5b. **Leaf-selectivity-gate sweep**: 0.10 / 0.25 / 0.50 —
   tunes `min_selectivity_for_encoded_eval_micros` to
   confirm the regression-defending guardrail at high
   selectivity is correctly placed.
6. **`min_buffer_rows_for_encoded_eval` sweep**: 64 /
   256 / 1024.
7. **Mixed predicate CPU**: `(A AND B)` with one encoded +
   one materialized leaf; rejection ≥ 10% (Prefilter
   engaged) vs. < 10% (DenseFallback).
8. **`(A OR B)` mixed predicate** (DenseFallback expected).
9. **Phase-B `None`-mask short-circuit**.
10. **Phase B + Phase C + Phase D end-to-end CPU**: ADR
    057-only baseline vs. ADR 058 enabled.
11. **Memory pressure** at concurrent query fan-out.
12. **Cross-replica determinism**: same config + same
    encoded bytes → byte-identical mask + aggregate
    partial.
13. **Pre-058 / post-058 mixed-slab read** throughput.
14. **Provider request-pattern leak**: page-grain
    selectivity correlation observable to L2.
15. **CSB short-circuit interaction**: confirm AGG short-
    circuit and PBC dense path are not bypassed.
16. **`Runs`-mask densification**: bench when `Runs`
    converts to `Bitmap`.
17. **count / min / max / sum / avg aggregate folds** over
    BitPacked / Rle / Dict / Delta.
18. **Masked aggregate folds** (predicate produces mask,
    fold consumes).
19. **Aggregate overflow detection**: tight integer types,
    overflow path matches ADR 040 byte-for-byte.
20. **Delta full-buffer range CPU** vs. dense materialize:
    is the kernel worth keeping enabled by default?
    (Codex iter-3 open question.)
21. **`count_non_null` from row-group stats** (CSB direct
    or near-direct) vs. COCD popcount: which wins on
    unfiltered cases?

Starting acceptance targets:

- **Correctness V1 ship gate**: mask byte-equality,
  aggregate-partial byte-equality, cross-replica determinism,
  and architecture guards pass with COCD disabled by default.
- **Promotion-to-default CPU gate**: Predicate-only Exact COCD
  ≥ 2× faster than ADR 057 dense PBC at low selectivity on
  representative `BitPacked` / `Rle` / `Dictionary` fixtures
  after specialized kernels land.
- **Encoded Prefilter wins only when it removes ≥ 10%
  rows**, or council revises the gate from evidence.
- **Aggregate kernels ≥ 2× faster than dense AGG** on
  representative `Rle` / `BitPacked` fixtures before
  promotion-to-default.
- **Dense-selection overhead ≤ 5%** vs. ADR 057 dense
  path when COCD economically falls back.
- **Mask byte-equality** vs. materialize-then-evaluate on
  every fixture (correctness gate).
- **Aggregate-partial byte-equality** vs. dense AGG on
  every fixture.
- **Cross-replica determinism**: byte-identical mask +
  aggregate-partial outputs.
- **No regression when `EncodingComputeConfig.enabled =
  false`**.
- **Phase D allocation drops ≥ 50%** on queries where
  predicate columns are not in projection.
- **ADR 057 Phase D bench scenarios** still pass with
  COCD disabled (regression check).
- **ADR 056 Phase 10 bench scenarios** still pass.
- **ADR 052 secondary-index point-reads** byte-identical
  with COCD on/off.
- **ADR 055 LSM compaction outputs** byte-identical with
  COCD on/off.
- **ADR 040 AGG result byte-identical** with COCD on/off
  (CSB direct-answer + dense AGG paths preserved
  exactly).
- **CDC, recovery, rehydration** byte-neutral.

Bench results: small / medium / large matrix per ADR 030.
Smoke artifact lands first; full matrix is the
promote-to-shipped gate.

Exit:

- Bench harness captures per-evaluator + per-aggregate CPU
  at each selectivity, mask-grain composition cost,
  mixed-leaf cost (with/without Prefilter), end-to-end
  Phase-B+C+D delta, determinism.

## Phase 9 — Architectural lockdown

Files:

- `tests/architecture_test.rs`

Guards (per ADR §16; ~27 guards):

1. Exactly one definition each of `EncodingComputeConfig`,
   `EncodingComputePredicatePlan`,
   `EncodingComputeEvaluatorKind`, `EncodedAggregateKind`,
   `EncodingComputeRejectCode`,
   `EncodingComputeFallbackReason`,
   `EncodedRowMask`, `SelectionRun`,
   `EncodedPredicateNode`, `EncodedResidualMode`,
   `EncodingComputePlan`.
2. Newtype-units for `DictMatchSetSize`,
   `PredicateLeafIndex`, `SelectivityFractionMicros`.
3. `EncodingComputeEvaluatorKind` discriminants 1..=6
   stable; `EncodedAggregateKind` discriminants 1..=8
   stable.
4. Exactly one dispatch entry point in
   `src/storage/encoding/compute/dispatch.rs`.
5. CDC, recovery, rehydration, LSM compaction code paths
   do not import `EncodingCompute*` types directly.
6. `EncodingComputeRejectCode` taxonomy closed to **five**
   V1 codes; discriminants 1..=5 stable.
7. Authentication / parse failures cannot reach any
   silent fallback branch.
8. `storage_l2_encoding_compute_*` metrics carry exactly
   the bounded labels.
9. No new `MsgType` containing `Cocd` / `Encoded` /
   `Compute`.
10. No new `FRAME_FLAG_*`.
11. No new `NodeCapability` variant.
12. No `ObjectStore` trait method addition.
13. No table descriptor field for COCD policy.
14. No envelope `format_version` bump; no ADR 022
    footer field addition.
15. No new public PBC opcode in `src/schema/pbc.rs::Op`
    (snapshot at Phase 0).
16. No PBC executor call from inside
    `src/storage/encoding/compute/`.
17. No `Wendelon`-prefixed COCD identifiers.
18. No `EncodingCompute*V[0-9]` Rust type suffixes.
19. Evaluator dispatch is not exposed as a plug-in /
    trait.
20. ADR 057 directory shape unchanged: file digests of
    `src/storage/encoding/types.rs` `EncodingDirectory`,
    `BufferEncoding`, `EncodingPageDescriptor` definitions
    match a snapshot taken before ADR 058 work begins.
21. `EncodingComputePredicatePlan` and
    `EncodingComputePlan` are internal — never serialized.
22. CSB direct-answer code path remains before COCD
    aggregate attempt in `src/storage/aggregate.rs` /
    `src/network/data_server.rs`.
23. `Fsst`, `Alp`, `Lz4Frame`, `Plain`, nested Arrow types,
    and variable-width Dictionary range predicates are
    not silently accepted by encoded kernels.
24. Mask length invariant: every evaluator's exit mask
    `rows` field equals `decoded_value_count`.
25. Dense reference parity tests exist for every target
    predicate evaluator and every target aggregate kernel
    (mask byte-equality and partial byte-equality vs.
    materialize-then-evaluate / dense AGG).
26. Mixed-aggregate-request policy is all-or-nothing in
    V1: `try_fold_encoded_aggregate_request` returns `Ok(None)`
    if any single expression in the request is
    COCD-unsupported. Test fixture exercises a request
    mixing one supported and one unsupported expression
    (e.g., `count_star` + `quantile_kll`) and verifies
    fallback to dense AGG produces a byte-identical
    `AggregatePartial` to the all-dense path.
27. COCD aggregate kernels enforce ADR 040's
    integer-only sum/avg type domain (no time/timestamp,
    no float, no decimal). A type-domain test exercises
    a `signed_sum` request against a Timestamp column
    and verifies fallback to dense AGG (which itself
    rejects with `AggRejectCode::TypeUnsupported` per
    `validate_expression_types`).

Exit:

- Future COCD edits cannot drift the substrate silently.

## Verification gate

```sh
cargo test --lib storage::encoding::compute
cargo test --test encoding_compute_mask --quiet
cargo test --test encoding_compute_evaluator --quiet
cargo test --test encoding_compute_predicate --quiet
cargo test --test encoding_compute_dispatch --quiet
cargo test --test encoding_compute_aggregate --quiet
cargo test --test encoding_compute_null --quiet
cargo test --test encoding_compute_planner --quiet
cargo test --test encoding_compute_secondary_index --quiet
cargo test --test encoding_compute_neutral --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench encoding_compute --no-run
cargo bench --bench encoding_compute -- --quick
cargo check --quiet
cargo check --benches --quiet
git diff --check
cargo fmt --check
```

Promotion-to-shipped gate (after Phase 1–9 land):

- All Phase 1–9 golden vectors pass.
- Architectural lockdown tests pass.
- Bench small / medium results pass starting acceptance
  targets (or council explicitly revises them per
  Provisional discipline).
- ADR 057 Phase 10 bench scenarios still pass with
  `EncodingComputeConfig.enabled = false` (regression
  check).
- ADR 056 Phase 10 bench scenarios still pass.
- ADR 044 column-fetch scenarios pass end-to-end with
  mixed-shape slab corpora.
- ADR 052 secondary-index point-reads byte-identical
  with COCD on / off.
- ADR 055 LSM compaction outputs byte-identical with
  COCD on / off.
- ADR 040 AGG result byte-identical with COCD on / off.
- CDC, recovery, rehydration byte-neutrality.
- Operator runbook
  `plans/ops/runbooks/encoding_compute.md` finalized.

## File-by-file expected edits

(Phase 0 audits actual paths.)

- `src/schema/units.rs` — extend (~30 LOC for 3 newtype-
  units).
- `src/storage/encoding/compute/mod.rs` — new (~300 LOC).
- `src/storage/encoding/compute/dispatch.rs` — new (~120
  LOC).
- `src/storage/encoding/compute/predicate.rs` — new (~400
  LOC).
- `src/storage/encoding/compute/mask.rs` — new (~250 LOC).
- `src/storage/encoding/compute/coerce.rs` — new (~280
  LOC).
- `src/storage/encoding/compute/null.rs` — new (~120 LOC).
- `src/storage/encoding/compute/kernels/mod.rs` — new
  (~50 LOC dispatch).
- `src/storage/encoding/compute/kernels/validity.rs` —
  new (~150 LOC).
- `src/storage/encoding/compute/kernels/bitpacked.rs` —
  new V1 label shim; specialized SIMD kernel deferred.
- `src/storage/encoding/compute/kernels/rle.rs` — new
  V1 label shim; specialized run-grain kernel deferred.
- `src/storage/encoding/compute/kernels/dictionary.rs` —
  new V1 label shim; specialized match-set kernel deferred.
- `src/storage/encoding/compute/kernels/delta.rs` — new
  V1 label shim; specialized prefix-sum kernel deferred.
- `src/storage/encoding/compute/aggregate.rs` — new (~600
  LOC: 8 kernels × supported cascades).
- `src/storage/encoding/compute/planner.rs` — new (~250
  LOC).
- `src/storage/encoding/compute/index.rs` — new (~120
  LOC).
- `src/storage/columnar.rs` — extend (~150 LOC for Phase B
  COCD plan integration).
- `src/storage/manager.rs` — touched (~120 LOC for
  Exact / Prefilter / DenseFallback dispatch).
- `src/storage/aggregate.rs` — extend (~80 LOC for COCD
  fold ordering).
- `src/storage/secondary_index.rs` — extend (~80 LOC).
- `src/storage/lsm_worker.rs` — guard test only.
- `src/storage/cdc.rs` — guard test only.
- `src/storage/recovery.rs` — guard test only.
- `src/storage/rehydration.rs` — guard test only.
- `src/network/data_server.rs` — touched (~40 LOC AGG
  ordering hook).
- `src/config.rs` — extend (~40 LOC).
- `src/metrics/registry.rs` — extend (~140 LOC).
- `src/audit.rs` — extend (~30 LOC).
- `tests/architecture_test.rs` — extend (~270 LOC, ~27
  guards).
- 8 new test files (~200–500 LOC each).
- `benches/encoding_compute.rs` — new (~700 LOC).
- `plans/ops/runbooks/encoding_compute.md` — new (~200
  lines).

Total ≈ 5500–6500 LOC. Mostly net-new in
`src/storage/encoding/compute/`; existing surfaces touched
lightly to wire Phase B dispatch, AGG ordering, and
secondary-index confirm-step.

## V1 design summary

ADR 058 ships the COCD substrate:

- **Node-local `EncodingComputeConfig`** as activation
  surface; deployment-wide for cross-replica determinism.
- **Closed predicate-evaluator catalog**: 6 evaluators
  across 4 cascade families.
- **Closed aggregate-kernel catalog**: 8 kernels for
  `count_star` / `count_non_null` / `min` / `max` / `sum`
  / `avg`.
- **No new public PBC opcodes** — Toron-visible PBC
  contract byte-identical.
- **Exact / Prefilter / DenseFallback predicate plan
  modes**: classifier picks per query.
- **Page-grain dispatch default** for page-emitting
  cascades; buffer-grain for the rest; full-buffer grain
  for Delta.
- **`EncodedRowMask` tagged enum**: `All` / `None` /
  `Bitmap` / `Runs` shapes; mask byte allocation bounded.
- **NULL semantics preserved (3VL)** at evaluator
  boundary by combining with validity bitmap.
- **Dual economic gates**: leaf-level selectivity gate at
  0.25 (every encoded predicate leaf, including Exact-
  mode leaves, requires CSB-estimated row-group
  selectivity ≤ 0.25; selectivity > 0.25 routes to
  `DenseFallback`) plus secondary Prefilter
  rejection gate at 0.10 (encoded leaves must reject
  ≥ 10% of rows for Prefilter mode to engage).
- **Aggregate folding is not gated by predicate
  selectivity** — has its own kernel-support checks and
  Phase 8 benchmark evidence / promotion-to-default gate.
- **CSB direct-answer ordering preserved** ahead of COCD
  aggregate fold.
- **ADR 057 directory unchanged**: COCD reads existing
  `BufferEncoding` / `EncodingCascade` shape.
- **ADR 052 secondary-index confirm-step accelerated**
  for COCD-supported cascades.
- **ADR 055 LSM, ADR 054 CDC, recovery, rehydration
  byte-neutral**.
- **ADR 040 AGG byte-identical** — CSB direct-answer
  ordering preserved.
- **Authentication / parse failures route to storage-
  error path**, never silent fallback.
- **Architectural lockdown** ~27 guards.
- **Approx aggregates / KLL/HLL / count_distinct / float
  aggregates / GROUP BY / Fsst / Alp deferred to
  follow-up ADRs.**

## Deferred to follow-up ADRs

- Public encoded PBC opcodes plus Toron query-session
  capability.
- Fsst evaluator.
- Alp evaluator.
- Per-page prefix-sum side-table for Delta range page-
  grain pruning.
- Page-level min/max/null stats for predicate short-
  circuit (ADR 039 follow-up).
- Encoded-grain AND/OR composition.
- Sparse-output narrowing in Phase C beyond Prefilter.
- Per-row partial evaluation within SIMD pages.
- `EncodingComputeCache` across queries.
- Adaptive dispatch routing.
- `InList` high-cardinality specialization.
- `Like` evaluator over Fsst / Dictionary.
- Grouped aggregate COCD (ADR 040 follow-up).
- HLL / KLL encoded fold kernels.
- Exact `count_distinct` over Dictionary.
- Float / decimal aggregate kernels.
- String / binary `min` / `max` from sorted Dictionary.
- Cascade-aware AGG short-circuit beyond CSB direct
  answer.
- CDC-side predicate filtering on subscriptions
  (ADR 054 follow-up).
- Wire-tier COCD (ADR 053 extension).
- ML-based dispatch routing.
- Public Toron-visible cascade-hint opcode.
