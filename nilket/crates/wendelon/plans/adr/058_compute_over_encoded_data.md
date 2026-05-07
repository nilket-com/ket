# ADR 058: Compute Over Encoded Data (COCD)

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/058_compute_over_encoded_data.md`

## Status

Council-converged and design-promoted. Implementation
phases pending; full small / medium / large bench evidence
is the promotion-to-shipped gate.

Innermost layer of the storage hierarchy that ADR 056 (row
groups) and ADR 057 (per-buffer pages + encoding cascades)
established. Opens on top of ADR 057's Phase 10 bench
artifact (`plans/impl/057_per_buffer_pages_bench_results.md`),
which fixes the cascade catalog, page grain, and selectivity
profiles COCD's dispatch design depends on.

The council process: six iterations between Claude, Codex,
and Gemini. Load-bearing forks resolved across iterations:
aggregate folding inclusion in V1 (deferred → included);
predicate plan-mode taxonomy (mask-grain → Exact / Prefilter
/ DenseFallback explicit modes); economic gating (single
selectivity gate → dual gates: leaf-level selectivity +
Prefilter rejection); Delta atom set (eq-only → full atom
set at full-buffer grain). Cross-author convergence achieved:
every surface 3-of-3 agreement.

V1 substrate commitments:

- substrate-only scope: per-row-group-buffer encoded
  predicate evaluation routed through Phase B of the ADR
  056 / 057 fetch planner, plus encoded global-aggregate
  folding routed after ADR 040's CSB direct-answer
  ordering;
- **closed predicate evaluator catalog (6 kinds)**:
  `BitPacked`, `Rle { inner: Plain | BitPacked }`,
  `Dictionary { inner: Plain | BitPacked }`,
  `Delta { inner: BitPacked }` (full atom set, full-buffer
  grain). `Plain`, `Lz4Frame`, `Fsst`, `Alp` cascades fall
  back to materialize-then-evaluate;
- **closed predicate-atom catalog**: `Eq`, `Neq`, `Gt`,
  `Gte`, `Lt`, `Lte`, `InList`, `IsNull`, `IsNotNull`.
  Logical composition (`And`, `Or`, `Not`) handled through
  the predicate plan-mode classifier;
- **closed aggregate-kernel catalog (8 kernels)**:
  `count_star`, `count_non_null`, `min`, `max`,
  `signed_sum`, `unsigned_sum`, `signed_avg`,
  `unsigned_avg`. `min`/`max` target fixed-width integer
  / time / Boolean and fixed-width Dictionary cascades;
  sum/avg are integer-only (no time/float/decimal),
  matching ADR 040 dense aggregate validation;
- **no new public PBC opcodes.** Toron-visible PBC contract
  (ADR 035 / ADR 036) byte-identical;
- **node-local `EncodingComputeConfig`** as activation
  surface (sibling to ADR 057's `EncodingWriterConfig` /
  `L2EncodingFetchConfig`); deployment-wide. No descriptor
  field, no `NodeCapability` bit;
- **Exact / Prefilter / DenseFallback predicate plan
  modes**:
  - **Exact**: every predicate leaf is targetable; PBC tree
    composes through mask algebra; PBC executor not
    invoked post-Phase-B;
  - **Prefilter**: conjunction (AND tree) with at least
    one encoded leaf and at least one residual leaf.
    Encoded leaves drop rows that cannot match the
    conjunction. Original PBC program re-runs against
    surviving rows. NOT and OR with a residual branch
    are not eligible for Prefilter;
  - **DenseFallback**: existing ADR 057 path runs
    unchanged;
- **dual economic gates**:
  - **Primary leaf-level selectivity gate**:
    `min_selectivity_for_encoded_eval_micros = 250_000`
    (0.25). Every encoded predicate leaf, including Exact-
    mode leaves, requires CSB-estimated row-group
    selectivity ≤ 0.25. Above the gate (selectivity >
    0.25), the leaf routes to `DenseFallback`
    because dense reuses the predicate fetch as
    projection input.
  - **Secondary Prefilter rejection gate**:
    `min_prefilter_rejection_micros = 100_000` (0.10).
    Prefilter mode requires CSB stats to estimate the
    encoded leaves drop ≥ 10% of rows.
  - Aggregate folding is **not** gated by predicate
    selectivity;
- **Page-grain dispatch default** for page-emitting
  cascades (`BitPacked`, `Rle { inner: BitPacked }`,
  `Dictionary { inner: BitPacked }`); buffer-grain for
  `Rle { inner: Plain }`, `Dictionary { inner: Plain }`;
  full-buffer grain for `Delta` (no page emission in ADR
  057);
- **`EncodedRowMask` tagged enum**: `All { rows }` /
  `None { rows }` / `Bitmap { rows, bytes }` /
  `Runs { rows, runs }`. RLE-friendly, all/none short-
  circuits, mask byte allocation bounded;
- **closed 5-code `EncodingComputeRejectCode` taxonomy**:
  `MaskLengthMismatch`, `EncodedValueOverflow`,
  `KernelDecodedLengthMismatch`,
  `AggregateOverflowMismatch`, `SelectionRangeOverflow`;
- **closed 9-variant `EncodingComputeFallbackReason` enum**:
  `ReaderDisabled`, `DirectoryMissing`,
  `UnsupportedProgram`, `UnsupportedCascade`,
  `UnsupportedArrowLayout`, `DenseSelection`,
  `TooManyMaskBytes`, `TooManyPages`, `AggregateUnsupported`;
- bounded `storage_l2_encoding_compute_*` metric family
  with `{decision, reason, kernel, cascade, kind,
  aggregate, violation_kind}` enumerations;
- **reader-CPU evidence gate before default enablement**:
  the checked-in bench harness must report predicate-only,
  mask-algebra, and aggregate-fold timings. The disabled V1
  ship gate is correctness and byte-equality; ≥ 2× CPU wins
  on representative `BitPacked` / `Rle` / `Dictionary`
  fixtures are the promotion-to-default bar for specialized
  cascade kernels. Provisional discipline still applies (ADR
  056's ≤ 15% miss codified the lesson; ADR 057's ≤ 75%
  inheritance preserved it);
- pinned V1 defaults: `max_dict_match_set_size = 4_096`,
  `min_buffer_rows_for_encoded_eval = 256`,
  `max_parallel_kernel_tasks = 4`,
  `max_mask_bytes_per_record = 256 KiB`,
  `max_encoded_pages_per_record = 256`,
  `min_selectivity_for_encoded_eval_micros = 250_000`,
  `min_prefilter_rejection_micros = 100_000`;
- no new wire `MsgType`, no new `FRAME_FLAG_*`, no new
  `NodeCapability` bit, no Cargo feature, no new digest
  domain, no `ObjectStore` trait change, no envelope
  `format_version` bump, no ADR 022 footer extension, no
  descriptor surface change, no public Toron-visible PBC
  opcode addition, no ADR 057 cascade catalog extension,
  no ADR 057 directory shape extension.

## Context

ADR 057 landed the encoding substrate: per-row-group-buffer
cascades (`Plain`, `Lz4Frame`, `BitPacked`, `Rle`,
`Dictionary`, `Delta`, `Fsst`, `Alp`), per-buffer SIMD page
directory, custom partial-buffer materializer. Phase 10 bench
shows density and point-read wins are real — 2.5× / 3.6× /
4.2× compression on int / Dictionary / Delta typed fixtures,
≥ 16× point-read selected-byte reduction, full-row-group
materialize 39–45% faster than ADR 056-only.

ADR 057 is deliberately **decode-only**. PBC still evaluates
against dense Arrow `RecordBatch` values. AGG still runs
through ADR 040's CSB direct-answer first and the dense
aggregate executor when direct-answer cannot respond. Three
costs remain on the hot analytical path:

1. **Predicate-only column materialization.** A
   Dictionary-encoded `status` column with 4 distinct values
   is materialized to N rows so PBC can compare each row to
   `'ACTIVE'`. The right answer: evaluate the predicate once
   against the dictionary value table (4 comparisons), find
   the matching index, run integer-equality against the
   bit-packed indices stream (one SIMD pass per page).
   Phase-B work is decoded-value-count-bound; encoded eval
   is dictionary-size-bound.
2. **RLE run-grain predicate evaluation.** A run-length-
   encoded `__row_routing_locality__` column with three runs
   answers any predicate against three `(run_value,
   run_length)` pairs.
3. **Dense aggregate folding for filtered globals.** A
   `COUNT(*) WHERE status = 'ACTIVE'` query reduces to "count
   rows where dict index ∈ match set" — popcount of a mask
   combined with the bit-packed equality test. ADR 040 CSB
   direct-answer handles unfiltered; ADR 058 handles
   filtered.

The Toron-pivot rationale (roadmap §"Toron-pivot stretch"):
density × point-read latency × predicate-and-aggregate-on-
encoded compose multiplicatively for analytical scans. ADR
056 settled the row axis; ADR 057 settled the intra-buffer
axis; ADR 058 settles the compute axis.

Constraints carry forward unchanged: ADR 003, ADR 016, ADR 020,
ADR 022, ADR 029, ADR 030, ADR 035, ADR 036, ADR 037, ADR 039,
ADR 040, ADR 042, ADR 043, ADR 044, ADR 052, ADR 053, ADR 054,
ADR 055, ADR 056, ADR 057.

## Architectural inextricability

Three design choices are physically inseparable here:

**1. Predicate evaluation + aggregate folding + cascade
dispatch + mask infrastructure = one substrate.** Predicate
evaluation produces masks; aggregate folding consumes masks;
cascade dispatch is shared. A predicate substrate without
aggregates leaves the AGG executor materializing what the
mask already implies; an aggregate substrate without
predicates leaves the AGG executor materializing predicate
columns it never needed. The four ship together.

**2. COCD is internal executor dispatch, not new public PBC
opcodes.** ADR 036 §11 and §7: new public opcodes are a Toron
contract concern; COCD is a storage physical plan choice. The
PBC program Toron emits is byte-identical with or without
COCD; the executor chooses internally. No `EVAL_DICT` /
`EVAL_RLE` / `EVAL_BITPACKED_RANGE` opcode addition; no
version-byte bump. 3-of-3 council convergence.

**3. The encoded-evaluator surface is a closed dispatch table,
not a plug-in trait.** Same closed-codec-catalog argument
ADR 043 / 053 / 057 commit to. Adding a new evaluator kind,
atom support expansion, or aggregate kernel requires a
follow-up ADR.

## Identity-vs-Locality framing

The encoded compute substrate is **identity-axis**:

- The evaluation unit is `(SlabId, RecordLocation, RowGroupIndex,
  IpcColumnOrdinal, IpcBufferIndex, EncodingPageIndex)` — all
  identity-bound coordinates inherited from ADR 056 / ADR 057.
- The output of predicate evaluation is a per-row-group
  `EncodedRowMask` that the planner composes with sibling
  predicate masks under PBC tree shape (Exact mode) or hands
  to dense PBC for residual evaluation (Prefilter mode).
- The output of aggregate folding is an
  `AggregatePartial` byte-identical to what the dense AGG
  executor would produce given the same input rows.
- Encoded-eval mask production is **byte-identical** to the
  mask the materialize-then-evaluate path would produce on
  the same predicate atom + same encoded bytes. Aggregate
  fold output is byte-identical to dense AGG. Cross-replica
  determinism follows directly.
- Unified Ring rule applies. There is no separate
  "compute plane" — encoded evaluators run in the same
  storage-read surface as ADR 057's Phase-D materializer.
- Readers (PBC executor at the materialized-batch level, AGG,
  secondary-index lookup paths) consume dense
  `ProjectedRecordBatch` values and `AggregatePartial`
  scalars regardless of dispatch leg.
- Cross-locality compaction does not apply — per-record
  substrate, downstream of admission, upstream of ADR 055's
  per-locality compactor.

## V1 scope

ADR 058 ships **only** the COCD substrate:

1. Closed predicate evaluator catalog over six kinds:
   - `BitPacked { bits, base }` — full atom set, page-grain;
   - `Rle { inner: Plain }` — full atom set, buffer-grain;
   - `Rle { inner: BitPacked }` — full atom set, page-grain;
   - `Dictionary { inner: Plain }` — full atom set,
     buffer-grain; range atoms accepted on fixed-width only;
   - `Dictionary { inner: BitPacked }` — full atom set,
     page-grain; range atoms accepted on fixed-width only;
   - `Delta { inner: BitPacked }` — full atom set,
     full-buffer grain (no page emission for Delta).

2. Closed predicate-atom catalog:
   `Eq`, `Neq`, `Gt`, `Gte`, `Lt`, `Lte`, `InList`, `IsNull`,
   `IsNotNull`. Logical composition handled through plan-mode
   classifier.

3. Closed aggregate-kernel catalog (8 kernels):
   `count_star`, `count_non_null`, `min`, `max`,
   `signed_sum`, `unsigned_sum`, `signed_avg`, `unsigned_avg`.
   `min`/`max` target fixed-width integer / time / Boolean
   and fixed-width Dictionary cascades; sum/avg are
   integer-only (Int8/16/32/64 for signed, UInt8/16/32/64
   for unsigned), matching ADR 040 dense aggregate
   validation exactly. Aggregate folding runs after ADR 040
   CSB direct-answer ordering.

4. Phase-B planner extension that classifies the predicate
   into Exact / Prefilter / DenseFallback and dispatches
   accordingly. ADR 040 AGG dispatch extension that attempts
   encoded fold after CSB direct-answer.

5. `EncodedRowMask` tagged enum for row-group-local
   selection; bounded by `max_mask_bytes_per_record`;
   convertible to/from Arrow `BooleanArray`.

6. ADR 052 secondary-index point-read confirm-step via
   encoded eval (defensive false-positive rejection on
   probabilistic-filter false-positive cases).

7. Bounded `storage_l2_encoding_compute_*` metric family.

8. Architectural lockdown.

It deliberately does **not** ship:

- new public Toron-visible PBC opcodes;
- `Fsst` / `Alp` cascade encoded evaluation — symbol-table
  search and ALP exception-stream walking are non-trivial
  evaluators that need their own focused proofs;
- arbitrary-depth cascade nesting (V1 inherits ADR 057's
  one-level-deep composition);
- `Plain` / `Lz4Frame` cascade encoded evaluation;
- nested / list / struct Arrow type predicate evaluation;
- variable-width Dictionary range predicates;
- public Toron-visible cascade-hint opcode;
- adaptive cascade re-selection;
- per-tenant evaluator tuning surface;
- learned / ML evaluator dispatch;
- a `EncodingComputeCache` across queries;
- approximate aggregates (`approx_distinct`, KLL/HLL);
- exact `count_distinct`;
- float / decimal aggregates;
- string / binary `min` / `max` from unordered Dictionary
  or FSST;
- grouped aggregation;
- mixed-cascade encoded-grain composition (the planner
  composes at mask grain, not encoded grain);
- per-row partial evaluation within a SIMD page;
- bulk slab-level COCD plan pre-fetch;
- sparse-output narrowing in Phase C beyond Prefilter
  (deferred follow-up that generalizes ADR 057's
  secondary-index page fast path);
- Delta page side-tables for page-grain range pruning.

## Non-goals (V1)

- **No descriptor field for COCD policy.** No
  `RecordBodyCompressionPolicy::EncodedCompute` variant.
- **No envelope `format_version` bump.**
- **No ADR 022 footer extension.**
- **No ADR 057 directory shape extension.**
- **No `SlabRecordHeader` change.**
- **No new `NodeCapability` bit.**
- **No new public HRFT frame family.**
- **No new wire-protocol surface to Toron.**
- **No `ObjectStore` trait change.**
- **No partial-slab rehydration.**
- **No new compression codec.**
- **No new cipher / nonce / AAD shape.** ADR 020
  byte-identical.
- **No new digest domain.**
- **No new public PBC opcode addition.**
- **No COCD on the wire-tier transport.** ADR 053
  unchanged.
- **No Vortex / Lance / DataFusion external dependency.**
- **No SQL / DDL surface.**
- **No reserved evaluator slots, compatibility shims, or
  numeric-version Rust type suffixes.**
- **No project-name prefix on new symbols.**
- **No AGG wire-envelope change.** ADR 040 byte-identical.
- **No GROUP BY.**
- **No dense fallback after kernel reject.** Invariant
  violations fail closed.

## Decision

### 1. Node-local executor config

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

V1 deployment: ships with `enabled = false`; flipped on after
Phase 8 bench acceptance. Cross-replica determinism (mask +
aggregate-partial byte-equality given identical config +
identical encoded bytes) holds trivially under deployment-
wide config.

`enabled_predicate_evaluators` and `enabled_aggregate_kernels`
are operator kill-switches: if a kernel is suspected of
producing wrong masks or wrong partials, operators flip its
bit off and the planner falls back to materialize-then-
evaluate / dense AGG for that kernel. Sets are closed to
V1's six predicate evaluators and eight aggregate kernels.

The reader does not need a node config probe per record:
directory presence (ADR 057) plus cascade kind plus
`EncodingComputeConfig.enabled` selects the path. There is no
`EncodingComputeFetchConfig` and no
`RecordBodyCompressionPolicy::EncodedCompute`.

### 2. Closed predicate evaluator catalog

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

Cascade-shape → evaluator-kind mapping:

```text
Plain                              → DenseFallback
Lz4Frame                           → DenseFallback
BitPacked                          → BitPacked            (page-grain)
Rle { inner: Plain }               → RlePlain             (buffer-grain)
Rle { inner: BitPacked }           → RleBitPacked         (page-grain)
Rle { inner: Lz4Frame }            → DenseFallback
Dictionary { inner: Plain }        → DictionaryPlain      (buffer-grain)
Dictionary { inner: BitPacked }    → DictionaryBitPacked  (page-grain)
Dictionary { inner: Lz4Frame }     → DenseFallback
Delta { inner: BitPacked }         → Delta                (full-buffer)
Delta { inner: Plain | Lz4Frame }  → DenseFallback
Fsst                               → DenseFallback
Alp                                → DenseFallback
```

Predicate-atom support:

| Evaluator                | Eq | Neq | Range | InList | IsNull | IsNotNull |
| ------------------------ |:--:|:---:|:-----:|:------:|:------:|:---------:|
| BitPacked                | ✓  |  ✓  |   ✓   |   ✓    |   ✓    |     ✓     |
| RlePlain                 | ✓  |  ✓  |   ✓   |   ✓    |   ✓    |     ✓     |
| RleBitPacked             | ✓  |  ✓  |   ✓   |   ✓    |   ✓    |     ✓     |
| DictionaryPlain          | ✓  |  ✓  | fixed-width only | ✓ | ✓ |     ✓     |
| DictionaryBitPacked      | ✓  |  ✓  | fixed-width only | ✓ | ✓ |     ✓     |
| Delta                    | ✓  |  ✓  |   ✓ (full-buffer) |   ✓    |   ✓    |     ✓     |

`Range` covers `Gt`, `Gte`, `Lt`, `Lte`. Variable-width
Dictionary range predicates fall back (Dictionary order is
first-encounter, not sorted).

Delta range predicates do not use page side tables — the
kernel reconstructs the running value in a streaming loop,
compares to the literal, emits a `Bitmap` mask. No page-grain
pruning. Phase 8 bench decides whether Delta range stays
enabled by default.

Closed — adding a new evaluator kind, atom support
expansion, or fixed-width-vs-variable-width range expansion
requires a follow-up ADR. Discriminants 1..=6 stable.

### 3. Closed aggregate kernel catalog

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

Mapping to existing ADR 040 `AggregateFunction`:

```text
CountStar      → AggregateFunction::CountStar
CountNonNull   → AggregateFunction::CountNonNull
Min            → AggregateFunction::Min
Max            → AggregateFunction::Max
SignedSum      → AggregateFunction::SignedSum
UnsignedSum    → AggregateFunction::UnsignedSum
SignedAvg      → AggregateFunction::SignedAverageParts
UnsignedAvg    → AggregateFunction::UnsignedAverageParts
```

Kernel applicability (COCD kernels never exceed ADR 040
dense validation; dense-supported shapes that COCD does not
yet cover — Utf8 / Binary `min`/`max` for example — fall
back to dense AGG via the all-or-nothing mixed-request
policy in §8):

```text
count_star      → mask popcount, or row-group row count
                  without mask
count_non_null  → popcount(mask AND validity)
min/max         → fixed-width integer/time/Boolean and
                  fixed-width Dictionary; variable-width
                  Dictionary deferred (unordered)
signed_sum      → fixed-width signed integer
                  (Int8/16/32/64) only
unsigned_sum    → fixed-width unsigned integer
                  (UInt8/16/32/64) only
signed_avg      → fixed-width signed integer only;
                  signed_sum + count_non_null partial shape
unsigned_avg    → fixed-width unsigned integer only;
                  unsigned_sum + count_non_null partial
                  shape
```

Sum and average kernels are integer-only by inheritance
from ADR 040's dense aggregate validation
(`src/storage/aggregate.rs` `validate_expression_types`):
no time/timestamp, no float, no decimal. Min/max in COCD V1
covers fixed-width integer / time / Boolean and fixed-width
Dictionary — a strict subset of ADR 040's dense min/max
surface, which also accepts Utf8 / Binary.

Two distinct deferral classes:

- **Dense-supported but COCD-deferred (fall back, don't
  reject).** ADR 040's dense surface already accepts
  Utf8 / Binary `min`/`max` (per
  `agg_arrow_type_tag(.., true)` in
  `src/storage/aggregate.rs::validate_expression_types`)
  and `APPROX_DISTINCT` / `QUANTILE_KLL` (per
  `src/schema/aggregate.rs::AggregateFunctionTag`); COCD V1
  simply does not encode-fold them yet. The COCD planner
  returns `Ok(None)` for these expressions and the request
  falls back to dense AGG. Exact-only sessions can still
  reject `APPROX_DISTINCT` / `QUANTILE_KLL` via the
  existing `ApproxNotAllowed` policy ahead of dispatch
  (unchanged from ADR 040). Follow-up ADRs may add COCD
  min/max kernels for sorted-Dictionary or Fsst once those
  evaluators land, plus encoded-grain HLL / KLL fold
  kernels.
- **Not in ADR 040 dense surface yet.** Exact
  `count_distinct`, float/decimal aggregates,
  time/timestamp sum/avg, and grouped aggregation all
  require an ADR 040 expansion ADR first; COCD inherits
  the expansion when it lands.

Aggregate ordering:

```text
1. ADR 040 CSB direct-answer runs first.
2. If a PBC predicate is present, COCD may produce an Exact
   mask or a Prefilter mask before AGG.
3. COCD attempts encoded aggregate folding using the mask
   from step 2, or an All mask for unfiltered aggregate
   requests.
4. Unsupported aggregate shapes fall back to dense ADR 057
   materialization plus existing AGG execution.
```

Overflow behavior matches ADR 040 dense partial-state
byte-for-byte. A kernel that cannot preserve the dense
overflow result falls back before contributing a partial.
A kernel that produces an inconsistent overflow state
rejects (`AggregateOverflowMismatch`).

### 4. `EncodedRowMask`

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

Mask shape is chosen by the evaluator: `RleBitPacked` /
`RlePlain` emit `Runs` natively; `BitPacked` /
`DictionaryBitPacked` emit `Bitmap`; `All` / `None`
short-circuit on disjoint literals; `Delta` emits `Bitmap`.
Mask byte allocation bounded by `max_mask_bytes_per_record`;
if a `Runs` mask densifies past the cap, it converts to
`Bitmap`.

Mask algebra is shape-specialized:

```text
All AND x      → x
None AND x     → None
All OR x       → All
None OR x      → x
NOT All        → None
NOT None       → All
Bitmap op Bitmap → bytewise op
Runs op Runs     → run algebra
Runs op Bitmap   → bounded conversion to Bitmap
```

Every mask carries `rows`; every evaluator exit validates
that `rows` equals the ADR 057 `decoded_value_count` for
the target buffer (`MaskLengthMismatch` reject otherwise).

### 5. Predicate plan modes

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
```

Classification rules:

```text
Exact:
  every leaf has an encoded kernel and Boolean
  composition can be preserved exactly. Mask AND / OR /
  NOT compose under PBC tree shape. PBC executor not
  invoked post-Phase-B; ADR 056's PBC re-evaluation
  discipline preserved.

Prefilter:
  conjunction (AND tree) with at least one encoded
  leaf and at least one residual leaf. The encoded
  leaves drop rows that cannot match the conjunction.
  Original PBC program re-runs against the surviving
  rows — exact output is preserved because PBC against
  a strict subset of rows produces a strict subset of
  the unfiltered PBC output. NOT and OR with a
  residual branch are NOT eligible:
  - OR(encoded, residual) — an encoded subset cannot
    disprove a residual branch.
  - NOT(residual) — three-valued logic complicates
    NOT semantics on unknown.

DenseFallback:
  unsupported predicate shape (top-level OR with an
  unsupported branch, NOT over an unsupported branch,
  arbitrary nested shapes). Existing ADR 057 path
  runs unchanged.
```

Per-leaf routing (within the encoded subset of the tree):

1. Look up `BufferEncoding` for the predicate column.
2. Apply rules:
   - Config disabled → `DenseFallback` (entire tree
     becomes DenseFallback).
   - Cascade unsupported → leaf is residual.
   - Buffer rows < `min_buffer_rows_for_encoded_eval` →
     leaf is residual.
   - Atom unsupported on evaluator → leaf is residual.
   - Literal coercion fails → leaf is residual.
   - **Estimated row-group selectivity from CSB stats >
     `min_selectivity_for_encoded_eval_micros` (default
     0.25) → leaf is residual.** At high selectivity the
     encoded path produces a mostly-true mask but the
     projection fetch still pulls most rows; dense
     materialize-then-evaluate (which reuses the predicate
     fetch as projection input) wins.
   - Else → leaf is encoded.
3. After per-leaf routing, classify the tree:
   - All leaves encoded → Exact.
   - Some leaves encoded, some residual, only AND
     composition along the encoded↔residual boundary →
     Prefilter (with rejection-gate check).
   - Otherwise → DenseFallback.

**Primary leaf-level selectivity gate.** Every encoded
predicate leaf, including Exact-mode leaves, requires
estimated row-group selectivity ≤
`min_selectivity_for_encoded_eval_micros` (default 0.25).
This gate handles the high-selectivity regression case:
encoded eval produces a mostly-true mask while the
projection fetch is still mostly-everything, so dense
materialize-then-evaluate wins overall by reusing one
fetch for both predicate and projection.

**Secondary Prefilter rejection gate.** Prefilter mode
engages only when CSB stats estimate the encoded leaves
drop ≥ `min_prefilter_rejection_micros` of rows (default
10%). Below threshold, the tree classifies as
DenseFallback — the encoded eval cost would not amortize
the residual materialize savings.

**Aggregate folding is not gated by predicate selectivity.**
It has its own kernel-support checks and Phase 8 bench
gate. Mask popcount + cascade-value walk strictly beats
materialize-then-fold on supported cascades.

Determinism: identical config + identical CSB stats +
identical predicate tree → identical plan classification.

### 6. Per-cascade evaluator surface

Each evaluator implements a uniform signature:

```rust
pub fn evaluate(
    encoded_bytes: &[u8],
    encoding: &BufferEncoding,
    cascade: &EncodingCascade,
    atom: &PredicateAtom,
    coercion: &LiteralCoercion,
) -> Result<EncodedRowMask, EncodingComputeRejectCode>;
```

`PredicateAtom`:

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
The runtime dispatch is bifurcated:

- value atoms (`Eq`, `Neq`, range, `InList`) go through the
  cascade value evaluator and then combine with the column
  validity mask when the row group has nulls;
- validity atoms (`IsNull`, `IsNotNull`) bypass the value
  evaluator and read the validity buffer / row-group null-count
  metadata directly.

The value-evaluator `evaluate(...)` entry must reject
`IsNull` / `IsNotNull`; those atoms are only correct through
the validity path.

`LiteralCoercion` carries the pre-computed literal in the
encoded-domain representation. Computed once per query, not
per row.

#### Per-evaluator sketches

V1 ships a correctness-first value evaluator: decode the
encoded buffer with ADR 057's deterministic decoder and run the
scalar predicate loop, then combine with validity. The
per-cascade sketches below are the specialization backlog for
promotion-to-default; they are not required for the disabled
correctness V1.

**BitPacked.** Lift literal to packed domain
(`literal - base`), bounds-check fits in `bits`. SIMD
unpack-and-compare or, when `bits` is byte-aligned,
direct-compare against packed bytes. Per-page granularity
via ADR 057 `page_end_offsets`. Out-of-range literal
short-circuits: `Eq` → `None`; `Neq` → `All` (modulo
validity).

**RlePlain / RleBitPacked.** Walk encoded run array; for each
`(run_value, run_length)`, evaluate atom against `run_value`
once; emit `SelectionRun` entries. Output is `Runs` mask
natively. RLE-friendly fixtures stay compact.

**DictionaryPlain / DictionaryBitPacked.** Two-step:
  1. Evaluate the atom against the inline dictionary value
     table once. Produce match set `S` of dictionary indices.
     If literal not in dict for `Eq` → `None` mask.
     If `|S| > max_dict_match_set_size` → fall back
     (`LiteralCoercionMiss`).
  2. Walk the bit-packed indices buffer; emit
     `mask[i] = (indices[i] ∈ S)`. Specialized fast paths:
     `|S| == 1` → SIMD direct equality;
     `|S| ≤ 4` → SIMD OR-compose direct equalities;
     `|S| > 4` → hash-set membership scan.

**Delta.** Stream-reconstruct values from `base` plus
`BitPacked` deltas; evaluate atom against each reconstructed
value. Full-buffer grain. Output is `Bitmap` mask.
Equality / null checks are trivial; range comparisons walk
the prefix sum once.

### 7. Phase-B integration

ADR 056's three-phase plan extends with a Phase B sub-step:

```text
Phase A: metadata + directory probe (unchanged)

Phase B: stats prune + COCD plan (extended)
  - run ADR 056 row-group stats pruning;
  - classify predicate as Exact / Prefilter /
    DenseFallback;
  - for Exact:
      fetch only encoded buffer bytes for predicate
      columns;
      dispatch each leaf to its evaluator;
      compose masks under PBC tree shape;
  - for Prefilter:
      fetch encoded buffer bytes for encoded leaves;
      AND-compose encoded leaf masks → prefilter mask;
      Phase C fetches residual predicate + projection
      columns for surviving rows;
      Phase D materializes; PBC re-runs original program
      against the materialized batch;
  - for DenseFallback:
      existing ADR 057 path — fetch buffer bytes,
      materialize, run PBC against materialized batch;
  - if combined Phase-B mask is None for a row group → skip
    Phase C entirely.

Phase C: projected fetch (unchanged)
  - over surviving row groups, fetch projection columns;
  - apply Phase-B mask at zip time.

Phase D: Encoding materialization (unchanged from ADR 057)
  - materialize projection columns only.
```

The predicate planner shape is internal to the
`L2FetchStrategy::RowGroup(plan)` leg; no fifth strategy
variant and no wire-visible plan field is added. In V1 the
manager builds the predicate Phase-B fetch plan locally from
the parsed row-group + encoding directories, then records only
the realized `PhaseBOutcome` / fetch audit already carried by
the row-group read path.

```rust
pub struct EncodingComputePlan {
    pub predicate_plan: EncodingComputePredicatePlan,
    pub residual_mode: EncodedResidualMode,
}

pub enum EncodedResidualMode {
    None,
    DensePbcAfterPrefilter,
    DenseAggregateAfterPrefilter,
}
```

Aggregates are planned separately in the manager as a private
per-row-group fetch plan and folded through
`try_fold_encoded_aggregate_request`; there is no public
`EncodedAggregatePlan` type in V1.

### 8. Aggregate fold integration

```rust
pub fn try_fold_encoded_aggregate_request(
    request: &AggregateRequest,
    context: &EncodedAggregateContext,
    mask: Option<&EncodedRowMask>,
) -> Result<Option<Vec<AggregatePartial>>, EncodingComputeRejectCode>;
```

`Ok(Some(partials))` — encoded path produced the same partial
sequence the dense AGG path would have produced for the whole
request.
`Ok(None)` — valid economic fallback to dense AGG (see
mixed-request policy below).
`Err(code)` — kernel invariant violation or corruption
reject; must not fall back silently.

For unfiltered aggregates, mask is `All { rows:
row_group.row_count }`.

**Mixed-request policy: all-or-nothing in V1.** An
`AggregateRequest` may carry multiple aggregate expressions
(e.g., `SELECT COUNT(*), MIN(x), MAX(x), SUM(y)`). V1 COCD
folds the request only when **every** expression is
COCD-supported under the cascade-shape and column-type
checks above; if any single expression is unsupported,
`try_fold_encoded_aggregate_request` returns `Ok(None)` and the
entire request falls back to dense AGG. This keeps
`AggregatePartial` assembly byte-identical to the dense AGG
path (one partial per request, expressions in declared
order) without requiring a hybrid response shape that
splices encoded and dense partials per expression. Per-
expression mixed support — folding the supported
expressions internally and materializing only the inputs
required for the unsupported expressions — is a follow-up
ADR opportunity; V1 keeps the partial-assembly contract
identical to ADR 040 dense AGG.

### 9. Composition with ADR 052 (secondary indexes)

ADR 057 §8 already extended `fetch_indexed_rows` with the
SIMD-page fast path. ADR 058 extends the confirmation step:

- when an index hit's record has Encoding directory AND the
  hit's confirmation predicate column is COCD-supported AND
  `EncodingComputeConfig.enabled`:
  evaluate the predicate directly on the page bytes
  containing the target row, confirm hit-or-miss without
  materializing the page;
- on hit, fall back to the ADR 057 page materializer to
  produce the row;
- on miss, drop the hit.

Marginal win — index hits already point at single rows.
Index identity, sidecar format, and rebuild logic stay
unchanged.

### 10. Composition with ADR 055 (LSM compaction)

LSM compaction is byte-neutral to ADR 058. Compaction reads
records, materializes via Phase D, recomputes cascades on
output. Whether COCD evaluates a read-side predicate via
encoded eval or materialize-then-eval does not affect
compaction output bytes — predicates are read-side;
compaction has no predicate.

Visible-state digest invariance preserved.

### 11. CDC, recovery, rehydration are byte-neutral

- CDC catchup reads full record bodies, runs Phase D
  materialization through the storage manager. CDC has no
  predicate filter on the substrate; COCD is dormant.
  Architectural guard: CDC does not import COCD-specific
  types directly.
- Recovery decodes the IPC stream, materializes Encoding
  buffers via Phase D, rebuilds redb indexes. Recovery has
  no predicate; COCD dormant.
- Rehydration installs whole slab bytes; subsequent L1 reads
  use Phase B / Phase D paths automatically.

### 12. Existing invariants stay fixed

- ADR 020 authentication unchanged.
- ADR 022 footer layout unchanged.
- ADR 035 PBC executor: existing materialized-batch
  evaluation path unchanged.
- ADR 036 PBC pushdown contract: PBC bytecode shape,
  opcodes, reject codes, capability negotiation
  byte-identical.
- ADR 039 CSB pruning runs before any record-body fetch.
- ADR 040 AGG short-circuit (CSB direct-answer) runs before
  any record-body fetch and before COCD aggregate folding.
- ADR 042 synthetic columns stay storage-only.
- ADR 043 codec catalog unchanged.
- ADR 044 `ColumnFetchRequirement` remains the source of
  required user / predicate / aggregate / storage columns.
- ADR 052 secondary-index sidecars retain identity-axis
  shape.
- ADR 055 compaction unchanged.
- ADR 056 row-group directory shape unchanged.
- ADR 057 encoding directory shape unchanged.

### 13. Capability and protocol surface

No new negotiated `NodeCapability`. ADR 058 is an internal
storage-side substrate. Toron-visible PBC programs, ADR 036
pushdown contract, data-server / control-server wire frames
all unchanged.

### 14. Observability

Metrics under `storage_l2_encoding_compute_*`:

```text
storage_l2_encoding_compute_plan_total{decision, reason}
storage_l2_encoding_compute_kernel_micros{kernel, cascade}
storage_l2_encoding_compute_rows_total{kind}
storage_l2_encoding_compute_mask_bytes
storage_l2_encoding_compute_pages_fetched_total{kind}
storage_l2_encoding_compute_aggregate_total{aggregate, decision, reason}
storage_l2_encoding_compute_invariant_violations_total{violation_kind}
```

Pinned label values:

```text
decision       = exact | prefilter | dense_fallback
reason         = reader_disabled | directory_missing
              | unsupported_program | unsupported_cascade
              | unsupported_arrow_layout | dense_selection
              | too_many_mask_bytes | too_many_pages
              | aggregate_unsupported
kernel         = bit_packed | rle_plain | rle_bit_packed
              | dictionary_plain | dictionary_bit_packed
              | delta
cascade        = bit_packed | rle | dictionary | delta
kind           = scanned | selected | avoided | materialized
aggregate      = count_star | count_non_null | min | max
              | signed_sum | unsigned_sum | signed_avg
              | unsigned_avg
violation_kind = mask_length_mismatch | encoded_value_overflow
              | kernel_decoded_length_mismatch
              | aggregate_overflow_mismatch
              | selection_range_overflow
```

No tenant / table / column / row-index / slab-id /
error-string / path / literal labels.

Audit events:

- `EncodingComputeSampled { kernel, rows_scanned,
  rows_selected }` (sampled, debug-grade);
- `EncodingComputeInvariantViolation { kind }`
  (page-grade).

### 15. Reject taxonomy

Closed V1 taxonomy (5 codes):

```rust
#[repr(u32)]
pub enum EncodingComputeRejectCode {
    MaskLengthMismatch          = 1,
    EncodedValueOverflow        = 2,
    KernelDecodedLengthMismatch = 3,
    AggregateOverflowMismatch   = 4,
    SelectionRangeOverflow      = 5,
}
```

Discriminants 1..=5 stable. New codes require a follow-up
ADR.

Distinct invariants per code:

- `MaskLengthMismatch`: produced mask length differs from
  `decoded_value_count`. Hard fail — bug in the evaluator.
- `EncodedValueOverflow`: encoded value falls outside the
  declared cascade parameters (e.g., bit-packed value with
  bit-pattern outside `2^bits`).
- `KernelDecodedLengthMismatch`: kernel's reconstruction
  length differs from `decoded_value_count` (e.g., RLE run
  expansion exceeds buffer length).
- `AggregateOverflowMismatch`: encoded aggregate kernel
  produces overflow flag inconsistent with what the dense
  AGG path would have emitted.
- `SelectionRangeOverflow`: `SelectionRun` byte range or row
  range exceeds the cascade's declared
  `decoded_value_count`.

Authentication / parse / decode failures route to storage
errors. Planner decisions to fall back use
`EncodingComputeFallbackReason`:

```rust
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

These are metric labels, not corruption rejects.

### 16. Architectural lockdown

`tests/architecture_test.rs` enforces (~27 guards; final
list pinned in Phase 9 of the impl plan):

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
7. Authentication / parse failures cannot reach any silent
   fallback branch.
8. `storage_l2_encoding_compute_*` metrics carry exactly
   the bounded labels.
9. No new `MsgType` containing `Cocd` / `Encoded` /
   `Compute`.
10. No new `FRAME_FLAG_*`.
11. No new `NodeCapability` variant.
12. No `ObjectStore` trait method addition.
13. No table descriptor field for COCD policy.
14. No envelope `format_version` bump; no ADR 022 footer
    field addition.
15. No new public PBC opcode in `src/schema/pbc.rs::Op`
    (snapshot at Phase 0).
16. No PBC executor call from inside
    `src/storage/encoding/compute/`. Encoded eval runs
    **before** the dense executor sees input; the path is
    one-way.
17. No `Wendelon`-prefixed COCD identifiers.
18. No `EncodingCompute*V[0-9]` Rust type suffixes.
19. Evaluator dispatch is not exposed as a plug-in / trait.
20. ADR 057 directory shape unchanged: file digests of
    `src/storage/encoding/types.rs` `EncodingDirectory`,
    `BufferEncoding`, `EncodingPageDescriptor` definitions
    match a snapshot taken before ADR 058 work begins.
21. `EncodingComputePredicatePlan` and
    `EncodingComputePlan` are internal — never serialized.
22. CSB direct-answer code path remains before COCD
    aggregate attempt in `src/storage/aggregate.rs` /
    `src/network/data_server.rs`.
23. `Fsst`, `Alp`, `Plain`, `Lz4Frame`, nested Arrow types,
    and variable-width Dictionary range predicates are not
    silently accepted by encoded kernels.
24. Mask length invariant: every evaluator's exit mask
    `rows` field equals `decoded_value_count`.
25. Dense reference parity tests exist for every target
    predicate evaluator and every target aggregate kernel
    (mask byte-equality and partial byte-equality vs.
    materialize-then-evaluate / dense AGG).
26. Mixed-aggregate-request policy is all-or-nothing in
    V1: `try_fold_encoded_aggregate_request` returns `Ok(None)`
    if any single expression in the request is
    COCD-unsupported.
27. COCD aggregate kernels enforce ADR 040's integer-only
    sum/avg type domain (no time/timestamp, no float, no
    decimal); type-domain divergence from
    `validate_expression_types` is an architectural-test
    failure.

## Rationale

### Why include aggregate folding

The mask infrastructure is the hard substrate. Once it
exists, folding common exact aggregates from cascade values
is the same physical scan with different output: `count_star`
is mask popcount; `count_non_null` is `(mask AND validity)
.popcount()`; `min` / `max` / `sum` walk the cascade once
with the mask filtering contributions.

Splitting predicate and aggregate substrates would force a
near-immediate ADR 040 follow-up that reopens the same mask
and cascade correctness questions. The marginal
implementation cost (~600 LOC for 8 kernels × 4 cascade
families) is small relative to the substrate.

CSB direct-answer (ADR 040) handles unfiltered fast paths;
COCD aggregate fold handles the *filtered* fast path that
CSB cannot answer.

### Why Exact / Prefilter / DenseFallback explicit modes

A binary "encoded or dense" switch is too coarse:

- **Exact** predicates skip dense PBC entirely.
- **Mixed conjunctions** can profit from encoded leaves if
  they reject enough rows before residual materialization.
- **Mixed disjunctions** cannot use the shortcut safely
  because an encoded branch cannot disprove an unsupported
  branch.

The three-mode taxonomy makes the correctness rules explicit
per mode. PBC re-evaluation in Prefilter mode is **not** a
violation of ADR 056's "PBC never re-evaluates post-Phase-B"
rule because the re-evaluation runs against a strict subset
(Prefilter survivors), not against re-decoded data. The mask
is authoritative in Exact mode; the original program is
authoritative in Prefilter mode.

### Why no public PBC opcodes

3-of-3 council convergence. ADR 036 §7 ("PBC capability
negotiation becomes mandatory before new opcodes") and §11
("targetability diagnostics belong on Toron expression
nodes") together: new public opcodes are a Toron contract
concern, not a storage optimization concern. Toron does not
need to know whether Wendelon evaluated `status = 'ACTIVE'`
against a dictionary or against a materialized array.

### Why `EncodedRowMask` tagged enum (3-of-3 convergence)

RLE-friendly fixtures stay compact (`Runs` representation);
Dictionary equality miss short-circuits to `None`; constant
columns short-circuit to `All`. A `Vec<bool>` /
`BooleanArray` representation throws away these structures
exactly when they're most valuable. Mask byte allocation is
bounded by config; `Runs` densifies to `Bitmap` if it
exceeds the cap. Conversion to Arrow `BooleanArray` happens
at the residual handoff in Prefilter mode.

### Why dual economic gates

Encoded evaluation is not free. The two gates handle two
distinct regression cases:

- **Primary leaf-level selectivity gate** at 0.25: applies
  to every encoded predicate leaf, including Exact-mode
  leaves. At high selectivity the encoded path produces a
  mostly-true mask but the projection fetch is still
  mostly-everything; dense materialize-then-evaluate wins
  by reusing one fetch for both predicate and projection
  input.
- **Secondary Prefilter rejection gate** at 0.10: applies
  to Prefilter mode (mixed-conjunction). Encoded leaves
  must reject enough rows to amortize the coordination
  cost of narrowing residual materialization.

Aggregate folding has neither gate — the mask is the input,
folding the mask + cascade values is strictly cheaper than
materializing then folding. Aggregate folding has its own
Phase 8 benchmark evidence and promotion-to-default gate
(≥ 2× faster than dense AGG on representative fixtures once
specialized kernels land).

### Why Delta full atom set, full-buffer grain

Full-buffer-grain Delta does not need per-page prefix-sum
side tables: the kernel walks the prefix sum once,
reconstructs each value, and emits a `Bitmap` mask. Range /
`InList` atoms are mostly free implementation cost on top
of equality. The win: HLC range queries (`__row_hlc__ >
1234567890`) — a common temporal predicate over Delta-
encoded HLC columns. Phase 8 bench evidence decides whether
Delta range stays enabled by default.

### Why no Fsst / Alp in V1 (3-of-3 convergence)

FSST string semantics (multi-byte symbol boundaries, escape
sequences) and ALP float semantics (NaN, signed zero,
exception streams, PBC float-comparison parity) are
correctness-heavy. Their likely V1 win is not worth loading
ADR 058 with those proofs. Defer until bench evidence
justifies.

### Why closed evaluator + aggregate catalogs

Same closed-codec-catalog discipline ADR 043 / 053 / 057
already commit to. A plug-in trait would let third-party
evaluators land without ADR review and would defeat the
cross-replica determinism contract.

### Why deployment-wide config (not per-table)

Same argument ADR 057 §"Why node-local writer config" makes:
COCD does not change logical rows, table semantics, PBC
programs, or Toron contracts. A descriptor field would make
a physical deployment knob look like logical schema policy.

### Why reader-CPU evidence gate

Disabled V1 is allowed to ship as a correctness-first
decode-then-scalar implementation. The benchmark harness is
still mandatory because it establishes the baseline for the
specialized-kernel follow-up and prevents promotion-to-default
without measured evidence.

Promotion-to-default targets:

- **Exact COCD ≥ 2× faster than ADR 057 dense PBC** at low
  selectivity on representative `BitPacked` / `Rle` /
  `Dictionary` fixtures.
- **Aggregate kernels ≥ 2× faster than dense AGG** on
  representative `Rle` / `BitPacked` fixtures.
- **Dense-selection overhead ≤ 5%** vs. ADR 057 dense path
  when COCD economically falls back.

If evidence shows tighter bars are achievable, the gate
tightens.

## Consequences

- **Predicate-only reads** can avoid materializing predicate
  columns that are not projected.
- **Filtered aggregates** can reuse encoded predicate masks
  and fold exact aggregate partials without dense AGG for
  supported cascades.
- **Phase D allocation drops** when predicate columns are
  not in projection.
- **Memory pressure drops** on concurrent query fan-out.
- **AGG CPU drops** on filtered aggregate queries that CSB
  cannot direct-answer.
- **Phase B CPU rises modestly** on dense-selectivity
  Prefilter queries if the rejection-gate misjudges; the
  gate's CSB-stat-based estimate is provisional.
- **Provider observes finer access pattern.** Encoded-eval
  plus SIMD-page fast path leaks predicate-selectivity
  correlation to the provider on top of ADR 057 leakage.
- **Mixed legacy / COCD-eval slabs** during rollout —
  logical results byte-identical; metrics show two
  populations.
- **No win on `Plain` / `Lz4Frame` / `Fsst` / `Alp`
  cascade columns.**
- **No grouped aggregation, approx aggregates,
  count_distinct, float / decimal aggregates in V1.**
- **The reader path grows another execution mode** and
  needs dense parity tests for every kernel.
- **Implementation surface ~5500–6500 LOC**, mostly
  net-new in `src/storage/encoding/compute/`.
- **Architectural test growth ~27 guards.**

## Rejected alternatives

- **`EVAL_DICT` / `EVAL_RLE` / `EVAL_BITPACKED_RANGE` as
  new public PBC opcodes.** Brainstorm 011 §2 framing.
  ADR 036 §7 rejects.
- **`EncodedPredicateKernel` plug-in trait.** Closed-
  catalog argument (3-of-3).
- **`StoragePbcExecutor` accepts `EncodedOrDenseBatch`.**
  Couples PBC to encoding; architectural-guard violation.
- **Runtime JIT materialization on kernel fallback.**
  Planning-time classification gives predictable
  performance.
- **Fsst / Alp evaluators in V1.** Deferred (3-of-3).
- **Aggregate push-into-encoded deferred to a follow-up
  ADR.** Mask infrastructure is the load-bearing
  substrate; aggregates consume it for free.
- **Pure leaf-grain dispatch with no explicit plan
  modes.** Three modes (Exact / Prefilter / DenseFallback)
  capture the correctness rules per leaf composition; a
  flat scheme conflates them.
- **4-code reject taxonomy.** Aggregate-overflow-mismatch
  needs its own discriminant; 5 codes.
- **Single Prefilter-only economic gate.** Misses the
  high-selectivity regression case where dense reuses one
  fetch as predicate + projection input; dual gates.
- **Sparse output narrowing in Phase C beyond Prefilter.**
  Deferred follow-up that generalizes ADR 057's
  secondary-index page fast path.
- **Encoded-grain AND/OR composition.** V1 composes at mask
  level.
- **Per-table COCD policy.** Same argument as ADR 057's
  rejection of per-table cascade policy.
- **`EncodingComputeFetchConfig` separate from
  `EncodingComputeConfig`.** Single config.
- **`EncodingCompute` in IPC custom_metadata.** No metadata
  to carry.
- **Cross-row-group mask sharing.** Each row group's mask
  is independent.
- **ML / learned dispatch routing.** Determinism, cost,
  audit.
- **`InList` with large literal sets** (> 4096) routing to
  encoded eval. Falls back when
  `|match_set| > max_dict_match_set_size`.
- **Reserve evaluator slots, compatibility shims, or
  numeric-version Rust type suffixes.**
- **Approx aggregates / KLL / HLL / count_distinct / float
  aggregates in V1.** Deferred.
- **Grouped aggregation in V1.** ADR 040 deferred GROUP BY
  for the AGG protocol; COCD does not smuggle it in.
- **Dense fallback after kernel reject.** Invariant
  violations fail closed.
- **Page-level min/max/null stats for predicate
  short-circuit.** ADR 039 follow-up territory.
- **Delta page side-tables for page-grain range pruning.**
  ADR 057 directory mutation; deferred.

## Open questions

Bench-resolved (Phase 8):

1. **Should Delta full-buffer range stay enabled by
   default** if Phase 8 shows < 2× CPU improvement but
   still reduces allocation? Bench-gate may disable Delta
   range without reopening the substrate.
2. **`min_selectivity_for_encoded_eval_micros`.** 100_000
   / 250_000 / 500_000 — V1 default 250_000 (0.25); bench
   tunes the leaf-level guardrail.
3. **`max_dict_match_set_size`.** 256 / 1024 / 4096 — V1
   default 4096; bench tunes dispatch cost vs. fallback
   frequency.
4. **`min_buffer_rows_for_encoded_eval`.** 64 / 256 / 1024
   — V1 default 256.
5. **Per-evaluator decode/dispatch latency curves.**
6. **Per-aggregate-kernel fold latency curves.**
7. **InList selectivity.** Large InList literal sets vs.
   small. Threshold for encoded vs. materialize routing.
8. **Mixed encoded/materialized leaf composition cost.**
   Mask allocation overhead at AND/OR fold across mask
   shapes.
9. **Reader-CPU gate revision.** Provisional 2× starting
   target.
10. **Prefilter rejection threshold.** 5% / 10% / 25% — V1
    default 10%.
11. **Page-grain vs. buffer-grain for `Rle { inner:
    BitPacked }`.** Page-grain default; bench measures
    competitive cases.
12. **`count_non_null` without predicate**: prefer CSB
    direct-answer when row-group null counts are present?
13. **Sparse survivor-page narrowing**: promotion gate or
    measured optimization behind core mask parity gate?

Architectural (council-resolution):

14. **Should exact `Not` be exposed through the internal
    predicate tree in V1, or kept in DenseFallback until
    PBC tree extraction already models it?**

Architectural (deferred to follow-up ADRs):

15. **Page-level min/max/null stats for predicate
    short-circuit.** ADR 039 follow-up.
16. **Per-page prefix-sum side-table for Delta range
    speedup** (page-grain pruning).
17. **Fsst evaluator.**
18. **Alp evaluator.**
19. **Encoded-grain AND/OR composition.**
20. **Sparse output narrowing in Phase C** beyond
    Prefilter.
21. **Public Toron-visible cascade-hint opcode.** ADR 036
    capability negotiation if COCD needs to be Toron-
    observable.
22. **`InList` high-cardinality specialization.**
23. **`Like` evaluator over Fsst / Dictionary.**
24. **Adaptive dispatch routing.**
25. **Grouped aggregate COCD.** ADR 040 follow-up.
26. **HLL / KLL encoded fold kernels.**
27. **Exact `count_distinct` over Dictionary.**
28. **Float / decimal aggregate kernels.**
29. **String / binary `min` / `max` from sorted
    Dictionary.**

## Forward-compatibility analysis

### Substrate sequencing

```text
record body
  └─ row-group RecordBatch                     (ADR 056)
       └─ encoded column buffer                 (ADR 057)
            └─ SIMD page (default 1024 values)  (ADR 057)
                 └─ encoded predicate evaluation (ADR 058)
                      → EncodedRowMask          (ADR 058)
                      → mask AND/OR composition (ADR 058)
                      → encoded aggregate fold  (ADR 058)
                 → projection materialize (Phase D, ADR 057)
```

ADR 058 ships the encoded-eval-to-mask + encoded-aggregate-
fold layer.

### Future evaluator additions

New evaluator kinds append to `EncodingComputeEvaluatorKind`
(discriminants 7, 8, ...). Existing dispatchers fall
through to `DenseFallback` for unknown discriminants;
pre-add slabs remain readable.

### Future aggregate kernel additions

New aggregate kinds append to `EncodedAggregateKind`
(discriminants 9, 10, ...). Existing fold attempts return
`AggregateUnsupported` for unknown kinds; pre-add slabs
remain answerable via dense AGG.

### Public PBC opcode if needed

If a future Toron version needs to *control* whether COCD
runs (per-query opt-out), ADR 036 capability negotiation
provides the mechanism. ADR 058 V1 does not pre-allocate
this surface.

### Wire-tier extension

ADR 053 wire-side compression unchanged. Wire-tier COCD is
a separate ADR if it ever makes sense.

## Deferred to follow-up ADRs

1. Public encoded PBC opcodes plus Toron query-session
   capability.
2. Fsst evaluator.
3. Alp evaluator.
4. Per-page prefix-sum side-table for Delta range speedup.
5. Page-level min/max/null stats for predicate short-
   circuit (ADR 039 follow-up).
6. Encoded-grain AND/OR composition.
7. Sparse output narrowing in Phase C beyond Prefilter.
8. Per-row partial evaluation within SIMD pages.
9. `EncodingComputeCache` across queries.
10. Adaptive dispatch routing.
11. `InList` high-cardinality specialization.
12. `Like` evaluator over Fsst / Dictionary.
13. Grouped aggregate COCD (ADR 040 follow-up).
14. HLL / KLL encoded fold kernels.
15. Exact `count_distinct` over Dictionary.
16. Float / decimal aggregate kernels.
17. String / binary `min` / `max` from sorted Dictionary.
18. Cascade-aware AGG short-circuit beyond CSB direct
    answer.
19. CDC-side predicate filtering on subscriptions
    (ADR 054 follow-up).
20. Wire-tier COCD (ADR 053 extension).
21. ML-based dispatch routing.

## References

- `plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md`
- `plans/adr/022_l2_slab_format_footer_indexed_range_reads.md`
- `plans/adr/035_vectorized_pbc_executor.md`
- `plans/adr/036_pbc_pushdown_contract.md`
- `plans/adr/039_slab_footer_column_statistics_v1.md`
- `plans/adr/040_aggregate_pushdown.md`
- `plans/adr/041_protocol_naming_decoupling.md`
- `plans/adr/042_analytical_multi_row_fragments.md`
- `plans/adr/043_arrow_ipc_body_compression.md`
- `plans/adr/044_l2_columnar_fetch.md`
- `plans/adr/051_descriptor_fingerprint_split.md`
- `plans/adr/052_secondary_indexes.md`
- `plans/adr/053_wire_arrow_ipc_body_compression.md`
- `plans/adr/054_cdc_subscription_substrate.md`
- `plans/adr/055_lsm_leveled_compaction_substrate.md`
- `plans/adr/056_row_group_fetch_and_page_directory.md`
- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
- `plans/impl/057_per_buffer_pages_bench_results.md`
- `plans/brainstorms/011_encoding_cascades_vortex.md`
- `plans/conventions/newtype-units.md`
- `plans/conventions/no-version-suffixes.md`
- `plans/conventions/no-premature-backward-compat.md`
- `plans/conventions/no-project-name-in-protocols.md`
- `plans/philosophy.md` (Unified Ring; Identity vs
  Locality)
