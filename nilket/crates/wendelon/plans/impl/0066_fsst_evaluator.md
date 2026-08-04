# Implementation Plan: ADR 066 — FSST Predicate Evaluator

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR:
`plans/adr/0066_fsst_evaluator.md`

Inherits substrate:

- `plans/adr/0057_per_buffer_pages_and_encoding_cascades.md`
  — `EncodingDirectory`, `BufferEncoding`,
  `EncodingCascade::Fsst` cascade variant, full-buffer-
  grain decode for FSST today, multi-buffer
  values + offsets + validity layout for variable-width
  Arrow types.
- `plans/adr/0058_compute_over_encoded_data.md` — closed
  6-variant `EncodingComputeEvaluatorKind` catalog;
  pre-kernel eligibility layer; Exact / Prefilter /
  DenseFallback predicate plan modes; `EncodedRowMask`
  algebra; reject taxonomy;
  `enabled_predicate_evaluators` allow-list; row-group
  predicate planner currently single-buffer for fixed-
  width leaves.
- `plans/adr/0059_native_cocd_kernels.md` — native kernel
  module layout under
  `src/storage/encoding/compute/kernels/`; central
  `kernels/validity.rs` 3VL composition; two-step
  dispatch discipline; promote-to-default cadence;
  default allow-list amendment cadence; SIMD strategy
  (stable rustc + auto-vectorization first; small SIMD
  helper crate as escalation; `std::arch` second; no
  nightly, no Cargo feature gate, no operator-visible
  knob).
- `plans/adr/0060_sparse_projection_fetch_narrowing.md`
  — mask-only `PhaseBOutcome` is the contract sparse
  projection consumes; FSST not in V1 sparse projection
  catalog (variable-width).
- `plans/adr/0063_page_level_predicate_stats.md` — page-
  stat eligibility excludes FSST (non-page-emitting).
- `plans/adr/0064_nullable_sparse_projection.md` —
  nullable raw validity helpers; FSST not in V1 catalog.
- `plans/adr/0065_dense_predicate_zip_with_sparse_projection.md`
  — bridge for cascade-uncovered predicates; FSST
  predicates that decline kernel continue to engage
  Case B/C; ADR 066 narrows ADR 065's V1 engagement
  surface for FSST `Eq` / `Neq` / `InList` Sub-case
  A1 atoms (FSST predicate column not in output);
  Sub-case A2 (overlap) and null predicates remain
  on existing paths.
- `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-planning-phase-names.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/capability-vs-feature-naming.md`,
  `plans/conventions/no-project-name-in-protocols.md`.

## Status

Council-converged 2026-05-09 across Claude / Codex /
Gemini. V1 implementation is staged default-off. Phase 7
diagnostic evidence, including full local encoding-compute and
row-group matrices, is recorded at
`plans/impl/0066_fsst_evaluator_bench_results.md`; default-on
promotion remains blocked pending council review and cleared gates.

Implementation contract:

- `src/storage/encoding/compute/kernels/fsst.rs` is
  the new kernel module; sibling to ADR 059's
  existing per-cascade kernel modules.
- `EncodingComputeEvaluatorKind` extends by exactly
  one variant (`Fsst`, label `"fsst"`); ADR 058's
  catalog discipline applies recursively.
- `evaluator_kind_for_cascade` updated:
  `EncodingCascade::Fsst → EncodingComputeEvaluatorKind::Fsst`.
- `encoding_compute_predicate_data_type_supported` updated: admits
  `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary`
  only through the `Fsst` evaluator kind path.
- The row-group predicate planner extends in place
  to fetch validity + offsets + values together for
  FSST predicate leaves; non-FSST cascades remain on
  the existing single-buffer fetch shape. Each FSST
  leaf counts as one logical encoded chunk for the
  existing encoded-page cap.
- Pre-kernel eligibility extends in place at the
  existing ADR 058 dispatch site; no new dispatch
  module.
- V1 ships **without** a plan-time fetch-byte
  economics gate: the dense FSST fallback already
  substitutes encoded byte ranges via
  `encoding_fetch_range_for_buffer_page`, so kernel
  and fallback fetch the same encoded payload. The
  kernel's win is CPU economics (avoiding a dense
  plaintext materialization pass); a CPU-cost gate
  is V2 if Phase 7 surfaces a dense-faster regime.
- `EncodingComputeConfig::default()`'s
  `enabled_predicate_evaluators` extends by `Fsst`
  only if Phase 7 evidence clears the council's
  promotion gates.
- The kernel evaluates `Eq`, `Neq`, `InList` on FSST
  cascade buffers via streaming token decode +
  decoded-length pre-filter; declines other atoms
  via the existing
  `EncodingComputeFallbackReason::UnsupportedProgram`
  (or equivalent) path. `IsNull` / `IsNotNull` reuse
  the existing ADR 059 validity-only path entirely
  (kernel not invoked for those atoms).
- The kernel produces mask-only `PhaseBOutcome` when
  engaged; ADR 060 / 064 Case A engages downstream
  for the remaining projection requirement (Sub-case
  A1 only — FSST predicate column not in output).
- **Sub-case A2 (FSST predicate column in output,
  predicate-output overlap)**: the encoding-compute
  planner detects overlap and declines to
  `EncodingComputeFallbackReason::UnsupportedCascade`,
  which falls through to the existing ADR 065 Case B
  path; kernel does not engage in V1 (variable-width
  sparse projection needed for kernel engagement here
  is V2-deferred per ADR 060 / 064).
- FSST predicates that decline kernel eligibility
  (atom mismatch, type mismatch, literal coercion
  failure) continue to populate
  `predicate_projection_by_group` and route to ADR
  065 Case B/C as today (regression coverage).
- **Corruption / runtime invariant failures**
  (malformed WFS1 frame, offsets non-monotonic,
  decoded-length mismatch) fail closed via
  `EncodingComputeRejectCode` per ADR 058
  discipline — they do **not** route to ADR 065
  Case B/C dense fallback.
- Buffer-grain dispatch only; no page-grain FSST.
- Per-buffer symbol-table parse only; no process-
  wide cache.
- The kernel reuses the writer's FSST decoder via
  the streaming token reader. The kernel walks the
  encoded WFS1 payload from byte 0 and uses Arrow
  offsets only as decoded row boundaries; **the
  kernel does not derive per-row compressed slices
  from Arrow offsets** — they are decoded byte
  positions, not encoded byte ranges.
- The kernel reads the FSST symbol table from the
  `WFS1` header authoritatively;
  `EncodingCascade::Fsst { symbol_table_byte_len }`
  is unmodified, and an architectural guard asserts
  the kernel does not consult the cascade field.
  Cascade-field disposition is out of scope per
  ADR 066's evaluator-only V1 boundary.
- ADR 057 `Fsst` cascade byte layout unchanged.
- ADR 060 / 063 / 064 / 065 surfaces unchanged.
- No public surface changes.

## Goal

Add a native FSST predicate evaluator to ADR 058's
catalog so equality and set-membership atoms
(`Eq` / `Neq` / `InList`) over FSST-encoded variable-
width string columns evaluate at encoded grain via
streaming token decode and emit mask-only
`PhaseBOutcome`. **Sub-case A1 queries** (FSST
predicate column not in output) migrate from ADR 065
Case B/C dense-zip to ADR 060 / 064 Case A mask-only
sparse projection. **Sub-case A2 queries** (FSST
predicate column in output, predicate-output overlap)
stay on the existing ADR 065 Case B path in V1
because variable-width sparse projection is V2-
deferred per ADR 060 / 064 — the encoding-compute
planner detects the overlap and declines, falling
through to the dense path with no kernel engagement. `IsNull` / `IsNotNull` continue on the
existing ADR 059 validity-only path with no FSST
kernel engagement. ADR 065 Case B/C remains
reachable for non-V1 atoms (range / future LIKE /
regex), for predicate-output overlap (Sub-case A2),
and for buffers that fail kernel eligibility.

ADR 066 is also the first variable-width native
predicate leaf in ADR 058's catalog; the row-group
predicate planner extends in place to fetch
validity + offsets + values together for FSST
leaves.

```text
write side: unchanged.

read side:
  Stage A (directory probe): unchanged
  Stage B (predicate evaluation):
    - FSST cascade buffer + V1 atom (Eq, Neq, InList)
      + Utf8/LargeUtf8/Binary/LargeBinary type +
      literal coerces to bytes + FSST predicate
      column NOT in requirement.output_columns
      (Sub-case A1) → ADR 066 native FSST kernel:
        - row-group planner fetches validity +
          offsets + values together (multi-buffer
          leaf);
        - parse WFS1 frame once per buffer (symbol
          table, escape byte, decoded-length recovery
          state); WFS1 header is the authoritative
          source for symbol-table length;
        - decode offsets buffer (32/64-bit Arrow
          offsets, dispatched through cascade
          decoder if encoded);
        - bucket literals by decoded byte length;
        - stream the encoded WFS1 payload once from
          byte 0; for each row i:
            - compute decoded length from
              offsets[i+1] - offsets[i];
            - if no literal of that length, set
              row_matches_any_literal = false and
              advance the streaming reader past the
              row's decoded bytes (no scratch copy,
              no byte comparison);
            - if one or more literals of that length,
              copy decoded bytes into per-row scratch
              (≤ longest-literal bytes) and byte-
              compare to length-matching literals to
              determine row_matches_any_literal;
            - apply op-specific inversion: bit =
              row_matches_any_literal for Eq/InList;
              bit = !row_matches_any_literal for Neq
              (length-mismatch rows survive Neq);
            - fold bit with validity via
              kernels/validity.rs;
        - the kernel does not slice the encoded
          buffer by Arrow offsets (offsets are
          decoded positions, not encoded ranges);
        - emit EncodedRowMask via existing shape
          selector;
        - compose validity via kernels/validity.rs;
        - return mask-only PhaseBOutcome.
    - FSST cascade + V1 atom + predicate-output
      overlap on FSST column (Sub-case A2) →
      existing ADR 065 Case B path (dense decode-
      then-scan; populates
      predicate_projection_by_group). Kernel does
      not engage in V1.
    - FSST cascade + IsNull/IsNotNull → existing
      ADR 059 validity-only path (kernel not
      invoked).
    - FSST cascade + non-V1 atom (range, future
      LIKE) → ADR 057 dense fallback (unchanged);
      FSST decode + dense string scan →
      predicate_projection_by_group → ADR 065 Case
      B/C.
    - FSST cascade + corruption / invariant
      failure (frame parse error, offsets non-
      monotonic, decoded-length mismatch) → fail-
      closed via EncodingComputeRejectCode; no
      ADR 065 Case B/C fallback.
    - non-FSST cascade → unchanged.
  Stage C (projection planning): unchanged.
  Stage D (encoding materialization): unchanged.
```

## Deliverables

- `src/storage/encoding/compute/kernels/fsst.rs`
  (new module):
  - `parse_fsst_frame(buffer_bytes: &[u8]) -> Result<FsstFrame, EncodingComputeRejectCode>`
    — buffer-grain WFS1 parser (symbol table, escape
    byte, decoded-length recovery state);
  - `decode_offsets(offsets: &[u8], data_type: &DataType, encoding: Option<&BufferEncoding>) -> Result<DecodedOffsets, EncodingComputeRejectCode>`
    — 32/64-bit Arrow offset decoder, dispatching
    through the existing cascade decoder if the
    offsets buffer is encoded;
  - `evaluate_fsst_equality(frame: &FsstFrame, offsets: &DecodedOffsets, validity: Option<&ValidityBitmap>, literals: &[LiteralBytes], op: PredicateAtomOp) -> Result<EncodedRowMask, EncodingComputeRejectCode>`
    — buffer-grain walker (streaming token decode +
    length pre-filter + byte comparison + mask
    emission).
- `src/storage/encoding/compute/kernels/mod.rs`:
  - register `fsst` submodule.
- `src/storage/encoding/compute/types.rs` (or
  wherever `EncodingComputeEvaluatorKind` lives —
  pinned in Phase 0):
  - add `Fsst` (label `"fsst"`) to
    `EncodingComputeEvaluatorKind`;
  - update `ALL`, `as_str`, `from_label`;
  - update `evaluator_kind_for_cascade` so
    `EncodingCascade::Fsst → EncodingComputeEvaluatorKind::Fsst`;
  - update `encoding_compute_predicate_data_type_supported` to admit
    `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary`
    only through the `Fsst` kind (evaluator-aware
    type eligibility).
- `src/storage/encoding/compute/eligibility.rs`
  (or equivalent — pinned in Phase 0):
  - extend pre-kernel eligibility to admit FSST +
    V1 atoms + V1 types + clean literal coercion;
  - decline non-V1 atoms / types / coercions via
    existing fallback paths.
- ADR 058 row-group predicate planner +
  surrounding manager helpers (as-built in the
  staged tree; line numbers move as the file
  evolves — current pins below):
  - **`encoding_compute_column_buffers`**
    (`src/storage/manager.rs:12157`) is the new
    helper that returns
    `(physical_ordinal, value_buffer_ordinal,
    offsets_buffer_ordinal: Option<…>)` and
    accepts both `buffer_count == 2` (fixed-
    width / no offsets) and `buffer_count == 3`
    (variable-width FSST: validity + offsets +
    values). The legacy
    `encoding_compute_value_buffer`
    (`:12149`) is now a thin wrapper around it
    for the two-ordinal callers.
  - **`push_encoded_leaf`**
    (`src/storage/manager.rs:12075`) builds each
    `EncodingComputePhaseBLeaf::Encoded` carrying
    `physical_ordinal`,
    `value_buffer_ordinal`, **and**
    `offsets_buffer_ordinal: Option<IpcBufferIndex>`
    (the offsets-aware shape).
    Type eligibility runs through
    `encoding_compute_predicate_data_type_candidate_supported`
    (`:12190`), which delegates to the cascade-
    aware
    `encoding_compute_predicate_data_type_supported_for_evaluator`
    (`:12195`) — non-FSST evaluator kinds do
    not inherit UTF-8 / Binary support.
  - **`build_encoding_compute_phase_b_plan`**
    (`src/storage/manager.rs:11078`) takes
    `output_columns: &ColumnOrdinalSet` (`:11083`)
    and gates Sub-case A2 directly: if the
    chosen evaluator is `Fsst` and the predicate
    column is in `output_columns`, the planner
    returns
    `EncodingComputeFallbackReason::UnsupportedCascade`
    (`:11151-11156`), which falls through to
    today's ADR 065 Case B path. The planner
    fetches validity + offsets + values together
    for FSST leaves and counts each FSST leaf as
    **one** logical encoded chunk for the existing
    `EncodingComputeConfig.max_encoded_pages_per_record`
    cap (the multi-buffer fetch is one logical
    leaf, not three).
  - **`try_read_encoding_compute_phase_b_from_l2`**
    (`src/storage/manager.rs:8950`) plumbs
    `output_columns: &ColumnOrdinalSet`
    (`:8962`) through to the planner, so Sub-
    case A2 gating fires before kernel
    invocation.
  - non-FSST cascades unchanged (planner returns
    the existing two-ordinal shape with
    `offsets_buffer_ordinal == None`).
- `src/storage/encoding/compute/config.rs` (or
  equivalent):
  - update `EncodingComputeConfig::default()`'s
    `enabled_predicate_evaluators` per Phase 7
    outcome.
- WFS1 header authority (no code change to the
  cascade record):
  - kernel parses the FSST symbol table from the
    `WFS1` header authoritatively;
  - `EncodingCascade::Fsst { symbol_table_byte_len }`
    is unmodified by ADR 066; cascade-field
    disposition is out of scope per §"Status";
  - architectural guard asserts the kernel does not
    consult the cascade field.
- `tests/encoding_compute_test.rs`:
  - FSST kernel correctness across closed atom ×
    type × nullability × selectivity matrix;
  - **boundary-crossing-symbol correctness fixture
    (load-bearing for streaming token decoder)**;
  - **no-compressed-row-slice regression fixture
    (load-bearing for the offsets-are-decoded-
    positions invariant)**;
  - length-pre-filter zero-byte-comparison
    verification on length-mismatch fixtures;
  - byte-identical mask vs dense oracle;
  - validity composition correctness;
  - corruption fixtures (malformed WFS1, non-
    monotonic offsets, truncated escapes — fail-
    closed via `EncodingComputeRejectCode`).
- `tests/encoding_compute_kernel_invariants.rs`:
  - FSST kernel invariant guards (atom-set
    closure; mask-length agreement; multi-buffer
    fetch shape; no full-buffer FSST decode in
    inner loop; no dense Arrow string materializer
    import; no per-row compressed slicing).
- `tests/architecture_test.rs`:
  - ADR 066 lockdown guards.
- `tests/row_group_sparse_projection_fetch.rs`:
  - FSST kernel mask-only output flows into ADR
    060 / 064 Case A;
  - FSST decline still engages ADR 065 Case B/C
    (regression).
- `tests/row_group_materialization.rs`:
  - end-to-end fixture: FSST predicate engages
    kernel → sparse projection on remaining
    columns → equality output equals full-dense
    oracle.
- `tests/encoding_secondary_index.rs`:
  - ADR 052 confirm-step on FSST-keyed secondary
    indexes (path coverage; conditional on
    existing test harness's ability to force the
    cascade without widening ADR 052; no ADR 052
    code changes).
- `benches/encoding_compute.rs` (or wherever the
  existing COCD bench lives — pinned in Phase 0):
  - representative FSST fixtures + closed atom set
    + decline fixtures + ADR 060 / 064 / 065
    composability rows + short-string and weak-
    compression adversarial rows (CPU-overhead
    diagnostics) + boundary-crossing-symbol fixture
    + no-compressed-row-slice regression fixture +
    corruption rows.
- `benches/row_group_fetch.rs`:
  - end-to-end ADR 065 → ADR 060 / 064 path on
    FSST predicate fixture.
- `plans/impl/0066_fsst_evaluator_bench_results.md`
  (at promotion only).
- `plans/adr/0058_compute_over_encoded_data.md`
  §"Where we are" amendment after promotion: row-
  group planner extends to multi-buffer for FSST;
  `Fsst` in evaluator catalog.
- `plans/adr/0059_native_cocd_kernels.md` §"Where
  we are" amendment if default allow-list extends.
- `plans/adr/0063_page_level_predicate_stats.md`
  §"Where we are" amendment: FSST native
  evaluation is row-group grain; FSST does not
  emit page stats in V1.
- `plans/adr/0065_dense_predicate_zip_with_sparse_projection.md`
  §"Where we are" amendment recording engagement-
  surface narrowing on FSST equality / membership
  / null predicates.
- `plans/wip/roadmap.md`: planning-cadence update
  at promotion.

## Non-goals

- No ADR 057 cascade shape changes.
- No removal of
  `EncodingCascade::Fsst { symbol_table_byte_len }`
  (cascade-field disposition is out of scope for
  ADR 066 per §"Status"; separate trivial follow-
  up).
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No page-grain FSST.
- No `Lt` / `Gt` / `Lte` / `Gte` / future `LIKE` /
  regex / substring / prefix FSST predicates.
- No FSST aggregate folds.
- No sparse projection eligibility lift for FSST.
- No process-wide symbol-table cache.
- **No compressed-literal byte equality (Strategy
  B) in V1.** Structurally unsafe under the
  current WFS1 encoder per ADR 066 §"Five
  forces" #3; symbols may cross Arrow value
  boundaries. Strategy B is V2 work gated on a
  writer amendment that enforces row-safe FSST
  framing.
- No constant-grain Disjoint short-circuit via
  literal unencodability (FSST escape byte makes
  it invalid).
- No new wire / descriptor / PBC opcode /
  capability / digest / `ObjectStore` surface.
- No new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` /
  `PredicateAtomOp` / `EncodedRowMask` variant.
- No Cargo feature gating FSST.
- No nightly rustc dependency.
- No retirement of ADR 057 dense FSST fallback (it
  remains the decline path).
- No retirement of ADR 065 Case B/C (it remains
  reachable for FSST predicates that decline the
  kernel).
- No Toron-visible PBC opcode addition.
- No bloom-filter prefilter on encoded equality.
- No expansion of the ADR 058 single-buffer fetch
  shape for non-FSST cascades.
- No ALP / Lz4Frame / Plain native predicate
  evaluator in this ADR.
- No encoded-length-bound pre-filter (skip stream
  advance for structurally impossible rows); V2
  optimization on top of V1's decoded-length pre-
  filter.

## Phase 0 — Contract audit

Read-only audit. The audit landed during
implementation; this section records the as-built
contract — what the staged tree pins. (Phases 1–5
that follow describe the shipped implementation in
the same as-built tense.)

Pinned sites (line numbers move as `manager.rs`
evolves; current pins below):

**FSST cascade and frame:**

- `EncodingCascadeKind::Fsst` definition site;
- the actual `WFS1` frame layout (per
  `src/storage/encoding/cascade/codec.rs`
  `encode_fsst_bytes` / `decode_fsst_bytes`):
  4-byte magic `b"WFS1"`, 1-byte symbol count
  (≤ 127), `2 × symbol_count` bytes of symbol
  table (each symbol is exactly a 2-byte pair),
  then the encoded payload. Payload semantics:
  byte `< 0x80` is a literal low-byte; byte
  `0xff` is the escape sentinel and the next
  byte is a literal high-byte (≥ 0x80); other
  bytes `≥ 0x80` (but not `0xff`) clear the
  high bit to index into the symbol table and
  expand to the 2-byte pair. **Decoded length
  is not stored in the frame** — it is passed
  to the decoder via
  `BufferEncoding.decoded_byte_len`.
- `EncodingCascade::Fsst { symbol_table_byte_len }`
  current state — verify the kernel can parse
  the symbol table from the WFS1 header (1-byte
  count + 2N pair-table bytes) without consulting
  this cascade field; the kernel reads
  `BufferEncoding.decoded_byte_len` for the
  expected decoded byte length (cascade-field
  disposition is out of scope for ADR 066 per
  §"Status");
- the FSST decoder entry point (Strategy A reuses
  this via the streaming token reader);
- whether the existing decoder separates symbol-
  table parse from value decode (preferred:
  separable; the kernel reuses the parser);
- **boundary-crossing symbol fixture
  construction**: build a fixture where a frequent
  two-byte symbol spans adjacent Arrow values;
  verify the streaming token decoder produces
  correct per-row decoded bytes when the symbol's
  decoded bytes land in two adjacent rows;
- **no-compressed-row-slice regression fixture**:
  build a fixture where a token crosses a row
  boundary AND the matching row begins inside that
  token; this is the load-bearing fixture against
  the compressed-row-slice mistake.

**Offsets buffer for variable-width Arrow types:**

- the typical writer cascade for `Utf8` /
  `LargeUtf8` / `Binary` / `LargeBinary` offsets
  buffers (likely `BitPacked` delta-of-Int32 or
  similar);
- whether `BufferEncoding` carries an
  `EncodingDirectory` entry for the offsets
  buffer;
- the existing fetch path that decodes encoded
  offsets;
- **confirm Arrow offsets for FSST-cascaded
  variable-width columns are decoded byte
  positions** (not encoded byte positions);
  `offsets[i+1] − offsets[i]` is the decoded
  length of row i; the encoded WFS1 values
  buffer has its own internal byte positions
  that Arrow offsets do not index. If Phase 0
  finds the writer emits encoded-position
  offsets instead, ADR 066 escalates to an ADR
  057 amendment first.
- the row-group predicate planner's current
  single-buffer assumption (validity + values
  today; needs validity + offsets + values for
  FSST leaves).

**Evaluator catalog and dispatch:**

- `EncodingComputeEvaluatorKind` — count of
  variants; whether `#[non_exhaustive]` or
  otherwise admits one-variant addition cleanly;
- `evaluator_kind_for_cascade` — current
  `Fsst → None` mapping;
- `encoding_compute_predicate_data_type_supported` — current type
  catalog (UTF-8 / Binary likely excluded);
- the dense-fallback site for FSST predicates:
  which pre-kernel eligibility gate fires; which
  `EncodingComputeFallbackReason` is observed
  (expected: `UnsupportedCascade`);
- `EncodingComputeConfig.enabled_predicate_evaluators`
  shape (set / vec / bitmap).

**Planner and fetch:**

- the row-group predicate planner's encoded-leaf
  fetch construction site (expand to multi-buffer
  for FSST);
- the encoded-page cap
  (`max_encoded_pages_per_record` per ADR 058) —
  confirm one FSST leaf counts as one chunk, not
  three;
- the row-group fetch plan structure
  (`RowGroupFetchPlan` per ADR 056) — whether
  multi-buffer leaves are expressible without a
  new type.

**Validity composition:**

- the `kernels/validity.rs` composition entry
  point ADR 066 needs;
- existing 3VL helpers reused.

**Bench harness:**

- the COCD bench harness
  (`benches/encoding_compute.rs` or equivalent) —
  whether it already generates FSST data; what
  shape; row counts; cardinalities;
- whether ADR 065's bench rows already include
  FSST predicate fixtures (those become Case A
  migration rows post-066);
- the row-group bench
  (`benches/row_group_fetch.rs`) — whether it can
  host the end-to-end FSST → Case A composition
  row.

**WFS1 header authority:**

- confirm the kernel can parse the FSST symbol
  table authoritatively from the `WFS1` header
  without consulting
  `EncodingCascade::Fsst { symbol_table_byte_len }`;
  if any existing reader path relies on the
  cascade field, surface that for council
  evaluation but treat its disposition as out of
  scope for ADR 066 (separate trivial follow-up).

Phase 0 also verifies the symbol-table parse cost
is bounded and worth amortizing once per buffer.
If Phase 0 finds the FSST cascade is implemented
in a way that prevents in-place multi-buffer
evaluation (e.g., symbol table is per-row, or
per-value framing requires decode beyond the
streaming reader's surface), implementation
escalates to a separate ADR 057 amendment first.
ADR 066 V1 does not silently expand scope.

**Exit gate (as-built):** Phase 0 audit findings
recorded in this section; the kernel is
implementable without an ADR 057 amendment and the
WFS1 header is a sufficient source of truth for the
symbol table.

## Phase 1 — FSST frame and offset readers

Passive helpers landed in `kernels/fsst.rs`; no
dispatch wiring yet. The shape below is the as-built
contract.

Files:
- `src/storage/encoding/compute/kernels/fsst.rs`
- `tests/encoding_compute_test.rs`

Work:

1. Factor the private `WFS1` parse logic
   (currently inline in
   `src/storage/encoding/cascade/codec.rs`
   `decode_fsst_bytes` near line 550) behind an
   internal helper `parse_fsst_frame` that
   validates the actual on-disk format:
   - 4 bytes magic (`b"WFS1"`);
   - 1 byte `symbol_count` (≤ 127, the writer
     caps at this in `encode_fsst_bytes` line
     513; the high bit of a payload byte is the
     symbol-vs-literal flag, so the index space
     is 7 bits and 0xff is reserved as the
     escape sentinel);
   - `2 × symbol_count` bytes of symbol table
     (each symbol is exactly a 2-byte pair —
     not a variable-length symbol);
   - The remainder is the encoded payload:
     bytes `< 0x80` are literal low-bytes;
     byte `0xff` is the escape sentinel and
     means the next byte is a literal high-byte
     (≥ 0x80); other bytes `≥ 0x80` (but not
     `0xff`) have the high bit cleared to index
     into the symbol table and expand to the
     2-byte pair.
   The decoded length is **not** embedded in
   the frame — it comes from
   `BufferEncoding.decoded_byte_len` for the
   FSST values buffer (the existing decoder
   takes `expected_byte_len: RecordBodyByteLen`
   from the caller). `parse_fsst_frame` returns
   an opaque `FsstFrame` carrying the symbol-
   table pair lookup and the (caller-supplied)
   expected decoded byte length for cross-
   check during streaming decode.

2. Add a streaming token reader that yields
   decoded bytes or two-byte symbols while
   tracking decoded byte position and current row
   index from the offsets buffer. The reader walks
   the encoded payload from byte 0 (it does
   **not** slice the encoded buffer by Arrow
   offsets; offsets are decoded byte positions,
   not encoded byte ranges). When a single FSST
   token contributes decoded bytes to two adjacent
   rows (because a symbol crosses a row boundary
   in the decoded stream), the reader assigns
   each decoded byte to the row whose decoded
   range it falls in. The reader does not allocate
   the full decoded values buffer; it emits
   decoded bytes on demand to per-row scratch.

3. Add `decode_offsets` for 32-bit (`Utf8` /
   `Binary`) and 64-bit (`LargeUtf8` /
   `LargeBinary`) Arrow offsets:
   - if `BufferEncoding` carries an
     `EncodingDirectory` entry for the offsets
     buffer, decode through the existing cascade
     decoder;
   - otherwise read fetched dense bytes directly;
   - validate offset count, monotonicity, non-
     negative offsets, and final offset matching
     the FSST frame's decoded byte length.

4. Tests:
   - parse correctness across representative
     tables (255-entry full table, sparse table,
     single-symbol table);
   - bad magic / truncated symbol table / invalid
     symbol reference / truncated escape — fail-
     closed via `EncodingComputeRejectCode`;
   - 32-bit and 64-bit offsets decode correctness;
   - encoded offsets buffer (decoded through
     cascade decoder) round-trips;
   - non-monotonic offsets / offset overflow /
     final-offset mismatch — fail-closed;
   - **boundary-crossing-symbol test**: row N
     ends mid-symbol; row N+1 begins mid-symbol;
     streaming token reader produces correct
     decoded bytes for both rows (load-bearing
     correctness test for Strategy A under WFS1
     stream-grain framing);
   - **no-compressed-row-slice regression test**:
     a frequent two-byte symbol crosses a row
     boundary and the matching row begins inside
     that token; verify the reader correctly
     assigns the symbol's decoded bytes to both
     adjacent rows; verify the kernel never
     attempts to slice the encoded buffer at
     `[offsets[i], offsets[i+1])` (architectural
     guard runs alongside).

**Exit gate:** WFS1 frame parser, streaming token
reader (boundary-safe; row-aware via offsets), and
offsets reader pass oracle tests against the
existing FSST decoder + writer. The reader provably
streams from start without per-row compressed
slicing. No dispatch wiring yet.

## Phase 2 — Equality kernel

Buffer-grain equality walker (Strategy A only)
landed in `kernels/fsst.rs`. The shape below is the
as-built contract.

Files:
- `src/storage/encoding/compute/kernels/fsst.rs`
- `tests/encoding_compute_test.rs`

Work:

1. `evaluate_fsst_equality(frame: &FsstFrame, offsets: &DecodedOffsets, validity: Option<&ValidityBitmap>, literals: &[LiteralBytes], op: PredicateAtomOp) -> Result<EncodedRowMask, EncodingComputeRejectCode>`
   — buffer-grain walker.

2. **Bucket literals by decoded byte length** at
   kernel entry:
   - `BTreeMap<usize, Vec<LiteralIndex>>` or
     equivalent;
   - longest-literal byte count → per-row scratch
     buffer size.

3. **Streaming walk + length pre-filter + byte
   comparison** (the kernel walks the encoded
   payload **once** from byte 0; rows are
   delineated via the offsets buffer's decoded
   byte positions). Compute an equality /
   membership match per row first, then apply
   op-specific inversion before validity
   composition:
   - initialize a streaming token reader at the
     start of the encoded WFS1 payload;
   - for each row i (in order):
     - compute decoded length from
       `offsets[i+1] − offsets[i]`;
     - look up decoded length in the literal-
       length bucket;
     - if no literal of that length: set
       `row_matches_any_literal = false`;
       advance the streaming reader past the
       row's decoded bytes (no scratch copy, no
       byte comparison);
     - if one or more literals of that length:
       copy decoded bytes into per-row scratch
       (≤ longest-literal bytes) as the
       streaming reader emits them; on row
       completion, byte-compare scratch to the
       length-matching literals to determine
       `row_matches_any_literal`;
     - apply op-specific inversion:
       - `Eq` / `InList`: bit =
         `row_matches_any_literal`;
       - `Neq`: bit =
         `!row_matches_any_literal` (a length-
         mismatch row cannot equal any literal,
         so it satisfies `Neq`);
     - fold bit with validity via
       `kernels/validity.rs`;
   - **the kernel does not slice the encoded
     buffer by Arrow offsets** — offsets are
     decoded byte positions; slicing the encoded
     buffer by `[offsets[i], offsets[i+1])` would
     index into a different stream than where the
     row's encoded bytes live.

4. **Validity composition** via
   `kernels/validity.rs`. `IsNull` / `IsNotNull`
   reuse the existing ADR 059 validity-only path
   entirely (kernel returns immediately for those
   atoms; dispatch is kept by ADR 058's existing
   path).

5. **Mask shape selection** via existing
   `EncodedRowMask` shape selector. Default
   `Bitmap` for high-cardinality fixtures;
   `Runs` only on clustered low-selectivity
   fixtures.

6. **No constant-grain Disjoint via literal
   unencodability.** Per ADR 066 §"What this ADR
   does not propose" — FSST escape byte encodes
   arbitrary bytes; the V1 Disjoint claim comes
   from the length pre-filter
   ("no literal length matches any row length").

7. **Tests** (Strategy A):
   - `Eq` selectivity 0% / 1% / 5% / 25% / 50% /
     95%;
   - `Neq` (inverse coverage);
   - `InList` with 1 / 4 / 16 constants;
   - mask byte-identical to dense oracle;
   - all-survivor / no-survivor edge cases;
   - length pre-filter zero-byte-comparison
     verified by bench instrumentation;
   - `Utf8` / `LargeUtf8` / `Binary` /
     `LargeBinary` coverage;
   - empty-string fixtures;
   - escaped high-bit byte fixtures;
   - long-string fixtures (test scratch buffer
     bounds);
   - nullable fixtures (validity composition);
   - `IsNull` / `IsNotNull` (validity-only;
     kernel-not-invoked path);
   - **boundary-crossing-symbol fixture: kernel
     mask byte-identical to dense oracle on row
     pairs where a symbol spans the boundary**
     (load-bearing correctness regression);
   - **no-compressed-row-slice regression fixture:
     a frequent two-byte symbol crosses a row
     boundary AND the matching row begins inside
     that token; kernel mask byte-identical to
     dense oracle (catches the "slice the encoded
     buffer by offsets" mistake)**;
   - corruption fixtures (malformed WFS1, non-
     monotonic offsets, decoded-length mismatch
     — fail-closed via
     `EncodingComputeRejectCode`).

**Exit gate:** Kernel equality walk produces
byte-identical masks vs dense oracle on every
correctness fixture across the closed atom × type
× nullability × selectivity matrix, including
boundary-crossing-symbol and no-compressed-row-slice
regression fixtures.

## Phase 3 — Dispatch and planning

The kernel is wired into ADR 058's dispatch and the
row-group planner is extended for FSST leaves. The
kernel reads the FSST symbol table from the WFS1
header authoritatively; no cascade-record changes.
V1 ships without a plan-time fetch-byte economics
gate (kernel and dense fallback fetch the same
encoded payload); CPU-cost adversarial cases are
policed by the Phase 7 bench. The shape below is
the as-built contract.

Files:
- `src/storage/encoding/compute/types.rs` (catalog)
- `src/storage/encoding/compute/eligibility.rs`
  (or equivalent — pinned in Phase 0)
- ADR 058 row-group predicate planner site
  (pinned in Phase 0)
- `src/storage/encoding/compute/config.rs` (or
  equivalent)

Work:

1. **Catalog extension:**
   - append `Fsst` (label `"fsst"`) to
     `EncodingComputeEvaluatorKind`;
   - update `ALL`, `as_str`, `from_label`, config
     validation tests, and architectural guards;
   - update `evaluator_kind_for_cascade`:
     `EncodingCascade::Fsst → EncodingComputeEvaluatorKind::Fsst`;
   - update `encoding_compute_predicate_data_type_supported` to admit
     `Utf8` / `LargeUtf8` / `Binary` /
     `LargeBinary` only through the `Fsst` kind
     (evaluator-aware type eligibility).

2. **Pre-kernel eligibility extension:**
   - admit FSST cascade buffers into the FSST
     kernel only when:
     - atom is `Eq` / `Neq` / `InList` (`IsNull` /
       `IsNotNull` are validity-only and never
       reach the FSST kernel; they continue on the
       existing ADR 059 validity-only path);
     - column type is `Utf8` / `LargeUtf8` /
       `Binary` / `LargeBinary`;
     - literal coerces cleanly to bytes;
     - offsets buffer is fetchable (decoded
       inline or via cascade decoder);
     - **the FSST predicate column is NOT in
       `requirement.output_columns`** (Sub-case A1;
       see ADR 066 Decision §12). Sub-case A2
       (predicate-output overlap on the FSST
       column) routes through the existing ADR 065
       Case B path with no kernel engagement; the
       overlap check fires inside
       `build_encoding_compute_phase_b_plan`, which
       declines to `UnsupportedCascade` and falls
       through to the dense path before the kernel
       is invoked.
   - decline non-V1 atoms / types / coercions via
     existing `UnsupportedProgram` fallback path
     (no new fallback variant). Sub-case A2 is not
     a "fallback" — it is the existing ADR 065
     Case B path preserved.

3. **Row-group planner shape extension:**
   - extend the encoded-leaf fetch construction to
     fetch validity + offsets + values together
     for FSST predicate leaves;
   - non-FSST cascades unchanged (continue to
     fetch validity + values);
   - count each FSST row-group leaf as one
     encoded chunk for the existing
     `max_encoded_pages_per_record` cap (the
     multi-buffer fetch is one logical leaf, not
     three).

4. **WFS1 header authority (no cascade-field
   change):**
   - the kernel parses the FSST symbol table from
     the `WFS1` header authoritatively;
   - `EncodingCascade::Fsst { symbol_table_byte_len }`
     is unmodified; cascade-field disposition is
     out of scope per §"Status";
   - architectural guard asserts the kernel does
     not consult the cascade field.

5. **Default enablement:**
   - Phase 7 outcome decides whether `Fsst` joins
     `EncodingComputeConfig::default()`'s
     `enabled_predicate_evaluators`;
   - if Phase 7 misses gates, kernel ships
     default-off with a separate Phase evidence
     row recording the gap.

6. **Tests:**
   - FSST `Eq` engages kernel; produces mask-
     only `PhaseBOutcome`;
   - FSST `Lt` declines; routes to dense;
     populates `predicate_projection_by_group`
     (ADR 065 Case B/C baseline preserved);
   - non-FSST cascade unaffected;
   - missing offsets / unsupported type /
     disabled evaluator config → decline to
     dense;
   - encoded-page cap respected (one FSST leaf
     counts as one chunk);
   - `enabled_predicate_evaluators` gate
     respected;
   - WFS1 header authority: kernel parses symbol
     table from header without consulting the
     cascade field; architectural guard passes.

**Exit gate:** Production read path engages FSST
kernel on eligible economical predicates; routes
declines to dense unchanged; multi-buffer leaf
fetch shape verified on FSST fixtures; kernel
reads symbol table from WFS1 header.

## Phase 4 — ADR 060 / 064 / 065 composition

Composition paths verified end-to-end. The shape
below is the as-built contract.

Files:
- `tests/row_group_sparse_projection_fetch.rs`
- `tests/row_group_materialization.rs`
- `tests/encoding_secondary_index.rs`

Work:

1. **Sub-case A1 tests** (FSST predicate column
   not in output): FSST kernel mask-only output
   flows into ADR 060 / 064 sparse projection
   (Case A) for the remaining (fixed-width)
   projection requirement; output byte-identical
   to full-dense oracle.

2. **Sub-case A2 tests** (FSST predicate column
   in output, predicate-output overlap on the
   FSST column): `build_encoding_compute_phase_b_plan`
   detects overlap, declines to
   `UnsupportedCascade`, and the read pipeline
   falls through to the existing ADR 065 Case B
   path (dense decode-then-scan; populates
   `predicate_projection_by_group`). Verify the
   path is byte-identical to today's behavior
   (regression coverage); kernel does not engage.

3. Add tests confirming FSST predicates that
   decline the kernel for other reasons (range,
   future LIKE, atom mismatch) continue to engage
   ADR 065 Case B/C as today. Corruption /
   invariant failure during kernel execution fails
   closed via `EncodingComputeRejectCode` (does
   **not** route to ADR 065 Case B/C dense
   fallback).

4. Verify ADR 052 secondary-index point reads on
   FSST-keyed columns gain kernel coverage
   through the existing confirm-step dispatch
   (no ADR 052 code changes; new path coverage
   only). **Conditional on existing test harness
   ability** to force the FSST cascade on a
   secondary-index column without widening ADR
   052; if not feasible, document as Phase 4
   gap and defer to a separate follow-up.

5. Verify ADR 065 zip output path remains
   untouched on FSST declines (no double-filter
   regression).

6. End-to-end fixture: FSST predicate engages
   kernel → Case A sparse projection on
   remaining columns → output equals full-dense
   oracle byte-for-byte.

7. **Boundary-crossing-symbol regression in the
   composition test**: end-to-end fixture
   includes a row pair where a symbol spans the
   boundary; verify the full pipeline produces
   correct output.

**Exit gate:** ADR 060 / 064 / 065 composition is
byte-identical pre / post on regression fixtures;
new Case A engagement on FSST equality fixtures
verified; boundary-crossing-symbol regression
covered end-to-end.

## Phase 5 — Architecture guards

ADR 066 lockdown guards landed in
`tests/architecture_test.rs`. The guard set:

- `EncodingComputeEvaluatorKind` count = previous
  + 1 (exactly one new variant `Fsst`).
- `evaluator_kind_for_cascade` maps `Fsst →
  EncodingComputeEvaluatorKind::Fsst`.
- `encoding_compute_predicate_data_type_supported` admits `Utf8` /
  `LargeUtf8` / `Binary` / `LargeBinary` only
  through the FSST evaluator path; architectural
  guard asserts non-FSST evaluator kinds do not
  inherit UTF-8 / Binary support.
- No new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` /
  `PredicateAtomOp` / `EncodedRowMask` /
  `EncodingCascadeKind` / `RowGroupRejectCode` /
  `RowGroupFallbackReason` / `EncodingRejectCode`
  / `EncodingFallbackReason` / `L2FetchStrategy`
  / `NodeCapability` variant.
- No new IPC `custom_metadata` key.
- No `ENCODING_DIRECTORY_FORMAT_VERSION` bump.
- No new wire `MsgType` / `FRAME_FLAG_*` /
  digest domain / `ObjectStore` trait method.
- No new descriptor field / fingerprint surface
  change.
- No new public PBC opcode.
- No Cargo feature.
- No nightly rustc dependency.
- ADR 057 `Fsst` cascade byte layout unchanged.
  `EncodingCascade::Fsst { symbol_table_byte_len }`
  is unmodified; an architectural guard asserts
  the kernel reads the symbol table from the
  `WFS1` header and does not consult the cascade
  field.
- **Streaming token decode is the only V1
  survivor-row comparison path; literal pre-
  encoding (Strategy B) is absent from the kernel
  module.**
- **The kernel does not derive per-row compressed
  slices from Arrow offsets.** Offsets are decoded
  byte positions; the kernel streams the encoded
  WFS1 payload from byte 0 and uses offsets only
  as decoded row boundaries. Architectural guard
  pins this; the no-compressed-row-slice
  regression fixture is in the correctness suite.
- `kernels/fsst.rs` may import the writer's FSST
  decoder for the streaming token reader; it must
  not import the dense Arrow string materializer
  or use full-buffer FSST decode as its inner
  loop.
- The row-group planner's FSST predicate leaf
  fetches validity + offsets + values together
  (multi-buffer fetch shape).
- Encoded-page cap counts one FSST leaf as one
  chunk (architectural guard asserts via
  fixture).
- **Boundary-crossing-symbol fixture is in the
  correctness suite; its mask is byte-identical
  to dense oracle.**
- **No-compressed-row-slice regression fixture is
  in the correctness suite** (token crosses row
  boundary AND matching row begins inside that
  token; mask byte-identical to dense oracle).
- ADR 060 / 063 / 064 / 065 surfaces unchanged.
- New helper names avoid planning-phase
  identifiers and version suffixes (`FsstFrame`,
  `DecodedOffsets`, `LiteralBytes`,
  `parse_fsst_frame`, `decode_offsets`,
  `evaluate_fsst_equality` are semantic).
- No project-name-prefixed identifiers.
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte
  layouts, ADR 056 / 057 / 063 directory shapes,
  ADR 058 / 059 evaluator catalogs (by addition
  only), ADR 060 / 064 sparse projection
  eligibility, ADR 061 closed chunk-size set,
  ADR 062 `RowGroupSizingProfile` catalog, ADR
  065 manager three-case dispatch: all
  unchanged.
- Cross-replica byte-equal mask output across
  two writers with the same FSST fixture and
  same predicate.
- ADR 060 / 064 mask-only sparse projection
  byte-identical pre / post on shared fixtures.
- ADR 065 Case B/C still reachable for FSST
  predicates that decline the kernel
  (regression coverage).

**Exit gate:** `cargo test --test
architecture_test` passes.

## Phase 6 — Focused tests

Focused test commands:

```sh
cargo test fsst --lib
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test row_group_sparse_projection_fetch
cargo test --test row_group_materialization
cargo test --test architecture_test
cargo test --test encoding_secondary_index
cargo test --test encoding_chunk_layout
```

Required coverage: per Phases 1–5.

**Exit gate:** All focused tests pass; existing
ADR 058 / 059 / 060 / 063 / 064 / 065 tests
remain green.

## Phase 7 — Bench matrix

Extend the COCD bench (existing
`benches/encoding_compute.rs` or equivalent —
Phase 0 pins the path) and the row-group bench
(`benches/row_group_fetch.rs`) with FSST rows.
Land evidence at promotion in:

```text
plans/impl/0066_fsst_evaluator_bench_results.md
```

**Bench dimensions:**

- row counts: 8K / 65K / 1M.
- FSST fixtures:
  - URL-shaped (high cardinality, low-entropy
    prefixes; high FSST compression);
  - log-line-shaped (medium cardinality,
    structured tokens);
  - free-form text (high cardinality, high
    entropy);
  - identifier-shaped (UUID, ULID — high
    cardinality, fixed width);
  - JSON-key-shaped (very low cardinality across
    keys, high cardinality across values);
  - **boundary-crossing-symbol fixture**
    (correctness regression for streaming token
    decoder; mask byte-identical to dense oracle
    is the gate);
  - **no-compressed-row-slice regression fixture**
    (token crosses row boundary AND matching row
    begins inside that token; load-bearing fixture
    against the compressed-row-slice mistake);
  - **short-string adversarial** (≤ 16 bytes per
    row; pathological case for streaming token
    decode amortization; kernel engages and the
    bench records kernel-vs-dense CPU overhead);
  - **weak-compression adversarial** (high
    entropy; near-pass-through symbol table;
    kernel engages and the bench records kernel-
    vs-dense CPU overhead).
- predicate atoms (kernel-engaged):
  - `Eq` selectivity 0% / 1% / 5% / 25% / 50% /
    95%;
  - `Neq` (length-mismatch rows must emit
    survivors via op-specific inversion);
  - `InList` with 1 / 4 / 16 constants;
- predicate atoms (validity-only path; kernel not
  invoked but regression coverage required):
  - `IsNull` / `IsNotNull`;
- decline fixtures (route to dense): `Lt`, `Gt`,
  `Lte`, `Gte`.
- column shape:
  - null-free fixtures;
  - nullable fixtures;
  - `Utf8` / `LargeUtf8` / `Binary` /
    `LargeBinary` coverage;
  - empty-string fixtures.
- length-pre-filter Disjoint fixture (literal
  length matches no row length; expect zero
  byte-comparison work via length pre-filter;
  streaming walk still happens).
- Prefilter mode fixture (FSST equality leaf +
  non-FSST residual leaf in conjunction).
- **Sub-case A1 rows** (FSST predicate column
  not in output): kernel engages → Case A sparse
  projection on remaining (fixed-width)
  projection columns. This is the V1 win.
- **Sub-case A2 rows** (FSST predicate column in
  output): kernel does NOT engage; manager
  dispatches through existing ADR 065 Case B
  path; record byte-identical regression vs.
  today's behavior.
- ADR 065 baseline rows (FSST decline → Case
  B/C; same fixture as ADR 066 Sub-case A1
  engagement to record byte savings via the
  Case A migration; Sub-case A2 fixtures match
  today's Case B baseline within noise).
- ADR 052 confirm-step row (FSST-keyed
  secondary-index point read; conditional per
  §Phase 4).
- Corruption rows (malformed WFS1, non-monotonic
  offsets, decoded-length mismatch — fail-
  closed).
- Cross-replica determinism rows.
- End-to-end ADR 065 → ADR 060 / 064 row (in
  `benches/row_group_fetch.rs`).

**Rows record:**

- route (`fsst_kernel_case_a1`,
  `dense_case_b_overlap_a2`,
  `dense_fallback_atom_unsupported`,
  `dense_fallback_type_unsupported`,
  `corruption_fail_closed_no_fallback`,
  `none_length_prefilter`,
  `case_a_after_fsst`,
  `case_b_after_fsst_decline`,
  `corruption_fail_closed`);
- dense decode plaintext bytes (FSST → Arrow);
- kernel symbol-table parse bytes (per buffer);
- kernel offsets-buffer plaintext bytes (per
  buffer);
- kernel decoded-scratch bytes (per buffer; sum
  of per-row decoded lengths under streaming
  Strategy A);
- length-pre-filter elimination rate (fraction
  of rows skipped without scratch copy or byte
  comparison);
- dense wall-clock;
- kernel wall-clock;
- speedup vs. dense;
- mask correctness (vs. dense oracle);
- mask shape (`Bitmap` / `Runs` / `All` /
  `None`);
- ADR 060 / 064 sparse projection plaintext
  bytes (when composing);
- total query plaintext bytes (predicate +
  projection) for both routes (kernel + Case A
  vs. dense + Case B/C);
- cross-replica digest;
- iterations.

**Promotion gates (matching ADR 060 / 064 / 065
cadence):**

1. **Predicate-evaluation wall-clock ≥ 1.5×.**
   At clustered 5% selectivity for `Eq` /
   `InList`, predicate evaluation wall-clock is
   at least 1.5× faster than dense materialize-
   then-PBC on representative FSST-compressible
   UTF-8 fixtures.
2. **End-to-end query wall-clock ≥ 1.2×** when
   the exact FSST mask feeds fixed-width sparse
   projection through ADR 065 → ADR 060 / 064
   Case A path.
3. **Dense-fallback overhead ≤ 5%** on truly
   declined paths (range, future LIKE,
   ineligible atoms / types / layouts) —
   measures the cost of threading FSST
   eligibility through dispatch when the kernel
   never engages.

**Headline diagnostics (recorded, not gating):**

- ≥ 2× CPU speedup on representative `Eq`
  fixtures (matches ADR 059's promote-to-
  default cadence).
- **Kernel-vs-dense CPU overhead recorded on
  short-string and weak-compression adversarial
  fixtures.** Kernel engages on these fixtures
  (V1 has no plan-time decline gate); the bench
  records kernel wall-clock vs dense decode +
  PBC per fixture to surface dense-faster
  regimes that would motivate a V2 CPU-cost
  gate. No V1 numeric threshold; observed
  overhead informs the V2 decision.
- Encoded-payload-vs-decoded-plaintext byte
  ratio per fixture (workload characterization,
  not a fetch-byte promotion gate; kernel and
  dense fallback fetch the same encoded
  payload).
- Total query plaintext byte reduction across
  the dense baseline (FSST decode + Arrow scan
  + ADR 065 Case B zip) vs. the kernel path
  (encoded walk + Case A sparse projection).
- Length-pre-filter elimination rate per
  fixture.

**Correctness gates:**

- Mask byte-identical to dense oracle on every
  correctness fixture across the closed atom ×
  type × nullability × selectivity matrix.
- **Boundary-crossing-symbol fixture mask byte-
  identical to dense oracle.**
- **No-compressed-row-slice regression fixture
  mask byte-identical to dense oracle.**
- Length pre-filter zero byte-comparison work
  on Disjoint-length fixtures (verified via
  bench instrumentation).
- Corrupt WFS1 frames and corrupt offsets fail
  closed through `EncodingComputeRejectCode`;
  no partial masks.
- ADR 060 / 064 mask-only composition byte-
  identical on engagement fixtures.
- ADR 065 Case B/C reachable for FSST
  predicates that decline the kernel.
- Cross-replica determinism passes on
  representative fixtures.

**Exit gate:** Bench artifact provides council
promotion or revision evidence.

## Phase 8 — Promotion

Files (at promotion):

- `plans/impl/0066_fsst_evaluator_bench_results.md`
  — Phase 7 evidence artifact.
- `plans/adr/0058_compute_over_encoded_data.md`
  §"Where we are" amendment: `Fsst` in
  evaluator catalog; row-group planner extends
  to multi-buffer for FSST leaves.
- `plans/adr/0059_native_cocd_kernels.md` §"Where
  we are" amendment if default allow-list
  extends to include `Fsst`.
- `plans/adr/0063_page_level_predicate_stats.md`
  §"Where we are" amendment: FSST native
  evaluation is row-group grain; FSST does not
  emit page stats in V1.
- `plans/adr/0065_dense_predicate_zip_with_sparse_projection.md`
  §"Where we are" amendment recording
  engagement-surface narrowing on FSST equality
  / membership / null predicates.
- `plans/wip/roadmap.md` planning-cadence update.

Work:

1. Pin Phase 7 bench parameters in the bench-
   results artifact.
2. Council decides default-enablement based on
   Phase 7 evidence.
3. Mark ADR 066 shipped only after code, tests,
   and bench evidence land together.

**Exit gate:** ADR 066 promoted; ADR 058
evaluator catalog extends by one; ADR 058 row-
group planner extends to multi-buffer for FSST
leaves; ADR 065 V1 surface narrows on FSST
`Eq` / `Neq` / `InList` Sub-case A1 predicates
(Sub-case A2 stays on Case B; null predicates
remain on the ADR 059 validity-only path).

## Verification gate

Before promotion:

```sh
cargo fmt --all
cargo test fsst --lib
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test architecture_test
cargo test --test row_group_sparse_projection_fetch
cargo test --test row_group_materialization
cargo test --test row_group_fetch_plan
cargo test --test encoding_directory
cargo test --test encoding_secondary_index
cargo test --test encoding_chunk_layout
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test multirow
cargo test --test config_loader
cargo check
cargo check --benches
cargo bench --bench encoding_compute -- --quick
cargo bench --bench row_group_fetch -- --quick
git diff --check
git diff --cached --check
```

Full bench:

```sh
cargo bench --bench encoding_compute
cargo bench --bench row_group_fetch
```

## Risks and mitigations

**FSST cascade encoding shape prevents in-place
multi-buffer evaluation.** Phase 0 audit gates
the implementation. If the symbol table is per-
row, or per-value framing requires decode beyond
what the streaming reader admits, ADR 066
escalates to a separate ADR 057 amendment first.
V1 does not silently expand scope.

**Arrow offsets accidentally treated as encoded
byte ranges.** This is the single most likely
implementation mistake — Arrow offsets are
decoded byte positions, not encoded byte
positions; slicing the encoded buffer by
`[offsets[i], offsets[i+1])` would index into a
different stream than where the row's encoded
bytes live. Mitigation: Phase 1 streaming token
reader walks the encoded payload from byte 0 and
explicitly does not slice by offsets; Phase 5
architectural guard asserts the kernel does not
derive per-row compressed slices from offsets;
Phase 1 / 2 / 7 no-compressed-row-slice
regression fixtures (token crosses row boundary,
matching row begins inside that token) catch the
mistake at the mask-equality gate.

**Boundary-crossing symbols break streaming
token decoder correctness.** The WFS1 encoder
compresses the entire values buffer as one
stream; symbols may span Arrow value boundaries.
Mitigation: Phase 1 streaming token reader
correctly assigns decoded bytes to adjacent rows
when a token spans a row boundary; Phase 1
boundary-crossing test is a load-bearing
correctness gate; Phase 2 / 4 / 7 include the
fixture in kernel correctness, end-to-end
composition, and bench correctness gate.

**Row-boundary drift between offsets and FSST
token stream.** Strategy A: per-row scratch
buffer must align with offsets; if the streaming
reader over-reads or under-reads relative to
the offsets-derived encoded length, mask is
wrong. Mitigation: Phase 1 cross-validates
final offset against FSST decoded length; per-
row decoded byte count cross-validates against
the streaming reader's emitted byte count;
oracle comparison against dense decode on every
fixture.

**Symbol-table parse cost dominates kernel time
on small selectivities.** Mitigation: Phase 7
bench instruments parse-vs-walk time
separately. If parse cost dominates, V2
considers process-wide caching with explicit
determinism + eviction story.

**Atom-set closure drift.** A future contributor
adds range / future-LIKE handling to the FSST
kernel without an ADR amendment. Mitigation:
Phase 5 architectural guard asserts the FSST
kernel atom set is exactly the V1 closed set
(`Eq` / `Neq` / `InList`).

**Promote-default before evidence.** Phase 8
gates default-enablement on Phase 7 bench
evidence per ADR 059 cadence.

**ADR 065 regression: FSST predicates that
decline kernel should still engage Case B/C.**
Phase 4 explicit regression coverage; Phase 7
bench rows record the Case B/C baseline
alongside Case A engagement.

**Cross-replica non-determinism.** Kernel is
pure deterministic (FSST encoding is greedy and
deterministic given a fixed table; streaming
token decoder is deterministic; byte-compare is
deterministic; length pre-filter is
deterministic; mask-shape selector is
deterministic). Phase 7 cross-replica
determinism row gates empirically.

**Kernel imports forbidden surfaces.** Phase 5
architectural guard asserts `kernels/fsst.rs`
may import the writer's FSST decoder (for the
streaming token reader) but must not import
the dense Arrow string materializer or use
full-buffer FSST decode as its inner loop.

**ADR 057 byte layout drift between writer and
reader.** Phase 7 bench asserts byte-identical
mask output across writer + reader pairs from
independent encoder paths.

**Symbol-table allocation churn.** Per-buffer
parse allocates a `FsstFrame` value. Bounded by
buffer count per query; `FsstFrame` is small
(≤ 2 KiB); held on stack or in a bounded
scratch arena. Per-row scratch buffer
(≤ longest-literal bytes) is similarly
bounded. Phase 7 records allocation count if
regression suspected.

**`enabled_predicate_evaluators` shape doesn't
admit a clean extension.** Phase 0 confirmed the
shape admits the `Fsst` addition without a config-
format change.

**Multi-buffer planner regression.** Extending
the row-group predicate planner's encoded-leaf
fetch shape from single-buffer (validity +
values) to multi-buffer (validity + offsets +
values) for FSST is a real change to a load-
bearing path. Mitigation: Phase 3 isolates the
change to FSST-cascade leaves; non-FSST
cascades take the existing single-buffer path;
architectural guard asserts the shape per
cascade; Phase 4 verifies ADR 060 / 064 / 065
composition is byte-identical pre / post on
regression fixtures.

**Kernel accidentally consults the cascade
field.** The kernel must read the FSST symbol
table from the `WFS1` header, not from
`EncodingCascade::Fsst { symbol_table_byte_len }`
(which is currently zero in the writer; cascade-
field disposition is out of scope per ADR 066).
Mitigation: Phase 5 architectural guard asserts
the kernel does not import or read the cascade
field; Phase 1 `parse_fsst_frame` reads the
header authoritatively and round-trips against
the writer's encoder.

**Kernel CPU regresses on short-string or weak-
compression adversarial workloads.** V1 ships
without a plan-time CPU-cost gate (kernel and
dense fallback fetch the same encoded payload,
so a fetch-byte gate has nothing to compare) and
the kernel engages on these fixtures.
Mitigation: Phase 7 bench records kernel-vs-dense
CPU overhead per short-string and weak-
compression adversarial fixture as a diagnostic
(no V1 numeric threshold); if observed overhead
is large or systemic, V2 considers a CPU-cost
gate or a writer-side pre-decision (e.g., emit
Plain cascade for short-string columns at write
time).

**Encoded-page cap miscounts FSST leaves.** If
a multi-buffer FSST leaf is counted as three
chunks instead of one, the cap fires earlier
and FSST leaves on wide schemas decline
unnecessarily. Mitigation: Phase 3 explicitly
sets the count to one per FSST leaf; Phase 5
architectural guard asserts via fixture.

**SIMD escalation if 1.5× wall-clock gate
misses.** Per ADR 059's escalation strategy:
first try tighter stable-Rust loop shape and
bounded scratch reuse; second escalation is a
small SIMD helper crate; nightly rustc, Cargo
feature gates, and operator-visible SIMD knobs
remain out of scope. Phase 7 records the
escalation outcome if needed.

**Strategy B (pre-encoded literal compare)
deferral pressure.** If Phase 7 evidence shows
per-row streaming decode dominates kernel time
and obviously gating workloads, V2 work on the
writer amendment + Strategy B is unblocked.
Mitigation: V1's structural rejection is
correctness-anchored, not performance-
anchored; the V2 path is open if the
performance case earns it.
