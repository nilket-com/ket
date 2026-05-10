# Implementation Plan: ADR 067 — Row-Canonical FSST Framing and Strategy B Predicate Evaluator

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/067_row_canonical_fsst.md`

Inherits substrate:

- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
  — `EncodingCascade` catalog, `BufferEncoding` shape,
  the existing `Fsst` cascade variant + WFS1 frame
  (unchanged), `select.rs` cascade selection, the
  multi-buffer values + decoded offsets + validity
  layout for variable-width Arrow types,
  `ENCODING_DIRECTORY_FORMAT_VERSION` discipline.
- `plans/adr/058_compute_over_encoded_data.md` — closed
  predicate evaluator catalog + dispatch +
  `EncodedRowMask` algebra + reject taxonomy +
  `enabled_predicate_evaluators` allow-list + row-group
  predicate planner.
- `plans/adr/059_native_cocd_kernels.md` — native
  kernel module layout under
  `src/storage/encoding/compute/kernels/`; central
  `kernels/validity.rs` 3VL composition; promote-to-
  default cadence; SIMD strategy (stable rustc + auto-
  vectorization first; no nightly, no Cargo feature
  gate).
- `plans/adr/060_sparse_projection_fetch_narrowing.md`,
  `plans/adr/064_nullable_sparse_projection.md` —
  mask-only `PhaseBOutcome` is the contract sparse
  projection consumes; FSST not in V1 sparse-
  projection catalog (variable-width).
- `plans/adr/065_dense_predicate_zip_with_sparse_projection.md`
  — bridge for cascade-uncovered predicates; `Fsst`
  Strategy A and `FsstRowSafe` Strategy B both produce
  the mask shape Case A consumes.
- `plans/adr/066_fsst_evaluator.md` + V1 perf-
  iteration post-mortem
  (`plans/impl/066_fsst_evaluator_bench_results.md`
  §"V1 Perf Iteration 1") — Strategy A reader stays
  the dispatch target for `EncodingCascade::Fsst`
  cascade buffers indefinitely.
- `plans/conventions/` — newtype-units, no-version-
  suffixes (note: `WFS2` magic is on-disk format
  version, not a code identifier), no-planning-phase-
  names, no-premature-backward-compat, capability-vs-
  feature naming, no-project-name-in-protocols.

## Status

Council-converged 2026-05-10 after five iterations
across Claude / Codex / Gemini, implementing the
row-canonical FSST writer amendment + Strategy B
predicate evaluator per
`plans/adr/067_row_canonical_fsst.md`.

The plan is sequenced so dense WFS2 decode lands
before Strategy B is allowed to engage (correctness
ordering — dense decode is the correctness oracle).
The writer's per-buffer actual-size guard ships in
Phase 2 alongside the encoder, so no Phase 7 bench
iteration runs without per-buffer density protection.
Strategy B kernel (Phase 3) follows once WFS2 round-
trips correctness fixtures.

The full council-decision list (12 items, organized
by topic) lives in
`plans/adr/067_row_canonical_fsst.md` §"Council
decisions"; the implementation contract below
restates the operationally-relevant subset for the
implementer.

## Implementation contract

- WFS1 and `EncodingCascade::Fsst` remain byte-for-
  byte unchanged. ADR 066 V1 reader stays the
  dispatch target for `Fsst` cascade buffers
  indefinitely.
- WFS2 dense decode lands before Strategy B; Strategy
  B never engages on a buffer that dense decode
  rejects.
- WFS2 writer enforces row-canonical encoding:
  - symbol-table training counts adjacent byte pairs
    only within Arrow row boundaries;
  - greedy substitution never emits a symbol pair
    whose two bytes span a row boundary;
  - tie-breaking is deterministic (descending count,
    ascending pair key — same as WFS1) so two
    encoders fed the same row bytes + symbol table
    produce byte-identical output.
- Writer + reader literal pre-encoder share a single
  canonical encoder helper. A property test compares
  `pre_encode_literal(R, T)` against
  `writer_encoded_row_bytes(R, T)` for randomly-
  chosen R and T; outputs must be byte-identical.
- Writer per-buffer actual-size guard: select
  `FsstRowSafe` only when both
  `wfs2_len ≤ decoded_plaintext_len` and
  `wfs2_len ≤ ceil(wfs1_len * 1.10)`.
  Per-buffer runtime check, not just a bench-fixture
  gate.
- `EncodingCascadeKind::FsstRowSafe = 9` (after
  existing `Fsst = 7`, `Alp = 8`).
  `EncodingCascade::FsstRowSafe { symbol_table_byte_len:
  RecordBodyByteLen }` carries the same shape as the
  existing `Fsst` variant for parallel cleanup later;
  the WFS2 frame header is authoritative for the
  symbol table.
- `ENCODING_DIRECTORY_FORMAT_VERSION` bumps by one.
  Old binaries reject the bumped version before
  inspecting cascades; rollout is reader-before-
  writer.
- `EncodingComputeEvaluatorKind::FsstRowSafe` (label
  `"fsst_row_safe"`) extends ADR 058's catalog by one;
  existing seven (after ADR 066) unchanged.
  `evaluator_kind_for_cascade` extends to map
  `EncodingCascade::FsstRowSafe → EncodingComputeEvaluatorKind::FsstRowSafe`.
  `encoding_compute_predicate_data_type_supported_for_evaluator`
  admits `Utf8` / `LargeUtf8` / `Binary` /
  `LargeBinary` for the new kind.
- Strategy B reader does not decode any encoded byte
  on the predicate path. The encoded payload is
  untouched except for bytes inside length-matching
  encoded ranges (and only via memcmp). Length-
  mismatch rows are skipped via the offsets table
  without touching encoded bytes at all.
  Architectural guard pins this.
- Per-buffer pre-encoded literal cache, bounded:
  `Eq` uses a small fixed-size scratch path; `InList`
  with longer literals uses a bounded heap allocation
  (`Vec<u8>` of capacity `2 × sum(literal_len)`); if
  a configured per-buffer scratch limit would be
  exceeded, the kernel declines the buffer to dense
  fallback rather than allocating unboundedly.
- Sub-case A1 (FSST predicate column not in output)
  feeds Case A sparse projection; Sub-case A2 (FSST
  column in output) routes through ADR 065 Case B
  via planner-level `UnsupportedCascade` decline.
  Same routing as ADR 066 V1.
- Corruption boundary (per ADR 067 §"Decision §9"):
  structural metadata fails closed via existing
  `EncodingComputeRejectCode` (bad magic, impossible
  `symbol_count`, row-count mismatch, non-monotonic
  encoded offsets, offsets outside payload). The
  kernel does not interpret encoded bytes on the
  predicate path, so it cannot detect a corrupted
  compared range that decodes to invalid plaintext —
  the only check on a compared range is `memcmp`
  against the pre-encoded canonical literal. Bytes
  outside compared ranges trust ADR 020 AEAD
  upstream. Full token-stream validation is the
  dense-decode path's job, not Strategy B's; the
  writer's canonical contract is enforced by
  property tests in the correctness suite, not by
  runtime checks on the read path.
- ADR 057 cascade byte layout for `Fsst` unchanged.
  ADR 060 / 063 / 064 / 065 / 066 surfaces unchanged.
- No public surface changes (no IPC custom_metadata
  key, no wire MsgType, no NodeCapability bit, no
  ObjectStore trait method, no Cargo feature, no
  operator knob beyond `enabled_cascades` and
  `enabled_predicate_evaluators`).

## Goal

```text
write side:
  values bytes + Arrow decoded offsets (variable-width
    string/binary column eligible for FSST cascade)
    -> WFS1 candidate encode (existing path)
    -> WFS2 candidate encode:
       - train symbol table within row boundaries only
       - canonical greedy encoding (no cross-row symbols)
       - emit WFS2 frame: magic + symbol table +
         row count + encoded offsets + payload
    -> per-buffer actual-size guard:
       wfs2_len <= decoded_plaintext_len
       AND wfs2_len <= ceil(wfs1_len * 1.10)
    -> select FsstRowSafe if guard clears, else Fsst
    -> emit EncodingCascade::FsstRowSafe { ... } in
       directory; values buffer holds WFS2 frame.

read side, predicate path (Strategy B):
  WFS2 frame + Arrow decoded offsets + validity
    -> parse WFS2 metadata (magic, symbol table,
       row count, encoded offsets)
    -> coerce predicate literal (existing path)
    -> pre-encode literal once per buffer using the
       canonical encoder + this buffer's symbol table
    -> per row:
         decoded length pre-filter (decoded offsets)
         encoded length pre-filter (WFS2 offsets)
         memcmp on length-matching encoded byte range
       -> exact mask-only PhaseBOutcome
    -> downstream: Sub-case A1 -> Case A sparse
       projection; Sub-case A2 -> ADR 065 Case B
       (planner declines UnsupportedCascade).

read side, dense path:
  WFS2 frame
    -> full token decode validates entire payload
    -> original values bytes
    Used for: evaluator-disabled reads, unsupported
    atoms, Sub-case A2 overlap, correctness oracle.
```

## Deliverables

- `src/storage/encoding/types.rs`:
  - `EncodingCascadeKind::FsstRowSafe = 9` added; `ALL`
    array grows from 8 to 9; `as_str` / `from_label`
    / `kind()` extend with label `"fsst_row_safe"`;
    `emits_pages_in_v1()` returns false for the new
    variant (matches `Fsst`).
  - `EncodingCascade::FsstRowSafe { symbol_table_byte_len:
    RecordBodyByteLen }` variant.
  - **Default-enabled cascades narrow vs `ALL`.** The
    current `EncodingWriterConfig::default()` derives
    `enabled_cascades` from `EncodingCascadeKind::ALL`
    (`types.rs:273`); growing `ALL` to include
    `FsstRowSafe` would silently flip default writers
    to emit WFS2 — bypassing ADR 067's Phase-7-
    evidence-gated promotion contract. Introduce a
    separate `EncodingCascadeKind::DEFAULT_ENABLED`
    array (or equivalent `is_default_enabled(self) -> bool`
    predicate) that **excludes** `FsstRowSafe` until
    Phase 7 evidence clears the council's promotion
    gates. `EncodingWriterConfig::default()` switches
    to deriving from `DEFAULT_ENABLED` instead of
    `ALL`. Operators can still opt in explicitly via
    `enabled_cascades = [..., "fsst_row_safe"]` in
    TOML. `ALL` continues to mean "all known kinds"
    for catalog-completeness contexts (label parsing,
    serialization round-trip tests, architectural
    guards). At promotion, the `DEFAULT_ENABLED` set
    extends to include `FsstRowSafe` as part of the
    Phase 8 amendment.
- `src/storage/encoding/directory.rs`:
  - bump `ENCODING_DIRECTORY_FORMAT_VERSION` by one;
  - extend `write_cascade` / `read_cascade` (or the
    equivalent serialization sites at lines 1189 and
    1244 per Phase 0 audit) with the new variant;
  - serialization shape mirrors `EncodingCascade::Fsst`
    (carries the same `symbol_table_byte_len` field).
- `src/storage/encoding/cascade/codec.rs`:
  - factor a shared canonical-encoder helper
    `encode_fsst_canonical_row(row_bytes, symbol_table,
    out: &mut Vec<u8>)`; reused by both the WFS2
    writer and the Strategy B kernel's literal pre-
    encoder.
  - factor a shared symbol-table builder
    `build_fsst_symbol_table(input, mode)` where
    `mode` is `WholeBuffer` (existing WFS1 behavior)
    or `RowBoundaryAware` (new WFS2 behavior — counts
    pairs only within rows).
  - `encode_fsst_row_safe_bytes(input, decoded_value_count,
    decoded_value_offsets) -> Result<Vec<u8>, _>`: emits
    a complete WFS2 frame.
  - `decode_fsst_row_safe_bytes(encoded, expected_byte_len,
    expected_value_count) -> Result<Vec<u8>, _>`: full
    dense decode that validates magic, symbol count,
    row count, offset table monotonicity, payload
    length, and decoded length cross-check. This is
    the correctness oracle.
  - WFS1 helpers (`encode_fsst_bytes` /
    `decode_fsst_bytes`) behaviorally unchanged;
    factoring may share private helpers but external
    behavior of WFS1 path stays byte-for-byte
    identical (regression-tested via existing FSST
    fixtures).
- `src/storage/encoding/select.rs`:
  - extend FSST candidate selection to encode both
    WFS1 and WFS2 candidates when `enabled_cascades`
    includes `fsst_row_safe`;
  - apply the per-buffer actual-size guard:
    `wfs2_len ≤ decoded_plaintext_len` and
    `wfs2_len ≤ ceil(wfs1_len * 1.10)`;
  - select `FsstRowSafe` when both conditions hold,
    else fall back to `Fsst`;
  - if `enabled_cascades` includes `fsst_row_safe`
    but not `fsst`, encode WFS2 only when value
    offsets are present and the guard clears against
    decoded plaintext alone (no WFS1 baseline);
  - current `EncodingCascade::Fsst { symbol_table_byte_len:
    RecordBodyByteLen::new(0) }` selection at line
    133-135 stays the fallback path unchanged.
- `src/storage/encoding/compute/types.rs`:
  - `EncodingComputeEvaluatorKind::FsstRowSafe`
    (discriminant 8 — after ADR 066's `Fsst = 7`),
    label `"fsst_row_safe"`; `ALL` grows; `as_str` /
    `from_label` extend.
- `src/storage/encoding/compute/dispatch.rs`:
  - extend `evaluator_kind_for_cascade` to map
    `EncodingCascade::FsstRowSafe { .. } → Some(EncodingComputeEvaluatorKind::FsstRowSafe)`
    (alongside the existing `Fsst → Fsst` mapping at
    line 332);
  - extend the dispatch entry that matches
    `NativeEvaluationEligibility::Eligible(EncodingComputeEvaluatorKind::FsstRowSafe)`
    (alongside the existing `Fsst` arm at line 60);
  - reuse ADR 066's literal coercion + multi-buffer
    fetch shape unchanged.
- `src/storage/encoding/compute/kernels/fsst_row_safe.rs`
  (new module, sibling to ADR 066's
  `kernels/fsst.rs`):
  - `parse_fsst_row_safe_frame(buffer_bytes,
    expected_byte_len, expected_value_count) -> Result<FsstRowSafeFrame, EncodingComputeRejectCode>`
    — parses WFS2 metadata, validates structural
    invariants;
  - `pre_encode_literal(symbol_table, literal_bytes,
    scratch: &mut Vec<u8>) -> Result<EncodedLiteral, _>`
    — uses the shared canonical-encoder helper from
    `cascade/codec.rs`;
  - `evaluate_fsst_row_safe_equality(frame,
    decoded_offsets, validity, literals, op) -> Result<EncodedRowMask, _>`
    — Strategy B kernel: per-row decoded-length
    prefilter, encoded-length prefilter, memcmp
    against pre-encoded literals.
  - `evaluate_fsst_row_safe_equality_with_stats(...)
    -> Result<(EncodedRowMask, FsstRowSafeEvaluationStats), _>`
    — bench-instrumented variant.
- `src/storage/encoding/compute/kernels/mod.rs`:
  - register `fsst_row_safe` submodule.
- `src/storage/manager.rs`:
  - admit `FsstRowSafe` cascade buffers in the
    existing FSST multi-buffer predicate planning
    path (the multi-buffer fetch shape from ADR 066
    is reused — `EncodingComputePhaseBLeaf::Encoded`
    already carries `offsets_buffer_ordinal: Option<...>`
    per ADR 066's manager amendment);
  - Sub-case A2 gate at `build_encoding_compute_phase_b_plan`
    (currently checks for FSST evaluator + output
    column overlap → declines `UnsupportedCascade`)
    extends to also fire for `FsstRowSafe` (the
    overlap routing is identical to `Fsst`).
- `src/storage/encoding/compute/config.rs` (or
  equivalent):
  - `EncodingComputeConfig::default()`'s
    `enabled_predicate_evaluators` extends by
    `FsstRowSafe` only if Phase 7 evidence clears the
    council's gates;
  - operators can opt in via TOML (`enabled_predicate_evaluators
    = ["bit_packed", ..., "fsst_row_safe"]`).
- `src/metrics/registry.rs`:
  - add bounded labels for `"fsst_row_safe"` wherever
    cascade or evaluator labels are enumerated.
- `examples/wendelon.toml.example`:
  - commented opt-in example for `fsst_row_safe`
    cascade and evaluator; comment makes clear the
    feature is gated on Phase 7 promotion until the
    council defaults it on.
- Tests:
  - `tests/encoding_cascade.rs` (or wherever cascade
    tests live):
    - `FsstRowSafe` round-trip (encode + dense
      decode) across representative fixtures;
    - architectural guard pins discriminant
      ordering;
    - WFS1 round-trips byte-identical on existing
      fixtures (regression);
    - canonical-encoder property test:
      `pre_encode_literal(R, T) == writer_encoded_row_bytes(R, T)`
      for randomly-chosen R and T;
    - WFS2 writer never emits a symbol pair spanning
      a row boundary (frame-inspection guard);
    - actual-size guard test: short-row fixture
      where `wfs2_len > ceil(wfs1_len * 1.10)`
      → writer falls back to `Fsst`;
    - WFS2 dense-decode corruption fixtures (bad
      magic, impossible symbol count, non-monotonic
      offsets, offset overflow, decoded-length
      mismatch).
  - `tests/encoding_compute_test.rs`:
    - Strategy B kernel correctness across `Eq` /
      `Neq` / `InList` × `Utf8` / `LargeUtf8` /
      `Binary` / `LargeBinary` × null-free /
      nullable × selectivity sweep;
    - mask byte-identical to WFS2 dense-decode
      oracle on every fixture;
    - boundary-crossing-symbol fixture (writer does
      NOT produce cross-row symbols on input where
      such pairs are frequent);
    - no-compressed-row-slice regression fixture
      (carry-forward; verifies reader does not
      slice encoded buffer by Arrow decoded
      offsets);
    - length-pre-filter Disjoint fixture (literal
      length matches no row length; expect zero
      memcmp work);
    - corruption fixtures (malformed WFS2 frame,
      non-monotonic encoded offsets, decoded-length
      cross-check failure — fail-closed via
      `EncodingComputeRejectCode`);
    - InList scratch-bound exhaustion fixture
      (very large `InList` with long literals →
      kernel declines to dense).
  - `tests/encoding_compute_kernel_invariants.rs`:
    - Strategy B kernel invariant guards (atom-set
      closure; mask-length agreement; multi-buffer
      fetch shape; no encoded-byte decode in inner
      loop; no `FsstDecodedByteReader` import; no
      dense Arrow string materializer import; no
      per-row literal encoding; per-buffer pre-
      encoded literal cache).
  - `tests/architecture_test.rs`:
    - ADR 067 lockdown guards (cascade catalog
      extension, evaluator catalog extension,
      directory format version bump, no new wire/
      protocol surface, etc.).
  - `tests/row_group_sparse_projection_fetch.rs`:
    - Strategy B mask-only output flows into ADR
      060 / 064 Case A;
    - Strategy B decline still engages ADR 065 Case
      B/C (regression).
  - `tests/row_group_materialization.rs`:
    - end-to-end fixture: Strategy B predicate
      engages kernel → Case A sparse projection on
      remaining columns → output equals full-dense
      oracle.
  - `tests/encoding_secondary_index.rs`:
    - ADR 052 confirm-step on `FsstRowSafe`-keyed
      secondary indexes (path coverage; conditional
      on existing test harness ability to force the
      cascade without widening ADR 052).
  - `tests/config_loader.rs`:
    - `enabled_cascades` and
      `enabled_predicate_evaluators` parse
      `"fsst_row_safe"` correctly.
- Benches:
  - `benches/encoding_compute.rs`:
    - WFS2 fixtures mirroring ADR 066's surface
      (URL, log-line, free-form text, identifier,
      JSON-key, short-string adversarial, weak-
      compression adversarial, boundary-pair
      fixture);
    - `Eq` selectivity sweep, `Neq` inverse
      coverage, `InList(1/4/16)`;
    - WFS1 Strategy A (ADR 066 V1) baseline
      alongside WFS2 Strategy B for direct
      comparison;
    - dense WFS2 decode oracle alongside both;
    - declined-path overhead rows.
  - `benches/encoding_compute_writer.rs` (new or
    extension to existing writer bench):
    - WFS1 vs WFS2 encoded length per fixture;
    - WFS2 offset-table bytes contribution;
    - WFS2 writer CPU vs WFS1 writer CPU;
    - actual-size guard outcome (FsstRowSafe
      selection rate per fixture).
  - `benches/row_group_fetch.rs`:
    - end-to-end ADR 065 → ADR 060 / 064 Case A
      path on Strategy B predicate fixture;
    - 8K / 65K / 1M row matrices mirroring ADR 066
      cadence;
    - ADR 066 V1 (Strategy A) baseline alongside
      Strategy B for migration delta.
- `plans/impl/067_row_canonical_fsst_bench_results.md`
  (created at promotion only).
- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
  §"Where we are" amendment after promotion: cascade
  catalog extends by `FsstRowSafe`; new WFS2 frame
  shipped; format version bumped; default cascade
  selection updated per Phase 7 outcome.
- `plans/adr/058_compute_over_encoded_data.md` §"Where
  we are" amendment: `FsstRowSafe` in evaluator
  catalog; default-on per Phase 7 outcome.
- `plans/adr/059_native_cocd_kernels.md` §"Where we
  are" amendment if default allow-list extends to
  include `FsstRowSafe`.
- `plans/adr/063_page_level_predicate_stats.md`
  §"Where we are" amendment: `FsstRowSafe` is row-
  group grain in V1; not page-emitting.
- `plans/adr/065_dense_predicate_zip_with_sparse_projection.md`
  §"Where we are" amendment: `FsstRowSafe` Sub-case
  A1 predicates migrate to Case A; ADR 065 V1
  engagement surface narrows further.
- `plans/adr/066_fsst_evaluator.md` §"Where we are"
  amendment: V1 Strategy A path remains for `Fsst`
  cascade buffers indefinitely; new writes default to
  `FsstRowSafe` with Strategy B (ADR 067) per Phase
  7 outcome.
- `plans/wip/roadmap.md`: planning-cadence update at
  promotion.

## Non-goals

- No mutation of WFS1 or `EncodingCascade::Fsst`.
- No removal of the ADR 066 V1 Strategy A reader.
- No silent / on-the-fly migration of existing WFS1
  buffers.
- No old-binary read compatibility for WFS2 (rollout
  is reader-before-writer; the
  `ENCODING_DIRECTORY_FORMAT_VERSION` bump enforces
  this).
- No `Lt` / `Gt` / `Lte` / `Gte` / future `LIKE` /
  regex / substring / prefix on FSST.
- No FSST aggregate folds (`string_min` /
  `string_max` / `count_distinct` on encoded form).
- No sparse projection eligibility lift for FSST
  (variable-width deferral inherits from ADR 060 /
  064).
- No process-wide pre-encoded-literal cache (per-
  buffer cache only).
- No process-wide symbol-table cache.
- No 64-bit encoded offsets in V1 (decline-to-dense
  for buffers exceeding 4 GiB encoded payload).
- No page-grain WFS2 (buffer-grain only).
- No new `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp`
  / `EncodedRowMask` variant.
- No new wire / descriptor / PBC opcode / capability
  / digest / `ObjectStore` surface.
- No Cargo feature gating `FsstRowSafe`.
- No nightly rustc dependency.
- No retirement of the dense FSST fallback path.
- No retirement of ADR 065 Case B/C (it remains
  reachable for `FsstRowSafe` predicates that
  decline the kernel and for Sub-case A2 overlap).
- No bloom-filter prefilter on encoded equality.
- No full WFS2 token-stream validation per query on
  the predicate path (full validation is dense
  decode's job; if council later requires it,
  benchmark as a first-class alternative — does not
  slip into V1's contract).
- No removal of the cascade-field
  `symbol_table_byte_len` from either `Fsst` or
  `FsstRowSafe` (separate trivial follow-up ADR).

## Phase 0 — Contract audit

Read-only audit. Findings land here at the first
implementation pass after this plan promotes to
`plans/impl/`.

**Existing FSST writer:**

- `src/storage/encoding/cascade/codec.rs` —
  `encode_fsst_bytes` (line 494) symbol-table
  training pattern, greedy substitution loop;
  `decode_fsst_bytes` (line 550) for round-trip
  comparison.
- `src/storage/encoding/select.rs` (line 128-135) —
  current FSST cascade selection. Confirm where
  Arrow value offsets are available at the call
  site.
- `EncodingWriterConfig.enabled_cascades` shape —
  pin operator-visible writer selection surface.

**Cascade catalog and frame:**

- `src/storage/encoding/types.rs` (line 105-124) —
  `EncodingCascade` variants. Confirm `Fsst = 7`,
  `Alp = 8` discriminants. Verify
  `EncodingCascadeKind::ALL` shape and how to add
  `FsstRowSafe = 9` cleanly.
- `src/storage/encoding/directory.rs` (lines 1189,
  1244) — cascade serialization. Verify
  `ENCODING_DIRECTORY_FORMAT_VERSION` location and
  bump procedure.

**Reader-side dispatch:**

- `src/storage/encoding/compute/dispatch.rs`
  (line 332 for `evaluator_kind_for_cascade`,
  line 60 for the `Eligible(Fsst)` dispatch arm) —
  pin the extension sites for the new evaluator
  kind.
- `src/storage/encoding/compute/types.rs`
  (line 20-28) — `EncodingComputeEvaluatorKind`
  catalog. Confirm `Fsst = 7` (from ADR 066) and
  add `FsstRowSafe = 8`. (Note: evaluator
  discriminant `8` is a separate enum from the
  cascade discriminant `EncodingCascadeKind::FsstRowSafe = 9`;
  the two catalogs share a label `"fsst_row_safe"`
  but have independent numbering. ADR 058 evaluator
  catalog has no `Alp` entry, so `8` is free here
  even though `Alp = 8` is already taken in the
  cascade catalog.)
- `src/storage/manager.rs:11151-11156` — Sub-case
  A2 gate (FSST evaluator + output overlap →
  `UnsupportedCascade`). Extend the predicate to
  also fire on `FsstRowSafe`.

**WFS2 frame design:**

- pin offset width: 32-bit covers buffers ≤ 4 GiB
  encoded payload (V1 scope); decline-to-dense for
  larger buffers.
- pin offset relative-to-frame vs relative-to-
  payload: relative to start of `encoded_payload`
  (matches Codex's draft; consistent with WFS1's
  payload-relative cursor semantics).
- pin row-count storage: u32 (matches offset
  width; row counts > 4 G in a single buffer are
  out of scope).

**Writer integration:**

- pin where the writer accesses Arrow value
  offsets at FSST encode time. ADR 066 confirmed
  the offsets are available in the multi-buffer
  read path; verify the same is true on the write
  path. If not, escalate to a separate threading-
  layer ADR before Phase 1 implementation begins.

**Validity composition:**

- `kernels/validity.rs` composition entry point
  (existing; reused unchanged from ADR 066).

**Bench harness:**

- `benches/encoding_compute.rs` — current FSST
  fixtures (carry-forward from ADR 066 V1 perf
  iteration).
- `benches/row_group_fetch.rs` — end-to-end FSST
  composition row.
- writer bench location for `WFS1 vs WFS2`
  compression-ratio diagnostic.

Phase 0 also verifies the canonical-encoder helper
can be cleanly factored out of `encode_fsst_bytes`
without destabilizing WFS1's existing byte-for-byte
output. If factoring would risk WFS1 regression, V1
ships two separate canonical-encoder
implementations (with a property test that they
produce identical output on within-row inputs)
rather than refactoring the WFS1 path.

**Exit gate:** Phase 0 audit findings recorded;
council confirms the writer + reader can be
implemented without an ADR 057 amendment beyond
the cascade-variant addition; canonical-encoder
factoring strategy decided.

## Phase 1 — Cascade catalog and directory format version

Add the new cascade variant + bump the format
version. Pure catalog change; no encoder/decoder
work yet.

**Files:**
- `src/storage/encoding/types.rs`
- `src/storage/encoding/directory.rs`

**Work:**

1. Add `EncodingCascadeKind::FsstRowSafe = 9` to the
   enum; extend `ALL` array; extend `as_str` /
   `from_label` with label `"fsst_row_safe"`.
2. Add `EncodingCascade::FsstRowSafe { symbol_table_byte_len:
   RecordBodyByteLen }` variant. Extend `kind()` to
   map to `FsstRowSafe`. Extend `emits_pages_in_v1()`
   to return false (matches `Fsst`).
3. **Narrow default-enabled cascades vs `ALL`.** The
   current `EncodingWriterConfig::default()` at
   `types.rs:273` derives `enabled_cascades` from
   `EncodingCascadeKind::ALL.into_iter().collect()`;
   growing `ALL` to include `FsstRowSafe` would
   silently flip default writers to emit WFS2,
   bypassing the Phase-7-evidence-gated promotion
   contract. Introduce
   `EncodingCascadeKind::DEFAULT_ENABLED` (a separate
   `&'static [Self]` array) or an
   `is_default_enabled(self) -> bool` predicate that
   **excludes** `FsstRowSafe` in this ADR's
   implementation. `EncodingWriterConfig::default()`
   switches to deriving from `DEFAULT_ENABLED`. `ALL`
   remains "all known kinds" for catalog completeness
   (label parsing, serialization, architecture
   guards). At Phase 8 promotion, the
   `DEFAULT_ENABLED` set extends to include
   `FsstRowSafe`.
4. Bump `ENCODING_DIRECTORY_FORMAT_VERSION` by one.
5. Extend `write_cascade` / `read_cascade` (lines
   1189 / 1244 per Phase 0) with the new variant
   serialization. Shape mirrors `EncodingCascade::Fsst`
   (4-byte discriminant + 4-byte
   `symbol_table_byte_len`).
6. Tests:
   - cascade kind label round-trip includes
     `"fsst_row_safe"`;
   - directory serialize / deserialize round-trips a
     `FsstRowSafe` cascade with the new format
     version;
   - architecture guard pins discriminant order:
     `Fsst = 7`, `Alp = 8`, `FsstRowSafe = 9`;
   - **architecture guard pins that
     `EncodingWriterConfig::default().enabled_cascades`
     does NOT contain `FsstRowSafe` in this ADR**
     (the Phase-7-evidence gate). Guard will be
     relaxed by the Phase 8 amendment if promotion
     clears.
   - WFS1 cascade fixtures remain byte-identical
     (regression).

**Exit gate:** `cargo test --test encoding_cascade
-- --nocapture` passes; format version bump visible
in directory write tests.

## Phase 2 — WFS2 writer + dense decode + selection guard

Add the row-canonical encoder, the WFS2 frame
encoder/decoder, and the per-buffer actual-size
selection guard. Dense decode lands first so the
correctness oracle is in place before Strategy B is
written.

**Files:**
- `src/storage/encoding/cascade/codec.rs`
- `src/storage/encoding/select.rs`

**Work:**

1. **Factor shared canonical helpers** (in
   `cascade/codec.rs`):
   - `enum SymbolTableMode { WholeBuffer,
     RowBoundaryAware }`
   - `fn build_fsst_symbol_table(input: &[u8],
     value_offsets: &[i32], mode: SymbolTableMode)
     -> SymbolTable` — for `WholeBuffer`, counts all
     adjacent pairs (existing WFS1 behavior — pass
     empty `value_offsets`); for `RowBoundaryAware`,
     counts only within-row pairs.
   - `fn encode_fsst_canonical_row(row_bytes: &[u8],
     symbol_table: &SymbolTable, out: &mut Vec<u8>)`
     — greedy encoding of a single row from a fresh
     state. Produces deterministic output. Reused by
     both the WFS2 writer and the Strategy B literal
     pre-encoder.

2. **WFS2 writer** (`encode_fsst_row_safe_bytes`):
   - validates input shape (offsets length =
     `decoded_value_count + 1`, monotonic, final
     offset = input length);
   - builds row-boundary-aware symbol table;
   - emits frame header: magic (`b"WFS2"`),
     symbol_count, symbol pairs, row_count (u32 LE),
     encoded offsets (u32 LE per row + sentinel);
   - encodes each row via `encode_fsst_canonical_row`,
     recording start offsets;
   - validates final encoded length fits u32 (else
     returns error → caller declines `FsstRowSafe`).

3. **WFS2 dense decode** (`decode_fsst_row_safe_bytes`):
   - validates magic, symbol_count ≤ 127, row_count,
     offset table monotonicity, final offset =
     payload length;
   - decodes the entire token stream into a
     `Vec<u8>` of size `expected_byte_len`;
   - cross-checks: each row's decoded byte count
     equals
     `decoded_offsets[i+1] - decoded_offsets[i]`
     (when caller supplies decoded offsets — used
     primarily by tests).
   - This is the correctness oracle. All Strategy B
     correctness fixtures compare against dense
     decode output.

4. **WFS1 helpers behaviorally unchanged.**
   `encode_fsst_bytes` / `decode_fsst_bytes` may
   share private symbol-table or symbol-encoding
   helpers (factored in step 1) but external
   behavior on existing WFS1 fixtures stays byte-
   identical. Regression-tested.

5. **Selection guard** (in `select.rs`):
   - extend FSST candidate selection to encode both
     WFS1 and WFS2 candidates when
     `enabled_cascades` includes `fsst_row_safe`;
   - apply guard:
     ```text
     select FsstRowSafe iff:
       wfs2_len <= decoded_plaintext_len
       AND wfs2_len <= ceil(wfs1_len * 1.10)
     ```
   - else fall back to `Fsst` (or to plain if
     `Fsst` is also rejected — same logic as
     today).
   - When `enabled_cascades` includes
     `fsst_row_safe` but not `fsst`: encode WFS2
     only; guard against decoded plaintext alone
     (no WFS1 baseline). If WFS2 fails the
     plaintext guard, decline FSST entirely (fall
     through to plain).
   - Compression-loss budget constant
     `FSST_ROW_SAFE_COMPRESSION_LOSS_BUDGET = 1.10`
     (draft value; Phase 7 evidence may suggest
     adjustment).

6. **Tests** (`tests/encoding_cascade.rs`):
   - WFS2 round-trip on URL / log-line / free-text /
     identifier fixtures: encode → dense decode
     yields byte-identical input.
   - WFS2 round-trip on Binary with high-bit bytes
     (escape path).
   - WFS2 round-trip on empty values buffer with
     non-zero row count (all rows empty).
   - WFS2 round-trip on single-row, no-symbol
     fixture (all literals).
   - **Boundary-symbol fixture**: input where a
     frequent two-byte pair straddles row boundaries
     in the decoded stream. Verify: writer's symbol
     table does NOT include that pair (because
     row-boundary-aware training excludes it), OR
     if it does include it, no row's encoded bytes
     contain a symbol token for it that spans the
     boundary. Frame inspection guard.
   - **Canonical-encoder property test**:
     `encode_fsst_canonical_row(R, T)` byte-equal
     to the corresponding row's bytes in
     `encode_fsst_row_safe_bytes(R, [0, R.len()],
     ...)` for randomly-chosen R and T (varied row
     lengths, byte distributions, including escapes).
   - **Actual-size guard test**: short-row fixture
     (16-byte UUID-shaped values) where WFS2 offset
     table overhead exceeds 10% of WFS1 size →
     selection falls back to `Fsst`.
   - Malformed WFS2 dense decode: bad magic, short
     header, impossible symbol count, unterminated
     escape, non-monotonic offsets, final offset
     mismatch, decoded-length mismatch — all return
     `EncodingRejectCode::DecodedLengthMismatch`.
   - WFS1 regression: existing FSST fixtures
     produce byte-identical output to pre-Phase-2
     baseline.

**Exit gate:** WFS2 writer + dense decode + selection
guard pass all Phase 2 tests. Canonical-encoder
property test green. WFS1 regression suite green.
Boundary-symbol guard green. Actual-size guard fires
correctly on short-row fixtures.

## Phase 3 — Strategy B kernel

Add the row-safe FSST predicate evaluator. Reuses
the canonical encoder from Phase 2 for literal pre-
encoding.

**Files:**
- `src/storage/encoding/compute/kernels/fsst_row_safe.rs`
  (new module)
- `src/storage/encoding/compute/kernels/mod.rs`
- `tests/encoding_compute_test.rs`
- `tests/encoding_compute_kernel_invariants.rs`

**Work:**

1. **`parse_fsst_row_safe_frame`** — frame
   metadata parser. Validates magic, symbol_count,
   row_count vs `BufferEncoding.decoded_value_count`,
   offset table shape, final offset = payload
   length. Returns `FsstRowSafeFrame` carrying the
   symbol table, encoded offsets slice, and encoded
   payload slice.

2. **`pre_encode_literal`** — calls the shared
   `encode_fsst_canonical_row` helper from
   `cascade/codec.rs` with the frame's symbol
   table. Returns `EncodedLiteral { encoded_bytes:
   Vec<u8>, decoded_len, encoded_len }`. Cache
   built once per buffer at kernel entry.

3. **Per-buffer scratch lifecycle:**
   - `Eq` (single literal): stack-resident
     `[u8; STACK_LITERAL_BUDGET]` (e.g., 256
     bytes).
   - `InList` (multi literal): `Vec<u8>` of
     capacity `2 × sum(literal_len)` (worst-case
     no-compression ratio). One allocation per
     kernel call, reused for all literals.
   - Per-buffer cap: if `2 × sum(literal_len) >
     PER_BUFFER_SCRATCH_BUDGET` (configurable;
     draft value 64 KiB), kernel declines to
     dense fallback via
     `UnsupportedProgram`.

4. **`evaluate_fsst_row_safe_equality`** — the
   per-row Strategy B body. Op-hoist into four
   inline loops (single-literal × invert / no-
   invert; multi-literal × invert / no-invert)
   per ADR 066's Phase A pattern:

   ```text
   for row in 0..rows:
     decoded_len_row = decoded_offsets[i+1] -
                       decoded_offsets[i]
     if decoded_len_row != decoded_len_literal (single)
        OR no literal of decoded_len_row (multi):
       row_matches = false
       (no encoded-byte read at all)
     else:
       encoded_len_row = encoded_offsets[i+1] -
                         encoded_offsets[i]
       if encoded_len_row != encoded_len_literal:
         row_matches = false
       else:
         row_matches = encoded_payload[encoded_offsets[i]
                          ..encoded_offsets[i+1]]
                       == literal.encoded_bytes
       (multi: iterate length-matching literals)
     bit = row_matches if !invert else !row_matches
     selected.push(bit)?
   ```

5. **Validity composition** via `kernels/validity.rs`
   (unchanged from ADR 066).

6. **Mask shape selection** via existing
   `EncodedRowMask` shape selector; default
   `Bitmap` for high-cardinality fixtures.

7. **Stats** (in
   `evaluate_fsst_row_safe_equality_with_stats`):
   - `rows`, `length_match_rows`,
     `length_mismatch_rows` (from decoded prefilter),
     `encoded_length_mismatch_rows` (from encoded
     prefilter), `memcmp_rows`, `memcmp_bytes`,
     `pre_encoded_literal_bytes` (cache size),
     `encoded_payload_bytes_touched` (sum of
     compared encoded byte lengths — much less than
     total payload).

8. **Architecture guards** in
   `kernels/fsst_row_safe.rs` (verified by
   `tests/encoding_compute_kernel_invariants.rs`):
   - module does not import `kernels/fsst::FsstDecodedByteReader`,
     `cascade::decode_fsst_bytes`, or
     `cascade::decode_fsst_row_safe_bytes` (no
     decode-on-predicate-path);
   - no per-row literal pre-encoding (cache lives
     above the row loop);
   - module names are semantic (no `Phase` /
     `Step` / version suffixes).

9. **Tests** (`tests/encoding_compute_test.rs`):
   - Strategy B mask byte-identical to dense WFS2
     decode + Arrow eq oracle on every fixture in
     the closed atom × type × nullability ×
     selectivity matrix.
   - `Eq` selectivity sweep (0% / 1% / 5% / 25% /
     50% / 95%).
   - `Neq` inverse coverage; verify length-mismatch
     rows survive.
   - `InList(1 / 4 / 16)`.
   - `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary`
     coverage.
   - Empty-string and empty-literal edge cases.
   - High-bit byte fixtures (escape path in pre-
     encoded literal).
   - Nullable fixtures (validity composition).
   - `IsNull` / `IsNotNull` route via existing ADR
     059 validity-only path; kernel not invoked.
   - Length-pre-filter Disjoint fixture: literal
     length matches no row length → zero memcmp
     work (verified via stats counter).
   - **Canonical-equivalence test**: pick a row
     that matches a literal under dense oracle;
     verify the encoded byte ranges are
     byte-identical (this is the canonical
     contract validation at the kernel level).
   - InList scratch-budget exhaustion fixture →
     decline to dense.
   - Corruption: malformed WFS2 frame, non-
     monotonic encoded offsets, decoded-length
     cross-check failure → fail-closed via
     `EncodingComputeRejectCode`.

**Exit gate:** Strategy B kernel produces byte-
identical masks to WFS2 dense oracle on every
correctness fixture. Length-pre-filter elimination
verified. Architecture guards pass.

## Phase 4 — Dispatch and manager integration

Wire Strategy B into ADR 058's dispatch and the row-
group planner.

**Files:**
- `src/storage/encoding/compute/types.rs` (catalog)
- `src/storage/encoding/compute/dispatch.rs`
- `src/storage/manager.rs`

**Work:**

1. **Catalog extension:**
   - `EncodingComputeEvaluatorKind::FsstRowSafe = 8`
     (after ADR 066's `Fsst = 7`); update `ALL`,
     `as_str`, `from_label`, config validation,
     architectural guards. (Reminder: this is the
     evaluator-catalog discriminant `8`, distinct
     from the cascade-catalog discriminant
     `EncodingCascadeKind::FsstRowSafe = 9` added
     in Phase 1. The two enums are independent;
     they happen to share the label
     `"fsst_row_safe"` but their numbering is
     separate.)

2. **Dispatch extension:**
   - extend `evaluator_kind_for_cascade` (line 332):
     `EncodingCascade::FsstRowSafe { .. } → Some(EncodingComputeEvaluatorKind::FsstRowSafe)`;
   - extend the `Eligible` dispatch arm at line 60
     to route `FsstRowSafe` through the Strategy B
     kernel;
   - reuse existing literal coercion and multi-
     buffer fetch shape from ADR 066.

3. **Manager Sub-case A2 gate extension:**
   - the existing FSST gate at
     `src/storage/manager.rs:11151-11156` checks
     `evaluator == Fsst && output_columns.contains(column)`;
   - extend to also fire on `FsstRowSafe`:
     `(evaluator == Fsst || evaluator == FsstRowSafe)
     && output_columns.contains(column)`;
   - both variants decline to `UnsupportedCascade`,
     falling through to the dense path (ADR 065
     Case B).

4. **Multi-buffer fetch shape unchanged.** ADR 066's
   `EncodingComputePhaseBLeaf::Encoded` already
   carries `offsets_buffer_ordinal: Option<...>`;
   `FsstRowSafe` reuses the same shape (validity +
   decoded offsets + values).

5. **`enabled_predicate_evaluators` extension:**
   - default-off in V1; promotion gated on Phase 7
     evidence;
   - operators opt in via TOML.

6. **Tests:**
   - dispatch routes `FsstRowSafe` to the new
     evaluator;
   - Sub-case A1 fixture: `FsstRowSafe` predicate
     column not in output → kernel engages → mask-
     only `PhaseBOutcome` → Case A composition;
   - Sub-case A2 fixture: `FsstRowSafe` predicate
     column in output → planner declines
     `UnsupportedCascade` → ADR 065 Case B path;
     kernel does not engage;
   - non-FSST cascade unaffected;
   - `Fsst` (ADR 066 V1) cascade unaffected — both
     Strategy A and Strategy B coexist.

**Exit gate:** Production read path engages
Strategy B on `FsstRowSafe` cascade buffers; routes
declines to dense unchanged; both Strategy A and
Strategy B coexist (Strategy A for `Fsst` cascade,
Strategy B for `FsstRowSafe` cascade); Sub-case A2
gating fires correctly for both variants.

## Phase 5 — Architecture guards

Land ADR 067 lockdown guards in
`tests/architecture_test.rs`:

- `EncodingCascadeKind` count = previous + 1
  (exactly one new variant `FsstRowSafe`); existing
  `Fsst = 7`, `Alp = 8`, `FsstRowSafe = 9`
  discriminants pinned.
- `EncodingComputeEvaluatorKind` count = previous +
  1 (exactly one new variant `FsstRowSafe`);
  existing seven (after ADR 066) unchanged.
- `evaluator_kind_for_cascade` maps `FsstRowSafe →
  EncodingComputeEvaluatorKind::FsstRowSafe`.
- `encoding_compute_predicate_data_type_supported_for_evaluator`
  admits `Utf8` / `LargeUtf8` / `Binary` /
  `LargeBinary` for `FsstRowSafe`.
- `ENCODING_DIRECTORY_FORMAT_VERSION` bumped by one;
  new value pinned.
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
- ADR 057 `Fsst` cascade byte layout unchanged.
  `EncodingCascade::Fsst { symbol_table_byte_len }`
  unmodified.
- WFS1 frame layout unchanged (regression-tested
  in Phase 2).
- **Strategy B kernel does not import the dense
  WFS2 decoder, the dense Arrow string materializer,
  or ADR 066's `FsstDecodedByteReader`** (no decode
  on predicate path).
- **Pre-encoded literal cache is per-buffer-per-
  literal, not per-row** (architectural guard via
  source inspection / module-private API).
- **Writer never emits a symbol pair spanning a row
  boundary** (verified by frame inspection on
  representative fixtures).
- **Writer + reader pre-encoder produce byte-
  identical output** for the same row bytes under
  the same symbol table (canonical contract guard:
  property test).
- **Per-buffer actual-size guard fires before
  `FsstRowSafe` selection** (verified by selection
  guard on representative short-row and long-row
  fixtures).
- **Boundary-crossing-symbol fixture is in the
  correctness suite** and verifies the writer
  correctly does NOT produce cross-row symbols.
- **No-compressed-row-slice regression fixture is
  in the correctness suite** and verifies the
  reader does not slice the encoded buffer by
  Arrow decoded offsets.
- ADR 020 §2 AAD layout, ADR 022 §3 / §5 byte
  layouts, ADR 056 / 057 / 063 directory shapes,
  ADR 058 / 059 evaluator catalogs *(by addition
  only)*, ADR 060 / 064 sparse projection
  eligibility, ADR 061 closed chunk-size set, ADR
  062 `RowGroupSizingProfile` catalog, ADR 065
  manager three-case dispatch, ADR 066 V1 reader:
  all unchanged.
- Cross-replica byte-equal mask output across two
  writers with the same WFS2 fixture and same
  predicate.
- ADR 060 / 064 mask-only sparse projection byte-
  identical pre / post on shared fixtures.
- ADR 065 Case B/C reachable for `FsstRowSafe`
  predicates that decline the kernel (regression).
- New helper names avoid planning-phase identifiers
  and version suffixes (`FsstRowSafeFrame`,
  `EncodedLiteral`, `parse_fsst_row_safe_frame`,
  `pre_encode_literal`,
  `evaluate_fsst_row_safe_equality`,
  `encode_fsst_canonical_row`,
  `build_fsst_symbol_table` are semantic; the
  on-disk magic `WFS2` is acceptable per industry
  convention for format-version markers).
- No project-name-prefixed identifiers.

**Exit gate:** `cargo test --test architecture_test`
passes.

## Phase 6 — Focused tests

Focused test commands:

```sh
cargo fmt --all -- --check
cargo test fsst_row_safe --lib
cargo test --test encoding_cascade
cargo test --test encoding_compute_test
cargo test --test encoding_compute_kernel_invariants
cargo test --test row_group_sparse_projection_fetch
cargo test --test row_group_materialization
cargo test --test architecture_test
cargo test --test encoding_secondary_index
cargo test --test encoding_chunk_layout
cargo test --test config_loader
```

Required coverage: per Phases 1–5.

Existing ADR 058 / 059 / 060 / 063 / 064 / 065 /
066 tests must remain green. WFS1 round-trip
fixtures byte-identical (regression).

**Exit gate:** All focused tests pass.

## Phase 7 — Bench matrix

Extend the COCD bench
(`benches/encoding_compute.rs`), the writer bench,
and the row-group bench (`benches/row_group_fetch.rs`)
with WFS2 / Strategy B rows. Land evidence at
promotion in
`plans/impl/067_row_canonical_fsst_bench_results.md`.

**Bench dimensions:**

- row counts: 8K / 65K / 1M (mirroring ADR 066).
- WFS2 fixtures (carry-forward surface from ADR
  066):
  - URL-shaped (high cardinality, low-entropy
    prefixes; high FSST compression).
  - log-line-shaped (medium cardinality, structured
    tokens).
  - free-form text (high cardinality, high
    entropy).
  - identifier-shaped (UUID, ULID — high
    cardinality, fixed width).
  - JSON-key-shaped (low cardinality keys, high
    cardinality values).
  - **boundary-crossing-symbol fixture** (writer
    correctness regression: writer does NOT emit
    cross-row symbols).
  - **no-compressed-row-slice regression fixture**
    (reader correctness regression: reader does
    not slice encoded buffer by Arrow decoded
    offsets).
  - **short-string adversarial** (≤ 16 bytes per
    row; pathological case for offset-table
    overhead; tests selection guard fallback to
    `Fsst`).
  - **weak-compression adversarial** (high
    entropy; near-pass-through symbol table; tests
    Strategy B's per-row work doesn't degrade in
    pessimal compression cases).
- predicate atoms (kernel-engaged):
  - `Eq` clustered selectivity 0% / 1% / 5% /
    25% / 50% / 95%.
  - `Neq` (inverse coverage; verifies length-
    mismatch rows correctly survive).
  - `InList` with 1 / 4 / 16 constants.
- predicate atoms (validity-only path; kernel not
  invoked but regression coverage):
  - `IsNull` / `IsNotNull`.
- decline fixtures (route to dense):
  - `Lt`, `Gt`, `Lte`, `Gte`, future `LIKE`
    (atom mismatch).
  - InList scratch-budget exhaustion (declined
    via `UnsupportedProgram`).
- column shape:
  - null-free, nullable.
  - `Utf8` / `LargeUtf8` / `Binary` /
    `LargeBinary`.
  - empty-string fixtures.
- **Sub-case A1 rows** (FSST predicate column not
  in output): Strategy B engages → Case A sparse
  projection on remaining columns. **The V1 win.**
- **Sub-case A2 rows** (FSST predicate column in
  output): planner declines → ADR 065 Case B path;
  kernel does not engage.
- **ADR 066 V1 (Strategy A) baseline** on `Fsst`
  cascade encoding of the same input: direct
  comparison Strategy A vs Strategy B at the
  evaluator-only and end-to-end boundaries.
- **WFS2 vs WFS1 compression-ratio** on each
  fixture: encoded byte length comparison.
- **Writer-CPU comparison**: WFS1-only encode time
  vs WFS1+WFS2 dual-encode time (measures the
  selection-guard cost).
- **Actual-size guard outcome**: per-fixture
  selection rate (`FsstRowSafe` chosen vs falling
  back to `Fsst`).
- Corruption fixtures (fail-closed; bench records
  zero-cost decline overhead).
- Cross-replica determinism rows (mask byte-equal
  across two encoder runs of the same input).
- End-to-end ADR 065 → ADR 060 / 064 rows on
  `FsstRowSafe` predicate fixtures.

**Rows record:**

- route (`fsst_row_safe_kernel_case_a1`,
  `fsst_row_safe_dense_case_b_overlap_a2`,
  `fsst_row_safe_dense_fallback_atom_unsupported`,
  `fsst_row_safe_dense_fallback_type_unsupported`,
  `fsst_row_safe_dense_fallback_scratch_budget`,
  `fsst_row_safe_corruption_fail_closed`,
  `fsst_a_strategy_a_baseline`,
  `fsst_row_safe_writer_selected`,
  `fsst_row_safe_writer_declined_to_fsst`).
- WFS2 encoded payload bytes per buffer.
- WFS2 encoded-offset table bytes per buffer.
- WFS2 vs WFS1 compression ratio.
- Pre-encoded literal cache bytes (per buffer per
  literal).
- Encoded-payload bytes touched by memcmp (sum of
  compared range lengths).
- Length-pre-filter elimination rate (decoded;
  encoded; total).
- Memcmp rows / bytes.
- Dense WFS2 decode wall-clock (oracle baseline).
- Dense WFS1 decode + Arrow eq wall-clock (ADR 066
  V1 baseline).
- Strategy B kernel wall-clock.
- Strategy A kernel wall-clock (ADR 066 V1 on
  `Fsst` cascade encoding of same input).
- Speedup vs dense WFS2.
- Speedup vs dense WFS1 (the compelling number).
- Speedup vs Strategy A on `Fsst` cascade.
- Mask correctness (vs dense oracle).
- Mask shape (`Bitmap` / `Runs` / `All` / `None`).
- ADR 060 / 064 sparse projection plaintext bytes
  (when composing).
- Total query plaintext bytes (predicate +
  projection) for kernel + Case A vs dense + Case
  B/C.
- Cross-replica digest.
- Writer wall-clock (WFS1-only vs WFS1+WFS2 dual-
  encode).
- Iterations.

**Promotion gates (council-converged at iteration
2; stricter than ADR 066 because Strategy B's
structural elimination of decode produces the
headroom):**

1. **Predicate-evaluation wall-clock ≥ 2.0×.** At
   clustered 5% selectivity for `Eq` and `InList`,
   Strategy B wall-clock ≥ 2× faster than dense
   WFS1 decode + Arrow eq on representative
   warehouse fixtures. (Stricter than ADR 066's
   1.5×.)
2. **End-to-end query wall-clock ≥ 1.5×** when an
   exact mask feeds fixed-width sparse projection
   through the ADR 065 → ADR 060 / 064 Case A
   path. (Stricter than ADR 066's 1.2×.)
3. **Dense-fallback overhead ≤ 5%** on truly
   declined paths (range, future `LIKE`, scratch-
   budget exhaustion, ineligible types/layouts).
4. **Compression-ratio loss ≤ 10%** on
   representative warehouse fixtures vs `Fsst`
   cascade on the same input (full WFS2 bytes
   including encoded-offset table).
5. **Cross-replica deterministic masks** on
   representative WFS2 fixtures (Codex's iteration-
   2 addition; the canonical-encoder contract
   demands it).
6. **Writer overhead within ADR 057 writer-cost
   budget**, or the bench artifact explicitly
   justifies higher write cost with read-side wins
   (dual-encode WFS1 + WFS2 has measurable cost on
   the write path).

Headline diagnostics (recorded, not gating):

- ≥ 5× CPU speedup on representative `Eq`
  fixtures.
- Encoded-byte-range memcmp throughput vs dense
  Arrow eq throughput (sanity check on memcmp
  vectorization for ~22-byte ranges).
- Length-pre-filter elimination rate (decoded
  prefilter; encoded prefilter; combined).
- WFS2 vs WFS1 encoded-bytes ratio per fixture.
- Strategy B vs Strategy A (ADR 066 V1) wall-clock
  delta — quantifies the Strategy B win over
  Strategy A.
- **ADR 065 Case B/C engagement-surface
  reduction** as `FsstRowSafe` Sub-case A1
  predicates migrate to Case A — quantifies the
  shrinkage at the row-group boundary.
- **ADR 052 secondary-index confirm-step
  behavior** when the indexed predicate uses
  `FsstRowSafe` — point-read latency delta vs.
  `Fsst` cascade.

Correctness gates:

- Mask byte-identical to dense WFS2 oracle on
  every fixture across the closed atom × type ×
  nullability × selectivity matrix.
- Boundary-crossing-symbol fixture: writer does
  NOT emit cross-row symbols; Strategy B mask
  byte-identical to oracle.
- No-compressed-row-slice regression fixture:
  reader does NOT slice encoded buffer by Arrow
  decoded offsets.
- Length pre-filter zero memcmp work on Disjoint-
  length fixtures.
- Corrupt WFS2 frames and corrupt encoded offsets
  fail closed; no partial masks.
- ADR 060 / 064 mask-only composition byte-
  identical on engagement fixtures.
- ADR 065 Case B/C reachable for `FsstRowSafe`
  predicates that decline the kernel.
- Cross-replica determinism passes on
  representative fixtures.
- WFS1 round-trips byte-identical on existing
  fixtures (regression).

**Exit gate:** Bench artifact provides council
promotion or revision evidence. Council reviews
combined Phase 7 evidence and decides default-on
enablement.

## Phase 8 — Promotion

Files (at promotion):

- `plans/impl/067_row_canonical_fsst_bench_results.md`
  — Phase 7 evidence artifact.
- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
  §"Where we are" amendment: `FsstRowSafe` in
  cascade catalog; new WFS2 frame; format version
  bumped; default cascade for variable-width
  string columns updated per Phase 7 outcome.
- `plans/adr/058_compute_over_encoded_data.md`
  §"Where we are" amendment: `FsstRowSafe` in
  evaluator catalog; default-on per Phase 7
  outcome.
- `plans/adr/059_native_cocd_kernels.md` §"Where
  we are" amendment if default allow-list extends
  to include `FsstRowSafe`.
- `plans/adr/063_page_level_predicate_stats.md`
  §"Where we are" amendment: `FsstRowSafe` is
  row-group grain in V1; not page-emitting.
- `plans/adr/065_dense_predicate_zip_with_sparse_projection.md`
  §"Where we are" amendment: `FsstRowSafe` Sub-
  case A1 predicates migrate to Case A; ADR 065
  V1 engagement surface narrows further.
- `plans/adr/066_fsst_evaluator.md` §"Where we
  are" amendment: V1 Strategy A path remains for
  `Fsst` cascade buffers indefinitely; new writes
  default to `FsstRowSafe` with Strategy B per
  Phase 7 outcome.
- `plans/wip/roadmap.md` planning-cadence update.

Work:

1. Pin Phase 7 bench parameters in the bench-
   results artifact.
2. Council decides default-enablement based on
   Phase 7 evidence.
3. If council promotes to default-on: extend
   `EncodingCascadeKind::DEFAULT_ENABLED` to
   include `FsstRowSafe`, and relax the Phase 1
   architecture guard that pins
   `EncodingWriterConfig::default().enabled_cascades`
   excludes it. If council declines: leave
   `DEFAULT_ENABLED` unchanged; `FsstRowSafe`
   remains opt-in via explicit `enabled_cascades`
   TOML.
4. Mark ADR 067 shipped only after code, tests,
   and bench evidence land together.

**Exit gate:** ADR 067 promoted; ADR 057 cascade
catalog extends by one (`FsstRowSafe`); ADR 058
evaluator catalog extends by one (`FsstRowSafe`);
ADR 066 §"Where we are" amended to record Strategy
A's role going forward.

## Verification gate

Before promotion:

```sh
cargo fmt --all
cargo test fsst_row_safe --lib
cargo test --test encoding_cascade
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

**Canonical-encoder drift between writer and
reader.** Highest correctness risk: if writer's
greedy encoder and reader's literal pre-encoder
diverge on tie-breaking or any edge case, Strategy
B returns false negatives silently. Mitigation:
single shared canonical helper
(`encode_fsst_canonical_row`) used by both sites;
property test compares writer-emitted row bytes
against literal pre-encoder output for randomly-
chosen R and T; outputs must always be byte-
identical (Phase 2 + Phase 3 test).

**Boundary-symbol leak.** If row-boundary-aware
training accidentally counts cross-row pairs (off-
by-one in the offset walking), a frequent cross-row
pair could enter the symbol table and be emitted
as a cross-row symbol. Mitigation: writer property
test inspects encoded frame and verifies no symbol
token straddles a row boundary in the decoded
stream; boundary-crossing-symbol fixture is the
load-bearing correctness gate.

**Per-buffer actual-size guard miscalibration on
short-row workloads.** If the 10% compression-loss
budget is too tight, short-row workloads always
fall back to `Fsst` (no Strategy B win). If too
loose, density regresses silently. Mitigation:
Phase 0 micro-bench measures the actual offset-
table-overhead distribution; council adjusts the
budget if Phase 7 evidence reveals systematic
miscalibration.

**Writer CPU regression from dual-encode.**
Encoding both WFS1 and WFS2 candidates per buffer
costs ~2× the FSST encode time. Mitigation: Phase
7 writer bench measures the overhead; if
prohibitive, V2 considers a sample-based pre-check
(estimate WFS2 size from a row sample, full encode
only when estimate is competitive). V1 ships with
eager dual-encode for correctness over cost.

**WFS1 byte-output regression from refactoring.**
Factoring shared canonical helpers out of
`encode_fsst_bytes` could change WFS1's output if
the refactor introduces a tie-breaking difference.
Mitigation: WFS1 regression suite (Phase 2 step 4)
verifies byte-identical output on existing
fixtures before any refactoring lands. If refactor
risks regression, V1 ships separate WFS1 +
canonical-row encoders with a property test
proving they agree on within-row inputs.

**Strategy B kernel CPU regresses on long
literals.** If `InList` literals are large
(hundreds of bytes), per-row memcmp work dominates.
Mitigation: Phase 7 bench includes long-literal
fixtures; per-buffer scratch-budget cap declines
the kernel to dense for unbounded cases; council
can adjust the budget based on Phase 7 evidence.

**Encoded-offset table dominates short-row
buffers.** 4 bytes per row offset overhead is
significant for ≤16-byte rows. Mitigation: per-
buffer actual-size guard (`wfs2_len ≤
ceil(wfs1_len * 1.10)`) catches this case and
falls back to `Fsst`. Bench fixtures verify the
fallback fires correctly.

**Format-version bump breaks rolling deployment.**
Old binaries reject the bumped
`ENCODING_DIRECTORY_FORMAT_VERSION` before
inspecting cascades. Mitigation: rollout
discipline is reader-before-writer; deploy new
readers everywhere first, then enable WFS2
writers. Document in deployment notes (post-
promotion).

**Atom-set closure drift.** Future contributor
adds range / `LIKE` / regex handling to
`FsstRowSafe` kernel without ADR amendment.
Mitigation: Phase 5 architectural guard pins the
V1 atom set (`Eq` / `Neq` / `InList`).

**Cross-replica non-determinism.** Strategy B
relies on (a) FSST encoding under a fixed symbol
table is greedy and deterministic, (b) row-
boundary flushing is deterministic, (c) pre-
encoding a literal under a fixed symbol table is
deterministic, (d) memcmp is deterministic. Phase
7 cross-replica determinism row gates empirically.

**Strategy B kernel imports forbidden surfaces.**
Phase 5 architectural guard asserts
`kernels/fsst_row_safe.rs` does not import
`kernels/fsst::FsstDecodedByteReader`,
`cascade::decode_fsst_bytes`,
`cascade::decode_fsst_row_safe_bytes`, or the
dense Arrow string materializer.

**Promote-default before evidence.** Phase 8
gates default-enablement on Phase 7 bench
evidence per ADR 059 cadence. Same discipline as
ADR 066.

**Catalog sprawl with two FSST variants.**
Maintenance cost of `Fsst` (Strategy A) +
`FsstRowSafe` (Strategy B). Mitigation: WFS1
helpers stay narrowly scoped; ADR 066 V1 reader
unchanged; shared canonical helpers reused by
WFS2 writer + Strategy B reader; architecture
guards keep the dispatch boundaries clean.

**Predicate corruption semantics overreach.** If
council requires full payload validation per
query, Strategy B's economic shape changes (full
payload scan per buffer). Mitigation: ADR 067
§"Decision §9" explicitly bounds the corruption-
detection responsibility to structural metadata +
compared encoded ranges + AEAD upstream; full
validation is the dense-decode path's job. If the
council later requires per-query full validation,
that's a separate ADR.
