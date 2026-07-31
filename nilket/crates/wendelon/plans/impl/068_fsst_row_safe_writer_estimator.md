# Implementation Plan: ADR 068 — Sample-Based FsstRowSafe Writer Estimator

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/068_fsst_row_safe_writer_estimator.md`

Inherits substrate:

- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
  — `EncodingCascade` catalog, `BufferEncoding` shape,
  `select.rs` cascade selection, the multi-buffer values
  + decoded offsets + validity layout for variable-width
  Arrow types, `ENCODING_DIRECTORY_FORMAT_VERSION`
  discipline (no bump in this ADR).
- `plans/adr/058_compute_over_encoded_data.md` — closed
  predicate evaluator catalog + dispatch surface
  (unchanged by ADR 068).
- `plans/adr/066_fsst_evaluator.md` — Strategy A reader
  for `EncodingCascade::FsstWholeBuffer`; remains the
  read-side dispatch target indefinitely for whole-
  buffer FSST buffers.
- `plans/adr/067_row_canonical_fsst.md` — row-canonical
  FSST writer + Strategy B reader + per-buffer exact
  guard. ADR 068's exact-guard preserving baseline
  preserves every emitted `FsstRowSafe` buffer
  satisfies the ADR 067 guard.
- `plans/conventions/` — newtype-units, no-version-
  suffixes (including the family-first ordering and
  be-explicit-on-both-sides sub-rules; the source
  rename below is the prerequisite `feat:` commit), no-
  planning-phase-names, no-premature-backward-compat,
  capability-vs-feature naming.

## Status

Council-converged 2026-05-10 across Claude / Codex /
Gemini after eight iterations under `plans/wip/`.
Phase 0 found symbol-table training above the P5
threshold. Iter 9 / 10 amended the plan to writer
column-cache amortization:
column-scoped amortization across row groups within
one encoding-directory writer pass.

The plan is sequenced so the cost-free metadata floor
(Phase 2) lands before any sampling or training work
(Phase 3). Writer column-cache adds a crate-private
`WriterCascadeContext` whose FSST symbol-table cache
is keyed by `IpcColumnOrdinal` and scoped to one
`build_encoding_directory_for_ipc_stream` invocation.
Architecture guards (Phase 5) lock down that cached
tables are decline-only evidence and cannot reach
`FsstRowSafe` emission without a current-buffer exact
guard. Current production routing engages the estimator
only on the directory-writer column-cache path where both
`WriterCascadeContext` and `IpcColumnOrdinal` are
available; no-context single-buffer selection remains
exact.

## Implementation contract

- **Naming-vocabulary prerequisite (separate `feat:`
  commit).** Before ADR 068 implementation begins,
  the source-side rename lands as a sibling commit:

  ```text
  EncodingCascade::Fsst        -> EncodingCascade::FsstWholeBuffer
  EncodingCascadeKind::Fsst    -> EncodingCascadeKind::FsstWholeBuffer
  "fsst" cascade label         -> "fsst_whole_buffer"
  WFS1 / wfs1 identifiers      -> FsstWholeBuffer / fsst_whole_buffer
  WFS2 / wfs2 identifiers      -> FsstRowSafe / fsst_row_safe
  b"WFS1"                      -> b"WBFS"
  b"WFS2"                      -> b"RSFS"
  ```

  The on-disk discriminant for the existing whole-
  buffer FSST cascade is unchanged. The magic-byte
  rename is an intentional pre-launch clean break per
  `plans/conventions/no-premature-backward-compat.md`
  — old binaries reject the new magic, new binaries
  reject the old magic.

  The `symbol_table_byte_len` cascade-field cleanup
  (an independent ADR 067 follow-up tracked in ADR
  067 §"V2 deferred") **may** ride along in the
  same `feat:` commit as a sibling pre-launch
  cleanup, but if it does it must carry its own ADR
  067 status reference in the commit message and is
  evaluated under ADR 067's gates, not ADR 068's.
  ADR 068's evidence, gates, scope, and Phase 7
  artifact cover only the estimator. The cleanup
  may also ship in a separate `feat:` commit if
  council prefers atomic per-rename commits; ADR
  068's design content does not depend on either
  commit shape.

- ADR 067's per-buffer exact guard remains
  load-bearing on every emitted `FsstRowSafe`
  buffer:

  ```text
  fsst_row_safe_encoded_len <= decoded_plaintext_len
  AND fsst_row_safe_encoded_len <=
        ceil(fsst_whole_buffer_encoded_len * 1.10)
  ```

- The estimator hooks `select_fsst_candidate` in
  `src/storage/encoding/select.rs`. It may skip the
  full row-safe FSST encode only when it returns
  `Decline(...)`. Otherwise it returns
  `RequireExactGuard` and the existing dual-encode
  path runs unchanged.

- Writer column-cache state is crate-private and writer-pass
  scoped. `build_encoding_directory_for_ipc_stream`
  creates a fresh `WriterCascadeContext` for each
  invocation. The stabilization loop in
  `encode_row_group_batches_with_directory` naturally
  drops the context between iterations because it calls
  the directory builder again after synthetic metadata
  bytes change.

- Phase 1-6 staging uses crate-private
  `FsstRowSafeEstimatorRouting { ExactOnly,
  Estimator }`. Production
  `select_fsst_candidate` calls the exact route during
  Phase 1-6. Tests and benches call with either routing
  for comparison. The writer column-cache production promotion routes
  only context-bearing directory-writer calls to
  `FsstRowSafeEstimatorRouting::Estimator`; no-context
  calls stay on `FsstRowSafeEstimatorRouting::ExactOnly`.
  No operator-visible config field, no `TOML` knob, no
  Cargo feature.

- Estimator-authorized `FsstRowSafe`-only emission
  (option 2) is out of scope. Architecture guards
  enforce it.

- Cached-table `RequireExactGuard` never carries
  exact lengths. Only symbol tables trained on the
  current buffer can produce exact lengths that allow
  `FsstRowSafe` selection.

- No new on-disk structure, format-version bump,
  `EncodingCascade` / `EncodingComputeEvaluatorKind`
  variant, IPC `custom_metadata` key, wire /
  descriptor / PBC / capability / digest /
  `ObjectStore` surface, Cargo feature, or new
  metric label.

## Goal

Reduce ADR 067's ~140 % writer-cost overhead vs
whole-buffer-FSST-only encoding without changing the
read-side contract, on-disk format, surface area, or
per-buffer correctness guard. Three decline
mechanisms (cost-free metadata floor, trained
metadata floor, sample confidence) target the row-
safe FSST full encode on buffers destined to fail
the exact guard. Buffers that pass the estimator
fall through to the existing ADR 067 dual-encode +
exact guard path.

## Phase plan

### Phase 0 — Audit and calibration

Pre-implementation pass that pins all eight Phase 0
items. No estimator code lands in this phase.

- Pin the hook site in `select_fsst_candidate`.
- Compare contiguous prefix vs deterministic spread
  sample shapes on warehouse fixtures (URL equality,
  URL `InList`, long repeated pair, weak-compression
  diagnostic, free-text / log-line if added).
  Measure projection error; pin one shape (P1).
- Audit ADR 067 row-safe FSST frame layout to pin
  `FIXED_FSST_ROW_SAFE_HEADER_BYTES` (P2).
- Tune `EARLY_DECLINE_SAFETY_MULTIPLIER` starting at
  1.20× for zero false declines on warehouse
  winners (P3).
- Audit codec boundary: can `encode_buffer_with_value_offsets`
  (or equivalent) accept pre-trained whole-buffer /
  row-safe FSST symbol tables for `RequireExactGuard`
  reuse? (P4)
- Measure symbol-table training wall-clock against
  **full-buffer greedy encode wall-clock** (the cost
  the estimator is trying to avoid). If training
  alone exceeds 25 % of full-buffer greedy encode
  wall-clock on representative warehouse fixtures,
  council reconvenes (P5). Phase 0 hit this branch:
  iter 10 selected writer column-cache amortization. The
  gate denominator remains full-buffer greedy encode
  wall-clock; the numerator becomes the amortized
  training share per warm evaluated buffer.
- Pick estimator module location: `cascade/estimator.rs`
  sibling vs co-located in `select.rs` (P6).
- Pick bench-internal counter shape: `AtomicUsize` vs
  trace events (P7).
- Identify or add a free-text / log-line warehouse
  fixture if the carry-forward set is too URL-heavy
  (P8).

Phase 0 deliverable: a short audit note attached to
this impl plan (or recorded in
`plans/impl/068_fsst_row_safe_writer_estimator_bench_results.md`
under "Phase 0 audit") that pins each item.

### Phase 1 — Estimator skeleton and deterministic sampling

Lands the type surface, the routing helper, and the
deterministic sampling helper. No decline logic yet.

- Add the estimator types per the ADR §"Decision §7".
  Phase 0 has already pinned P1 (sample shape), so
  Phase 1 wires only the chosen production shape into
  `FsstRowSafeEstimatorSampleShape`. The other shape
  remains as a bench-only prototype helper if Phase 0
  needs ongoing comparison evidence; it does not
  appear in the production `select_fsst_candidate`
  call path. Types added:
  - `FsstRowSafeEstimatorDecision`
  - `FsstRowSafeEstimatorDecline`
  - `FsstRowSafeEstimatorSampleShape` (production
    variant only — single discriminant in V1; second
    discriminant only if a bench-comparison helper
    needs it)
  - `FsstRowSafeEstimatorRouting`
  - `FsstRowSafeSampleParts`
  - `FsstRowSafeEstimatorStats` (bench-only
    diagnostic struct)
- Add `select_fsst_candidate_with_routing` wrapper
  around the existing `select_fsst_candidate`
  logic. Production call site continues to use
  `FsstRowSafeEstimatorRouting::ExactOnly` and
  delegates to `select_fsst_candidate_exact` (the
  renamed-or-wrapped existing logic).
- Add the deterministic sampling helper for the
  P1-pinned shape only, leveraging
  `deterministic_sample_indices` /
  `cascade_sample_rows`. The non-pinned shape, if
  present, is a bench-only prototype helper outside
  the production call path.
- No new public API. All types are `pub(crate)`.

### Phase 2 — Cost-free metadata-floor decline

Lands the first decline arm. The cheapest possible
check: zero training, zero sampling, zero encoding
work.

- Add `fsst_row_safe_metadata_floor_bytes(row_count:
  usize) -> Option<usize>` per ADR §"V1 contract
  summary §1". Uses checked arithmetic; returns
  `None` on overflow.
- Add
  `fsst_row_safe_metadata_payload_floor_bytes(row_count,
  decoded_plaintext_len, value_offsets) -> Option<usize>`.
  This cost-free helper adds a one-byte minimum payload
  for each non-empty row. It is exact for row-safe FSST
  and catches short non-empty rows before training or
  sampling.
- In `FsstRowSafeEstimatorRouting::Estimator`:
  compute the floors; if either exceeds
  `decoded_plaintext_len`, return
  `Decline(MetadataFloorExceedsPlaintext)` or
  `Decline(MetadataPayloadFloorExceedsPlaintext)`.
  `None` routes to `RequireExactGuard`.
- Add focused unit tests for boundary conditions
  (row counts at 0, 1, 255, 256, 257, very large).

### Phase 3 — Sample encode + trained metadata-floor decline

Lands the second decline arm and the sample-encode
infrastructure. The trained-floor arm runs
**after** symbol training and **before** sampled
encoding to skip the sample-encode CPU work when
trained metadata alone disqualifies row-safe FSST.

- Add the shared `fsst_row_safe_metadata_bytes(
  row_count: usize, fsst_row_safe_symbol_count:
  usize, exception_count: usize) -> Option<usize>`
  helper. Used by both the trained-floor arm
  (`exception_count = 0`) and the sample-projection
  metadata term (`exception_count = projected from
  sample`). Single arithmetic source of truth keeps
  the trained floor exact.
- Add full-buffer symbol-table training for both
  whole-buffer FSST and row-safe FSST. Reuse the
  existing ADR 066 / 067 training entry points;
  the codec boundary may need a small refactor to
  expose them at the estimator's call site.
- Add `WriterCascadeContext` and
  `ColumnFsstSymbolTableCache` as crate-private
  selection types. The context is keyed by
  `IpcColumnOrdinal` and is passed only by the
  encoding-directory writer path.
- Cold writer-column-cache misses run the current-buffer exact
  route and opportunistically warm or refresh the
  column cache from symbol tables already trained for
  that exact guard. They do not train solely for
  priming.
- After current-buffer training, before sampling: compute the
  trained metadata floor with the actual full-
  buffer-trained row-safe FSST symbol count and
  zero exceptions. If it exceeds
  `decoded_plaintext_len`, return
  `Decline(TrainedMetadataFloorExceedsPlaintext)`.
- Add `encode_fsst_whole_buffer_sample_len` and
  `encode_fsst_row_safe_sample_parts` helpers.
  Both take the **full-buffer-trained** symbol
  tables. Return sizes for projection without
  producing on-disk bytes.
- If P4 confirmed table reuse, plumb the trained
  symbol tables into a private reusable handle
  passed through `RequireExactGuard` only when the
  tables were trained on the current buffer. Warm writer-column-cache
  cache hits that return `RequireExactGuard` must
  fall back to the current-buffer exact route.
  Cross-buffer cached tables must not produce trained-
  floor or sample-confidence declines; a 1M weak-
  compression run showed cross-buffer table-dependent
  declines can lose exact row-safe winners. Warm cached
  evidence is therefore cost-free-floor-only until a
  future ADR proves a safe table-dependent rule.

### Phase 4 — Projection logic and sample-confidence decline

Lands the third decline arm and the conservative
`max` decision rule.

- Scale sampled payload lengths to full-buffer
  estimates using saturating integer arithmetic.
  Add the row-safe FSST metadata term computed via
  `fsst_row_safe_metadata_bytes(row_count,
  fsst_row_safe_symbol_count, projected_exception_count)`.
- Apply the conservative decision rule:

  ```text
  estimated_fsst_row_safe_encoded_len >
    max(decoded_plaintext_len,
        ceil(estimated_fsst_whole_buffer_encoded_len * 1.10))
      * EARLY_DECLINE_SAFETY_MULTIPLIER
  ```

- On overrun: return
  `Decline(SampledRowSafeExceedsPlaintext)` if the
  plaintext side is breached, or
  `Decline(SampledRowSafeExceedsWholeBufferGuard)`
  if the whole-buffer-FSST side is breached. (Both
  if both — pick the dominant breach for
  diagnostics; either decline reason is a valid
  early-out.)
- Otherwise return `RequireExactGuard` (carrying
  the reusable training handle if P4 confirmed
  reuse).
- `EARLY_DECLINE_SAFETY_MULTIPLIER` is a
  `pub(crate) const` integer/rational; not a config
  field, not exposed through any public surface.

### Phase 5 — Selection integration and architecture guards

Wires the estimator into `select_fsst_candidate` for
test/bench use, and locks down the contract so
estimator emission of `FsstRowSafe` cannot leak.

- Production `select_fsst_candidate` continues to
  call
  `select_fsst_candidate_with_routing(...,
  FsstRowSafeEstimatorRouting::ExactOnly)`. No
  behavior change in production.
- Test/bench helpers can pass either routing for
  comparison.
- Add `tests/architecture_test.rs` guards:
  - No `pub` field or method exposes
    `FsstRowSafeEstimatorRouting`,
    `FsstRowSafeEstimatorDecision`,
    `FsstRowSafeEstimatorDecline`, or
    `FsstRowSafeEstimatorStats`.
  - `WriterCascadeContext` and
    `ColumnFsstSymbolTableCache` remain
    crate-private.
  - Cached-table estimator paths cannot return
    current-buffer exact lengths.
  - `EncodingWriterConfig` has no
    `estimator_enabled` field (or any equivalent
    name).
  - `select_fsst_candidate_with_routing`'s
    `Estimator` branch, when reaching
    `Decline(...)`, never returns
    `EncodingCascade::FsstRowSafe` from the resulting
    selection. (Static check via test that exhausts
    the decline variants and asserts the post-
    decline cascade is `FsstWholeBuffer` or
    non-FSST.)

### Phase 6 — Focused tests

- **Compression-byte equality oracle** — for each
  warehouse winner fixture, assert that bytes
  produced via
  `FsstRowSafeEstimatorRouting::Estimator` are
  byte-identical to bytes produced via
  `FsstRowSafeEstimatorRouting::ExactOnly`.
- **Decline-fixture cascade match** — for each
  decline fixture, assert that the estimator's
  early-decline path selects the same final cascade
  as the exact route would have selected.
- **Cross-replica determinism** — encode the same
  buffer twice in the same process and across two
  process invocations; assert byte-equal cascade
  selection and emitted bytes for both routings.
- **Writer column-cache provenance** — assert that a cold exact
  route warms the `IpcColumnOrdinal` cache, and that
  a warm cached-table `RequireExactGuard` does not
  expose exact lengths or emit `FsstRowSafe` without
  re-running current-buffer exact selection.
- **Decline-arm coverage** — synthesize fixtures
  that fire each of the four
  `FsstRowSafeEstimatorDecline` variants exactly,
  and assert the diagnostic struct records the
  expected variant.
- **Trained-floor exactness** — synthesize a
  fixture where the trained floor exactly equals
  `decoded_plaintext_len - 1`, and another where it
  exactly equals `decoded_plaintext_len + 1`. The
  first must pass to `RequireExactGuard`, the
  second must
  `Decline(TrainedMetadataFloorExceedsPlaintext)`.
- **Overflow / uncertainty** — synthesize an input
  that overflows
  `fsst_row_safe_metadata_floor_bytes` /
  `fsst_row_safe_metadata_bytes`; assert the result
  is `RequireExactGuard`, never a `Decline`.

### Phase 7 — Bench matrix and promotion

Records all eight promotion gates from the ADR.
Lives in
`plans/impl/068_fsst_row_safe_writer_estimator_bench_results.md`.

- Extend `benches/encoding_compute.rs` to invoke
  the directory writer path with writer-column-cache row-group-count
  tiers (1 / 2 / 4 / 8 / 16) and both exact /
  estimator routings on each fixture × tier. Record:
  - per-buffer cascade choice;
  - per-buffer on-disk bytes;
  - cache misses, cache hits, and warm-hit decline
    rate;
  - writer wall-clock per buffer (split where the
    harness can attribute it: cold exact, warm
    estimator, fallback exact, total);
  - decline reason (when `Decline`);
  - whether `RequireExactGuard` came from current-
    buffer tables or cross-buffer cached tables.
- Fixture matrix by row-group-count tier:
  - **Warehouse winners (zero-false-decline gate):**
    URL equality, URL `InList`, long repeated pair,
    weak-compression warehouse diagnostic, free-
    text / log-line if added in Phase 0.
  - **Decline fixtures (≥ 1.5× speedup gate):**
    short-row workloads where row-safe FSST
    metadata dominates, weak-compression workloads
    where row-safe FSST cannot stay within 10 % of
    whole-buffer FSST, sample-promising-but-full-
    buffer-failing workloads.
  - **Plausible-winner fixtures (≤ 5 % overhead
    gate):** the warehouse-winner fixture set
    re-run as plausible-winner profiling.
  - **Mixed workload (≥ 30 % delta-closure soft
    gate):** combined warehouse + adversarial
    suite; record writer-cost delta vs whole-
    buffer-FSST-only.
- Profiles: `PointRead` and `Scan` per the ADR 062 /
  063 / 064 / 065 / 066 / 067 cadence.
- Read-side sanity row: on each warehouse-winner
  fixture, **predicate masks must be byte-identical**
  between the two routings, and **read wall-clock is
  recorded with no statistically significant
  regression** (within the bench harness's existing
  noise envelope; matches ADR 067 Phase 7's
  read-side sanity discipline).
- ADR 065 Case A/B/C route distribution recorded
  for each fixture as opportunity-loss diagnostic
  (would-have-engaged Case B/C if the estimator
  declined the buffer).
- Cross-replica determinism check: encode each
  fixture in two processes; assert byte-equal
  cascade selection and emitted bytes.

If all hard gates (1-7) pass and the soft gate (8)
either passes or is explicitly justified:

- **Promotion commit (single commit):** route the
  production directory-writer column-cache path through
  `FsstRowSafeEstimatorRouting::Estimator` when a
  `WriterCascadeContext` and `IpcColumnOrdinal` are
  present. No-context selection remains exact unless a
  separate evidence pass proves cold single-buffer
  overhead is acceptable. The routing enum stays in the
  codebase for bench/test comparison after promotion.
- Amend `plans/adr/067_row_canonical_fsst.md`
  §"Status" to record the new writer-cost delta and
  the closing of the V2 follow-up.
- Amend `plans/adr/057_per_buffer_pages_and_encoding_cascades.md`
  §"Where we are" with the updated writer-cost
  delta.
- If a bench-only second-shape helper was kept
  during Phase 1-6 for comparison evidence, decide
  whether to remove it in the promotion commit or
  keep it as a permanent bench-comparison helper.
  Production code already wires only the P1-pinned
  shape; this is a bench-cleanup-only decision.

If a hard gate fails: the bench artifact records
the gap. Council decides whether to revise V1
(adjust safety multiplier, adjust sample shape,
narrow eligibility) or open an option-2 ADR
(estimator-authorized `FsstRowSafe`-only emission
with separate false-positive promotion evidence).

## Files touched

Implementation (in scope for this ADR):

- `src/storage/encoding/select.rs` — hook site,
  `select_fsst_candidate_with_routing` wrapper,
  `FsstRowSafeEstimatorRouting`, writer column-cache
  `WriterCascadeContext`, and column-cache plumbing.
- `src/storage/encoding/directory.rs` — writer-pass
  context lifetime for encoding-directory builds.
- `src/storage/encoding/cascade/estimator.rs` (new,
  pending P6) — estimator types, sampling helper,
  metadata helpers, projection arithmetic, decision
  rule.
- `src/storage/encoding/cascade/sample.rs` (or
  equivalent) — deterministic variable-width
  sampling helper if a new module is needed.
- `tests/encoding_select.rs` — focused tests per
  Phase 6.
- `tests/architecture_test.rs` — architecture
  guards per Phase 5.
- `benches/encoding_compute.rs` — bench matrix per
  Phase 7.
- `plans/adr/068_fsst_row_safe_writer_estimator.md`
  — companion ADR (this file's sibling).
- `plans/impl/068_fsst_row_safe_writer_estimator_bench_results.md`
  (new) — Phase 7 evidence.

Out of scope (lands in the prerequisite `feat:`
commit):

- The `Fsst` → `FsstWholeBuffer`,
  `b"WFS1"`/`b"WFS2"` → `b"WBFS"`/`b"RSFS"`,
  `wfs1_*`/`wfs2_*` → `fsst_whole_buffer_*` /
  `fsst_row_safe_*` source-side rename across
  `src/`, `tests/`, `benches/`, and architecture
  guards.

## Risks and mitigations

- **R1 — False decline loses Strategy B read wins.**
  The main new risk per the opportunity-loss
  framing. *Mitigations:* conservative V1 safety
  multiplier (1.20×); zero false-decline hard gate
  on warehouse winners; cost-free row-safe floor
  avoids any sampling work on the most adversarial
  workloads (cheapest first cut); Phase 0 sample-
  shape pick tunes projection error before Phase 1
  hardens.
- **R2 — Estimator overhead exceeds savings.**
  *Mitigations:* ≤ 5 % plausible-winner overhead
  hard gate; cost-free row-safe floor avoids current-
  buffer training on short-row decline checks; warm
  `RequireExactGuard` still retrains the current buffer
  and is measured explicitly; current-buffer trained
  metadata-floor arm skips sample-encode CPU on a
  separate class of losers.
- **R3 — Trained-floor arithmetic drifts from frame
  layout.** A rename of an ADR 067 frame field could
  break the metadata floor's exactness silently.
  *Mitigations:* shared
  `fsst_row_safe_metadata_bytes` helper used by both
  the trained-floor arm and the projection metadata
  term — single arithmetic source of truth; Phase 0
  pins `FIXED_FSST_ROW_SAFE_HEADER_BYTES` against
  the actual frame layout (P2); overflow /
  uncertainty routes to `RequireExactGuard`, never
  to `Decline`.
- **R4 — Cross-replica nondeterminism.** Sampled
  encoding under different hash-map iteration order
  or CPU-capability dispatch could produce different
  cascade selections across replicas. *Mitigations:*
  deterministic sample row selection; integer-only
  projection arithmetic; cross-replica determinism
  hard gate; reuse ADR 067's existing canonical
  encoder which already enforces deterministic
  output.
- **R5 — Symbol-table training cost is the new
  floor.** Phase 0 confirmed per-buffer training
  cannot meet P5. *Mitigations:* writer column-cache keeps
  the writer-context routing surface scoped to one
  writer pass; cold misses warm the cache from current-
  buffer exact work only; cross-buffer cached tables no
  longer drive table-dependent declines after 1M false-
  decline evidence; cost-free row-safe floor catches the
  most adversarial workloads before training even runs.

## Definition of done

- All eight Phase 0 pins recorded in the bench-
  results artifact.
- Writer column-cache lifetime, column keying, and exact-length
  provenance covered by focused tests and architecture
  guards.
- All four `FsstRowSafeEstimatorDecline` variants
  exercised by focused tests.
- All seven hard promotion gates passing.
- Soft mixed-workload gate either passing or
  explicitly justified in the artifact.
- Promotion commit lands routing production directory-writer
  column-cache calls to `FsstRowSafeEstimatorRouting::Estimator`.
- ADR 067 §"Status" and ADR 057 §"Where we are"
  amended with the new writer-cost delta.
- Roadmap entry #24 updated to record promotion.
