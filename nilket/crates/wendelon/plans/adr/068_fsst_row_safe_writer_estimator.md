# ADR 068: Sample-Based FsstRowSafe Writer Estimator

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/068_fsst_row_safe_writer_estimator.md`

## Status

Proposed 2026-05-10. Council-converged across Claude /
Codex / Gemini after eight iterations under
`plans/wip/`. Entering Phase 0 audit + calibration.

This ADR is the tracked V2 follow-up to ADR 067's
explicit waiver of the ADR 057 ≤ 75 % writer-cost
gate. ADR 067 promoted default-on with ~140 % writer
overhead from eager dual-encode; ADR 067's bench-
results artifact pinned the follow-up as *"sample-
based estimator pre-check that picks one candidate
before the full encode."* This ADR is that follow-up.

This ADR does **not** propose any on-disk format
change, surface change, or operator-visible
configuration. It introduces an internal, deterministic,
sample-based pre-check inside `select_fsst_candidate`
that early-declines obvious row-safe FSST losers
without amending ADR 067's per-buffer exact guard.
Buffers the estimator does not early-decline fall
through to ADR 067's existing dual-encode path.

**Naming-vocabulary prerequisite.** This ADR is
written in the council's settled vocabulary
(`FsstWholeBuffer` / `FsstRowSafe`, `b"WBFS"` /
`b"RSFS"`, `whole-buffer FSST` / `row-safe FSST`).
The corresponding source-side rename (`Fsst` →
`FsstWholeBuffer`, `b"WFS1"` → `b"WBFS"`, `b"WFS2"`
→ `b"RSFS"`, `wfs1_*` / `wfs2_*` →
`fsst_whole_buffer_*` / `fsst_row_safe_*`) is the
prerequisite `feat:` commit per the iter-5 deferred-
feat scope. ADR 068 implementation begins after that
rename lands.

## V1 contract summary

- **Exact-guard preserving (option 1).** The
  estimator only declines obvious row-safe FSST
  losers. Any buffer it does not early-decline falls
  through to ADR 067's existing dual-encode +
  per-buffer exact guard:

  ```text
  fsst_row_safe_encoded_len <= decoded_plaintext_len
  AND fsst_row_safe_encoded_len <=
        ceil(fsst_whole_buffer_encoded_len * 1.10)
  ```

  Every emitted `FsstRowSafe` buffer still satisfies
  ADR 067's exact guard using actual full-buffer
  encoded lengths. Estimator-authorized
  `FsstRowSafe`-only emission (option 2) is V1-deferred
  and explicitly out of scope; it is an amendment to
  ADR 067's guard with separate false-positive
  promotion evidence.

- **Three decline mechanisms.**
  1. **Cost-free metadata-floor decline (pre-training).**
     Row-safe FSST structural metadata has a computable
     lower bound from `row_count` alone, with zero
     symbol-table bytes and zero exception entries:

     ```text
     fixed_fsst_row_safe_header_bytes
       + 8 * ceil(row_count / 256)   // block_offsets
       + row_count                   // encoded_lengths
     ```

     If this lower bound exceeds `decoded_plaintext_len`,
     row-safe FSST cannot pass ADR 067's plaintext half-
     guard. Decline immediately with no symbol-table
     training, sampling, or full encode work.
  2. **Trained metadata-floor decline (post-training,
     pre-sample-encode).** After full-buffer symbol-
     table training and **before** sampled payload
     encoding, recompute the row-safe FSST metadata
     floor using the **actual** full-buffer-trained
     row-safe FSST symbol count and zero exceptions.
     If this trained floor alone exceeds
     `decoded_plaintext_len`, row-safe FSST cannot
     pass the plaintext half-guard regardless of
     payload bytes. The check is essentially free
     (one comparison after training that is already
     happening); the savings is the sample-encoding
     work skipped on these buffers.
  3. **Sample confidence decline (post-sample-encode).**
     Deterministically sample rows using ADR 057's
     existing `cascade_sample_rows` budget, encode the
     sample under the **full-buffer-trained** whole-
     buffer + row-safe FSST symbol tables (sample-
     trained tables are forbidden — they predict a
     different codec), project full-buffer sizes from
     the sample, and decline only when the projection
     exceeds the exact guard by a council-pinned
     conservative safety margin.

- **Determinism is load-bearing.** Writer cascade
  selection determines on-disk bytes. The estimator
  must make byte-identical decisions across replicas
  fed the same buffer. Deterministic sample row
  selection, integer-only projection arithmetic, no
  wall-clock / random / hash-map-iteration / CPU-
  capability input.

- **Surfaces unchanged.** No new on-disk structure,
  format-version bump, `EncodingCascade` /
  `EncodingComputeEvaluatorKind` variant, IPC
  `custom_metadata` key, wire / descriptor / PBC /
  capability / digest / `ObjectStore` surface, Cargo
  feature, metric label, or operator-visible
  configuration. Phase 1-6 staging happens via
  crate-private `FsstRowSafeEstimatorRouting`
  (`ExactOnly` / `Estimator`) reachable only from
  test/bench helpers. Phase 7 promotion is a single
  commit that flips the production
  `select_fsst_candidate` call site to
  `FsstRowSafeEstimatorRouting::Estimator`. The
  routing enum stays in the codebase for
  bench/test comparison after promotion.

## Context

ADR 067 shipped row-canonical FSST framing
(`b"RSFS"`, `EncodingCascade::FsstRowSafe`) +
Strategy B predicate evaluation default-on
2026-05-10. Read-side gains were transformative:
predicate wall-clock 3.0–11.7× vs dense whole-buffer
FSST, Strategy B 4.3–9.2× vs Strategy A, end-to-end
1.81–5.43×, ≤ 8.7 % compression loss inside the
10 % guard, ≤ 3.1 % dense-fallback overhead, masks
byte-deterministic across replicas.

The writer cost is the open follow-up. ADR 067's V1
writer eagerly encodes both whole-buffer FSST and
row-safe FSST candidates for every FSST-eligible
variable-width buffer in order to evaluate the per-
buffer exact guard:

```text
select FsstRowSafe iff:
  fsst_row_safe_encoded_len <= decoded_plaintext_len
  AND fsst_row_safe_encoded_len <=
        ceil(fsst_whole_buffer_encoded_len * 1.10)
```

The bench artifact recorded ~140 % writer overhead
versus the whole-buffer-FSST-only baseline. Council
explicitly waived ADR 057's ≤ 75 % writer-cost gate
for ADR 067 default-on given the read-side wins, but
the waiver was bounded: a sample-based writer
estimator is the tracked follow-up.

The central design constraint is that ADR 067's
exact guard is not sample-derived. The whole-buffer-
FSST half-guard requires an actual whole-buffer FSST
encoded length. Replacing that side of the guard
with a sample estimate is a semantic change with a
new false-positive risk surface (sampled false
positives could silently choose `FsstRowSafe` for
buffers that would have failed the exact whole-
buffer-FSST ratio guard).

ADR 068 therefore reduces unnecessary dual-encode
work without changing the correctness or density
contract for emitted `FsstRowSafe` buffers. The
strong "estimator-authorizes-emission" path (option
2) is deferred to a future ADR predicated on
concrete Phase 7 evidence that option 1 cannot
recover the writer-cost delta.

## Decision

Add a deterministic sample-based pre-check to
`select_fsst_candidate` with three decline
mechanisms (above) and the staging surface above.
Detailed shape:

1. **Hook site.** `select_fsst_candidate` in
   `src/storage/encoding/select.rs`. Phase 0 audit
   pins whether the estimator helpers live in a new
   `cascade/estimator.rs` sibling or co-located in
   `select.rs`.

2. **Sampling.** Use ADR 057's existing
   `cascade_sample_rows` writer setting (no second
   sampling knob). Phase 0 chooses between contiguous
   prefix and deterministic spread sample shapes
   based on projection-error evidence on warehouse
   fixtures; V1 hardens exactly one shape as a
   crate-private constant.

3. **Symbol-table training.** Both whole-buffer FSST
   and row-safe FSST symbol tables are trained on the
   **full buffer**. Sample-trained tables are
   forbidden because they estimate a different codec
   than the writer would emit. The Phase 0 audit
   measures training cost vs the **full-buffer greedy
   encode cost** (the cost the estimator is trying to
   avoid); if training alone exceeds 25 % of full-
   buffer greedy encode wall-clock on representative
   warehouse fixtures, council reconvenes before
   Phase 1 hardens. The denominator is the avoided
   cost, not estimator wall-clock — this estimator
   trains on the full buffer but encodes only a small
   sample, so training will naturally dominate
   estimator wall-clock; that is acceptable as long
   as training stays cheap relative to the work it
   replaces.

4. **Projection arithmetic.** Integer-only, with
   saturating arithmetic. `Option<usize>` returns
   from metadata helpers; `None` (overflow /
   uncertainty) routes to `RequireExactGuard`, never
   to `Decline`. Projection scales sampled payload
   lengths to full-buffer estimates and adds the
   row-safe FSST metadata term computed from the
   full-buffer-trained symbol count plus projected
   exception count.

5. **Decision rule.** V1 uses the conservative `max`
   shape:

   ```text
   estimated_fsst_row_safe_encoded_len >
     max(decoded_plaintext_len,
         ceil(estimated_fsst_whole_buffer_encoded_len * 1.10))
       * EARLY_DECLINE_SAFETY_MULTIPLIER
   ```

   `EARLY_DECLINE_SAFETY_MULTIPLIER` starts at
   1.20× as an integer/rational constant. Phase 0
   tunes the value for zero false declines on
   warehouse winners and records how much additional
   decline-path saving the per-guard threshold shape
   would have produced (V2 evidence, not a V1 toggle).

6. **Symbol-table reuse on `RequireExactGuard`.** If
   Phase 0 confirms ADR 067's exact dual-encode path
   can accept pre-trained symbol tables,
   `RequireExactGuard` carries a private reusable-
   training handle and the dual-encode skips
   retraining. If not, V1 retrains and the plausible-
   winner overhead promotion gate measures the cost.

7. **Estimator types (private to encoding module).**

   ```rust
   pub(crate) enum FsstRowSafeEstimatorDecision {
       Decline(FsstRowSafeEstimatorDecline),
       RequireExactGuard,
   }

   pub(crate) enum FsstRowSafeEstimatorDecline {
       MetadataFloorExceedsPlaintext,
       TrainedMetadataFloorExceedsPlaintext,
       SampledRowSafeExceedsPlaintext,
       SampledRowSafeExceedsWholeBufferGuard,
   }

   pub(crate) enum FsstRowSafeEstimatorSampleShape {
       Prefix,
       DeterministicSpread,
   }

   pub(crate) enum FsstRowSafeEstimatorRouting {
       ExactOnly,
       Estimator,
   }

   pub(crate) struct FsstRowSafeSampleParts {
       pub payload_len: usize,
       pub exception_count: usize,
   }
   ```

   These types stay `pub(crate)`; they do not appear
   in any public API. Architecture guards (Phase 5)
   enforce that the estimator's `Decline` branches
   cannot reach `EncodingCascade::FsstRowSafe`
   emission.

## Rationale

### Preserve the exact guard for emission

ADR 067's safety net is exact precisely because it
compares actual row-safe FSST encoded length against
actual whole-buffer FSST encoded length and decoded
plaintext length. That precision is why default-on
was acceptable despite row-safe FSST's additional
metadata. Replacing the whole-buffer-FSST side with a
sample estimate is a semantic change — possibly the
right next step, but not one to make before seeing
how much cost the conservative pre-check recovers.

### Early decline targets the waste, not the read win

The waste ADR 068 can remove without changing ADR
067 is the row-safe FSST full encode for buffers that
were always going to decline:

- short-row buffers where row-safe FSST metadata
  dominates;
- weak-compression buffers where row-safe FSST cannot
  stay within 10 % of whole-buffer FSST;
- FSST-compressible-looking samples that are not
  competitive at full-buffer scale.

On warehouse fixtures where row-safe FSST is
plausibly good, the writer still dual-encodes and
keeps ADR 067's exact guard. ADR 068's baseline will
not eliminate writer overhead on the workloads that
actually select `FsstRowSafe`. That is intentional;
the promotion artifact sets that expectation rather
than treating it as a surprise failure.

### Trained floor saves sample-encode CPU

The trained metadata floor was the single most
debated arm across the council (three oscillations
in iter 3 / 5 / 6 / 7 / 8). Codex's iter-6 reframing
settled it: the check is essentially free (one
comparison after training that already has to happen
for sampled projection) and the savings is the
sample-encoding work skipped on buffers where
trained metadata alone disqualifies row-safe FSST.
It is not a sampled guard amendment; it is an exact
lower-bound check that uses the full-buffer-trained
symbol count.

### False decline is the main new risk

Because ADR 068 cannot emit `FsstRowSafe` without
exact verification, the new risk is not "bad
`FsstRowSafe` bytes got written." The new risk is
"a buffer that would have earned Strategy B is left
on whole-buffer FSST." That loses read performance,
can expand ADR 065 Case B/C engagement, and weakens
the post-ADR-067 default-on benefit. Promotion
gates focus on `FsstRowSafe` opportunity preservation
on representative warehouse fixtures, not only on
writer CPU reduction on adversarial decline fixtures.

### Determinism remains load-bearing

Two replicas fed the same values buffer, offsets,
data type, and writer config must make the same
early-decline decision. Deterministic sample row
selection, deterministic sampled encoding under
full-buffer symbol tables, integer-only threshold
arithmetic, no wall-clock / random / hash-map-
iteration / CPU-capability input.

## Consequences

- **Writer-cost waiver may persist.** The
  conservative baseline may reduce ADR 067 writer
  overhead without fully returning it under the ADR
  057 ≤ 75 % target. The promotion artifact records
  the residual delta and serves as the explicit
  motivation for an option-2 escalation ADR if
  warranted.
- **Symbol-table training cost is the new floor.**
  Even on declined buffers, the estimator trains
  full-buffer symbol tables. If Phase 0 confirms
  table reuse on `RequireExactGuard`, the marginal
  cost on the winner path is one greedy encode round;
  if not, the winner path retrains and the plausible-
  winner overhead gate measures it.
- **Estimator complexity is internal.** No operator
  surface, no on-disk impact, no protocol surface.
  The cost is one new internal module and the
  routing helper. The architectural-test guards
  (Phase 5) prevent the estimator's decline branches
  from ever emitting `FsstRowSafe` bytes.
- **Three decline arms vs simpler shapes.** Council
  considered both a single sample-confidence-only
  arm and a two-arm (cost-free floor + sample
  confidence) shape. The trained-floor arm earned its
  place because the metadata-bytes term is computed
  for projection arithmetic anyway; gating sample
  encoding on it is essentially free with non-trivial
  CPU savings on the workloads where it fires.

## Phase 0 Pins

These are empirical calibration items the Phase 0
audit closes against warehouse fixtures, not council
deliberation questions. None are council-blocking.

- **P1: Sample shape.** Contiguous prefix vs
  deterministic spread, picked by projection-error
  evidence on the warehouse fixture matrix (URL
  equality, URL `InList`, long repeated pair, weak-
  compression diagnostic, free-text / log-line if
  added per P8). V1 hardens exactly one shape; the
  unused helper is removed.
- **P2: `FIXED_FSST_ROW_SAFE_HEADER_BYTES`.** Exact
  fixed-header byte count, including whether the
  exception-count field is physically inside the
  fixed header per ADR 067 row-safe FSST frame
  layout. Pins both metadata helpers' arithmetic.
- **P3: `EARLY_DECLINE_SAFETY_MULTIPLIER`.** Starts
  at 1.20×; Phase 0 confirms zero false declines on
  warehouse winners or revises. Records how much
  additional decline-path saving the per-guard
  threshold shape would have produced (V2 evidence).
- **P4: Symbol-table reuse on `RequireExactGuard`.**
  Whether `encode_buffer_with_value_offsets` (or
  equivalent ADR 067 codec entry point) can accept
  pre-trained whole-buffer / row-safe FSST symbol
  tables. If yes, `RequireExactGuard` carries a
  reusable-training handle. If no, V1 retrains and
  the plausible-winner overhead gate measures it.
- **P5: Symbol-training cost vs avoided full encode.**
  Training wall-clock measured against **full-buffer
  greedy encode wall-clock** (the cost the estimator
  is trying to avoid), not against estimator wall-
  clock. If training alone exceeds 25 % of full-buffer
  greedy encode wall-clock on representative warehouse
  fixtures, council reconvenes to consider amortization
  shapes (per-page table cache, etc.) before Phase 1
  hardens. Training will naturally dominate estimator
  wall-clock since the estimator encodes only a small
  sample; that is acceptable.
- **P6: Estimator module location.** New
  `cascade/estimator.rs` sibling vs co-located in
  `select.rs`, picked on file size and call-site
  clarity.
- **P7: Bench-internal counter implementation.**
  `AtomicUsize` counters vs trace events; pin which
  shape surfaces the diagnostic stats without
  introducing new metric labels.
- **P8: Free-text / log-line warehouse fixture.**
  Identify or add a non-URL warehouse fixture if
  Phase 0 finds the carry-forward set is too URL-
  heavy and projection-error evidence is biased.

## Promotion criteria

Bench artifact at
`plans/impl/068_fsst_row_safe_writer_estimator_bench_results.md`
records the following gates. Hard gates are
correctness / contract; soft gates are performance
targets that the artifact can fail and still
promote with explicit recorded justification.

1. **Hard — exact-guard preservation.** Every
   emitted `FsstRowSafe` buffer satisfies the full
   ADR 067 exact guard using actual encoded lengths.
2. **Hard — compression-byte equality oracle.**
   Encoded bytes via the estimator route
   (`FsstRowSafeEstimatorRouting::Estimator`) match
   exact-route bytes
   (`FsstRowSafeEstimatorRouting::ExactOnly`)
   byte-for-byte on every warehouse winner fixture.
3. **Hard — zero false declines on warehouse
   winners.** On representative ADR 067
   `FsstRowSafe`-winner fixtures across 8K / 65K /
   1M rows: URL equality, URL `InList`, long repeated
   pair, weak-compression warehouse diagnostic, and
   free-text / log-line if added in Phase 0. The
   estimator must not early-decline any of these.
4. **Hard — decline-path writer speedup.** ≥ 1.5×
   speedup over ADR 067 eager dual-encode on decline
   fixtures, selecting the identical final cascade
   as the exact oracle.
5. **Hard — plausible-winner overhead bounded.**
   ≤ 5 % overhead on `RequireExactGuard` buffers
   where the exact guard selects `FsstRowSafe`,
   versus ADR 067's eager dual-encode path.
6. **Hard — cross-replica determinism.** Cascade
   selection and emitted bytes are byte-equal on
   representative fixtures across two writer runs
   with the same input and config.
7. **Hard — read-side invariants unchanged.**
   Existing ADR 067 `FsstRowSafe` predicate masks
   remain byte-identical on fixtures where
   row-safe FSST is emitted; Strategy A remains the
   read path for `FsstWholeBuffer` buffers.
8. **Soft — mixed-workload writer delta.** Closes
   ≥ 30 % of the ADR 067 writer-overhead delta vs
   whole-buffer-FSST-only on the mixed warehouse /
   adversarial suite, **or** the artifact records
   explicitly why a smaller reduction is still
   promotion-worthy. The council expects the
   conservative baseline may reduce the ~140 %
   overhead without reaching ADR 057's ≤ 75 %
   target.

If gates pass, Phase 7 promotion is a single commit
that flips production `select_fsst_candidate` to
`FsstRowSafeEstimatorRouting::Estimator`; the
routing enum stays in the codebase for bench/test
comparison after promotion. ADR 067 §"Status" and
ADR 057 §"Where we are" are amended to record the
new writer-cost delta.

If gates fail, the artifact records the gap. Council
decides whether to revise V1 (adjust safety
multiplier, adjust sample shape, narrow eligibility)
or open an option-2 ADR (estimator-authorized
`FsstRowSafe`-only emission with separate false-
positive promotion evidence).

## Council decisions

Compact archival summary. Iteration history lives
in git on `plans/wip/` (the WIP files are kept until
the user clears them).

1. **Option 1, not option 2, for V1.** Estimator
   only declines obvious losers; emission still
   requires ADR 067's exact guard. Option 2
   (estimator-authorized `FsstRowSafe`-only emission)
   is deferred to a separate ADR predicated on Phase
   7 evidence. Settled iter 1; held across all
   subsequent iterations.
2. **Three decline mechanisms.** Cost-free metadata
   floor (pre-training) + trained metadata floor
   (post-training, pre-sample-encode) + sample
   confidence (post-sample-encode). The trained-
   floor arm oscillated three times across iter 3 /
   5 / 6 / 7 / 8 before settling 3-0 in iter 8 on
   Codex's "saves sample-encode CPU; exact lower
   bound, not a sampled guard amendment" reframing.
3. **Full-buffer symbol-table training only.**
   Sample-trained tables forbidden in V1 because they
   estimate a different codec than the writer would
   emit. Settled iter 3.
4. **Conservative `max` decision rule.** Single
   integer/rational `EARLY_DECLINE_SAFETY_MULTIPLIER`
   starting at 1.20×; per-guard threshold deferred
   to V2 with Phase 0 measuring would-have-saved
   evidence. Settled iter 3.
5. **`fsst_row_safe_metadata_floor_bytes` and
   `fsst_row_safe_metadata_bytes` return
   `Option<usize>`.** Overflow-safe; `None` routes
   to `RequireExactGuard`, never to `Decline`.
   Settled iter 3.
6. **Crate-private `FsstRowSafeEstimatorRouting`
   enum for staging.** No `EncodingWriterConfig`
   field, no operator-visible toggle. Phase 1-6
   uses test/bench helpers; Phase 7 promotion is a
   single production-call-site commit. Settled
   iter 4 (Codex iter 3 reasoning).
7. **Compression-byte equality oracle as separate
   hard gate.** Stronger than cascade-choice equality
   alone; catches accidental encoder divergence.
   Settled iter 4.
8. **Shorter Rust-idiomatic enum variant names.**
   `MetadataFloorExceedsPlaintext` etc., not
   `FsstRowSafeMetadataFloorExceedsPlaintext`. The
   family-first sub-rule applies between sibling
   types (the routing enum's *type* name keeps the
   `FsstRowSafeEstimator` prefix), not within a
   single enum where the family is in the enum
   name. Settled iter 8 3-0.
9. **`FsstRowSafeEstimatorRouting` enum type name.**
   Family-prefixed per Codex iter 8; aligned 3-0.
10. **No on-disk format change, no surface change.**
    Estimator is purely internal. Architecture
    guards prevent decline branches from emitting
    `FsstRowSafe` bytes. Settled iter 1.
11. **Promotion artifact records residual writer-
    cost delta vs ADR 057 ≤ 75 % as option-2
    motivation.** The conservative baseline is not
    obligated to fully close the delta. Settled
    iter 2.
12. **Naming-vocabulary prerequisite is a separate
    `feat:` commit.** `Fsst` → `FsstWholeBuffer`,
    `b"WFS1"`/`b"WFS2"` → `b"WBFS"`/`b"RSFS"`,
    `wfs1_*`/`wfs2_*` → `fsst_whole_buffer_*` /
    `fsst_row_safe_*`. ADR 068 implementation begins
    after that rename lands. Settled iter 5.
