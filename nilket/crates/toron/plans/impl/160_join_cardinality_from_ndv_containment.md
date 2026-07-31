# Impl Plan — ADR 160: Join-Output Cardinality from NDV Containment

Pairs with `plans/adr/160_join_cardinality_from_ndv_containment.md`. A **complete Toron-local cut** (no
Wendelon change): a join-output cardinality estimate from existing NDV + the one estimate-channel
consumer wire that makes it live for 2-join strategy. Reuses existing NDV facts and the broadcast chooser.
Anchors verified on `main`; the load-bearing replay-origin premise verified (Phase 0 below).

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Join binding (estimate seam) | `bind_join` — `binder.rs:1426`; ends `new_with_facts(.., composite_unknown())` at `:1556`/`:1559` |
| Equi-keys as ordinals | `left_keys`/`right_keys: Vec<ColumnOrdinal>` — `binder.rs:1437–1447` (`bind_join_key_pair`) |
| NDV reader (existing, not reused directly) | `column_ndv_for_input(input, ordinal)` — `binder.rs:4097` (recurses `Filter`, so ADR 160 needs a clean-base join helper) |
| Filter-NDV guard (mirror D2) | `filter_equality_ndv_estimated_row_count` `origin != SingleBaseRelation → None` — `binder.rs:3962` |
| Estimate plumbing | `with_estimated_row_count:290`, `derive_estimated_byte_size:318` (single-base-only), `estimate_materialized_byte_size:365`, `estimate_row_source:343` |
| Facts origin | `PlanOrigin::{SingleBaseRelation, Composite}` `:201`; `composite_unknown:219`, `composite_with_row:229` |
| **Consumer wire (relax target)** | `build_byte_candidate` — gate `if facts.origin != SingleBaseRelation { return None }`, called `binder.rs:5337–5338`; `BuildByteSource::{Hard,Estimated}`; `BroadcastEstimated` reason `:5379` |
| Chooser | `choose_auto_join_strategy:5324`; `JoinInputOptimization { facts, probe_scan_linear }:490`; `left_plan.facts` fed at `:1469` |
| Replay safety (D6 proof) | trigger `BudgetExhausted { BroadcastReplication }` `lib.rs:634`; eligibility `broadcast_replay_eligibility:5285` (Auto = Replayable); `estimated_broadcast_over_cap_replays_as_hash_and_matches_digest` `lib.rs:949` |
| Headline harness | a 2-join plan + `run_distributed_join_query_..._replay_signal` runner (mirror 158/159 headlines) |

## Phase 0 — Premise verified (origin-agnostic replay)

**Confirmed** the over-cap→hash broadcast replay does not consult `PlanOrigin` at any joint: the *trigger*
is a runtime `BudgetExhausted { BroadcastReplication }` byte-budget failure (`lib.rs:634`), the
*eligibility* is keyed on `JoinStrategyIntent` (`broadcast_replay_eligibility:5285`; Auto/SqlHint =
Replayable, Direct/pinned = Terminal), and the *recovery* re-runs the join as hash on the replay id. So
relaxing `build_byte_candidate` for composite **estimated** build sides is replay-safe. The complete cut
stands (no dark-estimate fallback).

## Phase 1 — The containment estimate and composite facts (D1, D2, D3, D5)

- Add `PlanOptimizerFacts::composite_with_join_estimate(estimated_row_count: Option<RowCount>,
  output_schema: &Schema) -> Self`:
  - `origin = PlanOrigin::Composite`;
  - `estimated_row_count = estimated_row_count`;
  - `estimated_byte_size = estimated_row_count.and_then(|row| estimate_materialized_byte_size(row, output_schema))`;
  - `row_upper_bound = None`;
  - `byte_upper_bound = None`.
  Do not use `derive_estimated_byte_size` here unless that helper is deliberately refactored; today it
  returns early for non-`SingleBaseRelation`, and broadening it globally would make unrelated composites
  more broadcastable.
- Add clean-base helper
  `clean_base_column_ndv_for_join_input(input: &ExecutionPlan, requested_output_column: ColumnOrdinal)
  -> Option<DistinctCountFact>`:
  - match only root `ExecutionPlan::Scan`;
  - reject `Filter`, `Project`, `Aggregate`, `Take`, `Exchange`, `Join`, and other roots;
  - for `projection: None`, look up `requested_output_column` directly in scan `column_ndv`;
  - for `projection: Some(columns)`, map the output ordinal to the descriptor ordinal
    (`columns.get(requested_output_column.0 as usize)?`) before looking up `column_ndv`.
  Do not call `column_ndv_for_input` from this helper; that helper recurses through `Filter`.
- Helper `join_output_ndv_containment_estimate(left: &ScopedExecutionPlan, right: &ScopedExecutionPlan,
  left_key: ColumnOrdinal, right_key: ColumnOrdinal) -> Option<RowCount>`:
  - `left.facts.origin == SingleBaseRelation && right.facts.origin == SingleBaseRelation` else `None` (D2);
  - `l = estimate_row_source(left.facts.row_upper_bound, left.facts.estimated_row_count)?`; same for `r`;
  - `ndv_l = clean_base_column_ndv_for_join_input(&left.plan, left_key)?`;
    `ndv_r = clean_base_column_ndv_for_join_input(&right.plan, right_key)?`;
  - `denom = max(ndv_l, ndv_r)`; `denom != 0` else `None`;
  - `est = (l.get() as u128 * r.get() as u128).div_ceil(denom as u128)`; zero-row inputs may produce
    `0`, positive inputs floor at `≥1`, and the denominator naturally caps the estimate at `l*r`;
    `RowCount::new(u64::try_from(est).unwrap_or(u64::MAX))`. **Checked math throughout.**
- In `bind_join`, take `left_keys[0]`/`right_keys[0]` only when there is exactly one key pair (D3);
  pass the resulting estimate to `PlanOptimizerFacts::composite_with_join_estimate(est, &declared_output_schema)`.
  Replace the `new_with_facts(.., composite_unknown())` at `:1556`.

## Phase 2 — The consumer wire (D6)

- `build_byte_candidate`: keep the **hard** path `SingleBaseRelation`-only, but allow the **estimated**
  path for `Composite`:
  ```
  if let Some(hard) = facts.byte_upper_bound {            // hard / BroadcastInferred (terminal)
      if facts.origin == SingleBaseRelation {
          return (hard < threshold).then_some(Hard candidate);
      }
      // composite hard bound: no hard candidate (fall through)
  }
  if matches!(
      facts.origin,
      PlanOrigin::SingleBaseRelation | PlanOrigin::Composite
  ) {
      facts.estimated_byte_size                            // estimated / BroadcastEstimated (replayable)
          .filter(|b| *b < threshold)                      //   allowed for SingleBaseRelation OR Composite
          .map(|b| Estimated candidate)
  } else {
      None
  }
  ```
  Net: composite build sides become broadcast-eligible **only** through the replay-backed estimated path;
  composite hard bounds never broadcast.
- No change to `choose_auto_join_strategy` (it already reads `input.facts` via `build_byte_candidate`) or
  to `JoinInputOptimization` (it already carries `left_plan.facts`/`right_plan.facts`).

## Phase 3 — Fixtures + gates

- Headline `two_join_plan_broadcasts_small_inner_join_output_by_containment_without_replay`: `(orders ⋈
  customers) ⋈ regions`, base scans + seeded `column_ndv` (high-NDV inner keys → small `|orders⋈customers|`
  under threshold) → outer join broadcasts the inner output, `replayed == false`, digest == forced-hash
  baseline. Fails on `main` (inner = `composite_unknown` → no broadcast).
- Safety pair `overestimated_inner_join_broadcast_over_cap_replays_as_hash_and_matches_digest`: seed
  disjoint-ish NDV so containment over-broadcasts; tight broadcast budget → over-cap → replay as hash,
  byte-identical (the D6 backstop proof; mirror `lib.rs:949`).
- `single_join_output_estimate_matches_containment_formula`; `join_cardinality_fails_open_without_ndv`;
  `join_cardinality_skips_filtered_input_ndv`; `join_cardinality_maps_scan_projection_ndv`;
  `join_estimate_floored_at_one_and_capped_at_cross_product`; `join_cardinality_is_estimate_channel_not_hard_bound`;
  `composite_hard_bound_build_side_still_rejected`.
- Keep 158/159 + 144/147 + 155/156 single-input regressions green.

## Phase 4 — Guards

- `adr160_join_cardinality_is_estimate_channel_only` — join output sets only `estimated_*`; no
  `row/byte_upper_bound`/`EmptyRelation`.
- `adr160_join_containment_requires_clean_base_ndv` — estimate uses the clean-base helper, matches only
  root `ExecutionPlan::Scan`, and does not recurse through `ExecutionPlan::Filter`.
- `adr160_composite_broadcast_is_estimate_channel_only` — composite build side broadcasts only via the
  estimated/replayable path; `BroadcastInferred` stays `SingleBaseRelation`.
- `adr160_no_theta_minhash_or_join_intersection_fact` — no set-sketch or join-intersection fact yet.
- `adr136` markers gain no join entry (estimate-channel boundary).

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron two_join_plan_broadcasts_small_inner_join_output_by_containment
scripts/cargo-cgroup test -p toron overestimated_inner_join_broadcast_over_cap_replays_as_hash
scripts/cargo-cgroup test -p toron join_cardinality
scripts/cargo-cgroup test -p toron --test architecture_test adr160
scripts/cargo-cgroup test -p toron --test architecture_test          # FULL binary (expect 0 failed)
scripts/cargo-cgroup test -p toron --test architecture_test adr136   # markers unchanged for join
git diff --check
```

## Risks

- **Crossing the broadcast boundary** — only the *estimated* (replayable) path opens for composite; the
  hard/terminal path stays base-only. Pinned by `composite_hard_bound_build_side_still_rejected`.
- **Stale NDV over a Filter** — estimate fires only on root clean scans, not recursive filter inputs
  (D2); pinned by `join_cardinality_skips_filtered_input_ndv`.
- **Disjoint-key over-estimate** — containment's known miss; estimate-channel + 139/140 replay recover it
  (pinned by the safety-pair test). Named as the future Theta/MinHash evidence target, not fixed here.
- **Doubly-derived byte size** — composite estimated_byte_size is containment→rows→bytes; tolerable in the
  estimate channel (recoverable), and bounded by the cross-product cap.
- **E0063 breadth** — the cut only touches `bind_join` + `build_byte_candidate` (no new struct field), so
  no construction-site ripple; `--all-targets` for completeness.

## Deferred (named)

Multi-key equi-joins (product-of-selectivities, D3); filtered-input NDV decay (so `Filter(Scan)⋈…`
estimates); 3+-join output estimates (the outer join's own output, needs composite-input NDV);
measured disjoint/skew-key miss evidence → Theta/MinHash key-overlap producer; multi-join reordering;
the Rune cockpit join-cardinality gauge.
