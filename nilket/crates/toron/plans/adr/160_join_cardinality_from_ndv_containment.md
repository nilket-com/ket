# ADR 160: Join-Output Cardinality from NDV Containment

**Status:** Promoted. **3-of-3 on candidate A, reframed from the roadmap's "Theta from scratch" to the
NDV-containment baseline; the complete-cut shape confirmed on the verified origin-agnostic replay.** The
join arc's **baseline cardinality estimate**, reusing existing NDV (the 144 pattern, not a from-scratch
Theta sketch). Two halves in one complete Toron-local cut: (1) a join-output `estimated_row_count` from
System-R containment, and (2) the one consumer wire that makes it pay off — relaxing the broadcast
chooser to size a parent join's build side from a **composite estimated** input. Estimate-channel only,
139/140-backstopped; single-input estimates unchanged. No Wendelon change.

## Why complete, not dark — the grounding that set the shape

I first scoped this as a *dark* estimate (populate the join facts, 158-style, consumer deferred). Two
grounded facts moved it to a complete cut:

1. **The estimate would be inert.** `build_byte_candidate` (the broadcast chooser's build sizer, called
   at binder.rs:5337–5338) opens with `if facts.origin != PlanOrigin::SingleBaseRelation { return None }`.
   A nested join's output is `PlanOrigin::Composite`, so a populated join estimate is *rejected* — read
   by nothing. A dark estimate here is a fact in a struct field nobody consults: the too-granular,
   value-deferred shape ([[feedback_fold_adjacent_scope_not_granular_adrs]]). The topic surface is
   "2-join queries pick the right strategy"; completing it means wiring the consumer.

2. **The `SingleBaseRelation` gate is calibration, not a safety invariant — so relaxing it is safe
   (verified).** The over-cap→hash broadcast replay is **origin-agnostic** at all three of its joints:
   the *trigger* is a runtime `BudgetExhausted { BroadcastReplication }` byte-budget failure on the build
   side's actual replicated bytes (binder/lib runtime, lib.rs:634); the *eligibility* is keyed on
   `JoinStrategyIntent` (`broadcast_replay_eligibility:5285` — Auto/SqlHint = Replayable, Direct/pinned =
   Terminal); the *recovery* re-runs the join as hash on the replay id. None consult `PlanOrigin`. So a
   *wrong* containment estimate that over-broadcasts a join output is recovered by the exact same 139/140
   backstop that protects a wrong dimension-table broadcast (`estimated_broadcast_over_cap_replays_as_hash_and_matches_digest`,
   lib.rs:949, byte-identical digest). The gate kept broadcast to base dimensions out of conservatism.

So 160 lands the estimate **and** relaxes the gate *for the estimate channel only* — the hard/terminal
broadcast path (`BroadcastInferred`) stays `SingleBaseRelation`-only, because a derived cardinality is
never a hard bound.

## The estimate — System-R containment (prose for the humans)

For `L ⋈ R on L.x = R.y`: **`|L ⋈ R| ≈ |L|·|R| / max(ndv(L.x), ndv(R.y))`**. Assume the smaller key
domain is *contained* in the larger (the foreign-key shape); each of the `max(ndv)` distinct keys pairs
`|L|/ndv · |R|/ndv` rows. It is the **join analog of 144's NDV-uniform filter estimate** — same uniform
assumption, same honest imprecision, *same NDV facts reused*. `max` not `min`: the larger NDV is the more
selective (correct) denominator. Its known weakness — **disjoint or skew-correlated keys** (actual ≈ 0
or hot-key-dominated, estimate says `|L|·|R|/max`) — is named here as the future evidence target: a later
ADR measures that miss (the 154/157 pattern) and a Theta/MinHash key-overlap sketch fixes it (the 156/159
pattern). 160 is the baseline that makes the miss measurable.

## Decisions

**D1 — NDV-containment estimate in `bind_join`.** Before `new_with_facts` (binder.rs:1556), compute the
join-output `estimated_row_count` from the inputs' row estimates + join-key NDV and set it on composite
facts through a join-specific helper (`composite_with_join_estimate`). That helper calls
`estimate_materialized_byte_size` directly so the chooser can read a byte size when the output schema is
estimable. Do **not** globally relax `derive_estimated_byte_size`; it is intentionally single-base-only
today. If the estimate is unavailable, leave the join output unknown (fail-open).

**D2 — Estimate fires only when both inputs are clean base scans.** Require both
`PlanOrigin::SingleBaseRelation` and a root `ExecutionPlan::Scan` on each side. The existing
`column_ndv_for_input` helper recurses through `Filter`, which is useful for filter selectivity but
unsafe here: a filtered row estimate paired with stale base NDV can under-estimate join output. Add a
join-specific clean-base NDV lookup that matches root scans only and maps scan-projection output ordinals
back to descriptor ordinals before reading NDV. `Filter(Scan) ⋈ …` stays unknown until a later
filtered-NDV-decay cut. (This guards the **estimate's inputs**; D6 separately governs what the chooser
**consumes**.)

**D3 — Single equi-key first.** One pair → one `max(ndv_L, ndv_R)`. Multi-key (`AND`-ed pairs, product
of per-key selectivities) deferred — the single-key containment is the load-bearing proof.

**D4 — Estimate-channel only.** Populate `estimated_row_count`/`estimated_byte_size`; never
`row_upper_bound`/`byte_upper_bound`/`EmptyRelation` (containment is a heuristic, not a bound).
139/140-backstopped; `adr136` hard-field markers gain no join entry.

**D5 — Formula + guards.** `|L|,|R|` from each input's `estimate_row_source(row_upper_bound,
estimated_row_count)`; `estimated = ceil(|L|·|R| / max(ndv_L, ndv_R))`, with `u128` intermediates,
positive inputs floored at `≥1`, zero-row inputs allowed to estimate `0`, and a final `RowCount` clamp at
`u64::MAX` if needed. Missing input rows / NDV / `ndv == 0` → unknown (fail-open).

**D6 — Relax `build_byte_candidate` to consume a composite *estimated* build side (the consumer wire).**
Accept `PlanOrigin::Composite` via the **estimated** path (`estimated_byte_size` →
`BuildByteSource::Estimated` → `BroadcastEstimated`, replay-eligible); keep the **hard** path
(`byte_upper_bound` → `BroadcastInferred`, terminal) `SingleBaseRelation`-only. This makes a parent join
size its build side from a child join's containment estimate — the behavior change — while preserving the
hard/terminal-broadcast boundary. Backstopped by the verified origin-agnostic over-cap→hash replay.

## Headline gate (the behavior change, replay-backstopped)

`two_join_plan_broadcasts_small_inner_join_output_by_containment_without_replay`: `(orders ⋈ customers)
⋈ regions`, base scans with seeded `column_ndv`; high-NDV inner keys make `|orders ⋈ customers|` small →
the outer chooser reads the inner join's composite estimated byte size → **broadcasts it, `replayed ==
false`**, digest = forced-hash baseline. On `main` the inner join is `composite_unknown` →
`build_byte_candidate` returns `None` → outer join hash-by-default (no broadcast). Paired with
`overestimated_inner_join_broadcast_over_cap_replays_as_hash_and_matches_digest` — a *wrong* (disjoint-key)
containment over-broadcast recovers via 139/140 with byte-identical output (the safety proof for D6).

## Supporting gates

- `single_join_output_estimate_matches_containment_formula` (exact `|L|·|R|/max(ndv)` on seeded NDV);
- `join_cardinality_fails_open_without_ndv` (no NDV on a key → composite stays unknown → today's behavior);
- `join_cardinality_skips_filtered_input_ndv` (D2 — `Filter(Scan) ⋈ Scan` → unknown);
- `join_cardinality_maps_scan_projection_ndv` (scan-level projection remaps output ordinal to descriptor
  ordinal before reading NDV);
- `join_estimate_floored_at_one_and_capped_at_cross_product` (D5);
- `join_cardinality_is_estimate_channel_not_hard_bound` (D4);
- `composite_hard_bound_build_side_still_rejected` (D6 — only the *estimated* path opens; `BroadcastInferred`
  stays base-only);
- guards: `adr160_join_cardinality_is_estimate_channel_only`,
  `adr160_join_containment_requires_clean_base_ndv`,
  `adr160_composite_broadcast_is_estimate_channel_only` (no hard/terminal composite broadcast),
  `adr160_no_theta_minhash_or_join_intersection_fact`,
  `adr136` markers gain no join entry;
- 158/159 + 144/147 + 155/156 single-input regressions green (single-base broadcast unchanged).

## Consensus trail

iter-0: candidate **A**, reframed from the roadmap's "Theta from scratch" to **NDV-containment baseline**
(grounded: join cardinality is greenfield — `bind_join` emits `composite_unknown()` — and NDV facts
already exist, so it's the 144 reuse pattern, not the 156 new-sketch pattern). iter-1: grounded the seam
(keys are ordinals, the existing NDV reader is recursive over `Filter`, and the first cut needs a
clean-base NDV lookup) and flagged the open consumer-wire question. iter-2: grounded that the consumer wire is **gated shut**
(`build_byte_candidate` rejects composite) and reframed to a complete cut. **Final iteration confirmed the
load-bearing premise** — the over-cap→hash replay is origin-agnostic (trigger = byte budget, eligibility =
intent, recovery = force-hash; none consult `PlanOrigin`) — so the gate relaxation is replay-safe and the
complete cut stands (no 158-style dark fallback needed).

## Forward arc

`159 range selectivity live → **160 join cardinality from NDV containment (live for 2-join strategy)** →
measured disjoint/skew-key miss evidence → Theta/MinHash key-overlap producer → multi-join reordering`
· *(parallel: CONVERT/units; Rune cockpit — join cardinality is its next gauge)*.
