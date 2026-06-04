# Impl Plan — ADR 158: Range-Distribution Selectivity Consumer

Pairs with `plans/adr/158_range_distribution_selectivity_consumer.md`. **Toron-only consumer cut,
dark** (141/155 pattern): a new bucket/CDF fact carrier, the in-envelope range spend, fixture-seeded
gates. Descriptor sync emits empty; the Wendelon producer is **159**. No Wendelon/wire change.
Mirrors the ADR 155 consumer-dark structure. All anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Range estimate (refine here) | `binder.rs` `filter_range_selectivity_estimated_row_count:4215`, `estimated_rows_for_range:4559`, `estimated_rows_for_integral_range:4587`, `_real_range:4612` |
| Range fact reader (mirror) | `column_range_for_input:4546` (Scan + `Filter` recursion) |
| Carrier (mirror) | `TableOptimizerFacts.column_range`/`column_heavy_hitters` (`catalog.rs`); `DescriptorOptimizerFacts`; `for_descriptor`; `descriptor_optimizer_facts_from_storage` (dark emit) |
| Scan carrier | `Scan { column_range: Vec<(ColumnOrdinal, RangeFact)>, column_heavy_hitters: Vec<…>, .. }` (`ir/execution.rs:240`) |
| Projection remap | `optimize_for_required` — `ordinal_remap` + `column_ndv`/`column_range`/`column_heavy_hitters` rewrite (`binder.rs:~6098–6133`) |
| Scalar encode (reuse) | the optimizer-fact min/max/range scalar encoders (bucket bounds; **not** `ScalarValue`) |
| 157 fixture | `lib.rs::tests::skew_evidence` clustered-range fixture + replay-signal runner |
| Guards | new `crates/toron/tests/architecture/range_distribution.rs` |

## Phase 1 — Bucket/CDF fact (catalog.rs) + carrier (mirror `column_range`, dark)

- `ColumnRangeDistributionFact { arrow_type_tag: u8, non_null_count: u64, rank_error_ppm: u32,
  buckets: Vec<RangeDistributionBucket> }`; `RangeDistributionBucket { lower: Vec<u8>, upper:
  Vec<u8>, count: u64 }`. **No new range-distribution `*V1` type names.** Bucket bounds =
  existing optimizer scalar encoding.
- Validate at descriptor mapping and again before spend: buckets sorted, non-overlapping,
  `arrow_type_tag`-compatible, decode cleanly; else **drop/ignore the fact** (fail-open).
- `TableOptimizerFacts.column_range_distributions: BTreeMap<ColumnOrdinal, ColumnRangeDistributionFact>`;
  `DescriptorOptimizerFacts.column_range_distributions: Vec<…>` `#[serde(default)]`;
  `descriptor_column_range_distributions` (mirror `descriptor_column_range`);
  **`descriptor_optimizer_facts_from_storage` emits empty** (dark — 159 fills it). Update all facts
  initializers (`..Default::default()`).

## Phase 2 — Scan carrier + reader + projection remap

- `ir/execution.rs`: `Scan.column_range_distributions: Vec<(ColumnOrdinal, ColumnRangeDistributionFact)>`
  (beside `column_range`). **New Scan field → E0063 at every construction site → `--all-targets`.**
- `binder.rs`: populate in `scan_plan`; add `column_range_distribution_for_input` (mirror
  `column_range_for_input:4546`).
- **`optimize_for_required`: remap `column_range_distributions` via `ordinal_remap`** like the
  sibling facts (the 155 catch). Pin `range_distribution_fact_projection_remaps_with_scan_projection`.

## Phase 3 — Consumer spend (the heart, D4)

In the range path (`estimated_rows_for_integral_range`/`_real_range`, reached from
`filter_range_selectivity_estimated_row_count`):
1. envelope decode + clip from `RangeFact` first (unchanged);
2. interval ∩ envelope empty → `Some(0)` (unchanged — min/max owns zero-out);
3. if `column_range_distribution_for_input(...)` present & compatible (`arrow_type_tag` matches the
   RangeFact; buckets decode/sorted/non-overlapping):
   `estimate = Σ count(buckets fully inside [low,high]) + Σ partial-overlap(bucket) × (overlap /
   bucket_width)`, floored ≥1 on non-empty overlap, capped at `non_null_count`;
4. else → existing uniform (`non_null_count × overlap/domain`).
**Helper** `range_distribution_estimate(fact, low, high, inclusivity)`; **checked integer math**;
empty/zero-bucket → fail-open to uniform (never div-by-zero, never `Some(0)` from an absent fact);
never `EmptyRelation`.

## Phase 4 — Fixture + gates

- Test helper: extend 157's clustered-range catalog to seed `column_range_distributions` for `amount`
  (bucket `[0,100] => 9_000`, tail bucket `[101,10_000] => 1_000`).
- Binder tests: **`absent_range_distribution_falls_back_to_min_max_uniform`** — assert
  `estimated_row_count == RowCount::new(101)` *exactly* when the fact is absent (the darkness proof,
  not just "no crash"); `distribution_refines_in_envelope_min_max_still_zeroes_out_of_range`;
  `malformed_or_mismatched_range_distribution_fails_open`;
  `range_distribution_estimate_stays_estimate_channel_not_hard_bound`;
  `range_distribution_rank_error_is_visible_on_fact`;
  `range_distribution_fact_projection_remaps_with_scan_projection`.
- **Headline** (`lib.rs::skew_evidence`, reuse the replay-signal runner): seed the bucket fact →
  `range_distribution_fact_makes_clustered_range_filter_hash_without_replay` — `amount BETWEEN 0 AND
  100` estimates ~9_000 → chooser selects hash, bind reason **not** `BroadcastEstimated`, run under
  157's tight cap, `replayed == false`, digest = forced-hash.
- Keep 157 + 144/147 + 155/156 regressions green.

## Phase 5 — Guards (new tests/architecture/range_distribution.rs, adr158_*)

- `adr158_range_distribution_consumer_is_dark_toron_only` — no Wendelon producer/CSB flag/wire field;
  `descriptor_optimizer_facts_from_storage` emits empty.
- `adr158_no_wendelon_range_distribution_producer_yet`.
- `adr158_distribution_refines_in_envelope_only` — min/max still owns zero-out / envelope.
- `adr158_range_distribution_is_estimate_channel_only` — only `estimated_row_count`; no hard
  bound/`EmptyRelation`.
- `adr158_range_distribution_names_are_brand_clean` — no new range-distribution `*V1` type names.
- `adr158_kll_aggregate_state_not_toron_fact_contract` — carrier is bucket/CDF; no `KllSketch`/
  sketch-bytes type on the Toron fact.
- `adr158_public_explain_surface_stays_closed` — `optimizer_explain_schema` unchanged.
- retarget 157's `adr157_no_live_range_distribution_optimizer_fact_or_producer` → consumer-owned-by-158,
  producer-still-absent. **Land in `range_distribution.rs`**, not `optimizer.rs`.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets   # MANDATORY — new Scan/facts field ⇒ E0063
scripts/cargo-cgroup test -p toron range_distribution
scripts/cargo-cgroup test -p toron range_distribution_fact_makes_clustered_range_filter
scripts/cargo-cgroup test -p toron absent_range_distribution_falls_back_to_min_max_uniform
scripts/cargo-cgroup test -p toron --test architecture_test adr158
scripts/cargo-cgroup test -p toron --test architecture_test   # FULL binary (expect only adr077/adr031)
scripts/cargo-cgroup test -p toron --test architecture_test adr144   # range selectivity intact when absent
git diff --check
```

## Risks

- **Not actually dark** — the absent path must produce the *exact* uniform estimate (`== 101` in the
  gate), not a degenerate `Some(0)`/floor/panic; `descriptor_optimizer_facts_from_storage` must emit
  empty. The `absent_..._falls_back_to_min_max_uniform` gate is the darkness proof.
- **Min/max displaced** — distribution must refine *in-envelope only*; out-of-range still `0` from
  `RangeFact`.
- **Not fail-open** — malformed / type-mismatched / empty buckets → uniform fallback, never panic /
  div-by-zero / mis-estimate.
- **KLL leak** — the Toron fact is bucket/CDF; no `KllSketch`/sketch-bytes on the carrier (159 owns
  the producer algorithm choice). Guard it.
- **E0063 breadth** — new `Scan`/facts field hits every initializer; `--all-targets`.
- **Projection remap miss** — remap `column_range_distributions` in `optimize_for_required` or
  post-pushdown ordinals go stale (the 155 lesson). Pinned by the remap gate.

## Deferred (named)

The Wendelon range-distribution **producer** (ADR 159 — maintained KLL/equi-depth CSB payload +
opt-in policy + compaction merge + wire + descriptor sync; grounds KLL-vs-equi-depth; **brand-clean
names even when extending `ColumnStatsPolicyV1`**); Date32/Timestamp range distributions (numeric
first); multi-column/correlated; the future Rune cockpit (brainstorm 010); adr077/adr031 hygiene
(separate commit + full-architecture-binary review checklist).
