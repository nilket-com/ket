# Impl Plan — ADR 159: Range-Distribution Optimizer Fact Producer + Activation

Pairs with `plans/adr/0159_range_distribution_optimizer_fact_producer.md`. **Single cross-ring Toron ADR**
(Wendelon producer labor, Toron home + activation). Builds the live producer to 158's bucket/CDF
contract and flips the dark consumer on. **Every producer seam has an HLL sibling to mirror** — that is
the de-risking spine: KLL slots into the exact lifecycle HLL already occupies (build-at-seal → CSB
encode → tiered sketch-merge → coverage decline → compaction recompute → opt-in policy → budget).
Anchors verified on `main` (self-checked the merge crux + the live-producer test harness).

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Maintained sketch (mirror) | `HllRegisters` + `merge` (register-max) — `crates/wendelon/src/storage/sketch.rs:63`; existing `KllSketch` substrate — `sketch.rs:150` |
| Build at seal (mirror) | `collect_column_stats_in_one_pass` — `stats.rs:1227`; value insert mirrors `hll_insert_encoded_value:1760` |
| Policy lookup at build | `build_column_stats_block_internal` — `stats.rs:653`+ (`hll_spec` find at `:693`+) |
| CSB encode/decode (mirror) | HLL encode `stats.rs:1718` / decode `:2284` |
| Budget drop-order | `apply_opt_in_stats_budget` — `stats.rs:1061` (kind-order `[SmallSet, Bloom, Hll, HeavyHitters]`, greedy keep-until-full); `maybe_select_opt_in_stat:1100`, `OptInStatKind` arms `:1106`/`:1122` |
| Tiered merge → fact (mirror) | `ndv_fact_for_column` — `manager.rs:695` (decode each block, `merged.merge(&registers):748`, decline `Ok(None):711` on missing contributor) |
| Facts production entry | `table_optimizer_facts` — `manager.rs:5207`; coverage `has_uncovered_local_rows_for_descriptor:5216`/`:5275` |
| Opt-in policy + accuracy newtype | `ColumnStatsPolicyV1` `schema/table.rs:1332`; `ColumnHllSpecV1 { precision: HllPrecision }` `:1366`; existing `KllK` unit `schema/units.rs:316`; `AGG_KLL_DEFAULT_K = 200`, bounds `64..=2048` in `schema/aggregate.rs:23-25` |
| Multi-slab seal primitive (test) | `force_seal_active_for_test` — `manager.rs:1840` |
| Live-producer headline template | `produced_heavy_hitter_fact_makes_hot_key_filter_hash_without_seeding` — `lib.rs:1293`; fixture `skewed_join_fixture_with_produced_storage_heavy_hitter_fact`; runner `run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal` |
| Toron activation (flip dark) | `descriptor_optimizer_facts_from_storage` (emit-empty per 158) → map buckets into `DescriptorOptimizerFacts.column_range_distributions` |
| Consumer carrier (158, reuse) | `ColumnRangeDistributionFact`, `RangeDistributionBucket`, `Scan.column_range_distributions`, the in-envelope spend |

## Phase 1 — Extend `KllSketch` (storage/sketch.rs, beside `HllRegisters`)

- Reuse the existing `KllSketch`/`KllK` substrate; add the producer-facing APIs that are missing:
  value insertion from encoded column bytes, `merge(&mut self, other: &Self) -> io::Result<()>`
  (concatenate-compact; `k` mismatch → `InvalidData`, mirror `HllRegisters::merge:64-68`),
  encode/decode for CSB payloads, and `query_buckets(n) -> Vec<RangeDistributionBucket>` (N equal-mass
  quantiles over global min/max, bounds via the optimizer scalar encoders).
- Reuse `AGG_KLL_DEFAULT_K` (currently 200) and the existing KLL bounds (`64..=2048`); optionally expose
  associated constants on `KllK` for policy ergonomics. `const RANGE_DISTRIBUTION_BUCKET_COUNT: usize =
  64` (D3), with a debug-assert / guard that `N · rank_error ≲ 1`.
- Unit tests: merge associativity/commutativity over independently-built sketches; rank-error within
  the implementation's conservative bound on a known distribution; `query_buckets` monotonic
  non-overlapping bounds.

## Phase 2 — Build at seal + policy + budget

- `ColumnRangeDistributionSpec { column_name, k: KllK }` on `ColumnStatsPolicyV1` (`table.rs:1332`),
  `#[serde(default)]`, default-empty. **Brand-clean — no `*V1` on the new spec.**
- In `build_column_stats_block_internal:653`+, look up the range-distribution spec per field (mirror the
  `hll_spec` find at `:693`+); in `collect_column_stats_in_one_pass:1227` insert each non-null encoded
  value into the column's `KllSketch`.
- Encode KLL into the CSB payload (mirror HLL `:1718`); decode path (mirror `:2284`).
- `apply_opt_in_stats_budget:1071`: extend the kind-order to
  `[SmallSet, Bloom, Hll, HeavyHitters, RangeDistribution]` (lowest priority — dropped first); add
  `OptInStatKind::RangeDistribution` + the `select_range_distribution`/`payload_len` arms
  (`stats.rs:1106`+, `:1122`+).

## Phase 3 — Tiered merge → fact (the producer heart)

- `range_distribution_fact_for_column(candidate, blocks) -> io::Result<Option<ColumnRangeDistributionFact>>`
  (mirror `ndv_fact_for_column:695`): for each block, find the column entry or **`return Ok(None)`**
  (all-contributors decline, the `:711` pattern); decode its KLL; `merged.merge(&kll)`. After the loop,
  `query_buckets(RANGE_DISTRIBUTION_BUCKET_COUNT)` on the merged sketch → `ColumnRangeDistributionFact {
  arrow_type_tag, non_null_count (merged weight), rank_error_ppm (from k), buckets }`.
- Wire into `table_optimizer_facts:5207` alongside the NDV/heavy-hitter producers; respect the
  `has_uncovered_local_rows_for_descriptor:5216` coverage gate (absent ⇒ whole fact omitted).
- KLL bytes stay in the CSB; only the bucket fact enters the `TABLE_OPTIMIZER_FACTS_GET` response.

## Phase 4 — Toron activation (flip dark → live)

- `descriptor_optimizer_facts_from_storage`: **stop emitting empty** for range distributions (158's dark
  emit) and map the producer's buckets into `DescriptorOptimizerFacts.column_range_distributions`. This
  is the single behavior-change moment 158 reserved (158 "why dark" point 5).
- No other Toron change — 158 already owns the carrier, projection remap, reader, and spend.

## Phase 5 — Fixture + gates

- `skewed_join_fixture_with_produced_storage_range_distribution_fact` (mirror
  `skewed_join_fixture_with_produced_storage_heavy_hitter_fact`): ingest 157's clustered rows through the
  **real producer** across **≥2 sealed slabs** (`force_seal_active_for_test:1840`) with an opt-in policy
  on `amount` — *no* test-injected `ColumnRangeDistributionFact`.
- **Headline** `produced_range_distribution_fact_makes_clustered_filter_hash_without_seeding` (mirror
  `lib.rs:1293`): via the replay-signal runner, estimate ~9_000 from live merged buckets → hash, bind
  reason **not** `BroadcastEstimated`, `replayed == false`, matches the forced-hash baseline. Fails on
  `main`.
- `kll_merges_across_two_slabs_into_global_buckets`; `range_distribution_producer_declines_on_partial_coverage`
  (→ 158 min/max-uniform fallback); `range_distribution_recomputes_exactly_after_compaction`;
  `range_distribution_rank_error_ppm_reflects_k`; `range_distribution_bucket_count_respects_resolution_cap`;
  `kll_bytes_never_cross_the_optimizer_facts_wire`.
- Keep 158 + 142/156 + 144/147 regressions green.

## Phase 6 — Guards (new crates/wendelon/tests/ adr159 module)

- `adr159_kll_is_wendelon_private_not_on_wire` — no `KllSketch`/sketch-bytes type on the
  `TABLE_OPTIMIZER_FACTS_GET` response / Toron fact; only buckets.
- `adr159_range_distribution_is_estimate_channel_only` — producer emits only the bucket fact; no hard
  bound / `EmptyRelation`.
- `adr159_coverage_declines_on_partial` — partial coverage ⇒ fact omitted.
- `adr159_range_distribution_spec_brand_clean` — no new `*V1` type names.
- **`adr136` cross-ring marker check** — the hard-field marker list gains **no** range-distribution
  entry (the fields are estimate-channel) ([[cross-ring-field-inverts-adr136-guard]]).
- Retarget 158's `adr158_no_wendelon_range_distribution_producer_yet` → "producer owned by 159."
- Land producer guards in `crates/wendelon/tests/`; the Toron-side activation reuses 158's existing
  `tests/architecture/range_distribution.rs` guards.

## Verification

```sh
scripts/cargo-cgroup check -p wendelon --all-targets         # policy field + CSB wiring ⇒ E0063 sweep
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p wendelon range_distribution
scripts/cargo-cgroup test -p wendelon kll
scripts/cargo-cgroup test -p toron produced_range_distribution_fact_makes_clustered_filter
scripts/cargo-cgroup test -p toron  --test architecture_test            # FULL binary must stay green
scripts/cargo-cgroup test -p toron  --test architecture_test adr136     # marker list unchanged for range
scripts/cargo-cgroup test -p toron  --test architecture_test adr158     # consumer guards + retarget green
git diff --check
```

## Risks

- **Merge model misread** — the entire KLL choice rests on tiered slabs merging sketch-to-sketch with no
  row re-read. Self-verified at `sketch.rs:63` + `manager.rs:695-754`, but the headline **must** seal
  ≥2 slabs so the merge path actually runs (a single-slab proof passes for the wrong reason,
  [[adr_118_cost_bound_scope_projected_path]]).
- **Boundaries leak into the maintained state** — keep KLL boundary-free; buckets derived only
  *post-merge*. Any per-slab bucketing reintroduces the unmergeable equi-depth/equi-width trap.
- **KLL bytes on the wire** — Wendelon-private like HLL registers; guard it (D2).
- **Budget starves incumbents** — range-distribution is lowest-priority in `apply_opt_in_stats_budget`;
  verify it never displaces SmallSet/Bloom/Hll/HeavyHitters under a tight `max_ciphertext_bytes`.
- **Not estimate-channel** — buckets feed `estimated_row_count` only; the `adr136` marker check guards
  the field-name boundary.
- **False bucket precision** — `N · rank_error ≲ 1`; do not emit 64 buckets from a sketch that resolves
  fewer. Pinned by `range_distribution_bucket_count_respects_resolution_cap`.
- **E0063 breadth** — new `ColumnStatsPolicyV1` field + sketch payload wiring hits Wendelon initializers;
  `--all-targets` on **both** crates.

## Deferred (named)

Date32/Timestamp range distributions (numeric first); multi-column/correlated distributions; the Rune
optimizer cockpit reading `rank_error_ppm` (brainstorm 010); KLL `k` auto-tuning from observed
selectivity-miss feedback; keep the full `architecture_test` binary in the review checklist.
