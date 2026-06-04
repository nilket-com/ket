# ADR 159: Range-Distribution Optimizer Fact Producer + Activation

**Status:** Promoted. **3-of-3 on candidate A (producer + activation); placement resolved Toron-home on
the operator's provenance principle; F1 resolved KLL on the grounded merge model.** A **single
cross-ring Toron-sequence ADR** — Wendelon does the producer labor, Toron owns the home because the
*purpose* is the optimizer (would Wendelon build a maintained quantile sketch if Toron didn't exist?
No → Toron-home; [[adr-placement-interior-exterior]]). It builds the **live** range-distribution
producer to **158's bucket/CDF contract** and flips 158's dark consumer on: a maintained **KLL** sketch
per opt-in column, merged across tiered slabs, queried into equal-mass buckets at the wire. It spends
158's proven consumer math; the binder/chooser/replay machinery is unchanged.

## Why KLL — the maintenance model forces it (the load-bearing rationale)

KLL is not a preference among quantile representations; it is the **only** one that survives Wendelon's
stats lifecycle. 158-D2 deliberately left "KLL vs equi-depth" open ("KLL … queried into buckets before
the wire, *if chosen*"); grounding the CSB merge path closes it — and reverses the naive lean.

The lifecycle (self-verified against `crates/wendelon/src`): column stats are built **seal-time-exact in
one pass** per slab (`collect_column_stats_in_one_pass`), then **merged sketch-to-sketch across
independently-sealed tiered slabs with no row re-read** at fact-production
(`ndv_fact_for_column` → `HllRegisters::merge`, register-wise max), and **fully recomputed from
survivors at compaction** (never merge inputs — ADR 039 §17, because MVCC retention can prune rows and
invalidate merged bounds).

The load-bearing consequence: **slabs seal independently, so at seal time a slab knows only its own
local min/max — never the table-global domain.** Therefore any *boundary-carrying* histogram cannot be
merged across slabs without re-reading rows, which the merge path forbids:

1. **Equi-depth** — each slab's buckets equalize *its own* mass, so boundaries disagree slab-to-slab;
   merging them exactly needs the underlying rows. Not mergeable.
2. **Equi-width** — needs a global bucket grid agreed *before* sealing, but there is no declared numeric
   domain to fix one a-priori. Not mergeable.
3. **KLL** — *boundary-free*: it stores sampled values with compactor weights and merges by
   concatenate-compact (exactly HLL's lifecycle). Buckets are derived **post-merge** by querying the
   global CDF. The only representation that survives the tiered-aggregation path.

So the intuition "equi-depth is the simpler producer" is backwards — equi-depth is the one that *breaks*
the merge model. **KLL is the maintained Wendelon-private aggregate-state; equi-depth-*shaped* buckets
(equal-mass, read off the merged CDF over the global min/max) are merely the wire output.**

**This also explains why 159 is approximate where 156 was exact** (158-D3 vs 156). Mergeability and
exactness trade off across independently-sealed slabs. Heavy-hitter (156) keeps *exactness* by
**declining** when a local sketch is incomplete (its `complete` flag → exact-or-omit). A full
distribution cannot decline its way to exactness across many slabs and still fire on real tables — so
range-distribution **buys mergeability with rank-error**. `rank_error_ppm` is the *price of being
mergeable*, not a hedge — and it is honestly carried on the fact.

**What this buys in safety.** Because the producer can always merge and is always estimate-channel,
partial coverage degrades to *absence*, not to a wrong half-table histogram — and absence lands exactly
on 158's proven dark fallback (min/max-uniform). Approximation can still miss, so the safety claim is an
estimate-safety boundary: `rank_error_ppm` labeling, estimate-channel-only spending, and replay-backed
recovery. Worst case is an honest selectivity miss, not wrong rows or a hard-bound violation.

## Decisions

**D1 — Single cross-ring Toron ADR; Wendelon producer + thin Toron activation.** Wendelon owns the KLL
maintenance, CSB encoding, merge, and coverage; Toron owns only flipping
`descriptor_optimizer_facts_from_storage` from emit-empty (158's dark) to map-the-buckets, plus the
end-to-end live proof. **No** new Toron selectivity math (158 owns the spend), hard bound,
`EmptyRelation`, EXPLAIN row, or Rune surface.

**D2 — KLL is the maintained sketch, mirroring HLL at every seam.** Reuse and extend the existing
`KllSketch` substrate in `storage/sketch.rs` beside `HllRegisters`: producer-facing value insertion,
`merge` (concatenate-compact), encode/decode, and post-merge bucket query APIs. Built at seal in
`collect_column_stats_in_one_pass`; encoded into the CSB payload like HLL. **KLL bytes never cross the
wire** — Wendelon-private aggregate-state exactly like HLL registers; only the queried bucket projection
rides `TABLE_OPTIMIZER_FACTS_GET` (the schema-owned contract, inverse of 156's wire key; upholds 158's
`adr158_kll_aggregate_state_not_toron_fact_contract` from the producer side).

**D3 — Buckets queried post-merge; equal-mass shape; fixed count.** `range_distribution_fact_for_column`
in `manager.rs` (mirror `ndv_fact_for_column`): merge KLL across contributors, then query **N equal-mass
quantiles** into `RangeDistributionBucket { lower, upper, count }` over the merged global min/max;
bounds in the existing optimizer scalar encoding (158's contract — **not**
`ScalarValue`/`ScalarLiteralKey`). `non_null_count` = merged sketch weight; `rank_error_ppm` derived
from the sketch parameter `k`. **Bucket count `N` is a fixed producer-side constant (default 64),
decoupled from `k`** — `k` governs rank-error and sketch memory, `N` governs wire size and consumer
interpolation granularity. **Honesty cap: `N · rank_error ≲ 1`** — never emit more buckets than the
merged sketch can resolve (false precision otherwise). At Int64 ~24 B/bucket, N=64 ≈ 1.5 KB/column on
the wire.

**D4 — Coverage = all-contributors-or-decline (the NDV/HLL pattern, no complete-flag).** KLL summarizes
*all* values in its slab (unlike heavy-hitter's bounded candidate set), so it needs no `complete` flag:
if any visible contributor lacks the KLL sketch for the column → `Ok(None)` (the `ndv_fact_for_column`
decline) → Toron sees the fact absent → **158's dark min/max-uniform fallback fires**. Honest partial
coverage = absence, never a half-table distribution. Gated at the `has_uncovered_local_rows_for_descriptor`
coverage site.

**D5 — Compaction recomputes (no merge-staleness).** KLL is rebuilt from survivors at compaction like
every other stat (ADR 039 §17) — no stale-merged-sketch correctness gap.

**D6 — Opt-in policy; existing `KllK`; brand-clean names.** Add
`range_distributions: Vec<ColumnRangeDistributionSpec>` to `ColumnStatsPolicyV1`, mirroring
`hll_distincts`/`heavy_hitters`. `ColumnRangeDistributionSpec { column_name, k: KllK }`, reusing the
existing `KllK` newtype and aggregate constants (`AGG_KLL_DEFAULT_K` currently 200,
`AGG_KLL_MIN_K..=AGG_KLL_MAX_K` currently `64..=2048`); expose associated constants only if useful for
policy ergonomics. **Default-empty** (dark stays dark until a column opts in). `rank_error_ppm` is derived
conservatively from the sketch implementation and `k`; do not hard-code a "1%" story into the policy.
The 157 miss is large enough that even coarse rank error should erase it, while the bucket-resolution cap
prevents false precision. **The new spec type is brand-clean — no `*V1`**
([[no-v1-suffix-in-type-names]]; extending the pre-existing `ColumnStatsPolicyV1` is fine, adding a new
`…V1` is not). Wire range-distribution into `apply_opt_in_stats_budget` as the **lowest-priority** opt-in
kind (appended last in the drop order `[SmallSet, Bloom, Hll, HeavyHitters, RangeDistribution]`) — the
new heavy stat yields footer bytes to the established stats under budget pressure.

**D7 — Int64 headline lane (157/158 parity).** KLL over the totally-ordered Int64 domain; Float64 folds
through shared scalar decode; Date32/Timestamp/civil-time deferred (158-D6 parity).

**D8 — Estimate-channel only.** Buckets feed `estimated_row_count`; never
`row_upper_bound`/`byte_upper_bound`/`EmptyRelation`. The new carrier fields are estimate-channel, so the
`adr136` hard-field marker list must **not** gain a range-distribution entry
([[cross-ring-field-inverts-adr136-guard]]) — verified, not assumed.

## Headline gate (the dark→live activation proof)

`produced_range_distribution_fact_makes_clustered_filter_hash_without_seeding` (mirrors 156's
`produced_heavy_hitter_fact_makes_hot_key_filter_hash_without_seeding`, `lib.rs:1293`): a
`skewed_join_fixture_with_produced_storage_range_distribution_fact` ingests 157's clustered rows
(`amount: Int64`, 9_000 in `[0,100]`, tail outside) through the **real producer** — opt-in policy on
`amount`, KLL built at seal, **across ≥2 independently-sealed slabs** (`force_seal_active_for_test`,
`manager.rs:1840`) so the merge path the whole decision rests on is exercised — with **no test-injected
fact**. `orders ⋈ Filter(customers, amount BETWEEN 0 AND 100)` estimates ~9_000 from live merged buckets
→ chooser picks hash, `replayed == false`, matches the forced-hash baseline. **Fails on `main`**
(producer emits empty). Closes the 157→158→159 arc.

## Supporting gates

- `kll_merges_across_two_slabs_into_global_buckets` — two independently-sealed slabs over disjoint local
  ranges → merged buckets span the global range (the boundary-free property, the F1 crux, pinned);
- `range_distribution_producer_declines_on_partial_coverage` — one contributor lacks the sketch → fact
  absent → 158's min/max-uniform fallback still holds under a live producer (darkness composes);
- `range_distribution_recomputes_exactly_after_compaction` (D5);
- `range_distribution_rank_error_ppm_reflects_k` (D3/D6 honesty);
- `range_distribution_bucket_count_respects_resolution_cap` (D3 — `N · rank_error ≲ 1`);
- `kll_bytes_never_cross_the_optimizer_facts_wire` (D2 — only buckets on the wire);
- `range_distribution_spec_names_are_brand_clean` (D6 — no new `*V1`);
- guards (Wendelon `tests/`): `adr159_kll_is_wendelon_private_not_on_wire`,
  `adr159_range_distribution_is_estimate_channel_only`, `adr159_coverage_declines_on_partial`,
  `adr159_range_distribution_spec_brand_clean`; **verify `adr136` hard-field markers gain no range
  entry**; retarget 158's `adr158_no_wendelon_range_distribution_producer_yet` → "producer owned by
  159";
- 158 consumer gates + 142/156 producer regressions + 144/147 stay green.

## Consensus trail

iter-0: 3-of-3 candidate **A** (producer + activation) as 159. iter-0→1 **placement** resolved
Toron-home on the operator's provenance principle ("would Wendelon build this if Toron didn't exist? No
→ Toron-home"), overruling my own producer/consumer-split lean and Codex's full-relocate; Gemini reached
Toron-home via precedent ([[adr-placement-interior-exterior]] now records the principle behind the
precedent). iter-1 **F1** resolved **KLL** on the grounded merge model (boundary-free is the only
mergeable option across independently-sealed slabs) — I self-verified `HllRegisters::merge` +
`ndv_fact_for_column`'s tiered-merge/decline path, reversing my own iter-0 equi-depth lean
([[feedback_concession_cycle_ground_not_headcount]] — concede to grounding). iter-2 grounded the `k`
policy against the existing KLL constants + `apply_opt_in_stats_budget` drop-order. Final iteration
closed bucket-count (fixed N=64, resolution-capped) and the headline fixture (live produced-storage
fixture, mirror 156's, not the Toron-direct injector).

## Forward arc

`157 range-skew evidence → 158 dark range-distribution consumer → **159 Wendelon KLL producer +
activation (range selectivity LIVE)** → Theta join-key intersection → join selectivity → reordering` ·
*(parallel: CONVERT/units; Rune optimizer cockpit, brainstorm 010 — `rank_error_ppm` is the first gauge
it surfaces).*
