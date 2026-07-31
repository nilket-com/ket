# ADR 158: Range-Distribution Selectivity Consumer

**Status:** Promoted. **3-of-3 on the dark consumer + consumer-first split; the bucket/CDF
contract promoted from the grounded Claude+Codex position (Gemini retained a `Kll{bytes}` enum
variant — a cross-ring leak, resolved on grounding).** A **Toron-only consumer cut, built dark**
(the 141/155 pattern): it adds the carrier, projection remap, binder lookup, and estimate-channel
spend to refine range selectivity *when a distribution fact is present*, fixture-tested now, inert
in production until the Wendelon producer (ADR 159). It spends ADR 157's measured evidence; the
binder/chooser/replay machinery is otherwise unchanged.

## Why dark — the load-bearing rationale

Dark is not a hedge; it is the smallest behavior-correct cut that preserves the ring boundary and
keeps the producer honest. "Dark" means a precise, testable property: **with no producer (the fact
absent in production), the consumer's new branch is never taken and range selectivity is
estimate- and decision-equivalent to 144/147's min/max-uniform path.**

1. **157 proved the consumer *value*, not the producer *mechanism*.** The measured miss says a
   distribution fact would materially improve a real join decision; it does not say which maintained
   Wendelon payload should exist, how it merges across contributors, or how its rank-error contract
   is encoded.
2. **The producer is real Wendelon work (159 scale).** `KllSketch` is aggregate-state, not a
   maintained CSB payload; the CSB has min/max/HLL/small-set/heavy-hitter but **no** range-
   distribution flag, parser slot, policy spec, compaction merge, or table-optimizer-facts emission.
   The 155/156 split is the precedent (144 was different — min/max was already maintained).
3. **It proves the *hard* part — the selectivity math — cheaply, with zero storage work.** Seeding a
   bucket fact in the test catalog proves the in-envelope interpolation actually fixes the 157 miss
   (the clustered filter estimates ~9_000 and hashes without replay) *before* Wendelon writes a
   byte. The expensive cut (159) then only has to *produce* a contract already proven to fix it.
4. **It forces a clean Toron contract before producer convenience leaks in.** Fused, the consumer
   would inherit whatever is convenient for `KllSketch`'s internals. Dark 158 defines the fact shape
   Toron can spend; 159 decides whether KLL / equi-depth / other best *produces* it.
5. **Production-inert = low blast radius, decoupled merge-from-activation.** A dark consumer can sit
   in `main` for an arbitrary time before 159 — across releases, other ADRs — affecting nobody. The
   feature has exactly **one** legitimate behavior-change moment: when 159's producer emits facts
   (gated by its own coverage/freshness fences).

**What "not proven dark" would cost — why the proof is load-bearing.** If absence weren't estimate-
and decision-equivalent, merging 158 changes production plans *immediately*, on a feature that
delivers nothing until 159. A subtle empty-fact-handling bug (a sum over zero buckets returning
`Some(0)`, a `max(1,…)` floor, a `non_null_count / buckets.len()` div-by-zero) would make **every**
range filter in the fleet estimate ~0/1 rows → over-broadcast everything → fleet-wide 139/140 replay
tax (or a binder panic), un-attributable, with no producer behind it. So
`absent_range_distribution_falls_back_to_min_max_uniform` is not a nice-to-have — it is the **proof of
darkness**, and it must assert **exact equality** to the uniform estimate (not merely "doesn't
crash"). Two safety layers stack: (1) provable darkness protects *production* from the half-built
consumer; (2) estimate-channel-only protects *correctness always* (a wrong estimate can only cause a
recoverable broadcast/replay, never wrong rows). Skipping (1) leaves you relying solely on (2) —
correctness holds, but a silent fleet-wide perf regression ships until a human notices the replay
storm.

## Decisions

**D1 — Toron-only dark consumer.** Add a Toron-side `ColumnRangeDistributionFact` carrier and spend
it in range selectivity. **No Wendelon change** (159 owns producer/CSB/policy/wire);
`descriptor_optimizer_facts_from_storage` emits empty for range distributions in 158.

**D2 — Contract: bucket/CDF, sketch-agnostic, plainly named [Claude+Codex; resolves Gemini's
`Kll{bytes}`].** Toron consumes a stable **bucket** receipt, never Wendelon's private `KllSketch`
bytes (a `Kll{bytes}` variant would couple Toron's optimizer-fact to Wendelon aggregate-state
internals — the schema-owned-contract discipline, the inverse of 156's wire key):
```rust
pub struct ColumnRangeDistributionFact {
    arrow_type_tag: u8, non_null_count: u64,
    rank_error_ppm: u32,                       // explicit, integer, estimate-channel-visible
    buckets: Vec<RangeDistributionBucket>,     // sorted, non-overlapping, type-compatible w/ RangeFact
}
pub struct RangeDistributionBucket { lower: Vec<u8>, upper: Vec<u8>, count: u64 }  // estimate-channel mass
```
KLL is a 159 **producer candidate/internal algorithm if chosen** (the producer queries its maintained
representation into buckets before the wire); equi-depth is just `rank_error_ppm`-honest buckets.
**No new range-distribution `*V1` type names**
([[feedback_no_v1_suffix_in_type_names]]; `ColumnStatsPolicyV1` is pre-existing debt 158 doesn't
touch). Bucket bounds use the existing optimizer scalar encoding, never `ScalarValue`/`ScalarLiteralKey`.

**D3 — Approximate, not exact — channel honesty (distinct from 156).** A distribution is approximate
(KLL rank-error, or equi-depth bucket-granular). The consumer spends it as a **better estimate** —
estimate-channel only, 139/140-backstopped, never `row_upper_bound`/`byte_upper_bound`/`EmptyRelation`.
`rank_error_ppm` rides the fact (the brainstorm-010 gauge label made concrete).

**D4 — Consumer refines in-envelope only; spend order** (`estimated_rows_for_integral_range`/
`_real_range:4587`, via a `column_range_distribution_for_input` sibling to `column_range_for_input:4546`):
1. envelope decode/clip from `RangeFact` (min/max) first;
2. interval ∩ envelope empty → `RowCount::new(0)` exactly as today (min/max owns zero-out);
3. compatible distribution fact present → mass = Σ fully-covered bucket counts + partial-overlap
   interpolation `count × (overlap / bucket_width)` on boundary buckets, floored ≥1 on non-empty
   overlap, capped at `non_null_count`;
4. else (absent / malformed / type-mismatch / empty / overflow) → **144/147 min/max-uniform**
   (fail-open). NDV/heavy-hitter never consulted for range. **min/max never displaced.**

**D5 — Carrier mirrors `column_range`/`column_heavy_hitters`.** `column_range_distributions` on
`TableOptimizerFacts`/`DescriptorOptimizerFacts`/`Scan` + `for_descriptor` + dark
`descriptor_optimizer_facts_from_storage` emit-empty + **projection remap in `optimize_for_required`
via `ordinal_remap`** (the 155 catch — beside `column_ndv`/`column_range`/`column_heavy_hitters`) +
recursive `Filter` lookup.

**D6 — Type support starts with 157's lane.** `Int64` headline (157's `amount: Int64`); Int32/Float64
fold in if cheap through shared scalar decoding; Date32/Timestamp/civil-time edges are *not* the
load-bearing cut.

**D7 — Public surface closed; retarget; guards fresh module.** No SQL `EXPLAIN` gauge, no Rune API
(brainstorm 010 is doctrine, not 158 scope). Retarget 157's
`adr157_no_live_range_distribution_optimizer_fact_or_producer` → "consumer owned by 158; producer
still absent." Guards in `tests/architecture/range_distribution.rs` (not the over-ceiling `optimizer.rs`);
run the **full** architecture binary (adr077/adr031 remain the 2 known reds).

## Headline gate (closes 157's (b) loop, dark)

`range_distribution_fact_makes_clustered_range_filter_hash_without_replay`: 157's clustered dim
(`RangeFact(0, 10_000, non_null_count=10_000)`, 9_000 rows in `[0,100]`), seeded
`column_range_distributions` for `amount` (bucket `[0,100] => 9_000`, tail bucket outside the filter)
→ `orders ⋈ Filter(customers, amount BETWEEN 0 AND 100)` estimates ~9_000 (bucket mass, not uniform
101) → chooser picks hash, `replayed == false`, digest = forced-hash. Fails on `main` (no consumer /
fact field). 158 fixes the 157 miss when the fact is present, dark until 159.

## Supporting gates

- `absent_range_distribution_falls_back_to_min_max_uniform` (**the proof of darkness** — exact-equal);
- `distribution_refines_in_envelope_min_max_still_zeroes_out_of_range`;
- `malformed_or_mismatched_range_distribution_fails_open`;
- `range_distribution_estimate_stays_estimate_channel_not_hard_bound`;
- `range_distribution_rank_error_is_visible_on_fact`;
- `range_distribution_fact_projection_remaps_with_scan_projection` (155 parity);
- architecture (`range_distribution.rs`): `adr158_range_distribution_consumer_is_dark_toron_only`,
  `adr158_no_wendelon_range_distribution_producer_yet`, `adr158_distribution_refines_in_envelope_only`,
  `adr158_range_distribution_is_estimate_channel_only`, `adr158_range_distribution_names_are_brand_clean`,
  `adr158_kll_aggregate_state_not_toron_fact_contract`, `adr158_public_explain_surface_stays_closed`;
- 157 (uniform miss/replay/absorbed) + 144/147 + 155/156 regressions green.

## Consensus trail

3-of-3 on the dark Toron consumer + consumer-first split (Codex conceded the unify lean iter-1;
grounded — `KllSketch` is aggregate-state-not-payload → a new maintained sketch, the 155/156 pattern;
144 differed). **Contract resolved bucket/CDF-only** (Claude+Codex grounded — no `Kll{bytes}`
cross-ring leak; Gemini retained the enum but hedged that EquiDepth is the clearest and a `Kll`
variant must not leak aggregate-state). `rank_error_ppm` explicit, no-new-range-distribution-`*V1`
([[feedback_no_v1_suffix_in_type_names]]), approximate-not-exact honesty (distinct from 156's
exact-or-decline), 155 projection-remap parity. The operator's "why dark" + "what breaks if not dark"
expounded as the load-bearing rationale (provable-darkness = the safety property; absent must equal
uniform *exactly*).

## Forward arc

`157 range-skew evidence → 158 dark Toron range-distribution consumer → 159 Wendelon range-distribution
producer (live; grounds KLL vs equi-depth, merge, wire) → range selectivity live → Theta join-key
intersection → join selectivity → reordering` · *(parallel: CONVERT/units; Rune optimizer cockpit,
brainstorm 010)*.
