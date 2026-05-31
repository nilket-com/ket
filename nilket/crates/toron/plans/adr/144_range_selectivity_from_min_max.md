# ADR 144: Labeled Range Selectivity from Maintained Min/Max

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** Range predicates
(`< <= > >=`, `BETWEEN`) get no stats-backed selectivity today — the ADR 141 guard
`range_filter_does_not_use_ndv_selectivity` pins it. ADR 144 gives them a **labeled
approximate** selectivity from Wendelon's **already-maintained always-on per-column
min/max + null counts**, written to the **estimate channel only**, so a
range-filtered dimension can broadcast. **v1 is min/max**; KLL/quantile precision
for skewed columns is the named follow-on.

**Placement:** one **cross-ring ADR** mirroring 142 — Wendelon producer-exposure
(`crates/wendelon`, additive on the existing `TABLE_OPTIMIZER_FACTS_GET` blob) +
Toron mapping/consumer (`crates/toron`). The Toron consumer is fixture-testable now.
**Category:** Cross-ring optimizer-fact (range) producer-exposure + Toron estimate
consumer.
**Predecessors:** 141 (the equality-NDV consumer pattern this mirrors), 142 (the
live producer, descriptor-sidecar, full-coverage fence, and `Vec`-fact discipline),
138 (the byte-estimate consumer this feeds), 143 (SQL spends estimates),
[[adr_118_cost_bound_scope_projected_path]] (the maintained CSB stats source).
**Migration:** `plans/impl/144_range_selectivity_from_min_max.md`.

## Doctrine — exact bounds, approximate selectivity, honest channel
> Wendelon's CSB column-stats block already maintains **always-on** per-column
> `min`/`max`/`null_count`/`non_null_count` (`build_always_on_column_stats_block`,
> `stats.rs:638`) — *always-on*, unlike the *opt-in* HLL/small-set NDV the 141→143
> path rides, so range selectivity fires broadly day-one. Min/max and counts are
> *exact* for a fully-covered snapshot, but the row count you derive from them by
> assuming a **uniform** distribution is a guess (real columns skew). So a range
> fact rides the **estimate channel only** (`estimated_row_count`, never the hard
> `row_upper_bound`/`byte_upper_bound`, never elides a scan) — identical channel
> honesty to 141's approximate NDV; a skewed under-estimate that over-broadcasts is
> recovered by 139/140 replay.

## The load-bearing grounding — estimate from the fact's own counts, not `row_upper_bound`
The live Wendelon→Toron facts mapping (`descriptor_optimizer_facts_from_storage`,
`catalog.rs`) sets **`row_upper_bound: None`** and `materialized_byte_upper_bound:
None` — production facts carry only `column_ndv` (and now `column_range`). So a
range estimate computed as `base_rows · fraction` would be **inert in production**
(no `row_upper_bound` → no estimate) — the same "build-but-dark" trap 142 fixed for
141. Therefore the range fact **carries its own `non_null_count`**, and the estimate
is derived from it, never from `row_upper_bound`. (If a hard `row_upper_bound` is
ever present, the existing `with_estimated_row_count` clamp still applies.)

## Decision

**D1 — Additive cross-ring fact (mirror `column_ndv`).** Wendelon extends
`TableOptimizerFacts` (`schema/optimizer_facts.rs`, `#[serde(default)]`):
```rust
pub struct ColumnRangeFact {
    pub column_ordinal: u16,
    pub arrow_type_tag: u8,
    pub null_count: u64,
    pub non_null_count: u64,
    pub min: Option<Vec<u8>>,   // canonical bytes; None iff non_null_count == 0
    pub max: Option<Vec<u8>>,
}
// TableOptimizerFacts { …, #[serde(default)] pub column_range: Vec<ColumnRangeFact> }
```
Toron **maps** it into its own catalog structs beside `DistinctCountFact` (no
Wendelon-storage import — the 142 discipline) → `column_range: BTreeMap<ColumnOrdinal,
RangeFact>`, carried on `ExecutionPlan::Scan` beside `column_ndv`. The scalar tag
values and the supported signed-byte decode contract used by range facts are
schema-owned optimizer-fact API, re-exported through `wendelon-client`; Toron must
not import `wendelon::storage::stats` or duplicate private CSB constants. This is a
same-version facts payload change unless the implementation adds an explicit legacy
decoder: `#[serde(default)]` documents the empty default, but postcard arity must
not be treated as mixed-version compatibility. The 136/142 descriptor
identity-gating is unchanged. `ColumnStatsPolicyV1` default stays empty (it governs
opt-in bloom/HLL/small-set only; min/max is always-on).

**D2 — Estimate channel only.** Range selectivity writes `estimated_row_count`
only — never `row_upper_bound`, never `byte_upper_bound`, never an `EmptyRelation` —
even when the predicate is provably empty against min/max.

**D3 — Producer: full-coverage, fixed-width ordered tags.** Emit a column's range
fact only from **fully-covered** contributor stats (reuse 142's fence: uncovered
active rows / missing tiered contributor / identity mismatch → no facts). Merge all
matching contributors: `null_count`/`non_null_count` summed, `min`-of-mins /
`max`-of-maxes over canonical bytes (canonical ordering = value ordering). If
`non_null_count > 0`, **every** non-empty contributor must carry non-truncated
min/max — any missing or `COLUMN_STAT_MIN/MAX_TRUNCATED` ⇒ **omit the column** (a
partial/truncated merge narrows the range → systematic under-estimate →
over-broadcast, the aggressive direction). If `non_null_count == 0`, `min=max=None`
(a non-null range predicate estimates 0). Emit only **signed/unsigned integer +
date/timestamp** tags; **not** UTF-8/binary/float/decimal/boolean/nested or
KLL/quantile.

**D4 — Consumer: integer literals v1, fail-open otherwise.** Toron estimates
single-column `< <= > >=` and SQL `BETWEEN` (already lowered to `col >= low AND
col <= high`). v1 decodes only `Int32`/`Int64` (the types with a current Toron
literal/coercion surface); unsigned/date/timestamp facts ride the sidecar but
**fail open** until a literal surface exists; float/decimal/UTF-8/binary/boolean,
column-to-column comparisons, `IN`, `OR`, casts, and multi-column correlations all
fail open (today's blunt default — never an error).

**D5 — Discrete-domain interpolation over the non-null population.** For decoded
`min`,`max`,`non_null_count` `N`:
- `domain_width = max − min + 1`
- collect the column's top-level-`AND` range atoms, **intersect** to one clipped
  inclusive `[low, high]`; `overlap_width = high − low + 1` clipped to the envelope
- estimate `= ceil(N · overlap_width / domain_width)`; no overlap → `0`; full
  envelope → `N`; `min == max` → all-or-zero by evaluating the single value;
  all-null fact → `0`. The computed `0` is **still an estimate** (D2).

**D6 — `BETWEEN`/same-column is a real band, not min-of-two.** Because D5 intersects
same-column atoms first (tightest lower + tightest upper, contradictory → 0),
`BETWEEN 10 AND 19` estimates the band `(high−low+1)/domain`, not the looser
`min(P(≥10), P(≤19))`.

**D7 — Combination: 141 no-independence.** Equality-NDV estimates stay in the
equality helper, range estimates in the new range helper; a filter applies the
**single tightest** estimate across both — no selectivity product.

**D8 — Retarget the guard.** `range_filter_does_not_use_ndv_selectivity`
(`binder.rs:9119`) → `range_filter_uses_min_max_range_selectivity_not_ndv`: range
now has selectivity, but from the **min/max channel**, and NDV stays **equality-only**
(a range filter must not read `column_ndv`).

## Headline gate
`range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate`: a dimension
with live range facts for an `Int64` column and **no hard row bound** —
`dim.amount BETWEEN 10 AND 19` — derives `estimated_row_count` from
`column_range.non_null_count`, 138 estimates a smaller build side, and the binder
emits `BroadcastEstimated`; execution results unchanged. **Fails on `main`.**

## Supporting gates
- `range_fact_with_empty_stats_policy_is_produced_from_min_max`
- `range_fact_requires_full_contributor_coverage`
- `range_fact_merges_counts_min_and_max`
- `range_truncated_min_max_omits_fact`
- `range_filter_estimates_without_row_upper_bound` *(the inertness guard)*
- `range_lt_and_gt_estimate_clipped_fraction`
- `range_between_intersects_same_column_bounds`
- `range_multiple_bounds_keep_tightest_interval`
- `range_constant_column_min_equals_max_no_div_by_zero`
- `range_outside_envelope_estimates_zero_without_hard_bound`
- `range_utf8_and_float_facts_fail_open`
- `range_and_equality_take_single_tightest_estimate`
- architecture: `adr144_range_facts_are_descriptor_sidecars`,
  `adr144_default_stats_policy_remains_empty`,
  `adr144_range_scalar_tags_are_schema_owned`,
  `adr144_range_fact_wire_compatibility_is_explicit`,
  `adr144_range_selectivity_estimate_channel_only`,
  `adr144_ndv_helper_remains_equality_only`,
  `adr144_no_kll_quantile_optimizer_fact`

## Non-goals (named follow-ons)
KLL/quantile or histogram facts (skew precision — a new maintained producer);
UTF-8/binary/float/decimal/boolean range; date/timestamp consumer decode;
hard-from-range (needs the hard-from-exact freshness doctrine); null-aware
multi-column / correlated selectivity; join selectivity (Theta-sketch arc).

## Forward arc
`NDV equality (141) → 142 producer → 143 SQL → **144 min/max range** →
KLL/quantile precision → Theta-sketch join-key intersection → join selectivity →
join reordering`.

## Consensus trail
Topic + min/max-v1 shape 3-of-3 by iter-1 (Claude's iter-0 corrected Gemini's
"maintain KLL" framing — min/max is already maintained always-on). The
**load-bearing refinement** — carry `non_null_count` and estimate from it, **not**
`row_upper_bound` — is Codex's iter-2 catch, **verified**: live facts map
`row_upper_bound` to `None` (`catalog.rs`), so Claude's iter-1 `base_rows` estimate
would have shipped inert (the 141/142 build-but-dark lesson). Discrete interpolation
and the `BETWEEN`-band intersect (over Claude's iter-1 stacked-`min`) converged
Codex+Gemini; the `Int32/Int64`-only consumer scope is Codex's literal-surface
grounding. No remaining divergence.
