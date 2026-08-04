# ADR 141: Labeled NDV Equality Selectivity Estimates

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** The first stats-backed CBO cut:
Toron consumes a labeled, *approximate* per-column distinct-count (NDV) optimizer fact, derives
equality-filter selectivity into an estimate-only row channel, and feeds ADR 138's
`estimated_byte_size` — so a common star-schema **build-side filtered dimension** such as
Rune/direct `customers.filter(region == "north")`, which 137 leaves at full dim size and 138 then
estimates too large to broadcast, gets a realistic filtered estimate and broadcasts. Inert without
facts; never writes a hard bound; 139/140 replay covers a wrong estimate. The Wendelon producer
(**141-W**) and SQL join-filter pushdown are named follow-ons.

**Category:** Toron interior — optimizer IQ (binder-local consumer) + a cross-ring producer fact.
**Predecessors:** 137 (the filter arm + the PK-equality recognizer this generalizes; reads metadata
off `ExecutionPlan::Scan`), 138 (the `estimated_byte_size` channel this feeds; the hard/estimate
honesty split), 139/140 (replay — the safety net that licenses spending an approximate estimate),
136 (the descriptor-adjacent fact-sidecar pattern).
**Migration:** `plans/impl/0141_labeled_ndv_equality_selectivity.md`.

## Doctrine — improve the row estimate before the broadcast choice; keep it approximate
> 138 made row→byte estimates honest; 139/140 made under-estimates recoverable. 141 improves the
> *row* estimate before the choice is made: a `Filter` proving `col = <literal>` over a relation
> whose column has a known distinct-count survives ≈ `base_rows / NDV(col)` rows. NDV is
> **approximate** (an HLL estimate, or a uniform-distribution assumption over an exact distinct
> count), so it feeds the **estimate channel only** — never a hard bound — and a skewed-key
> under-estimate that over-caps a broadcast is recovered by 139/140 replay. This is the root-cause
> companion to 138/139/140: sharpen the estimate, don't just recover from a blunt one.

## Two grounded premises kept visible
1. **Wendelon's HLL/small-set distinct-counts are opt-in persisted CSB stats, not always-on
   row-group facts.** `ColumnStatsPolicyV1` defaults to empty; only min/max/null-count is always-on.
   So 141 is a 136-style **consumer-now / producer-named** pair, and 141-W must decide whether it
   exposes only opt-in stats or flips a default policy.
2. **SQL cannot express the headline star shape cheaply.** SQL lowers joins *before* `WHERE`
   (`sql_planner.rs:3693` wraps the `WHERE` `Filter` over the lowered join), rejects joins around
   derived tables, and has no side-local join-filter pushdown — so the join's dim *build* side is
   unfiltered and NDV selectivity can't reach it. The headline is therefore **Rune + direct-`LogicalPlan`**
   (`customers.filter(region=="north").inner_join(orders, …)` filters the build side directly);
   SQL join-filter pushdown is a named follow-on ([[rune_is_primary_interface]]).

## Decisions
**D1 — Labeled, approximate per-column NDV fact.** `DistinctCountFact { Exact(u64), Approx
{ estimate: u64, rel_err_ppm: u32 } }`: `Exact` = complete small-set coverage a producer can prove;
`Approx` = HLL output, carrying its relative standard error as a **quantized integer (ppm)** so the
fact (and the catalog fact structs) keep deriving `Eq` (Codex's catch — an `f64` `std_err` would
force `PartialEq`-only). Absence → unknown. It rides the descriptor-adjacent optimizer-fact sidecar
(136 pattern): the serialized `DescriptorOptimizerFacts` gains a serde-defaulted `column_ndv`
entry list keyed by `ColumnOrdinal`, and `for_descriptor` normalizes it into
`TableOptimizerFacts.column_ndv: BTreeMap<ColumnOrdinal, DistinctCountFact>` (these **catalog**
structs become `Clone`; `TableSchemaDescriptor` untouched). `scan_plan` carries the
projection-mapped NDV onto `ExecutionPlan::Scan` beside `primary_key_columns`, so the Filter arm
reads it exactly as 137 reads PK — keeping the hot `PlanOptimizerFacts` `Copy`.

**D2 — `estimated_row_count` channel; never a hard bound.** `PlanOptimizerFacts` gains
`estimated_row_count: Option<RowCount>` (stays **Copy**). NDV-derived selectivity writes **only**
this field — never `row_upper_bound`/`byte_upper_bound`/a hard receipt. **This holds even for
`Exact`:** an exact distinct *count* is not a hard claim that a particular literal is present or
absent; hard-from-exact needs a separate stats-freshness/coverage doctrine — deferred.
[[logical_identity_receipt_doctrine]].

**D3 — Equality-only selectivity, single most-selective factor (no independence product).** A
`Filter` recognizer (generalizing `filter_includes_primary_key_row_cap`): a non-null `col =
<literal>` over a single-base `Scan` with a usable `NDV(col)` derives `estimated_row_count = 0`
when `base_rows == 0`; otherwise `max(1, ceil(base_rows / max(1, ndv)))`, where **`base_rows` is
the hard `row_upper_bound`** and `ndv` is the fact's point value (`Exact(n)`→`n`,
`Approx{estimate}`→`estimate`). `ndv == 0` therefore gives no reduction rather than a divide by
zero; hard absence from exact-zero remains a follow-on. The `rel_err_ppm` is carried for a future
conservative-bias refinement, **unused this cut**. Top-level `AND` → take the **single most
selective** equality (largest NDV → smallest estimate); **never multiply** factors. A pre-existing
`estimated_row_count` takes the **`min`** with the new single-factor estimate — never another
multiplicative factor (no stacked-filter independence). Out: ranges/`BETWEEN`/inequalities (need
quantiles — the *second* stats cut), `IN`, `OR`, `LIKE`, multi-column, join selectivity.

**D4 — Feeds 138; hard-first chooser untouched.** `derive_estimated_byte_size` uses the tighter row
source — `min(row_upper_bound, estimated_row_count)` when both exist, `estimated_row_count` when
it's the only source, else today's `row_upper_bound` — and still outputs only `estimated_byte_size`.
The chooser still reads hard `byte_upper_bound` first, `estimated_byte_size` only when no hard byte
candidate exists. 138's hard/estimate split and 139/140 replay fences are intact.

**D5 — 141-T now, 141-W named.** 141-T (this cut) consumes labeled NDV facts; **fixture-testable now
with synthetic NDV, inert in production until 141-W** (absent → today's behavior). **141-W (named,
Wendelon-home):** expose table-identity-keyed per-column NDV adjacent to descriptor sync — merge
persisted CSB HLL across row-groups (mergeable), prefer a complete small-set as `Exact`, absence →
unknown — and decide the opt-in-vs-default-policy question. Not folded.

## Honesty fences
- Approximate ≠ hard: NDV-derived rows/bytes only in `estimated_row_count`/`estimated_byte_size`,
  guarded; **`Exact` label ≠ hard bound** this cut (D2).
- Equality-only because NDV only supports it; ranges get no NDV reduction (stated, not faked).
- No independence: single most-selective factor on `AND`; stacked filters take `min`, never re-multiply.
- Wrong estimate is cheap: under-count → 139/140 replay recovers an over-cap broadcast; over-count →
  a missed optimization. The shipped replay net is what licenses spending the approximate estimate.

## Behavior gates (Rune-first per [[rune_is_primary_interface]])
- `rune_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate` — **headline**: a dim that
  hashes when unfiltered, but broadcasts under `region == "north"` + `NDV(region)`
  (`broadcast_estimated`); **fails on `main`** (no NDV → full-row estimate → hash).
- `direct_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate` — same via a direct
  `LogicalPlan` (isolates the binder fact path from Rune syntax).
- `ndv_selectivity_writes_estimated_row_count_not_hard_bound` — NDV touches the estimate channel;
  EXPLAIN labels it estimated (+ exact-vs-approx).
- `exact_ndv_still_uses_estimate_channel` — `Exact` does not write a hard bound (D2).
- `and_filter_uses_single_equality_not_independence_product` — `a=1 AND b=2` uses the larger NDV,
  not `base/(NDV_a·NDV_b)`.
- `range_filter_does_not_use_ndv_selectivity` — `>`,`<`,`BETWEEN`,`IN`,`OR`,`LIKE` get no reduction.
- `missing_ndv_keeps_137_138_behavior` — absent fact → today's estimate (inert; do-no-harm).
- `ndv_underestimate_over_cap_replays_as_hash` — a too-small estimate over-caps and recovers via 139/140.
- Regressions: 129–140 green; 138 hard/estimate split + 139/140 replay fences untouched.

## Architecture guards
- `adr141_column_ndv_fact_is_descriptor_adjacent_not_schema_descriptor`.
- `adr141_ndv_fact_is_labeled_exact_or_approximate`.
- `adr141_selectivity_writes_estimated_row_count_only`; `adr141_exact_ndv_does_not_write_hard_bound_this_cut`.
- `adr141_estimated_rows_feed_estimated_bytes_not_hard_bytes`.
- `adr141_equality_only_no_range_in_or_multicolumn_selectivity`;
  `adr141_and_filters_do_not_multiply_ndv_selectivities`.
- `adr141_missing_ndv_is_inert`.
- `adr141_scan_ndv_is_bind_time_only_not_executed`.
- `adr141_no_table_schema_descriptor_mutation`; `adr141_no_wendelon_producer_in_toron`;
  `adr141_no_sql_join_filter_pushdown_in_this_cut`.
- `adr141_138_hard_estimate_split_untouched`; `adr141_139_140_replay_fences_untouched`.

## Non-goals
SQL join-filter pushdown / derived-table joins (the SQL headline); hard-from-exact
(freshness/coverage doctrine); quantile/length facts (second stats cut); multi-column + join
selectivity; join reordering; 140's A1; the Theta-sketch producer (forward arc below).

## Council history
iter-0: 3-of-3 on stats-backed selectivity-first via NDV. iter-1: Claude conceded HLL is **opt-in
not always-on** (Codex), adopted labeled `{Exact, Approx}` + estimate-channel-only-even-for-Exact +
the separate `estimated_row_count` channel (vs Gemini's loose "shrink `row_upper_bound`"). iter-2:
folded Codex's **SQL-out-of-headline** grounding and the **NDV-on-catalog/Scan keeps
`PlanOptimizerFacts` Copy** threading; locked equality-only, single most-selective factor (no
product); adopted Codex's **quantized `rel_err_ppm`** (preserve `Eq`) and **stacked-filter `min`**
refinements. Genuine 3-of-3 — no divergence to surface.
[[feedback_concession_cycle_ground_not_headcount]], [[logical_identity_receipt_doctrine]].

## Follow-ons
SQL join-filter pushdown (unlocks the SQL headline); 141-W (Wendelon NDV producer + the
opt-in-vs-default-policy decision); hard-from-exact (freshness/coverage doctrine); quantile/range
selectivity + length/byte quantiles (the second stats cut); multi-column stats; 140's A1.

### Forward arc — Theta-sketches for intersections (idea emerged here)
141 spends only **single-relation** equality selectivity (`base_rows / NDV(col)`). The next frontier
is **join** selectivity — `|fact ⋈ dim on key|` — which is what drives join **reordering**, the real
CBO payoff. Join cardinality needs the **intersection** of the two sides' join-key value sets, and
that is exactly what HLL (141's stat) *cannot* give: HLL is mergeable for **union/cardinality**, but
estimating an intersection from HLLs (inclusion-exclusion) compounds error badly. The idea that
emerged while scoping 141: **add Theta-sketches to Wendelon** — built for set **intersection** (and
union / A-not-B) on cardinality estimates — so a future producer maintains per-join-key Theta-sketches
and exposes a **labeled-approximate join-key-intersection fact** that Toron consumes for join
selectivity, feeding the *same estimate channel* this ADR establishes (never a hard bound). It needs
a new Wendelon sketch type + a Toron join-selectivity consumer, so it is **not the next cut** — but
the roadmap line is named:
`NDV equality selectivity (141) → quantile/range selectivity → Theta-sketch join-key intersection → join selectivity → join reordering`.

## Promotion criteria
A Rune (+ direct-IR) equality-filtered dim with an NDV fact infers broadcast via a labeled
*estimated* receipt (fails on `main`); NDV writes only `estimated_row_count`/`estimated_byte_size`,
never a hard bound (even for `Exact`); `AND` uses a single most-selective factor and stacked filters
take `min` (no product); ranges/`IN`/`OR` get no NDV; missing NDV is inert; the `Scan` NDV is
bind-time-only; `PlanOptimizerFacts` stays `Copy`; the NDV fact derives `Eq` (quantized error); 138
split + 139/140 fences green; the `adr141_*` guards + 129–140 guards green; `cargo test -p toron`,
`--test architecture_test -- adr141`, `cargo check -p toron --all-targets`, `git diff --check` all
green.
