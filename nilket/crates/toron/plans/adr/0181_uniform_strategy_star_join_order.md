# ADR 181: Uniform-Strategy Star Join Order

**Status:** Promoted. **Migration:** `plans/impl/181_*.md`.

ADR 181 extends 178's order actuation from **broadcast-only** stars to **any
uniform-strategy** star — including pure-**hash** stars (large dimensions that don't
broadcast) — which execute today and stay strategy-invariant under reorder. Binder-only;
reuses 178's reorder/restore + 179/180 measure/verdict wholesale.

> **iter-0 grounding correction (kept for the record):** iter-0 mis-framed *broadcast
> snowflake* as the cheap climb. The executor refutes it — `rewrite_broadcast_star_probe`
> (`fragment.rs:603-648`) rewrites every broadcast build as a **scan**
> (`first_scan_metadata` → `shuffle_input_scan`), so a snowflake (sub-join) build does not
> execute; snowflake order is the *execution cliff*, deferred. The grounded climb is this
> uniform-strategy cut.

## Grounding

- **The current implementation proves strategy through binding, not static labels.**
  `choose_join_strategy` (`binder/join.rs:402`) decides broadcast vs hash from each side's
  build facts, probe-linearity, and the broadcast threshold. In the strict star shape, a
  dimension build's own size is stable across orderings, but the growing probe side and
  placement rules are still evaluated by the binder. 181 should therefore classify the
  actual preflight `DecisionReceipt`s, not infer strategy from render text or relation names.
- **Uniformity is a preflight-proven contract.** A star is eligible only when the declared
  order and the selected order both bind to the same uniform strategy: all-broadcast with
  dimension-as-build placement, or all-hash. That is the strategy-invariance proof for this
  cut. If either order is mixed, or the selected order would change the strategy class, 181
  declines rather than silently becoming order+strategy co-optimization.
- **Hash stars execute today** (`execute_hash_join`, `exec.rs:2465`); they are not routed
  through the broadcast fragmenter, so extending *order* to them is binder-only.
- **179/180 are per-edge, replay-id-keyed** → they cover hash-star edges with no new
  surface; the 178 N-segment restore is strategy-agnostic → results-identical reuse.

## Decisions

**D1 - Recognize uniform-strategy stars.** Generalize order eligibility from broadcast-only
to: the declared and selected dimension orders both classify as one strategy — **all
broadcast** (178) **or all hash**. Classification is derived from bound preflight
`DecisionReceipt`s. **Mixed** or strategy-drifting stars decline (`MixedStrategyStar` or
`NonUniformStrategyStar`). Keep the existing fences (single probe, inner-equi, left-deep,
scan-linear probe, leaf dims).

**D2 - Strategy-invariant reorder (proven by preflight).** Reorder by the 178 cost rule +
confidence gate only after both preflights classify to the same uniform strategy. For
broadcast, each edge must be broadcast with the dimension as build; for hash, each edge
must be hash-partitioned. Reuse the 178 reorder + N-segment declared-order restore → output
columns are restored and normalized row results are identical.

**D3 - Mixed declines because the cost rule is strategy-blind.** A mixed star (some dims
broadcast, some hash) stays mixed under reorder, but ordering it well requires comparing a
*broadcast*-join cost to a *hash*-join cost for the same cardinality — 178's by-estimated-rows
rule does not. So mixed-strategy order needs a strategy-aware cost (deferred); 181 declines
it rather than mis-ordering it.

**D4 - Hash-star order is binder-only.** Hash stars already execute; 181 only chooses their
dimension order. No `fragment.rs`/executor change; the broadcast path is unchanged from 178.

**D5 - 179/180 cover it unchanged.** Per-edge measured cardinality (179) and verdict (180)
apply to hash-star edges by `JoinReplayId` with no new surface — verify, do not extend.

**D6 - Confidence gate as 178.** Same soft-margin/confidence gate; the win is still
NDV-selectivity-driven (soft), so reorder fires only on a robust expected win.

**D7 - Scope + reuse.** Single-level, single-probe, inner-equi, left-deep,
**uniform-strategy** (all-broadcast or all-hash) star. Reuses the recognizer (uniformity
extension), 178 reorder/restore, 179/180 net. **Decline:** mixed-strategy (deferred),
snowflake (execution cliff, deferred), multi-probe/galaxy. No executor/`fragment.rs`/
Wendelon/storage/PBC/wire change; no new optimizer fact; no optimizer shell.

## Headline Gates

- `uniform_hash_star_reorders_on_robust_win` - an all-hash star with a clear measured win
  reorders to the cost-preferred order; receipt shows hash strategy + reordered.
- `hash_star_reorder_is_results_identical` - output schema/column order is restored and the
  normalized row multiset matches the declared-order query (test at N>2 dims).
- `mixed_strategy_star_declines` - a broadcast+hash star declines (`MixedStrategyStar`),
  keeps declared order — the discriminating case (181's safety only covers uniform).

## Supporting Gates

- `broadcast_star_order_unchanged_from_178` - pure-broadcast stars behave exactly as 178.
- `hash_star_reorder_preserves_hash_strategy` - each reordered edge stays
  `HashPartitioned` with natural placement.
- `declared_and_selected_preflights_share_uniform_strategy` - both the declared and selected
  order preflights classify to the same uniform strategy before actuation.
- `hash_star_measured_verdict_extends` - 179 measured + 180 verdict produced for a hash star
  with no new surface.
- `hash_star_order_is_binder_only` - no `fragment.rs`/executor diff.

## Architecture Guards

- `adr181_uniform_strategy_recognizer` - order eligibility is all-broadcast OR all-hash;
  mixed declines (`MixedStrategyStar`).
- `adr181_uniform_strategy_preflights_declared_and_selected` - declared and selected
  preflight decision slices are both classified and must match.
- `adr181_reorder_is_strategy_invariant` - reorder reuses the 178 restore and only acts
  after the matching-uniform-strategy preflight.
- `adr181_hash_star_order_is_binder_only` - no executor/`fragment.rs` change.
- `adr181_reuses_178_restore_and_179_180_net` - reuses restore + measure/verdict; no second
  estimator.
- `adr181_no_optimizer_shell` - 177's no-cathedral guard holds.

## Non-Goals

- no **mixed**-strategy star order (needs strategy-aware cost — deferred);
- no **snowflake** order (execution cliff — deferred);
- no multi-probe/galaxy, outer joins, general graphs;
- no executor/`fragment.rs`/topology change; no new optimizer fact.

## Consensus Trail

The council accepted the iter-1 grounding correction (snowflake = execution cliff, not the
cheap climb) and chose the grounded uniform-strategy rung. iter-2 solidified its safety:
uniformity is not guessed from the SQL shape; it is proven by declared-order and
selected-order preflights over the binder's actual strategy decisions. That makes hash-star
order a binder-only, normalized-result-identical reuse of the 178-180 machinery. Mixed
stars decline because the cost rule is strategy-blind (the precise deferral reason);
snowflake is the named execution-cliff follow-on.

## Forward Arc

`180 verdict -> 181 uniform-strategy (hash) star order -> mixed-strategy star
(strategy-aware cost) / broadcast snowflake (execution cliff: relax fragmenter) -> bounded
enumeration -> outer -> bridges -> cycles -> memo`. 181 is the last cheap rung inside the
star shape; mixed-strategy and snowflake each cost a real new surface (a strategy-aware cost
model, or the executor).

## Deferred (named)

Mixed-strategy star order (strategy-aware cost model); **broadcast snowflake order
(execution cliff: `rewrite_broadcast_star_probe` scan-build → published-sub-plan build)**;
multi-probe/galaxy; outer-join order; verdict-driven acts (margin auto-tune / adaptive /
feedback) once verdicts accrue.
