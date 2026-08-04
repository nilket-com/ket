# ADR 143: SQL Inner-Join Side-Local Filter Pushdown

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** The
secondary-frontend completion of ADR 141's headline, now worth doing since ADR 142
made the NDV facts live. SQL lowers `WHERE` *above* the join chain
(`Filter(Join(...))`), so the binder's join-strategy chooser sees an **unfiltered**
dimension and hashes where Rune/direct-IR (`Join(..., Filter(dim))` by
construction) broadcast. ADR 143 pushes **side-local `WHERE` conjuncts onto their
tightest single base input during `lower_select`**, emitting existing
`LogicalPlan::Filter` nodes so the 137/138/141 fact spine + live 142 facts fire —
no binder, chooser, executor, replay, or Wendelon change.

**Placement:** Toron-ring frontend cut (`crates/toron/src/sql_planner.rs`). An
iter-1 cross-over (Claude↔Codex swapped binder-vs-frontend) resolved to the **SQL
frontend on grounding**: a binder LP rewrite would reimplement `collect_scope_refs`
+ `plan_qualifiers` on logical `Expr`, and its frontend-agnostic generality is
illusory because Rune emits the filtered shape directly — SQL is the sole
`Filter(Join)` producer. A binder phase is the right refactor only when a second
such frontend lands (Postgres/gRPC — long-horizon).
**Category:** Frontend logical-shape rewrite (SQL planner).
**Predecessors:** 141 (the NDV estimate consumer this feeds), 142 (made the facts
live), 137 (fact-spine preservation through `Filter`), 121 (`plan_qualifiers`
qualifier resolution), [[rune_is_primary_interface]] (SQL is the secondary
compatibility surface; bring it up to Rune, don't make Rune second-class).
**Migration:** `plans/impl/0143_sql_inner_join_side_local_filter_pushdown.md`.

## Doctrine — make SQL emit the filtered build-side shape Rune already constructs
> ADR 141 estimates a single-base equality filter from a labeled NDV fact; 142 made
> those facts live from Wendelon. The arc fires only when the `Filter` is already
> **below** the join, on the build input. `lower_select` builds the whole join
> chain, then `lower_where` wraps it (`sql_planner.rs:2782–2802`), so the chooser
> (`binder.rs:1420`, bottom-up, sees only children) is handed the full dimension →
> 138 over-estimates → hash. 143 splits the top-level `WHERE` conjuncts and pushes
> each **side-local** one onto its **tightest single base input** before the join
> is built — but **only when the entire FROM chain is `INNER`**. A *placement*
> change, never a *semantics* change.

## All-inner-chain fence (D2, load-bearing correctness)
Pushdown activates **only when every join in the FROM chain lowers to
`JoinKind::Inner`.** Any `LeftOuter`/`RightOuter`/`FullOuter`/`Semi`/`Anti` join
*anywhere* disables pushdown entirely; the full `WHERE` applies above the chain.

This is sharper than a loose "INNER joins only," and the sharpness is the
correctness. A **per-join** rule (push at an inner boundary, skip at an outer one)
and an **upfront** rule (wrap the owning table factor before the chain is built)
are both **unsound** when an outer join sits *above* a pushed input. Worked
counterexample with data:

```sql
SELECT c.id, b.x
FROM a INNER JOIN b ON a.k = b.k
       RIGHT OUTER JOIN c ON c.j = b.j
WHERE b.x = 1
```
`a={k:1}`, `b={k:1, j:9, x:2}`, `c={j:9, id:100}`:
- **Correct (filter above the chain):** `a⋈b = {x:2}`; RIGHT JOIN c matches on
  `j=9`; `WHERE x=1` drops it ⇒ **∅**.
- **`b WHERE x=1` pushed below:** `b WHERE x=1 = ∅`; `a⋈∅ = ∅`; `∅ RIGHT JOIN c`
  null-extends c ⇒ **{id:100, x:NULL}**.

∅ vs one row — wrong rows: `b` is on the RIGHT join's null-producing side, so
pre-filtering ≠ filtering after. Under an all-inner chain a single-side filter
commutes freely through every join, so every push is a provable identity.
All-inner is the smallest *correct* fence; the headline star schema is all-inner.
Per-conjunct path-aware pushdown through mixed chains (push iff the target base is
on the preserved side of every join above it) is a named follow-on.

## Decision

**D1 — Placement: SQL-frontend lowering.** In `lower_select`, reusing
`split_top_level_and`, `collect_scope_refs` (+ a new `ScopeRefs.inner` flag), and
`plan_qualifiers` (which recurses through `Filter`). Emit only `LogicalPlan::Filter`.
Binder fact derivation, chooser, execution, replay, and Wendelon facts unchanged.

**D2 — All-inner-chain fence.** Pushdown activates only when every join in the
FROM chain lowers to `JoinKind::Inner`; the counterexample above is part of the
decision because this is the load-bearing correctness fence.

**D3 — Tightest single input, including the leftmost base.** Push a conjunct onto
the single base it references at the point that base is introduced — the leftmost
base before the first join, each right input at its join step. Never push into the
accumulating left-deep composite (in `orders ⋈ customers ⋈ regions` the
`customers` filter must sit on `customers`, a right input at step 1, not above the
composite where the inner join's own broadcast decision would miss it). A conjunct
referencing a not-yet-introduced base stays pending and pushes at its own step.
**Leftmost-base pushdown is broadcast-relevant, not probe-only** — the chooser
evaluates a left build candidate (`binder.rs:4268`), so a dimension authored left
(`customers c INNER JOIN orders o … WHERE c.region='north'`) broadcasts only when
its leftmost filter is pushed.

**D4 — Side-local + supported, decoupled from estimability.** Pushable iff the
qualified column refs resolve unambiguously to exactly one introduced base and no
other side. Residual (→ `lower_where`): cross-side, unqualified/qualifier-free
constants, ambiguous/unknown qualifiers, `EXISTS`/subquery, any unsupported shape.
Push every supported side-local predicate — `dim.region='north'` (141 estimates)
and `dim.score>10` (141 can't, still earns runtime scan selectivity) alike. **A
classifier miss degrades to today's plan — never an error, never a dropped or
duplicated predicate.**

**D5 — Residual behavior is byte-identical.** Unpushed conjuncts recombine and
flow through the **unchanged** `lower_where`, preserving the EXISTS decorrelation
path verbatim. Pinned by a pushed-vs-above-join digest-equivalence gate
([[feedback_cut_must_deliver_the_name]]).

**D6 — Retarget the guard.** `adr141_no_sql_join_filter_pushdown_in_this_cut`
(`tests/architecture/optimizer.rs:1802`) → the `adr143_*` guards (below).

## Headline gate
`sql_equality_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate`:
`orders ⋈ customers WHERE c.region='north'` with live 142 NDV facts for
`customers.region` lowers `customers` to a `Filter` and the binder emits
`BroadcastEstimated`. **Fails on `main`.**

## Behavior gates
- `sql_inner_join_pushes_right_local_conjunct_to_right_input`
- `sql_inner_join_pushes_leftmost_local_conjunct_to_left_input`
- `sql_multi_dim_star_each_filtered_dim_broadcasts`
- `sql_cross_side_conjunct_remains_residual`
- `sql_unqualified_conjunct_remains_residual`
- `sql_constant_conjunct_remains_residual`
- `sql_outer_join_chain_disables_filter_pushdown` — the D2 counterexample shape +
  rows; asserts no `Filter` under any input **and** the result is ∅ (the live
  guard against the per-join/upfront fences)
- `sql_filter_pushdown_preserves_broadcast_hint_resolution`
- `sql_filter_pushdown_digest_matches_above_join_filter`

## Architecture guards
`adr143_sql_inner_side_local_filter_pushdown`,
`adr143_pushdown_requires_all_inner_join_chain`,
`adr143_pushdown_uses_tightest_single_input`,
`adr143_pushdown_preserves_residual_where`,
`adr143_no_binder_chooser_or_ndv_consumer_change`,
`adr143_no_derived_table_join_widening`.

## Non-goals (named follow-ons)
Binder LP rewrite / general logical optimizer phase; mixed-chain path-aware
pushdown; outer/semi/anti pushdown; **derived-table-in-join** (`FROM fact JOIN
(SELECT … WHERE …) d`, `sql_planner.rs:2778, 2784` — a separate frontend feature);
`JOIN ON` widening; range/quantile selectivity (the second stats producer, ADR 144
candidate); stats default-policy flip; hard-from-exact; 140 A1.

## Forward arc
`NDV equality selectivity (141) → 142 live producer → 143 SQL spends it →
quantile/range selectivity → Theta-sketch join-key intersection → join selectivity
→ join reordering`.

## Consensus trail
Topic 3-of-3 from iter-0. Placement 3-of-3 after the iter-1 cross-over resolved to
the SQL frontend on grounding (Codex conceded back iter-2). The **all-inner-chain
fence** — driven by Claude against a per-join (Codex) / upfront (Gemini) framing on
a data-grounded counterexample — adopted 3-of-3 by iter-3. Leftmost-base pushdown
kept in scope 3-of-3, with Codex's correction that it is broadcast-relevant (left
build candidate), not probe-only, folded in. No remaining divergence.
