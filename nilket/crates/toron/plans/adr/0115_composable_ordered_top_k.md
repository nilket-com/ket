# ADR 115: Composable Ordered Top-K

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini) on Fork A** — one global ordered top-k
barrier, capped reusable primitive. **Toron-only; reuses the existing `Sort` IR + `execute_sort` + coordinator-
final gather; no new IR/executor kernel/PBC/storage.** **Predecessors:** 010 (distributed planning + deterministic
ordering), 015 (terminal sort over grouped aggregate), 079/080/111 (Rune carrier pattern), 106 (derived sources +
the no-`lower_query`-delegation correction). **Migration:** `plans/impl/0115_composable_ordered_top_k.md`.

## Doctrine — ordered top-k; and SQL is declarative where Rune is authorial

`ORDER BY` is an operator-local ordering barrier, **not** durable relation metadata; a downstream operator may
re-order unless the outer query orders again. `ORDER BY` + `LIMIT`/`OFFSET` is **ordered top-k** — it *selects
rows* before later operators. Every 115 sort uses the existing coordinator-final mechanism:
`Sort { input: Exchange { partition_count: 1, key: [] } }` → the sort task sees all rows reaching the barrier,
then applies order/offset/limit via `execute_sort`.

**The frontend asymmetry (operator-steered, the durable stance):**

> **SQL remains declarative.** Rejecting multiple global ordered top-k barriers is fine — SQL users expect the
> optimizer to perform whatever legal rewrite it wants, so SQL multi-barrier may stay rejected indefinitely
> without shame. **Rune dataframe chains are lazy, but authored operator order is meaningful.** A Rune
> `.sort().limit()` is an *explicit ordered top-k barrier*, and repeated barriers are a valid **future Rune
> workflow** — Rune is the back door for authors who want to pin operator boundaries when the SQL optimizer
> wouldn't. So the V1 single-barrier cap is an **implementation cap, not Rune doctrine**, and **Rune is the reason
> we expect to lift it.** Future multi-barrier support must **preserve authored Rune `.sort().limit()` boundaries**
> — never flatten or "optimize away" a chain's barriers unless an explicit rule proves an equivalent rewrite.

This is why the one-barrier implementation **must** be the first use of a reusable barrier *primitive*, not a
one-off shape match: we already know long functional Rune chains will want repeated barriers, so 115 must not
paint the fragmenter into a corner.

## Fragmenter grounding — what "capped primitive" means in the actual code

- **`fragment_plan` is today a flat single-exchange-boundary splitter:** `count_exchanges == 0` → one stage;
  `== 1` → producer + consumer; **`> 1` → `Err(MultipleExchangeBoundaries)`**, with two hand-coded exceptions
  (`fragment_join_plan`, `fragment_terminal_sort_over_grouped_aggregate`). The *scheduler* already runs multi-stage
  DAGs (`StagePlan` + barriers); only the *fragmenter* is flat.
- A global sort barrier contributes one empty-key `Exchange{partition_count:1}` gather. In the simple
  sort-over-scan/filter/project shape, one barrier rides the existing `==1` split. **But grouped aggregate already
  has a repartition exchange**, so the V1 cap cannot be raw `count_exchanges > 1`. Fork A therefore adds a
  barrier-specific detector/count (`global_sort_barrier_count > 1` → pinned V1 error) and keeps aggregate/join
  exchange handling separate. **A→B is a separate, contained rework: single-barrier dispatch → recursive
  barrier-splitter** (the scheduler is ready; 2 and 3 barriers then cost the same). 115 builds none of that and
  wastes none of it — **provided** the one-barrier code is a named detect-and-split primitive, not a third bespoke
  shape matcher beside the join / sort-over-aggregate handlers.

**Shared (non-throwaway) vs the A→B delta:** the frontend (fence lift, query-tail factoring, Rune surface), the
IR/executor (non-terminal `Sort` placement, `execute_sort`, hidden aggregate-sort helper) are all reused unchanged
by B; only the fragmenter flat→recursive splitter is new in B.

## Scale boundary

**In:** SQL-derived `ORDER BY` + optional `LIMIT`/`OFFSET`; Rune `.sort`/`.limit`/`.offset` + `RuneSortKey`/`asc`/
`desc`; one global sort barrier feeding unary consumers (and coexisting with a grouped-aggregate shuffle); reuse
of the hidden aggregate-`ORDER BY` projection. **Out:** unordered bare-`LIMIT`/`take`; **multiple** global sort
barriers (V1 cap); generic multi-stage sort pipelines; distributed merge-sort; local top-k pushdown; non-aggregate
computed `ORDER BY` expansion; joins-around-derived; nested-derived / CTE / set-ops / window / DISTINCT; new IR or
executor kernels.

## Decisions (promotion-locked)

**D1 — Lift only the derived query-tail fence.** `validate_derived_query_shape` stops rejecting `query.order_by` /
`query_has_limit_or_offset`; `WITH`/`FETCH`/locks/`FOR`/settings/format/pipe/non-`SELECT` bodies stay rejected.
`validate_derived_select_shape` stays the narrow derived gate.

**D2 — Share query-tail lowering; no `lower_query` delegation.** Factor an `apply_order_by_limit_tail` helper
(owning `lower_limit_clause`, the `LIMIT`-without-`ORDER BY` reject, `lower_order_by`, hidden aggregate-`ORDER BY`
projection, the single-partition gather sort, and helper-column stripping) used by both `lower_query` and
`lower_derived_table`. The derived path stays `validate_derived_query_shape → validate_derived_select_shape →
lower_select → apply_order_by_limit_tail → alias_plan` (the ADR 106 correction).

**D3 — Reuse `Sort`; no `Limit` IR, no new executor kernel.** 115 broadens *placement* of the existing
`LogicalPlan::Sort` / `ExecutionPlan::Sort`.

**D4 — `LIMIT`/`OFFSET` require `ORDER BY`** in top-level + derived + Rune (this *ordered* cut; cheap bare-`LIMIT`
is the separate future unordered-take ADR — see §What to do next).

**D5 — Hidden aggregate sort helpers stay hidden** — stripped inside the derived boundary before alias-column
rename and before the outer query sees the schema.

**D6 — Rune typed `RuneSortKey { expr, descending, nulls_first }`** + `asc`/`desc` (+ `nulls_first`/`nulls_last`);
`RuneDataFrame::sort(Vec<RuneSortKey>)` rejects an empty key list and emits the single-partition gather sort. No
string sort syntax.

**D7 — Rune `.limit(n)`/`.offset(n)` attach to an immediate `Sort` root** (reject negative; reject a non-`Sort`
root) — preserving D4.

**D8 — One global sort barrier, a capped reusable primitive.** A named `global_sort_barrier` detector + splitter
(barrier = `Sort` over an empty-key single-partition `Exchange`; aggregate repartition exchanges are **not**
counted). Simple one-sort-over-scan shapes reuse the existing `==1` split; aggregate+sort shapes reuse/generalize
the existing sort-over-aggregate path. The cap is `global_sort_barrier_count > 1` →
`FragmentError::MultipleGlobalSortBarriers` ("multiple global sort barriers are not supported in V1"). **No third
bespoke shape matcher.** A grouped-aggregate shuffle may coexist with one sort barrier.

**D9 — SQL/Rune barrier asymmetry (doctrine).** The V1 cap is an *implementation cap*, not Rune doctrine. SQL
multi-barrier rejection may persist indefinitely (declarative). Rune authored `.sort().limit()` boundaries are
*meaningful*; repeated barriers are a valid future Rune workflow and are the reason we expect to lift the cap.
Future multi-barrier work must preserve authored Rune chain boundaries (no flatten/optimize-away without proven
equivalence).

**D10 — Non-root single-sort unary consumers supported.** A derived/Rune top-k may feed projection, alias-column
rename, and filter — generalize the existing terminal-sort-over-aggregate path just enough for those above the one
barrier (`Filter(Project(Sort(Exchange(…))))` and alias-projection shapes).

**D11 — Predicate movement does not cross top-k.** Predicates inside the sort input push as before; predicates
above ordered top-k do not move below it.

**D12 — No merge-sort, no top-k pushdown, no new IR/executor/PBC/storage.**

## Behavior gates

- `sql_derived_top_k_executes` (`SELECT id FROM (SELECT id FROM people ORDER BY id DESC LIMIT 2) AS p WHERE id>0`);
  `sql_derived_offset_executes`; `sql_derived_sort_alias_columns_apply_after_sort`;
  `sql_aggregate_derived_top_k_executes`.
- `multiple_global_sorts_reject_loudly` (derived top-k + outer `ORDER BY` → pinned V1 error) — in **SQL and Rune**.
- `limit_without_order_by_rejects_everywhere` (top-level / derived / Rune).
- `rune_dataframe_top_k_executes`; `rune_sql_relation_top_k_executes` (schema continuation through `ctx.sql`);
  `rune_limit_without_sort_rejects`; null-ordering fixture if cheap.
- Retarget `derived_table_source_scope_rejects_general_subquery_features` (drop the ORDER BY/LIMIT negative; keep
  all other fences).

## Architecture guards

- `adr115_derived_query_tail_fence_lifted_only`; `adr115_derived_lowering_does_not_delegate_to_lower_query`
  (106 path intact); `adr115_sort_reuses_existing_ir_and_executor` (no `Limit` node / kernel).
- `adr115_rune_ordered_topk_surface_registered` (`RuneSortKey`, `.sort`/`.limit`/`.offset`, `asc`/`desc`);
  `adr115_sort_builders_use_single_partition_exchange`.
- `adr115_global_sort_barrier_is_a_named_primitive` — the fragmenter has a named detect/split helper and the V1
  one-barrier policy is a **cap over that primitive**: simple shapes reuse the existing `==1` split, aggregate
  shapes reuse/generalize the existing sort-over-aggregate path, and the cap is **not** raw `count_exchanges > 1`;
  `adr115_multiple_global_sorts_reject_with_pin`.
- `adr115_rune_boundaries_are_authorial_not_sql_sugar` — docs/tests record that a Rune `.sort().limit()` is an
  authored barrier (D9), distinct from SQL's declarative rewrite freedom.
- `adr115_no_merge_sort_or_topk_pushdown`; `adr115_unordered_limit_is_future_take_not_sort` (preserve the
  ordered-vs-unordered distinction).

## Council history

3-of-3 on the topic (ordered top-k) and on **Fork A**. Codex surfaced the B-side tradeoff at iter-1; Claude +
Gemini converged on A; Codex iter-3 ratified. **Claude grounded the fragmenter reality** — simple one-barrier
plans ride the existing `count_exchanges == 1` split, while aggregate+sort needs the existing
sort-over-aggregate exception generalized for unary consumers; the cap is a `global_sort_barrier_count` cap, not
raw `count_exchanges > 1`. A↔B differ by *recursion in the barrier splitter*, not frontend/IR/executor
architecture; that work is the shared non-throwaway foundation. **The operator added the SQL/Rune asymmetry
(D9)** — the cap is an implementation cap, not Rune doctrine; Rune authored chains are the reason to expect the
cap lifted, which hardens the named-primitive requirement (don't paint the fragmenter into a corner). Preserved
for the future: the ordered-vs-unordered-`LIMIT` split and the nested-different-key top-k example (`top 100 by
revenue, then oldest 10 among those`) — the case a second barrier genuinely requires.

## What to do next

(1) **Unordered bounded `take`** (cheap bare `LIMIT`): "any n rows, no ordering/stability/fairness/randomness
guarantee — one Wendelon node returns up to n and stops"; early-stop scan + 059/060 cancellation + storage
row-limit pushdown. **Not** a `Sort` special case. (2) **Multiple ordered top-k barriers**, Rune-driven: relax the
D8 cap by making the fragmenter splitter recursive (scheduler ready), preserving authored Rune `.sort().limit()`
boundaries. (3) logical-type lifecycle; declared-additive types; joins-around-derived (needs qualifier ergonomics).

## Promotion criteria

SQL-derived and Rune-authored ordered top-k reuse the existing `Sort` substrate (single-partition gather); one
global sort barrier may feed unary consumers and coexist with a grouped-aggregate shuffle; stacked global sorts
reject with the pinned V1 error in SQL and Rune; `LIMIT` requires `ORDER BY` everywhere (bare-`LIMIT` documented as
future *take*, not future sort); the ADR 106 narrow-derived path stays intact (no `lower_query` delegation); hidden
aggregate-`ORDER BY` helpers don't leak; the fragmenter contains a **named barrier primitive** (cap, not shape
soup); Rune authored barriers are documented as explicit pipeline boundaries (D9); **no** merge-sort / top-k
pushdown / new IR / executor kernel / PBC / storage change; targeted + Toron suite green. Migration:
`plans/impl/0115_composable_ordered_top_k.md`.
