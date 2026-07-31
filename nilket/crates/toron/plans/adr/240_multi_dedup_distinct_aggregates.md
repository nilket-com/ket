# Toron ADR 240: Multi-Dedup Distinct Aggregates

**Status:** Draft (iter-1 of the adr/impl dual). **Migration:** `plans/impl/240_multi_dedup_distinct_aggregates.md`.
**Category:** Toron core query surface — the general DISTINCT-aggregate fan-out/recombine rewrite. **A
planner-owned desugar that generalizes the ADR 234 single-dedup rewrite into N-branch fan-out +
recombine over the existing `Project`/`Distinct`/`Aggregate`/`Join`/`Cross` primitives.** No new
physical operator, optimizer-evidence, durable key, system table, Wendelon change, or Rune-only surface.
This is the consumer ADR 239 was built for.

## Why this cut exists

ADR 234 landed exactly one DISTINCT-aggregate shape: a single distinct-argument identity feeding the
whole aggregate (`build_distinct_aggregate_rewrite`, sql_planner.rs:5788). Everything else is an
explicit reject whose error string literally ends "...require multi-dedup fan-out":

| Reject (verified in code) | Lifted by this ADR |
|---|---|
| `"mixed DISTINCT and non-DISTINCT aggregates require multi-dedup fan-out"` (:5795) | a plain branch recombines with the distinct branches |
| `"multiple DISTINCT aggregate arguments require multi-dedup fan-out"` (:5807) | one dedup branch per distinct-argument identity |
| `"AVG(DISTINCT) requires multi-dedup fan-out"` (:5870) | `AVG(DISTINCT x)` = ordinary `AVG(x)` over the dedup branch |
| `"computed DISTINCT aggregate arguments require multi-dedup fan-out"` (:5883) | `Project(__arg = expr) → Distinct → Aggregate` |

The substrate is already in the tree: ADR 234 `Distinct`, ADR 235 null-safe group-key join, ADR 239
keyless `Cross`. So this is a frontend rewrite that *composes landed primitives*, not new machinery.
ADR 239 exists specifically for the global case — without 240 its keyless `Cross` has no caller.

**The intuition (for the humans in the loop).** `COUNT(DISTINCT x)` cannot share a hash table with
`COUNT(DISTINCT y)` — each distinct set is its own dedup. The standard relational answer is to *fan
out*: dedup each distinct argument in its own branch, aggregate each branch ordinarily, then *recombine*
the per-branch results back into one row per group. Think of it like computing two separate
`SELECT DISTINCT` summaries and joining them on the group key — except the planner writes the join for
you. Grouped queries recombine on the `GROUP BY` key (a 235 null-safe join, so `GROUP BY NULL` still
forms one group); global queries have no key, so each branch is a single row and they recombine with a
cartesian — exactly ADR 239's keyless `Cross`.

## Grounding (verified)

- `build_distinct_aggregate_rewrite` is reached for **both** the grouped path (sql_planner.rs:3658) and
  the global path (:3713) via `lower_aggregate_projection` (:3902); the single-dedup global case
  already works. 240 generalizes the same rewrite locus — not from scratch.
- The rewrite wraps the already-lowered base via `rewrite.wrap_input(plan)`; at that point `plan` is the
  fully-lowered FROM (scan, filter, **or join**) with qualifiers/ordinals already resolved. `LogicalPlan`
  is `Clone`, so re-deriving N branches from a join base is mechanically identical to a scan base — see
  D8.
- The frontend rejects an empty aggregate list ("GROUP BY requires at least one aggregate call",
  sql_planner.rs:3656). A dedicated synthetic "group-domain" branch would need an *internal* empty-agg
  `Aggregate`; D6 shows it is unnecessary.
- Null-safe group-key joins (ADR 235) and keyless `Cross` (ADR 239) are both frontend-emittable
  (`null_safe_keys: true` joins; `JoinKind::Cross`, `on: []`).

## Decisions (promotion-locked)

**D1 — Keep `AggregateFunction` flagless; generalize the existing rewrite.** Multi-dedup is a
planner-owned desugar over `Project`/`Distinct`/`Aggregate`/`Join`/`Cross`. `build_distinct_aggregate_rewrite`
grows from "single branch wrapped inline" to "build the full fan-out + recombine plan." The single-branch
case becomes N=1 (one distinct branch, no plain branch, recombine-of-one = the branch itself), so ADR
234's output is subsumed unchanged. No `CountDistinct`/`SumDistinct` physical kernels.

**D2 — One dedup branch per structural distinct-argument identity.** `COUNT(DISTINCT x)`,
`SUM(DISTINCT x)`, `MIN(DISTINCT x)`, `MAX(DISTINCT x)`, `AVG(DISTINCT x)` over the *same* argument
share one `Project(keys, x) → Distinct → Aggregate` branch. A different argument identity (structural
equality, the existing `canonical_aggregate_signature_expr`) gets its own branch.

**D3 — Computed distinct arguments via explicit projection + planner ordinals.**
`COUNT(DISTINCT x + 1)` lowers to `Project(keys, __distinct_arg_k = x + 1) → Distinct → Aggregate`. All
branch and recombine references use generated ordinals/names, never user-visible aliases (ordinal-drift
discipline).

**D4 — Mixed distinct + non-distinct in scope.** Non-distinct aggregates form the plain recombine side:
normally one `Aggregate(group=keys, non_distinct_aggs)` over the base input. If multiple plain
aggregates would produce duplicate function-level output names inside that branch (for example
`SUM(id), SUM(amount)` both named `sum`), the planner may shard them into additional plain branches so
branch-normalization remains ordinal-safe; those shards still recombine like the distinct branches.

**D5 — `AVG(DISTINCT x)` = ordinary `AVG(x)` over the dedup set.** It shares the `x` dedup branch with
`SUM/COUNT(DISTINCT x)` and emits existing `AggregateFunction::Avg` in that branch. Existing `AVG`
already ignores NULL and returns `NULL` for an empty/all-NULL input, so no sum/count final projection or
new aggregate kernel is required unless implementation grounding proves direct `Avg` cannot compose.

**D6 — Grouped recombine uses ADR 235 null-safe group-key joins; NO dedicated group-domain branch.**
Every branch — plain or distinct — emits exactly one row per group present in the input: `Distinct(keys, x)`
retains at least one `(keys, *)` row per group (including `(keys, NULL)`), so a group whose distinct
column is entirely NULL still surfaces as `(keys, count = 0)`. The branch group-sets are therefore equal
and complete, and recombining them with null-safe joins on the group keys needs no synthetic
group-domain branch (which would require the forbidden internal empty-agg `Aggregate`). `GROUP BY NULL`
forms one group; the recombine joins are private and null-safe, while ordinary user joins stay
NULL-unequal.

**D7 — Global recombine uses ADR 239 `Cross` (via the scalar-local refinement).** With no group key,
every branch is a scalar aggregate producing exactly one row; recombine is `Cross(branch_0, branch_1, …)`
(left-deep), `1 × 1 × … = 1` row. This depends on the **ADR 239 scalar-local Cross refinement** (landed
first): because each global aggregate branch is recorded as a hard 1-row fact (`row_upper_bound <= 1`),
the `Cross` binds as a single-partition *local* cartesian rather than a broadcast — so the global
recombine uses a real keyless `Cross`, not a synthetic constant key. A branch that produced more than one
row would multiply the result, so global branch single-row-ness is pinned (D10).

**D8 — Join base inputs are in scope; no fence.** The rewrite re-derives each branch by cloning the
already-lowered base `plan`, so a join base composes identically to a scan base (qualifiers resolved
pre-rewrite). Fencing joins would be *more* code for a *smaller* feature. **Deferred:** optimizer
*sharing* of the duplicated base — each branch re-executes the base scan/join in this cut; a later
cut can common-subexpression the shared input. Correctness does not depend on sharing.

**D9 — No new physical / optimizer / durable / Wendelon / Rune surface *in this ADR*.** The multi-dedup
rewrite itself is frontend-only — it reuses `Aggregate`, `Distinct`, null-safe `Join`, and `Cross`. The
one binder-side enabler (global aggregate → 1-row fact + the scalar-local `Cross` strategy) is **not part
of this ADR**: it landed first as the narrow ADR 239 scalar-local refinement, so ADR 240 composes over
existing primitives. Multi-dedup adds no optimizer-evidence and no cross-ring change.

**D10 — Recombine cardinality is explicit, not incidental.** Global branches are pinned single-row
(so `Cross` cannot become a row multiplier); grouped branch group-sets are pinned complete (so a group
with an all-NULL distinct column is not dropped). The distributed gate is value-discriminating per the
ADR 236/238 cross-producer rule.

## Semantics

```sql
SELECT COUNT(DISTINCT amount), SUM(id)            FROM sales              -- plain + distinct recombine
SELECT COUNT(DISTINCT amount), COUNT(DISTINCT region) FROM sales          -- two dedup branches
SELECT COUNT(DISTINCT x), SUM(DISTINCT x), MAX(DISTINCT x) FROM t GROUP BY g  -- one shared x branch
SELECT AVG(DISTINCT amount)                       FROM sales              -- ordinary Avg over deduped amount
SELECT COUNT(DISTINCT amount + 1)                 FROM sales              -- computed arg, projected first
SELECT COUNT(DISTINCT c.id), SUM(o.total) FROM orders o JOIN customers c ON o.cid = c.id GROUP BY c.region
-- still rejected (deferred):
SELECT COUNT(DISTINCT x) FILTER (WHERE y > 0) FROM t                      -- aggregate FILTER
```

## Honesty gates (discriminating)

- **`mixed_distinct_and_plain_aggregate_executes`** — `COUNT(DISTINCT amount), SUM(id)`: distinct and
  plain branches recombine (fails if mixed stays rejected or the plain branch is dropped).
- **`mixed_plain_branch_duplicate_output_names_stay_ordinal_safe`** — `SUM(id), SUM(amount)` alongside a
  distinct aggregate returns both sums correctly, not two reads of the first `sum` output.
- **`multiple_distinct_arguments_execute`** — `COUNT(DISTINCT amount), COUNT(DISTINCT region)`: two
  dedup branches with distinct values per branch.
- **`same_distinct_argument_shares_one_branch`** — plan-shape guard: `COUNT/SUM/MAX(DISTINCT amount)`
  produce exactly one `Distinct` over `amount`.
- **`avg_distinct_uses_avg_over_dedup_and_null_semantics`** — all-NULL / empty distinct set returns
  `NULL`, not `0`; no new kernel or sum/count final projection is required.
- **`computed_distinct_argument_executes`** — `COUNT(DISTINCT amount + 1)` with NULLs and duplicates,
  resolved by ordinal not name.
- **`grouped_multi_dedup_all_null_distinct_column_keeps_group`** — a `GROUP BY g` group whose distinct
  column is entirely NULL still appears with `COUNT(DISTINCT x) = 0` (**locks D6**: the recombine domain
  is complete without a dedicated groups branch — fails if branches inner-join the all-NULL group away).
- **`global_multi_dedup_branch_is_single_row_before_cross`** — each global branch emits exactly one row
  (**locks D7/D10**: a >1-row branch would make `Cross` a multiplier).
- **`multi_dedup_over_join_executes`** — a distinct + plain aggregate over a `JOIN` base (locks D8).
- **`multi_dedup_distinct_aggregate_distributed_merges_across_producers`** — value-discriminating per
  236/238: a group's distinct values *and* a plain-aggregate contribution split across producers so the
  result is unreachable without distributed dedup + aggregate + recombine (worker count is supporting
  evidence, not the proof).

## Architecture guards (promotion-locked)

- `adr240_keeps_aggregate_function_flagless`
- `adr240_multi_dedup_rewrites_to_branch_aggregate_recombine`
- `adr240_same_distinct_arg_shares_one_dedup_branch`
- `adr240_computed_distinct_args_are_projected_before_dedup`
- `adr240_avg_distinct_is_avg_over_dedup`
- `adr240_grouped_recombine_uses_null_safe_group_key_join_without_group_domain_branch`
- `adr240_global_recombine_uses_cross_join`
- `adr240_multi_dedup_composes_over_join_base`
- `adr240_distributed_gate_is_value_discriminating`
- `adr240_no_new_physical_optimizer_durable_wendelon_or_rune_surface`

**Reconciled prior guards (named):** the four ADR 234 "...require multi-dedup fan-out" reject strings
(sql_planner.rs:5795/5807/5870/5883) are retargeted — the rewrites now accept those forms; only the
named-deferred forms (FILTER, window aggregates, grouping sets) stay rejected. ADR 235 null-safe-join
and ADR 239 Cross guards are reused unchanged.

## Consensus (transparent)

Claude and Codex iter-0 independently converged on the topic and the fan-out/recombine spine (flagless
`AggregateFunction`, branch-per-identity, computed-arg projection, global=`Cross`, distributed
value-discriminating). Iter-1 grounding corrected the AVG sketch: `AVG(DISTINCT x)` rides ordinary
`AggregateFunction::Avg` over the dedup branch, rather than a planner-owned sum/count final projection.
Two forks Claude raised were resolved by grounding, both toward the smaller cut: **D8** (join base is
plan-agnostic cloning, no fence — Codex's "include if mechanical" arm) and **D6** (no dedicated
group-domain branch — Claude's challenge to Codex's `groups` branch, grounded on `Distinct` retaining a
row per group). Gemini's divergences, if any, fold into the iter-1 review.

## Forward arc

```text
234 single-dedup distinct aggregate
235 null-safe group-key join   ─┐
239 keyless CROSS substrate    ─┴─> 240 multi-dedup distinct aggregates (this)
-> window functions (frame/partition/order; the next independent surface)
-> optimizer: share duplicated branch inputs (CSE over fan-out)
```

## Deferred (named)

Aggregate `FILTER (WHERE …)`; window aggregates; grouping sets / rollup / cube; volatile/non-deterministic
computed distinct expressions; optimizer sharing of duplicated branch scans/joins (each branch
re-executes its base input in this cut).
