# Toron ADR 237: Set Operation Multiplicity (INTERSECT ALL / EXCEPT ALL / MINUS ALL)

**Status:** Promoted. **Migration:** `plans/impl/0237_set_operation_multiplicity.md`.
**Category:** Toron core query surface — completes the set-op family 235 opened by adding the `ALL`
multiplicity variants. **One new physical primitive (row-multiplicity `Repeat`); one binder-only
logical marker (`SetOpAll`) preserves the pre-bind SQL surface until schemas are known.** Everything
else desugars onto the arc: 233 grouped `Aggregate` (count over all-column groups) + 235 null-safe
`Join` + `set_op_output_schema` + existing `CASE`/`IS NULL`/arithmetic. No new optimizer-evidence
surface, durable key, system table, Rune-only surface, or Wendelon change.

## Why this cut exists

235 shipped UNION/UNION ALL/INTERSECT/EXCEPT/MINUS in **DISTINCT** form and deferred the `ALL`
(multiplicity) variants as needing a new kernel. 236 hardened the distributed test substrate so these
cardinals are trustworthy. This cut completes the family: `INTERSECT ALL`, `EXCEPT ALL`, `MINUS ALL`,
with row multiplicity preserved.

## Grounding (verified)

- **The fences to lift** are in `lower_set_operation` (sql_planner.rs:3292): `INTERSECT ALL is not
  supported` (:3323), `EXCEPT ALL` / `MINUS ALL` (:3330-3332). `UNION ALL` already concatenates (235).
- **No multiplicity/repeat primitive exists** — `Repeat` is the one genuinely new op.
- **`CASE` + `IS NULL` are available** (`CaseWhen`, sql_planner.rs:3947/4231); no `least`/`greatest`/
  `coalesce` is required for the multiplicity math.
- **Reusable substrate:** 233 grouped `Aggregate` (group_expr = all columns, `count(*)`); 235 null-safe
  `Join` (`null_safe_keys`) + `set_op_output_schema` + arm normalization; 236 cross-producer cardinal
  discipline + helper for the distributed gate.

## Decisions (promotion-locked)

**D1 — One new physical primitive: `Repeat`, which consumes its count column and fails closed on
invalid counts.**
```text
LogicalPlan::SetOpAll { left, right, kind }   -- binder-only SQL marker
LogicalPlan::Repeat   { input, count: Expr }
ExecutionPlan::Repeat { input, count_column, output_schema }
```
`SetOpAll` is not an execution primitive; it exists so the planner can mark `INTERSECT/EXCEPT/MINUS
ALL` and let the binder perform schema-aware reconciliation and count-arm construction. The logical
`Repeat` node carries a count expression because logical plans are pre-bind and expression-oriented.
The binder resolves that expression to an **integer (Int64) input ordinal** for the physical node.
Physical `Repeat` **consumes** the resolved count column — `output_schema` = the input schema with
that ordinal removed (no trailing projection to hide an internal count).
Per-row contract:
- `count > 0` → emit that many copies of the payload row;
- `count == 0` → emit no row (valid; the `EXCEPT ALL` surplus case);
- `count IS NULL` or `count < 0` → **execution error** (a planner/binder bug or corrupt plan — fail
  closed, do not silently treat as zero);
- non-integer count column → **bind error**.
`Repeat` is pure row-multiplicity: partition-local, no shuffle.

**D2 — `ALL` membership desugars in the binder to count → null-safe join → CASE → Repeat.** The SQL
planner lifts the old fences by emitting `LogicalPlan::SetOpAll`; the binder binds both arms, applies
235's `set_op_output_schema` + arm normalization, and for reconciled output columns `k…`:
- `Lc = Aggregate(L, group = k…, [count(*) AS lc])`; `Rc` likewise.
- `INTERSECT ALL` → `Inner` null-safe `Join(Lc, Rc, on = k…)`, then `Project(k…, repeat_count = CASE
  WHEN lc < rc THEN lc ELSE rc END)`, then `Repeat`.
- `EXCEPT ALL` / `MINUS ALL` → `LeftOuter` null-safe `Join(Lc, Rc, on = k…)`, then `Project(k…,
  repeat_count = CASE WHEN rc IS NULL THEN lc WHEN lc > rc THEN lc - rc ELSE 0 END)`, then `Repeat`.
Only `CASE`, `IS NULL`, comparison, and subtraction — no new scalar. `MINUS ALL` shares the
`EXCEPT ALL` arm.

**D3 — Cardinal: NULL-equal multiplicity.** Grouped aggregation collapses NULL keys into one group;
the join matches them with `null_safe_keys: true` (235). So `(NULL,NULL) INTERSECT ALL (NULL) =
(NULL)` and `(NULL,NULL,NULL) EXCEPT ALL (NULL) = (NULL,NULL)`. Fails under plain-`=` join or if `ALL`
is misrouted through DISTINCT membership.

**D4 — Multiplicity math is `min` / `max(0, L−R)`.** Encoded by the D2 `CASE` expressions; a left-only
row has `rc IS NULL` → `lc` copies. `MINUS ALL` == `EXCEPT ALL`.

**D5 — Distributed in scope, riding the arc.** The all-column `Aggregate` counts a row's occurrences
**across producers** (233's group-key shuffle); the null-safe join rides 235; `Repeat` is
partition-local (fragments like `Project`/`Filter`). The distributed cardinal applies the **236
cross-producer rule**: a row whose copies are split across >1 producer must still yield the correct
`min`/`max` — expected value single-producer-unreachable, annotated via the 236 helper, not just
`worker_count > 1`.

**D6 — Scope = the three `ALL` membership variants.** In: `INTERSECT ALL`, `EXCEPT ALL`, `MINUS ALL`
(+ `Repeat`). Deferred (unchanged): `BY NAME`; per-arm `ORDER BY`/`LIMIT`; set-op derived/EXISTS
bodies; a general SQL-visible repeat / generate-series.

**D7 — No new optimizer / durable / Wendelon surface beyond physical `Repeat`.** The desugar reuses
aggregate/join/exchange/project; `SetOpAll` is a binder marker only; `Repeat` adds no
optimizer-evidence. Physical `Repeat` builds a
`UInt32` take-index and uses `arrow::compute::take`, estimating output rows against the task memory
budget (no unbounded index allocation).

## Semantics

```sql
-- now supported (multiplicity preserved)
(VALUES (1),(1),(1)) INTERSECT ALL (VALUES (1),(1))   -- min(3,2)=2 -> {1,1}
(VALUES (1),(1),(1)) EXCEPT ALL    (VALUES (1),(1))   -- max(0,3-2)=1 -> {1}
SELECT a FROM t MINUS ALL SELECT a FROM u             -- == EXCEPT ALL
-- still rejected (deferred)
SELECT a FROM t INTERSECT ALL BY NAME SELECT a FROM u
```

## Honesty gates (discriminating)

- **`intersect_all_is_min_not_distinct`** — `(1,1,1) INTERSECT ALL (1,1)` → `(1,1)` (distinguishes
  multiplicity from DISTINCT membership, which would collapse to one row).
- **`except_all_subtracts_exactly`** — `(1,1,1) EXCEPT ALL (1,1)` → `(1)`.
- **`intersect_except_all_are_null_equal`** — *cardinal*; `(NULL,NULL) INTERSECT ALL (NULL)` → `(NULL)`.
- **`except_all_zero_count_emits_no_row`** / `repeat_zero_count_emits_no_row` — the `count==0` case.
- **`repeat_rejects_null_and_negative_counts`** — fail-closed contract (D1).
- `minus_all_aliases_except_all` (digest equal).
- **`set_op_all_distributed_multiplicity_across_producers`** — distributed: a row whose copies are
  produced by >1 producer yields the correct `min`/`max` (236 cross-producer discipline; expected
  value single-producer-unreachable).
- Deferred fence: `intersect_all_by_name_is_explicit_unsupported`.

## Architecture guards (promotion-locked)

- `adr237_set_op_all_desugars_to_count_join_repeat` (no set-op-ALL-specific physical operator)
- `adr237_repeat_is_the_only_new_physical_multiplicity_primitive`
- `adr237_repeat_consumes_integer_count_column`
- `adr237_repeat_rejects_invalid_counts` (NULL/negative → error)
- `adr237_all_membership_uses_null_safe_join`
- `adr237_minus_all_aliases_except_all`
- `adr237_by_name_remains_deferred`
- `adr237_distributed_cardinal_uses_adr236_cross_producer_rule`

**Reconciled prior guards (named):**
- The set-op `ALL` rejects in `lower_set_operation` (235; sql_planner.rs:3323/3330/3332) → retargeted:
  the three membership `ALL` variants now lower; `BY NAME` stays rejected.
- 235 `Union` / null-safe-join / `fragment_union_plan` guards → reused unchanged.
- 233 grouped-aggregate / 236 cross-producer-cardinal guards → reused (the count + the distributed
  gate ride them).

## Consensus (transparent)

3-of-3. The design is Gemini's (its 236-labelled drafts proposed the aggregate-join-`Repeat` desugar,
recorded as the seed in ADR 236). Claude iter-1 grounded the fences, the `CASE`-based `min`/`max` (no
`least`/`greatest`/`coalesce`), and the 236 cross-producer discipline on the distributed gate. Codex
iter-1 converged and contributed two refinements, **both adopted** here: (1) **`Repeat` fails closed**
on NULL/negative counts rather than treating them as zero (invalid counts are planner bugs — surface
them; `count==0` stays valid); (2) **`Repeat` consumes its count column** (output = input minus that
ordinal), a cleaner physical contract than a trailing projection — Gemini concurred on both the
desugar and the consume-count contract. The logical form still carries an expression and lets the
binder resolve the ordinal. No residual divergence; Codex's open questions (Int64-only count,
take-index budget) are taken as the lean and recorded in the impl.

## Forward arc

```text
237 set-op ALL multiplicity (this) -> distributed predicate-fidelity substrate (236's named sibling)
-> multi-dedup distinct aggregates (234 deferred) -> window functions
```

## Deferred (named)

`BY NAME` quantifiers; per-arm `ORDER BY`/`LIMIT`; set-op derived/EXISTS bodies; a general SQL-visible
repeat / generate-series; predicate-fidelity substrate (236 sibling); multi-dedup distinct aggregates;
window functions.
