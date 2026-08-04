# ADR 294 — derived-table comma-list FROM body

## Status
Proposed.

## Context

ADR 293's q07 measured `blocked(unsupported_derived_multi_source@plan)` — raw:
`"derived table subquery must have exactly one FROM source"`. q07's official `shipping` derived table has a
**comma-list FROM body**:

```sql
FROM (SELECT … FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE …) AS shipping
```

The planner blocks this **before** the existing machinery runs. In the sqlparser AST, `Select.from` is a
`Vec<TableWithJoins>`: `FROM a JOIN b ON ...` is one FROM entry with joins, while `FROM a, b` is multiple FROM
entries. Toron already lowers top-level comma joins (`lower_select`: `is_comma_join = from.len() > 1` → ADR 239's
left-deep keyless `Cross` fold) and keys them in the binder (`recognize_cross_equi_joins`, ADR 270); ADR 254
already opened the single-FROM-entry *join-bodied* derived path. The only thing in the way is
`validate_derived_select_shape` (`sql_planner.rs:9242`) requiring `select.from.len() == 1`.

## Decision

A small **planner validator relaxation** — not a new join engine. Allow a derived body's FROM to be a validated
**multi-FROM-entry comma list**, then let the existing `lower_select` comma-join fold + ADR 270 binder recognition
do the work.

### D1 — relax `validate_derived_select_shape` (`sql_planner.rs`)
Replace the single-FROM-entry guard with per-entry validation. The validation must cover each comma-list entry
and that entry's explicit join factors, so the ADR does not accidentally open nested derived factors through
`from[1].joins` while preserving ADR 254's table-join body:
```rust
if select.from.is_empty() {
    return Err(unsupported("derived table subquery must have at least one FROM source"));
}
for source in &select.from {
    validate_derived_table_source(source)?;
}
```

The helper should accept ordinary `Table` factors, recurse only across the `TableWithJoins`'s factor positions,
and keep every existing fence: **nested derived tables**, **lateral/sampled derived factors**, and the other
unsupported source/clause forms still reject. Table-factor details already rejected by `lower_table_factor`
(arguments, hints, samples, etc.) do not need a second copy of that validation here; the point is to keep derived
source composition narrow, not to duplicate all table-factor checks.

### D2 — lowering: reuse, no new join code
`lower_derived_table` already calls `self.lower_select(select, cte)?`, and `lower_select` already folds
`from.iter().skip(1)` into the left-deep `Cross` chain that `recognize_cross_equi_joins` keys. So once the
validator accepts the comma list, the body lowers through the **shared** path (the same capability CTEs use),
presented as the named relation (`shipping`). Verify it composes; add no bespoke join logic.

### Architecture guard
Reconcile (don't delete) any architecture guard that pinned the exact single-FROM-entry reject string; retarget it
to the new per-entry rule.

## Scope / boundaries
- **Derived-body comma-list FROM only.** Nested derived tables, lateral, CTE expansion, general
  source-broadening, join reordering / cost work — all out of scope.
- **The self-join is the NEXT wall, not this cut.** q07's body also has `nation n1, nation n2` (two aliases of one
  table). Once the comma-list body lowers, the comma-join path *reaches* the self-join — which may bind (q07
  advances / greens) or wall → **ADR 295 = self-join alias recognition**. **Verify-first: re-measure q07's next
  row; record it honestly. Do not attempt the self-join here** (no scope creep, even if a "very small" fix looks
  implied).
- **Keyed, not cartesian** — the body's comma-join must key via `recognize_cross_equi_joins` (the q14/q17 lesson);
  a tiny-scale-only cartesian pass is not the win.
- Engine cut (`crates/toron/src/sql_planner.rs`); reachability-frontier, not q07 correctness.

## Acceptance
- **Focused engine test (isolates 294 from 295):** a derived table with a multi-FROM-entry comma-join body, **no
  self-join**, an equality predicate — binds + executes correct rows, e.g.
  `SELECT … FROM (SELECT … FROM supplier, customer WHERE s_nationkey = c_nationkey) AS d`. Discriminating: rejects
  pre-cut at `validate_derived_select_shape`.
- **Regression fences:** a nested-derived body still rejects; a nested/lateral derived factor in a later
  comma-list entry or explicit join factor still rejects; ADR 254 single-FROM-entry join-bodied derived tests
  unchanged.
- **q07 advances past `unsupported_derived_multi_source`** — verify-first records the new measured row (the
  self-join wall → ADR 295, a further wall, or `pass@execute` keyed at sf=0.01).
- **No regression:** full `-p toron --no-fail-fast` green (`validate_derived_select_shape` is shared by all
  derived/CTE paths); reach (q01-06/10/12/14/17/20) + correctness rail unchanged.

## Forward arc
Clears the derived comma-list FROM body (also unblocks **q08**, same shape). Then q07's next wall (likely the
self-join → ADR 295 — the high-value capability unblocking q07/q08/q21). After that arc, back to the 2:1
correctness cadence (q10/q20 catchup candidates).

## Why toron-home
A `crates/toron` planner cut (`validate_derived_select_shape` + the shared derived-body lowering); q07's row flips
in `toron-tpch` with no harness logic change. No Wendelon / cross-ring surface.

## Council provenance
3/3 (Claude/Codex/Gemini). Convergence: a **validator relaxation** reusing `lower_select` + ADR 270 recognition,
not a new join engine; keep nested-derived/lateral fenced; **self-join deferred to ADR 295** (measure-don't-predict
— verify-first re-measures q07); focused test *without* a self-join to isolate this cut; reconcile (not remove)
any guard on the single-FROM-entry reject (Codex). Full `-p toron` gate — this is a shared planner cut, not
harness-only.
