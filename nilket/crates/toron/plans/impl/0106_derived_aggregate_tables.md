# Impl Plan — ADR 106: Derived Aggregate Tables

**Migration for** `plans/adr/0106_derived_aggregate_tables.md`. **Scope:** Toron `sql_planner.rs` (relax two
fences) + a small `storage.rs` empty-projection row-count fix (Phase 2b) + behavior tests + guard
reconciliation. **Non-scope:** no `LogicalPlan`/`ExecutionPlan` variant, no binder semantics, no
executor/aggregate-kernel feature, no PBC/coordinator/Wendelon/`information_schema` surface, no storage
*contract* change (the Phase 2b fix is an adjacent in-memory-scan correctness fix, not a storage contract),
no `lower_query` delegation, no derived ORDER BY/LIMIT, no joins-around/inside or nested derived. A planner
fence-lift over the existing aggregate substrate.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Derived entry | `lower_derived_table` (`sql_planner.rs:3317`) — validate shapes → `lower_select` → `aggregate_output_catalog.is_some()` reject (`:3328`) |
| Structural gatekeeper | `validate_derived_select_shape` (`:5218-5259`) — fences group_by (`group_by_is_empty` `:5224`)/having (`:5228`)/agg-projection (`:5240`) **and** from-arity (`:5245`)/joins-inside (`:5251`)/nested-derived (`:5256`)/non-table source (`:5257`) |
| Query-shape fence (keep) | `validate_derived_query_shape` rejects `order_by`/`limit`/`with`/… → derived ORDER BY/LIMIT stay out |
| Aggregate lowering (reuse) | `lower_select` grouped `:2642-2693`, global `:2696-2736` → `Aggregate`(+Exchange) → `Filter`(HAVING `:2673`/`:2714`) → `Project` |
| Catalog discardable | `aggregate_output_catalog` consumed only by `lower_query` ORDER BY (`:2460`); derived ORDER BY fenced, HAVING already in plan |
| Alias rename (reuse) | `LogicalPlan::Alias` + `bind_alias_column_rename` (104), generic over any bound input; 105 coalesces a redundant adjacent `Project` |
| ADR 100 guard to retarget | `adr100_derived_table_source_is_narrow_not_general_subquery_support` (`tests/architecture/predicate_pushdown.rs:1583`) asserts `projection_contains_aggregate_call` + `group_by_is_empty` present |

## Phase 1 — Relax aggregate conditions in `validate_derived_select_shape` (`:5218`)

Remove **only** the three aggregate rejections; keep all structural/non-aggregate fences.

```rust
// in the boolean rejection chain (~:5219-5239): DELETE these two lines
        || !group_by_is_empty(&select.group_by)   // remove — GROUP BY now allowed
        || select.having.is_some()                // remove — HAVING now allowed

// the projection check (~:5240): drop the aggregate clause, keep window
        if projection_contains_window_function(&select.projection) {
            return Err(unsupported("unsupported derived table projection"));
        }
```

**Keep** unchanged: distinct/top/into/lateral_views/prewhere/cluster_by/distribute_by/sort_by/named_window/
qualify/value_table_mode/connect_by/non-standard flavor; `from.len() != 1` (`:5245`); joins-inside (`:5251`);
nested-derived (`:5256`); non-table source (`:5257`).

## Phase 2 — Drop the post-`lower_select` aggregate rejection in `lower_derived_table` (`:3324-3331`)

```rust
// before:
        let LoweredSelect {
            plan,
            aggregate_output_catalog,
        } = self.lower_select(select)?;
        if aggregate_output_catalog.is_some() {
            return Err(unsupported("aggregate derived tables are not supported"));
        }
        alias_plan(plan, Some(alias))

// after:
        // Aggregate (grouped + global) derived tables are admitted: lower_select already
        // builds Aggregate -> Filter(HAVING) -> Project. The catalog feeds only lower_query's
        // ORDER BY (fenced for derived sources), so drop it.
        let LoweredSelect { plan, .. } = self.lower_select(select)?;
        alias_plan(plan, Some(alias))
```

The `validate_derived_query_shape` / `validate_derived_select_shape` calls stay (D4). No `lower_query`
delegation.

## Phase 2b — Empty-projection row-count fix in `scan_in_memory_batch` (`storage.rs`)

Surfaced by the global-aggregate-derived case: `SELECT n FROM (SELECT COUNT(*) AS n FROM t) AS p` pushes an
**empty** column projection to the scan, and `batch.project(&[])` drops the row count → `COUNT(*)` returns
`0` instead of the true count. Add a generic helper and route the scan through it:

```rust
fn project_record_batch(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch, StorageError> {
    if indices.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        return Ok(RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options)?);
    }
    Ok(batch.project(indices)?)
}
```

Replace the `batch = batch.project(&indices)?` call in `scan_in_memory_batch` with
`batch = project_record_batch(&batch, &indices)?`. Behavior-correcting, generic (any zero-column scan
projection), no new storage contract (§ADR Adjacent fix). Mirror the same empty-projection branch in the
test `FixtureScan` (`tests/group_by_test.rs`) so `global_aggregate_derived_table_binds_and_executes`
(`n = 6`) passes.

## Phase 3 — Behavior tests

Split behavior tests by existing fixture ownership:

- In `tests/group_by_test.rs`, use the existing `sales` fixture, `plan_sql`, and `run_logical` helpers for
  execution-level aggregate behavior:
  - `aggregate_derived_table_binds_and_executes` — `SELECT bucket, total FROM (SELECT region AS bucket,
    SUM(amount) AS total FROM sales GROUP BY region) AS p WHERE total > 10 ORDER BY bucket`; expected rows
    `east = 40`, `west = 20`.
  - `global_aggregate_derived_table_binds_and_executes` — `SELECT n FROM (SELECT COUNT(*) AS n FROM sales)
    AS p`; expected one row, `n = 6`.
  - `aggregate_derived_having_binds_and_executes` — `SELECT bucket, n FROM (SELECT region AS bucket,
    COUNT(*) AS n FROM sales GROUP BY region HAVING COUNT(*) > 2) AS p`; expected row `east = 3`.
- In `tests/logical_type_ddl_test.rs`, near the ADR 100/104/105 derived and alias tests, use
  `logical_type_writer_coordinator()` + `setup_person_id_people` + `bind_sql` for alias and refinement
  behavior:
  - `aggregate_derived_alias_columns_rename_outputs` — `SELECT pid, total FROM (SELECT id, COUNT(*) AS n
    FROM people GROUP BY id) AS p(pid, total)`: names `[pid, total]`; assert the generic alias-column path
    handles aggregate output schemas. Add a project-count assertion only if the chosen SQL deliberately
    creates an inner final `Project` adjacent to the alias rename.
  - `aggregate_derived_outputs_are_not_refinement_transparent` — a derived aggregate with a `WHERE` on an
    output: `find_empty_relation` None, `contains_scan` true, and a small `contains_aggregate` helper true.

**Retarget** `derived_table_source_scope_rejects_general_subquery_features`: remove the two aggregate
reject-cases (`SELECT id FROM (SELECT COUNT(*) AS id FROM people) AS p WHERE id < 0` and
`SELECT id FROM (SELECT id, COUNT(*) AS n FROM people GROUP BY id) AS p WHERE id < 0`); **keep**
missing-alias, lateral, nested-derived, joins-inside, joins-around, derived `ORDER BY`/`LIMIT`, `WITH`,
`UNION`.

## Phase 4 — Architecture guards (`tests/architecture/predicate_pushdown.rs`)

**Retarget (D9)** `adr100_derived_table_source_is_narrow_not_general_subquery_support`: in its
`validate_derived_select_shape` required-marker loop (`:1617-1624`), **remove** `"projection_contains_aggregate_call"`
and `"group_by_is_empty"`; **keep** `"projection_contains_window_function"`, `"select.from.len() != 1"`,
`"!source.joins.is_empty()"`, `"TableFactor::Derived"`, `"unsupported derived table source"`, and the
query-shape/derived-arm markers.

**Add:**
- `adr106_derived_aggregates_reuse_existing_aggregate_ir` — `ir.rs`/`ir/execution.rs` enums gain no variant;
  `lower_derived_table` constructs no new plan node.
- `adr106_lower_derived_table_lifts_only_aggregate_fence` — `lower_derived_table` contains
  `validate_derived_query_shape` and `validate_derived_select_shape`, and does **not** contain `lower_query(`.
- `adr106_general_derived_source_fences_remain` — `validate_derived_select_shape` still contains
  `"nested derived tables"`, `"joins inside derived tables"`, `"select.from.len() != 1"`;
  `validate_derived_query_shape` still rejects `order_by`/`limit`.
- `adr106_alias_column_rename_remains_generic_bound_project` — `lower_derived_table` ends in
  `alias_plan(plan, Some(alias))`; `bind_alias_column_rename` unchanged.
- `adr106_no_ir_exec_pbc_storage_wendelon_information_schema_surface` — no ADR106 markers in `ir.rs` enums,
  `exec.rs`, `pbc.rs`, `storage.rs`, `coordinator.rs`, Wendelon sources, or the information-schema binder block.

Regression (stay green): 100/103/104 derived/alias gates, 105 coalescing gates, `alias_columns_*`, and the
`group_by_test` aggregate suite.

## Phase 5 — Verification

```sh
cargo fmt
cargo test -p toron --test group_by_test aggregate_derived
cargo test -p toron --test logical_type_ddl_test aggregate_derived
cargo test -p toron --test logical_type_ddl_test derived_table_source_scope
cargo test -p toron --test logical_type_ddl_test alias_columns
cargo test -p toron --test group_by_test
cargo test -p toron --test architecture_test adr106
cargo test -p toron --test architecture_test adr105
cargo test -p toron --test architecture_test adr104
cargo test -p toron --test architecture_test adr100
cargo test -p toron                                  # full suite green, no red guard
git diff --check
```

## Acceptance

Grouped and global aggregate-rooted derived tables bind and execute on the existing `Aggregate` substrate;
alias columns rename aggregate outputs via the 104 path (105 coalesces redundancy); inner HAVING resolves
against the inner aggregate catalog; aggregate outputs are non-transparent for elision; the general-derived
fences remain; the ADR 100 guard and the source-scope behavior test are reconciled in-change; no
IR/binder/executor/PBC/storage/Wendelon/`information_schema` surface change; Toron suite green.
