# Implementation Plan: ADR 036 — SQL `DELETE WHERE`

**Companion ADR:** `plans/adr/036_sql_delete_where.md`

## Goal

Implement local SQL `DELETE FROM table [WHERE
predicate]` as a narrow composition over the
existing ADR 035 row-identity / predicate-delete
substrate:

- SQL planner accepts the narrow DELETE surface;
- binder resolves target identity (Shape A) +
  validates Boolean predicate + classifies PBC
  pushdown + preserves residual for Toron
  evaluation;
- ADR 035 `coordinator/delete.rs` materializer
  refactored into a helper split that gives ADR
  036 a safe residual-filter insertion point;
- `Executor::execute_bound`
  `ExecutionPlan::Filter` arm refactored to call
  a new shared `pub(crate)
  filter_batches_with_bound_predicate` helper
  with byte-identical preservation;
- local coordinator wires DELETE through the
  refactored helpers with `runtime_filter: None`
  discipline and no SELECT retry loop;
- defensive executor + fragmenter + walker leaves
  + distributed reject mirror prior DML cycle
  pattern;
- SQL row count returned as a single-row
  `RecordBatch` with `deleted: Int64`.

This is a composition ADR over the locked ADR 035
substrate, not a substrate cycle.

Preserve:

- All ADR 029 catalog metadata-only invariants
  and storage-synthetic-name reject discipline.
- All ADR 030 VALUES INSERT architecture guards;
  `ExecutionPlan::Insert` stays expression-free.
- All ADR 030 D7 Toron-side synthetic-column
  construction prohibition.
- All ADR 030 D13 distributed-write reject
  pattern (extended to DELETE via D10).
- All ADR 031 `BoundExprProgram` invariants.
- All ADR 032 InsertSelect architecture guards;
  `materialize_insert_select_batch` still returns
  `Ok(None)` for zero rows.
- All ADR 033 CTAS architecture guards;
  `materialize_empty_record_batch` sibling
  helper.
- All ADR 034 catalog `drop_table` invariants +
  `lower_ddl_table_name` neutral DDL helper.
- All ADR 035 substrate invariants:
  `RowIdentityProjection` enum + `ScanRequest::row_identity`
  field; `DeleteRowsRequest` / `DeleteRowsOutcome` /
  `StorageDeleteRows` trait; in-memory tombstones
  + identity layout; Wendelon delete adapter; the
  18 architecture guards.
- ADR 035 helper names
  `materialize_delete_row_key_ids` and
  `execute_delete_with_candidates` are **load-
  bearing**; preserve through Phase 3 split.
- Existing `ExecutionPlan::Filter` SELECT WHERE
  behavior **byte-identical** through Phase 4
  helper extraction. Reference fixture pass-count
  check (Phase 4 verification).
- ADR 015 cycle properties: narrow-detector
  architecture guards; named-reject sites; no
  silent acceptance.

No new SQL syntax beyond bare `DELETE FROM table
[WHERE predicate]`; no new IR expression variant;
no new aggregate kernel; no PBC opcode change; no
fragmenter detector change for SELECT paths; no
cross-ring engagement; no Wendelon-side change;
no new `StorageWrite` overload; no new
`StorageDeleteRows` method; no new `StorageError`
variant; no `blake3` dependency; no external
allocator crate added; no storage reclaim trait
or helper introduced.

**Nine-phase patch order.** Phases 1-2 are
planner + IR + binder; Phase 3 is ADR 035 helper
split + `delete_count_batch`; Phase 4 is the
residual filter helper extraction (byte-identical
refactor); Phase 5 is local coordinator routing;
Phase 6 is distributed reject + fragmenter +
executor defensive arms + walker leaves; Phase 7
is functional tests; Phase 8 is architecture
guards; Phase 9 is full verification. Promotion-
time housekeeping (Phase 10) is out of
implementation PR scope.

## Implementation constraints

- **D1:** Narrow SQL surface only. Reject
  USING / RETURNING / ORDER BY / LIMIT / multi-
  target / alias / non-table factors / non-
  default `TableFactor::Table` options with
  stable per-clause diagnostics.
- **D2:** Exhaustive `Delete` (7 fields) + accepted
  `TableFactor::Table` (9 fields) destructure, no
  `..`.
- **D3:** Neutral DML table-name helper. Do not
  reuse `lower_ddl_table_name`.
- **D4:** Shape A `ExecutionPlan::Delete`
  identity carrier. No `FilterDisposition` field.
- **D5:** Binder accepts non-PBC predicates;
  residual filter applies. **Do not reject
  `compile_pbc(_) -> None`.**
- **D6:** `filter_batches_with_bound_predicate`
  helper extracted from `ExecutionPlan::Filter`
  with byte-identical preservation. Existing
  diagnostic `"filter predicate must return
  boolean"` preserved.
- **D7:** Coordinator delete-path ScanRequest
  contains `runtime_filter: None` (positive +
  negative against context inheritance); residual
  filter applies before identity extraction.
- **D8:** ADR 035 helper split preserves
  `materialize_delete_row_key_ids` wrapper name.
  `delete_count_batch` lives in
  `coordinator/delete.rs` (not
  `coordinator/write.rs`).
- **D9:** Result batch: one row, `Int64` column
  named `deleted`, value from
  `i64::try_from(outcome.deleted)`. No other
  counters exposed.
- **D10:** Distributed reject before
  `fragment_plan`; `FragmentError::DeleteNotFragmentable`.
- **D11:** Defensive executor / walker leaves +
  `"delete"` telemetry label.
- **D12:** Identity remains SQL-invisible (ADR
  029 D12 + ADR 030 D7 carry forward unchanged).
- **D13:** No `blake3` / external allocator /
  storage reclaim trait.
- **D14:** ADR 037 sequencing pin for cross-ring
  distributed-write-context substrate.
- **Claude iter-1 A3 carry-forward:** New test
  file `crates/toron/tests/delete_test.rs` (one
  statement form, one test file).

## Touchpoint sweep

| File | Phase |
|---|---|
| `crates/toron/src/ir.rs` | 1 |
| `crates/toron/src/ir/execution.rs` | 1, 6 (walkers) |
| `crates/toron/src/sql_planner.rs` | 1 (lowerer + neutral DML name helper + walker leaves) |
| `crates/toron/src/binder.rs` | 2 (`bind_delete` + `require_single_boolean_root` + arms in three walkers) |
| `crates/toron/src/coordinator/delete.rs` | 3 (helper split + `delete_count_batch`); 5 (routing additions if helpers live here) |
| `crates/toron/src/exec.rs` | 4 (`filter_batches_with_bound_predicate` extraction); 6 (executor defensive arms) |
| `crates/toron/src/coordinator.rs` | 5 (local DML intercept + routing through Phase 3 + 4 helpers); 6 (distributed reject before fragmenter) |
| `crates/toron/src/distributed/fragment.rs` | 6 (`FragmentError::DeleteNotFragmentable` + reject before join/exchange) |
| `crates/toron/src/distributed/scheduler.rs` | 6 (walker leaf) |
| `crates/toron/src/server/mod.rs` | 6 (walker leaves + `operator_kind_label` → `"delete"`) |
| `crates/toron/tests/delete_test.rs` (NEW) | 7 |
| `crates/toron/tests/architecture_test.rs` | 8 |
| `crates/toron/tests/wendelon_storage_execution_test.rs` | 6 (exhaustive helper arm if compiler flags); 7 (optional Wendelon SQL DELETE fixture) |

## Helper reuse summary

| Helper | Source | DELETE use |
|---|---|---|
| `Catalog::resolve_table` | ADR 029 | Reused unchanged (binder calls to resolve target identity) |
| `bind_expr_program` | ADR 031 | Reused unchanged (binder binds optional predicate) |
| `compile_pbc` | ADR 031 / existing | Reused unchanged (binder attempts full-predicate pushdown) |
| `scan_profile_hint` / equivalent primary-key-equality logic | Existing SELECT path | Reused unchanged |
| `materialize_delete_row_key_ids` | ADR 035 | **Refactored into wrapper** delegating to `materialize_delete_candidate_batches` + `extract_delete_row_key_ids` (wrapper name preserved for ADR 035 guards + tests) |
| `materialize_delete_candidate_batches` | NEW (Phase 3) | New; extracted from ADR 035 materializer |
| `extract_delete_row_key_ids` | NEW (Phase 3) | New; extracted from ADR 035 materializer |
| `execute_delete_with_candidates` | ADR 035 | Reused unchanged (empty-set short-circuit + delete invocation) |
| `delete_count_batch` | NEW (Phase 3) | New; located in `coordinator/delete.rs` |
| `filter_batches_with_bound_predicate` | NEW (Phase 4) | New; extracted from `Executor::execute_bound` `ExecutionPlan::Filter` arm with byte-identical preservation |
| `require_single_boolean_root` | NEW (Phase 2) | New private binder helper for Boolean-root validation |
| `lower_table_ref_object_name(name, context)` | NEW (Phase 1) | New neutral DML name helper |
| `lower_delete` | NEW (Phase 1) | SQL planner-side DELETE lowerer |
| `bind_delete` | NEW (Phase 2) | Binder-side DELETE arm |
| `FragmentError::DeleteNotFragmentable` | NEW (Phase 6) | Fragmenter-side reject variant |

Three new variants (logical IR + execution IR +
fragment error) + four new helpers
(`materialize_delete_candidate_batches`,
`extract_delete_row_key_ids`, `delete_count_batch`,
`filter_batches_with_bound_predicate`) + two new
private functions (`bind_delete`, `lower_delete`)
+ one renamed/factored helper
(`lower_table_ref_object_name`) + one new binder
helper (`require_single_boolean_root`) + one new
telemetry label. Net new code is bounded and
sharply scoped.

## Phase 0 — Preflight audit

```sh
# Confirm DELETE symbols are not yet present
rg -n "Statement::Delete|LogicalPlan::Delete|ExecutionPlan::Delete" crates/toron/src crates/toron/tests
# Expected: zero hits (ADR 035 guard adr035_no_sql_delete_yet currently green)

# Confirm ADR 035 helpers are present and currently `#[allow(dead_code)]`
rg -n "materialize_delete_row_key_ids|execute_delete_with_candidates|#\[allow\(dead_code\)\]" crates/toron/src/coordinator/delete.rs
# Expected: both helpers present; #[allow(dead_code)] on materialize_delete_row_key_ids + execute_delete_with_candidates

# Confirm QueryCoordinator deleter field is `#[allow(dead_code)]`
rg -n "deleter|StorageDeleteRows" crates/toron/src/coordinator.rs
# Expected: deleter field with #[allow(dead_code)]

# Confirm binder Boolean validation surface (Codex iter-2 audit)
rg -n "DataType::Boolean|require_boolean|FilterDisposition|compile_pbc" crates/toron/src/binder.rs
# Expected: builder-level require_boolean for operators, but no BoundExprProgram root validator yet

# Confirm ExecutionPlan::Filter arm + existing diagnostic (Codex iter-2 audit)
rg -n "ExecutionPlan::Filter|filter predicate must return boolean|filter_record_batch" crates/toron/src/exec.rs
# Expected: ExecutionPlan::Filter arm with inlined bound filter loop + error string preserved

# Confirm sqlparser Delete + TableFactor::Table field count (Codex iter-2 audit)
cargo doc -p sqlparser --no-deps 2>&1 | grep -A 20 "struct Delete"
# Expected: 7 fields (tables, from, using, selection, returning, order_by, limit)

# Confirm no shadow dependencies
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits

# Confirm ADR 035 architecture guards green (baseline)
cargo test -p toron --test architecture_test adr035
```

Expected baseline:

- No existing DELETE symbols in production or
  tests.
- ADR 035 `coordinator/delete.rs` helpers present
  with `#[allow(dead_code)]`.
- `QueryCoordinator::deleter` field with
  `#[allow(dead_code)]`.
- `binder.rs` has builder-level `require_boolean`
  for operators, no `BoundExprProgram` root
  validator.
- `ExecutionPlan::Filter` arm has inlined bound
  filter loop with error string `"filter predicate
  must return boolean"`.
- sqlparser 0.54 `Delete` has 7 fields;
  `TableFactor::Table` has 9 fields.

Preserve verbatim:

- All ADR 029 / 030 / 031 / 032 / 033 / 034 / 035
  invariants.
- All existing SELECT / INSERT / CREATE / CTAS /
  DROP / DELETE substrate test fixtures.

## Phase 1 — IR variants + SQL planner

**Files:** `crates/toron/src/ir.rs`,
`crates/toron/src/ir/execution.rs`,
`crates/toron/src/sql_planner.rs`.

**Tasks:**

1. Add to `LogicalPlan`:

   ```rust
   Delete {
       table: TableRef,
       predicate: Option<Expr>,
   }
   ```

2. Add to `ExecutionPlan`:

   ```rust
   Delete {
       legacy_table_id: TableId,
       table: TableObjectId,
       fingerprint: PhysicalDescriptorFingerprint,
       schema: SchemaRef,
       predicate: Option<BoundExprProgram>,
       pbc_program: Option<PbcProgram>,
       profile_hint: QueryProfileHint,
   }
   ```

   **No `FilterDisposition` field. No boxed
   children.** Mirrors ADR 030 INSERT Shape A
   precedent.

3. Update `ExecutionPlan::schema()`:

   ```rust
   Self::Delete { .. } => result_count_schema_int64("deleted"),
   ```

   The `schema` field carried on `ExecutionPlan::Delete`
   is the target table's user schema for predicate
   evaluation and candidate scans; it is not the
   DELETE statement's output schema. DELETE is the
   first local DML in this series that returns a row
   count, so `ExecutionPlan::schema()` should reflect
   the `deleted: Int64` result instead of following
   INSERT / CTAS empty-result conventions.
   Add the private `result_count_schema_int64`
   helper in `ir/execution.rs` if no equivalent
   result-count schema helper already exists.

4. Extend `SqlPlanner::plan` statement dispatch:

   ```rust
   Statement::Delete(delete) => self.lower_delete(delete),
   ```

5. Update unsupported-statement diagnostic to
   include `DELETE`.

6. Add or factor `lower_table_ref_object_name(name:
   &ObjectName, context: &'static str) ->
   Result<TableRef, FrontendError>` (neutral DML
   helper):
   - accepts one-part (`name`) or two-part
     (`schema.name`) names;
   - rejects three-part names with diagnostic
     using the `context` parameter;
   - returns `TableRef::from_object_name(name)`.

7. Implement `lower_delete(&self, delete: &Delete)
   -> Result<LogicalPlan, FrontendError>`:

   ```rust
   let Delete {
       tables,
       from,
       using,
       selection,
       returning,
       order_by,
       limit,
   } = delete;

   if !tables.is_empty() {
       return Err(unsupported(
           "DELETE supports exactly one target table",
       ));
   }
   if using.is_some() {
       return Err(unsupported("unsupported DELETE clause: USING"));
   }
   if returning.is_some() {
       return Err(unsupported("unsupported DELETE clause: RETURNING"));
   }
   if !order_by.is_empty() {
       return Err(unsupported("unsupported DELETE clause: ORDER BY"));
   }
   if limit.is_some() {
       return Err(unsupported("unsupported DELETE clause: LIMIT"));
   }

   // Accept only FromTable::WithFromKeyword
   let target = match from {
       FromTable::WithFromKeyword(target) => target,
       FromTable::WithoutKeyword(_) => {
           return Err(unsupported("DELETE requires FROM keyword"));
       }
   };

   if target.len() != 1 {
       return Err(unsupported(
           "DELETE supports exactly one target table",
       ));
   }
   let table_with_joins = &target[0];
   if !table_with_joins.joins.is_empty() {
       return Err(unsupported(
           "unsupported DELETE target: joins not supported",
       ));
   }

   // Exhaustive TableFactor::Table destructure
   let TableFactor::Table {
       name,
       alias,
       args,
       with_hints,
       version,
       with_ordinality,
       partitions,
       json_path,
       sample,
   } = &table_with_joins.relation else {
       return Err(unsupported(
           "DELETE target must be a single table",
       ));
   };

   // Reject non-default TableFactor::Table options
   if alias.is_some() {
       return Err(unsupported(
           "unsupported DELETE target: table alias",
       ));
   }
   if args.is_some() {
       return Err(unsupported(
           "unsupported DELETE target: table arguments",
       ));
   }
   if !with_hints.is_empty() {
       return Err(unsupported(
           "unsupported DELETE target: table hints",
       ));
   }
   if version.is_some() {
       return Err(unsupported(
           "unsupported DELETE target: table version",
       ));
   }
   if *with_ordinality {
       return Err(unsupported(
           "unsupported DELETE target: WITH ORDINALITY",
       ));
   }
   if !partitions.is_empty() {
       return Err(unsupported(
           "unsupported DELETE target: partitions",
       ));
   }
   if json_path.is_some() {
       return Err(unsupported(
           "unsupported DELETE target: json path",
       ));
   }
   if sample.is_some() {
       return Err(unsupported(
           "unsupported DELETE target: sample",
       ));
   }

   let table = lower_table_ref_object_name(name, "DELETE")?;
   let predicate = selection
       .as_ref()
       .map(|expr| self.lower_expr(expr))
       .transpose()?;

   Ok(LogicalPlan::Delete { table, predicate })
   ```

8. Add `Delete` arms to walker helpers in
   `sql_planner.rs`:
   - `plan_contains_join` — returns `false`;
   - `plan_qualifiers` — returns empty.

**Verification:**

```sh
cargo test -p toron sql_planner::tests
```

Existing CREATE TABLE / INSERT / CTAS / DROP TABLE
tests remain green.

## Phase 2 — Binder

**File:** `crates/toron/src/binder.rs`.

**Tasks:**

1. Audit existing Boolean validation surface:

   ```sh
   rg -n "DataType::Boolean|require_boolean" crates/toron/src/binder.rs
   ```

   Expected: builder-level `require_boolean` for
   operator type checking, but no
   `BoundExprProgram` root validator.

2. Factor a new private helper:

   ```rust
   fn require_single_boolean_root(
       program: &BoundExprProgram,
       context: &'static str,
   ) -> Result<(), BindingError> {
       if program.root_count() != 1 {
           return Err(BindingError::TypeMismatch {
               message: format!(
                   "{context} predicate must have exactly one root, found {}",
                   program.root_count()
               ),
           });
       }
       let Some(root_type) = program.root_type(0) else {
           return Err(BindingError::TypeMismatch {
               message: format!("{context} predicate root type is missing"),
           });
       };
       if root_type != &DataType::Boolean {
           return Err(BindingError::TypeMismatch {
               message: format!(
                   "{context} predicate must return Boolean, found {:?}",
                   root_type
               ),
           });
       }
       Ok(())
   }
   ```

   This uses the current `BoundExprProgram`
   accessors, `root_count()` and `root_type(0)`.

3. Add a `LogicalPlan::Delete` arm in
   `Binder::bind_plan`:

   ```rust
   LogicalPlan::Delete { table, predicate } => {
       self.bind_delete(table, predicate.as_ref()).await
   }
   ```

4. Implement `bind_delete`:

   ```rust
   async fn bind_delete(
       &self,
       table: &TableRef,
       predicate: Option<&Expr>,
   ) -> Result<ExecutionPlan, BindingError> {
       let binding = self.catalog.resolve_table(table).await?;

       let bound_predicate = match predicate {
           Some(expr) => {
               let program = bind_expr_program(
                   std::slice::from_ref(expr),
                   &binding.schema,
                   self.config,
               )?;
               require_single_boolean_root(&program, "DELETE WHERE")?;
               Some(program)
           }
           None => None,
       };

       let pbc_program = bound_predicate
           .as_ref()
           .and_then(compile_pbc);

       let profile_hint = match bound_predicate.as_ref() {
           Some(program) => {
               let scan = scan_plan(binding.clone(), None);
               scan_profile_hint(program, &scan).unwrap_or(QueryProfileHint::Scan)
           }
           None => QueryProfileHint::Scan,
       };

       Ok(ExecutionPlan::Delete {
           legacy_table_id: binding.legacy_table_id,
           table: binding.table,
           fingerprint: binding.fingerprint,
           schema: binding.schema.clone(),
           predicate: bound_predicate,
           pbc_program,
           profile_hint,
       })
   }
   ```

5. **Do not reject when `compile_pbc` returns
   `None`.** This is the residual-
   filter path; the bound predicate is preserved
   for Toron evaluation.

6. Add `Delete` arms to:
   - `collect_qualifiers` — DELETE is a leaf, no
     source qualifier contribution;
   - `terminal_exchange` — DELETE is not a
     terminal exchange; return `None`;
   - `optimize_for_required` — return the DELETE
     plan unchanged with an empty
     `OrdinalRemap::new()`.

**Verification:**

```sh
cargo test -p toron binder::tests
```

## Phase 3 — `coordinator/delete.rs` helper split + `delete_count_batch`

**File:** `crates/toron/src/coordinator/delete.rs`.

**Tasks:**

1. Refactor the existing
   `materialize_delete_row_key_ids` into a helper
   split:

   ```rust
   #[allow(dead_code)]  // kept until Phase 5 removes
   pub(crate) async fn materialize_delete_candidate_batches(
       storage: &dyn StorageScan,
       scan_request: ScanRequest,
   ) -> Result<Vec<RecordBatch>, StorageError> {
       if scan_request.row_identity != RowIdentityProjection::Append {
           return Err(StorageError::Internal {
               message: "delete candidate materializer requires row_identity=Append".to_string(),
           });
       }
       super::materialize_scan_stream(storage.scan(scan_request).await?).await
   }

   #[allow(dead_code)]
   pub(crate) fn extract_delete_row_key_ids(
       batches: &[RecordBatch],
   ) -> Result<Vec<Identity>, StorageError> {
       let mut row_key_ids = Vec::new();
       for batch in batches {
           // ... existing validation + extraction logic from
           //     the current materialize_delete_row_key_ids body ...
       }
       Ok(row_key_ids)
   }

   #[allow(dead_code)]
   pub(crate) async fn materialize_delete_row_key_ids(
       storage: &dyn StorageScan,
       scan_request: ScanRequest,
   ) -> Result<Vec<Identity>, StorageError> {
       let batches = materialize_delete_candidate_batches(storage, scan_request).await?;
       extract_delete_row_key_ids(&batches)
   }
   ```

2. Preserve all ADR 035 validation diagnostics in
   `extract_delete_row_key_ids`:
   - missing identity column;
   - wrong final column name;
   - non-`FixedSizeBinary(32)` type;
   - wrong array type;
   - null identity values;
   - non-32-byte identity values.

3. Add `delete_count_batch`:

   ```rust
   pub(crate) fn delete_count_batch(
       count: usize,
   ) -> Result<RecordBatch, ExecutionError> {
       let value = i64::try_from(count).map_err(|_| ExecutionError::Kernel {
           message: format!("delete count {count} exceeds Int64 range"),
       })?;
       let schema = Arc::new(Schema::new(vec![Field::new(
           "deleted",
           DataType::Int64,
           false,
       )]));
       let array = Arc::new(Int64Array::from(vec![value])) as ArrayRef;
       RecordBatch::try_new(schema, vec![array]).map_err(|err| ExecutionError::Compute(err.into()))
   }
   ```

4. The `#[allow(dead_code)]` annotations on
   `materialize_delete_row_key_ids` and
   `execute_delete_with_candidates` are kept here
   in Phase 3 and removed in Phase 5 once
   coordinator routing references them.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron coordinator::delete::tests
```

ADR 035 tests for `materialize_delete_row_key_ids`
remain green via the preserved wrapper name.

## Phase 4 — `filter_batches_with_bound_predicate` extraction

**File:** `crates/toron/src/exec.rs`.

**Pre-extraction reference fixture capture:**

```sh
cargo test -p toron --test architecture_test
cargo test -p toron sort_ordering
cargo test -p toron group_by
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron --test wendelon_storage_execution_test
```

Note pass counts per suite.

**Tasks:**

1. Extract the bound filter logic from
   `Executor::execute_bound`'s
   `ExecutionPlan::Filter` arm into a new
   `pub(crate)` method:

   ```rust
   pub(crate) fn filter_batches_with_bound_predicate(
       &self,
       predicate: &BoundExprProgram,
       batches: Vec<RecordBatch>,
       context: &'static str,
   ) -> Result<Vec<RecordBatch>, ExecutionError> {
       let mut filtered = Vec::with_capacity(batches.len());
       for batch in batches {
           let mask = self.evaluate_bound_single_root(predicate, &batch, context)?;
           let array = mask
               .as_any()
               .downcast_ref::<BooleanArray>()
               .ok_or_else(|| ExecutionError::InvalidExpression {
                   message: format!("{context} must return boolean"),
               })?;
           let filtered_batch = filter_record_batch(&batch, array)?;
           filtered.push(filtered_batch);
       }
       Ok(filtered)
   }
   ```

   This corresponds to the current inlined
   `ExecutionPlan::Filter` logic: evaluate one
   bound root, downcast to `BooleanArray`, and call
   `filter_record_batch`.

2. Add a narrow storage accessor used by the
   coordinator DELETE path:

   ```rust
   pub(crate) fn storage(&self) -> &dyn StorageScan {
       self.storage.as_ref()
   }
   ```

   This avoids adding a second scan-storage field
   to `QueryCoordinator`; the coordinator already
   owns an `Executor` configured with the read
   storage used by SELECT.

3. Refactor `ExecutionPlan::Filter` arm in
   `execute_bound` to call the helper with
   context `"filter predicate"`:

   ```rust
   ExecutionPlan::Filter { input, predicate, disposition } => {
       let batches = self.execute_bound(input, morsel).await?;
       if *disposition == FilterDisposition::FullyPushed {
           return Ok(batches);
       }
       self.filter_batches_with_bound_predicate(predicate, batches, "filter predicate")
   }
   ```

4. **Byte-identical preservation invariant:** the
   existing error message `"filter predicate must
   return boolean"` must be produced verbatim by
   the helper when called from the
   `ExecutionPlan::Filter` arm. Test fixture
   verification (Phase 7 + Phase 9) confirms
   pass-count parity.

**Post-extraction reference fixture verification:**

```sh
cargo test -p toron --test architecture_test
cargo test -p toron sort_ordering
cargo test -p toron group_by
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron --test wendelon_storage_execution_test
```

**Pass count per suite must match pre-extraction
reference exactly.** If any suite's pass count
differs, the extraction has changed behavior;
investigate before proceeding to Phase 5.

## Phase 5 — Local coordinator routing

**File:** `crates/toron/src/coordinator.rs`.

**Tasks:**

1. Extend the local DML coordinator-only intercept
   in `QueryCoordinator::execute_query`, while
   keeping DDL on the existing catalog helper:

   ```rust
   if matches!(
       plan,
       LogicalPlan::CreateTable { .. }
           | LogicalPlan::DropTable { .. }
   ) {
       let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
       return execute_catalog_plan(self.catalog.as_ref(), &bound).await;
   }

   if matches!(
       plan,
       LogicalPlan::Insert { .. }
           | LogicalPlan::InsertSelect { .. }
           | LogicalPlan::CreateTableAsSelect { .. }
           | LogicalPlan::Delete { .. }
   ) {
       let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
       return self.execute_write_plan(&bound).await;
   }
   ```

2. Implement `execute_delete` (or extend
   `execute_write_plan` with a `Delete` arm):

   ```rust
   async fn execute_delete(
       &self,
       plan: &ExecutionPlan,
   ) -> Result<Vec<RecordBatch>, QueryError> {
       let ExecutionPlan::Delete {
           legacy_table_id,
           table,
           fingerprint,
           schema,
           predicate,
           pbc_program,
           profile_hint,
       } = plan else {
           unreachable!("execute_delete called with non-Delete plan");
       };
       let context = self.executor.context();

       let scan_request = ScanRequest {
           session_id: context.session_id,
           query_id: context.query_id,
           tenant_id: context.tenant_id,
           table_id: *legacy_table_id,
           table_object_id: *table,
           physical_descriptor_fingerprint: *fingerprint,
           read_bound: context.read_bound,
           projected_columns: None,
           pbc_predicate: pbc_program.as_ref().map(|p| p.0.clone()),
           runtime_filter: None,
           row_identity: RowIdentityProjection::Append,
           profile_hint: Some(*profile_hint),
           trace_context: context.trace_context.clone(),
           morsel: None,
       };

       let mut batches = delete::materialize_delete_candidate_batches(
           self.executor.storage(),
           scan_request,
       )
       .await?;

       if let (Some(predicate), None) = (predicate.as_ref(), pbc_program.as_ref()) {
           batches = self.executor.filter_batches_with_bound_predicate(
               predicate,
               batches,
               "delete predicate",
           )?;
       }

       let row_key_ids = delete::extract_delete_row_key_ids(&batches)?;

       let outcome = delete::execute_delete_with_candidates(
           self.deleter.as_ref(),
           DeleteRowsRequest {
               session_id: context.session_id,
               query_id: context.query_id,
               tenant_id: context.tenant_id,
               table_id: *legacy_table_id,
               table_object_id: *table,
               physical_descriptor_fingerprint: *fingerprint,
               row_key_ids,
               profile_hint: Some(*profile_hint),
               trace_context: context.trace_context.clone(),
           },
       )
       .await?;

       Ok(vec![delete::delete_count_batch(outcome.deleted)?])
   }
   ```

3. **Critical correctness invariants** (architecture-
   guard enforced):
   - `runtime_filter: None` (not
     `self.context.runtime_filter` or
     `context.runtime_filter` inheritance);
   - residual filter applied **before**
     `extract_delete_row_key_ids` when
     `pbc_program.is_none()`;
   - no SELECT descriptor-evolution retry loop
     around the mutation (bind once, execute
     once).

4. Remove `#[allow(dead_code)]` annotations on:
   - `materialize_delete_row_key_ids` in
     `coordinator/delete.rs`;
   - `execute_delete_with_candidates` in
     `coordinator/delete.rs`;
   - `materialize_delete_candidate_batches` +
     `extract_delete_row_key_ids` if they were
     marked dead in Phase 3;
   - `QueryCoordinator::deleter` field.

**Verification:**

```sh
cargo test -p toron coordinator::tests
```

## Phase 6 — Distributed reject + fragmenter + executor defensive arms + walkers

**Files:**

- `crates/toron/src/coordinator.rs`
- `crates/toron/src/distributed/fragment.rs`
- `crates/toron/src/distributed/scheduler.rs`
- `crates/toron/src/server/mod.rs`
- `crates/toron/src/exec.rs`
- `crates/toron/tests/wendelon_storage_execution_test.rs`
  (exhaustive helper arm if compiler flags)

**Tasks:**

1. **Distributed coordinator**
   (`coordinator.rs`):
   - Bind first, recognize
     `ExecutionPlan::Delete`, reject before
     `fragment_plan` with diagnostic:

     ```text
     distributed DELETE execution is not wired to a write-context-bearing coordinator
     ```

2. **Fragmenter**
   (`distributed/fragment.rs`):
   - Add `FragmentError::DeleteNotFragmentable`
     variant with error string `"DELETE is a
     coordinator-only write operation"`;
   - Reject in `fragment_plan` **before**
     `contains_join(&plan)` and
     `count_exchanges(&plan)` (mirrors ADR 030 /
     032 / 033 / 034 reject ordering);
   - Walker leaves in `first_join`,
     `find_exchange`, `count_exchanges`,
     `first_scan_metadata`,
     `replace_exchange_with_scan`,
     `replace_first_join`: return None / 0 /
     unchanged for `ExecutionPlan::Delete`.

3. **Scheduler**
   (`distributed/scheduler.rs`):
   - Walker leaf in `first_scan_fingerprint`:
     return `None` for `ExecutionPlan::Delete`.

4. **Server** (`server/mod.rs`):
   - Walker leaves in `join_strategy_label`,
     `contains_join`, `contains_broadcast_join`,
     `scan_schema_for_table_id`: return None /
     false / None;
   - `operator_kind_label` returns `"delete"` for
     `ExecutionPlan::Delete` (distinct from
     `"insert"`, `"insert_select"`,
     `"create_table_as_select"`, `"drop_table"`,
     `"catalog"`).

5. **Executor** (`exec.rs`) defensive arms:
   - unbound `LogicalPlan::Delete` →
     `ExecutionError::NotImplemented { feature:
     "unbound DELETE execution" }`;
   - bound `ExecutionPlan::Delete` →
     `ExecutionError::NotImplemented { feature:
     "DELETE execution is coordinator-only" }`.

6. **Test helper exhaustiveness**
   (`wendelon_storage_execution_test.rs`): add
   `Delete` arm if the compiler flags any
   exhaustive test-side match.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --test distributed_process_test
```

## Phase 7 — Functional tests

**File:** new
`crates/toron/tests/delete_test.rs` (Claude
iter-1 A3 file-naming convention).

14 required functional fixtures + 1 optional
Wendelon end-to-end fixture:

### Fixture 1 — `planner_lowers_delete_where`

```rust
#[test]
fn planner_lowers_delete_where() {
    // DELETE FROM t WHERE id = 1
    // DELETE FROM app.t WHERE col > 0
    // assert LogicalPlan::Delete { table, predicate: Some(Expr::...) } fields
}
```

### Fixture 2 — `planner_lowers_delete_without_where`

```rust
#[test]
fn planner_lowers_delete_without_where() {
    // DELETE FROM t
    // assert LogicalPlan::Delete { table, predicate: None }
}
```

### Fixture 3 — `planner_rejects_unsupported_delete_shapes`

```rust
#[test]
fn planner_rejects_unsupported_delete_shapes() {
    // DELETE FROM t USING b WHERE t.id = b.id           -> USING reject
    // DELETE FROM t WHERE id = 1 RETURNING *            -> RETURNING reject
    // DELETE FROM t WHERE id = 1 ORDER BY col           -> ORDER BY reject
    // DELETE FROM t WHERE id = 1 LIMIT 10               -> LIMIT reject
    // DELETE FROM a, b WHERE a.id = b.id                -> multi-target reject
    // DELETE FROM t AS x WHERE x.id = 1                 -> alias reject
    // DELETE FROM t(args) WHERE id = 1                  -> table function reject (if parser allows)
}
```

### Fixture 4 — `binder_resolves_delete_shape_a_identity`

```rust
#[tokio::test]
async fn binder_resolves_delete_shape_a_identity() {
    // setup MockCatalog with table "t" + schema
    // bind LogicalPlan::Delete { table: TableRef("t"), predicate: Some(...) }
    // assert ExecutionPlan::Delete {
    //   legacy_table_id, table (TableObjectId), fingerprint, schema,
    //   predicate: Some(_), pbc_program: Some(_),
    //   profile_hint: ...
    // }
    // assert legacy_table_id matches MockCatalog binding
}
```

### Fixture 5 — `binder_rejects_unknown_delete_column`

```rust
#[tokio::test]
async fn binder_rejects_unknown_delete_column() {
    // DELETE FROM t WHERE missing_col > 0
    // assert BindingError::UnknownColumn { column: "missing_col" }
}
```

### Fixture 6 — `binder_rejects_non_boolean_delete_predicate`

```rust
#[tokio::test]
async fn binder_rejects_non_boolean_delete_predicate() {
    // DELETE FROM t WHERE col + 1
    // assert BindingError::TypeMismatch via require_single_boolean_root
    // assert message contains "DELETE WHERE" and "Boolean"
}
```

### Fixture 7 — `coordinator_deletes_rows_with_pbc_predicate`

```rust
#[tokio::test]
async fn coordinator_deletes_rows_with_pbc_predicate() {
    // setup InMemoryTableStorage; write 5 rows (id: 1..=5)
    // execute DELETE FROM t WHERE id > 2
    // assert result has one batch, schema { deleted: Int64 }, value 3
    // subsequent SELECT * FROM t returns 2 rows (id 1, 2)
}
```

### Fixture 8 — `coordinator_deletes_rows_with_residual_predicate` (R1 critical)

```rust
#[tokio::test]
async fn coordinator_deletes_rows_with_residual_predicate() {
    // setup InMemoryTableStorage; write 5 rows (id 1..=5, amount 1..=5)
    // execute DELETE FROM t WHERE amount + 1 > 4
    //   (non-PBC arithmetic: 2 rows match — amount in {4, 5} both have amount+1 > 4? Actually: 3, 4, 5)
    //   amount+1 > 4 -> amount > 3 -> amount in {4, 5} matches
    // assert result `deleted = 2` (not 5 — full-table delete is the regression to catch)
    // subsequent SELECT returns 3 rows (id 1, 2, 3 with amount 1, 2, 3)
    //
    // CRITICAL R1 CORRECTNESS FIXTURE — proves residual predicate path
    // does not degrade to full-table delete
}
```

### Fixture 9 — `coordinator_deletes_all_rows_without_where`

```rust
#[tokio::test]
async fn coordinator_deletes_all_rows_without_where() {
    // setup; write 5 rows
    // execute DELETE FROM t
    // assert result `deleted = 5`
    // subsequent SELECT returns 0 rows
}
```

### Fixture 10 — `coordinator_delete_zero_matches_returns_zero_count`

```rust
#[tokio::test]
async fn coordinator_delete_zero_matches_returns_zero_count() {
    // setup; write 5 rows
    // execute DELETE FROM t WHERE id > 100
    // assert result `deleted = 0`
    // subsequent SELECT returns 5 rows
}
```

### Fixture 11 — `coordinator_repeated_delete_reports_deleted_only`

```rust
#[tokio::test]
async fn coordinator_repeated_delete_reports_deleted_only() {
    // setup; write 5 rows
    // execute DELETE FROM t WHERE id = 1 — first time
    // assert result `deleted = 1`
    // execute DELETE FROM t WHERE id = 1 — second time
    // assert result `deleted = 0` (already-deleted not counted in SQL row count)
    // proves SQL row count maps only to .deleted (not .already_deleted)
}
```

### Fixture 12 — `coordinator_empty_candidate_set_short_circuits`

```rust
#[tokio::test]
async fn coordinator_empty_candidate_set_short_circuits() {
    // setup InMemoryTableStorage with 0 rows
    // execute DELETE FROM t WHERE id = 1
    // assert result `deleted = 0`
    // ADR 035 empty-set short-circuit preserved
}
```

### Fixture 13 — `distributed_coordinator_rejects_delete`

```rust
#[tokio::test]
async fn distributed_coordinator_rejects_delete() {
    // setup DistributedQueryCoordinator + MockCatalog with table "t"
    // execute DELETE FROM t WHERE id = 1
    // assert err.to_string() contains
    //   "distributed DELETE execution is not wired to a write-context-bearing coordinator"
    // verify reject fires before fragment_plan (no transport touched)
}
```

### Fixture 14 — `fragmenter_rejects_delete_defensively`

```rust
#[test]
fn fragmenter_rejects_delete_defensively() {
    // construct ExecutionPlan::Delete
    // call fragment_plan
    // assert FragmentError::DeleteNotFragmentable
    // verify reject fires before contains_join / count_exchanges
}
```

### Fixture 15 (optional) — `wendelon_sql_delete_hides_rows_from_future_scan`

```rust
#[tokio::test]
async fn wendelon_sql_delete_hides_rows_from_future_scan() {
    // setup live Wendelon node + schema
    // CREATE TABLE + INSERT 5 rows
    // execute DELETE FROM t WHERE id = 1 via SQL
    // assert result `deleted = 1`
    // subsequent SELECT * FROM t returns 4 rows (id 2..=5)
}
```

**Verification:**

```sh
cargo test -p toron --test delete_test
```

All 14 fixtures green (+ fixture 15 optional).

## Phase 8 — Architecture guards

**File:** `crates/toron/tests/architecture_test.rs`.

**Tasks:**

1. Remove ADR 035 guard
   `adr035_no_sql_delete_yet` (replaced by ADR
   036 positive guards).

2. Add the 23 ADR-036-specific guards from the
   promoted ADR §"Architecture Guard Set":

   - guards #1-#4: planner accepts + destructure
     exhaustive + table-factor destructure
     exhaustive + reject surface narrow;
   - guards #5-#9: variant counts + leaf shape +
     Shape A identity + no FilterDisposition;
   - guards #10-#11: binder Boolean validator
     reuse + residual-predicate acceptance;
   - guards #12-#13: coordinator scan request
     shape (Append + None + no runtime_filter
     inheritance);
   - guards #14-#17: residual filter ordering +
     ADR 035 wrapper-name preservation +
     execute_delete_with_candidates usage +
     deleted-only result mapping;
   - guards #18-#21: distributed reject +
     fragmenter defensive + executor defensive +
     telemetry label;
   - guard #22: synthetic-name reject preserved;
   - guard #23: filter helper byte-identical
     preservation (Phase 4 invariant).

3. Update the existing
   `architecture_guards_unchanged_plus_sort_variant`
   variant-count catch-all:
   - `LogicalPlan` count 14 → 15;
   - `ExecutionPlan` count 12 → 13.

Important guard mechanics:

- Guard #2 + #3 source-check exhaustive
  destructure with no `..` rest pattern in
  `lower_delete`.
- Guard #7 source-checks
  `ExecutionPlan::Delete` body for absence of
  `Box<...>`, `input`, `source`.
- Guard #9 source-checks
  `ExecutionPlan::Delete` body for absence of
  `FilterDisposition` literal.
- Guard #10 source-checks `bind_delete` body
  calls `require_single_boolean_root` (or final
  helper name) and does NOT contain inline
  `DataType::Boolean` checks.
- Guard #11 source-checks `bind_delete` body
  does NOT reject `compile_pbc(_) -> None` /
  `Err(_)` — residual path preserved.
- Guard #13 positive: literal `runtime_filter:
  None` in coordinator delete path. Negative:
  no `runtime_filter: self.context.runtime_filter`
  or `runtime_filter: context.runtime_filter`
  inheritance patterns.
- Guard #14 source-checks residual filter call
  ordering: `filter_batches_with_bound_predicate`
  call appears in source **before**
  `extract_delete_row_key_ids` call in the
  coordinator delete path.
- Guard #15 source-checks
  `materialize_delete_row_key_ids` wrapper name
  still exists in `coordinator/delete.rs` and
  delegates to the split helpers.
- Guard #17 source-checks the SQL result-mapping
  block for `outcome.deleted`; negative-patterns
  `outcome.requested` / `outcome.already_deleted`
  / `outcome.not_found` are scoped to that result-
  mapping block, not the whole `coordinator/delete.rs`
  file, because ADR 035's empty-outcome helper still
  legitimately constructs all four counters.
- Guard #23 source-checks
  `ExecutionPlan::Filter` arm in `exec.rs`
  calls `filter_batches_with_bound_predicate`
  helper; existing diagnostic `"filter predicate
  must return boolean"` preserved (positive
  contains-check on the helper or call site).

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

All 23 new guards green; all existing guards
(including ADR 035's 18 + ADR 034's 17 + ADR 033's
+ ADR 032's + prior) remain green.

## Phase 9 — Full verification

```sh
cargo fmt --all -- --check
cargo check -p toron
cargo test -p toron --test architecture_test
cargo test -p toron --test delete_test
cargo test -p toron --test create_table_test
cargo test -p toron --test ctas_test
cargo test -p toron --test drop_table_test
cargo test -p toron --test insert_test
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron --test sort_ordering_test
cargo test -p toron --test wendelon_storage_execution_test
cargo test -p toron materialize_delete_row_key_ids
cargo test -p toron execute_delete_with_candidates
cargo test -p toron
git diff --check
```

**Critical Phase 4 byte-identical refactor verification:**

Compare reference fixture pass counts captured
before Phase 4 extraction. All suites must report
exactly the same pass count post-extraction.

Optional Wendelon SQL DELETE fixture:

```sh
cargo test -p toron --test wendelon_storage_execution_test delete
```

## Definition of Done

- [ ] **Phase 1:** `LogicalPlan::Delete` +
      `ExecutionPlan::Delete` defined; `lower_delete`
      exhaustive destructure (Delete + TableFactor::Table);
      neutral DML name helper added.
- [ ] **Phase 2:** `bind_delete` resolves target +
      binds optional predicate + Boolean root
      validation via `require_single_boolean_root`
      + `compile_pbc` attempt + residual
      preserved on `None`; arms added to
      `collect_qualifiers` /
      `terminal_exchange` /
      `optimize_for_required`.
- [ ] **Phase 3:** `materialize_delete_candidate_batches`
      + `extract_delete_row_key_ids` +
      `materialize_delete_row_key_ids` wrapper +
      `delete_count_batch` (located in
      `coordinator/delete.rs`); ADR 035 validation
      diagnostics preserved.
- [ ] **Phase 4:** `filter_batches_with_bound_predicate`
      `pub(crate)` helper extracted from
      `ExecutionPlan::Filter` arm; byte-identical
      behavior preserved (reference fixture pass-
      count match required); existing diagnostic
      `"filter predicate must return boolean"`
      preserved verbatim.
- [ ] **Phase 5:** Local coordinator intercept;
      `execute_delete` builds ScanRequest with
      `runtime_filter: None` + `row_identity:
      Append` + `projected_columns: None`;
      residual filter applied when
      `predicate.is_some() && pbc_program.is_none()`;
      Phase 3 + Phase 4 helpers used; no SELECT
      retry loop; `#[allow(dead_code)]` annotations
      removed.
- [ ] **Phase 6:** Distributed reject before
      `fragment_plan` with stable diagnostic; new
      `FragmentError::DeleteNotFragmentable` reject
      before join/exchange analysis; executor
      defensive arms; walker leaves; telemetry
      label `"delete"`.
- [ ] **Phase 7:** All 14 fixtures pass
      (`delete_test.rs`); optional Wendelon
      fixture passes if added.
- [ ] **Phase 8:** All 23 ADR-036-specific
      architecture guards pass; ADR 035
      `adr035_no_sql_delete_yet` removed;
      paired variant-count catch-all adjusted
      (LogicalPlan 14 → 15; ExecutionPlan 12 →
      13).
- [ ] **Phase 9:** `cargo fmt --check` clean;
      `cargo test -p toron` green; Phase 4
      byte-identical reference fixture pass-count
      preserved.
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT / CTAS / DROP TABLE / DELETE
      substrate tests green.
- [ ] All existing SELECT-only plan-digest
      fixtures green.
- [ ] All existing SELECT WHERE corpus fixtures
      green (byte-identical refactor invariant).
- [ ] ADR 029 / 030 / 031 / 032 / 033 / 034 / 035
      invariants preserved.
- [ ] `StorageWrite` / `StorageScan` /
      `StorageDeleteRows` trait shapes byte-
      identical pre/post-ADR-036.
- [ ] No new `StorageError` variant added.
- [ ] No new Wendelon-side change.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] No storage reclaim trait / helper
      introduced.
- [ ] Roadmap moves ADR 036 from candidate to
      promoted; advances active section to ADR 037
      (cross-ring distributed-write-context
      substrate per D14 sequencing commitment).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | **Non-PBC predicate degrades to full-table DELETE** (the load-bearing correctness trap) | D5 accept-with-residual-filter + D7 residual filter ordering + guard #14 + fixture #8 `amount + 1 > 4` |
| R2 | Ambient `runtime_filter` inherits from executor context, silently narrows mutation candidates | D7 `runtime_filter: None` + guard #13 positive + negative pattern |
| R3 | Identity column leaks into SQL result paths | D8 + D12 + guards #17 + #22 |
| R4 | `ExecutionPlan::Delete` `FilterDisposition` field drift | D4 + guard #9 |
| R5 | Mutation retry after partial delete | D7 no retry loop |
| R6 | ADR 035 helper name churn breaks substrate tests | D8 wrapper-name preservation + guard #15 |
| R7 | `ExecutionPlan::Filter` SELECT WHERE behavior changes in Phase 4 helper extraction | D6 byte-identical refactor invariant + guard #23 + reference fixture pass-count check |
| R8 | Boolean-root validation duplicates inline logic | D5 + Phase 2 `require_single_boolean_root` + guard #10 |
| R9 | Premature generic DML row-count abstraction | D8 `delete_count_batch` location in `coordinator/delete.rs` |
| R10 | sqlparser `Delete` / `TableFactor::Table` shape change accepted silently | D2 + guards #2 + #3 |
| R11 | DELETE coordinator inherits descriptor-evolution retry behavior | D7 explicit "bind once, execute once" |
| R12 | `pbc_program: None` accidentally treated as full-table delete | D4 derivable-state table + guard #14 ordering |
| R13 | `lower_ddl_table_name` reused for DELETE diagnostics | D3 neutral helper + diagnostic context parameter |
| R14 | Plan Arena pressure increases | DELETE adds 1 leaf variant pair, no boxed children |
| R15 | UPDATE / RETURNING / cross-ring distributed-write smuggled into this ADR | D14 sequencing pin + D1 out-of-scope explicit |
| R16 | `compile_pbc` partial-pushdown changes accidentally cause `pbc_program: Some(_)` for non-fully-pushdown predicate | D5 — only set `Some(_)` when full predicate compiles; partial-pushdown treated as `None` for v1 |
| R17 | Phase 4 helper extraction changes SELECT WHERE error message | D6 byte-identical preservation + guard #23 positive contains-check |
| R18 | `execute_delete_with_candidates` empty-set short-circuit bypassed | Guard #16 + fixture #12 + Codex iter-2 Phase 5 task #7 routes through the helper |

## Commit shape

Preferred single commit:

```text
feat(toron): implement sql delete where (ADR 036)
```

If split for review:

1. `feat(toron): add delete sql and bound plan`
   (Phases 1 + 2 + IR variant additions).
2. `refactor(toron): extract filter helper for delete reuse`
   (Phase 4 byte-identical refactor + Phase 3
   helper split).
3. `feat(toron): execute local delete where`
   (Phase 5 + Phase 6 — coordinator routing +
   distributed reject + fragmenter + executor
   defensive arms).
4. `test(toron): guard delete where architecture`
   (Phase 7 + Phase 8 — 14 fixtures + 23
   architecture guards).

**Run Phase 4 reference fixture pass-count
verification immediately after commit 2** when
using the split shape. If pass count diverges,
investigate before proceeding to commit 3.
