# Implementation Plan: ADR 032 — `INSERT INTO ... SELECT`

**Companion ADR:** `plans/adr/032_insert_into_select.md`

## Goal

Add local `INSERT INTO ... SELECT` to Toron by composing
ADR 029 catalog identity, ADR 030 `StorageWrite`, and ADR
031 `BoundExprProgram` inside the source SELECT path —
without widening the VALUES insert path, without
distributed write execution, and without new expression
or storage APIs.

The full source pipeline already exists (binder, executor,
PBC, distributed coordinator, fragmenter). The write
substrate already exists (`StorageWrite` trait,
`coordinator/write.rs`, in-memory + Wendelon backends).
ADR 032 adds the two-variant plan IR, the binder helper
that fuses target-list resolution with source binding,
and the coordinator helper that materializes / coerces /
writes — and the four defensive reject sites that keep
the path coordinator-local.

Preserve:

- All ADR 029 catalog metadata-only invariants.
- All ADR 030 VALUES INSERT architecture guards;
  `ExecutionPlan::Insert` stays expression-free.
- All ADR 031 `BoundExprProgram` invariants;
  `BoundExprProgram` derives `PartialEq` only; logical
  `Expr` untouched; existing scan-projection /
  runtime-filter pushdown still applies to the source
  SELECT.
- Post-ADR-031 SELECT plan-digest baseline (D10 re-
  stabilization): existing SELECT-only fixtures must
  not regress.
- `StorageWrite` trait shape (one `RecordBatch`
  payload).
- ADR 015 cycle properties: narrow-detector
  architecture guards; named-reject sites; no silent
  acceptance.

No new SQL syntax beyond the bare `INSERT INTO [schema.]
table [(col, ...)] <select-statement>` form; no new IR
expression variant; no new aggregate kernel; no PBC
opcode change; no fragmenter detector change for SELECT
paths; no cross-ring engagement; no Wendelon-side
change; no `RecordBatch` field added to any plan
variant; no streaming `StorageWrite` overload; no row-
count or RETURNING result.

**Nine-step patch order.** Phases 1-7 are mechanical;
Phase 8 is verification; Phase 9 is promotion-time
housekeeping (out of implementation PR scope, recorded
here for completeness).

## Implementation constraints

- Do not widen `ExecutionPlan::Insert` (Shape A
  separate IR variants is the contract).
- Do not carry a `BoundExprProgram` directly in
  `InsertSelect` (the bound `Box<ExecutionPlan>` source
  carries any programs naturally).
- Do not compare source output names to target column
  names (positional mapping after `target_columns`
  resolution).
- Do not reject nullable-typed source feeding non-null
  target at bind time (runtime null-count check only).
- Do not call `evaluate_try_cast` in
  `coordinator/write.rs` (strict cast only; D8 / D13).
- Do not pass outer morsel into the source SELECT
  (`None` argument; D12).
- Do not add a streaming `StorageWrite::write` overload
  (single-batch write; D5 + D6).
- Do not return a row count from InsertSelect
  (`Ok(Vec::new())`; no RETURNING).
- Do not extend the SELECT descriptor-evolution retry
  loop to InsertSelect (D1; mirrors ADR 030 D12).
- Do not fragment `InsertSelect` (distributed reject
  before `fragment_plan`).
- Do not make the binder optimizer treat the source as
  a leaf (`optimize_for_required` descends with
  `required_output = None`).
- Do not add new public cast helpers (only
  `evaluate_cast` widens to `pub(crate)`).
- Do not add `blake3` (ADR 030 D7 carry-forward).
- Do not add external allocator crates.
- Do not silently delete ADR 030 / ADR 031 architecture
  guards.

## Touchpoint sweep

| File | Phase |
|---|---|
| `crates/toron/src/ir.rs` | 1 |
| `crates/toron/src/ir/execution.rs` | 1, 7 |
| `crates/toron/src/sql_planner.rs` | 2 |
| `crates/toron/src/binder.rs` | 3 |
| `crates/toron/src/exec.rs` | 4, 7 |
| `crates/toron/src/coordinator/write.rs` | 5 |
| `crates/toron/src/coordinator.rs` | 6 |
| `crates/toron/src/distributed/fragment.rs` | 7 |
| `crates/toron/src/distributed/scheduler.rs` | 7 |
| `crates/toron/src/server/mod.rs` | 7 |
| `crates/toron/tests/insert_test.rs` | 8 |
| `crates/toron/tests/architecture_test.rs` | 8 |

## Phase 0 — Preflight audit

**Files:** read-only.

```sh
# Confirm InsertSelect is unused in production source
rg -n "InsertSelect" crates/toron/src crates/toron/tests
# Expected: zero hits

# Baseline tests
cargo test -p toron --test insert_test
cargo test -p toron --test architecture_test

# Confirm ADR 030 / 031 invariants currently green
rg -n "ExecutionPlan::Insert" crates/toron/src/ir/execution.rs
rg -n "BoundExprProgram" crates/toron/src/ir/execution.rs
rg -n "evaluate_try_cast" crates/toron/src/coordinator
# Expected: zero hits in coordinator source

# Confirm no shadow dependencies
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits
```

Preserve verbatim:

- All `Catalog` trait methods + semantics (ADR 029).
- All ADR 030 `StorageWrite` / `InMemoryTableStorage` /
  `WendelonStorageWrite` paths.
- All ADR 031 `BoundExprProgram` / `BoundExprNode` /
  `BoundExprProgramBuilder` shapes and derives.
- `ExecutionPlan::Insert { rows: Vec<Vec<ScalarValue>>,
  ... }` (ADR 030 D4 + ADR 031 D10).
- `ExecutionPlan::CreateTable` (ADR 029).
- `Scan.projection` / `Sort.order_by` / `Join` keys
  (ordinal-only carry-forwards).

## Phase 1 — IR shape

**Goal.** Add `LogicalPlan::InsertSelect` +
`ExecutionPlan::InsertSelect`.

**Files:** `crates/toron/src/ir.rs`,
`crates/toron/src/ir/execution.rs`.

**Tasks:**

1. Add to `LogicalPlan`:

   ```rust
   InsertSelect {
       table: TableRef,
       columns: Option<Vec<String>>,
       source: Box<LogicalPlan>,
   }
   ```

   No `BoundExprProgram` or expression carrier here;
   the source is an unbound `LogicalPlan` until the
   binder runs.

2. Add to `ExecutionPlan`:

   ```rust
   InsertSelect {
       legacy_table_id: TableId,
       table: TableObjectId,
       fingerprint: PhysicalDescriptorFingerprint,
       schema: SchemaRef,
       target_columns: Vec<ColumnOrdinal>,
       source: Box<ExecutionPlan>,
   }
   ```

   `schema` is the full target payload schema.
   `target_columns` maps source output position to
   target column ordinal. `source` is the bound +
   optimized source SELECT plan.

3. Keep `ExecutionPlan::schema()` returning
   `Schema::empty()` for `InsertSelect`, matching the
   other write-shape variants (`Insert`, `CreateTable`).

4. Do **not** add `BoundExprProgram`, `PbcProgram`, or
   row vectors directly to `InsertSelect`.

5. Update any exhaustive `match` sites the compiler
   flags (executor, fragmenter, scheduler, server,
   walkers). For now they can either reject or delegate
   to leaf-treatment; Phases 4 / 6 / 7 wire the real
   behavior.

**Verification:**

```sh
cargo check -p toron
```

## Phase 2 — SQL lowering

**Goal.** `SqlPlanner::lower_insert` accepts non-VALUES
query bodies and emits `LogicalPlan::InsertSelect`.

**Files:** `crates/toron/src/sql_planner.rs`.

**Tasks:**

1. Introduce a private enum to replace `lower_insert_source`'s
   single-shape return:

   ```rust
   enum LoweredInsertSource {
       Values(Vec<Vec<InsertLiteral>>),
       Select(LogicalPlan),
   }
   ```

2. Preserve every existing VALUES-only validation when
   the source is `SetExpr::Values`:

   - non-literal expression reject (ADR 030);
   - empty `VALUES` reject;
   - row-arity vs explicit-column-list check (existing
     ADR 030 reject text).

3. For non-VALUES query bodies, call the existing
   `lower_query(query)` and wrap the result as
   `LoweredInsertSource::Select(...)`.

4. In `lower_insert`, route on the enum:

   - `Values(rows)` → existing `LogicalPlan::Insert`
     construction;
   - `Select(source)` → new `LogicalPlan::InsertSelect
     { table, columns, source: Box::new(source) }`.

5. Do not compare source output names to target column
   names at lowering time. Names are positional after
   the optional target column list is resolved at
   binding (D3).

6. Preserve **all** existing unsupported INSERT clause
   rejects from ADR 030:

   - `RETURNING`
   - `ON CONFLICT`
   - target alias
   - `REPLACE`, `OVERWRITE`, `PARTITION`, `FORMAT`,
     and related dialect clauses

   These are clause-level rejects that fire regardless
   of source body shape; ADR 032 does not lift any of
   them.

**Verification:**

```sh
cargo test -p toron sql_planner::tests
cargo test -p toron --test insert_test
```

Existing VALUES-only INSERT tests must remain green.

## Phase 3 — Binder

**Goal.** Add `Binder::bind_insert_select` and extract a
shared target-list resolution helper.

**Files:** `crates/toron/src/binder.rs`.

**Tasks:**

1. Extract target-list resolution from `bind_insert`
   into a shared helper:

   ```rust
   fn resolve_insert_target_columns(
       schema: &SchemaRef,
       columns: Option<&[String]>,
   ) -> Result<Vec<ColumnOrdinal>, BindingError>
   ```

   Responsibilities:

   - resolve names to ordinals via `schema.index_of`;
   - reject duplicate target columns with stable
     diagnostic;
   - reject unknown target column names with stable
     diagnostic;
   - reject omitted non-null target columns (those not
     in `columns` whose `is_nullable() == false`) with
     the same stable diagnostic ADR 030 D14 emits.

2. Use `resolve_insert_target_columns` in `bind_insert`
   (preserving existing behavior byte-identically) and
   in the new `bind_insert_select`.

3. Implement `bind_insert_select`:

   ```rust
   fn bind_insert_select(
       &self,
       table: &TableRef,
       columns: Option<&[String]>,
       source: &LogicalPlan,
   ) -> Result<ExecutionPlan, BindingError>
   ```

   Sequence:

   - resolve the target table through the catalog
     (mirrors `bind_insert` lookup);
   - call `resolve_insert_target_columns`;
   - bind the source via the normal SELECT binder
     (`self.bind_plan(source)?`);
   - leave the bound source as the `InsertSelect`
     child; do not directly call
     `optimize_for_required` here, because the
     existing top-level `push_down_scan_projection`
     pass will reach it through task 6 below;
   - compare source output arity with
     `target_columns.len()`; reject with stable
     diagnostic naming both counts;
   - validate source output type to target field type
     via `is_insert_select_coercible(source_ty,
     target_ty)` helper (numeric admissibility from
     ADR 025 strict cast lattice plus identity for
     Boolean / Utf8);
   - emit `ExecutionPlan::InsertSelect { legacy_table_id,
     table, fingerprint, schema, target_columns,
     source: Box::new(source_plan) }`.

4. Add `is_insert_select_coercible(source_ty: &DataType,
   target_ty: &DataType) -> bool` helper. Admissible
   pairs:

   - identity (any of the six types → itself);
   - any of `Int32` / `Int64` / `Float32` / `Float64`
     → any of the four numeric types (ADR 025 strict
     cast admits every numeric-to-numeric cell);
   - Boolean → Boolean only;
   - Utf8 → Utf8 only;
   - everything else rejected.

5. Do **not** reject nullable source feeding non-null
   target solely because the source field is nullable
   (D9 — runtime check handles actual data).

6. Update `optimize_for_required` to handle
   `ExecutionPlan::InsertSelect` by optimizing `source`
   with `required_output = None` and preserving target
   metadata + `target_columns`.

**Verification:**

```sh
cargo test -p toron binder::tests
cargo test -p toron --test insert_test
```

## Phase 4 — Cast helper visibility

**Goal.** Make `evaluate_cast` reachable from
`coordinator::write`.

**Files:** `crates/toron/src/exec.rs`.

**Tasks:**

1. Change only the strict cast entry point:

   ```rust
   pub(crate) fn evaluate_cast(...)
   ```

2. Keep `evaluate_try_cast` and the lower-level
   numeric cast kernels private. ADR 032 D8 / D13
   forbid TRY_CAST semantics in the coordinator write
   path.

3. Do not introduce a new Arrow cast dependency or a
   parallel cast implementation.

**Verification:**

```sh
cargo check -p toron
```

## Phase 5 — Coordinator materialization helper

**Goal.** Add the `materialize_insert_select_batch`
private helper.

**Files:** `crates/toron/src/coordinator/write.rs`.

**Tasks:**

1. Add helper:

   ```rust
   pub(super) fn materialize_insert_select_batch(
       target_schema: &SchemaRef,
       target_columns: &[ColumnOrdinal],
       source_batches: &[RecordBatch],
   ) -> Result<Option<RecordBatch>, ExecutionError>
   ```

2. Count total source rows before building any
   coerced array. If the count exceeds
   `TABLE_WRITE_MAX_ROWS`, return
   `ExecutionError::InvalidExpression` with a stable
   diagnostic naming both the actual count and the
   cap.

3. Return `Ok(None)` for zero source rows (D7 empty-
   source short-circuit; coordinator routing handles
   the no-op).

4. For each source batch:

   - map source column position to target ordinal via
     `target_columns`;
   - use `evaluate_cast` when source and target
     numeric types differ;
   - clone identity arrays for same-type Boolean,
     Utf8, and numeric columns;
   - fill omitted nullable target columns (those not
     in `target_columns`) with typed null arrays of
     the target field type;
   - reject unsupported target types or cast
     combinations before write with stable diagnostic.

5. After all batches are coerced, concatenate them
   with the target schema into one `RecordBatch`
   (because `WriteRequest` still carries one payload
   batch).

6. After concatenation, for every non-null target
   field, check the corresponding array's
   `null_count()`. If `> 0`, return
   `ExecutionError::InvalidExpression` with stable
   diagnostic naming the offending column (D9).

7. Return `Ok(Some(batch))`.

**Verification:**

```sh
cargo test -p toron coordinator::write::tests
```

## Phase 6 — Coordinator routing

**Goal.** Wire `LogicalPlan::InsertSelect` interception
in the local coordinator and `ExecutionPlan::InsertSelect`
rejection in the distributed coordinator.

**Files:** `crates/toron/src/coordinator.rs`.

**Tasks:**

1. In `QueryCoordinator::execute_query`, intercept
   `LogicalPlan::InsertSelect` next to
   `LogicalPlan::Insert`, **before** the SELECT
   descriptor-evolution retry loop. The interception
   site mirrors the existing ADR 030 INSERT
   interception.

2. Extend `execute_write_plan` for
   `ExecutionPlan::InsertSelect`:

   - execute the source with
     `self.executor.execute_bound(source, None)` (D12
     — never pass the outer morsel into the source
     write path);
   - collect source batches;
   - call
     `write::materialize_insert_select_batch(target_schema,
     target_columns, &source_batches)`;
   - if the helper returns `Ok(None)`, return
     `Ok(Vec::new())` without invoking the writer (D7);
   - otherwise build the same `WriteRequest` shape
     used by VALUES INSERT and call
     `StorageWrite::write` once;
   - ignore `WriteOutcome.rows` and return
     `Ok(Vec::new())` (D5).

3. In `DistributedQueryCoordinator::execute_query`,
   reject `ExecutionPlan::InsertSelect` before
   `fragment_plan` with the stable diagnostic
   `"distributed INSERT INTO ... SELECT execution is
   not wired to a write-context-bearing coordinator"`
   (parallels ADR 030 D13 wording but disambiguates
   the DML form).

**Verification:**

```sh
cargo test -p toron coordinator::tests
cargo test -p toron --test insert_test
```

## Phase 7 — Defensive rejects + walker leaf treatment

**Goal.** Lock down the remaining `InsertSelect`
match sites.

**Files:**

- `crates/toron/src/exec.rs`
- `crates/toron/src/distributed/fragment.rs`
- `crates/toron/src/distributed/scheduler.rs`
- `crates/toron/src/server/mod.rs`
- any exhaustive `ExecutionPlan` / `LogicalPlan`
  match sites surfaced by the compiler.

**Tasks:**

1. Add executor defensive `NotImplemented` arms for
   uncoordinated `ExecutionPlan::InsertSelect`
   execution. The coordinator intercepts the only
   legitimate reach; the executor arm is a guard
   against future refactor mistakes.

2. Add `FragmentError::InsertSelectNotFragmentable`
   variant; reject before `contains_join` /
   `count_exchanges` work.

3. Treat `ExecutionPlan::InsertSelect` as a leaf in
   distributed / server / scheduler metadata walkers:

   - join detection returns `false` / `None`;
   - exchange counting returns `0`;
   - scan metadata / fingerprint lookup returns
     `None`;
   - replacement walkers leave the plan unchanged;
   - telemetry labels use `"insert_select"` (not
     `"insert"`, which ADR 030 owns).

4. The binder's `optimize_for_required` is the **one
   intentional exception**: it descends into the
   source with `required_output = None` (handled in
   Phase 3 already). Document the exception with an
   inline comment in `optimize_for_required` that
   names ADR 032 D11.

**Verification:**

```sh
cargo test -p toron --test distributed_process_test
cargo test -p toron
```

## Phase 8 — Tests

**Goal.** 14 architecture guards + functional coverage
+ both required composition fixtures.

**File:** `crates/toron/tests/insert_test.rs`,
`crates/toron/tests/architecture_test.rs`.

### Functional tests in `insert_test.rs`

- Planner accepts `INSERT INTO dst SELECT id, amount
  FROM src`.
- Planner accepts `INSERT INTO dst (b, a) SELECT x, y
  FROM src` (explicit reorder).
- VALUES INSERT tests remain unchanged and still
  reject non-literal VALUES expressions.
- Binder emits `target_columns` in source-output order
  for explicit column lists (positional, not name-
  matched).
- Source output column names are ignored; only source
  position matters.
- Arity mismatch fails at bind time with both numbers
  in diagnostic.
- Unknown target column fails at bind time.
- Duplicate target column fails at bind time.
- Omitted non-null target column fails at bind time.
- End-to-end `INSERT INTO dst SELECT ... FROM src`
  writes rows and SELECT reads them back.
- Explicit target column reorder works end-to-end.
- Omitted nullable target columns are filled with NULL.
- Nullable source values into a non-null target fail
  before write with column name in diagnostic.
- Strict numeric cast overflow fails before write.
- Empty source returns success and does not call the
  writer (assert via writer instrumentation).
- Source row count over `TABLE_WRITE_MAX_ROWS` fails
  before write.
- Source SELECT scan projection observed to narrow
  when the binder optimizer descends.
- Distributed coordinator rejects `InsertSelect`
  before fragmentation with D1 diagnostic.
- Fragmenter `InsertSelectNotFragmentable` reject
  fires.

### Required composition fixtures (Definition of Done)

**Fixture A — Grouped aggregate source:**

```sql
CREATE TABLE src (id INT, amount BIGINT);
CREATE TABLE dst (id INT, total BIGINT);
INSERT INTO src VALUES (1, 100), (1, 200), (2, 50);
INSERT INTO dst (id, total)
SELECT id, SUM(amount) FROM src GROUP BY id;
SELECT id, total FROM dst ORDER BY id;
-- Expected: (1, 300), (2, 50)
```

**Fixture B — INNER JOIN source:**

```sql
CREATE TABLE left_table (id INT, label TEXT);
CREATE TABLE right_table (left_id INT, score BIGINT);
CREATE TABLE dst (id INT, label TEXT, score BIGINT);
INSERT INTO left_table VALUES (1, 'a'), (2, 'b');
INSERT INTO right_table VALUES (1, 100), (2, 200);
INSERT INTO dst (score, id, label)
SELECT score, id, label
FROM left_table INNER JOIN right_table
ON left_table.id = right_table.left_id;
SELECT id, label, score FROM dst ORDER BY id;
-- Expected: (1, 'a', 100), (2, 'b', 200)
```

Both fixtures must pass before promotion.

### Architecture guards in `architecture_test.rs`

14 ADR-032-specific guards total. Existing ADR 030 /
ADR 031 preservation guards continue to run, but are not
counted as new ADR 032 guards. Each new guard's
diagnostic string names ADR 032 + the specific decision.

**ADR-032-specific (14):**

1. `adr032_logical_plan_variant_count_is_12` — pin
   `LogicalPlan` variant count growth 11 → 12.
2. `adr032_execution_plan_variant_count_is_10` — pin
   `ExecutionPlan` variant count growth 9 → 10.
3. `adr032_sql_planner_splits_values_and_select_sources`
   — source-check `lower_insert_source` keeps VALUES
   literal lowering separate from SELECT lowering.
4. `adr032_insert_stays_values_only` — source-check
   `ExecutionPlan::Insert` still contains `rows:
   Vec<Vec<ScalarValue>>`; absence of `BoundExprProgram`
   / `PbcProgram` in the variant body.
5. `adr032_insert_select_carries_target_columns` —
   source-check `ExecutionPlan::InsertSelect` contains
   `target_columns: Vec<ColumnOrdinal>` + `source:
   Box<ExecutionPlan>`.
6. `adr032_insert_select_no_direct_bound_expr_program`
   — source-check `InsertSelect` variant body for
   absence of direct `BoundExprProgram` / `PbcProgram`
   field.
7. `adr032_local_coordinator_intercepts_before_retry`
   — source-check `coordinator.rs` `execute_query`
   body: `InsertSelect` match arm precedes the SELECT
   retry loop marker.
8. `adr032_distributed_coordinator_rejects_before_fragment`
   — source-check distributed coordinator body:
   `InsertSelect` reject precedes `fragment_plan`
   call.
9. `adr032_fragmenter_rejects_before_join_or_exchange`
   — source-check `distributed/fragment.rs`:
   `InsertSelectNotFragmentable` reject precedes
   `contains_join` / `count_exchanges`.
10. `adr032_distributed_walkers_treat_insert_select_as_leaf`
   — source-check distributed / server / scheduler
   walker match arms return leaf for `InsertSelect`.
11. `adr032_coordinator_uses_only_evaluate_cast`:

    ```rust
    #[test]
    fn adr032_coordinator_uses_only_evaluate_cast() {
        let write = read_to_string(
            manifest_dir().join("src/coordinator/write.rs")
        ).unwrap();
        assert!(
            write.contains("evaluate_cast"),
            "coordinator/write.rs must call evaluate_cast (ADR 032 D8/D13)"
        );
        assert!(
            !write.contains("evaluate_try_cast"),
            "coordinator/write.rs must not call evaluate_try_cast (ADR 032 D8/D13 forbid TRY_CAST semantics)"
        );
    }
    ```

12. `adr032_binder_enforces_source_target_arity`:

    ```rust
    #[test]
    fn adr032_binder_enforces_source_target_arity() {
        let binder = read_to_string(
            manifest_dir().join("src/binder.rs")
        ).unwrap();
        let body = brace_blocks_after_marker(&binder, "fn bind_insert_select")
            .into_iter()
            .next()
            .expect("bind_insert_select body must exist (ADR 032 D4)")
            .1;
        assert!(
            body.contains("target_columns.len()"),
            "bind_insert_select must reference target_columns.len() for arity check (ADR 032 D4)"
        );
        assert!(
            body.contains("source"),
            "bind_insert_select must reference source for arity comparison (ADR 032 D4)"
        );
    }
    ```

13. `adr032_storage_write_trait_unchanged` — source-
    check `storage.rs` for unchanged `StorageWrite`
    trait signature with single `RecordBatch` payload.
14. `adr032_insert_select_does_not_add_plan_arena_dependency`
    — source-check `Cargo.toml` for no new arena
    dependency while deferring the plan-arena substrate.

**Verification:**

```sh
cargo test -p toron --test architecture_test
cargo test -p toron --test insert_test
```

## Phase 9 — Promotion-time housekeeping (out of PR scope)

Recorded here for completeness; performed at promotion
time, not inside the implementation PR.

1. Roadmap bump: move ADR 032 from candidate to
   promoted; advance active section to ADR 033.
2. Archive WIP iteration docs per established
   convention (`plans/wip/{name}_iter_*_adr_032.md` +
   `plans/wip/{name}_iter_*_impl_032.md` move out of
   active rotation).
3. Update bench-results file pointer if a Phase-10-
   equivalent bench cycle lands.

## Implementation risks

| ID | Risk | Mitigation |
|---|---|---|
| R1 | VALUES INSERT path silently regresses through `lower_insert` refactor | Phase 2 keeps `LoweredInsertSource::Values` byte-identical to current VALUES handling. |
| R2 | Distributed `InsertSelect` accidentally fragments | Phases 6 + 7 add three independent reject sites; Phase 8 guard #7 + #8 + #9 pin them. |
| R3 | Binder optimizer fails to descend into source | Phase 3 task 6 explicit; Phase 8 behavioral test asserts narrowed scan projection. |
| R4 | Source-schema nullability false-rejects | Phase 3 task 5 explicit non-rejection. |
| R5 | Outer morsel propagates into source SELECT | Phase 6 task 2 `None` argument explicit. |
| R6 | Total row cap not enforced | Phase 5 task 2 cap check before any cast work. |
| R7 | Empty source still calls writer | Phase 5 task 3 + Phase 6 task 2 `Ok(None)` short-circuit. |
| R8 | Strict cast accidentally swapped for try-cast | Phase 4 only widens `evaluate_cast`; Phase 8 guard #10 source-checks presence + absence. |
| R9 | Source arity check loosens | Phase 8 guard #11 source-checks `bind_insert_select` body. |
| R10 | `target_columns` reorder dropped | Phase 1 IR carrier explicit; Phase 3 binder unit test asserts ordinals. |
| R11 | JOIN source path uncovered | Phase 8 Fixture B required for DoD. |
| R12 | Grouped aggregate source uncovered | Phase 8 Fixture A required for DoD. |
| R13 | Walkers grow broad `InsertSelect` traversal | Phase 7 task 3 leaf-treatment; Phase 8 guard #9. |
| R14 | `Insert` VALUES guards weaken via shared helper | `resolve_insert_target_columns` extraction preserves existing `bind_insert` behavior byte-identically; existing ADR 030 guards survive. |
| R15 | Plan-digest invariant re-broken | Phase 8 verifies SELECT-only fixtures unchanged. |
| R16 | `evaluate_cast` widening leaks `evaluate_try_cast` | Phase 4 explicit visibility split; Phase 8 guard #10 forbids `evaluate_try_cast` in `coordinator/write.rs`. |

## Definition of Done

- [ ] `LogicalPlan::InsertSelect { table, columns,
      source: Box<LogicalPlan> }` defined.
- [ ] `ExecutionPlan::InsertSelect { legacy_table_id,
      table, fingerprint, schema, target_columns:
      Vec<ColumnOrdinal>, source: Box<ExecutionPlan> }`
      defined.
- [ ] `LogicalPlan` variant count is 12;
      `ExecutionPlan` variant count is 10.
- [ ] `ExecutionPlan::Insert` still carries
      `rows: Vec<Vec<ScalarValue>>` with no
      `BoundExprProgram` / `PbcProgram`.
- [ ] `SqlPlanner::lower_insert` routes non-VALUES
      query bodies through `lower_query` via a private
      `LoweredInsertSource` enum.
- [ ] All existing VALUES INSERT validations preserved
      byte-identically.
- [ ] `Binder::bind_insert_select` resolves target,
      binds + optimizes source, validates arity and
      type admissibility, emits
      `ExecutionPlan::InsertSelect`.
- [ ] `resolve_insert_target_columns(schema, columns)`
      helper shared between `bind_insert` and
      `bind_insert_select`.
- [ ] `is_insert_select_coercible(source_ty,
      target_ty)` helper covers ADR 032 lattice;
      rejects unsupported pairs.
- [ ] `crate::exec::evaluate_cast` is `pub(crate)`;
      `evaluate_try_cast` and lower-level cast
      kernels stay private.
- [ ] `coordinator/write.rs::materialize_insert_select_batch`
      returns `Ok(None)` for zero rows; enforces row
      cap; performs strict cast coercion; fills
      omitted nullable columns with typed null;
      rejects nulls in non-null target fields.
- [ ] `QueryCoordinator::execute_query` intercepts
      `LogicalPlan::InsertSelect` before SELECT retry
      loop.
- [ ] `QueryCoordinator::execute_write_plan` executes
      source with `None` morsel and calls
      `StorageWrite::write` at most once.
- [ ] `DistributedQueryCoordinator::execute_query`
      rejects `InsertSelect` before `fragment_plan`
      with D1 diagnostic.
- [ ] `FragmentError::InsertSelectNotFragmentable`
      reject fires before `contains_join` /
      `count_exchanges`.
- [ ] Distributed / server / scheduler walkers treat
      `InsertSelect` as leaf; binder
      `optimize_for_required` is the documented
      exception.
- [ ] Telemetry labels use `"insert_select"`.
- [ ] Defensive `Executor::execute_bound` reject for
      uncoordinated `InsertSelect`.
- [ ] **All 14 architecture guards pass.**
- [ ] **Fixture A (grouped aggregate source) passes
      end-to-end.**
- [ ] **Fixture B (INNER JOIN source) passes end-to-
      end.**
- [ ] All existing VALUES INSERT tests green.
- [ ] All existing SELECT-only plan-digest fixtures
      green (D10 re-stabilization).
- [ ] ADR 029 catalog metadata-only preserved.
- [ ] ADR 030 `ExecutionPlan::Insert` expression-free
      preserved.
- [ ] ADR 031 `BoundExprProgram` `PartialEq`-only
      preserved.
- [ ] `StorageWrite` trait shape unchanged.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] Roadmap moves ADR 032 from "candidate" to
      "promoted".
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# Development cadence
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron --test architecture_test
cargo test -p toron --test insert_test
cargo test -p toron --test group_by_test
cargo test -p toron --test hash_join_kinds_test
cargo test -p toron --test distributed_process_test
cargo test -p toron

# Post-PR audits
rg -n "InsertSelect" crates/toron/src crates/toron/tests
rg -n "evaluate_try_cast" crates/toron/src/coordinator
# Expected: zero hits in coordinator source

rg -n "fn bind_insert_select|fn resolve_insert_target_columns" \
  crates/toron/src/binder.rs
# Expected: both helpers present

rg -n "materialize_insert_select_batch" crates/toron/src/coordinator/write.rs
# Expected: helper present

rg -n "InsertSelectNotFragmentable" crates/toron/src/distributed
# Expected: present in fragment.rs

rg -n "trait StorageWrite|payload: RecordBatch" crates/toron/src/storage.rs
# Expected: trait shape unchanged

rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits
```
