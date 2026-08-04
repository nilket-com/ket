# Implementation Plan: ADR 034 — `DROP TABLE`

**Companion ADR:** `plans/adr/0034_drop_table.md`

## Goal

Implement narrow catalog `DROP TABLE [IF EXISTS]
table_name` as a metadata-only DDL operation by
extending the existing DDL path:

- planner accepts strict one-table DROP;
- binder emits identity-free leaf execution plan;
- catalog owns existence and `IF EXISTS` semantics;
- coordinators route DROP through the shared DDL
  helper alongside `CreateTable`;
- executor + fragmenter defensively reject direct
  reach;
- walkers treat DROP as a leaf;
- no physical storage reclaim is promised.

This is a metadata DDL ADR, not row deletion, storage
admin, distributed data-plane write context, or Plan
Arena.

Preserve:

- All ADR 029 catalog metadata-only invariants and
  storage-synthetic-name reject discipline.
- All ADR 030 VALUES INSERT architecture guards;
  `ExecutionPlan::Insert` stays expression-free.
- All ADR 031 `BoundExprProgram` invariants.
- All ADR 032 InsertSelect architecture guards;
  `materialize_insert_select_batch` still returns
  `Ok(None)` for zero rows.
- All ADR 033 CTAS architecture guards;
  `materialize_empty_record_batch` sibling helper
  contract.
- ADR 015 cycle properties: narrow-detector
  architecture guards; named-reject sites; no silent
  acceptance.

No new SQL syntax beyond bare `DROP TABLE [IF EXISTS]
table_name`; no new IR expression variant; no new
aggregate kernel; no PBC opcode change; no fragmenter
detector change for SELECT paths; no cross-ring
engagement; no Wendelon-side change; no new
`StorageWrite` overload; no row-count or RETURNING
result; no `blake3` dependency; no external allocator
crate; no storage reclaim trait or helper; no row-
identity discovery path.

**Nine-phase patch order.** Phases 1-6 are mechanical;
Phase 7 is functional tests; Phase 8 is architecture
guards; Phase 9 is full verification. Promotion-time
housekeeping (Phase 10) is out of implementation PR
scope.

## Implementation constraints (Codex iter-0 + iter-1 + iter-2 + Claude iter-1 C1/C2/A3)

- **D5 / R1 carry-forward (ADR 033):**
  `ExecutionPlan::DropTable` carries no `TableId`,
  `TableObjectId`, or `PhysicalDescriptorFingerprint`
  — only `name` and `if_exists`.
- **D6:** Binder must not call `resolve_table`,
  `resolve_table_id`, or `drop_table` in the
  `DropTable` arm.
- **D7:** `Catalog::drop_table` returns
  `Result<Option<TableBinding>, CatalogError>`.
- **D7 / D8:** `DROP TABLE existing` returns
  `Ok(Some(binding))`; `IF EXISTS missing` returns
  `Ok(None)`; missing without `IF EXISTS` returns
  `TableNotFound`; Wendelon known returns
  `AdminUnavailable`; Wendelon unknown with `IF
  EXISTS` returns `Ok(None)`.
- **D8:** `MockCatalog::drop_table` removes from
  `by_id`; uses `by_ref.retain` to remove every
  alias pointing to the dropped table id; does NOT
  repoint aliases; does NOT reuse / decrement
  `next_table_id`.
- **D9:** No storage reclaim. No `StorageReclaim` /
  `StorageDrop` trait. No in-memory storage map
  cleanup. No stale-bound-plan invalidation.
- **D10:** Both coordinators route DROP through
  `execute_catalog_plan` (shared with CREATE TABLE).
- **D12:** Executor + fragmenter defensively reject
  direct reach. Walkers treat DROP as leaf.
- **D13 (Claude iter-0 C2):** Telemetry label
  `"drop_table"` (distinct from CREATE TABLE's
  `"catalog"`).
- **D4 (Claude iter-1 C2):** Rename
  `lower_create_table_name` → `lower_ddl_table_name`.
  Update both existing call sites (CREATE TABLE in
  `lower_create_table`, CTAS in
  `lower_create_table_as_select`). DROP uses the
  renamed helper.
- **D16:** No `bumpalo` / `typed-arena` / `slab` /
  `generational-arena` / `blake3` / storage
  synthetic row construction.
- **Claude iter-1 C1 / A1:** 17th architecture guard
  for `Catalog` trait narrowness — positive: trait
  contains `async fn drop_table`; negative: trait
  does NOT contain `truncate_table`, `replace_table`,
  `drop_schema`, `alter_table`, `rename_table`, or
  `drop_table_outcome`.
- **Claude iter-1 A3:** New test file
  `crates/toron/tests/drop_table_test.rs` (one
  statement form, one test file, SQL surface
  verbatim with `_test.rs` suffix).

## Touchpoint sweep

| File | Phase |
|---|---|
| `crates/toron/src/ir.rs` | 1 |
| `crates/toron/src/ir/execution.rs` | 1, 6 (walkers) |
| `crates/toron/src/sql_planner.rs` | 2 (lowerer + rename + walker leaves) |
| `crates/toron/src/binder.rs` | 3 |
| `crates/toron/src/catalog.rs` | 4 |
| `crates/toron/src/coordinator.rs` | 5 |
| `crates/toron/src/exec.rs` | 6 |
| `crates/toron/src/distributed/fragment.rs` | 6 |
| `crates/toron/src/distributed/scheduler.rs` | 6 |
| `crates/toron/src/server/mod.rs` | 6 |
| `crates/toron/tests/drop_table_test.rs` (NEW) | 7 |
| `crates/toron/tests/architecture_test.rs` | 8 |
| `crates/toron/tests/wendelon_storage_execution_test.rs` | 6 (exhaustive helper arm if compiler flags) |

## Helper reuse summary

| Helper | Source | DROP use |
|---|---|---|
| `Catalog::create_table` | ADR 029 | Reused unchanged |
| `Catalog::resolve_table` | ADR 029 | Reused unchanged |
| `execute_catalog_plan` | ADR 029 | Extended to handle `CreateTable` + `DropTable` arms |
| `lower_create_table_name` → `lower_ddl_table_name` (renamed) | ADR 029 (renamed in this cycle) | Reused by CREATE TABLE + CTAS + DROP TABLE |
| `Catalog::drop_table` | **NEW** (Phase 4) | New trait method |
| `MockCatalog::drop_table` | **NEW** (Phase 4) | New impl with exact alias cleanup |
| `WendelonCatalog::drop_table` | **NEW** (Phase 4) | New impl with `AdminUnavailable` for known tables |
| `lower_drop_table` | **NEW** (Phase 2) | SQL planner-side DROP lowerer |
| `FragmentError::DropTableNotFragmentable` | **NEW** (Phase 6) | Fragmenter-side reject variant |

One renamed helper + three new catalog methods + one
SQL lowerer + one fragmenter error variant + one
telemetry label. Net new code is bounded.

## Phase 0 — Preflight audit

```sh
# Confirm DropTable is unused
rg -n "DropTable|drop_table|Statement::Drop" crates/toron/src crates/toron/tests
# Expected: zero hits

# Confirm ADR 029 / 030 / 031 / 032 / 033 baseline green
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test ctas_test
cargo test -p toron --test insert_test

# Confirm name helper is currently CREATE-named
rg -n "fn lower_create_table_name" crates/toron/src/sql_planner.rs
# Expected: one definition + two call sites (lower_create_table + lower_create_table_as_select)

# Confirm execute_catalog_plan currently handles only CreateTable
rg -n "async fn execute_catalog_plan|ExecutionPlan::CreateTable" crates/toron/src/coordinator.rs

# Confirm Catalog trait has no drop_table
rg -n "async fn drop_table|pub trait Catalog" crates/toron/src/catalog.rs

# Confirm no shadow dependencies
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits
```

Expected baseline:

- No existing `DropTable` symbol in production or
  tests.
- `lower_create_table_name` has one definition + two
  callers (CREATE TABLE + CTAS).
- `execute_catalog_plan` handles only `CreateTable`.
- `Catalog` trait has no `drop_table` method.

Preserve verbatim:

- All `Catalog` existing methods + semantics (ADR
  029).
- All ADR 030 `StorageWrite` / `InMemoryTableStorage`
  / `WendelonStorageWrite` paths.
- All ADR 031 `BoundExprProgram` shape and derives.
- All ADR 032 `materialize_insert_select_batch`
  semantics including `Ok(None)` zero-row return.
- All ADR 033 CTAS path (`bind_create_table_as_select`
  + `materialize_empty_record_batch` + coordinator
  CTAS arm).
- All distributed / server / scheduler / fragmenter
  reject patterns for `Insert` / `InsertSelect` /
  `CreateTableAsSelect`.

## Phase 1 — IR shape

**Files:** `crates/toron/src/ir.rs`,
`crates/toron/src/ir/execution.rs`.

**Tasks:**

1. Add to `LogicalPlan`:

   ```rust
   DropTable {
       name: TableRef,
       if_exists: bool,
   }
   ```

2. Add to `ExecutionPlan`:

   ```rust
   DropTable {
       name: TableRef,
       if_exists: bool,
   }
   ```

   **No `TableId` / `TableObjectId` /
   `PhysicalDescriptorFingerprint` / boxed source
   child** (D5 / mirrors ADR 033 R1).

3. Update `ExecutionPlan::schema()`:

   ```rust
   Self::DropTable { .. } => Arc::new(Schema::empty()),
   ```

   Matches `Insert`, `InsertSelect`, `CreateTable`,
   `CreateTableAsSelect`.

4. Update any exhaustive `match` sites the compiler
   flags. Walkers wire up real leaf behavior in
   Phase 6.

**Verification:**

```sh
cargo check -p toron
```

Expected interim failures: match exhaustiveness only.

## Phase 2 — SQL lowering + helper rename

**File:** `crates/toron/src/sql_planner.rs`.

**Tasks:**

1. Import `ObjectType` from sqlparser if not already
   present.

2. **Rename helper** (D4 / Claude iter-1 C2):

   ```rust
   fn lower_create_table_name(...) -> ...
   ```

   →

   ```rust
   fn lower_ddl_table_name(...) -> ...
   ```

   Update both existing call sites:

   - `lower_create_table` (ADR 029);
   - `lower_create_table_as_select` (ADR 033).

3. Extend `SqlPlanner::plan` statement dispatch:

   ```rust
   Statement::Drop { .. } => self.lower_drop_table(&ast[0]),
   ```

   Prefer a helper signature that takes `&Statement`
   so the body can destructure `Statement::Drop`
   exhaustively.

4. Update unsupported-statement diagnostic:

   ```text
   unsupported SQL statement; only SELECT, CREATE TABLE, INSERT, and DROP TABLE are supported
   ```

5. Implement `lower_drop_table(&self, statement:
   &Statement) -> Result<LogicalPlan, FrontendError>`
   with exhaustive destructure (no `..`):

   ```rust
   let Statement::Drop {
       object_type,
       if_exists,
       names,
       cascade,
       restrict,
       purge,
       temporary,
   } = statement else {
       return Err(unsupported(
           "internal DROP lowering called with non-DROP statement",
       ));
   };
   ```

6. Validate in this order with stable diagnostics:

   - `object_type != ObjectType::Table` →
     `"unsupported DROP object type: only TABLE is
     supported"`;
   - `names.len() != 1` → `"DROP TABLE supports
     exactly one table name"`;
   - `cascade == true` → `"unsupported DROP TABLE
     clause: CASCADE"`;
   - `restrict == true` → `"unsupported DROP TABLE
     clause: RESTRICT"`;
   - `purge == true` → `"unsupported DROP TABLE
     clause: PURGE"`;
   - `temporary == true` → `"unsupported DROP TABLE
     clause: TEMPORARY"`.

7. Lower the name with the renamed helper:

   ```rust
   let name = lower_ddl_table_name(&names[0])?;
   ```

   The helper preserves ADR 029's one- or two-part
   name rule (`table` or `schema.table`); three-part
   names reject.

8. Emit:

   ```rust
   LogicalPlan::DropTable {
       name,
       if_exists: *if_exists,
   }
   ```

9. Add `DropTable` leaves to logical helper walkers:

   - `plan_contains_join`;
   - `plan_qualifiers`;
   - any other walker the compiler flags.

   DROP is a statement boundary like CTAS /
   InsertSelect; do not let source qualifiers leak
   into the outer planner scope (DROP has no source,
   but the leaf treatment is consistent).

**Verification:**

```sh
cargo test -p toron sql_planner::tests
cargo test -p toron --test drop_table_test  # once Phase 7 lands
```

Existing CREATE TABLE / CTAS tests must remain green
after the rename.

## Phase 3 — Binder pass-through

**File:** `crates/toron/src/binder.rs`.

**Tasks:**

1. Add a `LogicalPlan::DropTable` arm in
   `Binder::bind_plan`:

   ```rust
   LogicalPlan::DropTable { name, if_exists } => Ok(ExecutionPlan::DropTable {
       name: name.clone(),
       if_exists: *if_exists,
   }),
   ```

2. **Do not call any catalog method from this arm**
   (D6). Architecture guard #8 negative-pattern
   checks the binder arm body for absence of
   `.resolve_table(` / `.resolve_table_id(` /
   `.drop_table(`.

3. Add `DropTable` leaves to:

   - `collect_qualifiers` (logical side: DROP is a
     leaf, no source contribution);
   - `terminal_exchange` (execution side: DROP is
     not a terminal exchange; return `None`);
   - `optimize_for_required` (execution side:
     return the same plan + `OrdinalRemap::new()`;
     no source child to descend into).

**Verification:**

```sh
cargo test -p toron binder::tests
cargo test -p toron --test drop_table_test  # once Phase 7 lands
```

## Phase 4 — Catalog API + implementations

**File:** `crates/toron/src/catalog.rs`.

**Tasks:**

1. Extend `Catalog` trait:

   ```rust
   async fn drop_table(
       &self,
       name: &TableRef,
       if_exists: bool,
   ) -> Result<Option<TableBinding>, CatalogError>;
   ```

2. Implement `MockCatalog::drop_table`:

   ```rust
   async fn drop_table(
       &self,
       name: &TableRef,
       if_exists: bool,
   ) -> Result<Option<TableBinding>, CatalogError> {
       let mut inner = self.inner.write().expect("MockCatalog lock poisoned");
       let Some(table_id) = inner.by_ref.get(name).copied() else {
           if if_exists {
               return Ok(None);
           }
           return Err(CatalogError::TableNotFound {
               table: table_name(name),
           });
       };
       let binding = inner
           .by_id
           .remove(&table_id)
           .ok_or_else(|| CatalogError::TableNotFound {
               table: table_name(name),
           })?;
       inner.by_ref.retain(|_, id| *id != table_id);
       Ok(Some(binding))
   }
   ```

   Constraints:

   - Do NOT decrement or reuse `next_table_id` —
     recreate-after-drop must get a fresh monotonic
     id so stale in-memory storage data for the old
     id is unreachable through the new binding.
   - Do NOT repoint aliases — `by_ref.retain` only
     removes; never reassign an unqualified alias to
     a different schema-qualified table.

3. Implement `WendelonCatalog::drop_table`:

   ```rust
   async fn drop_table(
       &self,
       name: &TableRef,
       if_exists: bool,
   ) -> Result<Option<TableBinding>, CatalogError> {
       if self.by_ref.contains_key(name) {
           return Err(CatalogError::AdminUnavailable {
               message: "DROP TABLE is not wired to Wendelon catalog administration".to_string(),
           });
       }
       if if_exists {
           Ok(None)
       } else {
           Err(CatalogError::TableNotFound {
               table: table_name(name),
           })
       }
   }
   ```

4. **No storage trait changes.** Do not touch
   `StorageWrite`, `InMemoryTableStorage`, or
   Wendelon data-plane write wrappers. ADR 034
   changes catalog reachability only; storage
   reclaim is future work (D9).

**Verification:**

```sh
cargo test -p toron catalog::tests
cargo test -p toron --test drop_table_test  # once Phase 7 lands
```

## Phase 5 — Coordinator routing

**File:** `crates/toron/src/coordinator.rs`.

**Tasks:**

1. Extend `execute_catalog_plan` to handle both
   `CreateTable` and `DropTable`:

   ```rust
   ExecutionPlan::DropTable { name, if_exists } => {
       catalog.drop_table(name, *if_exists).await?;
       Ok(Vec::new())
   }
   ```

   Keep `CreateTable` arm in the same helper.

2. Local coordinator intercept:

   ```rust
   if matches!(
       plan,
       LogicalPlan::CreateTable { .. } | LogicalPlan::DropTable { .. }
   ) {
       let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
       return execute_catalog_plan(self.catalog.as_ref(), &bound).await;
   }
   ```

   This branch must remain **before** the SELECT
   descriptor-evolution retry loop (D10).

3. Distributed coordinator intercept:

   ```rust
   if matches!(
       bound,
       ExecutionPlan::CreateTable { .. } | ExecutionPlan::DropTable { .. }
   ) {
       return execute_catalog_plan(self.catalog.as_ref(), &bound).await;
   }
   ```

   This branch must remain **before**
   INSERT / INSERT SELECT / CTAS rejects and **before**
   `fragment_plan` (D10).

**Verification:**

```sh
cargo test -p toron coordinator::tests
cargo test -p toron --test drop_table_test  # once Phase 7 lands
```

## Phase 6 — Defensive rejects + walker leaf treatment

**Files:**

- `crates/toron/src/exec.rs`
- `crates/toron/src/distributed/fragment.rs`
- `crates/toron/src/distributed/scheduler.rs`
- `crates/toron/src/server/mod.rs`
- `crates/toron/tests/wendelon_storage_execution_test.rs`
  (exhaustive helper arm if compiler flags)

**Tasks:**

1. **Executor** (`exec.rs`):

   - `LogicalPlan::DropTable` arm in unbound reject
     path: `ExecutionError::NotImplemented { feature:
     "unbound DROP TABLE execution" }`;
   - `ExecutionPlan::DropTable` arm in bound reject
     path: `ExecutionError::NotImplemented { feature:
     "DROP TABLE execution is coordinator-only" }`.

2. **Fragmenter** (`distributed/fragment.rs`):

   - Add `FragmentError::DropTableNotFragmentable`
     variant with error string `"DROP TABLE is a
     coordinator-only catalog operation"`;
   - Reject in `fragment_plan` BEFORE
     `contains_join(&plan)` (mirrors ADR 032 / 033
     reject ordering);
   - Walker leaves in `first_join`, `find_exchange`,
     `count_exchanges`, `first_scan_metadata`,
     `replace_exchange_with_scan`,
     `replace_first_join`: return None / 0 /
     unchanged.

3. **Scheduler** (`distributed/scheduler.rs`):

   - Walker leaf in `first_scan_fingerprint`: return
     `None`.

4. **Server** (`server/mod.rs`):

   - Walker leaves in `join_strategy_label`,
     `contains_join`, `contains_broadcast_join`,
     `scan_schema_for_table_id`: return None / false
     / None;
   - `operator_kind_label` returns `"drop_table"`
     for the new variant (D13 / Claude iter-0 C2;
     distinct from `"catalog"` for CREATE TABLE,
     `"insert"` for INSERT, `"insert_select"` for
     InsertSelect, `"create_table_as_select"` for
     CTAS).

5. **Test helper exhaustiveness**
   (`wendelon_storage_execution_test.rs`): add
   `DropTable` arm if the compiler flags any
   exhaustive test-side match.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --test distributed_process_test
```

## Phase 7 — Functional tests

**File:** new `crates/toron/tests/drop_table_test.rs`
(Claude iter-1 A3 file-naming convention).

11 required functional fixtures:

### Fixture 1 — `planner_accepts_drop_table_surface`

```rust
#[test]
fn planner_accepts_drop_table_surface() {
    // DROP TABLE sales
    // DROP TABLE IF EXISTS app.sales
    // assert LogicalPlan::DropTable fields
}
```

### Fixture 2 — `planner_rejects_unsupported_drop_table_shapes`

```rust
#[test]
fn planner_rejects_unsupported_drop_table_shapes() {
    // DROP VIEW v
    // DROP TABLE a, b
    // DROP TABLE sales CASCADE
    // DROP TABLE sales RESTRICT
    // DROP TABLE sales PURGE
    // DROP TEMPORARY TABLE sales
    // DROP TABLE a.b.c
}
```

### Fixture 3 — `binder_emits_identity_free_drop_table`

```rust
#[tokio::test]
async fn binder_emits_identity_free_drop_table() {
    // bind with empty MockCatalog
    // assert ExecutionPlan::DropTable { name, if_exists }
    // proves binder does not resolve
}
```

### Fixture 4 — `mock_catalog_drop_table_semantics`

```rust
#[tokio::test]
async fn mock_catalog_drop_table_semantics() {
    // existing returns Some(binding)
    // dropped table no longer resolves
    // missing without IF EXISTS returns TableNotFound
    // missing with IF EXISTS returns None
}
```

### Fixture 5 — `mock_catalog_drop_table_schema_alias_cleanup_is_exact`

```rust
#[tokio::test]
async fn mock_catalog_drop_table_schema_alias_cleanup_is_exact() {
    // create app.sales, then other.sales
    // verify unqualified `sales` points to app.sales (ADR 029 alias-no-overwrite)
    // drop other.sales: unqualified sales still resolves to app.sales
    // drop app.sales: unqualified sales no longer resolves
    // do NOT assert alias repointing
}
```

### Fixture 6 — `query_coordinator_executes_drop_table`

```rust
#[tokio::test]
async fn query_coordinator_executes_drop_table() {
    // create, insert, select succeeds
    // drop
    // SQL select fails at bind
    // DROP TABLE IF EXISTS missing succeeds
}
```

### Fixture 7 — `query_coordinator_recreate_after_drop_isolated`

```rust
#[tokio::test]
async fn query_coordinator_recreate_after_drop_isolated() {
    // create, insert, drop
    // recreate same name
    // select through normal SQL does not see old rows
    // (MockCatalog assigns new monotonic TableId)
}
```

### Fixture 8 — `query_coordinator_ctas_cleanup_allows_rerun` (D14)

```rust
#[tokio::test]
async fn query_coordinator_ctas_cleanup_allows_rerun() {
    // create source
    // CTAS target
    // drop target
    // rerun same CTAS target
    // select target succeeds
    // proves the manual cleanup path ADR 033 D7 named (Claude iter-0 C1)
}
```

### Fixture 9 — `distributed_coordinator_executes_drop_table_before_fragment`

```rust
#[tokio::test]
async fn distributed_coordinator_executes_drop_table_before_fragment() {
    // MockCatalog + empty StaticWorkerRegistry + failing transport
    // create table through catalog or coordinator
    // run distributed DROP
    // verify table no longer resolves
    // failing transport should not be touched (metadata-only path)
}
```

### Fixture 10 — `wendelon_catalog_drop_table_semantics`

```rust
#[tokio::test]
async fn wendelon_catalog_drop_table_semantics() {
    // build WendelonCatalog::from_descriptors
    // known DROP returns AdminUnavailable
    // unknown DROP TABLE IF EXISTS missing returns success (Ok(None))
    // unknown DROP TABLE missing returns TableNotFound
}
```

### Fixture 11 — `executor_and_fragmenter_reject_direct_drop_table_execution`

```rust
#[tokio::test]
async fn executor_and_fragmenter_reject_direct_drop_table_execution() {
    // construct ExecutionPlan::DropTable
    // executor rejects bound + unbound
    // fragmenter returns DropTableNotFragmentable
}
```

**Verification:**

```sh
cargo test -p toron --test drop_table_test
```

## Phase 8 — Architecture guards

**File:** `crates/toron/tests/architecture_test.rs`.

**17 ADR-034-specific guards + 1 paired adjustment to
the existing variant-count catch-all = 18 architecture
guard touchpoints.**

Each guard names ADR 034 + its specific decision /
contribution in the diagnostic string (per the ADR 015
cycle narrow-detector pattern).

| # | Guard | Pins |
|---|---|---|
| 1 | `adr034_sql_planner_accepts_drop_table` | Dispatcher contains `Statement::Drop`; unsupported diagnostic includes `DROP TABLE` |
| 2 | `adr034_drop_table_destructure_is_exhaustive` | `lower_drop_table` body contains all DROP fields; no `..` |
| 3 | `adr034_drop_table_reject_surface_is_narrow` | Checks `ObjectType::Table`; rejects multi-name + `cascade` + `restrict` + `purge` + `temporary` |
| 4 | `adr034_drop_table_name_lowering_is_ddl_specific` | Neutral `lower_ddl_table_name` exists; CREATE + CTAS + DROP all call it; rejects three-part names; does NOT call `table_ref_from_object_name` |
| 5 | `adr034_logical_variant_count_is_14` | `LogicalPlan` 13 → 14 |
| 6 | `adr034_execution_variant_count_is_12` | `ExecutionPlan` 11 → 12 |
| 7 | `adr034_execution_drop_table_has_no_identity_or_box` | Body contains only `name` + `if_exists`; no `TableId` / `TableObjectId` / `PhysicalDescriptorFingerprint` / `Box<` |
| 8 | `adr034_binder_drop_table_is_pass_through` | Binder arm emits `ExecutionPlan::DropTable`; no `.resolve_table(` / `.resolve_table_id(` / `.drop_table(` |
| 9 | `adr034_catalog_drop_table_returns_optional_binding` | Trait signature: `Result<Option<TableBinding>, CatalogError>` |
| 10 | `adr034_mock_catalog_drop_table_cleans_aliases` | Body removes from `by_id`; uses `by_ref.retain`; compares against dropped id; no alias repointing |
| 11 | `adr034_wendelon_catalog_drop_table_is_admin_unavailable` | Known path returns `AdminUnavailable` with `DROP TABLE` message; unknown `if_exists` path returns `Ok(None)` |
| 12 | `adr034_coordinators_intercept_drop_table` | Local DropTable branch before retry loop; distributed DropTable branch before `fragment_plan`; `execute_catalog_plan` contains `.drop_table(` |
| 13 | `adr034_executor_and_fragmenter_reject_drop_table` | Executor strings present; `DropTableNotFragmentable` present; fragmenter reject before join/exchange |
| 14 | `adr034_distributed_walkers_treat_drop_table_as_leaf` | Fragmenter / scheduler / server helper arms include `ExecutionPlan::DropTable { .. }` as leaf |
| 15 | `adr034_operator_label_is_drop_table` | `operator_kind_label` maps DropTable to `"drop_table"` |
| 16 | `adr034_does_not_add_storage_reclaim_or_plan_arena` | No `StorageReclaim` / `StorageDrop` / table-reclaim trait in `storage.rs`; no `blake3`; no arena dependencies |
| 17 | `adr034_catalog_trait_grows_by_exactly_one_method` (Claude iter-1 C1) | Trait contains `async fn drop_table`; negative-pattern against `truncate_table` / `replace_table` / `drop_schema` / `alter_table` / `rename_table` / `drop_table_outcome` |

Sample guard #17 (Claude iter-1 C1 / A1):

```rust
#[test]
fn adr034_catalog_trait_grows_by_exactly_one_method() {
    let catalog = read_to_string(
        manifest_dir().join("src/catalog.rs")
    ).unwrap();
    let trait_block = brace_blocks_after_marker(&catalog, "pub trait Catalog")
        .into_iter()
        .next()
        .expect("Catalog trait body")
        .1;
    // Positive: drop_table is present.
    assert!(
        trait_block.contains("async fn drop_table"),
        "ADR 034 D7: Catalog must expose drop_table"
    );
    // Negative: no peer DDL deletion / mutation helpers.
    for forbidden in [
        "async fn truncate_table",
        "async fn replace_table",
        "async fn drop_schema",
        "async fn drop_table_outcome",
        "async fn alter_table",
        "async fn rename_table",
    ] {
        assert!(
            !trait_block.contains(forbidden),
            "ADR 034 C1: Catalog must not grow beyond drop_table (found {forbidden})"
        );
    }
}
```

Paired adjustment (Claude iter-1 A4 pattern from ADR
033):

| Existing guard | Adjustment |
|---|---|
| `architecture_guards_unchanged_plus_sort_variant` | `LogicalPlan` count assert bumps 13 → 14 + add `DropTable` content assert; `ExecutionPlan` count assert bumps 11 → 12 + add `DropTable` content assert |

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

## Phase 9 — Full verification

```sh
cargo fmt --check
cargo check -p toron
cargo test -p toron --test drop_table_test
cargo test -p toron --test create_table_test
cargo test -p toron --test ctas_test
cargo test -p toron --test insert_test
cargo test -p toron --test architecture_test
cargo test -p toron
```

Expected:

- All tests pass.
- Only the existing `rune v0.13.4` future-incompat
  warning may appear.
- No new warnings from ADR 034.

Post-PR audits:

```sh
rg -n "DropTable|drop_table" crates/toron/src crates/toron/tests
rg -n "fn lower_drop_table" crates/toron/src/sql_planner.rs
rg -n "fn lower_ddl_table_name" crates/toron/src/sql_planner.rs
# Expected: rename is complete; lower_create_table_name has zero hits.
rg -n "fn lower_create_table_name" crates/toron/src/sql_planner.rs
# Expected: zero hits
rg -n "DropTableNotFragmentable" crates/toron/src/distributed
# Expected: present in fragment.rs
rg -n "async fn drop_table" crates/toron/src/catalog.rs
# Expected: trait definition + MockCatalog impl + WendelonCatalog impl
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits
```

## Phase 10 — Promotion-time housekeeping (out of PR scope)

1. Roadmap bump: move ADR 034 from candidate to
   promoted; advance Forward sequence to ADR 035
   candidates (row-identity / predicate-delete
   substrate per D15 sequencing commitment).
2. Archive WIP iter-0 / iter-1 / iter-2 docs.
3. Update bench-results pointer if applicable.

## Locked patch order

1. **Phase 0** — Preflight audit (read-only).
2. **Phase 1** — IR shape (`ir.rs` +
   `ir/execution.rs` variants).
3. **Phase 2** — SQL lowering + helper rename
   (`sql_planner.rs`).
4. **Phase 3** — Binder pass-through (`binder.rs`).
5. **Phase 4** — Catalog API + implementations
   (`catalog.rs`).
6. **Phase 5** — Coordinator routing
   (`coordinator.rs`).
7. **Phase 6** — Defensive rejects + walker leaves
   (`exec.rs` + `distributed/*` + `server/mod.rs`).
8. **Phase 7** — Functional tests
   (`tests/drop_table_test.rs`: 11 fixtures).
9. **Phase 8** — Architecture guards
   (`tests/architecture_test.rs`: 17 new + 1
   paired adjustment).
10. **Phase 9** — Full verification.

Mechanical ordering: Phases 1-3 precede Phase 4
(binder needs IR variants; catalog trait extension
is independent of binder but compiles after binder
to satisfy exhaustive matches). Phase 5 gates Phase
6 (walker rejects become real when coordinator
routing exists). Phases 7-9 are verification.

## Definition of Done

- [ ] `LogicalPlan::DropTable { name, if_exists }`
      defined (13 → 14).
- [ ] `ExecutionPlan::DropTable { name, if_exists }`
      defined (11 → 12); **no TableId /
      TableObjectId / fingerprint fields, no boxed
      child** (D5).
- [ ] `lower_create_table_name` renamed to
      `lower_ddl_table_name`; both call sites
      (`lower_create_table` + `lower_create_table_as_select`)
      updated (D4 / Claude iter-1 C2).
- [ ] `SqlPlanner::plan` dispatches `Statement::Drop`
      to `lower_drop_table`; supported-statement
      diagnostic includes `DROP TABLE`.
- [ ] `lower_drop_table` exhaustively destructures
      `Statement::Drop` with no `..`; rejects non-
      table object type + multi-name + CASCADE +
      RESTRICT + PURGE + TEMPORARY with stable
      diagnostics (D2 / D3).
- [ ] `Binder::bind_plan` `DropTable` arm emits
      `ExecutionPlan::DropTable` directly; no
      catalog calls (D6).
- [ ] `Catalog::drop_table(&self, name, if_exists)
      -> Result<Option<TableBinding>, CatalogError>`
      trait method added (D7).
- [ ] `MockCatalog::drop_table` removes from
      `by_id`; uses `by_ref.retain`; does not
      repoint aliases; does not reuse
      `next_table_id` (D8).
- [ ] `WendelonCatalog::drop_table` returns
      `AdminUnavailable` for known tables (with
      `DROP TABLE` in the message); returns
      `Ok(None)` for unknown tables with `IF
      EXISTS` (D7).
- [ ] `execute_catalog_plan` extended to handle
      both `CreateTable` and `DropTable` arms.
- [ ] Local `QueryCoordinator::execute_query`
      intercepts `LogicalPlan::DropTable` next to
      `CreateTable`, before SELECT retry loop
      (D10).
- [ ] `DistributedQueryCoordinator::execute_query`
      routes `ExecutionPlan::DropTable` through
      `execute_catalog_plan` before `fragment_plan`
      (D10).
- [ ] `FragmentError::DropTableNotFragmentable`
      reject fires before `contains_join` /
      `count_exchanges` (D12).
- [ ] Distributed / scheduler / server walkers
      treat `DropTable` as leaf (D12 / guard #14).
- [ ] Telemetry label `"drop_table"` (D13 / guard
      #15 / Claude iter-0 C2).
- [ ] Defensive executor + unbound / bound DROP
      rejects (D12).
- [ ] **All 17 ADR-034-specific architecture
      guards pass** (including Claude iter-1 C1
      catalog-trait-narrowness guard #17).
- [ ] **Paired adjustment to
      `architecture_guards_unchanged_plus_sort_variant`
      passes.**
- [ ] **All 11 functional fixtures pass.**
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT / CTAS tests green.
- [ ] All existing SELECT-only plan-digest fixtures
      green (composition-cycle carry-forward).
- [ ] ADR 029 catalog metadata-only preserved.
- [ ] ADR 030 `ExecutionPlan::Insert` expression-
      free preserved.
- [ ] ADR 031 `BoundExprProgram` `PartialEq`-only
      preserved.
- [ ] ADR 032 InsertSelect invariants preserved.
- [ ] ADR 033 CTAS invariants preserved (including
      `materialize_insert_select_batch` `Ok(None)`
      zero-row contract + `materialize_empty_record_batch`
      sibling helper).
- [ ] `StorageWrite` trait shape unchanged.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] No storage reclaim trait / helper introduced.
- [ ] Roadmap moves ADR 034 from candidate to
      promoted; advances active section to ADR 035
      (row-identity / predicate-delete substrate
      per D15 sequencing commitment).
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# Development cadence
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron --test drop_table_test
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test ctas_test
cargo test -p toron --test insert_test
cargo test -p toron --test distributed_process_test
cargo test -p toron

# Post-PR audits
rg -n "DropTable|drop_table" crates/toron/src crates/toron/tests
rg -n "fn lower_drop_table|fn lower_ddl_table_name" crates/toron/src/sql_planner.rs
rg -n "fn lower_create_table_name" crates/toron/src/sql_planner.rs
# Expected on the last: zero hits (rename complete).
rg -n "async fn drop_table" crates/toron/src/catalog.rs
# Expected: trait definition + MockCatalog impl + WendelonCatalog impl.
rg -n "DropTableNotFragmentable" crates/toron/src/distributed
rg -n "\"drop_table\"" crates/toron/src/server/mod.rs
# Expected: present in operator_kind_label.
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits.

# Verify ADR 033 helper untouched
rg -A 3 "fn materialize_insert_select_batch" \
  crates/toron/src/coordinator/write.rs | grep -q "Ok(None)" \
  || echo "ADR 033 invariant violation: materialize_insert_select_batch zero-row contract changed"
```

## Implementation risks

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Bound DROP variant accidentally gains identity fields | Phase 1 explicit shape; Phase 8 guard #7. |
| R2 | Binder resolves table or calls `drop_table` | Phase 3 task 2 explicit non-call; Phase 8 guard #8 negative-pattern. |
| R3 | Coordinator order swaps (DROP routed through fragment instead of catalog helper) | Phase 5 explicit ordering; Phase 8 guard #12 byte-position comparison. |
| R4 | `Catalog` trait grows beyond `drop_table` | Phase 8 guard #17 negative-pattern enforcement. |
| R5 | MockCatalog repoints aliases on drop | Phase 4 task 2 explicit no-repointing; Phase 8 guard #10 positive `retain` + negative repointing assertions. |
| R6 | WendelonCatalog accepts known-table DROP | Phase 4 task 3 explicit `AdminUnavailable`; Phase 8 guard #11. |
| R7 | Telemetry label drift | Phase 6 task 4 explicit; Phase 8 guard #15. |
| R8 | DDL name helper rename incomplete (one call site missed) | Phase 2 task 2 explicit two-site update; Phase 8 guard #4 source-checks all three callers (CREATE + CTAS + DROP) use neutral helper. |
| R9 | sqlparser `Statement::Drop` shape changes silently accepted | Phase 8 guard #2 enforces exhaustive destructure with no `..`. |
| R10 | DELETE WHERE smuggled in via DROP cycle | D15 sequencing commitment in ADR; D2 / D9 / D16 out-of-scope; no row-identity code added. |
| R11 | Storage reclaim helper added | Phase 8 guard #16 negative-pattern check on `storage.rs` + `Cargo.toml`. |
| R12 | Stale-bound-plan after drop behavior over-specified | D9 makes stale bound plans out of scope; Fixture 6 pins SQL rebind/recreate behavior instead of storage behavior. |
| R13 | Existing CREATE TABLE / CTAS tests regress through DDL helper rename | Mechanical two-site update; existing tests verify behavior unchanged; Phase 9 runs ADR 029 + 033 test files. |
| R14 | Plan Arena pressure increases | DROP adds leaf variant; no boxed child; net pressure essentially unchanged; guard #16 forbids arena dependency. |
| R15 | CTAS rollback test (#8) misleads users about atomicity | D14 + D7 (ADR 033 carry-forward) explicitly document non-atomicity; fixture name `_cleanup_allows_rerun` doesn't suggest atomicity. |
| R16 | Multi-target `DROP TABLE a, b` accidentally accepted | Phase 2 task 6 explicit `names.len() != 1` reject; Phase 7 Fixture 2; Phase 8 guard #3. |
| R17 | Walker leaf treatment incomplete (some walker arm not added) | Phase 1 task 4 lets compiler flag exhaustive matches; Phase 6 systematic enumeration; Phase 8 guard #14 enumerated checks. |
