# Implementation Plan: ADR 031 — Bound Expression Program

**Companion ADR:** `plans/adr/031_bound_expr_program.md`

## Goal

Replace Toron's recursive bound physical expression IR
(`ExecutionExpr`) with an index-based
`BoundExprProgram` across binder, executor, PBC
compiler, fragmenter, scheduler, runtime-filter, and
distributed task carriers. Logical `Expr` is
untouched; ADR 028 grouped-aggregate signature
substrate stays on logical `Vec<Expr>`; ADR 029 +
030 CREATE TABLE / INSERT paths are unaffected.

Preserve:

- Logical `Expr` + `LogicalPlan` SQL planner
  contracts unchanged.
- ADR 028 D2 `GroupedAggregateSignature` planner-
  private over logical `Vec<Expr>` with all six
  structural visitor helpers unchanged.
- ADR 029 `Catalog` metadata-only.
- ADR 030 `ExecutionPlan::Insert` expression-free.
- ADR 030 `WendelonStorageWrite` /
  `wendelon_client::Client::table_write` /
  `await_finalization: true` wire path.
- ADR 027 `ExecutionSortKey` ordinal-only.
- `Join` keys + `Scan.projection` ordinal-only.
- All cross-ADR invariants from ADR 022 / 023 / 024 /
  025 / 026 / 027 / 028 / 029 / 030 except the
  byte-identical SELECT plan-digest invariant (D14
  explicit break).

No new SQL syntax; no new expression operations; no
new aggregate kernel; no PBC opcode change; no
fragmenter detector change for SELECT paths; no
cross-ring engagement; no Wendelon-side change.

**Ten-phase rollout** with one compile-restoring
switch (Phase 5). Phases 1-4 build parallel surfaces
while keeping the codebase buildable; Phase 5 flips
`ExecutionPlan` field types; Phases 6-10 migrate
consumers, remove old substrate, harden tests, and
verify.

## Implementation constraints

- Do not replace logical `Expr`.
- Do not add arena lifetimes (`'arena`) to public
  types.
- Do not add external allocator crates (`bumpalo` /
  `typed-arena` / `slab` / `generational-arena`).
- Do not implement typed opcode tape, SIMD, JIT, or
  CSE in this cycle.
- Do not deduplicate literals in v1.
- Do not derive `Eq` or `Hash` on
  `BoundExprProgram` / `BoundExprNode` /
  `BoundAggregateCall`.
- Do not store program roots as side fields on each
  plan variant.
- Do not migrate `ExecutionSortKey`, `Scan.projection`,
  or `Join` keys.
- Do not change ADR 030 `ExecutionPlan::Insert`.
- Do not change ADR 029 `ExecutionPlan::CreateTable`.
- Do not preserve old `ExecutionPlan` serde bytes.
- Do not leave PBC on the old expression tree.
- Do not migrate ADR 028 grouped-aggregate signatures
  off logical `Expr`.
- Do not add `blake3` to `crates/toron/Cargo.toml`.
- Do not add a persistent `tree_view` helper that
  preserves the recursive abstraction under a new
  name.
- Do not silently delete ADR 024 / 025 / 026 / 028
  architecture guards.

## Touchpoint sweep (code-grounded, current `main`)

| File | References | Phase |
|---|---:|---|
| `crates/toron/src/ir/execution.rs` | 31 | 1, 5, 7 |
| `crates/toron/src/binder.rs` | 103 | 3, 5, 7 |
| `crates/toron/src/exec.rs` | 26 | 2, 5, 6, 7 |
| `crates/toron/src/pbc.rs` | 40 | 4, 5, 7 |
| `crates/toron/src/exec/runtime_filter.rs` | 14 | 6 |
| `crates/toron/src/distributed/fragment.rs` | 9 | 6 |
| `crates/toron/src/distributed/scheduler.rs` | 4 | 6 |
| `crates/toron/src/distributed/task.rs` | 2 | 6 |
| `crates/toron/src/server/mod.rs` | (small) | 6 |
| `crates/toron/tests/architecture_test.rs` | (guards) | 8 |
| `crates/toron/tests/bound_expr_program_test.rs` (NEW, optional) | — | 9 |

Total production `ExecutionExpr` references at
baseline: **229 across 8 files** (verified by
`rg -n "ExecutionExpr" crates/toron/src`).

## Phase 0 — Preflight audit

**Files:** read-only.

```sh
# Confirm current ExecutionExpr consumer surface
rg -n "ExecutionExpr" crates/toron/src

# Confirm architecture guards that pin boxed execution expressions
rg -n "ExecutionExpr|adr024_|adr025_|adr026_|adr028_binder_executor" \
  crates/toron/tests/architecture_test.rs

# Confirm baseline green
cargo test -p toron --test architecture_test
cargo test -p toron --test group_by_test
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron --test distributed_process_test
cargo test -p toron

# Confirm no shadow dependencies
rg -n "blake3" crates/toron crates/toron/Cargo.toml
rg -n "bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
```

Expected baseline:

- 229 `ExecutionExpr` production references across 8
  files.
- ADR 024/025/026/028 guards at
  `architecture_test.rs:558, 665, 914, 994-1286,
  1260-1500, 1611, 1838`.
- All test suites green.
- No `blake3` / `bumpalo` / `typed-arena` / `slab` /
  `generational-arena` dependencies in toron's
  `Cargo.toml`.

**Preserve verbatim:**

- All `Catalog` trait methods + semantics (ADR 029).
- All ADR 030 `StorageWrite` / `InMemoryTableStorage`
  / `WendelonStorageWrite` paths.
- All `LogicalPlan` walkers in `sql_planner.rs`,
  `binder.rs`.
- `Scan.projection` / `Sort.order_by` / `Join.left_keys`
  / `Join.right_keys` / `CreateTable` / `Insert`
  `ExecutionPlan` variants.
- ADR 028 `GroupedAggregateSignature` + six
  structural visitor helpers.

## Phase 1 — IR types and builder

**Goal.** Add `BoundExprId`, `BoundBinaryOp`,
`BoundUnaryOp`, `BoundExprNode`, `BoundExprProgram`,
`BoundAggregateCall`, `BoundExprProgramBuilder`.

**Files:** `crates/toron/src/ir/execution.rs`.

**Tasks:**

1. **Add types** per ADR 031 §"Decision":

   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
   pub struct BoundExprId(pub u32);

   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
   pub enum BoundBinaryOp {
       Add, Sub, Mul, Div,
       Eq, Neq, Lt, Lte, Gt, Gte,
       And, Or,
   }

   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
   pub enum BoundUnaryOp { Not, IsNull, IsNotNull }

   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
   pub enum BoundExprNode {
       Column(ResolvedColumn),
       Literal(u32),
       Cast { input: BoundExprId, target: DataType },
       TryCast { input: BoundExprId, target: DataType },
       Binary { op: BoundBinaryOp, left: BoundExprId, right: BoundExprId },
       Unary { op: BoundUnaryOp, input: BoundExprId },
       TimeBucket { interval: std::time::Duration, input: BoundExprId },
   }

   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
   pub struct BoundExprProgram {
       pub nodes: Vec<BoundExprNode>,
       pub node_types: Vec<DataType>,
       pub literals: Vec<ScalarValue>,
       pub roots: Vec<BoundExprId>,
   }

   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
   pub struct BoundAggregateCall {
       pub function: AggregateFunction,
       pub args: BoundExprProgram,
   }
   ```

   `BoundExprId` derives `Eq + Hash` (it's a `u32`
   newtype). `BoundExprProgram` / `BoundExprNode` /
   `BoundAggregateCall` derive `PartialEq` only.

2. **Add `BoundExprProgramBuilder`:**

   ```rust
   pub struct BoundExprProgramBuilder {
       nodes: Vec<BoundExprNode>,
       node_types: Vec<DataType>,
       literals: Vec<ScalarValue>,
       roots: Vec<BoundExprId>,
   }
   ```

   Methods:

   - `new() -> Self`
   - `append_column(ResolvedColumn) -> BoundExprId`
   - `append_literal(ScalarValue) -> BoundExprId`
   - `append_cast(input: BoundExprId, target: DataType, output_type: DataType) -> BoundExprId`
   - `append_try_cast(input: BoundExprId, target: DataType, output_type: DataType) -> BoundExprId`
   - `append_binary(op: BoundBinaryOp, left: BoundExprId, right: BoundExprId, output_type: DataType) -> BoundExprId`
   - `append_unary(op: BoundUnaryOp, input: BoundExprId, output_type: DataType) -> BoundExprId`
   - `append_time_bucket(interval: Duration, input: BoundExprId, output_type: DataType) -> BoundExprId`
   - `push_root(id: BoundExprId)`
   - `build() -> BoundExprProgram`

   Builder invariants enforced by construction:

   - `node_types.len() == nodes.len()` after every
     append.
   - every child ID passed to `append_*` is in
     bounds (`< nodes.len()`).
   - every child ID < parent ID (topological order
     by construction since builder only returns IDs
     for already-appended nodes).
   - root IDs passed to `push_root` are in bounds.
   - zero-root programs are valid (for `COUNT(*)`).
   - literals append in encounter order; no dedup.

3. **Add helper methods on `BoundExprProgram`:**

   ```rust
   pub fn root_count(&self) -> usize;
   pub fn root_type(&self, root_index: usize) -> Option<&DataType>;
   pub fn single_root(&self) -> Option<BoundExprId>;
   pub fn single_column_root(&self) -> Option<&ResolvedColumn>;
   pub fn root_columns(&self) -> Option<Vec<&ResolvedColumn>>;
   pub fn is_empty_roots(&self) -> bool;
   pub fn validate(&self) -> Result<(), BoundExprProgramError>;
   pub fn pretty(&self) -> String;
   ```

   `pretty()` renders the program as a top-down tree
   walking from each root for readable test failures.

4. **Add `BoundExprProgramError`** for `validate()`
   defensive checks (only used in tests; programs
   built via builder are valid by construction).

5. **Add unit tests** in `ir/execution.rs::tests`:

   - builder appends Column / Literal / Cast / Binary
     / Unary / TimeBucket nodes; output types
     recorded correctly.
   - root types reported correctly.
   - malformed root IDs fail `validate`.
   - malformed child ordering fails `validate`.
   - zero-root program valid.
   - `pretty()` handles shared child nodes + zero-
     root programs.

**Verification:**

```sh
cargo test -p toron ir::execution
cargo check -p toron  # rest of crate still compiles unchanged
```

## Phase 2 — Program evaluator in parallel

**Goal.** Add `Executor::evaluate_bound_program`
linear evaluator while keeping
`evaluate_bound_expr` intact.

**Files:** `crates/toron/src/exec.rs`,
`crates/toron/src/ir/execution.rs`.

**Tasks:**

1. **Add `Executor::evaluate_bound_program`:**

   ```rust
   pub fn evaluate_bound_program(
       &self,
       program: &BoundExprProgram,
       batch: &RecordBatch,
   ) -> Result<Vec<ArrayRef>, ExecutionError>
   ```

   Implementation:

   - allocate `Vec<ArrayRef>` of size `program.nodes.len()`.
   - iterate `program.nodes.iter().enumerate()`
     linearly.
   - evaluate child arrays by `BoundExprId` lookup
     into the scratch `Vec<ArrayRef>`.
   - store output at the same index.
   - return arrays for `program.roots` in root order.
   - reuse existing typed helpers: `evaluate_cast`,
     `evaluate_try_cast`, arithmetic / comparison /
     boolean / null kernels, `TimeBucket` logic.

   **No recursion** over `BoundExprNode`. The flat-
   loop discipline is part of the contract.

2. **Add `#[cfg(test)]` parity converter:**

   ```rust
   #[cfg(test)]
   pub(crate) fn execution_expr_to_program(
       expr: &ExecutionExpr,
   ) -> BoundExprProgram
   ```

   Test-only. Used in Phases 2-5 parity tests
   while both substrates coexist. Deleted at Phase 7
   before the `ExecutionExpr` type itself is removed,
   so the Phase 8 recursive source guard can assert
   zero `ExecutionExpr` hits in `crates/toron/src`.

3. **Add parity tests** comparing
   `evaluate_bound_expr` vs `evaluate_bound_program`
   for:

   - column read
   - literal evaluation
   - arithmetic (Int32 / Int64 / Float32 / Float64)
   - comparison
   - boolean composition
   - explicit cast (success + error)
   - try-cast (null on failure)
   - null checks
   - `TimeBucket`
   - multi-root projection

**Verification:**

```sh
cargo test -p toron exec::tests
```

## Phase 3 — Binder program production in parallel

**Goal.** Add `bind_expr_into_program` /
`bind_expr_program` while keeping old `bind_expr`
intact.

**Files:** `crates/toron/src/binder.rs`.

**Tasks:**

1. **Add internal helpers:**

   ```rust
   fn bind_expr_into_program(
       builder: &mut BoundExprProgramBuilder,
       expr: &Expr,
       input_schema: &SchemaRef,
       config: &PlannerConfig,
   ) -> Result<BoundExprId, BindingError>

   fn bind_expr_program(
       exprs: &[Expr],
       input_schema: &SchemaRef,
       config: &PlannerConfig,
   ) -> Result<BoundExprProgram, BindingError>
   ```

   `bind_expr_program` constructs a fresh builder,
   binds each `Expr` via `bind_expr_into_program`,
   pushes each result as a root, returns the built
   program. Multi-root by design (for project /
   exchange / group keys).

2. **Preserve binder semantics:**

   - Column resolution (`ColumnOrdinal` +
     `ResolvedColumn` lookup).
   - Literal typing.
   - Explicit cast support (ADR 025 lattice).
   - Try-cast support (ADR 026 lattice).
   - Arithmetic widening + injected casts (ADR 024).
   - Comparison literal coercion.
   - Boolean type requirements.
   - `TimeBucket` experimental gate.
   - PBC eligibility annotation on resolved columns.

3. **Aggregate-args binding via `bind_expr_program`:**

   ```rust
   fn bind_aggregate_call(
       call: &LogicalAggregateCall,
       input_schema: &SchemaRef,
       config: &PlannerConfig,
   ) -> Result<BoundAggregateCall, BindingError>
   ```

   Returns `BoundAggregateCall { function,
   args: BoundExprProgram }` with one root per
   aggregate arg (zero roots for `COUNT(*)`).

4. **Add parity tests:**

   - Bind `Expr` via old `bind_expr` → convert to
     `BoundExprProgram` via `execution_expr_to_program`.
   - Bind same `Expr` via `bind_expr_into_program`
     directly.
   - Assert matching root arity, root types, and
     evaluation results. Do not require byte-identical
     `BoundExprProgram` bytes here: programs
     constructed through different paths may have
     different internal append order while remaining
     behaviorally identical.

5. **Do not yet change `ExecutionPlan` fields.**
   Phase 3 is a buildable checkpoint; the binder
   has both code paths.

**Verification:**

```sh
cargo test -p toron binder::tests
cargo test -p toron --test group_by_test
```

## Phase 4 — PBC compiler migration in parallel

**Goal.** Add `PbcCompiler::compile_program`
entrypoint reading `Option<&BoundExprProgram>` while
keeping old `compile` intact.

**Files:** `crates/toron/src/pbc.rs`,
`crates/toron/src/binder.rs`.

**Tasks:**

1. **Add new compile entrypoint:**

   ```rust
   pub(crate) fn compile_program(
       &self,
       predicate: Option<&BoundExprProgram>,
       projection: Option<&[ColumnOrdinal]>,
   ) -> Result<Vec<u8>, PushdownDeclineReason>
   ```

   Single-root contract enforced at runtime:

   ```rust
   if let Some(predicate) = predicate {
       let root = predicate.single_root()
           .ok_or(PushdownDeclineReason::ExpressionShapeUnsupported(
               "PBC predicate must have exactly one root".into(),
           ))?;
       // ... compile starting from root ...
   }
   ```

2. **Add `compile_node` with explicit stack:**

   ```rust
   fn compile_node(
       &self,
       program: &BoundExprProgram,
       id: BoundExprId,
       bytecode: &mut Vec<u8>,
   ) -> Result<(), PushdownDeclineReason>
   ```

   Uses an explicit `Vec<BoundExprId>` stack for
   postorder traversal — not recursion. Mirrors the
   executor's flat-loop discipline (D4 carry-
   forward).

3. **Preserve PBC semantics verbatim:**

   - Eligible: `Column`, non-null `Literal`,
     comparison (`Binary { op: Eq/Neq/Lt/Lte/Gt/Gte }`),
     boolean (`Binary { op: And/Or }`),
     null check (`Unary { op: IsNull/IsNotNull }`).
   - Declined: `Cast`, `TryCast`, arithmetic
     (`Binary { op: Add/Sub/Mul/Div }`),
     `TimeBucket`.
   - Projection bytecode stays ordinal-based; not
     compiled from a general expression program.

4. **Keep old `compile` temporarily** so other call
   sites compile. Phase 5 removes old `compile` or
   renames `compile_program` back to `compile`.

5. **Add PBC parity tests:**

   - Eligible predicate bytecode byte-identical to
     old compiler output (use `execution_expr_to_program`
     converter for parity input).
   - Cast / TryCast / arithmetic / TimeBucket
     decline reasons byte-identical.
   - Null-literal decline unchanged.

**Verification:**

```sh
cargo test -p toron pbc::tests
cargo test -p toron --test architecture_test
```

## Phase 5 — Switch `ExecutionPlan` field types

**Goal.** Flip expression-bearing plan fields from
`ExecutionExpr` to `BoundExprProgram`. **Only
compile-restoring step in the migration.**

**Files:** `crates/toron/src/ir/execution.rs`,
`crates/toron/src/binder.rs`,
`crates/toron/src/exec.rs`,
`crates/toron/src/pbc.rs`.

**Tasks:**

1. **Change `ExecutionPlan` field types:**

   - `Filter { predicate: BoundExprProgram, ... }`
   - `Project { exprs: BoundExprProgram, ... }`
   - `Aggregate { group_expr: BoundExprProgram, aggr_expr: Vec<BoundAggregateCall>, ... }`
   - `Exchange { key: BoundExprProgram, ... }`

2. **Keep unchanged:**

   - `Scan.projection: Option<Vec<ColumnOrdinal>>`
   - `Sort.order_by: Vec<ExecutionSortKey>`
   - `Join.left_keys: Vec<ColumnOrdinal>`
   - `Join.right_keys: Vec<ColumnOrdinal>`
   - `CreateTable` (ADR 029)
   - `Insert` (ADR 030)

3. **Update binder plan construction call sites:**

   - `bind_filter` produces one-root predicate
     program via `bind_expr_program(&[expr], ...)`.
   - `bind_project` produces multi-root expression
     program via `bind_expr_program(&exprs, ...)`.
   - `bind_aggregate` produces group_expr program +
     `Vec<BoundAggregateCall>` via
     `bind_aggregate_call`.
   - `bind_exchange` produces multi-root key
     program via `bind_expr_program(&keys, ...)`.
   - PBC handoff uses `PbcCompiler::compile_program`.

4. **Update executor call sites:**

   - `Filter` evaluator expects one root + boolean
     result.
   - `Project` evaluator roots align with output
     schema fields.
   - `Aggregate` group-key arrays come from
     `group_expr` program; aggregate args from each
     call's `args` program.
   - `Exchange` key arrays come from `key` program.

5. **Rename `compile_program` → `compile`** (or
   remove the old `compile`; whichever keeps the
   diff cleaner). After Phase 5, only one PBC
   compile entrypoint exists.

6. **`cargo check -p toron` must be green** at the
   end of Phase 5. This is the only acceptable
   compile-restoring window in the migration.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron --test group_by_test
```

## Phase 6 — Distributed + runtime helpers

**Goal.** Migrate fragmenter / scheduler / runtime-
filter / task / server helpers to read
`BoundExprProgram` via narrow column-key helpers.

**Files:**

- `crates/toron/src/exec/runtime_filter.rs`
- `crates/toron/src/distributed/fragment.rs`
- `crates/toron/src/distributed/scheduler.rs`
- `crates/toron/src/distributed/task.rs`
- `crates/toron/src/server/mod.rs`

**Tasks:**

1. **Runtime filter** (`exec/runtime_filter.rs`):

   - Replace `&[ExecutionExpr]` parameters with
     `&BoundExprProgram`.
   - Use `single_column_root` / `root_columns` for
     eligibility checks.
   - Keep single-column eligibility rules unchanged.

2. **Fragmenter** (`distributed/fragment.rs`):

   - Update `ExchangeBoundary.key` carrier from
     `Vec<ExecutionExpr>` → `BoundExprProgram`.
   - Update aggregate-peel group / projection /
     having carriers.
   - `ExecutionSortKey` stays ordinal-only (ADR 027
     carry-forward).
   - `Join` keys stay `Vec<ColumnOrdinal>`.

3. **Scheduler** (`distributed/scheduler.rs`):

   - Update shuffle-producer partition-key
     inspection to use `single_column_root`.
   - Use narrow helpers for runtime-filter probe
     descriptor logic.

4. **Task wire carriers** (`distributed/task.rs`):

   - `TaskOutputBinding::ShuffleProducer`
     partition-key field: `Vec<ExecutionExpr>` →
     `BoundExprProgram`.
   - Update serde round-trip tests.

5. **Server / test helpers** (`server/mod.rs`):

   - Update pattern matches + labels only where
     needed.
   - Do not introduce broad program walking into
     unrelated server paths.

**Verification:**

```sh
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron --test distributed_process_test
cargo test -p toron
```

## Phase 7 — Remove transitional IR

**Goal.** Remove `ExecutionExpr` and
`evaluate_bound_expr` from production source.

**Files:**

- `crates/toron/src/ir/execution.rs`
- `crates/toron/src/binder.rs`
- `crates/toron/src/exec.rs`
- `crates/toron/src/pbc.rs`
- tests that used the transitional converter

**Tasks:**

1. Remove `ExecutionExpr` enum + variants from
   `ir/execution.rs`.
2. Remove `evaluate_bound_expr` from `exec.rs`.
3. Remove old `PbcCompiler::compile` if any tree-
   shaped entrypoint remains (only the program
   entrypoint should exist after Phase 5).
4. Remove production `ExecutionExpr` consumers in
   binder; delete `execution_expr_to_program`.
5. Remove old tree-based construction helpers from
   tests where parity coverage is no longer needed.
6. **Audit:**

   ```sh
   rg -n "ExecutionExpr|Box<ExecutionExpr>" crates/toron/src
   ```

   Expected: **zero hits in `crates/toron/src`**.

**Verification:**

```sh
cargo check -p toron
cargo test -p toron
```

## Phase 8 — Architecture guards

**Goal.** 10 new ADR 031 guards + 6 adjusted ADR
024/025/026/028 guards + 2 verify-unchanged guards.

**File:** `crates/toron/tests/architecture_test.rs`.

**Tasks:**

### 10 new ADR 031 guards

1. **`adr031_bound_expr_program_shape`** —
   source-reads `BoundExprProgram { nodes,
   node_types, literals, roots }` + `BoundExprId(pub
   u32)`.

2. **`adr031_bound_expr_node_has_no_boxes`** — uses
   `source_lines(&manifest_dir().join("src"))`
   recursive scan:

   ```rust
   for (path, line, text) in source_lines(&manifest_dir().join("src")) {
       assert!(
           !text.contains("Box<ExecutionExpr>"),
           "ADR 031 D11: production source must not contain Box<ExecutionExpr> at {}:{}: {}",
           path.display(), line, text
       );
       assert!(
           !text.contains("ExecutionExpr::"),
           "ADR 031: production source must not reference ExecutionExpr:: at {}:{}: {}",
           path.display(), line, text
       );
   }
   // Sidecar: BoundExprNode itself has no Box variants.
   let execution = read_to_string(manifest_dir().join("src/ir/execution.rs")).unwrap();
   let node_enum = execution.split("pub enum BoundExprNode").nth(1)
       .and_then(|rest| rest.split("}").next())
       .expect("BoundExprNode enum body");
   assert!(!node_enum.contains("Box<"));
   ```

   Asserts `BoundExprNode` has exactly 7 variants
   (`Column`, `Literal`, `Cast`, `TryCast`,
   `Binary`, `Unary`, `TimeBucket`).

3. **`adr031_bound_expr_program_partial_eq_only`** —
   source-checks `BoundExprProgram` /
   `BoundExprNode` / `BoundAggregateCall` derive
   `PartialEq` but not `Eq` or `Hash`.

4. **`adr031_execution_plan_uses_bound_expr_program`** —
   asserts `Filter` / `Project` / `Aggregate` /
   `Exchange` field shapes contain
   `BoundExprProgram`; asserts `BoundAggregateCall`.

5. **`adr031_execution_plan_insert_stays_expression_free`** —
   asserts `ExecutionPlan::Insert` still has
   `rows: Vec<Vec<ScalarValue>>`; no
   `BoundExprProgram`.

6. **`adr031_join_and_sort_remain_ordinal_keyed`** —
   asserts `Join` keys + `ExecutionSortKey` stay
   ordinal-only.

7. **`adr031_pbc_reads_bound_expr_program`** —
   source-reads `PbcCompiler::compile` signature
   for `Option<&BoundExprProgram>`; asserts
   absence of `Option<&ExecutionExpr>`; asserts
   `single_root` usage.

8. **`adr031_executor_evaluates_program_linearly`** —
   source-reads `Executor::evaluate_bound_program`
   for linear `program.nodes.iter().enumerate()`
   iteration; rejects evaluator recursion over
   `BoundExprNode`.

9. **`adr031_binder_emits_bound_expr_program`** —
   asserts binder uses
   `BoundExprProgramBuilder`; asserts no
   production `ExecutionExpr::Cast` / `TryCast`
   emission.

10. **`adr031_adr028_signature_stays_logical_expr`** —
    asserts `GroupedAggregateSignature` still
    stores `Vec<Expr>`; ADR 028 structural visitor
    helpers preserved.

### 6 adjusted ADR 024/025/026/028 guards

| Guard | Anchor | Adjustment |
|---|---|---|
| `architecture_guards_unchanged_plus_sort_variant` | line 558 | `ExecutionPlan` variant count stays 9; add `BoundExprProgram` content asserts for `Filter` / `Project` / `Aggregate` / `Exchange`; preserve existing `Sort` / `CreateTable` / `Insert` asserts. |
| `adr024_scalar_arithmetic_guards_are_narrow` | line 914 | Replace `Box<ExecutionExpr>` arithmetic-variant pins (lines 931-936) with `BoundBinaryOp::{Add, Sub, Mul, Div}` + `BoundExprNode::Binary` shape. |
| ADR 025 cast guards | lines 994, 1054, 1109, 1135, 1207, 1246, 1286 | Replace `ExecutionExpr::Cast(Box::new(inner), target.clone())` emission pins with builder `BoundExprNode::Cast` emission. |
| ADR 026 try-cast guards | lines 1260, 1336, 1373, 1389, 1413, 1446 | Replace `ExecutionExpr::TryCast(...)` pins with `BoundExprNode::TryCast`. |
| ADR 026 PBC decline guard | line 1500 | Replace `ExecutionExpr::TryCast(_, _)` decline match with `BoundExprNode::TryCast { .. }` decline match. |
| `adr028_binder_executor_aggregate_args_preserved` | line 1838 | `AggregateCall { args: Vec<ExecutionExpr> }` → `BoundAggregateCall { args: BoundExprProgram }`; preserve computed-arg semantics. |

### 2 verify-unchanged guards

| Guard | Anchor | Reason |
|---|---|---|
| `aggregate_kernel_catalog_guards_are_narrow` | line 665 | Kernel catalog logical/signature side; untouched. |
| `adr028_aggregate_signature_uses_planner_private_visitors` | line 1611 | Preserved by ADR 031 D15. |

**Total: 10 new + 6 adjusted + 2 verify-unchanged =
18 architecture guard touchpoints.** No ADR 024/025/
026/028 guard silently deleted.

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

## Phase 9 — Plan-digest fixture inventory + parity hardening

**Goal.** Categorize plan-digest fixtures affected
by D14 break; add focused parity coverage.

**Files:** existing tests + optional new
`crates/toron/tests/bound_expr_program_test.rs`.

**Tasks:**

1. **Inventory plan-digest fixtures:**

   ```sh
   rg -n "plan_digest|plan_hash|digest.*plan|plan.*digest" crates/toron/tests/
   ```

   Categorize each hit:

   - **(a) Regenerate** — fixture specifically tests
     SELECT plan-digest reproducibility for the
     SELECT corpus (was the ADR 023 D15 / ADR 029
     D15 / ADR 030 D17 guard fixtures). Regenerate
     expected digests against the new
     `BoundExprProgram` serde shape.
   - **(b) Convert to result-digest** — fixture used
     plan-digest as a proxy for behavioral
     equivalence. Convert to compare query result
     rows instead. Preferable when feasible.
   - **(c) Delete** — fixture intent superseded.

   Document the categorization in the impl PR
   commit message or a companion comment. The plan-
   digest break should be explicit in the diff, not
   implicit.

2. **Required focused parity tests:**

   - Builder validates topological ordering + root
     bounds.
   - One-root filter predicate evaluation.
   - Multi-root project evaluation.
   - Arithmetic parity (Int32 / Int64 / Float32 /
     Float64).
   - Explicit cast parity (success + error).
   - Try-cast nulling parity.
   - `TimeBucket` parity.
   - Global `COUNT(*)` zero-root aggregate args.
   - Grouped aggregate computed args (ADR 028
     carry-forward).
   - Exchange partition key parity (single + multi-
     column).
   - Runtime filter single-column key helper.
   - PBC engaged corpus bytecode unchanged.
   - PBC decline reasons unchanged.

3. **Broad parity suites:**

   ```sh
   cargo test -p toron --test group_by_test
   cargo test -p toron --test runtime_filter_pushdown_test
   cargo test -p toron --test distributed_process_test
   cargo test -p toron --test sort_ordering_test
   cargo test -p toron --test hash_join_kinds_test
   cargo test -p toron --test create_table_test  # ADR 029 carry-forward
   cargo test -p toron --test insert_test         # ADR 030 carry-forward
   ```

## Phase 10 — Final verification

```sh
cargo fmt --check
cargo build -p toron --tests
cargo test -p toron --test architecture_test
cargo test -p toron

# Post-PR audits
rg -n "ExecutionExpr|Box<ExecutionExpr>" crates/toron/src
# Expected: zero production hits

rg -n "Eq, Hash|Hash, Eq|derive\(.*Hash" crates/toron/src/ir/execution.rs
# Expected: no Eq/Hash on BoundExprProgram/BoundExprNode/BoundAggregateCall

rg -n "BoundExprProgram" \
  crates/toron/src/ir/execution.rs \
  crates/toron/src/binder.rs \
  crates/toron/src/exec.rs \
  crates/toron/src/pbc.rs
# Expected: used in all four files

rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
# Expected: zero hits
```

If formatting diff: `cargo fmt && cargo fmt --check`.

Expected: all green; ADR 029 + 030 fixtures + guards
still pass; SELECT result corpus byte-identical;
plan-digest fixtures handled per the Phase 9
regenerate / convert / delete inventory.

## Implementation risks

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Field-type migration creates long non-compiling window | Phases 1-4 build parallel surfaces; Phase 5 is the only compile-restoring switch. |
| R2 | PBC bytecode order changes | Compiler emits explicit postorder via explicit stack over `BoundExprId`; Phase 4 parity tests compare bytecode. |
| R3 | Aggregate args lose `COUNT(*)` zero-root shape | Zero-root `BoundExprProgram` fixture + Phase 8 guard. |
| R4 | Runtime filters grow broad expression traversal | Phase 6 uses `single_column_root` / `root_columns` only; no broad walking. |
| R5 | Older ADR 024/025/026/028 architecture guards silently loosened by deletion | Phase 8 architecture-guard table maps every guard to a replacement or verify-only status. |
| R6 | Plan serde byte change is mistaken for regression | D14 explicit; Phase 9 fixture inventory categorizes regenerate / convert / delete. |
| R7 | `ExecutionExpr` remains as shadow IR via accidental import | Phase 7 production `rg` audit + recursive architecture guard scan. |
| R8 | `Eq` / `Hash` slips into program-bearing IR | Phase 8 `adr031_bound_expr_program_partial_eq_only` guard. |
| R9 | Roots leak into per-variant side fields | Phase 8 `adr031_bound_expr_program_shape` guard. |
| R10 | ADR 030 INSERT accidentally migrated to programs | Phase 8 `adr031_execution_plan_insert_stays_expression_free` guard. |
| R11 | ADR 029 CREATE TABLE accidentally migrated | `ExecutionPlan::CreateTable` unchanged at Phase 5; verify via existing ADR 029 guards. |
| R12 | ADR 028 signature substrate accidentally migrated to programs | Phase 8 `adr031_adr028_signature_stays_logical_expr` guard. |
| R13 | Mixed-version distributed task frames cause silent confusion | D14 explicit "not a target"; no compatibility shim added. |
| R14 | Literal dedup / CSE creeps in during substrate migration | D8 forbids; Phase 1 builder appends literals in encounter order with no dedup logic. |
| R15 | Architecture guard count growth | 10 new + 6 adjusted + 2 verify-unchanged = 18 touchpoints; substrate-replacing scope warrants. |

## Definition of Done

- [ ] `BoundExprId`, `BoundBinaryOp`, `BoundUnaryOp`,
      `BoundExprNode` (7 variants),
      `BoundExprProgram { nodes, node_types,
      literals, roots }`, `BoundAggregateCall`
      defined in `ir/execution.rs`.
- [ ] `BoundExprProgramBuilder` enforces
      topological + root-bound invariants by
      construction.
- [ ] Helpers (`root_count`, `root_type`,
      `single_root`, `single_column_root`,
      `root_columns`, `is_empty_roots`, `validate`,
      `pretty`).
- [ ] `Executor::evaluate_bound_program` linear loop
      over `program.nodes.iter().enumerate()`; no
      recursion over `BoundExprNode`.
- [ ] Binder emits `BoundExprProgram` directly via
      `bind_expr_into_program` / `bind_expr_program`
      / `BoundExprProgramBuilder`.
- [ ] `PbcCompiler::compile` reads
      `Option<&BoundExprProgram>` with runtime-
      asserted single-root predicate contract.
- [ ] `ExecutionPlan::Filter.predicate` /
      `Project.exprs` / `Aggregate.group_expr` /
      `Aggregate.aggr_expr` / `Exchange.key` all
      carry `BoundExprProgram` /
      `Vec<BoundAggregateCall>` (Phase 5).
- [ ] `Scan` / `Sort` / `Join` / `CreateTable` /
      `Insert` execution-plan variants unchanged.
- [ ] Fragmenter / scheduler / runtime-filter /
      task / server helpers migrated to
      `BoundExprProgram` via narrow column-key
      helpers (Phase 6).
- [ ] `ExecutionExpr` removed from production
      source; post-PR audit shows zero hits.
- [ ] No `Box<ExecutionExpr>` in production source.
- [ ] No `Eq` / `Hash` derives on
      `BoundExprProgram` / `BoundExprNode` /
      `BoundAggregateCall`.
- [ ] No `blake3` / `bumpalo` / `typed-arena` /
      `slab` / `generational-arena` declared in
      `crates/toron/Cargo.toml`.
- [ ] **10 new ADR 031 architecture guards pass.**
- [ ] **6 adjusted ADR 024/025/026/028 guards pass**
      with `BoundExprNode`-shape replacements at
      confirmed anchors.
- [ ] **2 verify-unchanged guards** (kernel catalog
      + ADR 028 signature) still pass.
- [ ] Plan-digest fixtures categorized + adjusted
      per Phase 9 (regenerate / convert / delete).
- [ ] SQL result digests byte-identical pre/post-
      ADR-031 for the SELECT corpus.
- [ ] PBC engaged/declined corpus behaviorally
      identical.
- [ ] Distributed process tests deterministic
      across worker counts + retry paths.
- [ ] Wire serde round-trips pass within one code
      version (no mixed-version compatibility).
- [ ] ADR 028 signature substrate preserved
      (`GroupedAggregateSignature` stays on logical
      `Vec<Expr>`; six structural visitor helpers
      preserved).
- [ ] ADR 029 catalog metadata-only preserved.
- [ ] ADR 030 `ExecutionPlan::Insert` expression-
      free preserved.
- [ ] Roadmap moves ADR 031 entry from "candidate"
      to "promoted".
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# Development cadence (Phases 1-4 buildable throughout)
cargo check -p toron

# Compile-restoring gate (after Phase 5)
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron --test architecture_test
cargo test -p toron --test create_table_test
cargo test -p toron --test insert_test
cargo test -p toron --test group_by_test
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron --test distributed_process_test
cargo test -p toron --test sort_ordering_test
cargo test -p toron --test hash_join_kinds_test
cargo test -p toron

# Post-PR audits
rg -n "ExecutionExpr|Box<ExecutionExpr>" crates/toron/src
rg -n "Eq, Hash|Hash, Eq|derive\(.*Hash" crates/toron/src/ir/execution.rs
rg -n "BoundExprProgram" \
  crates/toron/src/ir/execution.rs \
  crates/toron/src/binder.rs \
  crates/toron/src/exec.rs \
  crates/toron/src/pbc.rs
rg -n "blake3|bumpalo|typed-arena|generational-arena|^slab" crates/toron/Cargo.toml
```
