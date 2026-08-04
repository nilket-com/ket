# ADR 031: Bound Expression Program — Index-Based Physical Expression IR

## Status

Promoted. Three-way council convergence (Claude / Codex
/ Gemini) reached across three substantive iterations
plus a ratification pass. Iter-0 was seeded by Codex's
iter-0 ADR 029 BoundExprProgram doc (third instance of
the topic-concession-compresses-next-cycle pattern after
ADR 020→021 and ADR 027→028). Iter-1 settled the
load-bearing shape decisions (self-contained multi-root
program, mandatory topological order, `PartialEq`-only
discipline, `ResolvedColumn` + `node_types` sidecar,
separate aggregate role programs, PBC migration in
scope). Iter-2 ratified the final 15-decision shape
and locked the architecture guard set + 10-phase
migration plan. Iter-3 was promotion preparation with
zero new D-level decisions.

**First substrate-replacing pure-Toron ADR.** ADR 028
through 030 were substrate-introducing (added new IR
alongside existing); ADR 031 is the first to *replace*
an existing physical IR (`ExecutionExpr` →
`BoundExprProgram`).

**First cycle to intentionally break a previously-
promoted invariant.** ADR 023 D15, ADR 029 D15, and
ADR 030 D17 all promised "Existing SELECT query corpus
plan digests byte-identical pre/post-ADR-NNN." ADR
031 D14 explicitly breaks this — `ExecutionExpr →
BoundExprProgram` changes serialized `ExecutionPlan`
bytes by design. New cycle property: substrate-
replacing ADRs may break previously-promoted
invariants with explicit D-level ratification.

**First substrate-replacing not-cross-ring ADR.** ADR
029 + 030 needed two-pass Wendelon-API verification
(cross-ring discovery cost). ADR 031 is pure Toron;
no Wendelon-side discovery needed. Cycle property: the
two-pass Wendelon-verification budget applies to
cross-ring ADRs; substrate-replacing pure-Toron ADRs
converge faster.

Thirteenth instance of the ADR 015 cycle (after 016 /
017 / 018 / 019 / 022 / 023 / 024 / 025 / 026 / 027 /
028 / 029 / 030).

## Scale boundary

**Primary:** Toron physical expression IR + binder +
executor + PBC compiler. Surface replaces recursive
`ExecutionExpr` trees with serializable index-based
`BoundExprProgram` in all `ExecutionPlan` expression-
bearing variants; binder emits programs directly via
a builder enforcing topological + root-bound
invariants; executor evaluates programs in a linear
loop over the node vector; PBC migrates to a single-
root program contract.

In scope:

- New types in `crates/toron/src/ir/execution.rs`:
  `BoundExprId(pub u32)`, `BoundBinaryOp`,
  `BoundUnaryOp`, `BoundExprNode`, `BoundExprProgram`,
  `BoundAggregateCall`, `BoundExprProgramBuilder`.
- `Filter.predicate`, `Project.exprs`, `Aggregate.group_expr`,
  `Aggregate.aggr_expr`, and `Exchange.key` switch from
  `ExecutionExpr` / `Vec<ExecutionExpr>` /
  `AggregateCall` to `BoundExprProgram` /
  `Vec<BoundAggregateCall>`.
- Binder emits `BoundExprProgram` directly via
  `bind_expr_into_program` / `bind_expr_program` /
  `BoundExprProgramBuilder`; no production
  `ExecutionExpr → BoundExprProgram` lowering pass.
- Executor `evaluate_bound_program` linear loop over
  `program.nodes.iter().enumerate()`; reuses existing
  cast / try-cast / arithmetic / comparison /
  boolean / null / `TimeBucket` typed kernels.
- `PbcCompiler::compile` reads
  `Option<&BoundExprProgram>` with single-root
  predicate contract (runtime-asserted; no newtype
  wrapper).
- Narrow column-key helpers on `BoundExprProgram`
  (`root_count`, `root_type`, `single_root`,
  `single_column_root`, `root_columns`,
  `is_empty_roots`) plus validation / debug helpers
  (`validate`, `pretty`).
- Distributed task / fragmenter / scheduler / runtime-
  filter / server helpers migrate to read
  `BoundExprProgram` via narrow column-key helpers
  (no broad program traversal added).
- Removal of `ExecutionExpr` and `evaluate_bound_expr`
  from production source at Phase 7.
- 10 new ADR 031 architecture guards + 6 adjusted ADR
  024/025/026/028 guards + 2 verify-unchanged ADR 028
  / kernel-catalog guards.

Out of scope:

- Logical `Expr` arena/index migration (logical layer
  remains boxed-readable; planner contract preserved).
- Logical `LogicalPlan` arena/index migration.
- External allocator crates (`bumpalo` / `typed-arena`
  / `slab` / `generational-arena`).
- Arena lifetime threading (`'arena`) through binder /
  planner / executor / wire types.
- JIT compilation.
- Typed opcode tape / bytecode interpreter (Brainstorm
  004's deeper layer; future ADR).
- Common subexpression elimination (CSE).
- Literal deduplication.
- New SQL syntax or expression operations.
- PBC opcode expansion.
- `ExecutionPlan::Insert` migration (ADR 030 D4 carry-
  forward; INSERT continues to carry
  `Vec<Vec<ScalarValue>>`).
- `ExecutionPlan::CreateTable` migration (ADR 029
  carry-forward).
- `Join` left_keys / right_keys (stay
  `Vec<ColumnOrdinal>`).
- `ExecutionSortKey` (stays ordinal-only per ADR 027
  hidden-sort-projection contract).
- `Scan.projection` (stays
  `Option<Vec<ColumnOrdinal>>`).
- ADR 028 `GroupedAggregateSignature` substrate (stays
  planner-private over logical `Vec<Expr>` with the six
  ADR 028 structural visitor helpers untouched).
- Wendelon-side anything — ADR 031 is pure Toron.
- `Eq` / `Hash` on `BoundExprProgram` / `BoundExprNode`
  / `BoundAggregateCall` (float-bearing IR; preserves
  ADR 028 discipline).
- Mixed-version distributed task-frame compatibility
  (full-fleet upgrades only).

## Context

Across ADR 023 / 024 / 025 / 026 / 028 cycles, the
recursive `Box<ExecutionExpr>` pattern surfaced as a
load-bearing pain point for any future SIMD or
vectorized-kernel work. The expression IR is heavily
allocated, pointer-chasing, and tree-walk-shaped —
exactly what cache-aware execution needs to avoid.

Brainstorm 004 (Memory Arenas and Custom Allocators)
proposed four candidate approaches: external arena
crates (`bumpalo`, `typed-arena`), `cranelift-bforest`-
inspired index-based pools, roll-your-own pools, and
a logical/physical expression representation split.
Brainstorm 004 §Q2 specifically flagged the lifetime-
contagion concern: arena-lifetime threading would
require `'arena` parameters through binder, planner,
and executor; index-based pools avoid that contagion
entirely.

ADR 031 is the **narrow realization** of Brainstorm
004's expression-split recommendation: replace bound
physical `ExecutionExpr` with an index-based
`BoundExprProgram`, while leaving logical `Expr`
unchanged. Logical `Expr` remains the SQL planner
contract — readable, serializable, suitable for
planner-side rewrites. Bound physical execution gains
the cache-friendly substrate without lifetime
threading.

The load-bearing decisions are: **does the program own
its roots** (vs roots as side fields on each plan
variant), **what's the operation enum shape** (compact
grouped `BoundBinaryOp` / `BoundUnaryOp` vs per-op
variants), **does the binder produce programs directly
or lower from `ExecutionExpr`** (direct production
becomes the final shape; no transitional lowering in
production), **does PBC migrate in scope or out**
(in scope — current `PbcCompiler::compile` takes
`&ExecutionExpr`; deferring PBC blocks the migration),
**can we preserve serialized `ExecutionPlan` byte
stability** (no — substrate-replacing change
intentionally breaks it), and **what about ADR 028's
`GroupedAggregateSignature` substrate** (unchanged;
signature dedup keys on logical `Vec<Expr>`, fires
before binding produces bound aggregates).

The council converged on a self-contained multi-root
program owning its roots; compact grouped operation
enums (`BoundBinaryOp` for arithmetic / comparison /
boolean, `BoundUnaryOp` for not / null-checks);
binder-direct program production with no production
lowering pass; PBC migration in scope with a single-
root predicate contract; intentional plan-digest
break promoted to a D-level decision (D14); ADR 028
substrate untouched (D15).

## Decision

### D1 — Scope is bound physical expressions only

ADR 031 replaces `ExecutionExpr` in `ExecutionPlan`
expression-bearing variants and adjacent physical-
expression carriers. Logical `Expr` in `crates/toron/
src/ir.rs` remains unchanged. The SQL planner's
`Expr` lowering contract is preserved verbatim.

The transitional boundary is the binder: SQL planner
emits logical `LogicalPlan` with `Expr` trees; binder
resolves columns + types + PBC eligibility and emits
`ExecutionPlan` with `BoundExprProgram` carriers.
No production `ExecutionExpr → BoundExprProgram`
lowering pass survives migration (D11).

### D2 — Self-contained multi-root `BoundExprProgram`

The program owns its roots:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundExprProgram {
    pub nodes: Vec<BoundExprNode>,
    pub node_types: Vec<DataType>,
    pub literals: Vec<ScalarValue>,
    pub roots: Vec<BoundExprId>,
}
```

`Filter.predicate`, `Project.exprs`, `Aggregate.group_expr`,
`BoundAggregateCall.args`, and `Exchange.key` all carry
complete programs, not `program + root_ids` side-
field pairs. Arity is enforced by context:

- Filter predicate: `roots.len() == 1`.
- Project exprs: one root per output field.
- Aggregate group_expr: one root per group key.
- Exchange key: one root per partition-key
  expression.
- `BoundAggregateCall.args`: `roots.len() ==
  aggregate_arity`; `COUNT(*)` is the zero-root
  program.

A physical expression program without its roots is
an arena fragment + external metadata. Owning roots
gives executor, PBC, runtime-filter helpers, and
debug/pretty printing one shared contract.

### D3 — Stable typed `BoundExprId`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BoundExprId(pub u32);
```

`BoundExprId` may derive `Eq` + `Hash` (it's a `u32`
newtype); expression programs and nodes may not (D7).
No generational index in v1 — programs are append-
only during binding and immutable after construction.

### D4 — Topological invariant is mandatory

Every child ID must be less than the parent ID that
references it. The production builder
(`BoundExprProgramBuilder`) enforces this by
construction (it only returns IDs for already-appended
nodes; every `append_*` method accepts only existing
child IDs).

The executor depends on this invariant for linear
evaluation: iterate `program.nodes.iter().enumerate()`,
allocate `Vec<ArrayRef>` for node outputs, read child
arrays by ID lookup, store output at the same index,
return arrays for `roots` in root order. No recursion
over `BoundExprNode`.

### D5 — Compact grouped operation enums

The physical node set uses grouped binary/unary
operation enums:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundBinaryOp {
    Add, Sub, Mul, Div,
    Eq, Neq, Lt, Lte, Gt, Gte,
    And, Or,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundUnaryOp {
    Not, IsNull, IsNotNull,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundExprNode {
    Column(ResolvedColumn),
    Literal(u32),  // index into BoundExprProgram::literals
    Cast { input: BoundExprId, target: DataType },
    TryCast { input: BoundExprId, target: DataType },
    Binary {
        op: BoundBinaryOp,
        left: BoundExprId,
        right: BoundExprId,
    },
    Unary {
        op: BoundUnaryOp,
        input: BoundExprId,
    },
    TimeBucket {
        interval: std::time::Duration,
        input: BoundExprId,
    },
}
```

`BoundExprNode` has 7 variants (vs the 18+ per-op
variants of the old `ExecutionExpr`). Reasons:

1. **Enum-size discipline.** Smaller match dispatch
   table → lower instruction-cache pressure in the
   executor's tight loop.
2. **Carrier symmetry.** One `Binary { op, left, right }`
   variant carries `(BoundExprId, BoundExprId)` once
   instead of 12 times.
3. **Future SIMD / typed-kernel dispatch alignment.**
   Future typed-kernel dispatch keys on `(op,
   output_type)` rather than `(node_discriminant,
   output_type)`; the grouped shape pre-aligns.

Per-op exhaustive-match in helpers (e.g., is_arithmetic,
is_comparison) matches on `BoundBinaryOp` not just
`BoundExprNode`. Acceptable cost; helpers stay thin.

### D6 — Sidecar `node_types` authoritative for execution

`program.node_types[id]` stores each node's bound
output type. The builder enforces:

```text
node_types[id] == node_output_type(nodes[id])
```

For column nodes, that means
`node_types[id] == nodes[id].as_column().data_type`.

`ResolvedColumn` stays in `BoundExprNode::Column` for
v1 — it carries the ordinal, display name,
nullability, and PBC eligibility that several helper
paths already need. The duplication between
`ResolvedColumn.data_type` and `node_types[id]` is
accepted in v1; `node_types[id]` is authoritative for
execution-time dispatch. Future cleanup ADR may
thin `Column` to just (ordinal, nullable,
pbc_eligible) if the duplication becomes load-bearing.

### D7 — `PartialEq` only for program-bearing types

`BoundExprProgram`, `BoundExprNode`, and
`BoundAggregateCall` derive `PartialEq` only.

No `Eq`. No `Hash`. Reasons:

- `BoundExprProgram.literals: Vec<ScalarValue>`
  includes `ScalarValue::Float32` / `Float64`. NaN
  comparisons violate `Eq` reflexivity.
- Cross-program identity comparisons via `Eq` on
  nodes would silently misbehave because literal
  slots and `node_types` are sidecar contextual state.
- `Hash` would repeat ADR 028's pitfall of public
  `Hash` over float-bearing IR.

Preserves ADR 028 D2's discipline (public
`AggregateFunction` / `Expr` / `ScalarValue` remain
non-`Eq` / non-`Hash`; visitor-based structural
equality fires inside planner-private signature
keys).

### D8 — No literal dedup or CSE in v1

The binder appends literals and nodes deterministically
in encounter order. No deduplication of repeated
literals. No identification of common subexpressions
across roots.

Multi-root programs are still useful without CSE: one
expression carrier per operator, one evaluator
entrypoint, one pretty-printer, one helper surface.
CSE + literal dedup wait on a future expression-
optimizer ADR.

### D9 — Aggregate role boundaries stay separate

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundAggregateCall {
    pub function: AggregateFunction,
    pub args: BoundExprProgram,
}
```

`Aggregate` operator carries:

- `group_expr: BoundExprProgram` — multi-root program
  for group keys.
- `aggr_expr: Vec<BoundAggregateCall>` — one program
  per aggregate call's args.

Do not merge group keys and aggregate arguments into
one aggregate-wide program. They evaluate in
different phases (group key evaluation precedes
aggregate-args evaluation per group) and have
different output mapping rules. `COUNT(*)` arguments
use the natural zero-root program.

### D10 — `ExecutionPlan` field shape

Expression-bearing variants:

```rust
ExecutionPlan::Filter {
    input: Box<ExecutionPlan>,
    predicate: BoundExprProgram,        // exactly one root
    disposition: FilterDisposition,
}

ExecutionPlan::Project {
    input: Box<ExecutionPlan>,
    exprs: BoundExprProgram,            // one root per output field
    output_schema: SchemaRef,
}

ExecutionPlan::Aggregate {
    input: Box<ExecutionPlan>,
    group_expr: BoundExprProgram,       // one root per group key
    aggr_expr: Vec<BoundAggregateCall>,
    output_schema: SchemaRef,
}

ExecutionPlan::Exchange {
    input: Box<ExecutionPlan>,
    partition_count: u32,
    key: BoundExprProgram,              // one root per partition-key expression
}
```

Unchanged variants:

- `Scan.projection: Option<Vec<ColumnOrdinal>>` —
  ordinal projection; no expression migration.
- `Sort.order_by: Vec<ExecutionSortKey>` — ADR 027
  hidden-sort projection makes sort keys ordinal-only.
- `Join.left_keys` / `Join.right_keys:
  Vec<ColumnOrdinal>` — physical join keys stay
  ordinal-only.
- `ExecutionPlan::CreateTable` — ADR 029 carry-
  forward; metadata-only.
- `ExecutionPlan::Insert { rows: Vec<Vec<ScalarValue>> }`
  — ADR 030 D4 carry-forward; INSERT remains
  expression-free.

### D11 — Binder emits programs directly

The final architecture has no production
`ExecutionExpr → BoundExprProgram` lowering pass.

The binder owns column resolution, type coercion,
nullability decisions, PBC eligibility, and physical
program construction. It uses
`BoundExprProgramBuilder` internally with helper
entrypoints:

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

A test-only `#[cfg(test)]`
`execution_expr_to_program` converter is permitted
during phased migration (used for parity testing
when both substrates coexist in the codebase between
Phases 2 and 5). The converter must be deleted before
Phase 8 because Phase 7 removes the `ExecutionExpr`
type itself. The final production source must have
zero `ExecutionExpr` hits. No production lowering
pass survives.

The long-standing invariant that `bind_expr` is
aggregate-free survives in spirit: expression
binding still must not learn aggregate calls.
Aggregate binding owns aggregate-call structure and
uses expression binding only for group keys and
aggregate arguments.

### D12 — PBC migration in scope; single-root contract

`PbcCompiler::compile` reads a single-root
`BoundExprProgram` predicate:

```rust
pub(crate) fn compile(
    &self,
    predicate: Option<&BoundExprProgram>,
    projection: Option<&[ColumnOrdinal]>,
) -> Result<Vec<u8>, PushdownDeclineReason>
```

`None` predicate is allowed (projection-only
compilation). `Some(predicate)` must have exactly
one root; malformed root shape returns
`PushdownDeclineReason::ExpressionShapeUnsupported`.

The single-root contract is **runtime-asserted at the
compile entrypoint**, not statically typed. No
`SingleRootBoundExprProgram` newtype — the program
type stays unified; callers enforce arity via
context.

`compile_expr(&self, expr: &ExecutionExpr,
bytecode: &mut Vec<u8>)` becomes
`compile_node(&self, program: &BoundExprProgram,
id: BoundExprId, bytecode: &mut Vec<u8>)`. Node
compilation uses an explicit stack over `BoundExprId`,
not recursion — mirrors the executor's flat-loop
discipline.

PBC semantics are unchanged:

- **Eligible**: column, non-null literal, comparison,
  boolean, null check.
- **Declined**: `Cast`, `TryCast`, arithmetic
  (`Binary` with arithmetic `op`), `TimeBucket`.
- **Projection bytecode** stays ordinal-based; not
  compiled from a general expression program.

### D13 — Narrow column-key helpers

Runtime-filter, scheduler, and fragmenter paths only
need to prove "this key is a single column" — they
should not learn full program traversal.

`BoundExprProgram` exposes:

```rust
pub fn root_count(&self) -> usize;
pub fn root_type(&self, root_index: usize) -> Option<&DataType>;
pub fn single_root(&self) -> Option<BoundExprId>;
pub fn single_column_root(&self) -> Option<&ResolvedColumn>;
pub fn root_columns(&self) -> Option<Vec<&ResolvedColumn>>;
pub fn is_empty_roots(&self) -> bool;
```

Validation / debug helpers:

```rust
pub fn validate(&self) -> Result<(), BoundExprProgramError>;
pub fn pretty(&self) -> String;
```

`pretty()` addresses readability for test failures
after `Debug`-over-`Vec<BoundExprNode>` becomes
opaque. Do not add a persistent recursive `tree_view`
helper — that would preserve the old recursive
abstraction under a new name.

### D14 — Serialized `ExecutionPlan` bytes intentionally change

ADR 031 is **the first cycle to intentionally break
the byte-identical SELECT plan-digest invariant**
promised by ADR 023 D15, ADR 029 D15, and ADR 030
D17. `ExecutionExpr → BoundExprProgram` changes
serialized `ExecutionPlan` bytes by design; plan
digests computed over `ExecutionPlan` will change.

This is a deliberate substrate-replacing exception,
not a regression. Future substrate-replacing ADRs
should expect that this invariant has boundary
conditions; they can break it but must explicitly
ratify the break at D-level.

The compatibility contract that survives:

- **SQL result digests** byte-identical for the
  existing SELECT query corpus (rows, types, order —
  what users observe).
- **PBC engaged/declined decisions** unchanged for
  the existing corpus.
- **Distributed process tests** deterministic across
  worker counts + retry paths.
- **Wire serde round-trips** pass within one code
  version.

**Mixed-version distributed task-frame compatibility
is not a target.** Toron's full-fleet upgrade model
means clusters upgrade atomically; ADR 031 does not
add a compatibility shim for old `ExecutionExpr` wire
bytes.

### D15 — ADR 028 logical signature substrate untouched

Planner-private grouped aggregate signature keying
remains over logical `Expr`:

```text
GroupedAggregateSignature = (StableAggregateKernel, Vec<Expr>)
```

The six ADR 028 structural visitor helpers
(`hash_expr_structural`, `expr_structural_eq`,
`hash_scalar_structural`, `scalar_structural_eq`,
`hash_data_type_structural`, `data_type_structural_eq`)
remain logical-planner code. They do not move to
`BoundExprProgram`.

The handoff is:

```text
SQL planner logical Expr → binder → BoundExprProgram
```

Signature dedup fires *during* SQL planning, before
binding produces bound aggregates. The signature
keying never sees `BoundExprProgram`. ADR 031 must
not add `Eq` or `Hash` to logical `Expr` or
`ScalarValue`.

## Reject removal (ADR 015 cycle's thirteenth instance)

ADR 031 is substrate-replacing rather than reject-
narrowing in the traditional ADR 015 cycle shape. No
new SQL reject is added; no existing SQL reject is
narrowed. The cycle continuation is the broader
discipline: source-level boundary clarity at named-
reject granularity, no silent acceptance, no silent
drop.

New errors / pushdown declines:

- `PushdownDeclineReason::ExpressionShapeUnsupported`
  is reused for malformed PBC predicate root shape
  (existing variant; no new error type).
- `BoundExprProgramError` (a new error type for
  optional `validate()` helper); only used in tests
  + builder defensive checks.

No new SQL surface added.

## Architecture guard adjustments

### 10 new ADR 031 guards

| Guard | Purpose |
|---|---|
| `adr031_bound_expr_program_shape` | Source-reads `BoundExprProgram { nodes, node_types, literals, roots }` + `BoundExprId(pub u32)` shape. |
| `adr031_bound_expr_node_has_no_boxes` | `source_lines(&manifest_dir().join("src"))` recursive scan for absence of `Box<ExecutionExpr>` + `ExecutionExpr::` in production source. Asserts `BoundExprNode` has exactly 7 variants (`Column`, `Literal`, `Cast`, `TryCast`, `Binary`, `Unary`, `TimeBucket`). |
| `adr031_bound_expr_program_partial_eq_only` | Asserts `BoundExprProgram` / `BoundExprNode` / `BoundAggregateCall` derives include `PartialEq` but not `Eq` or `Hash`. |
| `adr031_execution_plan_uses_bound_expr_program` | Asserts `Filter` / `Project` / `Aggregate` / `Exchange` fields use `BoundExprProgram`; asserts `BoundAggregateCall { args: BoundExprProgram }`. |
| `adr031_execution_plan_insert_stays_expression_free` | Asserts `ExecutionPlan::Insert` still has `rows: Vec<Vec<ScalarValue>>`; no `BoundExprProgram` in the Insert variant (ADR 030 D4 carry-forward). |
| `adr031_join_and_sort_remain_ordinal_keyed` | Asserts `Join` keys + `ExecutionSortKey` stay `Vec<ColumnOrdinal>` / ordinal-only (ADR 027 + Join carry-forwards). |
| `adr031_pbc_reads_bound_expr_program` | Source-reads `PbcCompiler::compile` signature for `Option<&BoundExprProgram>`; asserts absence of `Option<&ExecutionExpr>`; asserts `single_root` usage in body. |
| `adr031_executor_evaluates_program_linearly` | Source-reads `Executor::evaluate_bound_program` for linear `program.nodes.iter().enumerate()` iteration; asserts absence of evaluator recursion over `BoundExprNode`. |
| `adr031_binder_emits_bound_expr_program` | Asserts binder uses `BoundExprProgramBuilder`; asserts no production `ExecutionExpr::Cast` / `TryCast` emission. |
| `adr031_adr028_signature_stays_logical_expr` | Asserts `GroupedAggregateSignature` still stores `Vec<Expr>`; ADR 028 structural visitor helpers preserved (D15 enforcement). |

### 6 adjusted older guards

| Guard | Anchor | Adjustment |
|---|---|---|
| `architecture_guards_unchanged_plus_sort_variant` | `architecture_test.rs:558` | `ExecutionPlan` variant count stays 9 (no variant added/removed); content asserts swap `ExecutionExpr` → `BoundExprProgram` per-variant. |
| `adr024_scalar_arithmetic_guards_are_narrow` | `architecture_test.rs:914` | Replace `Box<ExecutionExpr>` arithmetic-variant pins (lines 931-936) with `BoundBinaryOp::{Add, Sub, Mul, Div}` + `BoundExprNode::Binary` shape. Preserve "narrow" semantic. |
| ADR 025 cast guards | `architecture_test.rs:994, 1054, 1109, 1135, 1207, 1246, 1286` | Replace `ExecutionExpr::Cast(Box::new(inner), target.clone())` emission pins with builder `append_cast(...)` returning `BoundExprId`. Preserve cast-lattice + hidden-sort-projection + target-type-normalization semantics. |
| ADR 026 try-cast guards | `architecture_test.rs:1260, 1336, 1373, 1389, 1413, 1446` | Replace `ExecutionExpr::TryCast(Box::new(...))` pins with builder `append_try_cast(...)`. |
| ADR 026 PBC decline guard | `architecture_test.rs:1500` | Replace `ExecutionExpr::TryCast(_, _)` decline match with `BoundExprNode::TryCast { .. }` decline match. |
| `adr028_binder_executor_aggregate_args_preserved` | `architecture_test.rs:1838` | `AggregateCall { args: Vec<ExecutionExpr> }` → `BoundAggregateCall { args: BoundExprProgram }`; preserve computed-arg semantics. |

### 2 verify-unchanged guards

| Guard | Anchor | Reason |
|---|---|---|
| `aggregate_kernel_catalog_guards_are_narrow` | `architecture_test.rs:665` | Kernel catalog is logical-side / signature substrate; untouched by ADR 031. |
| `adr028_aggregate_signature_uses_planner_private_visitors` | `architecture_test.rs:1611` | Preserved by D15 (signature stays on logical `Vec<Expr>`). |

Total: **10 new + 6 adjusted + 2 verify-unchanged = 18
architecture guard touchpoints.** Each ADR 024/025/
026/028 guard gets a deliberate `BoundExprNode`-shape
replacement; **none silently deleted**.

## SoA-readiness check

ADR 021's four invariants preserved:

1. **Catalog stays pure metadata for kernels.**
   `Catalog` unchanged by ADR 031.
2. **`#[repr(u8)]` on `StableAggregateKernel`
   unchanged.** Aggregate kernel catalog is logical-
   side; not touched.
3. **Centralized dispatch unchanged.** Executor
   still dispatches per-row through the kernel
   catalog; `BoundExprProgram` is the expression
   substrate, not the kernel substrate.
4. **No row-path dyn dispatch.** The flat-loop
   evaluator over `BoundExprProgram` uses static
   `match` on `BoundExprNode`; no `dyn`.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Touched but
  preserved: the seam is now cleaner. A future JIT
  ADR can lower `BoundExprProgram` to native code
  without changing the SQL planner's logical `Expr`
  contract. Index-based programs are easier JIT
  input than recursive trees.
- **Brainstorm 002 (WeakControlRing).** Untouched —
  ADR 031 is pure Toron; no consensus / control-ring
  touch.
- **Brainstorm 003 (Disaggregated Compute).** Touched
  but preserved: wire serde for `ExecutionPlan`
  changes (D14), but `NodeRoleSet` routing semantics
  are unchanged. Distributed task carriers migrate
  to `BoundExprProgram` per Phase 6.
- **Brainstorm 004 (Memory Arenas).** Narrowly
  realized. ADR 031 implements one of Brainstorm
  004's four candidate approaches (index-based pool
  for bound physical expressions). Other three
  (logical-Expr arena migration, opcode tape, JIT
  from tape) remain Brainstorm 004 candidates for
  future cycles when evidence accumulates.

No seam closes.

## What Not To Do

- **Don't replace logical `Expr`** (D1 — logical
  layer is SQL planner contract; out of scope).
- **Don't add arena lifetimes** (no `'arena` parameter
  threading; Brainstorm 004 §Q2 reasoning).
- **Don't add external allocator crates** (`bumpalo`
  / `typed-arena` / `slab` / `generational-arena` —
  no Cargo.toml change of this kind).
- **Don't implement typed opcode tape, SIMD, JIT, or
  CSE in this cycle** (Brainstorm 004's deeper layers
  defer to future ADRs).
- **Don't deduplicate literals in v1** (D8).
- **Don't derive `Eq` or `Hash`** on `BoundExprProgram`
  / `BoundExprNode` / `BoundAggregateCall` (D7 —
  preserves ADR 028 float-safety discipline).
- **Don't store program roots as side fields** on
  each plan variant (D2 — self-contained multi-root).
- **Don't migrate `ExecutionSortKey`** (ADR 027 D3
  / D10 carry-forward; stays ordinal-only).
- **Don't migrate `Join` keys** (stay
  `Vec<ColumnOrdinal>`).
- **Don't migrate `Scan.projection`** (stays
  `Option<Vec<ColumnOrdinal>>`).
- **Don't migrate `ExecutionPlan::Insert`** (ADR 030
  D4 carry-forward; expression-free).
- **Don't preserve old `ExecutionPlan` serde bytes**
  (D14 — intentionally broken).
- **Don't leave PBC on the old expression tree**
  (D12 — PBC migration required in same ADR).
- **Don't migrate ADR 028 grouped-aggregate
  signatures off logical `Expr`** (D15 — visitor
  helpers stay logical).
- **Don't add a persistent `tree_view` helper** —
  preserves the old recursive abstraction under a
  new name (D13).
- **Don't keep a production `ExecutionExpr → BoundExprProgram`
  lowering pass** (D11 — final shape is direct
  binder emission; transitional converter is test-
  only).
- **Don't silently delete ADR 024/025/026/028
  architecture guards** — each gets a deliberate
  replacement per the architecture-guard table.
- **Don't add `blake3`** to `crates/toron/Cargo.toml`
  (ADR 030 D7 carry-forward; not needed for ADR 031
  either).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Roots leak into per-variant side fields | D2 + `adr031_bound_expr_program_shape` guard pins `roots` inside `BoundExprProgram`. |
| R2 | Old plan-digest invariant is silently accepted as still active | D14 explicitly breaks it + promotes to D-level; Phase 9 fixture inventory enumerates affected tests. |
| R3 | `ExecutionExpr` survives as a second physical IR | D11 + Phase 7 production cleanup + `adr031_bound_expr_node_has_no_boxes` recursive source scan. |
| R4 | PBC is deferred and binder cannot compile cleanly | D12 makes PBC migration required + Phase 4 lands `compile_program` in parallel. |
| R5 | ADR 028 logical signatures accidentally migrate to `BoundExprProgram` | D15 + `adr031_adr028_signature_stays_logical_expr` guard pins logical `Vec<Expr>` keying. |
| R6 | `Eq` / `Hash` leaks into float-bearing expression program identity | D7 + `adr031_bound_expr_program_partial_eq_only` guard rejects the derives. |
| R7 | Runtime-filter / scheduler paths grow broad walkers | D13 helper surface keeps them column-oriented; impl Phase 6 uses only `single_column_root` / `root_columns`. |
| R8 | Aggregate group and arg programs merge too early | D9 preserves role boundaries; `Aggregate` carries `group_expr: BoundExprProgram` + `aggr_expr: Vec<BoundAggregateCall>`. |
| R9 | Literal dedup or CSE changes construction order during substrate migration | D8 forbids v1 dedup/CSE; future ADR engages. |
| R10 | ADR 030 INSERT is dragged into expression-program migration | D10 + `adr031_execution_plan_insert_stays_expression_free` guard. |
| R11 | Field-type migration creates long non-compiling window | Phases 1-4 build parallel surfaces; Phase 5 is the only compile-restoring switch. |
| R12 | PBC bytecode order changes | Compiler emits explicit postorder via explicit stack over `BoundExprId`; parity tests compare bytecode. |
| R13 | `COUNT(*)` zero-arg aggregate args shape lost | Zero-root `BoundExprProgram` fixture + guard. |
| R14 | Architecture guards silently loosen | Architecture-guard table maps every ADR 024/025/026/028 guard to a replacement or verify-only status. |
| R15 | Plan serde byte change is mistaken for regression | D14 promotes the break; impl Phase 9 categorizes plan-digest fixtures (regenerate / convert-to-result-digest / delete). |
| R16 | `ExecutionExpr` shadow IR remains via accidental import | Phase 7 production `rg` audit + recursive architecture guard scan. |
| R17 | Mixed-version distributed task frames create confusion | D14 explicit "not a target"; no compatibility shim. |
| R18 | `ResolvedColumn` + `node_types` duplication grows | D6 documents the v1 trade-off; future cleanup ADR if duplication becomes load-bearing. |
| R19 | Architecture guard count growth | 10 new + 6 adjusted + 2 verify-unchanged = 18 touchpoints; substrate-replacing scope warrants. |

## Recorded dissent

**None at iter-3.**

Iter-1 Gemini positions retracted at iter-2/3 (sixth
consecutive cycle):

- Roots as side fields on plan variants → retracted
  in favor of self-contained multi-root (D2).
- PBC migration out of scope → retracted in favor of
  in-scope migration (D12).
- `Eq` / `Hash` on `BoundExprNode` → retracted in
  favor of `PartialEq` only (D7).

This extends the Gemini iter-1/2 retraction streak
to **6-of-6** across ADR 025 / 026 / 028 / 029 / 030
/ 031. Stable cycle property: Gemini's wider-scope
iter-1/2 framings surface the alternate-design space
the council needs to reject explicitly; useful for
collective understanding but not load-bearing for
promotion.

## Process observations

### Fastest convergence in the council's history

ADR 031 took 3 substantive iterations + 1 ratification
pass = **4 total with 0 flip-flops**. Compare:

| ADR | Substrate? | Iters | Flip-flops |
|---|---|---|---|
| ADR 023 | Yes | 4 + consolidation | 1 |
| ADR 024 | Yes | 4+ | Multi |
| ADR 028 | Yes | 5 | 2 |
| ADR 029 | Yes (statement layer) | 4 | 0 |
| ADR 030 | Yes (statement layer, DML) | 4 | 1 |
| **ADR 031** | **Yes (substrate-replacing)** | **3 + 1 ratification** | **0** |

Two factors drove the speed:

1. **Topic-concession-compresses-next-cycle pattern
   at 3-of-3.** Codex's iter-0 ADR 029
   `BoundExprProgram` seed doc drove this cycle's
   iter-0 directly. Topic never reopened.
2. **First substrate-replacing not-cross-ring ADR.**
   ADR 029 + 030 needed two-pass Wendelon-API
   discovery (cross-ring cost). ADR 031 is pure
   Toron — no Wendelon-side work.

### Four Claude iter-0/1 contributions promoted to D-level

| Claude contribution | Promoted to |
|---|---|
| iter-0 C1 (plan-digest break as cycle property) | D14 |
| iter-0 C2 (ADR 028 signature substrate untouched) | D15 |
| iter-1 C5 (PBC single-root contract + signature) | D12 |
| iter-1 C6 (compact `BoundBinaryOp` / `BoundUnaryOp` grouping) | D5 |

Claude iter-0 C3 did not become a D-level decision;
it landed as the architecture-guard replacement plan
and line-anchor discipline. First cycle where
code-evidence-grounded + narrowly-scoped iter-0/1
Claude contributions reach promotion intact without
iter-2/3 retraction.

### Gemini iter-1/2 retraction streak at 6-of-6

Sixth consecutive cycle (ADR 025 / 026 / 028 / 029 /
030 / 031) where Gemini's iter-1/2 broader framings
retracted at iter-2/3. Stable cycle property.

### First cycle to break a previously-promoted invariant

ADR 023 D15 + ADR 029 D15 + ADR 030 D17 promised
"SELECT plan digest byte-identical." ADR 031 D14
explicitly breaks it. New cycle property:
substrate-replacing ADRs may break previously-
promoted invariants with explicit D-level
ratification.

### First substrate-replacing pure-Toron ADR

ADR 028 (computed aggregate args) was substrate-
introducing pure-Toron. ADR 029 + 030 were
substrate-introducing cross-ring (statement layer +
Wendelon integration). ADR 031 is the first
**substrate-replacing** + **pure-Toron** combination.
New cycle property: pure-Toron substrate-replacing
ADRs converge faster than cross-ring substrate-
introducing ADRs because no Wendelon-side discovery
pass is needed.

## Future work

ADR 032+ candidates:

- **Typed expression tape / opcode interpreter**
  (Brainstorm 004's deeper realization). Lower
  `BoundExprProgram` to a flat typed-instruction tape
  with kernel dispatch on `(op, output_type)`.
  Substrate for future SIMD/JIT.
- **Common subexpression elimination (CSE) + literal
  deduplication.** Both deferred from ADR 031 D8;
  future expression-optimizer ADR can fold them.
- **Cranelift JIT compilation** (Brainstorm 001 deeper
  realization). Lower `BoundExprProgram` (or future
  tape) to native code for hot kernels.
- **Logical `Expr` arena migration.** Brainstorm 004's
  broader framing; revisit when evidence accumulates
  that the planner-side `Box<Expr>` pattern is load-
  bearing performance pain.
- **`INSERT INTO ... SELECT`** (composes ADR 029 +
  030 + read-side ADRs).
- **`UPDATE` / `DELETE` / `ON CONFLICT` / `RETURNING`**
  — DML siblings to ADR 030.
- **`DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW` / CTAS** — DDL siblings to ADR 029.
- **Constraints** (PRIMARY KEY / FOREIGN KEY / CHECK
  / column-level UNIQUE) / DEFAULT / GENERATED.
- **Temporal types** (DATE, TIMESTAMP) + **DECIMAL**
  — ADR 029 type-lattice extension.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **HAVING alias visibility ADR.** Loosens ADR 023's
  `"HAVING does not resolve SELECT aliases"` reject.
- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023's `"ORDER BY <aggregate-call> requires
  the aggregate to appear in the SELECT list"`
  reject.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **PBC partial-pushdown of computed-arg aggregates**
  (cross-ring; workload-evidence-pressured).
- **Cross-ring metadata-write contract ADR** — needed
  to lift `WendelonCatalog::create_table` from
  `AdminUnavailable`.
- **Cross-ring distributed-write-context ADR** —
  needed to lift ADR 030 D13's distributed INSERT
  reject.
- **Read-your-writes session HLC advancement ADR.**
- **Richer Toron `Hlc` type.**
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **Per-statement `durability` override.**
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).
- **Wendelon AGG consumer wiring** (cross-ring; PBC
  partial-pushdown).

## Definition of Done

- [ ] `BoundExprId(pub u32)` typed index in
      `ir/execution.rs`.
- [ ] `BoundBinaryOp` (12 variants) +
      `BoundUnaryOp` (3 variants) +
      `BoundExprNode` (7 variants) defined.
- [ ] `BoundExprProgram { nodes, node_types,
      literals, roots }` defined; derives `PartialEq`
      only.
- [ ] `BoundAggregateCall { function, args:
      BoundExprProgram }` defined.
- [ ] `BoundExprProgramBuilder` enforces topological
      + root-bound invariants by construction.
- [ ] Helpers: `root_count`, `root_type`,
      `single_root`, `single_column_root`,
      `root_columns`, `is_empty_roots`, `validate`,
      `pretty`.
- [ ] `Executor::evaluate_bound_program` linear loop
      over `program.nodes.iter().enumerate()`; no
      recursion over `BoundExprNode`.
- [ ] Binder emits `BoundExprProgram` directly via
      `bind_expr_into_program` /
      `bind_expr_program` / `BoundExprProgramBuilder`;
      no production `ExecutionExpr → BoundExprProgram`
      lowering pass.
- [ ] `PbcCompiler::compile` signature reads
      `Option<&BoundExprProgram>` with runtime-asserted
      single-root predicate contract.
- [ ] `ExecutionPlan::Filter.predicate`,
      `Project.exprs`, `Aggregate.group_expr`,
      `Aggregate.aggr_expr`, `Exchange.key` all
      carry `BoundExprProgram` /
      `Vec<BoundAggregateCall>`.
- [ ] `ExecutionPlan::Scan`, `Sort`, `Join`,
      `CreateTable`, `Insert` carriers unchanged.
- [ ] Distributed task / fragmenter / scheduler /
      runtime-filter / server helpers migrate to
      `BoundExprProgram` via narrow column-key
      helpers; no broad program traversal added.
- [ ] `ExecutionExpr` removed from production
      source (`crates/toron/src/`); post-PR audit
      `rg -n "ExecutionExpr"` shows zero production
      hits.
- [ ] No `Box<ExecutionExpr>` in production source.
- [ ] No `Eq` / `Hash` derives on
      `BoundExprProgram` / `BoundExprNode` /
      `BoundAggregateCall`.
- [ ] No `blake3` dependency declared in
      `crates/toron/Cargo.toml` (ADR 030 D7 carry-
      forward).
- [ ] No external allocator crate dependency
      (`bumpalo` / `typed-arena` / `slab` /
      `generational-arena`).
- [ ] **10 new ADR 031 architecture guards pass.**
- [ ] **6 adjusted ADR 024/025/026/028 guards
      pass** with `BoundExprNode`-shape replacements
      at confirmed anchors.
- [ ] **2 unchanged guards** (kernel catalog at
      `architecture_test.rs:665` + ADR 028
      signature at line 1611) still pass.
- [ ] Plan-digest fixtures categorized and
      adjusted (regenerate / convert-to-result-
      digest / delete per impl Phase 9).
- [ ] **SQL result digests** byte-identical
      pre/post-ADR-031 for the existing SELECT
      query corpus.
- [ ] **PBC engaged/declined corpus** behaviorally
      identical.
- [ ] **Distributed process tests** deterministic
      across worker counts + retry paths.
- [ ] Wire serde round-trips pass within one code
      version.
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
