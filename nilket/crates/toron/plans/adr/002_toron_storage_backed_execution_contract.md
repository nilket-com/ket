# ADR 002: Toron Storage-Backed Execution Contract (Pushdown Engagement)

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/002_toron_storage_backed_execution_contract.md`

## Status

Accepted. Pre-implementation. Phase 0 calibration pending.

Builds on three topic-iteration rounds plus three ADR-draft
iterations in `plans/wip/`. Council convergence is substantive
across all three voices on every contract decision. Codex's
"Pushdown-Engaged Storage Execution Contract" framing and
Gemini's "First Query with Integrity" V1 definition are the
load-bearing antecedents.

This ADR is Toron's V1 contract for what it means to execute a
query *correctly* against Wendelon-backed storage with smart-
storage pushdown engaged. It does **not** decide how the executor
is internally organized (sync vs async, buffered vs streaming) —
that is ADR 003 territory. It does **not** decide distribution,
shuffle, or worker fault tolerance — also ADR 003.

## V1 contract summary

- **PBC pushdown is load-bearing in the runtime path.** The PBC
  compiler stops being a sidecar artifact and becomes a planner
  pass; `ScanRequest.pbc_predicate` is populated for every
  eligible filter; `WendelonStorageScan` consumes the payload;
  the Toron executor skips filter evaluation when storage has
  authored the filtered result.
- **Schema-bound IR via a binder pass.** Frontend-produced
  `LogicalPlan` carries string column references; a binder pass
  produces `ExecutionPlan` with typed `ResolvedColumn`
  references carrying name, ordinal, data type, nullability,
  and PBC eligibility. Eval-time string lookups are forbidden
  by architecture guard.
- **Minimum-viable catalog.** A `Catalog` trait resolves
  `(schema_name, table_name)` to a `TableObjectId` plus pinned
  `PhysicalDescriptorFingerprint`. Two implementations required
  for promotion: `MockCatalog` for fixtures and `WendelonCatalog`
  for live storage. Hardcoded table-name mappings in Rune and
  SQL frontends are removed.
- **Descriptor-fingerprint pinning.** The coordinator pins a
  fingerprint at query admission; `ScanRequest` carries it;
  Wendelon validates; mismatch returns a typed
  `DescriptorEvolved` error; the coordinator retries once with
  the new descriptor then fails with `SchemaInstability`.
- **Aggregate operator contract.** `CountAll`, `Count(expr)`,
  `Sum(expr)`, and `GROUP BY` are V1 stable contract. Existing
  advanced kernels (`LinearRegress`, `ApproxQuantile`,
  `TimeBucket`) stay in IR but are tagged `experimental` and
  rejected by the planner unless an experimental capability
  flag is set.
- **Streaming `ScanStream` interface.** The storage boundary
  exposes Arrow batches as an async stream with schema,
  end-of-stream signalling, and storage-authored status. V1
  may materialize internally; the interface remains streaming
  so ADR 003 can choose end-to-end streaming without a wire
  break.
- **Declined-pushdown taxonomy.** A crate-private
  `PushdownDeclineReason` enum drives bench-row diagnostics
  and test fixtures; no public surface, no metric label, no
  fallback variant.
- **Storage-authored status mapping.** `StorageError` is a
  structured enum (ADR 034 alignment); `.to_string()`
  boundaries across the codebase replaced with
  `thiserror`-derived chains that preserve origin.
- **All-or-nothing pushdown in V1, IR-reserved for residual.**
  The `ExecutionPlan::Filter` shape carries a
  `disposition: FilterDisposition` field with two V1 variants
  (`FullyPushed` and `EvaluateInToron`). A V1.x ADR adds a
  `PartialResidual` variant without breaking the IR. V1's
  planner never emits `PartialResidual`.
- **Projection dual-path.** The planner selects the union of
  filter-referenced, project-referenced, aggregate-input, and
  group-by referenced columns; pushes the column ordinals via
  `ScanRequest.projected_columns`; Toron's `Project` operator
  produces the final output shape and order.

## Context

ADR 001 promoted Toron's foundation. The seed implementation
landed M0-M11 in one stretch — useful as a skeleton, but did not
satisfy ADR 001's hard gates. Critically:

- `exec.rs:36` hardcodes `pbc_predicate: None` — the PBC
  compiler in `pbc.rs` is a parallel artifact that the executor
  never engages. Toron is currently an Arrow runtime that
  happens to call Wendelon, not the smart-storage compute ring
  ADR 001 committed to.
- `Expr::Column(String)` resolves at evaluation time via
  per-batch `column_by_name` lookup; no typed binding, no PBC
  eligibility tracking, no schema invariant enforcement.
- `ScanRequest` carries `TenantId(0)` and `HlcBound(0)` as
  dummies; Rune and SQL frontends map table names with a
  hardcoded `match`; no catalog plumbing exists.
- Arrow null semantics across operators are not specified; the
  seed's behavior matches Arrow defaults but the contract is
  implicit.
- `LogicalPlan::Aggregate { group_expr: !empty }` errors
  `NotImplemented` — the most-requested OLAP operation is
  missing.
- `wendelon::schema::pbc` is a parallel test target rather than
  the production runtime path.
- Errors are `.to_string()`'d at every module boundary, losing
  structured Wendelon-origin information.
- Advanced kernels (`LinearRegress`, `ApproxQuantile`,
  `TimeBucket`) exist in IR and frontends without a stability
  contract.

This ADR closes those gaps as one coherent contract because they
are inseparable: pushdown requires schema binding which requires
identity which requires catalog which requires storage-authored
errors. Drawing a narrow line through the middle would produce
an immediate follow-up ADR.

### Why pushdown engagement is the keystone

Without runtime-path PBC, Toron's architectural premise (smart-
storage compute ring) is unproven. Every other improvement —
executor model, distributed shuffle, advanced kernels — polishes
a system that doesn't do its primary job. ADR 002 makes that
job load-bearing and creates the Wendelon-native fixture matrix
that all later ADRs benefit from.

### Why streaming the interface but not the executor

Pinning the boundary shape now avoids a wire break later. The
executor's internal organization (buffered vs streaming, pull
vs push) is a perf-driven decision that depends on what
Wendelon delivers after pushdown — different from the contract
Toron exposes upward. ADR 003 will decide the executor; ADR
002 guarantees the interface won't change underneath it.

### Why descriptor-fingerprint pin, not full schema registry

PBC ops reference column ordinals encoded against a specific
descriptor. If the descriptor evolves between planning and
execution, the PBC program is silently wrong (worse than visibly
broken). Wendelon already has ADR 051 fingerprint
infrastructure; Toron consumes it as a pin-and-validate
protocol. Full DDL propagation, async invalidation, and
multi-coordinator schema consensus are V1.x — ADR 002 only
needs detect-and-fail-or-retry.

## Decision

### 1. IR split: `LogicalPlan` → `ExecutionPlan` via binder pass

The current `LogicalPlan` becomes the *frontend-produced*
representation. A new `ExecutionPlan` is the *planner-bound*
representation that the executor and PBC compiler consume.

```rust
// Frontend output (Rune, SQL, hand-built): string-typed.
pub enum LogicalPlan {
    Scan { table: TableRef, /* ... */ },
    Filter { input: Box<LogicalPlan>, predicate: LogicalExpr },
    Project { input: Box<LogicalPlan>, exprs: Vec<LogicalExpr> },
    Aggregate {
        input: Box<LogicalPlan>,
        group_expr: Vec<LogicalExpr>,
        aggr_expr: Vec<(AggregateFunction, Vec<LogicalExpr>)>,
    },
}

pub enum LogicalExpr {
    Column(ColumnName),
    Literal(ScalarValue),
    // ... V0 expression catalog
}

// Planner output: schema-bound, typed.
pub enum ExecutionPlan {
    Scan {
        table: TableObjectId,
        fingerprint: PhysicalDescriptorFingerprint,
        schema: SchemaRef,
        projection: Option<Vec<ColumnOrdinal>>,
        pbc_program: Option<PbcProgram>,
        profile: QueryProfileHint,
    },
    Filter {
        input: Box<ExecutionPlan>,
        predicate: ExecutionExpr,
        disposition: FilterDisposition,
    },
    Project {
        input: Box<ExecutionPlan>,
        exprs: Vec<ExecutionExpr>,
        output_schema: SchemaRef,
    },
    Aggregate {
        input: Box<ExecutionPlan>,
        group_expr: Vec<ExecutionExpr>,
        aggr_expr: Vec<AggregateCall>,
        output_schema: SchemaRef,
    },
}

pub enum ExecutionExpr {
    Column(ResolvedColumn),
    Literal(ScalarValue),
    // ... typed variants of V0 expression catalog
}

pub struct ResolvedColumn {
    pub name: ColumnName,
    pub ordinal: ColumnOrdinal,
    pub data_type: DataType,
    pub nullable: bool,
    pub pbc_eligible: bool,
}

pub enum FilterDisposition {
    /// V1 only. PBC pushed the entire predicate;
    /// Toron skips filter evaluation.
    FullyPushed,
    /// V1 only. Nothing was pushed;
    /// Toron evaluates the entire filter.
    EvaluateInToron,
    // V1.x will add `PartialResidual { residual_predicate }`
    // — reserved by the IR, never emitted by V1's planner.
}
```

The binder pass takes a `LogicalPlan` and a `Catalog` and
produces either an `ExecutionPlan` or a structured binding
error. It is the single place where string column references
resolve.

### 2. Catalog trait (minimum-viable)

```rust
pub trait Catalog: Send + Sync {
    async fn resolve_table(
        &self,
        schema: &SchemaName,
        table: &TableName,
        read_bound: HlcReadBound,
    ) -> Result<ResolvedTable, CatalogError>;
}

pub struct ResolvedTable {
    pub table_object_id: TableObjectId,
    pub fingerprint: PhysicalDescriptorFingerprint,
    pub arrow_schema: SchemaRef,
    pub pbc_eligible_columns: BitSet,  // by ordinal
}
```

**Two implementations are required for ADR 002 promotion:**

- `MockCatalog`: hand-configured for in-memory and parity
  fixtures. Catalog tests must remain runnable without
  Wendelon.
- `WendelonCatalog`: wraps `wendelon-client` for descriptor and
  PBC-eligibility resolution against live Wendelon storage.

The catalog is consumed during binding / admission only. It
never travels into the executor as a late-bound lookup
mechanism, never serializes over the worker wire, and never
becomes a competing schema-truth authority. Workers receive
bound `ExecutionPlan` with descriptors already pinned.

DDL propagation, schema registry governance, schema evolution
policy, and multi-coordinator catalog management are deferred
to V1.x.

### 3. `ScanRequest` contract (final V1 shape)

```rust
pub struct ScanRequest {
    pub session: SessionId,
    pub query: QueryId,
    pub tenant: TenantId,
    pub table: TableObjectId,
    pub fingerprint: PhysicalDescriptorFingerprint,
    pub read_bound: HlcReadBound,
    pub profile: QueryProfileHint,
    pub projected_columns: Option<Vec<ColumnOrdinal>>,
    pub pbc_predicate: Option<PbcProgram>,
    pub trace: Option<TraceContext>,
}
```

All fields are populated for real — no `TenantId(0)` dummies.
`QueryProfileHint` is `Scan` by default; the planner upgrades
to `PointRead` when the IR shape is `Filter(predicate over
primary key) -> Scan(table)`.

### 4. `ScanStream` as a streaming interface

```rust
#[async_trait::async_trait]
pub trait StorageScan: Send + Sync {
    async fn scan(
        &self,
        req: ScanRequest,
    ) -> Result<ScanStream, StorageError>;
}

pub struct ScanStream {
    pub schema: SchemaRef,
    pub session: SessionId,
    pub batches: BoxStream<
        'static,
        Result<RecordBatch, StorageError>,
    >,
}
```

V1's `InMemoryStorageScan` and `WendelonStorageScan` may
materialize internally and yield batches eagerly; the interface
allows true streaming without a contract break when ADR 003
decides.

### 5. PBC pushdown algorithm (all-or-nothing per filter)

The planner walks each `Filter` predicate's `ExecutionExpr`
tree. A predicate is *pushable* iff **every** node satisfies
all of:

1. **deterministic** — no nondeterministic functions, no
   time-varying inputs.
2. **referenced columns are PBC-eligible** —
   `ResolvedColumn::pbc_eligible == true` per the cascade
   catalog known at bind time.
3. **expression shape and data types are in the PBC catalog**
   — the seed's `pbc.rs` opcode set extended only by Phase 0
   audit additions.
4. **null literals are representable** — `NullLiteral`
   operands that cannot be represented in PBC decline pushdown
   rather than forcing a lossy lowering.
5. **compiled against the pinned descriptor** — ordinals
   resolved against the `ResolvedTable.fingerprint` pinned at
   admission, not a stale snapshot.

If all five hold, the planner compiles the entire predicate to
a `PbcProgram`, sets `Filter.disposition = FullyPushed`, and
attaches the program to the `Scan` node's `pbc_program`. The
Toron executor **skips** filter evaluation entirely — Wendelon
has authored the filtered result.

If any criterion fails, the planner records a
`PushdownDeclineReason` internally and sets `Filter.disposition
= EvaluateInToron`. There is no "residual" because nothing was
pushed.

V1 deliberately does **not** support subtree splitting (push
part, keep part). That is V1.x territory and adds a
`PartialResidual { residual_predicate }` variant to
`FilterDisposition` without breaking the ADR 002 IR.

### 6. Projection dual-path

The planner identifies the column set Toron requires from
Wendelon. The set is the **union** of:

- columns referenced by the filter predicate;
- columns referenced by final projection expressions;
- columns referenced by aggregate input expressions;
- columns referenced by group-by expressions.

This union goes into `ScanRequest.projected_columns`. Wendelon
(via sparse projection — ADR 060) returns only those columns;
Toron's `Project` operator consumes the already-reduced
batches and produces the final output shape and order. The
`Scan` node carries the column ordinals to fetch; it does
**not** become the final projection operator.

If projection is identity (`SELECT *` over no filter /
aggregate), `projected_columns` is `None` and Wendelon returns
all columns.

### 7. Aggregate operator contract and Arrow null semantics

V1 stable aggregate contract:

- `CountAll` — counts rows including null-valued rows.
- `Count(expr)` — counts non-null `expr` values (ANSI / Arrow
  `count_kernel` semantics).
- `Sum(expr)` — sums non-null numeric `expr` values; **returns
  `Null` for groups whose `expr` is all-null** (Arrow
  `sum_kernel` semantics, not the SQL `0` convention).
  Behavior is **uniform across `Int{8,16,32,64}`,
  `UInt{8,16,32,64}`, `Float32`, `Float64`** — every numeric
  type in the V1 IR catalog follows Arrow's all-null → null
  contract. Decimal types are deferred (V1 IR has no Decimal).
- `GROUP BY` — hash-based; output ordering is **stable by
  lexicographic group-key ordering** (Codex iter-3 pin). The
  runtime sorts after hash-aggregation; parity tests use the
  canonical record-batch normalization policy (§Resolved Q2)
  for cross-path comparison.

Arrow null rules (pinned to Arrow kernel behavior, not ANSI):

- Boolean operators: three-valued (Kleene).
  `NULL AND false = false`, `NULL AND true = NULL`,
  `NULL OR true = true`, `NULL OR false = NULL`.
- Comparisons: any null operand yields null.
- Filter: predicate result of `NULL` excludes the row (only
  `true` keeps).
- Arithmetic: any null operand yields null (null propagation).
- Aggregates over `expr`: nulls excluded; all-null group
  returns null. `CountAll` is the only V1 aggregate that
  counts rows including null-valued rows.
- Group keys: nulls participate in grouping per Arrow grouping
  semantics (one null-group per distinct null-column tuple).

Advanced kernels (`LinearRegress`, `ApproxQuantile`,
`TimeBucket`) remain in IR and frontends but are gated:

```rust
pub struct PlannerConfig {
    pub allow_experimental_kernels: bool,
}
```

Default `false`. With `false`, a plan referencing experimental
kernels fails at the binder with `BindingError::ExperimentalKernel`.
Each experimental kernel earns its own ADR (V1.x or later) to
graduate to stable contract.

### 8. Descriptor-fingerprint-pin protocol

1. The coordinator resolves a query's tables via `Catalog`.
   Each `ResolvedTable` carries the current
   `PhysicalDescriptorFingerprint`. The coordinator pins these
   fingerprints for the query's lifetime.
2. The planner binds and compiles PBC against the pinned
   descriptor's column ordinals.
3. `ScanRequest.fingerprint` is the pinned value.
4. Wendelon's `table_query` validates fingerprint; mismatch
   returns `StorageError::DescriptorEvolved`.
5. On `DescriptorEvolved`, the coordinator re-resolves the
   table once, re-plans, re-compiles PBC, and retries the
   query. If the retry hits `DescriptorEvolved` again, the
   query fails with `QueryError::SchemaInstability`.

Single retry, not unbounded. Schema instability is a real
failure mode that should surface to the user.

### 9. Declined-pushdown taxonomy (private)

```rust
pub(crate) enum PushdownDeclineReason {
    ExpressionShapeUnsupported,
    DataTypeNotInPbcCatalog,
    ColumnNotInPbcEligibleCascade,
    NullLiteralUnrepresentable,
    NondeterministicExpression,
    ExperimentalKernelInPredicate,
    DescriptorMismatchAtPlanning,
    InternalCompilerError,
}
```

Crate-private; diagnostic-only for bench rows and test
fixtures. Production behavior collapses every decline to the
existing non-pushdown Toron path. No public surface, no
fallback variant, no metric label, no operator-visible toggle.

### 10. Storage-authored status mapping (structured errors)

Replace all `.to_string()` boundaries with a structured error
taxonomy via `thiserror`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("table not found: {table:?}")]
    TableNotFound { table: TableObjectId },
    #[error("descriptor evolved: pinned {expected:?}, current {current:?}")]
    DescriptorEvolved {
        expected: PhysicalDescriptorFingerprint,
        current: PhysicalDescriptorFingerprint,
    },
    #[error("permission denied for tenant {tenant:?}")]
    PermissionDenied { tenant: TenantId },
    #[error("snapshot out of range: bound {bound:?}")]
    SnapshotOutOfRange { bound: HlcReadBound },
    #[error("PBC program invalid: {reason}")]
    PbcProgramInvalid { reason: String },
    #[error("storage transport: {0}")]
    Transport(#[from] TransportError),
    #[error("internal storage error: {0}")]
    Internal(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("compute: {0}")]
    Compute(#[from] arrow::error::ArrowError),
    #[error("binding: {0}")]
    Binding(#[from] BindingError),
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}
```

Wendelon-side errors map through `WendelonStorageScan` into
`StorageError` variants preserving origin. `.to_string()` at
module boundaries is forbidden by architecture guard.

### 11. Architecture guards (`tests/architecture_test.rs`)

Seven guards enforce the contract:

1. **No eval-time string lookup.** No code path may call
   `RecordBatch::column_by_name` in operator evaluation;
   `ResolvedColumn::ordinal` is the only column access path.
2. **PBC engaged on every eligible filter.** A `Filter` whose
   predicate is wholly PBC-eligible, when present above a
   `WendelonStorageScan`, must result in non-`None`
   `pbc_predicate` in the `ScanRequest`.
3. **No `.to_string()` error boundaries.** Module-crossing
   errors must use the typed enum chain.
4. **No `Column(String)` in `ExecutionExpr`.** Compile-time
   structural separation between `LogicalExpr` and
   `ExecutionExpr`.
5. **Experimental kernels gated.** With
   `allow_experimental_kernels = false`, the binder rejects
   plans containing experimental kernels.
6. **Fingerprint required.** `ScanRequest` cannot be
   constructed without a `fingerprint`; no default.
7. **`ScanStream` collection localized.** `ScanStream::collect`
   / `.batches.collect()` / `.try_collect()` patterns appear
   only in `coordinator.rs` and test fixtures; absent from
   `exec.rs` and operator modules.

## Rationale

### Why split `LogicalPlan` from `ExecutionPlan`

The seed conflates parser output and execution input. Every
operator does string→column resolution per batch. Splitting via
a binder pass:

- Localizes schema validation to one pass (catch errors at
  plan time, not run time).
- Eliminates per-batch lookup cost in hot loops.
- Makes PBC eligibility a typed property, not a method call.
- Lets the wire protocol serialize `ExecutionPlan` (fully
  bound, no catalog dependency on the worker side).

This is a classic compiler IR / MIR split, not novel.

### Why minimum-viable catalog, not a registry

Catalog DDL propagation, async invalidation, and multi-
coordinator schema consensus are real V1.x problems but they
need evidence. ADR 002 needs only "resolve a name to a typed
descriptor with a fingerprint." That's a trait and a Wendelon-
backed implementation. Anything more is speculation.

### Why all-or-nothing pushdown in V1

Subtree splitting (push part of a predicate, keep part)
requires a planner-side cost model: when is the partial push
worth the extra residual evaluation? V1 has no cost model.
All-or-nothing is correct, deterministic, and trivially
explainable. V1.x can add splitting under its own ADR with
bench evidence.

### Why fingerprint-pin not skip

PBC against a stale descriptor is silently wrong. Skipping the
check would mean silently-wrong-results when Wendelon evolves
a descriptor mid-query — exactly the failure mode the
ADR-Worthy Threshold question 4 ("would corrupt results rather
than fail") calls out as ADR-grade. Single retry plus failure
is the cheapest correct shape.

### Why experimental kernels gated, not removed

The seed has Rune and SQL frontends already lowering to
`LinearRegress` etc. Removing the IR variants would break the
seed; keeping them ungated would promise stability we don't
have. Gating with a planner config flag preserves the seed's
existence proof while making the V1 contract honest.

### Why structured errors, not strings

`.to_string()` boundaries discard Wendelon-origin information.
For a multi-tenant production system, distinguishing
"permission denied for tenant X" from "internal Wendelon
error" matters for both UX and observability. `thiserror` is
the ecosystem standard; no novel mechanism.

### Why projection dual-path

Wendelon already has sparse projection (ADR 060). Toron not
using it means fetching columns we throw away on the network
edge. The planner does the union-of-references analysis once;
Wendelon does the work. Toron's `Project` operator operates
on already-narrowed batches.

## Consequences

- **The seed's `pbc.rs` graduates from sidecar to production
  path.** Already tested in isolation; ADR 002 wires it into
  `Executor` via the binder-produced `ExecutionPlan`.
- **`Expr::Column(String)` lookup hot path is gone.**
  Materially affects per-batch evaluation cost.
- **The seed's frontend table-name `match` blocks are removed**
  in favor of `Catalog` lookups.
- **`AggregateFunction::*` widens to include `Count(expr)` and
  a grouped form**, and gates experimental kernels behind the
  planner config flag.
- **All `.to_string()` error boundaries are rewritten.**
- **`ScanRequest` becomes a real contract with all fields
  populated**; the seed's dummy `TenantId(0)` is removed at
  the binder-side, not the executor.
- **A new `Catalog` module lands**, plus `Mock` and `Wendelon`
  implementations.
- **One new architecture-test file** enforces the six guards.
- **Mechanical sympathy with Wendelon.** Toron stops being a
  generic Arrow runtime that happens to call Wendelon and
  starts being a compute ring that exploits storage-local
  physics — page stats, sparse projection, encoded validity,
  all-null elision, FSST. The ADR 060 / 063 / 064 / 069 / 070
  investment now pays out through the compute ring.
- **The bench artifact at
  `plans/impl/002_storage_execution_contract_bench_results.md`**
  records hard-gate pass/fail and soft-gate observations for
  the Wendelon-native fixture matrix.

## Promotion criteria

Bench artifact records hard / soft gates. Hard gates block
promotion; soft gates can fail with recorded justification
(Wendelon ADR 070 precedent).

### Hard gates

1. **PBC engagement observed.** For every fixture with a
   PBC-eligible filter, `ScanRequest.pbc_predicate` is
   `Some(_)` on the wire; verified by instrumentation, not
   assumed.
2. **Semantic parity.** PBC-engaged path and pure-Toron
   baseline produce byte-identical Arrow output for every
   fixture (after the canonical record-batch normalization
   policy is applied; batch-boundary differences are
   normalized, not assumed away).
3. **Declined-pushdown deterministic.** Every fixture
   exercising ineligible expressions records a stable
   `DeclineReason` across two independent runs.
4. **Declined-pushdown correctness.** For fixtures whose
   predicates fail eligibility (`FilterDisposition::EvaluateInToron`),
   the path through Wendelon-backed storage + Toron filter
   evaluation produces byte-identical Arrow output to the
   pure-Toron in-memory baseline on the same input. There is
   no "PBC-engaged path" for an ineligible predicate to
   compare against; the baseline is the in-memory Arrow
   reference.
5. **Fingerprint-pin enforcement.** A fixture that mutates a
   descriptor between admission and execution surfaces
   `DescriptorEvolved`, retries once, succeeds; a second
   mutation between retry and execution surfaces
   `QueryError::SchemaInstability` and fails the query.
6. **Architecture guards pass.** All seven guards in
   `tests/architecture_test.rs` enforce the contract: no
   eval-time string lookup, PBC engaged on eligible filters,
   no `.to_string()` boundaries, no `Column(String)` in
   `ExecutionExpr`, experimental kernels gated, fingerprint
   required on `ScanRequest`, `ScanStream` collection
   localized to the coordinator edge.
7. **GROUP BY correctness.** Hand-rolled `GROUP BY a, b` over
   a fixture produces results matching a hand-computed oracle
   for every group, with stable output ordering by group key,
   and `SUM(all-null group)` returns `Null` per Arrow
   semantics.
8. **Structured-error round-trip.** Wendelon-authored error
   surfaces as the expected `StorageError` variant with origin
   preserved (not stringified).
9. **Projection-pushdown minimality.** For every fixture,
   `ScanRequest.projected_columns` cardinality equals exactly
   the cardinality of the union of (filter-referenced ∪
   project-output ∪ aggregate-input ∪ group-by-referenced)
   column ordinals. A `SELECT a FROM t WHERE b > 1` query
   surfaces `{ordinal(a), ordinal(b)}` even though `b` is not
   in the output; a `SELECT a FROM t` query surfaces just
   `{ordinal(a)}`. Wendelon-fetched bytes reflect the
   narrowed projection per ADR 060 sparse projection
   substrate.
10. **`ScanStream` collection localized.** Stream-to-`Vec`
    collection appears only at the coordinator's result-
    materialization edge and in test harnesses. Executable
    operator code consumes the stream shape directly, even
    if it internally buffers. Enforced by architecture guard
    (§11.7).

### Soft gates

Threshold values are Phase 0 calibration targets. Recorded but
not blocking promotion (Wendelon ADR 070 precedent).

10. **Bytes-fetched reduction tracks selectivity.** On a
    90%-selective filter (filter rejects 90% of rows),
    PBC-engaged path fetches ≤ ~10% of the pure-Toron baseline
    bytes. General rule: bytes fetched ≈ (1 - selectivity) ×
    baseline.
11. **Wall-clock speedup.** ≥ 2× on selective-filter fixtures
    where pushdown engages; within 5% of baseline where
    pushdown declines (no regression on the unpushable path).
12. **Range-count behavior.** Recorded per fixture; regressions
    reported with byte-reduction justification (matches
    Wendelon ADR 070 §"Range / GET count" pattern).
13. **Binder-pass overhead.** ≤ 1ms per plan for plans up to
    20 nodes (catalog lookup excluded).

## Resolved open questions

The council surfaced four open questions during iter-2 / iter-3.
Resolutions:

### Q1 — Wendelon descriptor-fingerprint validation surface

*Does Wendelon already expose enough descriptor-fingerprint
validation at the scan API, or does ADR 002 require a storage
API addition?*

**Likely a small Wendelon-side `feat:`, not a Wendelon ADR.**
ADR 051 defines the fingerprint; whether
`wendelon-client::table_query` accepts a pin parameter at its
public surface is a Phase 0 P0.2 audit item. If absent, file a
`feat:` ahead of ADR 002 promotion (a public-surface addition
is not Wendelon-ADR-grade — Wendelon ADRs are for storage
format or core state machine changes). If validation requires
deeper Wendelon internals work, then a Wendelon ADR; this is
unlikely.

### Q2 — `SUM` over all-null group across numeric types

*Which exact Arrow aggregate behavior will be pinned for `SUM`
over all-null groups across integer, float, and decimal
types?*

**Arrow `sum_kernel` semantics, uniform across all numeric
types in the V1 IR catalog.** All-null group returns `Null`,
not `0`. Resolved in §7 above. Decimal is deferred — V1 has no
`Decimal` in `ScalarValue` or the IR type catalog; when
decimal arrives in a future ADR, that ADR pins decimal-
specific aggregate behavior.

### Q3 — Trace context placement

*How much trace context belongs in `ScanRequest` versus an
out-of-band task context?*

**`ScanRequest` carries an `Option<TraceContext>` field** for
OpenTelemetry-style propagation across the Toron-Wendelon
boundary. Storage-authored spans need the parent span id to
thread correctly. An out-of-band task-context channel is a
future ADR if/when the trace surface grows beyond the storage
edge. V1's trace context is `Option<TraceContext>` because
in-memory fixtures don't require it.

### Q4 — Canonical record-batch normalization policy

*Which canonical record-batch normalization policy should be
used in promoted parity tests?* (Codex iter-3 surfaced.)

**Concat-and-reslice.** The parity harness, after collecting
every `RecordBatch` from both PBC-engaged and pure-Toron
paths, applies the following normalization before byte-
identical comparison:

1. Concatenate all output batches from each path into a
   single logical `RecordBatch` per path.
2. Re-slice that single batch at a fixed batch size (V1
   default: 65,536 rows; configurable per fixture for tier
   tests).
3. Hash the resulting batch stream for byte-identical
   comparison.

For grouped aggregate output, the runtime already produces
lexicographic group-key ordering (§7), so post-normalization
both paths produce identical row ordering and identical batch
boundaries.

This normalization is **test-side only**. The runtime contract
makes no promise about batch sizes — Wendelon and Toron may
emit different batch boundaries based on their internal
buffering; the normalization erases that difference for
parity verification.

## Deferred (V1.x or later)

- **Subtree-split pushdown.** Push part of a predicate,
  evaluate residual in Toron. Requires cost model.
- **Full DDL propagation.** Async invalidation broadcasts,
  multi-coordinator schema consensus, descriptor versioning
  beyond fingerprint pin.
- **Advanced kernel graduation.** Each of `LinearRegress`,
  `ApproxQuantile`, `TimeBucket` to its own ADR.
- **Capability-table aggregate dispatch.** Trait-based
  aggregate catalog replacing the enum.
- **Streaming executor end-to-end.** Whether the executor
  internally preserves the `ScanStream`'s streaming property.
  ADR 003 territory.
- **Multi-process coordinator-worker plan serialization.** ADR
  003 territory.
- **Multi-tenant resource governance.** ADR 005+ candidate.
- **Rune sandbox enforcement.** V1 launch concern, separate
  ADR.
- **Wendelon-side crate extraction.** Impl-plan import
  allowlist in V1; crate extraction when V1.x consumer
  pressure exists.
- **Decimal numeric types.** Their own ADR; pins decimal-
  specific aggregate semantics.

## What Not To Do

- **Do not push partial subtrees in V1.** All-or-nothing is
  the contract; splitting requires a cost model we don't have.
- **Do not call `RecordBatch::column_by_name` in operator
  evaluation.** `ResolvedColumn::ordinal` is the only path.
- **Do not stringify errors at module boundaries.** Use the
  typed enum chain.
- **Do not let frontends emit `LinearRegress` etc. without the
  experimental flag.** Production plans must reject by default.
- **Do not skip fingerprint validation.** Silently-wrong-
  results is the failure mode we are paying complexity to
  prevent.
- **Do not promote without a Wendelon-native fixture matrix.**
  TPC-H or hand-rolled in-memory fixtures are not sufficient
  evidence for the pushdown contract.
- **Do not introduce per-operator error types.** One
  crate-level `ExecutionError` enum, structured via
  `thiserror`.
- **Do not expose `PushdownDeclineReason` publicly.** Private
  diagnostic only; no metric labels.
- **Do not extract Wendelon types into a shared crate as part
  of ADR 002.** Use the impl-plan import allowlist;
  extraction is V1.x.
- **Do not retry unboundedly on `DescriptorEvolved`.** Single
  retry; second mismatch fails the query.
- **Do not put projection logic in `Scan`.** `Scan` carries
  the column ordinals it fetches; `Project` is a separate
  operator that may further narrow.
- **Do not let `ScanStream` collapse to materialization at
  every callsite.** Intermediate operators consume the stream
  shape; `.collect()` is allowed only at the coordinator's
  result-materialization edge.
- **Do not scatter parity-test normalization logic.** One
  canonical concat-and-reslice helper shared across every
  parity test.

## Council decisions

Compact archival summary. Iteration history lives in git on
`plans/wip/`.

1. **Pushdown is load-bearing in ADR 002.** All three voices
   converged topics iter-2. Codex's framing won over Claude
   iter-1's narrower "pushdown only" scope.
2. **Schema-bound IR via binder pass.** All three voices.
3. **Streaming `ScanStream` interface.** Codex + Gemini
   topics iter-2; Claude conceded topics iter-3.
4. **Descriptor-fingerprint pin.** Claude topics iter-3; Codex
   ADR iter-1 adopted; Gemini ADR iter-2 adopted.
5. **Hard / soft gate fixture matrix.** Lifted from Wendelon
   ADR 070 pattern.
6. **V1 = "First Query with Integrity."** Gemini topics iter-2.
7. **Advanced kernels experimental.** All three voices.
8. **All-or-nothing pushdown in V1, IR-reserved for
   residual.** Claude topics iter-3 + Codex ADR iter-1 framing.
9. **`SUM(all-null group) = Null`** pinned to Arrow kernel
   semantics, not SQL convention.
10. **Projection dual-path with minimality gate.** All three
    voices.
11. **Wendelon-side ADR 071 not needed.** Gemini topics iter-2.
12. **Evidence-driven ADR 003 scope.** Codex topics iter-2.
13. **Architecture guards themselves are a hard gate** (not
    just enforcement). Codex ADR iter-1.
14. **Mechanical sympathy** as the Consequences framing.
    Gemini ADR iter-1.
15. **Explicit pushdown eligibility criteria** (5 numbered
    conditions). Codex ADR iter-2.
16. **Projection selection rule** as union of filter / project
    / aggregate-input / group-by referenced columns. Codex
    ADR iter-2.
17. **GROUP BY output stability** by lexicographic group-key
    ordering at the runtime; parity tests use the canonical
    normalization. Codex ADR iter-2 (flexibility) + iter-3
    (lex pin).
18. **`SUM` all-null uniform across numeric types** explicitly
    pinned. Resolves Codex Q2.
19. **Wendelon fingerprint validation surface** treated as
    `feat:` not Wendelon ADR. Resolves Codex Q1.
20. **`TraceContext` in `ScanRequest`** as `Option<...>` for
    V1. Resolves Codex Q3.
21. **`FilterDisposition` enum** replaces my earlier `bool`
    flag. Codex iter-3 framing: `FullyPushed`/`EvaluateInToron`
    in V1, `PartialResidual` reserved for V1.x.
22. **10th hard gate + 7th architecture guard for
    `ScanStream` collection localization.** Codex iter-3
    promoted my R14 mitigation to a hard gate.
23. **Canonical record-batch normalization = concat + reslice.**
    Test-side only. Resolves Codex iter-3 Q4.
