# Implementation Plan: ADR 002 — Toron Storage-Backed Execution Contract

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:**
`plans/adr/0002_toron_storage_backed_execution_contract.md`
**Bench-results artifact (post-promotion):**
`plans/impl/0002_storage_execution_contract_bench_results.md`

## Inherits substrate

- `plans/adr/0001_toron_foundation_and_engine_contract.md` —
  greenfield engine, `StorageScan` trait, one crate, V0 IR
  vocabulary. ADR 002 widens.
- `crates/wendelon/plans/adr/034_*.md` — compute / storage
  contract surfaces (query-session identity, trace context,
  storage-authored status mapping). ADR 002 fulfills.
- `crates/wendelon/plans/adr/051_*.md` — physical descriptor
  fingerprint split. ADR 002 consumes via fingerprint-pin.
- `crates/wendelon/plans/adr/060_*.md` — sparse projection
  fetch narrowing. Wendelon-side; ADR 002 fixtures exercise.
- `crates/wendelon/plans/adr/062_*.md` — `PointRead` / `Scan`
  row-group sizing profile. ADR 002 selects in planner.
- `crates/wendelon/plans/adr/063_*.md` — page-level predicate
  stats. Indirect via PBC pushdown.
- `crates/wendelon/plans/adr/064_*.md` — nullable sparse
  projection. Exercised in fixtures.
- `crates/wendelon/plans/adr/069_*.md` — encoded validity
  sparse projection. Exercised in fixtures.
- `crates/wendelon/plans/adr/070_*.md` — all-null page-skip.
  Exercised in fixtures.
- `crates/wendelon/plans/conventions/` — newtype-units,
  no-version-suffixes, no-planning-phase-names,
  no-premature-backward-compat, no-project-name-in-protocols,
  capability-vs-feature-naming.

## Status

Implemented. Promotion evidence complete in
`plans/impl/0002_storage_execution_contract_bench_results.md`.

## Implementation contract

- The seed's `pbc.rs` is wired into the runtime path. PBC
  engagement is observable via instrumented `ScanRequest`.
- `LogicalPlan` and `ExecutionPlan` are structurally distinct;
  every `Expr::Column(String)` is replaced by
  `ResolvedColumn { ordinal, data_type, nullable, pbc_eligible }`
  at planner output.
- Eval-time `RecordBatch::column_by_name` is removed from
  operator code (architecture-guarded).
- `Catalog` trait and `MockCatalog` / `WendelonCatalog`
  implementations land. Both are promotion blockers.
- `ScanRequest` carries `SessionId`, `QueryId`, `TenantId`,
  `TableObjectId`, `PhysicalDescriptorFingerprint`,
  `HlcReadBound`, `QueryProfileHint`,
  `Option<Vec<ColumnOrdinal>>`, `Option<PbcProgram>`,
  `Option<TraceContext>` — no dummies.
- `StorageScan` returns
  `ScanStream { schema, session, batches: BoxStream<...> }`.
- `Aggregate { group_expr: !empty }` works; hash-based with
  stable output ordering by group key.
- Experimental kernels (`LinearRegress`, `ApproxQuantile`,
  `TimeBucket`) gated by
  `PlannerConfig::allow_experimental_kernels`, default `false`.
- `thiserror`-derived `StorageError` and `ExecutionError`
  replace every `.to_string()` boundary in the seed.
- Wendelon-native fixture matrix exists at
  `plans/impl/0002_storage_execution_contract_bench_results.md`.
- All 10 hard gates pass; soft gates met or explicitly
  justified.

## Goal

Make Toron's smart-storage value proposition load-bearing in
the runtime path. After ADR 002, every Toron query against a
Wendelon-backed table that has an eligible filter and
projection *provably* engages PBC pushdown, fetches narrowed
bytes, and preserves Arrow null semantics from the storage
edge through final materialization.

## Phase plan

### Phase 0 — Audit, calibration, council pins

Pre-implementation pass. No code lands. Deliverable: a
"Phase 0 audit" section in the bench-results artifact pinning
each item.

- **P0.1: Wendelon public surface audit.** Confirm the import
  allowlist compiles cleanly against current `crates/wendelon`
  head.

  **Production allowlist:**
  - `wendelon::schema::pbc::{OP_*, execute, ...}` for PBC
    ops and reference executor.
  - `wendelon::schema::{TenantId, TableObjectId,
    HybridTimestamp, PhysicalDescriptorFingerprint,
    TableSchemaDescriptor, KllSketch, KllK}`.
  - `wendelon_client::{Client, TableQueryOptions,
    TableSchemaDescriptor}`.
  - `wendelon::network::pool::{BufPool, PooledBuf}`
    (carryover from seed M9).
  - `wendelon::storage::file_ops::{AsyncFileOps,
    BlockingFileOps}` (carryover from seed M10).

  **Test-only allowlist** (gated by `#[cfg(test)]`):
  - Wendelon fixture builders and local test harnesses
    (e.g., `wendelon::test_support::*` if exposed).

  **Forbidden** (production and test):
  - `wendelon::network::raft::*`
  - `wendelon::network::quic::*` (use the client API, not
    raw QUIC)
  - `wendelon::storage::lsm::*`
  - any ad-hoc page reader bypassing the scan API
  - anything inside Wendelon's node / raft / transport
    layers.

- **P0.2: Fingerprint availability.** Confirm
  `wendelon_client::table_query` accepts a fingerprint pin
  parameter. If not, file a Wendelon-side `feat:` to add it
  (not a Wendelon ADR — public-surface addition only).

- **P0.3: PBC opcode catalog.** Confirm
  `wendelon::schema::pbc::*` exposes every opcode the seed's
  `pbc.rs` imports for the V1 filter-pushdown expression
  catalog (comparisons, booleans, null tests, literals).
  If a filter-expression opcode gap appears, file a Wendelon-
  side `feat:` ahead of ADR 002 promotion. **Aggregate
  pushdown is explicitly out of scope** — ADR 002's PBC
  contract is filter pushdown + projection narrowing only;
  grouped aggregate executes in Toron. Do not file
  aggregate-pushdown opcodes during this audit.

- **P0.4: Fixture corpus identification.** Wendelon's existing
  fixtures from ADR 060 / 064 / 069 / 070 are the starting
  point. Specifically:
  - Sparse projection fixtures (multi-column, varying
    selectivity).
  - Encoded validity fixtures.
  - Page stats fixtures (filter narrowing).
  - All-null page elision fixtures.
  - Mixed-nullity row groups.
  - Hand-rolled COUNT(*), SUM, GROUP BY fixtures.
  - Multi-tenant fixture (two tenants, same table, isolated).
  - Descriptor mutation fixture (admit, change, retry).
  - Experimental-kernel decline fixture (TimeBucket in IR;
    binder rejects with default config).

- **P0.5: Architecture-guard literal text.** Draft exact Rust
  predicates for the six guards in ADR §11. Identify the
  test-side mechanism: source-text grep, structural AST
  inspection, or runtime instrumentation.

- **P0.6: Soft-gate threshold calibration.** Run microbenchmark
  estimates against a candidate fixture to calibrate the
  placeholder thresholds (bytes ≈ (1 - selectivity) ×
  baseline, 2× wall-clock on selective filters, 1ms binder
  pass).

- **P0.7: `BindingError` taxonomy pre-draft.** Enumerate the
  bind-time failures the binder must produce: column not in
  schema, ambiguous column reference, type mismatch in
  comparison, experimental-kernel-not-enabled, unresolved
  table.

- **P0.8: `chore:` cleanup commits.** Per topics iter-3:
  - Honest roadmap rewrite (M5 / M6 / M9 not green; remove
    "shipped" claims that don't meet ADR 001 hard gates).
  - Comment cleanup (replace `// V0`, `// Mock`, `// Dummy`
    with either real code or tracked gap notes).
  - Test relocation from `src/lib.rs` to `tests/` directory.
  - Impl-plan checklist honesty (un-check items that don't
    meet hard gates).
  These land before Phase 1 code begins.

### Phase 1 — Structured errors

Land first because every later phase consumes the typed error
enum. No behavior change visible to callers; refactor only.

- **New file `src/error.rs`.** Defines:
  - `StorageError` (ADR §10).
  - `ExecutionError` (ADR §10).
  - `BindingError` (P0.7 taxonomy).
  - `CatalogError` (new — table not found, schema not found,
    fingerprint resolution failure).
  - `QueryError` (top-level wrapper composing the above;
    includes `SchemaInstability` for descriptor-evolution
    double-fail).
- **All `.to_string()` boundaries rewritten.**
  - `src/exec.rs`: `ExecutionError::Storage(_)` and
    `ExecutionError::Compute(_)` with `#[from]`.
  - `src/storage.rs`: replace `StorageError::Internal(String)`
    with structured variants.
  - `src/server/{mod, spill}.rs`: structured errors instead
    of `Result<_, String>`.
  - `src/pbc.rs`: keep `LoweringDeclineReason` (already
    typed) but rename to `PushdownDeclineReason` and mark
    crate-private.
- **`thiserror` dep added** to `crates/toron/Cargo.toml`.
- **Tests:**
  - Round-trip: every Wendelon-authored error preserves
    origin through the boundary (no information loss).
  - `BindingError::ExperimentalKernel` constructible.
  - Architecture guard catches new public
    `Result<_, String>` signatures.

### Phase 2 — IR split and binder pass

- **New file `src/ir/logical.rs`.** Defines `LogicalPlan`
  (frontend output) and `LogicalExpr` (string column refs).
  The current `src/ir.rs`'s `LogicalPlan` migrates here
  largely intact, minus the `Project::exprs` field-naming
  hack — output column names come from the binder.
- **New file `src/ir/execution.rs`.** Defines `ExecutionPlan`,
  `ExecutionExpr`, `ResolvedColumn`, `PbcProgram` newtype,
  `ColumnOrdinal` newtype, `ColumnName` newtype, `SessionId`,
  `QueryId`.
- **New file `src/binder.rs`.** Pure function `bind(plan:
  LogicalPlan, catalog: &dyn Catalog, config: &PlannerConfig)
  -> Result<ExecutionPlan, BindingError>`.
  - Walks `LogicalPlan` bottom-up.
  - At `Scan { table }`, calls `catalog.resolve_table()`,
    captures `TableObjectId`, fingerprint, schema, PBC-
    eligible column set.
  - At every `Expr::Column(name)`, resolves to
    `ResolvedColumn` with ordinal, type, nullability, PBC
    eligibility.
  - At `Aggregate`, validates aggregate-function args (column
    types match expected). Computes output schema.
  - At every `Expr::LinearRegress | ApproxQuantile |
    TimeBucket` occurrence, if
    `!config.allow_experimental_kernels`, returns
    `BindingError::ExperimentalKernel`.
- **Rewrite `src/sql_planner.rs` and `src/rune_engine.rs`** to
  emit `LogicalPlan` only (no `ExecutionPlan`). Hardcoded
  table-name `match` blocks removed; SQL / Rune emit
  `TableRef { schema, table }` to be resolved by the binder.
- **Tests:**
  - Round-trip: every fixture `LogicalPlan` binds to a typed
    `ExecutionPlan` with all column refs resolved.
  - Negative: unknown column →
    `BindingError::UnknownColumn`.
  - Negative: type mismatch → `BindingError::TypeMismatch`.
  - Negative: `LinearRegress` with default config →
    `BindingError::ExperimentalKernel`.
  - Positive: `LinearRegress` with
    `allow_experimental_kernels = true` binds.
  - Architecture guard: `ExecutionExpr` enum does not contain
    `Column(String)` variant (compile-time check).

### Phase 3 — Catalog trait and implementations

- **New file `src/catalog.rs`.** Defines `Catalog` trait (ADR
  §2), `ResolvedTable` struct, `MockCatalog` for fixtures.
- **New file `src/storage/wendelon_catalog.rs`.**
  `WendelonCatalog` wraps `wendelon_client::Client`; resolves
  `(SchemaName, TableName)` to `ResolvedTable` by querying
  Wendelon's descriptor service. Caches descriptors by
  `(TableRef, HlcReadBound)` for the lifetime of a query
  session.
- **Tests:**
  - `MockCatalog` returns hand-configured descriptors for
    fixture queries.
  - `WendelonCatalog` integration test (in-process Wendelon
    fixture) round-trips descriptor resolution.
  - Fingerprint round-trip: resolved table's fingerprint
    matches the descriptor's stored fingerprint.

### Phase 4 — PBC pushdown wiring (THE keystone)

- **Rewrite `src/pbc.rs`.** The seed's `PbcCompiler` operates
  on `Expr` (string columns); rewrite to operate on
  `ExecutionExpr` (resolved columns). Column ordinals come
  directly from `ResolvedColumn::ordinal`, no `HashMap<String,
  u32>` lookup.
- **New planner pass: `src/planner/pushdown.rs`.** Walks
  `ExecutionPlan` and for every `Filter` above `Scan`:
  - Tests whether the entire `predicate` is pushable
    (all-or-nothing per ADR §5; five eligibility criteria).
  - If pushable: compiles to `PbcProgram`, attaches to
    `Scan.pbc_program`, sets `Filter.disposition =
    FilterDisposition::FullyPushed`. Toron skips
    evaluation.
  - If unpushable: records `PushdownDeclineReason` in a
    per-plan diagnostic, sets `Filter.disposition =
    FilterDisposition::EvaluateInToron`.
  - For `Project` above `Scan`, computes the projected column
    ordinal set (union of filter-referenced, project-output,
    aggregate-input, and group-by columns); attaches to
    `Scan.projection`.
  - For `Scan` with no parent or with `Aggregate` parent
    only, projection is the aggregate-input column set.
  - Selects `QueryProfileHint::PointRead` iff the pushed
    predicate matches the primary-key column with equality;
    `Scan` otherwise.
- **Rewrite `src/exec.rs`.** Consumes `ExecutionPlan`, not
  `LogicalPlan`. `Scan` constructs `ScanRequest` with
  `pbc_predicate` populated from `Scan.pbc_program` and
  `projected_columns` from `Scan.projection`. `Filter`
  branches on `disposition`: `FullyPushed` → skip evaluation,
  pass through; `EvaluateInToron` → evaluate predicate
  normally.
- **Rewrite `src/storage.rs`.** `ScanRequest` carries the V1
  contract shape (ADR §3). `WendelonStorageScan` passes the
  PBC payload and fingerprint to
  `wendelon_client::table_query`.
- **`StorageScan` returns `ScanStream`** (ADR §4). V1 impl is
  trivial: wrap the existing `Vec<RecordBatch>`
  materialization as `futures::stream::iter(...)`.
- **Tests:**
  - Parity: PBC-engaged path and pure-Toron path produce
    byte-identical Arrow output on every fixture (with
    canonical record-batch normalization applied).
  - Decline: `TimeBucket` in filter records
    `ExperimentalKernelInPredicate` (with experimental
    flag); not pushed even when flag is set, because the
    seed's PBC catalog does not include `TimeBucket`.
  - Projection: a query selecting 2 of 10 columns results in
    a `projected_columns` set of size 2 in the
    `ScanRequest`.
  - Profile hint: `Filter(col_pk == lit)` resolves to
    `QueryProfileHint::PointRead`;
    `Filter(col_non_pk == lit)` resolves to
    `QueryProfileHint::Scan`.

### Phase 5 — Aggregate operator and GROUP BY

- **Extend `src/exec.rs::Aggregate`.** Replace the
  `if !group_expr.is_empty() { NotImplemented }` branch with
  a hash-aggregation path:
  - Compute group keys per row via `ExecutionExpr`
    evaluation.
  - Maintain a `HashMap<GroupKey, AggregateState>`
    accumulator.
  - For `CountAll`, `Count(expr)`, `Sum(expr)`: standard
    aggregator types.
  - On stream end, materialize results as a `RecordBatch`
    with **lexicographic group-key ordering** per ADR §7.
    The runtime sorts; parity tests apply the canonical
    concat-and-reslice normalization separately.
  - **`Sum(expr)` over all-null group returns `Null`**, not
    `0` — Arrow kernel semantics. Pinned by ADR §7.
  - **Group keys with nulls** participate in grouping per
    Arrow grouping semantics (one null-group per distinct
    null-column tuple).
- **Aggregate IR shape update.** `AggregateFunction` enum
  widens:
  - `CountAll` (existing, args ignored, document as such).
  - `Count(ExecutionExpr)` (new; counts non-null).
  - `Sum(ExecutionExpr)` (existing; restricted to numeric
    types via binder validation; all-null group → `Null`).
  - `LinearRegress` (tag `@experimental`, gated by
    `allow_experimental_kernels`).
  - `ApproxQuantile(f64)` (tag `@experimental`).
- **Tests:**
  - `SELECT a, b, COUNT(*), SUM(c) FROM t GROUP BY a, b`
    over a Wendelon-native fixture produces results matching
    a hand-computed oracle, with stable output ordering.
  - `Count(col_with_nulls)` excludes nulls; `CountAll`
    counts them.
  - `SUM(all_null_col)` returns `Null` (single-group case).
  - **Cross-type `SUM` all-null parity**: one fixture per V1
    numeric type — `Int8`, `Int16`, `Int32`, `Int64`,
    `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`,
    `Float64`. Each produces `Null` on all-null input.
  - `SUM(...)` GROUP BY with one all-null group and one
    mixed group: all-null group returns `Null`, mixed
    returns sum of non-null values.
  - Empty group result: `SELECT COUNT(*) FROM empty_table`
    returns single-row `0`.
  - Null group key: `GROUP BY col_with_nulls` produces a
    distinct group for the null bucket.
  - Experimental: `LinearRegress` with default config
    rejected at binder.
  - Experimental: `LinearRegress` with flag set passes
    binder, runs (existing seed implementation).

### Phase 6 — Descriptor-fingerprint-pin protocol

The single descriptor-evolution retry lives in the
**coordinator**, not the executor or a future query-service
layer. The retry requires re-binding (which requires the
catalog, which the executor must not see) and re-planning
(which is not the executor's job). The executor stays pure —
given an `ExecutionPlan`, run it. The coordinator owns query
lifecycle, retry policy, and catalog interaction.

- **New file `src/coordinator.rs`** (or extend
  `src/server/mod.rs`'s `ToronCoordinator`):
  - `Coordinator::admit_query(plan: LogicalPlan, session:
    SessionId) -> Result<QueryId, QueryError>`:
    1. Walk `LogicalPlan` to collect `TableRef`s.
    2. For each, `catalog.resolve_table(...)` → pinned
       fingerprint.
    3. Pass pinned fingerprints to the binder via
       `PlannerConfig::pinned_fingerprints`.
    4. Bind and lower to `ExecutionPlan`.
    5. Return `QueryId`.
  - `Coordinator::execute_query(query: QueryId) ->
    Result<...>`:
    1. Dispatch `ExecutionPlan` to executor.
    2. If `StorageError::DescriptorEvolved` surfaces and
       this is the first retry, invalidate cached
       descriptor, re-admit, re-execute.
    3. If second occurrence, fail with
       `QueryError::SchemaInstability`.
- **Tests:**
  - Static descriptor: query admits, plans, executes
    successfully.
  - Single mutation: descriptor changes between admission
    and execution; coordinator retries with new descriptor;
    succeeds.
  - Double mutation: descriptor changes again between retry
    and execution; coordinator fails with
    `QueryError::SchemaInstability`.

### Phase 7 — Architecture guards

- **New file `crates/toron/tests/architecture_test.rs`.**
  Implements the seven guards (ADR §11):
  1. No eval-time string lookup: grep / AST inspection that
     `column_by_name` is not called outside `binder.rs` and
     `wendelon_catalog.rs`.
  2. PBC engaged on eligible filters: runtime instrumentation
     test that `ScanRequest.pbc_predicate` is `Some(_)` when
     the filter predicate is wholly PBC-eligible.
  3. No `.to_string()` error boundaries: grep that
     `Result<_, String>` is not in any public or
     `pub(crate)` function signature.
  4. No `Column(String)` in `ExecutionExpr`: compile-time
     structural check (the variant doesn't exist).
  5. Experimental kernels gated: binder rejects
     `LinearRegress` with default config.
  6. Fingerprint required: `ScanRequest` cannot be
     constructed without a fingerprint (no default).
  7. `ScanStream` collection localized: grep that
     `ScanStream::collect` / `.batches.collect()` /
     `.try_collect()` patterns appear only in
     `coordinator.rs` and test fixtures, nowhere in
     `exec.rs` or operator modules.
- **Tests:** all seven pass.

### Phase 8 — Bench matrix and promotion

Records all promotion gates from the ADR. Lives in
`plans/impl/0002_storage_execution_contract_bench_results.md`.

- **Bench home:** `benches/storage_execution.rs` (new).
  Mirrors Wendelon's per-ADR bench-results pattern.
- **Bench flag:** `--storage-execution-only`.
- **Required query shapes** (parity harness):
  - scan + projection (no filter);
  - scan + filter (no projection narrowing);
  - filter + projection where filter column is not in
    output;
  - `COUNT(*)`;
  - `COUNT(nullable_col)`;
  - `SUM(nullable_col)` over mixed-null group;
  - `SUM(all_null_col)` (must return `Null`);
  - `GROUP BY key, SUM(value)`;
  - `GROUP BY col_with_nulls` (null-group distinct bucket);
  - all-null input page (ADR 070 fixture);
  - sparse projection input (ADR 060 fixture);
  - encoded-validity input (ADR 069 fixture);
  - page-stats-skippable predicate (ADR 063 fixture);
  - descriptor-evolved retry path;
  - descriptor-evolved double-fail path.
- **Axes:**
  - Fixture: Wendelon-native sparse-projection / encoded-
    validity / page-stats / all-null-elision; hand-rolled
    COUNT / SUM / GROUP BY; multi-tenant; descriptor-
    mutation.
  - PBC engagement: enabled vs declined (toggled by
    predicate shape).
  - Row tiers: 8K / 65K / 1M / 10M rows.
  - Profile: `PointRead` / `Scan`.
- **Per-row metrics:**
  - bytes fetched (baseline vs PBC-engaged);
  - wall-clock (baseline vs PBC-engaged);
  - range count;
  - PBC engagement observed (boolean, asserted);
  - selected profile hint (`PointRead` / `Scan`);
  - declined-pushdown reason (if any);
  - semantic parity digest (Arrow batch byte hash, after
    canonical concat-and-reslice normalization at 65,536-row
    batches per ADR §"Resolved Q4");
  - cross-replica determinism digest.

- **Canonical record-batch normalization** (one helper, shared
  across all parity tests, mandated by ADR §"Resolved Q4"):

  ```rust
  fn canonical_normalize(
      batches: Vec<RecordBatch>,
      batch_size: usize, // V1 default: 65_536
  ) -> Vec<RecordBatch>;
  ```

  Concat → reslice → hash. Both PBC-engaged and pure-Toron
  paths run through the same helper before byte-identical
  comparison.
- **Verification commands:**

  ```text
  # Local quick verification
  cargo fmt
  cargo check --workspace
  cargo test -p toron
  cargo test -p toron pbc
  cargo test -p toron storage
  cargo test -p toron --test architecture_test
  cargo test -q binder
  cargo test -q pushdown
  cargo test -q aggregate
  cargo test -q descriptor_pin

  # Pre-promotion full verification
  cargo test --workspace
  cargo bench --bench storage_execution --no-run
  cargo bench --bench storage_execution -- --quick --storage-execution-only
  cargo bench --bench storage_execution -- --storage-execution-only --rows=8192,65536,1048576
  git diff --check
  ```

- **Suggested commit slices:**
  1. `chore: adr 002 prep — roadmap honesty + comment
     cleanup` (Phase 0 P0.8 housekeeping)
  2. `plan: adr 002 toron storage backed execution contract`
     (ADR + impl land in `plans/adr/` and `plans/impl/`)
  3. `feat: adr 002 phase 1 — structured error taxonomy`
  4. `feat: adr 002 phase 2 — ir split and binder pass`
  5. `feat: adr 002 phase 3 — catalog trait and impls`
  6. `feat: adr 002 phase 4 — pbc pushdown wiring
     (keystone)`
  7. `feat: adr 002 phase 5 — grouped aggregate`
  8. `feat: adr 002 phase 6 — fingerprint pin protocol`
  9. `feat: adr 002 phase 7 — architecture guards`
  10. `feat: adr 002 phase 8 — bench matrix + promotion`
      (lands `bench_results.md`; marks ADR 002 shipped on
      roadmap)
  Slice boundaries can shift during implementation, but
  reviews should keep error-taxonomy, IR-split, catalog,
  pushdown-wiring, aggregate, fingerprint-pin, and bench
  matrix separable. Wendelon-side `feat:` commits (if
  needed per Phase 0 P0.2 / P0.3) interleave as separate
  commits with their own message naming the Wendelon
  surface added.

- **Roadmap update.** `plans/wip/roadmap.md` records ADR 002
  promotion; forward sequence updated to reflect ADR 003
  candidates (executor model / schema registry / resilient
  shuffle) evidence-driven from this bench matrix.

If hard gates pass, promote. If a hard gate fails, the
council decides among three explicit paths (Wendelon ADR 070
precedent):

1. **Revise V1:** narrow eligibility, tighten oracle, fix a
   determinism source.
2. **Scope down:** ship PBC engagement + binder + structured
   errors as plumbing; defer fingerprint-pin to ADR 002.x.
3. **Shelve:** keep the seed unchanged, document the gap,
   reopen when fixture matrix tells us where to look.

## Files touched

Implementation:

- `crates/toron/src/error.rs` (new) — structured error
  taxonomy.
- `crates/toron/src/ir/mod.rs` (new — restructure from
  `src/ir.rs`).
- `crates/toron/src/ir/logical.rs` (new) — `LogicalPlan`,
  `LogicalExpr`.
- `crates/toron/src/ir/execution.rs` (new) — `ExecutionPlan`,
  `ExecutionExpr`, `ResolvedColumn`, newtypes.
- `crates/toron/src/binder.rs` (new) — `bind` function.
- `crates/toron/src/catalog.rs` (new) — `Catalog` trait,
  `MockCatalog`.
- `crates/toron/src/storage/mod.rs` (refactor) —
  `ScanRequest`, `ScanStream`, `StorageScan`.
- `crates/toron/src/storage/wendelon_catalog.rs` (new) —
  `WendelonCatalog`.
- `crates/toron/src/storage/wendelon_scan.rs` (refactor —
  extract from current `storage.rs`) — `WendelonStorageScan`.
- `crates/toron/src/storage/in_memory.rs` (refactor — extract
  from current `storage.rs`) — `InMemoryStorageScan`.
- `crates/toron/src/pbc.rs` (rewrite) — operates on
  `ExecutionExpr`.
- `crates/toron/src/planner/mod.rs` (new — restructure from
  empty `planner.rs`).
- `crates/toron/src/planner/pushdown.rs` (new) — pushdown
  selection pass.
- `crates/toron/src/exec.rs` (rewrite) — consumes
  `ExecutionPlan`, populated `ScanRequest`, GROUP BY.
- `crates/toron/src/coordinator.rs` (new — or extension of
  `src/server/mod.rs`) — `admit_query`, `execute_query`,
  fingerprint-pin protocol.
- `crates/toron/src/sql_planner.rs` (refactor) — emits
  `LogicalPlan` only.
- `crates/toron/src/rune_engine.rs` (refactor) — emits
  `LogicalPlan` only.
- `crates/toron/tests/architecture_test.rs` (new) — seven
  guards.
- `crates/toron/benches/storage_execution.rs` (new) — bench
  matrix.
- `crates/toron/Cargo.toml` — add `thiserror`, `futures-util`.

Planning / evidence:

- `plans/adr/0002_toron_storage_backed_execution_contract.md`
- `plans/impl/0002_toron_storage_backed_execution_contract.md`
  (this file)
- `plans/impl/0002_storage_execution_contract_bench_results.md`
  (post-promotion)

Roadmap:

- `plans/wip/roadmap.md` — update Status, add ADR 002 to
  Shipped Foundations, adjust Forward Sequence to reflect
  evidence-driven ADR 003 scope.

Wendelon-side `feat:` (if Phase 0 P0.2 / P0.3 surface gaps):

- `crates/wendelon/crates/wendelon-client/...` — fingerprint
  pin parameter on `table_query`, if absent.
- `crates/wendelon/src/schema/pbc/...` — additional opcodes
  if needed for PBC catalog completion.

Out of scope:

- Executor streaming end-to-end (`ScanStream` is consumed
  internally as buffered in V1). ADR 003 territory.
- Multi-process plan serialization. ADR 003 territory.
- Subtree-split pushdown. V1.x.
- Full DDL propagation. V1.x.
- Advanced kernel graduation. Each its own ADR.
- Trait-based aggregate dispatch. V1.x.
- Wendelon-side crate extraction. Import allowlist only.
- Multi-tenant resource governance. ADR 005+.
- Rune sandbox enforcement. Separate ADR.

## Risks and mitigations

- **R1: Wendelon public surface gaps.** PBC ops or
  fingerprint pin parameter may not be in `wendelon-client`
  yet.
  *Mitigation:* Phase 0 P0.2 / P0.3 audit; surface as
  Wendelon `feat:` work ahead of ADR 002 promotion.
- **R2: Binder pass perf cost.** Per-plan binding may exceed
  the 1ms soft gate on large plans.
  *Mitigation:* Phase 0 P0.6 calibration; soft gate is
  recorded-not-blocking; binder is hot-path-cacheable if
  needed.
- **R3: PBC engagement test flakiness.** Architecture guard
  2 uses runtime instrumentation; risk of false negatives
  if the test setup doesn't trigger the eligible-filter
  path.
  *Mitigation:* explicit per-fixture instrumented test;
  cross-check `pbc_predicate.is_some()` post-planning.
- **R4: Fingerprint-pin double-retry semantics.** A
  descriptor evolving twice within the retry window produces
  `SchemaInstability` — but the line between "rapid DDL"
  and "Wendelon bug" is blurry.
  *Mitigation:* single retry only; clear error message.
  V1.x can add backoff or DDL coordination.
- **R5: GROUP BY perf on large cardinality.** Hash-
  aggregation on one thread (no parallelism in this
  ADR) is the V1 shape; high-cardinality groups will be
  slow.
  *Mitigation:* ADR 003 (executor model) likely introduces
  morsel-parallel aggregate; V1 ships it on one thread.
- **R6: Refactor regression in existing M3 / M5 / M6
  paths.** Rewriting `exec.rs` and `storage.rs` is invasive;
  risk of breaking the seed's existing tests.
  *Mitigation:* phase ordering — Phase 1 (errors) → Phase 2
  (IR split) lands behind a temporary type alias; existing
  tests keep passing through each phase. Final phase
  removes the alias.
- **R7: SQL / Rune frontend table-name removal.** Removing
  the hardcoded `match name { "sales" => TableId(1), ... }`
  breaks the seed's M7 and M8 tests.
  *Mitigation:* update the test fixtures to use
  `MockCatalog` with the same logical name → object id
  mapping.
- **R8: Wendelon-side `KllSketch` import (seed M11) without
  experimental flag.** The seed's `Aggregate::ApproxQuantile`
  uses Wendelon's KLL substrate. Gating behind experimental
  flag is correct, but the binder change risks breaking the
  M11 test.
  *Mitigation:* update M11 test to construct planner with
  `allow_experimental_kernels = true`.
- **R9: `tokio` dep already taken at M0 — already a violated
  ADR 001 commitment.** ADR 002 does not relitigate;
  ratifies the decision via the impl plan.
  *Mitigation:* note in Phase 0 audit; no action needed.
- **R10: Wendelon-Toron type duplication.** ADR 002 keeps
  the conversion at `WendelonStorageScan` boundary (no
  crate extraction).
  *Mitigation:* document the import allowlist; revisit in
  V1.x.
- **R11: Bench-fixture availability.** ADR 070 fixtures may
  not be reusable as-is (different schema, different test
  harness).
  *Mitigation:* Phase 0 P0.4 audit; if Wendelon fixtures
  need Toron-side wrapping, build the harness in Phase 8.
- **R12: Closed-system trust contract drift.** If
  Wendelon's fingerprint semantics evolve under our feet,
  ADR 002's protocol breaks silently.
  *Mitigation:*
  `wendelon::schema::PhysicalDescriptorFingerprint` is a
  Wendelon-stable type per ADR 051; ADR 002 records the
  dependency.
- **R13: Frontend output schema instability.** Removing
  `col_0, col_1, ...` field naming hack in `Project`
  requires picking output column names; SQL has `AS`
  syntax, Rune doesn't yet.
  *Mitigation:* SQL frontend uses `AS` aliases or original
  column name; Rune defaults to original column name;
  future Rune ADR can add aliasing.
- **R14: `ScanStream` collapses to materialization.** If
  every executor call site immediately `.collect()`s the
  stream into `Vec<RecordBatch>`, the streaming interface
  is a fiction and ADR 003 cannot switch to true streaming
  without breaking every site.
  *Mitigation:* the executor's `.collect()` calls land in
  exactly one place — the result-materialization edge
  facing the coordinator. Intermediate operators consume
  the stream shape (`BoxStream`) directly even if they
  internally buffer. Architecture guard (Phase 7 bonus):
  grep that `ScanStream::collect` / `.batches.collect()`
  patterns appear only in `coordinator.rs` and test
  fixtures.
- **R15: Wendelon-side fingerprint validation surface gap.**
  If `wendelon-client::table_query` doesn't accept a
  fingerprint pin parameter, ADR 002 requires a Wendelon
  `feat:`.
  *Mitigation:* Phase 0 P0.2 audits this; if absent, file
  the Wendelon `feat:` ahead of ADR 002 promotion. Per
  Gemini iter-2, this is *not* a Wendelon-ADR-grade change
  — it's a public-surface addition only.
- **R16: Batch-boundary normalization in parity tests.**
  Byte-identical Arrow comparison is noisy if PBC-engaged
  and pure-Toron paths yield batches of different sizes.
  Weakening the parity assertion would hide real bugs.
  *Mitigation:* define one canonical record-batch
  normalization (concat all batches, re-slice at a fixed
  batch size, or apply a sort-stabilization step) shared
  by every parity test. Document the policy in the bench
  results artifact.
- **R17: One-thread GROUP BY perf on high
  cardinality.** Hash-aggregation in one thread is the V1
  shape.
  *Mitigation:* ADR 002 cares about correctness and
  parity, not throughput. ADR 003 likely introduces
  morsel-parallel aggregate. V1 ships it on one thread;
  bench artifact records cardinality / time as diagnostic.
- **R18: Removing hardcoded SQL / Rune table mappings breaks
  seed M7 / M8 tests.** The seed's `match name { "sales"
  => TableId(1), ... }` blocks are referenced by existing
  tests.
  *Mitigation:* move the logical name → object id mappings
  into `MockCatalog` fixtures rather than preserving
  frontend shortcuts; update M7 / M8 tests to construct
  the catalog with the same names.
- **R19: Cross-numeric-type `SUM` consistency.** Arrow
  kernels handle Int / UInt / Float independently; we need
  to verify all-null behavior is uniform.
  *Mitigation:* Phase 5 explicit per-type fixture matrix —
  one `SUM(all_null_col)` test per numeric type in the V1
  catalog. Decimal absence makes this finite.

## Definition of done

- All 8 Phase 0 pins recorded in the bench-results artifact.
- The Phase 0 P0.8 `chore:` cleanup items land before
  Phase 1 code.
- `src/error.rs` lands; every `.to_string()` boundary in the
  seed replaced.
- `src/ir/{logical, execution}.rs` lands; binder pass
  produces typed `ExecutionPlan`; `Expr::Column(String)`
  exists only in `LogicalExpr`.
- `src/catalog.rs` + `MockCatalog` + `WendelonCatalog` land;
  hardcoded table-name `match` blocks removed from
  `sql_planner` and `rune_engine`.
- `src/pbc.rs` operates on `ExecutionExpr`; planner pass
  attaches `PbcProgram` to eligible `Scan` nodes;
  `ScanRequest` populated.
- `Aggregate { group_expr: !empty }` works; GROUP BY fixture
  matches hand-computed oracle.
- Experimental kernels gated by
  `PlannerConfig::allow_experimental_kernels`.
- Descriptor-fingerprint pin protocol implemented; mutation
  fixture exercises retry-once-then-fail.
- All 7 architecture guards pass.
- All 10 hard promotion gates pass; soft gates met or
  explicitly justified (after Phase 0 P0.6 calibration).
- Bench artifact at
  `plans/impl/0002_storage_execution_contract_bench_results.md`
  records the full fixture matrix.
- Roadmap updated to record ADR 002 promotion and reframe
  ADR 003 scope as evidence-driven from bench results.
