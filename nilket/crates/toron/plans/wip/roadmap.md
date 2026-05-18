# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archaeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not at
implementation commits. The council operates on a block-chain
rhythm: plan → implement → plan → implement, and this file is
a snapshot of the most recent plan. Between sessions the
roadmap lags the code on purpose — an item can read "ADR N
implementation phases pending" while several phases have
already landed on `main`. That is expected. Check `git log`
and the relevant impl plan / bench-results file for actual
status, and roll roadmap edits into the next planning session
along with the forward-sequence rewrite.

## Status

- **ADR 001 — Foundation and Engine Contract.** Promoted + implemented.
- **ADR 002 — Storage-Backed Execution Contract.** Promoted + implemented.
  Bench: `plans/impl/002_storage_execution_contract_bench_results.md`.
- **ADR 003 — Distributed Runtime and Data Movement.** Promoted + implemented.
  Bench: `plans/impl/003_distributed_runtime_bench_results.md`.
- **ADR 004 — Distributed Inner Equi-Join.** Promoted + implemented.
  Bench: `plans/impl/004_distributed_inner_equi_join_bench_results.md`.
- **ADR 005 — Hash-Join Build Path Efficiency.** Promoted + implemented.
  Bench: `plans/impl/005_hash_join_build_path_efficiency_bench_results.md`.
- **ADR 006 — Runtime Filter Pushdown.** Promoted + implemented. Advisory
  Bloom runtime filters for hash-partitioned inner equi-joins via two-wave
  execution. Bench: `plans/impl/006_runtime_filter_pushdown_bench_results.md`.
- **ADR 007 — Rune Sandbox Enforcement.** Promoted + implemented.
  Process-internal sandbox contract for user-supplied Rune scripts.
- **ADR 008 — Outer / Semi / Anti Joins.** Promoted + implemented. `JoinKind`
  to 8 variants; bench:
  `plans/impl/008_outer_semi_anti_joins_bench_results.md`.
- **ADR 009 — Correlated `EXISTS` / `NOT EXISTS` Decorrelation.** Promoted +
  implemented. SQL-AST rewrite in `SqlPlanner::lower_select` lowering
  correlated `EXISTS`/`NOT EXISTS` to `LeftSemi`/`LeftAnti` joins.
- **ADR 010 — Top-Level SQL Clause Accounting and Deterministic Result
  Ordering.** Promoted + implemented. Exhaustive `Query`/`Select` field
  accounting plus `LogicalPlan::Sort` / `ExecutionPlan::Sort` over a single
  terminal gather-sort.
- **ADR 011 — Aggregate Projection Accounting and `GROUP BY` SQL
  Reachability.** Promoted + implemented. Strict aggregate projection-item
  accounting; narrow single-table `GROUP BY` wired to the grouped aggregate
  path.
- **ADR 012 — Mixed-Wildcard Projection Accounting and Planner Hardening.**
  Promoted + implemented. Non-aggregate projection silent-drop closed;
  `TIME_BUCKET` arity hardened.
- **ADR 013 — Runtime Filter Application at the Storage-Scan Boundary.**
  Promoted + implemented. Both `StorageScan` impls apply
  `request.runtime_filter`; Wendelon table-query wire carries
  `runtime_filter_len`; production Wendelon-enabled selective row in
  `plans/impl/006_runtime_filter_pushdown_bench_results.md`.
- **ADR 014 — Exact Small-Set Runtime Filters.** Promoted + implemented.
  ExactSet runtime-filter kind for hash-partitioned inner equi-joins with
  small build sets (gate: serialized exact payload ≤ Bloom
  `MIN_FILTER_BYTES`); coordinator lowers exact-to-Bloom on overflow. Proving
  production Wendelon row appended to
  `plans/impl/006_runtime_filter_pushdown_bench_results.md`.
- **ADR 015 — Terminal Ordering for Grouped Aggregate SQL.** Promoted +
  implemented. Composes ADR 010 (`Sort`) and ADR 011 (`Aggregate`) to enable
  `GROUP BY ... ORDER BY [LIMIT/OFFSET]` via one narrow fragmenter detector
  that emits a three-stage pipeline
  (producers → grouped aggregate consumers → terminal sort). No new IR.
- **ADR 016 — Post-Aggregate Filtering with `HAVING`.** Promoted +
  implemented. Second instance of the ADR 015 cycle shape: removes one
  planner reject, adds a localized planner-side rewrite of `HAVING`
  aggregate-call references against projection-output field names (matched
  on the lowered `(AggregateFunction, Vec<Expr>)` IR pair, case-insensitive
  and argument-normalized), and extends the ADR 015 detector with one
  optional `Filter` between the terminal-gather `Exchange` and the
  `Aggregate`. No new IR, no new binder seam, no new `FilterDisposition`.
- **ADR 017 — `ORDER BY <aggregate-call>` for Grouped Aggregate SQL.**
  Promoted + implemented. Third instance of the ADR 015 cycle shape:
  `lower_select` returns a private `LoweredSelect` carrying the grouped
  aggregate scope, the ADR 016 aggregate-call rewrite walker is clause-
  neutral, and grouped `ORDER BY SUM(...)` / `COUNT(...)` references lower
  to projected aggregate output fields. No new IR, no binder expression
  surface change, no fragmenter detector change.
- **ADR 018 — Scalar-Aggregate `HAVING` / `ORDER BY` SQL.** Promoted +
  implemented. Fourth instance of the ADR 015 cycle shape,
  generalizing the ADR 016 / ADR 017 substrate to the scalar (no-`GROUP
  BY`) aggregate surface: `lower_select`'s global-aggregate branch
  constructs `GroupedAggregateScope::from_projection(&[], &aggr_expr)`
  and inlines the optional HAVING-attachment; the `HAVING`-without-
  `GROUP BY` reject conditions on the absence of an aggregate
  projection. HAVING-only scalar plans route via `single_task_stage`
  (zero exchanges); ordered scalar plans route via the generic
  one-exchange producer/consumer path (ADR 010's `Sort + Exchange(1, [])`
  wrap). The grouped-aggregate branch is byte-identical pre- and
  post-ADR-018 — no helper extraction, no reject-ordering refactor.
  Closes ADR 016 §1 and ADR 017 §10 catch-all rejects. No new IR, no
  binder expression surface change, no fragmenter detector change, no
  walker signature change.
- **ADR 019 — MIN / MAX Aggregate Kernels.** Promoted + implemented.
  First kernel-catalog widening under the post-ADR-018 cycle.
  Three-way council convergence ratified MIN+MAX bundled over the
  early-iter AVG candidate, with the conservative `Int32` / `Int64`
  type-domain intersection (defers float to a dedicated NaN-policy ADR,
  defers `Utf8` to preserve `AggState: Copy`). `MIN` and `MAX` will
  remove themselves from the `lower_aggregate_function` catch-all via
  narrow named arms; identity-type output (`Int32` → `Int32`, `Int64`
  → `Int64`); `Option<T>` state with no-non-NULL-seen → NULL
  finalization. Bench:
  `plans/impl/019_min_max_aggregate_kernels_bench_results.md`.
  Wendelon-side already ships parallel
  `Min { column }` / `Max { column }` variants at
  `crates/wendelon/src/schema/aggregate.rs:94-125`; ADR 02X engages
  the cross-ring consumer wiring. No new IR variant beyond the enum
  widening (5 → 7), no fragmenter detector change, no cross-ring
  engagement, `AggState` stays `Copy`.
- **ADR 020 — AVG Aggregate Kernel.** Promoted + implemented. Second
  kernel-catalog widening under the post-ADR-018 cycle. AVG completes
  the basic analytical set (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`); removes
  itself from the `sql_planner.rs:556` catch-all via one named
  arm. Three-way council convergence ratified AVG-alone over Gemini's
  bundle-with-kernel-registry-refactor position (dissent recorded in
  §V1 §9; ADR 021 ratifies the refactor). Multi-state `(total: i128,
  count: u64)` integer accumulator and `(total: f64, count: u64)`
  float accumulator; Float64-uniform output (DuckDB / SQLite parity;
  Decimal output awaits V1.x Decimal ADR); `count == 0 → NULL`
  finalization pinned via explicit `vec![None]` assertions; sticky-NaN
  behavior pinned by hard-gate regression test. Legacy unbound path
  uses a single `AvgFloat` accumulator (favorable asymmetry vs MIN/MAX
  — no first-batch type dance, no empty-input error). ADR 021 later
  supersedes the temporary catalog-wide
  `aggregate_function_enum_guards_are_narrow` guard with
  `aggregate_kernel_catalog_guards_are_narrow`.
  Bench:
  `plans/impl/020_avg_aggregate_kernel_bench_results.md`.
  Wendelon-side already ships `SignedAverageParts { sum: i128, count:
  u64, overflow: bool }` at `crates/wendelon/src/schema/aggregate.rs:94-125`
  — structural equivalence with different error discipline (Toron's
  `checked_add` + explicit `ExecutionError::Kernel` is the semantic
  equivalent of Wendelon's overflow bit); ADR 02X pins the cross-ring
  contract when it engages. No new IR variant beyond the enum widening
  (7 → 8), no fragmenter detector change, no cross-ring engagement,
  `AggState` stays `Copy`.
- **ADR 021 — Aggregate Kernel Catalog Refactor.** Promoted + implemented. Internal
  substrate ADR honoring ADR 020's Gemini dissent ("the maintenance
  cliff justifies bundling"). Centralizes stable aggregate metadata
  into one private `aggregate_kernel.rs` module with an
  `AggregateKernelSpec` table indexed by a `#[repr(u8)]`
  `StableAggregateKernel` compact tag. Six stable rows
  (`CountAll`/`Count`/`Sum`/`Min`/`Max`/`Avg`); planner, binder, and
  executor-adjacent helpers delegate to the catalog; the three
  private binder helpers `sum_output_data_type` /
  `min_max_output_data_type` / `avg_output_data_type` are removed
  (logic folds into an `AggregateOutputRule` enum with `Int64`,
  `Float64`, `SameAsInput`, `SumNumeric` variants). Codex + Claude
  2-of-3 majority across iter-0 through iter-3 landed on
  catalog-only mechanism (no runtime trait dispatch in the row
  path) and refactor-alone scope (no Float MIN/MAX bundle). Gemini's
  four-iteration dissent on both — trait-dispatch with row-path
  `accumulate(state, array, row)` and Float MIN/MAX bundling as a
  "sticky-NaN proof case" — recorded; Float MIN/MAX promotes as
  ADR 022 against the new spine. `AggState` stays in `exec.rs` and
  stays `Copy`; row-update bodies (`add_sum_value`, `add_avg_value`,
  MIN/MAX update helpers), per-kernel state helpers, and per-kernel
  finalize helpers are byte-identical pre- and post-refactor.
  Architecture guard rename:
  `aggregate_function_enum_guards_are_narrow` →
  `aggregate_kernel_catalog_guards_are_narrow` (source-read
  catalog-exhaustiveness with module-privacy and `AggState: Copy`
  assertions); companion `bind_expr_remains_aggregate_free` adds the
  ADR 015 reject-string-absence pattern to the aggregate surface.
  SoA / SIMD forward-compatibility recorded as council-converged
  consideration: catalog is pure metadata (no fn pointers in spec);
  `StableAggregateKernel` `#[repr(u8)]` is the future plan-payload
  shape; centralized dispatch points (`init_bound_state_for_call`
  and optional `finalize_scalar_for_call`) are the SoA rewrite
  seams; no row-path dyn dispatch (Codex's audit-clarity argument
  is also the SoA-readiness guard). Bench:
  `plans/impl/021_aggregate_kernel_catalog_refactor_bench_results.md`.
  No minimum-speedup gate (refactor; expectation is no regression).
  No SQL surface change, no IR shape change, no fragmenter detector
  change, no cross-ring engagement.
- **ADR 022 — Float MIN / MAX Aggregate Kernels.** Promoted. First
  downstream user of the ADR 021 catalog spine; honors Gemini's
  recorded ADR 021 dissent ("sticky-NaN proof case") while leaving
  the trait-dispatch dissent on the recorded shelf per the
  SoA-readiness argument. Widens `MIN` / `MAX` to accept `Float32`
  and `Float64` under a sticky-NaN total-order policy:
  `AggregateInputDomain::IntMinMax` renames to `OrderedMinMax` and
  the acceptance predicate widens to
  `Int32 | Int64 | Float32 | Float64`; the existing MIN / MAX
  catalog rows are preserved (six total). NaN policy:
  sticky-NaN — any NaN input produces NaN output for both MIN and
  MAX (matches ADR 020 AVG; avoids `(AVG, MIN)` returning
  `(NaN, finite)` over the same column). Signed-zero policy:
  deterministic `-0.0 < +0.0` via `f32::total_cmp` /
  `f64::total_cmp` (reuses ADR 010 float-sort-key total order).
  `AggState` widens 9 → 13 variants
  (`MinFloat32`, `MinFloat64`, `MaxFloat32`, `MaxFloat64`);
  `AggState: Copy` preserved. Four `*_sticky_total` helpers
  (`f32`/`f64` × `min`/`max`) co-located with `update_min_state` /
  `update_max_state` implement the sticky-NaN branch + `total_cmp`
  ordering; no row-path trait dispatch (ADR 021 SoA-readiness
  invariant stands); never use `f64::min` / `f64::max` (one-sided
  `minNum` NaN behavior masks sticky-NaN). Binder error strings
  enumerated:
  `"MIN expects Int32, Int64, Float32, or Float64 input"`
  (Codex iter-1 dissent against `"numeric"`, ratified iter-2 —
  MIN / MAX still rejects unsigned ints and other out-of-scope
  domains such as Decimal, Date / Timestamp, Boolean, and Utf8).
  Narrow Float32 literal coercion companion in
  `coerce_literal_value`: four new arms
  (`Int32 → Float32`, `Int64 → Float32`, `Int64 → Float64`,
  `Float64 → Float32`) make `HAVING MIN(f32_col) > 0` and
  `HAVING MIN(f32_col) > 0.0` reachable; not a general numeric
  coercion lattice. NaN payload non-contractual; tests assert
  `is_nan()` predicate equality not bit identity. Distributed
  digest fixture uses finite values only. Architecture guard
  adjustments: ADR 019 guard switches `IntMinMax` → `OrderedMinMax`
  with admitted-type source-read; ADR 021 guard preserved-string
  list updates and `AggState` variant-count assertion bumps to 13.
  Three-way council convergence reached across four iterations on
  every load-bearing decision; one resolved divergence (error-string
  wording) ratified in Codex's favor. No new IR variant, no new
  `StableAggregateKernel` row, no new `AggregateOutputRule` variant,
  no fragmenter detector change, no cross-ring engagement
  (ADR 02X inherits Wendelon-side divergence-check forward work),
  no `AggState: Copy` removal. Bench:
  `plans/impl/022_float_min_max_aggregate_kernels_bench_results.md`
  (no minimum-speedup gate; > 10% per-row regression threshold).
- **ADR 023 — Post-Aggregate Projection.** Promoted. First
  substrate-and-surface ADR under the post-ADR-018 cycle after
  four consecutive kernel-domain ADRs (019 / 020 / 021 / 022).
  Decouples user-facing SELECT projection from the `Aggregate`
  operator's natural output schema by composing existing
  `LogicalPlan::Project` over `LogicalPlan::Aggregate` with an
  aggregate-output catalog. `GroupedAggregateScope` →
  `AggregateOutputCatalog` (rename surfaces the legibility
  signal — 17 references in `sql_planner.rs`, 12 in scalar
  paths post-ADR-018; the type is per-query name binding,
  distinct from ADR 021's `AggregateKernelSpec` kernel-metadata
  catalog). `LogicalPlan::Project` gains
  `output_names: Vec<Option<String>>` (Shape A; parallel-vector
  invariant `output_names.len() == exprs.len()` enforced by a
  Project-construction helper or direct-site `debug_assert!`,
  plus binder validation); `None` preserves derived-name
  behavior; `Some(name)` carries a SELECT alias;
  no new IR variant; no `Expr::Alias`. Plan shape:
  `Sort? → Exchange(1, []) → Project? → Filter? → Aggregate →
  Exchange(N, group_keys) → inner` with `Project` and `Filter`
  both optional and both executed on the aggregate-consumer
  task (Stage 1). `Project` emission is gated by a
  four-condition predicate: any alias, any computed expression
  (existing `Expr` surface only), any reorder/subset, or any
  HAVING-only aggregate; otherwise omitted to preserve
  byte-identical `ExecutionPlan` digests for currently-accepted
  queries.
  **HAVING aggregate calls not in SELECT — enabled** (Aggregate
  computes `union(SELECT, HAVING)`; Filter consumes; Project
  drops); **ORDER BY hidden aggregate calls — rejected** with
  `"ORDER BY <aggregate-call> requires the aggregate to appear
  in the SELECT list"`; ORDER BY aggregate-call resolution is
  limited to bare aggregate SELECT outputs, not aggregate
  requirements used only inside computed SELECT expressions;
  **HAVING alias visibility — rejected**
  with `"HAVING does not resolve SELECT aliases; spell the
  aggregate explicitly"`. Computed projection bounded to the
  existing `Expr` surface (comparison, boolean, null-predicate,
  nested, literal, column, `TIME_BUCKET`); arithmetic
  (`SUM(x) * 1.1`, `SUM(x) + COUNT(*)`) deferred to a scalar
  arithmetic IR ADR. `GroupedAggregateSignature` finite enum
  preserved; widening to `(AggregateFunction, Vec<Expr>)`
  blocked by derive-Eq/Hash on `AggregateFunction` (contains
  `ApproxQuantile(f64)`) and `Expr` (contains
  `ScalarValue::Float32`/`Float64`); deferred to a future ADR
  bundling the refactor with computed-aggregate-argument
  signature widening. Non-aggregate alias enablement is a
  3/3-ratified companion (`SELECT amount AS a FROM sales`
  accepted; once `output_names` exists, preserving the reject
  costs more than accepting it). One fragmenter detector
  (`is_terminal_sort_over_grouped_aggregate`) widens by one
  optional `[Project]` peel (Project before Filter); ADR 015's
  "one narrow detector" property preserved. SoA-readiness
  invariants from ADR 021 preserved (catalog stays pure
  metadata; no row-path dyn dispatch; centralized dispatch
  seams unchanged; Shape A's parallel vectors are mildly
  SoA-friendlier than the rejected struct-vector). Five new
  architecture guards: rename completion, strict-projection
  reject absence, `output_names` field presence,
  detector-shape peel order, diagnostic preservation. ADR 015
  cycle property at the planner-projection layer; fifth
  instance of the cycle (after ADR 016 / 017 / 018 / 019). No
  new IR variant beyond the field addition; no new
  `StableAggregateKernel` row; no new `AggregateOutputRule`
  variant; no `AggState` change; no `AggState: Copy` removal;
  no fragmenter recognition site beyond one detector body
  widening; no cross-ring engagement. Three-way council
  convergence reached across four iterations; two recorded
  dissents (name: Claude preferred `PostAggregateScope`; scope
  language: Gemini retained generic computed-projection
  phrasing) carried by 2-of-3 majority per ADR 022 enumerated-
  error-string precedent. Plan-digest preservation for
  currently-accepted queries is the bench gate (no minimum-
  speedup, identity-shape `Project` suppression).
- **ADR 024 — Scalar Arithmetic Expression IR.** Promoted +
  implemented.
  First substrate-and-surface ADR after ADR 023 that lifts a
  specific user-facing reject named in ADR 023's negative-case
  fixture set (the four arithmetic forms
  `SUM(amount) + 1` / `SUM(amount) * 1.1` /
  `SUM(x) + COUNT(*)` / `amount + tax`). Adds five IR
  variants (`Cast` + four arithmetic operators) to `Expr` and
  `ExecutionExpr`; binder-only widening Cast; no SQL `CAST(x
  AS T)` syntax exposed. Four binary operators: `+`, `-`,
  `*`, `/`. Primitive numeric domain: `Int32`, `Int64`,
  `Float32`, `Float64`; conservative-precise promotion lattice
  (pure-Int preserves width; pure-Float preserves width;
  mixed Int+Float always promotes to `Float64`). Per-lattice
  division output (truncating integer division per PostgreSQL/
  DuckDB/SQLite SQL norm). Checked integer overflow returns
  `ExecutionError::Kernel { message: "arithmetic overflow:
  {Op}({Type})" }` (reuses ADR 020 SUM precedent; no new
  variant). Integer zero divisor → NULL; integer divide
  overflow remains checked overflow; float division-by-zero →
  IEEE 754 (composes with ADR 022 sticky-NaN). Critical
  aggregate-argument shape guard
  preserves the column-only invariant
  (`"aggregate arguments must be simple column references"`)
  so `SUM(amount + tax)` continues to reject once arithmetic
  lowering exists. Wendelon PBC arithmetic AND Cast out of
  scope; `PbcCompiler` declines both explicitly; WHERE
  arithmetic evaluates as Toron residual. Computed aggregate
  arguments stay deferred (sibling ADR with derive-Eq/Hash
  refactor + `GroupedAggregateSignature` widening). 20-path
  evaluator surface (4 widening Cast paths + 16 same-type
  arithmetic kernels = 4 ops × 4 types). Seven new
  architecture guards (Expr/ExecutionExpr variant counts,
  promotion-lattice narrowness, SQL planner lowering arms,
  checked-overflow kernel-error preservation, aggregate-
  argument column-only guard, Cast binder-only widening, PBC
  decline). Three-way council convergence reached across
  four iterations; one major implementation-shape flip-flop
  (Cast IR rotated across Codex iter-0/1/2/3/4 and Claude
  iter-0/1/2/3/4) ratified 3/3 Yes by iter-4. SoA-readiness
  invariants preserved (Cast IR strengthens JIT seam shape).
  Sixth instance of the ADR 015 cycle (after 016 / 017 / 018
  / 019/022 / 023). No new `LogicalPlan` / `ExecutionPlan`
  variant; no new `ExecutionError` variant; no new
  `TaskFailureKind` variant; no new `AggregateOutputRule`
  variant; no new `StableAggregateKernel` row; no `AggState`
  change; no fragmenter detector change; no cross-ring
  engagement; no `GroupedAggregateSignature` widening. Plan
  digests for currently-accepted queries byte-identical
  (additive surface). The seven-phase migration is
  materialized in
  `plans/impl/024_scalar_arithmetic_expression_ir.md`.
- **ADR 025 — SQL `CAST(x AS T)` Syntax — Numeric Primitive
  Lattice.** Promoted + implemented.
  First substrate-downstream ADR after ADR 024's
  binder-only widening Cast IR substrate; lifts the
  `SqlExpr::Cast` reject named in ADR 024's
  `arithmetic_bind_injects_only_widening_casts` negative
  fixture. Exposes standard SQL `CAST(expr AS type)` for
  Toron's four primitive numeric types. Extends the
  conversion lattice from ADR 024's 4-cell widening to the
  full 16-cell numeric primitive matrix: 4 identity
  (diagonal) + 4 widening (ADR 024 inherited) + 1 integer
  narrowing (`Int64 → Int32`) + 1 float narrowing
  (`Float64 → Float32`) + 4 float-to-int + 2 int-to-Float32
  lossy. 8 new evaluator paths beyond ADR 024's 4. SQL type
  name normalization (`INTEGER`/`INT`/`INT4`/`INT32` →
  `Int32`; `BIGINT`/`INT64` → `Int64`; `REAL`/`FLOAT4`/
  `FLOAT32` → `Float32`; `DOUBLE`/`DOUBLE PRECISION`/
  `FLOAT8`/`FLOAT64` → `Float64`). Bare `FLOAT`/`FLOAT(n)`
  rejected (precision ambiguous across dialects);
  `SMALLINT`/`TINYINT`/`DECIMAL`/`VARCHAR`/`BOOLEAN`/
  temporal/binary/structured types rejected. `TRY_CAST`/
  `SAFE_CAST`/`::`/`FORMAT` rejected with stable per-kind
  diagnostics. Overflow / domain-violation returns
  `ExecutionError::Kernel { message: "cast failed: {Source}
  to {Target}: {cause}" }` (reuses ADR 020/022/024
  precedent; no new error variant). Float-to-int rejects
  NaN/Inf/out-of-range and truncates toward zero
  (explicit ADR 025 convention). `Float64 → Float32`
  preserves NaN/±Inf, rejects finite overflow. Critical
  binder policy split: `cast_if_needed` + `is_widening_cast`
  remain ADR 024-pristine (4-cell widening); new
  `bind_explicit_cast` + `is_supported_explicit_numeric_cast`
  cover the full 16-cell lattice. **No identity-cast
  elision in `bind_explicit_cast`** (Codex iter-2
  correctness correction adopted 3/3): preserving
  `ExecutionExpr::Cast(x, T)` for `source == target` keeps
  the PBC-ineligibility boundary visible, preventing
  user-written `CAST(int_col AS Int32)` predicates from
  becoming PBC-pushable. Identity short-circuit lives in
  `evaluate_cast` (`Ok(array.clone())`) only. **Exclusive
  upper bound for float-to-int range check** (Codex iter-2
  correctness correction): `F64_I32_UPPER = 2147483648.0`
  (i.e., `i32::MAX + 1`, exact in f64) and `F64_I64_UPPER =
  9223372036854775808.0` (i.e., `i64::MAX + 1`, exact in
  f64); strict-less-than comparator rules out the
  `i32::MAX as f32 = 2147483648.0` off-by-one trap.
  `Float32 → Int*` kernels convert source to f64 before
  range check to avoid f32 rounding traps. Manual per-pair
  kernels (3/3 ratified over Gemini iter-1's
  `arrow_cast::cast_with_options` delegation, which Gemini
  iter-2 retracted along with broader Utf8/Boolean scope).
  Aggregate-argument column-only rule preserved
  (`SUM(CAST(amount AS BIGINT))` continues to reject;
  widening to computed aggregate arguments stays deferred).
  Cast remains PBC-ineligible. Post-aggregate CAST allowed
  (composes with ADR 023's
  `Project` over `Aggregate`). Seven new architecture guards
  (SQL Cast lowering, target-type normalization,
  policy-split with no-identity-elision negative assertion,
  16-cell lattice, kernel-error prefix, syntax rejects,
  float-to-int exclusive-upper-bound). Two ADR 024 guards
  adjusted (one renamed). Seventh instance of the
  ADR 015 cycle (after 016 / 017 / 018 / 019 / 022 / 023 /
  024). Three-way council convergence reached across two
  iterations; faster cadence than ADR 023/024 attributed to
  substrate-downstream nature (ADR 024 already shipped the
  `Cast(Box<Expr>, DataType)` IR variant unchanged). Two
  correctness corrections from Codex iter-2 ratified 3/3
  (identity-elision; float-to-int boundary). One scope
  reversal recorded: Gemini iter-1 broader Utf8/Boolean
  scope + Arrow-kernel delegation retracted by Gemini
  iter-2. SoA-readiness invariants from ADR 021 preserved
  (catalog untouched; per-(source, target) kernel dispatch
  preserves JIT specialization seam from ADR 024). No new
  IR variant; no new `LogicalPlan`/`ExecutionPlan` variant;
  no new `ExecutionError` variant; no new `TaskFailureKind`
  variant; no new `AggregateOutputRule` variant; no new
  `StableAggregateKernel` row; no `AggState` change; no
  fragmenter detector change; no cross-ring engagement; no
  `GroupedAggregateSignature` widening. Plan digests for
  no-CAST queries byte-identical (additive surface). The
  five-phase migration is materialized in
  `plans/impl/025_sql_cast_numeric_primitive_lattice.md`.
- **ADR 026 — `TRY_CAST(x AS T)` / `SAFE_CAST(x AS T)` —
  Error-Tolerant Numeric Primitive Cast.** Promoted.
  First substrate-downstream ADR after ADR 025 that lifts
  both `TRY_CAST` and `SAFE_CAST` reject arms pinned by
  ADR 025's `cast_syntax_variants_are_rejected` guard.
  Exposes the SQL error-tolerant variant under the same
  16-cell numeric primitive lattice (D3 alias: both
  `CastKind::TryCast` and `CastKind::SafeCast` lower to
  the same `Expr::TryCast`). Eleven numbered decisions
  D1-D11 cover IR shape, failure-modes-that-null,
  SQL surface, preserved rejects, nullability,
  aggregate-argument shape, identity preservation, executor
  strategy, PBC ineligibility, cast-family hidden-sort-key
  gate, and inner-error propagation. Adds new
  `Expr::TryCast(Box<Expr>, DataType)` IR variant + twin
  `ExecutionExpr::TryCast` variant (D1) — pure-addition;
  Expr enum 9 → 10. Five ADR 025 data-domain failure modes
  become NULL under TryCast: integer narrowing overflow,
  float-to-int NaN/Inf/out-of-range, Float64 → Float32
  finite overflow (D2). Critical contract: "every ADR 025
  strict Cast data-domain error becomes NULL under
  TryCast; every ADR 025 strict Cast success stays
  identical." `TryCast.nullable() = true` regardless of
  inner.nullable() (D5) — key divergence from ADR 025 D7
  passthrough. Aggregate-argument column-only rule
  preserved — `SUM(TRY_CAST(amount AS BIGINT))` continues
  to reject (D6). **No bind-time identity elision** (D7,
  parallel to ADR 025 D8): `WHERE TRY_CAST(id AS INT) = 1`
  preserves `ExecutionExpr::TryCast` at bind time,
  preventing PBC-pushability. Executor strategy: 6 new
  per-pair `try_cast_*` fail-to-null kernels for fallible
  cells (`Int64→Int32`, `Float64→Float32`, four float-to-
  int paths); 6 infallible cells reuse ADR 025 strict
  kernels directly (D8). Reuses ADR 025's `F64_I32_UPPER`
  / `F64_I64_UPPER` exclusive-upper-bound constants and
  truncation-toward-zero semantic. **Do not implement
  TryCast by wrapping `evaluate_cast` with catch-and-null**
  — would (a) stop at first failure instead of per-row
  NULLs, (b) swallow internal executor bugs, (c) violate
  D11. PBC remains ineligible with widened diagnostic
  `"arithmetic, Cast, and TryCast are not PBC-eligible"`
  (D9). **D10: cast-family hidden-sort-key gate
  extension** — ADR 025's implementation added the
  hidden-projection mechanism (`expr_contains_cast` +
  `sort_needs_hidden_projection` + helpers) for
  `ORDER BY CAST(SUM(amount) AS Float32) DESC`; ADR 026
  renames `expr_contains_cast` to
  `expr_contains_cast_family` and widens the match to
  `Cast | TryCast` (narrow extension; the broader "ORDER
  BY computed expressions" cycle remains future work).
  The `adr026_try_cast_order_by_uses_hidden_sort_projection`
  guard extends ADR 025's Cast-only source-read coverage
  for the hidden-projection mechanism to the cast-family
  case; broader computed-expression coverage remains for
  a dedicated future ADR.
  **D11: inner strict Cast errors propagate, NOT
  swallowed by outer TryCast** (Codex iter-1 critical
  clarification promoted to numbered decision at
  iter-2): `TRY_CAST(CAST(big_amount AS INT) AS BIGINT)`
  with overflowing inner Cast fails with the inner
  Cast's `ExecutionError::Kernel`; outer TryCast does
  NOT null this. Structurally enforced via natural `?`
  operator dispatch in evaluator. Spelling-neutral
  binder diagnostic `"unsupported TRY_CAST/SAFE_CAST: ..."`
  (binder no longer knows which spelling once both kinds
  collapse to `Expr::TryCast`). Eighth instance of the
  ADR 015 cycle (after 016 / 017 / 018 / 019 / 022 /
  023 / 024 / 025). Three-way council convergence
  reached across three iterations; substantive shape at
  iter-2 on D1-D10, D11 promoted from Codex iter-1
  clarification, iter-3 closed three implementation-
  precision items (logical-IR `is_pbc_pushdown_eligible`
  arm, binder ancillary helpers `collect_expr_columns` +
  `rewrite_expr_ordinals`, hidden-projection guard
  naming as a new extension guard, not a rename). One implementation-shape
  dissent (Gemini iter-1/2 `null_on_failure: bool`
  flag-on-strict-kernel) retracted at iter-3 in favor of
  separate `try_cast_*` kernels; pattern matches ADR 025
  Dissent B retraction. Confirms substrate-downstream
  cadence hypothesis (2-3 iterations; ADR 025 took 2,
  ADR 026 took 3). Third consecutive ADR cycle where a
  Codex iter-1/2 correctness argument surfaces a load-
  bearing semantic boundary at design time (ADR 024:
  aggregate-argument shape guard; ADR 025: identity-Cast
  PBC boundary + float-to-int exclusive bound; ADR 026:
  inner-Cast-error propagation). SoA-readiness invariants
  preserved (catalog untouched; per-(source, target)
  TryCast kernel dispatch preserves JIT specialization
  seam from ADR 025). Seven new architecture guards
  (TryCast IR surface, planner lowering, binder policy,
  numeric primitive lattice, null-only-data-domain-
  failures, PBC declines, hidden-sort-key gate). Two
  existing guards adjusted (one renamed). Three new
  integration tests (`pbc_declines_try_cast_filter`,
  `pbc_declines_identity_try_cast_filter`,
  `order_by_try_cast_aggregate_uses_hidden_projection`).
  No new `LogicalPlan` / `ExecutionPlan` variant; no
  new `ExecutionError` variant; no new `TaskFailureKind`
  variant; no new `AggregateOutputRule` variant; no new
  `StableAggregateKernel` row; no `AggState` change; no
  fragmenter detector change; no cross-ring engagement;
  no `GroupedAggregateSignature` widening. Plan digests
  for no-TryCast queries byte-identical (additive
  surface). The six-phase migration is materialized in
  `plans/impl/026_try_cast_numeric_primitive_lattice.md`.
- **ADR 027 — `ORDER BY` Computed Expressions over
  Aggregate Output.** Promoted. Third consecutive
  substrate-downstream ADR (ADR 025 / 026 / 027) and
  the cycle that ratifies the hidden-projection mechanism
  originally shipped by ADR 025 (and narrowly extended by
  ADR 026 D10) as the general path for non-direct computed
  `ORDER BY`
  keys on aggregate queries. Ten numbered decisions
  D1-D10 cover gate generalization, visible-output
  validation, preserved rejects, sort-key ordinal-only
  contract, hidden-projection contract, alias
  visibility, expression surface, literal-only keys,
  and non-aggregate scope. **D1 generalizes the gate**
  to `!sort_key_is_direct_output_reference(&key.expr)`
  and **removes** `expr_contains_cast_family`
  outright (recursive inspection moves into the D2
  validator). Direct output columns (`ORDER BY region`)
  and ordinals (`ORDER BY 1`) continue to bypass
  hidden projection; plan digests for those queries
  remain byte-identical. **D2 adds
  `validate_hidden_sort_key_refs`** — an exhaustive
  `Expr` walker that rejects qualified column
  references and non-visible-output column references
  at design time with the stable diagnostic
  `"ORDER BY computed aggregate expression must
  reference visible output columns"`. Prevents raw
  input columns from leaking into the post-aggregate
  projection (Codex iter-1 correctness clarification
  promoted to numbered decision at iter-2). D3-D7
  preserve ADR 023 hidden-aggregate-key reject,
  ADR 024 D6 computed-aggregate-argument reject,
  ordinal-only `ExecutionSortKey`, hidden-projection
  contract (`__toron_order_key_{idx}` naming +
  collision suffixing + duplicate-name reject + final-
  projection stripping), and top-level ORDER BY alias
  visibility (HAVING alias reject from ADR 023
  unchanged). D8 pins the expression surface as the
  currently-lowered `Expr` (Cast, TryCast, arithmetic,
  comparisons, boolean, null predicates, Not,
  TimeBucket); future expression families inherit
  ADR 027 acceptance automatically once they lower.
  D9 accepts literal-only computed sort keys
  (`ORDER BY 1 + 1`); D10 keeps non-aggregate computed
  `ORDER BY` out of scope (`bind_sort_key_ordinal`
  catch-all preserved). The reject-removal property
  of the ADR 015 cycle holds: the catch-all stops
  rejecting computed aggregate ORDER BY keys but adds
  new raw-input-column and qualified-column rejects
  at the D2 validator. Ninth instance of the ADR 015
  cycle (after 016 / 017 / 018 / 019 / 022 / 023 /
  024 / 025 / 026). Three-way council convergence
  reached across three iterations (iter-0 topic
  split, iter-1 topic concession, iter-2 substantive
  convergence on D1-D10, iter-3 cosmetic alignment
  with **zero precision items** — tighter convergence
  than ADR 026's three iter-3 items). Iter-0 topic
  split (Claude: computed-aggregate-args substrate
  vs Codex + Gemini: ORDER BY ratification) resolved
  2-of-3 in favor of this ratification; the deferred
  substrate is carried forward as the ADR 028
  candidate (matches ADR 020→021 split pattern).
  Fourth consecutive cycle where a Codex iter-1/2
  correctness argument surfaces a load-bearing safety
  boundary at design time (ADR 024: aggregate-arg
  shape guard; ADR 025: identity-Cast PBC + float-to-
  int exclusive bound; ADR 026: inner-Cast-error
  propagation; **ADR 027: visible-output validator**).
  Substrate-downstream cadence holds across three
  consecutive cycles (ADR 025: 2 iters; ADR 026: 3
  iters; ADR 027: 3 iters). Three new architecture
  guards (gate-generalized, visible-outputs validator,
  contract-pinned) + two existing ADR 025 / 026
  hidden-sort-key guards adjusted (neither renamed,
  neither deleted). SoA-readiness invariants from
  ADR 021 preserved (catalog untouched; hidden-key
  columns evaluate via the row-layer expression
  evaluator already shipped by ADR 024 / 025 / 026).
  No new IR variant; no new operator; no new kernel;
  no fragmenter detector change; no Eq/Hash refactor;
  no `GroupedAggregateSignature` widening; no
  cross-ring engagement; no new `ExecutionError` /
  `TaskFailureKind` variant; no new `LogicalPlan` /
  `ExecutionPlan` variant. Plan digests for direct
  `ORDER BY` queries byte-identical pre/post-ADR-027
  (gate bypass for `Column` / `Literal` keys). The
  seven-step migration is materialized in
  `plans/impl/027_order_by_computed_expressions.md`.
- **ADR 028 — Computed Aggregate Arguments
  (`SUM(x + y)` / `SUM(TRY_CAST(amount AS BIGINT))`).**
  Promoted. **First substrate-introducing ADR since
  ADR 024** after three substrate-downstream cycles
  (025 / 026 / 027). Lifts the longest-standing
  evidence-pressure reject in Toron: the ADR 024 D6
  rule that `"aggregate arguments must be simple
  column references"`, preserved verbatim by ADR 025 /
  026 / 027. The stable aggregate kernel family
  (`CountAll` plus one-arg `Sum` / `Count` / `Avg` /
  `Min` / `Max`) accepts the lowered `Expr` surface as
  arguments: `SELECT SUM(amount + tax) FROM sales`,
  `SELECT region, SUM(TRY_CAST(amount AS BIGINT)) FROM
  sales GROUP BY region`, `SELECT AVG(price * 1.1)
  FROM sales`. Eleven decisions D1-D11 cover column-
  only reject removal, visitor-based planner-private
  signature key, binder-owned type-domain validation,
  post-lowering structural equality, nested-aggregate
  SQL-walk-time reject, `COUNT(*)` zero-arg signature
  with logical placeholder preserved, kernel-name-only
  output names, row-layer executor seam reuse, null/
  error/PBC inheritance through composition, plan-
  digest backward compat for column-only-arg queries,
  qualified-reference policy unchanged. **D2 critical
  decision (visitor-based, no parallel key types):**
  `GroupedAggregateSignature` becomes a struct
  `{ kernel: StableAggregateKernel, args: Vec<Expr> }`
  with planner-private custom `PartialEq` + `Eq` +
  `Hash` impls implemented via six visitor helpers
  over the existing `Expr` tree. Public
  `AggregateFunction` / `Expr` / `ScalarValue` remain
  non-`Eq` / non-`Hash` — visitors are standalone
  helpers, NOT trait impls. `StableAggregateKernel`
  (already `Eq + Hash`) keys function side,
  sidestepping the `AggregateFunction::ApproxQuantile(f64)`
  float-derive blocker and type-segregating
  experimental kernels (`stable_kernel_for_function`
  returns `None` for `ApproxQuantile` / `LinearRegress`).
  Float bit-pattern handling encapsulated planner-
  private via `Option::map(f32::to_bits)` /
  `Option::map(f64::to_bits)`. Zero parallel-IR
  allocation. Five iterations total with two flip-
  flops: D2 P1 vs P2 (Gemini iter-0/1/2 advocated
  public manual `Eq + Hash`; retracted iter-3 to
  planner-private — third consecutive cycle of
  Gemini's "wider trait surface" retraction matching
  ADR 025 / 026 patterns), and **iter-4 user-
  evidence-pressured D2 realization flip-flop**: iter-3
  ratified D2 P1 with a materialized `AggregateArgKey`
  tree (Box-everywhere); iter-4 refined to the
  visitor approach after user evidence flagged the
  "Box everything" concern at merge staging. First
  user-evidence-pressured substrate refinement at
  staging in the cycle's history; scoped re-open of
  D2's implementation shape preserved D1, D3-D11 from
  iter-3. Iter-5 folded four Codex precision items
  (P1-P4) including a load-bearing correctness fix on
  `data_type_structural_eq` reflexivity (P3 —
  `matches!` with arm restrictions would violate `Eq`
  contract for hand-built non-lattice `DataType`
  values; body must be `left == right`). Fifth
  consecutive cycle of Codex correctness-clarification
  pattern (iter-0 + iter-3 + iter-5 surfacing — most
  distributed in the cycle's history). Tenth instance
  of the ADR 015 cycle (after 016 / 017 / 018 / 019 /
  022 / 023 / 024 / 025 / 026 / 027). Three-way
  council convergence reached across five iterations.
  Substrate-introducing cadence hypothesis confirmed
  (4+ iters with at least one flip-flop); ADR 028
  came in at 5 iters with 2 flip-flops, stretched
  by one user-evidence-pressured iteration but
  produced a strictly-better realization. Nested
  aggregate calls reject at SQL-walk time with
  stable diagnostic `"aggregate arguments must not
  contain aggregate calls"` (D5). Binder retains
  per-kernel type-domain validation authority (D3 —
  `SUM(amount > 0)` rejects as Boolean not
  SumNumeric; `AVG(TRY_CAST(x AS INT))` OK).
  Structural equality is post-lowering (D4 — no
  algebraic normalization; `SUM(amount + tax) ≠
  SUM(tax + amount)`). `COUNT(*)` produces a zero-
  arg signature `{ kernel: CountAll, args: Vec::new() }`
  ignoring input args; **`lower_count_aggregate`'s
  synthetic `Expr::column("count")` placeholder for
  `COUNT(*)` is preserved verbatim** in the
  `LogicalPlan` shape (D6 + iter-3 precision) so that
  column-only-arg plan digests remain byte-identical
  (D10). Output names stay kernel-name strings
  (`sum` / `count` / `min` / `max` / `avg`); no
  HAVING alias visibility introduced (D7 — ADR 023
  reject preserved). Row-layer executor seam reused
  verbatim — existing `evaluate_expr` /
  `evaluate_bound_expr` already handle `Vec<Expr>` /
  `Vec<ExecutionExpr>` aggregate arguments per-batch
  before kernel accumulation (D8 — no new operator,
  no hidden pre-aggregate `Project`). Computed-arg
  aggregates inherit ADR 022 sticky-NaN / ADR 024
  arithmetic loud-failure / ADR 024 D8 div/0 null /
  ADR 025 strict Cast `cast_failed` / ADR 026
  TryCast fail-to-null + D11 inner-Cast-error
  propagation; remain PBC-residual via Cast / TryCast
  / arithmetic ineligibility composition (D9).
  Qualified-reference policy preserved verbatim
  (D11). Eleven new ADR 028 architecture guards (column-
  only diagnostic removal, planner-private visitor
  surface, exhaustive visitor coverage, float-bit
  preservation, `data_type_structural_eq` reflexivity
  correctness boundary from P3, `CountAll` zero-arg
  signature + placeholder preservation, nested-
  aggregate SQL-walk reject, experimental-kernel
  exclusion, kernel-name-only output, binder/executor
  seam preservation, public IR non-`Eq` / non-`Hash`).
  Two existing guards adjusted (ADR 024 D6 column-
  only-rule + ADR 020 AVG signature shape — Codex
  caught the AVG adjustment at iter-3). SoA-readiness
  invariants preserved (catalog untouched; no row-
  path dyn dispatch; per-kernel dispatch operates on
  already-bound `ArrayRef`s). **Brainstorm 004
  (Memory Arenas) strengthens as a future ADR
  candidate** — multiple cycles (ADR 023 / 024 /
  025 / 026 / 028 iter-3 → iter-4) have now surfaced
  the Box-everywhere expression-IR pattern; the
  cumulative pressure is well-prepared for an arena-
  introducing cycle when workload evidence triggers
  it. No new `LogicalPlan` / `ExecutionPlan` variant;
  no new `ExecutionError` / `TaskFailureKind`
  variant; no new `AggregateOutputRule` variant; no
  new `StableAggregateKernel` row; no `AggState`
  shape change; no fragmenter detector change; no
  cross-ring engagement; no PBC opcode change. Plan
  digests for column-only-argument queries byte-
  identical pre/post-ADR-028. The seven-phase
  migration is materialized in
  `plans/impl/028_computed_aggregate_arguments.md`.
- **ADR 029 — SQL `CREATE TABLE` (Narrow DDL Surface).**
  Promoted. **First DDL ADR in Toron** — widens the
  statement layer from SELECT-only to also accept the
  narrow `CREATE TABLE [IF NOT EXISTS] [schema.]table_name
  (column TYPE [NULL | NOT NULL], ...)` shape after five
  consecutive expression-IR cycles (ADR 024 / 025 / 026 /
  027 / 028) without touching the DDL surface. User-
  evidence framing surfaced the priority gap: "we should
  be able to create tables before going super deep into
  approximate quantiles." Fifteen decisions D1-D15
  cover narrow SQL shape with explicit out-of-scope set;
  statement planner widens with exhaustive
  `sqlparser::ast::CreateTable` destructure (no `..`)
  guarding against silent acceptance on dependency
  upgrades; `LogicalPlan::CreateTable` + `ExecutionPlan::CreateTable`
  leaf variants carrying `arrow::datatypes::SchemaRef`;
  **DDL is coordinator-only** (`QueryCoordinator` +
  `DistributedQueryCoordinator` both in
  `crates/toron/src/coordinator.rs` intercept BEFORE
  their respective downstream stages; `Executor` +
  `Fragmenter` defensive rejects); `Catalog::create_table`
  returns `Result<TableBinding, CatalogError>`;
  `MockCatalog` becomes `RwLock<MockCatalogInner>`
  interior-mutable; **`mock_table_identity` helper**
  derives `PhysicalDescriptorFingerprint` through
  `TableSchemaDescriptor::new_with_layout` because
  `from_bytes` is crate-private to Wendelon; `IF NOT
  EXISTS` uses strict Arrow `Schema` equality; **DDL-
  specific name + type lowering helpers** (separate from
  read-side `table_ref_from_object_name` and ADR 025's
  `lower_cast_target_type`); **MockCatalog reference
  insertion uses `entry(...).or_insert(...)` for the
  unqualified-alias path** (no silent overwrite); 6-type
  column lattice (`Int32` / `Int64` / `Float32` /
  `Float64` / `Utf8` / `Boolean`); narrow column options
  (`NULL` / `NOT NULL` only); **storage synthetic column
  name reject at planner time** (`__row_hlc__` /
  `__row_key_id__` / `__row_routing_locality__` /
  `__row_table_object_id__` / `__row_tombstone__`); DDL
  success returns `Ok(Vec::new())` from coordinators;
  **`SELECT * FROM newly_created_table` NOT promised**
  by ADR 029 (catalog visibility only; readback awaits
  ADR 030 = INSERT); existing SELECT plan digests
  byte-identical (SELECT bind stays inside descriptor-
  evolution retry loop). **`TableId` is tuple-struct
  `TableId(pub u64)`** (use tuple construction, not
  `TableId::new`); **`TableRef` has no `Display`** (use
  crate-private `table_name(&TableRef)` helper for
  diagnostics). `WendelonCatalog::create_table` returns
  `CatalogError::AdminUnavailable` until a cross-ring
  metadata-write ADR engages. Four iterations with
  **zero substantive flip-flops** on D1-D15 after iter-2
  council convergence (iter-0 topic split BoundExprProgram
  vs CREATE TABLE resolved cleanly at iter-1 via
  sequencing commitment; iter-3 was pure code-evidence
  precision-folding). Narrowest substrate-introducing
  flip-flop profile to date (ADR 023: 1; ADR 024: multi;
  ADR 028: 2). **Sixth consecutive cycle of Codex
  correctness-clarification pattern** — six precision
  items across iter-2 and iter-3 (coordinator/catalog
  ownership, MockCatalog interior mutability, DDL-
  specific helpers, descriptor-derived fingerprint,
  alias-no-overwrite, storage-synthetic-name reject),
  most code-evidence-grounded surfacing of any cycle.
  **Fourth consecutive cycle of Gemini "wider trait
  surface" iter-1 → iter-2 retraction pattern** (D4 /
  D5 / D9 / D14 explicitly labeled "I retract my
  Iteration 1 design" at iter-2). **Third consecutive
  topic-concession-compresses-next-cycle pattern** (ADR
  020 → 021; ADR 027 → 028; ADR 029 → 031 via Codex
  iter-0 `BoundExprProgram` doc seeding the ADR 031
  cycle). **New process pattern recorded** — pre-
  existing-Wendelon-types ADRs need a code-evidence
  pass on the Wendelon-side API surface, not just
  Toron-side; ADR 030 (INSERT) and the eventual cross-
  ring AGG consumer wiring ADR will share this
  characteristic. Eleventh instance of the ADR 015
  cycle. Three-way council convergence reached across
  four iterations. Substrate-introducing cadence
  hypothesis confirmed (4+ iters with at least one
  flip-flop tolerance; ADR 029 came in at 4 iters with
  0 flip-flops via topic concession). Fifteen new ADR
  029 architecture guards + one adjusted statement-
  catch-all guard. SoA-readiness invariants preserved
  (catalog stays pure metadata for kernels; no row-path
  dyn dispatch; `#[repr(u8)]` on `StableAggregateKernel`
  unchanged). **Sequencing commitment:** ADR 030 =
  `INSERT INTO ... VALUES (...)` (paired with this cycle
  — CREATE TABLE alone is unusable without INSERT);
  ADR 031 = `BoundExprProgram` (Brainstorm 004's narrow
  realization from Codex iter-0). No new IR expression
  variant; no new aggregate kernel; no fragmenter
  detector change for SELECT paths; no PBC opcode
  change; no cross-ring engagement; no Wendelon
  persistent descriptor writes. Plan digests for
  existing SELECT query corpus byte-identical pre/post-
  ADR-029. The nine-phase migration is materialized in
  `plans/impl/029_create_table_ddl.md`.
- **ADR 030 — SQL `INSERT INTO ... VALUES (...)`
  (Literal-Only DML Surface).** Promoted. **First DML
  ADR in Toron** — paired companion to ADR 029, lifting
  D14: `CREATE TABLE; INSERT; SELECT` becomes testable
  end-to-end. Accepted SQL is narrow:
  `INSERT INTO [schema.]table [(col, ...)] VALUES (literal, ...), ...`
  with no `INSERT ... SELECT`, no `RETURNING`, no
  `ON CONFLICT`, no defaults, no distributed execution.
  Sixteen decisions D1-D16. **D5 critical decision:**
  new `StorageWrite` trait beside `StorageScan` —
  `Catalog` stays metadata-only (D6 + D14); data-plane
  writes do NOT extend `Catalog`. **D4 critical
  decision:** `ExecutionPlan::Insert` carries
  `Vec<Vec<ScalarValue>>`, NOT `RecordBatch` —
  preserves serde derivation; coordinator write helper
  materializes the Arrow batch from `(schema, rows)`
  immediately before `StorageWrite::write`. **D7
  critical decision:** Wendelon synthetic columns stay
  server-owned — Toron MUST NOT construct the five
  `__row_*__` columns and MUST NOT import `blake3`;
  architecture guards source-check
  `coordinator/write.rs` for absence of `STORAGE_ROW_*`
  references and `crates/toron/Cargo.toml` for absence
  of `blake3` declaration. **D8 critical decision:**
  `WendelonStorageWrite` uses the high-level
  `wendelon_client::Client::table_write(tenant_id, descriptor, payload, options)`
  wrapper, NOT the low-level
  `wendelon::client::WendelonClient::table_write(TableWriteRequest)`
  — the wrapper handles `parent_write_id`,
  `client_hlc`, `expected_epoch`, `schema_generation`,
  row-count encoding, etc.; architecture guard asserts
  no `TableWriteRequest {` construction in
  `crates/toron/src/`. **D9 critical decision:**
  `WriteOptions { await_finalization: true, ..Default::default() }`
  overrides the wrapper default — SQL `INSERT` returns
  only after Wendelon commit confirmation so the user-
  evidence payoff fixture has read-after-write
  semantics. **D11 critical decision:**
  `InMemoryTableStorage` is additive
  (`Mutex<HashMap<TableId, InMemoryTableData>>` —
  multi-table); existing `InMemoryStorageScan`
  untouched. **D12 critical decision:** local
  `QueryCoordinator` intercepts INSERT **BEFORE** the
  SELECT descriptor-evolution retry loop — INSERT does
  NOT retry on descriptor evolution in v1. **D13
  critical decision:** distributed INSERT returns a
  stable unsupported diagnostic **BEFORE**
  `fragment_plan` — mirrors ADR 029's
  `WendelonCatalog::create_table → AdminUnavailable`
  pattern. **D15 critical decision:**
  `TABLE_WRITE_MAX_ROWS = 65_536` bind-time row cap,
  matching Wendelon's constant. **D16 critical
  decision:** materialization in new
  `coordinator/write.rs` private submodule —
  coordinator write routing already has the
  `ExecutionContext` needed to build `WriteRequest`;
  storage stays shape-neutral. **`InsertLiteral` IR carrier**
  (`Null` / `Boolean(bool)` / `Int64(i64)` / `UInt64(u64)` /
  `Float64(f64)` / `Utf8(String)`) — does NOT reuse
  `Expr` for VALUES (parallel anti-delegation
  discipline to ADR 029 D11). DML-specific
  `lower_insert` / `lower_insert_target_name` /
  `lower_insert_literal` helpers — do NOT delegate to
  SELECT-side lowering. Binder owns target-schema-aware
  coercion via `coerce_insert_literal` over ADR 029's
  six-type lattice; typed-NULL fill for omitted
  nullable columns; reject omitted NOT NULL; row cap +
  empty-VALUES rejects. Four iterations with **one
  substantive flip-flop** (Codex iter-1 self-correction:
  `RecordBatch` → `Vec<Vec<ScalarValue>>` in
  `ExecutionPlan::Insert` to preserve serde derivation)
  and **two iter-2 retractions from Claude** (C1
  wrapper crate path — iter-1 read the low-level
  `WendelonClient` source rather than the
  `wendelon-client` wrapper Toron actually imports; C4
  `From<wendelon_client::ClientError> for StorageError`
  already exists at `error.rs:48`). Narrowest flip-flop
  profile post-ADR-029. **Fifth consecutive cycle of
  Gemini iter-1/2 retraction streak** (ADR 025 / 026 /
  028 / 029 / 030) — Gemini iter-3 explicitly retracted
  its iter-2 D5 (`Catalog::insert_rows`), D6 (Toron-
  side `__row_*__` construction), and D7 (BLAKE3 row
  identity derivation). **Second consecutive pre-
  existing-Wendelon-types cycle** (after ADR 029).
  **New cycle property recorded** — cross-ring ADRs
  need **two-pass Wendelon-side verification**: at
  least one iteration surface-reading the relevant
  API + at least one peer iter to catch wrong-file or
  wrong-import errors (ADR 029: iter-3 surfaced
  `PhysicalDescriptorFingerprint::from_bytes` is
  crate-private; ADR 030: Claude iter-2 retracted C1/C4
  wrong-source-file). Twelfth instance of the ADR 015
  cycle. Three-way council convergence reached across
  four iterations. Twelve new ADR 030 architecture
  guards + two adjusted ADR 029 guards (statement
  catch-all widens to `"only SELECT, CREATE TABLE, and
  INSERT are supported"`; variant counts 10→11 / 8→9).
  SoA-readiness invariants preserved.
  `WendelonStorageWrite` error mapping mirrors
  `WendelonStorageScan` (TableNotFound for missing
  descriptor, DescriptorEvolved for identity mismatch,
  Internal for Arrow/schema failure). Distributed
  INSERT explicitly unsupported in v1 (D13); future
  cross-ring distributed-write-context ADR lifts.
  Read-your-writes session HLC advancement deferred
  (D12 + Codex iter-3 residual Q3); `WriteOutcome`
  discards `commit_hlc`. Per-statement durability
  override deferred (uses wrapper default
  `DurabilityClass::QUORUM`). No new IR expression
  variant; no new aggregate kernel; no fragmenter
  detector change for SELECT paths; no PBC opcode
  change; no cross-ring metadata-write engagement; no
  Toron-side Wendelon row-identity construction; no
  `blake3` dependency; no `RecordBatch` inside any
  `ExecutionPlan` variant. Existing SELECT corpus plan
  digests byte-identical pre/post-ADR-030. The ten-
  phase migration is materialized in
  `plans/impl/030_insert_into_values.md`.
- **ADR 031 — Bound Expression Program (Index-Based
  Physical Expression IR).** Promoted. **First
  substrate-replacing pure-Toron ADR** — replaces
  recursive `Box<ExecutionExpr>` trees in
  `ExecutionPlan` physical-expression fields with a
  serializable index-based `BoundExprProgram` that
  owns its roots, enforces topological ordering by
  construction, and evaluates via a flat linear loop.
  Narrow realization of Brainstorm 004's expression-
  representation split; logical `Expr` remains the
  boxed-readable SQL planner contract. Fifteen
  decisions D1-D15. **D2 critical decision:**
  `BoundExprProgram { nodes, node_types, literals,
  roots }` is self-contained — plan variants carry
  complete programs, not `program + root` side-field
  pairs; arity is enforced by context (predicate =
  one root; project / exchange / group-key = multi-
  root; `COUNT(*)` = zero-root). **D5 critical
  decision:** compact grouped operation enums
  (`BoundBinaryOp` with 12 variants, `BoundUnaryOp`
  with 3); `BoundExprNode` has 7 variants (vs 18+
  per-op in the old `ExecutionExpr`) — enum-size
  discipline + carrier symmetry + future typed-
  kernel dispatch alignment. **D6 critical decision:**
  `node_types[id]` is authoritative for execution
  dispatch; `ResolvedColumn` stays in
  `BoundExprNode::Column` for v1; duplication with
  `ResolvedColumn.data_type` accepted (future cleanup
  ADR may thin). **D7 critical decision:**
  `BoundExprProgram` / `BoundExprNode` /
  `BoundAggregateCall` derive `PartialEq` only — no
  `Eq` / `Hash` because `literals` includes float-
  bearing `ScalarValue`; preserves ADR 028 D2
  discipline. **D9 critical decision:** aggregate
  role boundaries stay separate — `Aggregate.group_expr`
  is one `BoundExprProgram`; `Aggregate.aggr_expr`
  is `Vec<BoundAggregateCall>` with one `args`
  program per call. **D11 critical decision:**
  binder emits programs directly via
  `BoundExprProgramBuilder`; no production
  `ExecutionExpr → BoundExprProgram` lowering pass
  survives migration. **D12 critical decision:** PBC
  migration in scope; `PbcCompiler::compile` reads
  `Option<&BoundExprProgram>` with runtime-asserted
  single-root predicate contract (no
  `SingleRootBoundExprProgram` newtype); PBC
  semantics preserved verbatim (column / non-null
  literal / comparison / boolean / null-check
  eligible; `Cast` / `TryCast` / arithmetic /
  `TimeBucket` declined). **D13 critical decision:**
  narrow column-key helpers (`single_column_root`,
  `root_columns`, `root_type`, `is_empty_roots`)
  keep runtime-filter / scheduler / fragmenter paths
  column-oriented — no broad program traversal added.
  **D14 critical decision and first cycle to
  intentionally break a previously-promoted
  invariant:** ADR 023 D15 + ADR 029 D15 + ADR 030
  D17 promised "SELECT plan-digest byte-identical
  pre/post-ADR-NNN". ADR 031 D14 explicitly breaks
  this — `ExecutionExpr → BoundExprProgram` changes
  serialized `ExecutionPlan` bytes by design. The
  break is a substrate-replacing exception, not a
  regression. Compatibility shifts to result-digest
  parity for the SELECT corpus + PBC engaged/declined
  behavior + distributed determinism + wire serde
  round-trips within one code version. **Mixed-
  version distributed task frames are explicitly not
  a target.** **D15 critical decision:** ADR 028's
  `GroupedAggregateSignature` substrate stays
  planner-private over logical `Vec<Expr>`; the six
  ADR 028 structural visitor helpers
  (`hash_expr_structural` / `expr_structural_eq` /
  `hash_scalar_structural` / `scalar_structural_eq`
  / `hash_data_type_structural` /
  `data_type_structural_eq`) are not migrated.
  Three substantive iterations + one ratification
  pass = **4 total with 0 flip-flops** — fastest
  convergence in the council's history. **Sixth
  consecutive cycle of Gemini iter-1/2 retraction**
  (ADR 025 / 026 / 028 / 029 / 030 / 031): Gemini
  iter-2/3 retracted iter-1 positions on roots-as-
  side-fields, PBC out of scope, and `Eq` on
  `BoundExprNode`. **Four Claude iter-0/1
  contributions promoted directly to D-level**
  (C1→D14 plan-digest break; C2→D15 ADR 028
  untouched; C5→D12 PBC single-root contract;
  C6→D5 compact grouped enum); first cycle where
  iter-0/1 contributions reach promotion without
  iter-2/3 wordsmithing. **Third instance of the
  topic-concession-compresses-next-cycle pattern**
  (ADR 020→021; ADR 027→028; ADR 029→031 via Codex's
  iter-0 ADR 029 `BoundExprProgram` seed doc).
  Thirteenth instance of the ADR 015 cycle. Ten new
  ADR 031 architecture guards + six adjusted ADR
  024/025/026/028 guards (anchored at
  `architecture_test.rs:558, 914, 994-1286, 1260-1500,
  1838`; no guard silently deleted) + two verify-
  unchanged guards (kernel catalog at line 665, ADR
  028 signature at line 1611) = 18 architecture
  guard touchpoints. SoA-readiness invariants
  preserved (catalog metadata-only; `#[repr(u8)]`
  `StableAggregateKernel` unchanged; centralized
  dispatch unchanged; no row-path dyn dispatch).
  No new SQL syntax; no new expression operations;
  no new aggregate kernel; no PBC opcode change; no
  fragmenter detector change for SELECT paths; no
  cross-ring engagement; no Wendelon-side change;
  no `blake3` / `bumpalo` / `typed-arena` / `slab` /
  `generational-arena` dependency added; no `Eq` /
  `Hash` derives on program-bearing types. ADR 029
  catalog metadata-only preserved; ADR 030
  `ExecutionPlan::Insert` expression-free preserved.
  SQL result digests byte-identical pre/post-ADR-
  031 for the SELECT corpus (plan-digest fixtures
  categorized and adjusted per Phase 9 inventory). The
  ten-phase migration is materialized in
  `plans/impl/031_bound_expr_program.md`.
- **ADR 032 — SQL `INSERT INTO ... SELECT` (Local Read-
  Write Composition).** Promoted. **First composition
  ADR** after the ADR 024 → ADR 031 substrate arc
  (eight consecutive substrate-introducing / substrate-
  downstream / substrate-replacing cycles). Adds local
  `INSERT INTO ... SELECT` by composing ADR 029 catalog
  identity, ADR 030 `StorageWrite`, ADR 031
  `BoundExprProgram` inside the bound source SELECT,
  ADR 025 strict cast kernels for array coercion, and
  the existing SELECT execution pipeline — without
  widening the VALUES insert path, without distributed
  write execution, and without new expression or storage
  APIs. Thirteen decisions D1-D13.
  **D1 critical decision:** local-coordinator-only;
  `QueryCoordinator::execute_query` intercepts
  `LogicalPlan::InsertSelect` before the SELECT
  descriptor-evolution retry loop; distributed
  coordinator rejects `ExecutionPlan::InsertSelect`
  before `fragment_plan` with stable diagnostic
  `"distributed INSERT INTO ... SELECT execution is not
  wired to a write-context-bearing coordinator"`;
  fragmenter rejects before `contains_join` /
  `count_exchanges` via new
  `FragmentError::InsertSelectNotFragmentable`;
  defensive executor reject for uncoordinated reach.
  **D2 critical decision (Shape A):** separate IR
  variants — new `LogicalPlan::InsertSelect { table,
  columns, source: Box<LogicalPlan> }` and new
  `ExecutionPlan::InsertSelect { legacy_table_id, table,
  fingerprint, schema, target_columns:
  Vec<ColumnOrdinal>, source: Box<ExecutionPlan> }`;
  existing VALUES `Insert` variants stay literal-only
  (ADR 030 D4 + ADR 031 D10 carry-forwards).
  `LogicalPlan` 11 → 12 variants; `ExecutionPlan`
  9 → 10 variants.
  **D3 critical decision (positional mapping):**
  `target_columns: Vec<ColumnOrdinal>` records destination
  ordinals in source-output order; binder resolves the
  optional target column list to ordinals; coordinator
  never re-parses the target column list; source
  output names are not compared to target column names
  (positional after column-list resolution).
  **D4 critical decision (binder validation):**
  shared `resolve_insert_target_columns(schema, columns)`
  helper extracted from `bind_insert` and reused by
  new `bind_insert_select`; duplicate-name +
  unknown-column + omitted-not-null rejects bind-time;
  source-output-arity vs `target_columns.len()` reject
  bind-time; source-type-to-target-type admissibility
  reject bind-time via `is_insert_select_coercible`
  helper (ADR 025 strict-cast numeric lattice plus
  identity for Boolean / Utf8); nullable-source-to-
  non-null-target NOT bind-time rejected (runtime
  check is D9).
  **D5 critical decision (materialize before write):**
  coordinator executes bound source once via
  `Executor::execute_bound(source, None)`, materializes
  all batches, coerces / reorders through new
  `materialize_insert_select_batch(target_schema,
  target_columns, source_batches)` private helper in
  `coordinator/write.rs`, calls `StorageWrite::write`
  exactly once for non-empty result; returns
  `Ok(Vec::new())` (no row count, no RETURNING).
  **D6 critical decision:**
  `TABLE_WRITE_MAX_ROWS = 65_536` applies to total
  source row count (not per-batch); over-cap fails
  before write with `ExecutionError::InvalidExpression`;
  multi-batch / chunked writes out of scope; single
  `RecordBatch` payload preserves `StorageWrite` trait
  shape.
  **D7 critical decision (empty-source no-op):** zero-
  row source returns `Ok(Vec::new())` without calling
  `StorageWrite::write`; helper returns `Ok(None)`;
  intentionally does not solve empty-table storage
  initialization.
  **D8 critical decision (strict array coercion):**
  numeric source arrays coerced via existing ADR 025
  strict cast kernels through `crate::exec::evaluate_cast`
  entry point; Boolean / Utf8 require identity type;
  unsupported combinations fail before write;
  `TRY_CAST` semantics explicitly rejected (numeric
  overflow fails the statement rather than nulling
  cells; users who want fail-to-null spell `TRY_CAST`
  inside the source SELECT); supported target-type
  lattice is ADR 030's six-type set (`Boolean`,
  `Int32`, `Int64`, `Float32`, `Float64`, `Utf8`).
  **D9 critical decision (runtime null-count
  enforcement):** after coercion + typed-null fill,
  every non-null target field with `null_count() > 0`
  fails before `StorageWrite::write` with column name
  in diagnostic; runtime check on actual data, not
  bind-time schema-nullability check (Codex iter-1 R4
  refinement — filtered nullable-typed sources with
  zero actual nulls succeed).
  **D10 critical decision (SELECT plan stability
  resumes):** ADR 032 additive over post-ADR-031
  SELECT plan-digest baseline; existing SELECT-only
  plan shapes byte-identical pre/post-ADR-032; new
  `InsertSelect` variants only appear under
  `INSERT INTO ... SELECT` statements. Re-stabilizes
  the invariant ADR 031 D14 broke (break-then-
  restabilize cycle property: substrate-replacing
  ADRs may break invariants with explicit D-level
  ratification; subsequent composition ADRs re-
  stabilize the new baseline).
  **D11 critical decision (walker leaf treatment +
  binder optimizer exception):** distributed /
  scheduler / server metadata walkers treat
  `ExecutionPlan::InsertSelect` as a leaf (join
  detection false; exchange counting zero; scan
  metadata none; replacement walkers unchanged);
  binder's `optimize_for_required` is the one
  intentional exception (descends into source with
  `required_output = None` so ADR 013 / 014 runtime-
  filter and scan-projection pushdown still apply to
  the source SELECT).
  **D12 critical decision (outer morsels do not
  partition DML):** source SELECT executes with
  `None` morsel argument; preserves ADR 030 VALUES
  INSERT behavior; defensive guardrail against
  future refactor accidentally propagating outer
  morsels into the write path.
  **D13 critical decision (`evaluate_cast`
  visibility):** `crate::exec::evaluate_cast` widens
  from private to `pub(crate)` so
  `coordinator::write` can invoke ADR 025 strict cast
  kernels; `evaluate_try_cast` and lower-level numeric
  cast kernels stay private; only cast-helper
  visibility change.
  Two substantive iterations + one promotion-drafting
  pass = **3 total with 0 flip-flops** — **fastest
  composition convergence to date** (vs ADR 031's
  3 + 1 ratification = 4 total). Three factors drove
  the speed: first composition ADR after the
  substrate arc (no new substrate added); pure-Toron
  + pure-local (no Wendelon-side discovery pass; no
  distributed-write-context threading; ADR 030 D13
  distributed reject pattern mirrored); Codex iter-1
  absorbed Claude iter-0 C1-C4 + Q1-Q4 wholesale
  (plan-digest re-stabilization C1 → D10; strict-cast
  C2 → D8; walker leaf-treatment C3 → D11 with R2
  refinement; aggregate composition fixture C4 →
  Required Fixture A). **Three Claude iter-0 / iter-1
  contributions promoted directly to D-level** (C1 →
  D10; C2 → D8; C5 → D13 + architecture guard); plus
  C3 → D11 (with Codex R2 refinement); C4 → Required
  Fixture A; C6 → architecture guard wording for
  `adr032_binder_enforces_source_target_arity`; C7 →
  Required Fixture B (INNER JOIN). **Two cycle-
  property breaks recorded:** Gemini iter-1/2
  retraction streak broken at 6-of-7 (Gemini iter-1
  converged directly with Codex iter-1 shape without
  proposing broader-scope framings); topic-concession-
  compresses-next-cycle streak broken at 3-of-3 (ADR
  031 → 032 transition did not generate iter-0 alt-
  topic seed). Both breaks recorded as cycle-property
  refinements: broader-scope framings and seed-
  pattern carry-forward are **substrate-cycle
  phenomena**, not universal cycle phenomena;
  composition cycles may not provoke these tendencies.
  **First zero-invariant-break cycle since ADR 028**
  (ADR 029 / 030 additive, zero break; ADR 031
  substrate-replacing, broke plan-digest invariant
  per D14; ADR 032 composition, zero break + re-
  stabilizes per D10 — pattern: composition cycles
  are zero-break by construction). Codex iter-1
  contributed the most distinctive code-evidence
  reasoning (R1 `target_columns` carrier, R2 binder-
  may-descend exception, R4 runtime null-check vs
  schema null-check — all non-obvious refinements
  Claude iter-0 missed; healthy collaboration sign).
  Fourteen ADR-032-specific architecture guards, plus
  in-place updates to existing LogicalPlan /
  ExecutionPlan variant-count coverage. Two required end-to-end
  composition fixtures: **Fixture A (grouped
  aggregate source — SUM)** exercises ADR 028
  signature substrate + ADR 031 `BoundExprProgram`
  aggregate evaluation; **Fixture B (INNER JOIN
  source)** exercises ADR 008 / 009 join substrate +
  explicit target column list reorder via D3
  `target_columns` carrier. Both required for
  Definition of Done. Fourteenth instance of the ADR
  015 cycle. SoA-readiness invariants preserved (no
  kernel added; centralized dispatch unchanged; no
  row-path dyn dispatch). No new SQL syntax beyond
  the bare `INSERT INTO ... <select-statement>` form;
  no new IR expression variant; no new aggregate
  kernel; no PBC opcode change; no fragmenter
  detector change for SELECT paths; no cross-ring
  engagement; no Wendelon-side change; no
  `RecordBatch` field added to any plan variant; no
  streaming `StorageWrite` overload; no row-count or
  RETURNING result; no `blake3` dependency; no
  external allocator crate. ADR 029 catalog metadata-
  only preserved; ADR 030 `ExecutionPlan::Insert`
  expression-free preserved; ADR 031 `BoundExprProgram`
  `PartialEq`-only preserved; `StorageWrite` trait
  shape unchanged. Existing SELECT corpus plan
  digests byte-identical pre/post-ADR-032 (D10 re-
  stabilization). The nine-step migration is
  materialized in
  `plans/impl/032_insert_into_select.md`.
- **ADR 033 — SQL `CREATE TABLE AS SELECT` (Local DDL +
  DML Composition).** Promoted. **Second composition
  ADR** after ADR 032, completing the local DDL/DML
  composition loop: ADR 029 creates a catalog table
  from a declared schema; ADR 030 writes literal rows
  via `StorageWrite`; ADR 032 writes SELECT output
  into an existing table; ADR 033 creates a table
  whose schema is inferred from the SELECT and writes
  the SELECT output in one statement. Fourteen
  decisions D1-D14 + ten Codex R-items (R1-R7 at iter-
  0; R1-R3 at iter-1).
  **D1 topic decision:** local `CREATE TABLE AS SELECT`.
  **D11 critical decision:** local-coordinator-only;
  `QueryCoordinator::execute_query` intercepts
  `LogicalPlan::CreateTableAsSelect` before the SELECT
  retry loop; distributed coordinator rejects before
  `fragment_plan` with stable diagnostic `"distributed
  CREATE TABLE AS SELECT execution is not wired to a
  write-context-bearing coordinator"`; fragmenter
  rejects via new
  `FragmentError::CreateTableAsSelectNotFragmentable`;
  defensive executor rejects for unbound + bound
  uncoordinated reach.
  **D2 critical decision (Shape A):** separate IR
  variants alongside (not replacing) ADR 029
  `CreateTable` and ADR 032 `InsertSelect` variants.
  New `LogicalPlan::CreateTableAsSelect { table:
  TableRef, source: Box<LogicalPlan> }` and new
  `ExecutionPlan::CreateTableAsSelect { table:
  TableRef, schema: SchemaRef, source: Box<ExecutionPlan> }`.
  **The bound variant deliberately does NOT carry
  `TableId`, `TableObjectId`, or `PhysicalDescriptorFingerprint`**
  (Codex iter-0 R1) — binding cannot mutate catalog;
  coordinator obtains binding from `create_table` and
  uses it to populate `WriteRequest`.
  `LogicalPlan` 12 → 13; `ExecutionPlan` 10 → 11.
  **D3 critical decision (narrow SQL surface):** v1
  supports bare `CREATE TABLE name AS <query>` only;
  rejects non-empty `CreateTable.columns` (Codex
  iter-0 R3 — sqlparser's `columns` field is
  `Vec<ColumnDef>`, not a clean alias list; supporting
  "column list means aliases under CTAS but typed
  columns elsewhere" muddles ADR 029's DDL surface);
  rejects `IF NOT EXISTS` (Codex iter-0 R4 —
  `Catalog::create_table` returns `TableBinding` not
  "created vs already existed"; `IF NOT EXISTS` could
  accidentally append SELECT rows into an existing
  table); rejects `OR REPLACE` / `LIKE` / `CLONE` /
  constraints / storage options / etc. via existing
  ADR 029 reject path. Column renaming uses SELECT
  aliases (`CREATE TABLE dst AS SELECT x AS a, y AS
  b FROM src`).
  **D4 critical decision (schema inference):** target
  schema inferred from bound source SELECT output —
  names, types, nullability inherited verbatim;
  validated against ADR 029 lattice (six types) +
  duplicate-name reject + Wendelon synthetic name
  reject.
  **D5 critical decision (Codex iter-0 R2):** binder
  binds source SELECT BEFORE any catalog mutation;
  `CREATE TABLE dst AS SELECT * FROM missing` must
  fail without leaving phantom catalog entry. Binding
  cheap; execution potentially expensive.
  **D6 critical decision (Codex iter-1 R1):**
  coordinator sequence — `catalog.create_table` →
  `executor.execute_bound(source, None)` →
  `materialize_insert_select_batch` (Some) or
  `materialize_empty_record_batch` (None) → single
  `writer.write` → `Ok(Vec::new())`. **Create-before-
  execute is intentional** (fails fast on table-exists
  / admin-unavailable before spending source SELECT
  work); load-bearing safety invariant is source
  BINDING before catalog mutation (D5), not source
  EXECUTION before catalog mutation.
  **D7 critical decision (Codex iter-0 R7):** non-
  atomic catalog-then-write explicitly documented;
  `create_table` success + write failure = phantom
  table (v1 limitation; rollback needs future `DROP
  TABLE` ADR or transactional metadata-write contract
  ADR).
  **D8 critical decision (Codex iter-0 R6 + Claude
  iter-1 C1):** empty source SELECT must create
  queryable empty table via zero-row write through
  new sibling helper `materialize_empty_record_batch(schema)`
  in `coordinator/write.rs`. **ADR 032's
  `materialize_insert_select_batch` contract preserved
  verbatim** — still returns `Ok(None)` for zero rows;
  the CTAS branch handles `None` via sibling helper.
  No modification to ADR 032 helper. Sibling helper
  reuses existing private `typed_null_array(_, 0, _)`
  for empty typed array construction.
  **D9 critical decision (Codex iter-1 R2 + Claude
  iter-2 A1):** Wendelon storage synthetic name
  reject site is the binder's schema-inference path;
  binder imports the five Wendelon constants directly
  (or via neutral helper); **forking `__row_*__`
  literals in `binder.rs` is forbidden** — silent-
  drift risk between two enforcement sites.
  Architecture guard #7 enforces via negative-pattern
  check.
  **D10 critical decision (Codex iter-1 R3):** no
  source-to-target type coercion in CTAS — target
  schema IS source schema by construction; CTAS
  passes identity `ColumnOrdinal` target columns to
  `materialize_insert_select_batch`; users spell
  `CAST` in SELECT if they want type adjustments. The
  `evaluate_cast` `pub(crate)` visibility ADR 032 D13
  introduced is not even consumed by the CTAS path.
  **D11-D14 carry-forwards:** local-only execution
  (D11 mirrors ADR 029 D4 + ADR 030 D12 + ADR 032
  D1); binder optimizer may descend into source while
  distributed walkers treat CTAS as leaf (D12 mirrors
  ADR 032 D11); outer morsels do not partition CTAS
  source (D13 mirrors ADR 030 + ADR 032 carry-
  forward); SELECT plan-digest stability resumes (D14
  mirrors ADR 032 D10 — composition cycles are zero-
  break by construction).
  Three iterations / 0 flip-flops — **ties ADR 032
  as the fastest cycle in council history**. **First
  three-way independent iter-0 topic convergence in
  council history** — all three council members
  picked CTAS as lead at iter-0 with no topic
  dissent. Three factors drove the speed: three-way
  topic convergence (no iter-1 topic reopen); pure-
  Toron and pure-local (no Wendelon-side discovery
  pass; ADR 029 catalog path + ADR 032 materialization
  helper directly reused); Codex iter-0 R1-R7
  absorbed wholesale by Claude/Gemini iter-1 with
  three additional Codex iter-1 R-items at the
  coordinator-ordering and synthetic-name discipline
  layer. **Sixth consecutive cycle of Codex
  correctness-clarification pattern** — 10 R-items
  total across iter-0 + iter-1, each from code-
  evidence audit of catalog / binder / coordinator
  boundary. **First cycle where Claude iter-0 was
  retracted four-for-four on load-bearing v1-surface
  defaults** (D3 column rename, D5 all-nullable, D7
  atomicity wording, D8 empty-source no-write —
  Codex iter-0 R3/R5/R4/R6 corrected each); recorded
  as cycle property: composition cycles over
  substrate-rich foundations (ADR 029 / 030 / 031 /
  032 stack) have more subtle code-evidence
  constraints than substrate cycles. **Gemini iter-
  1/2 retraction streak still broken at 6-of-8**:
  Gemini iter-1 absorbed Codex iter-0 wholesale on
  the stricter v1 surface with no broader-scope
  framings — second consecutive composition cycle
  confirming broader-scope framings are a **substrate-
  cycle phenomenon**. **Topic-concession-compresses-
  next-cycle pattern still broken at 3-of-5**: ADR
  032 → 033 also generated no iter-0 alt-topic seed
  — second consecutive composition cycle confirming
  the seed pattern is also substrate-cycle-specific.
  **Second consecutive zero-substrate-invariant-break
  cycle**: composition cycles are zero-break by
  construction (now two data points: 032 + 033).
  **Three Claude iter-1/iter-2 contributions
  promoted**: iter-1 C1 → D8 + sibling helper +
  guard #13; iter-1 C4 → 4 required composition
  fixtures; iter-2 A1 → architecture guard #7 strict
  negative-pattern wording.
  Eighteen new ADR 033 architecture guards + one
  paired adjustment to the existing
  `architecture_guards_unchanged_plus_sort_variant`
  variant-count catch-all = 19 architecture guard
  touchpoints. Four required end-to-end composition
  fixtures for Definition of Done: Fixture A (SUM
  grouped aggregate source), Fixture B (INNER JOIN
  source), Fixture C (empty-source initialization —
  empirical verification of D8), Fixture D (failure-
  before-catalog — regression test for D5). New test
  file `crates/toron/tests/ctas_test.rs` (Claude
  iter-2 A2; preserves `insert_test.rs` scope at ADR
  030 + 032).
  Fifteenth instance of the ADR 015 cycle. SoA-
  readiness invariants preserved. Brainstorm-seam
  touch check: 001 (Cranelift JIT) untouched; 002
  (WeakControlRing) untouched; 003 (Disaggregated
  Compute) touched but preserved (one new distributed
  reject arm); 004 (Memory Arenas) untouched. No new
  SQL syntax beyond bare `CREATE TABLE name AS
  <query>`; no new IR expression variant; no new
  aggregate kernel; no PBC opcode change; no
  fragmenter detector change for SELECT paths; no
  cross-ring engagement; no Wendelon-side change;
  no `RecordBatch` field added to any plan variant;
  no streaming `StorageWrite` overload; no row-count
  or RETURNING result; no `blake3` dependency; no
  external allocator crate added; no source-to-
  target type coercion at CTAS layer. ADR 029
  catalog metadata-only preserved; ADR 030
  `ExecutionPlan::Insert` expression-free preserved;
  ADR 031 `BoundExprProgram` `PartialEq`-only
  preserved; ADR 032
  `materialize_insert_select_batch` `Ok(None)` zero-
  row contract preserved (guard #17); `StorageWrite`
  trait shape unchanged. Existing SELECT corpus plan
  digests byte-identical pre/post-ADR-033 (D14 re-
  stabilization). The eight-phase migration is
  materialized in
  `plans/impl/033_create_table_as_select.md`.
- **ADR 034 — SQL `DROP TABLE` (Local DDL Deletion).**
  Promoted. **Third composition ADR** after ADR 032 +
  033, completing the post-substrate composition
  trilogy. Narrow metadata-only DDL deletion: removes
  catalog reachability via `Catalog::drop_table`; no
  row deletion, no storage reclaim, no Wendelon
  metadata administration. Sixteen decisions D1-D16.
  **D1 critical decision:** narrow `DROP TABLE [IF
  EXISTS] table_name` only; rejects `CASCADE` /
  explicit `RESTRICT` / `PURGE` / `TEMPORARY` /
  multi-name / non-table object types with stable
  diagnostics; exhaustive `Statement::Drop`
  destructure (no `..`).
  **D4 critical decision (Claude iter-1 C2):** DDL
  name helper renamed `lower_create_table_name` →
  `lower_ddl_table_name`; CREATE TABLE (ADR 029) +
  CTAS (ADR 033) + DROP TABLE (ADR 034) all use the
  neutral helper; sets the pattern for future ALTER
  TABLE / CREATE INDEX / CREATE VIEW DDL ADRs.
  **D5 critical decision:** leaf IR variants
  `LogicalPlan::DropTable { name, if_exists }` +
  `ExecutionPlan::DropTable { name, if_exists }` —
  **no `TableId` / `TableObjectId` /
  `PhysicalDescriptorFingerprint` / boxed source
  child** (mirrors ADR 033 R1 symmetry; catalog
  resolves identity at mutation time).
  **D6 critical decision:** binder pass-through; no
  catalog calls in the binder arm; existence and
  `IF EXISTS` semantics centralize in the catalog
  mutation.
  **D7 critical decision (Codex iter-1 R1):**
  `Catalog::drop_table(name, if_exists) ->
  Result<Option<TableBinding>, CatalogError>`;
  existing → `Ok(Some(binding))`; missing with
  `IF EXISTS` → `Ok(None)`; missing without `IF
  EXISTS` → `TableNotFound`; Wendelon known →
  `AdminUnavailable` (mirrors ADR 029
  `create_table`); Wendelon unknown with `IF
  EXISTS` → `Ok(None)`.
  **D8 critical decision:** `MockCatalog::drop_table`
  removes from `by_id`; uses `by_ref.retain` to
  remove every alias pointing to the dropped table
  id; **no alias repointing**; no `next_table_id`
  reuse — recreate-after-drop gets a fresh
  monotonic id so stale in-memory storage data for
  the old id is unreachable through the new
  binding.
  **D9 critical decision:** no storage reclaim
  guarantee — catalog reachability only; physical
  file deletion, in-memory storage map cleanup,
  Wendelon descriptor deletion, stale-bound-plan
  invalidation, automatic CTAS rollback all
  deferred to future storage-admin ADR.
  **D10 critical decision:** coordinator routing
  via shared `execute_catalog_plan` helper
  (extended from ADR 029); both local and
  distributed coordinators intercept DROP and
  route through metadata helper before SELECT
  retry loop / `fragment_plan`.
  **D13 critical decision (Claude iter-0 C2):**
  telemetry label `"drop_table"` (distinct from
  CREATE TABLE's `"catalog"`; mirrors ADR 032 /
  033 per-statement-form discipline).
  **D14 critical decision:** CTAS cleanup
  composition test required — proves the manual
  recovery path ADR 033 D7 named for non-atomic
  CTAS failures (Claude iter-0 C1 promoted to
  test coverage by Codex iter-1).
  **D15 critical decision (Claude iter-0 C4):**
  ADR 035 sequencing commitment — ADR 035 = row-
  identity / predicate-delete substrate cycle that
  settles five questions (row-key-id discovery
  path, identity carrier shape,
  `StorageDeleteRows` trait, in-memory deletion
  semantics, delete-count result mapping). Then
  ADR 036 = SQL `DELETE WHERE` as a narrow
  composition over the substrate. **Restores the
  topic-concession-compresses-next-cycle pattern at
  4-of-6** — substrate-defer ADRs seed the next
  cycle.
  **D16 critical decision:** Plan Arena remains
  named debt; DROP adds one leaf variant + no
  boxed child; no `bumpalo` / `typed-arena` /
  `slab` / `generational-arena` / `blake3` /
  storage synthetic row construction.
  **`LogicalPlan` 13 → 14; `ExecutionPlan` 11 →
  12.** Three iterations / 0 flip-flops — **third
  consecutive composition cycle at 3-iter cadence**
  matching ADR 032 + 033. **Iter-0 topic split**
  (Claude + Codex on DROP TABLE; Gemini on DELETE
  WHERE) resolved decisively at iter-1 when Gemini
  engaged Codex iter-0's E1 code-evidence audit
  documenting Wendelon's row-key-id-based delete
  API (`wendelon::client::TableDeleteRowsRequest
  { row_key_ids: Vec<Identity> }`) + Toron's
  user-schema-only scan boundary + ADR 030 D7's
  prohibition on Toron-side synthetic column
  construction. **Gemini iter-1/2 retraction
  streak restored at 7-of-9** after the 6-of-8
  plateau; Claude iter-0 C5 prediction confirmed
  verbatim. **Seventh consecutive cycle of Codex
  correctness-clarification pattern**: Codex
  iter-0 was unusually complete (13 D-level
  sketches + E1 audit); Codex iter-1 added three
  R-items (D14 CTAS test, D15 sequencing, D16
  Plan Arena guardrail). **Seven Claude iter-0 /
  iter-1 contributions promoted**: iter-0 C2 →
  D13 telemetry label; iter-0 C4 → D15 sequencing
  commitment; iter-0 C1 → D14 CTAS cleanup test
  coverage; iter-0 Q7 → architecture guard #16
  storage-reclaim negative-pattern; iter-1 C1 →
  architecture guard #17 catalog-trait
  narrowness; iter-1 C2 → D4 DDL helper rename;
  iter-1 A3 → test-file naming convention
  codified. **Composition trilogy complete** (ADR
  032 + 033 + 034); ADR 029 → 030 → 031 was the
  substrate trilogy. ADR 035 starts a new
  substrate phase with the row-identity /
  predicate-delete cycle. **Composition-cycle
  properties at 3 data points:** (1) zero-
  substrate-invariant-break by construction; (2)
  3-iter convergence floor; (3) Plan Arena
  preserved as named-debt; (4) broader-scope
  framings are substrate-cycle phenomena
  (reinforced by Gemini iter-1 retraction); (5)
  topic-concession-seed pattern returns when cycle
  defers substrate work (ADR 034 → 035 generates
  seed via D15). Sixteenth instance of the ADR
  015 cycle. **17 ADR-034-specific architecture
  guards + 1 paired adjustment** to the existing
  `architecture_guards_unchanged_plus_sort_variant`
  variant-count catch-all = 18 architecture guard
  touchpoints. 11 required functional fixtures
  (planner / binder / catalog / coordinator /
  distributed / Wendelon / defensive-reject /
  CTAS-cleanup-composition). New test file
  `crates/toron/tests/drop_table_test.rs` (Claude
  iter-1 A3 file-naming convention: one statement
  form, one test file). SoA-readiness invariants
  preserved. Brainstorm-seam touch check: 001 +
  002 + 004 untouched; 003 touched but preserved
  (one new metadata-routing arm). No new SQL
  syntax beyond bare `DROP TABLE [IF EXISTS]
  table_name`; no new IR expression variant; no
  new aggregate kernel; no PBC opcode change; no
  fragmenter detector change for SELECT paths; no
  cross-ring engagement; no Wendelon-side change;
  no new `StorageWrite` overload; no row-count or
  RETURNING result; no `blake3` dependency; no
  external allocator crate added; no storage
  reclaim trait or helper introduced. ADR 029 /
  030 / 031 / 032 / 033 invariants preserved
  (including ADR 032
  `materialize_insert_select_batch` `Ok(None)`
  zero-row contract + ADR 033
  `materialize_empty_record_batch` sibling
  helper). Existing SELECT corpus plan digests
  byte-identical pre/post-ADR-034 (composition-
  cycle carry-forward). The nine-phase migration
  is materialized in
  `plans/impl/034_drop_table.md`.
- **ADR 035 — Row Identity / Predicate-Delete
  Substrate.** Promoted. **First cross-ring
  substrate cycle since ADR 030** and the first
  cycle of the new substrate phase following the
  composition trilogy (032 → 033 → 034). Additive
  substrate cycle (new types / trait / submodule /
  Wendelon query metadata and client-option fields;
  nothing replaced) that
  settles the five load-bearing questions ADR 034
  D15 sequenced to unblock ADR 036 (SQL `DELETE
  WHERE`) as a narrow composition cycle. Fifteen
  decisions D1-D15.
  **D1 critical decision:** new
  `RowIdentityProjection` enum (`None` + `Append`)
  on `ScanRequest`; parallel definition in Toron
  (`toron::storage`) and Wendelon (`wendelon::client`)
  with no cross-crate re-export.
  **D4 critical decision (Claude iter-1 C7):**
  privileged identity-projection setting carried
  through Wendelon `TableQueryRequestMeta`,
  `TableQueryRequest`, and
  `wendelon-client::TableQueryOptions` (Option A —
  single query option / metadata field, not new wire
  RPC method, not new `RequiredColumnAxis` variant).
  Wendelon's
  ordinary `insert_required_column` PBC PROJECT
  storage-column reject preserved verbatim.
  **D5 critical decision:** typed
  `DeleteRowsRequest { row_key_ids: Vec<Identity> }`
  at the Toron-to-Storage trait boundary — no
  `RecordBatch` payload (wire encoding stays inside
  Wendelon's client).
  **D6 critical decision:** dedicated
  `DeleteRowsOutcome` preserving delete accounting:
  `requested` from the Toron request length plus
  Wendelon's three response counters (`deleted` /
  `already_deleted` / `not_found`); not
  `WriteOutcome` reuse. ADR 036 maps `deleted`
  alone to SQL row count.
  **D7 critical decision:** dedicated
  `StorageDeleteRows` trait separate from
  `StorageWrite`; write trait shape byte-identical.
  **D9 critical decision:** in-memory storage uses
  tombstones (`HashSet<Identity>`), not physical
  removal; storage reclaim deferred to future ADR.
  **D10 critical decision (Claude iter-0 C3):**
  deterministic hash-free mock identity layout
  `[table_object_id; 16] + [zero; 8] +
  [ordinal_be; 8]` — no `blake3` / `hash` / `sha`
  in Toron (ADR 030 D7 carry-forward extended to
  Toron mock identity being opaque-not-
  cryptographic; Wendelon's append-only / primary-
  key identity generation remains storage-owned via
  BLAKE3 server-side).
  **D11 critical decision (Claude iter-0 C4 +
  iter-1 C8 + iter-2 pin):** storage adapters
  reject `row_identity == Append &&
  projected_columns.is_some()` with
  `StorageError::Internal { message: "scan with
  row_identity=Append cannot be combined with
  projected_columns" }`; reuses existing variant
  (no new `StorageError::InvalidScanRequest`).
  **D12 critical decision (Claude iter-0 C1):**
  candidate materializer ships in ADR 035 via new
  `crates/toron/src/coordinator/delete.rs`
  submodule with `materialize_delete_row_key_ids`
  helper that validates trailing-column name +
  type + width + null and extracts
  `Vec<Identity>`; identity never returns to user
  result paths.
  **D13 critical decision:** Wendelon delete
  adapter calls the high-level
  `wendelon-client::Client::table_delete` wrapper
  (singular, **not** `table_delete_rows`) with
  `await_finalization: true`; maps `requested` from
  request length and the other three counters from
  Wendelon's response.
  **D15 critical decision (Claude iter-0 C6):**
  ADR 036 sequencing commitment — SQL `DELETE
  WHERE` as narrow composition cycle over ADR 035
  substrate; expected 3-iter composition-cycle
  cadence matching ADR 032 / 033 / 034.
  Four iterations / 0 flip-flops — **first cross-
  ring substrate cycle to hit 4-iter cadence with
  iter-2 as promotion-drafting** (faster than ADR
  029 / 030; both ran iter-3 promotion). Iter-0
  produced complete Codex sketch (D1-D12 + E1-E4
  code-evidence audit) + Claude endorsement (six
  contributions C1-C6 resolving four of Codex's
  five open questions) + Gemini iter-1
  substrate-shape disagreements (four wrong
  defaults: bool, RecordBatch, single counter,
  blake3). Iter-1 settled the load-bearing shape
  via Codex iter-1 D1-D15 + three additional code-
  evidence audits (E5 / E6 / E7) absorbed
  wholesale by Claude iter-1 plus four Claude
  iter-1 cross-ring precision items (C7 Wendelon
  field shape Option A; C8 `StorageError` variant
  pin; C9 architecture guard #17 cross-ring
  boundary wording; C10 4-iter cross-ring cadence
  call). Gemini iter-2 retracted three of four
  iter-1 wrong positions (substrate shape
  alignment). Iter-2 was promotion-drafting with
  four wording precision items (D1 enum location;
  D4 `TableQueryRequest` field shape; D11
  `StorageError::Internal` variant pin; guard #17
  mechanism via `wendelon_manifest_dir()` helper).
  **Gemini iter-1/2 retraction streak restored at
  8-of-10** after the 7-of-9 ADR 034 mark.
  **Eighth consecutive cycle of Codex correctness-
  clarification pattern.** **Claude iter-0 /
  iter-1 combined 10-of-10 contribution promotion
  rate** — highest combined rate in council
  history. Seventeenth instance of the ADR 015
  cycle. **18 ADR-035-specific architecture
  guards** + 10 Toron functional fixtures + 3
  Wendelon functional fixtures. Toron fixtures
  live in new
  `crates/toron/tests/delete_substrate_test.rs`
  (Claude iter-1 A3 file-naming convention carry-
  forward from ADR 034: one substrate cycle, one
  test file). Cross-ring substrate cycle —
  Wendelon-side patch required: extend
  `TableQueryOptions`, `TableQueryRequest`, and
  wire-visible `TableQueryRequestMeta` with the
  row-identity setting; add privileged identity-
  projection handler that appends `__row_key_id__`
  for the `Append` mode while preserving the ordinary
  `insert_required_column` PBC PROJECT storage-
  column reject verbatim. No new SQL syntax in
  this ADR; no new IR variants; no new aggregate
  kernel; no PBC opcode change; no fragmenter
  detector change for SELECT paths; no distributed
  DELETE coordinator routing (ADR 036 will mirror
  ADR 030 D13 distributed-INSERT reject pattern);
  no new `StorageError` variant; no new
  `RequiredColumnAxis` variant on Wendelon; no
  new Wendelon wire RPC or identity-specific query
  method (the high-level `wendelon-client`
  `table_delete` wrapper is in scope); no `blake3`
  dependency;
  no external allocator crate added; no storage
  reclaim trait or helper introduced. ADR 029 /
  030 / 031 / 032 / 033 / 034 invariants
  preserved (including ADR 030 D7 Toron-side
  synthetic-column construction prohibition
  extended to Toron mock identity being opaque-
  not-cryptographic; ADR 034 `Catalog::drop_table`
  + `lower_ddl_table_name` helper preserved).
  Existing SELECT corpus plan digests byte-
  identical pre/post-ADR-035 (additive substrate
  cycle carry-forward). The nine-phase migration
  is materialized in
  `plans/impl/035_row_identity_predicate_delete_substrate.md`.

V1 target is **"First Query with Integrity"** plus
**"Distributed Execution Correctness"**: correct analytical
results against a Wendelon-native cluster with pushdown
maximized, identity flow real, stream-shaped storage
boundary, multi-process coordinator/worker mechanics
surviving a worker SIGKILL with byte-identical normalized
output, and the first two-input relational operator working
across hash-partitioned and bounded broadcast strategies.
Multi-tenant governance, executor model upgrade, sort-merge,
and pipelined cross-stage execution
remain post-V1.

## Prioritization philosophy

Toron is the compute ring of the Nilket stack; Wendelon is
the storage ring. We control both ends. The boundary exists
for scaling dynamics and resource isolation, not because
storage and compute are independent products.

**Minimizing re-work of the long term vision.** We favor architectural 
directions that align with our far-horizon goals, avoiding dead-ends or 
designs that necessitate significant rewrites as the system evolves.

**Toron's reason for existence is smart-storage pushdown.** Every
improvement — executor model upgrade, advanced kernels, frontend
hardening — polishes a system that does its primary job along its
primary axis.

**ADR-by-ADR cadence.** Wendelon ADR 070 pattern: narrow,
evidence-driven, single-decision contracts. Each ADR earns its
scope from the prior ADR's bench evidence or a fresh code-evidence
defect; mega-scopes get split.

**Evidence drives scope.** The most informative artifact for the
next ADR is the previous ADR's bench results — or a code-evidence
silent-drop / inert-feature defect surfaced during selection.
**Evidence Pressure > Launch Pressure** when both are live: the
council picks the topic with measured signal, not the one with
the loudest backlog.

**Scale-boundary convention (adopted iter-5 of ADR 006).** Every
ADR from ADR 006 forward includes an explicit **Scale boundary**
section after Status and before Context. The decomposition makes
scope-creep into adjacent boundaries visible at review time and
sharpens ADR selection: the same operator at different scales is
a different contract.

**Minimize strategic rework, not ADR scope.** When two narrow
ADR shapes are otherwise equivalent, prefer the one that
preserves plausible long-term seams the noted brainstorms
depend on (e.g., `NodeRoleSet`, ring-router asymmetry, IR-arena
compatibility, `WeakControlRing` coordinator API, JIT-friendly
expression shape) and avoids likely reversal. **Evidence still
drives scope** — brainstorms remain "noted but not committed"
and do not pull near-term scope forward. An ADR that closes off
a seam should do so explicitly and with reason, not by accident.
The selection round names the touched seams; the council
ratifies the trade-off in the open.

## Forward sequence

Earlier items have firmer scope; later items are direction,
not commitment. Selection rounds include a brainstorm-seam
check per the Prioritization philosophy — most candidates
touch no seams, but the question is asked.

### ADR 035 promoted; ADR 036 candidates

ADR 035 selected and promoted **Row Identity /
Predicate-Delete Substrate** — Toron's **first
cross-ring substrate cycle since ADR 030** and the
first cycle of the new substrate phase following the
composition trilogy (032 → 033 → 034). Substrate-
introducing additive cycle: new types, new trait,
new submodule, new Wendelon query metadata and
client-option fields; nothing replaced. Settles the
five load-bearing questions
ADR 034 D15 sequenced to unblock ADR 036 (SQL
`DELETE WHERE`) as a narrow composition cycle.

Fifteen decisions D1-D15 cover: new
`RowIdentityProjection` enum (`None` + `Append`) on
`ScanRequest`, defined parallel in Toron and Wendelon
with no cross-crate re-export; no SQL DELETE syntax /
IR variants in this ADR; transient identity column
never SQL-visible (planner / binder synthetic-name
rejects preserved); **privileged identity projection
setting on Wendelon `TableQueryRequestMeta`,
`TableQueryRequest`, and
`wendelon-client::TableQueryOptions`** (Option A —
single query option / metadata field, not new wire
RPC method / not new `RequiredColumnAxis` variant),
with ordinary
`insert_required_column` PBC PROJECT storage-column
reject preserved verbatim; typed
`DeleteRowsRequest { row_key_ids: Vec<Identity> }`
at the trait boundary (no `RecordBatch` payload);
dedicated `DeleteRowsOutcome` preserving delete
accounting (`requested` from the Toron request
length plus Wendelon's three response counters:
`deleted` / `already_deleted` / `not_found`);
dedicated
`StorageDeleteRows` trait separate from
`StorageWrite` (write trait shape byte-identical);
empty candidate set short-circuits in coordinator
helper without calling storage; in-memory storage
uses tombstones (`HashSet<Identity>`), not physical
removal; **deterministic hash-free mock identity
layout `[table_object_id; 16] + [zero; 8] +
[ordinal_be; 8]`** (no `blake3` / `hash` / `sha`
in Toron — ADR 030 D7 carry-forward extended);
identity scan rejects user projection with
`StorageError::Internal { message }` reuse (no new
error variant); new `crates/toron/src/coordinator/delete.rs`
submodule with `materialize_delete_row_key_ids`
helper that validates trailing-column name + type
+ width + null and extracts `Vec<Identity>`;
Wendelon delete adapter calls the high-level
`wendelon-client::Client::table_delete` wrapper
(singular, **not** `table_delete_rows`) with
`await_finalization: true`; PBC and runtime filters
remain advisory; ADR 036 sequencing commitment for
SQL `DELETE WHERE` as narrow composition over the
substrate.

Four iterations / 0 flip-flops — **first cross-ring
substrate cycle to hit 4-iter cadence with iter-2
as promotion-drafting** (faster than ADR 029 + 030
which both ran iter-3 promotion; ADR 030 had one
flip-flop). Iter-0 produced complete Codex sketch
(D1-D12 + E1-E4 code-evidence audit) + Claude
endorsement (six contributions C1-C6 resolving four
of Codex's five open questions) + Gemini iter-1
disagreements (four substrate-shape wrong defaults).
Iter-1 settled load-bearing shape via Codex iter-1
D1-D15 + three additional code-evidence audits
(E5 / E6 / E7) absorbed wholesale by Claude iter-1
plus four Claude iter-1 cross-ring precision items
(C7 Wendelon field shape Option A; C8 `StorageError`
variant pin; C9 architecture guard #17 cross-ring
boundary wording; C10 4-iter cross-ring cadence
call); Gemini iter-2 retracted three of four iter-1
wrong positions (bool → enum; RecordBatch trait
boundary → `Vec<Identity>`; single counter → four
counters; blake3 → deterministic-no-hash). Iter-2
was promotion-drafting with four wording precision
items (D1 enum location, D4 `TableQueryRequest`
field shape, D11 `StorageError::Internal` variant
pin, guard #17 mechanism via `wendelon_manifest_dir()`
helper).

**Gemini iter-1/2 retraction streak restored at
8-of-10** after the 7-of-9 ADR 034 mark. Gemini
iter-1 produced substrate-shape disagreements per
the substrate-cycle pattern (composition cycles
032 + 033 + 034 had no substrate ambiguity for
Gemini to engage with; substrate-introducing ADR
035 returned to the iter-1-disagreement-iter-2-
retraction shape). Updated cycle property:
substrate-cycle wrong defaults retract reliably
when Codex + Claude joint iter-1 evidence
consolidates.

**Eighth consecutive cycle of Codex correctness-
clarification pattern.** Codex iter-0 was unusually
complete (12 D-level sketches + four code-evidence
audits + five open questions); Codex iter-1 added
three more code-evidence audits (E5 PBC PROJECT
reject discipline, E6 `Client::table_delete`
singular method name, E7 storage-owned identity
generation) and tightened three Wendelon-side
decisions (D4 privileged-not-general projection
mode, D11 storage-error variant pin, D13 delete
adapter method name). Pattern now established
across ADR 024 / 025 / 026 / 028 / 029 / 030 / 031
/ 032 / 033 / 034 / 035.

**Claude iter-0 / iter-1 combined 10-of-10
contribution promotion rate** — highest combined
rate in council history. iter-0 C1-C6 all promoted
to Codex iter-1 D-level decisions
(C1 → D12; C2 → D6 + ADR 036 hook; C3 → D10 with
explicit "Ratify Claude C3" citation; C4 → D11;
C5 → Gemini retraction verbatim confirmation;
C6 → D15). iter-1 C7-C10 all promoted to ADR 035
D4 (Wendelon field shape) + D11 (StorageError
variant) + guard #17 (cross-ring boundary check) +
cycle-property data point (4-iter cross-ring
cadence call confirmed). Pattern: explicit
resolution of Codex iter-0 / iter-1 open questions
paired with code-evidence grounding promotes
near-100%.

**Trilogy alternation continues.** ADR 029 → 030 →
031 was the substrate trilogy (DDL + DML +
expression-IR-replacing). ADR 032 → 033 → 034 was
the composition trilogy (INSERT SELECT + CTAS +
DROP TABLE) over that substrate. ADR 035 starts a
new substrate phase. ADR 036 will likely be the
first composition cycle in the new phase (SQL
`DELETE WHERE` per D15 sequencing). Pattern
hypothesis at 7 data points.

**Substrate-cycle properties at first 035 data
point:**

1. **Cross-ring substrate cycles have a 4-iter
   floor.** (ADR 029, 030, 035 — three data
   points)
2. **Cross-ring substrate cycles can hit iter-2
   promotion-drafting when iter-1 produces joint
   substrate-lock + cross-ring audit.** (ADR 035
   new data point; revises the older ADR 029 / 030
   iter-3 promotion shape)
3. **Substrate cycles can be zero-existing-invariant
   break when additive (new trait / new types /
   new request fields), not substrate-replacing.**
   (ADR 035 first data point; contrasts with ADR
   031 `BoundExprProgram` substrate-replacing)
4. **Two-pass Wendelon-API discovery budget
   applies to cross-ring substrate cycles.** (ADR
   029 / 030 / 035 — three data points)
5. **Composition cycles seed substrate cycles via
   explicit sequencing decisions in their final
   iteration.** (ADR 034 D15 → ADR 035 — pattern
   data point #1 in the new sequencing-pin shape)

Seventeenth instance of the ADR 015 cycle. **18
ADR-035-specific architecture guards** (10 Toron
fixtures + 3 Wendelon fixtures). Toron fixtures
live in new `crates/toron/tests/delete_substrate_test.rs`
(Claude iter-1 A3 file-naming convention carry-
forward from ADR 034: one substrate cycle, one
test file).

**Ten Claude iter-0 / iter-1 contributions
promoted:**

- iter-0 C1 ADR 035 implements materializer +
  `coordinator/delete.rs` submodule → D12.
- iter-0 C2 `DeleteRowsOutcome.deleted` alone as
  SQL row count → D6 + ADR 036 hook.
- iter-0 C3 deterministic hash-free mock identity
  layout → D10 (explicitly cited).
- iter-0 C4 reject `projected_columns.is_some()`
  with `Append` → D11.
- iter-0 C5 Gemini iter-2 retraction prediction →
  confirmed verbatim.
- iter-0 C6 ADR 036 sequencing pin → D15.
- iter-1 C7 Wendelon `TableQueryRequest` field
  shape Option A → D4 + impl Phase 9.
- iter-1 C8 pin `StorageError::Internal` variant
  for D11 reject → D11 wording + impl Phase 5.
- iter-1 C9 architecture guard #17 cross-ring
  boundary wording → guard #17 with
  `wendelon_manifest_dir()` mechanism (iter-2
  pin).
- iter-1 C10 4-iter cross-ring cadence call →
  promoted as cycle-property data point.

**Cross-ring substrate cycle.** Wendelon-side
patch required: extend `TableQueryOptions`,
`TableQueryRequest`, and wire-visible
`TableQueryRequestMeta` with the row-identity
setting; add privileged identity-projection handler
that appends `__row_key_id__` for the `Append` mode
while preserving the ordinary `insert_required_column`
PBC PROJECT storage-column reject verbatim. Three
Wendelon-side fixtures (privileged projection
appends identity column; ordinary PBC PROJECT
still rejects; identity projection respects
predicate filters and tombstones).

No new SQL syntax in this ADR; no new IR variants;
no new aggregate kernel; no PBC opcode change; no
fragmenter detector change for SELECT paths; no
distributed DELETE coordinator routing (ADR 036
will mirror ADR 030 D13 distributed-INSERT reject
pattern); no new `StorageError` variant; no new
`RequiredColumnAxis` variant on Wendelon; no new
Wendelon RPC method; no `blake3` dependency; no
external allocator crate added; no storage reclaim
trait or helper introduced. ADR 029 / 030 / 031 /
032 / 033 / 034 invariants preserved (including
ADR 030 D7 Toron-side synthetic-column construction
prohibition extended to Toron mock identity layout
being opaque-not-cryptographic; ADR 034
`Catalog::drop_table` + `lower_ddl_table_name`
helper preserved). Existing SELECT corpus plan
digests byte-identical pre/post-ADR-035 (additive
substrate cycle carry-forward). The nine-phase
migration is materialized in
`plans/impl/035_row_identity_predicate_delete_substrate.md`.

ADR 036 selection is **firmly committed** by ADR
035 D15 sequencing commitment. SQL `DELETE WHERE`
as a narrow composition cycle:

- `LogicalPlan::Delete { table, predicate }` (or
  equivalent narrow leaf);
- binder validates predicate against target
  schema;
- coordinator builds the scan/filter candidate
  path using ADR 035 materializer;
- `StorageDeleteRows::delete_rows` executes local
  tombstones or Wendelon `table_delete`;
- local coordinator only in v1; distributed
  coordinator rejects before `fragment_plan`
  (mirrors ADR 030 D13 distributed-INSERT
  pattern);
- SQL row count maps to
  `DeleteRowsOutcome.deleted`.

**ADR 036 lead candidate (committed):**

- **ADR 036 — SQL `DELETE WHERE`.** Composition
  cycle over ADR 035 substrate. Expected 3-iter
  composition-cycle cadence matching ADR 032 /
  033 / 034. Pure-Toron (no Wendelon-side change;
  Wendelon `table_delete` adapter wired by ADR
  035). No new IR substrate; one new leaf IR
  variant (`LogicalPlan::Delete` +
  `ExecutionPlan::Delete`). Trilogy alternation
  hypothesis test continues: substrate (035) →
  composition (036).

**Code-evidence-driven candidates carried forward:**

- **`UPDATE WHERE`** — composition over ADR 035
  row-identity substrate + new partial-write
  contract. Sequence: ADR 035 → 036 (DELETE) →
  later UPDATE ADR. Composition cycle after
  partial-write substrate (which may or may not
  bundle).
- **`CREATE OR REPLACE TABLE`** — lower to `DROP
  IF EXISTS x; CREATE TABLE x AS SELECT ...` once
  DROP exists. Composition over ADR 029 / 033 /
  034. Small cycle; carries forward to a later
  composition slot.
- **`INSERT ... RETURNING` / `CTAS ... RETURNING`
  / `DELETE ... RETURNING`** — DML result-shape
  extension; modest user value until DEFAULT or
  GENERATED columns or sequences exist.
- **Cross-ring distributed-write-context ADR.**
  Lifts ADR 030 D13 + ADR 032 D1 + ADR 033 D11 +
  ADR 035 (no distributed DELETE; ADR 036 will
  mirror) distributed write rejects. ADR 034's
  Wendelon `drop_table` `AdminUnavailable` belongs
  to the metadata-write contract below.

**Substrate candidates carried forward:**

- **Storage reclaim ADR.** Physical file
  deletion, in-memory storage map cleanup,
  Wendelon descriptor deletion, tombstone
  collection; lifts ADR 034 D9 + ADR 035 D9
  carry-forward.
- **Stale-bound-plan invalidation ADR.**
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint
  substrate first.
- **Streaming / multi-batch `StorageWrite::write`
  / `StorageDeleteRows::delete_rows`** — lifts
  ADR 032 D5 + D6 + ADR 033 D8 + ADR 035 single-
  batch cap for bulk loads / deletes.
- **Write retries after descriptor evolution** —
  ADR 030 D12 + ADR 032 + ADR 033 + ADR 034 +
  ADR 035 carry-forward.
- **Read-your-writes session HLC advancement
  ADR.**
- **Per-statement `durability` override.**
- **Cross-ring metadata-write contract ADR.**
  Needed to lift `WendelonCatalog::create_table` +
  `WendelonCatalog::drop_table` from
  `AdminUnavailable`; engages transactional CTAS +
  atomic DROP.
- **Richer Toron `Hlc` type** — prerequisite for
  read-your-writes.
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **Hidden-aggregate `ORDER BY` keys ADR.**
- **HAVING alias visibility ADR.**
- **Non-aggregate computed `ORDER BY` ADR.** ADR
  027 D10 deferred.
- **Temporal column types (DATE, TIMESTAMP)** for
  ADR 029 / 030 / 032 / 033 type lattices.
- **DECIMAL** for ADR 029 / 030 / 032 / 033
  type lattices.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`ALTER TABLE` / `CREATE INDEX` / `CREATE VIEW`
  / `DROP VIEW` / `DROP SCHEMA` / `DROP INDEX`** —
  DDL siblings using the renamed
  `lower_ddl_table_name` helper pattern from ADR
  034 D4.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED columns.
- **Wendelon AGG consumer wiring** — cross-ring;
  PBC partial-pushdown.
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).

**Code-evidence-driven candidates (substrate-debt):**

- **Plan Arena / index-based plan IR.** Brainstorm
  004 broader framing; ADR 032 + 033 + 034 added
  a combined ~33 walker leaf arms (~11 per cycle);
  ADR 035 added zero walker arms (substrate cycle
  was additive — new types / trait / module).
  Pressure is growing but still bounded; ADR 036
  will likely add one leaf variant (`Delete`)
  matching ADR 034 R1 carry-forward shape.
- **Typed expression tape / opcode interpreter.**
  Brainstorm 004 deeper realization over ADR
  031's `BoundExprProgram`.
- **Common subexpression elimination (CSE) +
  literal deduplication.** ADR 031 D8 carry-
  forward.
- **Cranelift JIT compilation.** Brainstorm 001.
- **Logical `Expr` arena migration.** Brainstorm
  004 broader framing.
- **PBC partial-pushdown of computed-arg
  aggregates** — cross-ring; workload-evidence-
  pressured.

**Code-evidence-driven candidates carried forward:**

- **`CREATE OR REPLACE TABLE`** — lower to `DROP
  IF EXISTS x; CREATE TABLE x AS SELECT ...` once
  DROP exists. Composition over ADR 029 / 033 /
  034. Small cycle; carries forward to a later
  composition slot.
- **`UPDATE WHERE`** — composition over ADR 035
  row-identity substrate + new partial-write
  contract. Sequence: ADR 035 → 036 (DELETE) →
  later UPDATE ADR.
- **`INSERT ... RETURNING` / `CTAS ... RETURNING`
  / `DELETE ... RETURNING`** — DML result-shape
  extension; modest user value until DEFAULT or
  GENERATED columns or sequences exist.
- **Cross-ring distributed-write-context ADR.**
  Lifts ADR 030 D13 + ADR 032 D1 + ADR 033 D11
  distributed write rejects. ADR 034's Wendelon
  `drop_table` `AdminUnavailable` belongs to the
  metadata-write contract below.

**Substrate candidates carried forward:**

- **Storage reclaim ADR.** Physical file
  deletion, in-memory storage map cleanup,
  Wendelon descriptor deletion; lifts ADR 034 D9
  carry-forward.
- **Stale-bound-plan invalidation ADR.**
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint
  substrate first.
- **Streaming / multi-batch `StorageWrite::write`**
  — lifts ADR 032 D5 + D6 + ADR 033 D8 single-
  batch cap for bulk loads.
- **Write retries after descriptor evolution** —
  ADR 030 D12 + ADR 032 + ADR 033 + ADR 034
  carry-forward.
- **Read-your-writes session HLC advancement
  ADR.**
- **Per-statement `durability` override.**
- **Cross-ring metadata-write contract ADR.**
  Needed to lift `WendelonCatalog::create_table` +
  `WendelonCatalog::drop_table` from
  `AdminUnavailable`; engages transactional CTAS +
  atomic DROP.
- **Richer Toron `Hlc` type** — prerequisite for
  read-your-writes.
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **Hidden-aggregate `ORDER BY` keys ADR.**
- **HAVING alias visibility ADR.**
- **Non-aggregate computed `ORDER BY` ADR.** ADR
  027 D10 deferred.
- **Temporal column types (DATE, TIMESTAMP)** for
  ADR 029 / 030 / 032 / 033 type lattices.
- **DECIMAL** for ADR 029 / 030 / 032 / 033
  type lattices.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`ALTER TABLE` / `CREATE INDEX` / `CREATE VIEW`
  / `DROP VIEW` / `DROP SCHEMA` / `DROP INDEX`** —
  DDL siblings using the renamed
  `lower_ddl_table_name` helper pattern from ADR
  034 D4.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED columns.
- **Wendelon AGG consumer wiring** — cross-ring;
  PBC partial-pushdown.
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).

**Code-evidence-driven candidates (substrate-debt):**

- **Plan Arena / index-based plan IR.** Brainstorm
  004 broader framing; ADR 032 + 033 + 034 added
  a combined ~33 walker leaf arms (~11 per cycle);
  pressure is growing but still bounded.
- **Typed expression tape / opcode interpreter.**
  Brainstorm 004 deeper realization over ADR
  031's `BoundExprProgram`.
- **Common subexpression elimination (CSE) +
  literal deduplication.** ADR 031 D8 carry-
  forward.
- **Cranelift JIT compilation.** Brainstorm 001.
- **Logical `Expr` arena migration.** Brainstorm
  004 broader framing.
- **PBC partial-pushdown of computed-arg
  aggregates** — cross-ring; workload-evidence-
  pressured.

The archived selection notes below preserve forward-
sequence content from prior planning passes.

#### ADR 034 → ADR 035 archived selection notes

ADR 034 selected and promoted **SQL `DROP TABLE`
(Local DDL Deletion)** — Toron's **third composition
ADR** after ADR 032 + 033, completing the post-
substrate composition trilogy. Narrow metadata-only
DDL deletion: removes catalog entries via
`Catalog::drop_table`; no row deletion, no storage
reclaim, no Wendelon metadata administration.

Sixteen decisions D1-D16 cover: narrow `DROP TABLE
[IF EXISTS] table_name` SQL surface (CASCADE /
explicit RESTRICT / PURGE / TEMPORARY / multi-name /
non-table object types all rejected at planner);
exhaustive `Statement::Drop` destructure (no `..`);
DDL name helper renamed `lower_create_table_name` →
`lower_ddl_table_name` with CREATE / CTAS / DROP all
calling the neutral helper; leaf IR variants with no
pre-drop table identity (mirrors ADR 033 R1
symmetry); binder pass-through with no catalog
calls; new `Catalog::drop_table` trait method
returning `Result<Option<TableBinding>, CatalogError>`
with clean `IF EXISTS` semantics +
`AdminUnavailable` for Wendelon known tables;
`MockCatalog::drop_table` exact alias cleanup using
`by_ref.retain` with no alias repointing + no
`next_table_id` reuse; no storage reclaim guarantee;
coordinator routing via shared `execute_catalog_plan`
helper extended from ADR 029; defensive executor +
fragmenter rejects with new
`FragmentError::DropTableNotFragmentable`; distinct
telemetry label `"drop_table"`; CTAS cleanup
composition fixture required; ADR 035 sequencing
commitment for the row-identity / predicate-delete
substrate cycle; Plan Arena remains named debt.

Three iterations / 0 flip-flops — third consecutive
composition cycle at the 3-iter cadence matching
ADR 032 + 033. Iter-0 produced a topic split
(Claude + Codex on DROP TABLE vs Gemini on DELETE
WHERE) that resolved decisively at iter-1 when
Gemini engaged Codex iter-0's E1 code-evidence
audit and retracted. Iter-1 settled the load-
bearing shape via Codex iter-1's D1-D16 absorbed
wholesale by Claude / Gemini iter-1. Iter-2 was
promotion-drafting.

Gemini iter-1/2 retraction streak restored at
7-of-9 after the 6-of-8 plateau. Claude iter-0 C5
prediction confirmed verbatim — first cycle in
council history where iter-0 process predictions
came true exactly. Seventh consecutive cycle of
Codex correctness-clarification pattern.
Topic-concession-compresses-next-cycle pattern back
at 4-of-6 (ADR 034 → 035 generates the row-identity
/ predicate-delete substrate seed via D15).

Seven Claude iter-0 / iter-1 contributions promoted
(iter-0 C2 → D13 telemetry label; iter-0 C4 → D15
ADR 035 sequencing; iter-0 C1 → D14 CTAS cleanup
test; iter-0 Q7 → architecture guard #16; iter-1 C1
→ architecture guard #17 catalog narrowness; iter-1
C2 → D4 DDL helper rename; iter-1 A3 → test-file
naming convention codified). Composition trilogy
complete (ADR 029 → 030 → 031 substrate trilogy;
ADR 032 → 033 → 034 composition trilogy over that
substrate; ADR 035 starts new substrate phase).

Sixteenth instance of the ADR 015 cycle. 17 ADR-
034-specific architecture guards + 1 paired
adjustment = 18 architecture guard touchpoints. 11
required functional fixtures in new
`crates/toron/tests/drop_table_test.rs` (Claude
iter-1 A3 file-naming convention).

No new SQL syntax beyond bare `DROP TABLE [IF
EXISTS] table_name`; no new IR expression variant;
no new aggregate kernel; no PBC opcode change; no
fragmenter detector change for SELECT paths; no
cross-ring engagement; no Wendelon-side change; no
new `StorageWrite` overload; no row-count or
RETURNING result; no `blake3` dependency; no
external allocator crate added; no storage reclaim
trait or helper introduced. ADR 029 / 030 / 031 /
032 / 033 invariants preserved.

The nine-phase migration is materialized in
`plans/impl/034_drop_table.md`.

#### ADR 033 → ADR 034 archived selection notes

ADR 033 selected and promoted **SQL `CREATE TABLE AS
SELECT` (Local DDL + DML Composition)** — Toron's
**second composition ADR** after ADR 032. Completes the
local DDL/DML composition loop: ADR 029 creates a
catalog table from a declared schema; ADR 030 writes
literal rows via `StorageWrite`; ADR 032 writes SELECT
output into an existing table; ADR 033 creates a table
whose schema is inferred from the SELECT and writes the
SELECT output in a single statement.

Fourteen decisions D1-D14 + ten Codex R-items (R1-R7
at iter-0; R1-R3 at iter-1) cover: separate IR variants
alongside (not replacing) ADR 029 / 030 / 032 variants;
no pre-create table identity in the bound CTAS variant
(binding cannot mutate catalog; coordinator obtains
binding from `create_table`); narrow v1 SQL surface (no
column rename list, no `IF NOT EXISTS`, no `OR REPLACE`
/ `LIKE` / `CLONE` / constraints); schema inferred
verbatim from bound source SELECT output (names, types,
nullability) and validated against ADR 029 lattice +
duplicate-name reject + Wendelon synthetic-name reject;
source binds before catalog mutation (so missing-source-
table failures don't leave phantom catalog entries);
coordinator sequence catalog.create_table → execute_bound(source,
None) → materialize_insert_select_batch (Some) /
materialize_empty_record_batch (None) → single
writer.write → Ok(Vec::new()); non-atomic catalog-then-
write explicitly documented as v1 limitation; empty
source creates queryable empty table via new sibling
helper (ADR 032's helper contract preserved); Wendelon
synthetic constants reused directly or via neutral
helper with no forked literals; no source-to-target
type coercion (target
schema IS source schema by construction); local-
coordinator-only with three independent distributed
reject sites; binder optimizer may descend into source
while distributed walkers treat CTAS as leaf; SELECT
plan-digest stability resumes at post-ADR-032 baseline.

Three iterations / 0 flip-flops — **ties ADR 032 as the
fastest cycle in council history**. **First three-way
independent iter-0 topic convergence** in council
history: Claude / Codex / Gemini all picked CTAS as
lead at iter-0 with no topic dissent or iter-1 topic
reopen. Three factors drove the speed:

1. **Three-way iter-0 topic convergence.** No iter-1
   topic-reopen needed.
2. **Pure-Toron and pure-local.** No Wendelon-side
   discovery pass; no distributed-write-context
   threading; ADR 029 catalog path + ADR 032
   materialization helper directly reused.
3. **Codex iter-0 R1-R7 absorbed wholesale.** Claude
   iter-1 and Gemini iter-1 ratified Codex iter-0's
   seven precision items without amendment; Codex
   iter-1 added three more R-items at the coordinator-
   ordering and synthetic-name discipline layer.

**Sixth consecutive cycle of Codex correctness-
clarification pattern** — 10 R-items total across
iter-0 (R1 no pre-create identity, R2 bind source
before catalog mutation, R3 no v1 column list, R4 no v1
`IF NOT EXISTS`, R5 schema validation with lattice +
name rules, R6 empty source initializes queryable
table, R7 non-atomicity acceptable if named) + iter-1
(R1 create-before-source-execution intentional, R2
Wendelon constants reused without forked literals, R3
no coercion in CTAS). Each surfaced from
code-evidence audit of catalog / binder / coordinator
boundary.

**First cycle where Claude iter-0 was retracted four-
for-four on load-bearing v1-surface defaults** — D3
(column rename list accepted), D5 (all-nullable target),
D7 (atomicity wording), D8 (empty-source no-write).
Codex iter-0 R3 / R5 / R4 / R6 corrected each. All
four retracted cleanly at iter-1 without further
reopen. Recorded as cycle property: **composition
cycles over substrate-rich foundations have more
subtle code-evidence constraints than substrate cycles**
because the substrate already introduced the carriers,
helpers, and contracts that the composition layer must
respect. Iter-0 design sketches at the composition
layer benefit from deferring v1-surface defaults to
iter-1 code-evidence audits.

**Gemini iter-1/2 retraction streak still broken at
6-of-8.** Second consecutive composition cycle (032 +
033) where Gemini did not propose broader-scope framings
(no Plan Arena push, no streaming `StorageWrite` push,
no distributed-write-context push). Updated cycle
property after two composition-cycle data points:
**broader-scope framings are a substrate-cycle
phenomenon, not a universal cycle phenomenon.**

**Topic-concession-compresses-next-cycle pattern still
broken at 3-of-5.** Second consecutive composition cycle
generating no iter-0 alt-topic seed. Updated cycle
property: **seed-carry-forward is also substrate-cycle-
specific.**

**Second consecutive zero-substrate-invariant-break
cycle.** ADR 032 + ADR 033 are both composition cycles.
Pattern: composition cycles are zero-break by
construction. Two data points.

**Three Claude iter-1/iter-2 contributions promoted:**

- iter-1 C1 empty-source sibling helper shape → D8 +
  new helper `materialize_empty_record_batch` +
  architecture guard #13.
- iter-1 C4 + iter-2 fixture additions → 4 required
  composition fixtures (A SUM / B JOIN / C empty / D
  failure-before-catalog).
- iter-2 A1 strict-pin guard wording for synthetic-
  name reject → architecture guard #7 negative-
  pattern enforcement against forked `__row_*__`
  literals.

Iter-1 C2 ordering pins were absorbed into Codex iter-1
architecture guards #6 + #11; iter-2 A2 (`ctas_test.rs`
file adoption), A3 (helper-reuse table for implementer),
A4 (variant-count guard pairing) were folded into the
impl plan.

Fifteenth instance of the ADR 015 cycle. Eighteen new
ADR 033 architecture guards + one paired adjustment to
the existing
`architecture_guards_unchanged_plus_sort_variant`
variant-count catch-all = 19 architecture guard
touchpoints. SoA-readiness invariants preserved.
Brainstorm-seam touch check: 001 + 002 + 004 untouched;
003 touched but preserved (one new distributed reject
arm).

No new SQL syntax beyond bare `CREATE TABLE name AS
<query>`; no new IR expression variant; no new aggregate
kernel; no PBC opcode change; no fragmenter detector
change for SELECT paths; no cross-ring engagement; no
Wendelon-side change; no `RecordBatch` field added to
any plan variant; no streaming `StorageWrite` overload;
no row-count or RETURNING result; no `blake3`
dependency; no external allocator crate added; no
source-to-target type coercion at CTAS layer. ADR 029
/ 030 / 031 / 032 invariants preserved. Existing SELECT
corpus plan digests byte-identical pre/post-ADR-033
(D14 re-stabilization). The eight-phase migration is
materialized in
`plans/impl/033_create_table_as_select.md`.

ADR 034 selection opens a new space. The local DDL/DML
composition loop is now complete:

- CREATE TABLE with declared schema (ADR 029);
- INSERT VALUES literal rows (ADR 030);
- INSERT INTO ... SELECT from query output (ADR 032);
- CREATE TABLE AS SELECT with inferred schema (ADR 033).

The next composition layer is data-modifying DML
(UPDATE / DELETE) or result-shape extension (RETURNING).
The next substrate layer is cross-ring distributed-
write-context (lifts ADR 030 D13 + ADR 032 D1 + ADR 033
D11) or DROP TABLE (enables OR REPLACE for CTAS and
rollback-after-write-failure semantics).

**ADR 034 lead candidates (no firm commitment):**

- **`DELETE WHERE`** — completes the basic CRUD set
  (CREATE / INSERT / READ / DELETE); engages
  `__row_tombstone__` substrate Wendelon already owns
  per ADR 030 D7; establishes the row-identification
  predicate substrate UPDATE will need. Substrate-
  introducing-ish (predicate carrier is new). Likely
  3-4 iterations with one flip-flop on predicate
  shape.
- **`DROP TABLE`** — first DDL deletion ADR;
  prerequisite for `OR REPLACE` and for any rollback-
  on-write-failure CTAS extension; engages catalog
  mutation semantics. Pure composition over ADR 029
  catalog path. Likely 3-iteration cycle.
- **Cross-ring distributed-write-context ADR** —
  lifts ADR 030 D13 + ADR 032 D1 + ADR 033 D11
  distributed rejects. Threads tenant / session / HLC
  context through `DistributedQueryCoordinator`.
  Cross-ring substrate work; two-pass Wendelon-API
  discovery budget required. Slower cycle (4+ iters).
  No current workload evidence pressuring it.
- **`INSERT ... RETURNING` / `CTAS ... RETURNING`** —
  DML result-shape extension; shifts ADR 030 / 032 /
  033 `Ok(Vec::new())` contract. Composes naturally
  with ADR 032 / 033 materialize-before-write paths.
  Modest user value until DEFAULT / GENERATED columns
  or sequences exist.

**Substrate candidates carried forward:**

- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023 reject; narrow composition over ADR 028
  signature substrate.
- **HAVING alias visibility ADR.** Loosens ADR 023
  reject; pairs with hidden-aggregate ORDER BY.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **`UPDATE WHERE`** — DML sibling to DELETE; needs
  row-identification predicate substrate (DELETE
  establishes) + new partial-write contract.
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint substrate
  first.
- **Streaming / multi-batch `StorageWrite::write`** —
  lifts ADR 032 D5 + D6 + ADR 033 D8 single-batch cap
  for bulk loads.
- **Write retries after descriptor evolution** — ADR
  030 D12 + ADR 032 + ADR 033 carry-forward.
- **Read-your-writes session HLC advancement ADR.**
- **Per-statement `durability` override.**
- **Cross-ring metadata-write contract ADR** — needed
  to lift `WendelonCatalog::create_table` from
  `AdminUnavailable`; engages transactional CTAS.
- **Richer Toron `Hlc` type** — prerequisite for
  read-your-writes.
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **Temporal column types (DATE, TIMESTAMP)** for
  ADR 029 / 030 / 032 / 033 type lattices.
- **DECIMAL** for ADR 029 / 030 / 032 / 033 type
  lattices.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`ALTER TABLE` / `CREATE INDEX` / `CREATE VIEW`**
  — DDL siblings to ADR 029 / 033.
- **Constraints** (PRIMARY KEY / FOREIGN KEY / CHECK
  / column-level UNIQUE) / DEFAULT / GENERATED
  columns.
- **Wendelon AGG consumer wiring** — cross-ring;
  PBC partial-pushdown.
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).

**Code-evidence-driven candidates carried forward:**

- **Typed expression tape / opcode interpreter** —
  Brainstorm 004 deeper realization over ADR 031's
  `BoundExprProgram`.
- **Common subexpression elimination (CSE) + literal
  deduplication** — ADR 031 D8 carry-forward.
- **Cranelift JIT compilation** (Brainstorm 001).
- **Logical `Expr` arena migration** — Brainstorm
  004 broader framing.
- **Plan Arena / index-based plan IR** — Brainstorm
  004 broader framing; ADR 032 + 033 added a combined
  ~22 walker leaf arms; pressure is growing but still
  bounded.
- **PBC partial-pushdown of computed-arg aggregates**
  — cross-ring; workload-evidence-pressured.

The archived selection notes below preserve forward-
sequence content from prior planning passes.

#### ADR 032 → ADR 033 archived selection notes

ADR 032 selected and promoted **SQL `INSERT INTO ...
SELECT` (Local Read-Write Composition)** — Toron's
**first composition ADR** after the ADR 024 → ADR 031
substrate arc (eight consecutive substrate-introducing
/ substrate-downstream / substrate-replacing cycles).
Adds local `INSERT INTO ... SELECT` by composing ADR
029 catalog identity, ADR 030 `StorageWrite`, ADR 031
`BoundExprProgram` inside the bound source SELECT, ADR
025 strict cast kernels for array coercion, and the
existing SELECT execution pipeline — without widening
the VALUES insert path, without distributed write
execution, and without new expression or storage APIs.

Thirteen decisions D1-D13 cover: local-coordinator-
only execution with three independent reject sites
(distributed coordinator, fragmenter, defensive
executor); Shape A separate IR variants
(`LogicalPlan::InsertSelect` + `ExecutionPlan::InsertSelect`)
alongside (not replacing) ADR 030 VALUES `Insert`
variants; positional source-to-target mapping with a
bound `target_columns: Vec<ColumnOrdinal>` carrier in
source-output order; shared
`resolve_insert_target_columns` binder helper extracted
from `bind_insert`; binder-time arity + type-
admissibility validation via `is_insert_select_coercible`
mirroring ADR 025 strict-cast lattice; materialize-
before-write coordinator path with `Executor::execute_bound(source,
None)` + new `materialize_insert_select_batch` private
helper in `coordinator/write.rs`;
`TABLE_WRITE_MAX_ROWS = 65_536` total-row cap;
empty-source `Ok(Vec::new())` short-circuit without
calling the writer; strict ADR 025 cast (no TRY_CAST)
for numeric coercion; runtime null-count enforcement
against non-null target fields (not bind-time schema
nullability); post-ADR-031 SELECT plan-digest baseline
re-stabilized; distributed / scheduler / server
metadata walkers treat `InsertSelect` as a leaf with
one documented binder-optimizer exception
(`optimize_for_required` descends with `required_output
= None` so source scan-projection pushdown survives);
outer morsels do not partition DML writes (`None`
morsel argument); `crate::exec::evaluate_cast`
visibility widens to `pub(crate)` as the only cast-
helper exposure (`evaluate_try_cast` stays private).

Two substantive iterations + one promotion-drafting
pass = **3 total with 0 flip-flops** — **fastest
composition convergence to date** (vs ADR 031's
3 + 1 ratification = 4 total). Three factors drove
the speed: first composition ADR after the substrate
arc (no new substrate added; only two plan-IR
variants + one coordinator helper + one binder
helper + four reject sites); pure-Toron and pure-
local (no Wendelon-side discovery pass; no
distributed-write-context threading; ADR 030 D13
distributed reject pattern directly mirrored); Codex
iter-1 absorbed Claude iter-0 C1-C4 + Q1-Q4
resolutions wholesale (plan-digest re-stabilization
C1 → D10; strict-cast C2 → D8; walker leaf-treatment
C3 → D11 with R2 refinement; aggregate composition
fixture C4 → Required Fixture A).

**Two cycle-property breaks recorded:**

- **Gemini iter-1/2 retraction streak broken at 6-of-
  7** (was 6-of-6 across ADR 025 / 026 / 028 / 029 /
  030 / 031). Gemini iter-1 converged directly with
  Codex iter-1 shape without proposing broader-scope
  framings (e.g., streaming `StorageWrite`,
  distributed write context in same ADR) that iter-
  2/3 needed to retract. Updated cycle property:
  broader-scope framings are a **substrate-cycle
  phenomenon**, not a universal cycle phenomenon.
- **Topic-concession-compresses-next-cycle pattern
  broken at 3-of-3** (was 3-of-3 at ADR 020→021,
  027→028, 029→031). ADR 031 → 032 transition did
  not generate an iter-0 alt-topic seed (ADR 031's
  "ADR 032 candidates" list was generated forward,
  not as iter-0 dissent). Updated cycle property:
  topic-concession seed pattern is also a substrate-
  cycle phenomenon.

Both breaks recorded as cycle-property refinements,
not regressions. Composition cycles may not provoke
the same broader-scope and seed-carry-forward
tendencies that substrate cycles do.

**First zero-invariant-break cycle since ADR 028.**
ADR 029 added new substrate (additive, zero break);
ADR 030 added new substrate (additive, zero break);
ADR 031 broke plan-digest invariant by substrate-
replacing construction (D14, ratified); ADR 032
composes and re-stabilizes (zero break + D10 re-
stabilizes the post-ADR-031 SELECT plan-digest
baseline). Pattern: composition cycles are zero-break
by construction.

**Three Claude iter-0 / iter-1 contributions
promoted directly to D-level decisions:**

- iter-0 C1 (plan-digest re-stabilization) → D10
- iter-0 C2 (strict-cast ADR 025 kernels) → D8
- iter-1 C5 (`evaluate_cast` visibility guard) → D13
  + `adr032_coordinator_uses_only_evaluate_cast`
  architecture guard

Plus iter-0 C3 (walker leaf-treatment) → D11 with
Codex R2 refinement (binder optimizer exception);
iter-0 C4 (aggregate composition fixture) → Required
Fixture A (SUM grouped aggregate); iter-1 C6 (source-
arity guard wording) → `adr032_binder_enforces_source_target_arity`
architecture guard; iter-1 C7 (JOIN composition
fixture elevation) → Required Fixture B (INNER JOIN).
Healthy collaboration sign — the council is not
single-channel even in fast-converging composition
cycles. Codex iter-1 contributed R1 (`target_columns`
carrier), R2 (binder-may-descend exception), R3
(strict-cast not TRY_CAST), R4 (runtime null-check
vs schema null-check), R5 (outer morsels do not
partition DML) — all non-obvious refinements Claude
iter-0 missed.

Fourteenth instance of the ADR 015 cycle. Fourteen
ADR-032-specific architecture guards, plus in-place
updates to existing LogicalPlan / ExecutionPlan
variant-count coverage. Two required end-to-end
composition fixtures for Definition of Done: Fixture
A (grouped aggregate source — SUM) exercises ADR 028
signature substrate + ADR 031 `BoundExprProgram`
aggregate evaluation; Fixture B (INNER JOIN source)
exercises ADR 008 / 009 join substrate + explicit
target column list reorder via D3 `target_columns`
carrier. One fixture alone leaves either substrate
path uncovered; both required.

SoA-readiness invariants preserved (no kernel added;
centralized dispatch unchanged; no row-path dyn
dispatch). Brainstorm-seam touch check: 001
(Cranelift JIT) untouched; 002 (WeakControlRing)
untouched; 003 (Disaggregated Compute) touched but
preserved (one new distributed reject arm; no
fragmentation work added); 004 (Memory Arenas)
untouched.

No new SQL syntax beyond the bare `INSERT INTO ...
<select-statement>` form; no new IR expression
variant; no new aggregate kernel; no PBC opcode
change; no fragmenter detector change for SELECT
paths; no cross-ring engagement; no Wendelon-side
change; no `RecordBatch` field added to any plan
variant; no streaming `StorageWrite` overload; no
row-count or RETURNING result; no `blake3`
dependency; no external allocator crate added; no
`Eq` / `Hash` derives on program-bearing types. ADR
029 catalog metadata-only preserved; ADR 030
`ExecutionPlan::Insert` expression-free preserved;
ADR 031 `BoundExprProgram` `PartialEq`-only
preserved; `StorageWrite` trait shape unchanged.
Existing SELECT corpus plan digests byte-identical
pre/post-ADR-032 (D10 re-stabilization). The nine-
step migration is materialized in
`plans/impl/032_insert_into_select.md`.

ADR 033 selection opens a new space. The composition
arc has begun: ADR 032 was the first composition
ADR over the ADR 024 → 031 substrate. Future
composition ADRs continue the pattern; substrate-
introducing / -replacing work returns when workload
evidence pressures specific substrate gaps. The
foundational substrate remains
(`BoundExprProgram` for physical execution;
`StorageWrite` for data-plane writes; `Catalog`
metadata-only with mutation discipline; ADR 028
signature substrate for grouped aggregates; ADR 029
six-type lattice for DDL; ADR 030 `InsertLiteral`
lattice for DML; ADR 032 InsertSelect carrier for
read-write composition).

**ADR 033 lead candidates (no firm commitment):**

- **`CREATE TABLE AS SELECT (CTAS)`** — natural
  composition extension after ADR 032. Combines ADR
  029 DDL with ADR 032 read-write composition in one
  statement instead of two. Reuses ADR 032's
  `materialize_insert_select_batch` for the data path
  and ADR 029's catalog `create_table` for the
  schema path; the new contract is whether the
  schema is inferred from the SELECT output or
  explicitly declared.
- **Cross-ring distributed-write-context ADR** —
  lifts ADR 030 D13 + ADR 032 D1 distributed
  rejects. Threads tenant / session / HLC context
  through `DistributedQueryCoordinator`. Cross-ring
  ADR; budget two-pass Wendelon-API discovery per
  the post-ADR-030 cycle property.
- **`UPDATE` / `DELETE`** — DML siblings to ADR 030
  / 032. `__row_tombstone__` becomes load-bearing
  for DELETE; Wendelon-side already owns tombstone
  stamping per ADR 030 D7. `UPDATE` needs row-
  identity-preserving write semantics that ADR 030
  / 032 do not yet handle.
- **`INSERT ... RETURNING` clause** — DML result-
  shape extension; shifts ADR 030 / 032
  `Ok(Vec::new())` contract to emit row counts and/
  or projected columns. Composes naturally with
  ADR 032's materialize-before-write path.

**Substrate candidates carried forward:**

- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023 reject.
- **HAVING alias visibility ADR.** Loosens ADR 023
  reject.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint substrate
  first.
- **Streaming / multi-batch `StorageWrite::write`**
  — lifts ADR 032 D5 + D6 single-batch cap for bulk
  loads.
- **Write retries after descriptor evolution** —
  ADR 030 D12 + ADR 032 carry-forward.
- **Read-your-writes session HLC advancement ADR**
  — defines whether coordinators consume
  `wendelon_client::TableWriteReceipt.commit_hlc`.
  Touches `ExecutionContext`, distributed retry
  contracts, and plan-digest stability if HLC
  bounds enter binding.
- **Per-statement `durability` override** — ADR 030
  / 032 use wrapper default `DurabilityClass::QUORUM`.
- **Cross-ring metadata-write contract ADR** —
  needed to lift `WendelonCatalog::create_table`
  from `AdminUnavailable`.
- **Richer Toron `Hlc` type** — `HlcBound(pub u64)`
  is lossy vs Wendelon's `HybridTimestamp` (16
  bytes); prerequisite for read-your-writes.
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **Temporal column types (DATE, TIMESTAMP)** for
  ADR 029 type lattice + ADR 030 / 032 coercion
  lattice.
- **DECIMAL** for ADR 029 type lattice + ADR 030 /
  032 coercion lattice.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW`** — DDL siblings to ADR 029.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED columns — DDL siblings to ADR 029;
  `DEFAULT` interacts with ADR 030 / 032's
  "reject omitted NOT NULL" rule.
- **Wendelon AGG consumer wiring** — cross-ring;
  PBC partial-pushdown.
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).

**Code-evidence-driven candidates carried forward:**

- **Typed expression tape / opcode interpreter** —
  Brainstorm 004's deeper realization over ADR 031's
  `BoundExprProgram`. Lowers programs to a flat
  typed-instruction tape with kernel dispatch on
  `(op, output_type)`. Substrate for future
  SIMD/JIT.
- **Common subexpression elimination (CSE) + literal
  deduplication** — both deferred from ADR 031 D8.
- **Cranelift JIT compilation** (Brainstorm 001) —
  lower `BoundExprProgram` or future tape to native
  code for hot kernels.
- **Logical `Expr` arena migration** — Brainstorm
  004's broader framing.
- **PBC partial-pushdown of computed-arg aggregates**
  (cross-ring; workload-evidence-pressured).

The archived selection notes below preserve forward-
sequence content from prior planning passes.

#### ADR 031 → ADR 032 archived selection notes

ADR 031 selected and promoted **Bound Expression
Program (Index-Based Physical Expression IR)** —
Toron's **first substrate-replacing pure-Toron ADR**
— replacing recursive `Box<ExecutionExpr>` trees in
`ExecutionPlan` physical-expression fields with a
serializable index-based `BoundExprProgram` that owns
its roots, enforces topological ordering by
construction, and evaluates via a flat linear loop.
Narrow realization of Brainstorm 004's expression-
representation split; logical `Expr` remains the
boxed-readable SQL planner contract.

Fifteen decisions D1-D15 cover: scope as bound
physical expressions only; self-contained multi-root
`BoundExprProgram { nodes, node_types, literals,
roots }`; stable typed `BoundExprId(pub u32)` with
`Eq + Hash`; mandatory topological invariant (child
ID < parent ID); compact grouped operation enums
(`BoundBinaryOp` with 12 variants, `BoundUnaryOp`
with 3, `BoundExprNode` with 7); sidecar
`node_types[id]` authoritative for execution
dispatch; `PartialEq`-only discipline for program-
bearing types; no literal dedup or CSE in v1;
aggregate role boundaries kept separate
(`group_expr` is one program, `aggr_expr` is
`Vec<BoundAggregateCall>`); `ExecutionPlan::Filter` /
`Project` / `Aggregate` / `Exchange` carry programs;
binder emits programs directly via
`BoundExprProgramBuilder` with no production
lowering pass; PBC migration in scope with runtime-
asserted single-root predicate contract; narrow
column-key helpers keep distributed paths column-
oriented; **D14 plan-digest serde break — first
cycle to intentionally break a previously-promoted
invariant** (ADR 023 D15 + ADR 029 D15 + ADR 030
D17 byte-identical plan-digest invariant explicitly
broken); ADR 028 logical signature substrate
untouched.

Three substantive iterations + one ratification pass
= **4 total with 0 flip-flops** — fastest
convergence in the council's history. Two factors
drove the speed: third-instance topic-concession-
compresses-next-cycle pattern (Codex's iter-0 ADR
029 `BoundExprProgram` seed doc drove this cycle's
iter-0 directly; topic never reopened), and first
substrate-replacing not-cross-ring ADR (ADR 029 +
030 needed two-pass Wendelon-API discovery; ADR
031 is pure Toron, no Wendelon-side discovery).

**Sixth consecutive cycle of Gemini iter-1/2
retraction** (ADR 025 / 026 / 028 / 029 / 030 /
031): Gemini iter-2/3 retracted three iter-1
positions (roots as side-fields on plan variants;
PBC migration out of scope; `Eq` derivation on
`BoundExprNode`). Stable cycle property: Gemini's
wider-scope iter-1/2 framings surface the alternate-
design space the council needs to reject explicitly;
useful for collective understanding but not load-
bearing for promotion.

**Four Claude iter-0/1 contributions
promoted directly to D-level decisions** — first
cycle where iter-0/1 contributions reach promotion
without iter-2/3 wordsmithing or retraction:

- iter-0 C1 (plan-digest break as cycle property)
  → D14
- iter-0 C2 (ADR 028 signature substrate untouched)
  → D15
- iter-1 C5 (PBC single-root contract + signature)
  → D12
- iter-1 C6 (compact `BoundBinaryOp` / `BoundUnaryOp`
  grouping) → D5

Plus iter-0 C3 (architecture-guard replacement
plan) + iter-2 P1/P2/P3 (line-anchor precision +
`source_lines` recursive scan + plan-digest fixture
inventory) went directly into the impl plan. New
cycle property: code-evidence-grounded + narrowly-
scoped iter-0 contributions reach promotion intact.

Thirteenth instance of the ADR 015 cycle. Ten new
ADR 031 architecture guards + six adjusted ADR
024/025/026/028 guards (anchored at
`architecture_test.rs:558, 914, 994-1286, 1260-1500,
1838`; no guard silently deleted) + two verify-
unchanged guards (kernel catalog at line 665, ADR
028 signature at line 1611) = 18 architecture guard
touchpoints. SoA-readiness invariants preserved.
Brainstorm-seam touch check: Brainstorm 001 + 003
seams cleaner (JIT input is now index-based; wire
serde changes but distributed routing semantics
preserved); Brainstorm 002 untouched; Brainstorm
004 narrowly realized (one of four candidate
approaches; the other three remain future
candidates).

No new SQL syntax; no new expression operations; no
new aggregate kernel; no PBC opcode change; no
fragmenter detector change for SELECT paths; no
cross-ring engagement; no Wendelon-side change; no
`blake3` / `bumpalo` / `typed-arena` / `slab` /
`generational-arena` dependency added; no `Eq` /
`Hash` derives on program-bearing types. ADR 028
signature substrate preserved; ADR 029 catalog
metadata-only preserved; ADR 030
`ExecutionPlan::Insert` expression-free preserved.
SQL result digests byte-identical pre/post-ADR-031
for the SELECT corpus (plan-digest fixtures
categorized and adjusted per Phase 9 inventory). The
ten-phase migration is materialized in
`plans/impl/031_bound_expr_program.md`.

ADR 032 selection opens a new space. ADR 029 / 030 /
031 closed the substrate-introducing + substrate-
replacing arc that started at ADR 024. The
foundational substrate is now in place
(`BoundExprProgram` for physical execution;
`StorageWrite` for data-plane writes; `Catalog`
metadata-only with mutation discipline; ADR 028
signature substrate for grouped aggregates; ADR 029
six-type lattice for DDL; ADR 030 `InsertLiteral`
lattice for DML). ADR 032 candidates fall into
several categories: composition ADRs that exercise
the substrate (`INSERT INTO ... SELECT`,
`UPDATE` / `DELETE`, hidden-aggregate `ORDER BY`,
HAVING alias visibility); type-lattice extensions
(temporal types, DECIMAL, Utf8/Boolean CAST/TRY_CAST);
cross-ring contract ADRs (metadata-write for
`WendelonCatalog::create_table`; distributed-write
context for ADR 030 D13; read-your-writes session
HLC); and Brainstorm-004-deeper-realization ADRs
(typed opcode tape; CSE + literal dedup; Cranelift
JIT).

**ADR 032 lead candidates (no firm commitment):**

- **`INSERT INTO ... SELECT`** — composes ADR 029 +
  030 + read-side ADRs. Most natural immediate DML
  extension after ADR 030. Engages bound
  `BoundExprProgram` substrate for the SELECT side.
- **Typed expression tape / opcode interpreter** —
  Brainstorm 004's deeper realization. Lowers
  `BoundExprProgram` to a flat typed-instruction
  tape with kernel dispatch on `(op, output_type)`.
  Substrate for future SIMD/JIT.
- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023's `"ORDER BY <aggregate-call> requires
  the aggregate to appear in the SELECT list"`
  reject. Plan-shape ADR; composes with ADR 028
  signature substrate via
  `AggregateOutputCatalog::resolve_for_order_by`.
- **Cross-ring distributed-write-context ADR** —
  lifts ADR 030 D13's distributed INSERT reject.
  Threads tenant / session / HLC context through
  `DistributedQueryCoordinator`.

**Code-evidence-driven candidates carried forward:**

- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **PBC partial-pushdown of computed-arg aggregates**
  (cross-ring; workload-evidence-pressured).
- **Common subexpression elimination (CSE) + literal
  deduplication** — both deferred from ADR 031 D8;
  future expression-optimizer ADR.
- **Cranelift JIT compilation** (Brainstorm 001) —
  lower `BoundExprProgram` or future tape to native
  code for hot kernels.
- **Logical `Expr` arena migration** — Brainstorm
  004's broader framing; revisit when evidence
  accumulates that planner-side `Box<Expr>` is
  load-bearing pain.

**Substrate candidates carried forward:**

- **`UPDATE` / `DELETE`** — DML siblings to ADR 030;
  `__row_tombstone__` becomes load-bearing for
  DELETE; Wendelon-side already owns tombstone
  stamping per ADR 030 D7.
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint
  substrate first.
- **`RETURNING` clause** — DML result-shape
  extension; shifts ADR 030's `Ok(Vec::new())`
  contract.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW` / `CREATE TABLE AS SELECT (CTAS)`**
  — DDL siblings to ADR 029.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED columns — DDL siblings to ADR 029.
- **HAVING alias visibility ADR.** Loosens ADR
  023's `"HAVING does not resolve SELECT aliases"`
  reject.
- **Temporal column types (DATE, TIMESTAMP)** for
  ADR 029's type lattice + ADR 030's INSERT
  coercion lattice.
- **DECIMAL** for ADR 029's type lattice + ADR
  030's INSERT coercion lattice.
- **Cross-ring metadata-write contract ADR** —
  needed to lift `WendelonCatalog::create_table`
  from `AdminUnavailable`.
- **Read-your-writes session HLC advancement ADR**
  — defines whether coordinators consume
  `wendelon_client::TableWriteReceipt.commit_hlc`.
- **Richer Toron `Hlc` type** — `HlcBound(pub u64)`
  is lossy vs Wendelon's `HybridTimestamp`
  (16 bytes); prerequisite for read-your-writes.
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **Per-statement `durability` override.**
- **Wendelon AGG consumer wiring** (cross-ring;
  PBC partial-pushdown).
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).

The post-ADR-030 archived selection notes below
(forward-sequence content from the prior planning
pass) remain unchanged.

#### ADR 030 → ADR 031 archived selection notes

ADR 030 selected and promoted **SQL `INSERT INTO ...
VALUES (...)` (Literal-Only DML Surface)** — Toron's
**first DML ADR** — the paired companion to ADR 029.
Lifts ADR 029 D14: `CREATE TABLE; INSERT; SELECT`
becomes testable end-to-end against `InMemoryTableStorage`
(in-memory test path) and against Wendelon (production
path via `wendelon_client::Client::table_write`). The
accepted shape is narrow — literal-only `VALUES`, no
`INSERT ... SELECT`, no `RETURNING`, no `ON CONFLICT`,
no defaults, no distributed execution. Sixteen
decisions D1-D16.

**D5 critical decision:** new `StorageWrite` trait
beside `StorageScan` —
`async fn write(WriteRequest) -> Result<WriteOutcome, StorageError>`
single-method trait; `WriteOutcome` is
`#[non_exhaustive]` with `rows: usize` only (forward-
compatible for future `commit_hlc`). **D6 + D14 critical
decision:** `Catalog` stays metadata-only — data-plane
writes do NOT extend `Catalog`; ADR 029's metadata/
data-plane separation extends to writes.

**D4 critical decision:** `ExecutionPlan::Insert`
carries `Vec<Vec<ScalarValue>>`, NOT `RecordBatch` —
preserves serde derivation (`ExecutionPlan` derives
`Serialize + Deserialize` at `ir/execution.rs:24`). The
coordinator write helper materializes the Arrow batch
from `(schema, rows)` immediately before
`StorageWrite::write`.

**D7 critical decision:** Wendelon synthetic columns
stay server-owned. Toron MUST NOT construct the five
`__row_*__` columns; Toron MUST NOT import `blake3`.
Architecture guards source-check `coordinator/write.rs`
for absence of `STORAGE_ROW_*` references and
`crates/toron/Cargo.toml` for absence of `blake3`
declaration. Wendelon's `pack_admitted_record_batch_by_bucket*`
owns row identity and synthetic column injection per
Wendelon ADR 045.

**D8 critical decision:** `WendelonStorageWrite` uses
the high-level
`wendelon_client::Client::table_write(tenant_id, descriptor, payload, options)`
wrapper (`crates/wendelon/crates/wendelon-client/src/lib.rs:574`),
NOT the low-level
`wendelon::client::WendelonClient::table_write(TableWriteRequest)`
(`crates/wendelon/src/client/core.rs:447`). The
wrapper handles `parent_write_id`, `client_hlc`,
`expected_epoch`, `schema_generation`, `child_ordinal_base`,
`proxy_source`, row-count encoding, `proxy_hop`,
`durability`, and `await_finalization` internally.
Architecture guard asserts no `TableWriteRequest {`
construction in `crates/toron/src/`.

**D9 critical decision:**
`WriteOptions { await_finalization: true, ..Default::default() }`
overrides the wrapper default (`false`). SQL `INSERT`
returns only after Wendelon commit confirmation — the
user-evidence payoff fixture
(`CREATE TABLE; INSERT; SELECT`) requires read-after-
write semantics that `await_finalization: false`
would race against.

**D11 critical decision:** `InMemoryTableStorage` is
additive (multi-table via
`Mutex<HashMap<TableId, InMemoryTableData>>`); existing
`InMemoryStorageScan` is untouched. ADR 030 does NOT
refactor the read-side InMemory backend — the
additive-not-consolidate discipline preserves all ADR
029 fixtures unchanged.

**D12 critical decision:** local `QueryCoordinator`
intercepts INSERT **BEFORE** the SELECT descriptor-
evolution retry loop. INSERT does NOT retry on
descriptor evolution in v1; write-retry semantics
defer to a future idempotency ADR. Default
constructor wires a private `UnsupportedStorageWrite`
sentinel; new `with_writer` constructor for tests +
server wiring.

**D13 critical decision:** distributed INSERT returns
a stable unsupported diagnostic **BEFORE**
`fragment_plan`. Mirrors ADR 029's
`WendelonCatalog::create_table → AdminUnavailable`
pattern. Lifts in a future cross-ring-write-context
ADR that threads tenant / session / HLC through
`DistributedQueryCoordinator`.

**D15 critical decision:**
`TABLE_WRITE_MAX_ROWS = 65_536` bind-time row cap,
matching Wendelon's
`wendelon/src/schema/table.rs:46` constant. Bind-time
enforcement keeps in-memory and Wendelon-backed
semantics aligned.

**D16 critical decision:** materialization lives in
`coordinator/write.rs` (new private submodule), NOT
in `storage.rs`. Coordinator write routing already has
the `ExecutionContext` needed to build `WriteRequest`;
storage stays shape-neutral.

**`InsertLiteral` IR carrier** (`Null` / `Boolean(bool)` /
`Int64(i64)` / `UInt64(u64)` / `Float64(f64)` /
`Utf8(String)`) — does NOT reuse `Expr` for VALUES
(parallel anti-delegation discipline to ADR 029 D11).
DML-specific `lower_insert` / `lower_insert_target_name`
/ `lower_insert_literal` helpers — do NOT delegate to
SELECT-side `lower_expr` or `table_ref_from_object_name`.
Binder owns target-schema-aware coercion via
`coerce_insert_literal` over ADR 029's six-type
lattice; typed-NULL fill for omitted nullable columns;
reject omitted NOT NULL; row cap + empty-VALUES
rejects. INSERT column lists can only name fields that
exist in the target schema, so ADR 029 D12's storage-
synthetic-name reject at CREATE-TABLE time is the
layered protection — ADR 030 needs no additional
runtime reject for these names.

Four iterations with **one substantive flip-flop**
(Codex iter-1 self-correction: `RecordBatch` →
`Vec<Vec<ScalarValue>>` in `ExecutionPlan::Insert` to
preserve serde derivation) and **two iter-2 retractions
from Claude** (C1 wrapper crate path — iter-1 read the
low-level `WendelonClient` source rather than the
`wendelon-client` wrapper Toron actually imports; C4
`From<wendelon_client::ClientError> for StorageError`
already exists at `error.rs:48`). The topic-concession-
compresses-next-cycle pattern from ADR 029's D1 / D15
sequencing made the topic question never reopen —
narrowest flip-flop profile post-ADR-029 (4 iters with
1 substantive flip-flop; ADR 029 had 4 iters with 0).

**Fifth consecutive cycle of Gemini iter-1/2 retraction
streak** (ADR 025 / 026 / 028 / 029 / 030): Gemini
iter-3 explicitly retracted its iter-2 D5
(`Catalog::insert_rows`), D6 (Toron-side `__row_*__`
construction), and D7 (BLAKE3 row identity derivation).
Stable cycle property recorded: Gemini's wider-scope
iter-1/2 framings surface the alternate-design space
the council needs to reject explicitly; useful for
collective understanding but should not be load-bearing
for promotion.

**Second consecutive pre-existing-Wendelon-types
cycle** (after ADR 029). **New cycle property recorded
— two-pass Wendelon-side verification:** cross-ring
ADRs need at least one iteration surface-reading the
relevant API + at least one peer iter to catch wrong-
file or wrong-import errors. ADR 029 had this with
iter-3 surfacing
`PhysicalDescriptorFingerprint::from_bytes` is crate-
private. ADR 030 had this with Claude iter-2
retracting C1 / C4 (wrong source file: looked at
`crates/wendelon/src/client/core.rs` rather than the
`crates/wendelon/crates/wendelon-client/` wrapper
Toron's `Cargo.toml:25` actually imports). Future
cross-ring ADRs should budget the wrong-import-
discovery peer iter explicitly.

Twelfth instance of the ADR 015 cycle (statement
catch-all narrows to
`"only SELECT, CREATE TABLE, and INSERT are supported"`).
Twelve new ADR 030 architecture guards + two adjusted
ADR 029 guards (statement catch-all wording + variant
counts 10→11 / 8→9 with `Insert {` content
assertions). SoA-readiness invariants preserved.
`WendelonStorageWrite` error mapping mirrors
`WendelonStorageScan` (TableNotFound for missing
descriptor, DescriptorEvolved for identity mismatch,
Internal for Arrow/schema failure); `TenantId`
conversion uses
`wendelon::schema::TenantId::new(request.tenant_id.0)`
— exact mirror of scan path at `storage.rs:206`.
Distributed INSERT explicitly unsupported in v1 (D13).
Read-your-writes session HLC advancement deferred
(D12 + Codex iter-3 residual Q3) — `WriteOutcome`
discards `TableWriteReceipt.commit_hlc`. Per-statement
durability override deferred (uses wrapper default
`DurabilityClass::QUORUM`). Float NaN/inf permitted
(mirror Arrow native semantics). No new IR expression
variant; no new aggregate kernel; no fragmenter
detector change for SELECT paths; no PBC opcode
change; no cross-ring metadata-write engagement; no
Toron-side Wendelon row-identity construction; no
`blake3` dependency; no `RecordBatch` inside any
`ExecutionPlan` variant. Existing SELECT corpus plan
digests byte-identical pre/post-ADR-030. The ten-phase
migration is materialized in
`plans/impl/030_insert_into_values.md`.

ADR 031 selection remains **firmly committed** by ADR
029's D1 / D15 sequencing decision and Codex iter-0
ADR 029's `BoundExprProgram` seed doc. Brainstorm 004's
narrow realization is the cycle's expected substrate-
introducing work — the third consecutive instance of
the topic-concession-compresses-next-cycle pattern
(ADR 020 → 021; ADR 027 → 028; ADR 029 → 031).

**ADR 031 lead candidate (committed):**

- **ADR 031 — Bound Expression Program** (Brainstorm
  004's narrow realization). Codex's iter-0 ADR 029
  doc seeded this cycle. Substrate-introducing
  expression-IR work that addresses the Box-everywhere
  baseline raised across ADR 023 / 024 / 025 / 026 /
  028 cycles. Expect 4+ iterations with multi flip-flop
  per the substrate-introducing cadence hypothesis.
  Per the ADR 029 + ADR 030-established cycle pattern,
  budget at least one wrong-import-discovery peer iter
  if the work touches Wendelon-defined types.

**Code-evidence-driven candidates carried forward:**

- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023's `"ORDER BY <aggregate-call> requires the
  aggregate to appear in the SELECT list"` reject
  (preserved by ADR 027 D3 + ADR 028 ratification +
  ADR 029 + ADR 030 carry-forwards). Plan-shape ADR;
  composes with the ratified ADR 028 signature
  substrate via `AggregateOutputCatalog::resolve_for_order_by`.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **PBC partial-pushdown of computed-arg aggregates**
  (cross-ring; workload-evidence-pressured).
- **Brainstorm 004 — Memory Arenas / Index-based IR**
  (alternate framing — broader than ADR 031's
  `BoundExprProgram` narrow realization). Multiple
  cycles surfaced the Box-everywhere pattern; the
  council may choose to land ADR 031's narrow
  realization first and revisit the broader system-
  wide migration after evidence accumulates.

**Substrate candidates carried forward:**

- **`INSERT INTO ... SELECT`** — composes ADR 029 +
  ADR 030 + read-side ADRs. The most natural
  immediate DML extension after ADR 030.
- **`UPDATE` / `DELETE`** — DML siblings to ADR 030.
  `__row_tombstone__` becomes load-bearing for
  DELETE; Wendelon-side already owns tombstone
  stamping per ADR 030 D7.
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint substrate
  first.
- **`RETURNING` clause** — DML result-shape extension;
  shifts ADR 030's `Ok(Vec::new())` contract.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW` / `CREATE TABLE AS SELECT (CTAS)`** —
  DDL siblings to ADR 029.
- **Constraints** (PRIMARY KEY / FOREIGN KEY / CHECK /
  column-level UNIQUE) / DEFAULT / GENERATED columns
  — DDL siblings to ADR 029; `DEFAULT` interacts with
  ADR 030's "reject omitted NOT NULL" rule.
- **HAVING alias visibility ADR.** Loosens ADR 023's
  `"HAVING does not resolve SELECT aliases"` reject
  (preserved by ADR 027 D7 + ADR 028 D11 + ADR 029 +
  ADR 030 carry-forwards).
- **Temporal column types (DATE, TIMESTAMP)** for
  ADR 029's type lattice + ADR 030's INSERT coercion
  lattice.
- **DECIMAL** for ADR 029's type lattice + ADR 030's
  INSERT coercion lattice.
- **Cross-ring metadata-write contract ADR** — needed
  to lift `WendelonCatalog::create_table` from
  `AdminUnavailable`.
- **Cross-ring distributed-write-context ADR** —
  needed to lift ADR 030 D13's distributed INSERT
  reject. Threads tenant / session / HLC context
  through `DistributedQueryCoordinator`.
- **Read-your-writes session HLC advancement ADR** —
  defines whether coordinators consume
  `wendelon_client::TableWriteReceipt.commit_hlc` to
  advance session read bounds. Touches
  `ExecutionContext` (currently `read_bound`-named),
  distributed retry contracts, and plan-digest
  stability if HLC bounds enter binding.
- **Richer Toron `Hlc` type** — `HlcBound(pub u64)`
  is lossy vs Wendelon's `HybridTimestamp` (16 bytes).
  Prerequisite for the read-your-writes ADR.
- **Idempotency / write-retry-after-descriptor-evolution
  ADR** — ADR 030 D12 explicitly does not retry INSERT
  on descriptor evolution; a future ADR defines the
  semantic + interacts with the
  `parent_write_id` idempotency key the
  `wendelon-client` wrapper already constructs.
- **Per-statement `durability` override** — ADR 030
  uses wrapper default `DurabilityClass::QUORUM`; a
  future ADR adds SQL syntax for per-statement
  override.
- **Wendelon AGG consumer wiring (ADR 02X)**
  (cross-ring).

The post-ADR-029 archived selection notes below
(forward-sequence content from the prior planning
pass) remain unchanged.

#### ADR 029 → ADR 030 archived selection notes

ADR 029 selected and promoted **SQL `CREATE TABLE`
(Narrow DDL Surface)** — Toron's **first DDL ADR** —
widening the statement layer from SELECT-only after
five consecutive expression-IR cycles (ADR 024 / 025 /
026 / 027 / 028) without DDL-surface work. User-evidence
framing surfaced the priority gap: "we should be able
to create tables before going super deep into
approximate quantiles." The accepted shape is narrow —
`CREATE TABLE [IF NOT EXISTS] [schema.]table_name
(column TYPE [NULL | NOT NULL], ...)` — with all other
DDL/DML strictly out of scope. Fifteen decisions D1-D15.
**D2 critical decision:** statement planner widens with
**exhaustive `sqlparser::ast::CreateTable` destructure
without `..`** as the compile-time guard against silent
acceptance when sqlparser dependency upgrades introduce
new clauses. **D4 critical decision:** DDL is
coordinator-only — both `QueryCoordinator` and
`DistributedQueryCoordinator` (both in
`crates/toron/src/coordinator.rs`) intercept BEFORE
their respective downstream stages; the data-plane
`Executor` + `Fragmenter` carry defensive rejects only.
**D7 critical decision:** `mock_table_identity` helper
derives `PhysicalDescriptorFingerprint` via
`TableSchemaDescriptor::new_with_layout` because
`PhysicalDescriptorFingerprint::from_bytes` is crate-
private to Wendelon. **D10 critical decision:**
`MockCatalog` reference insertion uses
`entry(...).or_insert(...)` for the unqualified-alias
path so schema-qualified creates never overwrite
existing aliases. **D12 critical elaboration:** the
five Wendelon storage synthetic column names
(`__row_hlc__` / `__row_key_id__` /
`__row_routing_locality__` / `__row_table_object_id__`
/ `__row_tombstone__`) reject at planner time, before
descriptor construction. **D14 critical decision:**
`SELECT * FROM newly_created_table` is NOT promised by
ADR 029 — catalog visibility only; readback requires
ADR 030 (INSERT). Four iterations with **zero
substantive flip-flops** on D1-D15 content after iter-2
council convergence (iter-0 topic split BoundExprProgram
vs CREATE TABLE resolved cleanly at iter-1 via explicit
sequencing commitment; iter-3 was pure code-evidence
precision-folding). Narrowest substrate-introducing
flip-flop profile in the cycle's history. Sixth
consecutive cycle of the Codex correctness-clarification
pattern — six precision items across iter-2 and iter-3
(coordinator/catalog ownership, MockCatalog interior
mutability, DDL-specific name + type helpers,
descriptor-derived fingerprint, alias-no-overwrite,
storage-synthetic-name reject) all surfaced from actual
codebase inspection. Fourth consecutive cycle of Gemini
"wider trait surface" iter-1 → iter-2 retraction (Gemini
iter-2 explicitly labels four "I retract my Iteration 1"
framings: D4 / D5 / D9 / D14). **New process pattern
recorded** — pre-existing-Wendelon-types ADRs need a
code-evidence pass on the Wendelon-side API surface,
not just Toron-side; ADR 030 (INSERT) and the eventual
cross-ring AGG consumer wiring ADR will share this
characteristic. Eleventh instance of the ADR 015 cycle.
Fifteen new ADR 029 architecture guards + one adjusted
statement-level catch-all guard. SoA-readiness
invariants preserved. **`WendelonCatalog::create_table`
returns `CatalogError::AdminUnavailable`** until a
cross-ring metadata-write contract ADR engages. No new
IR expression variant; no new aggregate kernel; no
fragmenter detector change for SELECT paths; no PBC
opcode change; no cross-ring engagement; no Wendelon
persistent descriptor writes. Existing SELECT query
corpus plan digests byte-identical pre/post-ADR-029.
The nine-phase migration is materialized in
`plans/impl/029_create_table_ddl.md`.

ADR 030 selection is **firmly committed** by ADR 029's
D1 / D15 sequencing decision. `CREATE TABLE` alone is
unusable without a way to populate the table; `INSERT
INTO ... VALUES (...)` is the paired companion.

**ADR 030 lead candidate (committed):**

- **ADR 030 — `INSERT INTO ... VALUES (...)`.** Narrow
  insert surface — single-statement, literal-only
  value lists, no `INSERT ... SELECT`, no `RETURNING`,
  no `ON CONFLICT`, no batched multi-row optimizations
  beyond the natural parser-level form. Routes through
  the same planner → binder → coordinator → catalog
  path established by ADR 029, with a new
  `LogicalPlan::Insert` / `ExecutionPlan::Insert` leaf
  variant and a new `Catalog::insert_rows` or similar
  trait extension. Expected to be a **pre-existing-
  Wendelon-types ADR** (touches the storage-write
  surface — `RecordBatch` materialization, row-identity
  metadata population including the five
  `__row_*__` columns reserved by ADR 029 D12,
  fingerprint discipline). Substrate-introducing
  scope at the statement layer; expect 3-4 iterations
  with at least one code-evidence Wendelon-side pass
  at iter-2 (per the ADR 029-established new pattern).

**ADR 031 lead candidate (committed):**

- **ADR 031 — Bound Expression Program** (Brainstorm
  004's narrow realization). Codex's iter-0 ADR 029
  doc seeded this cycle the same way Gemini iter-0
  ADR 020 seeded ADR 021 and Claude iter-0 ADR 027
  seeded ADR 028 — third consecutive instance of the
  topic-concession-compresses-next-cycle pattern.
  Substrate-introducing expression-IR work that
  addresses the Box-everywhere baseline raised across
  ADR 023 / 024 / 025 / 026 / 028 cycles. Expect 4+
  iterations with multi flip-flop per the substrate-
  introducing cadence hypothesis.

**Code-evidence-driven candidates carried forward:**

- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023's `"ORDER BY <aggregate-call> requires the
  aggregate to appear in the SELECT list"` reject
  (preserved by ADR 027 D3 + ADR 028 ratification +
  ADR 029 carry-forward). Plan-shape ADR; composes
  with the ratified ADR 028 signature substrate via
  `AggregateOutputCatalog::resolve_for_order_by`.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **PBC partial-pushdown of computed-arg aggregates**
  (cross-ring; workload-evidence-pressured).
- **Brainstorm 004 — Memory Arenas / Index-based IR**
  (alternate framing — broader than ADR 031's
  `BoundExprProgram` narrow realization). Multiple
  cycles surfaced the Box-everywhere pattern; the
  council may choose to land ADR 031's narrow
  realization first and revisit the broader system-
  wide migration after evidence accumulates.

**Substrate candidates carried forward:**

- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW` / `CREATE TABLE AS SELECT (CTAS)`** —
  DDL siblings to ADR 029.
- **Constraints** (PRIMARY KEY / FOREIGN KEY / CHECK /
  column-level UNIQUE) / DEFAULT / GENERATED columns
  — DDL siblings to ADR 029.
- **HAVING alias visibility ADR.** Loosens ADR 023's
  `"HAVING does not resolve SELECT aliases"` reject
  (preserved verbatim by ADR 027 D7 + ADR 028 D11 +
  ADR 029 carry-forward).
- **Temporal column types (DATE, TIMESTAMP)** for
  ADR 029's type lattice.
- **DECIMAL** for ADR 029's type lattice.
- **Cross-ring metadata-write contract ADR** — needed
  to lift `WendelonCatalog::create_table` from
  `AdminUnavailable`.
- **Wendelon AGG consumer wiring (ADR 02X)**
  (cross-ring).

The post-ADR-028 archived selection notes below
(forward-sequence content from the prior planning
pass) remain unchanged.

#### ADR 028 → ADR 029 archived selection notes

ADR 028 selected and promoted **Computed Aggregate
Arguments** — the first substrate-introducing ADR since
ADR 024 after three substrate-downstream cycles (025 /
026 / 027). Lifts the ADR 024 D6 column-only rule
preserved verbatim by ADR 025 / 026 / 027 — the
longest-standing evidence-pressure reject in Toron.
The stable aggregate kernel family (`CountAll` plus
one-arg `Sum` / `Count` / `Avg` / `Min` / `Max`) now accepts the
lowered `Expr` surface as arguments (`SUM(amount +
tax)`, `SUM(TRY_CAST(amount AS BIGINT))`, `AVG(price *
1.1)`, etc.). Eleven decisions D1-D11. **D2 critical
decision:** `GroupedAggregateSignature` becomes a
planner-private struct keyed by `StableAggregateKernel
+ Vec<Expr>` with custom `PartialEq` / `Eq` / `Hash`
impls implemented via six visitor helpers
(`hash_expr_structural`, `expr_structural_eq`,
`hash_scalar_structural`, `scalar_structural_eq`,
`hash_data_type_structural`, `data_type_structural_eq`).
**Public `AggregateFunction` / `Expr` / `ScalarValue`
remain non-`Eq` / non-`Hash`**; visitors are standalone
planner-private helpers, NOT trait impls on IR types.
Five iterations with two flip-flops: D2 P1 vs P2
(Gemini iter-0/1/2 advocated public manual `Eq + Hash`;
retracted iter-3 to planner-private — third consecutive
cycle of Gemini's "wider trait surface" retraction
pattern) and **iter-4 user-evidence-pressured D2
realization flip-flop** (iter-3 ratified materialized
`AggregateArgKey` tree; iter-4 refined to visitor
approach on `Vec<Expr>` after user flagged the "Box
everything" concern at merge staging — first user-
evidence-pressured substrate refinement in the cycle's
history; scoped re-open preserved D1, D3-D11 from
iter-3). Iter-5 folded four Codex precision items
including the load-bearing `data_type_structural_eq`
reflexivity correctness fix (P3 — `matches!` with arm
restrictions would violate `Eq` for hand-built non-
lattice `DataType` values; body must delegate to
`left == right`). Tenth instance of the ADR 015 cycle.
Three-way council convergence across five iterations.
Substrate-introducing cadence hypothesis confirmed
(4+ iters, at least one flip-flop). Five-of-five Codex
correctness-clarification cycle continues (most
distributed surfacing yet: iter-0 + iter-3 + iter-5).
Three-of-three Gemini "wider trait surface" retraction
pattern confirms. Topic-concession-compresses-next-
cycle pattern at 2-of-2 (ADR 020 → 021; ADR 027 → 028).
**Brainstorm 004 (Memory Arenas) strengthens as a
post-ADR-028 candidate** — multiple cycles have now
surfaced the Box-everywhere expression-IR pattern.
Nested aggregate calls reject at SQL-walk time with
stable diagnostic `"aggregate arguments must not
contain aggregate calls"` (D5). `COUNT(*)` produces a
zero-arg signature; `lower_count_aggregate`'s synthetic
placeholder is preserved verbatim for column-only-arg
plan-digest stability (D6 + D10). Row-layer executor
seam reused — no new operator, no hidden pre-aggregate
`Project` (D8). Computed-arg aggregates inherit ADR
022 sticky-NaN / ADR 024 arithmetic loud-failure /
ADR 025 / 026 Cast / TryCast policies through
composition; remain PBC-residual (D9). Experimental
kernels (`LinearRegress`, `ApproxQuantile`) remain
excluded from grouped aggregate signature matching via
D2 type segregation through `stable_kernel_for_function`
returning `None`. Eleven
new ADR 028 architecture guards + two adjusted
(ADR 024 D6 column-only + ADR 020 AVG signature
shape). No new IR variant; no new operator; no new
kernel; no fragmenter detector change; no Eq/Hash
refactor; no public IR trait widening; no cross-ring
engagement; no PBC opcode change. Plan digests for
column-only-argument queries byte-identical pre/post-
ADR-028. The seven-phase migration is materialized in
`plans/impl/028_computed_aggregate_arguments.md`.

ADR 029 selection starts from the post-ADR-028
remainder. The Box-everywhere pattern has now surfaced
across multiple cycles, with ADR 028 iter-4's user-
evidence pressure making the case for an arena ADR
substantive. The substrate-downstream candidates from
ADR 028 (PBC partial-pushdown, hidden-aggregate
ORDER BY, non-aggregate computed ORDER BY) compose
naturally with the freshly-ratified signature
substrate.

**Substrate-introducing lead candidate (Brainstorm-
seam-strengthened):**

- **Brainstorm 004 — Memory Arenas / Index-based IR
  ADR.** Multiple recent cycles (ADR 023 / 024 / 025 /
  026 / 028 iter-3 → iter-4) have surfaced the Box-
  everywhere expression-IR pattern. ADR 028 iter-4's
  user-evidence-pressured refinement avoided *adding*
  to the pattern but didn't address the existing
  baseline. A system-wide arena/index migration ADR
  would shift `Expr` / `LogicalPlan` / `ExecutionPlan`
  children from `Box<T>` to `Vec<Node>` + `u32`
  indices, eliminating per-node allocation churn on
  the planner hot path. **Substrate-introducing ADR**
  — expect 4+ iterations with multi flip-flop;
  comparable scope to ADR 024 (which took 4+ iters).
  Promotes if the cumulative Box-pattern evidence
  outweighs the absence of bench-quantified planner
  allocation cost.

**Code-evidence-driven candidates from ADR 028
remainder:**

- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023's `"ORDER BY <aggregate-call> requires the
  aggregate to appear in the SELECT list"` reject
  (preserved by ADR 027 D3 and ADR 028 ratification).
  Plan-shape ADR; now composes naturally with the
  ratified ADR 028 signature substrate via
  `AggregateOutputCatalog::resolve_for_order_by`.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred. Generalizes the ADR 027 hidden-
  projection mechanism off the
  `aggregate_output_catalog.is_some()` gate.
- **PBC partial-pushdown of computed-arg aggregates**
  (cross-ring; workload-evidence-pressured). Now that
  computed-arg aggregates exist (ADR 028), the cross-
  ring ADR can consider whether PBC bytecode should
  grow arithmetic / Cast / TryCast opcodes to push
  `SUM(amount + tax)`-style computation to Wendelon.

**Substrate candidates carried forward:**

- **Utf8 ↔ numeric CAST + TRY_CAST.** Adds Utf8 source
  and target paths to ADR 025 / 026 lattices
  simultaneously. Parsing semantics ADR — locale,
  whitespace handling, scientific-notation acceptance.
  Extends both strict and tolerant variants. Computed-
  aggregate-argument acceptance inherits via ADR 028
  D2 visitor exhaustive-match.
- **Boolean ↔ numeric CAST + TRY_CAST.** Truth-table
  semantics. Extends both lattices.

**Carried substrate candidates from ADR 023's
named-deferred set:**

- **HAVING alias visibility ADR.** Loosens ADR 023's
  `"HAVING does not resolve SELECT aliases"` reject
  (preserved verbatim by ADR 027 D7 and ADR 028 D11).
  SQL-compat surface; own planner name-resolution
  decision matrix.

The post-ADR-027 archived selection notes below
(forward-sequence content from the prior planning
pass) remain unchanged.

#### ADR 027 → ADR 028 archived selection notes

ADR 027 selected and promoted **`ORDER BY` Computed
Expressions over Aggregate Output** — the third consecutive
substrate-downstream ADR (ADR 025 / 026 / 027) and the
cycle that ratifies the hidden-projection mechanism
originally shipped by ADR 025 (and narrowly extended by
ADR 026 D10) as the general path for non-direct computed
`ORDER BY` keys on
aggregate queries. Ten numbered decisions D1-D10. **D1
generalizes the gate** to
`!sort_key_is_direct_output_reference(&key.expr)` and
**removes** `expr_contains_cast_family` outright (recursive
inspection moves into the D2 validator). Direct output
columns and ordinals continue to bypass hidden projection;
plan digests for those queries remain byte-identical. **D2
adds `validate_hidden_sort_key_refs`** — an exhaustive
`Expr` walker that rejects qualified-column references and
non-visible-output column references at design time with
the stable diagnostic
`"ORDER BY computed aggregate expression must reference
visible output columns"`. Prevents raw input columns from
leaking into the post-aggregate projection (Codex iter-1
correctness clarification promoted to numbered decision at
iter-2 — fourth consecutive cycle of this pattern). D3-D7
preserve ADR 023 hidden-aggregate-key reject, ADR 024 D6
computed-aggregate-argument reject, ordinal-only
`ExecutionSortKey`, hidden-projection contract
(`__toron_order_key_{idx}` naming + collision suffixing +
duplicate-name reject + final-projection stripping), and
top-level ORDER BY alias visibility (HAVING alias reject
unchanged). D8 pins the expression surface as the
currently-lowered `Expr`; future expression families
inherit ADR 027 acceptance automatically. D9 accepts
literal-only computed sort keys (`ORDER BY 1 + 1`); D10
keeps non-aggregate computed `ORDER BY` out of scope.
Ninth instance of the ADR 015 cycle. Three-way council
convergence across three iterations: iter-0 topic split
(Claude: computed-aggregate-args substrate vs Codex +
Gemini: ORDER BY ratification) resolved 2-of-3 in favor of
this ratification, Claude conceded at iter-1, iter-2
substantive convergence on D1-D10, iter-3 cosmetic
alignment with **zero precision items** (tighter
convergence than ADR 026's three iter-3 items). Confirms
substrate-downstream cadence (ADR 025: 2 iters; ADR 026:
3 iters; ADR 027: 3 iters). Topic-split pattern matures
(matches ADR 020 → 021 pattern; deferred substrate carried
to ADR 028 lead). Three new architecture guards
(gate-generalized, visible-outputs validator,
contract-pinned) + two existing ADR 025 / 026 guards
adjusted (neither renamed, neither deleted). SoA-readiness
invariants preserved. No new IR variant; no new operator;
no new kernel; no fragmenter detector change; no Eq/Hash
refactor; no `GroupedAggregateSignature` widening; no
cross-ring engagement; no new `LogicalPlan` /
`ExecutionPlan` variant. Plan digests for direct
`ORDER BY` queries byte-identical pre/post-ADR-027. The
seven-step migration is materialized in
`plans/impl/027_order_by_computed_expressions.md`.

The post-ADR-027 candidate set (computed-aggregate-args
substrate lead, hidden-aggregate ORDER BY keys,
non-aggregate computed ORDER BY, Utf8 / Boolean CAST +
TRY_CAST, HAVING alias visibility, ADR 024 arithmetic
siblings, Wendelon PBC opcode extension) folded into
the ADR 029 selection set at the top. Computed-
aggregate-args led the round and promoted as ADR 028;
the surviving candidates carry forward to ADR 029.
Brainstorm 004 (Memory Arenas) added as a new
substrate-introducing lead candidate after ADR 028
iter-4's user-evidence pressure made the Box-everywhere
pattern an explicit cycle property.

#### ADR 026 → ADR 027 archived selection notes

ADR 026 selected and promoted
**`TRY_CAST(x AS T)` / `SAFE_CAST(x AS T)` — Error-
Tolerant Numeric Primitive Cast** — the first substrate-
downstream ADR after ADR 025 that lifts both
`TRY_CAST` and `SAFE_CAST` reject arms ADR 025
pinned in its `cast_syntax_variants_are_rejected`
guard. Exposes the SQL error-tolerant cast variant
under the same 16-cell numeric primitive lattice
that ADR 025 established. Both `TRY_CAST` and
`SAFE_CAST` lower to the same `Expr::TryCast` IR
variant as syntactic aliases. Decisions D1-D11
cover IR shape (D1 — new variant pair, not flag on
Cast), failure modes (D2 — 5 ADR 025 data-domain
errors become NULL), SQL surface (D3 — alias
bundle), preserved rejects (D4 — `::` + FORMAT
stay rejected), nullability (D5 — always nullable),
aggregate-arg shape (D6 — column-only preserved),
identity preservation (D7 — no bind-time elision,
parallel to ADR 025 D8), executor strategy (D8 —
6 fail-to-null + 6 strict-reuse kernels), PBC
ineligibility (D9 — widened diagnostic), cast-
family hidden-sort-key gate (D10), and inner-error
propagation (D11 — `TRY_CAST(CAST(...))` propagates
inner Cast errors, NOT swallowed). New
`Expr::TryCast(Box<Expr>, DataType)` IR variant
(Expr enum 9 → 10) with twin `ExecutionExpr::TryCast`.
`TryCast.nullable() = true` regardless of inner —
key divergence from ADR 025 D7. **D11 critical
clarification** (Codex iter-1 → iter-2 promotion):
inner strict Cast errors propagate via the natural
`?` operator dispatch in the evaluator; outer
TryCast only nulls its OWN per-pair conversion
failures, NOT child errors. Six new per-pair
`try_cast_*` fail-to-null kernels for fallible
cells; 6 infallible cells reuse ADR 025 strict
kernels directly. Reuses ADR 025's `F64_I32_UPPER`
/ `F64_I64_UPPER` exclusive-upper-bound constants
and `cast_failed` Kernel-error prefix. **Do not
implement TryCast by wrapping `evaluate_cast` with
catch-and-null** — would (a) stop at first failure
instead of per-row NULLs, (b) swallow internal
executor bugs, (c) violate D11. PBC diagnostic
widens to `"arithmetic, Cast, and TryCast are not
PBC-eligible"`. **D10 cast-family gate extension**
extends ADR 025's hidden-projection guard coverage for
the cast-family case via
`expr_contains_cast_family` (renamed from
`expr_contains_cast`, body widened to
`Cast | TryCast`); the broader retroactive
ratification of the full mechanism (general
computed-expression gate, hidden-column naming,
etc.) remains future work. Eighth instance of the
ADR 015 cycle (after 016 / 017 / 018 / 019 / 022 /
023 / 024 / 025). Three-way council convergence
reached across three iterations; substantive shape
at iter-2 on D1-D10, D11 promoted from Codex iter-1
correctness clarification at iter-2, iter-3 closed
three implementation-precision items (logical-IR
`is_pbc_pushdown_eligible` arm, binder ancillary
helpers `collect_expr_columns` /
`rewrite_expr_ordinals`, hidden-projection guard
as a new extension guard rather than a rename). One implementation-
shape dissent (Gemini iter-1/2 `null_on_failure:
bool` flag-on-strict-kernel) retracted at iter-3 in
favor of separate `try_cast_*` kernels; pattern
matches ADR 025 Dissent B retraction. Confirms
substrate-downstream cadence hypothesis (ADR 025:
2 iters; ADR 026: 3 iters). Third consecutive
cycle where a Codex iter-1/2 correctness argument
surfaces a load-bearing semantic boundary at design
time (ADR 024: aggregate-arg shape guard; ADR 025:
identity-Cast PBC + float-to-int exclusive bound;
ADR 026: inner-Cast-error propagation). Seven new
architecture guards (TryCast IR surface, planner
lowering, binder policy, numeric primitive lattice,
null-only-data-domain-failures, PBC declines,
hidden-sort-key gate). Two existing guards
adjusted (one renamed:
`adr025_cast_syntax_variants_are_rejected →
adr026_cast_syntax_doublecolon_and_format_remain_rejected`).
Three new integration tests. SoA-readiness
invariants preserved (catalog untouched; per-
(source, target) TryCast kernel dispatch preserves
JIT specialization seam). No new `LogicalPlan` /
`ExecutionPlan` variant; no new `ExecutionError`
variant; no new `TaskFailureKind` variant; no new
`AggregateOutputRule` variant; no new
`StableAggregateKernel` row; no `AggState` change;
no fragmenter detector change; no cross-ring
engagement; no `GroupedAggregateSignature` widening.
Plan digests for no-TryCast queries byte-identical
(additive surface). The six-phase migration is
materialized in
`plans/impl/026_try_cast_numeric_primitive_lattice.md`.

The post-ADR-026 candidate set (computed aggregate
arguments substrate, Utf8 ↔ numeric CAST + TRY_CAST,
Boolean ↔ numeric CAST + TRY_CAST, HAVING alias
visibility, hidden-aggregate ORDER BY keys, ADR 024
arithmetic siblings, Wendelon PBC opcode extension)
folded into the ADR 028 selection set above. The
ORDER BY ratification led that round and promoted as
ADR 027; the computed-aggregate-args substrate-
introducing candidate from Claude's ADR 027 iter-0
carries forward as the ADR 028 lead.

**Kernel-domain candidates (carried from ADR 022
remainder):**

- **`Utf8` `MIN` / `MAX`.** ADR 019 V1 §2 deferred
  for `AggState` Copy-refactor. Substrate refactor
  with cascading effects across existing `match
  states[agg_idx]` patterns; forces `AggState` to
  lose `Copy`. ADR 021's catalog preserves
  `AggState: Copy` as a non-load-bearing property
  — Utf8 MIN/MAX is the first ADR that would
  change it. ADR 022 widened the `OrderedMinMax`
  domain to `Int32 | Int64 | Float32 | Float64`;
  Utf8 would either extend that domain (collation /
  locale decision matrix) or land a sibling
  `Utf8MinMax` domain. Bundleable with a
  collation/locale decision matrix, or kept
  bytewise-only.
- **Decimal MIN / MAX.** Natural extension of ADR
  022's `OrderedMinMax` substrate — preserves
  total-order property; the `is_ordered_min_max_input`
  body extends with `Decimal128` / `Decimal256`
  arms. Composes with the V1.x Decimal numeric-types
  ADR; promotes when Decimal lands. ADR 022's
  enumerated error string is the right kind of
  brittleness here: a Decimal MIN/MAX ADR will
  update the strings in step.

**Cross-ring candidate — ADR 02X (Wendelon AGG
consumer wiring):**

The Wendelon-side AGG protocol exists, including
`SignedAverageParts` whose `(sum: i128, count: u64)`
shape is now structurally equivalent (modulo
overflow handling) to Toron's `AggState::AvgInt`.
Toron-side `wendelon_client::TableQueryOptions`
does not expose the protocol. The consumer-side
ADR engages the cross-ring contract for the
partial/final two-phase shape and lifts
ADR 011/019/020's per-partition scalar ceiling.
**This is the right home for two-phase aggregation
work** — bench artifact required (cross-ring perf
characteristic). **Inherits an explicit forward-
work item from ADR 022:** verify Wendelon-side
Float MIN / MAX semantics match Toron's sticky-NaN
before engaging cross-ring aggregate pushdown for
these kernels. Wendelon protocol layer is
type-agnostic; if executor-side mismatch surfaces,
ADR 02X reconciles (either align Wendelon
executor or refuse Float MIN / MAX pushdown).
Promotes when workload evidence surfaces aggregate-
pushdown pressure.

**Other ADR 011 follow-ons** carry forward as candidates if
their evidence outranks the substrate-and-surface set:
- **Kahan-Neumaier stability ADR.** Composes across SUM and
  AVG together; MIN/MAX is order-independent and doesn't
  participate. Promotes if production workloads surface
  precision-loss evidence on Float64 AVG.
- **`DISTINCT`** — `GROUP BY <all output columns>` with no
  aggregates. Loosens ADR 011's zero-aggregate reject; own
  decision matrix.
- **AVG as a window function.** Window aggregates need their
  own decision matrix.
- **`GROUP BY` over joins.** Composes ADR 008's join surface
  with ADR 011's grouped aggregate; only candidate with a
  brainstorm-seam concern (disaggregated-compute placement,
  brainstorm 003).

**Kernel graduation evidence pressure (carried from ADR 018
iter-1 sweep).** The experimental-kernel bound/unbound parity
gap — `LinearRegress` / `ApproxQuantile` are accepted by planner +
binder but the bound executor returns `NotImplemented`. Gated
behind `allow_experimental_kernels` (default false), so
production SQL paths don't reach it. Post-ADR-021 these kernels
stay outside the stable catalog via the small adjacent
`experimental_aggregate_sql_name` helper; graduation moves a
kernel from experimental-helper to stable-catalog row. The gap
tightens the evidence case for the first kernel graduation but
did not promote as ADR 023 lead or ADR 024 lead and does not
promote as ADR 025 lead by itself.

**V1.x runtime-filter extensions** (Candidate C below) remain a
cluster of narrow follow-on ADRs; ADR 014's `RuntimeFilterKind`
enum is the surface they extend. Promotion still requires
per-extension evidence; ADR 014's descriptor refactor does not
auto-select the next extension.

**Code-evidence defects** (silent-drop, inert-feature) outrank
all of the above when one surfaces. The selection-round practice
established in ADR 014's cycle stays: candidates are nominated,
evidence is hunted, the council picks the topic the code points
to.

**Standing A–E candidates** rotate forward beneath the lead.
Candidate B (cross-process wire efficiency) is parked unless
non-selective workload pressure surfaces — the ADR 013/014
runtime-filter row reduction landed a clear hit on the selective
workload. Candidate C (V1.x runtime-filter extensions beyond
ExactSet — broadcast, outer/anti, planner cost model,
encoded-type widening) remains a cluster of narrow follow-on
ADRs, each requiring its own evidence trigger.

#### Candidate A — Multi-tenant resource governance

**Scale boundary:** cluster-wide.

Per-query to per-tenant budgets, token-bucket admission,
scheduler-slot allocation, memory-rejection vs spill-trigger
thresholds. Promotes if V1 launch pressure surfaces
multi-tenancy before joins-adjacent perf work.

ADR 003's per-query `QueryBudget` is the natural antecedent;
the per-tenant layer wants real multi-tenant workload evidence
before its shape gets pinned. ADR 007 explicitly deferred
per-tenant Rune budgets to this work (L-3 from the topics
cycle).

#### Candidate B — Cross-process wire efficiency

**Scale boundary:** transport.

The compute-ring fallback if later production-scale Wendelon
runtime-filter evidence misses launch needs or non-selective
workloads dominate post-V1. Batch-size policy, Arrow IPC encode/decode
tuning, `WireEnvelope` overhead audit, output-publish path
streamlining. No cross-ring dependency.

Promotes if production-scale evidence shows the selective
runtime-filter path misses its target or if non-selective workloads
dominate the V1 launch shape. Lower leverage than ADR 006 on
selective joins (moves bytes faster vs avoids moving rejected
bytes), but uniform across selective and non-selective workloads.
Phase 0 sub-bucket selection discipline preserved if C promotes
(no grab-bag wire-perf ADR).

#### Candidate C — V1.x runtime-filter extensions

**Scale boundary:** cross-ring (continuation of ADR 006).

A cluster of follow-on work to ADR 006 and ADR 013, now unblocked by
production Wendelon-side row landing:

- ~~**Exact `InList` filters** for small build sets~~ — **covered
  by ADR 014** (ExactSet runtime-filter kind). The roadmap's
  ≥50%-with-material-FP gates cleared on the ADR 006 Phase 0 row;
  ADR 014 introduces the `RuntimeFilterKind::ExactSet` descriptor
  variant rather than the SQL-`InList` / PBC-`InList` framing
  this bullet originally implied.
- **Broadcast-strategy runtime filters.** ADR 006 V1 limits
  filter eligibility to hash-partitioned joins. Broadcast
  filters reuse the descriptor contract with a different
  attachment path.
- **Outer / semi / anti runtime-filter pushdown.** ADR 008
  reserved this — runtime filters on outer / anti probe sides
  would incorrectly drop rows the outer-preservation semantics
  require. A "filter + null-pad" combined operator is the
  separable design.
- **Planner cost-model opt-in.** Always-two-wave in V1;
  planner-stats opt-in once stats exist.
- **Filter eligibility extension to canonical-encoder
  additions.** When the canonical encoder grows support for
  `Date`, `Timestamp`, or `Decimal`, filter eligibility extends
  with it.

Each sub-item is plausibly its own narrow ADR. Held as topic cards
until one extension wins the next planning pass.

#### Candidate D — Worker-side Rune UDF execution

**Scale boundary:** operator-level + process-internal
(distributed sandbox).

Reserved. ADR 007 explicitly scoped row-level Rune UDFs out.
This is the substantial new ADR if a future feature needs
worker-side script execution — covers distributed sandbox +
per-worker budget interaction + the `TaskFailureKind`
reason-string mapping ADR 007 reserved as the forward seam.

#### Candidate E — Executor model upgrade

**Scale boundary:** operator-level (executor substrate).

Demoted by ADR 004 sub-attribution and re-confirmed by ADR 005
+ ADR 006 (probe + output combined < 15% of wall on selective
workloads; even smaller post-filter). Reserved for the case
where a future workload (large probe-side without filtering,
multi-way joins, complex aggregates) shows probe-side
dominance. Stays demoted.

### Advanced kernel graduations *(one ADR per kernel)*

Wendelon ADR 070 narrow-decision pattern. Each promotes
under its own ADR with bench evidence. Inherited from
prior roadmap, unchanged:

- **`LinearRegress` stable contract.** Numerical hardening
  (condition-number guard, centered formula, rank check)
  + stability across numeric types.
- **`ApproxQuantile` stable contract.** Real rank-based
  estimation against KLL retained values + cross-impl
  parity with Wendelon's KLL substrate.
- **`TimeBucket` stable contract.** Bucket-alignment
  semantics across timezone, duration, and pre-epoch
  inputs.

### Brainstorms and high-level vision

The far-horizon shapes the council is most excited about.
These are speculative architectures — not committed scope —
but each opens a genuinely new axis of capability the V1
substrate is being kept *compatible with*, not closed off
from. The cycle's discipline of preserving load-bearing seams
explicitly serves these futures.

- **Brainstorm 001: Pure Cranelift JIT as a First-Class
  Query Specializer.** Compile hot kernels — predicates,
  UDFs, hash-join probe loops — directly to native machine
  code. The interpretation tax that every analytical query
  pays today disappears on the inner loops that actually
  dominate wall-clock. Bare-metal throughput on the kernels
  that matter most. The seam is kept open: no V1 contract
  bakes in interpretation-only invariants. Reserved for
  post-V1 deliberation when bench evidence quantifies the
  interpretation overhead at production scale.
- **Brainstorm 002: WeakControlRing.** An in-memory Raft
  ring for HA coordinators, cluster-wide quotas, and shuffle
  barrier synchronization — *without* the persistence tax
  of NVMe / WAL that traditional Raft pays. The insight:
  control-plane state has different durability needs than
  data-plane state; a weakened ring algebra lets coordinator
  HA scale without storage coupling. Seam: ADR 003's
  `WorkerRegistry` trait + `WorkerLease` / `LeaseEpoch`
  fields are the deliberate hooks. Promotes when V1
  production exposes coordinator-HA as a binding constraint.
- **Brainstorm 003: Disaggregated Compute Roles & Unified
  Ring Algebra.** The most ambitious of the four. Break the
  symmetric Toron/Wendelon node model into specialized
  hardware roles — stateful NVMe data nodes paired with
  elastic compute-only nodes — while preserving a single
  executable, a unified ring algebra, and the operator
  contracts that work today. Three concrete visions are
  noted: hardware-optimized asymmetry, weak/strong ring
  algebra, data-broker / ToronWorker topology. Each
  unlocks a different cost-efficiency story at scale. Seams:
  `NodeRoleSet` bitmask expansion, `RingRouter` routing
  durable writes only to `NodeRole::Data`, ADR 003's
  registry/lease surfaces (shared with Brainstorm 002). The
  cycle currently passes through these seams without closing
  them off — for example, ADR 020's "GROUP BY over joins"
  candidate is parked specifically because its fragmenter
  routing decision would prematurely commit to one of the
  three placements.
- **Brainstorm 004: Memory Arenas and Custom Allocators for
  Query Planning.** AST and query-plan churn dominate
  allocation profiles in dispatch-heavy workloads. Arena
  allocation collapses the per-node `Box`/`drop` overhead
  into a single bulk free, improving cache locality on the
  hot planner path. Three sketched approaches: external
  arena crates (`bumpalo` / `typed-arena`, lifetime
  contagion), Cranelift-style index-based pool (SOA-aligned,
  no lifetime threading), domain-specific pool. Seam: IR
  type shape — `LogicalPlan`, `ExecutionPlan`, `Expr`
  currently own children via `Box`, so deeper boxing raises
  future retrofit cost. Evidence-gated by ADR 005's
  allocator-churn note; no allocator commitment lands
  without profiling, but the IR shape is kept
  arena-compatible.

The cumulative ambition: a compute ring that JITs the inner
loops, scales coordinator state without storage coupling,
disaggregates hardware roles for cost-efficiency, and
allocates query state with cache-aware discipline — all on
the same Wendelon-native substrate V1 is hardening today.
Each brainstorm is independent; the value of keeping their
seams open compounds.

**Pushdown axis** (cumulative observation, not a
brainstorm). Toron's pushdown story now covers static
predicates (ADR 002), shuffle-key affinity (ADR 003), and
*runtime* join-key filtering (ADR 006 generation plus ADR
013 storage-scan application). Future pushdown surfaces:
aggregate partial pushdown (already half-Wendelon-bound),
top-K / sort pushdown, projection-with-computed-column
pushdown. Each is its own ADR; the cumulative shape is
"Toron binds; Wendelon executes the leaves; the wire
carries results, not bytes." ADR 006 and ADR 013 together
form the runtime-pushdown substrate that V1.x runtime-
filter extensions (Candidate C above) build on. Not
numbered as a brainstorm because the content is an
existing-trajectory observation rather than a speculative
architecture — file-backed Brainstorms 001-004 are the
speculative set.

### V1.x items *(post-V1, evidence-driven)*

Items the council has explicitly deferred from V1 ADRs.
Promoted to their own ADRs when evidence drives them.

- **Non-equi joins / theta-joins.** Own ADR.
- **Sort-merge join.** ADR 010 pins the top-level `ORDER BY`
  contract and the `Sort` operator; sort-merge join builds on
  that ordering antecedent. Still its own ADR — needs the
  join-side ordering and merge-path decisions.
- **Cross-join.** Own ADR or V1.x SQL surface.
- **Cost-based build/probe swap; cost-based strategy
  selection.** Requires Wendelon stats surface and Toron
  cost model. Will land `BuildSide` field on
  `ExecutionPlan::Join` when it engages.
- **Multi-way join planning.** Requires cost model.
- **Skew detection + mitigation.** ADR 004 ships
  observation only (soft gate). Mitigation needs heavy-
  hitter detection, salt-and-replicate, dynamic
  partition-count override. Own ADR.
- **SQL join hint syntax.** `/*+ BROADCAST(t) */` and
  similar. V1.x SQL surface.
- **`USING` clause; comma-join; `CROSS JOIN`.** V1.x SQL
  sugar.
- **Floating-point join keys.** Requires canonical-
  encoding ADR (NaN, signed zero, subnormals).
- **Intra-task build / probe pipelining.** V1.x executor
  work; depends on Candidate E (executor model upgrade)
  outcome.
- **Hash-spill-to-memory demotion.** ADR 004 ships one-way
  spill; demotion is V1.x when bench shows churn benefits.
- **SQL-surface auto-rename of duplicate output column
  names.** ADR 004 requires explicit qualification.
- **Pipelined cross-stage execution.** ADR 003 ships
  strict stage barriers; pipelining adds latency wins at
  the cost of failure-recovery complexity. V1.x when the
  failure-recovery story is concrete.
- **Locality-aware fragment routing.** ADR 003 +
  ADR 004 are location-agnostic; locality requires new
  Wendelon public-API surface.
- **RDMA / RoCEv2 / SIW transport tier.** M9 seeds the
  transport-tier-negotiation shape; QUIC preferred
  through V1.
- **Operator-internal streaming end-to-end.** ADR 003
  requires stream-shaped task output; full operator-
  internal streaming is V1.x executor work (Candidate E —
  executor model upgrade — territory).
- **Higher retry counts.** ADR 003 ships `max_attempts =
  2`; future ADR raises with evidence of which failure
  modes benefit.
- **Encrypted spill governance.** Spill lifecycle beyond
  M10 seed: key rotation, AAD policy, crash recovery,
  observability, multi-query isolation. ADR 003 +
  ADR 004 share the M10 AES-256-GCM-SIV substrate without
  key rotation.
- **Full DDL propagation.** Async invalidation broadcasts,
  multi-coordinator schema consensus, descriptor
  versioning beyond fingerprint-pin.
- **Wire-protocol cross-version compat.** ADR 003 ships
  reject-on-mismatch; rolling upgrades are V1.x.
- **Decimal numeric types.** Their own ADR; pins decimal-
  specific aggregate semantics.
- **Wendelon-side crate extraction.** `wendelon-types`
  (PBC ops, identity, fingerprint), `wendelon-net`
  (transport facade), `wendelon-schema` (descriptor
  types). Defer until multi-consumer pressure (Polariton,
  Baryon, external Toron SDK) materializes.

### V2+ items

Direction, not near-term plan.

- **Replicated / disaggregated shuffle service.** ADR 003
  per-worker partition store is the V1 substrate; a
  standalone service is V2 when evidence shows producer-
  hosted partitions are the next bottleneck.
- **Direct P2P shuffle as fast path.** V2 optimization
  over the V1 pull-addressed contract.
- **Hybrid vector search.** Vector ANN with metadata
  pushdown into Wendelon. Waits for Wendelon vector-aware
  placement.
- **Visualization-as-compute IR operators.** Density
  tiles, 2D binning, quantile-per-pixel. Waits for
  Polariton's tile protocol.
- **External federation** (Postgres / S3 / BigQuery
  adapters). Explicit ADR 001 §"Deferrals" item — "one
  external adapter as a capability proof," never on the
  Wendelon-native hot path.
- **Materialized views; incremental view maintenance.**
  Requires the executor model to support both batch and
  incremental shapes; far horizon.

### Refinement plumbing *(opportunistic, no ordering)*

Small improvements that don't fit a single ADR.

- **Capability-table aggregate dispatch** replacing the
  enum (when the aggregate catalog grows beyond hand-
  listable).
- **Newtype hygiene pass** — `Symbol` / `ColumnId` instead
  of bare `String` for column refs in narrower contexts.
- **PBC catalog growth** — additional ops promoted from
  experimental to stable.
- **ADR file renaming pass** — drop the redundant
  `toron_` prefix from `plans/adr/001-003` and
  `plans/impl/001-003` filenames (ADR 004's filename
  already follows the cleaner pattern). Mechanical sed +
  cross-reference update.

## Backlog (parked, may not return)

Ideas explored across iteration rounds in `plans/wip/`
that did not earn forward placement. Kept here as a
record of what the council considered and let go. Items
labeled by ADR cycle. Older cycles compress to brief
summaries; recent cycles retain detail until evidence
fades.

### ADR 016 cycle (compressed)

**ADR 016 (post-aggregate filtering with HAVING):**
- Group-keys-only `HAVING` (no aggregate-call rewrite)
  rejected — canonical `HAVING SUM(amount) > N` fails;
  worse user surface than no HAVING.
- Binder widening with `Expr::AggregateCall` arm rejected
  — only relocates structural-match logic; `bind_expr`
  lacks projection context; ADR 009 SQL-AST-rewrite
  precedent stands.
- Raw `sqlparser::ast::Function` `PartialEq` as rewrite
  match key rejected — preserves user casing and would
  byte-mismatch same call; lowered `(AggregateFunction,
  Vec<Expr>)` pair is canonical key.
- First-match resolution for duplicate-output-name HAVING
  references rejected — four-way classification rejects
  ambiguous instead of silently picking first.
- Bundling ADR 015's `ORDER BY <aggregate-call>` close
  into ADR 016 rejected — kept narrow; landed as ADR 017.
- Post-aggregate `Filter` outside terminal-gather exchange
  rejected — inside-gather is wire-efficient and
  preserves three-stage symmetry.
- New `FilterDisposition` variant rejected — existing
  `EvaluateInToron` applies via PBC-push failing for
  post-aggregate columns.
- Permissive-engine HAVING lifting and `HAVING <ordinal>`
  form both rejected — standard-SQL contract.

### ADR 010 cycle

- **Suspend ADR 010 / pivot the council to the Wendelon-side
  ADR 006 production row.** Considered (Gemini iter-0, Codex
  iter-0); rejected. Sound against the roadmap's printed A–E
  candidate list — every entry post-V1 or evidence-blocked — but
  the list predated the silent-drop defect. "Evidence drives
  scope" includes the code. The Wendelon row stays parallel work;
  it is not an ADR 010 blocker.
- **"ORDER BY only" scope (Claude topic iter-0).** Considered;
  superseded by Codex's clause-accounting broadening. Fixing four
  named clauses leaves the *class* open — the next `sqlparser`
  field is the next silent drop.
- **Two-bucket clause accounting (lower / reject).** Considered
  (Claude ADR iter-0); rejected (Codex iter-1). `sqlparser` 0.54
  `Select` carries non-semantic parser-metadata fields
  (`select_token`, `top_before_distinct`,
  `window_before_qualify`); routing them to "unsupported"
  overclaims. Three buckets — lowered / rejected-when-present /
  metadata-ignored — is the honest model.
- **`LIMIT` without `ORDER BY` via an implicit total-order
  tie-break.** Considered; rejected (Gemini topic iter-1). A bare
  `LIMIT` that silently triggers a full-dataset sort hides a
  performance cliff. Reject and force the user to declare
  ordering intent.
- **`ORDER BY` keys resolvable against pre-projection input.**
  Considered (Gemini ADR iter-0, Codex topic iter-1); rejected
  (Claude topic iter-2 S1, Codex ADR iter-1). It forces `Sort`
  below `Project` plus hidden-column machinery and leaves the
  total-order tie-break surface undefined. Output-schema-only
  keys; input-only keys are a named V1.x extension.
- **Tie-break keys constructed inside `SortExec`.** Considered
  (Claude ADR iter-0); rejected (Codex iter-1) for bind-time
  appending — the physical plan is then self-contained and
  inspectable.
- **Reject float sort keys in V1.** Considered (Claude ADR iter-0
  P0.5 fork); rejected (Codex iter-2). A sort key needs only a
  stable total order (Arrow IEEE 754) — distinct from float
  *join* keys, which need sound equality semantics and stay
  rejected.
- **Defer `ORDER BY` ordinals to V1.x.** Considered (Claude ADR
  iter-0 P0.7 fork); rejected (Codex iter-2). 1-based positive
  output ordinals resolve by position with no alias or
  hidden-column machinery — same "output-schema-only" class as
  column names.
- **Clause accounting as a table test only.** Considered;
  rejected (Claude topic iter-2 S2). A table test catches only
  enumerated clauses; the compile-time no-`..` destructure
  catches a clause nobody has seen yet.
- **New `TaskFailureKind::SortMemoryExhausted`.** Considered;
  rejected. Sort memory exhaustion is a budget event;
  `TaskFailureKind::BudgetExhausted { dimension:
  BudgetDimension::Memory, .. }` already exists. ADR 003
  7-variant guard preserved.
- **Leaving broadcast ordered joins out of the fragmenter
  path.** Considered; rejected (Codex iter-2). SQL lowering emits
  the same `Sort(Exchange₁(…))` shape regardless of join
  strategy; the post-join terminal gather-sort is shared and
  strategy-agnostic, not hash-partitioned-only.
- **Folding `DISTINCT` into ADR 010.** Considered; rejected.
  `DISTINCT` is `GROUP BY <all output columns>` with no
  aggregates — an aggregate-path concern. Closing its silent drop
  is in scope; implementing it is not.
- **Distributed range sort / external merge sort (spill) in
  V1.** Considered; rejected. Range-partition exchange,
  ordered-merge, skew mitigation, cost model — each its own
  scope. V1 is one in-memory terminal gather-sort; both
  extensions are named, evidence-driven V1.x ADRs.

### ADR 009 cycle

- **Treating ADR 009 as the first post-V1 ADR (to select
  Candidate A, multi-tenant governance).** Considered — the
  roadmap's candidate ordering lists A first; rejected. The
  V1-target paragraph lists multi-tenant governance as post-V1,
  and no post-ADR-008 evidence surfaces multi-tenancy as the
  binding constraint while a routine V1 SQL-surface gap is open.
  Claude iter-0 surfaced the roadmap internal tension; Gemini
  iter-1 and Codex iter-1 resolved it explicitly — ADR 009 is a
  V1 ADR.
- **Candidate B (cross-process wire efficiency) as ADR 009.**
  Considered; rejected as premature. B is the compute-ring
  fallback whose promotion condition ("ADR 006's selective bench
  misses") could not be evaluated until the Wendelon row landed.
  Selecting B then would have front-run that evidence; it now
  reopens only if later production-scale evidence misses.
- **General subquery execution engine.** Considered; rejected.
  ADR 009 is a narrow SQL-AST rewrite, not a subquery IR node +
  executor. The general engine is unbounded scope; the rewrite
  covers the routine analytical pattern with zero new execution
  surface.
- **Subquery joins in the V1 body.** Considered (Claude iter-0's
  "base relation or join"); rejected (Codex iter-1). Subquery
  joins widen side-resolution and residual-placement rules —
  scope bleed the narrow-ADR discipline exists to stop. Named,
  deferred extension; ADR 009 V1 is one `TableFactor::Table`.
- **Uncorrelated `EXISTS` in the same cycle.** Considered;
  rejected. Not a semi/anti rewrite — a scalar cardinality
  check. It falls out of the two-scope classifier as
  zero-correlation-pairs and stays unsupported.
- **Inner-only residual defined as "both sides resolve in the
  inner scope."** Considered (Claude iter-1 S1 phrasing);
  rejected (Codex iter-1) for "no outer references and lowerable
  against the inner relation" — the latter correctly admits
  literal predicates like `dim.active = true`.
- **A new unqualified-name resolution rule for subqueries.**
  Considered; rejected. ADR 009 should not grow fragile
  name-resolution machinery; unqualified / ambiguous correlation
  rejects, and the contract widens for free if later planner
  work hardens scoping.
- **`IN` / `NOT IN` decorrelation.** Considered as adjacent
  scope; rejected. `NOT IN`'s three-valued NULL logic is a
  separate decision matrix; its own ADR.
- **New error variant for unsupported `EXISTS` shapes.**
  Considered; rejected. No new failure class — the existing
  unsupported-expression class is the right surface (ADR 008
  P0.8 discipline).
- **New architecture guard for ADR 009.** Considered; rejected.
  ADR 009 changes no serialized shape and adds no variant —
  there is nothing to pin. It asserts the ADR 003 / 004 / 008
  guard set stays green instead.

### ADR 008 cycle

- **Six-variant `JoinKind` (no `RightSemi` / `RightAnti`).**
  Considered (Claude iter-0); rejected. `sqlparser` 0.54 exposes
  both directions natively and the executor's build-left /
  probe-right machinery supports both with identical
  infrastructure (`Right*` probe-time-decided; `Left*` after-pass
  on build-side bitmap). Excluding right-side would fragment the
  SQL surface without simplifying the implementation. Codex's
  symmetry argument was load-bearing.
- **Broadcast rewrite for non-Inner kinds** (sides-swap
  `RightOuter` → `LeftOuter`, fallback `FullOuter` →
  `HashPartitioned`). Considered (Claude iter-0); rejected.
  Strategy-dependent semantics blur the operator-level scale
  boundary — the binder would accept `Broadcast` but the planner
  would rewrite it, making the "what's the strategy" question
  ambiguous. Cleaner contract: binder rejects `Broadcast` for
  `kind != Inner`; SQL planner already defaults to
  `HashPartitioned` for non-Inner kinds. A strategy-rewrite ADR
  can follow if evidence supports it.
- **`EXISTS` / `NOT EXISTS` decorrelation to `Semi` / `Anti`.**
  Considered (Claude iter-0, Gemini iter-1); rejected for ADR
  008 scope. Subquery-planner contract requires its own decision
  matrix: which patterns are decorrelated, how does the planner
  detect them, how does the binder fold them. ADR 008 ships the
  `Semi` / `Anti` operators; a later subquery-planner ADR can
  leverage them. Promoted as ADR 009.
- **Per-kind probe loops without a row-selection intermediate.**
  Considered (Claude iter-0); rejected in favor of Codex's
  `JoinOutputRow` design. Per-kind loops duplicate null-fill code
  across paths and make the in-memory / spilled selection logic
  divergent (each kind reasoning about emission separately).
  Centralizing on `JoinOutputRow` means one selection/materializer
  contract — the spilled path emits the same selection
  representation after lookup (no spill-format change; ADR 005
  build-side-hash-page format is reused unchanged).
- **Sentinel-based null fill.** Considered as performance
  alternative; rejected. Breaks the Arrow contract, requires
  per-type sentinel choices, couples to downstream readers, and
  hidden semantics make outer-join output not interchangeable
  with inner-join output. Arrow's native `NullBuffer` is the
  canonical representation.
- **Loosen runtime-filter eligibility to outer / semi / anti.**
  Considered; rejected for ADR 008. A Bloom filter on the build
  side of a left-outer join would eliminate probe rows that
  don't match the build — but those are exactly the rows the
  outer join must preserve with null fill. `RightAnti` depends
  on emitting unmatched probe rows. A "filter + preserve"
  combined operator is the separable design; reserved as ADR 010
  Candidate C (V1.x runtime-filter extensions).
- **New `TaskFailureKind::JoinSemantics` variant.** Considered;
  rejected. No new failure class exists — the eight join kinds
  use existing build/probe/spill machinery and the existing
  budget envelope. Invalid plans surface as `FrontendError`;
  runtime resource failures map to existing
  `BudgetExhausted` / `TaskFailed`. ADR 003 7-variant guard
  stays load-bearing.
- **Second `ExecutionPlan::Join` or `LogicalPlan::Join` physical
  variant** (e.g., `ExecutionPlan::SemiJoin`). Considered;
  rejected. Per-kind physical variants duplicate the operator
  shape (two inputs, keys, strategy, schema) and weaken ADR
  004's single-variant architecture guard. The semantic
  difference belongs in `JoinKind`. `LogicalPlan::Join`
  single-variant guard added in Phase 1E pairs with the existing
  `ExecutionPlan::Join` guard.
- **Rolling coordinator/worker mixed-version semantics for
  `JoinKind`.** Considered as forward-compat affordance;
  rejected. Externally-tagged serde adds variants as new strings
  on the wire; older binaries deserialize unknown variants as
  errors. Promising rolling support would require either
  version-negotiation (cross-ring scope) or a serde fallback
  variant (changes wire semantics for every receiver). Single
  workspace bump, full-fleet upgrade matches the posture from
  prior ADRs.
- **Per-kind task-telemetry counters** (e.g.,
  `outer_rows_null_filled`). Considered; rejected. Existing
  `build_rows` / `probe_rows` / `output_rows` counters are
  sufficient for V1; kind-specific aggregates can be surfaced
  downstream of the task wire if needed. Adding telemetry fields
  is an ADR 003 task-telemetry contract change worth carrying
  only when bench evidence is insufficient.
- **Seven separate ADRs, one per kind.** Considered as
  scope-discipline alternative; rejected. The seven kinds share
  the same build-left / probe-right substrate, the same
  null-fill machinery, the same spill semantics. Splitting would
  duplicate contract framing five times without adding
  evidence-driven decisions. The scale-boundary section already
  prevents scope creep.
- **Inner-join regression gate at ±2%** (Claude iter-0 proposal)
  vs ±10% (council convergence). Considered; rejected in favor
  of ±10%. The `JoinOutputRow` refactor introduces measurable
  allocation overhead through the shared materializer; ±10%
  accommodates this without leaving the refactor with no
  guardrail. R13 mitigations are reversible if the bench shows
  >10% regression.
- **Drop IR variants if `sqlparser` regresses a spelling.**
  Considered as cleanup discipline; rejected. The serde wire
  contract and the architecture guard both depend on the IR
  variant set being stable. Fallback is to gate SQL lowering to
  whatever the current dialect parses; keep IR variants.

### ADR 004 – ADR 007 cycles (compressed)

Older cycles condensed to load-bearing rejected ideas; full
narrative lives in git history at the corresponding
`plan: adr NNN ...` commits.

**ADR 004 (distributed inner equi-join):**
- Hash-only V1 rejected — star-schema workloads need bounded
  broadcast.
- Unbounded broadcast rejected — three-cap regime is
  non-negotiable.
- Cost-based strategy selection rejected — V1 has no cost model;
  programmatic selection is the contract.
- Runtime filters as soft/hard gate rejected — cross-ring
  contract deserved its own ADR (became ADR 006).
- Operator-state hash spill deferred to V1.x rejected — V1
  needs realistic large-build joins; encrypted spill in scope.
- Reject duplicate output column names at binder rejected —
  naming is SQL-planner concern, not binder.
- Separate `ExecutionPlan::HashJoin` / `BroadcastJoin` variants
  rejected — `Join { strategy }` field-discriminated scales to
  V1.x cleanly.
- `BuildSide` enum on `Join` in V1 rejected — single-value
  enums violate the no-reservation convention.
- Inline broadcast payload in `StartTask` rejected — conflicts
  with ADR 003 pull-addressing.

**ADR 005 (hash-join build path):**
- Streaming push-based executor rejected by ADR 004 telemetry
  (probe + output ~7% of wall); demoted, reserved for future
  evidence shift.
- Sub-attribution-first investigation rejected — build bucket
  is sharper; conditional ADR 006 rule resolves the residual.
- Fast-path eligibility extending to all primitive integer types
  rejected — conservative `Int64`/`Int32` default; each
  extension carries logical-type-tag obligations.
- Pinning a specific hash table layout rejected — contract pins
  invariants, layout is calibration evidence.
- Hash function change rejected — `xxh3_64` parity required for
  partition-assignment consistency (ADR 003 §5).
- Concurrent / sharded hash-table build in V1 rejected —
  determinism, per-thread memory, allocator questions need
  their own ADR.
- New telemetry columns within ADR 005 rejected — ADR 004's
  columns sufficient.

**ADR 006 (runtime filter pushdown):**
- Filter generation inside `execute_inner_join` rejected —
  Codex iter-2 stage-order-honesty: filter built in the join
  consumer runs after both producer-side shuffles complete,
  too late to attack bytes-movement.
- Broadcast-only V1 + hash-partitioned-as-ADR-007 rejected —
  half-ships the contract.
- Split into ADR 006 (stage-order) + ADR 007 (filter impl)
  rejected — two ADRs for one feature.
- Exact `InList` filters in V1 rejected — Bloom-only proves
  the cross-ring + stage-order contract first (ADR 014 later
  added ExactSet).
- Cost-based planner opt-in rejected — no planner-stats surface
  exists; always-two-wave in V1.
- Row-group skip as required V1 enforcement rejected — Bloom
  membership is per-encoded-key; row-group skip stays
  opportunistic.
- New runtime-filter telemetry columns rejected — bench
  evidence suffices.
- Always-emit fragments (no eligibility check) rejected —
  composite-key, non-canonical, broadcast joins should not pay
  filter generation overhead.
- `nilket-shared` as required new crate rejected (softened to
  default recommendation only) — extending an existing
  Wendelon-published schema crate is preferred before
  multi-consumer pressure justifies a new crate.

**ADR 007 (Rune sandbox enforcement):**
- Fuel-only V1 sandbox rejected — memory bomb fitting within
  fuel can still OOM the host; memory enforcement is V1 hard
  requirement.
- `Context::with_default_modules()` with deny-list tests
  rejected — allow-list discipline (`Context::new()` + explicit
  registration) inverts the failure mode.
- New `TaskFailureKind` variants (`SandboxFuelExhausted`,
  `SandboxMemoryExhausted`) rejected — preserves the ADR 003
  7-variant wire contract; reason-string mapping
  (`"RuneSandbox::<label>"`) is the right cross-wire surface.
- Raw missing-function strings in `CapabilityDenied` rejected
  in favor of `CapabilityCategory` enum (more robust to Rune
  version drift).
- Row-level Rune UDF execution rejected — Rune is
  frontend-scoped in V1; worker-side execution is a future ADR.
- Subprocess sandbox with full seccomp + rlimit rejected —
  helper-process isolation is lightweight enough.
- WASM runtime migration rejected — would replace M7's
  authoring surface entirely; reserved for future ADR if Rune
  sandbox proves insufficient.
- Wall-timeout as primary CPU accounting rejected — fuel is
  primary; wall-timeout is defense-in-depth.
- `execute_script(script, fuel)` compatibility shim that
  ignores the fuel arg rejected — dead `_fuel` parameter cannot
  remain load-bearing.

### ADR 003 cycle (preserved from prior roadmap)

- **New IR level below `ExecutionPlan`** (`PhysicalFragment`
  vocabulary). Considered; rejected because `TaskSpec`
  wraps the bound plan without adding translation surface.
- **Pure spill-to-pull as V1 default shuffle.** Considered
  for max resilience; rejected because disk-write +
  decrypt latency on every shuffle is unacceptable for
  in-memory-sized queries.
- **Transport-native flow control alone as backpressure
  contract.** Considered; rejected because wire-level
  flow control can't bound application-level memory or
  shuffle bytes.
- **Schema-registry hardening as ADR 003 alternative.**
  Considered; rejected for lack of high-frequency DDL
  pressure in ADR 002 bench evidence.
- **Stable worker assignment as the determinism gate.**
  Considered; rejected because worker pools are dynamic
  and assignment parity isn't a useful user-facing
  contract.

## What Not To Do Next

Enduring guardrails for upcoming work. Items tied to
specific shipped ADRs live in those ADRs' §"What Not To Do"
sections; this list is the cross-cutting set.

- **Don't adapt DataFusion / Ballista.** ADR 001 rejected;
  ADR 002 / 003 reinforced. The greenfield decision is
  final unless a future ADR cites concrete evidence
  reversing the surface-area-mismatch argument.
- **Don't bundle scope across contract surfaces.**
  Distributed joins, executor model upgrade, runtime
  filter pushdown, outer joins, multi-tenant governance,
  and Rune sandbox enforcement are each their own ADR.
  ADR 003 was intentionally narrow; ADR 004 inherits the
  discipline; ADR 005 stays narrow.
- **Don't ungate experimental kernels by default.** ADR
  002 requires `PlannerConfig::allow_experimental_kernels
  = false`. Each kernel graduates under its own narrow
  ADR.
- **Don't put `Result<_, String>` in any public or module-
  crossing signature.** ADR 002 G3 enforces; ADR 003 G9
  enforces for distributed surface; ADR 004 inherits.
- **Don't skip descriptor-fingerprint validation on PBC
  pushdown.** Silently-wrong-results is the failure mode
  ADR 002 paid complexity to prevent.
- **Don't extract Wendelon types into a shared crate
  before V1.x consumer pressure.** ADR 001 / 002 / 003
  import allowlist holds.
- **Don't bypass the Wendelon import allowlist.**
  Production code consumes `wendelon::schema::{pbc, *types}`,
  `wendelon_client::*`, `wendelon::network::pool::*`,
  `wendelon::storage::file_ops::*`. Forbidden:
  `wendelon::network::raft::*`, `wendelon::network::quic::*`,
  `wendelon::storage::lsm::*`, ad-hoc page readers,
  anything inside Wendelon's node / raft / transport
  internals.
- **Don't query Wendelon for ring placement.** ADR 003
  V1 is location-agnostic; ADR 004 inherits. Locality
  routing is V1.x.
- **Don't initiate P2P-push from producer to consumer.**
  ADR 003 G11 enforces. Consumers always pull via
  `PullPartition`.
- **Don't use `tokio::time::sleep` for backpressure** in
  the production worker path. ADR 003 G10 enforces. The
  per-query budget envelope is the contract.
- **Don't dispatch `LogicalPlan` to workers.** ADR 003
  G12 enforces. Coordinator binds; workers receive
  `TaskSpec`.
- **Don't let `MorselId` cross the wire.** ADR 003 G14
  enforces. Worker-local scheduling detail only.
- **Don't introduce a new IR level below `ExecutionPlan`.**
  `TaskSpec` wraps; it doesn't transform.
- **Don't conflate task-lifecycle failures with partition-
  store wire errors.** `TaskFailureKind` is 7 variants;
  `PartitionMissing` is a wire response carrying
  `PartitionMissingReason`, not a `TaskFailed` variant.
- **Don't start Stage N+1 tasks before Stage N manifests
  are accepted.** ADR 003 G13 enforces strict barrier;
  ADR 004 G18 generalizes for multi-upstream.
- **Don't promise byte-identical Arrow output without
  canonical normalization.** ADR 003 §13 normalization
  (concat → full-row sort → reslice → digest) is the
  contract; ADR 004 reuses unchanged.
- **Don't require stable worker assignment for
  determinism.** ADR 003 output parity is the contract.
- **Don't promote a kernel without a bench-evidence
  artifact.** Wendelon ADR 070 pattern — narrow ADR +
  bench results + roadmap update, every time.
- **Don't open the next ADR with a mega-scope.** One
  contract surface per ADR; bundling defers real
  decisions.
- **Don't promote stabilization watch items into roadmap
  items.** Observability metrics, architectural-test
  guards, post-merge dashboards belong in runbooks and
  impl plans. Decision-enabling measurement belongs
  inside the owning ADR.
- **Don't introduce non-pinned join algorithms.** ADR 004
  G16 enforces. Sort-merge, nested-loop, non-equi joins,
  cross join — each is its own future ADR. Outer / semi / anti
  joins now pinned by ADR 008 (eight-variant `JoinKind`); the
  underlying hash-build / probe-right algorithm is unchanged.
- **Don't reject duplicate output column names at the
  binder.** ADR 004 architectural division puts naming
  in the SQL planner, ordinals in the bound IR.
- **Don't synthesize `Exchange` in the fragmenter.** The
  planner emits explicit `Exchange` above each join /
  aggregate input; the fragmenter never adds operators.
- **Don't allow broadcast to bypass caps.** ADR 004
  pinned three caps; over-cap fails terminal; no silent
  strategy switch.
- **Don't reserve placeholder fields, message slots, or
  PBC ops for deferred features.** Convention
  `no-premature-backward-compat.md`. The future ADR adds
  the surface when it pins the contract.
- **Don't bundle ADR 003 telemetry residual into ADR 004
  feature commits.** Separate `chore:` slice (ADR 004
  council decision 19).
- **Don't conflate join-build key hashing with
  exchange-partition hashing.** ADR 003 §5 partition assignment
  uses `xxh3_64(canonical_encoded_bytes)`; ADR 005's fast-path
  key representation is operator-local. Helper names must make
  the boundary explicit ("join build key" vs "exchange partition
  hash"), never a generic "fast hash" that blurs them. ADR 005
  hard gate 14 conditional architecture guard.
- **Don't let an ADR 005 fast-path encoder lose the logical-type
  tag.** `Int32(1)` and `Int64(1)` must remain distinct
  build-side hash keys. ADR 005 hard gate 7 unit-test enforced.
- **Don't promote ADR 005 on wall-clock alone.** `build_ms` is
  the primary signal; digest equality and phase attribution are
  load-bearing. ADR 005 hard gate 16.
- **Don't soften an ADR 005 `build_ms` target miss** into a
  vague "improved" narrative. The bench artifact records the
  explicit gap; the council selects accept-narrower / scope-down
  / shelve.
- **Don't add new contract surface in ADR 005.** No new
  `MessageKind`, IR variant, `BudgetDimension`, task telemetry
  field, public API, Wendelon scan contract, or architecture
  guard (default; conditional escape only). ADR 005 hard gates
  3, 12-14.
- **Don't parallelize hash-table build inside a worker in
  ADR 005.** Sharded concurrent insertion is V1.x with its own
  determinism + budget accounting work.
- **Don't bundle runtime-plumbing investigation into ADR 005.**
  Conditional ADR 006 governed by the three-band rerun rule.
- **Don't generate ADR 006 runtime filters inside
  `execute_inner_join`.** Too late in ADR 004's stage graph;
  the probe-side shuffle has already happened. Filter
  generation lives in the build-side exchange producer wave
  (ADR 006 Decision §1).
- **Don't let ADR 006's runtime-filter hash conflate with the
  exchange-partition hash.** ADR 003 §5 partition assignment
  uses `xxh3_64(canonical_encoded_bytes)`; ADR 006's
  runtime-filter hashing is operator/cross-ring scoped.
  Helper names are semantic ("runtime filter key" vs
  "exchange partition hash"). Generalizes ADR 005's pattern.
  ADR 006 hard gate 15.
- **Don't let an ADR 006 runtime filter affect join
  correctness.** Advisory always — false positives allowed,
  false negatives forbidden. If Wendelon drops the filter for
  any reason, output is byte-identical to filter-disabled.
  ADR 006 hard gate 7 (false-positive tolerance) + hard
  gate 10 (no false negatives).
- **Don't count ADR 013 advisory-drop behavior as production
  runtime-filter application evidence.** Advisory drop is the
  safety fallback; promotion evidence needs an applied Wendelon
  filter with fewer probe rows/bytes and digest equality.
- **Don't add ADR 013 runtime-filter key columns to scan output.**
  Wendelon may fetch probe keys through
  `ColumnFetchRequirement::predicate_columns`, but PBC output is
  still governed by `output_columns`.
- **Don't put `RuntimeFilterDescriptor` into
  `TableQueryOptions`.** ADR 013 keeps options `Copy`; the
  descriptor is query payload carried by runtime-filter-specific
  table-query entry points.
- **Don't start probe-side dispatch before the ADR 006
  build-side runtime-filter barrier opens** when filters are
  enabled. ADR 006 hard gate 13 — the two-wave barrier is
  contractually load-bearing.
- **Don't deserialize ADR 006's runtime-filter descriptor as
  a Toron-private type in Wendelon.** Shared cross-ring schema
  surface only (default: extend an existing Wendelon-published
  schema crate; alternative: `nilket-shared` common module).
- **Don't multiply ADR 006 filter kinds in V1.** Bloom only.
  Exact `InList` is V1.x once the cross-ring contract is
  proven.
- **Don't add ADR 006 planner cost-model opt-in in V1.** Always
  two-wave when the filter is enabled; planner-stats opt-in
  is V1.x once stats exist.
- **Don't couple ADR 006 promotion to lockstep Wendelon-side
  landing.** Advisory invariant makes split-shipment safe.
  Toron-side shipped first; the Wendelon-enabled production row
  arrived through ADR 013.
- **Don't extend ADR 006 filter eligibility to outer / semi /
  anti / sort-merge / non-equi / broadcast joins.** Each is
  its own future ADR (V1.x). Inner + hash-partitioned only in
  V1.
- **Don't soften an ADR 006 selective-row reduction miss**
  into a vague "improved" narrative. The bench artifact
  records the explicit gap; the council selects
  accept-narrower / scope-down (narrow filter eligibility) /
  shelve.
- **Don't promote a runtime-filter ADR without a
  no-false-negative unit test.** The advisory invariant is
  load-bearing; false negatives are contract violations.
  ADR 006 hard gate 10.
- **Don't skip the non-selective guard benchmark.** The
  two-wave barrier cost is real; the guard proves it's
  bounded against ADR 005's `build_ms` baseline. ADR 006
  hard gate 19.
- **Don't skip the Scale boundary section in new ADRs**
  (convention adopted iter-5 of ADR 006). Even when the
  boundary feels obvious, naming it explicitly makes
  scope-creep into adjacent boundaries visible at review
  time. Omit only when truly N/A and say so explicitly.
- **Don't call `Context::with_default_modules()` in the Rune
  engine** (ADR 007 hard gate 19). The V1 contract is
  allow-list (`Context::new()` + explicit registration); a
  future refactor reverting to default-allow opens the
  sandbox. Source-level architecture grep guard at
  `tests/architecture_test.rs` enforces this.
- **Don't ship a fuel-only V1 Rune sandbox.** Memory
  enforcement is V1 hard requirement (ADR 007 hard gate 8). A
  memory bomb that fits within fuel can still OOM the host —
  Rune-native cap or helper-process isolation, not "deferred."
- **Don't add new `TaskFailureKind` variants for sandbox
  failures.** ADR 003 7-variant wire contract is
  architecture-guarded. Sandbox failures crossing the task wire
  map to `TaskFailureKind::TaskFailed { retryable: false,
  reason: "RuneSandbox::<label>" }`.
- **Don't let Rune sandbox failures fall through to
  `RuneRuntime`.** Use the `FrontendError::RuneSandbox`
  variant carrying the typed `RuneSandboxViolation` enum.
  `RuneRuntime` is for genuine script-logic errors only.
- **Don't expose host paths, internal VM state, or Rune
  source positions in coordinator-facing error messages**
  (ADR 007 hard gate 20). `CapabilityCategory` enum abstracts
  over raw module paths for stability and non-leakiness.
- **Don't add native functions to the Rune allow-list without
  threat-model review.** Each new function is a potential
  sandbox-escape vector. Extension is a separate narrow ADR.
- **Don't couple Rune sandbox enforcement to per-tenant
  governance.** Tenant identity is L-3 (multi-tenant
  governance) territory. ADR 007 ships per-*script* budgets;
  per-tenant aggregation is a future ADR when workload evidence
  justifies.
- **Don't treat `wall_timeout` as primary CPU accounting.**
  Fuel is primary; `tokio::time::timeout` around
  `async_complete()` is a defense-in-depth belt that fires only
  at .await yield points (Rune 0.13.4 drives the VM via a
  synchronous `vm.run()` inside `async_resume()`). Real
  preemption of a CPU-bound loop with broken fuel comes from the
  Phase 4 helper-process parent-side SIGKILL deadline, not the
  Tokio timeout. The infinite-loop hard gate (gate 7) must
  assert `FuelExhausted`, not merely `Timeout`.
- **Don't run row-level Rune UDFs in worker tasks under
  ADR 007.** Rune is frontend-scoped in V1 (script →
  `LogicalPlan` → distribution). Worker-side execution is a
  substantial future ADR.
- **Don't introduce a second `ExecutionPlan::Join` or
  `LogicalPlan::Join` physical variant** (ADR 008 hard gates
  22, 23). Per-kind physical variants duplicate the operator
  shape and weaken ADR 004's single-variant architecture guard.
  The semantic difference belongs in `JoinKind`.
- **Don't use sentinel values for null fill in outer joins.**
  Arrow's native `NullBuffer` is the canonical representation;
  sentinels break the contract and couple to downstream readers.
  ADR 008 hard gate 10.
- **Don't loosen runtime-filter eligibility beyond `Inner`.**
  ADR 006 invariant is load-bearing for outer / anti
  correctness. A Bloom filter on the build side of a left-outer
  join would drop the very rows the outer must preserve;
  `RightAnti` depends on emitting unmatched probe rows. ADR 008
  hard gate 17 enforces; a "filter + preserve" combined operator
  is the separable future design (ADR 010 Candidate C).
- **Don't allow `JoinStrategy::Broadcast` for non-Inner
  `JoinKind`.** Broadcast replicates the build side; preserved-
  side or existence variants would emit duplicate unmatched
  rows in every probe partition. ADR 008 hard gate 16 (binder
  rejection) + hard gate 18 (fragmenter defensive guard) form
  the defense-in-depth pair.
- **Don't rewrite `RightOuter` ↔ `LeftOuter` at planner time as
  a workaround for broadcast.** Strategy-dependent semantics
  blur the operator-level scale boundary. The cleaner contract
  is reject-and-defer; a strategy-rewrite ADR can follow if
  evidence supports it.
- **Don't add `EXISTS` / `NOT EXISTS` decorrelation under ADR
  008.** Subquery-planner contract; promoted as ADR 009. ADR 008
  ships the `Semi` / `Anti` operators; ADR 009 layers the
  correlated-`EXISTS` rewrite on top.
- **Don't promise rolling coordinator/worker mixed-version
  semantics for `JoinKind`.** Serde-tagged variants are additive
  but older binaries reject unknown variants. ADR 008 ships at a
  single workspace version bump; cluster upgrades are
  full-fleet, not rolling. Matches the posture from prior ADRs.
- **Don't drop `JoinKind` IR variants if `sqlparser` regresses
  a spelling.** Keep the IR variants; gate SQL lowering to
  whatever parses. Dropping IR variants breaks the serde wire
  contract and ADR 008 hard gate 25 (variant-count architecture
  guard).
- **Don't add per-kind task-telemetry counters for the new join
  kinds.** ADR 003 task-telemetry contract change worth carrying
  only when bench evidence is insufficient. Existing
  `build_rows` / `probe_rows` / `output_rows` counters are
  sufficient for V1; kind-specific aggregates surface downstream
  of the task wire if needed.
- **Don't add a subquery node to Toron's IR for `EXISTS`
  decorrelation.** ADR 009 is a targeted SQL-AST rewrite in
  `SqlPlanner::lower_select`; the logical and physical join
  contract is reused unchanged. A subquery IR node is a general
  subquery-engine decision, not ADR 009's.
- **Don't lower the whole subquery predicate through `lower_expr`
  before classifying.** ADR 009's two-scope classifier operates
  on the `sqlparser` AST so unsafe outer-reference shapes reject
  precisely; a generic lowering pass first would route a mixed
  predicate into a generic filter path. Classify, then lower only
  the accepted `CorrelatedPair` columns and `InnerResidual`
  predicates.
- **Don't invent a new unqualified-name resolution rule for
  subqueries.** ADR 009 rejects unqualified / ambiguous
  correlation rather than guessing. If the existing qualifier
  logic cannot pin one side to the outer scope and one to the
  inner scope, the rewrite is unsupported — "too narrow and
  correct" over matching a permissive engine.
- **Don't decorrelate uncorrelated `EXISTS`, `IN`, or `NOT IN`
  under ADR 009.** Uncorrelated `EXISTS` is a scalar cardinality
  check, not a semi-join; `NOT IN` carries three-valued NULL
  logic that must not be smuggled into anti-join semantics. Each
  is a named, deferred extension with its own decision matrix.
- **Don't widen the ADR 009 subquery body beyond one
  `TableFactor::Table`.** Subquery joins, aggregates, `LIMIT`,
  `DISTINCT`, set ops, and nested subqueries are unsupported and
  reject through the existing path. The classifier's
  `Unsupported` arm is the structural scope boundary.
- **Don't duplicate binder semantic checks in the SQL planner
  for ADR 009.** The planner classifies shape and builds the
  logical rewrite; the binder stays the authority for type
  compatibility, float-key rejection, computed-key rejection,
  exchange shape, and non-Inner broadcast rejection.
- **Don't let `lower_query` / `lower_select` destructure with a
  `..` rest pattern.** ADR 010's exhaustive three-bucket
  no-`..` destructure is the load-bearing clause-accounting
  guard — a `sqlparser` bump that adds a field must be a compile
  error, not a silent widening. A source-level architecture
  guard enforces it; the parser-metadata allow-list
  (`select_token`, `top_before_distinct`,
  `window_before_qualify`) is explicit.
- **Don't silently drop any top-level `Query` / `Select`
  clause.** ADR 010's invariant: every parsed field is
  lowered, rejected-when-present, or bound-and-ignored as parser
  metadata. Silently-dropped `ORDER BY` was the V1 integrity
  defect ADR 010 closed; do not reintroduce the class.
- **Don't accept bare `LIMIT` / `OFFSET` / `FETCH`.** Without an
  `ORDER BY` contract, distributed truncation is arbitrary.
  ADR 010 rejects them; deterministic top-N is
  `ORDER BY ... LIMIT n`.
- **Don't place `Sort` below `Project` or support `ORDER BY` on
  non-output / alias / computed keys.** ADR 010 keys resolve
  against the output schema only (column names + 1-based
  positive ordinals); `Sort` sits above `Project` so the
  total-order tie-break surface is exactly the output columns.
  Input-only keys are a named V1.x extension.
- **Don't ship a partial-order `ORDER BY`.** ADR 010's contract
  is a total order — user keys then every remaining output
  column, ascending `NULLS LAST`, appended at bind time. Ties
  resolved arbitrarily reopen silent nondeterminism.
- **Don't let the fragmenter infer a hidden gather, and don't
  prove the distributed gather-sort with output digest alone.**
  The planner emits the terminal `Exchange { partition_count: 1 }`
  explicitly; the post-join gather-sort is strategy-agnostic
  (hash-partitioned and broadcast) and gated by stage-shape
  inspection. ADR 010 Phase 4 is the named primary risk.
- **Don't add a new `TaskFailureKind`, `BudgetDimension`, IR
  level, or wire surface for ADR 010.** `LogicalPlan` /
  `ExecutionPlan` each gain exactly one variant (`Sort`); sort
  memory exhaustion reuses `BudgetDimension::Memory` →
  `ExecutionError::BudgetExhausted` →
  `TaskFailureKind::BudgetExhausted`. No sort spill, no
  distributed range sort in V1 — both are named V1.x ADRs.
- **Don't implement `DISTINCT` / `GROUP BY` / `HAVING` under
  ADR 010.** They loud-reject; implementing them is
  aggregate-path ADR work. ADR 010 closes their silent drop
  only.
