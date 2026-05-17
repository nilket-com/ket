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

### ADR 028 promoted; ADR 029 candidates

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
