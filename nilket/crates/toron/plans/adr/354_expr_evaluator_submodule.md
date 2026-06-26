# ADR 354 - carve exec/expr.rs (the expression evaluator) + no-qNN-reference de-branding

## Status
Proposed.

## Context

The next root carve. `exec.rs` root is **9,772 lines**; the expression-evaluation cluster is its largest sub-system and
a clean **leaf** (calls IR types + Arrow/arithmetic kernels; no upward dispatch dependency). Carve it into
`exec/expr.rs`, same 350-353 playbook (coupling map first, full-cluster, behavior-neutral, guard-ratcheted, ground on
`cargo-cgroup check`). Folds in a **de-branding rider** (Codex): remove query-number **comments** from executor modules
and strengthen the guard from no-qNN-*identifiers* to no-qNN-*references*.

### API correction (Codex) - split impl Executor, not a 30-symbol re-export
The premise "wide free-function re-export" was wrong. Expr's public surface is **`impl Executor` methods**:
`evaluate_bound_program`, `evaluate_bound_program_without_storage`, `filter_batches_with_bound_predicate`, and
`evaluate_expr` is a `#[cfg(test)]` legacy `Executor` method; these are used outside `exec` **through `Executor`**. So:
- **Move the evaluator as a split `impl Executor` block** in `exec/expr.rs` - call shapes stay identical
  (`self.evaluate_bound_program(...)`, `Executor::evaluate_bound_program_without_storage(...)`,
  `executor.filter_batches_with_bound_predicate(...)`), so the ~261 call sites **do not change**.
- **Preserve method visibility deliberately.** Split-impl methods used from sibling modules keep their reachable
  visibility: `evaluate_bound_single_root` and `filter_batch_with_bound_predicate` become `pub(in crate::exec)`;
  `filter_batches_with_bound_predicate`, `evaluate_bound_program`, and `evaluate_bound_program_without_storage` keep
  their existing broader surfaces.
- **Re-export only the tiny external free-function surface that already exists** - mainly `evaluate_cast` (used by
  `coordinator/write.rs`).
- **Keep helper free functions private inside `expr.rs`** (`evaluate_arithmetic`, `evaluate_case`, `evaluate_binary`,
  the typed/temporal family, ...) - they're called by the moved methods, not externally.

## Decision - carve the full expr cluster (split-impl), narrow re-export, de-brand

### The boundary - the full expr cluster
- **Methods (split impl Executor):** `evaluate_bound_program`, `evaluate_bound_program_without_storage`,
  `evaluate_bound_single_root`, `filter_batches_with_bound_predicate`, `filter_batch_with_bound_predicate`,
  `#[cfg(test)] evaluate_expr`.
- **Free helpers (private in expr.rs):** `evaluate_bound_binary`/`unary`, `evaluate_in_list`/`case`/`like`/`substring`/
  `at_time_zone`/`extract`/`date_trunc`/`time_bucket`, the arithmetic/cast family (`evaluate_arithmetic`,
  `evaluate_temporal_arithmetic`, `evaluate_calendar_arithmetic`, typed/interval, `evaluate_decimal_division`,
  `evaluate_unbound_arithmetic`, `evaluate_try_cast`).
- **Re-export (narrow):** `evaluate_cast` (+ any other existing externally-used free fn Phase 0 finds) via
  `pub(in crate::exec)` / `pub(crate)` as today.
- expr eval unit tests that move cleanly; mixed-harness tests stay (documented).

### Stay root (scope fence)
`execute_bound`/dispatch, scan/exchange, **system-table / recursive / materialization helpers** - including
`batches_to_scalar_rows`, `scalar_value_from_array`, and `materialize_scalar_rows` - stay rooted **unless Phase 0
classifies a specific helper as expr**. The IR types stay in `ir.rs`; kernels in Arrow / `aggregate_kernel.rs`.

### The de-branding rider (behavior-neutral, comments only)
- Remove query-number **comments** from executor modules (current offenders: `Q6`/`Q1` comments in `exec.rs` and
  `exec/aggregate.rs`); translate any load-bearing intent into invariant prose (not query archaeology).
- **Strengthen the guard:** `exec_decomposition.rs` checks **no query-number references** (comments *and* identifiers)
  in `src/exec/` modules, not just identifiers.

### The guard - extend `exec_decomposition.rs`
- exec root declares `mod expr;`; `src/exec/expr.rs` exists; the moved expr symbols are no longer in the exec root.
- exec-root line count ratchets **down** below 9,772 (Phase-0 measured, fails on expr drift-back) - the biggest drop.
- **no query-number references** (comments + identifiers) across `src/exec/` modules.
- public surface (`Executor`, `ExecutionContext`, `materialize_scalar_rows`, the `pub(crate)` `evaluate_cast`/
  `filter_batches_with_bound_predicate`) reachable as before.

## Phase 0 - the coupling map (three-bucket; produce BEFORE moving)
For each expr symbol: kind (impl-method / free helper / test) / external-vs-internal caller / leaf-property (no upward
dispatch call) / qNN-comment present -> bucket move-now (expr cluster) / stay-root (dispatch, system-table/recursive/
materialization, non-expr interspersed) / defer. Confirm the split-impl methods + the single `evaluate_cast` re-export
cover all callers; confirm cross-module split-impl methods have `pub(in crate::exec)` where needed; confirm helpers can
stay private. Enumerate the qNN-comment offenders. Set the root ratchet.

## Acceptance (behavior-neutral, full-cluster, split-impl, de-branded)
- `exec.rs` -> root + `exec/expr.rs` (full expr cluster as split impl + private helpers + narrow `evaluate_cast`
  re-export); **~261 call sites unchanged** (call shapes preserved); system-table/recursive/materialization rooted.
- **qNN comments removed** from `src/exec/`; the guard enforces **no qNN references**.
- **Safety net green + unchanged:** `--lib`, `--test architecture_test`, `--no-fail-fast`, `toron-tpch`,
  **`correctness-matrix` 22/22 + `matrix` 22/22 bit-identical** (the sf=1 board is the tripwire).
- `exec_decomposition.rs` extended + green; exec root ratcheted down (biggest reduction yet).
- no perf/streaming/budget/cost-model change.

## Guardrails
- **Split impl Executor** - the evaluator methods move as a split impl block; call shapes unchanged; no wide re-export.
- **Method visibility is part of the API** - sibling-module callers must still reach the split-impl methods without
  routing through free-function wrappers.
- **Narrow re-export** - only `evaluate_cast` (+ any existing external free fn); helpers stay private in expr.rs.
- **Scope fence** - system-table/recursive/materialization stay rooted unless Phase 0 proves expr; not a dispatch
  cleanup.
- **De-branding** - remove qNN comments; guard on no-qNN-references (comments + identifiers); behavior-neutral.
- **Behavior-neutral** - pure move + comment removal; sf=1 board bit-identical.
- **LSP diagnostics advisory** - stale errors expected (350-353); authoritative = `cargo-cgroup check` + guard + board.

## Risks
- **Helper privacy** - a "helper" actually called from another exec module needs `pub(in crate::exec)`, not private;
  Phase 0's caller column catches it (keep the surface minimal, but correct).
- **qNN-reference guard false positives** - a substring like `q1` in unrelated prose; scope the guard to query-number
  patterns precisely (the `monotonic`/`tonic` lesson - avoid over-broad substring matches).
- **Behavior drift** - the sf=1 tripwire (22/22 bit-identical) + `--no-fail-fast`.
- **Scope creep** - system-table/recursive/materialization dragged in; the scope fence + exactly-one-carve bound it.

## Forward arc (the ratchet)
350-353 -> **354 (`exec/expr.rs`, the leaf + de-branding)** -> later (context, the public face, last) -> **then** step 2
(the perf/regression rail) on a navigable exec. After expr, the root is mostly dispatch + context + remaining
executors. Gemini's full structure is the end state.

## Why toron-home
Pure `crates/toron` source-structure cut + de-branding (exec.rs -> exec/expr.rs + guard). No Wendelon / cross-ring
surface; no behavior change.

## Council provenance
Codex + Claude. Same 176/350-353 pattern, now the expr evaluator. Codex's tightenings (adopted): (1) **hybrid API** -
move the evaluator as a **split `impl Executor` block** (call shapes unchanged), re-export only the tiny external free
fn (`evaluate_cast`), keep helpers private - **not** a 30-symbol re-export; (2) **premise correction** - expr is not
just free leaf fns (`evaluate_expr` is a `#[cfg(test)]` method; public methods used outside via `Executor`), so
split-impl is the right mechanism; (3) **de-branding rider** - remove qNN **comments** from executor modules (Q6/Q1 in
exec.rs/aggregate.rs) and strengthen the guard to **no-qNN-references**; (4) leave system-table/recursive/
materialization rooted unless Phase 0 classifies them as expr.
