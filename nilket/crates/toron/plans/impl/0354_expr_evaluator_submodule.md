# ADR 354 - Implementation plan: carve exec/expr.rs (split-impl) + no-qNN-reference de-branding

Pairs with `plans/adr/0354_expr_evaluator_submodule.md`. A pure `crates/toron` source-structure cut + behavior-neutral
de-branding: carve the expr evaluator out of the `exec.rs` root (9,772 lines) into `src/exec/expr.rs` as a **split
`impl Executor` block** (call shapes unchanged) + private helpers + a narrow `evaluate_cast` re-export; remove qNN
comments from executor modules and strengthen the guard to **no-qNN-references**. Exactly one carve.

## Phase 0 - the coupling map (the go/no-go; produce BEFORE moving)
- **Enumerate the expr cluster:** methods (`evaluate_bound_program`, `evaluate_bound_program_without_storage`,
  `evaluate_bound_single_root`, `filter_batches_with_bound_predicate`, `filter_batch_with_bound_predicate`,
  `#[cfg(test)] evaluate_expr`); free helpers (`evaluate_bound_binary`/`unary`, `in_list`/`case`/`like`/`substring`/
  `at_time_zone`/`extract`/`date_trunc`/`time_bucket`, the arithmetic/cast family).
- **Per symbol:** kind / external-vs-internal caller / leaf-property (no upward dispatch call) / qNN-comment present.
- **Caller column:** confirm the split-impl methods keep all `self.`/`executor.`/`Executor::` call shapes; find which
  split-impl methods are called from sibling modules -> `pub(in crate::exec)` where needed
  (`evaluate_bound_single_root` for aggregate, `filter_batch_with_bound_predicate` for join/streaming). Find which free
  helpers (if any) are called from other exec modules -> `pub(in crate::exec)` (else private). Confirm only
  `evaluate_cast` (+ any existing external free fn) needs a root re-export (coordinator/write.rs).
- **Scope fence:** system-table / recursive / materialization helpers stay rooted unless a specific one is provably
  expr; `batches_to_scalar_rows`, `scalar_value_from_array`, and `materialize_scalar_rows` are rooted by default; not a
  dispatch cleanup.
- **qNN offenders:** list the `Q6`/`Q1` (and any other) query-number comments in `src/exec/` to remove.
- **Threshold:** measure exec-root + `expr.rs` line counts after the move; set the root ratchet below 9,772 with churn
  slack, fails on expr drift-back.

## D1 - the carve (split-impl; pure move)
- Create `src/exec/expr.rs`; move the evaluator methods as a **split `impl Executor` block** (verbatim, no logic
  change); preserve method visibility for sibling-module callers (`pub(in crate::exec)` where needed); move the free
  helpers as **private** fns (or `pub(in crate::exec)` only where a cross-module caller needs them); declare `mod expr;`
  in the exec root.
- **Re-export only** `evaluate_cast` (+ any existing external free fn) at the prior `pub(crate)` / `pub(in crate::exec)`
  visibility - call sites unchanged.
- **Do not move** system-table/recursive/materialization or dispatch (scope fence).
- Local expr tests move; mixed-harness tests stay (documented).
- `Executor`, `ExecutionContext`, `materialize_scalar_rows` reachable as before; the ~261 call shapes unchanged.

## D2 - de-branding (behavior-neutral, comments only)
- Remove the `Q6`/`Q1` (and any other) query-number comments from `src/exec/` modules (exec.rs, aggregate.rs, and the
  moved expr.rs); translate load-bearing intent to invariant prose where needed (no query archaeology).

## D3 - extend the guard (`exec_decomposition.rs`)
- `mod expr;` declared; `src/exec/expr.rs` exists; the moved expr symbols are no longer in the exec root.
- exec-root line count ratcheted below 9,772 (fails on expr drift-back).
- **no query-number references** (comments + identifiers) across `src/exec/` modules - scope the pattern precisely to
  query-number forms (avoid over-broad substring matches, the `monotonic`/`tonic` lesson); public-surface assertion
  unchanged.

## Tests / safety net (behavior-neutral is the whole game)
- **The sf=1 board is the tripwire:** `correctness-matrix` 22/22 + `matrix` 22/22 **bit-identical**.
- Full `--lib`, `--test architecture_test` (incl. the extended guard), `--no-fail-fast`, `toron-tpch` green.
- `check -p toron --all-targets` + `-p toron-tpch --all-targets` (incl. `coordinator/write.rs` still resolves
  `evaluate_cast`).
- **LSP diagnostics advisory** - stale errors during the move expected (350-353); authoritative = `scripts/cargo-cgroup
  check` + the architecture guard + the board commands.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # incl. exec_decomposition.rs (no-qNN-references)
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix        # 22/22 unchanged (behavior-neutral)
scripts/cargo-cgroup run -p toron-tpch -- matrix                    # 22/22 unchanged
git diff --check
#   exec.rs -> root + exec/expr.rs (split-impl); qNN comments gone; root ratcheted < 9,772; board bit-identical
```
- Run architecture_test AFTER fmt (edit -> fmt -> fmt --check -> full architecture_test -> seal).

## Results doc
`crates/toron/plans/impl/0354_expr_evaluator_submodule_results.md`: the coupling map (expr cluster, methods vs private
helpers, the caller column proving call shapes unchanged, which split-impl methods/helpers needed `pub(in crate::exec)`,
the `evaluate_cast` re-export); exec-root + `expr.rs` line counts before/after + the guard threshold; the qNN comments
removed + the strengthened guard; the scope-fence confirmation (system-table/recursive/materialization stayed rooted); the
behavior-neutral proof (sf=1 22/22 bit-identical + full suite); confirmation the LSP errors during the move were stale.

## Risk / blast radius
- **Split-impl keeps call shapes** - ~261 sites unchanged; only `evaluate_cast` re-exported.
- **Split-impl visibility** - cross-module method callers still compile without wrapper indirection.
- **Helper privacy correctness** - cross-module-called helpers get `pub(in crate::exec)`; the caller column catches it.
- **De-branding precise** - the no-qNN-reference guard scopes to query-number patterns (no over-broad matches).
- **Scope fence** - system-table/recursive/materialization rooted; not a dispatch cleanup.
- **Behavior-neutral** - pure move + comment removal; sf=1 tripwire + `--no-fail-fast`.
- **No perf/logic/budget/cost-model change.**

## Seal
`impl: adr 354 expr evaluator submodule`. Do not commit wip drafts.
