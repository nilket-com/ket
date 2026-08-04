# Implementation Plan — ADR 255: `WITH RECURSIVE` (local `UNION ALL` fixpoint, first cut)

## Summary

A real recursive-CTE mechanism: new IR nodes (`RecursiveCte` + `RecursiveRef`, logical + execution), a
frontend lowering that detects the `seed UNION ALL recursive` shape and scopes the self-reference, a
binder arm (seed-schema-then-recursive-term), and a **local `UNION ALL` fixpoint loop** in the executor
that injects the working table via the existing `SystemTable` node. `UNION` (distinct) recursion,
joins-in-recursion, and distributed recursion are rejected.

## Verify-first probes (do these BEFORE committing to the executor shape)

1. **Substitution executor** (the riskiest piece): confirm a `SystemTable` built from a prior result's
   `Vec<RecordBatch>` (convert batches → `Vec<Vec<ScalarValue>>` via the existing scalar-row helpers)
   executes correctly when spliced in place of a leaf. Prototype: build a tiny plan
   `Project(Filter(SystemTable rows))`, run it, confirm rows. This validates "clone recursive term,
   replace `RecursiveRef` with `SystemTable`, `execute_bound`".
2. **Exchange-free recursion**: confirm the canonical recursive term `SELECT n+1 FROM t WHERE n<5` lowers
   to scan(RecursiveRef)+Filter+Project with **no Exchange** (so it stays a single local task). A join in
   the recursive term introduces an Exchange → that's the deferred-distributed boundary.

## Phase 1 — IR nodes (trips many match sites — see risk notes)

- `ir.rs`: add `LogicalPlan::RecursiveCte { name: String, column_names: Vec<String>, seed:
  Box<LogicalPlan>, recursive: Box<LogicalPlan> }` and `LogicalPlan::RecursiveRef { name: String,
  column_names: Vec<String> }`. No `all` flag — `UNION ALL` is the only representable form this cut.
- `ir/execution.rs`: add `ExecutionPlan::RecursiveCte { seed: Box<ExecutionPlan>, recursive:
  Box<ExecutionPlan>, output_schema: SchemaRef }` and `ExecutionPlan::RecursiveRef { output_schema:
  SchemaRef }`. Derive the SAME traits as siblings (Serialize/Deserialize/Clone/Debug/PartialEq) —
  `RecursiveRef` serializes because it lives inside the (single-task) recursive subplan.
- Add variants **at the end** of each enum (postcard variant indices stay stable → existing goldens
  unaffected; verify via `--lib`).

## Phase 2 — Frontend lowering (`sql_planner.rs`, extends 254)

Replace the `if with.recursive { return Err("WITH RECURSIVE is not supported") }` reject. For a recursive
`WITH` with exactly one CTE (multiple → reject "multiple recursive CTEs are not supported"):

1. Require `cte.query.body == SetExpr::SetOperation { op: SetOperator::Union, set_quantifier: All, left:
   seed, right: recursive }` (else "a recursive CTE body must be `seed UNION ALL recursive`"). A non-`ALL`
   `UNION` rejects ("recursive `UNION` (distinct) is not supported; use `UNION ALL`") — the dedup form is
   deferred.
2. `column_names` = explicit `cte.alias.columns` if present, else derived from the seed's projected
   output names.
3. Lower `seed` via `lower_query_body(seed, outer_scope)` (anchor — no self-ref).
4. Build `let recursive_ref = LogicalPlan::RecursiveRef { name, column_names }`; lower `recursive` in a
   scope = outer + `name → &recursive_ref` (the self-reference). Reject if the lowered recursive term
   does **not** contain a `RecursiveRef` (no self-reference → not recursive).
5. Assemble `LogicalPlan::RecursiveCte { name, column_names, seed, recursive }`, register `name → &it`
   in the CTE scope so outer references resolve (254 path, unchanged).
6. Reject any recursive term that expands beyond the first-cut scan+filter+project shape — joins,
   aggregates, subqueries, and nested set-op forms stay out of this ADR. The goal here is to keep the
   recursive loop exchange-free and local; richer recursive shapes land with the distinct/
   graph-traversal follow-on.

## Phase 3 — Binder (`binder.rs`)

New `bind_plan_with_trace` arms:
- `RecursiveRef { name, column_names }` → resolve schema from the recursive-binding context (the seed's
  bound schema, relabeled to `column_names`); emit `ExecutionPlan::RecursiveRef { output_schema }`. The
  context carries the active recursive CTE's schema (a field threaded through binding, or a small scope
  stack keyed by name).
- `RecursiveCte { seed, recursive, .. }` → bind `seed` → `S`; record `name → S` in the recursive
  context; bind `recursive` (its `RecursiveRef` binds to `S`); reconcile seed/recursive output schemas
  with the existing `Union` schema-alignment; emit `ExecutionPlan::RecursiveCte { seed, recursive,
  output_schema: S }`. Facts: row bound is unknown/unbounded (recursion) — carry a conservative
  composite-unknown fact.

## Phase 4 — Executor (`exec.rs`) — the fixpoint loop

New `execute_bound` arm `ExecutionPlan::RecursiveCte { seed, recursive, output_schema }` →
`execute_recursive_cte`:

```
let mut working = self.execute_bound(seed, morsel).await?;
let mut result  = working.clone();
for _ in 0..RECURSIVE_CTE_MAX_ITERATIONS {
    if rows_total(&working) == 0 { return Ok(finalize(result, output_schema)); }
    let injected = substitute_recursive_ref(recursive, &working, output_schema)?; // clone + replace leaf
    let next = self.execute_bound(&injected, morsel).await?;
    if rows_total(&next) == 0 { return Ok(finalize(result, output_schema)); }      // delta-empty
    result.extend(next.clone());
    working = next;                                                                // semi-naive
}
return Err(ExecutionError::… "recursive query did not terminate within N iterations");
```

- `substitute_recursive_ref`: walk a clone of `recursive`, replace each `ExecutionPlan::RecursiveRef`
  leaf with `ExecutionPlan::SystemTable { rows: batches_to_scalar_rows(working), schema: output_schema }`.
  (Reuses the SystemTable execution path — no new leaf executor.)
- `UNION ALL` = pure append (no dedup); termination is delta-empty or the cap. (Distinct dedup is the
  deferred form.)
- `RECURSIVE_CTE_MAX_ITERATIONS`: a named const safety cap (config-overridable later).

## Phase 5 — Fragmenter (`distributed/fragment.rs`) — local-only gate

`RecursiveCte` is opaque to fragmentation: `count_exchanges`/`contains_join` treat it as a leaf (do not
recurse into seed/recursive), so it lands in a single local task. The Phase-2 reject already guarantees
the recursive term stays within the first-cut local shape; add a fragmenter-level assertion/gate that a
`RecursiveCte` subtree introduces no cross-stage exchange. A distributed recursive term is rejected
upstream, not silently mis-fragmented.

## Phase 6 — Tests, guards, verification, seal

### Behavioral gates (`tests/recursive_cte_test.rs`, new — adr176 allowlist)

`recursive_cte_counter_series_enumerates`, `recursive_cte_terminates_on_empty_recursive_term`,
`recursive_cte_nonterminating_hits_iteration_cap`, `recursive_cte_outer_query_consumes_result`,
`recursive_cte_union_distinct_and_malformed_shapes_reject` (covers `UNION` distinct, join-in-recursion,
non-`UNION ALL` body, no self-reference). A self-contained fixture (a small base table for the outer
consume; the series example needs no storage).

### Architecture guards (`tests/architecture/recursive_cte.rs`, new module + mod.rs)

`adr255_recursive_cte_has_logical_and_execution_nodes`, `adr255_recursive_cte_executes_a_bounded_fixpoint`,
`adr255_recursive_cte_is_local_only`.

### Verification gate (every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets        # new IR variants → many constructor/match sites
scripts/cargo-cgroup test  -p toron --test recursive_cte_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr255
scripts/cargo-cgroup test  -p toron --test architecture_test       # FULL (new IR variant trips scanners; new test file → adr176 allowlist)
scripts/cargo-cgroup test  -p toron --test distributed_process_test
scripts/cargo-cgroup test  -p toron --lib                          # postcard goldens (new ExecutionPlan variants)
cargo fmt -p toron -- --check
git diff --check
```

### Seal

`impl: adr 255 with recursive`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **New IR variants trip many match sites** (the JoinKind-variant lesson, broader): adding `LogicalPlan`
  + `ExecutionPlan` variants forces arms in the binder, executor, fragmenter (`contains_join`,
  `count_exchanges`, any plan walker), `Display`/`Debug`, the optimizer, and any exhaustive `match plan`.
  `--all-targets` + the FULL architecture run are the catch-all; expect to touch several files
  mechanically. Run the FULL `architecture_test` before sealing.
- **Postcard goldens**: new variants appended at enum END keep existing variant indices → goldens should
  be stable; **verify via `--lib`**, re-pin (or bump the domain tag) in this cut if flagged.
- **Substitution executor is the load-bearing risk**: the clone-and-replace-leaf each iteration must
  preserve schema/types. Probe #1 grounds it before the full loop. If `SystemTable` round-trip is lossy
  for some type, a dedicated in-memory `RecordBatch` leaf may be needed instead (a scoped escalation).
- **Termination**: the iteration cap is a hard safety net, not the primary terminator — `UNION ALL`
  terminates when the recursive term self-limits via `WHERE` (delta-empty). The cap test proves no-hang.
  (The dedup-convergence terminator arrives with the deferred `UNION` distinct form.)
- **`column_names` derivation**: explicit `t(cols)` else seed projection names; reject arity mismatch
  between the declared columns and the seed.
- **Facts**: a recursive result has an unknown row bound — carry a conservative composite-unknown fact so
  downstream cost/fragmentation does not assume a small cardinality.
