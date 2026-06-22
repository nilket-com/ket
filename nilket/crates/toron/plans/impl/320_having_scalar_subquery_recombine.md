# ADR 320 - Implementation plan: HAVING scalar subquery recombine after aggregation (q11)

Pairs with `plans/adr/320_having_scalar_subquery_recombine.md`. Engine cut (`crates/toron` SQL lowering plus binder
only if Phase 0 proves an admission gap): scalar pull-up + post-aggregate recombine. **Phase 0 proves the
exchange-free-probe objection before any behavior change.** Narrow slice: uncorrelated one-row global-aggregate
HAVING scalar only.

## Phase 0 - ground the recombine placement (no behavior change until decided)
- Reproduce q11's wall (`unsupported_having_scalar_subquery@plan`, sql_planner.rs:3811-3821). Record the rejected
  shape: an `is_aggregate_query` with `having` containing an uncorrelated provably-scalar (global-aggregate)
  subquery.
- Probe the broadcast scalar `Cross` admission (binder/join.rs) with a synthetic post-aggregate scalar recombine:
  `Aggregate(...) Cross Alias(global_aggregate_scalar)`. Current code appears to admit a provably one-row right side
  via `ScalarBroadcastCross`; prove that before assuming a binder change is needed.
- Decide the smallest route and **write it down before coding**:
  - (i) the objection is stale -> keep the cut in frontend placement/rewrite, recombining the scalar post-aggregate
    through the existing scalar `Cross`; or
  - (ii) the synthetic post-aggregate Cross still rejects -> first name the actual admission/fragmenter constraint,
    then apply the narrow binder fix.

## D1 - recombine the HAVING scalar after aggregation (`sql_planner.rs`; binder only if Phase 0 demands it)
- At 3811-3821, stop rejecting the **uncorrelated one-row global-aggregate** HAVING scalar; instead collect it for
  **post-aggregate** recombine (after the GROUP BY aggregate is built), reusing `pull_up_scalar_subqueries` /
  `join_scalar_subplans` / the ADR 240 scalar-local `Cross` (`row_upper_bound <= 1`) at the post-aggregate
  position.
- Lower the HAVING predicate as a Filter **above** the aggregate, referencing the recombined `__scalar_N` column.
- Keep the reject for the deferred cases (correlated / multi-row-assert / non-aggregate-cardinality /
  aggregate-grouped-projection-unless-same-placement) - a precise reject each, not the broad bucket where the
  message supports it.

## Tests
- **Engine unit (`binder/tests.rs`):** a grouped query `SELECT k, SUM(x) FROM t GROUP BY k HAVING SUM(x) >
  (SELECT SUM(x) * c FROM t)` binds to: aggregate -> post-aggregate scalar recombine (1-row `Cross`) -> HAVING
  Filter on the scalar; executes with each group compared to the *single* global scalar. Equivalence vs an
  explicitly-recombined plan (same rows).
- **Deferred-stay-rejected:** a correlated HAVING scalar, a multi-row HAVING subquery, and a non-aggregate scalar
  each still reject (precise message); revert the recombine -> q11-shape rejects at 3821 (discriminating).
- **Keyed/tractable:** the recombine is a 1-row broadcast (no cartesian); sf=0.01 memory/time check.
- **Architecture:** full `architecture_test` (scalar pull-up / admission shared + guarded).

## Phase 3 - re-measure q11 + the matrix
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode dev    --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode strict --sf 0.01 --allow-blocked
```
- **Success = q11 moves OFF `unsupported_having_scalar_subquery@plan`** - dev = `pass@execute` (or deeper), strict
  = likely `unsupported_decimal_arithmetic@bind` on `ps_supplycost * ps_availqty` (-> name **ADR 321**). A deeper
  measured blocker is a good red row, not a failed cut. Update the q11 harness row.
- Re-run `matrix` + `correctness-matrix`: q11 advances; **q15/q18/q19 and all others unchanged**.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) - FULL toron (shared scalar/lowering path)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q11 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/320_having_scalar_subquery_recombine_results.md`: the Phase-0 placement finding
(build-vs-probe exchange constraint, chosen route); the recombine + its equivalence test; q11's new measured rows
(off the HAVING-scalar wall -> deeper blocker or green, with the strict Decimal x Int -> ADR 321 if hit); the matrix
delta (q11 only); confirmation the deferred cases still reject + q15/q18/q19 unchanged.

## Risk / blast radius
- **Phase-0 first** - name the exchange-placement route before coding; wrong read = wrong fix.
- **Scope drift** - keep the deferred list rejected (correlated / multi-row-assert / non-aggregate-cardinality /
  generic); pin them with stay-rejected tests.
- **Shared scalar/recombine path** - full `--no-fail-fast` + `architecture_test`; must not change WHERE-scalar
  behavior.
- **Correctness** - one global scalar, compared per group (not re-aggregated); pin the equivalence.
- **No q11-green over-claim** - acceptance is movement off the wall; the Decimal x Int strict red names ADR 321.

## Seal
`impl: adr 320 having scalar subquery recombine`. Do not commit wip drafts.
