# ADR 322 — Implementation plan: q18 grouped membership-subquery body

Pairs with `plans/adr/0322_q18_grouped_membership_body.md`. Engine cut (`crates/toron` frontend IN lowering)
and the shared `o_totalprice` fixture. IN-only; key on the body output.

## Phase 0 — ground the admission path + the consolidation outcome (before assuming green)
1. **Validator sharing:** confirm IN/membership lowering calls the **same** `validate_exists_*` validators EXISTS
   uses, and that they reject q18's body *because of* `GROUP BY`/`HAVING`. The HAVING is an aggregate-vs-literal
   threshold (`SUM(l_quantity) > 300`) — plain HAVING.
2. **Key visibility:** confirm the current membership `inner_key` is a base-table-qualified `l_orderkey`; after a
   grouped Aggregate that column is the group key but is exposed as the body's projected output — the key must bind
   to the body output, not the base column.
3. **Fixture:** `o_totalprice` is absent (ADR 315 deferred). Once D1/D2 clear `@plan`, q18 binds and resolves it.
4. **Consolidation (load-bearing unknown):** after admitting + `o_totalprice`, re-measure — does ADR 318's A1
   (`logical_scope_for_outer_demotion` handles `LogicalPlan::Aggregate`) key the semi-join over the grouped body,
   or a deeper wall? **Don't assume.**

## D1 — IN-only grouped-body admission (`sql_planner.rs`)
- Add `validate_in_select_shape` (or a narrowly-named caller flag threaded only through the IN lowering) that admits
  a `GROUP BY` + aggregate-vs-literal `HAVING` body. **Leave `validate_exists_select_shape`/`query_shape`
  rejecting grouped bodies for the EXISTS path**, and leave grouped `NOT IN` rejected — do not relax the shared
  validator or the anti-join path without a measured row.
- Lower the grouped body via the existing grouped-aggregate lowering (the same path q-grouped queries use).

## D2 — semi-join key on the body output
- Key the IN-semi-join against the **grouped body's projected output column** (the group key as exposed by the
  Aggregate/projection), not a base-table `lineitem.l_orderkey`. Verify the key column id resolves against the body
  subplan output, then let the existing IN-semi-join / ADR 318 A1 recombine recognize it.

## Fixture
- Add `o_totalprice` to the orders fixture (`OrderGenerator`, the declared physical type) **after** D1/D2; rerun
  the full `toron-tpch` + goldens to confirm no fixture regression.

## Tests
- **q18 keyed** — the q18 harness keyed-plan assertion: the membership semi-join keys (Inner/semi, `left_keys=1
  right_keys=1`) on the body output, not `Cross left_keys=0 right_keys=0`.
- **Grouped EXISTS / NOT IN stay rejected** (the no-widen guard): an EXISTS body and a NOT IN body with
  `GROUP BY`/`HAVING` still reject (discriminating — proves the admission is IN-only).
- **Engine unit:** `x IN (SELECT k FROM t GROUP BY k HAVING agg > lit)` lowers to a grouped-body subplan + a
  semi-join keyed on the body output; a correlated / HAVING-scalar membership body still rejects.
- **Architecture:** full `architecture_test` (the validators / semi-join lowering are guarded).

## Phase 3 — re-measure q18 + the matrix
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q18 --mode dev    --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q18 --mode strict --sf 0.01 --allow-blocked
```
- **Success = q18 moves OFF `unsupported_query_surface@plan`** → keyed `pass@execute` both modes → **reach 21** (A1
  consolidated); or a deeper measured blocker (record + name the next ADR). Confirm keyed + tractable memory/time.
- Re-run `matrix` + `correctness-matrix`: q18 advances; **q11/q15/q19 + q04/q22 EXISTS + all others unchanged**.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (shared validator + fixture)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q18 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q18 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0322_q18_grouped_membership_body_results.md`: the IN-only admission + the
body-output key; q18's new measured rows (off the wall → keyed green or a deeper blocker); whether ADR 318's A1
consolidated; the matrix delta (q18 only); confirmation grouped EXISTS and grouped NOT IN still reject +
`o_totalprice` / q11/q15/q19 / q04/q22 unchanged.

## Risk / blast radius
- **No EXISTS / NOT IN widening** — IN-only admission; grouped-EXISTS and grouped-NOT-IN stay-rejected tests are the
  guard.
- **Key = body output** — not the base column (invisible post-Aggregate); wrong key → cartesian or bind error.
- **Consolidation hypothesis** — the re-measure decides; a deeper wall is named, not pre-solved.
- **Shared `o_totalprice` fixture** — full `-p toron-tpch` + goldens.
- **Keyed-not-cartesian** — the semi-join must key; assert it + check sf=0.01 memory/time.

## Seal
`impl: adr 322 q18 grouped membership body`. Do not commit wip drafts.
