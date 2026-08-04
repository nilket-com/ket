# ADR 272 — Results: tpch.q03 (measured row)

## Measured row
```
q03 dev=pass@execute  strict=pass@execute
q01/q06/q14 unchanged
```

q03 — the first **3-table** query — reaches execute end-to-end in **both** modes. The predicted GROUP-BY-over-
join wall did **not** materialize: the 3-table comma join, GROUP BY over the join output (keys from
`lineitem`/`orders`), `SUM(Decimal)`, and `ORDER BY revenue DESC, o_orderdate` (an aggregate alias) all bind
and execute on landed machinery. No engine feature was added; the attempt measured a **pass**, so q03 does
**not** name ADR 273 — the board simply gains a fourth green query.

## The join is keyed, not a cartesian (honesty check)
A tiny-scale pass can mask a cartesian (the q14-pre-270 trap). Verified it isn't: q03 dev runs at `--sf 0.01`
in ~56 ms. A 3-table *cartesian* over `customer × orders × lineitem` at that scale exceeds 10¹² rows and would
never return, so completing proves ADR 270's n-ary comma-join recognition fires on q03's real shape
(`c_custkey = o_custkey AND l_orderkey = o_orderkey` lift left-deep into keyed inner joins). Pinned by
`q03_runs_keyed_at_default_scale`.

This is the first **end-to-end, real-query** confirmation of ADR 270's n-ary recognition — previously only the
`cross_join_test` unit test (synthetic `left_t/right_t/third_t`) covered 3-table routing.

## What was built (harness only)
- **Fixtures** (`lib.rs`): `customer` (`c_custkey`, `c_mktsegment`) and `orders` (`o_orderkey`, `o_custkey`,
  `o_orderdate` via `TPCHDate → Date32`, `o_shippriority`) from `tpchgen`'s `CustomerGenerator`/
  `OrderGenerator`; `l_orderkey` **appended** to the dev/strict `lineitem` schemas + generators (append-only,
  so q01/q06/q14 column positions and the q06 digest don't move). customer/orders carry no decimals → identical
  dev/strict. Honest `facts_for_batch` for all three.
- **q03 SQL** (`Q3_DEV_SQL` / `STRICT_Q3_SQL`): tpchgen-2.0.2's exact Q3 text, `:1`/`:2` → `'BUILDING'` /
  `DATE '1995-03-15'`, **no LIMIT** (the bundled text carries none). Same text both modes.
- **Wiring**: `run_dev_q3` / `run_strict_q3` (the first 3-table users of ADR 269's `run_query_with_tables`),
  CLI `run --query q03 --mode dev|strict`, `status_rows` → q03 `implemented`.
- **Telemetry**: no change — q03 passes, so no new recognized blocker is added (the honesty rule: a blocker is
  added only when a real wall is measured).

## Tests
- `harness.rs::q03_executes_in_both_modes` — dev + strict `pass@execute` (+ digest), q03 `implemented`.
- `harness.rs::q03_runs_keyed_at_default_scale` — dev q03 at `--sf 0.01` reaches execute (keyed, not a
  cartesian).
- q01/q06/q14 harness tests, the architecture guard, and all toron-tpch tests stay green; appending
  `l_orderkey` did not move q01/q06/q14.

## Forward arc
q03 passing closes no wall, so there is no ADR-273-from-q03. The next gauge move is another attempt — q10
(4-table, adds `nation`) or q05 (6-table, the stronger n-ary stress) — chosen from a fresh matrix read. The
**correlated-subquery** family (q04/q17/q20/q22, `sql_planner.rs:9388` hard reject) and **date-interval
arithmetic** (q04's `+ interval '3' month`) remain named separate arcs.

## Gate
fmt clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green (q03 incl., q01/q06/q14
unchanged) · live matrix shows `q03 dev=pass@execute strict=pass@execute`.
