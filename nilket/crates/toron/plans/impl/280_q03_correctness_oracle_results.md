# ADR 280 — Results: strict q03 correctness oracle (first multi-table answer-check)

## Measured row
```
correctness-matrix:  q01 strict=pass@compare · q03 strict=pass@compare · q06 strict=pass@compare
reachability matrix: q01/q03/q04/q05/q06/q10/q14 dev+strict pass@execute (unchanged)
```
q03 strict — the first answer-check over a JOIN (`customer ⋈ orders ⋈ lineitem`) — reaches `pass@compare`.
Three correctness cells now stand alongside the seven reach cells.

## Measured schema (Phase 0 — confirmed against live output, not assumed)
Running `correctness --query q03` exercised `assert_strict_q3_schema` against strict q03's actual Arrow output;
it passed, confirming:
```
l_orderkey Int64 · revenue Decimal128(32,4) · o_orderdate Date32 · o_shippriority Int32
```
`revenue = SUM(l_extendedprice * (1 - l_discount))` is q01's `sum_disc_price` expression, and it measured the
same `Decimal128(32,4)` ADR 276 found — the prior held, but the assertion is what proves it (a `(31,4)` drift
would be `fail@compare`, pinned by `strict_q3_output_schema_is_asserted`).

## What was built (`crates/toron-tpch`, harness-only — no engine change)
- **`strict_q3_oracle(sf) -> Q3Oracle`** — traverses the Customer/Order/LineItem generators: the `BUILDING`
  custkey set; orders with `o_orderdate.to_unix_epoch() < 9204` (`DATE '1995-03-15'`, verified; cross-checks
  against q01's `10471` for `1998-09-02`) joined by `o_custkey`; lineitem with `l_shipdate > 9204` joined by
  `l_orderkey`. Groups into a `BTreeMap<i64 /*l_orderkey, the unique group key*/, Q3Group>` with raw scale-4
  `revenue += extprice_raw * (100 - disc_raw)` (q01's `sum_disc_price` accumulation), plus `selected_rows`.
- **`compare_strict_q3`** — schema assert, then the discriminating split:
  - **Answer-correctness:** a bijection keyed by `l_orderkey` (duplicate emitted keys rejected up front, not
    silently overwritten), every cell's raw `i128`/`i32` matched, nullness checked on every slot.
  - **Order-correctness:** the emitted sequence respects the `ORDER BY revenue DESC, o_orderdate` *prefix* —
    not the executor's `bind_sort_keys` totalized tiebreak (an implementation detail the oracle must not bet on).
- **`check_strict_q3`** — execute → schema assert → non-vacuity (`selected_rows ≥ 1` AND `groups.len() > 1`) →
  compare; deepest-phase carry on an execution error. CLI: `correctness --query q03|q3`.

## Tests (7, `tests/correctness_test.rs`)
- `strict_q3_matches_independent_oracle` — `check_strict_q3(0.01)` → `Compare`/`Pass`.
- `strict_q3_output_schema_is_asserted` — the schema gate accepts `(32,4)`, rejects a `(31,4)` revenue drift.
- `q3_comparator_accepts_either_tie_order` — the design's discriminating test: two groups tied on
  `(revenue, o_orderdate)` pass in **either** emit order (the comparator keys by `l_orderkey`, not the tiebreak).
- `q3_order_prefix_violation_is_fail` — revenue ascending → `fail@compare`.
- `q3_correctness_mismatch_is_fail_at_compare` — one off-by-one revenue → `fail@compare`.
- `q3_duplicate_output_key_is_fail_at_compare` — a duplicate emitted `l_orderkey` is rejected.
- `strict_q3_oracle_selects_multiple_groups` — `selected_rows ≥ 1` and `> 1` group at sf 0.01.

## Gate (harness-only)
fmt --check clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green (correctness binary
18, incl. the 7 q03 tests) · `correctness --query q03 --sf 0.01` → `pass` · `correctness-matrix` → q03
`pass@compare` (q01/q06 unchanged) · `matrix` unchanged (seven reach cells) · `git diff --check` clean. No
`-p toron` change.

## Forward arc
q05 correctness is the next rail step — it reuses this join-oracle shape and adds the 6-table cyclic graph (a
genuine separate increment). The engine frontier (runtime `column ± interval`, scalar/`IN` correlated
q17/q20/q22) stays open and independent of the rail.
