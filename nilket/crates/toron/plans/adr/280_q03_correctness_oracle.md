# ADR 280 — strict q03 correctness oracle (first multi-table answer-check)

## Status
Proposed.

## Context
The gauge has seven reach cells dev+strict green (q01/q03/q04/q05/q06/q10/q14) and two correctness cells (q06
strict, ADR 273; q01 strict, ADR 276). ADR 280 adds the first **multi-table** correctness cell: **q03 strict**
— the first answer-check over a JOIN (`customer ⋈ orders ⋈ lineitem`), so it makes the rail a regression
tripwire for the comma-join recognition, decimal revenue expression, multi-key GROUP BY, and ORDER BY path.
Strict-only;
harness-only (`crates/toron-tpch`) — q03 reachability is already green, so no engine surface.

q03 strict (`Q3_DEV_SQL`, used verbatim for strict): `SELECT l_orderkey, SUM(l_extendedprice * (1 -
l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment =
'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND
l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC,
o_orderdate` — **no LIMIT, no third ORDER BY key** (the bundled text carries none).

## Measured output schema (Phase 0 — read from the executor, not derived)
The oracle compares the exact raw `i128` at the revenue column's exact scale, so the per-column `(p,s)` is
**read from strict q03's actual Arrow output** before any value compare. The strong prior (to confirm, not
assume): `revenue = SUM(l_extendedprice * (1 - l_discount))` is q01's `sum_disc_price` expression verbatim,
which ADR 276 *measured* to `Decimal128(32,4)` (not the on-paper `(31,4)`). Expected, pending Phase-0
measurement:
```
l_orderkey      Int64
revenue         Decimal128(32,4)
o_orderdate     Date32
o_shippriority  Int32
```
A schema drift is itself `fail@compare` (it would mean comparing revenue at the wrong scale).

## Decision
A strict-only q03 correctness oracle on the ADR 273/276 rail:

1. **Phase 0 — assert the measured schema.** Pin the 4-column schema above and assert strict q03's actual
   output matches it (count, names, exact `DataType` incl. revenue's `(p,s)`) before comparing values.
2. **Independent oracle.** Traverse `CustomerGenerator` → the `BUILDING` custkey set; `OrderGenerator` →
   orders with `o_orderdate.to_unix_epoch() < 9204` (`DATE '1995-03-15'`, verified: 25 yr + 6 leap to 1995-01-01
   then +73 days; cross-checks against q01's 10471 for 1998-09-02) whose `o_custkey` is in the set, keyed by
   `o_orderkey → (o_orderdate, o_shippriority)`; `LineItemGenerator` → rows with `l_shipdate.to_unix_epoch() >
   9204` whose `l_orderkey` hits an order. `l_orderkey` is the **unique group key** (a given orderkey carries one
   `(orderdate, shippriority)` from its 1:1 order), so group into a `BTreeMap<i64 /*l_orderkey*/, …>`.
3. **Mirror executor arithmetic exactly** (raw `i128`, the measured scale). `revenue += extprice_raw * (100 -
   discount_raw)` [scale 4] — the same accumulation as q01's `sum_disc_price`. No float, no tolerance. (q03 has
   no AVG, so no `avg_decimal_raw` is needed.)
4. **Compare answer + order separately — the discriminating decision.** Unlike q01 (whose ORDER BY keys are its
   unique group keys → a total order), q03's `ORDER BY revenue DESC, o_orderdate` does **not** totally order the
   rows (two orders can tie on `(revenue, o_orderdate)`; the unique key `l_orderkey` is not in the ORDER BY).
   The executor's `bind_sort_keys` then totalizes with appended tiebreakers — an implementation detail this
   oracle must not bet on. So:
   - **Answer-correctness:** build a map from each emitted `l_orderkey` to its row, rejecting duplicate emitted
     keys before insertion; assert it is a bijection with the oracle map (same keys, row count equal), and per
     group assert `revenue` raw `i128`,
     `o_orderdate` `i32`, `o_shippriority` `i32`, and nullness on every slot. Any mismatch → `fail@compare`.
   - **Order-correctness:** assert the emitted sequence respects the SQL ORDER BY *prefix* — `revenue`
     non-increasing, and `o_orderdate` non-decreasing within equal `revenue`. This pins what SQL specifies
     without pinning the executor's arbitrary-but-deterministic tie order.
5. **Non-vacuous (named precisely).** `selected_rows ≥ 1` (Σ contributing lineitem rows) **and** `oracle.len() >
   1` (genuinely multi-group, so the bijection alignment is exercised), else `fail@compare`. Designed for ties
   even if the tiny fixture shows none — no underspecified-fixture dodge.

## Watch-items (operator) → guards
- **Schema asserted, not assumed** — Phase 0 fails loudly on any `DataType` drift (the silent-wrong-scale trap;
  do not hard-code `(32,4)` without confirming q03's actual output).
- **Right query** — `BUILDING`, cutoff day `9204`, no LIMIT.
- **Exact, not approximate** — raw `i128` equality per cell; no tolerance.
- **Order verified by spec, not by tiebreak** — multiset/bijection by `l_orderkey` for answers + ORDER-BY-prefix
  monotonicity for order; do not row-compare against a totalization guess.
- **Distinct rail, no engine change** — reuse `CorrectnessAttempt`/`correctness.jsonl`; reachability untouched.
  `--query` accepts q06/q01 and now q03; unsupported = usage error (exit 2, no row).

## Acceptance
- `correctness --query q03 --sf 0.01` → `pass`; `correctness-matrix` → `q03 strict=pass@compare` (q06/q01 rows +
  reachability matrix unchanged).
- Tests: `check_strict_q3(0.01)` → `phase=Compare status=Pass`; the Phase-0 schema assertion (a wrong-type batch
  trips it); the answer-bijection + ORDER-BY-prefix comparator pinned on a **crafted tie case** (two groups with
  equal revenue+orderdate — proves the prefix check accepts either tie order and the bijection still matches);
  an injected one-cell mismatch → `fail@compare`; `selected_rows ≥ 1` and `oracle.len() > 1`.
- Harness-only gate; no `-p toron` change; implementation must compile + pass.

## Forward arc
After q03, q05 correctness can reuse this join-oracle shape + add the 6-table cyclic graph — a genuine separate
increment. The engine frontier (runtime `column ± interval`, scalar/`IN` correlated q17/q20/q22) stays open and
independent of the rail.

## Why toron-home
`crates/toron-tpch` — the correctness rail. No cross-ring surface.
