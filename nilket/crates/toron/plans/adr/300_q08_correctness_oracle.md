# ADR 300 — q08 correctness oracle

## Status
Proposed.

## Context

The **gap-closer** — last unchecked-green cell. Board: 13 reach, 12 correctness; this cut makes it **13/13,
boards level**. q08 is the most complex oracle: an 8-factor derived body + a q14-style **ratio** + a `nation`
self-join with two roles + a thin 2-row output (so synthetic tests carry the discrimination). q08 is non-empty
at sf=0.01 (2 rows, ADR 297 — not masked-empty).

```sql
SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM (
  SELECT EXTRACT(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume,
         n2.n_name AS nation
  FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
  WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA'
    AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY o_year ORDER BY o_year
```

**The nation self-join (the new subtlety):** `n1` = the **customer's** nation, region-filtered to **AMERICA**
(the denominator scope); `n2` = the **supplier's** nation, used in the `CASE` (the **BRAZIL** numerator). So
mkt_share = (BRAZIL-supplier volume) / (all-supplier volume) over orders by AMERICA customers for that part
type. Don't conflate which nation is region-filtered (customer) vs CASE-checked (supplier).

## Decision

A strict-only q08 correctness oracle, harness-only; the default acceptance rail remains sf=0.01. Reuse the
q14/q17 decimal division helper (`decimal_division_raw`) + q07's self-join resolution + `discounted_price_raw`
(scale-4 volume) + grouped-by-`o_year`.

### D1 — `strict_q08_oracle`
- Filters: `p_type == "ECONOMY ANODIZED STEEL"`; customer's nation (`c_nationkey → n1`) in the **AMERICA** region
  (`n1.n_regionkey → r_name == "AMERICA"`); `o_orderdate ∈ [1995-01-01, 1996-12-31]` **inclusive** (`BETWEEN`).
- Per qualifying lineitem: `o_year = year(o_orderdate)`; `volume = discounted_price_raw` (raw scale 4); supplier's
  nation `n2 = nation[supplier[l_suppkey].s_nationkey]`. Accumulate per `o_year`: `den_raw += volume`; and
  `num_raw += volume` **iff `n2 == "BRAZIL"`**.
- Per `o_year`: `mkt_share = decimal_division_raw(num_raw, measured_sum_p, 4, den_raw, measured_sum_p, 4)` (the
  q14/q17 engine-faithful path; Phase 0 measures the SUM operand precision and final output `(p,s)`).
  `Q08Oracle { groups: BTreeMap<i32, DecimalRaw>, denominator_rows, brazil_rows }`.

### D2 — `check_strict_q08` + `compare_strict_q08` + wiring
- **Schema** — **Phase-0 measure and pin** `o_year` Int32 + `mkt_share` `Decimal128(p, s)` (the division output
  type; **not yet grounded — measure it, don't bake**); `is_null` per cell.
- **Bijection by `o_year`** (1995/1996): exact `mkt_share` (raw `i128` at the measured scale); row counts equal.
- **Order = `ORDER BY o_year`** — total order (unique years); assert exact ascending.
- CLI `correctness --query q08|q8`; `correctness_matrix` renders `q08 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely (the hardest cell)
1. **The ratio division** — `mkt_share = num/den` mirrors the engine's `decimal_division_output_type` + half-away
   (the q14/q17 lesson; reuse `decimal_division_raw`). **Phase-0 measures the output `(p,s)`.** Not float, not a
   percent, not `SUM(row_ratio)`.
2. **The nation self-join — don't conflate.** Region filter on the **customer's** nation (n1→AMERICA); `CASE` on
   the **supplier's** nation (n2→BRAZIL). A swap is plausible-but-wrong — a synthetic tripwire must catch it.
3. **CASE-true (BRAZIL) discrimination — possibly HOLLOW at sf=0.01.** Output is 2 rows; **Phase-0 measures
   `brazil_rows`**. If `brazil_rows == 0` (BRAZIL absent → mkt_share = 0/den, the `CASE`-true never fires), the
   ratio numerator is untested live → **synthetic tests are mandatory** (a constructed group with BRAZIL volume
   exercises the ratio + CASE-true). The q14-PROMO / q20-threshold lesson, sharpened for a thin answer.
4. **`EXTRACT(year)` → Int32**; `volume` = `discounted_price_raw`; inclusive `BETWEEN`; `is_null` per cell.

## Acceptance
- `q08 strict=pass@compare` at sf=0.01; **rail → 13 (boards level: 13 reach / 13 correctness)**; reach + other
  cells unchanged.
- **Non-vacuity:** 2 year groups, `denominator_rows > 0`; `brazil_rows` measured (if 0, synthetics are
  load-bearing and the results doc says so).
- **Discriminating suite:** reject a wrong `mkt_share`, a **truncated/wrong-rounding ratio** (q14/q17 division
  discipline), a **customer/supplier self-join conflation** (region vs CASE — synthetic), a non-AMERICA customer
  or non-ECONOMY-ANODIZED-STEEL part admitted, a `BETWEEN`-boundary error, a wrong `o_year`, a missing/extra
  group, a NULL cell, an order violation, and a synthetic BRAZIL-volume group proving the ratio + CASE-true.

## Boundaries
- Strict-only. The default correctness rail runs at sf=0.01; no q17-style special scale policy is planned.
  Harness q08 SQL unchanged. Independent traversal; no `crates/toron` engine change. Reachability untouched;
  `status_rows` already lists q08 and does not change in this cut.

## Forward arc
**Boards level at 13/13** — the trust gap closed. The cadence then reopens the reach frontier (q09 substring LIKE
+ 6-table is the next machete), then catchups resume the 2:1 rhythm.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.

## Council provenance
2/2 (Claude/Codex; Gemini did not draft — the 2-of-3 bar). Aligned: ratio via `decimal_division_raw` (not float /
percent / row-ratio); the nation self-join (n1=customer→AMERICA vs n2=supplier→BRAZIL — conflation tripwire);
**Phase-0 measures the `mkt_share` `(p,s)` and `brazil_rows`** (numerator may be hollow → synthetics mandatory).
Reuses q14 ratio + q07 self-join + `discounted_price_raw`. ADR 300 closes the trust gap to zero.
