# ADR 299 — q07 correctness oracle

## Status
Proposed.

## Context

Correctness catchup over q07 (of the unchecked-green {q07, q08}). Board: 13 reach, 11 correctness. **This cut puts
the gap to one** (unchecked-green → {q08}). q07 reuses q05's grouped revenue but adds a self-join, a disjunctive
nation-pair filter, `EXTRACT(year)`, and an inclusive `BETWEEN`. Grounded non-vacuous at sf=0.01 (46 contributing
rows, **4 groups**, both directions + both years present).

```sql
SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
FROM (
  SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
         EXTRACT(year FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume
  FROM supplier, lineitem, orders, customer, nation n1, nation n2
  WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year
```

## Decision

A strict-only q07 correctness oracle, harness-only; the default acceptance rail remains sf=0.01. Reuse q05's
`discounted_price_raw` (raw scale-4 volume) + the grouped `BTreeMap`. The new surfaces are the self-join nation
resolution, the disjunctive nation-pair filter, `EXTRACT(year)`, and the inclusive `BETWEEN`.

### D1 — `strict_q07_oracle`
- `nation`: `n_nationkey → n_name`.
- Per `lineitem` with `l_shipdate ∈ [9131, 9861]` **inclusive both ends** (`BETWEEN` = `1995-01-01 .. 1996-12-31`;
  `1997-01-01` is day 9862, so equivalently `< 9862`): resolve `supp_nation = nation[supplier[l_suppkey].
  s_nationkey]` and `cust_nation = nation[customer[orders[l_orderkey].o_custkey].c_nationkey]`; keep iff
  `(supp_nation, cust_nation) ∈ {(FRANCE,GERMANY), (GERMANY,FRANCE)}`; `l_year = year(l_shipdate)` (1995/1996);
  `volume = discounted_price_raw`; accumulate `revenue` by `(supp_nation, cust_nation, l_year)`.
- `Q07Oracle { groups: BTreeMap<(String, String, i32), i128>, selected_rows }` (4 groups: 2 pairs × {1995,1996}).

### D2 — `check_strict_q07` + `compare_strict_q07` + wiring
- **Schema** (grounded; Phase-0 confirm): `supp_nation` Utf8, `cust_nation` Utf8, `l_year` **Int32**, `revenue`
  **Decimal128(32,4)**; `is_null` per cell.
- **Bijection by `(supp_nation, cust_nation, l_year)`**: exact `revenue` (raw `i128`); row counts equal; no
  duplicate.
- **Order = `ORDER BY supp_nation, cust_nation, l_year`** — **total** order (4 unique 3-key groups); assert exact
  ascending (no tie tolerance).
- **Non-vacuity:** `selected_rows > 0` and the 4 groups present (both nation pairs × both years — grounded).
- CLI `correctness --query q07|q7`; `correctness_matrix` renders `q07 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely
1. **Self-join resolution — don't swap.** `supp_nation` = the **supplier's** nation (via `l_suppkey`),
   `cust_nation` = the **customer's** (via `l_orderkey → o_custkey`). A swap is the classic q07 bug — a
   discriminating test on asymmetric data must catch it.
2. **`BETWEEN` is INCLUSIVE both ends** — `l_shipdate <= 9861` (1996-12-31 is *in*; 1997-01-01 = 9862 is out).
   Boundary test.
3. **Disjunctive nation-pair filter** — exactly the two ordered pairs; others excluded (the filter discriminates).
4. **`EXTRACT(year)` → Int32**; **`volume` = `discounted_price_raw`** (scale 4); **`revenue` Decimal128(32,4)**.
5. **Multi-key total order** + `is_null` per cell; exact raw `i128` revenue.

## Acceptance
- `q07 strict=pass@compare` at sf=0.01; rail → 12 (gap to one); reach + other correctness cells unchanged.
- **Discriminating suite:** reject a **supp/cust-swapped** group (the self-join tripwire), a wrong `revenue`, a
  non-{FR/GER} pair admitted (disjunction), a `BETWEEN`-boundary error (1996-12-31 excluded or 1997-01-01
  included), a wrong `l_year`, a missing/extra/duplicate group, a NULL cell, and an order violation.

## Boundaries
- Strict-only. The default correctness rail runs at sf=0.01; no q17-style special scale policy is needed. Harness
  q07 SQL unchanged. Independent traversal; no `crates/toron` engine change. Reachability untouched; `status_rows`
  already lists q07 and does not change in this cut.

## Forward arc
Rail → 12 — **gap to one** (only q08 unchecked-green). **ADR 300 = q08 correctness** closes the gap (ratio +
8-factor derived, already non-empty: 2 rows). Then q09 (substring LIKE) reach.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.

## Council provenance
3/3 (Claude/Codex/Gemini), identical design + grounded facts (temp probe removed): schema `supp_nation`/
`cust_nation` Utf8, `l_year` **Int32**, `revenue` **Decimal128(32,4)**; sf=0.01 = 46 rows / 4 groups / both
directions + both years; inclusive `[9131, 9861]`. Folded: self-join supp-via-supplier / cust-via-order
(don't-swap tripwire); inclusive-`BETWEEN` boundary; disjunctive pair filter; `EXTRACT(year)` Int32; multi-key
total order. Reuses q05's `discounted_price_raw` + grouped `BTreeMap`.
