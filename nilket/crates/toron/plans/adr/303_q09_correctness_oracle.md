# ADR 303 — q09 correctness oracle

## Status
Proposed.

## Context

The catchup that **verifies ADR 302's subtraction over real data** and closes the gap again (correctness 13 → 14,
level with reach 14). Board: 14 reach, 13 correctness. q09's `amount = revenue − cost` is a **new rail
arithmetic** (the first subtraction-profit, **signed** — negative amounts possible). Non-empty + rich at sf=0.01
(no date filter → many `(nation, o_year)` groups; masked-empty trap cleared).

```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
  SELECT n_name AS nation, EXTRACT(year FROM o_orderdate) AS o_year,
         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
  FROM part, supplier, lineitem, partsupp, orders, nation
  WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
    AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year ORDER BY nation, o_year DESC
```

## Decision

A strict-only q09 correctness oracle at sf=0.01, harness-only; reuse the q05/q07 grouped pattern
(`discounted_price_raw` revenue + grouped `BTreeMap`). New: the cost product, the subtraction (ADR 302), the
composite partsupp lookup, the contains-LIKE.

### D1 — `strict_q09_oracle`
- `green` parts: `p_partkey` where `p_name.contains("green")` (**contains**, not prefix/suffix).
- `partsupp` keyed by **`(ps_partkey, ps_suppkey)` → `ps_supplycost_raw`** (the composite key).
- Per lineitem with `l_partkey ∈ green` and a matching `partsupp[(l_partkey, l_suppkey)]` (and an order for
  `l_orderkey`):
  - `revenue_raw = discounted_price_raw(l_extendedprice, l_discount)` (scale 4);
  - `cost_raw = ps_supplycost_raw * l_quantity_raw` (scale 2 × scale 2 = scale 4) — a **new product**;
  - `amount_raw = revenue_raw − cost_raw` (scale 4, **signed**);
  - `nation = nation[supplier[l_suppkey].s_nationkey]`; `o_year` is a general Date32-to-calendar-year
    conversion matching `EXTRACT(year FROM o_orderdate)` (no date filter — all years; do not reuse q07/q08's
    1995/1996-only helper);
  - `sum_profit[(nation, o_year)] += amount_raw`.
- `Q09Oracle { groups: BTreeMap<(String, i32), i128>, selected_rows, negative_amount_rows }`.

### D2 — `check_strict_q09` + `compare_strict_q09` + wiring
- **Schema** (grounded; Phase-0 measure/assert first, drift → fail): `nation` Utf8, `o_year` **Int32**,
  `sum_profit` **`Decimal128(33,4)`** (`amount` = `(32,4) − (31,4)` → `(33,4)` per ADR 302; SUM keeps it).
  `is_null` per cell.
- **Bijection by `(nation, o_year)`**: exact `sum_profit` (raw `i128`, **signed**); row counts equal; no
  duplicate.
- **Order = `ORDER BY nation ASC, o_year DESC`** — mixed direction; verify adjacent pairs (nation ascending, then
  o_year descending within a nation). Total order (unique groups) → assert exact.
- CLI `correctness --query q09|q9`; `correctness_matrix` renders `q09 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely
1. **The subtraction-profit (verify ADR 302)** — `amount = revenue − cost`, signed; the oracle computes
   `revenue_raw − cost_raw` (scale 4); SUM can be negative. Proves 302's `+`/`-` produces the *right* (incl.
   negative) profit over real data.
2. **Negative discrimination** — Phase-0 measures `negative_amount_rows`. If live data has negatives, assert that
   fact; if it does not, record the fact and make the synthetic negative-amount test mandatory. The plan must not
   fail solely because tpchgen's sf=0.01 distribution happens to produce only positive per-row amounts.
3. **Composite partsupp lookup** — `(ps_partkey, ps_suppkey)` → `ps_supplycost` (a single-key lookup grabs the
   wrong cost).
4. **`LIKE '%green%'` = contains** (`.contains`, not prefix/suffix — the q14-prefix/q02-suffix complement).
5. **Mixed-direction order** (nation ASC, o_year DESC) + `is_null` per cell + exact signed `i128`.

## Acceptance
- `q09 strict=pass@compare` at sf=0.01; **correctness → 14 (boards level again: 14 reach / 14 correctness)**;
  reach + other cells unchanged.
- **Discriminating suite:** reject a wrong `sum_profit`, a **sign error** (a dropped cost term / negative computed
  as positive — live if present, synthetic otherwise), a wrong `ps_supplycost` (composite-key mismatch), a
  `green`-as-prefix/suffix slip, a wrong `o_year`, a missing/extra/duplicate group, a NULL cell, and a
  mixed-direction order violation.

## Boundaries
- Strict-only; sf=0.01. Harness q09 SQL unchanged. Independent traversal; no `crates/toron` engine change.
  Reachability untouched.

## Forward arc
**Boards level at 14/14** — the trust gap re-closed, now including the subtraction-profit arithmetic. The cadence
reopens the reach frontier (q13/q19/q22 machetes), then the 2:1 rhythm resumes.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.

## Council provenance
3/3 (Claude/Codex/Gemini). Codex grounded the schema (`sum_profit` **Decimal128(33,4)**, `o_year` Int32; assert
measured-first). Folded: the subtraction-profit (verify ADR 302) + the **negative-amount discrimination** (assert
live negatives only if Phase 0 measures them, with synthetic backup either way); the composite partsupp lookup;
`%green%` contains; mixed-direction order; a general q09 Date32 year helper rather than q07/q08's bounded helper.
Reuses `discounted_price_raw` + the grouped `BTreeMap`; new = the cost product + the signed subtraction.
