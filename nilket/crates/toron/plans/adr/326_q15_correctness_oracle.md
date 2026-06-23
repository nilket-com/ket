# ADR 326 — q15 correctness oracle

## Status
Proposed.

## Context

Next correctness cut after q11 (correctness 18 → target 19). q15 is the ADR 318 composite-CTE query (top supplier
by revenue). Unlike q11, **no flagged bug** — so this is verify-first: a faithful oracle either confirms q15 strict
`pass@compare` or honestly exposes one.

```sql
WITH revenue AS (
  SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
  FROM lineitem WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-01-01' + 3 months
  GROUP BY l_suppkey)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier, revenue
WHERE s_suppkey = supplier_no AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
ORDER BY s_suppkey
```

## Decision — an independent generator-traversal oracle, mirroring the executor

### D1 — the oracle
- **revenue CTE:** per `l_suppkey`, `total_revenue = SUM(l_extendedprice * (1 - l_discount))` over the shipdate
  window. Mirror the engine's **Decimal precision** for the product (`l_extendedprice [Decimal] * (1 - l_discount)
  [Decimal]`) and the SUM (the q01/q06 precision-fit doctrine).
- **MAX:** `m = MAX(total_revenue)` over the CTE, **at the same precision as the CTE column** (the equality below
  depends on it).
- **Join + filter:** `supplier ⋈ revenue on s_suppkey = supplier_no`; keep rows where `total_revenue == m` (**exact
  Decimal equality**).
- **Ties (load-bearing):** `total_revenue = MAX` returns **every** supplier at the max — emit all ties, not one.
- **Order/schema:** `ORDER BY s_suppkey`; output `(s_suppkey, s_name, s_address, s_phone, total_revenue)`. Assert
  schema first; check `is_null` on every slot.

### D2 — verify-first + non-vacuity (the contract)
- Run the oracle vs q15 **strict** at sf=0.01. **q15 strict = `pass@compare`** is the target — or, if it mismatches,
  name the bug honestly (the q11 lesson). The likeliest mismatch sources, mirror exactly: the product precision,
  the SUM precision, and **the scalar `MAX` precision vs the CTE column precision** (the `==` silently drops the max
  supplier if they differ).
- **Non-vacuity (mandatory):** the `= MAX` filter selects the top supplier(s) — assert a **proper subset** (≥1, and
  fewer than all suppliers in the window). An all/empty result would fake-pass; the selectivity makes the green
  meaningful.
- **Strict-only canonical row**; dev is a control if used, not a matrix cell.

## Acceptance
- **q15 strict = `pass@compare`** (correctness **19**) — or an honest, named red.
- Non-vacuous (proper subset: the max supplier(s), not all/empty); **ties handled** (all maxes emitted).
- Mirrors the engine's legitimate Decimal precision (product + SUM + MAX), exact `==`, `ORDER BY s_suppkey`, schema
  + per-slot nullness.
- Full `-p toron-tpch` green; reach unchanged (22/22); other correctness cells unchanged.

## Risks
- **MAX-equality precision** — the scalar `MAX` and the CTE `total_revenue` must be the same precision, or the `==`
  silently drops the max supplier; mirror exactly. The likeliest place a bug (or oracle mismatch) hides.
- **Ties** — emit all suppliers at the max; a one-row assumption is wrong if sf=0.01 ties.
- **Vacuity** — assert the proper-subset selectivity (not all/empty).
- **Product precision** — `l_extendedprice * (1 - l_discount)` must match the engine (q01/q06 helper); the `1`
  literal is Int (ADR 266 coercion), unaffected by ADR 325 (only decimal-point literals changed).

## Forward arc
q15 correctness closes (19) or names a bug. Then the rest of the backlog: q18/q19/q21 oracles (the other
newly-reached queries).

## Why toron-home
The oracle lives in `toron-tpch` (generator traversal + the precision helpers). No engine change expected
(verify-first; a surfaced bug names a follow-on). No Wendelon / cross-ring surface.

## Council provenance
Operator + Claude (quorum-of-one this round; Codex context restarted). q15 = the ADR 318 composite-CTE top-supplier
query, verify-first (no flagged bug). The load-bearing details: the **MAX-equality precision** (scalar vs CTE
column — exact `==` or a silent drop), **ties** (all suppliers at the max), and **non-vacuity** (the `= MAX` filter
selects a proper subset). Strict-only row, mirror-the-executor precision, the q11/q24 anti-fake-pass discipline.
