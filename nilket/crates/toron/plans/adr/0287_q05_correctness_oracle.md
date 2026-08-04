# ADR 287 — q05 correctness oracle

## Status
Proposed.

## Context

After ADR 286, the reachability board has 10 dev+strict green cells (q01–q06, q10, q14, q17, q20). The
**correctness rail** — proving the exact answer against an independent oracle, not just that the shape reaches
execute — covers only q01/q03/q06. This widening gap (10 reach vs 3 correctness) is the trust gap: a reach cell
proves a plan runs; a correctness cell proves it computes the right answer.

q05 is the strongest next correctness proof — the **first six-table cyclic-join answer check**, with a
well-bounded answer:

```sql
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
  AND r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01'
GROUP BY n_name ORDER BY revenue DESC
```

No division and no AVG rounding. The revenue arithmetic is the **same
`SUM(l_extendedprice*(1-l_discount))`** q01/q03 already prove (q03's oracle accumulates raw **scale-4**
revenue), so the Decimal logic is reuse; the new work is the six-table traversal with the cyclic constraint and
the q05 ordering prefix.

## Decision

A strict-only q05 correctness oracle, mirroring the `check_strict_q3` shape
(`execute_query_with_tables` → schema assert → non-vacuity guard → compare).

### D1 — `strict_q5_oracle` (independent generator traversal)
Traverse the generators directly (`Region/Nation/Supplier/Customer/Order/LineItem`), not the engine's plan:
- region `r_name == "ASIA"` → its nations; `o_orderdate ∈ [1994-01-01, 1995-01-01)` via Date32 day-counts
  (half-open, mirroring the engine's literalized bounds);
- per lineitem, resolve `order→customer` and `supplier→nation→region`, applying **every** join gate, including
  the cyclic `c_nationkey == s_nationkey == n_nationkey` (a lineitem counts only when its customer's nation
  equals its supplier's nation — both supplier-edge equalities, not just `l_suppkey = s_suppkey`);
- accumulate **raw scale-4 `i128`** revenue (`extprice_raw * (100 - disc_raw)`, the q03 arithmetic) grouped by
  `n_name`; q05 has no division/AVG rounding, so exact raw equality plus the schema assertion is the correctness
  contract.

### D2 — `check_strict_q5` + wiring
- Execute strict q05; **measure, then pin the executor schema** first — `n_name` (Utf8) and the revenue column.
  Do not assume the revenue precision before measurement (the `×` widens scale 2+2 → 4; the precision is the
  engine's, likely `Decimal128(32,4)`). Phase 0 reads the live executor schema, then the implementation records
  and asserts that exact `Decimal128(p, 4)` type — never the `(15,2)` input type.
- Non-vacuity: `selected_rows ≥ 1` **and** `groups.len() > 1` (reuse q03's guard).
- Compare: bijection on `n_name → revenue` (unique keys, exact values, `is_null` checked on every Arrow slot),
  plus an **`ORDER BY revenue DESC` prefix** check only — q05's ORDER BY has no secondary key, so do **not** pin
  the unstated tie-break order.
- CLI `correctness` route for `q05`/`q5`; once the row is logged, `correctness_matrix` renders
  `strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely
A correctness cell is a tripwire only if the oracle is **at least as strict as the comparison contract** — a
lax comparator is a lying green. The load-bearing points (each is where a prior oracle was caught):
1. **Revenue type = measured `Decimal128(p, 4)`** (scale 4), asserted from the live schema — not `(15,2)`.
2. **The cyclic `c_nationkey = s_nationkey` gate** — apply every gate (the ADR 275 miss).
3. **Decimal scale/precision schema + `is_null` per slot** (the ADR 276 miss on q01).
4. **ORDER BY prefix only** — don't pin unstated tie-breaks.

## Boundaries
- Strict-only (Decimal128); dev Float64 correctness deferred (q01/q03/q06 rail convention).
- Independent generator traversal — assert values, not physical plan/paths. No cost-model claims.
- `sf=0.01` (the rail's scale). No reachability changes; no new query. No interval runtime (literalized dates).

## Acceptance
- `q05 strict=pass@compare` in the correctness matrix at `sf=0.01`; reach board unchanged.
- A correctness test asserts the compare runs green and is **discriminating** — it would fail if the engine's
  revenue value, the `n_name` grouping, or the revenue-DESC ordering drifted.

## Forward arc
Correctness rail → 4 cells (q01/q03/q05/q06). Next rail candidate: q17 correctness (the division/rounding oracle
ADR 283 deferred). Reach breadth (q07/q08/q09) remains available once the rail catches up.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition (oracle + check + wiring); no `crates/toron` engine change, no Wendelon
/ cross-ring surface.
