# ADR 309 — q13 correctness oracle

## Status
Proposed.

## Context

Correctness catchup over q13. Board: 16 reach, 15 correctness. This cut closes the gap again
(correctness 15 -> 16, leveling with reach 16). q13 is the first outer-join ON-residual probe,
combining `LEFT OUTER JOIN` with a side-local `ON` filter, `COUNT(o_orderkey)` non-null semantics,
count-of-counts grouping, and a two-key descending sort.

```sql
SELECT c_count, COUNT(*) AS custdist
FROM (
  SELECT c_custkey, COUNT(o_orderkey) AS c_count
  FROM customer LEFT OUTER JOIN orders
    ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
  GROUP BY c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
```

## Decision

A strict-only q13 correctness oracle, harness-only; the default acceptance rail remains sf=0.01.
Traverse generators directly and compare exact `Int64` counts.

### D1 — `strict_q13_oracle`
- `o_comment NOT LIKE '%special%requests%'`: check if the comment contains `"special"` followed by
  `"requests"` at or after the end of that `"special"`. Exclude matching orders.
- Map `c_custkey -> count_of_qualifying_orders` for each customer, defaulting to `0` (null-extension).
- Group the customers by the order count (`c_count`) and aggregate how many customers have that count (`custdist`).
- `Q13Oracle` carries the sorted `(c_count, custdist)` groups plus non-vacuity counters:
  `total_customers`, `qualifying_orders`, `comment_excluded_orders`, `zero_count_customers`,
  `positive_count_customers`, `no_order_customers`, and `only_excluded_order_customers`.

### D2 — `check_strict_q13` + `compare_strict_q13` + wiring
- **Schema** (Phase-0 confirm): `c_count` Int64, `custdist` Int64; `is_null` per cell.
- **Bijection by `c_count`**: exact `custdist` (raw `i64`); row counts equal; no duplicate.
- **Order = `ORDER BY custdist DESC, c_count DESC`** — total order; assert exact descending sequence.
- **Non-vacuity**: Phase 0 measures the live `sf=0.01` counters and accepts the default scale only if
  `total_customers > 0`, `groups.len() > 1`, `zero_count_customers > 0`, `positive_count_customers > 0`,
  `qualifying_orders > 0`, and `comment_excluded_orders > 0`. The strongest ADR-308-specific live proof is
  `only_excluded_order_customers > 0`; if absent, record that fact and rely on the synthetic for that sub-case,
  but do not weaken the required live zero-count and comment-exclusion gates.
- CLI `correctness --query q13`; `correctness-matrix` renders `q13 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely
1. **Wildcard `%special%requests%` sequence matching**: `"special"` must appear before `"requests"`; zero
   characters between them is valid (`"specialrequests"` matches). A simpler
   `contains("special") && contains("requests")` check is incorrect because it accepts `"requests special"`.
2. **LEFT OUTER JOIN preservation**: A customer with no orders or whose orders are all comment-excluded must
   remain with `c_count = 0`. Pin **both** sub-cases in one synthetic (residual + null-extension combine): a
   no-orders customer AND a customer whose *only* order is a `special...requests` both land in the `0` group.
3. **`COUNT(o_orderkey)` non-null semantics**: excluded orders do not increment `c_count`; qualifying orders do.
4. **Descending ordering**: Both fields are sorted descending.

## Acceptance
- `q13 strict=pass@compare` at sf=0.01; rail -> 16; reach + other correctness cells unchanged.
- **Discriminating suite**: reject a wrong wildcard sequence check (for example accepting `"requests special"`),
  a wrong `custdist`, a missing/extra/duplicate group, a NULL cell, and an order violation. Include synthetic
  coverage for no-orders customers, only-excluded-order customers, and mixed qualifying/excluded orders.

## Boundaries
- Strict-only. Harness q13 SQL unchanged. Independent traversal; no `crates/toron` engine change. Reachability untouched; `status_rows` already lists q13.

## Forward arc
Boards level at 16/16. Cadence reopens the reach frontier (q11/q15/q16/q18/q19/q21 remain).

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.
