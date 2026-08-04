# ADR 289 — q14 correctness oracle

## Status
Proposed.

## Context

The correctness rail covers q01/q03/q05/q06/q17. With q17 successfully answer-checking Decimal division and
correlated subqueries, strict q14 is the natural next correctness query to wire into the rail:

```sql
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
  THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
  / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'
```

Unlike q17, q14 is **non-empty at the standard rail scale** (`sf=0.01`). It verifies the **Decimal division
ratio** (SUM / SUM quotient) scaled by the Decimal literal `100.00`, conditional logic via `CASE WHEN`, and
string prefix matching (`p_type LIKE 'PROMO%'`).

## Decision

Implement a strict-only q14 correctness oracle at **sf=0.01**, mirroring the verification pattern of earlier
correctness queries (execute → schema-assert → non-vacuity → compare).

### D1 — `strict_q14_oracle` (independent generator traversal at sf=0.01)
1. Traverse `part` and `lineitem` generators at `sf=0.01`. Join them on `l_partkey = p_partkey`.
2. Filter shipdate range `[1995-09-01, 1995-10-01)`. The date bounds map to Date32 day counts `9374..9404`.
3. Compute the raw `i128` scale-4 discounted price for each matching row:
   - `disc_price_raw = l_extendedprice_raw * (100 - l_discount_raw)` (both inputs are scale-2; product is scale-4).
4. Accumulate:
   - `sum_all_raw` (sum of all matching `disc_price_raw`).
   - `sum_promo_raw` (sum of `disc_price_raw` where `p_type.starts_with("PROMO")`).
5. Scale the promo sum by literal `100.00` (raw `10000` at scale 2) to obtain the numerator:
   - `numerator = sum_promo_raw * 10000` (scale 6).
6. Divide by denominator `sum_all_raw` (scale 4) using the existing `decimal_division_raw` helper:
   - Output schema: `promo_revenue Decimal128(38, 10)` (measured live at Phase 0).
   - Division scale delta: `10 + 4 - 6 = 8`.
   - Multiplier: `10^8 = 100,000,000`.
   - Scaled numerator: `numerator * 10^8`.
   - Quotient: `scaled_numerator / sum_all_raw`, rounded half-away-from-zero.
7. Non-vacuity guard: **`sum_all_raw > 0`, ≥1 contributing lineitem row, AND ≥1 PROMO-branch row**
   (`p_type.starts_with("PROMO")`). The PROMO requirement is load-bearing: a denominator-only guard passes
   vacuously when no PROMO part matches (numerator `0` → ratio `0`), never exercising the `CASE`-true arm. This
   is the q17 masked-empty lesson applied to the CASE branch. Phase 0 must confirm PROMO rows > 0 at `sf=0.01`;
   if zero, re-scope before trusting the comparator.

### D2 — `check_strict_q14` + `compare_strict_q14` + wiring
- Execute strict q14 at sf=0.01; assert the Phase-0-measured `promo_revenue` schema first.
- Compare the single scalar: exact value (raw `i128` at the measured scale `10`), `is_null` checked.
- Wire `correctness --query q14` in `crates/toron-tpch/src/main.rs`.

## The discipline: the oracle must mirror the executor completely
1. **Half-away-from-zero** rounding on the ratio quotient — validated via a **truncation-vs-half-away test** that fails if the quotient truncates.
2. **Replicate CASE WHEN logic** and LIKE prefix match (`starts_with("PROMO")`) exactly.
3. **Non-vacuity requires matching data** — denominator positive **and ≥1 PROMO-branch row** (so the `CASE`-true arm is exercised, not a hollow `0/den` green).
4. Independent generator traversal; `is_null` per Arrow slot.

## Boundaries
- Strict-only (Decimal128); dev Float64 correctness deferred (rail convention).
- **sf=0.01 default** (non-empty). Official q14 SQL unchanged.
- Independent traversal — assert the value, not the plan. Mirror the executor, don't reimplement it from the engine.

## Acceptance
- `q14 strict=pass@compare` at sf=0.01; reach board + other correctness cells unchanged.
- A **discriminating** suite in `crates/toron-tpch/tests/correctness_test.rs`: the compare rejects a truncated
  quotient, an injected value mismatch, a dropped contributing row, a **promo-rows-zero oracle** (hollow
  CASE-true branch), and a vacuous result.

## Forward arc
Correctness rail → 6 cells (q01/q03/q05/q06/q14/q17). Reach breadth (q07/q08/q09) remains available.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition (oracle + check + wiring); no `crates/toron` engine change, no Wendelon / cross-ring surface.
