# ADR 306 — q22 correctness oracle

## Status
Proposed.

## Context

q22 reached `pass@execute` in ADR 305, but it did so through machinery the correctness rail has not yet verified
together: a **correlated `NOT EXISTS` anti-join**, a **non-correlated AVG-threshold scalar subquery**, and
**SUBSTRING**. This oracle proves those semantics over real data instead of leaving q22 as reach-only. Board:
15 reach / 14 correctness → q22 closes it to 15/15.

```sql
SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
FROM (
  SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
  FROM customer
  WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')
    AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer
                     WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone FROM 1 FOR 2) IN (…7…))
    AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
) AS custsale
GROUP BY cntrycode ORDER BY cntrycode
```

## Decision

A strict-only q22 correctness oracle, harness-only — a **2-pass** independent traversal over `customer` +
`orders`. The target scale is `sf=0.01`; Phase 0 must prove that scale is non-vacuous before the implementation
locks it in.

### D1 — `strict_q22_oracle`
- **Pass 1 — the AVG threshold (global scalar):** over customers with `cntrycode ∈ 7` **and** `c_acctbal > 0`,
  accumulate `sum`/`count`; `avg_threshold_raw = avg_decimal_raw(sum, count, 2, 6, 19)` → **`Decimal128(19,6)`**
  (ADR 268). **Reuse the existing `avg_decimal_raw` helper** (lib.rs:1494, the q01 AVG path) so the oracle's
  rounding == the engine's.
- **Pass 2 — filtered customers:** build the `orders` `o_custkey` `HashSet` (mirror q04's EXISTS set). Keep a
  customer iff: `cntrycode = c_phone[0..2] ∈ 7`; **`c_acctbal_raw·10⁴ > avg_threshold_raw`** (mixed-(p,s) → align
  to scale 6, ADR 286); **`c_custkey ∉ orders`** (the NOT-EXISTS anti-join). Group by `cntrycode`:
  `numcust += 1`, `totacctbal += c_acctbal_raw` (scale 2).
- `Q22Oracle { groups: BTreeMap<String,(i64, i128)>, avg_threshold_raw, with_orders_excluded, selected_rows }`.

### D2 — `check_strict_q22` + `compare_strict_q22` + wiring
- **Schema** (Phase-0 measure/assert first, drift → fail): `cntrycode` Utf8, `numcust` **Int64** (COUNT),
  `totacctbal` **`Decimal128(p,2)`** (SUM(c_acctbal); measure `p`). `is_null` per cell.
- **Bijection by `cntrycode`**: exact `numcust` (i64) + `totacctbal` (raw `i128`); counts equal; no duplicate.
- **Order = `cntrycode ASC`**.
- CLI `correctness --query q22`; `correctness_matrix` renders `q22 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely (most semantically loaded oracle yet)
1. **The `NOT EXISTS` anti-join (FIRST anti-join correctness check)** — q04 verified EXISTS/semi-join; q22 is the
   **inverse**. A customer **with** an order must be **excluded**, **without** **included**. The most-unverified
   semantic q22 silently exercised. Synthetic: the same customer flips membership when an order is added.
2. **The AVG-threshold scalar subquery** — `avg` over `c_acctbal > 0 ∧ in-7`, `Decimal128(19,6)` via
   `avg_decimal_raw`; then the mixed-(p,s) `c_acctbal > avg` (ADR 286). Synthetic: a customer **at/just-below** avg
   excluded, **just-above** included; the oracle's avg == the engine's.
3. **SUBSTRING prefix** → `cntrycode` = first 2 chars (char-counted, ADR 305); the **IN-7** filter (a non-7 /
   wrong-prefix excluded).
4. **COUNT(*) + SUM(c_acctbal) per group**, `cntrycode ASC`, `is_null` per cell, exact `i64`/`i128`.

## Phase 0 — measure schema + non-vacuity (masked-empty check)
TPC-H leaves many customers order-less, so q22 is expected to produce non-empty groups at `sf=0.01` — but the
implementation must **measure the actual row count + per-group `numcust`** before accepting that scale. A hollow
result is not an acceptable `pass@compare`: if `sf=0.01` has no selected rows, retarget to the smallest tractable
scale with selected rows (the q17 correctness precedent) or stop and revise the plan. Synthetics remain mandatory
either way: the live aggregate exercises the real query, while synthetic fixtures pin exact boundary cases such as
anti-join exclusion and threshold comparison.

## Acceptance
- `q22 strict=pass@compare` at the measured non-vacuous correctness scale (expected `sf=0.01`); **correctness →
  15 (boards level: 15/15)**; reach + other cells unchanged.
- **Mandatory discriminating suite:** anti-join flip (with/without an order); threshold boundary (at/below vs above
  avg, + avg matches engine); SUBSTRING prefix + IN-7 (a non-7 / wrong-prefix excluded); a wrong `numcust`/
  `totacctbal`; missing/extra/duplicate `cntrycode`; a NULL cell; a `cntrycode`-order violation.

## Boundaries
- Strict-only; target `sf=0.01` unless Phase 0 proves it hollow. Harness q22 SQL unchanged. Independent traversal;
  no `crates/toron` engine change. Reachability untouched.

## Forward arc
**Boards level 15/15** — now including the anti-join + scalar-threshold + SUBSTRING. The cadence reopens the reach
frontier (q11/q13/q15/q16/q18/q19/q21 remain; q13 non-equi-`ON` and q19 disjunctive are the largest known cuts).

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.

## Council provenance
3/3 (Claude/Codex/Gemini). Both peers grounded the **`avg_decimal_raw(sum, count, 2, 6, 19)` reuse** (lib.rs:1494,
the q01 AVG path) so the threshold rounding mirrors the engine. Folded: the 2-pass oracle; the anti-join (q04
EXISTS HashSet inverted) as the FIRST anti-join check; the `c_acctbal·10⁴ > avg` scale-6 comparison (ADR 286);
SUBSTRING char-prefix (ADR 305); mandatory synthetics; Phase-0 non-vacuity. `totacctbal` SUM precision = measured.
