# ADR 288 — q17 correctness oracle

## Status
Proposed.

## Context

The correctness rail covers q01/q03/q05/q06 — all **SUM-only**, none exercising a rounding step. q17 is the
strongest next proof: the **first division correctness oracle** and the **first to validate q17's correlated
scalar over real matching data** (q17's reach cell, ADR 281/282, only ever ran over *empty* data — a hollow
green this cut closes).

```sql
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX'
  AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)
```

**The masked-empty trap (load-bearing):** no part matches `Brand#23`+`MED BOX` at sf ≤ 0.01, so at the rail's
default scale q17 returns a single **NULL** `avg_yearly` — a correctness check there would assert `NULL == NULL`,
vacuous. Grounded sweep (built binary): q17 is empty at sf=0.01, **non-empty at sf=0.02** (2 matching parts, ~6
contributing lineitems, non-NULL `avg_yearly`; tractable — sub-second query / ~3.8s end-to-end run). **This cut
runs at sf=0.02 explicitly**, not the rail default.

**Rounding (the new rail challenge):** Decimal `/` and AVG round **half away from zero** (ADR 084; the
executor's Decimal division and AVG-finalize helpers). The oracle must mirror it on **both** division steps.

## Decision

A strict-only q17 correctness oracle at **sf=0.02**, mirroring `check_strict_q5`'s shape (execute → schema-assert
→ non-vacuity → compare), but q17's result is a **single scalar row** and the oracle is the most
arithmetic-heavy yet.

### D1 — `strict_q17_oracle` (independent generator traversal at sf=0.02)
1. Per `p_partkey`: `0.2 * AVG(l_quantity over that partkey's lineitems)` — AVG with **half-away** rounding via
   the existing `avg_decimal_raw` helper (scale 2 → the AVG output scale), then `* 0.2` → the per-part threshold.
2. Eligible parts = `p_brand='Brand#23'` AND `p_container='MED BOX'`; per eligible part, keep its lineitems
   (`l_partkey=p_partkey`) with `l_quantity < threshold`.
3. `SUM(l_extendedprice)` of kept lineitems (raw scale-2 `i128`), then `/ 7.0` with **half-away** division
   rounding to the measured output scale → the expected scale-6 `i128`.
4. Non-vacuity: **≥1 eligible part, ≥1 contributing lineitem, non-NULL result** — stronger than "query
   executed"; this is the anti-empty-green guard.

### D2 — `check_strict_q17` + `compare_strict_q17` + wiring
- Execute strict q17 at sf=0.02; **Phase-0 measure and pin the live output schema** — `avg_yearly`
  `Decimal128(p, s)` (measured `Decimal128(21, 6)`; the impl asserts the measured type, never a guess). The
  Decimal **division output type** is the thing to pin.
- Non-vacuity guard (D1.4) before comparing.
- Compare the single scalar: exact value (raw `i128` at the measured scale), `is_null` checked. No grouping/order
  to compare.
- CLI `correctness` route for `q17`; the logged row records `sf=0.02` and renders
  `q17 strict=pass@compare`. Because `sf=0.01` is a known masked-empty path, q17 is a named exception to the
  correctness rail default: absent `--sf` means `0.02`, explicit `--sf 0.02` is accepted, and any other explicit
  q17 correctness scale is a harness usage error (exit 2, no telemetry row).

### Shared rounding helper
A focused/shared half-away helper matching the executor, used for both the `0.2*AVG` threshold and the `/7.0`
quotient — so the oracle's rounding policy is one auditable place, not duplicated ad hoc.

## The discipline: the oracle must mirror the executor completely
1. **Half-away-from-zero** on **both** `0.2*AVG(l_quantity)` (the correlated-scalar threshold) **and**
   `SUM(l_extendedprice)/7.0` (`avg_yearly`) — a **truncation-vs-half-away test** that fails if the quotient
   truncates, plus an **injected-mismatch test**; otherwise the first division oracle can look green without
   proving the rounding policy.
2. **Replicate the correlated scalar exactly** (per-partkey AVG + `0.2*` + the `<` filter) — proving the q17
   correlated-scalar path over real rows.
3. **Non-vacuity requires matching data** (≥1 part + ≥1 row + non-NULL) — the anti-empty trap.
4. Independent generator traversal; `is_null` per Arrow slot; Phase-0 live schema.

## Boundaries
- Strict-only (Decimal128); dev Float64 correctness deferred (rail convention).
- **sf=0.02 explicit** (q17's first non-empty scale; sf=0.01 is masked-empty — the results record why q17's
  correctness scale differs from the rail's 0.01). Official q17 SQL unchanged.
- Independent traversal — assert the value, not the plan. Mirror the executor, don't reimplement it from the
  engine. No interval runtime.

## Acceptance
- `q17 strict=pass@compare` at sf=0.02; reach board + other correctness cells (q01/q03/q05/q06) unchanged.
- A **discriminating** suite: the compare rejects a truncated (non-half-away) quotient, an injected value
  mismatch, a dropped contributing row, and a vacuous (empty/NULL) result — proving it's ≥ as strict as the
  engine.
- The q17 correctness CLI scale policy is pinned: default/absent scale records `sf=0.02`; explicit `--sf 0.02`
  is accepted; explicit `--sf 0.01` is a usage error with no row.

## Forward arc
Correctness rail → 5 cells (q01/q03/q05/q06/q17). q14 stays as a later, simpler division/CASE correctness cell
(SUM/SUM ratio, non-empty at sf=0.01). Reach breadth (q07/q08/q09) remains available.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition (oracle + check + wiring); no `crates/toron` engine change, no Wendelon
/ cross-ring surface.
