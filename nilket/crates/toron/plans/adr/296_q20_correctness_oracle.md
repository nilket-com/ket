# ADR 296 — q20 correctness oracle

## Status
Proposed.

## Context

Correctness catchup #2 of the 2:1 cadence (q10 was #1). Board: 12 reach, 9 correctness. q20 is the
**highest-trust** catchup — the first oracle to answer-check a **membership-result set** from the nested-IN +
correlated-scalar composition (ADR 285/286): it proves that shape returns the *right* suppliers, not just that it
reaches execute. Non-empty at sf=0.01 (masked-empty trap cleared; strict reaches execute in ~52ms — keyed).

```sql
SELECT s_name, s_address FROM supplier, nation
WHERE s_suppkey IN (
  SELECT ps_suppkey FROM partsupp
  WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
    AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem
      WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
        AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01')
) AND s_nationkey = n_nationkey AND n_name = 'CANADA'
ORDER BY s_name
```

The output is a simple 2-column set (`s_name`, `s_address`, both Utf8), but the oracle *logic* is the most
involved yet — it replicates the nested membership **and** the correlated threshold.

## Decision

A strict-only q20 correctness oracle at sf=0.01, harness-only; reuse the `strict_q5_oracle` generator traversal.

### D1 — `strict_q20_oracle`
1. **forest parts:** `p_partkey` where `p_name.starts_with("forest")` (prefix, not contains).
2. **CANADA suppliers:** `n_nationkey` where `n_name == "CANADA"`; `s_suppkey → (s_name, s_address)` for CANADA
   suppliers.
3. **Per partsupp row** with `ps_partkey ∈ forest`: `sum_qty_raw = SUM(l_quantity)` (raw scale 2) over lineitems
   with `l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate ∈ [1994-01-01, 1995-01-01)`; keep the
   row per the threshold (below). A supplier qualifies if **≥1** of its forest partsupp rows passes (`IN`
   semantics — distinct `ps_suppkey`).
4. Emit `(s_name, s_address)` for CANADA suppliers in the qualifying set; sort by `s_name`.
5. `Q20Oracle { rows, selected_partsupp_rows, qualifying_suppliers, threshold_pass_rows,
   threshold_fail_rows }`.

### The threshold — grounded, exact integer (no f64)
`ps_availqty` (Int32) `> 0.5 * SUM(l_quantity)`. **Toron coerces the `0.5` literal to `Decimal128(15,2)`** (scale
2), so `0.5 * SUM` is `Decimal128(31,4)` (raw `50 * sum_qty_raw`, scale 4); ADR 286 then aligns `ps_availqty` to
that comparison type (raw `ps_availqty * 10_000`). The oracle predicate is therefore the **exact integer**
comparison:
```
ps_availqty * 10_000 > 50 * sum_qty_raw          // strict >, scale 4, no float, no division, no rounding
```
(Grounded by Codex against current Toron — *not* a paper-derived scale; the `0.5`-coerced-to-scale-2 is the
load-bearing fact. The live `strict_q20_matches_independent_oracle` test cross-validates the scale.)

### `SUM` over an absent correlated group — SQL semantics, not engine mirroring
A forest partsupp with **no** matching 1994 lineitems has `SUM(l_quantity) = NULL` under SQL semantics →
`ps_availqty > NULL` → unknown → the row **fails** (does not qualify). **Do not bake `sum=0` (which would make
the row pass).** Phase-0 should measure whether the live sf=0.01 data naturally exercises absent groups, but the
oracle encodes the SQL rule. If the engine treats empty SUM as 0 and the live data exposes it, the correctness row
must fail at compare rather than mirror the bug.

### `compare_strict_q20`
- Schema first: `s_name` Utf8, `s_address` Utf8 (Phase-0 pin); `is_null` per cell.
- **Answer = bijection by `s_name`** (globally unique in TPC-H) of `(s_name, s_address)`; row counts equal; no
  duplicate.
- **Order = `ORDER BY s_name`** — assert non-decreasing `s_name` (effectively total; tie-tolerant if it weren't
  unique).

## The discipline: the oracle must mirror expected semantics completely
1. **Threshold = `ps_availqty*10_000 > 50*sum_qty_raw`** — exact integer, strict `>`, grounded scale 4 (not f64,
   not scale 3).
2. **Absent-group `SUM` semantics** — SQL-standard NULL → row fails; measure whether live data exercises it, but
   do not mirror an engine-side 0 bug.
3. **Threshold must be DISCRIMINATING** — live non-vacuity requires output rows, selected forest partsupp rows,
   and at least one threshold pass. If the live scale also has threshold failures, assert that. If it does not,
   the implementation must record that fact and add a **synthetic boundary test** (a partsupp just above/below
   `0.5*SUM` flips inclusion). The synthetic test covers the threshold edge, but it is not a substitute for a
   non-empty live q20 answer.
4. **Membership + every gate** — `forest%` prefix, CANADA, the shipdate range, the partkey+suppkey correlation;
   `is_null`; `ORDER BY s_name`.

## Acceptance
- `q20 strict=pass@compare` at sf=0.01; rail → 10; reach + other correctness cells unchanged.
- **Discriminating suite:** reject a wrong supplier set (extra/missing/duplicate `s_name`), a wrong `s_address`, a
  **threshold-boundary flip** (a partsupp at/near `0.5*SUM`, strict `>`), an **absent-group misclassification** (a
  no-lineitem partsupp must fail by SQL's NULL rule), a `forest`-as-contains slip, a non-CANADA supplier
  admitted, a NULL cell, and an `s_name` order violation; live non-vacuity requires a non-empty q20 answer and
  at least one threshold pass. If live data does not naturally include a threshold failure, the synthetic
  boundary test covers the exclusion edge and the results doc says so explicitly.

## Boundaries
- Strict-only; sf=0.01. Harness q20 SQL unchanged. Independent traversal; no `crates/toron` engine change.
  Reachability untouched (q20 stays `pass@execute`).

## Forward arc
Rail → 10 (q01/q02/q03/q05/q06/q10/q12/q14/q17/q20) — completing the 2-correctness leg. Then a reach press:
**q08** is unblocked from the ADR 294 derived-table comma-list work; q09 (substring LIKE + 6-table) is another
candidate.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.

## Council provenance
3/3 on q20. Resolutions folded: **Codex's grounded threshold** `ps_availqty*10_000 > 50*sum_qty_raw` (`0.5`
coerced to `Decimal128(15,2)` → scale-4 product; ADR 286 Int alignment) over Claude's ungrounded scale-3 and
Gemini's f64 — grounding beats assertion. Added: the **absent-group `SUM` = NULL→fail** semantics (don't bake
`sum=0`); live non-vacuity plus a threshold-boundary synthetic if live data lacks failures; forest prefix;
bijection by `s_name`. Reuses `strict_q5_oracle` + the q17 correlated-threshold shape.
