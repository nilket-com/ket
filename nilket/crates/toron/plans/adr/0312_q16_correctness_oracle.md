# ADR 312 — q16 correctness oracle

## Status
Proposed.

## Context

q16 became a genuine keyed reach-17 cell (ADR 311). This oracle answer-checks it — the rail's **first
`COUNT(DISTINCT)`**, plus the null-aware NOT-IN anti-semijoin, the multi-wildcard LIKE, the `<>`/`NOT LIKE`/`IN`
filters, and a 4-key mixed sort. Board: 17 reach / 16 correctness → q16 closes it to 17/17.

```sql
SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%')
GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
```

## Decision

A strict-only q16 correctness oracle at sf=0.01, harness-only. q16 is **mode-agnostic** (Utf8/Utf8/Int32/Int64,
no decimals → dev==strict, identical digest); strict-only per rail convention.

### Scale policy (the Codex caveat, made explicit)
At sf=0.01 q16 is non-empty, but `%Customer%Complaints%` matches **no supplier**, so the NOT-IN exclusion is **not
live** (`harness::q16_default_fixture_is_nonempty_but_anti_join_is_not_live`). **Decision: keep the cell at sf=0.01
for rail consistency + speed and make the synthetic anti-join exclusion test mandatory** (the q22 precedent). The
live run verifies COUNT(DISTINCT), the filters, grouping, and the sort; the synthetic verifies the exclusion
branch. *Alternative (not taken):* if the council later wants the correctness **row itself** to exercise live
exclusion, Phase 0 measures a larger **tractable** scale where excluded suppliers exist and makes that policy
explicit — deferred unless requested.

### D1 — `strict_q16_oracle`
- **Excluded set:** `HashSet<i64>` of `s_suppkey` (`SupplierGenerator`) where `s_comment` matches
  `%Customer%Complaints%` (positional: "Customer" then "Complaints" after — reuse/generalize the q13 matcher).
- **part lookup:** `p_partkey → (p_brand, p_type, p_size)`, keeping only `p_brand != "Brand#45"`,
  `!p_type.starts_with("MEDIUM POLISHED")`, `p_size ∈ {49,14,23,45,19,3,36,9}`.
- **Per `partsupp` row** with a kept part and `ps_suppkey ∉ excluded`: insert `ps_suppkey` into the group's set,
  keyed `(p_brand, p_type, p_size)`. `supplier_cnt = set.len()`. Track the kept row count per group long
  enough to derive `dedup_groups`.
- Emit sorted `(supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC)`.
- `Q16Oracle { groups: BTreeMap<(String,String,i32), HashSet<i64>>, excluded_suppliers, kept_partsupp_rows,
  dedup_groups, pruned_parts }`.

### D2 — `check_strict_q16` + `compare_strict_q16` + wiring
- **Schema** (Phase-0 measure/assert): `p_brand` Utf8, `p_type` Utf8, `p_size` **Int32**, `supplier_cnt`
  **Int64**. `is_null` per cell.
- **Bijection by `(p_brand, p_type, p_size)`**: exact `supplier_cnt` (i64); counts equal; no duplicate.
- **Order = `(supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC)`** — 1 DESC + 3 ASC; verify adjacent pairs.
- CLI `correctness --query q16`; `correctness_matrix` renders `q16 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely
1. **`COUNT(DISTINCT ps_suppkey)` — the rail's first count-distinct.** One suppkey supplying multiple parts in a
   group counts **once** (`HashSet` dedup); a naive `COUNT(*)` over-counts. Synthetic: one suppkey, two parts in a
   group → `supplier_cnt` = 1. Live: assert `dedup_groups > 0` (pre-promotion grounding at sf=0.01 found 1
   dedup group), so the default correctness row exercises real distinct dedup.
2. **The NOT-IN anti-join exclusion — not live at sf=0.01 → MANDATORY synthetic.** A synthetic supplier with
   `%Customer%Complaints%` → its `s_suppkey` excluded → drops out of every group's distinct count.
3. **The positional LIKE `%Customer%Complaints%`** — accept `"…Customer…Complaints…"`, reject
   `"Complaints…Customer"` / bare `"Customer"`. (null-aware NOT-IN edge is moot — `s_suppkey` non-null, no NULLs
   in the excluded set; note it, don't synthesize a NULL key.)
4. **Filters live** (`<> 'Brand#45'`, `NOT LIKE 'MEDIUM POLISHED%'` prefix, `p_size IN`); the 4-key sort;
   `is_null` per cell.

## Phase 0 — measure schema + non-vacuity
Schema (Utf8/Utf8/Int32/Int64). Measure: # groups; `dedup_groups > 0`; `pruned_parts > 0`; kept partsupp rows.
**Assert `excluded_suppliers == 0` at sf=0.01** → the anti-join + LIKE are synthetic-only (mandatory). q16 is
otherwise rich/non-empty. Pre-promotion grounding observed: 296 groups, 1,196 kept partsupp rows, 1 dedup group,
1,701 pruned parts, and 0 excluded suppliers; the impl records the live numbers rather than relying on those
constants.

## Acceptance
- `q16 strict=pass@compare` at sf=0.01; **correctness → 17 (boards level: 17/17)**; reach + other cells unchanged.
- **Mandatory discriminating suite:** COUNT-DISTINCT dedup (one suppkey, two parts → counted once); the anti-join
  exclusion (synthetic supplier-complaint → suppkey dropped); the positional LIKE (Customer→Complaints accept,
  Complaints→Customer reject); a wrong `supplier_cnt`; a filter slip (`<>`/`NOT LIKE`/`IN`);
  missing/extra/duplicate group; a NULL cell; a 4-key-order violation.

## Boundaries
- Strict-only; sf=0.01. Harness q16 SQL unchanged. Independent traversal; no `crates/toron` engine change.
  Reachability untouched.

## Forward arc
**Boards level 17/17** — now including COUNT(DISTINCT), the null-aware anti-semijoin, and a 4-key sort. q11 / q15
remain likely-green coverage; q19 (disjunctive) / q21 (triple self-join) are the heavier reach frontier.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.

## Council provenance
3/3 (Claude/Codex/Gemini), full convergence. Codex's caveat folded + adopted as policy: keep the cell at sf=0.01,
**synthetic anti-join exclusion test mandatory** (the q22 precedent), with the larger-tractable-scale option
deferred unless requested. COUNT(DISTINCT) via per-group `HashSet` (naive COUNT(*) over-counts); the positional
`%Customer%Complaints%` matcher; the 4-key mixed sort; schema Utf8/Utf8/Int32/Int64; `excluded_suppliers` =
Phase-0 measure.
