# ADR 292 — q02 correctness oracle

## Status
Proposed.

## Context

Catchup #2 of the council's 2-correctness : 1-reach cadence (q12 was #1). Rail: q01/q03/q05/q06/q12/q14/q17 (7).
q02 is the **broadest oracle yet** — a five-table join + correlated `MIN` + wide 8-column projection +
mixed-direction `ORDER BY` — but two facts keep it tractable: **no `LIMIT`** (the harness q02 omits the official
`LIMIT 100`, ADR 284) and **no projection arithmetic** (all 8 columns are stored values; `s_acctbal` is the
supplier's balance, not computed). So q02 tests **join + correlated MIN + projection + ordering**, not rounding.

```sql
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
  AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region
    WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey AND r_name = 'EUROPE')
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
```

## Decision

A strict-only q02 correctness oracle at sf=0.01, reusing `q02_tables(true, sf)` (ADR 284) + the multi-table
generator-traversal pattern (execute → schema-assert → non-vacuity → compare). Harness-only; no engine change.

### D1 — `strict_q02_oracle` (independent generator traversal)
```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Q02Row {
    pub s_acctbal: i128, pub s_name: String, pub n_name: String, pub p_partkey: i64,
    pub p_mfgr: String, pub s_address: String, pub s_phone: String, pub s_comment: String,
}
pub struct Q02Oracle { pub rows: Vec<Q02Row>, pub matched_parts: usize, pub min_tie_parts: usize }
```
- `region` → EUROPE keys; `nation` → EUROPE nations (`n_nationkey → n_name`); `supplier` filtered to EUROPE
  nations, retaining raw `s_acctbal` + projected fields + `n_name`; `part` filtered `p_size == 15` &&
  `q02_type_matches_brass(p_type)`, a tiny public helper that implements `ends_with("BRASS")` (**suffix**, not
  contains), retaining `p_mfgr`.
- **EUROPE-scoped correlated MIN:** per eligible `ps_partkey`, `min_cost = MIN(ps_supplycost)` over **EUROPE
  suppliers only** (the subquery repeats `supplier, nation, region` + `r_name='EUROPE'` — a *global* min would be
  a wrong oracle). Track `matched_parts` and `min_tie_parts` (parts with ≥2 suppliers at the min).
- **Emit every** `(part, supplier)` whose raw `ps_supplycost == min_cost` → a full `Q02Row`. **A part with
  several min-cost suppliers yields several rows — include them all** (the correlated-MIN tie; don't collapse to
  one supplier per part).

### D2 — `compare_strict_q02` (answer correctness ⟂ order correctness)
- **Schema first** — Phase-0 measure + pin all 8 types. Priors: `s_acctbal` `Decimal128(15,2)`, `p_partkey`
  **`Int64`** (reject the `Int32` variant), the other six `Utf8`. `is_null` rejected on every cell before reading.
- **Answer correctness = full-row multiset.** Compare a **multiset** of full 8-tuple `Q02Row` values
  (canonical-sort both sides, element-wise) — **not** a key-map. q02 is `SELECT` without `DISTINCT`, so a
  visible-key map (e.g. `(p_partkey, s_name)`) could silently dedup a genuine duplicate row. Exact raw `i128`
  `s_acctbal`; exact `String`/`i64` for the rest.
- **Order correctness (tie-tolerant).** Walk the emitted sequence; verify each adjacent pair respects
  `s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC`. Do **not** pin a hidden tie-break when two rows tie on
  all four keys (the q05 prefix lesson, extended to four mixed-direction keys). The comparator must not row-compare
  emitted output against the oracle's `ORDER BY` sort; answer equality and SQL order are separate checks.

## The discipline: the oracle must mirror the executor completely
1. **EUROPE-scoped MIN**, not global — the load-bearing correlated-subquery semantics.
2. **Emit all min-cost ties** — multiple rows per part; a one-row-per-part oracle is wrong.
3. **Full-row multiset** (no DISTINCT → no key-map dedup).
4. **Measured schema** — `p_partkey Int64`, `s_acctbal Decimal128(15,2)`; assert live, don't bake.
5. **Tie-tolerant mixed-direction order check**, separate from answer correctness; `is_null` per slot.
6. **No hard-coded row count.** The full set is small at sf=0.01 (measured), but the invariant is *engine == the
   independent oracle* + non-vacuity — record the count in results, never assert a magic constant.

## Boundaries
- Strict-only; sf=0.01. **Harness q02 SQL unchanged** (no LIMIT — do not add it). Independent traversal; no
  `crates/toron` engine change. Reachability untouched (q02 stays `pass@execute`).

## Acceptance
- `q02 strict=pass@compare` at sf=0.01; rail → 8; reach + other correctness cells unchanged.
- **Non-vacuity:** `matched_parts > 0`. Phase-0 records the emitted row count and `min_tie_parts`. If
  `min_tie_parts == 0` at sf=0.01, the oracle stays tie-correct and **synthetic** comparator tests cover the tie
  policy (a dropped same-part min supplier must fail) — the live fixture need not exercise every branch.
- A **discriminating** suite: reject a wrong projected value (Decimal `s_acctbal` / string / `p_partkey`), a
  **dropped min-cost supplier** (the MIN-tie path, synthetic), a non-min/extra row, a missing row, an unexpected
  duplicate row, a NULL in any column, a wrong column type (incl. `p_partkey Int32`), a `%BRASS`-as-contains slip,
  and an **order-violating** sequence (incl. the mixed `DESC`/`ASC`).

## Forward arc
Rail → 8. This completes catchup #2 → the cadence calls for a **reach** ("machetes into the jungle") — q07 (nation
self-join, likely a new engine wall) or q09 (substring LIKE + 6-table) are the candidates.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition (oracle + check + wiring); no `crates/toron` engine change, no Wendelon
/ cross-ring surface.

## Council provenance
3/3 on q02 correctness. Resolutions folded: `p_partkey` **Int64** (Codex's correction of Claude's iter-0 Int32);
full-row **multiset** not a key-map (SELECT without DISTINCT); **no hard-coded `rows.len()==4`** — record it,
compare vs the oracle + non-vacuity (Codex's grounding over Gemini's count invariant); EUROPE-scoped MIN, all min
ties emitted, `%BRASS` suffix. Reuses ADR 284's `q02_tables` + the q05 multi-table oracle pattern.
