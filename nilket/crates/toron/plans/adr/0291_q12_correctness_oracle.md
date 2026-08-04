# ADR 291 — q12 correctness oracle

## Status
Proposed.

## Context

The correctness catchup after ADR 290's q12 reach cut. Rail: q01/q03/q05/q06/q14/q17 (6). q12 is a **new oracle
shape** — the first **integer-count** correctness cell (all prior cells are Decimal SUM/ratio) and the first with
**two aggregate output columns per group**, with **no Decimal arithmetic, no division, no tie-order policy**:

```sql
SELECT l_shipmode,
  SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
  SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL','SHIP')
  AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01'
GROUP BY l_shipmode ORDER BY l_shipmode
```

## Decision

A strict-only q12 correctness oracle at sf=0.01, reusing `q12_tables(true, sf)` (ADR 290) + the `strict_q5_oracle`
grouped-`BTreeMap` pattern (execute → schema-assert → non-vacuity → compare). Harness-only; no engine change.

### D1 — `strict_q12_oracle` (independent generator traversal)
```rust
pub struct Q12Group { pub high_line_count: i64, pub low_line_count: i64 }
pub struct Q12Oracle {
    pub groups: BTreeMap<String, Q12Group>,
    pub selected_rows: usize, pub high_rows: usize, pub low_rows: usize,  // branch-coverage guards
}
```
- Build `o_orderkey → o_orderpriority` from `OrderGenerator`.
- Per `LineItemGenerator` row, apply **every** filter exactly: `l_shipmode ∈ {MAIL, SHIP}`,
  `l_commitdate < l_receiptdate`, `l_shipdate < l_commitdate`,
  `l_receiptdate ∈ [day(1994-01-01)=8766, day(1995-01-01)=9131)`, and the join (order exists by `l_orderkey`).
- Group by `l_shipmode`; classify `high = o_orderpriority ∈ {'1-URGENT','2-HIGH'}`, else `low`; accumulate
  `(high_line_count, low_line_count)` as `i64`; tally `selected_rows`/`high_rows`/`low_rows`.

### D2 — `check_strict_q12` + `compare_strict_q12` + wiring
- Execute strict q12 at sf=0.01; **Phase-0 measure and pin** the count column type. Strong prior: `CASE → Int32`,
  `SUM(Int32) → Int64`, so `high_line_count`/`low_line_count` are **`Int64`** — but assert the *measured* live
  schema; drift is `fail@compare`, never a silent oracle adjustment.
- **Non-vacuity** (global floor): both `MAIL` and `SHIP` groups present, `selected_rows > 0`, `high_rows > 0`,
  `low_rows > 0` (each CASE arm exercised — the q14 PROMO-branch lesson). Phase-0 measures whether **per-group**
  strictly-positive (each mode has high>0 AND low>0) holds at sf=0.01; if so, strengthen the guard per group —
  do not assume it without measuring.
- **Comparator**: schema first; `is_null` rejected on every cell; row count == oracle group count; no duplicate
  `l_shipmode`; exact `(high_line_count, low_line_count)` per group; **emitted order == `ORDER BY l_shipmode`**.
  q12 has a **total ordering by the unique group key**, so reject out-of-order rows — *no tie tolerance* (unlike
  q05's revenue-DESC-with-ties).

## The discipline: the oracle must mirror the executor completely
1. **Measure the count output type** — first integer result on the rail; `Int64` is the prior, assert the live
   type.
2. **Both-CASE-arms non-vacuity** — `high_rows > 0` AND `low_rows > 0` AND both groups, so neither classification
   is a hollow branch.
3. **Replicate every filter** — the two **column-to-column** Date32 inequalities + the date range + the
   `l_shipmode` set + the priority string classification (the q05 every-gate lesson).
4. **Exact order** — total ordering by unique key; reject out-of-order (no tie permutation tolerance).
5. Independent traversal; `is_null` per slot; schema-assert first; exact integer counts.

## Boundaries
- Strict-only (rail convention); sf=0.01 (q12 non-empty/keyed there). The ADR 290 q12 reachability SQL remains
  unchanged: tpchgen Q12 with parameters inlined and the interval upper bound literalized.
- Independent generator traversal — assert the values, not the plan. No `crates/toron` engine change. Reachability
  untouched (q12 stays `pass@execute`).

## Acceptance
- `q12 strict=pass@compare` at sf=0.01; rail → 7 (q01/q03/q05/q06/q12/q14/q17); reach + other correctness cells
  unchanged.
- A **discriminating** suite in `correctness_test.rs`: rejects a wrong high/low count, a **high/low-swapped**
  group (misclassification), a missing/duplicate group, a NULL cell, a wrong column type, **out-of-order** groups,
  and a hollow (one-arm) oracle — proving it's ≥ as strict as the engine.

## Forward arc
Rail → 7 (integer-count + multi-column-per-group shape covered). Next catchup candidates: q02 (correlated MIN +
wide projection) or q10 (4-table grouped revenue, DESC-with-ties); or another reach.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition (oracle + check + wiring); no `crates/toron` engine change, no Wendelon
/ cross-ring surface.

## Council provenance
3/3 on q12 correctness. Resolutions folded: count type measured-not-baked (`Int64` prior; assert live schema);
non-vacuity = global floor (both groups + `high_rows`/`low_rows` > 0), strengthen per-group only if Phase-0
confirms (Codex's grounding over Antigravity's unconditional per-group assertion); exact-order comparator (total
ordering by unique key, no tie tolerance — distinct from q05). Reuses ADR 290's `q12_tables` + the q05 grouped
oracle pattern.
