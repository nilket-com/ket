# ADR 295 — q10 correctness oracle

## Status
Proposed.

## Context

Correctness catchup #1 of the 2:1 cadence (after the q07/294 reach leg). Board: 12 reach, 8 correctness. q10 is
the cleanest catchup — it composes two **already-proven** oracle patterns without a new arithmetic or SQL-surface
policy, and is non-empty at sf=0.01 (the q17 masked-empty trap checked and cleared).

```sql
SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'
  AND l_returnflag = 'R' AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
```

- **q05's grouped revenue** — `SUM(l_extendedprice*(1-l_discount))` = `discounted_price_raw` (raw scale-4 `i128`),
  grouped `BTreeMap`, `ORDER BY revenue DESC` with **tolerated ties**.
- **q02's wide projected-row comparison** — 8 mixed-type columns (Int64 + two Decimals + five Utf8), strict null
  checks, exact full-row equality.
- **No `LIMIT`** (the harness omits the official `LIMIT 20`, like q02) → full-set comparison, no cutoff ambiguity.

## Decision

A strict-only q10 correctness oracle at sf=0.01, harness-only; reuse `discounted_price_raw` + the
`strict_q5_oracle` grouped pattern + the q02 wide-row multiset.

### D1 — `strict_q10_oracle` (independent generator traversal)
```rust
pub struct Q10Row {
    pub c_custkey: i64, pub c_name: String, pub revenue: i128, pub c_acctbal: i128,
    pub n_name: String, pub c_address: String, pub c_phone: String, pub c_comment: String,
}
pub struct Q10Oracle { pub rows: Vec<Q10Row>, pub selected_rows: usize }
```
- `NationGenerator` → `n_nationkey → n_name`; `CustomerGenerator` → `c_custkey → {name, acctbal_raw,
  nationkey, address, phone, comment}`.
- `OrderGenerator` → keep `o_orderdate ∈ [day(1993-10-01), day(1994-01-01))`, map `o_orderkey → o_custkey`
  (date-filtered order map only; not the non-vacuity proof).
- `LineItemGenerator` → keep `l_returnflag == "R"`, join via the filtered order map → customer → nation;
  accumulate `discounted_price_raw(l_extendedprice, l_discount)` (raw scale 4) **by `c_custkey`**, and count
  `selected_rows` only after the lineitem contributes to a group.
- Emit one `Q10Row` per selected customer (copy the customer/nation attributes — they're functionally dependent
  on `c_custkey`).

### D2 — `check_strict_q10` + `compare_strict_q10` + wiring
- **Phase-0 measure + pin the schema.** Strong prior: `c_custkey Int64`, `c_name Utf8`, `revenue
  Decimal128(32,4)` (the same scale-4 revenue as q05), `c_acctbal Decimal128(15,2)`, `n_name`/`c_address`/
  `c_phone`/`c_comment` Utf8. Assert the *measured* types; any name/count/`(p,s)` drift → `Compare`/`Fail`.
- **Non-vacuity:** `selected_rows > 0` **and** `rows` non-empty (reject a hollow pass); if the measured oracle
  naturally has >1 row, pin that stronger guard.
- **Comparator** (answer ⟂ order):
  - schema first; `is_null` rejected on every projected cell;
  - **answer = bijection keyed by `c_custkey`** (unique), checking **every** projected column exactly — exact raw
    `i128` for `revenue` (scale 4) and `c_acctbal` (scale 2), exact `String`/`i64` for the rest; reject duplicate
    emitted `c_custkey`; row counts equal;
  - **order = `revenue DESC` prefix, tie-tolerant** — adjacent rows satisfy `revenue[i] >= revenue[i+1]`; do
    **not** invent a hidden tie-breaker for equal revenue (q05's lesson).
- CLI `correctness --query q10`; `correctness_matrix` renders `q10 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely
1. **Measure the schema** (revenue `Decimal128(32,4)`, `c_acctbal Decimal128(15,2)`, `c_custkey Int64`) — assert
   live, don't bake.
2. **Key by `c_custkey`** (the other 6 GROUP BY columns are FD on it) but **check all 8 projected columns** exactly.
3. **Replicate every filter** — `o_orderdate` range, `l_returnflag='R'`, the 4-table join.
4. **DESC tie-tolerant order**; `is_null` per slot; exact raw Decimal equality (no float/format/tolerance).

## Boundaries
- Strict-only; sf=0.01; **no LIMIT** (full-set). Independent traversal; no `crates/toron` engine change.
  Reachability untouched (q10 stays `pass@execute`).

## Acceptance
- `q10 strict=pass@compare` at sf=0.01; rail → 9; reach + other correctness cells unchanged.
- A **discriminating** suite: reject a wrong `revenue`, a wrong `c_acctbal`, a wrong string attribute, a missing
  row, an extra row, a duplicate `c_custkey`, a NULL in each projected column, and a `revenue`-ascending order
  violation; **accept** equal-revenue tie permutations.

## Forward arc
Rail → 9. **ADR 296 = q20 correctness** (the second vegetable) — the highest-trust catchup: the first oracle to
answer-check a **membership-result set** from the nested-IN + correlated-scalar composition (ADR 285/286). q04
(correlated-EXISTS count) and q07 (volume-shipping grouped SUM) are further candidates.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition (oracle + check + wiring); no `crates/toron` engine change, no Wendelon
/ cross-ring surface.

## Council provenance
3/3 (Claude/Codex/Gemini) — identical design. Folded: Phase-0-measured schema (revenue `Decimal128(32,4)` prior,
assert live); key by `c_custkey` (FD) but check every column; DESC **tie-tolerant**, no hidden tie-breaker;
non-vacuity = contributing `selected_rows > 0` + non-empty rows; no LIMIT (full-set). Reuses
`discounted_price_raw` + the q05
grouped pattern + the q02 wide-row multiset.
