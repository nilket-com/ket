# ADR 298 — q04 correctness oracle

## Status
Proposed.

## Context

Correctness catchup #1 of the leg (q04 first of the unchecked-green {q04, q07, q08}). Board: 13 reach, 10
correctness. q04 is the cleanest catchup — a COUNT-grouped oracle whose one genuinely new surface is the
**correlated EXISTS** (a semijoin); no Decimal, no division, no derived table, no self-join. Non-empty and
EXISTS-discriminating at sf=0.01 (Codex grounded the data: 582 date-window orders, **535 pass / 47 fail** EXISTS,
all 5 priority groups present).

```sql
SELECT o_orderpriority, COUNT(*) AS order_count FROM orders
WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
  AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority ORDER BY o_orderpriority
```

## Decision

A strict-only q04 correctness oracle, harness-only; reuse the q12 count-grouped pattern (`BTreeMap<String, i64>`
with Int64 counts, schema -> non-vacuity -> bijection -> order). The acceptance/default rail scale is sf=0.01.
The new part is the **EXISTS-as-semijoin**.

### D1 — `strict_q04_oracle`
```rust
pub struct Q04Oracle {
    pub groups: BTreeMap<String, i64>,          // o_orderpriority -> order_count
    pub orders_pass_exists: usize, pub orders_fail_exists: usize,
}
```
1. **Qualifying-order set (EXISTS, one pass):** `HashSet<i64>` of `l_orderkey` for which **≥1** lineitem has
   `l_commitdate < l_receiptdate` (one pass over `LineItemGenerator`).
2. Per `order` with `o_orderdate ∈ [8582, 8674)` (= `[1993-07-01, 1993-10-01)`; the `+ INTERVAL '3' MONTH` folds):
   if `o_orderkey ∈` the set → `groups[o_orderpriority] += 1` (count the **order, once**), `orders_pass_exists += 1`;
   else `orders_fail_exists += 1`.

### D2 — `check_strict_q04` + `compare_strict_q04` + wiring
- **Schema** (Phase-0 measure/pin): `o_orderpriority` Utf8, `order_count` **Int64**; `is_null` per cell.
- **Bijection by `o_orderpriority`**: unique keys; row counts equal; exact `order_count` (Int64); no duplicate.
- **Order = `ORDER BY o_orderpriority`** — **total** order (priorities unique); assert exact ASC (no tie
  tolerance, unlike the revenue-DESC oracles).
- **Non-vacuity:** `orders_pass_exists > 0` **and** `orders_fail_exists > 0` (the EXISTS discrimination — the
  load-bearing guard) **and** `groups` non-empty. The non-vacuity *test* additionally asserts all 5 TPC-H
  priorities (1-URGENT/2-HIGH/3-MEDIUM/4-NOT SPECIFIED/5-LOW), each count > 0 (Codex's grounded completeness).
- CLI `correctness --query q04|q4`; `correctness_matrix` renders `q04 strict=pass@compare`.

## The discipline: the oracle must mirror the executor completely
1. **EXISTS = SEMIJOIN, not a join** — count each *order* **once** if ≥1 lineitem qualifies; **never** per
   matching lineitem. Build the qualifying-*order* set, then count orders (an order with 3 qualifying lineitems
   still counts 1).
2. **EXISTS must be DISCRIMINATING** — `orders_pass_exists > 0` AND `orders_fail_exists > 0` (grounded 535/47), so
   the filter is exercised — else a count-all-orders oracle passes without proving EXISTS (the q12-both-arms /
   q20-threshold lesson).
3. **Measure the count type** — Phase 0 confirms `COUNT(*)` -> `Int64`; the schema helper then pins it.
4. **Total-order check** + `is_null` per cell; the folded date window exactly.

## Acceptance
- `q04 strict=pass@compare` at sf=0.01; rail → 11; reach + other correctness cells unchanged.
- **Discriminating suite:** reject a wrong `order_count`, an **order that fails EXISTS counted** (synthetic: a
  date-window order with no qualifying lineitem must be excluded), a **per-lineitem overcount** (synthetic: one
  order with multiple qualifying lineitems counts 1 — the semijoin tripwire), a dropped/extra/duplicate priority,
  a NULL cell, and an `o_orderpriority` order violation.

## Boundaries
- Strict-only. The default correctness rail runs at sf=0.01; no special q17-style scale policy is needed. Harness
  q04 SQL is unchanged (the `+ INTERVAL '3' MONTH` constant-folds; the oracle uses the folded `[8582, 8674)`
  window — no runtime interval kernel). Independent traversal; no `crates/toron` engine change. Reachability
  untouched (q04 stays `pass@execute`).

## Forward arc
Rail → 11 (q01/q02/q03/q04/q05/q06/q10/q12/q14/q17/q20). Remaining unchecked-green: q07 (grouped revenue +
self-join), q08 (ratio, 8-factor derived) — the next catchups. q09 (substring LIKE) remains a likely reachability
frontier.

## Why toron-home / harness
A `toron-tpch` correctness-rail addition; no `crates/toron` engine change, no Wendelon / cross-ring surface.

## Council provenance
3/3 (Claude/Codex/Gemini), identical design. Codex grounded the data (535 pass / 47 fail, 5 groups) — non-vacuous
+ EXISTS-discriminating; epoch window `[8582, 8674)`. Folded: the EXISTS semijoin (count orders once) + its two
synthetic tripwires (order-once overcount, EXISTS-excludes); non-vacuity = `pass>0` AND `fail>0` (+ 5-group
completeness in the test); `Int64` count measured; total-order `o_orderpriority`. Reuses the q12 count-grouped
pattern.
