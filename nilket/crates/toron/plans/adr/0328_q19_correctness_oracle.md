# ADR 328 - q19 correctness oracle

## Status
Proposed.

## Context

Next correctness cut after q18 (correctness 20 -> target 21). q19 = Discounted Revenue (the ADR 319 OR-key query):
`SUM(l_extendedprice * (1 - l_discount))` over `lineitem join part` where the row matches **any of 3 disjunctive
branches**. A **scalar SUM - always exactly one output row.** So the vacuity check cannot be on output rows; it must
count **qualifying *input* rows**. The filter is brutally selective (3 specific brands x containers x narrow
quantity ranges x AIR shipmodes x DELIVER IN PERSON), so at sf=0.01 the live qualifying-input set is almost
certainly **empty -> `revenue = NULL` -> `NULL == NULL` fake-pass**. A mandatory synthetic is the likely path, and it
must exercise the disjunction.

## q19 shape (grounded, lib.rs:164)
Three OR branches, each `p_partkey = l_partkey AND brand AND container IN (...) AND l_quantity in [lo, lo+10] AND
p_size BETWEEN 1 AND s AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'`:
| branch | brand | containers | qty | size |
|---|---|---|---|---|
| 1 | Brand#12 | SM CASE/BOX/PACK/PKG | 1-11 | 1-5 |
| 2 | Brand#23 | MED BAG/BOX/PKG/PACK | 10-20 | 1-10 |
| 3 | Brand#34 | LG CASE/BOX/PACK/PKG | 20-30 | 1-15 |

## Decision - an independent generator-traversal oracle; vacuity measured on qualifying input rows

### D1 - the oracle
- Join `lineitem join part` on `p_partkey = l_partkey`; a row qualifies if it matches **any** branch's full conjunction
  (brand + container-set + quantity-range + size-range + shipmode-set + shipinstruct). All literals are
  strings/integers - ADR-325-unaffected.
- `revenue = SUM(l_extendedprice * (1 - l_discount))` over qualifying rows - **reuse the q15 product precision**
  (`Decimal128(32,4)`), a single scalar.
- **Empty/NULL:** SUM over zero qualifying rows is SQL `NULL`; the oracle + comparator agree on the empty case - but
  a NULL/0 result is **not** a meaningful check (see vacuity).
- Comparator: assert the single-column `Decimal128(32,4)` schema; compare the raw value + nullness exactly.

### D2 - vacuity is on qualifying INPUT rows (Codex's correction)
- q19's output is **always one row** (scalar SUM), so output-row count proves nothing. **Measure the count of
  qualifying input rows** (lineitem/part rows that match a branch). Assert **>=1** - else the SUM is over the empty
  set and the green is vacuous (`NULL == NULL`).
- Measure that count on the **live** sf=0.01 set; almost certainly 0 -> a **mandatory synthetic** is required.

### D3 - the synthetic must prove the disjunction
- one qualifying row per **branch** (1, 2, 3) -> all included, contributing known revenue;
- **per-dimension control rows** that miss by exactly one predicate (wrong brand / wrong container / quantity just
  out of range / size out of range / wrong shipmode / wrong shipinstruct) -> each **excluded**.
- This catches an engine that drops a branch, over-includes (ignores the OR-residual after keying on
  `p_partkey = l_partkey`), or fumbles a range boundary - i.e. it validates the ADR 319 OR-key extraction, not just
  that *some* filter ran. A naive "any matching row" synthetic wouldn't.

### D4 - canonical wiring (the q15 lesson; q18 got it right)
- Add the `correctness --query q19` CLI arm; confirm `correctness-matrix` shows `q19 strict=pass@compare`
  (count **21**). Run these in the seal gate, not just unit tests.

## Acceptance
- **q19 strict = `pass@compare`** (correctness **21**) - or an honest, named red.
- **Non-vacuous on qualifying input rows (>=1):** live if sf=0.01 has matches (unlikely), else a **mandatory
  synthetic** with per-branch inclusions **and** per-dimension exclusions. A synthetic-only pass must say so.
- Mirrors the engine's `Decimal128(32,4)` revenue precision; exact value + nullness; single-row scalar schema.
- **Canonical board:** `correctness --query q19` + `correctness-matrix` green (count 21), run as part of the seal.
- Strict-only canonical row; reach unchanged (22/22); other correctness cells unchanged.

## Risks
- **Vacuity, measured wrong** - q19 always returns one row; assert **qualifying-input-row >= 1**, never output-row
  count. The selective filter makes the synthetic almost certainly mandatory.
- **Disjunction correctness** - the per-dimension control rows are what prove the OR-key extraction (range
  boundaries + the shared shipmode/shipinstruct); a naive synthetic wouldn't.
- **Canonical wiring** - `correctness --query q19` + `correctness-matrix` in the seal (the q15 miss).
- **Precision / empty-SUM** - `Decimal128(32,4)`; SUM over empty is NULL - agree on it, but never let a NULL result
  be the "pass".

## Forward arc
q19 correctness closes (21). Then q21 (non-equi EXISTS) - the last oracle, with the subtlest result (the correlated
NOT-EXISTS exclusion).

## Why toron-home
The oracle (+ synthetic) lives in `toron-tpch`; the canonical CLI wiring is `main.rs`/`lib.rs`. No engine change
expected (verify-first). No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Codex's correction folded: q19 is a **scalar SUM** (always one output row), so vacuity
must be measured on **qualifying input rows** (>=1), not output count. Load-bearing: input-row non-vacuity, the
**disjunction-proving synthetic** (per-branch in + per-dimension out, validating ADR 319's OR-key extraction), the
`Decimal128(32,4)` comparator (reused from q15), and the **canonical wiring** (q18's discipline). Strict-only row,
mirror-the-executor precision, the anti-fake-pass discipline.
