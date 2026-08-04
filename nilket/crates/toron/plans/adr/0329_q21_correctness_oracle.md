# ADR 329 - q21 correctness oracle (series closeout)

## Status
Proposed.

## Context

The finale. q21 = Suppliers Who Kept Orders Waiting (the ADR 323 correlated EXISTS / NOT EXISTS query). Closing it
takes the board to **correctness 22 / reach 22 - the whole TPC-H board green, both rails complete.** It is the
subtlest cell of the series: a supplier qualifies only if it is the **sole late supplier** on a status-F SAUDI
order, with a correlated **EXISTS** proving another supplier is present and a correlated **NOT EXISTS** (non-equi
residual) proving no other supplier is late.

```sql
SELECT s_name, COUNT(*) AS numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS     (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
  AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey
                                              AND l3.l_receiptdate > l3.l_commitdate)
  AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100
```

## Decision - an independent generator-traversal oracle, with a synthetic that proves the anti-join both ways

### D1 - the oracle
- Traverse `SupplierGenerator`, `OrderGenerator`, `LineItemGenerator`, `NationGenerator` directly.
- Group lineitems by `l_orderkey`; per order compute the supplier set and the late-supplier set
  (`l_receiptdate > l_commitdate`).
- A lineitem `l1` contributes iff: `l1` is late; the order has another supplier (some supplier other than `l1`,
  the EXISTS); and **no other supplier is late** (no late supplier other than `l1`, the NOT EXISTS). Restrict to
  status-F orders and SAUDI ARABIA suppliers (`supplier` joined `nation`).
- `numwait[s_name]` = count of contributing `l1` rows; `ORDER BY numwait DESC, s_name`, `LIMIT 100`.
- Output `(s_name: Utf8, numwait: Int64)`. Assert schema first; reject nulls slot-by-slot; compare by stable
  identity (`s_name`) with the `numwait DESC, s_name` order asserted.

### D2 - non-vacuity (on contributing `l1` rows) + the discrimination synthetic
- **Non-vacuity:** count contributing `l1` rows, not output rows. Measure the live count at sf=0.01 (very
  selective - likely thin). **Measure, do not predict** (q19's live set surprised us). If 0, the synthetic also
  covers vacuity.
- **The discrimination synthetic is mandatory regardless of live count** (the q19 lesson): the live data will not
  reliably exercise both the EXISTS and the NOT-EXISTS non-equi residual. It must distinguish, each asserted:
  - **QUALIFY:** SAUDI supplier `S1` late on a status-F order with another supplier `S2` that is on-time -> `S1`
    counted (EXISTS holds via `S2`; NOT EXISTS holds - no other late supplier).
  - **CONTROL, NOT EXISTS fails:** same order but `S2` is also late -> `S1` excluded (proves the non-equi residual
    `l_suppkey <> l1 AND late`).
  - **CONTROL, EXISTS fails:** single-supplier order (`S1` only, late) -> `S1` excluded (proves EXISTS needs another
    supplier).
  - **CONTROL, l1 on-time:** `S1` not late -> excluded (the outer late filter).
  - **CONTROL, wrong nation/status:** non-SAUDI or non-F -> excluded.
  This is what validates the ADR 323 keyed semi/anti probe (key on `l_orderkey`, residual `l_suppkey <>`) - the
  place a dropped NOT EXISTS or a key-only anti join would hide a wrong answer.

### D3 - canonical wiring (the q15 lesson; q18/q19 kept it)
- Add the `correctness --query q21` CLI arm; confirm `correctness-matrix` shows `q21 strict=pass@compare`
  (count **22**). Run these in the seal gate, not just unit tests.

## Acceptance - closes the board
- **q21 strict = `pass@compare`** -> **correctness 22 / reach 22 (full board, both rails green)** - or an honest,
  named red.
- **Non-vacuous** on contributing `l1` rows (live if any, else synthetic); the **discrimination synthetic** proves
  EXISTS + NOT EXISTS both ways (qualify + the four controls), each asserted.
- Mirrors `numwait` (`Int64` COUNT), `s_name` (`Utf8`), `ORDER BY numwait DESC, s_name`, `LIMIT 100`, schema +
  slot-by-slot nullness; tie rows by stable `s_name` identity.
- **Canonical board:** `correctness --query q21` + `correctness-matrix` green (count 22), run as part of the seal.
- Strict-only canonical row; reach unchanged (22/22); other correctness cells unchanged.

## Risks
- **EXISTS/NOT-EXISTS correctness (the crux)** - the synthetic's controls prove the anti-join residual
  (another-late -> excluded; sole-supplier -> excluded). A naive "some supplier waited" synthetic would not catch a
  dropped NOT EXISTS or a key-only anti join (the ADR 323 risk made concrete).
- **Vacuity** - count contributing `l1` rows, not output rows; the synthetic is mandatory for discrimination anyway.
- **Canonical wiring** - `correctness --query q21` + `correctness-matrix` in the seal.
- **ORDER / ties / LIMIT** - `numwait DESC, s_name` is deterministic (s_name tiebreak); keep `LIMIT 100` in the
  official shape.

## Forward arc
q21 closes the board: **correctness 22 / reach 22 - full TPC-H, both rails green.** The series is done; remaining
open items are the deferred follow-ons named along the way (non-equi-only EXISTS, grouped EXISTS, scientific-notation
literal exactness, INSERT VALUES literal preservation) - each waits for a measured need.

## Why toron-home
The oracle (+ synthetic) lives in `toron-tpch`; the canonical CLI wiring is `main.rs` / `lib.rs`. No engine change
expected (verify-first). No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude (3/3 convergent). q21 = the ADR 323 sole-late-supplier query. Load-bearing: non-vacuity
on **contributing `l1` rows** (not output), the **discrimination synthetic** that proves EXISTS + NOT EXISTS both ways
(qualify + four controls, validating ADR 323's keyed semi/anti residual), the `numwait`/`s_name` schema with
stable-identity tie compare, and the **canonical wiring** (the q15 lesson, kept by q18/q19). Strict-only row,
mirror-the-executor, the anti-fake-pass discipline. The last cell.
