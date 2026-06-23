# ADR 327 — q18 correctness oracle

## Status
Proposed.

## Context

Next correctness cut after q15 (correctness 19 → target 20). q18 = Large Volume Customer (the ADR 322
grouped-membership query). The traversal shape is easy; **the real risk is non-vacuity** — at sf=0.01 the live
result may be empty or too thin to prove the `> 300` membership filter, and a live-only answer check would
fake-pass. Phase 0 must measure that first and decide whether synthetic coverage is mandatory.

```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)
  AND c_custkey = o_custkey AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100
```

## Decision — an independent generator-traversal oracle, with mandatory synthetic coverage if the live set is thin

### D1 — the oracle
- Traverse `CustomerGenerator`, `OrderGenerator`, `LineItemGenerator` directly.
- **IN set:** per `l_orderkey`, `SUM(l_quantity) > 300` (the grouped membership; `300` is an Int threshold,
  untouched by ADR 325).
- **Outer:** `customer ⋈ orders ⋈ lineitem` (`c_custkey = o_custkey`, `o_orderkey = l_orderkey`); keep rows whose
  `o_orderkey ∈ IN set`.
- **Group/agg:** `GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)`, `SUM(l_quantity)`, mirroring
  the engine's strict decimal typing for `o_totalprice` and `SUM(l_quantity)` (the q01/q06 precision doctrine).
- **Order/limit:** `ORDER BY o_totalprice DESC, o_orderdate`, then `LIMIT 100`, applied in that exact order.
- **Comparator:** assert schema first; reject NULLs slot-by-slot; and **compare rows by a stable identity key**
  (e.g. `o_orderkey`) so the check is **not brittle on tie rows** — assert the ordering where `(o_totalprice,
  o_orderdate)` is distinct, but within a tie group match by identity (the engine's intra-tie order may not be
  deterministic).
- Output `(c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum)`.

### D2 — non-vacuity + mandatory synthetic coverage
- Measure q18's live strict row count at sf=0.01. If empty or too thin to prove the filter, a **mandatory
  synthetic** fixture is required (the q16 precedent), carrying both:
  - one **passing** order whose lineitems sum `> 300` (must be **included**), and
  - one **control** order whose sum is `≤ 300` (must be **excluded**) — proving the membership filter both ways.
- If the live set is too small to naturally exercise `LIMIT 100`, the synthetic should carry enough qualifying rows
  to exercise the limit path too.

### D3 — canonical wiring (the q15 lesson)
- Add the `correctness --query q18` CLI arm and confirm `correctness-matrix` shows `q18 strict=pass@compare`
  (count **20**). **These canonical gates are part of the seal, not optional** — q15 shipped with a correct oracle
  but unwired; do not repeat. Unit tests alone do not move the board.

## Acceptance
- **q18 strict = `pass@compare`** (correctness **20**) — or an honest, named red.
- **Non-vacuous:** the membership filter proven on a real subset (live), or via the mandatory synthetic (passing-in
  + control-out). A synthetic-only pass must say so.
- Mirrors the engine's `SUM(l_quantity)`/`o_totalprice` decimal precision, `ORDER BY o_totalprice DESC, o_orderdate`,
  `LIMIT 100`, schema, and slot-by-slot null handling; tie rows compared by stable identity.
- **Canonical board:** `correctness --query q18` + `correctness-matrix` green (count 20), run as part of the seal.
- Strict-only canonical row; reach unchanged (22/22); other correctness cells unchanged.

## Risks
- **Vacuity (the big one)** — if no order exceeds 300 at sf=0.01, a live-only oracle is a fake-pass; the synthetic
  (passing-in + control-out) is mandatory.
- **Canonical wiring** — run `correctness --query q18` + `correctness-matrix` in the seal gate (the q15 miss).
- **ORDER/LIMIT/ties** — match the engine's order where keys are distinct; compare tie rows by stable identity;
  exercise LIMIT via the synthetic if the live set is small.
- **Type/precision** — mirror `l_quantity`/`o_totalprice` exactly.

## Forward arc
q18 correctness closes (20). Then q19 (OR-key) and q21 (non-equi EXISTS) oracles — the last two, with the least
obvious result shapes (more upfront care).

## Why toron-home
The oracle (+ any synthetic) lives in `toron-tpch`; the canonical CLI wiring is `main.rs`/`lib.rs`. No engine change
expected (verify-first; a surfaced bug names a follow-on). No Wendelon / cross-ring surface.

## Council provenance
Seeded by Claude iter-0 and grounded by Codex against the local q18 surface. q18 = the ADR 322
grouped-membership query. Load-bearing: **non-vacuity** (live count measured; mandatory synthetic with passing-in +
control-out if thin), the **stable-identity-key tie comparison** (not brittle on `o_totalprice`/`o_orderdate`
ties), and the **canonical wiring** (`correctness --query q18` + `correctness-matrix` part of the seal — the q15
lesson). Strict-only row, mirror-the-executor precision, the anti-fake-pass discipline.
