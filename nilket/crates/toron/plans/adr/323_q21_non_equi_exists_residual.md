# ADR 323 — q21 non-equi correlated EXISTS residual (residual-aware keyed semi/anti join)

## Status
Proposed.

## Context

The **last reach red**. q21's measured wall (ADR 317): `unsupported_query_surface@plan` / "unsupported EXISTS
subquery predicate" — the correlated EXISTS/NOT-EXISTS bodies carry a **non-equi outer-reference**
(`l_suppkey <> l1.l_suppkey`) beside an equi one. This is **the final reach engine cut, not a frontend cut**:
greening q21 = full reach (22/22).

```sql
… AND EXISTS     (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
  AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey
                                              AND l3.l_receiptdate > l3.l_commitdate)
```

## The wall (grounded) — why this is an exec cut
`classify_exists_conjunct` (sql_planner:~5695) admits only `InnerResidual` (inner-only) and an **equi**
`CorrelatedPair {outer, inner}` (keys the decorrelated semi/anti join); a non-equi outer-reference is rejected.
The deeper truth: **`on_filter` on `LogicalPlan::Join` is a planning surface, not proof of an executable residual
join.** `on_filter` does not appear in `exec.rs`; the semi/anti exec path (exec.rs:3241/3575) is **key-only**. For
an inner join a residual can lower to a post-join Filter, but **semi/anti joins emit the left side only** — so
q21's cross-side residual `l_suppkey <> l1.l_suppkey` is *not output-filterable* (the right `l2.l_suppkey` is gone
after the join). The residual must participate in the **match predicate itself**:

```
matched = key_matches && residual_is_true(left_row, right_row)
```

## Decision — a residual-aware keyed semi/anti join (D2 is the center)

A `crates/toron` cut spanning classifier → bind → exec:

- **D1 — classification:** add `ExistsConjunct::CorrelatedResidual(predicate)` for a non-equi outer-reference
  (references both outer and inner, not an equi pair, not inner-only). Mirror the same state in the binder-side
  `DeferredExistsConjunct` classifier so qualified and deferred/unqualified EXISTS shapes follow one rule.
- **D2 — the executable residual (the center):** carry/bind a **residual match expression** for keyed
  `LeftSemi`/`LeftAnti` joins, and **evaluate it inside the keyed probe loop** — `matched = key_matches &&
  residual_is_true(left_row, right_row)` — *without* materializing a cartesian or degrading to a full inner join.
  The equi `CorrelatedPair`(s) key the probe (`l_orderkey`); the residual is checked within the matched key group.
  This retargets ADR 308's binder residual gate: side-local ON residuals still push below ordinary joins, but a
  keyed semi/anti cross-side residual becomes a bound join match predicate instead of hitting the current
  "requires side-local predicate" reject.
- **D3 — fixture:** add `o_orderstatus` (q21's `o_orderstatus = 'F'`) **only after** D1/D2 move the `@plan` wall
  and the binder resolves it (the q18/`o_totalprice` pattern).

### NOT EXISTS is the load-bearing correctness case
A left row is excluded **only if** there exists a right row with the **same `l_orderkey` AND `l_suppkey <>
l1.l_suppkey` AND the inner residuals true**. A **key-only anti join is too strong** (it would exclude on the key
alone); a **post-filter is impossible** (right columns are gone). So the residual must compose *inside* the
anti-join match probe. The same optional-residual path must leave the ADR 258/259 NOT-IN null-guard/membership
joins byte-semantic when the residual is absent. Pin q21's `NOT EXISTS` with a **values test against a brute-force
correlated reference**, not just a shape test — a subtle wrong answer hides exactly here.

## Scope / boundaries + telemetry
- **In scope:** a non-equi outer-reference **with ≥1 equi `CorrelatedPair`** (the probe keys on the equi, the
  non-equi is the in-probe residual), for **EXISTS and NOT EXISTS** (q21 has both).
- **Still rejected:** a **non-equi-ONLY** correlation (no equi key → a correlated theta with no hash key = a
  cartesian) — reject with the precise reason **`unsupported_non_equi_exists_correlation`** (folded here, sharper
  than the generic bucket). Correlated scalar / aggregate EXISTS bodies stay rejected.

## Acceptance — verify-the-flip → full reach
- **q21 moves off `unsupported_query_surface@plan`** (with `o_orderstatus`) → keyed semi/anti with the in-probe
  residual `pass@execute` both modes → **reach 22/22 (full reach)**. Or a deeper measured blocker (record + name).
- **Keyed/tractable** at sf=0.01 — the semi/anti joins key on `l_orderkey` (the residual is *in-probe*, never the
  sole key; not a fake-green cartesian — the q16 lesson). Keyed-plan assertion + memory/time.
- **NOT-EXISTS values test** — an EXISTS and a NOT EXISTS each with `(equi-pair AND non-equi-outer-ref [AND
  inner-residual])` match a brute-force correlated reference (the correctness gate, not just shape).
- **`unsupported_non_equi_exists_correlation`** — a non-equi-only EXISTS stays rejected with that exact reason.
- **No regression:** the key-only semi/anti path unchanged for existing equi-only shapes (q04/q22 EXISTS, the
  NOT-IN ADR 258/259 stack); `o_orderstatus` leaves orders-fixture queries + goldens unmoved; q11/q15/q18/q19 +
  correctness unchanged. Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test`.

## Risks
- **D2 correctness (the crux)** — the residual must be evaluated *in the probe*, composing with the anti-join
  match semantics while leaving the residual-absent NOT-IN stack unchanged; the NOT-EXISTS values test is
  mandatory. Key-only = too strong; post-filter = impossible.
- **Keyed-not-cartesian** — ≥1 equi pair required; the residual is in-probe, never the sole key; assert keyed +
  sf=0.01 memory/time.
- **Shared exec semi/anti path** — adding an optional residual must not change the key-only path; full suite is the
  net.
- **Shared `o_orderstatus` fixture** — full `-p toron-tpch` + goldens.

## Forward arc
q21 greens → **reach 22/22 — full reach**. The board becomes reach-complete; open work is the **correctness
backlog** (q11's `* 0.0001` literal-scale, q15/q18/q19/q21 oracles). The non-equi-only-correlation and
grouped-EXISTS shapes wait for a measured row to name them.

## Why toron-home
A `crates/toron` cut (EXISTS-conjunct classifier + bound/exec residual-aware keyed semi/anti probe) + the shared
`o_orderstatus` fixture. q21's row flips in `toron-tpch`. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Codex grounded — and Claude verified (`on_filter` absent from `exec.rs`; semi/anti exec
is key-only) — that the logical `on_filter` is a planning surface, **not** an executable semi/anti residual: the
right side isn't emitted, so the residual must join the **match predicate** (`matched = key_matches &&
residual_is_true`). Claude conceded the "residual slot is enough" framing. **D2 (the in-probe residual) is the
center**; NOT EXISTS is the load-bearing correctness case (key-only too strong, post-filter impossible), and
ADR 308's current residual rejection is the binder guard this cut must explicitly retarget. The final reach engine
cut.
