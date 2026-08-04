# ADR 322 — q18 grouped membership-subquery body

## Status
Proposed.

## Context

The next reach cut before q21. q18's measured wall (ADR 315): `unsupported_query_surface@plan` — the IN
subquery's body is a **grouped aggregate** (`GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`), rejected by the
membership-shape validator. Admitting it (IN-only) then **tests the consolidation hope**: does ADR 318's
A1 substrate already key the composite IN-semijoin?

```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)
  AND c_custkey = o_custkey AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100
```

## The wall (grounded)
IN/membership lowering runs `validate_exists_query_shape` + `validate_exists_select_shape` — **the same validators
EXISTS uses** — then manually lowers only the table factor + a simple-WHERE classifier. Those validators reject a
`GROUP BY`/`HAVING` body with "unsupported EXISTS subquery shape". q18's grouped IN inner → rejected at `@plan`.

## Decision — admit a grouped-aggregate body, IN-only

A `crates/toron` frontend cut. Two load-bearing constraints (Codex's grounding):

### D1 — IN-only admission (do NOT relax the shared EXISTS validator)
Add an **IN-specific** admission (`validate_in_select_shape`, or a narrowly-named caller flag) that admits a
`GROUP BY` + aggregate-vs-literal `HAVING` body for the **IN** path only. The shared `validate_exists_*`
validators **keep rejecting** grouped bodies for the EXISTS path — grouped EXISTS is unmeasured and not needed for
q18; a blanket relaxation would silently widen it. Pin grouped EXISTS as *still rejected*.

### D2 — key the semi-join on the grouped body OUTPUT column
After admitting, lower the grouped body (the existing grouped-aggregate lowering) to a subplan whose output is the
group key, then key the IN-semijoin against **that body-output column** — *not* a base-table-qualified
`lineitem.l_orderkey`, which may no longer be visible after the Aggregate/projection. The current `inner_key`
construction cannot be assumed; the membership key is the body's projected output.

### Fixture
Add `o_totalprice` (from `OrderGenerator`) to the shared orders fixture — **only once D1/D2 clear the `@plan`
wall** (ADR 315 deferred it; the binder then resolves `o_totalprice`). Verify no regression
(q03/q04/q05/q10/q12/q13/q22 + goldens).

## Scope / boundaries
- **In scope:** grouped-aggregate body (GROUP BY + aggregate-vs-literal HAVING) in the **IN membership** path only;
  the body-output semi-join key; `o_totalprice`. Reuse ADR 318's A1 composite scope for the keying.
- **Deferred:** **grouped EXISTS** (strict IN only — if it deserves support, let q21 or another measured row name
  it), grouped **NOT IN**, and correlated / HAVING-scalar membership bodies. A *deeper* composite-semijoin wall
  (if A1 doesn't key it) → its own ADR, not pre-solved here.

## Acceptance — verify-the-flip (q18 may not fully green here)
- **q18 moves off `unsupported_query_surface@plan`** — verify-first re-measure (with `o_totalprice` present):
  - **Greens via A1** (the IN-semijoin keys on the grouped body output, ADR 318's composite scope) → `pass@execute`
    keyed → **reach 21** — confirms the A1 substrate consolidated to q18.
  - **A deeper composite wall** → honest deeper blocker, named → next ADR. Either is a good measured row.
- **Keyed/tractable** at sf=0.01 — the semi-join over the grouped aggregate must be keyed (no fake-green cartesian,
  the q16 lesson). Keyed-plan assertion.
- **No widening:** grouped **EXISTS** and grouped **NOT IN** stay rejected (stay-rejected tests); q04/q22
  EXISTS/NOT-EXISTS unchanged.
- **No regression:** `o_totalprice` leaves q03/q04/q05/q10/q12/q13/q22 + goldens unmoved; q11/q15/q19 + correctness
  unchanged. Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test`.

## Risks
- **Don't widen EXISTS or NOT IN** — IN-only admission (separate validator / narrow flag); the grouped-EXISTS and
  grouped-NOT-IN stay-rejected tests are the guard.
- **The key is the body output, not the base column** — base-table `l_orderkey` may be invisible post-Aggregate;
  key against the body's projected output. Wrong key = a fake-green cartesian or a bind error.
- **The consolidation is a hypothesis** — A1 keying the composite IN-semijoin is the hope, decided by the
  post-admission re-measure; if a deeper wall, name it, don't pre-solve.
- **Shared `o_totalprice` fixture** — full `-p toron-tpch` + goldens.

## Forward arc
q18 greens → **reach 21**, leaving only **q21** (non-equi EXISTS) unreached. If A1 keyed q18's composite semi-join,
the q15 substrate paid off across queries. If a deeper composite wall, that's the next (likely last reach-engine)
ADR before q21. Grouped EXISTS waits for a measured row to name it.

## Why toron-home
A `crates/toron` frontend cut (IN-specific shape admission + body-output semi-join key) + the shared
`o_totalprice` fixture; reuses ADR 318's A1 composite scope + the existing grouped-aggregate lowering / IN-semijoin.
q18's row flips in `toron-tpch`. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Codex's grounding folded: (1) **IN-only** admission — the validator is shared
with EXISTS, so a blanket relax would widen grouped EXISTS (unmeasured); use an IN-specific validator/flag
and pin grouped EXISTS / grouped NOT IN still-rejected; (2) **key the semi-join on the grouped body output column**, not a
base-table `l_orderkey` that may be invisible after Aggregate/projection (don't assume the current `inner_key`);
(3) fork → **strict IN only**. Agreed: `o_totalprice` only after the first wall clears, keyed-plan assertion, and
the post-admission q18 run decides whether ADR 318's composite-input work consolidates here.
