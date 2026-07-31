# ADR 318 — q15 composite/aggregate CTE join keying

## Status
Proposed.

## Context

The **first engine cut from the failure atlas**. q15's measured wall (`unsupported_cross_join@bind`) is a
join-keying *substrate* problem: `supplier, revenue` where `revenue` is an aggregate CTE. **The ADR does not assume
the fix.** Phase 0 reads q15's *actual pre-admission join shape* and decides **B / A1 / A2** before any engine fix
is chosen — the difference between a clean substrate cut and wandering join code with a plausible story. Title
stays broad; the impl is precise.

## Phase 0 — decide B / A1 / A2 from pre-admission evidence (load-bearing; before any engine fix)
`q15_supplier_revenue_join_is_keyed` proves the current bind wall, but when q15 hits the keyless-Cross broadcast
guard it can fail before a bound `ExecutionPlan` summary exists. Phase 0 must therefore add a temporary probe or
durable diagnostic helper that records the normalized logical shape after outer-join demotion / cross-equi
recognition and before join strategy admission. Record that diagnostic and answer:
1. Does `s_suppkey = supplier_no` still sit above a wrapper, never reaching the supplier/revenue Cross?
2. Does the supplier/revenue join remain `JoinKind::Cross` with no keys while its output includes both
   `s_suppkey` and `supplier_no` / `total_revenue`?
3. Does the recognizer produce a keyed `Inner` join, but bind/admission/fragment metadata rejects the composite
   `revenue` side afterward?
4. Is the `total_revenue = (scalar MAX(revenue))` filter **above or below** the `supplier`/`revenue` join?

Classify:
- **B — equality stranded above a wrapper.** The `s_suppkey = supplier_no` equality (and/or the scalar-MAX filter)
  sits *above* a `DeferredScalarSubquery`/filter wrapper, never reaching the Cross. → push-through missing/
  insufficient (the ADR 282 path, `push_deferred_scalar_outer_filter`). *Small.*
- **A1 — equality not recognized: composite-side column ownership opaque.** The Cross carries `supplier_no`/
  `total_revenue`, but the recognizer can't attribute `supplier_no` to the `revenue` (aggregate) side, so it never
  lifts the equi-key. → **recognizer / scope ownership over composite outputs.**
- **A2 — equality recognized, then rejected later.** The normalized shape is keyed, but join admission or fragment
  metadata rejects because the composite `revenue` side lacks what the keyed path needs. → **join admission /
  fragment metadata over composite inputs.**

**Hypothesis to confirm, not assume:** A (A1 or A2), since ADR 282 already pushes outer conjuncts through
`DeferredScalarSubquery` (so pure B would likely already key) and the reject is the broadcast-build-side guard. The
Phase 0 diagnostic is the arbiter.

## Decision — cut the smallest keyed-join fix for the measured subcause
A `crates/toron` binder cut, route chosen by Phase 0:
- **B** → extend `push_deferred_scalar_outer_filter` (and any wrapper nesting) so the equality reaches the Cross →
  existing `lift_cross_equi` keys it.
- **A1** → teach the recognizer's scope/ownership to attribute a composite/aggregate output column to its side, so
  `lift_cross_equi` lifts the equi-key over a non-base input.
- **A2** → admit a composite/aggregate build side to the **keyed** join path. A keyed hash join has real keys; the
  single-bucket Cross guard remains for keyless cartesian joins. Smallest change that lets the keyed path build a
  composite side.

**No engine edit until Phase 0 names B/A1/A2.** Do not broaden beyond q15's measured shape.

## Acceptance
- **`q15_supplier_revenue_join_is_keyed` flips green at sf=0.01** — keyed `Inner` join (`left_keys=1 right_keys=1`),
  not `Cross left_keys=0 right_keys=0` (the anti-fake-green-cartesian gate, ADR 311 habit).
- **q15 dev+strict move from `blocked(unsupported_cross_join@bind)` to `pass@execute`** — or to a *deeper measured
  blocker* if keying exposes one (honest red, named → next ADR). Keyed memory/time at sf=0.01 (not a q16-style
  multi-GB cartesian).
- **q18/q19 unchanged unless intentionally moved — do NOT claim q18 benefit until re-measured** (q18's grouped
  membership body is a separate wall; its composite question is downstream of its own cut). Re-run the matrix; any
  flip beyond q15 must be deliberate + verified.
- Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test` green (the recognizer/admission is a
  shared path); other reach + correctness cells unchanged.

## Risks
- **Don't assume A** — the summary decides; B is a much smaller cut, and shipping the substrate fix for a B wall is
  over-engineering. A1 ≠ A2: A1 is recognizer/scope (composite *outputs*), A2 is admission/fragment metadata
  (composite *inputs*) — wrong subcause = wrong file.
- **Substrate breadth (A1/A2)** — touches the shared recognizer/admission; smallest fix q15 needs, full suite as
  the net, keyed-plan assertion proving no fake-green cartesian (a composite keyed *wrong* can silently cartesian,
  the q16 lesson).
- **Scope honesty** — q18's downstream benefit is a hypothesis until re-measured post-q15.

## Forward arc
q15 → keyed-green (reach 18) or a deeper measured blocker. Then q19 (OR-key extraction, on a clearer join-keying
substrate) → q11 (HAVING-scalar) → q18 (grouped EXISTS-body, *then* re-measure the composite path) → q21 (non-equi
EXISTS). If A1/A2 land, composite/derived/aggregate join inputs become keyable engine-wide.

## Why crates/toron
A `crates/toron` binder cut (the join recognizer + admission for non-base/composite inputs, or the deferred-scalar
push-through). q15's row flips in `toron-tpch` with no harness logic change. No Wendelon / cross-ring surface.

## Grounding evidence
q15's current helper proves the measured row (`unsupported_cross_join@bind`) but can stop before an `ExecutionPlan`
summary exists. The binder already has deferred-scalar filter pushdown and cross-equi recognition, and the Cross
strategy guard fails loud when a keyless cartesian has no broadcast-eligible build side. That makes A plausible,
but not proven. Phase 0 therefore captures the pre-admission logical shape and chooses B/A1/A2 from evidence before
the implementation route is selected.
