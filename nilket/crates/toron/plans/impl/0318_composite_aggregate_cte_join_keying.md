# ADR 318 — Implementation plan: q15 composite/aggregate CTE join keying

Pairs with `plans/adr/0318_composite_aggregate_cte_join_keying.md`. Engine cut (`crates/toron` binder). **Phase 0
decides B / A1 / A2 from pre-admission evidence before any engine fix is chosen.** Anchors current-tree.

## Phase 0 — ground B / A1 / A2 (NO behavior fix until this names the subcause)
Start with the existing `q15_supplier_revenue_join_is_keyed(strict, 0.01)` to reproduce the bind wall. Because it
can fail before a bound `ExecutionPlan` exists, add a temporary probe or durable diagnostic helper that records the
normalized logical shape after outer-join demotion / cross-equi recognition and before join strategy admission.
Record that diagnostic in the results doc — first-class, not yes/no. Then answer the probes and classify:
| probe | finding | subcause |
|---|---|---|
| `s_suppkey = supplier_no` sits above a `DeferredScalarSubquery`/filter wrapper and never reaches the supplier/revenue Cross | wrapper push-through missing or insufficient | **B** |
| supplier/revenue remains `JoinKind::Cross` with no keys while output includes both `s_suppkey` and `supplier_no`/`total_revenue` | recognizer can't own the composite output column | **A1** |
| normalized shape is keyed, but bind/admission/fragment metadata rejects the composite side | composite input fails keyed-join metadata/admission | **A2** |
Also record: is the `total_revenue = (scalar MAX(revenue))` filter **above or below** the join (the wrapper
position). **Write the named subcause into the results doc before writing behavior-changing engine code.**

## D1 — the fix (exactly one route, chosen by Phase 0)
- **B** — extend `push_deferred_scalar_outer_filter` and any wrapper handling so `s_suppkey = supplier_no`
  reaches the comma-join Cross; existing `lift_cross_equi` keys it. Smallest push-through extension; mirror ADR
  282/311.
- **A1** — make the recognizer's scope/ownership attribute a composite/aggregate output column (`supplier_no`,
  `total_revenue`) to its plan side, so `lift_cross_equi` lifts the equi-key over a non-base input. Recognizer/
  scope change only; do not touch admission.
- **A2** — admit a composite/aggregate side to the **keyed** hash-join path. A keyed join has real keys; the
  keyless-Cross broadcast guard remains for cartesian joins. Smallest admission/fragment-metadata change that lets
  the keyed path build a composite side. Do not touch the recognizer.
- **Do not pre-implement multiple routes.** One measured subcause → one smallest fix. Reconcile any IR-variant /
  join architecture guard the change trips.

## Tests
- **`q15_supplier_revenue_join_is_keyed`** flips to keyed (`Inner`, `left_keys=1 right_keys=1`), not `Cross
  left_keys=0 right_keys=0` — the anti-fake-green-cartesian gate. Update the existing q15 harness assertion to
  require keyed.
- **Engine unit test** for the chosen route: a minimal `base ⋈ (aggregate subplan) ON k=k` (the q15 shape) binds to
  a keyed `Inner` join; revert the fix → it strands/keyless (discriminating). For B, a deferred-scalar wrapper case;
  for A1, a composite-output-column-ownership case; for A2, a composite-build-side admission case.
- **Architecture:** full `architecture_test` (recognizer/admission is shared + guarded).

## Phase 3 — re-measure q15 + the matrix
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q15 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q15 --mode strict --sf 0.01
```
- Expect `pass@execute` both modes (keyed memory/time, not a multi-GB cartesian) → reach 18; or a deeper measured
  blocker (record + name the next ADR). Confirm keyed via the assertion above.
- **Re-run `matrix` + `correctness-matrix`:** q15 flips; **q18/q19 and all others unchanged** unless deliberately
  moved — and do **not** claim q18 benefit unless q18's measured row actually changed.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (shared binder path)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q15 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q15 --mode strict --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0318_composite_aggregate_cte_join_keying_results.md`: **the recorded Phase-0 diagnostic +
the named subcause (B/A1/A2)**; the chosen smallest fix; q15's keyed-plan assertion (before/after `kind`/key-counts);
q15 dev+strict new rows (pass@execute or deeper blocker, with keyed memory/time); the matrix delta (q15 only, or
documented intentional moves); confirmation q18/q19 unchanged unless measured.

## Risk / blast radius
- **Phase-0 discipline** — name B/A1/A2 from the diagnostic *before* engine code; wrong subcause = wrong file (B
  push-through ≠ A1 recognizer ≠ A2 admission).
- **Shared recognizer/admission** — full `--no-fail-fast` + `architecture_test` are the net.
- **Fake-green cartesian** — the keyed-plan assertion (keyed, not Cross) is mandatory; a composite keyed wrong can
  silently cartesian (q16 lesson) — check sf=0.01 memory/time too.
- **No q18 claim** until re-measured; this ADR moves q15 only.

## Seal
`impl: adr 318 composite aggregate cte join keying`. Do not commit wip drafts.
