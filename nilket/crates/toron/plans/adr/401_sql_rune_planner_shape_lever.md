# ADR 401 - SQL/Rune planner-shape lever (q03)

## Status
Proposed.

## Context

400's fork verdict is the **SQL/Rune planner-shape lever**. SQL q03 executes ~2.9x slower than the hand-authored Rune
shape (Toron SQL 29.4x DuckDB vs Rune 10.1x) - a **user-visible gap on the default path real users run**. **401
diagnoses the SQL-vs-Rune bound-plan shape delta and fixes it only if it maps to a generic planner rule** - no q03
special case.

**Grounded premise (not "add join ordering"):** Toron already has join-order machinery (`binder/join_order.rs`, the
soft-margin ladder `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP` / `join_order_edges` / `JoinStrategyIntent`), and Rune q03 is a
real hand-authored logical shape via `auto_join` (toron-tpch lib.rs:467). So the delta is the existing SQL path's
*choice*; Phase 0 diagnoses **why the SQL path lands on a slower shape**.

## Decision

### Phase 0 - persisted-path shape-delta table with layer attribution + optimizer evidence
1. **Persisted sf=1 isolation proof** - the 391 `rune_q03_isolation_proof` uses `build_perf_query_spec`, **not** the
   persisted data-source path (lib.rs:3356). 401 is timed on persisted sf=1, so Phase 0 **adds a persisted isolation
   proof OR proves the memory and persisted bound plans are identical** - the shape table must describe the *timed* path.
2. **Shape-delta table with a layer column** - per aspect, record SQL vs Rune and **where the delta is introduced**:

| aspect | SQL q03 | Rune q03 | differs? | layer introduced |
| --- | --- | --- | --- | --- |
| join order | | | | SQL lowering / binder join-order policy / pushdown / physicalization |
| build side per join | | | | |
| filter placement | | | | |
| projection timing | | | | |
| exchange placement | | | | |

The layer matters: a **SQL-lowering** fix leaves Rune unchanged; a **binder-policy** fix may touch **both** SQL and Rune
unless carefully fenced (so it can break the Rune control).
3. **Optimizer evidence, not just tree shape** - since the soft-margin ladder is the mechanism, record its available
   inputs (per-relation row/byte/fact estimates) and the **decision/reason row** where possible. **If the policy lacks
   post-filter cardinality or multi-relation cost evidence, that is a cost-model gap -> stop-line.**

### Diagnosis
Classify the delta (join order / build side / filter placement / projection timing / exchange placement / combination)
and the fixability: existing policy has the info and picks wrong (**-> generic rule fix**) vs lacks the info/cost model
(**-> cost-model gap, stop-line**).

### The cut - generic planner rule only, or stop-line
- **Generic rule:** fix the rule (soft-margin ladder ordering / build-side / placement) so SQL picks the Rune-equivalent
  shape. Generic; no q03 branch. If it's a **binder-policy** layer, **fence it so the Rune control is not altered** (or
  accept a documented, correctness-neutral Rune change and re-baseline the control).
- **Stop-line:** if the delta needs a cost model / info the planner lacks, **seal a diagnostic** with the named delta +
  fixability gap - no q03-shaped heuristic.

## Acceptance
- **Phase 0 recorded on the persisted path:** the shape-delta table (with layer attribution) + optimizer-evidence rows
  (soft-margin inputs + decision/reason), current bound fingerprints (re-bound, not old 393/397 values).
- **Either** a generic planner-rule fix **or** a sealed diagnostic (stop-line) with the named delta + cost-model gap.
- **Shape closure (not only speedup):** if a fix lands, SQL's bound shape becomes **Rune-equivalent for the classified
  delta** (fingerprint/shape convergence on that aspect), or any remaining structural difference is explicitly explained.
  A speedup alone is insufficient - it could be unrelated substrate or noise.
- **SQL q03 correctness unchanged** (oracle/order).
- **Primary: SQL q03 sf=1 absolute median improves beyond the runs=7 band AND `SQL/Rune` closes materially toward 1x**
  (both Toron rows stable -> trustworthy). **Rune unchanged as the control** (or a documented, re-baselined change if a
  fenced binder-policy fix necessarily touches it).
- **No regression on q05/q10-style join shapes** - generic rule; re-run their correctness + a shape check; ideally they
  improve.
- **Full engine gate:** `--no-fail-fast` + `toron-tpch --no-fail-fast`; no cap bump; de-branded; no committed
  `.perf.data`.
- **Scope honesty:** improves the default SQL path; **not expected to make sf=10 heavy cells complete** (the >360s reach
  is the execution-model thread).

## Guardrails
- **Ground the existing policy** - diagnose the soft-margin ladder's choice, don't add ordering from scratch.
- **Timed-path proof** - persisted-path isolation, not the memory-spec helper, or prove they're identical.
- **Layer attribution** - know the fix locus; a binder-policy fix that moves Rune must be fenced or the control
  re-baselined.
- **Shape closure over speedup** - the SQL shape must converge to Rune on the delta, not just get faster.
- **Generic rule or diagnostic (stop-line)** - no q03-shaped planner hack; cost-model gap -> named residual.
- **SQL/Rune primary (both Toron, stable); no sibling regression; full engine gate; de-branded.**

## Risks
- **Memory vs persisted bound plans differ** - Phase 0 proves/uses the persisted path; else the shape table is wrong.
- **Binder-policy fix moves Rune** - fence it, or re-baseline the control with a documented reason.
- **Cost-model gap** - stop-line: named diagnostic, defer the cost-model arc.
- **Speedup without shape closure** - rejected; the acceptance requires shape convergence.
- **Sibling regression** - generic-rule + q05/q10 no-regression + full gate.

## Forward Arc
401 closes (or names) the SQL/Rune shape gap - the user-visible default-path win, ideally generalizing to q05/q10. Then:
the substrate residual (near the stop-line), q08's `reoriented join build accumulator` wall (398), and the big rock - the
**execution-model / streaming-completion** change for the >360s heavy cells. Article-grade throughout.

## Grounding evidence (file:line)
join-order machinery: `binder/join_order.rs` (soft-margin ladder `CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP`,
`join_order_edges`, `JoinStrategyIntent`). Rune q03 hand-authored `auto_join`: toron-tpch lib.rs:467. isolation proof uses
`build_perf_query_spec` (memory), lib.rs:3356 - needs a persisted variant. SQL/Rune ~2.9x (399 sf=1). 397 pushdown generic
(both paths). 400: sf=10 heavy cells unmoved (>360s). value gate 395; perf rail 356.
