# Toron ADR 230: Probe-Linearity Dominance Rotation Gate

**Status:** Promoted. **Migration:** `plans/impl/0230_probe_linearity_dominance_rotation_gate.md`.
**Category:** Toron optimizer instrumentation discipline — add one read-only row to the existing
`information_schema.optimizer_receipt_gates` cockpit so ADR 229's probe-linearity evidence acts
as a formal optimizer-rotation gate. **No new information-schema table, `SystemTableKind`, Rune
handle, durable key, daemon flush, or Wendelon surface; no planner / resolver / fragmenter /
executor policy change.** The optimizer arc's actuation-discipline terminus.

## Why this cut exists

ADR 229 made the probe-linearity fence cause durable, recent-window scoped, and visible as a
14-row detail table, with a strict forward rule: relax a fence only if production evidence shows
a *dominant* consumer/fence row; otherwise rotate out. That rule is still **institutional, not
structural** — a future actuation could cherry-pick a seeded detail row, cite the 229 table
directly, and repeat the ADR 226 failure (manufacturing an actionable state no production query
can enter). 230 closes the gap at the established ADR 217 cockpit altitude — the cockpit already
owns optimizer rotation-readiness gates — by adding the sixth finite row
`probe_linearity_fence_dominance`, and by making it the **only** evidence a future
probe-linearity actuation may cite. In dev the row reads `insufficient_evidence` (cold), so no
actuation can fire — exactly correct.

## Grounding (verified)

- `optimizer_receipt_gates` has five rows (`adr217_receipt_gates_are_explicit_five_row_system_table`):
  `graph_margin_divergence` (join_order.rs:678), `preserved_outer_ordering` (797),
  `decision_conflict_pressure` (853), `windowing_decay_pressure` (1254),
  `counter_handoff_readiness` (1275). The gate appends a sixth.
- The receipt-gate status vocabulary is **closed**
  (`adr217_receipt_gate_status_vocabulary_is_closed`): `InsufficientEvidence` / `Inactive` /
  `Actionable` / `BlockedExternalSignal` (join_order.rs:467–470) — the gate reuses it, no new term.
- `receipt_gate_snapshot()` locks runtime evidence **once** for verdicts/decisions/health/window
  (`adr217_receipt_gate_snapshot_is_single_locked_snapshot`); it does not yet carry
  probe-linearity rows.
- ADR 229 owns the input surface: `OptimizerProbeLinearityFenceRow` (`consumer`, `fence`, recent
  /lifetime counts, per-consumer totals, share-per-mille) with status
  `OptimizerProbeLinearityFenceStatus::{Absent, InsufficientEvidence, Observed, Dominant}`
  (runtime_evidence.rs:1433–1450); `probe_linearity_fence_snapshot()` (392) and the private
  `probe_linearity_fence_rows_from_state` (1331) are the builders.
- The existing `OptimizerReceiptGateRow` already has `primary_signal`/`secondary_signal` fields
  (used by the windowing gate); the new gate reuses the row schema — **no schema change**.
- `information_schema.optimizer_receipt_gates` is already first-class in Rune via ADR 228's
  `ctx.optimizer_receipt_gates()` — the gate inherits it; no new handle.

## Decisions (promotion-locked)

**D1 — One sixth receipt-gate row, not a new surface.** `probe_linearity_fence_dominance`
appends after `counter_handoff_readiness` in `optimizer_receipt_gate_rows`, so the prior five
keep their positions. Existing row schema and status vocabulary; no new table/`SystemTableKind`/
durable key/Rune handle.

**D2 — Consume ADR 229 status; do not reimplement dominance math.** The gate derives its status
from the 229 detail rows in the same locked snapshot: `actionable` (≥1 detail row is
`Dominant`); `inactive` (recent evidence exists, none `Dominant` — split/tie/`observed`);
`insufficient_evidence` (no recent sample-ready row); `blocked_external_signal` unreachable for
this gate. ADR 229 remains the sole owner of the floor, strict-majority test, and
consumer/fence manifest.

**D3 — Report the winning production home.** When `actionable`, `primary_signal` names the
strongest dominant pair (`consumer=hash_fallback,fence=sort`); `secondary_signal` carries the
proof (`recent_count`, `consumer_recent_total`, `recent_share_per_mille`, evidence health).
Ties resolve by greater `recent_count`, then by the 229 manifest order — stable.

**D4 — Single locked snapshot.** `OptimizerReceiptGateSnapshot` carries
`probe_linearity_fences: Vec<OptimizerProbeLinearityFenceRow>`, populated **inside**
`receipt_gate_snapshot()` via the same private state-row builder ADR 229 uses
(`probe_linearity_fence_rows_from_state`). The receipt-gate report must **not** call
`probe_linearity_fence_snapshot()` or re-lock runtime evidence — so the summary cannot disagree
with the 229 rows it consumes inside the receipt-gate report.

**D5 — Read-only and policy-free.** No planner / resolver / margin selector / recognizer /
fragmenter / executor / storage / daemon / Wendelon / wire path reads the row; querying records
no evidence and changes no plan bytes.

**D6 — Actuation-boundary discipline (the load-bearing decision).** A future optimizer actuation
that cites probe-linearity **must** be armed by an `actionable` `probe_linearity_fence_dominance`
row derived from the real ADR 229 evidence lane — never a seeded detail row, never
EXPLAIN/counterfactual sampling, never the 229 table cited directly. This makes the 226
production-home lesson **structural** at the actuation boundary, not institutional. Without D6
the gate is a convenience summary; with it, the gate is the arc's honest actuation lock.
Existing probe-linearity classifiers, refusal causes, and committed-decline recording remain
allowed; the discipline applies to positive relaxation/actuation from ADR 229 evidence.

## Schema

`information_schema.optimizer_receipt_gates` schema is **unchanged**; this cut adds one row to
the existing manifest (5 → 6).

## Honesty gates (discriminating)

- `probe_linearity_gate_is_cold_without_evidence` — empty ledger → `insufficient_evidence`.
- `probe_linearity_gate_requires_dominant_real_lane` — seeded helper rows that do not enter the
  durable/runtime evidence path cannot make the gate `actionable`.
- `probe_linearity_gate_selects_strongest_dominant_fence` — one dominant pair → `actionable`,
  `primary_signal` names it.
- `probe_linearity_gate_rejects_split_pressure` — two near-even `observed` causes stay
  `inactive` (no plurality shortcut).
- `probe_linearity_gate_uses_same_locked_snapshot_as_229_detail` — the summary row agrees with
  the snapshot-carried 229 rows it consumes (no second lock / no
  `probe_linearity_fence_snapshot()` in the report path).
- `probe_linearity_gate_is_read_only_no_plan_change`.
- `probe_linearity_gate_has_no_policy_consumers`.

## Architecture guards (promotion-locked)

- `adr230_dominance_gate_is_sixth_receipt_gate_row_reusing_cockpit`
- `adr230_dominance_gate_status_from_real_229_evidence_only`
- `adr230_dominance_gate_uses_single_locked_receipt_snapshot`
- `adr230_no_optimizer_actuation_without_actionable_dominance_gate` (D6 — the load-bearing one)
- `adr230_dominance_gate_has_no_policy_consumer`
- `adr230_no_new_system_table_rune_handle_durable_key_or_wendelon_surface`

**Reconciled prior guards (named):**
- ADR 217 five-row cockpit → **six rows**; status vocabulary + single-locked-snapshot reused
  unchanged.
- ADR 223/224/225 `no_receipt_gate_manifest_change` / no-new-receipt-gate guards → retargeted
  to allow the sixth row while preserving their memo/blocker invariants.
- ADR 222 `windowing_decay_pressure` → unchanged (230 only appends a probe-linearity row; it does
  not restore the pre-222 "windowing always inactive" model).
- ADR 229 fence evidence/detail table → unchanged (the gate is a consumer-side summary).
- ADR 228 `optimizer_receipt_gates` Rune handle → unchanged (the gate row rides it; no new
  handle, finite-eight set intact).
- ADR 216 decision matrix → unchanged at 140/220; ADR 226 zero-admissible / no-generic-search →
  unchanged.

## Consensus (transparent)

3-of-3, after a clean concession cross-over resolved on grounding: Claude iter-1 converged to
Codex's gate; Codex iter-2 conceded toward Claude's DISTINCT — the ADR 094-class swap. The
resolution was a verified fact, not a re-swap: `ExecutionPlan::Aggregate` executes **locally in
Toron** (`exec.rs:759`), so Claude **retracted** the "DISTINCT is necessarily cross-ring"
overclaim (the Wendelon `columnar.rs:1840` path is the pushdown-aggregate optimization, not
general GROUP BY). Both then agreed: 230 = the dominance gate (the small Toron-local
actuation-discipline terminus), DISTINCT = the next feature direction (scope-open, narrow first
cut Toron-local). Codex authored the gate design; Claude contributed the D6 actuation-boundary
guard as its load-bearing reason. No residual divergence.

## Forward arc

```text
229 probe-linearity fence cause readout
-> 230 probe-linearity dominance rotation gate (this; the optimizer arc's actuation-discipline
   terminus — in dev it reads insufficient_evidence, so no actuation can fire)
-> [if production ever shows an actionable dominant fence] one named relaxation, armed only by
   this gate; otherwise the optimizer arc stays rotated-out.
-> next direction: DISTINCT / missing core-query features (a narrow explicit top-level DISTINCT
   is Toron-local — exec.rs:759 groups locally, fence joins for the first cut; scope confirmed
   at the DISTINCT arc's own iter-0, not pre-judged cross-ring).
```

## Deferred (named)

DISTINCT / set operations / window functions (the next feature direction); any fence relaxation
(armed only by an actionable gate); a fence chosen before a dominant row; memo admission
(226-closed); counter-handoff elasticity (externally blocked); probe-linearity window-key GC
(the additive-namespace caveat from 222/229).
