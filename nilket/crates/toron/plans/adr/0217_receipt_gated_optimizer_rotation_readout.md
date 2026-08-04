# Toron ADR 217: Receipt-Gated Optimizer Rotation Readout

**Status:** Promoted. **Migration:** `plans/impl/0217_receipt_gated_optimizer_rotation_readout.md`.
**Category:** Toron optimizer instrumentation — the first *consumer* of the
207→216 durable evidence substrate. A read-only `information_schema` surface that
turns the durable verdict and decision lanes into explicit next-cut **gate rows**.
No actuation, no planner / resolver / recognizer / margin-selector / fragmenter /
executor / daemon-flush / wire / Wendelon-production change; no new durable key or
counter flush path.

## Why this cut exists

207→216 made the optimizer's two evidence lanes durable and replicated: verdicts
answer *"was the optimizer right?"*, decisions answer *"what did the optimizer
do?"*. Both are read-only — **nothing rotates behavior on them yet**. ADR 216's
forward arc names five candidate rotations (per-kind graph margins, safe-side
outer ordering, decision-conflict/memo pressure, windowing/decay, counter
handoff), scattered across prose and separate tables.

The instrument-first move (Codex iter-0, council-adopted): before building *an*
actuator, build the **cockpit that proves which actuator's signal is actually
ready**. Without it, ADR 218 gets chosen by prose order or a seeded fixture
rather than by a receipt. This continues the established altitude — ADR 213 built
a graph readout before graph application; 214 built outer-ordering receipts
before any outer actuator; 216 built durable decision telemetry and explicitly
deferred consumers. 217 makes the arc's gates **legible and checkable** — the
concrete step toward "memo, now checkable."

## Grounding (verified)

- The optimizer info-schema readout family already exists:
  `SystemTableKind::{InformationSchemaOptimizerDecisions,
  InformationSchemaOptimizerRuntimeVerdicts, InformationSchemaOptimizerMarginAdvisory,
  InformationSchemaOptimizerEvidenceStatus}`, wired in `binder.rs` by
  `bind_information_schema_relation` and rendered via `binder/explain.rs`
  (`information_schema_optimizer_*_schema()`). 217 adds a **consolidating** table
  that interprets these, not a duplicate of their rows.
- `OptimizerRuntimeEvidence` exposes `verdict_snapshot()`, `decision_snapshot()`,
  and `status_snapshot()` (runtime_evidence.rs).
- `margin_advisory_report_rows()` already renders family **and per-graph-kind**
  detail from one verdict snapshot via `is_graph_margin_advisory_kind` +
  `recommend_soft_margin`; floor `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE = 20`
  (join_order.rs:7,189). The graph gate **reuses this math**, not a second policy.
- `PreservedOuterOrder` is a **verdict-lane** kind, excluded from tree/graph
  advisory predicates *and* from `decision_recording_kinds()` — the outer gate is
  a separate family row, never a margin-family row (214 continuity).
- `SystemTableKind` has **exactly 9 variants** today (ir/execution.rs:213); this
  cut adds the 10th, so the `adr102` finiteness + explicit-dispatch guards must be
  retargeted (both exist: nominal_type_identity.rs:2101,2120).

**The computability split (the cut's honest spine — Claude iter-1, council-adopted).**
Of the five gates, only three are computable from receipts that exist today:

| Gate | Signal today | Status it can reach |
|---|---|---|
| `graph_margin_divergence` | per-kind verdicts (floor 20) | up to `actionable` |
| `preserved_outer_ordering` | `PreservedOuterOrder` MissedWin (verdict lane) | up to `actionable` |
| `decision_conflict_pressure` | 216 durable decision lane | up to `actionable` |
| `windowing_decay_pressure` | **no age/saturation substrate** | `inactive` only |
| `counter_handoff_readiness` | **no deployment elasticity signal** | `blocked_external_signal` only |

That asymmetry is the instrument's value: it tells the council 218's actuator
must come from the three live gates, and that windowing and counter-handoff each
need a *substrate* cut before their gate can ever be actionable. A readout that
faked a status for the two dead gates would re-introduce the desire-based
selection this cut exists to kill — so the asymmetry is pinned by a guard.

## Decisions (promotion-locked)

**D1 — One read-only consolidating table: `information_schema.optimizer_receipt_gates`.**
A finite report, not a rules engine; no row drives any planner path. It
interprets the existing verdict/decision/advisory/health readouts into rotation
status; it does not duplicate their raw rows.

**D2 — Static five-row manifest** (stable order): `graph_margin_divergence`,
`preserved_outer_ordering`, `decision_conflict_pressure`,
`windowing_decay_pressure`, `counter_handoff_readiness`. No dynamic registry, no
operator-configured rows, no sibling durable table.

**D3 — One atomic evidence snapshot.** A report-only
`receipt_gate_snapshot()` captures verdict rows + decision rows + evidence health
under a **single** runtime-evidence lock acquisition; the gate builder consumes
that one snapshot. It must not call `verdict_snapshot()`/`decision_snapshot()`/
`status_snapshot()` independently and risk internally inconsistent rows.

**D4 — Stable, report-shaped schema** (textual, for operator/planning review, not
a planner API): `gate_name` Utf8 · `gate_family` Utf8 · `status` Utf8 ·
`sample_count` Int64 · `primary_signal` Utf8 · `secondary_signal` Utf8 · `reason`
Utf8 · `next_cut` Utf8.

**D5 — Status vocabulary, grounded per gate.** `insufficient_evidence` (samples
< floor 20) · `inactive` (evidence present, below the actionability threshold) ·
`actionable` (signal clears the *same* floor + hysteresis the advisory readout
already uses) · `blocked_external_signal` (Toron-actionable shape, but requires an
unbuilt external signal). Gate math, conservative and reusing existing thresholds:
- `graph_margin_divergence` — `actionable` only when **≥2** graph kinds are
  sample-ready *and* their `recommend_soft_margin` recommendations differ (one hot
  kind does not justify per-kind knobs).
- `preserved_outer_ordering` — `actionable` only when the `PreservedOuterOrder`
  verdict slice is sample-ready and the margin math recommends `Loosen`; stays
  separate from tree/graph families.
- `decision_conflict_pressure` — `actionable` only when one `(kind, provenance)`
  decision lane shows both `Reordered` and blocked/near-miss (`KeptThinMargin` +
  `Declined(*)`) above the floor. A memo *discussion* signal, not memo.
- `windowing_decay_pressure` — **always `inactive`** (reason: substrate unbuilt);
  reports cumulative magnitude only, invents no decay algorithm; can never be
  `actionable` in this cut.
- `counter_handoff_readiness` — **always `blocked_external_signal`** (reason:
  elasticity signal unbuilt); renders the Toron-visible state only; Toron must not
  infer elasticity from optimizer counters; can never be `actionable` in this cut.

**D6 — Read-only; planning byte-identical.** No planner, resolver, recognizer,
margin selector, fragmenter, executor, daemon flush, Wendelon facade, or storage
code reads the gate rows. `Manual` and `ApplyAdvisory` planning are byte-identical
with and without the readout query. No new durable key, no new counter, no flush
path — it consumes the 216 substrate.

**D7 — 218's actuator is cited from an `actionable` row, not chosen by prose.**
ADR 218 rotates exactly the gate this readout marks `actionable`, or deliberately
explains why it is doing substrate work instead. If no row is actionable, the
honest next move is substrate maturation (windowing/decay or the elasticity
signal), not an actuator.

**D8 — Rune parity is first-class.** The table is reachable as
`information_schema.optimizer_receipt_gates` through the shared
`bind_information_schema_relation` path; a Rune parity test mirrors
`rune_queries_optimizer_decisions`. Rune is the primary surface; SQL compatibility
is not a substitute.

## Honesty gates

Red on `main` (the table does not exist):
- `optimizer_receipt_gates_projects_five_static_rows` — exactly the five manifest
  rows, stable order.
- `receipt_gates_render_insufficient_evidence_on_cold_ledger` — cold ledger: the
  three computable gates `insufficient_evidence`, windowing `inactive`, counter
  handoff `blocked_external_signal`, **no row `actionable`**.

Load-bearing discriminators (each fails under a plausible-wrong impl):
- `graph_kind_divergence_row_requires_two_ready_distinct_recommendations` — fails
  if one ready kind flips it, or if graph kinds are aggregated.
- `preserved_outer_missed_win_row_is_separate_from_margin_families` — fails if the
  outer signal folds into tree/graph margin scope (214).
- `decision_conflict_pressure_uses_durable_decision_rows` — fails if it reads
  verdicts instead of the 216 decision lane.
- `windowing_and_counter_handoff_never_actionable` — pins D5; fails if a stub
  fabricates a status for the two substrate-blocked gates.
- `receipt_gate_readout_is_read_only_no_plan_change` — Manual + ApplyAdvisory
  byte-identical with/without the query (the read-only pin).
- `receipt_gates_use_single_evidence_snapshot` — fails under per-row re-snapshot.

## Architecture guards (promotion-locked)

- `adr217_receipt_gate_table_is_explicit_information_schema_surface`
- `adr217_gate_rows_are_finite_static_manifest` (exactly the five named)
- `adr217_gate_readout_uses_single_runtime_evidence_snapshot`
- `adr217_graph_gate_reuses_margin_advisory_math`
- `adr217_preserved_outer_gate_is_not_margin_family`
- `adr217_decision_conflict_uses_decision_snapshot_not_verdicts`
- `adr217_windowing_and_counter_handoff_are_substrate_blocked`
- `adr217_no_new_durable_keys_flush_paths_or_wendelon_surface`
- `adr217_readout_is_read_only_no_policy_consumers`

**Reconciled prior guards (named retargets):**
- `adr102_system_table_kind_has_finite_introspection_family` → **9 → 10**, with
  `InformationSchemaOptimizerReceiptGates` explicitly listed.
- `adr102_information_schema_dispatch_is_explicit` → include the new const + branch.
- `adr216_decision_telemetry_is_read_only_no_policy_consumers` → allow the
  receipt-gate **report** as an info-schema readout consumer while continuing to
  ban planner/resolver policy consumers of `decision_snapshot()`/gate rows.
- ADR 213/215 graph-advisory guards → continue to pin **family** graph
  application; the readout may *observe* per-kind divergence but applies no
  per-kind knob.
- ADR 214 outer-family guards → continue to exclude `PreservedOuterOrder` from
  tree/graph advisory scopes; 217 adds a separate gate row, not an advisory scope.
The 207/214/216 verdict+decision matrix-count guards stay (we read, not extend).

## Consensus (transparent)

3-of-3 + operator. **Codex iter-0** reframed away from an actuator to the
instrument (adopted). **Claude iter-0** proposed per-kind graph margins as the
readiest actuator and **conceded** — grounding showed the readout is the correct
altitude; that grounding became the **computability split** (3 computable / 2
substrate-blocked), which **both peers explicitly adopted** ("Claude wins" —
Gemini iter-2; "Claude's iter-1 correction is adopted" — Codex iter-1). **Gemini**
synthesized the table shape and the four-value status vocabulary. **Codex iter-1**
contributed the full guard-retarget set (adr102 ×2, adr216, 213/214) and the
single-locked-snapshot consistency requirement (D3). No residual divergence: the
graph-margin actuator survives as a *row* in the manifest, not as this cut.

## Forward arc

`216 durable telemetry → 217 receipt-gated rotation readout (this) → 218 = the one
actuator an `actionable` row justifies (today: graph-kind margins, preserved-outer
loosen, or decision-conflict/memo) → windowing/decay or elasticity only once a
substrate cut makes their gate computable → memo when the decision-conflict row
goes actionable`.

## Deferred (named)

Every actuator (the readout picks one for 218, builds none); per-kind graph knobs;
safe-side outer ordering; the windowing/decay **substrate** (timestamp/saturation
tracking — prerequisite for that gate); the deployment **elasticity signal**
(prerequisite for counter-handoff); mixed-chain pushdown; memo/Cascades.
