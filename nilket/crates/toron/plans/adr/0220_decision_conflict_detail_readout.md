# Toron ADR 220: Decision-Conflict Detail Readout

**Status:** Promoted. **Migration:** `plans/impl/0220_decision_conflict_detail_readout.md`.
**Category:** Toron optimizer instrumentation — a read-only `information_schema`
surface over the durable decision lane that makes memo pressure legible per group.
No planner behavior change, no policy consumer, no new durable key/counter/flush,
no executor / fragmenter / wire / Wendelon surface. The 216 matrix stays 140/220.

## Why this cut exists

216 made decisions durable; 217 compressed them into one coarse cockpit row,
`decision_conflict_pressure` (strongest `(kind, provenance)` group only); 218/219
consumed the graph and preserved-outer gates. Decision conflict is the remaining
computable 217 gate. The cockpit row says *where* pressure is; it hides the other
groups, one-sided pressure, decline dominance, and the native/demoted split — so
it cannot make **memo** designable. memo's premise ("rotate when durable decisions
show local choices *fighting*") is uncomputable until the fighting is legible per
group. 220 is that legibility, read-only — the last instrument before memo.

## Grounding (verified)

- `information_schema.optimizer_decisions` already exposes raw durable rows
  (`decision_kind, outcome, decline_reason, count, provenance`, explain.rs:262) —
  a grouped copy would be redundant. The readout's value is the **interpretation**:
  the conflict partition + status that raw counts don't express.
- **Toron has no view mechanism** (no `CreateView`/`LogicalPlan::View`;
  information_schema is exclusively `SystemTableKind`). So the reusable readout is a
  new finite system table — and a table (over memo hand-rolling a `GROUP BY`) is
  justified because D4 makes it the single source of truth shared with the 217
  gate, which a per-caller query could silently drift from.
- The 217 conflict model is already encoded in `decision_conflict_pressure_gate`
  (join_order.rs:716) and `strongest_decision_conflict_signal`
  (join_order.rs:770–800): `Reordered → reordered`; `KeptThinMargin |
  Declined(_) → blocked_or_near_miss`; **`KeptOptimal => {}` (neutral)**; floor
  `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE = 20`.
- `decision_recording_kinds()` returns **7** kinds (PreservedOuterOrder excluded);
  with `{Native, Demoted}` that is a fixed **14-row** manifest.
- `receipt_gate_snapshot()` already provides decisions + health under one lock.
- `SystemTableKind` has **10** variants today; this adds the 11th (both adr102
  guards retarget).

## Decisions (promotion-locked)

**D1 — One read-only table: `information_schema.optimizer_decision_conflicts`.** It
reports the interpreted conflict model per `(decision_kind, provenance)`; a sibling
of `optimizer_receipt_gates`, not a replacement for the five-row cockpit.

**D2 — Static 14-row manifest.** Every recording kind × `{native, demoted}`,
including cold zero rows; stable order `decision_recording_kinds()` then
`native, demoted`. `PreservedOuterOrder` absent (verdict-only, 216).

**D3 — Conflict model exactly matches the 217 gate.** `reordered_count = Reordered`;
`blocked_or_near_miss_count = kept_thin_margin_count + declined_count`;
`KeptOptimal` reported as a diagnostic count but **neutral** for conflict pressure;
readiness threshold is the floor (20), not `>0`.

**D4 — One shared typed aggregation (the load-bearing pin).** Promote the private
grouping into `decision_conflict_reports(&OptimizerReceiptGateSnapshot) ->
Vec<DecisionConflictReport>`. The table renders all 14 reports; the 217
`decision_conflict_pressure` row **selects its strongest report from the same
vector**. A guard asserts the cockpit row equals the max-conflict detail row — no
duplicate threshold, no comparator drift. This is also what forces the model to
match 217 (rejecting an alternate KeptOptimal/threshold definition by construction).

**D5 — Table-local status vocabulary.** Exactly one of `cold`, `inactive`,
`one_sided_reordered`, `one_sided_blocked`, `conflicted` (both sides ≥ floor). The
one-sided/inactive states are the readout's "why *not* memo yet" signal.

**D6 — Decline reasons visible without row explosion.** `optimizer_decisions`
remains the per-reason source of truth; this readout reports aggregate
`declined_count` + the group's top decline reason/count (stable enum tie-break) —
14 rows, not 14×7.

**D7 — One snapshot, no verdict inference.** Consumes one `receipt_gate_snapshot()`
(decisions + health); no per-row `decision_snapshot()`, no conflict derived from
verdict rows. Planning byte-identical (`Manual` and `ApplyAdvisory`).

**D8 — No behavior rotation; no durable surface.** No resolver/recognizer/actuator/
margin-selector/memo reads this table or its helper (the only non-table consumer is
the 217 cockpit selector). No new durable key/counter/flush; 216 matrix stays
140/220; the 217 five-row cockpit manifest unchanged.

**D9 — Rune parity.** Reachable through `bind_information_schema_relation`; SQL and
Rune produce identical rows.

## Schema (column order locked)

```text
decision_kind Utf8 · provenance Utf8 · status Utf8 · reordered_count Int64 ·
kept_optimal_count Int64 · kept_thin_margin_count Int64 · declined_count Int64 ·
blocked_or_near_miss_count Int64 · total_count Int64 · conflict_strength Int64 ·
top_decline_reason Utf8(nullable) · top_decline_count Int64 · evidence_health Utf8 ·
reason Utf8 · next_cut Utf8
```
`conflict_strength = min(reordered_count, blocked_or_near_miss_count)`;
`total_count = reordered + kept_optimal + kept_thin_margin + declined`.

## Honesty gates (discriminating)

- `decision_conflict_gate_and_detail_share_strongest_report` — 217 cockpit row ==
  max-conflict detail row (the D4 invariant).
- `decision_conflict_keptoptimal_is_neutral_not_blocked` — ready `KeptOptimal` never
  makes a row `conflicted` (fails under the rejected definition).
- `optimizer_decision_conflicts_projects_static_14_rows` — cold ledger → 14 rows,
  stable order.
- `decision_conflict_detail_marks_conflicted_only_when_both_sides_ready` — floor,
  not `>0`.
- `decision_conflict_detail_is_interpretation_not_decision_count_reprojection` —
  exposes status + `blocked_or_near_miss_count` + `conflict_strength`.
- `decision_conflict_detail_preserves_top_decline_reason`.
- `decision_conflict_detail_ignores_verdict_rows`;
  `preserved_outer_order_absent_from_decision_conflict_detail`.
- `decision_conflict_readout_is_read_only_no_plan_change`;
  `rune_queries_optimizer_decision_conflicts`.

## Architecture guards (promotion-locked)

- `adr220_decision_conflict_table_is_explicit_information_schema_surface`
- `adr220_decision_conflict_rows_are_static_14_row_manifest`
- `adr220_conflict_model_matches_217_gate` (Reordered vs KeptThinMargin+Declined;
  KeptOptimal neutral; floor 20)
- `adr220_conflict_gate_and_detail_share_one_aggregation`
- `adr220_conflict_detail_uses_decision_rows_not_verdicts`
- `adr220_no_new_durable_key_counter_flush_or_evidence_matrix`
- `adr220_decision_conflict_readout_is_read_only_no_policy_consumers`
- `adr220_windowing_and_counter_handoff_stay_substrate_blocked`

**Reconciled prior guards (named):**
- `adr102` finiteness + explicit-dispatch → **10 → 11**, include
  `InformationSchemaOptimizerDecisionConflicts` + its branch.
- ADR 217 five-row cockpit manifest **unchanged**; only the
  `decision_conflict_pressure` implementation is retargeted to select from the
  shared report vector.
- `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` → **unchanged and
  asserted unchanged** (PreservedOuterOrder stays out; 140/220 intact).
- ADR 219 decision-conflict **non-consumer** guard → allow exactly this read-only
  detail readout + the 217 cockpit selector; resolver/graph/outer policy still
  forbidden.

## Consensus (transparent)

3-of-3 + operator. Claude raised the redundancy trap (interpretation, not a grouped
re-projection) and resolved the view question by grounding (no view surface → a new
`SystemTableKind`, justified by the single-source pin). Codex contributed the
static 14-row manifest, the top-decline finitization, the locked schema, and the
cockpit-equals-max-of-detail invariant. The one earlier divergence — Gemini iter-1's
`KeptOptimal`-as-conflict-side + `>0` threshold — was resolved against the existing
217 code (join_order.rs:770–800) and the floor discipline; Gemini iter-2 adopted the
217 model. No residual divergence.

## Forward arc

`220 decision-conflict detail readout (this) → memo/Cascades only when a specific
(kind, provenance) row is `conflicted`, sample-ready both sides, stable under
evidence health, with an explicit policy contract → wider safe-side outer shapes /
windowing-decay substrate / elasticity as their gates become computable`.

## Deferred (named)

Memo/Cascades and any decision-driven policy (220 makes it designable — the next
real fork); wider safe-side outer shapes; windowing/decay durable-age substrate;
deployment elasticity signal; mixed-chain pushdown.
