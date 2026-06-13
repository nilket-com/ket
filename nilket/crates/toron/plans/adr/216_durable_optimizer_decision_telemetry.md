# ADR 216: Durable Optimizer Decision Telemetry

**Status:** Promoted. **Migration:** `plans/impl/216_durable_optimizer_decision_telemetry.md`.
**Category:** Toron optimizer instrumentation — completes the durable evidence substrate 207 began.
No actuation, no policy consumer, no planner behavior change, no executor / fragmenter /
storage-engine / wire / Wendelon-production surface change (reuses the 207 counter facade).

## Why this cut exists

207→215 made the **verdict** lane (was the optimizer *right*?) durable, replicated, family-scoped,
and applied. The **decision** lane — what the optimizer *did*: Reordered / KeptOptimal /
KeptThinMargin / Declined-with-reason — is still process-local `Mutex<BTreeMap>` in
`OptimizerDecisionLog`, 207's named deferral, untouched for nine cycles, and it dies with the
coordinator. The arc's end-boss gate is **memo, "only when receipts show local decisions
*fighting*"** — a decision-lane question, uncomputable from verdicts, today **inexpressible** (the
word 213 used for outer-ordering before 214 built its receipts). This cut is the instrument that
makes that gate checkable — the 213/214 pattern, read-only, no consumer.

## Grounding (verified)

- Decisions record at `join_order.rs:3573` (`self.decision_recorder.record(...)`) at **bind time,
  always-on** — *not* gated by `measured_accrual_sample_rate`. The verdict lane is sampled (default
  off). This asymmetry is the cut's central design constraint.
- `OptimizerDecisionKind::PreservedOuterOrder` (decision_log.rs:19) is a **verdict-only kind
  sharing the enum** — it reaches the counterfactual verdict emitter (explain.rs:699), never the
  decision recorder (214: observe-only). `Overflow` is a defensive map-bound sentinel, not a real
  kind. So the durable decision matrix's kind basis is the seven recording kinds, not the nine enum
  members.
- The decision outcome space: `Reordered | KeptOptimal | KeptThinMargin | Declined(reason×7)`.
- `information_schema.optimizer_decisions` already exists (kind / outcome / decline_reason / count).
- The daemon owns a 60s `OPTIMIZER_EVIDENCE_REFRESH_INTERVAL` task (daemon.rs:306) — the existing
  background evidence task this cut extends from refresh to refresh-plus-flush.

## Decisions (promotion-locked)

**D1 — Durable decision lane on the 207 substrate; reuse the existing table.** The decision lane
moves into the `OptimizerRuntimeEvidence` handle's pending/cache/durable model;
`information_schema.optimizer_decisions` is retargeted to the durable snapshot — first four columns
(`decision_kind`, `outcome`, `decline_reason`, `count`) unchanged in order, `provenance`
tail-appended (D3). No sibling table, no new `SystemTableKind` (adr102 unchanged).

**D2 — Structured decision keys; decline-reason is key material.** A decision key distinct from the
verdict key (`OptimizerRuntimeDecisionKey { kind, outcome, lane }`, or an
`OptimizerEvidenceKey::{Verdict, Decision}` wrapper — impl choice, either satisfies the
"structured-not-strings" guard). Durable spelling: `optimizer_evidence/decision/{kind}/{outcome}`
and `…/{kind}/demoted/{outcome}`, where a decline's `{outcome}` is `declined/{reason}` — the reason
is preserved, never flattened to one `declined` bucket.

**D3 — Total/Demoted provenance, dual-write, selected at BIND ENTRY (Codex iter-3 correction).**
Mirrors 214: a decision on a demoted binding increments Total + Demoted; native increments Total
only; rows derive Native = Total − Demoted. **But the selection point differs from verdicts**:
verdict provenance (214) inspects the completed render trace *after* bind; decisions are emitted
*during* bind, so the lane must be chosen *before* binding the normalized plan — run outer-join
demotion at bind entry, compute Native/Demoted from whether demotion receipts exist, then bind with
a provenance-bearing live recorder. Decisions are never reclassified post-hoc (that would require
buffering outside the recorder contract or misclassifying early decisions).

**D4 — Periodic background delta-flush, NOT per-query (the load-bearing constraint).** The
always-on `record()` stays exactly as cheap as today — a local pending-delta increment, zero
Wendelon contact — so planning is **byte-identical and free of storage latency**. Durability arrives by
flushing the accumulated decision delta on the existing 60s daemon interval (extending the refresh
task), *not* the coordinator post-success path. The alternative (flush touched decision keys
post-success) imposes a Wendelon `counter_fetch_add` round-trip *per query* for an always-on lane —
a latency regression the verdict lane is spared only because it is sampled. The verdict
post-success flush is unchanged; the two lanes touch disjoint key spaces (no double-flush). Local
reports include pending deltas immediately. **Durability tradeoff, stated:** a crash loses up to one
interval of un-flushed decision deltas — acceptable for telemetry (memo reads trends, not exact
counts) and strictly better than today's total loss on process exit.

**D5 — The NoOp recorder gate survives.** `OptimizerDecisionRecorder` + `NoOpDecisionRecorder`
stay: the live recorder delegates to the durable handle (carrying bind-entry provenance), the ZST
still discards scratch-recognition, counterfactual, and declined-re-bind artifacts. Committed-only
is preserved structurally; the existing committed-not-scratch tests become durable tests.

**D6 — Finite rectangular matrix: 140 decision keys, 220 evidence keys (Codex iter-3, grounded).**
The durable decision matrix is the seven recording kinds (`StarCandidate`, `FlatStar`,
`SnowflakeRoot`, `SnowflakeBranch`, `GalaxyRoot`, `BridgeRoot`, `BridgeChainRoot`) × ten outcomes
(`Reordered`, `KeptOptimal`, `KeptThinMargin`, plus the seven decline reasons) × {Total, Demoted} =
**7 × 10 × 2 = 140**; combined with the 80 verdict keys, the evidence keyspace is **220**. Some
kind/outcome cells are unreachable today — accepted, mirroring the verdict lane's fixed-matrix
discipline (absent → 0). The guard pins both the **count (140/220)** AND the **exclusion**
(`PreservedOuterOrder` and `Overflow` are not in the recording kind set, verified against the
recorder call graph — so the number cannot be reached by including the wrong kind). `Overflow`
remains a legacy process-local diagnostic outside the durable matrix, or is retired by named
retarget.

**D7 — Read-only completion.** No margin resolver, graph/outer gate, memo rule, or advisory reads
decision telemetry in this cut. It is the instrument; consumers are later cuts.

**D8 — Shared health; boundaries.** Decision counters share the verdict counters' durable
mode/state, replication-unsupported latch, refresh, and dropped/ambiguous-delta semantics; the
status row's pending/dropped/ambiguous counts become evidence-wide, not verdict-only — no second
health table. No planner behavior change; no executor / fragmenter / storage / wire /
Wendelon-production surface.

## Honesty gates

Red on `main` (decisions are process-local today):
- `durable_optimizer_decisions_survive_coordinator_restart` — a fresh coordinator on the same
  durable fixture, after the periodic flush, reads pre-restart decision rows.
- `cross_coordinator_decisions_are_shared_after_refresh`.

**Load-bearing discriminators:**
- `decision_record_is_latency_identical_no_per_query_flush` — **D4's pin**: no decision
  `counter_fetch_add` on the bind/coordinator-success path (fails under a per-query-flush impl).
- `demoted_decision_dual_writes_total_and_demoted_sibling` — D3, with provenance selected at bind
  entry (fails under post-hoc classification when an early decision is misattributed).
- `native_decision_rows_are_total_minus_demoted` — provenance derivation.
- `decline_reason_is_preserved_in_durable_key` — D2 (fails if declines flatten).
- `preserved_outer_order_records_no_durable_decision` — the matrix-exactness pin.
- `scratch_bind_decisions_do_not_reach_durable_lane` — the NoOp gate.
- `optimizer_decisions_table_appends_provenance_only` — column-order stability.
- `manual_planning_byte_identical_with_durable_decision_telemetry` — no behavior change.
- `decision_telemetry_survives_durable_refresh`; `rune_queries_optimizer_decisions` — round-trip +
  parity.

## Architecture guards (promotion-locked)

- `adr216_decision_telemetry_uses_existing_optimizer_decisions_table`
- `adr216_decision_keys_are_structured_not_strings`
- `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` (count + the PreservedOuterOrder /
  Overflow exclusion, call-graph-verified)
- `adr216_decision_flush_is_periodic_not_per_query`
- `adr216_decision_provenance_is_bind_entry_not_posthoc`
- `adr216_noop_recorder_still_gates_scratch_decisions`
- `adr216_decision_telemetry_is_read_only_no_policy_consumers`
- `adr216_no_executor_fragmenter_storage_wire_or_wendelon_surface`

**Reconciled prior guards (named):** ADR 187's bounded-decision-log guard → durable finite-matrix
enumeration (the dynamic-map overflow reason retired); ADR 187's decision-log-storage guard →
durable handle + recorder trait; ADR 187's information-schema guard → same table + `SystemTableKind`,
durable row source, tail provenance; ADR 188/207 "verdict-only evidence" wording →
verdict-plus-decision without changing verdict key spelling or verdict flush semantics; ADR 214
provenance continuity → same Total/Demoted derivation, selected at bind entry for decisions. The
207/214 verdict matrix-count guards stay verdict-scoped. adr102 unchanged.

## Consensus (transparent)

Topic 3-of-3 (Branch B, after Claude's iter-1 correction that outer ordering is self-arming-inert
but carries a real semantic-preservation cost — deferred absent a live signal). Design 3-of-3:
Codex iter-2 did the heavy framing (existing-table reuse, structured keys, decline-as-key-material,
NoOp-gate preservation, provenance); Claude iter-2 added **D4** (periodic flush — neither peer's
sketch handled the always-on/sampled write-cost asymmetry) and the matrix-exactness stance (refused
an ungrounded count); Codex iter-3 grounded that count to **140/220** by enum enumeration (adopted —
grounding over the refusal) and contributed the **bind-entry-provenance** correction (D3 — decisions
emit during bind, so the lane is chosen before, not inspected after); Gemini iter-2 affirmed D4/D6.

## Forward arc

`216 durable decision telemetry (this) → receipt-gated behavior rotation (safe-side outer ordering
on a material `preserved_outer_order` MissedWin signal; graph-kind margins on real detail-row
divergence; windowing/decay on cumulative staleness across both durable lanes; counter handoff on
elasticity) → mixed-chain pushdown (named shape) → **memo, now checkable** — when durable decisions
show local choices fighting`. Counter handoff still preempts on a real elasticity signal.

## Deferred (named)

Decision-driven policy (any consumer — memo, decision-aware advisory); windowing/decay (now
spanning both durable lanes); counter locality handoff (elasticity-gated); per-kind graph knobs;
safe-side outer ordering; mixed-chain pushdown; memo/Cascades.
