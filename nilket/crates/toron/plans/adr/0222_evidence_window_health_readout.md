# Toron ADR 222: Evidence-Window Health Readout

**Status:** Promoted. **Migration:** `plans/impl/0222_evidence_window_health_readout.md`.
**Category:** Toron optimizer instrumentation — makes the existing
`windowing_decay_pressure` cockpit row computable from a separate, durable window
namespace. No optimizer actuation, no policy consumer, no new `information_schema`
table, no new `SystemTableKind`, no executor / fragmenter / wire / Wendelon change.
The lifetime 80+140=220 evidence matrix remains canonical and unchanged.

## Why this cut exists

217 made the rotation cockpit deliberately asymmetric: three rows computable, while
`windowing_decay_pressure` was hard-coded `inactive`/`windowing_decay_substrate_unbuilt`
(no age/decay substrate) and `counter_handoff_readiness` stayed
`blocked_external_signal` (external). 220 made decision conflict legible per
`(kind, provenance)`, but its forward arc requires a conflicted row to be **stable
under evidence health** before memo can be argued honestly — and that predicate is not
computable today. 222 builds the missing substrate and retargets that one dead cockpit
row from a placeholder to a computed evidence-health readout, **without consuming it in
planning**. It is the last instrumentation gap before memo.

## Grounding (verified)

- `receipt_gate_snapshot()` already captures verdict rows, decision rows, and evidence
  health under one runtime-evidence lock — the extension point.
- Lifetime keys are `optimizer_evidence/verdict/…` and `optimizer_evidence/decision/…`;
  the 216 guard pins 80 verdict + 140 decision = 220. Untouched here.
- `record_verdict`/`record_decision` (runtime_evidence.rs:172/199) only increment local
  pending maps under the mutex — no hot-path I/O; an extra window increment is local
  bookkeeping, not a network round-trip.
- The 60s evidence loop (daemon.rs:73/306) already calls `flush_decision_deltas` (:253)
  then `refresh_durable_cache`; window flushes ride that loop.
- The counter API is `counter_read` + `counter_fetch_add` — **additive only, no
  reset/TTL** (the constraint that shapes the namespace design below).

## Decisions (promotion-locked)

**D1 — Lifetime matrix canonical and unchanged.** The 80 verdict + 140 decision keys
remain the only lifetime matrix; all current advisory/policy consumers read them
exactly as today. Window keys are a side namespace and must not change
`adr216_decision_matrix_is_140_keys_and_evidence_matrix_220`.

**D2 — Separate bounded window namespace.** A durable side namespace rooted at
`optimizer_evidence/window/{window_id}/…` with the lifetime key suffix, split by
verdict/decision. The readout reads a **fixed manifest of the current and previous
windows only** — no prefix scan over history. Older window keys remain durable history
ignored by construction (the additive counter has no reset; bounded read, unbounded
history — see Deferred for GC).

**D3 — Deterministic window-id source.** Window ids come from an **injected epoch
source**, never ad-hoc wall-clock in gate math: production uses the daemon evidence
interval granularity; tests install a fixed/manual epoch. Window selection is
deterministic and reviewable.

**D4 — Dual local recording, shared flush discipline.** `record_verdict`/`record_decision`
increment both the lifetime pending and the current-window pending **under the same
lock** (the only hot-path change — one local increment). `flush_window_deltas` uses the
same `counter_fetch_add` + timeout/ack/ambiguous-drop/replication-unsupported semantics
as the lifetime flush and rides the existing 60s loop. No new query-path I/O, no new
write primitive.

**D5 — Computed gate, existing 217 status vocabulary.** From one snapshot:
`lifetime_total`, `recent_window = current + previous window counts`,
`stale_lifetime = lifetime_total − recent_window`, `floor = JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE`:
```text
if lifetime_total < floor          -> insufficient_evidence
else if stale_lifetime >= floor
        && stale_lifetime > recent_window -> actionable   (evidence is stale/decayed)
else                                -> inactive            (recent evidence is live)
```
`sample_count` stays the lifetime total; `primary_signal` reports
`recent_window/stale_lifetime/lifetime_total`; `secondary_signal` keeps evidence health.
The status **must** derive from the window split, never lifetime totals alone: a large
lifetime total with matching current+previous window evidence is `inactive`, while a
large lifetime total absent from the current+previous manifest is `actionable` stale
evidence. It never renders `blocked_external_signal`.

**D6 — Read-only; no policy consumer; memo deferred.** No resolver/recognizer/margin/
actuator/daemon-admission/Wendelon path consumes the window gate; planning byte-identical.
memo's stability standing is now computable; memo stays gated (still needs a conflicted
row sitting in a healthy window + the multi-candidate-search contract).

**D7 — Counter handoff stays external.** This cut does not make
`counter_handoff_readiness` computable or infer elasticity from optimizer counters.

**D8 — Cockpit row only.** The computed status lives in the existing
`windowing_decay_pressure` row — no new table, no `SystemTableKind`, no adr102 retarget.
A per-key detail window table is deferred.

## Honesty gates (discriminating)

- `windowing_gate_inactive_when_recent_window_matches_lifetime` — the critical
  discriminator against lifetime-total-only actionability.
- `windowing_gate_actionable_when_lifetime_dominated_by_stale_windows` — proves the
  dead row is now computed.
- `windowing_gate_insufficient_on_cold_ledger`.
- `lifetime_matrix_counts_unchanged_by_windowing` — 80/140/220 hold.
- `window_read_is_bounded_current_previous_manifest` — no prefix scan.
- `receipt_gate_snapshot_reads_windows_under_same_lock`.
- `windowing_readout_is_read_only_no_plan_change`.
- `counter_handoff_readiness_stays_blocked_external_signal`.
- `rune_queries_optimizer_receipt_gates_with_windowing`.

## Architecture guards (promotion-locked)

- `adr222_window_keys_are_separate_namespace_not_in_220_matrix`
- `adr222_windowing_gate_is_computed_not_placeholder`
- `adr222_windowing_gate_uses_window_split_not_lifetime_totals`
- `adr222_window_read_is_bounded_current_previous_manifest`
- `adr222_window_snapshot_is_single_locked_snapshot`
- `adr222_windowing_readout_has_no_policy_consumers`
- `adr222_counter_handoff_stays_blocked_external_signal`
- `adr222_no_new_information_schema_or_system_table_kind`

**Reconciled prior guards (named):**
- `adr217_blocked_placeholders_cannot_render_actionable` (optimizer_accrual.rs:2185) →
  retarget: counter handoff still blocked; windowing is no longer a placeholder and may
  render `actionable` under the computed stale-window condition.
- `adr220_windowing_and_counter_handoff_stay_substrate_blocked` → retarget: windowing
  substrate is now intentionally built (counter stays blocked; decision-conflict reports
  still do not feed window/counter rows).
- `adr216_decision_matrix_is_140_keys_and_evidence_matrix_220` → **unchanged and
  asserted unchanged** (window keys are a separate namespace).
- adr102 information_schema / SystemTableKind guards → unchanged (no table, no kind);
  217 five-row cockpit manifest unchanged.

## Consensus (transparent)

3-of-3 on windowing-health (Claude iter-0 dissolved the two objections that had
deferred it — separate namespace avoids lifetime-matrix churn; dual recording is a
local increment, not hot-path I/O; Gemini named it the natural pre-memo substrate;
Codex framed the bounded durable-read constraint). **Divergence resolved by
grounding:** Claude iter-1 proposed a lighter **refresh-delta** design (the window as an
in-memory delta of the durable lifetime counters between refresh generations — no new
durable surface, no churn, no dual-write) and **conceded** it. It is genuinely lighter,
but it makes the windowing signal the one in-memory, per-coordinator, **non-restart-safe**
exception in an otherwise durable, replicated substrate — and memo's "stable under
evidence health" gate rests on exactly this signal, so it must live in the durable lane
like everything 207→216. The durable namespace's cost — unbounded window-key churn (the
additive counter has no reset/TTL) — is real but **named and bounded** (current+previous
read; a window-key GC is the deferred follow-on).

## Forward arc

`222 evidence-window health readout (this) → memo/Cascades only when a specific
decision-conflict row is `conflicted`, the window row reports recent evidence
(`inactive` for `recent_window_matches_lifetime`, not stale-pressure `actionable`),
and a bounded multi-candidate-search contract exists → SQL parenthesized join factors
(221 caveat) / mixed-chain grounding / elasticity as separate signals`.

## Deferred (named)

Memo/Cascades and any decision-driven policy; the refresh-delta lighter design (the
no-durable-surface alternative, if churn ever outweighs restart-safety); durable
expiry/GC for historical window keys; a per-key
`information_schema.optimizer_evidence_windows` detail table; counter-handoff elasticity
signal; SQL parenthesized join factors; mixed-chain pushdown.
