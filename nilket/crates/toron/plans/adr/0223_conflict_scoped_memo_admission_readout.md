# Toron ADR 223: Conflict-Scoped Memo Admission Readout

**Status:** Promoted. **Migration:** `plans/impl/0223_conflict_scoped_memo_admission_readout.md`.
**Category:** Toron optimizer instrumentation - a read-only admission surface over
the 220 decision-conflict detail and 222 evidence-window health. No memo engine, no
actuation, no planner policy consumer, no new durable key/counter/flush path, no
executor / fragmenter / wire / Wendelon surface. This adds one finite
`information_schema` table and retargets the no-memo guards to allow the readout
while continuing to ban a search engine.

## Why this cut exists

220 made conflict pressure legible per `(decision_kind, provenance)`. 222 made the
missing freshness predicate computable: a conflicted row can now be distinguished
between recent evidence and stale lifetime-only accumulation. That completes the
instrumentation needed to ask the next question honestly:

```text
Which conflict row is recent, healthy, and backed by a bounded multi-candidate
search contract?
```

The answer is not an actuator yet. Current recognizer dispatch is still
specialist-first, and the only non-redundant memo value is a future bounded
multi-candidate search. ADR 223 therefore creates the admission readout that says
why memo is not admissible yet, or which exact row would become admissible once a
bounded shape contract exists. It prevents the next memo ADR from smuggling in a
generic search under a seeded fixture.

## Grounding (verified)

- `decision_conflict_reports(&OptimizerReceiptGateSnapshot)` already returns the
  fixed 14 `(decision_kind, provenance)` reports and is shared by the 217 cockpit row
  and the 220 detail table.
- `windowing_decay_pressure_gate(&snapshot)` is computed from
  `snapshot.window.{recent_count, stale_lifetime_count, lifetime_count}` and is
  `inactive` with reason `recent_window_matches_lifetime` exactly when the recent
  current+previous window covers the lifetime signal.
- `receipt_gate_snapshot()` is the common one-lock evidence snapshot that contains
  decisions, verdicts, evidence health, and window health.
- `SystemTableKind` currently has 11 variants after ADR 220/222; this table makes it
  12 and must retarget the ADR102 finite-family and explicit-dispatch guards.
- Existing no-memo fences ban `src/optimizer.rs`, `src/optimizer/`, `fn memo`, and
  `Cascades` in source. This cut should keep those bans, but allow the literal
  information_schema readout name and typed admission helper.

## Decisions (promotion-locked)

**D1 - One read-only table: `information_schema.optimizer_memo_admission`.** The table
is an admission contract, not a planner API. It interprets existing decision-conflict
and window-health signals. It does not duplicate raw decision rows and does not change
the five-row `optimizer_receipt_gates` cockpit.

**D2 - Static 14-row manifest.** One row for every
`decision_recording_kinds() x {native, demoted}` group, matching ADR 220 order.
Cold zero rows render too. `PreservedOuterOrder` remains absent because it is
verdict-only in the decision lane.

**D3 - Admission status vocabulary.** Exactly:

```text
cold              - the 220 conflict report is not conflicted
stale_evidence    - conflict exists, but the 222 window row is not healthy recent evidence
degraded_evidence - conflict is recent, but runtime evidence health is not ok
blocked_shape     - conflict is recent and healthy, but no bounded search contract exists
admissible        - conflict is recent, healthy, and a bounded candidate contract exists
```

The initial implementation is allowed to render **zero `admissible` rows**. The
first cut may report `blocked_shape` for all conflicted groups because it is still
read-only and does not implement candidate generation.

**D4 - Recent evidence means the 222 recent-window state, not a large lifetime total.**
Admission may pass the evidence gate only when the typed window health equivalent of
`windowing_decay_pressure` is `inactive` for `recent_window_matches_lifetime`.
`actionable` stale-window pressure, `insufficient_evidence`, and any degraded runtime
evidence state block admission. **The 222 gate is a GLOBAL window status in this cut**,
so the `stale_evidence` vs `blocked_shape` split is the same global window state across
all rows (informational, not per-group). That is acceptable **only because no row is
`admissible`**. The actuation ADR that makes `admissible` reachable **must** sharpen
"recent evidence" to **per-group window recency** (is *this* `(kind, provenance)` group's
conflict evidence recent — computable from 222's per-key window namespace), or it would
admit a group conflicted on stale evidence while the global substrate is fresh. This is
a named prerequisite, not built here.

**D5 - Bounded shape contract is explicit and local.** Introduce a small typed
admission classifier that maps each decision group to either a named bounded search
contract or `blocked_shape`. This ADR does not add a generic search engine, memo
arena, memoized expression graph, candidate enumeration, or cost-based selector.
The classifier can conservatively return `blocked_shape` for every group until a
later actuation ADR proves one group.

**D6 - One snapshot, shared aggregations.** Build rows from one
`receipt_gate_snapshot()`. Reuse `decision_conflict_reports(&snapshot)` and a typed
window-health helper derived from the same snapshot. Do not call
`decision_snapshot()`, `verdict_snapshot()`, `status_snapshot()`, or a second
`receipt_gate_snapshot()` per row.

**D7 - Read-only; planning byte-identical.** No resolver, recognizer, actuation,
margin selector, fragmenter, executor, daemon, Wendelon facade, or storage code reads
the admission rows. `Manual` and `ApplyAdvisory` plans and traces are byte-identical
with and without querying the table.

**D8 - No durable surface.** No new optimizer evidence keys, counters, flushes,
window keys, or runtime recording paths. ADR 216's 140/220 matrix and ADR 222's
window namespace stay unchanged.

**D9 - Rune parity.** The table is reachable through shared
`bind_information_schema_relation`; SQL and Rune return identical rows.

## Schema (column order locked)

```text
decision_kind Utf8
provenance Utf8
admission_status Utf8
conflict_strength Int64
recent_window Int64
stale_lifetime Int64
lifetime_total Int64
bounded_candidate_count Int64
evidence_health Utf8
reason Utf8
next_cut Utf8
```

`bounded_candidate_count = 0` unless a bounded shape contract exists. `next_cut`
names the missing contract, not an engine.

## Honesty gates (discriminating)

- `optimizer_memo_admission_projects_static_14_rows` - cold ledger renders the 14
  ADR 220 groups in stable order.
- `memo_admission_cold_when_conflict_not_conflicted` - one-sided rows do not become
  admissible.
- `memo_admission_stale_when_conflict_recent_window_missing` - a conflicted lifetime
  row with no current+previous window evidence reports `stale_evidence`.
- `memo_admission_blocked_shape_when_conflicted_recent_and_healthy` - recent healthy
  conflict still does not imply an engine; it reports `blocked_shape` until a bounded
  contract exists.
- `memo_admission_degraded_when_evidence_health_not_ok` - a recent conflict under a
  non-`Ok` runtime evidence state reports `degraded_evidence` (the defined status has a
  discriminating test, not just an enum variant).
- `memo_admission_uses_same_snapshot_as_conflict_and_window_health` - no independent
  decision/window snapshots.
- `memo_admission_readout_is_read_only_no_plan_change`.
- `rune_queries_optimizer_memo_admission`.
- `memo_admission_adds_no_durable_key_counter_or_flush`.

## Architecture guards (promotion-locked)

- `adr223_memo_admission_table_is_explicit_information_schema_surface`
- `adr223_memo_admission_rows_are_static_14_group_manifest`
- `adr223_admission_uses_decision_conflict_reports_and_window_snapshot`
- `adr223_recent_evidence_requires_222_window_inactive_recent_reason`
- `adr223_blocked_shape_is_not_admissible_without_bounded_contract`
- `adr223_no_memo_engine_or_policy_consumer`
- `adr223_no_new_durable_key_counter_flush_or_wendelon_surface`
- `adr223_no_receipt_gate_manifest_change`

**Reconciled prior guards (named):**

- ADR102 finiteness + explicit-dispatch guards: **11 -> 12**, adding
  `InformationSchemaOptimizerMemoAdmission`.
- ADR220 no-policy-consumer guard: allow exactly this read-only admission table and
  its helper as another table/readout consumer of `decision_conflict_reports`; continue
  to ban resolver/graph/outer policy consumers.
- ADR222 no-policy-consumer guard: allow admission to observe `snapshot.window`;
  continue to ban planner paths from consuming `windowing_decay_pressure`.
- ADR221 / ADR197 no-memo fences: retarget to allow the
  `optimizer_memo_admission` readout name and typed admission helper, while still
  banning `src/optimizer.rs`, `src/optimizer/`, `fn memo`, `Cascades`, memo arenas,
  candidate memoization, and planner consumption.
- ADR217 five-row receipt-gate manifest: unchanged.
- ADR216 140/220 and ADR222 window namespace guards: unchanged.

## Consensus (transparent)

The council selected **Conflict-Scoped Memo Admission Readout** despite the valid
counterargument that memo itself is still gated. This ADR adopts the conservative
reading: 223 is not memo actuation and does not claim a bounded search exists. It
uses the 217/220/222 instrumentation chain to make the admission predicate explicit,
including the honest `blocked_shape` state. That resolves the discipline fork:
we do not wait silently for production signals, and we do not build the engine
ahead of proof; we build the cockpit row that tells us which exact proof is missing.

## Forward arc

`223 conflict-scoped memo admission readout (this) -> one bounded candidate-contract
ADR for exactly one admissible group -> receipt-armed memo actuation only after a row
is both recent/healthy and backed by that contract -> wider candidate families`.

## Deferred (named)

Any memo engine or actuation; generic Cascades search; candidate memoization; bounded
candidate contracts for specific groups; **per-group window recency (the actuation-cut
prerequisite for `admissible`, D4)**; SQL parenthesized join factors; mixed-chain
pushdown; window-key GC; deployment elasticity signal; per-key window detail table.
