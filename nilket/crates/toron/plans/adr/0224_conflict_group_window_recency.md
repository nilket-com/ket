# Toron ADR 224: Conflict-Group Window Recency

**Status:** Promoted. **Migration:** `plans/impl/0224_conflict_group_window_recency.md`.
**Category:** Toron optimizer instrumentation — a read-only refinement of
`information_schema.optimizer_memo_admission`. No memo engine, no actuation, no planner
policy consumer, no new durable evidence key/counter/flush, no executor / fragmenter /
wire / storage / daemon / Wendelon surface. Still renders **zero `admissible` rows**.

## Why this cut exists

ADR 223 made memo admission explicit but knowingly used ADR 222's **global**
`windowing_decay_pressure` as the recency predicate for every row — safe only while
every row is non-admissible (223 D4 flagged it). The false positive is concrete:

```text
window 10: flat_star/native is conflicted
window 12: unrelated bridge_root/native evidence is fresh
```

The global window looks fresh while `flat_star/native` is stale; 223 would upgrade the
stale row from `stale_evidence` to `blocked_shape`, and a future bounded contract could
then make it `admissible` on stale group evidence. 224 retires that known caveat —
per-group recency — **before** any candidate contract is allowed to fire. It is the
prerequisite 223 D4 named.

## Grounding (verified)

- The window namespace is already **per-key**:
  `OptimizerEvidenceWindowCounterKey::Decision { window, key }`, key =
  `OptimizerRuntimeDecisionKey { kind, outcome, lane }`. `all_window_counter_keys(current,
  previous)` materializes current+previous per-key decision counters — **no new key
  namespace**.
- `decision_rows_from_state` projects lifetime decision rows as
  `native = total.saturating_sub(demoted)` then emits the demoted sibling separately
  (runtime_evidence.rs) — group-window rows must mirror this exactly.
- 220's conflict pressure is `conflict_strength = reordered.min(blocked_or_near_miss)`
  (join_order.rs:931), and `Conflicted` requires both sides ≥ floor — so a *recent
  conflict* is `min(recent_reordered, recent_blocked_or_near_miss) >= floor`.
- `receipt_gate_snapshot()` is the one lock holding lifetime decision rows, evidence
  health, and global window health. `optimizer_memo_admission` currently applies one
  global `windowing_decay_pressure(snapshot)` to all 14 rows.
- The missing mechanism is **projection, not recording**: expose current+previous
  decision window rows in the same snapshot, compute recent conflict with the 220 model.

## Decisions (promotion-locked)

**D1 — Group-window decision rows in the receipt snapshot.** Extend
`OptimizerReceiptGateSnapshot` with a typed **internal** view of current+previous
decision rows. Not a new `information_schema` table, `SystemTableKind`, or durable
surface; built under the same one lock (no second snapshot).

**D2 — Mirror lifetime native/demoted projection.** Recent decision rows are projected
from window total + demoted lanes exactly as lifetime rows: `native =
total.saturating_sub(demoted)`, demoted separate. Recent native evidence must not
refresh the demoted sibling, and vice versa.

**D3 — Reuse the 220 conflict model for recent rows.** Over the same 14
`(decision_kind, provenance)` manifest: `Reordered` → reordered; `KeptThinMargin +
Declined(*)` → blocked-or-near-miss; `KeptOptimal` neutral; `PreservedOuterOrder` absent.

**D4 — Recent conflict means recent *conflict*, not recent activity.** A lifetime-
conflicted row is recent only when the current+previous row for the same
`(decision_kind, provenance)` is itself conflicted under the 220 floor:

```text
min(recent_reordered, recent_blocked_or_near_miss) >= JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE
```

Recent one-sided activity, recent `KeptOptimal`, or recent evidence in a sibling lane
does **not** satisfy recency (both sides of the conflict must be recent — mirroring 220).

**D5 — Admission window columns become group-local.** The 223 schema is unchanged, but
`recent_window`, `stale_lifetime`, `lifetime_total` are now group-local
conflict-relevant sample counts for that row, not global substrate counts:

```text
conflict_relevant = reordered + blocked_or_near_miss
recent_window     = recent conflict_relevant
lifetime_total    = lifetime conflict_relevant
stale_lifetime    = lifetime_total.saturating_sub(recent_window)
```

`conflict_strength` stays the lifetime minority count `min(reordered, blocked_or_near_miss)`
(the 220 pressure score).

**D6 — Status precedence stays bounded.**
`cold` (not lifetime-conflicted) · `stale_evidence` (lifetime-conflicted, not
recent-conflicted) · `degraded_evidence` (recent-conflicted, health != Ok) ·
`blocked_shape` (recent-conflicted, healthy, no contract) · `admissible` (future
bounded contract only). **Zero `admissible` rows this cut.**

**D7 — Global cockpit stays global.** `optimizer_receipt_gates` keeps its five-row
manifest and the 222 global `windowing_decay_pressure` row. 224 is a consumer-side
refinement inside memo admission, not a replacement for substrate health.

**D8 — Read-only, no engine, no durable surface.** No resolver/recognizer/actuation/
margin-selector/planner-mode/fragmenter/executor/daemon/Wendelon/storage reads the
group-window rows; no memo arena, candidate graph, generic search, Cascades surface, or
candidate memoization; no new durable keys/counters/flush.

## Schema (unchanged from 223; only window-count semantics change)

```text
decision_kind Utf8 · provenance Utf8 · admission_status Utf8 · conflict_strength Int64 ·
recent_window Int64 · stale_lifetime Int64 · lifetime_total Int64 ·
bounded_candidate_count Int64 · evidence_health Utf8 · reason Utf8 · next_cut Utf8
```

## Honesty gates (discriminating)

- **Cardinal — `memo_admission_stale_when_group_conflict_is_outside_recent_window`:**
  conflict for `flat_star/native` in window 10, advance to 12, seed unrelated recent
  conflict in 12 → global window fresh, but `flat_star/native` reads `stale_evidence`
  (must fail on `main` — proves the global-freshness loophole is closed).
- `memo_admission_blocked_shape_when_same_group_conflict_is_recent`.
- `memo_admission_group_recency_requires_recent_conflict_not_one_sided_activity` — recent
  `Reordered`-only / `KeptThinMargin`-only / `KeptOptimal` does not satisfy recency (D4).
- `memo_admission_group_recency_is_lane_scoped` (D2 — native/demoted separation).
- `memo_admission_window_columns_are_group_local_conflict_samples` (D5).
- `memo_admission_group_recency_uses_same_receipt_snapshot`.
- `memo_admission_still_renders_zero_admissible`.
- `optimizer_receipt_gates_windowing_manifest_stays_global_five_rows`.
- `rune_queries_optimizer_memo_admission_with_group_recency`.
- `memo_admission_group_recency_adds_no_durable_key_counter_or_flush`.

## Architecture guards (promotion-locked)

- `adr224_receipt_snapshot_carries_group_window_decision_rows`
- `adr224_group_window_rows_mirror_lifetime_native_demoted_projection`
- `adr224_group_recency_reuses_220_conflict_manifest`
- `adr224_memo_admission_uses_group_recency_not_global_window_for_rows`
- `adr224_group_recency_requires_recent_conflict_not_activity`
- `adr224_group_recency_is_lane_scoped`
- `adr224_global_windowing_receipt_gate_manifest_unchanged`
- `adr224_no_admissible_rows_without_bounded_candidate_contract`
- `adr224_no_new_window_key_counter_flush_or_wendelon_surface`
- `adr224_no_memo_engine_or_policy_consumer`

**Reconciled prior guards (named):**
- ADR 222 snapshot/window guards → allow the receipt snapshot to carry a typed
  current+previous decision-window view; the window-key matrix and flush paths unchanged.
- ADR 223 recency guard → replace the global
  `window.reason == recent_window_matches_lifetime` row predicate with group-local
  recent conflict status.
- ADR 217 five-row cockpit / ADR 216 140-220 / ADR 222 window-key count → unchanged.
- ADR 221/197 no-memo fences → **unchanged** (no engine; the pre-existing
  `optimizer_memo_admission` readout name remains the only allowance).

## Consensus (transparent)

3-of-3. Council chose Codex iter-0's smaller prerequisite over Claude iter-0's
engine-start; Claude **conceded** and converged, contributing the lane-scoping callout
(D2/D4). Gemini stated the stricter predicate — recency is recent *conflict strength*,
not mere recent activity. Claude iter-1 proposed a looser sum-based recency and
**conceded** to the `min(...) >= floor` form, which mirrors 220's `Conflicted` exactly
(`conflict_strength >= floor`, join_order.rs:931). Codex iter-1 is the integrated spine
(snapshot projection, native/demoted mirror, group-local columns, status precedence). No
residual divergence.

## Forward arc

`224 conflict-group window recency (this) → one bounded candidate contract for a single
decision group (lifts `bounded_candidate_count` ≥2; makes `admissible` reachable only on
a lifetime-conflicted, recent-conflicted, healthy, contract-backed row; retires the
no-memo fences for that one shape) → receipt-armed memo actuation → wider candidate
families`.

## Deferred (named)

Bounded candidate contracts; any `admissible` row; memo actuation/selection; generic
Cascades search; memo arena; candidate memoization; per-key window detail table;
window-key GC (Wendelon counter-expiry + Toron consumer); SQL parenthesized join factors;
mixed-chain pushdown; deployment elasticity signal.
