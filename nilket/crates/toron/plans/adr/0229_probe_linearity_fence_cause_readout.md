# Toron ADR 229: Probe-Linearity Fence Cause Readout

**Status:** Promoted. **Migration:** `plans/impl/0229_probe_linearity_fence_cause_readout.md`.
**Category:** Toron optimizer instrumentation — make the ADR 227 probe-linearity fence taxonomy
durable and queryable across the cluster, with a Rune handle. Read-only: no actuation,
fragmenter, policy, or plan change; the coarse `OptimizerDecisionDeclineReason::ProbeNotScanLinear`
and the 140/220 decision matrix are unchanged. The arc's **terminating measurement**: it
converts the "relax-or-rotate" decision from a guess into an evidence-gated outcome.

## Why this cut exists

ADR 227 made the probe-linearity fence cause a typed `ProbeLinearityFence`, but it lives only
at EXPLAIN altitude — the durable decision lane stays coarse `probe_not_scan_linear` (confirmed:
no probe-linearity counter in `runtime_evidence.rs`). So ADR 227's forward predicate — "relax a
fence cause only if it dominates in production" — is **uncomputable from durable cluster
evidence**; an operator would have to scrape logs or replay EXPLAIN. 229 records the typed cause
at the two real committed-decline sites and exposes a finite readout with recent-window
dominance, so a later relaxation can target the fence production evidence names rather than the
fence planning prose prefers — or, if no fence dominates, the readout proves the arc should
rotate out (a 226-style honest negative, with cluster evidence).

## Grounding (verified)

- `ProbeLinearityFence` (227) is the typed cause: `scan_row_limit`, `take`, `sort`, `aggregate`,
  `exchange`, `non_broadcast_join`, `non_scan_leaf` (7), carried through both consumers but
  written nowhere durable.
- Two production consumers commit the coarse decline: the ordinary hash fallback reason
  originates at `hash_join_decision(HashByDefaultReason::ProbeNotScanLinear(fence))`
  (join.rs:662) and is committed by `bind_scoped_join` after the final decision reason is known;
  the star/snowflake consumer commits through the **accrual** path (join_order.rs:4222/4226).
  The `analyze()` site (join_order.rs:2392) is render/recommendation, **not** the lane —
  recording there would count counterfactual/EXPLAIN binds.
- `record_decision` / `flush_decision_deltas` (runtime_evidence.rs:307 + daemon loop) and the
  ADR 222 window-id discipline (`OptimizerEvidenceWindowId`, `all_window_counter_keys`) are the
  durable-evidence pattern to mirror; `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE = 20` is the floor.
- `SystemTableKind` has 12 variants (architecture tests assert 12) → 13 with this table; ADR
  228's Rune handle set is finite-seven (`RUNE_OPTIMIZER_READOUT_TABLES: [&str; 7]`,
  rune_engine.rs:26; guard `adr228_rune_optimizer_readout_handles_are_finite_seven`) → eight.

## Decisions (promotion-locked)

**D1 — Separate durable detail namespace, not matrix expansion.** New sibling keys in
`OptimizerRuntimeEvidence`: lifetime counters under
`optimizer_evidence/probe_linearity/{consumer}/{fence}` and window counters under the existing
ADR 222 window prefix, `optimizer_evidence/window/{window}/probe_linearity/{consumer}/{fence}`.
The coarse `ProbeNotScanLinear` decline reason and the 140/220 decision matrix (ADR 216/226)
are **untouched** — no new `OptimizerDecisionOutcome`.

**D2 — Finite 14-row manifest, enum-derived (no drift).** 2 consumers
(`ProbeLinearityConsumer { HashFallback, StarDecline }`) × 7 fences. Rows derive from
`ProbeLinearityFence::label()` — the 227 enum is the sole source; no parallel string classifier.

**D3 — Committed-site recording, real-bind only.** Record only after a committed hash fallback
decision has been selected in `bind_scoped_join`, and in the star/snowflake accrual path
(join_order.rs:4222/4226), under the existing `OptimizerDecisionRecorder` vs
`NoOpDecisionRecorder` boundary. `choose_auto_join_strategy` / join.rs:662 stays a pure
decision constructor; EXPLAIN-only, scratch, and counterfactual binds render the cause but write
**no** durable evidence (the 226 production-home discipline, applied to recording). The
`analyze()` render site (2392) is **not** a recording site.

**D4 — Recent-window dominance (ADR 222 discipline).** Status from current+previous window
counts, not lifetime: `absent` · `insufficient_evidence` (consumer recent total < floor) ·
`observed` · `dominant` (`recent_count >= floor && consumer_recent_total >= floor &&
recent_count * 2 > consumer_recent_total` — a strict majority). Lifetime counts render for
context; a stale lifetime majority is not a relaxation reason.

**D5 — Durable, bounded-manifest flush.** The namespace flushes through the daemon evidence
loop exactly like `flush_decision_deltas` (Wendelon `counter_fetch_add`), reading a bounded
current+previous manifest (14 lifetime + 14×2 window keys), never a historical prefix scan. It
is durable and cluster-visible, not process-local — production dominance is a cluster claim. No
new Wendelon counter *type*, only new keys.

**D6 — Rune handle parity.** Add `ctx.optimizer_probe_linearity_fences()` as the eighth finite
handle (ADR 228 pattern), so the cockpit stays first-class in Rune; it must pass the 228
plan-equivalence honesty gate (handle plan equals the SQL `information_schema` scan).

**D7 — `SystemTableKind` 12 → 13; ADR 102 / 228 retargets.** One new info-schema table; the
ADR 102 finiteness + explicit-dispatch guards move 12→13, and the ADR 228 finite-seven handle
guard + no-drift manifest move 7→8.

**D8 — Read-only; terminating measurement.** No resolver / recognizer / actuation /
margin-selector / fragmenter / executor / policy consumer reads the rows; querying records no
evidence; user query plans byte-identical. This is the arc's last instrument: a `dominant` row
grounds a future named relaxation (the production-home proof), or no-dominance proves rotate-out.

## Schema (column order locked)

```text
consumer Utf8 · fence Utf8 · recent_count Int64 · lifetime_count Int64 ·
consumer_recent_total Int64 · consumer_lifetime_total Int64 · recent_share_per_mille Int64 ·
status Utf8 · reason Utf8 · next_cut Utf8
```

## Honesty gates (discriminating)

- `optimizer_probe_linearity_fences_projects_static_14_rows` — cold table, stable order.
- `hash_fallback_sort_counts_hash_row_only` / `star_decline_exchange_counts_star_row_only` —
  consumer isolation.
- `real_bind_records_committed_fence_not_analyze_render` — records at the commit site, not the
  2392 render site (locks D3 precision; fails on `main`).
- `probe_linearity_fence_readout_ignores_counterfactual_explain` — EXPLAIN renders the cause,
  writes no durable count.
- `probe_linearity_fence_dominance_requires_recent_majority` — lifetime-heavy but stale rows are
  not `dominant`.
- `probe_linearity_fence_tie_or_split_is_observed_not_dominant` — no plurality shortcut.
- `probe_linearity_fence_readout_is_read_only_no_plan_change`.
- `rune_optimizer_probe_linearity_fences_matches_ctx_sql_plan_and_rows` — the new handle is a
  pure alias (228 D5).
- `durable_probe_linearity_fence_counters_survive_restart_and_share_across_coordinators` — the
  new namespace is durable and cluster-visible, not process-local.

## Architecture guards (promotion-locked)

- `adr229_probe_linearity_detail_namespace_not_decision_matrix`
- `adr229_probe_linearity_manifest_is_2_by_7_enum_derived`
- `adr229_probe_linearity_records_only_at_committed_decline_real_bind`
- `adr229_probe_linearity_dominance_uses_recent_window_not_lifetime`
- `adr229_probe_linearity_has_no_policy_consumer`
- `adr229_probe_linearity_rune_handle_present_and_aliased`
- `adr229_system_table_kind_and_adr102_dispatch_retargeted_12_to_13`

**Reconciled prior guards (named):**
- ADR 216 decision matrix → **unchanged at 140/220** (detail namespace, no new outcome).
- ADR 222 window discipline → reused; the decision-window manifest unchanged, a sibling
  probe-linearity window namespace added (bounded current+previous, no prefix scan).
- ADR 227 fence taxonomy → the enum is the sole source; coarse decline reason unchanged.
- ADR 228 Rune handles → **finite-seven → eight** (+ no-drift manifest retarget); plan-equivalence
  gate applies to the new handle.
- ADR 102 finite system-table + explicit dispatch → **12 → 13**.
- ADR 226 zero-admissible / no-generic-search → unchanged.

## Consensus (transparent)

3-of-3. At 228 iter-1 Claude conceded this cause-distribution readout was the better-grounded
*optimizer* cut and framed it as the terminating measurement; the operator chose the Rune
rotation (228) and named this the fallback. 229 iter-0 Claude's grounding sweep falsified every
other candidate (Rune outer-join parity already by construction; DISTINCT/set-ops not SQL
features; mixed-chain covered by runtime filters; elasticity externally blocked), and the
operator selected this. Codex grounded the real-bind-only recording and the typed-source rule;
Gemini specified the namespace, dominance math, and Rune handle; Claude sharpened the
committed-site recording precision, the ADR 228 handle reconciliation, and the durable
222-mirrored flush. No residual divergence.

## Forward arc

```text
228 rune optimizer readout handles
-> 229 probe-linearity fence cause readout (this; the arc's last instrument)
-> one named fence relaxation only if a consumer/fence row is dominant in production and clears
   the production-home test; otherwise rotate out of the optimizer (the readout records the
   negative).
```

## Deferred (named)

Any fence relaxation / probe physicalization; a fence chosen before a `dominant` row; memo
admission (226-closed); mixed-chain pushdown (covered by runtime filters); counter-handoff
elasticity (externally blocked); set operations / DISTINCT / window functions
(missing-capability cuts for a later, non-optimizer arc); probe-linearity window-key GC (the
additive-namespace caveat inherited from 222, ≤ 28 keys/window).
