# Impl Plan — ADR 216: Durable Optimizer Decision Telemetry

Pairs with `plans/adr/216_durable_optimizer_decision_telemetry.md`.

Touch surface:
- `binder/decision_log.rs` (decision key vocabulary + codec + bind-entry provenance).
- `binder/runtime_evidence.rs` (decision lane in the handle + periodic flush API).
- `binder.rs` (bind-entry provenance selection).
- `binder/explain.rs` (decisions-table rows + provenance column).
- `daemon.rs` (interval evidence task).
- Tests and guards.

No Wendelon production change; no new table or `SystemTableKind`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Decision record site (always-on) | `self.decision_recorder.record(...)` join_order.rs:3573; trait + `NoOpDecisionRecorder` decision_log.rs:155-172 |
| Decision vocabulary | `OptimizerDecisionKind` (PreservedOuterOrder + Overflow are NOT recording kinds), `OptimizerDecisionOutcome` (Reordered/KeptOptimal/KeptThinMargin/Declined(reason×7)), labels :176+ |
| Verdict durable model (mirror) | `OptimizerRuntimeEvidence` pending/in-flight/cache, `record_verdict`, `flush_keys`, Total/Demoted dual-write (207/214) runtime_evidence.rs |
| Bind-entry demotion (provenance source) | the outer-join demotion normalizer run at bind entry (209/210); `outer_join_demotions` receipts on the render trace (explain.rs:80) |
| Existing table | `information_schema.optimizer_decisions` schema/rows in explain.rs; `SystemTableKind::InformationSchemaOptimizerDecisions` |
| Interval evidence task | `OPTIMIZER_EVIDENCE_REFRESH_INTERVAL` daemon.rs:73, task :306-312 (currently `refresh_durable_cache`) |
| Local log being replaced | `OptimizerDecisionLog { counts, overflow_count, max_keys }` decision_log.rs:92-150 |
| 187 guards (retarget) | grep `adr187` in tests/architecture/ for the bounded/storage/info-schema guards |

## Phase 0 — Red tests first

1. `durable_optimizer_decisions_survive_coordinator_restart` — red on main.
2. `decision_record_is_latency_identical_no_per_query_flush` — **D4's pin**: no decision
   `counter_fetch_add` on the bind/coordinator-success path. Written before the flush wiring.
3. `preserved_outer_order_records_no_durable_decision` — matrix-exactness pin.
4. `demoted_decision_dual_writes_total_and_demoted_sibling` — with provenance selected at bind entry
   (the fixture must demote at bind entry and confirm the *first* recorded decision is Demoted).
5. `manual_planning_byte_identical_with_durable_decision_telemetry`.
6. `scratch_bind_decisions_do_not_reach_durable_lane`.

## Phase 1 — Recording-kind set + keys (decision_log.rs)

- **Ground the recorder kind set first**: trace every `decision_recorder.record(kind, _)` call;
  encode `decision_recording_kinds() -> [OptimizerDecisionKind; 7]` (the seven recording kinds) with
  a unit test that every record-site kind is a member and `PreservedOuterOrder`/`Overflow` are not.
- `OptimizerRuntimeDecisionKey { kind, outcome, lane }` (or the `OptimizerEvidenceKey` wrapper);
  `OptimizerDecisionOutcome::label`/`from_label`; decline keys carry the reason.
- `all_decision_counter_keys()` = 7 × 10 × 2 = 140, enumerable; codec test pins the spelling and the
  count (140 decision, 220 combined).

## Phase 2 — Decision lane in the handle (runtime_evidence.rs)

- Decision pending/cache beside verdicts (separate key type; Total/Demoted dual-write reuses the 214
  shape). `record_decision(kind, outcome, provenance)` increments local pending only — **no flush,
  no await**.
- `decision_snapshot()` deriving Native = Total − Demoted; zero-omission unchanged.
- `flush_decision_deltas()` async: drains decision pending, `counter_fetch_add`es deltas (same
  facade, dual-write, dropped/ambiguous semantics). Called only by the interval task (Phase 4).

## Phase 3 — Bind-entry provenance + recorder delegation (binder.rs + decision_log.rs)

- **At bind entry** (where the 209/210 demotion normalizer runs): compute provenance =
  `outer_join_demotions.is_empty()` → Native/Demoted, and construct the live recorder carrying it,
  *before* binding the normalized plan (Codex iter-3 — decisions emit during bind; do not inspect
  the trace post-hoc). Verify-first: the cleanest carrier (a provenance field on the recorder, or a
  recorder constructed per-bind with the provenance) — thread it, not a global.
- Live `record` delegates to `runtime_evidence.record_decision(kind, outcome, self.provenance)`;
  `NoOpDecisionRecorder` unchanged.
- Retire the local bounded map (`counts`/`max_keys`/`overflow_count`), or keep a thin `Overflow`
  legacy diagnostic if a guard requires it (D6/C5 — decide at impl).

## Phase 4 — Periodic flush (daemon.rs)

Extend the 60s interval task: call `flush_decision_deltas()` alongside `refresh_durable_cache()`.
One added line in the established loop, drop-safe on shutdown; optional best-effort flush at
graceful shutdown to narrow the loss window. The verdict post-success flush in coordinator.rs is
**untouched**.

## Phase 5 — Decisions table + parity (explain.rs)

Retarget `optimizer_decisions` rows to `decision_snapshot()`; append `provenance` at the tail
(columns 1–4 unchanged). Rune parity test (the 213/214/215 loop pattern).

## Phase 6 — Guards

The eight `adr216_*` from the ADR. `adr216_decision_flush_is_periodic_not_per_query` is a source
scan (no decision flush/`counter_fetch_add` in the coordinator success path or bind helpers; the
only call is the daemon interval task). `adr216_decision_provenance_is_bind_entry_not_posthoc`
pins the bind-entry selection. Named 187/188/207/214 retargets per the ADR; assert adr102 and the
verdict matrix guards unchanged.

## Named falsifications (for the review)

1. Flush decisions post-success per-query: `decision_record_is_latency_identical_no_per_query_flush`
   must fail.
2. Include `PreservedOuterOrder` in `decision_recording_kinds()`:
   `preserved_outer_order_records_no_durable_decision` must fail.
3. Select provenance from the post-bind trace instead of bind entry: a fixture where an early
   decision precedes demotion-receipt finalization misattributes —
   `demoted_decision_dual_writes_total_and_demoted_sibling` must fail.
4. Flatten declines: `decline_reason_is_preserved_in_durable_key` must fail.
5. Route scratch binds to durable: `scratch_bind_decisions_do_not_reach_durable_lane` must fail.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib decision
scripts/cargo-cgroup test  -p toron --lib runtime_evidence
scripts/cargo-cgroup test  -p toron --test wendelon_storage_execution_test durable_optimizer_decisions
scripts/cargo-cgroup test  -p toron --test architecture_test adr216
scripts/cargo-cgroup test  -p toron --test architecture_test adr187
scripts/cargo-cgroup test  -p toron --test architecture_test adr207
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL binary — non-optional
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene

- No m-word; no deprecated product-stage/version idiom; no council-internal labels in promoted/code text
  (the 215 lesson).
- Durable decision key strings are a new format surface (decisions were never durable — no existing
  goldens); pin the spelling + count with a codec test.
- `--all-targets` after the key/handle struct changes.
- LiveWendelonFixture flake discipline on the restart/cross-coordinator fixtures.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. The exact `decision_recording_kinds()` membership (Phase 1 — derive from the call graph; the ADR
   commits to the seven recording kinds, confirm each reaches `record()`).
2. The bind-entry provenance carrier (Phase 3 — recorder field vs per-bind construction).
3. Retire the local bounded map fully vs keep a thin `Overflow` diagnostic (Phase 3).
4. Native-row rendering: derived vs raw at the explain site (214 precedent).
