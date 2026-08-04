# Impl Plan — ADR 207: Durable Optimizer Evidence / Telemetry

Pairs with `plans/adr/0207_durable_optimizer_evidence_telemetry.md`. Consumer-only cut: the new mechanism is
the `OptimizerRuntimeEvidence` handle (pending + durable-cache lanes over the ADR 075 counter facade);
everything else is retargeting a closed caller set and two read-only surfaces. No Wendelon production change.
Code anchors below are verified; the ADR 075 high-level fetch-add facade is a prerequisite if it has not
landed before ADR 207 implementation starts.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Verdict lane today | `binder/decision_log.rs:79` (`verdict_counts`), `record_verdict` :114, `verdict_snapshot` :151 |
| Label fns (key bytes) | `optimizer_decision_kind_label` decision_log.rs:186, `optimizer_runtime_verdict_label` :223 |
| Recording sites (sampled, post-success) | coordinator.rs:1340 (`run_streaming_read_driver`, post `dispatch_loop` Ok) and ~:2450 (local `execute_query` success) |
| Sample gate | `should_measure_runtime_verdict` coordinator.rs:1486; `measured_accrual_sample_rate: 0` binder.rs:151 |
| Advisory snapshot consumers | `effective_join_order_soft_margin_step` join_order.rs:180-193; `margin_advisory_report` :204-218 |
| Info-schema verdict table | binder.rs:1873 (`verdict_snapshot()` render) |
| Daemon construction | daemon.rs:131-183 (`Arc<wendelon_client::Client>` + tenant before coordinator); harness ctor :141 |
| Counter facade | ADR 075 target facade: `Client::counter_fetch_add`, `Client::counter_read`, `counter_identity_for_key`; current source anchors the underlying HRFT client at `crates/wendelon/src/client/core.rs:1287` (`counter_add`) and `:1347` (`counter_read`), with `ClientWriteId` in `CounterAddRequestMeta` at `crates/wendelon/src/schema/core.rs:1046` |
| rf>1 reject | `WriteStatus::COUNTER_REPLICATION_UNSUPPORTED` wendelon data_server.rs:7733 |
| `/metrics` registry | `MetricsRegistry` network/observability.rs:166, `render_text` :243, `render_*_metrics` helpers |
| Guards home | `tests/architecture/optimizer_accrual.rs` |
| Live fixture | `tests/wendelon_storage_execution_test.rs` (`LiveWendelonFixture`) |

## Phase 1 — `runtime_evidence.rs`: keys + handle (pure, unit-testable)

New `crates/toron/src/binder/runtime_evidence.rs`, registered from binder.rs:

- `verdict_counter_key(kind, verdict) -> Vec<u8>` = `optimizer_evidence/verdict/{kind_label}/{verdict_label}`;
  `all_verdict_counter_keys()` enumerating the 35-key matrix (7 kinds, `Overflow` excluded, × 5 classes).
- `OptimizerRuntimeEvidence`: one `Mutex<RuntimeEvidenceState>` — `{ pending, in_flight, durable_cache:
  BTreeMap<OptimizerRuntimeVerdictKey, u64>, mode, state, dropped_deltas, ambiguous_deltas,
  refresh_failures, last_error }` — plus `binding: Option<DurableEvidenceBinding { client: Arc<Client>,
  tenant_id, flush_timeout }>`. Constructors `local_only()` / `with_wendelon(binding)`.
- Sync API: `record_verdict(kind, verdict) -> key` (pending saturating-add, returns touched key);
  `verdict_snapshot()` = `durable_cache + pending + in_flight` per key, **zero rows omitted** (D3: in-flight
  is conceptually pending — a mid-flight snapshot counts the delta exactly once); `status_snapshot()`.
- Lane migration under the single lock; **never hold the mutex across an await** (std `Mutex`): move
  unflushed → in-flight; ack clears in-flight and sets `durable_cache[key] = max(cached, current_value)`
  (D4 larger-wins); post-attempt drop clears in-flight + bumps diagnostics. If an implementation reserves a
  delta before discovering no call will be made, restore that never-attempted delta to pending.
- Async API (coordinator/daemon only): `flush_keys(keys)` — per unique key: skip without moving pending if
  unbound, latched, circuit-open, or zero delta; otherwise move the delta to in-flight and call
  `tokio::time::timeout(flush_timeout, counter_fetch_add(key_bytes, delta))`. Ack on success;
  `COUNTER_REPLICATION_UNSUPPORTED` → drop + latch `ReplicationUnsupported`; any other post-attempt
  error/timeout → drop + ambiguous diagnostics, **no re-call**, and mark the lane unavailable/stale. Repeated
  unavailable failures may open a circuit breaker so later deltas remain local pending until refresh/probe
  closes it. `refresh_durable_cache()` — 35 point reads, `None → 0`; failure keeps the prior cache, sets
  `Stale`, bumps `refresh_failures`, and successful reads merge without regressing larger cached values.

Unit tests: 35 unique keys + representative spot checks; local-only snapshot == pre-207 behavior;
cache+pending+in-flight counts once across record/flush/ack interleavings; larger-wins under out-of-order
acks and refresh interleavings; never-attempted deltas stay pending; post-attempt ambiguous failures drop
only the in-flight delta; unavailable circuit-open state keeps later deltas local; status transitions.

## Phase 2 — Move the verdict lane; retarget the closed caller set

- `PlannerConfig.runtime_evidence: Arc<OptimizerRuntimeEvidence>` (default `local_only()` — every existing
  test unchanged), beside `decision_log` (binder.rs:136).
- Retarget (grep `record_verdict|verdict_snapshot` — closed set): coordinator.rs:1340 + ~:2450 (record),
  join_order.rs:186-187 + :205-206 (advisory), binder.rs:1873 (info-schema), verdict-lane unit tests. Remove
  `verdict_counts` / `record_verdict` / `verdict_snapshot` from `OptimizerDecisionLog` — no compatibility
  wrappers. Decision lane + recorder ZST untouched (D1).
- `scripts/cargo-cgroup check -p toron --all-targets` (new `PlannerConfig` field; benches/examples construct
  it — the ADR 116 lesson).

## Phase 3 — Coordinator flush at the two success sites

At both sampled success sites: collect keys from the record loop, then
`config.runtime_evidence.flush_keys(touched).await`. Diagnostic-only — must not alter the Ok result or
streaming success; unsampled queries never touch evidence I/O (D5).

## Phase 4 — Daemon wiring + refresh

In `ToronDaemon::start_with_client` (daemon.rs:131-183): build `with_wendelon(...)`, run one startup
`refresh_durable_cache().await` before coordinator construction, hand the Arc into `PlannerConfig` and
`MetricsRegistryConfig`, spawn the drop-safe daemon-owned refresh interval task (configurable, generous
default; pattern: the descriptor-sync loop). `start_with_wendelon_client_for_harness` (daemon.rs:141) gives
fixtures identical wiring.

## Phase 5 — `information_schema.optimizer_evidence_status`

One row: `mode`, `state`, `pending_deltas`, `dropped_deltas`, `ambiguous_deltas`, `refresh_failures`,
`last_error` (nullable). Files: binder.rs table constant + bind branch; `ir/execution.rs` `SystemTableKind`
variant; `binder/explain.rs` schema + row builder; SQL **and Rune** tests in binder/tests.rs. Querying the
status table records no decisions and no verdicts.

## Phase 6 — `/metrics` evidence health

`MetricsRegistryConfig` gains `runtime_evidence: Option<Arc<OptimizerRuntimeEvidence>>`; `render_text`
(observability.rs:243) gains `render_optimizer_evidence_metrics` beside the existing helpers:
pending/dropped/ambiguous/refresh-failure counts + `optimizer_evidence_durable_enabled{state="…"}` gauge.
Labels bounded to the state enum. No OTLP changes anywhere (guard-pinned). Update observability listener
tests / sample constructors for the new optional field.

## Phase 7 — Fixtures + guards

Live Wendelon (`wendelon_storage_execution_test.rs`, `measured_accrual_sample_rate = 1`):

| test | locks | fails under |
|---|---|---|
| `durable_runtime_verdict_counters_survive_coordinator_restart` | **honesty gate** — counts outlive the coordinator | `main` (process-local log reads zero after restart) |
| `cross_coordinator_verdicts_are_shared` | cluster visibility (second coordinator records nothing, sees the count after refresh) | per-node keys / local snapshots |

Local/fault-injected (fake binding where an rf>1 fixture is oversized — the architecture guard still pins
the real `COUNTER_REPLICATION_UNSUPPORTED` branch):
`render_never_double_counts_a_flushed_verdict` (fails under `durable + whole_local_log`),
`in_flight_flush_delta_remains_visible_once` (mid-flight snapshot counts once — fails if flush removes the
delta from view), `ack_flush_uses_larger_wins_cache_update` (out-of-order acks cannot regress),
`ambiguous_flush_failure_drops_delta_no_fresh_write_id`, `durable_lane_unavailable_query_still_succeeds`,
`manual_margin_mode_plans_byte_identical_with_durable_lane_enabled`,
`apply_advisory_uses_cached_durable_snapshot_once`, `counter_read_absent_maps_to_zero_in_consumer`,
`optimizer_evidence_status_reports_degraded_sink`, `metrics_render_optimizer_evidence_status`,
`rune_queries_optimizer_evidence_status`.

Architecture guards in `tests/architecture/optimizer_accrual.rs`: the fifteen `adr207_*` guards from the
ADR, plus the two named retargets — ADR 188's no-storage wording → "durable evidence flows only through the
ADR 075 counter facade"; ADR 191's resolve-once guard → also rejects evidence storage reads/awaits inside
bind helpers.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib runtime_evidence
scripts/cargo-cgroup test  -p toron --test wendelon_storage_execution_test durable_runtime_verdict
scripts/cargo-cgroup test  -p toron --test architecture_test adr207
scripts/cargo-cgroup test  -p toron --test architecture_test adr188
scripts/cargo-cgroup test  -p toron --test architecture_test adr191
scripts/cargo-cgroup test  -p toron --test observability_listener_test optimizer_evidence
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Hygiene (the bitten-before list)

- Counter values are *strictly increasing* — never the m-word in src/tests (the adr043 substring scan).
- The restart/cross-coordinator fixtures are LiveWendelonFixture-class: rerun a failing test in isolation
  3-5× before reporting a finding; the binary-level result is canonical.
- No postcard preimage changes expected → zero golden re-pins; a shifted golden is a finding, not a re-pin.
- All cargo through `scripts/cargo-cgroup`; plain type names, no V1 suffixes.
