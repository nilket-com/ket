# ADR 207: Durable Optimizer Evidence / Telemetry

**Status:** Promoted. **Migration:** `plans/impl/207_durable_optimizer_evidence_telemetry.md`.
**Category:** Toron-home consumer of Wendelon ADR 075 (`COUNTER_ADD` / `counter_read`) — the cross-ring
producer/consumer pair 075 D9 names explicitly ("Toron ADR 207 resumes as fixed-key `fetch_add(+1)` + point
reads"). The 072/073 precedent: Wendelon owns the storage primitive; Toron owns the purpose.

## Why this cut exists

The 187-206 arc built an optimizer that keeps receipts: every shape decision accrues, and measured verdicts
(Vindicated / MissedWin / OverEager / MisRanked / Indeterminate) judge whether actuation was right. But the
receipts live in one process's `Mutex<BTreeMap>` (`decision_log.rs:79`), and Toron coordinators are assumed
unstable on a days horizon. A coordinator restart erases every verdict it ever earned, and no coordinator
can see another's. The forward arc's end state ("memo only when receipts show local decisions fighting")
needs measured evidence that outlives one writer and can be read by other coordinators.

"Evidence" and "Telemetry" in the title are two halves: **evidence** is the closed loop (durable counters the
189/191 advisory can read back); **telemetry** is the open loop (a queryable status surface + `/metrics` so a
human can see the sink's health).

## Grounding (verified against the code)

- Verdict accrual is **sampled, default off** (`measured_accrual_sample_rate: 0`, binder.rs:151; gate
  `should_measure_runtime_verdict`, coordinator.rs:1486). Both recording sites run after execution success:
  the streaming path records after `dispatch_loop` completes (coordinator.rs:1330-1343), while the local path
  records after rows are materialized but before returning them to the caller (~:2450). The durable write
  rate is operator-throttled; only sampled local queries can be delayed, and only by the flush timeout.
- The verdict keyspace is **closed**: 7 real `OptimizerDecisionKind`s (`Overflow` excluded — the verdict lane
  cannot record it, `decision_log.rs:114-126`) × 5 `OptimizerRuntimeVerdictClass`es = **35 keys**.
- Wendelon ADR 075 names the Toron-facing facade as `Client::counter_fetch_add(...)`,
  `Client::counter_read(...)`, and `counter_identity_for_key`. Current source anchors for the underlying
  wire path are `crates/wendelon/src/client/core.rs:1287` (`counter_add`) and `:1347` (`counter_read`);
  `CounterAddRequestMeta` carries the `ClientWriteId` at `crates/wendelon/src/schema/core.rs:1046`. If the
  high-level fetch-add facade is not present when ADR 207 implementation starts, finish that ADR 075 facade
  first; Toron must not hand-roll raw counter frames or mint ad hoc write ids.
- FAA responds with `previous_value`/`current_value` — every flush ack refreshes that key's cache for free.
- Advisory resolves **once at bind entry, synchronously**, from `verdict_snapshot()` at exactly two call
  sites (`effective_join_order_soft_margin_step` join_order.rs:180-193; `margin_advisory_report` :204-218).
- Daemon startup owns `Arc<wendelon_client::Client>` + tenant before building the coordinator
  (daemon.rs:131-183).
- **Toron has no OTLP metrics API**: `telemetry.rs` is span/trace export end-to-end (`build_otlp_dispatch`,
  `DroppedSpanObserver`); the operator metric surface is the `/metrics` text registry
  (`network/observability.rs:166` `MetricsRegistry`, `render_text` :243). This settled the council's one
  late fork (see Consensus).
- Wendelon rejects `COUNTER_ADD` on unsupported replicated deployments with
  `COUNTER_REPLICATION_UNSUPPORTED` **before mutation** (data_server.rs:7733) — the consumer must degrade
  visibly, never fail queries.

## Decisions (promotion-locked)

**D1 — Extract `OptimizerRuntimeEvidence`; the decision lane stays local.** Runtime-verdict evidence moves
out of `OptimizerDecisionLog` into an explicit handle carried by `PlannerConfig` as
`Arc<OptimizerRuntimeEvidence>` beside `decision_log`. It owns a local **pending** lane, a **durable cache**
lane, an `Option<DurableEvidenceBinding { client, tenant, flush_timeout }>`, and a status block. The 187
plan-time decision lane, recorder trait, and `NoOpDecisionRecorder` ZST are untouched. Decisions explain what
the optimizer did; verdicts are the measured feedback that must survive the writer — only verdicts persist.

**D2 — Fixed 35-key counter matrix; absence maps to zero in the consumer.** Durable keys are
`optimizer_evidence/verdict/{kind_label}/{verdict_label}` via the existing label fns
(`decision_log.rs:186/:223`). Reads are point reads of every known key; `counter_read → None` maps to `0` in
Toron (075 D6: storage tells the truth about existence; the consumer owns the default). No prefix scan,
registry, per-node key, descriptor sidecar, or dynamic discovery — the matrix is enumerable at compile time.

**D3 — Snapshot = `durable_cache + pending`; atomic lane migration; in-flight stays visible; zero rows
omitted.** `record_verdict` increments pending only. A flush moves a key's unflushed delta into an
**in-flight** slot (conceptually still the pending lane — a snapshot taken mid-flight still counts it), calls
one `counter_fetch_add`, and on ack — in one critical section — clears the in-flight delta and updates the
cache. A verdict counts exactly once while unflushed, once while in flight, and once after the ack lands in
the cache. Zero-valued keys render no row (today's shape, even though refresh materializes all 35 keys). With
no binding, the render is exactly the pre-207 local behavior.

**D4 — Ack refresh is larger-wins.** On a successful flush, `durable_cache[key] = max(cached,
receipt.current_value)`. Counter values strictly increase, so larger-wins makes out-of-order acks (two
concurrent sampled queries) and overlapping refreshes harmless: the cache can transiently under-read, never
double-count or regress; the periodic refresh heals any under-read.

**D5 — Awaited post-success flush with a short timeout; no queue, no coalescer.** After a sampled successful
query records verdicts, the coordinator awaits `flush_keys` for the unique touched keys. The streaming path
flushes after dispatch completes; the local path flushes after rows are materialized and can delay the sampled
return by at most the flush timeout. Flush results are diagnostic-only and unsampled queries never do evidence
I/O. Grounding kills the coalescer's motivation: the write rate is sample-gated (default 0), so a background
queue buys little and costs drain-on-shutdown hooks, test nondeterminism, and a crash window of silently queued
evidence. For a durable-capable sampled query, the restart gate pins "recorded means attempted-durable before
the process can forget it." Coalescing returns as a named follow-on only if an always-on verdict lane lands.

**D6 — Undercount beats double-count; attempted means ambiguous.** One flush = one facade call = one
write_id; the facade's internal budget is the only retry. Any failure **after the call was attempted**
(error, timeout) is ambiguous — the add may have applied — so the in-flight delta is **dropped**,
dropped/ambiguous diagnostics increment, and the delta is never re-issued under a fresh write_id.
Restore-to-pending is reserved for deltas whose call was never made (no binding, latched off, zero delta).
A caller-owned write_id path (exactly-once retry) is the named follow-on.

**D7 — Cached refresh feeds reports and opt-in advisory; bind never awaits storage.** Daemon startup
point-reads all 35 keys; every flush ack refreshes its key for free; a daemon-owned background interval keeps
read-mostly coordinators aware of other writers. Refresh failure keeps the old cache and marks the lane
stale — never surfaces to queries. Both advisory call sites consume the handle's synchronous combined
snapshot unchanged in shape: ApplyAdvisory becomes cluster-history-aware only for operators already opted
into history-dependent planning; the default (`Manual`) binds byte-identical plans. The 191 resolve-once
discipline is re-pinned (the comment at join_order.rs:176-179 stays true verbatim).

**D8 — Telemetry home: `information_schema.optimizer_evidence_status` + `/metrics`; no OTLP this cut.** One
diagnostic row — mode (`local_only`/`durable`/`degraded`), state
(`ok`/`stale`/`replication_unsupported`/`unavailable`), pending/dropped/ambiguous deltas, refresh failures,
last error — queryable from SQL **and Rune** (Rune-primary discipline), with the same bounded, state-derived
health rendered on the existing `/metrics` text surface. The status table is the queryable home; `/metrics`
is the scrape surface — different consumers, not duplication. OTLP optimizer counters are **deferred**: the
grounded OTLP substrate exports traces only; a metrics export is its own observability ADR.

**D9 — Degraded modes are explicit, visible, fail-open.** `COUNTER_REPLICATION_UNSUPPORTED` (rf>1) latches
durable writes off for the process. Other durable sink failures mark the lane `unavailable`/`stale`; after an
implementation-defined circuit-breaker threshold, later flush attempts may be skipped until a successful
refresh or probe re-enables them. In both cases, newly recorded evidence remains locally visible instead of
being repeatedly dropped through a known-bad sink. Queries are never failed and sampled queries are never
delayed beyond the flush timeout. The state is visible in D8's surfaces. `Option<DurableEvidenceBinding>` is
the structural off-switch; the latch is reserved for runtime-observed sink state. Wendelon multi-node counter
replication remains a Wendelon follow-on.

**D10 — Boundaries.** No Wendelon production change. No executor / fragmenter / storage-scan / PBC / wire /
grammar change; no memo / Cascades / cost-model machinery. No new postcard preimage type (keys are Wendelon
KV bytes) — zero golden re-pins expected. Plans and EXPLAIN receipts byte-identical under Manual; existing
`optimizer_runtime_verdicts` columns unchanged in semantics.

## Honesty gate

`durable_runtime_verdict_counters_survive_coordinator_restart`: against a live single-node Wendelon fixture,
a sampled measured query accrues and flushes a runtime verdict; a fresh coordinator/evidence handle on the
same tenant refreshes and reports the count **without any post-restart recording**. Must **fail on `main`**
(the log is process-local; a restart reads zero).

**Load-bearing discriminators:** `cross_coordinator_verdicts_are_shared` (kills per-node-key or
local-snapshot regressions); `render_never_double_counts_a_flushed_verdict` (fails under the naive
`durable + whole_local_log` render); `in_flight_flush_delta_remains_visible_once` (a mid-flight snapshot
neither drops nor doubles the delta); `ack_flush_uses_larger_wins_cache_update` (out-of-order acks cannot
regress the cache); `ambiguous_flush_failure_drops_delta_no_fresh_write_id` (undercount allowed,
duplicate-add impossible); `durable_lane_unavailable_query_still_succeeds` (fail-open pinned);
`counter_read_absent_maps_to_zero_in_consumer` (absent keys produce no rows);
`manual_margin_mode_plans_byte_identical_with_durable_lane_enabled` (the D7 opt-in boundary);
`apply_advisory_uses_cached_durable_snapshot_once`; `optimizer_evidence_status_reports_degraded_sink`;
`metrics_render_optimizer_evidence_status`; `rune_queries_optimizer_evidence_status`.

## Architecture guards (promotion-locked)

- `adr207_verdict_evidence_keys_are_fixed_matrix_no_registry`
- `adr207_counter_absent_maps_to_zero_in_consumer`
- `adr207_snapshot_is_durable_cache_plus_pending_not_local_plus_durable`
- `adr207_in_flight_deltas_are_not_reissued_or_hidden`
- `adr207_ack_flush_uses_larger_wins_cache_update`
- `adr207_flush_is_awaited_post_success_not_background_queue`
- `adr207_no_fresh_write_id_after_ambiguous_flush`
- `adr207_uses_counter_fetch_add_facade_not_raw_counter_frames`
- `adr207_bind_path_never_awaits_storage_for_evidence`
- `adr207_decision_lane_stays_local`
- `adr207_replication_unsupported_latches_degraded`
- `adr207_status_surface_and_metrics_report_sink_health`
- `adr207_no_otlp_optimizer_metric_expansion`
- `adr207_no_wendelon_production_change`
- `adr207_evidence_failure_never_fails_queries`

**Reconciled prior guards (named retargets):** ADR 188's no-storage wording retargets to "durable evidence
flows only through the ADR 075 counter client facade; no executor / fragmenter / Wendelon production
change". ADR 191's resolve-once guard additionally rejects evidence storage reads or awaits inside bind
helpers.

## Rejected alternatives

**Background coalescer now** — write rate is sample-gated and default off; awaited flush is simpler,
deterministic, and drain-free. **Persist plan-time decisions too** — diagnostics, not the measured feedback
loop; follow-on. **OTLP optimizer counters now** — assumes a metrics substrate the code doesn't have.
**Blob/table writes** — read-modify-write loses concurrent increments; a table adds scan machinery for a
35-key matrix. **Retry ambiguous flushes with a fresh call** — fresh write_id, can double-count.
**Await Wendelon during bind** — violates the 191 resolve-once boundary.

## Consensus (transparent)

3-of-3 on the full spine across Codex iter-3 / Gemini iter-3 / Claude iter-2: fixed matrix,
verdict-lane-only durability, extracted handle, lane migration with free ack-refresh, awaited post-success
flush, attempted-means-ambiguous undercount discipline, cached synchronous advisory, degraded fail-open
latch, status table + `/metrics`. The convergence trail, credited: Codex conceded his background coalescer
to the write-rate grounding; Claude conceded his pre-dispatch restore carve-out to Codex's sharper
attempted-means-ambiguous rule and his single-home framing to Codex's dedicated status table; Gemini
conceded OTLP counters when grounding showed `telemetry.rs` exports traces only — the one late fork,
resolved by code, not headcount. Codex's iter-3 in-flight-visibility clarification (D3) and Claude's
larger-wins ack (D4) are both folded. Prior cycle context: this topic was framed before Wendelon ADR 075
and stashed for its producer primitive (075 "Seed"/D9); this cut is the resumed consumer.

## Forward arc

`075 counter producer → 207 durable verdict evidence/telemetry (this) → caller-owned write_id exactly-once
flush (if undercount ever matters) → evidence-ordered optimizer bets (drive-from-middle, peak-vs-sum,
memo/search — only when durable receipts show local decisions fighting) → Wendelon rf>1 counter replication
when a deployment needs it`.

## Deferred (named)

Durable 187 decision-lane counts; OTLP optimizer metrics (needs a metrics-substrate ADR); caller-owned
write_id exactly-once flush; Wendelon multi-node counter replication; background coalescing (iff always-on
accrual lands); counter reset / evidence decay; persisted tuned margins; per-kind advisory policy;
memo/search; drive-from-a-middle-bridge; peak-vs-sum chain costing (needs a mis-ranking fixture);
multi-region joint actuation (still unreachable).
