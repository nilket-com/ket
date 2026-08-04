# Impl Plan 069: Toron Table-Query Trace Consumer

**Status:** Promoted, paired with
`plans/adr/0069_toron_table_query_trace_consumer.md`.
**Posture:** Consume Wendelon ADR 074's `TableQueryTraceContext` carrier:
populate `TableQueryOptions.trace_context` in `WendelonStorageScan` from the
`ScanRequest` so a sampled Toron query links `wendelon.table.query` under
`toron.storage.scan`. **Small + additive** — a derivation helper, one population
site, a `!enabled()`-first three-state mapping, and the producer-era guard
reconciliation. **No Wendelon production change; no `observability::telemetry`
drift.** Five phases.

## Invariants carried in

- Telemetry **disabled ⇒ `Absent` ⇒ byte-identical** to today's
  `TableQueryOptions::default()`; **disabled wins over a `Some`
  `ScanRequest.trace_context`**. Enabled+unsampled ⇒ `Unsampled` (no Wendelon
  re-sample); enabled+sampled ⇒ `Sampled` parented on the storage-scan
  continuation id (**never** the task span). Toron sends only
  `trace_id`+`parent_span_id` (no Wendelon child-id, no
  `traceparent`/`ComputeTraceContextV1`/`TaskSpec.trace`);
  no new span attributes; ADR-060 cancellation hook shape unchanged; storage
  results / runtime-filter / projection / row-identity / descriptor / error
  mapping unchanged. No Wendelon production change; `crates/nilket` runtime-free.

## Phase 0 — Audit (verified anchors)

- **Carrier (074):** `wendelon_client::TableQueryTraceContext { Absent,
  Unsampled, Sampled{trace_id:[u8;16], parent_span_id:[u8;8]} }`;
  `TableQueryOptions.trace_context` default `Absent`.
- **Population site:** `WendelonStorageScan::scan` builds
  `TableQueryOptions::default()` at `storage.rs:785`; one `options` value feeds
  both the ADR-060 attempt-hook call (`table_query_with_runtime_filter_and_
  attempt_hook`, `:814`) and the no-hook call (`table_query_with_runtime_filter`,
  `:842`).
- **Decision source:** `ScanRequest.trace_context: Option<storage::TraceContext>`
  (`storage.rs:61`) — `Some` iff sampled (068 presence-as-sampled), `{trace_id,
  span_id}` where `span_id` is the **task** span. `crate::telemetry::enabled()`
  (`telemetry.rs:309`) splits disabled from enabled-unsampled.
- **Parent id:** private `storage_scan_span_id(&ScanRequest)` (`telemetry.rs:736`)
  is the **exact** id `storage_scan_span` (`:448`) emits — reuse, don't recompute.
- **Guards to reconcile (build-fails once `storage.rs` references the carrier):**
  Wendelon `adr074_no_toron_production_change` (walks `crates/toron/src`, forbids
  `TableQueryTraceContext`/`trace_parent_span_id`/`TABLE_QUERY_TRACE_FLAG`/
  `wendelon.table.query`) and Toron
  `adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074`
  (`!storage.contains("TableQueryTraceContext")`).

## Phase 1 — Derivation helper (`telemetry.rs`)

- Add `pub(crate) fn storage_scan_trace_context(request: &ScanRequest) ->
  Option<TraceContext>` = `request.trace_context.map(|p| TraceContext {
  trace_id: p.trace_id, span_id: storage_scan_span_id(request) })`. Keep
  `storage_scan_span_id` **private**; `storage_scan_span` behavior unchanged. No
  `wendelon-client` import in `telemetry.rs`.

## Phase 2 — Three-state mapping (`storage.rs`, testable seam)

- Add `wendelon_table_query_trace_context_with_enabled(&ScanRequest, bool) ->
  wendelon_client::TableQueryTraceContext`: `!enabled ⇒ Absent`; else
  `storage_scan_trace_context(req)` → `Some ⇒ Sampled{trace_id, parent_span_id}`,
  `None ⇒ Unsampled`. Plus the thin `wendelon_table_query_trace_context(req)` =
  `…_with_enabled(req, crate::telemetry::enabled())`. The `_with_enabled` seam is
  the unit-test surface (no global-subscriber mutation).

## Phase 3 — Population wiring (`storage.rs`)

- After `let mut options = TableQueryOptions::default();` (`:785`) and **before**
  the cancellation branch (`:808`), set `options.trace_context =
  wendelon_table_query_trace_context(&request);`. Both call paths consume the
  same `options`. Touch nothing else (pbc, runtime filters, descriptor checks,
  row-identity, cancellation registration, error mapping unchanged).

## Phase 4 — Guard reconciliation (the producer-era flip)

- **Wendelon `adr074_no_toron_production_change`** (`wendelon/tests/architecture_
  test.rs`): retarget from "Toron forbids the carrier tokens" to its true
  intent — *ADR 074 made no Toron production change* — and assert the 069
  consumer owns population while Wendelon **production** is untouched. **Test-only
  Wendelon edit.**
- **Toron `adr068_…owned_by_wendelon_adr074`**: retarget the
  `!storage.contains("TableQueryTraceContext")` assertion to verify the consumer
  population (Sampled uses `storage_scan_span_id`; `!enabled→Absent`;
  `enabled+None→Unsampled`).

## Phase 5 — Guards, fixtures, evidence

- ADR's 16 + convention/meta guards in `toron/tests/architecture_test.rs` (incl.
  parent-equals-storage-scan-span-id, parent-is-not-task-span, set-once-on-shared-
  options, disabled-wins, span-id-stays-private, no-duplicate-hash,
  hook-shape-preserved, no-Wendelon-production-change, only-table-query-path, the
  reconciliation guard).
- Seam-level fixtures (Phase 2 `_with_enabled` mappings — sampled/unsampled/absent
  + the task-span-trap negative + disabled-byte-identical + both-paths-share-
  options); storage-result + cancellation tests unchanged.
- `plans/impl/0069_toron_table_query_trace_consumer_evidence.md`:
  `architecture_test adr069`, full `architecture_test` (toron), **`adr068` +
  Wendelon `adr074` suites green after reconciliation**, Wendelon 074
  carrier/OTLP-absent fixtures unchanged, `cargo test -p toron`, `cargo check -p
  toron`, `clippy -D warnings`, `git diff --check`.

## Watch-items

1. **The task-span trap** — `request.trace_context.map(Sampled)` looks right and
   parents Wendelon under `toron.task`. The parent **must** be
   `storage_scan_trace_context(request).unwrap().span_id`; guard it.
2. **Disabled wins** — `!enabled() ⇒ Absent` even with a `Some` context (a
   disabled node emits no `toron.storage.scan`); the `_with_enabled` seam makes
   this directly testable.
3. **Guard reconciliation is mandatory** — the build fails when `storage.rs`
   references the carrier until both 074-era guards are retargeted (one is in
   `wendelon/tests`; test-only).
4. **One options value** — set `trace_context` once at `:785`; don't branch-set.
5. **Reuse the private hash** — call `storage_scan_span_id` via the semantic
   helper; don't duplicate `STORAGE_SCAN_SPAN_DOMAIN`/blake3 in `storage.rs`, and
   don't widen `storage_scan_span_id`'s visibility.

## Acceptance checklist

- [ ] Phase 1: `storage_scan_trace_context` helper; `storage_scan_span_id` stays
      private; no `wendelon-client` dep in `telemetry.rs`.
- [ ] Phase 2: `_with_enabled` three-state mapping (disabled-first); thin wrapper.
- [ ] Phase 3: `options.trace_context` set once at `:785`; both paths share it.
- [ ] Phase 4: both 074-era producer-only guards retargeted (Wendelon test-only).
- [ ] Phase 5: guards + seam fixtures + evidence; 068 + 074 suites green;
      Wendelon production untouched.
