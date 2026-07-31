# ADR 069: Toron Table-Query Trace Consumer

**Status:** Promoted (plan) — **3-of-3 (Codex, Gemini, Claude)** on topic and the
full design; one minor API nit resolved 2-of-3 (keep `storage_scan_span_id`
private — Codex+Claude — vs Gemini's also-`pub(crate)`; recorded in Council
History).
**Seed:** Toron ADR 068 made distributed Toron traces real through `toron.query
→ dispatch → task → storage.scan` but **stopped at the storage boundary**
(`wendelon-client::TableQueryOptions` had no trace carrier). Wendelon ADR 074
added the producer/carrier and **deferred Toron population to this consumer**
(the 072→Toron-053 / 073→Toron-060 producer/consumer pattern). 069 closes the
ADR 068 → Wendelon 074 → 069 trace chain.
**Category:** Toron compute-ring **consumer** of a Wendelon producer surface;
Toron-home. **No Wendelon production change; no new `observability::telemetry`.**

## Summary

Populate `wendelon_client::TableQueryOptions.trace_context` in
`WendelonStorageScan` from the `ScanRequest`, so a sampled Toron query produces
the linked `wendelon.table.query` child span under `toron.storage.scan`. Small +
additive: one derivation helper, one mapping helper, one assignment, and a
producer-era guard reconciliation. The correctness hinge is parentage; the
subtlety is disabled-vs-unsampled.

## Decisions (13, Promotion-Locked)

**D1 Consumer-only, Toron-home.** Production edits are Toron-only (plus the
cross-ring architecture-guard retargets in §Prior-Guard Reconciliation). No
Wendelon **production** change, no protocol-layout change, no
`observability::telemetry` drift, no SQL-wire / audit / Prometheus / non-table-
query change.

**D2 Three-state mapping, `!enabled()` checked first.** A testable seam (no
global-subscriber mutation in tests):
```rust
// storage.rs
fn wendelon_table_query_trace_context(request: &ScanRequest)
    -> wendelon_client::TableQueryTraceContext {
    wendelon_table_query_trace_context_with_enabled(request, crate::telemetry::enabled())
}
fn wendelon_table_query_trace_context_with_enabled(
    request: &ScanRequest, telemetry_enabled: bool,
) -> wendelon_client::TableQueryTraceContext {
    if !telemetry_enabled {
        return wendelon_client::TableQueryTraceContext::Absent;
    }
    match crate::telemetry::storage_scan_trace_context(request) {
        Some(c) => wendelon_client::TableQueryTraceContext::Sampled {
            trace_id: c.trace_id,
            parent_span_id: c.span_id,
        },
        None => wendelon_client::TableQueryTraceContext::Unsampled,
    }
}
```

**D3 Disabled wins over context presence.** `!enabled()` ⇒ `Absent` **even if
`ScanRequest.trace_context` is `Some`** — a disabled local node emits no
`toron.storage.scan` (`storage_scan_span` returns `Span::none()`), so a Wendelon
child parented on a span this node never exported would be a broken tree. Also
required for mixed-enabled deployments + disabled byte-equivalence.

**D4 Enabled + `None` ⇒ authoritative `Unsampled`.** Preserves ADR 068's
no-resample rule at the Wendelon boundary (Wendelon must not root-sample a query
Toron head-sampled out).

**D5 Enabled + `Some` ⇒ `Sampled` on the storage-scan parent.** `trace_id =
ctx.trace_id`, `parent_span_id =
storage_scan_trace_context(request).unwrap().span_id` — the derived
`storage_scan_span_id(request)` value, surfaced through the semantic helper.

**D6 Parent = the storage-scan continuation id, never the task span.** The load-bearing
correction: the plausible-wrong impl `request.trace_context.map(Sampled)` uses
the incoming **task** span id and parents `wendelon.table.query` under
`toron.task`, skipping `toron.storage.scan`. The `parent_span_id` Toron sends
must equal the id 068's `storage_scan_span` emits **and** the value Wendelon 074
derives its child span id from.

**D7 Helper split; `storage_scan_span_id` stays private.** `telemetry.rs` exposes
**only** `pub(crate) fn storage_scan_trace_context(&ScanRequest) ->
Option<TraceContext>`, reusing the **private** `storage_scan_span_id` (no
duplicate hash; no `wendelon-client` dep in `telemetry.rs`). It is a *derivation*
helper; the three-state *policy* lives in `storage.rs` (D2). *(Codex+Claude:
expose the semantic helper, not the hash primitive — Gemini's iter-4 also
`pub(crate)`'d `storage_scan_span_id`; the narrower surface keeps the recipe
encapsulated and is guarded by `..._remains_private_to_telemetry`.)*

**D8 One shared `options` value.** Assign `options.trace_context` once right
after `TableQueryOptions::default()` (`storage.rs:785`), **before** the
cancellation branch — so the ADR-060 attempt-hook path (`:814`) and the no-hook
path (`:842`) get identical options **by construction** (parity is free, not
enforced across two sites).

**D9 Cancellation parity.** ADR 060's attempt hook is structurally unchanged
(same `WendelonTableQueryAttempt` registration, same request-id capture, same
pre-write cancel errors); trace options are orthogonal.

**D10 No Toron-derived Wendelon span id; no legacy carriers.** Toron sends only
`trace_id`+`parent_span_id` (Wendelon 074 owns child-id derivation incl.
`request_id`). No `traceparent` string, no `ComputeTraceContextV1`, no
`TaskSpec.trace` revival, no sidecar/extension.

**D11 Privacy unchanged.** The carrier conveys only trace ids; 069 adds **no**
span attributes (no SQL/literal/table/tenant/descriptor/query-id/request-id/peer/
error-string). ADR 074 owns Wendelon's sanitized linked span.

**D12 Behavior-equivalent outside telemetry.** Storage results, runtime-filter
payloads, projection, row-identity, descriptor-evolution checks, cancellation
outcomes, and error mapping are unchanged. `!enabled()` ⇒ `Absent` ⇒
byte-identical to today's `TableQueryOptions::default()`.

**D13 Table-query read path only; carry-forward.** Only `WendelonStorageScan`'s
`TableQueryOptions.trace_context` is populated. The sibling `trace_context`
fields on the write/delete/etc. request structs (`storage.rs:76/89/102`) stay
unpopulated until those Wendelon messages grow carriers (future consumers).

## Architecture Guards (16 + convention/meta)

`adr069_wendelon_trace_consumer_is_toron_only`,
`adr069_only_storage_scan_populates_table_query_trace_context`,
`adr069_trace_context_set_once_on_shared_options`,
`adr069_disabled_local_telemetry_sends_absent_even_with_context`,
`adr069_enabled_unsampled_sends_unsampled_carrier`,
`adr069_enabled_sampled_sends_sampled_carrier`,
`adr069_parent_span_id_equals_storage_scan_trace_context_span_id`,
`adr069_parent_span_id_is_not_request_trace_context_span_id`,
`adr069_storage_scan_span_id_remains_private_to_telemetry`,
`adr069_storage_rs_does_not_duplicate_storage_scan_hash_recipe`,
`adr069_cancellation_attempt_hook_shape_is_preserved`,
`adr069_no_toron_derived_wendelon_table_query_span_id`,
`adr069_no_traceparent_compute_trace_context_v1_or_taskspec_trace`,
`adr069_no_sql_tenant_table_descriptor_peer_queryid_attrs`,
`adr069_no_wendelon_production_change`,
`adr069_only_table_query_read_path_carrier_populated`.
**Convention/meta:** `adr069_nilket_root_stays_runtime_free`,
`adr069_reconciles_074_producer_only_toron_population_guards`,
`adr069_adr068_and_adr074_guards_remain_green` (modulo the reconciliation).

## Prior-Guard Reconciliation

069 intentionally flips the two 074-era "Toron-doesn't-populate-yet" guards
(they fail at build time once `storage.rs` references the carrier):
1. **Wendelon `adr074_no_toron_production_change`** (`crates/wendelon/tests/
   architecture_test.rs`) walks `crates/toron/src` and **forbids**
   `TableQueryTraceContext` / `trace_parent_span_id` / `TABLE_QUERY_TRACE_FLAG` /
   `wendelon.table.query`. Its true intent — "*ADR 074* changed no Toron
   production" — is a provenance statement, not a standing prohibition on the
   named consumer. **Retarget** to assert Wendelon production is unchanged + the
   069 consumer owns population; **test-only edit to the Wendelon crate,
   production untouched** (D1 holds).
2. **Toron `adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074`**
   asserts `!storage.contains("TableQueryTraceContext")` — retarget to verify the
   consumer population exists and is well-formed (D5/D6).
*(Lesson for producer ADRs: a "consumer-doesn't-exist-yet" check should be a
one-time/provenance guard, not a standing source-scan forbidding the token the
planned consumer must use.)*

## Integration Fixtures (seam-level + composed)

`with_enabled(req, false)` ⇒ `Absent` even when `req.trace_context` is `Some`;
enabled + `None` ⇒ `Unsampled`; enabled + `Some` ⇒ `Sampled` with `parent_span_id
== storage_scan_trace_context(req).span_id`; sampled fixture proves `parent_span_id
!= req.trace_context.unwrap().span_id` (the task-span trap); single populated
`options` flows to both table-query call paths; disabled is byte-identical to
default; storage-result + ADR 060 cancellation tests unchanged; **ADR 068 +
Wendelon 074 carrier/absent fixtures green** after the reconciliation. The
end-to-end `…→ storage.scan → wendelon.table.query` chain is proven
**compositionally** (Toron carrier-out fixture + 074's carrier-in tests), with a
cross-process OTLP fixture optional (not a hard bar).

## Out of Scope

Write/delete/other-message trace carriers (future consumers); a cross-process
multi-ring OTLP integration test as a hard bar; any Wendelon production change;
SQL-wire trace injection; the deferred `UPDATE`-without-`WHERE` (a future Toron
ADR).

## Council History

iter-0: drafts on the consumer topic (Gemini's iter-0 was still
`UPDATE`-without-`WHERE`; it moved onto this consumer at iter-1, closing the
numbering fork — 069 = the consumer). iter-1: the `None`-mapping fork resolved to
the `enabled()`-based three-state split (the only no-op-when-disabled *and*
no-resampling-hole mapping). iter-2: Codex added the `_with_enabled` testable
seam; the two-guard reconciliation specified; 3-of-3 on the design. iter-3/4:
promotion-grade duals. The lone residual is the §D7 API nit (private
`storage_scan_span_id` + semantic helper — Codex+Claude — vs Gemini's also-
`pub(crate)` primitive); promoted on the narrower surface.

## Promotion criteria met

D-set 13; 16 + convention/meta guards; consumer-only (no Wendelon production / no
substrate drift); the load-bearing parent is the storage-scan continuation id;
disabled-wins three-state mapping; hook/no-hook parity by construction; the
producer-era guard reconciliation specified. Migration:
`plans/impl/069_toron_table_query_trace_consumer.md`. Closes the ADR 068 →
Wendelon 074 → 069 trace chain.

## Conclusion

The Toron-home consumer of Wendelon 074: a `!enabled()`-first three-state mapping
populating `TableQueryOptions.trace_context` once at `storage.rs:785`, parented
on the storage-scan continuation id (never the task span), reusing the private
derivation helper, with the two 074-era producer-only guards reconciled. Small,
additive, no-op when disabled — it lights up the end-to-end Toron→Wendelon trace
that 068/074 deliberately deferred here.
