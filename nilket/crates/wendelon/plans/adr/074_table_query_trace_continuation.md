# Wendelon ADR 074: Table-Query Trace Continuation (Producer)

**Status:** Promoted (plan) — **2-of-3 (Codex, Claude)** on the producer/consumer
split and local-span preservation; Gemini's parallel iter-4 held the
fold-Toron / sanitized-only-replacement line (recorded in Council History).
**Seed:** Toron ADR 068 §D18 — Toron OTLP tracing stopped at `toron.storage.scan`
because `wendelon-client::TableQueryOptions` and `TableQueryRequestMeta` had no
trace carrier. 074 is the Wendelon-owned producer that closes the cross-ring gap.
**Placement:** Wendelon sequence — Exterior Substrate. Drafted in the Toron
cross-ring council (working label "ADR 069"), relocated + renumbered to Wendelon
074 per `../../../toron/plans/conventions/ring-product-placement.md` (code-locality
+ server-side parentage) and the ADR 071/072/073 precedent.
**Category:** Wendelon storage-ring — table-query telemetry continuation;
**producer-only** (no Toron production change), with a cross-referenced Toron
consumer follow-up.

## Summary

Carry Toron's trace context through `TABLE_QUERY_REQ` by widening the fixed
`TableQueryRequestMeta` with a binary trace tail, giving Wendelon a producer
surface that a Toron consumer can parent under `toron.storage.scan`. Wendelon
**consumes the ADR 068 external-context seam** (`otel.*`) — no new shared
substrate. The privacy line (no SQL/PII) holds; tracing is behavior-equivalent
when absent.

## Decisions (14, Promotion-Locked)

**D1 Producer-only scope.** 074 changes `wendelon` + `wendelon-client` only:
`TableQueryOptions`, `TableQueryRequestMeta`, table-query request decode, and the
Wendelon table-query span. **No Toron production code lands here** (the 072/073
producer precedent).

**D2 Three-state carrier.** Brand-clean `wendelon-client` type:
`pub enum TableQueryTraceContext { Absent, Unsampled, Sampled { trace_id:[u8;16],
parent_span_id:[u8;8] } }`; `TableQueryOptions::default() = Absent`. `Absent` =
direct/legacy client (Wendelon keeps current behavior); `Unsampled` = upstream
no-sample decision is authoritative; `Sampled` = continue under the supplied
parent. (The ADR 068 worker no-resample rule at the ring boundary.)

**D3 Fixed binary tail in `TableQueryRequestMeta`.** Widen the Pod struct at the
tail, **136 → 168 bytes, prefix offsets unchanged**: `trace_id:[u8;16]` (off
136), `trace_parent_span_id:[u8;8]` (off 152), `trace_flags:u8` (off 160),
`_trace_pad:[u8;7]` (off 161). Update the `size_of == 136` assertion (table.rs:320)
→ 168 and pin the new offsets. Flags `0x01` present, `0x02` sampled. **No
traceparent string, no Postcard, no trailing-extension/sidecar, no
`TableQueryRequestMetaV1`, no `ComputeTraceContextV1`** (the 073 no-premature-
compat posture).

**D4 Fail-open carrier validation.** A malformed *present* carrier is treated as
`Unsampled`, **never** a framing/program error: `sampled` with zero `trace_id`,
`sampled` with zero `parent_span_id`, unknown flag bits, impossible combinations.
Telemetry must not reject or alter `TABLE_QUERY_REQ`.

**D5 Wendelon derives the linked span id.** For `Sampled`:
`blake3("wendelon.table_query.span.v1" ‖ trace_id ‖ parent_span_id ‖
request_id)[0..8]`, nonzero-guarded. The 073 per-attempt `TableQueryRequestId`
gives retries distinct spans; `request_id` is **never** exported as an attribute.

**D6 Reuse the ADR 068 explicit-context seam.** The linked span sets only
`otel.{trace_id, parent_span_id, span_id, sampled}`; the shared layer adopts +
strips them and treats `otel.sampled` as authoritative. **No new
`observability::telemetry` API or behavior.**

**D7 Span behavior — preserve Wendelon's local span (the iter-3 red-team
resolution; Codex iter-5 concurred).** The privacy line only requires raw ids
stay out of *Toron's* trace, not that Wendelon delete its local span:
- `Absent`: the existing `data_ring.table_query.request` span is **unchanged**.
- `Sampled`: emit a sanitized linked `wendelon.table.query` span (D6/D8); the
  raw `data_ring.table_query.request` span **may be preserved as a
  Wendelon-local, *unlinked*** span (its own trace id, Wendelon's own sampling).
- `Unsampled`: emit no linked span and **do not root-sample** the raw local span
  for this request.

**D8 Sibling-not-nested invariant.** If both the local raw span and the linked
span exist for a `Sampled` request, they are **siblings in different traces**,
never parent/child — the raw span must **not** be created while
`wendelon.table.query` is the current tracing span (else in-process contextual
parenting would pull raw ids into Toron's trace).

**D9 Linked-span privacy.** Allowed: bounded classes (consistency, row-identity
projection, runtime-filter state, planner path, outcome, cancellation outcome),
raw magnitudes (rows, bytes, elapsed micros, batch counts), bounded error class.
**Forbidden:** SQL/literals, table/column names, tenant id, table object id,
descriptor fingerprint, request id, Toron query id, peer address, free-form
error strings.

**D10 Sampling authority.** Upstream `Unsampled` is authoritative — Wendelon does
**not** root-sample those requests even at its own sampling rate 1.0. Only
`Absent`/direct-client requests keep Wendelon's independent root sampling.

**D11 Behavior equivalence when absent.** `Absent` preserves current Wendelon
table-query + OTLP HTTP/gRPC behavior **exactly** (the regression net — those
fixtures pass unchanged). Wendelon telemetry disabled exports nothing regardless
of carrier state. **No new feature gate** (existing `telemetry`/`otlp-*`).

**D12 Compatibility posture (073).** Client + server move together; size/offsets
pinned; no mixed-version extension format (a separate protocol-negotiation ADR if
ever required).

**D13 No drift.** No Control-Ring, SQL-wire, audit, cancellation-protocol, OTLP-
metrics, OTel-SDK, or non-table-query-message change. `crates/nilket`
runtime-free; fixture worker unchanged; new surface brand-clean (no `*V1`).

**D14 Carry-forward.** The Toron consumer (populate
`TableQueryOptions.trace_context` from `ScanRequest`, parent = `toron.storage.
scan`) is the next **Toron-sequence** ADR, cross-referenced — not owned here
(072→Toron 053 / 073→Toron 060 pattern). Gemini's `UPDATE`-without-`WHERE` is the
parallel Toron-069.

## Architecture Guards (16 + 3 convention/meta)

`adr074_table_query_options_trace_context_is_absent_by_default`,
`adr074_table_query_trace_context_has_absent_unsampled_sampled_states`,
`adr074_table_query_trace_carrier_is_fixed_binary_not_traceparent`,
`adr074_table_query_meta_widens_tail_without_extension_or_v1_sidecar`,
`adr074_trace_meta_offsets_are_pinned`,
`adr074_wendelon_span_id_is_derived_nonzero_from_request_attempt`,
`adr074_linked_span_uses_explicit_otel_context_fields`,
`adr074_unsampled_carrier_bypasses_root_sampling`,
`adr074_sampled_emits_sanitized_linked_span_local_raw_span_preserved`,
`adr074_local_raw_span_is_never_nested_under_linked_span`,
`adr074_linked_span_has_no_tenant_table_request_descriptor_query_attrs`,
`adr074_malformed_trace_carrier_is_fail_open_unsampled`,
`adr074_no_new_shared_observability_telemetry_seam`,
`adr074_absent_context_preserves_wendelon_otlp_behavior_fixtures`,
`adr074_no_toron_production_change`,
`adr074_no_control_ring_sql_wire_audit_cancellation_or_non_table_query_drift`.
**Convention/meta:** `adr074_new_trace_surface_is_brand_clean_no_v1_suffix`,
`adr074_carrier_does_not_transmit_compute_trace_context_v1`,
`adr074_prior_wendelon_telemetry_and_table_query_guards_continue_to_pass`.

## Integration Fixtures (16)

`table_query_options_default_trace_context_absent`;
`table_query_meta_absent_roundtrips_zero_flags`;
`table_query_meta_sampled_roundtrips_binary_trace_context`;
`table_query_meta_unsampled_roundtrips_explicit_suppression`;
`sampled_zero_trace_id_fails_open_unsampled`;
`sampled_zero_parent_span_id_fails_open_unsampled`;
`unknown_trace_flags_fail_open_unsampled`;
`sampled_carrier_exports_sanitized_wendelon_table_query_span`;
`sampled_linked_span_has_no_raw_identity_attrs`;
`sampled_local_raw_span_is_sibling_not_child_of_linked_span`;
`unsampled_carrier_suppresses_linked_and_raw_root_export_at_rate_one`;
`sampled_carrier_exports_linked_span_at_rate_zero`;
`malformed_trace_carrier_does_not_reject_table_query`;
`absent_context_preserves_wendelon_otlp_http_fixture`;
`absent_context_preserves_wendelon_otlp_grpc_fixtures`;
`cargo_check_wendelon_feature_matrix`.
*(The end-to-end `toron.query → … → storage.scan → wendelon.table.query`
single-trace fixture belongs to the Toron consumer ADR — 074 is producer-only.)*

## Out of Scope

Toron population of the carrier (→ Toron consumer ADR); trace on non-table-query
messages; `traceparent`/`tracestate` wire strings; SQL-wire trace injection;
OTLP metrics; OTel-SDK migration; audit/cancellation protocol changes;
mixed-version negotiation; retiring `ComputeTraceContextV1`; sanitizing every
existing `data_ring.*` span.

## Council History

Drafted in the Toron cross-ring council as working "ADR 069" (iters 0–3): topic
2-of-3 (Codex, Claude — Gemini ran a parallel `UPDATE`-without-`WHERE` topic, the
natural Toron-069). Codex iter-2 conceded **Wendelon-home placement** on the
071/073 precedent; the design converged mutually (Codex's fixed-binary carrier +
Claude's sanitized-span privacy branch). Relocated + renumbered to **Wendelon
074** at iter-4. **Final 2-of-3 (Codex iter-5, Claude iter-4):** the
**producer/consumer split** (072/073 precedent — producer-only Wendelon ADR, the
Toron population a separate cross-referenced ADR) and **local-span preservation**
(Claude's iter-3 red-team — keep Wendelon's raw `data_ring.table_query.request`
span; add the sanitized linked span only when sampled). Gemini's parallel iter-4
held **fold-the-Toron-change** and **sanitized-only span replacement**; both were
resolved against by precedent (the split) and by the Wendelon-local-observability
regression (preservation), and are recorded here for the consumer ADR's authors.

## Promotion criteria met

D-set 14; 16 + 3 convention/meta guards; 16 fixtures; behavior-preserving when
absent (the unchanged Wendelon OTLP fixtures are the regression net); reuses the
ADR 067/068 substrate + seam (no new shared API); fixed-binary wire (073
posture); producer-only (no Toron drift). Migration:
`plans/impl/074_table_query_trace_continuation.md`. **Next consumer:** Toron
sequence — populate `TableQueryOptions.trace_context` from `ScanRequest`.

## Conclusion

074 closes the cross-ring trace gap 068 left open: a fixed-binary three-state
trace carrier on `TABLE_QUERY_REQ`, a Wendelon-derived child span adopting
Toron's parent via the 068 seam, the privacy line held by a sanitized linked span
while Wendelon's rich raw span is preserved locally — producer-only, behavior-
equivalent when absent, with the Toron population owned by a cross-referenced
consumer ADR.
