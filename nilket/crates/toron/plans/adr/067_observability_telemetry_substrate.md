# ADR 067: `observability::telemetry` Substrate (Wendelon OTLP extraction)

**Status:** Promoted (3-of-3 — Claude, Codex, Gemini)
**Seed:** ADR 065 established `observability::{http, audit}` and reserved
`telemetry`. Wendelon has a mature, **hand-rolled** OTLP trace exporter in
`metrics/telemetry.rs` (over `reqwest`/`h2`/`http`/rustls + `tracing`) and a
W3C `ComputeTraceContextV1` (`integration/compute.rs`). Toron has `tracing` +
rich `TaskTelemetry` but **no exporter**. 067 extracts the substrate into the
reserved third facet (the exact 065 pattern); Toron telemetry is **ADR 068**.
**Category:** Interior shared substrate — `observability::telemetry`. Behavior-
equivalent for Wendelon (first consumer); no Toron emission. **Strict
extraction — not an SDK migration.**

## Decisions (19, Promotion-Locked — Codex iter-4 D1–D19)

**D1 Third facet.** `observability::telemetry` — feature-gated module beside
`http`/`audit`; **`default = []`; `telemetry ⊀ http`/`audit`** (065 precise-
feature invariant).

**D2 Wendelon-first extraction.** Extract OTLP mechanics from
`wendelon/src/metrics/telemetry.rs` into `observability::telemetry`; Wendelon
stays the **first consumer**, behavior preserved.

**D3 Behavior equivalence is the proof.** The existing Wendelon OTLP HTTP/gRPC
fixtures remain the **regression net** (unchanged): export path, mTLS, payload,
batching, shutdown, queue saturation, config semantics, feature-gated errors.

**D4 Shared mechanics only.** Shared owns the trace layer, bounded export queue,
drop-on-saturation, shutdown handle, batching, export timeout, **W3C
trace-context validation**, trace/span-id helpers, OTLP protobuf encoding, and
HTTP/protobuf + gRPC export.

**D5 Ring policy ring-owned.** Wendelon keeps `WENDELON_LOG`, fmt-layer
composition, scope name `wendelon.traces`, resource attrs (`service.name =
wendelon`, `wendelon.node_id`), the `wendelon_otlp_spans_dropped_total` metric,
sampling normalization, node-id semantics, config-loader surface, span taxonomy.

**D6 Feature retarget, no default-dep growth.** `otlp-http`/`otlp-grpc` become
optional `observability` features; Wendelon's same-named features **delegate**.
No default-dependency growth in `observability`/Wendelon/Toron/`nilket`.

**D7 Preserve the current exporter stack — no SDK migration.** 067 must **not**
introduce `opentelemetry-otlp`, `tonic`, Jaeger/Zipkin-native, or an OTel SDK.
The exporter is hand-rolled (reqwest/h2/http/rustls); an SDK migration is a
separate ADR.

**D8 HTTP/protobuf parity.** Preserve `/v1/traces`,
`content-type: application/x-protobuf`, endpoint normalization, mTLS client PEM
concatenation, CA bundle loading, timeout/error behavior.

**D9 gRPC parity.** Preserve
`/opentelemetry.proto.collector.trace.v1.TraceService/Export`, HTTPS-only
endpoint validation, h2-over-TLS, `content-type: application/grpc`,
`te: trailers`, gRPC length-prefix framing, trailers/grpc-status validation, h2
connection reuse, failed-connection reset.

**D10 Shared W3C trace context.** Move W3C `traceparent`/`tracestate`
validation into `observability::telemetry` as a shared primitive.

**D11 Compute-contract compatibility.** Keep
`wendelon::integration::compute::ComputeTraceContextV1` as a **compatibility
surface** that delegates to / wraps / type-aliases the shared brand-clean trace
context. Per `plans/conventions/continuous-refactoring.md`, 067 cleans the
**new shared surface** but does not rename this existing Wendelon contract.
**Retiring the `V1` name is a later contract ADR, not an incidental extraction
edit.**

**D12 No `QueryId`→TraceId substrate decision.** The substrate validates +
carries W3C trace context; it does **not** map `QueryId`/tenant/session/
descriptor/worker/Wendelon identity into external trace IDs or default span
attributes.

**D13 Consumer profile seam.** The shared trace layer accepts consumer-provided
resource attributes, scope name/version, dropped-span observer, local-node
identity, warning hook, and connector material that can't move without
importing ring modules.

**D14 Ring-agnostic shared crate.** `observability::telemetry` imports no
Toron/Wendelon modules; no ring-specific literals beyond neutral OTel/W3C
constants.

**D15 No Toron consumer in 067.** No Toron telemetry config/exporter/sampler/
span-taxonomy/propagation/`QueryId` correlation; Toron `TaskTelemetry` +
storage trace-context fields unchanged.

**D16 Non-OTLP observability out of scope.** `wendelon/src/metrics/exporter.rs`
is the **Prometheus** `/metrics` HTTP exporter, **not** OTLP — it is **not
moved** by 067.

**D17 Facet independence.** `telemetry` ↮ `audit`/`http` (no cross-references).

**D18 No runtime/protocol drift.** No SQL-wire / worker-protocol / Wendelon-
storage / audit / metrics-HTTP-exporter / client-API change beyond routing
Wendelon's OTLP setup through the shared module.

**D19 Carry-forward.** ADR 068 = Toron telemetry consumer; `audit.fail_closed`
remains a carried candidate.

## Architecture Guards (20 + 4 convention + meta)

`adr067_observability_telemetry_module_exists_and_is_feature_gated`,
`adr067_telemetry_feature_is_independent_of_http_and_audit`,
`adr067_otlp_http_and_grpc_features_are_non_default`,
`adr067_wendelon_otlp_features_delegate_to_observability`,
`adr067_wendelon_build_dispatch_uses_observability_telemetry`,
`adr067_wendelon_otlp_behavior_fixtures_remain_present`,
`adr067_shared_telemetry_has_no_wendelon_or_toron_imports`,
`adr067_wendelon_policy_literals_remain_ring_owned`,
`adr067_wendelon_dropped_span_metric_name_remains_ring_owned`,
`adr067_w3c_trace_context_validation_is_shared`,
`adr067_compute_trace_context_v1_is_preserved_as_compatibility_surface`,
`adr067_queryid_to_traceid_mapping_is_absent`,
`adr067_otlp_queue_remains_bounded_try_send_fail_open`,
`adr067_otlp_shutdown_handle_remains_explicit`,
`adr067_http_and_grpc_export_paths_remain_feature_gated`,
`adr067_no_new_opentelemetry_sdk_or_tonic_dependency`,
`adr067_metrics_http_exporter_is_not_moved_as_otlp`,
`adr067_no_toron_telemetry_config_or_exporter_yet`,
`adr067_no_audit_http_sql_wire_worker_or_storage_behavior_change`,
`adr067_prior_adr065_066_observability_guards_continue_to_pass`.
**Convention:** `adr067_nilket_root_stays_runtime_free`,
`adr067_fixture_worker_source_unchanged`,
`adr067_new_shared_surface_is_brand_clean_and_has_no_v1_suffix`,
`adr067_wendelon_v1_trace_context_suffix_is_compatibility_scoped`.
**Meta:** `adr067_prior_adr045_to_066_guards_continue_to_pass_except_otlp_feature_retarget`.

## Prior-Guard Reconciliation

The **only** intentional retarget is OTLP feature/mechanics ownership:
Wendelon's `otlp-http`/`otlp-grpc` features delegate to `observability` while
Wendelon keeps policy + behavior; meta scoped `…_except_otlp_feature_retarget`.
**`ComputeTraceContextV1` is *not* silently renamed** — the new shared surface
is brand-clean (no `*V1`), and the existing Wendelon `V1` suffix is explicitly
**compatibility-scoped** (its retirement is a future contract ADR).

## Continuous-Refactoring Disposition

067 applies `plans/conventions/continuous-refactoring.md` at the extraction
boundary: the **new** `observability::telemetry` API is brand-clean and carries
no `*V1` suffix. The existing Wendelon
`integration::compute::ComputeTraceContextV1` name remains as a bounded
compatibility surface because renaming it would change a compute-integration
contract rather than merely move OTLP mechanics. That cleanup should happen in
a separate contract ADR or an explicitly-scoped compatibility sweep, not as an
incidental part of the telemetry substrate extraction.

## Integration Fixtures (15)

builds-with-only-{telemetry,otlp-http,otlp-grpc}-feature; traceparent-accepts-
w3c-v00 / rejects-uppercase-zero-or-malformed; tracestate-rejects-control-chars-
and-overlong; **wendelon-compute-trace-context-v1-delegates-to-shared-
validation**; **wendelon-otlp-http-export-still-posts-mtls-trace-batch**;
**wendelon-otlp-grpc-export-still-posts-mtls-h2-trace-batch**; grpc-reuses-h2-
connection-across-batches; config-feature-errors-compatible; queue-saturation-
drops-and-counts-sampled-spans; shutdown-closes-exporter-without-runtime-
blocking; **substrate-has-no-default-query/tenant/session-attributes**;
toron-has-no-telemetry-config-or-otlp-exporter-after-067.

## Out of Scope (→ ADR 068 / later)

Toron telemetry exporter/config/sampling/span-taxonomy; `QueryId`→W3C-trace-root
mapping + cross-ring `traceparent` propagation; replacing the hand-rolled
exporter with an SDK; Jaeger/Zipkin-native; OTLP metrics export; moving the
Prometheus metrics HTTP exporter; `audit.fail_closed`.

## Council History

iter-0: 3-way (Claude telemetry-extract / Gemini telemetry-Toron-first / Codex
fail-closed). iter-1: Codex moved to telemetry-extract → topic 3-of-3;
Claude+Codex on extract-first + the privacy line; Gemini held Toron-first/
`QueryId==TraceId`. iter-2: Claude verified the grounding (telemetry.rs +
`ComputeTraceContextV1`). iter-3 (dual): Claude proposed the `ComputeTraceContextV1`
extraction + V1-rename; Claude's impl wrongly listed `exporter.rs` in the
extraction surface. iter-4: **Codex corrected it** — `exporter.rs` is the
Prometheus exporter (not moved), the stack is hand-rolled (no SDK migration),
and `ComputeTraceContextV1` stays compatibility-scoped (shared type brand-clean;
V1-retirement = a later contract ADR); **Gemini moved off Toron-first to
extract-first**. 3-of-3.

## Promotion criteria met

D-set 19, 3-of-3; 20 + 4 convention + meta guards; the otlp-feature retarget
(self-enforcing; meta `…_except_otlp_feature_retarget`); behavior-equivalent
(Wendelon OTLP HTTP/gRPC fixtures unchanged); ring-agnostic; no SDK migration;
no Toron consumer / Prometheus-exporter move / drift. Migration:
`plans/impl/067_observability_telemetry_substrate.md`. Toron consumer = ADR 068.

## Conclusion

067 establishes the **third and final pillar** of the shared observability
namespace — `observability::{http, audit, telemetry}` — by extracting
Wendelon's hand-rolled OTLP exporter + the W3C trace-context byte/behavior-
equivalently, ring-agnostic, with the `QueryId`↔trace-root mapping and cross-
ring propagation deferred to the ADR 068 Toron consumer.
