# ADR 068: Toron Telemetry Consumer (OTLP trace integration)

**Status:** Promoted (grounded convergence; the alternate literal-mapping /
wire-envelope-carrier / Wendelon-transmit line is recorded in Council History).
**Seed:** ADR 067 extracted `observability::telemetry` (the hand-rolled OTLP
exporter + W3C trace-context), Wendelon-first, and **deferred to 068** (D12) the
`QueryId`↔trace-root mapping and cross-ring propagation. 068 makes Toron the
second consumer — the 065→066 audit arc applied to telemetry.
**Category:** Toron compute-ring — consumer of `observability::telemetry`.
Consumer, **not** substrate: the shared crate stays ring-neutral. Behavior-
preserving when disabled.

## Decisions (20, Promotion-Locked)

**D1 Toron-owned wrapper.** New `crates/toron/src/telemetry.rs` composing
`EnvFilter(TORON_LOG)` + fmt layer + `observability::telemetry::OtlpTraceLayer`
via a Toron `TelemetryProfile`. Ring-owned: `TORON_LOG`, scope `toron.traces`,
resource attrs (`service.name = toron`, `service.version`, `toron.node_id` —
the numeric component of the 066 `toron-n-<u32>` identity, kept consistent),
`toron_otlp_spans_dropped_total`, warning output.

**D2 Default-disabled features.** Toron features delegating to 067, `default=[]`:
`telemetry = ["observability/telemetry"]`,
`otlp-http = ["observability/otlp-http"]`,
`otlp-grpc = ["observability/otlp-grpc"]`. `http`/`audit` stay independent; no
default-dependency growth.

**D3 Toron-owned config surface.** Add `[observability.telemetry]`, parsed by a
Toron-owned `RawTelemetryConfig` (the serde face) and normalized into the shared
`TraceExportConfig` — the 067 type is a **construction target, not the serde
surface** (matches the existing `Raw*Config` pattern). **Absence ⇒ exact no-op**
(today's fmt-only behavior, byte-identical). Presence validates endpoint /
protocol / sampling-rate / queue-size / timeout / cert-key-CA **at startup**,
with Toron-named diagnostics.

**D4 Enabled install is explicit.** Absent telemetry keeps the current
best-effort fmt subscriber. Configured telemetry must install the global
subscriber successfully **or fail daemon startup** — silently losing an
explicitly-configured exporter is worse than a startup error.

**D5 Derived trace root — not the literal `QueryId`.**
`trace_id = blake3("toron.trace.v1" || query_id_uuid_bytes)[0..16]`, unkeyed,
domain-separated, with an all-zero guard. `QueryId = Uuid::now_v7()`, so a
literal export would ship the v7 **submission-time prefix + ordering** into the
trace backend; the digest removes that while staying deterministic and
operator-recomputable (no key lifecycle). `blake3` becomes a direct Toron dep.
`query_id` is **never** a span attribute (D14).

**D6 Head sampling at the query root only.** Compute the sampled decision once
at `toron.query` admission from the derived trace-id and
`sampling_rate_per_million` (`u64::from_be_bytes(trace_id[0..8]) % 1_000_000 <
rate`). **Exactly one Toron root-sampling call site;** dispatch / worker /
storage paths never call a sampler.

**D7 Authoritative explicit-context seam (additive, ring-neutral 067
extension).** Extend `observability::telemetry` with neutral reserved fields
exported as constants — `otel.trace_id`, `otel.span_id`, `otel.parent_span_id`,
`otel.sampled`. When `otel.sampled` is present the shared layer treats it as
**authoritative and must not call root sampling** (`false` suppresses export
even at rate 1.0; `true` adopts the supplied ids even at rate 0.0); reserved
fields are **stripped from exported attributes**. Spans without reserved fields
retain exact 067 behavior — the 067 `no-ring-imports` guard and HTTP/gRPC
behavior fixtures stay green. Ring-neutrality is untouched; the only shared
change is one additive neutral seam.

**D8 Sampled-as-wire-presence; explicit suppression at workers.** On the wire,
`TaskExecutionContext.trace_context = Some(storage::TraceContext { trace_id,
span_id })` means sampled; `None` means disabled-or-unsampled. At worker span
creation both become explicit layer input: `Some` ⇒ `otel.sampled=true` + ids,
`None` ⇒ **`otel.sampled=false`** — the worker never reaches the layer's root
path, closing the resampling gap while keeping the compact carrier.

**D9 Live carrier only.** Use `TaskExecutionContext.trace_context` (serialized in
`TaskSpec.context`, copied into worker `ExecutionContext`). Do **not** widen or
reinterpret the dead `TaskSpec.trace` carrier (no live reader). Remove the dead
`task::TraceContext`/`TaskSpec.trace` only if a no-live-reader + wire-compat
audit proves it safe; otherwise leave it dormant and guard against use.

**D10 Toron-derived boundary span ids.** Span ids that must be propagated before
their span exists (`toron.query`, `toron.dispatch`, `toron.task`) are
**Toron-derived via domain-separated blake3** (nonzero-guarded), not allocated
by the layer's private counter — Toron must know the id before sending the
context. Interior local spans may keep the layer's generated ids.

**D11 Span taxonomy.** `toron.query` (root) → optional `toron.planning` →
`toron.dispatch` (per task-attempt) → `toron.task` (worker) →
`toron.storage.scan`. **Proof set: query / dispatch / task / storage.scan.**

**D12 `TaskTelemetry` attachment — raw magnitudes, bounded labels.** Attach
`TaskTelemetry` to `toron.task` under `toron.task.*`: numeric counters + phase
`*_micros` as **raw** values (performance magnitude, not query content, and OTLP
per-span numerics are not indexed dimensions); categorical labels (`stage_role`,
`operator_kind`, `strategy`, bounded `worker_count` bucket) **normalized/bounded**.

**D13 Privacy / cardinality line (066 symmetry).** Default spans **must not**
contain raw SQL, literal/parameter values, table/column names, tenant id,
session id, descriptor fingerprint, Wendelon peer address, `query_id`, or
data-bearing error strings. Allowed: static span names, resource attrs, coarse
classes, bounded labels, numeric timings, row counts, byte counts.

**D14 Error attribution.** Failed queries/tasks set the span status to `Error`
and record a bounded `ErrorClass` attribute — never a free-form, data-bearing
error string.

**D15 Fail-open runtime, fail-closed config.** Export errors / queue saturation /
malformed explicit-context fields / shutdown-flush failures **never** alter
readiness, SQL admission, task execution, cancellation, or storage. Invalid
configured telemetry is startup-fatal.

**D16 Aggregate drop metric.** `toron_otlp_spans_dropped_total` only, **no
labels**, on the existing metrics listener — mirrors Wendelon's ring-owned
accounting without importing Wendelon metrics policy.

**D17 Explicit daemon shutdown.** Store a `TelemetryHandle` on `ToronDaemon`;
drain listeners/connections, then flush within a bounded timeout tied to the
export timeout. Flush failure may be reported, never observable during query
execution.

**D18 Wendelon hop — Toron-side span only; transmission deferred (grounded).**
`wendelon-client::TableQueryOptions` (`lib.rs:157`) has **no trace field**, and
the Wendelon request path builds it without consuming `request.trace_context`.
068 emits the **Toron-side** `toron.storage.scan` span and may populate
`ScanRequest.trace_context`, but **adds no
`TableQueryOptions`/`TableQueryRequestMeta` trace carrier and no Wendelon-side
continuation** — that is a Wendelon-owned wire change (→ ADR 069).

**D19 Prior behavior pinned.** Wendelon OTLP HTTP/gRPC fixtures unchanged (the
067 regression net); no audit / Prometheus-metrics / SQL-wire-framing / storage-
result / worker-fixture / `nilket`-runtime drift; no OTel-SDK / tonic / prost.

**D20 Carry-forward.** ADR 069 = Wendelon table-query trace carrier + Wendelon-
side span continuation; `audit.fail_closed` remains carried.

## Architecture Guards (26 + 3 convention + meta)

`adr068_toron_telemetry_features_are_default_disabled`,
`adr068_toron_otlp_features_delegate_to_observability`,
`adr068_observability_telemetry_config_is_default_absent_noop`,
`adr068_telemetry_config_uses_toron_raw_config_surface`,
`adr068_enabled_telemetry_requires_successful_subscriber_install`,
`adr068_toron_telemetry_wrapper_uses_observability_telemetry`,
`adr068_toron_policy_literals_are_ring_owned`,
`adr068_toron_node_id_matches_toron_n_u32_identity`,
`adr068_trace_id_is_derived_blake3_not_literal_query_id`,
`adr068_query_id_is_not_exported_as_span_attribute`,
`adr068_sampling_is_head_based_deterministic_and_query_root_only`,
`adr068_explicit_sampled_field_is_authoritative`,
`adr068_worker_never_resamples_absent_trace_context`,
`adr068_external_context_fields_are_shared_ring_neutral_and_stripped`,
`adr068_propagation_uses_task_execution_context_trace_context`,
`adr068_taskspec_trace_is_not_a_live_carrier`,
`adr068_boundary_span_ids_are_toron_derived_nonzero`,
`adr068_worker_task_spans_attach_raw_task_telemetry_with_bounded_labels`,
`adr068_error_status_and_bounded_error_class_on_failed_spans`,
`adr068_storage_scan_spans_do_not_change_storage_results`,
`adr068_no_wendelon_table_query_trace_carrier_in_this_adr`,
`adr068_no_sql_table_tenant_session_descriptor_peer_queryid_span_attrs`,
`adr068_toron_otlp_queue_is_bounded_fail_open`,
`adr068_toron_dropped_span_metric_is_aggregate_only`,
`adr068_toron_shutdown_handle_is_explicit`,
`adr068_wendelon_otlp_behavior_fixtures_remain_unchanged`.
**Convention:** `adr068_nilket_root_stays_runtime_free`,
`adr068_fixture_worker_source_unchanged`,
`adr068_toron_telemetry_surface_is_brand_clean_no_v1`.
**Meta:** `adr068_prior_adr045_to_067_guards_continue_to_pass_except_external_context_seam`.

## Prior-Guard Reconciliation

The only intentional change to a prior surface is the **additive, ring-neutral
explicit-context seam** in `observability::telemetry` (D7): no ring imports, no
Toron policy, and spans without reserved `otel.*` fields stay byte-equivalent —
so the 067 `shared_telemetry_has_no_wendelon_or_toron_imports` guard and the
OTLP HTTP/gRPC behavior fixtures continue to pass; the meta guard scopes it.
Wendelon's OTLP config / attrs / scope / dropped-metric / fixtures are
**untouched** (D19). The 065/066 reserved-telemetry guards are not re-touched by
068 (they were already materialized by 067).

## Continuous-Refactoring Disposition

068 touches the Toron trace-propagation surface, so consolidating onto the one
binary `storage::TraceContext` and removing the dead
`task::TraceContext`/`TaskSpec.trace` (D9) is the adjacent cleanup — **gated on a
verified no-live-reader + wire-compat-neutral check**, else leave-and-guard. The
new `toron::telemetry` surface is brand-clean (no `*V1`).

## Integration Fixtures (16)

builds-with `{none, telemetry, otlp-http, otlp-grpc}`; config-absent-loads-
byte-identical; config-rejects-{empty-endpoint, bad-sampling, zero-queue, zero-
timeout, missing-cert/key/CA, unavailable-protocol}; derived-trace-id-stable-
nonzero-and-≠-literal-uuid; `otel.sampled=false`-suppresses-at-rate-1.0;
`otel.sampled=true`-adopts-ids-at-rate-0.0; **sampling-0-leaves-contexts-`None`-
and-emits-no-worker-task-span** (the resampling regression); sampling-1-emits-
nested-query/dispatch/task/storage-in-one-trace; worker-task-span-parents-on-
dispatch-via-execution-context; task-span-carries-raw-counters+phases; failed-
span-has-error-status+bounded-class; queue-saturation-increments-drop-metric-
without-rejecting-SQL; toron-otlp-http-posts-`/v1/traces`-`service.name=toron`-
scope-`toron.traces`; toron-otlp-grpc-reuses-h2; **Wendelon-OTLP-fixtures-still-
pass-unchanged**; source-scan-rejects-SQL/table/tenant/session/descriptor/peer/
query-id-attrs.

## Out of Scope (→ ADR 069 / later)

Wendelon table-query trace carrier + Wendelon-side span continuation (069);
keyed/secret trace-id derivation (a later hardening ADR if the collector trust
boundary demands it); OTel-SDK swap; OTLP metrics export; Prometheus-exporter
move; `tracestate` vendor policy; SQL-wire trace injection; `audit.fail_closed`.

## Council History

iter-0: 3-of-3 on the topic (Toron telemetry over `observability::telemetry`,
not `audit.fail_closed`). iters-1–3 converged on the harder details through
source-grounding: `QueryId = Uuid::now_v7()` ⇒ **derived** (not literal)
trace-id (the v7 submission-time leak); the live carrier is
`TaskExecutionContext.trace_context` while `TaskSpec.trace` is not a live
carrier; the 067 layer root-samples when it has no in-process parent ⇒ the
**`otel.sampled` authority + worker `None`→`false`** fix; and
`TableQueryOptions` has **no trace field** ⇒ Wendelon transmission **deferred to
069**. The final draft also folds in the span-taxonomy formalization, the
`toron.telemetry.*` attribute prefixing, error attribution, and node-id
consistency from the alternate line while rejecting literal trace roots,
wire-envelope propagation, and incidental Wendelon transmission.

## Promotion criteria met

D-set 20; 26 + 3 convention + meta guards; behavior-preserving when disabled
(absent = exact no-op); reuses the 067 substrate with one additive ring-neutral
seam (Wendelon fixtures unchanged); derived non-reversible trace root; worker
no-resample proven; Wendelon transmission explicitly deferred. Migration:
`plans/impl/068_toron_telemetry_consumer.md`.

## Conclusion

068 turns Toron's rich-but-unexported `TaskTelemetry` and its previously
unpopulated execution trace context into real distributed OTLP traces over the
067 substrate: derived query-correlated roots, deterministic head sampling
propagated as context presence with authoritative worker suppression, a
five-span Toron taxonomy with raw performance magnitudes behind a guard-pinned
privacy line — stopping cleanly at the Toron storage-scan span, with cross-ring
continuation owned by ADR 069.
