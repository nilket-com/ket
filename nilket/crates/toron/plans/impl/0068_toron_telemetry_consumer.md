# Impl Plan 068: Toron Telemetry Consumer (OTLP trace integration)

**Status:** Promoted, paired with
`plans/adr/0068_toron_telemetry_consumer.md`.
**Posture:** Make Toron the second `observability::telemetry` consumer
(mirroring the 065→066 audit arc). Default-disabled `[observability.telemetry]`;
**derived** query-correlated trace roots; deterministic head sampling propagated
as context **presence** with authoritative worker suppression; a five-span
`toron.*` taxonomy with raw performance magnitudes behind a guard-pinned privacy
line. **Consumer ADR**: the only shared change is one **additive, ring-neutral**
explicit-context seam in 067. **Stops at the Toron storage-scan span** — no
Wendelon wire change. Seven phases.

## Invariants carried in

- Telemetry **absent ⇒ exact no-op** (no span, no wire delta, no
  readiness/SQL/storage change; config byte-identical). Unsampled ⇒ wire/storage
  contexts `None` **and** worker spans suppressed via explicit `otel.sampled=
  false`. Runtime export failure fail-open; invalid configured telemetry
  startup-fatal. 067 substrate stays ring-neutral (the seam adds no ring
  imports; spans without `otel.*` stay byte-equivalent); Wendelon OTLP fixtures
  unchanged. No audit / Prometheus-metrics / SQL-wire / storage-result /
  worker-fixture drift; no OTel-SDK / tonic / prost; `nilket` runtime-free; new
  surface brand-clean (no `*V1`).

## Phase 0 — Audit (verified anchors)

- **No Toron exporter today** — `TORON_LOG`/`OtlpTraceLayer`/`build_dispatch`
  absent from `toron/src`; `daemon::install_tracing` is fmt-only/best-effort
  (silently ignores an already-installed subscriber).
- **Config** — `ObservabilityConfig` (`config.rs:271`), `RawObservabilityConfig`
  (`config.rs:493`); `AuditConfig` disabled/enabled enum (`config.rs:278`) is the
  shape to mirror; no telemetry section yet.
- **Live carrier** — `TaskExecutionContext.trace_context:
  Option<storage::TraceContext>` (`context.rs:12`); `storage::TraceContext =
  { trace_id:[u8;16], span_id:[u8;8] }` (`storage.rs:28`). Flow: coordinator
  `ExecutionContext.trace_context` → `TaskSpec.context` → worker copy
  (`server/mod.rs:162`) → `ScanRequest.trace_context` (`exec.rs:274,665`).
  **`None` at every construction site** (`storage.rs:1208,1341,1361,1381,1401`);
  the current Wendelon request path does not consume it — plumbed but inactive.
- **Dead carrier** — `task::TraceContext { trace_id:String }` on `TaskSpec.trace`
  (`task.rs:72,137`); only construction is the `wire.rs:338` fixture.
- **Wendelon hop NOT wired** — `TableQueryOptions` (`wendelon-client/lib.rs:157`)
  has no trace field; `storage.rs:785` builds it without `request.trace_context`
  (D18: deferred to ADR 069).
- **`QueryId = Uuid::now_v7()`** (`identity.rs:9`); root minted at the
  coordinator (`coordinator.rs:881`); query executed via
  `execute_query_with_context` (`daemon.rs`). **`TaskTelemetry`** (`task.rs:77`):
  labels(:84)/counters(:92)/phases(:106). **Metrics home**:
  `network/observability.rs`. **Reserved-field stripping point**:
  `AttributeVisitor` in `observability/src/telemetry/mod.rs`. **`blake3`**
  present transitively via `observability/audit` — add as a direct Toron dep.

## Phase 1 — Cargo + Toron-owned config

- Toron `Cargo.toml`: `telemetry`/`otlp-http`/`otlp-grpc` delegate; `default=[]`;
  `blake3` direct dep. Add `RawTelemetryConfig` (TOML serde face) +
  `ToronTelemetryConfig` (typed) under `RawObservabilityConfig`; parse
  `[observability.telemetry]`, **normalize into the shared `TraceExportConfig`**
  with Toron-named diagnostics (`observability.telemetry.{endpoint,protocol,
  sampling_rate,max_queue_size,export_timeout_s,...}`). **Absent = no-op**;
  present validates at startup. Do **not** derive `Deserialize` on the 067 type.

## Phase 2 — Shared explicit-context seam (additive, ring-neutral; D7)

- In `observability::telemetry`: public field constants `OTEL_TRACE_ID_FIELD`,
  `OTEL_SPAN_ID_FIELD`, `OTEL_PARENT_SPAN_ID_FIELD`, `OTEL_SAMPLED_FIELD`;
  private fixed-width lowercase-hex parsing (32→`[u8;16]`, 16→`[u8;8]`, nonzero).
  Extend `AttributeVisitor` to split reserved fields out (never in
  `CompletedSpan.attributes`). In `on_new_span`: parse reserved fields first; if
  `otel.sampled` present **never call `should_sample_root_span`** (`false` ⇒
  store unsampled, export nothing on close; `true` ⇒ use supplied ids); no
  reserved field ⇒ exact 067 behavior. Malformed context is fail-open
  (mark unsampled, no runtime-affecting warning). Tests: false-suppresses-at-1.0;
  true-exports-at-0.0; reserved-fields-stripped; **Wendelon OTLP fixtures
  unchanged**.

## Phase 3 — Toron telemetry wrapper

- `crates/toron/src/telemetry.rs`: `TORON_LOG`, `OTLP_SCOPE_NAME="toron.traces"`,
  `TelemetryHandle`, `build_dispatch`, `install_global`,
  `ToronDroppedSpanObserver` (→ `toron_otlp_spans_dropped_total`),
  `ToronTelemetryWarningHook`. Resource attrs `service.name=toron`,
  `service.version=env!(CARGO_PKG_VERSION)`, `toron.node_id` from the 066
  `toron-n-<u32>` numeric component. fmt-only when no trace config; add
  `OtlpTraceLayer` when present + feature-supported.

## Phase 4 — Metrics + daemon install/shutdown

- Aggregate `toron_otlp_spans_dropped_total` (no labels) on the metrics listener.
  Replace `install_tracing` with logic returning a `TelemetryHandle`: absent ⇒
  fmt best-effort; present ⇒ **fail startup** if global install fails. Store the
  handle on `ToronDaemon`; on shutdown, drain listeners/connections then flush
  within the configured export timeout (report flush errors, never affect
  completed runtime behavior).

## Phase 5 — Trace identity, span ids, sampling (one root site)

- Toron helpers (domain-separated blake3, all-zero-guarded):
  `trace_id_for_query`, `query_span_id`, `dispatch_span_id`, `task_span_id`,
  hex formatters for the reserved fields, and
  `head_sample(trace_id, rate) = u64::from_be_bytes(trace_id[0..8]) % 1_000_000 <
  rate`. **Exactly one `head_sample` call site** (`toron.query`). Tests: stable,
  nonzero, trace-id ≠ literal UUID bytes, sampling 0/1 behavior.

## Phase 6 — Coordinator / worker / storage spans

- **Coordinator** (`execute_query_with_context`): `toron.query` with explicit
  trace-id + query span-id + sampled decision (no SQL/tenant/session/query-id).
  Optional `toron.planning` (`bind`) only if non-intrusive. Scheduler dispatch:
  `toron.dispatch` with explicit trace/span/parent ids + `otel.sampled=true` for
  sampled queries; set `TaskExecutionContext.trace_context =
  Some({trace_id, dispatch_span_id})` iff sampled, else `None`.
- **Worker** (`server::execute_task_local_with_telemetry`): **always** create
  `toron.task` with explicit `otel.sampled` — `Some` ⇒ `true` + ids;
  **`None` ⇒ `false`** (never re-sample). Declare task-telemetry fields with
  `tracing::field::Empty` at creation, record raw counters/phases (bounded
  labels) + bounded `ErrorClass`/error status before close. Advance
  `ExecutionContext.trace_context` to the task span only for sampled tasks.
- **Storage** (`storage.rs`): `toron.storage.scan` with coarse kind/outcome; may
  populate `ScanRequest.trace_context`; **do not** touch `TableQueryOptions`/
  `TableQueryRequestMeta` (→ 069). `storage.rs:785` otherwise unchanged.

## Phase 7 — Guards, carrier cleanup, fixtures, evidence, roadmap

- ADR's 26 + 3 convention + meta guards (incl. per-feature `cargo check`,
  config-absent-no-op, raw-config-surface, derived-not-literal-id, no-query-id-
  attr, root-sampling-only-at-query-root, explicit-sampled-authority, worker-no-
  resample, seam-ring-neutral-and-stripped, execution-context-carrier,
  `TaskSpec.trace`-not-live, boundary-ids-toron-derived, privacy source-scan,
  bounded-fail-open, aggregate-drop-metric, explicit-shutdown).
- **Carrier cleanup (D9):** verify `TaskSpec.trace`/`task::TraceContext` has no
  live reader + wire-compat-neutral → remove; else leave-and-guard.
- 16 fixtures (ADR list), incl. Toron OTLP HTTP/gRPC modeled on Wendelon's tests,
  the unsampled-no-worker-span regression, and **Wendelon OTLP fixtures pass
  unchanged**.
- `plans/impl/0068_toron_telemetry_consumer_evidence.md`:
  `architecture_test adr068`, full `architecture_test` (toron+wendelon),
  `cargo test -p observability -p toron -p wendelon`, per-feature `cargo check`,
  `clippy … -D warnings`, `git diff --check` (capped-linker).
- Roadmap: 068 promoted — Toron telemetry consumer live; ADR 069 = Wendelon
  table-query trace carrier + Wendelon-side continuation.

## Watch-items

1. **067 seam behavior-equivalence is the substrate risk** — spans without
   `otel.*` must export byte-identically; the 067 HTTP/gRPC fixtures are the net.
2. **No-op when absent / unsampled** — `None` (today) is the exact no-op; the
   worker `None`→`otel.sampled=false` translation is the resampling regression.
3. **Boundary span ids must be known before propagation** — Toron-derive them;
   do not rely on the layer's private counter.
4. **`tracing::Span::record` ignores undeclared fields** — declare task-telemetry
   fields at `toron.task` creation.
5. **Sole live carrier** — `TaskExecutionContext.trace_context`; don't resurrect
   `TaskSpec.trace`.
6. **Wendelon transmission deferred** — populating `ScanRequest.trace_context` is
   Toron-internal; `TableQueryOptions` is out of scope (→ 069).
7. **Derived id, not literal** — `blake3` direct dep; nonzero guard;
   deterministic; `query_id` never an attribute.

## Acceptance checklist

- [ ] Phase 1: features delegate (`default=[]`); Toron `RawTelemetryConfig` →
      shared `TraceExportConfig`; absent=no-op; present validates + installs-or-fails.
- [ ] Phase 2: ring-neutral explicit-context seam with authoritative `otel.sampled`;
      reserved fields stripped; 067 guards + Wendelon fixtures pass.
- [ ] Phase 3: Toron telemetry wrapper + profile (ring-owned policy).
- [ ] Phase 4: aggregate drop metric, install-or-fail, explicit shutdown handle.
- [ ] Phase 5: derived nonzero ≠-literal trace/span ids; one `head_sample` site.
- [ ] Phase 6: query/dispatch/task/storage spans; worker `None`→`false` no-resample;
      raw counters, bounded labels, error class; storage span without Wendelon carrier.
- [ ] Phase 7: guards + carrier cleanup + fixtures + evidence + roadmap; ADR 069 carry.
