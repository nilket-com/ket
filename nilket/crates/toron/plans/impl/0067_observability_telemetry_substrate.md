# Impl Plan 067: `observability::telemetry` substrate (Wendelon OTLP extraction)

**Status:** Promoted, paired with
`plans/adr/0067_observability_telemetry_substrate.md`.
**Posture:** Extract Wendelon's **hand-rolled** OTLP exporter + the W3C
trace-context into `observability::telemetry` (the reserved third facet),
**behavior-equivalent** for Wendelon (first consumer). **Strict extraction —
no SDK migration; no Toron exporter; no `QueryId` mapping.** The only behavioral
risk surface is Wendelon's OTLP output, pinned by the existing OTLP HTTP/gRPC
fixtures (the 065 regression-net lesson). Seven phases.

## Invariants carried in

- Wendelon OTLP output (HTTP/protobuf + gRPC payloads, endpoints, mTLS, config
  semantics, `WENDELON_LOG`, shutdown, queue) **unchanged**; only the
  otlp-feature delegation is retargeted. `nilket` runtime-free; fixture worker
  unchanged; no Toron telemetry; no audit/SQL-wire/worker/storage/metrics-HTTP
  drift; **no new OTel-SDK/tonic dep**.

## Phase 0 — Audit (verified anchors)

- **Extraction surface = `wendelon/src/metrics/telemetry.rs` ONLY** —
  `TraceExportProtocol` (:12), `TraceExportConfig` (:27), `TelemetryHandle` +
  `OtlpShutdownHandle` (:44), `build_dispatch` (:59), `OtlpTraceLayer::new`
  (:70/:96), `WENDELON_LOG` (:145), and the OTLP HTTP/gRPC export code
  (`/v1/traces`, `application/x-protobuf`, `TraceService/Export`,
  `application/grpc` all live here). Consumers: `cli.rs`, `config.rs`,
  `node.rs`.
- **NOT moved:** `wendelon/src/metrics/exporter.rs` is the **Prometheus**
  `/metrics` HTTP exporter (`METRICS_PATH = "/metrics"`, `MetricsExporterHandle`)
  — not OTLP (D16). *(Correcting the iter-3 draft, which wrongly listed it.)*
- **Hand-rolled stack** — `reqwest` (rustls-tls/http2/blocking), `h2` (opt),
  `http` (opt); **no `opentelemetry`/`tonic`/`prost`** (D7).
- **W3C trace-context:** `ComputeTraceContextV1` + `validate_traceparent`
  (`integration/compute.rs`); Toron does not reference it yet.
- **Regression net (unchanged):** `wendelon/tests/otlp_http_export.rs`,
  `otlp_grpc_export.rs`.

## Phase 1 — Create `observability::telemetry`

- `crates/observability`: add `telemetry`, `otlp-http`, `otlp-grpc` features
  (+ the `h2`/`http` optional deps; `reqwest` if not already neutral);
  `default = []`; `telemetry ⊀ http/audit`. `lib.rs`:
  `#[cfg(feature="telemetry")] pub mod telemetry;`.

## Phase 2 — Move exporter mechanics → shared (hand-rolled, no SDK)

- Move from `metrics/telemetry.rs`: the trace layer, bounded export queue +
  drop-on-saturation, shutdown handle, batching, export timeout, OTLP protobuf
  encoding, HTTP/protobuf export (`/v1/traces`, `application/x-protobuf`, mTLS
  PEM concat, CA bundle, timeouts — D8), gRPC export
  (`TraceService/Export`, HTTPS-only, h2-over-TLS, `application/grpc`,
  `te: trailers`, length-prefix framing, trailers/grpc-status, h2 reuse — D9).
  **Keep the hand-rolled reqwest/h2/http stack; introduce no OTel SDK.** No
  Wendelon imports — ring policy (resource attrs, scope `wendelon.traces`,
  `WENDELON_LOG`, `wendelon_otlp_spans_dropped_total`, sampling, node-id) is a
  **consumer `TelemetryProfile`** parameter (D5/D13).

## Phase 3 — Shared W3C trace context + ComputeTraceContextV1 compat (D10/D11)

- Move W3C `traceparent`/`tracestate` validation into
  `observability::telemetry` as a **brand-clean** shared primitive (no `*V1`).
- **Keep `wendelon::integration::compute::ComputeTraceContextV1`** as a
  compatibility surface that delegates to / wraps / type-aliases the shared
  type. **Do NOT rename it** in 067 (its retirement is a later contract ADR);
  the convention guard exempts it as compatibility-scoped.
- Treat this as the explicit continuous-refactoring cut: clean the **new**
  shared type name now; preserve the existing Wendelon compute-integration
  spelling in the wrapper until a separate contract cleanup owns the rename.

## Phase 4 — Wendelon first consumer (behavior-equivalent)

- `wendelon::metrics::telemetry::build_dispatch` stays the **ring-owned
  wrapper**: composes `EnvFilter`/`WENDELON_LOG` + fmt layer + the Wendelon
  `TelemetryProfile` + the shared trace layer. `otlp-http`/`otlp-grpc` features
  **delegate** to `observability/otlp-http`/`otlp-grpc`; `observability.traces`
  config + feature-error messages preserved. **`otlp_http_export`/
  `otlp_grpc_export` tests pass unchanged** — the behavior-equivalence proof.

## Phase 5 — Toron (no change beyond workspace)

- **No** Toron telemetry feature/exporter/config in 067 (→ 068). Toron's
  `TaskTelemetry` + trace-context fields unchanged. Add the negative guard
  (`toron_has_no_telemetry_config_or_otlp_exporter_after_067`).

## Phase 6 — Guards + retargets + fixtures + evidence

- The ADR's 20 + 4 convention + meta guards, incl. **per-feature `cargo check`**,
  **no-ring-imports**, **no-OTel-SDK/tonic dep**, **Prometheus-exporter-not-
  moved**, **W3C-validation-shared + no-QueryId-mapping**, **bounded-fail-open**,
  **ComputeTraceContextV1-compat-scoped**, **ring-policy-literals-ring-owned**,
  **no-Toron-exporter**.
- **Retarget:** otlp-feature delegation (Wendelon-owns-mechanics → delegated;
  meta `…_except_otlp_feature_retarget`). Self-enforcing. `ComputeTraceContextV1`
  **not** renamed (compat-scoped guard).
- 15 fixtures (ADR list): per-feature builds; W3C accept/reject + tracestate;
  **ComputeTraceContextV1-delegates-to-shared**; **wendelon-otlp-http/grpc-mtls
  -equivalence**; h2-reuse; config-errors-compatible; queue-saturation; shutdown;
  **no-identity-export-by-default**; toron-no-exporter.
- `plans/impl/0067_observability_telemetry_substrate_evidence.md`:
  `architecture_test adr067`, full `architecture_test` (toron+wendelon),
  `cargo test -p observability -p wendelon`, the per-feature `cargo check`s,
  `clippy … -D warnings`, `git diff --check` (capped-linker).

## Phase 7 — Roadmap

- 067 promoted — `observability::telemetry` extracted (Wendelon behavior-
  equivalent); the observability namespace is complete (`http`+`audit`+
  `telemetry`); ADR 068 = Toron telemetry consumer.

## Watch-items

1. **Behavior-equivalence is the whole risk** — `otlp_http_export`/
   `otlp_grpc_export` tests **unchanged** (don't edit the net — the 065 lesson);
   HTTP/gRPC parity specifics (D8/D9) preserved.
2. **Strict extraction, no SDK** — keep the hand-rolled reqwest/h2/http stack;
   no `opentelemetry`/`tonic` (D7) — guarded.
3. **`exporter.rs` (Prometheus) is NOT moved** (D16) — only `telemetry.rs`.
4. **Ring-agnostic** — no Wendelon/Toron imports; ring policy via the
   consumer `TelemetryProfile` (resource attrs / scope / log-env / dropped-
   metric / sampling / node-id).
5. **`ComputeTraceContextV1` stays compat-scoped** (shared type brand-clean;
   no rename in 067).
6. **No `QueryId`→TraceId in the substrate** (068 decides); W3C validation only.
7. **No Toron exporter** (→ 068); **features non-default**; `telemetry ⊀
   http/audit`.

## Acceptance checklist

- [ ] Phase 1: `observability::telemetry` + otlp-http/otlp-grpc (non-default;
      ⊀ http/audit; hand-rolled deps moved, no SDK).
- [ ] Phase 2: exporter mechanics → shared (HTTP+gRPC parity); no Wendelon
      imports; ring policy via `TelemetryProfile`.
- [ ] Phase 3: shared brand-clean W3C trace-context; `ComputeTraceContextV1`
      kept as compat surface (not renamed).
- [ ] Phase 4: Wendelon `build_dispatch` ring-owned wrapper; features delegate;
      **otlp_http/grpc tests unchanged + passing**.
- [ ] Phase 5: Toron no telemetry feature/exporter/config; negative guard.
- [ ] Phase 6: guards (per-feature builds, no-SDK, exporter-not-moved,
      no-QueryId-mapping, ring-policy-owned, compat-scoped-V1); otlp-feature
      retarget; behavior-equivalence fixtures; evidence.
- [ ] Phase 7: roadmap; 068 carry.
