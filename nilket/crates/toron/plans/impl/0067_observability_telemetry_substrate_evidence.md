# Impl Evidence 067: `observability::telemetry` Substrate

**Date:** 2026-05-22
**Base:** `2da9ad0 plan: adr 067 observability telemetry substrate`

## Summary

ADR 067 is implemented. The Wendelon hand-rolled OTLP trace exporter and
brand-clean W3C trace-context validation now live in the shared
`observability::telemetry` facet behind default-disabled features. The
`telemetry`, `otlp-http`, and `otlp-grpc` features are independent of the
existing `http` and `audit` facets; the OTLP features keep the reqwest/h2/http
stack and do not introduce an OpenTelemetry SDK, tonic, or prost dependency.

Wendelon is the first consumer. Its `metrics::telemetry` module is now a
ring-owned wrapper that keeps Wendelon policy local: `WENDELON_LOG`, resource
attributes, scope name, dropped-span metric, sampling defaults, warning output,
and global dispatch composition remain in Wendelon. The shared trace layer owns
bounded queueing, fail-open span drops, shutdown, batching, export timeouts,
OTLP protobuf encoding, HTTP/protobuf export, and gRPC/h2 export behavior.

`ComputeTraceContextV1` remains Wendelon's compatibility surface for compute
integration while delegating validation to `observability::telemetry`.
No Toron telemetry exporter, telemetry config, `QueryId` to trace-id mapping,
Prometheus metrics exporter migration, SQL wire change, audit behavior change,
worker change, or storage behavior change was added in this ADR.

The ADR 067 architecture guards pin the shared feature boundary, Wendelon
delegation, brand-clean substrate, W3C validation extraction, bounded
fail-open OTLP queue, explicit shutdown handle, no-SDK dependency posture,
no-Toron-exporter posture, and the ADR 045 through ADR 066 retargets.

## Verification

- `cargo check -p observability --features telemetry` - passed.
- `cargo check -p observability --features otlp-http` - passed.
- `cargo check -p observability --features otlp-grpc` - passed after fixing a
  local helper placement issue in the gRPC TLS error path.
- `cargo check -p wendelon` - passed.
- `cargo check -p wendelon --features otlp-http` - passed.
- `cargo check -p wendelon --features otlp-grpc` - passed.
- `cargo test -p toron --test architecture_test adr067_` - passed: 25 passed.
- `cargo test -p toron --test architecture_test adr048_` - passed after
  retargeting the ADR 048 source guard for observability telemetry deps.
- `cargo test -p toron --test architecture_test adr065_` - passed after
  retargeting the ADR 065 reserved-telemetry guard to the materialized facet.
- `cargo test -p toron --test architecture_test adr066_` - passed: 21 passed.
- `cargo test -p observability --features telemetry` - passed; audit and HTTP
  integration tests are now feature-gated and do not run without their facets.
- `cargo test -p wendelon --features otlp-http --test otlp_http_export` -
  passed.
- `cargo test -p wendelon --features otlp-grpc --test otlp_grpc_export` -
  passed: 2 passed.
- `cargo test -p wendelon --test config_loader` - passed: 27 passed.
- `cargo test -p wendelon --features otlp-http --test config_loader trace_export_with_feature`
  - passed.
- `cargo test -p wendelon --features otlp-grpc --test config_loader trace_export_with_feature`
  - passed.
- `cargo test -p wendelon --test compute_integration_contract trace_context` -
  passed.
- `cargo test -p observability --features http,audit,telemetry,otlp-http,otlp-grpc`
  - passed.
- `CARGO_BUILD_JOBS=1 cargo test -p observability -p wendelon` - passed.
- `cargo test -p toron --test architecture_test` - passed: 796 passed.
- `CARGO_BUILD_JOBS=1 cargo clippy -p observability -p wendelon --all-targets -- -D warnings`
  - passed.
- `git diff --check` - passed.

## Notes

- The first broad `cargo test -p observability -p wendelon` attempt failed
  because generated hidden linker temporary files filled the target filesystem.
  Only those generated `target/debug/deps/.*` temp files were removed, freeing
  build space, and the serialized rerun with `CARGO_BUILD_JOBS=1` passed.
- The Wendelon OTLP HTTP and gRPC behavior fixtures remained unchanged and
  passed against the extracted shared trace layer.
