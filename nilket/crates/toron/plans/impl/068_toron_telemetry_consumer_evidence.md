# Impl Evidence 068: Toron Telemetry Consumer

**Date:** 2026-05-22
**Base:** `d3b4371 plan: adr 068 toron telemetry consumer`

## Summary

ADR 068 is implemented. Toron is now the second
`observability::telemetry` consumer behind default-disabled `telemetry`,
`otlp-http`, and `otlp-grpc` features. The Toron-owned config surface parses
`[observability.telemetry]` into the shared trace export config, treats an
absent section as a no-op, validates configured telemetry at startup, and keeps
export failure fail-open at runtime.

The ADR 067 shared substrate gained the additive, ring-neutral explicit-context
seam required by Toron: reserved `otel.*` span fields are stripped from exported
attributes, explicit sampled/unsampled/malformed contexts bypass root
resampling, and the legacy root sampling path remains the only behavior for
spans without explicit context. Lowercase W3C-sized nonzero ids are accepted;
malformed or incomplete explicit context is treated as unsampled.

Toron owns the consumer policy: resource attributes, scope name, dropped-span
metric, warning behavior, explicit shutdown, deterministic query-root trace ids,
boundary span ids, head sampling, and the `toron.query` -> `toron.dispatch` ->
`toron.task` -> `toron.storage.scan` propagation chain. The worker no-resample
rule is closed by emitting an explicit `otel.sampled=false` task span when a
task arrives without propagated context.

The implementation stops at the Toron storage-scan span. Wendelon wire
continuation remains deferred to ADR 069, and the unchanged Wendelon OTLP
fixtures pass against the seam-modified shared layer.

## Verification

- `cargo fmt --all` - passed.
- `git diff --check` - passed.
- `cargo check -p toron` - passed.
- `cargo check -p toron --features telemetry` - passed.
- `cargo check -p toron --features otlp-http` - passed.
- `cargo check -p toron --features otlp-grpc` - passed.
- `cargo test -p toron --quiet` - passed.
- `cargo test -p toron --test architecture_test --quiet` - passed: 826 passed.
- `cargo test -p toron --test config_test --quiet` - passed: 40 passed.
- `cargo test -p toron --test config_test --features otlp-http --quiet` -
  passed: 40 passed.
- `cargo test -p toron telemetry::tests --quiet` - passed: 2 matched tests.
- `cargo test -p toron --test observability_listener_test toron_metrics_endpoint_emits_prometheus_text_format_with_brand_clean_names --quiet`
  - passed.
- `cargo test -p observability --features otlp-http --lib --quiet` - passed:
  5 passed.
- `cargo test -p wendelon --test otlp_http_export --features otlp-http --quiet`
  - passed: 1 passed.
- `cargo test -p wendelon --test otlp_grpc_export --features otlp-grpc --quiet`
  - passed: 2 passed.

## Independent Review

The promoted changeset was independently reviewed with the following green
checks:

- 30/30 `adr068_` architecture guards.
- 826/826 full `architecture_test` guards, with no prior-ADR regression at the
  ADR 067/068 seam.
- 5/5 observability seam unit tests, including the explicit-context sampled and
  malformed-context cases.
- 40/40 Toron config tests, including telemetry validation.
- 3/3 unchanged Wendelon OTLP fixtures against the seam-modified shared layer.

The review spot-checked the promoted ADR decisions in code: the D7 explicit
context seam, D8 worker no-resample rule, D5/D6/D10 deterministic trace and span
id derivation, D8/D9 propagation chain, D12/D13/D14 bounded task telemetry
attributes, and D3 Toron-owned config validation.

## Deferred Clean-Disk Matrix

The following evidence matrix remains to be run from a build volume with enough
free space for broad clippy and test artifacts:

- `CARGO_BUILD_JOBS=1 cargo clippy -p toron -p observability -p wendelon --all-targets -- -D warnings`
- `CARGO_BUILD_JOBS=1 cargo test -p observability -p wendelon`
- Any broader per-feature workspace check matrix beyond the Toron feature checks
  listed above.

This deferral is intentional. At evidence time, `df -h .` reported the build
filesystem at `1.8T` size, `1.7T` used, `11G` available, and `100%` use. ADR
067 previously hit linker temporary-file exhaustion during a broad build on the
same filesystem class; this evidence avoids consuming the last build space and
records the exact clean-disk follow-up instead.

## Notes

- The Wendelon OTLP HTTP and gRPC fixtures remained unchanged and passed against
  the extracted shared trace layer plus the ADR 068 explicit-context seam.
- No Wendelon table-query trace carrier was added; `TableQueryOptions` remains
  an ADR 069 concern.
- Disabled Toron telemetry keeps the no-op posture: no exporter config, no
  trace propagation, no OTLP queue, and no startup-fatal telemetry install path.
