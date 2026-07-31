# ADR 074 implementation evidence

ADR 074 is implemented as a Wendelon-local table-query trace-continuation
producer. It widens `TableQueryRequestMeta` to 168 bytes with a fixed binary
trace tail, adds `TableQueryTraceContext` to `wendelon-client::TableQueryOptions`,
decodes malformed carriers fail-open as unsampled, emits a sanitized
`wendelon.table.query` span through the ADR 068 `otel.*` seam, preserves the
existing raw `data_ring.table_query.request` span for absent/sampled requests,
and suppresses raw root sampling for explicit unsampled carriers.

Date: 2026-05-22.

## Commands

- `cargo fmt --package wendelon --package wendelon-client --package toron` -
  pass.
- `cargo check -p wendelon --lib` - pass.
- `cargo check -p wendelon --no-default-features --features non-fips --lib` -
  pass.
- `cargo check -p wendelon-client --no-default-features --features non-fips --lib`
  - pass.
- `cargo check -p wendelon --no-default-features --features fips --lib` - pass.
- `cargo check -p wendelon-client --no-default-features --features fips --lib`
  - pass.
- `cargo clippy -p wendelon --lib -- -D warnings` - pass.
- `cargo clippy -p wendelon-client --lib -- -D warnings` - pass.
- `cargo test -p wendelon-client table_query --lib` - pass, 2 client trace
  carrier tests.
- `cargo test -p wendelon --lib trace_carrier` - pass, 4 server carrier/span
  tests covering fail-open decode, sanitized sampled linked span, sibling raw
  span, and unsampled raw-root suppression.
- `cargo test -p wendelon --lib table_query_linked_span_id` - pass, derived
  nonzero per-attempt span id test.
- `cargo test -p wendelon --lib table_query_decode` - pass, 5 existing
  table-query decode tests.
- `cargo test -p wendelon --lib table_query_wire_contract_carries_descriptor_pin_pbc_and_runtime_filter_len`
  - pass, 168-byte layout and offset pins.
- `cargo test -p wendelon --lib table_query_request_validation_checks_descriptor_and_bytecode_len`
  - pass.
- `cargo test -p wendelon --test table_query_telemetry` - pass, 4 fixed-tail
  trace-carrier fixtures.
- `cargo test -p wendelon --test architecture_test adr074` - pass, 19 ADR 074
  architecture/convention/meta guards.
- `cargo test -p wendelon --test architecture_test adr073` - pass, 23 prior
  table-query cancellation guards after the meta widened.
- `cargo test -p wendelon --test architecture_test` - pass, 109 Wendelon
  architecture guards.
- `cargo test -p toron --test architecture_test adr068` - pass, 30 Toron ADR
  068 guards, including the retargeted "carrier is Wendelon ADR 074 owned"
  guard.
- `cargo test -p wendelon --features otlp-http --test otlp_http_export` - pass,
  unchanged HTTP OTLP fixture.
- `cargo test -p wendelon --features otlp-grpc --test otlp_grpc_export` - pass,
  unchanged gRPC OTLP fixtures.
- `cargo test -p observability` - pass.
- `cargo test -p observability --features otlp-http explicit` - pass, shared
  ADR 068 explicit-context seam unit tests.
- `git diff --check` - pass.

## Notes

- A broad `cargo test -p wendelon --tests` attempt was stopped by the known
  filesystem capacity constraint while linking unrelated integration test
  binaries (`No space left on device`). The focused ADR 074 fixtures, full
  source architecture suite, unchanged OTLP fixtures, and touched library
  clippy/check matrix are green.
- `cargo check -p wendelon --no-default-features --lib` is intentionally invalid
  in this crate because exactly one of `fips` or `non-fips` must be selected; the
  explicit `fips` and `non-fips` profiles both pass.
- No Toron production code was changed. The only Toron edit retargets the ADR 068
  architecture guard that previously asserted the pre-ADR-074 carrier gap.
