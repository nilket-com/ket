# ADR 073 implementation evidence

ADR 073 is implemented as a Wendelon-local table-query cancellation
producer. It adds per-attempt `TableQueryRequestId` identity to
`TABLE_QUERY_REQ`, the `0x3E`/`0x3F` cancel wire pair on the existing
bidirectional data ring, an active table-query registry with RAII removal
and bounded pre-registration tombstones, cooperative
`CancellationToken` checkpoints through table-query execution, aggregate
metrics, and the ADR 073 architecture guards.

Date: 2026-05-21.

## Commands

- `cargo fmt` - pass.
- `cargo check -p wendelon --tests` - pass.
- `cargo test -p wendelon --test architecture_test adr073 -- --test-threads=1`
  - pass, 23 ADR 073 architecture/convention/meta guards.
- `cargo test -p wendelon table_query_cancel -- --test-threads=1` - pass,
  focused cancellation registry, decode, and pre-plan cancellation tests.
- `cargo test -p wendelon table_query_request -- --test-threads=1` - pass,
  focused request-id validation and retry-attempt coverage.
- `cargo test -p wendelon query_wire_contract_has_no_stats_or_slab_surface -- --test-threads=1`
  - pass, wire-contract regression guard.
- `cargo test -p wendelon table_query_decode -- --test-threads=1` - pass,
  5 focused table-query decode tests.
- `cargo test -p wendelon --test architecture_test -- --test-threads=1`
  - pass, 90 Wendelon architecture guards.
- `cargo clippy -p wendelon --all-targets -- -D warnings` - pass.
- `cargo test -p wendelon -- --test-threads=1` - pass, full Wendelon
  package suite.

## Notes

- The ADR 073 implementation is producer-only. No Toron production
  cancellation consumer is claimed; that remains the Toron ADR 060 follow-up.
- The implementation uses `tokio_util::sync::CancellationToken`, not a
  hand-rolled `Notify` signal, carrying forward the ADR 059 review lesson.
- A stale ADR 072 Toron-sync architecture guard was retargeted from
  "no consumer exists" to "the current consumer is real and non-stubbed" so
  the full architecture suite reflects the post-ADR 072/ADR 053 state.
