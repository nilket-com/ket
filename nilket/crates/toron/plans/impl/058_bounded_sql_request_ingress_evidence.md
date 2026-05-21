# ADR 058 Implementation Evidence: Bounded SQL Request Ingress

Date: 2026-05-21

## Summary

ADR 058 is implemented in the current worktree. The SQL frontend now applies
two default-on byte safety floors:

- `max_request_frame_bytes` is checked by
  `read_request_frame_bounded` after reading the SQL length prefix and before
  allocating the request payload.
- `max_statement_bytes` is checked in `handle_sql_connection` before
  constructing `SqlPlanner` or calling `SqlPlanner::plan`.

The implementation is server-inbound only. Generic SQL `read_frame` /
`write_frame` and outbound result batches keep the ADR 043 postcard wire
shape. Oversized pre-decode frames close the stream without a SQL error
frame; oversized decoded `Execute.statement` requests return the existing
`SqlErrorCode::ResourceExhausted`.

## Implementation Anchors

- `crates/toron/src/sql_wire.rs`
  - `SQL_REQUEST_FRAME_BYTES_HARD_CEILING` and
    `SQL_STATEMENT_BYTES_HARD_CEILING` set to 64 MiB.
  - `DEFAULT_SQL_REQUEST_FRAME_BYTES` and
    `DEFAULT_SQL_STATEMENT_BYTES` set to 4 MiB.
  - `read_request_frame_bounded` rejects `RequestFrameTooLarge` before
    `vec![0u8; len]`.
- `crates/toron/src/config.rs`
  - `[sql].max_request_frame_bytes` and `[sql].max_statement_bytes`.
  - Omitted values default to 4 MiB, `0` is rejected, values above 64 MiB
    are rejected, and `max_statement_bytes <= max_request_frame_bytes`.
- `crates/toron/src/daemon.rs`
  - SQL accept options carry both caps into the connection task.
  - Request-frame rejects increment `RejectReason::RequestFrameTooLarge`
    and do not synthesize a query id.
  - Statement rejects increment `RejectReason::StatementTooLarge` and use
    `SqlErrorCode::ResourceExhausted`.
- `crates/toron/src/network/observability.rs`
  - Existing bounded listener reject metric family gained
    `request_frame_too_large` and `statement_too_large` reasons only.

## Fixtures And Guards

- Architecture guards: 16 ADR 058 guards + 3 convention guards added to
  `tests/architecture_test.rs`.
- Config fixtures cover defaults, zero rejection, hard ceilings, and
  `statement <= frame`.
- Wire fixtures cover prefix-only oversized rejection before payload read and
  below-cap bounded reads.
- SQL listener fixtures cover transport-close-without-SQL-frame,
  `ResourceExhausted` before planning, `Cancel` not being statement-capped,
  and outbound result batches exceeding the inbound request cap.
- Observability fixture covers bounded reject reasons and no SQL text /
  identity label expansion.

## Verification

All commands below passed on 2026-05-21:

```text
cargo check -p toron --tests
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron --test architecture_test adr058 -- --test-threads=1
cargo test -p toron --test architecture_test -- --test-threads=1
cargo test -p toron --test config_test -- --test-threads=1
cargo test -p toron --test toron_sql_wire_test -- --test-threads=1
cargo test -p toron --test observability_listener_test -- --test-threads=1
cargo test -p toron --test sql_listener_test -- --test-threads=1
cargo test -p toron -- --test-threads=6
```

Observed counts from the focused gates:

- `architecture_test`: 577 passed.
- `config_test`: 36 passed.
- `toron_sql_wire_test`: 3 passed.
- `observability_listener_test`: 22 passed.
- `sql_listener_test`: 17 passed.
- Full `cargo test -p toron -- --test-threads=6`: passed, including Toron
  doc-tests and the Wendelon storage execution suite's expected ignored
  schema-update fixture.
