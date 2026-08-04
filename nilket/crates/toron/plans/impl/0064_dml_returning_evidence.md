# Impl Evidence 064: DML `RETURNING` Rows

**Date:** 2026-05-21
**Base:** `6fb90f2 plan: adr 064 dml returning`

## Summary

ADR 064 is implemented in the Toron SQL/planner/binder/coordinator/listener
path. `RETURNING` is now supported for `INSERT VALUES`, `INSERT SELECT`,
`UPDATE`, and `DELETE`; CTAS remains unchanged. Returning DML reuses the
existing SQL wire result path: Arrow `ResultBatch` frames followed by
`Complete { row_count }`. Non-returning DML remains Complete-only.

The implementation uses Toron-local returning IR and binds projections against
the target user schema. `RETURNING *` expands only to user columns, including
DELETE where the internal appended row identity is stripped before projection.
Aggregate, window, subquery, qualified wildcard/column, unknown-column, and
storage-synthetic references remain rejected.

Coordinator execution follows the ADR 064 safety order: materialize affected
rows, precompute and validate the returning projection before mutation,
perform the write/delete, verify the storage receipt, and emit returning rows
only after storage success. DELETE returning fails closed unless the receipt is
exactly all-deleted. ADR 054 descriptor-evolved write/delete handling still
invalidates without retrying or emitting rows.

Prior guards were retargeted where ADR 064 intentionally flips the previous
RETURNING-absence contract:

- INSERT/UPDATE/DELETE planner fixtures now accept the supported returning
  surfaces and keep unrelated unsupported shapes rejected.
- ADR 062 and ADR 063 returning-absence / Complete-only guards now assert the
  narrower non-returning or CTAS-only invariants.
- The ADR 064 D13 wire guard pins reuse of the existing
  `ResultBatch`/`Complete` response path, the existing Arrow IPC
  `ResultBatch` schema carrier, and the absence of any new SQL protocol
  version or returning-specific SQL wire frame.
- The ADR 064 meta-guard is scoped as
  `adr064_prior_adr030_to_063_guards_continue_to_pass_except_retarget`.

## Verification

- `cargo fmt --check` - passed.
- `cargo check -p toron --tests` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `cargo test -p toron --test architecture_test -- --test-threads=1` -
  passed: 721 passed, including the post-review D13 SQL-wire guard.
- `cargo test -p toron --test insert_test --test delete_test --test update_test -- --test-threads=6`
  - passed: insert 30 passed; delete 20 passed; update 21 passed.
- `cargo test -p toron --test sql_listener_test sql_listener_dml_returning_sends_result_batches_then_complete -- --test-threads=1`
  - passed.
- `cargo test -p toron -- --test-threads=6` - passed; one pre-existing
  Wendelon admin fixture remains ignored.
- `git diff --check` - passed.

## Notes

- The full test run exercised the loopback and mTLS SQL listener fixtures; the
  expected negative-handshake, QUIC connection-close, and OpenRaft shutdown log
  lines were present while tests passed.
- ADR 064 remains pinned to `sqlparser = "0.54"`; the 0.62 migration remains a
  separate ADR.
