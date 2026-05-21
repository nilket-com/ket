# Impl Evidence 063: PK-Table `UPDATE ... WHERE`

**Date:** 2026-05-21
**Base:** `9c032ed plan: adr 063 pk table update`

## Summary

ADR 063 is implemented in the Toron SQL/planner/binder/coordinator path.
`UPDATE <table> SET <col> = <expr>[, ...] WHERE <predicate>` now lowers for
primary-key tables only. The binder rewrites UPDATE as a full-row projection
over the matched rows, preserving unassigned columns and the immutable primary
key while evaluating every SET expression against the original row.

The coordinator executes the source read through the existing read path, writes
non-empty payloads through `StorageWrite`, returns `Complete { row_count }`
through the existing affected-count batch, and preserves ADR 054's descriptor-
evolved invalidate-without-retry contract. Zero-row UPDATEs return row count
zero without calling `StorageWrite`.

Unsupported surfaces remain closed: no no-WHERE UPDATE, no `RETURNING`, no
FROM/OR/alias/join/tuple/qualified assignment surface, no primary-key mutation,
no append-only/no-PK UPDATE, no worker-local write-back, no new Wendelon wire,
descriptor, storage, or transaction primitive, and no sqlparser upgrade.

Prior guards were retargeted where UPDATE intentionally changes the surface:

- `adr062_update_surface_remains_absent` is now
  `adr062_update_surface_is_present_under_pk_update_contract_after_adr063`.
- Older top-level statement diagnostics and IR variant-count guards now account
  for the single new UPDATE variant.
- INSERT-SELECT materialization guards now point at the generalized shared
  write-source helper used by both INSERT-SELECT and UPDATE.

## Verification

- `cargo check -p toron --tests` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `cargo test -p toron --test architecture_test adr063 -- --test-threads=1`
  - passed: 28 passed.
- `cargo test -p toron --test architecture_test -- --test-threads=1` -
  passed: 693 passed.
- `cargo test -p toron --test update_test -- --test-threads=1` - passed:
  17 passed.
- `cargo test -p toron --test create_table_test planner_preserves_non_create_table_statement_rejects -- --test-threads=1`
  - passed after the expected top-level diagnostic retarget.
- `cargo test -p toron --test sql_listener_test sql_listener_update_complete_frame_carries_row_count -- --test-threads=1`
  - passed.
- `cargo test -p toron --test wendelon_storage_execution_test live_wendelon_sql_pk_update_supersedes_visible_row -- --test-threads=1`
  - passed.
- `cargo test -p toron -- --test-threads=6` - passed; one pre-existing
  Wendelon admin fixture remains ignored.
- `git diff --check` - passed.

## Notes

- The full test run exercised the loopback and mTLS SQL listener fixtures; the
  expected negative-handshake and shutdown log lines were present while tests
  passed.
- ADR 063 remains pinned to `sqlparser = "0.54"`; the 0.62 migration remains a
  separate ADR because the planning spike found broad AST drift.
