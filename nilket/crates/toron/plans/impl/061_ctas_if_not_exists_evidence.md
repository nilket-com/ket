# Impl Evidence 061: CTAS `IF NOT EXISTS`

**Date:** 2026-05-21
**Base:** `07e5840 plan: adr 061 ctas if not exists`

## Summary

ADR 061 is implemented in the Toron SQL/coordinator path. CTAS now accepts
`IF NOT EXISTS`, threads the flag through logical and execution IR, exposes a
catalog `CreateTableOutcome { binding, created }`, and gates CTAS writes on
`created == true`. Compatible existing or raced-in targets return empty
success without writing; incompatible targets return `SchemaMismatch`.

The Wendelon-backed schema-create race is handled without a Wendelon
server/wire change: an `IF NOT EXISTS` schema-update rejection attempts the
existing descriptor-sync path, returns `created=false` on exact schema match,
returns `SchemaMismatch` on mismatch, and preserves the original admin error
when no current descriptor can be proven.

Prior guards were retargeted for ADR 033, ADR 041, and ADR 053:

- ADR 033 keeps CTAS explicit-column-list rejection but no longer asserts CTAS
  `IF NOT EXISTS` rejection.
- ADR 041 flips CTAS `IF NOT EXISTS` from rejected to supported while keeping
  the OR-REPLACE + IF-NOT-EXISTS conflict rejected.
- ADR 053 keeps descriptor sync as a bind-miss mechanism, with the ADR 061 D12
  create-race reconciliation as the explicit create-path exception.

## Verification

- `cargo check -p toron --tests` — passed.
- `cargo clippy -p toron --all-targets -- -D warnings` — passed.
- `cargo test -p toron --test ctas_test -- --test-threads=6` — passed:
  28 passed.
- `cargo test -p toron catalog::tests::wendelon_catalog_if_not_exists_recovers_compatible_remote_create_race -- --test-threads=1`
  — passed.
- `cargo test -p toron --test architecture_test adr061 -- --test-threads=1`
  — passed: 17 passed, 627 filtered out.
- `cargo test -p toron --test architecture_test -- --test-threads=1` —
  passed: 644 passed.
- `cargo test -p toron -- --test-threads=6` — passed; one pre-existing ignored
  Wendelon admin fixture remains ignored.
- `git diff --check` — passed.

## Notes

- No new SQL wire frame, worker protocol, Wendelon server/wire surface, storage
  primitive, `RETURNING`, row count, or PK DDL was added.
- The live Wendelon admin schema-update fixture remains ignored as before; the
  ADR 061 D12 race behavior is covered by a local fake schema-update client and
  descriptor-sync client in the catalog unit suite.
