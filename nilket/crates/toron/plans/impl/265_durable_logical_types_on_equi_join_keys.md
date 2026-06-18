# Implementation Plan — ADR 265: Durable logical types on equi-join keys

## Summary
Make the bind-time logical-type validation registry-aware so durable (runtime-registered) logical types
resolve on equi-join keys and `Expr::LogicalAscription`, not just seed types. One offender
(`validate_logical_ascription_physical`), two callers to thread, four tests. SQL logical `CAST` already
resolves named durable targets through `ctx.logical_type_specs` and stays on that path. No new
IR/opcode/engine.

## Phase 1 — registry-aware validation
- `binder/arithmetic.rs` `validate_logical_ascription_physical`: add a `registry: &LogicalTypeRegistry`
  parameter; replace `logical_type_spec(logical_type)` with
  `LogicalTypeResolver::with_registry(registry).spec(logical_type)`. Keep the physical-match check as-is.
  **No `.or_else(seed_only)` / fallback** around the registry lookup (the no-silent-fallback guard) —
  `with_registry` already resolves seed-or-durable in one pass.
- `logical_type_name` (`ir.rs:84`) stays seed-only — error-message id→name is deferred (cosmetic). The
  error text for the negative test may render a durable id as `LogicalTypeId(N)`; that's fine (the test
  asserts the rejection, not the rendered name).

## Phase 2 — thread the registry to the two callers
- Logical ascription: `bind_logical_ascription` (`arithmetic.rs:765`) passes `ctx.logical_type_registry`
  (already on `ExprBindContext`, `binder.rs:871`). Do not route SQL logical `CAST` through this helper;
  `bind_sql_logical_cast` already uses `ctx.logical_type_specs`.
- Equi-join: thread `self.catalog.logical_type_registry()` from `bind_scoped_join` (`binder.rs:~2606`) →
  `bind_join_key_pair` (`binder/join.rs:430`) → `resolved_join_key` (`binder/join.rs:1173`, both
  `validate_logical_ascription_physical` call sites at `:1177`/`:1183`).
- Compiler-guided: the new parameter surfaces every call site; thread, don't default.

## Phase 3 — tests (`crates/toron/tests/logical_type_ddl_test.rs`, reusing `setup_person_id_people`)
- Rename + flip `join_bodied_derived_table_with_logical_type_is_adr265_known_gap` →
  `join_bodied_derived_table_with_logical_type_now_binds_and_executes`: assert bind Ok **and** execute
  produces a result (the ADR 264 tripwire flips).
- `plain_equi_join_on_durable_logical_type_binds_and_executes`: `SELECT people.id FROM people JOIN people
  AS other ON people.id = other.id` binds + executes. `PersonId` is non-seed → proves registry threading.
- `direct_durable_logical_ascription_binds_with_catalog_registry`: build a `LogicalPlan::Project` over
  `people` with `Expr::LogicalAscription { expr: id, logical_type: PersonId }`; assert bind succeeds and
  the projected logical identity is still `PersonId`. This covers the non-join caller of
  `validate_logical_ascription_physical`.
- `equi_join_on_incompatible_durable_logical_types_rejects`: register a **second** durable logical type in
  the fixture, build a column of it, join `people.id = <other-typed>.col`, assert a clean logical-type
  mismatch reject (not `unknown logical type`, not a silent accept). Guards against over-reach.

## Verification gate (the ADR 264 seal gate; cargo via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test   # FULL — source-scanning guards
scripts/cargo-cgroup test -p toron --no-fail-fast             # the WHOLE integration suite (empty skip list)
scripts/cargo-cgroup test -p toron-tpch                       # the reachability gauge
git diff --check
```
Acceptance: the flipped + new tests pass; the full suite stays green; no fence loosened (the negative test
still rejects).

## Seal
`impl: adr 265 durable logical types on equi join keys`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes
- **No-silent-fallback (operator guard).** Resolve only through the registry-threaded resolver; never wrap
  it in a seed-only fallback that would mask a missing-registry threading bug. The non-seed `PersonId`
  positive test is the enforcement — it fails if the registry isn't actually passed through.
- **Don't loosen compatibility.** The fix expands *resolution* (durable ids resolve), not the
  physical-match / identity check. The incompatible-durable-keys negative test pins this.
- **Thread, don't default.** Adding `registry` as a defaulted/`Option` parameter would re-open the same
  blind spot; make it required so the compiler forces every caller to pass the real registry.
