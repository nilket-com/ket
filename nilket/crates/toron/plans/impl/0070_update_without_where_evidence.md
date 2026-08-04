# Impl Evidence 070: Toron `UPDATE` without `WHERE`

**Date:** 2026-05-22
**Base:** `66e0b60 plan: adr 070 update without where`

## Summary

ADR 070 is implemented. Toron now accepts PK-table `UPDATE` statements without a
`WHERE` clause by carrying update predicates as `Option<Expr>` through the
logical plan and binding `None` to the existing coordinator-owned update path.

The no-WHERE branch is absence, not a synthetic predicate: it binds
`Project -> Scan` with `RowIdentityProjection::None`, `QueryProfileHint::Scan`,
no `Filter`, and no PBC program. The existing WHERE branch still binds the
predicate, preserves boolean validation, keeps PK-equality `PointRead`, and
reuses the same `ExecutionPlan::Update` source-bearing write shape.

The PK-table gate, PK immutability check, RETURNING projection, descriptor
evolution invalidation, type coercion/nullability lattice, zero-row no-op,
coordinator ownership, and worker non-fragmentability all remain on the
existing paths. DELETE no-WHERE behavior is unchanged, and there are no
Wendelon, storage, wire, descriptor, config, budget, or `nilket` runtime-surface
changes.

## Verification

- `cargo fmt -p toron` - run to normalize Rust formatting after the initial
  check reported diffs.
- `cargo fmt --check -p toron` - passed.
- `cargo test -p toron --test update_test` - passed: 32 passed.
- `cargo test -p toron --test architecture_test adr070` - passed: 22 passed.
  This includes 20 ADR 070 guards plus 2 retargeted ADR 063 guard names that
  include `adr070`.
- `cargo test -p toron --test architecture_test adr062` - passed: 24 passed.
- `cargo test -p toron --test architecture_test adr063` - passed: 28 passed.
- `cargo test -p toron --test architecture_test adr064` - passed: 30 passed.
- `cargo test -p toron --test architecture_test` - passed: 865 passed.
- `cargo test -p toron` - passed, including the 332 lib tests, 865
  architecture guards, 32 update integration tests, 11 passed / 1 ignored
  Wendelon storage execution tests, and doc-tests.
- `cargo check -p toron` - passed.
- `cargo check -p toron --no-default-features` - passed.
- `cargo check -p toron --no-default-features --features telemetry` - passed.
- `cargo check -p toron --no-default-features --features otlp-http` - passed.
- `cargo check -p toron --no-default-features --features otlp-grpc` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `git diff --check` - passed.

## Notes

- The old planner reject string no longer appears in Toron production source.
  It remains only in architecture tests that assert the stale surface is absent.
- `FullTable` appears only in guard deny-lists, not in Toron production source.
- At evidence time `df -h .` reported the build filesystem at `1.8T` size,
  `1.7T` used, `37G` available, and `98%` use.
