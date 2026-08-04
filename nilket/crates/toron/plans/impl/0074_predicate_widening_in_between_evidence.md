# Impl Evidence 074: Predicate Widening - `IN` + `BETWEEN`

**Date:** 2026-05-22
**Base:** `a8a2d75 plan: adr 074 predicate widening in between`

## Summary

ADR 074 is implemented as a Toron-local predicate widening. Toron now accepts
list `IN` / `NOT IN` and `BETWEEN` / `NOT BETWEEN` predicates while keeping the
change inside the planner, IR, binder, executor, PBC compiler, and Toron tests.

`IN` is first-class in Toron IR and bound IR so the PBC compiler can emit
Wendelon's existing compact `OP_IN_LIST` opcode. `BETWEEN` remains planner sugar
over existing comparison and boolean IR. `LIKE` / `ILIKE` / `SIMILAR TO` /
`RLIKE` / `CASE` / subquery `IN` / `IN UNNEST` / row-value `IN` / `ALTER TABLE`
remain explicitly unsupported.

No Wendelon, storage, SQL-wire, worker-wire, audit, telemetry, or dependency
surface changed.

## Implementation

- Added `Expr::InList { expr, list, negated }` and PBC pushdown eligibility that
  requires the target and every member to be eligible.
- Added `BoundExprNode::InList`, builder support, pretty-printing, nullable
  propagation, child walking, binding offset/remap support, and storage
  synthetic-column validation.
- Lowered `SqlExpr::InList` with `MAX_IN_LIST_ITEMS = 32` and explicit rejects
  for empty, over-limit, subquery, unnest, row-value, and literal-NULL members.
- Desugared `SqlExpr::Between` into `And(Gte, Lte)` and `Or(Lt, Gt)` without
  adding `Expr::Between`, a bound node, an executor arm, or a PBC opcode.
- Bound `IN` operands through the existing comparison literal-coercion and type
  equality policy.
- Evaluated residual `IN` by folding existing Arrow equality and boolean `or`
  kernels, with `NOT IN` applying the existing boolean `not`.
- Emitted PBC `IN` as target, then members, then existing `OP_IN_LIST` plus
  little-endian u16 count; `NOT IN` appends existing `OP_NOT`.
- Retargeted the ADR 014 SQL-`IN` absence guard so ADR 074 owns list `IN` while
  subquery `IN` remains unsupported.
- Added ADR 074 architecture guards and predicate-widening integration fixtures.
- Preserved the sqlparser 0.62 join behavior exposed during broad verification
  by accepting `JoinOperator::{Join, Left, Right}` as the existing inner/outer
  join surface and explicitly rejecting the newly named unsupported operators.

## PBC Bytecode Snapshot

For `status IN ('active', 'pending')` where `status` is column ordinal 2, the
unit snapshot is:

```text
OP_PUSH_COL 2
OP_PUSH_STR "active"
OP_PUSH_STR "pending"
OP_IN_LIST 2
```

In bytes, using Wendelon's existing opcode constants:

```text
01 02 00 00 00
03 06 00 61 63 74 69 76 65
03 07 00 70 65 6e 64 69 6e 67
30 02 00
```

`NOT IN` emits the same prefix followed by existing `OP_NOT` (`0x22`). No
desugared equality/`OR` tree appears in the PBC program.

## Verification

- `cargo fmt -p toron` - passed.
- `cargo check -p toron --all-targets` - passed.
- `cargo check -p toron --all-targets --all-features` - passed.
- `cargo test -p toron --test predicate_widening_test` - passed: 15 tests.
- `cargo test -p toron --test architecture_test adr074` - passed: 19 filtered
  guards, including ADR 074 and cross-ADR references containing `adr074`.
- `cargo test -p toron --test architecture_test` - passed: 935 guards.
- `cargo test -p toron --test join_kind_test` - passed: 6 tests.
- `cargo test -p toron --lib in_list` - passed: 3 PBC `IN_LIST` unit tests.
- `cargo test -p toron --test sql_listener_test
  loopback_sql_uses_config_tenant_for_bootstrapped_descriptor -- --nocapture` -
  passed after an unrelated transient `AddrInUse` failure during one broad
  parallel run.
- `cargo test -p toron` - passed on clean rerun, including the full Toron test
  suite and the existing ignored Wendelon admin-operation fixture.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `git diff --check` - passed.

## Requirement Audit

- **D1 accepted surface:** covered by planner and execution fixtures for `IN`,
  `NOT IN`, `BETWEEN`, `NOT BETWEEN`, and explicit reject neighbors.
- **D2/D3 first-class `IN`:** covered by source guards and bound-program tests.
- **D4 native PBC:** covered by PBC bytecode snapshot, Wendelon compile check,
  and no-new-Wendelon-opcode guard.
- **D5 residual parity:** covered by residual `IN` execution fixture and reuse of
  the existing equality / boolean `or` / `not` kernels.
- **D6 bounds:** covered by 32-item success and empty / over-limit / NULL /
  subquery / unnest / row-value reject fixtures.
- **D7 typing:** covered by binder reuse of comparison literal coercion and the
  `in_list_type_mismatch_fails_at_binding` fixture.
- **D8 `BETWEEN` sugar:** covered by planner shape, execution fixtures, PBC
  equivalence fixture, and no-new-IR/opcode guards.
- **D9 NULL posture:** explicit NULL members reject; nullable columns flow
  through existing Arrow comparison and boolean semantics.
- **D10/D11 parser accounting:** explicit lower/reject arms for the 0.62 AST
  neighbors; `LIKE` family remains ADR 075 material.
- **D12 ADR 014 reconciliation:** old no-SQL-`IN` guard retargeted to allow
  list `IN` and keep subquery `IN` unsupported.
- **D13 no cross-ring drift:** `git status` shows only Toron implementation,
  tests, and this evidence file changed; no dependency changes.
