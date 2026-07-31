# Impl Evidence 075: Partial-Pushdown Residual Split + Literal `LIKE`/`ILIKE`

**Date:** 2026-05-23
**Base:** `43f5dde plan: adr 075 partial pushdown residual split like`

## Summary

ADR 075 is implemented as a Toron-local predicate widening and scan-planning
change. Toron now accepts literal-pattern `LIKE` / `ILIKE` predicates and keeps
them residual-only, while the binder splits top-level bound `AND` predicates so
PBC-eligible conjuncts still reach storage.

No Wendelon opcode, wire, storage, audit, telemetry, or dependency surface was
added. Wendelon still receives only existing PBC programs; `LIKE` remains a
Toron residual filter.

## Implementation

- Added one logical variant, `Expr::Like { expr, pattern, negated,
  case_insensitive }`, and one bound variant, `BoundExprNode::Like`.
- Lowered `SqlExpr::Like` and `SqlExpr::ILike` for literal string patterns only.
  `LIKE ANY`, dynamic patterns, `ESCAPE`, `SIMILAR TO`, `RLIKE`, PostgreSQL
  operator spellings, `CASE`, and subquery/UNNEST/row-value `IN` remain rejected.
- Bound `LIKE` input as `Utf8` only and returned Boolean.
- Evaluated residual `LIKE` through Arrow's existing comparison kernels:
  `like`, `ilike`, `nlike`, and `nilike`.
- Made PBC explicitly decline `BoundExprNode::Like`; no `LIKE` PBC opcode or
  Wendelon change was introduced.
- Added a bound-program splitter that walks only top-level `BoundBinaryOp::And`
  nodes. It classifies each conjunct by an actual `compile_pbc` attempt, combines
  pushed conjuncts, compiles once more, and falls back to all-residual if the
  final compile or scan attach fails.
- Retargeted `FilterDisposition`: `FullyPushed` still means the filter is
  skipped; `EvaluateInToron` may now sit above a scan that also has PBC.
- Applied the split to SELECT/CTAS/INSERT SELECT filter binding, UPDATE source
  scans, and DELETE candidate scans. DELETE stores only the residual predicate
  in `Delete.predicate`; fully pushed DELETE has `predicate: None`.
- Retargeted scan projection pruning so pushed-only PBC columns do not have to
  be projected back into Toron batches.

## Bytecode and Plan Snapshot

For:

```sql
SELECT status FROM sales WHERE id = 3 AND status LIKE 'c%'
```

the bound plan has:

- scan PBC present for the pushed `id = 3` conjunct;
- residual `BoundExprNode::Like` in the Toron `Filter`;
- `FilterDisposition::EvaluateInToron`;
- scan projection `Some([ColumnOrdinal(2)])`, proving the pushed-only `id`
  column is not projected back;
- no `LIKE` opcode in PBC.

For fully pushed filters such as `SELECT note FROM sales WHERE id = 1`, the
point-read profile is preserved while the projected scan schema may omit the
primary-key column after PBC attachment.

## Verification

- `cargo fmt --all` - passed.
- `cargo check -p toron --all-targets` - passed.
- `cargo check -p toron --all-targets --all-features` - passed.
- `cargo test -p toron --lib` - passed: 352 tests.
- `cargo test -p toron --test predicate_widening_test` - passed: 18 tests.
- `cargo test -p toron --test update_test --test delete_test` - passed: 33
  UPDATE tests and 21 DELETE tests.
- `cargo test -p toron --test wendelon_storage_execution_test` - passed: 11
  tests, 1 unchanged ignored fixture.
- `cargo test -p toron --test architecture_test` - passed: 938 guards.
- `cargo test -p toron` - passed, including doc tests.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `git diff --check` - passed.

## Requirement Audit

- **D1/D2/D3 accepted surface and IR:** one `Like` variant, one bound node,
  literal pattern only, explicit unsupported-neighbor rejects.
- **D4 typing:** binder requires `DataType::Utf8` input and Boolean output.
- **D5 execution:** residual evaluation uses Arrow comparison kernels; no direct
  regex or new string dependency.
- **D6 residual-only pushdown:** `Like` is not PBC-eligible, PBC declines it, and
  architecture guards assert no new opcode.
- **D7 split boundary:** only top-level bound `AND` is split; `OR` and `NOT` are
  not distributed.
- **D8 classification and fallback:** each conjunct is classified by
  `compile_pbc`; combined pushed predicates compile once more before attach.
- **D9 disposition semantics:** partial pushdown is represented as PBC on scan
  plus residual `Filter { disposition: EvaluateInToron }`.
- **D10 profile hint:** pushed predicates drive point-read profile selection.
- **D11 projection pruning:** pushed-only columns can be absent from the scan
  projection returned to Toron.
- **D12 UPDATE/DELETE:** UPDATE source scans and DELETE candidate scans both use
  the split; partial DELETE wraps the PBC scan in a residual filter.
- **D13 no cross-ring drift:** Wendelon storage execution fixtures stayed green,
  and no Wendelon files or dependency manifests changed.

## Residual Risk

`LIKE` semantics are Arrow's SQL-like kernel semantics for Utf8 arrays. Collation,
escape syntax, dynamic patterns, regex-like operators, and PBC/native Wendelon
string matching remain explicit future work.
