# Impl Plan — ADR 101: Rune `LIKE`/`ILIKE` Pattern Predicates

**Migration for** `plans/adr/0101_rune_like_ilike_pattern_predicates.md`. **Toron-only, Rune frontend.**
Deliberately boring: 4 `RuneExpr` methods + a `build_like` helper → existing `Expr::Like`, 4 registrations,
behavior fixtures over the existing `sales` fixture, stale guard retargets. **No**
SQL/IR/binder/executor/PBC/storage/Wendelon change.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Rune method precedent (both-polarity) | `in_list`/`not_in_list` `rune_engine.rs:550-556` → `build_in_list` `:559` (`VmResult`, validates empty/oversized) |
| Infallible precedent (the right model) | `eq`/`gt`/`lt`/`and`/`or` `:427-469`, `logical_not` `:475` → return `RuneExpr` directly |
| Registration site | `create_toron_module` — `m.associated_function("logical_not", RuneExpr::logical_not)?` `:626` and the `in_list` registrations nearby |
| Logical IR target | `Expr::Like { expr, pattern, negated, case_insensitive }` `ir.rs:528` |
| Binder (unchanged) | `bind_like` `binder.rs:4704`; error `"LIKE expects Utf8 input, got {expr_type:?}"` `:4720` |
| PBC residual (unchanged) | `BoundExprNode::Like` declined `pbc.rs:128`, residual set `:181` |
| Guard retargets | `adr082_typed_literal_cut_does_not_expand_operator_surface` `rune_frontend.rs:674-705`; `adr086_rune_in_list_methods_are_registered` forbid-list site |
| Behavior fixture | `sales_schema()`/`sales_batch()` `rune_relation_handles_test.rs:1300-` |

## Phase 1 — Rune methods + helper (`rune_engine.rs`)

Add to the `RuneExpr` impl block (next to `in_list`/`logical_not`):

```rust
pub fn like(self, pattern: String) -> RuneExpr      { build_like(self.expr, pattern, false, false) }
pub fn not_like(self, pattern: String) -> RuneExpr  { build_like(self.expr, pattern, true,  false) }
pub fn ilike(self, pattern: String) -> RuneExpr     { build_like(self.expr, pattern, false, true) }
pub fn not_ilike(self, pattern: String) -> RuneExpr { build_like(self.expr, pattern, true,  true) }

fn build_like(expr: Expr, pattern: String, negated: bool, case_insensitive: bool) -> RuneExpr {
    RuneExpr { expr: Expr::Like { expr: Box::new(expr), pattern, negated, case_insensitive } }
}
```

Infallible `-> RuneExpr` (D3) — mirror `logical_not`, **not** `in_list`'s `VmResult`. No empty-pattern
guard.

## Phase 2 — Register in `create_toron_module` (`rune_engine.rs:~626`)

Beside the existing `m.associated_function("logical_not", RuneExpr::logical_not)?;` /
`"in_list"`/`"not_in_list"` registrations:

```rust
m.associated_function("like", RuneExpr::like)?;
m.associated_function("not_like", RuneExpr::not_like)?;
m.associated_function("ilike", RuneExpr::ilike)?;
m.associated_function("not_ilike", RuneExpr::not_ilike)?;
```

(`like`/`ilike` are not Rune keywords — `.like(...)` etc. are safe method names; confirmed against the
ADR 081 reserved-keyword guard, which is keyword-based, not a name denylist.)

## Phase 3 — Behavior fixtures (`rune_relation_handles_test.rs`, over the existing `sales` fixture)

Fixture ground truth (verified `:1300-`): `id:Int64 [1,2,3,4,5,6]`, `amount:Int32`, `status:Utf8 =
[open, open, closed, open, open, open]` (only id 3 is `closed`).

- `rune_like_filters_utf8_column_residual` — `ctx.table("sales").filter(col("status").like("o%"))`:
  bound predicate is `BoundExprNode::Like { negated: false, case_insensitive: false, .. }`; disposition
  `FilterDisposition::EvaluateInToron`; scan carries no PBC predicate; ids `[1,2,4,5,6]`.
- `rune_ilike_filters_utf8_column_residual` — `col("status").ilike("CLO%")`: `case_insensitive: true`;
  ids `[3]`.
- `rune_not_like_and_not_ilike_filter_rows` — `not_like("o%")` → `[3]`; `not_ilike("CLO%")` →
  `[1,2,4,5,6]`; both `negated: true`.
- `rune_like_residual_splits_from_pushable_conjunct` —
  `col("id").eq(lit_i64(3)).and(col("status").like("c%"))`: the `id = 3` equality stays scan-PBC-eligible,
  `Like` remains in the residual predicate, overall disposition `EvaluateInToron`; ids `[3]`.
- `rune_like_reuses_binder_utf8_type_error` — `col("amount").like("1%")`: bind fails via `bind_like`;
  assert the error **contains** `LIKE expects Utf8 input, got Int32` (assert the substring, matching the
  `binder.rs:4720` format with the fixture's `Int32` carrier).

Use the established Rune test scaffolding (`rune_plan`/bind/`execute_logical`/`i64_column`) and the `sales`
fixture rows above — do **not** borrow expected values from the SQL widening fixtures.

## Phase 4 — Retarget stale surface guards

In `adr082_typed_literal_cut_does_not_expand_operator_surface` (`rune_frontend.rs:674-705`):
- add `"like"`, `"not_like"`, `"ilike"`, `"not_ilike"` to the **retained** array (`:674-693`);
- change the **deferred** array (`:700`) from `["not", "like", "ilike", "safe_cast"]` to
  `["not", "safe_cast"]`.

In `adr086_rune_in_list_methods_are_registered`, update the registered-name forbidden list so admitted
`like`/`ilike` no longer fail the in-list guard. Keep the real forbidden names (`in`, `not`, `safe_cast`)
for that guard.

## Phase 5 — New `adr101_*` architecture guards (`rune_frontend.rs`)

- `adr101_rune_like_ilike_methods_are_registered` — `create_toron_module` registers the four; `RuneExpr`
  defines them with `pattern: String`/`-> RuneExpr`; bodies build `Expr::Like` (no new IR variant).
- `adr101_rune_like_methods_are_infallible` — the four method sources do not contain `VmResult`; no
  empty-pattern rejection; `build_in_list` remains the sole `VmResult` list-validation precedent.
- `adr101_stale_surface_guards_are_retargeted` — registered names include `like`/`ilike`; the ADR 082
  deferred set is exactly `["not", "safe_cast"]`; the ADR 086 forbidden-name guard no longer rejects
  `like`/`ilike`.
- `adr101_reuses_075_like_policy` — `lower_like`/`bind_like`/executor remain the semantic owners; no Rune
  `LIKE ANY`/`ESCAPE`/dynamic-pattern/regex surface.
- `adr101_like_remains_residual_only` — `pbc.rs` still declines `BoundExprNode::Like`; no new PBC opcode.
- `adr101_no_cross_ring_surface` — no SQL planner / storage / wire / Wendelon / worker mention of the new
  helpers.

## Phase 6 — Verification

```sh
cargo test -p toron --test rune_relation_handles_test like        # ADR101 behavior gates
cargo test -p toron --test architecture_test adr101               # new guards
cargo test -p toron --test architecture_test rune                 # retargeted deferred guard still green
cargo test -p toron                                               # full suite, no regression
```

## Acceptance

The four methods build `Expr::Like` and are registered; `LIKE`/`ILIKE`/negated variants filter a Utf8
column with the established executor results (`[1,2,4,5,6]` / `[3]` / etc.); pattern predicates stay
residual and split beside a pushable `id = 3`; non-Utf8 input reuses the exact `bind_like` error
(`got Int32`); stale surface guards allow the admitted `like`/`ilike` methods while keeping `not` and
`safe_cast` out; full Toron suite green with no SQL/IR/binder/executor/PBC/storage/Wendelon change.

## Risks / notes

- **Return-type uniformity:** Rune's `associated_function` accepts both infallible (`-> RuneExpr`) and
  fallible (`-> VmResult<T>`) instance fns; mixing `like` (infallible) beside `in_list` (`VmResult`) is
  fine and matches the existing `logical_not`-vs-`in_list` split.
- **Assert the error as a substring**, not full-string equality — `{expr_type:?}` renders the Arrow
  `DataType` debug; pin `"got Int32"` to the fixture's `amount` carrier.
- **Guard scope:** edit the ADR 082 deferred-list guard and the ADR 086 registered-name forbidden list.
  The ADR 081 reserved-keyword guard should not move; it is keyword-based, not a feature denylist.
