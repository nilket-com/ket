# Implementation Plan: ADR 012 — Mixed-Wildcard Projection Accounting and Planner Hardening

**Companion ADR:**
`plans/adr/012_mixed_wildcard_projection_accounting_and_planner_hardening.md`

## Status

Promoted to `plans/impl/` alongside the ADR and implemented.
Consolidated by Codex at iter-5 from the iter-3/iter-4 council
drafts; Gemini iter-5 and Claude iter-5 ratified.

## Substrate

- `crates/toron/src/sql_planner.rs`
  - `SqlPlanner::lower_select` currently returns `Ok(plan)` from
    inside the non-aggregate projection loop on
    `SelectItem::Wildcard(_)`.
  - `SqlPlanner::lower_expr` currently indexes `TIME_BUCKET`
    args without checking length.
- `crates/toron/tests/clause_accounting_test.rs`
  - ADR 010 field accounting regression surface.
- `crates/toron/tests/aggregate_projection_accounting_test.rs`
  - ADR 011 aggregate projection accounting, including aggregate
    wildcard rejection.
- `crates/toron/tests/architecture_test.rs`
  - Existing source-shape guards for planner accounting.
- `crates/toron/tests/wendelon_storage_execution_test.rs`
  - Existing valid `TIME_BUCKET` behavior coverage.

## Goal

Close two frontend-planner defects:

1. Mixed unqualified wildcard in non-aggregate projection rejects
   instead of silently returning the input plan and dropping
   sibling projection items.
2. Malformed `TIME_BUCKET` arities reject instead of panicking.

No other SQL surface widens.

## Files

Expected:

- `crates/toron/src/sql_planner.rs`
- `crates/toron/tests/non_aggregate_projection_accounting_test.rs`
- `crates/toron/tests/architecture_test.rs`
- promoted ADR/impl docs and roadmap, after implementation

Not expected:

- `Cargo.toml`
- `Cargo.lock`
- any Toron IR file
- binder / exec / fragmenter / wire code
- Wendelon code

## Phase 0 — Audit

- **P0.1:** Confirm `sqlparser` parses bare `*` as
  `SelectItem::Wildcard(_)` and qualified `t.*` as
  `SelectItem::QualifiedWildcard(_, _)`.
- **P0.2:** Confirm aggregate projections already reject wildcard
  mixed with aggregate calls.
- **P0.3:** Confirm bare `SELECT *` plan shape is the input
  subtree with no `Project`.
- **P0.4:** Confirm `TIME_BUCKET` arguments appear as
  `FunctionArguments::List(l)` and arity is `l.args.len()`;
  confirm the zero-argument spelling (`FunctionArguments::None`
  vs an empty `List`) so the arity check covers `TIME_BUCKET()`.
- **P0.5:** Confirm the architecture guard can target the old
  anti-pattern without flagging the new pre-loop `matches!` fast
  path.

Add short `// P0.x:` comments only if the audit finding is not
obvious from the code. (`// P0.x:` comments annotate where an
audit decision landed and are prose, not identifiers — see
`plans/conventions/no-planning-phase-names.md`.)

## Phase 1 — Tests first

Create:

```text
crates/toron/tests/non_aggregate_projection_accounting_test.rs
```

Test names:

- `bare_wildcard_projection_stays_byte_identical`
- `explicit_projection_stays_byte_identical`
- `mixed_wildcard_projection_rejects_order_independently`
- `multiple_unqualified_wildcards_reject`
- `wildcard_mixed_with_qualified_wildcard_rejects`
- `qualified_wildcard_mixed_before_unqualified_wildcard_rejects`
- `explicit_projection_still_lowers`
- `time_bucket_wrong_arity_rejects_cleanly`
- `valid_time_bucket_still_lowers`

Coverage requirements:

- `SELECT * FROM sales` succeeds and has no `Project`.
- `SELECT region FROM sales` succeeds.
- `SELECT region, amount FROM sales WHERE amount > 10` preserves
  existing plan shape.
- `SELECT region, * FROM sales` rejects.
- `SELECT *, region FROM sales` rejects.
- `SELECT *, * FROM sales` rejects.
- `SELECT *, sales.* FROM sales` rejects.
- `SELECT sales.*, * FROM sales` rejects.
- `TIME_BUCKET()` rejects.
- `TIME_BUCKET(ts)` rejects.
- `TIME_BUCKET(ts, '1h', ts)` rejects.
- `TIME_BUCKET(ts, '1h')` still lowers to `Expr::TimeBucket`.

The reject coverage is **order-independent and exhaustive over
sibling positions**, not a sample: today the defect is
position-dependent (`SELECT *, sales.*` returns the input plan
because the leading `*` short-circuits; `SELECT sales.*, *`
errors on `sales.*` first), and the fix makes any wildcard with
any sibling reject regardless of position.

`SELECT sales.*, *` rejects via the **existing** `_ =>
unsupported("unsupported select item")` arm — the loop reaches
the leading `QualifiedWildcard` before the trailing `*`, so it
never reaches the new mixed-wildcard arm. This is intentional:
both orderings reject as honest errors, only the error string
differs, on a construct (`QualifiedWildcard`) that was never
supported. The test
`qualified_wildcard_mixed_before_unqualified_wildcard_rejects`
asserts *that it rejects*, not a specific message — so the
asymmetry does not read as a bug to the next reader.

Run the ADR 011 aggregate projection test as a regression gate.

## Phase 2 — Fix `lower_select`

Use this implementation shape:

```rust
if matches!(projection, [SelectItem::Wildcard(_)]) {
    return Ok(plan);
}

let mut exprs = Vec::new();
for item in projection {
    match item {
        SelectItem::UnnamedExpr(sql_expr) => {
            exprs.push(self.lower_expr(sql_expr)?);
        }
        SelectItem::Wildcard(_) => {
            return Err(unsupported("mixed wildcard projections are not supported"));
        }
        _ => return Err(unsupported("unsupported select item")),
    }
}
```

Rules:

- Keep bare wildcard as the only success fast path.
- Do not expand `*`.
- Do not add support for `QualifiedWildcard`.
- Do not alter grouped or global aggregate projection paths.
- Prefer the `matches!` fast path over loop state.
- `SELECT sales.*, *` continues to reject via the `_` arm — do
  not restructure the loop to route it through the new
  mixed-wildcard arm; the message asymmetry is acceptable (see
  Phase 1) and unifying it would make the `Wildcard` arm dead
  code or require a pre-loop scan.

## Phase 3 — Fix `TIME_BUCKET` arity

Use exact arity:

```rust
let args = match &f.args {
    sqlparser::ast::FunctionArguments::List(l) => &l.args,
    _ => return Err(unsupported("TIME_BUCKET expects arguments")),
};
if args.len() != 2 {
    return Err(unsupported("TIME_BUCKET expects exactly two arguments"));
}
```

Then keep the existing logic:

- lower the first argument with `lower_arg`;
- require the second argument to be a single-quoted interval
  string;
- preserve accepted intervals: `1m`, `1h`, `1d`;
- preserve unsupported interval errors.

## Phase 4 — Architecture guard

Add a source-shape guard in
`crates/toron/tests/architecture_test.rs`.

Guard intent:

- the old projection-loop anti-pattern is absent:

```text
SelectItem::Wildcard(_) => {
    return Ok(plan);
}
```

- the guard must not fail the valid pre-loop fast path:

```text
matches!(projection, [SelectItem::Wildcard(_)])
```

Implementation preference:

- follow existing source-reading helper patterns in
  `architecture_test.rs`;
- do not add `syn`;
- if source extraction gets brittle, keep the behavioral tests
  and drop the source guard rather than adding a parser
  dependency.

## Phase 5 — Documentation

Implementation promotion updated
`crates/toron/plans/wip/roadmap.md` to mark ADR 012 implemented.
No bench-results artifact was added.

## Verification

Minimum:

```text
cargo fmt -p toron --check
cargo check --workspace --locked
cargo test --locked -p toron --test non_aggregate_projection_accounting_test --no-fail-fast
cargo test --locked -p toron --test aggregate_projection_accounting_test --test clause_accounting_test --test architecture_test --no-fail-fast
git diff --check
```

Promotion:

```text
cargo test --locked -p toron --no-fail-fast
cargo test --locked --workspace --no-fail-fast
```

Expected:

- no `Cargo.toml` / `Cargo.lock` churn;
- no IR variant changes;
- no Wendelon changes;
- no runtime-filter contract changes.

## Definition of done

- Bare `SELECT *` behavior preserved.
- Mixed wildcard projection rejects in every sibling position.
- Qualified wildcard remains unsupported and honest.
- Explicit non-aggregate projections preserved.
- Aggregate projection wildcard behavior from ADR 011 preserved.
- `TIME_BUCKET` wrong arities reject without panic.
- Valid `TIME_BUCKET` still lowers.
- Architecture/source guard green, or explicitly omitted because
  behavioral tests are sufficient and the source guard was too
  brittle.
- Full Toron test suite green before promotion.
