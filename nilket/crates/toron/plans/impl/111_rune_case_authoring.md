# Impl Plan — ADR 111: Rune CASE Authoring

**Migration for** `plans/adr/111_rune_case_authoring.md`. **Scope:** `rune_engine.rs` (one carrier + two free
fns + registration) + Rune behavior tests + one guard (+ surface-name retarget). **Non-scope:** no IR/bound/
binder/executor/PBC/storage/Wendelon change, no simple-CASE/`if_else` shortcut, no missing-ELSE, no
worker-side Rune. Pure authoring admission over 108's `Expr::Case`; binds through 110's `bind_case`.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Carrier precedent | `RuneNamedExpr { expr, name }` (`rune_engine.rs:180`), `RuneAggr { func, args }` (`:264`) |
| Free-fn + type registration | `m.function(["col"], col).build()?` / `m.ty::<T>()` (`create_toron_module`, `:602`) |
| `VmResult` precedent | `build_in_list` (`:575`) — schema-free non-empty check via `VmResult::panic` |
| Node (reuse) | `Expr::Case { when_then: Vec<(Box<Expr>, Box<Expr>)>, else_expr: Option<Box<Expr>> }` (108) |
| Inherited policy | `bind_case` (108 type-unify + empty-reject; 110 identity); PBC decline; eager-eval |
| Guard bucket | `tests/architecture/rune_frontend.rs` |

## Phase 1 — Carrier + constructors (`rune_engine.rs`)

```rust
#[derive(Any, Debug, Clone)]
pub struct RuneWhenThen {
    pub(crate) condition: Expr,
    pub(crate) result: Expr,
}

/// Searched-CASE branch: WHEN `condition` THEN `result`. Condition Boolean-ness, branch type unification,
/// and ADR 110 logical identity are enforced later by `bind_case`; the Rune surface adds no schema policy.
pub fn when(condition: RuneExpr, result: RuneExpr) -> RuneWhenThen {
    RuneWhenThen { condition: condition.expr, result: result.expr }
}

/// Searched CASE → existing `Expr::Case`, authored order preserved. The only Rune-side check is the
/// schema-free non-empty precondition (matching `in_list`); everything else is inherited from `bind_case`.
pub fn case(branches: Vec<RuneWhenThen>, else_expr: RuneExpr) -> VmResult<RuneExpr> {
    if branches.is_empty() {
        return VmResult::panic("CASE requires at least one WHEN branch");
    }
    let when_then = branches
        .into_iter()
        .map(|b| (Box::new(b.condition), Box::new(b.result)))
        .collect::<Vec<_>>();
    VmResult::Ok(RuneExpr {
        expr: Expr::Case { when_then, else_expr: Some(Box::new(else_expr.expr)) },
    })
}
```

(Confirm the exact `VmResult::panic` spelling against `build_in_list` at impl.)

## Phase 2 — Register (`create_toron_module`, `:602+`)

```rust
m.ty::<RuneWhenThen>()?;
m.function(["when"], when).build()?;
m.function(["case"], case).build()?;
```

Beside `col`/`lit`/`time_bucket`. No `associated_function` (top-level constructors). Entire production change.

## Phase 3 — Behavior tests (`tests/rune_relation_handles_test.rs`, `rune_plan` + execution harness)

Per ADR §Behavior gates:
- `rune_case_projection_executes` — `df.project([case([when(col("amount").gt(lit(0)), lit_str("pos")), …],
  lit_str("zero"))])` executes (string + numeric-result variant).
- `rune_case_numeric_widening_stays_naked`, `rune_case_empty_branches_reject` (`case([], lit(0))` → Rune
  panic), `rune_case_type_errors_inherited` (non-Boolean / ununifiable → bind error).
- `rune_case_filter_is_residual_and_keeps_pushable_conjunct` — a `case`-filter binds `EvaluateInToron`; a
  pushable sibling conjunct still reaches scan PBC (assert via the bound plan).
- `rune_case_eager_eval_masked_error_fails` — masked overflow/hard-error fails (inherited 108).
- `rune_case_carries_logical_identity` — `case([when(c, col("id").as_logical_type::<CustomerId>())], lit(7))`
  → `CustomerId` (110 inheritance, via the 087 `rune::Params` host-marker path).
- `rune_case_mixed_identities_reject` / `rune_case_identity_widening_rejects` — inherited 110.

## Phase 4 — Architecture guard (`tests/architecture/rune_frontend.rs`)

`adr111_rune_case_lowers_to_existing_expr_case`:
- `rune_engine.rs` defines `struct RuneWhenThen`, `fn when`, `fn case`; `create_toron_module` registers
  `m.ty::<RuneWhenThen>()`, `m.function(["when"]`, `m.function(["case"]`;
- `case` constructs `Expr::Case` with `else_expr: Some`, returns `VmResult`, rejects empty branches;
- the Rune engine gains **no** local binder/executor/PBC policy marker (`BindingError`, `BoundExpr`,
  `ExecutionPlan`, `PushdownDeclineReason`, `OP_CASE`, or the PBC decline diagnostic). Reuse the `adr079_*`
  cross-ring/worker-Rune guards + 108/110 CASE guards — don't restate CASE policy here.
- Retarget the Rune surface-name discipline: `case`/`when` admitted; `select`/`match`/`if`/`else`/`as`/`is`/
  `in`/`not`/`safe_cast` stay unadmitted.

## Phase 5 — Verification

```sh
cargo fmt
cargo test -p toron --test rune_relation_handles_test rune_case
cargo test -p toron --test architecture_test adr111
cargo test -p toron --test architecture_test adr108 && cargo test -p toron --test architecture_test adr110
cargo test -p toron --test architecture_test adr101
cargo test -p toron --test architecture_test adr082
cargo test -p toron
git diff --check
```

## Acceptance

`case([when(c, t), …], else)` builds `Expr::Case` and binds/executes through 108/110 with **no** engine
change; empty branches reject at the Rune surface (`VmResult`); required else; authored order preserved;
condition-Boolean / type-unify / 110 identity (preserve, mixed-reject, widening-reject) / eager-eval /
residual-PBC all inherited; the `adr111_*` guard pins the desugar-only nature; Rune surface-name guards
reconciled; 108/110 guards green; Toron suite green. Conditionals arc complete across both frontends.
