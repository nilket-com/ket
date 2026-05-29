# Impl Plan — ADR 125: Rune Broadcast Join Authoring

**Migration for** `plans/adr/125_rune_broadcast_join_authoring.md`.
**Scope:** Toron interior — `sql_planner.rs` (helper rename + strategy
branch + 2 internal callsite updates), `rune_engine.rs` (new
`broadcast_join` method + module registration + 1 callsite update + import
update). **Non-scope:** `exec.rs`, `distributed/fragment.rs`,
`distributed/scheduler.rs`, `binder.rs` broadcast validator slice
(`:3664-3679`), `pbc.rs`, `ir*.rs` (`JoinStrategy::Broadcast` enum
variant reused), Wendelon source, partition store, SQL grammar.

## Change surface

**Production:**
- `crates/toron/src/sql_planner.rs` — rename `build_hash_join_plan` →
  `build_join_plan(.., strategy)` with internal strategy branch + 2
  internal callsite updates.
- `crates/toron/src/rune_engine.rs` — add `RuneDataFrame::broadcast_join`
  method + `m.associated_function("broadcast_join", ...)` registration +
  1 callsite update in `join_with_kind` + import-line update.

**Tests + guards:**
- `crates/toron/src/lib.rs` — add G-A authored-broadcast equivalence test
  + G-D authored cap-failure test (mirroring `:594` + `:606` shapes).
- `crates/toron/src/sql_planner.rs` — crate-local unit test for G-F
  direct `build_join_plan(.., JoinStrategy::Broadcast)` non-INNER reject
  if no existing crate-local test module is a better fit.
- `crates/toron/tests/architecture/rune_frontend.rs` — retarget the three
  ADR 122 guards (`:1713`, `:1749`, `:1816`) in-change; add six new
  ADR 125 guards (Gᵃ¹–Gᵃ⁶).
- `crates/toron/tests/rune_relation_handles_test.rs` — G-B (receiver
  convention), G-C (logical identity), G-E (qualifier resolution), and G-G
  (existing Rune methods regression), using the existing `rune_plan`
  harness rather than creating a parallel Rune integration test file.

## Pre-flight anchors (verify before editing)

| Concern | Anchor |
|---|---|
| `JoinStrategy` enum def | `ir.rs:615` |
| `DEFAULT_JOIN_PARTITION_COUNT = 4` | `sql_planner.rs:38` |
| `build_hash_join_plan` def | `sql_planner.rs:4042-4063` |
| `JoinStrategy::HashPartitioned` literal in helper | `sql_planner.rs:4062` |
| SQL caller 1: `lower_join` | `sql_planner.rs:2646` |
| SQL caller 2: `lower_correlated_*` | `sql_planner.rs:3598` |
| Rune caller: `join_with_kind` | `rune_engine.rs:404-417` (call at `:414`) |
| Rune `RuneDataFrame` join methods | `rune_engine.rs:230-262` (9 methods over 8 kinds) |
| Rune module + registration | `rune_engine.rs:833 create_toron_module`; `:878-879` pattern |
| Broadcast fragmenter | `fragment.rs:473-565` |
| Broadcast binder validator | `binder.rs:3664-3679` |
| `JoinStrategy::Broadcast` dispatcher | `fragment.rs:351` |
| Existing hand-rolled equivalence test | `lib.rs:594-603` |
| Existing hand-rolled cap-failure test | `lib.rs:606-625` |
| 122 builder-shape guards | `tests/architecture/rune_frontend.rs:1713, :1749, :1816` |

## Phase 0 — Failing gates first (test-driven)

Land all behavior gates (G-A through G-I) and architecture guards
(Gᵃ¹–Gᵃ⁶) **before** implementation. Honesty check
([[feedback_cut_must_deliver_the_name]]):

- G-A, G-B, G-C, G-D, G-E, G-F, Gᵃ¹–Gᵃ⁶ must fail against `main`
  because the Rune `broadcast_join` authoring path and strategy-aware
  builder do not exist yet.
- G-G, G-H, G-I are regression pins — must pass against `main` AND stay
  green after the cut.
- The retargeted 122 guards must fail against `main`-with-rename-only (a
  partial implementation that renames but doesn't add the broadcast arm
  would let the helper-rename guard pass but Gᵃ⁴ fail).

Test homes:
- G-A, G-D, G-H, G-I: `crates/toron/src/lib.rs` distributed-test module
  (where the existing `:594` + `:606` tests live).
- G-B, G-C, G-E, G-G:
  `crates/toron/tests/rune_relation_handles_test.rs`, beside the existing
  Rune join gates and `rune_plan` helper.
- G-F: a crate-local unit test in `crates/toron/src/sql_planner.rs` or
  another crate-local test module, because `build_join_plan` is
  `pub(crate)` and integration tests cannot call it directly.
- Gᵃ¹–Gᵃ⁶ + 122 retargets:
  `crates/toron/tests/architecture/rune_frontend.rs`.

## Phase 1 — Helper rename + strategy-aware refactor

In `sql_planner.rs:4042-4063`, replace `build_hash_join_plan` with
`build_join_plan`:

```rust
pub(crate) fn build_join_plan(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: JoinKind,
    on: Vec<(Expr, Expr)>,
    strategy: JoinStrategy,
) -> Result<LogicalPlan, FrontendError> {
    let (left_keys, right_keys) = derive_join_exchange_keys(&on, &left, &right)?;
    match strategy {
        JoinStrategy::HashPartitioned => Ok(LogicalPlan::Join {
            left: Box::new(LogicalPlan::Exchange {
                input: Box::new(left),
                partition_count: DEFAULT_JOIN_PARTITION_COUNT,
                key: left_keys,
            }),
            right: Box::new(LogicalPlan::Exchange {
                input: Box::new(right),
                partition_count: DEFAULT_JOIN_PARTITION_COUNT,
                key: right_keys,
            }),
            kind,
            on,
            strategy,
        }),
        JoinStrategy::Broadcast => {
            if kind != JoinKind::Inner {
                return Err(unsupported(
                    "broadcast join is only supported for INNER joins",
                ));
            }
            Ok(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Exchange {
                    input: Box::new(left),
                    partition_count: 1,
                    key: left_keys,
                }),
                right: Box::new(right),
                kind,
                on,
                strategy,
            })
        }
    }
}
```

Notes:
- `derive_join_exchange_keys` unchanged (already takes `&on, &left,
  &right` and produces left/right key vecs).
- The HashPartitioned arm is bit-identical to the old body.
- The Broadcast arm constructs `partition_count: 1` for LEFT, leaves
  RIGHT bare (no `Exchange` wrap) — matches what `fragment.rs:484-489`
  expects.
- `unsupported(...)` is the existing helper used at `sql_planner.rs:4074`.

## Phase 2 — Update 3 production callsites

```rust
// sql_planner.rs:2646 — was:
//     plan = build_hash_join_plan(plan, right, kind, on)?;
// becomes:
plan = build_join_plan(plan, right, kind, on, JoinStrategy::HashPartitioned)?;

// sql_planner.rs:3598 — was:
//     build_hash_join_plan(...)
// becomes:
build_join_plan(/* same args */ , JoinStrategy::HashPartitioned)

// rune_engine.rs:414 (inside join_with_kind) — was:
//     match build_hash_join_plan(left, right, kind, pairs) { ... }
// becomes:
join_with_strategy(left, right, kind, on, JoinStrategy::HashPartitioned)
```

The new shared `join_with_strategy` helper performs the existing
`expr_to_join_keys` bridge and calls
`build_join_plan(left, right, kind, pairs, strategy)`.

Also update the import at `rune_engine.rs:7`:
```rust
use crate::sql_planner::{SqlPlanner, build_join_plan, expr_to_join_keys};
```

Grep before editing for any internal `sql_planner.rs` test helpers that
called the old name; update those to use the new helper with explicit
`JoinStrategy::HashPartitioned`.

## Phase 3 — Add Rune `broadcast_join` method

In `rune_engine.rs`, near the other 122 join methods (`:230-262`):

```rust
impl RuneDataFrame {
    // ... existing methods ...

    /// Broadcast-join `self` (the small/build side) against `right` (the probe side).
    /// Receiver is the LEFT/build/broadcast side; INNER-only.
    /// `small.broadcast_join(big, on)` reads "broadcast `small`, probe `big`."
    pub fn broadcast_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
        join_with_strategy(
            self.plan,
            right.plan,
            JoinKind::Inner,
            on,
            JoinStrategy::Broadcast,
        )
    }
}

fn join_with_kind(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: JoinKind,
    on: RuneExpr,
) -> VmResult<RuneDataFrame> {
    join_with_strategy(left, right, kind, on, JoinStrategy::HashPartitioned)
}

fn join_with_strategy(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: JoinKind,
    on: RuneExpr,
    strategy: JoinStrategy,
) -> VmResult<RuneDataFrame> {
    let pairs = match expr_to_join_keys(&on.expr) {
        Ok(pairs) => pairs,
        Err(error) => return VmResult::panic(error.to_string()),
    };
    match build_join_plan(left, right, kind, pairs, strategy) {
        Ok(plan) => VmResult::Ok(RuneDataFrame { plan }),
        Err(error) => VmResult::panic(error.to_string()),
    }
}
```

The doc-comment is load-bearing — it pins the receiver convention at the
method's surface. The shared helper keeps `.join(...)` and
`.broadcast_join(...)` on the same predicate-to-key bridge.

In `create_toron_module` at `:833`, near `:878-879`:
```rust
m.associated_function("broadcast_join", RuneDataFrame::broadcast_join)?;
```

Place it right after the 8 kind-specific methods + the `join` alias for
discoverability.

## Phase 4 — Retarget 122 architecture guards (discipline #4)

In `tests/architecture/rune_frontend.rs`:

**`adr122_join_builder_is_pair_shaped_and_shared`** (`:1713-1727`)
becomes:
```rust
let builder = source_block(&planner, "pub(crate) fn build_join_plan");
assert!(builder.contains("strategy: JoinStrategy"));
assert!(planner.contains(
    "build_join_plan(plan, right, kind, on, JoinStrategy::HashPartitioned)"
));
assert!(lower_exists.contains("build_join_plan("));  // SQL :3598 caller
assert!(rune.contains(
    "join_with_strategy(left, right, kind, on, JoinStrategy::HashPartitioned)"
));
assert!(rune.contains("JoinStrategy::Broadcast"));
assert!(rune.contains("build_join_plan(left, right, kind, pairs, strategy)"));
assert!(!planner.contains("fn build_hash_join_plan"));
```

**`adr122_join_builder_inserts_terminal_hash_exchanges`** (`:1749-1758`)
becomes:
```rust
let builder = source_block(&planner, "pub(crate) fn build_join_plan");
let hash_arm = builder
    .split("JoinStrategy::HashPartitioned =>")
    .nth(1)
    .and_then(|rest| rest.split("JoinStrategy::Broadcast =>").next())
    .expect("HashPartitioned arm");
assert!(hash_arm.contains("partition_count: DEFAULT_JOIN_PARTITION_COUNT"));
assert_eq!(hash_arm.matches("LogicalPlan::Exchange").count(), 2);
```

**`adr122_no_executor_pbc_wendelon_or_scheduler_change`** (`:1816+`):
extend the reject-set loop to also reject `adr125_*` markers. Matches
the 123→124 widening pattern verbatim.

## Phase 5 — Add ADR 125 architecture guards

In `tests/architecture/rune_frontend.rs`, after the retargeted 122
guards:

```rust
#[test]
pub(crate) fn adr125_rune_exposes_single_broadcast_join_authoring_method() {
    let rune = std::fs::read_to_string(...).expect("read rune_engine");
    assert!(rune.contains("pub fn broadcast_join(self, right: RuneDataFrame, on: RuneExpr)"));
    assert!(rune.contains("m.associated_function(\"broadcast_join\""));
    for kind in ["left", "right", "full", "left_semi", "right_semi", "left_anti", "right_anti"] {
        let method = format!("broadcast_{kind}_join");
        assert!(!rune.contains(&method), "unexpected per-kind broadcast method: {method}");
    }
}

#[test]
pub(crate) fn adr125_strategy_aware_join_builder_is_single_helper() {
    let planner = std::fs::read_to_string(...).expect("read sql_planner");
    assert_eq!(planner.matches("pub(crate) fn build_join_plan(").count(), 1);
    assert!(!planner.contains("fn build_hash_join_plan"));
    let builder = source_block(&planner, "pub(crate) fn build_join_plan");
    assert!(builder.contains("JoinStrategy::HashPartitioned =>"));
    assert!(builder.contains("JoinStrategy::Broadcast =>"));
}

#[test]
pub(crate) fn adr125_frontend_rejects_broadcast_non_inner() {
    let planner = std::fs::read_to_string(...).expect("read sql_planner");
    let builder = source_block(&planner, "pub(crate) fn build_join_plan");
    let broadcast_arm = builder
        .split("JoinStrategy::Broadcast =>")
        .nth(1)
        .expect("Broadcast arm");
    assert!(broadcast_arm.contains("kind != JoinKind::Inner"));
    assert!(broadcast_arm.contains("broadcast join is only supported for INNER joins"));
}

#[test]
pub(crate) fn adr125_broadcast_authoring_builds_left_exchange_one_and_bare_right() {
    let planner = std::fs::read_to_string(...).expect("read sql_planner");
    let builder = source_block(&planner, "pub(crate) fn build_join_plan");
    let broadcast_arm = broadcast_strategy_arm(&builder);
    assert!(broadcast_arm.contains("partition_count: 1"));
    assert_eq!(broadcast_arm.matches("LogicalPlan::Exchange").count(), 1);
    assert!(broadcast_arm.contains("right: Box::new(right)"));
    assert!(!broadcast_arm.contains("DEFAULT_JOIN_PARTITION_COUNT"));
}

#[test]
pub(crate) fn adr125_no_executor_fragmenter_binder_pbc_wendelon_change() {
    for path in [...] {
        let source = std::fs::read_to_string(...).expect("read source");
        assert!(!source.contains("ADR 125"), "{path}");
        assert!(!source.contains("adr125"), "{path}");
    }
}

#[test]
pub(crate) fn adr125_no_sql_hint_authoring_yet() {
    let planner = std::fs::read_to_string(...).expect("read sql_planner");
    let lower_join = source_block(&planner, "fn lower_join");
    assert!(lower_join.contains("JoinStrategy::HashPartitioned"));
    assert!(!lower_join.contains("JoinStrategy::Broadcast"));
}
```

Use a small helper such as `broadcast_strategy_arm` that slices from
`JoinStrategy::Broadcast =>` to the end of the `build_join_plan` block, or
balances braces. Do not split on the first `}`; that can stop inside the
`if kind != JoinKind::Inner` block or the left `Exchange` literal.

## Phase 6 — Add behavior gates

**G-A** (`crates/toron/src/lib.rs`, distributed-test module): mirror the
`:594` test plumbing but author via Rune for both queries. If the
scaffolding for "Rune source → LogicalPlan → distributed run" doesn't
already exist for tests, add the smallest local helper that runs a Rune
script through `RuneEngine` and returns the resulting `LogicalPlan`.

**G-D** (`crates/toron/src/lib.rs`): mirror `:606` cap-failure test with
the Rune-authored broadcast plan.

**G-B / G-C / G-E / G-G** (`tests/rune_relation_handles_test.rs`): small
in-process tests using the existing `rune_plan` helper to build the
authored plan, then either inspect the `LogicalPlan` structure (G-B,
G-G), bind and inspect the resulting schema (G-C), or compare
qualifier-resolution between hash and broadcast paths (G-E).

**G-F** (`src/sql_planner.rs` crate-local test): directly call
`build_join_plan` with a non-INNER `JoinKind` and
`JoinStrategy::Broadcast`; assert the unsupported frontend error.

## Phase 7 — Verification

Run in order:
```sh
cargo fmt
cargo test -p toron broadcast_join          # new behavior tests
cargo test -p toron --test architecture_test adr122
cargo test -p toron --test architecture_test adr125
cargo test -p toron rune_authored_broadcast_join_digest_matches_hash_join_over_tcp
cargo test -p toron rune_authored_broadcast_join_respects_cap_terminally
cargo test -p toron distributed_broadcast_join_digest_matches_hash_join_over_tcp
cargo test -p toron distributed_broadcast_join_over_cap_fails_terminally_over_tcp
cargo test -p toron
cargo check -p toron --all-targets
git diff --check
```

The `--all-targets` check per
[[feedback_verify_all_targets_on_new_ir_field]]: the helper rename is a
function-signature change that propagates to any bench/example using
`build_hash_join_plan`; `cargo check --all-targets` catches what `cargo
test` alone skips.

## Risk register

- **Helper rename ripple beyond 3 callers.** Grounding identified exactly
  3 production callers. `cargo check --all-targets` catches any 4th in
  benches/examples/tests. The rename is mechanical — no missed semantics.
- **G-A integration scaffolding may not exist for Rune authoring →
  distributed run.** The existing `:594` test uses hand-rolled IR. If
  the Rune-source-to-LogicalPlan path requires test-harness wiring not
  present in `src/lib.rs`, add a small local `RuneEngine` helper instead
  of routing around the authoring path.
- **`broadcast_join` keyword collision with Rune.** Per ADR 081
  doctrine, Rune authoring method names must not collide with Rune
  keywords. `broadcast` and `join` are not Rune 0.14 keywords; verify
  by attempting compilation.
- **122 builder-shape guard retargeting strictness.** The retarget must
  assert the new shape but accept the same downstream behavior. Lean:
  substring matches over exact-line matches; match existing 122 guard
  style at `:1716-1725`.
- **Rune `RuneDataFrame` method ordering in `create_toron_module`.**
  Place `broadcast_join` registration adjacent to the 8 kind methods at
  `:878-879+` for discoverability; no functional impact.
- **Strategy literal count drift.** After the rename, strategy choices are
  intentionally explicit at callsites, so there are multiple production
  `JoinStrategy::HashPartitioned` references. The architecture claim is
  not "single literal site"; it is "one strategy-aware builder and
  explicit strategy selection." The Gᵃ² guard scopes to exactly one
  `pub(crate) fn build_join_plan` definition plus both strategy arms.

## Acceptance

ADR 125 is accepted when:
- All 9 behavior gates (G-A through G-I) pass.
- All 6 ADR 125 architecture guards (Gᵃ¹–Gᵃ⁶) pass.
- The 3 retargeted ADR 122 guards pass.
- 115–124 regression suites green.
- `cargo check -p toron --all-targets` clean.
- `git diff --check` clean.
- No file outside `sql_planner.rs`, `rune_engine.rs`, `lib.rs`,
  `tests/rune_relation_handles_test.rs`, and
  `tests/architecture/rune_frontend.rs` is touched.
- The receiver convention (LEFT = build = small) is documented at the
  `broadcast_join` method's doc-comment AND pinned by G-B's structural
  assertion. **The cut delivers its name: a single Rune authoring
  method that constructs the existing broadcast execution plan, with
  no substrate change.**
