# Implementation Plan: ADR 027 — `ORDER BY` Computed Expressions over Aggregate Output

**Companion ADR:** `plans/adr/0027_order_by_computed_expressions.md`

## Goal

Generalize the existing ADR 025 / 026 hidden-projection
mechanism so aggregate queries can execute computed
`ORDER BY` keys over the visible aggregate output schema:

```sql
SELECT region, SUM(amount)
FROM sales
GROUP BY region
ORDER BY SUM(amount) + 100 DESC
```

while preserving:

- ADR 010's ordinal-only `ExecutionSortKey` contract.
- ADR 023's hidden-aggregate-key and HAVING-alias rejects.
- ADR 024 D6's computed-aggregate-argument reject.
- ADR 025 / 026 `Cast` / `TryCast` behavior (PBC
  ineligibility, lattice, identity-no-elision, kernel-error
  prefix).
- ADR 022's loud-failure / sticky-NaN philosophy (for any
  inner expression errors propagated up through the hidden
  projection).

No new IR variant. No new operator. No new kernel. No
fragmenter change. No Eq/Hash refactor. No
`GroupedAggregateSignature` widening.

Seven-step rollout: baseline confirmation → gate
generalization → visible-output validator → wire validator
into projection → architecture guards → fixtures → final
verification.

## Touchpoint sweep (code-grounded, current `main`)

| File | Anchor | Phase |
|---|---|---|
| `crates/toron/src/sql_planner.rs::sort_needs_hidden_projection` | widen gate to non-direct-key trigger | 1 |
| `crates/toron/src/sql_planner.rs::expr_contains_cast_family` | **remove entirely** | 1 |
| `crates/toron/src/sql_planner.rs::sort_key_is_direct_output_reference` | preserved (no change) | 1 (no-op) |
| `crates/toron/src/sql_planner.rs::validate_hidden_sort_key_refs` | **NEW** exhaustive walker | 2 |
| `crates/toron/src/sql_planner.rs::add_hidden_sort_projection` | wire validator before appending each hidden key | 3 |
| `crates/toron/src/sql_planner.rs::visible_output_names` | preserved (no change) | 3 (no-op) |
| `crates/toron/src/sql_planner.rs::has_duplicate_names` | preserved (no change) | 3 (no-op) |
| `crates/toron/src/sql_planner.rs::unique_hidden_sort_name` | preserved (no change) | 3 (no-op) |
| `crates/toron/src/binder.rs::bind_sort_key_ordinal` | preserved (catch-all for non-aggregate computed sort keys, D10) | 3 (no-op) |
| `crates/toron/tests/architecture_test.rs::adr025_order_by_cast_uses_hidden_sort_projection` | adjust body for generalized mechanism | 4 |
| `crates/toron/tests/architecture_test.rs::adr026_try_cast_order_by_uses_hidden_sort_projection` | adjust body for generalized mechanism | 4 |
| `crates/toron/tests/architecture_test.rs` | 3 NEW ADR 027 guards | 4 |
| `crates/toron/tests/group_by_test.rs` | positive + negative + regression + plan-shape fixtures | 5 |

## Phase 0 — Baseline Confirmation

Before edits, confirm starting state via grep:

```sh
rg -n "expr_contains_cast_family|sort_needs_hidden_projection|add_hidden_sort_projection|unique_hidden_sort_name|sort_key_is_direct_output_reference" crates/toron/src/sql_planner.rs
rg -n "adr025_order_by_cast_uses_hidden_sort_projection|adr026_try_cast_order_by_uses_hidden_sort_projection" crates/toron/tests/architecture_test.rs
```

Verify:

- `expr_contains_cast_family` has exactly **one** production
  caller (`sort_needs_hidden_projection`). Mechanical
  removal is safe.
- `sort_key_is_direct_output_reference` body is
  `matches!(expr, Expr::Column(_) | Expr::Literal(_))`.
- `add_hidden_sort_projection` already:
  - derives visible names via `visible_output_names`;
  - rejects duplicates via `has_duplicate_names`;
  - appends hidden keys via `unique_hidden_sort_name`;
  - rewrites the `SortExpr` to reference hidden columns;
  - returns a `FinalProjection` that strips hidden keys.
- `unique_hidden_sort_name` uses `__toron_order_key_{idx}`
  with numeric-suffix collision handling.

**Preserve verbatim** (ADR 025 / 026 contracts):

- `unique_hidden_sort_name` body.
- `visible_output_names` body.
- `has_duplicate_names` body.
- `add_hidden_sort_projection`'s sandwich shape:
  `Project(visible + hidden) → Exchange → Sort →
  Project(visible only)`.
- Duplicate-name diagnostic
  `"ORDER BY computed aggregate expression requires unique output names"`.
- `bind_sort_key_ordinal` catch-all diagnostic.

## Phase 1 — Gate Generalization

**Goal.** Widen `sort_needs_hidden_projection` to
non-direct-key trigger; remove `expr_contains_cast_family`.

**Files:** `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **Update `sort_needs_hidden_projection`:**
   ```rust
   fn sort_needs_hidden_projection(order_by: &[SortExpr]) -> bool {
       order_by
           .iter()
           .any(|key| !sort_key_is_direct_output_reference(&key.expr))
   }
   ```

2. **Remove `fn expr_contains_cast_family` entirely.**
   Mechanical — single caller verified at Phase 0.

3. **Preserve `sort_key_is_direct_output_reference`**
   unchanged.

**Verification:**

```sh
cargo check -p toron
```

Expect architecture-test compile-time / runtime failures
until Phase 4 — the existing ADR 025 / 026 guards still
reference the removed helper.

## Phase 2 — Visible-Output Validator

**Goal.** Add `validate_hidden_sort_key_refs` walker that
prevents raw-input-column and qualified-column leakage.

**Files:** `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **Add `validate_hidden_sort_key_refs`:**
   ```rust
   fn validate_hidden_sort_key_refs(
       expr: &Expr,
       visible_names: &HashSet<String>,
   ) -> Result<(), FrontendError> {
       match expr {
           Expr::Column(column) => {
               if column.qualifier.is_some()
                   || !visible_names.contains(&column.name.0)
               {
                   return Err(unsupported(
                       "ORDER BY computed aggregate expression must reference visible output columns",
                   ));
               }
               Ok(())
           }
           Expr::Literal(_) => Ok(()),
           Expr::Cast(inner, _) | Expr::TryCast(inner, _) => {
               validate_hidden_sort_key_refs(inner, visible_names)
           }
           Expr::Add(left, right)
           | Expr::Sub(left, right)
           | Expr::Mul(left, right)
           | Expr::Div(left, right)
           | Expr::Eq(left, right)
           | Expr::Neq(left, right)
           | Expr::Lt(left, right)
           | Expr::Lte(left, right)
           | Expr::Gt(left, right)
           | Expr::Gte(left, right)
           | Expr::And(left, right)
           | Expr::Or(left, right) => {
               validate_hidden_sort_key_refs(left, visible_names)?;
               validate_hidden_sort_key_refs(right, visible_names)
           }
           Expr::Not(inner)
           | Expr::IsNull(inner)
           | Expr::IsNotNull(inner)
           | Expr::TimeBucket(_, inner) => {
               validate_hidden_sort_key_refs(inner, visible_names)
           }
       }
   }
   ```

   Notes:
   - The match must be exhaustive over the current `Expr`
     surface. Compiler exhaustiveness catches future variant
     additions and forces a deliberate decision at that
     time.
   - Use the **same** stable diagnostic for both
     qualified-column and non-visible-column rejects (D2
     contract).
   - `Expr::Literal` accepts unconditionally (D9 — no column
     references to validate).

**Verification:** `cargo check -p toron` should still
report the existing ADR 025 / 026 guard failures until
Phase 4.

## Phase 3 — Wire Validator into Hidden Projection

**Goal.** Call `validate_hidden_sort_key_refs` in
`add_hidden_sort_projection` before appending each
non-direct hidden key.

**Files:** `crates/toron/src/sql_planner.rs`

**Tasks:**

1. **After existing duplicate visible-name validation**,
   build the visible-name set:
   ```rust
   let visible_names: HashSet<String> = output_names.iter().cloned().collect();
   ```

2. **In the existing ORDER BY loop**, call the validator
   for non-direct keys before appending the expression:
   ```rust
   for (idx, sort_expr) in order_by.iter().enumerate() {
       if sort_key_is_direct_output_reference(&sort_expr.expr) {
           rewritten_order_by.push(sort_expr.clone());
           continue;
       }
       // NEW (D2): validate computed key against visible output names
       validate_hidden_sort_key_refs(&sort_expr.expr, &visible_names)?;
       // ... existing logic: generate hidden name via unique_hidden_sort_name,
       //     append expression to projected_exprs, rewrite SortExpr to hidden column ...
   }
   ```

3. **Preserve all other `add_hidden_sort_projection`
   behavior:**
   - `output_names` derivation via `visible_output_names`.
   - `has_duplicate_names` reject before any hidden keys
     are appended.
   - `unique_hidden_sort_name` generation with collision
     suffixing.
   - `FinalProjection` construction stripping hidden
     columns.
   - Second `Project` wrapping restoring visible-output
     schema.

**Verification:**

```sh
cargo check -p toron
```

## Phase 4 — Architecture Guards

**Goal.** Adjust 2 existing guards; add 3 new ADR 027
guards.

**Files:** `crates/toron/tests/architecture_test.rs`

**Tasks:**

1. **Adjust `adr025_order_by_cast_uses_hidden_sort_projection`:**
   - Drop any `expr_contains_cast_family` source-read.
   - Source-read `sort_needs_hidden_projection` for the
     non-direct-key predicate.
   - Source-read `validate_hidden_sort_key_refs` for the
     `Expr::Cast(inner, _) | Expr::TryCast(inner, _)` arm
     (verifies Cast continues to flow through the
     generalized mechanism).
   - Keep existing assertions for Project insertion,
     duplicate-name diagnostic, hidden naming, and final
     projection.

2. **Adjust `adr026_try_cast_order_by_uses_hidden_sort_projection`:**
   - Same shape — source-read the generalized gate and
     verify the `TryCast` arm in the validator.

3. **Add `adr027_hidden_sort_projection_gate_is_generalized`:**
   ```rust
   #[test]
   fn adr027_hidden_sort_projection_gate_is_generalized() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       let gate = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
           .into_iter().next().expect("sort_needs_hidden_projection body").1;
       assert!(gate.contains("!sort_key_is_direct_output_reference(&key.expr)"));
       assert!(
           !gate.contains("expr_contains_cast_family"),
           "gate must not call removed helper (ADR 027 D1)"
       );

       assert!(
           !planner.contains("fn expr_contains_cast_family"),
           "expr_contains_cast_family must be removed from production source (ADR 027 D1)"
       );
   }
   ```

4. **Add `adr027_hidden_sort_key_refs_are_visible_outputs`:**
   ```rust
   #[test]
   fn adr027_hidden_sort_key_refs_are_visible_outputs() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       let validator = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
           .into_iter().next().expect("validate_hidden_sort_key_refs body").1;

       // Column qualifier reject + visible-name membership
       assert!(validator.contains("column.qualifier.is_some()"));
       assert!(validator.contains("visible_names.contains(&column.name.0)"));
       // Stable diagnostic
       assert!(validator.contains(
           "ORDER BY computed aggregate expression must reference visible output columns"
       ));
       // Literal acceptance
       assert!(validator.contains("Expr::Literal(_) => Ok(())"));
       // Exhaustive Expr recursion
       assert!(validator.contains("Expr::Cast(inner, _) | Expr::TryCast(inner, _)"));
       for op in ["Add", "Sub", "Mul", "Div"] {
           assert!(validator.contains(&format!("Expr::{op}(left, right)")));
       }
       for op in ["Eq", "Neq", "Lt", "Lte", "Gt", "Gte", "And", "Or"] {
           assert!(validator.contains(&format!("Expr::{op}(left, right)")));
       }
       for op in ["Not", "IsNull", "IsNotNull"] {
           assert!(validator.contains(&format!("Expr::{op}(inner)")));
       }
       assert!(validator.contains("Expr::TimeBucket(_, inner)"));
   }
   ```

5. **Add `adr027_hidden_sort_projection_contract_is_pinned`:**
   ```rust
   #[test]
   fn adr027_hidden_sort_projection_contract_is_pinned() {
       let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
           .expect("read SQL planner source");

       // Helpers preserved
       assert!(planner.contains("fn visible_output_names"));
       assert!(planner.contains("fn has_duplicate_names"));
       assert!(planner.contains("fn unique_hidden_sort_name"));

       let mechanism = brace_blocks_after_marker(&planner, "fn add_hidden_sort_projection")
           .into_iter().next().expect("add_hidden_sort_projection body").1;

       // Validator call site (D2 wiring)
       assert!(mechanism.contains("validate_hidden_sort_key_refs"));
       // Naming convention (D6)
       assert!(planner.contains("__toron_order_key_"));
       // Duplicate-name diagnostic
       assert!(planner.contains(
           "ORDER BY computed aggregate expression requires unique output names"
       ));
       // Final projection construction
       assert!(mechanism.contains("FinalProjection"));
   }
   ```

6. **Preserve `bind_sort_key_ordinal` catch-all
   diagnostic** assertion in whichever existing guard
   covers it (ADR 010 / 015 / 023 area).

**Verification:**

```sh
cargo test -p toron --test architecture_test
```

## Phase 5 — Group-By + Execution Fixtures

**Goal.** Exhaustive positive / negative / regression /
plan-shape coverage.

**Files:** `crates/toron/tests/group_by_test.rs`

**Tasks:**

1. **Positive arithmetic ORDER BY:**
   ```sql
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) + 100 DESC
   SELECT SUM(amount) FROM sales ORDER BY SUM(amount) + 100
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) * 1.5 DESC
   SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY SUM(amount) / COUNT(*) DESC
   ```

2. **Alias composition (D7):**
   ```sql
   SELECT region, SUM(amount) AS s FROM sales GROUP BY region ORDER BY s + 1 DESC
   SELECT region, SUM(amount) AS s FROM sales GROUP BY region ORDER BY s / 10 DESC
   ```

3. **Multi-key mixed direct/computed:**
   ```sql
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) + 1 DESC, region
   ```

4. **HAVING + computed ORDER BY composition:**
   ```sql
   SELECT region, SUM(amount) FROM sales GROUP BY region
     HAVING SUM(amount) > 10
     ORDER BY SUM(amount) + 1 DESC
   ```

5. **Literal-only computed key (D9):**
   ```sql
   SELECT SUM(amount) FROM sales ORDER BY 1 + 1
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 1 + 1, region
   ```

6. **Regression — Cast / TryCast continue to work (R6):**
   ```sql
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY CAST(SUM(amount) AS FLOAT32) DESC
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY TRY_CAST(SUM(amount) AS FLOAT32) DESC
   ```

7. **Negative — hidden aggregate key reject (D3):**
   ```sql
   SELECT region FROM sales GROUP BY region ORDER BY SUM(amount)
   SELECT region FROM sales GROUP BY region ORDER BY SUM(amount) + 1
   ```
   Expects ADR 023's diagnostic:
   `"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"`.

8. **Negative — raw input column reject (D2):**
   ```sql
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY amount + 1
   ```
   Expects new ADR 027 diagnostic:
   `"ORDER BY computed aggregate expression must reference visible output columns"`.

9. **Negative — qualified column reject (D2):**
   ```sql
   SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY sales.region + 1
   ```
   Same diagnostic.

10. **Negative — computed aggregate argument reject (D4 preserved):**
    ```sql
    SELECT SUM(amount + tax) FROM sales
    SELECT SUM(CAST(amount AS BIGINT)) FROM sales
    SELECT SUM(TRY_CAST(amount AS BIGINT)) FROM sales
    ```
    Expects ADR 024 D6 diagnostic:
    `"aggregate arguments must be simple column references"`.

11. **Negative — duplicate visible output names (D6):**
    ```sql
    SELECT region, region FROM sales GROUP BY region ORDER BY SUM(amount) + 1
    ```
    Expects:
    `"ORDER BY computed aggregate expression requires unique output names"`.

12. **Plan-shape — direct ORDER BY bypasses hidden projection (R1):**
    - `ORDER BY region` → no hidden Project sandwich.
    - `ORDER BY 1` → no hidden Project sandwich.
    - Spot-check plan digests for five direct-ORDER-BY
      queries against pre-patch baselines.

13. **Non-aggregate computed ORDER BY out of scope (D10):**
    ```sql
    SELECT amount FROM sales ORDER BY amount + 1
    ```
    Verify: `aggregate_output_catalog.is_some()` guard
    routes through ADR 010 path; `bind_sort_key_ordinal`
    catch-all rejects with
    `"ORDER BY keys must reference output columns or positive ordinals"`.

**Verification:**

```sh
cargo test -p toron --test group_by_test
```

## Phase 6 — Final Verification

```sh
cargo fmt --check
cargo check -p toron
cargo test -p toron --test architecture_test
cargo test -p toron --test group_by_test
cargo test -p toron
```

If a formatting diff is found, run `cargo fmt` and rerun
`cargo fmt --check`.

Optional when workspace deps / feature flags change:

```sh
cargo build -p toron
```

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Plan digest regression for direct-ORDER-BY queries | Phase 5 task 12 spot-checks five queries. |
| R2 | `expr_contains_cast_family` removal breaks ADR 025 / 026 guards | Phase 4 tasks 1-2 adjust guards. |
| R3 | `validate_hidden_sort_key_refs` walker misses recursion site | Exhaustive `Expr` match; Phase 4 task 4 source-read covers every variant arm. |
| R4 | Raw-input column slips through if synthetic-name collision exists | D6 + `unique_hidden_sort_name` + D2 validator wired at the gate entry. |
| R5 | Hidden Project's column ordering disrupts ADR 023 catalog | Final Project restores visible-output schema; catalog binding unchanged. |
| R6 | Cast / TryCast regress | Phase 5 task 6 regression fixtures + Phase 4 guard adjustments. |
| R7 | Multi-key ORDER BY misroutes direct keys | D1 trigger uses `any(...)` — gate activates iff at least one key is non-direct; Phase 5 task 3 fixture. |
| R8 | Literal-only computed sort keys trigger hidden projection unnecessarily | D9 accepts them; Phase 5 task 5 fixture. |
| R9 | Future expression families silently inherit ADR 027 | D8 + compiler exhaustiveness on the validator. |
| R10 | Validator HashSet construction cost | O(visible-output-count); scope-local. |
| R11 | Non-aggregate computed ORDER BY triggers hidden projection | D10 + `aggregate_output_catalog.is_some()` guard. |
| R12 | HAVING + ORDER BY composition fails | Phase 5 task 4 fixture. |
| R13 | Diagnostic uniformity loses debugging context | D2 explicit on shared diagnostic for both reject cases. |
| R14 | `expr_contains_cast_family` has callers outside `sort_needs_hidden_projection` | Phase 0 baseline review confirms single-caller; mechanical removal. |

## Definition of Done

- [ ] `expr_contains_cast_family` removed from
      `sql_planner.rs`.
- [ ] `sort_needs_hidden_projection` uses the non-direct-key
      trigger.
- [ ] `validate_hidden_sort_key_refs` walker added with
      exhaustive `Expr` recursion and the stable
      visible-output diagnostic.
- [ ] `add_hidden_sort_projection` calls the validator
      before appending each hidden key.
- [ ] `visible_output_names`, `has_duplicate_names`,
      `unique_hidden_sort_name` preserved verbatim.
- [ ] `ExecutionSortKey` ordinal-only contract preserved
      (D5).
- [ ] `bind_sort_key_ordinal` catch-all preserved.
- [ ] `validate_aggregate_argument` column-only rule
      preserved (D4).
- [ ] ADR 023 hidden-aggregate-key reject preserved (D3).
- [ ] ADR 023 HAVING-alias reject preserved (D7).
- [ ] `__toron_order_key_{idx}` naming + duplicate-name
      reject + final-projection stripping ratified as
      contract (D6).
- [ ] Top-level ORDER BY accepts SELECT aliases inside
      computed sort keys (D7).
- [ ] Literal-only computed sort keys
      (`ORDER BY 1 + 1`) accepted (D9).
- [ ] Non-aggregate computed ORDER BY remains out of scope
      (D10).
- [ ] 3 new ADR 027 architecture guards pass.
- [ ] 2 existing guards (ADR 025 / 026 hidden-sort-key)
      adjusted to verify Cast and TryCast continue to flow
      through the generalized mechanism.
- [ ] Phase 5 fixtures pass (positive arithmetic / alias /
      multi-key / HAVING composition / literal-only /
      regression Cast+TryCast; negative
      hidden-aggregate-key / raw-input-column /
      qualified-column / computed-aggregate-arg /
      duplicate-name / non-aggregate; plan-shape
      direct-ORDER-BY parity).
- [ ] Plan digests for direct-ORDER-BY queries byte-identical
      pre/post-ADR-027.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.

## Verification commands

```sh
# Development cadence — catch missed exhaustive match sites
cargo check -p toron

# Pre-PR checks
cargo fmt --check
cargo test -p toron --test architecture_test
cargo test -p toron --test group_by_test
cargo test -p toron

# Optional when workspace deps change
cargo build -p toron
```
