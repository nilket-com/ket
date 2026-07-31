# Impl Plan — ADR 105: Adjacent Identity `Project` Coalescing

**Migration for** `plans/adr/105_adjacent_identity_project_coalescing.md`. **Scope:** Toron `binder.rs`
(two private helpers + one hook edit) + behavior tests + architecture guards. **Non-scope:** no SQL planner
change, no IR/`ExecutionPlan` variant, no executor/PBC/storage/coordinator/Wendelon/`information_schema`
surface, no project-into-scan folding, no general project fusion. One helper call in an existing optimizer
arm.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Post-bind optimizer entry | `bind()` → `push_down_scan_projection(plan)` after `bind_plan` (`binder.rs:64`) |
| Recursive pass | `optimize_for_required` (`binder.rs:3592`); `push_down_scan_projection` delegates (`:3588`) |
| Hook point | `ExecutionPlan::Project` arm final reconstruction (`binder.rs:3696`); `output_remap` at `:3694` |
| Program APIs | `BoundExprProgram.roots` (field), `.node`, `.root_count`, `.root_logical_type` (`ir/execution.rs:108,586,555,602`) |
| Metadata decode | `decode_column_logical_type(field)` → `Ok(NAKED)` for unannotated; already imported in `binder.rs` (`ir.rs:320`) |
| 104 rename source | `bind_alias_column_rename` emits ordinal-identity `Project` (`binder.rs:845`) |
| Rune recurrence | `RuneDataFrame::project` / `.project_named` wrap `LogicalPlan::Project` (`rune_engine.rs:215,226`) |
| Receipt trace (unaffected) | `stored_refinement_receipt` recurses transparent `Project` chains (`binder.rs:1605`), runs pre-pass |

## Phase 1 — Add coalescing helpers (`binder.rs`, near `optimize_for_required` support fns)

```rust
/// Coalesce `Project(parent identity over Project(child))` into one
/// `Project` that keeps the parent's output schema while reusing the
/// child's input and expression program. ADR 105 D2–D5.
fn collapse_adjacent_identity_project(
	input: ExecutionPlan,
	parent_exprs: BoundExprProgram,
	parent_output_schema: SchemaRef,
) -> ExecutionPlan {
	let collapsible = matches!(
		&input,
		ExecutionPlan::Project { output_schema, .. }
			if is_metadata_transparent_identity_project(&parent_exprs, output_schema)
	);
	if collapsible {
		let ExecutionPlan::Project {
			input: child_input,
			exprs: child_exprs,
			..
		} = input
		else {
			unreachable!("collapsible implies the child is a Project");
		};
		return ExecutionPlan::Project {
			input: child_input,
			exprs: child_exprs,
			output_schema: parent_output_schema,
		};
	}
	ExecutionPlan::Project {
		input: Box::new(input),
		exprs: parent_exprs,
		output_schema: parent_output_schema,
	}
}

/// True iff `parent_exprs` is a dense positional-identity pass-through of
/// `child_output_schema`: every root a direct column at ordinal = index,
/// matching physical type, nullability, and durable logical identity.
/// Names are deliberately ignored. ADR 105 D3/D4.
fn is_metadata_transparent_identity_project(
	parent_exprs: &BoundExprProgram,
	child_output_schema: &SchemaRef,
) -> bool {
	let fields = child_output_schema.fields();
	if parent_exprs.root_count() != fields.len() {
		return false;
	}
	for (idx, field) in fields.iter().enumerate() {
		let field = field.as_ref();
		let Some(root_id) = parent_exprs.roots.get(idx).copied() else {
			return false;
		};
		let Some(BoundExprNode::Column(column)) = parent_exprs.node(root_id) else {
			return false;
		};
		let ordinal = ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow"));
		if column.ordinal != ordinal {
			return false;
		}
		if column.data_type != *field.data_type() || column.nullable != field.is_nullable() {
			return false;
		}
		// NAKED matches NAKED on both sides; a mismatch implies a logical
		// ascription/cast (block) or decode error (block, conservative).
		if parent_exprs.root_logical_type(idx) != decode_column_logical_type(field).ok() {
			return false;
		}
	}
	true
}
```

Notes: `roots` is a public field (not a method); `node`/`root_logical_type` return `Option`. Do **not**
compare field names — names intentionally come from `parent_output_schema` (D4). Decode errors return
`false` here; this is optimization-only, and binder/catalog paths own fail-closed metadata validation.
The borrow-then-move shape avoids reconstructing the child on the non-collapse path.

## Phase 2 — Hook the `Project` arm (`binder.rs:3696`)

Keep all existing pruning / remap / schema logic. Replace only the final reconstruction:

```rust
            // ... after `output_remap = ordinal_remap(&requested_outputs_vec)` (binder.rs:3694):
            Ok((
                collapse_adjacent_identity_project(input, exprs, output_schema),
                output_remap,
            ))
```

Do **not** alter `output_remap` (D5). Because `optimize_for_required` recurses bottom-up, deeper identity
chains collapse pairwise as the recursion returns.

## Phase 3 — SQL behavior tests (`tests/logical_type_ddl_test.rs`)

Local helper:

```rust
fn project_count(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Project { input, .. } => 1 + project_count(input),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => project_count(input),
		ExecutionPlan::Join { left, right, .. } => project_count(left) + project_count(right),
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => project_count(source),
		_ => 0,
	}
}
```

Tests (queries + assertions per ADR §Behavior gates):
`explicit_derived_alias_columns_collapse_adjacent_identity_project`,
`explicit_derived_alias_columns_collapse_after_outer_projection`,
`plain_derived_outer_projection_collapses_adjacent_projects`,
`base_table_alias_columns_keep_project_boundary` (assert one project, not zero),
`wildcard_derived_alias_columns_keep_project_boundary`.

Keep green: `alias_columns_preserve_refinement_elision_base_explicit_and_wildcard`,
`alias_columns_computed_projection_still_not_transparent`,
`alias_columns_duplicate_names_inherit_ambiguity`,
`derived_table_source_scope_rejects_general_subquery_features`.

## Phase 4 — Rune behavior test (`tests/rune_relation_handles_test.rs`)

```rust
#[tokio::test]
async fn rune_project_named_chains_collapse_adjacent_projects() {
	let logical = rune_plan(r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id, amount FROM sales")
               .project_named([col("id").named("sale_id"), col("amount").named("net")])
               .project_named([col("sale_id").named("id2")])
        }
    "#).await.expect("project_named chain");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await.expect("bind project_named chain");
	assert_schema_names(bound.schema().as_ref(), &["id2"]);
	assert_eq!(execution_project_count(&bound), 1);
}
```

Add an `execution_project_count` helper equivalent to `project_count` if one is not already present. Pins
the non-alias recurrence so the helper does not drift back to an ADR-104-only special case.

## Phase 5 — Architecture guards (`tests/architecture/predicate_pushdown.rs`)

- `adr105_project_coalescing_lives_in_existing_post_bind_optimizer` — `bind` contains
  `push_down_scan_projection(plan)`; `optimize_for_required` contains
  `collapse_adjacent_identity_project(input, exprs, output_schema)`.
- `adr105_coalescing_is_adjacent_project_only` — `collapse_adjacent_identity_project` matches a child
  `ExecutionPlan::Project` and contains **no** direct removal markers for `ExecutionPlan::Scan`,
  `ExecutionPlan::SystemTable`, `ExecutionPlan::EmptyRelation`, `ExecutionPlan::Filter`,
  `ExecutionPlan::Aggregate`, `ExecutionPlan::Exchange`, `ExecutionPlan::Sort`, `ExecutionPlan::Join`,
  `ExecutionPlan::Update`, or `ExecutionPlan::InsertSelect`.
- `adr105_identity_requires_ordinal_and_metadata_match` — `is_metadata_transparent_identity_project`
  contains `BoundExprNode::Column`, `ColumnOrdinal(u32::try_from(idx)`, `column.ordinal`,
  `field.data_type()`, `field.is_nullable()`, `root_logical_type(idx)`, `decode_column_logical_type(field)`.
- `adr105_identity_check_ignores_names_parent_schema_wins` — identity helper does **not** contain
  `field.name`; coalescer contains `exprs: child_exprs` and `output_schema: parent_output_schema`.
- `adr105_no_sql_ir_exec_pbc_storage_wendelon_information_schema_surface` — no ADR105 helper markers in
  `sql_planner.rs`, `ir.rs` enum surfaces, `exec.rs`, `pbc.rs`, `storage.rs`, `coordinator.rs`, Wendelon
  sources, or the information-schema binder block.

## Phase 6 — Verification

```sh
cargo fmt
cargo test -p toron --test logical_type_ddl_test            # collapse + 104 regression
cargo test -p toron --test rune_relation_handles_test rune_project_named_chains
cargo test -p toron --test architecture_test adr105
cargo test -p toron --test architecture_test adr104
cargo check -p toron
cargo test -p toron                                          # full suite green, no red guard
git diff --check
```

## Acceptance

Adjacent parent-identity `Project` over child `Project` coalesces inside the existing post-bind optimizer;
parent schema wins; SQL alias and Rune `.project_named` names survive; base-table and wildcard alias rename
projects are not folded into scans; computed/non-transparent projections do not gain refinement receipts;
`output_remap` unchanged; N-deep chains collapse pairwise; ADR 100/104 gates remain green; no
SQL/IR/executor/PBC/storage/Wendelon/`information_schema` surface change; Toron suite green.
