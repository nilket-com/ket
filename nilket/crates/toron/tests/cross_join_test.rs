//! ADR 239 — CROSS JOIN substrate. SQL-lowering and binder-level behavior gates.
//!
//! The executor-level cartesian/multiplicity proof lives in `hash_join_kinds_test.rs` (reuses the
//! `Executor` harness) and the distributed value-discriminating gate in `distributed_process_test.rs`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableOptimizerFacts, TableRef};
use toron::distributed::budget::ByteCount;
use toron::error::BindingError;
use toron::ir::{
	AggregateFunction, ExecutionPlan, Expr, JoinKind, JoinStrategy, JoinStrategyIntent,
	LogicalPlan, TableId,
};
use toron::sql_planner::SqlPlanner;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

// ---------------------------------------------------------------------------
// SQL lowering (frontend only; no catalog needed)
// ---------------------------------------------------------------------------

#[test]
fn cross_join_lowers_to_keyless_cross() {
	match SqlPlanner::new()
		.plan("SELECT * FROM left_t CROSS JOIN right_t")
		.expect("plan cross join")
	{
		LogicalPlan::Join { kind, on, .. } => {
			assert_eq!(kind, JoinKind::Cross);
			assert!(on.is_empty(), "CROSS JOIN must be keyless (on: [])");
		}
		other => panic!("expected a Cross join, got {other:?}"),
	}
}

#[test]
fn comma_join_lowers_to_keyless_cross() {
	match SqlPlanner::new()
		.plan("SELECT * FROM left_t, right_t")
		.expect("plan comma join")
	{
		LogicalPlan::Join { kind, on, .. } => {
			assert_eq!(kind, JoinKind::Cross, "comma join == CROSS JOIN");
			assert!(on.is_empty());
		}
		other => panic!("expected a Cross join, got {other:?}"),
	}
}

#[test]
fn n_ary_comma_join_folds_left_deep_cross() {
	// `FROM a, b, c` -> `Cross(Cross(a, b), c)`: the outer Cross's LEFT is itself a Cross, proving the
	// separate `from.len() > 1` fence was lifted into a left-deep fold (not a single binary join).
	let LogicalPlan::Join { kind, on, left, .. } = SqlPlanner::new()
		.plan("SELECT * FROM a, b, c")
		.expect("plan n-ary comma join")
	else {
		panic!("expected an outer Cross join");
	};
	assert_eq!(kind, JoinKind::Cross);
	assert!(on.is_empty());
	match *left {
		LogicalPlan::Join { kind, on, .. } => {
			assert_eq!(kind, JoinKind::Cross, "the inner fold must also be Cross");
			assert!(on.is_empty());
		}
		other => panic!("expected a left-deep inner Cross, got {other:?}"),
	}
}

#[test]
fn cross_join_where_filters_after_cartesian() {
	// D3: WHERE composes as a Filter over the cartesian; no ON/equi extraction.
	match SqlPlanner::new()
		.plan("SELECT * FROM left_t CROSS JOIN right_t WHERE left_t.id = 1")
		.expect("plan cross join with where")
	{
		LogicalPlan::Filter { input, .. } => match *input {
			LogicalPlan::Join { kind, on, .. } => {
				assert_eq!(kind, JoinKind::Cross);
				assert!(on.is_empty(), "WHERE must not be extracted into ON keys");
			}
			other => panic!("expected a Filter over a Cross join, got {other:?}"),
		},
		other => panic!("expected a Filter, got {other:?}"),
	}
}

#[test]
fn cross_join_rejects_apply_natural_using_on_variants() {
	for sql in [
		"SELECT * FROM left_t CROSS APPLY right_t",
		"SELECT * FROM left_t NATURAL JOIN right_t",
		"SELECT * FROM left_t JOIN right_t USING (id)",
		"SELECT * FROM left_t CROSS JOIN right_t ON left_t.id = right_t.id",
	] {
		assert!(
			SqlPlanner::new().plan(sql).is_err(),
			"must stay rejected: {sql}"
		);
	}
}

// ---------------------------------------------------------------------------
// Binder: broadcast-only distributed + column order (D4 / D6)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cross_join_forces_broadcast_strategy() {
	let catalog = cross_catalog(Some(1_024));
	let bound = bind(&plan_cross(), &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("a CROSS join with a broadcast-eligible build must bind");

	assert_eq!(
		find_join_strategy(&bound),
		Some(JoinStrategy::Broadcast),
		"CROSS must force broadcast, never a (single-bucket) empty-key hash partition"
	);
}

#[tokio::test]
async fn cross_join_oversized_build_fails_loud() {
	// D4: build side over the broadcast cap -> explicit error, never a hidden single-task / empty-key hash.
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let err = bind(&plan_cross(), &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect_err("an oversized CROSS build must fail loud");

	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message.contains("broadcast-eligible")),
		"expected an explicit broadcast-eligibility failure, got {err:?}"
	);
}

#[tokio::test]
async fn cross_join_never_hash_partitions_empty_keys() {
	let catalog = cross_catalog(Some(1_024));
	let bound = bind(&plan_cross(), &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("bind cross join");

	assert_eq!(find_join_strategy(&bound), Some(JoinStrategy::Broadcast));
	assert!(
		!has_multi_partition_empty_key_exchange(&bound),
		"CROSS must not hash-partition on the empty key (the single-bucket distributed trap)"
	);
}

#[tokio::test]
async fn cross_join_preserves_sql_column_order_under_broadcast() {
	// left_t = (id: Int64, left_value: Utf8); right_t = (id: Int64, right_value: Int32).
	// Cross forces a deterministic SQL right-build (SwappedRightBuild). After the physical swap +
	// `restore_declared_join_order_project`, the SQL-visible columns stay left-then-right. A leaked
	// raw right-build order would instead show [Int64, Int32, Int64, Utf8].
	let catalog = cross_catalog(Some(1_024));
	let bound = bind(&plan_cross(), &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("bind cross join");

	let types: Vec<DataType> = bound
		.schema()
		.fields()
		.iter()
		.map(|field| field.data_type().clone())
		.collect();
	assert_eq!(
		types,
		vec![
			DataType::Int64,
			DataType::Utf8,
			DataType::Int64,
			DataType::Int32
		],
		"broadcast right-build must restore SQL-visible left-then-right column order"
	);
}

#[tokio::test]
async fn n_ary_comma_join_binds_left_deep_broadcast_cross_chain() {
	// Regression for ADR 239's left-deep comma fold: the outer Cross sees a previously-bound
	// broadcast Cross on its probe side. `broadcast_probe_linearity` must treat broadcast Cross like
	// broadcast Inner in the star-linear walk, or this binds as "not broadcast eligible" despite each
	// build side being small.
	let catalog = cross_catalog(Some(1_024));
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM left_t, right_t, third_t")
		.expect("plan n-ary comma join");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("n-ary comma Cross chain should bind");

	assert_eq!(join_strategy_count(&bound, JoinStrategy::Broadcast), 2);
	assert!(
		!has_multi_partition_empty_key_exchange(&bound),
		"nested comma Cross joins must not introduce empty-key hash exchanges"
	);
}

// ---------------------------------------------------------------------------
// ADR 270 — comma-join equi-join recognition (binder-side, syntax-agnostic).
//
// Discriminating lever: a *keyless* Cross with an oversized build fails loud
// (`cross_join_oversized_build_fails_loud`), but a *keyed* Inner join hash-partitions. So "an oversized
// build binds HashPartitioned" proves the equi was lifted into join keys (not left as a residual filter
// over a keyless Cross).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn adr270_comma_equi_recognized_as_keyed_inner() {
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM left_t, right_t WHERE left_t.id = right_t.id")
		.expect("plan comma equi join");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("a comma equi-join binds as a keyed inner join, not a rejected keyless cross");
	assert_eq!(
		find_join_strategy(&bound),
		Some(JoinStrategy::HashPartitioned),
		"an oversized build hash-partitions — only possible because the equi was lifted into keys"
	);
}

#[tokio::test]
async fn adr270_explicit_cross_join_equi_recognized_as_keyed_inner() {
	// Syntax-agnostic: an explicit CROSS JOIN with a cross-bridging equi rewrites the same way (it would
	// otherwise fail loud on the oversized build, exactly like the keyless comma cross).
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM left_t CROSS JOIN right_t WHERE left_t.id = right_t.id")
		.expect("plan explicit cross join with equi");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("explicit CROSS JOIN + bridging equi binds as a keyed inner join");
	assert_eq!(
		find_join_strategy(&bound),
		Some(JoinStrategy::HashPartitioned),
		"the explicit-cross equi is lifted into keys (syntax-agnostic recognition)"
	);
}

#[tokio::test]
async fn adr270_non_equi_comma_stays_keyless_cross_and_fails_loud() {
	// A non-equi cross-side predicate is not a join key: the comma join stays a keyless Cross, so an
	// oversized build still fails loud (nothing was lifted).
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM left_t, right_t WHERE left_t.id < right_t.id")
		.expect("plan non-equi comma join");
	let err = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect_err(
			"a non-equi comma join stays a keyless Cross and an oversized build fails loud",
		);
	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message.contains("broadcast-eligible")),
		"expected the keyless-cross broadcast-eligibility failure, got {err:?}"
	);
}

#[tokio::test]
async fn adr270_n_ary_comma_equi_lifts_both_edges() {
	// `FROM left_t, right_t, third_t WHERE left_t.id = right_t.id AND right_t.id = third_t.id`:
	// the outer Cross(Cross(left_t, right_t), third_t) bridges right_t.id = third_t.id, while
	// left_t.id = right_t.id is left-only at the outer Cross and must be ROUTED into the inner Cross to
	// lift. Both edges become keyed inner joins (two hash-partitioned joins under oversized builds).
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM left_t, right_t, third_t \
			 WHERE left_t.id = right_t.id AND right_t.id = third_t.id",
		)
		.expect("plan n-ary comma equi join");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("n-ary comma equi join binds as a keyed inner-join chain");
	assert_eq!(
		join_strategy_count(&bound, JoinStrategy::HashPartitioned),
		2,
		"both comma edges lift into keyed inner joins (the inner edge required routing down)"
	);
}

#[tokio::test]
async fn adr270_unsupported_join_key_type_stays_residual_filter() {
	// Float join keys are explicitly unsupported by the keyed-join path. Pre-270, this query could still
	// bind as a small broadcast Cross plus residual Float equality filter. ADR 270 must not turn that
	// legal filtered-cartesian shape into a `JoinKeyTypeUnsupported` error by lifting the equality.
	let catalog = float_cross_catalog(Some(1_024));
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM left_floats, right_floats WHERE left_floats.f = right_floats.f")
		.expect("plan float equality over comma join");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("unsupported key type stays a residual filter over a broadcast Cross");
	assert_eq!(
		find_join_strategy(&bound),
		Some(JoinStrategy::Broadcast),
		"float equality must stay residual; lifting would hit the unsupported float join-key path"
	);
}

#[tokio::test]
async fn scalar_cross_join_binds_as_local_cartesian() {
	// ADR 239 scalar-local Cross refinement: a CROSS recombine of two global-aggregate branches (each
	// provably one row) must bind as a local cartesian — NOT broadcast (the prior behavior failed loud
	// because aggregate branches carry no broadcast-eligible build) and NOT an empty-key hash
	// repartition (a single-partition gather only). This is the path ADR 240 global multi-dedup needs.
	let catalog = cross_catalog(None);
	let plan = LogicalPlan::Join {
		left: Box::new(global_count_distinct_branch("left_t", "left_value")),
		right: Box::new(global_count_distinct_branch("right_t", "right_value")),
		kind: JoinKind::Cross,
		on: vec![],
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let bound = bind(&plan, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("scalar CROSS recombine of global aggregates must bind locally");

	assert_eq!(
		find_join_strategy(&bound),
		Some(JoinStrategy::HashPartitioned),
		"a scalar CROSS binds as a single-partition local cartesian, not broadcast"
	);
	assert!(
		!has_multi_partition_empty_key_exchange(&bound),
		"scalar-local Cross must gather to a single partition, never an empty-key hash repartition"
	);
}

/// A global `COUNT(DISTINCT col)` branch: `Aggregate(group=[], Distinct(Project(scan, [col])))` — a
/// provably single-row plan (`row_upper_bound = 1`), the shape ADR 240 global recombine fans out.
fn global_count_distinct_branch(table: &str, col: &str) -> LogicalPlan {
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Distinct {
			input: Box::new(LogicalPlan::Project {
				input: Box::new(LogicalPlan::ScanNamed {
					table: TableRef::new(table),
					projection: None,
				}),
				exprs: vec![Expr::qualified_column(TableRef::new(table), col)],
				output_names: vec![Some(col.to_string())],
			}),
		}),
		group_expr: vec![],
		aggr_expr: vec![(
			AggregateFunction::Count,
			vec![Expr::column(col.to_string())],
		)],
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn plan_cross() -> LogicalPlan {
	SqlPlanner::new()
		.plan("SELECT * FROM left_t CROSS JOIN right_t")
		.expect("plan cross join")
}

fn find_join_strategy(plan: &ExecutionPlan) -> Option<JoinStrategy> {
	match plan {
		ExecutionPlan::Join { strategy, .. } => Some(*strategy),
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => find_join_strategy(input),
		_ => None,
	}
}

fn join_strategy_count(plan: &ExecutionPlan, expected: JoinStrategy) -> usize {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} => {
			usize::from(*strategy == expected)
				+ join_strategy_count(left, expected)
				+ join_strategy_count(right, expected)
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => join_strategy_count(input, expected),
		ExecutionPlan::Union { left, right, .. } => {
			join_strategy_count(left, expected) + join_strategy_count(right, expected)
		}
		_ => 0,
	}
}

/// A broadcast build publishes through a single-partition (partition_count == 1) exchange, which is
/// fine. A *hash* partition on the empty key is the forbidden single-bucket trap — a
/// `partition_count > 1` exchange with an empty key program.
fn has_multi_partition_empty_key_exchange(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			(*partition_count > 1 && key.is_empty_roots())
				|| has_multi_partition_empty_key_exchange(input)
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => has_multi_partition_empty_key_exchange(input),
		ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
			has_multi_partition_empty_key_exchange(left)
				|| has_multi_partition_empty_key_exchange(right)
		}
		_ => false,
	}
}

fn cross_catalog(right_byte_upper_bound: Option<u64>) -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor(1, "left_t", left_schema().as_ref()))
		.expect("register left_t");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(2, "right_t", right_schema().as_ref()),
		)
		.expect("register right_t");
	catalog
		.register_descriptor(
			TableId(3),
			descriptor(3, "third_t", third_schema().as_ref()),
		)
		.expect("register third_t");
	// left_t is the probe under the deterministic SQL right-build; its size does not gate broadcast.
	seed_byte_upper_bound(&mut catalog, "left_t", 64 * 1024 * 1024);
	if let Some(bytes) = right_byte_upper_bound {
		seed_byte_upper_bound(&mut catalog, "right_t", bytes);
		seed_byte_upper_bound(&mut catalog, "third_t", bytes);
	}
	catalog
}

fn float_cross_catalog(right_byte_upper_bound: Option<u64>) -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(11),
			descriptor(11, "left_floats", float_schema().as_ref()),
		)
		.expect("register left_floats");
	catalog
		.register_descriptor(
			TableId(12),
			descriptor(12, "right_floats", float_schema().as_ref()),
		)
		.expect("register right_floats");
	seed_byte_upper_bound(&mut catalog, "left_floats", 64 * 1024 * 1024);
	if let Some(bytes) = right_byte_upper_bound {
		seed_byte_upper_bound(&mut catalog, "right_floats", bytes);
	}
	catalog
}

fn auto_broadcast_config(max_build_bytes: u64) -> PlannerConfig {
	PlannerConfig {
		auto_broadcast_max_build_bytes: Some(ByteCount::new(max_build_bytes)),
		..PlannerConfig::default()
	}
}

fn seed_byte_upper_bound(catalog: &mut MockCatalog, table: &str, bytes: u64) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: Some(ByteCount::new(bytes)),
				..TableOptimizerFacts::default()
			},
		)
		.expect("seed optimizer facts");
}

fn left_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("left_value", DataType::Utf8, true),
	]))
}

fn right_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("right_value", DataType::Int32, false),
	]))
}

fn third_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("third_value", DataType::Boolean, false),
	]))
}

fn float_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("f", DataType::Float64, false),
	]))
}

fn descriptor(id: u8, name: &str, schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}
