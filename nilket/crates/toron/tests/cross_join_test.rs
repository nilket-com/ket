//! ADR 239 — CROSS JOIN substrate. SQL-lowering and binder-level behavior gates.
//!
//! The executor-level cartesian/multiplicity proof lives in `hash_join_kinds_test.rs` (reuses the
//! `Executor` harness) and the distributed value-discriminating gate in `distributed_process_test.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableOptimizerFacts, TableRef};
use toron::distributed::budget::ByteCount;
use toron::error::{BindingError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, BoundExprNode, BoundExprProgram, ExecutionPlan, Expr, FilterDisposition,
	JoinKind, JoinStrategy, JoinStrategyIntent, LogicalPlan, TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
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
async fn adr311_not_in_filter_pushes_comma_equi_through_left_anti_stack() {
	// q16-shaped: the NOT IN desugar wraps the comma join in two LeftAnti joins, then the residual
	// left_t.id = right_t.id sits above that anti stack. With oversized build inputs, a stranded keyless
	// Cross fails loud; binding proves the equality pushed through the LeftAnti output side and keyed the
	// comma join.
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT left_t.id FROM left_t, right_t \
			 WHERE left_t.id = right_t.id \
			   AND left_t.id NOT IN (SELECT id FROM third_t)",
		)
		.expect("plan NOT IN over comma join");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("NOT IN anti stack should not strand the comma equality as a keyless Cross");

	assert_eq!(
		join_kind_count(&bound, JoinKind::Inner),
		1,
		"the underlying comma join is keyed as Inner"
	);
	assert_eq!(
		join_kind_count(&bound, JoinKind::Cross),
		0,
		"the NOT IN stack must not hide a keyless Cross"
	);
	assert_eq!(
		join_kind_count(&bound, JoinKind::LeftAnti),
		2,
		"NOT IN still lowers to null-guard + membership anti-joins"
	);
}

#[tokio::test]
async fn adr319_or_branch_common_equi_keys_comma_join() {
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM left_t, right_t \
			 WHERE (left_t.id = right_t.id AND left_t.left_value = 'small') \
			    OR (right_t.id = left_t.id AND right_t.right_value = 23) \
			    OR (left_t.id = right_t.id AND right_t.right_value = 34)",
		)
		.expect("plan OR-branch comma equi");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("common OR-branch equi key should lift, avoiding the keyless Cross");

	assert_eq!(
		join_kind_count(&bound, JoinKind::Inner),
		1,
		"the common OR-branch equality lifts into one keyed inner join"
	);
	assert_eq!(
		join_kind_count(&bound, JoinKind::Cross),
		0,
		"the OR residual must not leave a keyless Cross behind"
	);
	assert_eq!(
		find_join_strategy(&bound),
		Some(JoinStrategy::HashPartitioned),
		"oversized build hash-partitions only if the OR-hidden key was extracted"
	);
}

#[tokio::test]
async fn adr319_or_branch_missing_key_stays_keyless_cross_and_fails_loud() {
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM left_t, right_t \
			 WHERE (left_t.id = right_t.id AND left_t.left_value = 'small') \
			    OR right_t.right_value = 23",
		)
		.expect("plan partial-key OR");
	let err = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect_err("a key present in only one OR branch must not be lifted");
	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message.contains("broadcast-eligible")),
		"expected the keyless-cross broadcast-eligibility failure, got {err:?}"
	);
}

#[tokio::test]
async fn adr319_or_without_common_key_stays_keyless_cross_and_fails_loud() {
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM left_t, right_t \
			 WHERE (left_t.id = right_t.id AND left_t.left_value = 'small') \
			    OR (left_t.id < right_t.id AND right_t.right_value = 23)",
		)
		.expect("plan OR without common equi key");
	let err = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect_err("an OR without a common bridging equality stays keyless");
	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message.contains("broadcast-eligible")),
		"expected the keyless-cross broadcast-eligibility failure, got {err:?}"
	);
}

#[tokio::test]
async fn adr319_or_branch_key_extraction_matches_explicit_factored_filter() {
	let extracted = run_cross_sql(
		"SELECT left_n.k, right_n.right_value FROM left_n, right_n \
		 WHERE (left_n.k = right_n.k AND left_n.left_value = 'small') \
		    OR (right_n.k = left_n.k AND right_n.right_value = 23) \
		 ORDER BY left_n.k, right_n.right_value",
	)
	.await;
	let explicit = run_cross_sql(
		"SELECT left_n.k, right_n.right_value FROM left_n, right_n \
		 WHERE left_n.k = right_n.k \
		   AND (left_n.left_value = 'small' OR right_n.right_value = 23) \
		 ORDER BY left_n.k, right_n.right_value",
	)
	.await;
	let expected = vec![(1, 23), (1, 34), (2, 23), (3, 99)];
	assert_eq!(int64_int32_rows(&extracted), expected);
	assert_eq!(int64_int32_rows(&explicit), expected);
}

#[tokio::test]
async fn adr440_common_or_side_predicate_routes_to_scan_child() {
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM left_t, right_t \
			 WHERE (left_t.id = right_t.id AND right_t.right_value = 23 AND left_t.left_value = 'small') \
			    OR (right_t.right_value = 23 AND right_t.id = left_t.id AND left_t.left_value = 'large') \
			    OR (left_t.id = right_t.id AND left_t.id = 30 AND right_t.right_value = 23)",
		)
		.expect("plan common side-local OR conjunct");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("bind common side-local OR conjunct");

	let (predicate, disposition) = scan_filter_for_table(&bound, TableId(2))
		.expect("common right-side conjunct should reach the right scan boundary");
	assert!(matches!(
		disposition,
		FilterDisposition::FullyPushed | FilterDisposition::EvaluateInToron
	));
	assert!(
		program_contains_column_named(predicate, "right_value"),
		"the routed scan filter must contain the common right-side predicate"
	);
}

#[tokio::test]
async fn adr440_common_or_predicate_missing_from_one_arm_stays_above_join() {
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM left_t, right_t \
			 WHERE (left_t.id = right_t.id AND right_t.right_value = 23 AND left_t.left_value = 'small') \
			    OR (right_t.id = left_t.id AND right_t.right_value = 23 AND left_t.left_value = 'large') \
			    OR (left_t.id = right_t.id AND right_t.right_value = 34)",
		)
		.expect("plan non-common side-local OR conjunct");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("bind non-common side-local OR conjunct");

	assert!(
		scan_filter_for_table(&bound, TableId(2)).is_none(),
		"a conjunct absent from one arm must not be routed to the scan"
	);
}

#[tokio::test]
async fn adr440_common_or_factoring_preserves_nullable_three_valued_logic() {
	let extracted = run_cross_sql(
		"SELECT left_n.k, right_n.right_value FROM left_n, right_n \
		 WHERE (left_n.k = right_n.k AND left_n.left_value = 'small' AND right_n.right_value = 23) \
		    OR (right_n.right_value = 34 AND right_n.k = left_n.k AND left_n.left_value = 'small') \
		    OR (left_n.left_value = 'small' AND left_n.k = right_n.k AND right_n.right_value = 99) \
		 ORDER BY left_n.k, right_n.right_value",
	)
	.await;
	let explicit = run_cross_sql(
		"SELECT left_n.k, right_n.right_value FROM left_n, right_n \
		 WHERE left_n.k = right_n.k \
		   AND left_n.left_value = 'small' \
		   AND (right_n.right_value = 23 OR right_n.right_value = 34 OR right_n.right_value = 99) \
		 ORDER BY left_n.k, right_n.right_value",
	)
	.await;

	assert_eq!(int64_int32_rows(&extracted), int64_int32_rows(&explicit));
	assert_eq!(
		int64_int32_rows(&extracted),
		vec![(1, 23), (1, 34), (3, 99)]
	);
}

#[tokio::test]
async fn adr440_common_or_duplicate_and_reordered_conjuncts_remove_one_occurrence_per_arm() {
	let extracted = run_cross_sql(
		"SELECT left_n.k, right_n.right_value FROM left_n, right_n \
		 WHERE (left_n.left_value = 'small' AND left_n.k = right_n.k AND left_n.left_value = 'small' AND right_n.right_value = 23) \
		    OR (right_n.right_value = 34 AND left_n.left_value = 'small' AND right_n.k = left_n.k) \
		 ORDER BY left_n.k, right_n.right_value",
	)
	.await;
	let explicit = run_cross_sql(
		"SELECT left_n.k, right_n.right_value FROM left_n, right_n \
		 WHERE left_n.k = right_n.k AND left_n.left_value = 'small' \
		   AND ((left_n.left_value = 'small' AND right_n.right_value = 23) OR right_n.right_value = 34) \
		 ORDER BY left_n.k, right_n.right_value",
	)
	.await;

	assert_eq!(int64_int32_rows(&extracted), int64_int32_rows(&explicit));
	assert_eq!(int64_int32_rows(&extracted), vec![(1, 23), (1, 34)]);
}

#[tokio::test]
async fn adr440_common_or_empty_reduced_arm_removes_the_residual_or() {
	let catalog = nullable_key_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT left_n.k, right_n.right_value FROM left_n, right_n \
			 WHERE (left_n.k = right_n.k AND left_n.left_value = 'small') \
			    OR (right_n.k = left_n.k AND left_n.left_value = 'small' AND right_n.right_value = 23)",
		)
		.expect("plan empty reduced OR arm");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("bind empty reduced OR arm");
	let result = Executor::new(Arc::new(CrossFixtureScan {
		batches: HashMap::from([
			(TableId(21), nullable_left_batch()),
			(TableId(22), nullable_right_batch()),
		]),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute empty reduced OR arm");
	let batch = result.first().expect("result batch");

	assert_eq!(int64_int32_rows(batch), vec![(1, 23), (1, 34), (3, 99)]);
	assert_eq!(
		filter_count(&bound),
		1,
		"the empty reduced arm must leave only the routed child filter"
	);
}

#[tokio::test]
async fn adr440_common_or_unsafe_side_predicate_stays_above_join() {
	let catalog = cross_catalog(Some(64 * 1024 * 1024));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM left_t, right_t \
			 WHERE (left_t.id = right_t.id AND left_t.id + 0 > 0 AND right_t.right_value = 23) \
			    OR (right_t.id = left_t.id AND left_t.id + 0 > 0 AND right_t.right_value = 34)",
		)
		.expect("plan unsafe common side-local OR conjunct");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("bind unsafe common side-local OR conjunct");

	assert!(
		scan_filter_for_table(&bound, TableId(1)).is_none(),
		"an expression rejected by the pushdown-safety guard must stay above the join"
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
		on_filter: None,
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

fn join_kind_count(plan: &ExecutionPlan, expected: JoinKind) -> usize {
	match plan {
		ExecutionPlan::Join {
			left, right, kind, ..
		} => {
			usize::from(*kind == expected)
				+ join_kind_count(left, expected)
				+ join_kind_count(right, expected)
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => join_kind_count(input, expected),
		ExecutionPlan::Union { left, right, .. } => {
			join_kind_count(left, expected) + join_kind_count(right, expected)
		}
		_ => 0,
	}
}

async fn run_cross_sql(sql: &str) -> RecordBatch {
	let catalog = nullable_key_catalog();
	let logical = SqlPlanner::new().plan(sql).expect("plan cross fixture sql");
	let bound = bind(&logical, &catalog, &auto_broadcast_config(1024 * 1024))
		.await
		.expect("bind cross fixture sql");
	let batches = Executor::new(Arc::new(CrossFixtureScan {
		batches: HashMap::from([
			(TableId(21), nullable_left_batch()),
			(TableId(22), nullable_right_batch()),
		]),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute cross fixture sql");
	assert_eq!(batches.len(), 1, "single-partition result");
	batches.into_iter().next().expect("result batch")
}

fn int64_int32_rows(batch: &RecordBatch) -> Vec<(i64, i32)> {
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	let values = batch
		.column(1)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("right_value column");
	let mut rows = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		assert!(!ids.is_null(row), "id row {row} should not be null");
		assert!(
			!values.is_null(row),
			"right_value row {row} should not be null"
		);
		rows.push((ids.value(row), values.value(row)));
	}
	rows
}

fn scan_filter_for_table(
	plan: &ExecutionPlan,
	table_id: TableId,
) -> Option<(&BoundExprProgram, FilterDisposition)> {
	match plan {
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} if matches!(input.as_ref(), ExecutionPlan::Scan { local_table_id, .. } if *local_table_id == table_id) => {
			Some((predicate, *disposition))
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => scan_filter_for_table(input, table_id),
		ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
			scan_filter_for_table(left, table_id).or_else(|| scan_filter_for_table(right, table_id))
		}
		_ => None,
	}
}

fn program_contains_column_named(program: &BoundExprProgram, name: &str) -> bool {
	program
		.nodes
		.iter()
		.any(|node| matches!(node, BoundExprNode::Column(column) if column.name.0 == name))
}

fn filter_count(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Filter { input, .. } => 1 + filter_count(input),
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => filter_count(input),
		ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
			filter_count(left) + filter_count(right)
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

fn nullable_key_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(21),
			descriptor(21, "left_n", nullable_left_schema().as_ref()),
		)
		.expect("register left_n");
	catalog
		.register_descriptor(
			TableId(22),
			descriptor(22, "right_n", nullable_right_schema().as_ref()),
		)
		.expect("register right_n");
	catalog
}

fn nullable_left_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("k", DataType::Int64, true),
		Field::new("left_value", DataType::Utf8, true),
	]))
}

fn nullable_right_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("k", DataType::Int64, true),
		Field::new("right_value", DataType::Int32, false),
	]))
}

fn nullable_left_batch() -> RecordBatch {
	RecordBatch::try_new(
		nullable_left_schema(),
		vec![
			Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
			Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(3)])),
			Arc::new(StringArray::from(vec![
				Some("small"),
				Some("large"),
				Some("small"),
				Some("small"),
			])),
		],
	)
	.expect("left batch")
}

fn nullable_right_batch() -> RecordBatch {
	RecordBatch::try_new(
		nullable_right_schema(),
		vec![
			Arc::new(Int64Array::from(vec![101, 102, 103, 104, 105])),
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(1),
				Some(2),
				None,
				Some(3),
			])),
			Arc::new(Int32Array::from(vec![
				Some(23),
				Some(34),
				Some(23),
				Some(23),
				Some(99),
			])),
		],
	)
	.expect("right batch")
}

struct CrossFixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for CrossFixtureScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;
		if let Some(pbc) = request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(projected_columns) = &request.projected_columns {
			let schema = batch.schema();
			let fields = projected_columns
				.iter()
				.map(|ordinal| schema.field(ordinal.0 as usize).clone())
				.collect::<Vec<_>>();
			let columns = projected_columns
				.iter()
				.map(|ordinal| batch.column(ordinal.0 as usize).clone())
				.collect::<Vec<_>>();
			batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
				.map_err(StorageError::from)?;
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
}
