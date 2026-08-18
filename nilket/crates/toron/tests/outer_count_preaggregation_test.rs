//! ADR 521 — proof-gated outer COUNT preaggregation.
//!
//! The permanent battery runs both knob arms over the same proof-bearing
//! catalog and pins the exact Arrow field contract as well as the result
//! digest. The fixture includes duplicate right keys, unmatched preserved
//! keys, a right-only key, and a NULL right key.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{DistinctCountFact, MockCatalog, TableOptimizerFacts, UniqueKeyProof};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, ColumnOrdinal, ExecutionPlan, Expr, JoinKind, JoinStrategyIntent,
	LogicalPlan, TableId,
};
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordLayoutPolicy,
	RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

#[tokio::test]
async fn proof_gated_rewrite_preserves_rows_digest_and_field_contract() {
	let (mut catalog, storage) = fixture(false);
	catalog
		.seed_unique_key_evidence(
			&toron::catalog::TableRef::new("oc_customer"),
			vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(0)])
					.expect("single-column proof"),
			],
		)
		.expect("seed proof");
	let logical = outer_count_plan();
	let mut off = PlannerConfig::default();
	off.outer_count_preaggregation = false;
	let mut on = off.clone();
	on.outer_count_preaggregation = true;
	let off_plan = bind(&logical, &catalog, &off).await.expect("off bind");
	let on_plan = bind(&logical, &catalog, &on).await.expect("on bind");

	assert!(matches!(off_plan, ExecutionPlan::Aggregate { .. }));
	assert!(matches!(on_plan, ExecutionPlan::Project { .. }));
	assert_eq!(off_plan.schema(), on_plan.schema());
	assert_eq!(off_plan.schema().field(0), on_plan.schema().field(0));
	assert_eq!(off_plan.schema().field(1), on_plan.schema().field(1));
	assert_eq!(off_plan.schema().field(1).name(), "count");
	assert!(!off_plan.schema().field(1).is_nullable());

	let off_rows = Executor::new(Arc::clone(&storage))
		.execute_bound(&off_plan, None)
		.await
		.expect("off execute");
	let on_rows = Executor::new(storage)
		.execute_bound(&on_plan, None)
		.await
		.expect("on execute");
	assert_eq!(
		canonical_arrow_ipc_digest(&off_rows, SortPolicy::FullRowLex).expect("off digest"),
		canonical_arrow_ipc_digest(&on_rows, SortPolicy::FullRowLex).expect("on digest")
	);
	assert_eq!(
		collect_int64_rows(&on_rows),
		vec![(1, 2), (2, 1), (3, 0), (4, 0)]
	);
}

#[tokio::test]
async fn all_filtered_right_side_coalesces_every_preserved_key_to_zero() {
	let (mut catalog, storage) = fixture(false);
	catalog
		.seed_unique_key_evidence(
			&toron::catalog::TableRef::new("oc_customer"),
			vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(0)])
					.expect("single-column proof"),
			],
		)
		.expect("seed proof");
	let mut logical = outer_count_plan();
	let LogicalPlan::Aggregate { input, .. } = &mut logical else {
		unreachable!()
	};
	let LogicalPlan::Exchange { input, .. } = input.as_mut() else {
		unreachable!()
	};
	let LogicalPlan::Join { right, .. } = input.as_mut() else {
		unreachable!()
	};
	*right = Box::new(LogicalPlan::Filter {
		input: right.clone(),
		predicate: Expr::Gt(
			Box::new(Expr::column("o_id")),
			Box::new(Expr::Literal(toron::ir::ScalarValue::Int64(Some(1_000)))),
		),
	});
	let mut off = PlannerConfig::default();
	off.outer_count_preaggregation = false;
	let mut on = off.clone();
	on.outer_count_preaggregation = true;
	let off_plan = bind(&logical, &catalog, &off).await.expect("off bind");
	let on_plan = bind(&logical, &catalog, &on).await.expect("on bind");
	assert_eq!(off_plan.schema(), on_plan.schema());
	let off_rows = Executor::new(Arc::clone(&storage))
		.execute_bound(&off_plan, None)
		.await
		.expect("off execute");
	let on_rows = Executor::new(storage)
		.execute_bound(&on_plan, None)
		.await
		.expect("on execute");
	assert_eq!(
		canonical_arrow_ipc_digest(&off_rows, SortPolicy::FullRowLex).expect("off digest"),
		canonical_arrow_ipc_digest(&on_rows, SortPolicy::FullRowLex).expect("on digest")
	);
	assert_eq!(
		collect_int64_rows(&on_rows),
		vec![(1, 0), (2, 0), (3, 0), (4, 0)]
	);
}

#[tokio::test]
async fn field_contract_survives_parent_projection_pruning() {
	let (mut catalog, _) = fixture(false);
	catalog
		.seed_unique_key_evidence(
			&toron::catalog::TableRef::new("oc_customer"),
			vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(0)])
					.expect("single-column proof"),
			],
		)
		.expect("seed proof");
	let logical = LogicalPlan::Project {
		input: Box::new(outer_count_plan()),
		exprs: vec![Expr::column("count")],
		output_names: vec![Some("c_count".to_string())],
	};
	let mut off = PlannerConfig::default();
	off.outer_count_preaggregation = false;
	let mut on = off.clone();
	on.outer_count_preaggregation = true;
	let off_plan = bind(&logical, &catalog, &off).await.expect("off bind");
	let on_plan = bind(&logical, &catalog, &on).await.expect("on bind");
	assert_eq!(off_plan.schema(), on_plan.schema());
	assert_eq!(on_plan.schema().field(0).name(), "c_count");
	assert!(!on_plan.schema().field(0).is_nullable());
}

#[tokio::test]
async fn primary_key_identity_without_proof_declines() {
	let (catalog, _) = fixture(false);
	let mut config = PlannerConfig::default();
	config.outer_count_preaggregation = true;
	let plan = bind(&outer_count_plan(), &catalog, &config)
		.await
		.expect("bind");
	assert!(
		matches!(plan, ExecutionPlan::Aggregate { .. }),
		"identity PK is not semantic uniqueness evidence"
	);
}

#[tokio::test]
async fn nullable_counted_column_declines() {
	let (mut catalog, storage) = fixture(true);
	catalog
		.seed_unique_key_evidence(
			&toron::catalog::TableRef::new("oc_customer"),
			vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(0)])
					.expect("single-column proof"),
			],
		)
		.expect("seed proof");
	let mut off = PlannerConfig::default();
	off.outer_count_preaggregation = false;
	let mut on = off.clone();
	on.outer_count_preaggregation = true;
	let logical = outer_count_plan();
	let off_plan = bind(&logical, &catalog, &off).await.expect("off bind");
	let on_plan = bind(&logical, &catalog, &on).await.expect("bind");
	assert!(matches!(on_plan, ExecutionPlan::Aggregate { .. }));
	assert_eq!(format!("{off_plan:?}"), format!("{on_plan:?}"));
	let off_rows = Executor::new(Arc::clone(&storage))
		.execute_bound(&off_plan, None)
		.await
		.expect("off execute");
	let on_rows = Executor::new(storage)
		.execute_bound(&on_plan, None)
		.await
		.expect("on execute");
	assert_eq!(
		canonical_arrow_ipc_digest(&off_rows, SortPolicy::FullRowLex).expect("off digest"),
		canonical_arrow_ipc_digest(&on_rows, SortPolicy::FullRowLex).expect("on digest")
	);
}

#[tokio::test]
async fn estimated_ndv_and_misordered_composite_proof_decline() {
	let (mut ndv_catalog, _) = fixture(false);
	let mut facts = TableOptimizerFacts::default();
	facts
		.column_ndv
		.insert(ColumnOrdinal(0), DistinctCountFact::Exact(4));
	ndv_catalog
		.seed_optimizer_facts(&toron::catalog::TableRef::new("oc_customer"), facts)
		.expect("seed NDV");
	assert_declines(&ndv_catalog).await;

	let (mut ordered_catalog, _) = fixture(false);
	ordered_catalog
		.seed_unique_key_evidence(
			&toron::catalog::TableRef::new("oc_customer"),
			vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(1), ColumnOrdinal(0)])
					.expect("ordered composite proof"),
			],
		)
		.expect("seed composite proof");
	assert_declines(&ordered_catalog).await;
}

#[tokio::test]
async fn non_left_outer_null_safe_extra_key_and_wrong_aggregate_decline() {
	let (mut catalog, _) = fixture(false);
	catalog
		.seed_unique_key_evidence(
			&toron::catalog::TableRef::new("oc_customer"),
			vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(0)])
					.expect("single-column proof"),
			],
		)
		.expect("seed proof");

	let mut inner = outer_count_plan();
	*join_mut(&mut inner).0 = JoinKind::Inner;
	assert_declines_plan(&catalog, &inner).await;

	let mut null_safe = outer_count_plan();
	*join_mut(&mut null_safe).1 = true;
	assert_declines_plan(&catalog, &null_safe).await;

	let mut extra_key = outer_count_plan();
	join_mut(&mut extra_key)
		.2
		.push((Expr::column("c_payload"), Expr::column("o_id")));
	assert_declines_plan(&catalog, &extra_key).await;

	let mut count_all = outer_count_plan();
	let LogicalPlan::Aggregate { aggr_expr, .. } = &mut count_all else {
		unreachable!()
	};
	*aggr_expr = vec![(AggregateFunction::CountAll, Vec::new())];
	assert_declines_plan(&catalog, &count_all).await;

	let mut left_count = outer_count_plan();
	let LogicalPlan::Aggregate { aggr_expr, .. } = &mut left_count else {
		unreachable!()
	};
	*aggr_expr = vec![(AggregateFunction::Count, vec![Expr::column("c_payload")])];
	assert_declines_plan(&catalog, &left_count).await;
}

async fn assert_declines(catalog: &MockCatalog) {
	assert_declines_plan(catalog, &outer_count_plan()).await;
}

async fn assert_declines_plan(catalog: &MockCatalog, logical: &LogicalPlan) {
	let mut config = PlannerConfig::default();
	config.outer_count_preaggregation = true;
	let plan = bind(logical, catalog, &config).await.expect("bind");
	assert!(matches!(plan, ExecutionPlan::Aggregate { .. }));
}

fn join_mut(plan: &mut LogicalPlan) -> (&mut JoinKind, &mut bool, &mut Vec<(Expr, Expr)>) {
	let LogicalPlan::Aggregate { input, .. } = plan else {
		unreachable!()
	};
	let LogicalPlan::Exchange { input, .. } = input.as_mut() else {
		unreachable!()
	};
	let LogicalPlan::Join {
		kind,
		null_safe_keys,
		on,
		..
	} = input.as_mut()
	else {
		unreachable!()
	};
	(kind, null_safe_keys, on)
}

fn outer_count_plan() -> LogicalPlan {
	let customer = LogicalPlan::ScanNamed {
		table: toron::catalog::TableRef::new("oc_customer"),
		projection: None,
	};
	let orders = LogicalPlan::ScanNamed {
		table: toron::catalog::TableRef::new("oc_orders"),
		projection: None,
	};
	let joined = LogicalPlan::Join {
		left: Box::new(customer),
		right: Box::new(orders),
		kind: JoinKind::LeftOuter,
		on: vec![(Expr::column("c_id"), Expr::column("o_cid"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(joined),
			partition_count: 4,
			key: vec![Expr::column("c_id")],
		}),
		group_expr: vec![Expr::column("c_id")],
		aggr_expr: vec![(AggregateFunction::Count, vec![Expr::column("o_id")])],
	}
}

fn collect_int64_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
	let mut rows = Vec::new();
	for batch in batches {
		let keys = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("key");
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count");
		for row in 0..batch.num_rows() {
			rows.push((keys.value(row), counts.value(row)));
		}
	}
	rows.sort_unstable();
	rows
}

fn fixture(nullable_count: bool) -> (MockCatalog, Arc<dyn StorageScan>) {
	let customer_schema = Arc::new(Schema::new(vec![
		Field::new("c_id", DataType::Int64, false),
		Field::new("c_payload", DataType::Int64, false),
	]));
	let order_schema = Arc::new(Schema::new(vec![
		Field::new("o_id", DataType::Int64, nullable_count),
		Field::new("o_cid", DataType::Int64, true),
	]));
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(
				1,
				"oc_customer",
				customer_schema.as_ref(),
				RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("c_id")),
			),
		)
		.expect("register customer");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(
				2,
				"oc_orders",
				order_schema.as_ref(),
				RowIdentityPolicy::AppendOnly,
			),
		)
		.expect("register orders");
	let customers = RecordBatch::try_new(
		Arc::clone(&customer_schema),
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
			Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
		],
	)
	.expect("customers");
	let order_ids: Arc<dyn Array> = if nullable_count {
		Arc::new(Int64Array::from(vec![
			Some(10),
			Some(11),
			Some(12),
			None,
			Some(14),
		]))
	} else {
		Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14]))
	};
	let orders = RecordBatch::try_new(
		Arc::clone(&order_schema),
		vec![
			order_ids,
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(1),
				Some(2),
				None,
				Some(99),
			])),
		],
	)
	.expect("orders");
	(
		catalog,
		Arc::new(FixtureScan {
			batches: HashMap::from([(TableId(1), customers), (TableId(2), orders)]),
		}) as Arc<dyn StorageScan>,
	)
}

fn descriptor(
	id: u8,
	name: &str,
	schema: &Schema,
	identity: RowIdentityPolicy,
) -> TableSchemaDescriptor {
	let layout = match identity {
		RowIdentityPolicy::AppendOnly | RowIdentityPolicy::AppendOnlyContentHash => {
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default())
		}
		RowIdentityPolicy::PrimaryKey(_) => RecordLayoutPolicy::SingleRow,
	};
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema,
		identity,
		LocalityPolicy::default_table_spread(),
		layout,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

struct FixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for FixtureScan {
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
		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
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

/// ADR 553: `deterministic` has gated this rewrite since ADR 521, but until
/// `RANDOM()` was registered every expression in the engine reported itself
/// pure, so the declining branch had never run. The pair is the evidence: the
/// same plan shape rewrites with a pure right-side projection and declines with
/// a non-deterministic one. A decline alone would not distinguish this guard
/// from any other reason the rewrite might refuse.
#[tokio::test]
async fn adr553_non_deterministic_right_chain_declines_while_the_pure_shape_rewrites() {
	let (mut catalog, _) = fixture(false);
	catalog
		.seed_unique_key_evidence(
			&toron::catalog::TableRef::new("oc_customer"),
			vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(0)])
					.expect("single-column proof"),
			],
		)
		.expect("seed proof");

	let mut config = PlannerConfig::default();
	config.outer_count_preaggregation = true;

	let pure_predicate = Expr::Gt(
		Box::new(Expr::column("o_id")),
		Box::new(Expr::Literal(toron::ir::ScalarValue::Int64(Some(0)))),
	);
	let pure = bind(&right_filtered_plan(pure_predicate), &catalog, &config)
		.await
		.expect("bind pure");
	assert!(
		!matches!(pure, ExecutionPlan::Aggregate { .. }),
		"a pure right-side filter must still allow the rewrite, or the \
		 non-deterministic case below proves nothing"
	);

	let impure_predicate = Expr::Lt(
		Box::new(Expr::ScalarFunction {
			id: toron::ir::ScalarFunctionId::Random,
			args: Vec::new(),
			parameter: toron::ir::ScalarFunctionParameter::None,
		}),
		Box::new(Expr::Literal(toron::ir::ScalarValue::Float64(Some(0.5)))),
	);
	let impure = bind(&right_filtered_plan(impure_predicate), &catalog, &config)
		.await
		.expect("bind impure");
	assert!(
		matches!(impure, ExecutionPlan::Aggregate { .. }),
		"a non-deterministic right-side filter must decline the rewrite"
	);
}

/// The `outer_count_plan` shape with a filter on the right branch, so one
/// predicate can be swapped between the two arms of the pair. A filter leaves
/// the right schema intact, which an extra projection would not.
fn right_filtered_plan(predicate: Expr) -> LogicalPlan {
	let customer = LogicalPlan::ScanNamed {
		table: toron::catalog::TableRef::new("oc_customer"),
		projection: None,
	};
	let orders = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: toron::catalog::TableRef::new("oc_orders"),
			projection: None,
		}),
		predicate,
	};
	let joined = LogicalPlan::Join {
		left: Box::new(customer),
		right: Box::new(orders),
		kind: JoinKind::LeftOuter,
		on: vec![(Expr::column("c_id"), Expr::column("o_cid"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(joined),
			partition_count: 4,
			key: vec![Expr::column("c_id")],
		}),
		group_expr: vec![Expr::column("c_id")],
		aggr_expr: vec![(AggregateFunction::Count, vec![Expr::column("o_id")])],
	}
}
