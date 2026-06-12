use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::PlannerConfig;
use toron::catalog::{DistinctCountFact, MockCatalog, TableOptimizerFacts, TableRef};
use toron::coordinator::QueryCoordinator;
use toron::distributed::budget::ByteCount;
use toron::distributed::identity::QueryId;
use toron::distributed::task::RowCount;
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{ColumnOrdinal, LogicalPlan, QueryProfileHint, SessionId, TableId};
use toron::sql_planner::SqlPlanner;
use toron::storage::{HlcBound, InMemoryTableStorage, StorageWrite, TenantId, WriteRequest};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

const STAR_SQL: &str = "SELECT o.id, c.id, p.id, s.id \
	FROM orders o \
	INNER JOIN customers c ON o.customer_id = c.id \
	INNER JOIN products p ON o.product_id = p.id \
	INNER JOIN stores s ON o.store_id = s.id";

// ADR 193: after the mixed-strategy root actuates, a recognized snowflake stays on the observe-only
// path only when its shape is outside the root actuator's authorized domain. A non-scan-linear probe
// (the fact table arrives via a `LIMIT` subplan, wider than the row count so rows/order are preserved)
// declines actuation with `ProbeNotScanLinear` while `analyze_snowflake` still recognizes the full
// tree — the post-193 evidence path these observation/verdict tests exercise.
const SNOWFLAKE_SQL: &str = "SELECT o.id, c.id, r.id, g.id, s.id \
	FROM (SELECT * FROM orders LIMIT 1000000) o \
	INNER JOIN customers c ON o.customer_id = c.id \
	INNER JOIN regions r ON c.region_id = r.id \
	INNER JOIN segments g ON c.segment_id = g.id \
	INNER JOIN stores s ON o.store_id = s.id";

#[derive(Debug, Clone, Copy)]
struct StarFactRows {
	customers: u64,
	products: u64,
	stores: u64,
}

const DEFAULT_STAR_FACTS: StarFactRows = StarFactRows {
	customers: 5_000,
	products: 1_000,
	stores: 100,
};

const EQUAL_STAR_FACTS: StarFactRows = StarFactRows {
	customers: 1_000,
	products: 1_000,
	stores: 1_000,
};

const PRODUCT_FIRST_STAR_FACTS: StarFactRows = StarFactRows {
	customers: 5_000,
	products: 100,
	stores: 1_000,
};

#[derive(Debug, Clone, Copy)]
enum StarDataShape {
	AllMatch,
	DeclaredThenProductSelective,
	ProductFirstThenStoreThenCustomerSelective,
}

#[tokio::test]
async fn explain_analyze_runs_and_captures_star_join_actuals() {
	let batch = analyze_star_batch().await;

	assert_eq!(batch.schema().field(0).name(), "decision_kind");
	assert_eq!(batch.schema().field(4).name(), "estimated_rows");
	assert_eq!(batch.schema().field(5).name(), "actual_rows");
	assert_eq!(batch.schema().field(6).name(), "ratio");

	let kinds = string_values(&batch, 0);
	let runtime_rows = row_indices_for_kind(&batch, "join_order_runtime");
	let targets = string_values_for_rows(&batch, 1, &runtime_rows);
	let details = string_values_for_rows(&batch, 3, &runtime_rows);
	let estimated = int64_values_for_rows(&batch, 4, &runtime_rows);
	let actual = int64_values_for_rows(&batch, 5, &runtime_rows);

	assert_eq!(runtime_rows.len(), 3);
	assert_eq!(row_indices_for_kind(&batch, "join_order_verdict").len(), 1);
	assert!(kinds.iter().any(|kind| kind == "join_order_verdict"));
	assert!(targets.iter().any(|target| target.contains("dimension=c")));
	assert!(targets.iter().any(|target| target.contains("dimension=p")));
	assert!(targets.iter().any(|target| target.contains("dimension=s")));
	assert!(actual.iter().all(|rows| *rows == 4));
	assert!(
		estimated
			.iter()
			.zip(actual.iter())
			.all(|(estimate, actual)| estimate != actual),
		"fixture should expose a measured estimate miss: estimated={estimated:?} actual={actual:?}",
	);
	assert!(details.iter().all(|detail| detail.contains("source=")));
}

#[tokio::test]
async fn snowflake_runtime_evidence_reports_measured_edge_cardinalities_by_level() {
	let batch = analyze_snowflake_batch().await;
	let runtime_rows = row_indices_for_kind(&batch, "join_order_snowflake_runtime");
	let targets = string_values_for_rows(&batch, 1, &runtime_rows);
	let actual = int64_values_for_rows(&batch, 5, &runtime_rows);
	let build_rows = int64_values_for_rows(&batch, 8, &runtime_rows);
	let probe_rows = int64_values_for_rows(&batch, 9, &runtime_rows);

	assert_eq!(
		row_indices_for_kind(&batch, "join_order_snowflake").len(),
		1
	);
	assert_eq!(runtime_rows.len(), 4);
	assert!(
		targets
			.iter()
			.any(|target| target.contains("level=o dimension=c"))
	);
	assert!(
		targets
			.iter()
			.any(|target| target.contains("level=o dimension=s"))
	);
	assert!(
		targets
			.iter()
			.any(|target| target.contains("level=c dimension=r"))
	);
	assert!(
		targets
			.iter()
			.any(|target| target.contains("level=c dimension=g"))
	);
	assert_eq!(actual, vec![5, 4, 3, 2]);
	assert!(build_rows.iter().all(|rows| *rows > 0));
	assert!(probe_rows.iter().all(|rows| *rows > 0));
}

#[tokio::test]
async fn snowflake_runtime_evidence_covers_root_and_branch() {
	let batch = analyze_snowflake_batch().await;
	let runtime_rows = row_indices_for_kind(&batch, "join_order_snowflake_runtime");
	let targets = string_values_for_rows(&batch, 1, &runtime_rows);

	assert!(
		targets
			.iter()
			.any(|target| target.contains("level=o dimension=c")),
		"root-level snowflake edge should be measured: {targets:?}"
	);
	assert!(
		targets
			.iter()
			.any(|target| target.contains("level=c dimension=r")),
		"branch-level snowflake edge should be measured: {targets:?}"
	);
}

#[tokio::test]
async fn snowflake_runtime_rows_correlate_by_replay_id() {
	let batch = analyze_snowflake_batch().await;
	let runtime_rows = row_indices_for_kind(&batch, "join_order_snowflake_runtime");
	let targets = string_values_for_rows(&batch, 1, &runtime_rows);
	let actual = int64_values_for_rows(&batch, 5, &runtime_rows);
	let observed = targets
		.iter()
		.zip(actual.iter().copied())
		.collect::<BTreeMap<_, _>>();

	assert_eq!(
		observed.get(&"join[0] level=o dimension=c".to_string()),
		Some(&5)
	);
	assert_eq!(
		observed.get(&"join[1] level=c dimension=r".to_string()),
		Some(&4)
	);
	assert_eq!(
		observed.get(&"join[2] level=c dimension=g".to_string()),
		Some(&3)
	);
	assert_eq!(
		observed.get(&"join[3] level=o dimension=s".to_string()),
		Some(&2)
	);
}

#[tokio::test]
async fn snowflake_explain_analyze_emits_plan_runtime_and_verdict() {
	let batch = analyze_snowflake_batch().await;

	assert_eq!(
		row_indices_for_kind(&batch, "join_order_snowflake").len(),
		1
	);
	assert_eq!(
		row_indices_for_kind(&batch, "join_order_snowflake_runtime").len(),
		4
	);
	assert_eq!(
		row_indices_for_kind(&batch, "join_order_snowflake_verdict").len(),
		3
	);
	assert!(row_indices_for_kind(&batch, "join_order_verdict").is_empty());
	assert!(row_indices_for_kind(&batch, "join_order_runtime").is_empty());
}

#[tokio::test]
async fn snowflake_verdict_reports_per_level_and_aggregate() {
	let batch = analyze_snowflake_batch().await;
	let root = row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "level=o");
	let branch = row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "level=c");
	let aggregate =
		row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "snowflake");

	assert_eq!(string_value_at(&batch, 2, root), "missed_win");
	assert_eq!(string_value_at(&batch, 2, branch), "vindicated");
	assert_eq!(string_value_at(&batch, 2, aggregate), "missed_win");
	let aggregate_detail = string_value_at(&batch, 3, aggregate);
	assert!(
		aggregate_detail.contains("levels=[o:missed_win,c:vindicated]"),
		"{aggregate_detail}"
	);
}

#[tokio::test]
async fn snowflake_level_estimate_wanted_reorder_judges_against_declared() {
	let batch = analyze_snowflake_batch().await;
	let root = row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "level=o");
	let branch = row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "level=c");
	let root_detail = string_value_at(&batch, 3, root);
	let branch_detail = string_value_at(&batch, 3, branch);

	assert!(
		root_detail.contains("estimated_action=reordered"),
		"{root_detail}"
	);
	assert!(
		root_detail.contains("measured_action=reordered"),
		"{root_detail}"
	);
	assert_eq!(string_value_at(&batch, 2, root), "missed_win");
	assert!(
		branch_detail.contains("estimated_action=reordered"),
		"{branch_detail}"
	);
	assert!(
		branch_detail.contains("measured_action=kept_insufficient_margin"),
		"{branch_detail}"
	);
	assert_eq!(string_value_at(&batch, 2, branch), "vindicated");
	for row in row_indices_for_kind(&batch, "join_order_snowflake_verdict") {
		let outcome = string_value_at(&batch, 2, row);
		assert_ne!(outcome, "mis_ranked");
		assert_ne!(outcome, "over_eager");
	}
}

#[tokio::test]
async fn snowflake_verdict_detail_keeps_estimate_and_measurement_separate() {
	let batch = analyze_snowflake_batch().await;
	let root = row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "level=o");
	let detail = string_value_at(&batch, 3, root);

	assert!(detail.contains("estimated_action=reordered"), "{detail}");
	assert!(detail.contains("measured_action=reordered"), "{detail}");
	assert!(detail.contains("declared_order=[c,s]"), "{detail}");
	assert!(
		detail.contains("estimated_preferred_order=[s,c]"),
		"{detail}"
	);
	assert!(detail.contains("measured_applied_order=[s,c]"), "{detail}");
	assert!(detail.contains("strategies=["), "{detail}");
	// ADR 192: the observed (non-actuating) snowflake is genuinely mixed under the test cap — the
	// root edges resolve to hash, which is what keeps it observed rather than broadcast-actuated.
	assert!(detail.contains("c:hash_partitioned/natural"), "{detail}");
	assert!(detail.contains("s:hash_partitioned/natural"), "{detail}");
}

#[tokio::test]
async fn snowflake_verdict_missed_win_recommends_would_reorder() {
	let batch = analyze_snowflake_batch().await;
	let root = row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "level=o");
	let aggregate =
		row_index_for_kind_and_target(&batch, "join_order_snowflake_verdict", "snowflake");

	assert_eq!(string_value_at(&batch, 2, root), "missed_win");
	assert!(
		string_value_at(&batch, 3, root).contains("recommendation=WOULD_REORDER"),
		"{}",
		string_value_at(&batch, 3, root)
	);
	assert_eq!(string_value_at(&batch, 2, aggregate), "missed_win");
	assert!(
		string_value_at(&batch, 3, aggregate).contains("recommendation=WOULD_REORDER"),
		"{}",
		string_value_at(&batch, 3, aggregate)
	);
}

#[tokio::test]
async fn snowflake_runtime_evidence_is_observational() {
	let (catalog, storage) = snowflake_fixture().await;
	let coordinator = coordinator_with_config(
		catalog,
		storage,
		PlannerConfig {
			// ADR 192: keep the snowflake mixed/observed (see analyze_snowflake_batch).
			auto_broadcast_max_build_bytes: Some(ByteCount::new(65_536)),
			..PlannerConfig::default()
		},
	);
	let planner = SqlPlanner::new();
	let query = planner.plan(SNOWFLAKE_SQL).expect("plan snowflake query");
	let analyze = planner
		.plan(&format!("EXPLAIN ANALYZE {SNOWFLAKE_SQL}"))
		.expect("plan snowflake analyze");

	let before = execute_single_batch(&coordinator, &query).await;
	let analysis = execute_single_batch(&coordinator, &analyze).await;
	let after = execute_single_batch(&coordinator, &query).await;

	assert_eq!(
		row_indices_for_kind(&analysis, "join_order_snowflake_runtime").len(),
		4
	);
	assert_eq!(
		row_indices_for_kind(&analysis, "join_order_snowflake_verdict").len(),
		3
	);
	assert_eq!(
		format!("{:?}", before.schema()),
		format!("{:?}", after.schema())
	);
	assert_eq!(int64_values(&before, 0), int64_values(&after, 0));
	assert_eq!(int64_values(&before, 0), vec![1, 2]);
}

#[tokio::test]
async fn star_runtime_evidence_unchanged() {
	let batch = analyze_star_batch().await;

	assert_eq!(row_indices_for_kind(&batch, "join_order_runtime").len(), 3);
	assert_eq!(row_indices_for_kind(&batch, "join_order_verdict").len(), 1);
	assert!(row_indices_for_kind(&batch, "join_order_snowflake_runtime").is_empty());
	assert!(row_indices_for_kind(&batch, "join_order_snowflake_verdict").is_empty());
}

#[tokio::test]
async fn measured_vs_estimated_record_carries_both_sides() {
	let batch = analyze_star_batch().await;
	let runtime_rows = row_indices_for_kind(&batch, "join_order_runtime");
	let estimated = int64_values_for_rows(&batch, 4, &runtime_rows);
	let actual = int64_values_for_rows(&batch, 5, &runtime_rows);
	let ratio = float64_values_for_rows(&batch, 6, &runtime_rows);
	let sources = string_values_for_rows(&batch, 7, &runtime_rows);
	let build_rows = int64_values_for_rows(&batch, 8, &runtime_rows);
	let probe_rows = int64_values_for_rows(&batch, 9, &runtime_rows);

	assert_eq!(runtime_rows.len(), 3);
	for idx in 0..runtime_rows.len() {
		assert!(estimated[idx] > 0);
		assert!(actual[idx] > 0);
		assert!(build_rows[idx] > 0);
		assert!(probe_rows[idx] > 0);
		assert!(!sources[idx].is_empty());
		assert!(
			(ratio[idx] - (actual[idx] as f64 / estimated[idx] as f64)).abs() < f64::EPSILON,
			"ratio should be actual/estimated at runtime row {idx}"
		);
	}
}

#[tokio::test]
async fn known_miss_star_runtime_evidence_shows_divergence() {
	let batch = analyze_star_batch().await;
	let runtime_rows = row_indices_for_kind(&batch, "join_order_runtime");
	let estimated = int64_values_for_rows(&batch, 4, &runtime_rows);
	let actual = int64_values_for_rows(&batch, 5, &runtime_rows);

	assert!(
		estimated
			.iter()
			.zip(actual.iter())
			.all(|(estimate, actual)| estimate != actual),
		"fixture must remain a deterministic estimate miss: estimated={estimated:?} actual={actual:?}"
	);
}

#[tokio::test]
async fn measured_record_correlates_by_replay_id() {
	let batch = analyze_star_batch().await;
	let runtime_rows = row_indices_for_kind(&batch, "join_order_runtime");
	let targets = string_values_for_rows(&batch, 1, &runtime_rows);
	let dimensions = targets
		.iter()
		.map(|target| {
			target
				.split("dimension=")
				.nth(1)
				.expect("target carries dimension")
				.to_string()
		})
		.collect::<BTreeSet<_>>();
	let replay_ids = targets
		.iter()
		.map(|target| {
			assert!(target.starts_with("join["));
			let close = target.find(']').expect("target carries replay id");
			target[5..close].parse::<u32>().expect("replay id is u32")
		})
		.collect::<BTreeSet<_>>();

	assert_eq!(runtime_rows.len(), 3);
	assert_eq!(
		dimensions,
		BTreeSet::from(["c".to_string(), "p".to_string(), "s".to_string()])
	);
	assert_eq!(replay_ids.len(), 3);
}

#[tokio::test]
async fn cockpit_renders_estimated_vs_actual() {
	let batch = analyze_star_batch().await;
	let schema = batch.schema();
	let fields = schema.fields();
	let field_names = fields.iter().map(|field| field.name()).collect::<Vec<_>>();
	let runtime_rows = row_indices_for_kind(&batch, "join_order_runtime");
	let details = string_values_for_rows(&batch, 3, &runtime_rows);

	assert_eq!(
		field_names,
		vec![
			"decision_kind",
			"target",
			"outcome",
			"detail",
			"estimated_rows",
			"actual_rows",
			"ratio",
			"estimate_source",
			"build_rows",
			"probe_rows",
		]
	);
	assert!(details.iter().all(|detail| {
		detail.contains("estimated=")
			&& detail.contains("actual=")
			&& detail.contains("ratio=")
			&& detail.contains("source=")
			&& detail.contains("build_rows=")
			&& detail.contains("probe_rows=")
	}));
}

#[tokio::test]
async fn verdict_vindicated_when_measured_178_would_apply_same_order() {
	let batch = analyze_star_batch_with(EQUAL_STAR_FACTS, StarDataShape::AllMatch).await;
	let row = join_order_verdict_row(&batch);

	assert_eq!(string_value_at(&batch, 2, row), "vindicated");
	let detail = string_value_at(&batch, 3, row);
	assert!(detail.contains("recommendation=KEEP_APPLIED"), "{detail}");
	assert!(
		detail.contains("measured_action=kept_already_optimal"),
		"{detail}"
	);
	assert!(
		detail.contains("applied_order=[c,p,s]; measured_applied_order=[c,p,s]"),
		"{detail}"
	);
}

#[tokio::test]
async fn verdict_misranked_when_reorder_picks_wrong() {
	let batch = analyze_star_batch_with(
		PRODUCT_FIRST_STAR_FACTS,
		StarDataShape::ProductFirstThenStoreThenCustomerSelective,
	)
	.await;
	let row = join_order_verdict_row(&batch);

	assert_eq!(string_value_at(&batch, 2, row), "mis_ranked");
	let detail = string_value_at(&batch, 3, row);
	assert!(detail.contains("recommendation=WOULD_REORDER"), "{detail}");
	assert!(
		detail.contains("applied_order=[p,s,c]; measured_applied_order=[c,s,p]"),
		"{detail}"
	);
}

#[tokio::test]
async fn verdict_missed_win_when_margin_declined_a_real_win() {
	let batch = analyze_star_batch_with(
		EQUAL_STAR_FACTS,
		StarDataShape::DeclaredThenProductSelective,
	)
	.await;
	let row = join_order_verdict_row(&batch);

	assert_eq!(string_value_at(&batch, 2, row), "missed_win");
	let detail = string_value_at(&batch, 3, row);
	assert!(detail.contains("recommendation=WOULD_REORDER"), "{detail}");
	assert!(
		detail.contains("applied_order=[c,p,s]; measured_applied_order=[p,s,c]"),
		"{detail}"
	);
}

#[tokio::test]
async fn verdict_overeager_when_measurement_would_keep_declared() {
	let batch = analyze_star_batch().await;
	let row = join_order_verdict_row(&batch);

	assert_eq!(string_value_at(&batch, 2, row), "over_eager");
	let detail = string_value_at(&batch, 3, row);
	assert!(
		detail.contains("recommendation=WOULD_KEEP_DECLARED"),
		"{detail}"
	);
	assert!(
		detail.contains("applied_order=[s,p,c]; measured_applied_order=[c,p,s]"),
		"{detail}"
	);
}

#[tokio::test]
async fn verdict_reports_per_dim_estimate_accuracy() {
	let batch = analyze_star_batch().await;
	let detail = string_value_at(&batch, 3, join_order_verdict_row(&batch));

	assert!(detail.contains("accuracy=["), "{detail}");
	for dimension in ["c", "p", "s"] {
		assert!(
			detail.contains(&format!("{dimension}:estimated=")),
			"{detail}"
		);
		assert!(detail.contains(":actual=4:"), "{detail}");
		assert!(detail.contains(":ratio="), "{detail}");
		assert!(detail.contains(":accuracy=over_estimate"), "{detail}");
	}
}

#[tokio::test]
async fn verdict_uses_output_rows_not_build_or_probe_rows() {
	let batch = analyze_star_batch().await;
	let detail = string_value_at(&batch, 3, join_order_verdict_row(&batch));

	assert!(detail.contains(":actual=4:"), "{detail}");
	assert!(!detail.contains("build_rows"), "{detail}");
	assert!(!detail.contains("probe_rows"), "{detail}");
}

#[tokio::test]
async fn verdict_is_interpretation_only() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let planner = SqlPlanner::new();
	let query = planner.plan(STAR_SQL).expect("plan star query");
	let analyze = planner
		.plan(&format!("EXPLAIN ANALYZE {STAR_SQL}"))
		.expect("plan star analyze");

	let before = execute_single_batch(&coordinator, &query).await;
	let analysis = execute_single_batch(&coordinator, &analyze).await;
	let after = execute_single_batch(&coordinator, &query).await;

	assert_eq!(
		string_value_at(&analysis, 0, join_order_verdict_row(&analysis)),
		"join_order_verdict"
	);
	assert_eq!(
		format!("{:?}", before.schema()),
		format!("{:?}", after.schema())
	);
	assert_eq!(int64_values(&before, 0), int64_values(&after, 0));
}

#[tokio::test]
async fn verdict_does_not_act() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let planner = SqlPlanner::new();
	let explain = planner
		.plan(&format!("EXPLAIN {STAR_SQL}"))
		.expect("plan star explain");
	let analyze = planner
		.plan(&format!("EXPLAIN ANALYZE {STAR_SQL}"))
		.expect("plan star analyze");

	let before = execute_single_batch(&coordinator, &explain).await;
	let _analysis = execute_single_batch(&coordinator, &analyze).await;
	let after = execute_single_batch(&coordinator, &explain).await;

	assert_eq!(string_values(&before, 3), string_values(&after, 3));
	assert!(
		string_values(&after, 3)
			.iter()
			.any(|detail| detail.contains("applied_order=[s,p,c]")),
		"EXPLAIN should still report the original ADR178 applied order"
	);
}

#[tokio::test]
async fn ordinary_explain_has_no_runtime_verdict() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {STAR_SQL}"))
		.expect("plan star EXPLAIN");
	let batch = execute_single_batch(&coordinator, &logical).await;

	assert!(row_indices_for_kind(&batch, "join_order_verdict").is_empty());
	assert_eq!(row_indices_for_kind(&batch, "join_order").len(), 1);
}

#[tokio::test]
async fn cockpit_renders_verdict() {
	let batch = analyze_star_batch().await;
	let row = join_order_verdict_row(&batch);

	assert_eq!(string_value_at(&batch, 0, row), "join_order_verdict");
	assert_eq!(string_value_at(&batch, 1, row), "star_path");
	let detail = string_value_at(&batch, 3, row);
	assert!(detail.contains("measured_applied_order="), "{detail}");
	assert!(detail.contains("measured_action="), "{detail}");
	assert!(detail.contains("recommendation="), "{detail}");
	assert!(detail.contains("accuracy=["), "{detail}");
}

#[tokio::test]
async fn non_star_join_is_not_captured_by_star_analyze_allowlist() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN ANALYZE SELECT o.id, c.id \
			 FROM orders o INNER JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan non-star EXPLAIN ANALYZE");
	let batch = execute_single_batch(&coordinator, &logical).await;

	assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn runtime_measurement_is_observational() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let planner = SqlPlanner::new();
	let query = planner.plan(STAR_SQL).expect("plan star query");
	let analyze = planner
		.plan(&format!("EXPLAIN ANALYZE {STAR_SQL}"))
		.expect("plan star analyze");

	let before = execute_single_batch(&coordinator, &query).await;
	let _analysis = execute_single_batch(&coordinator, &analyze).await;
	let after = execute_single_batch(&coordinator, &query).await;

	assert_eq!(
		format!("{:?}", before.schema()),
		format!("{:?}", after.schema())
	);
	assert_eq!(int64_values(&before, 0), int64_values(&after, 0));
}

#[tokio::test]
async fn explain_analyze_was_previously_rejected_now_implemented() {
	let logical = SqlPlanner::new()
		.plan("EXPLAIN ANALYZE SELECT * FROM orders")
		.expect("EXPLAIN ANALYZE should plan");

	assert!(matches!(logical, LogicalPlan::ExplainAnalyze { .. }));
}

async fn analyze_star_batch() -> RecordBatch {
	analyze_star_batch_with(DEFAULT_STAR_FACTS, StarDataShape::AllMatch).await
}

async fn analyze_star_batch_with(facts: StarFactRows, data_shape: StarDataShape) -> RecordBatch {
	let (catalog, storage) = star_fixture_with(facts, data_shape).await;
	let coordinator = coordinator(catalog, storage);
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN ANALYZE {STAR_SQL}"))
		.expect("plan EXPLAIN ANALYZE");

	assert!(matches!(logical, LogicalPlan::ExplainAnalyze { .. }));
	execute_single_batch(&coordinator, &logical).await
}

async fn analyze_snowflake_batch() -> RecordBatch {
	let (catalog, storage) = snowflake_fixture().await;
	let coordinator = coordinator_with_config(
		catalog,
		storage,
		PlannerConfig {
			// ADR 192: a cap that leaves the customers branch (10_000 rows) hash but the small dims
			// broadcast keeps the snowflake genuinely mixed, so it stays observed (these tests
			// exercise the 182-188 observation/verdict machinery on a non-actuating snowflake).
			auto_broadcast_max_build_bytes: Some(ByteCount::new(65_536)),
			..PlannerConfig::default()
		},
	);
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN ANALYZE {SNOWFLAKE_SQL}"))
		.expect("plan snowflake EXPLAIN ANALYZE");

	assert!(matches!(logical, LogicalPlan::ExplainAnalyze { .. }));
	execute_single_batch(&coordinator, &logical).await
}

#[tokio::test]
async fn hash_star_measured_verdict_extends() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator_with_config(
		catalog,
		storage,
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(1)),
			..PlannerConfig::default()
		},
	);
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN ANALYZE {STAR_SQL}"))
		.expect("plan hash star EXPLAIN ANALYZE");

	let batch = execute_single_batch(&coordinator, &logical).await;
	let runtime_rows = row_indices_for_kind(&batch, "join_order_runtime");
	let verdict_row = join_order_verdict_row(&batch);
	let verdict_detail = string_value_at(&batch, 3, verdict_row);

	assert_eq!(runtime_rows.len(), 3);
	assert_eq!(
		string_value_at(&batch, 0, verdict_row),
		"join_order_verdict"
	);
	assert_eq!(string_value_at(&batch, 2, verdict_row), "over_eager");
	assert!(verdict_detail.contains("strategy=hash_partitioned"));
}

async fn execute_single_batch(coordinator: &QueryCoordinator, plan: &LogicalPlan) -> RecordBatch {
	let batches = coordinator
		.execute_query(plan, None)
		.await
		.expect("execute query");
	let schema = batches
		.first()
		.map(RecordBatch::schema)
		.expect("at least one batch");
	arrow::compute::concat_batches(&schema, batches.iter()).expect("concat batches")
}

fn coordinator(catalog: Arc<MockCatalog>, storage: Arc<InMemoryTableStorage>) -> QueryCoordinator {
	coordinator_with_config(
		catalog,
		storage,
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(64 * 1024 * 1024)),
			..PlannerConfig::default()
		},
	)
}

fn coordinator_with_config(
	catalog: Arc<MockCatalog>,
	storage: Arc<InMemoryTableStorage>,
	config: PlannerConfig,
) -> QueryCoordinator {
	QueryCoordinator::new(
		catalog,
		Executor::with_context(
			storage,
			ExecutionContext::new(
				SessionId(1),
				QueryId::from_u128(179),
				TenantId(1),
				HlcBound(1),
			),
		),
		config,
	)
}

async fn star_fixture() -> (Arc<MockCatalog>, Arc<InMemoryTableStorage>) {
	star_fixture_with(DEFAULT_STAR_FACTS, StarDataShape::AllMatch).await
}

async fn star_fixture_with(
	facts: StarFactRows,
	data_shape: StarDataShape,
) -> (Arc<MockCatalog>, Arc<InMemoryTableStorage>) {
	let mut catalog = MockCatalog::new();
	let storage = Arc::new(InMemoryTableStorage::new());

	let orders = descriptor(
		21,
		"orders",
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
			Field::new("product_id", DataType::Int64, false),
			Field::new("store_id", DataType::Int64, false),
		])),
		"id",
	);
	let customers = single_id_descriptor(22, "customers");
	let products = single_id_descriptor(23, "products");
	let stores = single_id_descriptor(24, "stores");

	catalog
		.register_descriptor(TableId(21), orders.clone())
		.expect("register orders");
	catalog
		.register_descriptor(TableId(22), customers.clone())
		.expect("register customers");
	catalog
		.register_descriptor(TableId(23), products.clone())
		.expect("register products");
	catalog
		.register_descriptor(TableId(24), stores.clone())
		.expect("register stores");

	seed_star_facts(&mut catalog, facts);

	write_table(
		storage.as_ref(),
		TableId(21),
		&orders,
		orders_batch(
			orders.arrow_schema().expect("orders schema").into(),
			data_shape,
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(22),
		&customers,
		id_batch(
			customers.arrow_schema().expect("customers schema").into(),
			customer_ids(data_shape),
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(23),
		&products,
		id_batch(
			products.arrow_schema().expect("products schema").into(),
			product_ids(data_shape),
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(24),
		&stores,
		id_batch(
			stores.arrow_schema().expect("stores schema").into(),
			store_ids(data_shape),
		),
	)
	.await;

	(Arc::new(catalog), storage)
}

async fn snowflake_fixture() -> (Arc<MockCatalog>, Arc<InMemoryTableStorage>) {
	let mut catalog = MockCatalog::new();
	let storage = Arc::new(InMemoryTableStorage::new());

	let orders = descriptor(
		31,
		"orders",
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
			Field::new("store_id", DataType::Int64, false),
		])),
		"id",
	);
	let customers = descriptor(
		32,
		"customers",
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region_id", DataType::Int64, false),
			Field::new("segment_id", DataType::Int64, false),
		])),
		"id",
	);
	let regions = single_id_descriptor(33, "regions");
	let segments = single_id_descriptor(34, "segments");
	let stores = single_id_descriptor(35, "stores");

	for (table_id, descriptor) in [
		(TableId(31), &orders),
		(TableId(32), &customers),
		(TableId(33), &regions),
		(TableId(34), &segments),
		(TableId(35), &stores),
	] {
		catalog
			.register_descriptor(table_id, descriptor.clone())
			.expect("register snowflake descriptor");
	}
	seed_snowflake_facts(&mut catalog);

	write_table(
		storage.as_ref(),
		TableId(31),
		&orders,
		record_batch(
			orders.arrow_schema().expect("orders schema").into(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
				Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
				Arc::new(Int64Array::from(vec![1000, 1000, 2000, 3000, 4000])),
			],
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(32),
		&customers,
		record_batch(
			customers.arrow_schema().expect("customers schema").into(),
			vec![
				Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
				Arc::new(Int64Array::from(vec![100, 200, 100, 300, 400])),
				Arc::new(Int64Array::from(vec![10000, 10000, 20000, 30000, 40000])),
			],
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(33),
		&regions,
		id_batch(
			regions.arrow_schema().expect("regions schema").into(),
			&[100, 200, 300],
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(34),
		&segments,
		id_batch(
			segments.arrow_schema().expect("segments schema").into(),
			&[10000, 20000],
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(35),
		&stores,
		id_batch(
			stores.arrow_schema().expect("stores schema").into(),
			&[1000],
		),
	)
	.await;

	(Arc::new(catalog), storage)
}

fn seed_snowflake_facts(catalog: &mut MockCatalog) {
	for (table, rows, ndv) in [
		("orders", 100_000, vec![(1, 100), (2, 100)]),
		("customers", 10_000, vec![(0, 100), (1, 100), (2, 100)]),
		("regions", 1_000, vec![(0, 100)]),
		("segments", 500, vec![(0, 100)]),
		("stores", 100, vec![(0, 100)]),
	] {
		catalog
			.seed_optimizer_facts(
				&TableRef::new(table),
				TableOptimizerFacts {
					row_upper_bound: Some(RowCount::new(rows)),
					column_ndv: ndv
						.into_iter()
						.map(|(ordinal, distinct)| {
							(ColumnOrdinal(ordinal), DistinctCountFact::Exact(distinct))
						})
						.collect::<BTreeMap<_, _>>(),
					..TableOptimizerFacts::default()
				},
			)
			.expect("seed snowflake optimizer facts");
	}
}

fn seed_star_facts(catalog: &mut MockCatalog, facts: StarFactRows) {
	for (table, rows, ndv) in [
		("orders", 100_000, vec![(1, 100), (2, 100), (3, 100)]),
		("customers", facts.customers, vec![(0, 100)]),
		("products", facts.products, vec![(0, 100)]),
		("stores", facts.stores, vec![(0, 100)]),
	] {
		catalog
			.seed_optimizer_facts(
				&TableRef::new(table),
				TableOptimizerFacts {
					row_upper_bound: Some(RowCount::new(rows)),
					column_ndv: ndv
						.into_iter()
						.map(|(ordinal, distinct)| {
							(ColumnOrdinal(ordinal), DistinctCountFact::Exact(distinct))
						})
						.collect::<BTreeMap<_, _>>(),
					..TableOptimizerFacts::default()
				},
			)
			.expect("seed optimizer facts");
	}
}

fn orders_batch(schema: SchemaRef, data_shape: StarDataShape) -> RecordBatch {
	match data_shape {
		StarDataShape::AllMatch => record_batch(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
				Arc::new(Int64Array::from(vec![10, 20, 10, 30])),
				Arc::new(Int64Array::from(vec![100, 100, 200, 300])),
				Arc::new(Int64Array::from(vec![1000, 2000, 1000, 3000])),
			],
		),
		StarDataShape::DeclaredThenProductSelective
		| StarDataShape::ProductFirstThenStoreThenCustomerSelective => record_batch(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
				Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
				Arc::new(Int64Array::from(vec![100, 200, 100, 300])),
				Arc::new(Int64Array::from(vec![1000, 1000, 2000, 3000])),
			],
		),
	}
}

fn customer_ids(data_shape: StarDataShape) -> &'static [i64] {
	match data_shape {
		StarDataShape::AllMatch | StarDataShape::DeclaredThenProductSelective => &[10, 20, 30, 40],
		StarDataShape::ProductFirstThenStoreThenCustomerSelective => &[10],
	}
}

fn product_ids(data_shape: StarDataShape) -> &'static [i64] {
	match data_shape {
		StarDataShape::AllMatch | StarDataShape::ProductFirstThenStoreThenCustomerSelective => {
			&[100, 200, 300]
		}
		StarDataShape::DeclaredThenProductSelective => &[100],
	}
}

fn store_ids(data_shape: StarDataShape) -> &'static [i64] {
	match data_shape {
		StarDataShape::AllMatch => &[1000, 2000, 3000],
		StarDataShape::DeclaredThenProductSelective
		| StarDataShape::ProductFirstThenStoreThenCustomerSelective => &[1000, 2000],
	}
}

async fn write_table(
	storage: &InMemoryTableStorage,
	table_id: TableId,
	descriptor: &TableSchemaDescriptor,
	payload: RecordBatch,
) {
	storage
		.write(WriteRequest {
			session_id: SessionId(1),
			query_id: QueryId::from_u128(179),
			tenant_id: TenantId(1),
			table_id,
			table_object_id: descriptor.table_object_id,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			payload,
			profile_hint: Some(QueryProfileHint::Scan),
			trace_context: None,
		})
		.await
		.expect("write fixture table");
}

fn single_id_descriptor(id: u8, name: &str) -> TableSchemaDescriptor {
	descriptor(
		id,
		name,
		Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
		"id",
	)
}

fn descriptor(id: u8, name: &str, schema: SchemaRef, primary_key: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema.as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn id_batch(schema: SchemaRef, values: &[i64]) -> RecordBatch {
	record_batch(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
}

fn record_batch(schema: SchemaRef, columns: Vec<Arc<dyn arrow::array::Array>>) -> RecordBatch {
	RecordBatch::try_new(schema, columns).expect("record batch")
}

fn string_values(batch: &RecordBatch, column: usize) -> Vec<String> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("string array");
	(0..array.len())
		.map(|row| array.value(row).to_string())
		.collect()
}

fn string_values_for_rows(batch: &RecordBatch, column: usize, rows: &[usize]) -> Vec<String> {
	rows.iter()
		.map(|row| string_value_at(batch, column, *row))
		.collect()
}

fn string_value_at(batch: &RecordBatch, column: usize, row: usize) -> String {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("string array");
	array.value(row).to_string()
}

fn int64_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("int64 array");
	(0..array.len()).map(|row| array.value(row)).collect()
}

fn int64_values_for_rows(batch: &RecordBatch, column: usize, rows: &[usize]) -> Vec<i64> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("int64 array");
	rows.iter().map(|row| array.value(*row)).collect()
}

fn float64_values_for_rows(batch: &RecordBatch, column: usize, rows: &[usize]) -> Vec<f64> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<arrow::array::Float64Array>()
		.expect("float64 array");
	rows.iter().map(|row| array.value(*row)).collect()
}

fn row_indices_for_kind(batch: &RecordBatch, expected_kind: &str) -> Vec<usize> {
	let kinds = batch
		.column(0)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("decision_kind string array");
	(0..batch.num_rows())
		.filter(|row| kinds.value(*row) == expected_kind)
		.collect()
}

fn row_index_for_kind_and_target(
	batch: &RecordBatch,
	expected_kind: &str,
	expected_target: &str,
) -> usize {
	let kinds = batch
		.column(0)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("decision_kind string array");
	let targets = batch
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("target string array");
	let rows = (0..batch.num_rows())
		.filter(|row| kinds.value(*row) == expected_kind && targets.value(*row) == expected_target)
		.collect::<Vec<_>>();
	assert_eq!(
		rows.len(),
		1,
		"expected one {expected_kind}/{expected_target} row"
	);
	rows[0]
}

fn join_order_verdict_row(batch: &RecordBatch) -> usize {
	let rows = row_indices_for_kind(batch, "join_order_verdict");
	assert_eq!(rows.len(), 1, "expected one join_order_verdict row");
	rows[0]
}
