use super::support::{manifest_dir, source_files};

#[test]
pub(crate) fn adr083_task_execution_context_has_no_default_quantile_seed() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("read task execution context source");
	let constructor = context
		.split_once("pub const fn new(")
		.expect("TaskExecutionContext::new")
		.1
		.split_once("\n\t}")
		.expect("TaskExecutionContext::new body")
		.0;

	// The seed is a required parameter. A constructor that supplies one itself
	// hands every caller a constant, and Wendelon ADR 83's guarantee requires
	// randomness drawn independently of the data.
	assert!(
		constructor.contains("quantile_execution_seed: [u8; 32]"),
		"TaskExecutionContext::new must take the execution seed as a parameter"
	);
	assert!(
		!constructor.contains("[0xA5; 32]"),
		"TaskExecutionContext::new must not embed a placeholder seed"
	);

	// The named test seed exists, and only the test constructor reaches for it.
	assert!(context.contains("pub const LOCAL_TEST_QUANTILE_SEED: [u8; 32]"));
	let test_constructor = context
		.split_once("pub const fn local_test(")
		.expect("TaskExecutionContext::local_test")
		.1
		.split_once("\n\t}")
		.expect("TaskExecutionContext::local_test body")
		.0;
	assert!(test_constructor.contains("LOCAL_TEST_QUANTILE_SEED"));
}

#[test]
pub(crate) fn adr083_fragment_plan_construction_requires_real_execution_context() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let constructor = fragment
		.split_once("pub fn new(")
		.expect("FragmentPlanConfig::new")
		.1
		.split_once("\n\t}")
		.expect("FragmentPlanConfig::new body")
		.0;
	assert!(constructor.contains("task_context: TaskExecutionContext"));
	assert!(!constructor.contains("TaskExecutionContext::local_test()"));

	let test_constructor = fragment
		.split_once("pub fn new_for_test(")
		.expect("FragmentPlanConfig::new_for_test")
		.1
		.split_once("\n\t}")
		.expect("FragmentPlanConfig::new_for_test body")
		.0;
	assert!(test_constructor.contains("TaskExecutionContext::local_test()"));

	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let production = coordinator
		.split_once("#[cfg(test)]")
		.expect("coordinator test boundary")
		.0;
	assert_eq!(production.matches("FragmentPlanConfig::new(").count(), 2);
	assert!(!production.contains("FragmentPlanConfig::new_for_test("));
	assert!(!production.contains(".with_task_context("));

	let tpch = std::fs::read_to_string(manifest_dir().join("../toron-tpch/src/lib.rs"))
		.expect("read TPCH source");
	assert_eq!(tpch.matches("FragmentPlanConfig::new(").count(), 3);
	assert!(!tpch.contains("FragmentPlanConfig::new_for_test("));
	assert!(!tpch.contains(".with_task_context("));

	let test_convenience_calls = source_files(&manifest_dir().join("src"))
		.into_iter()
		.flat_map(|path| {
			std::fs::read_to_string(&path)
				.expect("read Toron source")
				.lines()
				.filter(|line| line.contains("FragmentPlanConfig::new_for_test("))
				.map(move |_| path.clone())
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();
	assert_eq!(test_convenience_calls.len(), 3);
	assert!(
		test_convenience_calls
			.iter()
			.all(|path| path.ends_with("lib.rs") || path.ends_with("distributed/fragment.rs"))
	);
}

#[test]
pub(crate) fn adr546_quantile_has_one_stable_authoritative_execution_path() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	// ADR 561: the mapping is total, so the quantile arm carries no Option.
	assert!(
		catalog.contains(
			"AggregateFunction::ApproxQuantile(_) => StableAggregateKernel::ApproxQuantile"
		)
	);
	// Experimental name resolution derives from the entries' experimental flag
	// rather than a hand match, so APPROX_QUANTILE cannot drift into it.
	assert!(catalog.contains("spec.experimental && spec.sql_name == upper"));
	assert!(!catalog.contains("\"APPROX_QUANTILE\" => Some(\"APPROX_QUANTILE\")"));

	let aggregate = std::fs::read_to_string(manifest_dir().join("src/exec/aggregate.rs"))
		.expect("read aggregate executor source");
	assert!(aggregate.contains("KllSketch::new_with_randomness"));
	assert!(aggregate.contains(".query_quantile(quantile.q)"));
	assert!(!aggregate.contains(".retained_values()"));
	assert!(aggregate.contains("MAX_QUANTILE_STATES_PER_EXECUTION: usize = 1_024"));
	assert!(aggregate.contains("quantile_populations_and_declared_bytes"));
	assert!(
		!aggregate.contains("aws_lc_rs::rand::fill"),
		"aggregate paths must consume the coordinator seed, not mint local randomness"
	);

	let streaming = std::fs::read_to_string(manifest_dir().join("src/exec/join/streaming.rs"))
		.expect("read streaming join executor source");
	assert_eq!(
		streaming
			.matches("init_global_aggregate_state(&self.context, aggr_expr)?")
			.count(),
		2,
		"both streaming-join global aggregate paths must retain the execution seed and admission"
	);

	let legacy = std::fs::read_to_string(manifest_dir().join("src/exec.rs"))
		.expect("read legacy executor source");
	assert!(!legacy.contains("retained_values"));
	assert!(legacy.contains("APPROX_QUANTILE requires the bound aggregate executor"));

	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	assert!(fragmenter.contains("distribute_quantile_state_allowance(&mut stages)"));
	assert!(fragmenter.contains("const EXECUTION_CAP: usize = 1_024"));

	let wendelon = manifest_dir().join("../wendelon/src");
	let sketch = std::fs::read_to_string(wendelon.join("storage/sketch.rs"))
		.expect("read Wendelon sketch source");
	let sketch_fields = sketch
		.split_once("pub struct KllSketch {")
		.expect("KllSketch declaration")
		.1
		.split_once("impl KllSketch")
		.expect("KllSketch implementation")
		.0;
	assert!(sketch_fields.contains("levels: Vec<Vec<CanonicalValue>>"));
	assert!(!sketch_fields.contains("values: Vec<Vec<u8>>"));
	assert!(!sketch.contains("1_000_000 /"));

	let storage_aggregate = std::fs::read_to_string(wendelon.join("storage/aggregate.rs"))
		.expect("read Wendelon aggregate source");
	assert!(!storage_aggregate.contains("levels.into_iter().flatten()"));

	let storage_stats = std::fs::read_to_string(wendelon.join("storage/stats.rs"))
		.expect("read Wendelon stats source");
	assert!(!storage_stats.contains("COLUMN_STATS_KLL_COMPACTION_SEED"));
}

#[test]
pub(crate) fn adr547_tukey_relation_does_not_create_a_plot_specific_database_surface() {
	let production = [
		"src/ir.rs",
		"src/ir/execution.rs",
		"src/sql_planner.rs",
		"src/aggregate_kernel.rs",
		"src/exec/aggregate.rs",
	]
	.into_iter()
	.map(|relative| {
		std::fs::read_to_string(manifest_dir().join(relative))
			.unwrap_or_else(|error| panic!("read {relative}: {error}"))
	})
	.collect::<String>()
	.to_ascii_lowercase();
	for plot_specific in ["boxplot", "box_plot", "tukey", "whisker", "polariton"] {
		assert!(
			!production.contains(plot_specific),
			"ADR 547 must remain ordinary relational composition, found {plot_specific}"
		);
	}

	let group_by = std::fs::read_to_string(manifest_dir().join("tests/group_by_test.rs"))
		.expect("read group-by tests");
	for marker in [
		"adr547_tukey_relation_preserves_fences_whiskers_outliers_and_null_policy",
		"vec![\"key\", \"low\", \"q1\", \"median\", \"q3\", \"high\", \"value\"]",
		"ADR547_TUKEY_SQL",
	] {
		assert!(
			group_by.contains(marker),
			"missing ADR 547 control {marker}"
		);
	}
	let distributed =
		std::fs::read_to_string(manifest_dir().join("tests/distributed_process_test.rs"))
			.expect("read distributed tests");
	assert!(
		distributed.contains("adr547_tukey_relation_corresponds_locally_and_across_worker_counts")
	);
}
