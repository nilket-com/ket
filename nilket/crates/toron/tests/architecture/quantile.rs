use super::support::manifest_dir;

#[test]
pub(crate) fn adr546_quantile_has_one_stable_authoritative_execution_path() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains(
		"AggregateFunction::ApproxQuantile(_) => Some(StableAggregateKernel::ApproxQuantile)"
	));
	assert!(catalog.contains("\"LINEAR_REGRESS\" => Some(\"LINEAR_REGRESS\")"));
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
