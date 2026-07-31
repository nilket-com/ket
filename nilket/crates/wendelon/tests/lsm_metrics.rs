use std::time::Duration;
use wendelon::metrics::{LsmCompactionResult, LsmDigestInvariantKind, MetricsRegistry};
use wendelon::schema::CompactionLevelIndex;

#[test]
fn lsm_metrics_render_bounded_label_sets() {
	let registry = MetricsRegistry::default();
	registry.observe_lsm_compaction(
		CompactionLevelIndex::L1,
		LsmCompactionResult::ManifestRace,
		4,
		4096,
		Duration::from_millis(12),
	);
	registry.set_lsm_level_summary(CompactionLevelIndex::L3, 2, 8192);
	registry.set_lsm_gc_pressure(3, 1024);
	registry.observe_lsm_digest_invariance_violation(LsmDigestInvariantKind::VisibleDomain);

	let rendered = registry.render_prometheus();

	for level in ["0", "1", "2"] {
		for result in [
			"ok",
			"manifest_race",
			"sidecar_digest_mismatch",
			"visible_digest_invariance",
			"aborted",
		] {
			assert!(rendered.contains(&format!(
				"wendelon_lsm_compactions_total{{level=\"{level}\",result=\"{result}\"}}"
			)));
		}
		assert!(rendered.contains(&format!(
			"wendelon_lsm_compaction_input_slabs{{level=\"{level}\"}}"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_lsm_compaction_output_bytes{{level=\"{level}\"}}"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_lsm_compaction_duration_millis{{level=\"{level}\"}}"
		)));
	}

	for level in ["0", "1", "2", "3"] {
		assert!(rendered.contains(&format!(
			"wendelon_lsm_level_slab_count{{level=\"{level}\"}}"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_lsm_level_total_bytes{{level=\"{level}\"}}"
		)));
	}

	assert!(rendered.contains("wendelon_lsm_pending_delete_slabs 3"));
	assert!(rendered.contains("wendelon_lsm_retired_slab_pinned_bytes 1024"));
	assert!(
		rendered
			.contains("wendelon_lsm_digest_invariance_violations_total{kind=\"visible_domain\"} 1")
	);
	assert!(
		rendered
			.contains("wendelon_lsm_digest_invariance_violations_total{kind=\"version_entry\"} 0")
	);
	assert!(!rendered.contains("tenant_id"));
	assert!(!rendered.contains("table_object_id"));
	assert!(!rendered.contains("subscription_id"));
	assert!(!rendered.contains("slab_id"));
}
