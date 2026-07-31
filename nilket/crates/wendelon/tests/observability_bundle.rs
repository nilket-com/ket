#[test]
fn observability_examples_reference_exported_metrics() {
	let rules = std::fs::read_to_string("examples/observability/recording-rules.yaml").unwrap();
	let alerts = std::fs::read_to_string("examples/observability/alerts.yaml").unwrap();

	assert!(rules.contains("job:wendelon_group_commit_window_seconds:p99_5m"));
	assert!(rules.contains("wendelon_group_commit_window_seconds_bucket"));
	assert!(rules.contains("wendelon_l1_read_latency_seconds_bucket"));
	assert!(rules.contains("wendelon_l2_range_read_latency_seconds_bucket"));
	assert!(rules.contains("wendelon_l2_upload_latency_seconds_bucket"));

	assert!(alerts.contains("absent(wendelon_raft_current_leader)"));
	assert!(alerts.contains("wendelon_audit_materialization_lag_entries > 1000"));
	assert!(alerts.contains("wendelon_audit_sink_failure_total{sink=\"local\"}"));
	assert!(alerts.contains("wendelon_tenant_kek_cache_hit_total"));
	assert!(alerts.contains("wendelon_quic_handshake_failures_total"));

	assert!(!rules.contains("raft_commit_latency_seconds_bucket"));
	assert!(!rules.contains("rpc_latency_seconds_bucket{operation=\"read\"}"));
	assert!(!alerts.contains("cert_expiry_seconds"));
}
