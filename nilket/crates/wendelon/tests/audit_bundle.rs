#[test]
fn audit_ops_doc_describes_sink_schema_asymmetry() {
	let audit = std::fs::read_to_string("plans/ops/audit.md").unwrap();

	assert!(audit.contains("not schema-identical"));
	assert!(audit.contains("PersistedAuditRecord"));
	assert!(audit.contains("AuditEvent"));
	assert!(audit.contains("RFC 5424"));
	assert!(audit.contains("Local NDJSON"));
	assert!(audit.contains("Archive mirror"));
	assert!(audit.contains("Kafka"));
	assert!(audit.contains("payload_mode"));
	assert!(audit.contains("authority_model"));
	assert!(audit.contains("Syslog"));
	assert!(audit.contains("audit.ndjson"));
}

#[test]
fn operator_docs_point_to_audit_sink_guidance() {
	let observability = std::fs::read_to_string("plans/ops/observability.md").unwrap();
	let configuration = std::fs::read_to_string("plans/ops/configuration.md").unwrap();
	let example = std::fs::read_to_string("examples/wendelon.toml.example").unwrap();

	assert!(observability.contains("[Audit](./audit.md)"));
	assert!(configuration.contains("plans/ops/audit.md"));
	assert!(example.contains("plans/ops/audit.md"));
}
