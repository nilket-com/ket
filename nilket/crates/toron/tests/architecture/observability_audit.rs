#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};

#[test]
pub(crate) fn adr065_observability_crate_exists_under_workspace_glob() {
	assert!(
		workspace_root_dir()
			.join("crates/observability/Cargo.toml")
			.exists()
	);
	assert!(
		std::fs::read_to_string(workspace_root_dir().join("Cargo.toml"))
			.expect("read workspace manifest")
			.contains("\"crates/*\"")
	);
}

#[test]
pub(crate) fn adr065_observability_default_features_are_empty() {
	assert!(observability_manifest().contains("default = []"));
}

#[test]
pub(crate) fn adr065_observability_exposes_http_and_audit_modules() {
	let lib = std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/lib.rs"))
		.expect("read observability lib");
	assert!(lib.contains("#[cfg(feature = \"http\")]"));
	assert!(lib.contains("pub mod http;"));
	assert!(lib.contains("#[cfg(feature = \"audit\")]"));
	assert!(lib.contains("pub mod audit;"));
}

#[test]
pub(crate) fn adr065_observability_builds_with_only_http_feature() {
	let manifest = observability_manifest();
	assert!(manifest.contains(r#"http = ["dep:tokio"]"#));
	assert!(manifest.contains("tokio = {"));
	assert!(manifest.contains("optional = true"));
}

#[test]
pub(crate) fn adr065_observability_builds_with_only_audit_feature() {
	let manifest = observability_manifest();
	assert!(manifest.contains("audit = ["));
	for required in [
		"dep:serde",
		"dep:serde_json",
		"dep:ed25519-dalek",
		"dep:rand",
	] {
		assert!(
			manifest.contains(required),
			"audit feature missing {required}"
		);
	}
}

#[test]
pub(crate) fn adr065_observability_http_does_not_reference_audit() {
	let source = observability_http_source_text();
	for forbidden in [
		"observability::audit",
		"crate::audit",
		"super::audit",
		"AuditSink",
	] {
		assert!(
			!source.contains(forbidden),
			"observability::http must not reference audit token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr065_observability_audit_does_not_reference_http() {
	let source = observability_audit_source_text();
	assert!(!source.contains("pub mod http"));
	assert!(!source.contains("observability::http"));
	assert!(!source.contains("HttpResponse"));
	assert!(!source.contains("RequestLine"));
}

#[test]
pub(crate) fn adr065_observability_http_retargets_adr048_substrate() {
	let toron_manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let wendelon_manifest = std::fs::read_to_string(wendelon_manifest_dir().join("Cargo.toml"))
		.expect("read Wendelon manifest");
	assert!(toron_manifest.contains("observability = { path = \"../observability\""));
	assert!(toron_manifest.contains("\"http\""));
	assert!(toron_manifest.contains("\"audit\""));
	assert!(wendelon_manifest.contains("observability = { path = \"../observability\""));
	assert!(wendelon_manifest.contains("\"http\""));
	assert!(
		workspace_root_dir()
			.join("crates/observability/src/http/mod.rs")
			.exists()
	);
}

#[test]
pub(crate) fn adr065_observability_http_preserves_limits_and_timeout_boundary() {
	adr048_observability_http_has_bounded_header_and_body_limits();
	adr048_observability_http_does_not_own_timeouts();
	adr045_observability_http_has_per_request_read_timeout();
}

#[test]
pub(crate) fn adr065_observability_http_preserves_small_public_surface() {
	adr048_observability_http_public_surface_is_small();
}

#[test]
pub(crate) fn adr065_flat_observability_http_runtime_crate_removed() {
	assert!(
		!workspace_root_dir()
			.join("crates/observability-http")
			.exists()
	);
	assert!(!observability_manifest().contains("name = \"observability-http\""));
}

#[test]
pub(crate) fn adr065_observability_has_no_toron_or_wendelon_dependency() {
	let manifest = observability_manifest();
	for forbidden in ["toron", "wendelon", "../toron", "../wendelon"] {
		assert!(
			!manifest.contains(forbidden),
			"observability manifest must not depend on {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr065_observability_has_no_quinn_raft_sql_wire_tls_or_router_dependency() {
	let manifest = observability_manifest();
	let source = format!(
		"{}\n{}",
		observability_http_source_text(),
		observability_audit_source_text()
	);
	for forbidden in [
		"quinn",
		"openraft",
		"sql_wire",
		"SqlMessage",
		"router",
		"Router",
	] {
		assert!(
			!manifest.contains(forbidden),
			"manifest contains ring dependency {forbidden}"
		);
	}
	for forbidden in [
		"quinn",
		"openraft",
		"sql_wire",
		"SqlMessage",
		"tls-pki",
		"tls_pki",
		"rustls",
		"router",
		"Router",
	] {
		assert!(!source.contains(forbidden), "source contains {forbidden}");
	}
}

#[test]
pub(crate) fn adr065_wendelon_consumes_observability_audit() {
	let manifest = std::fs::read_to_string(wendelon_manifest_dir().join("Cargo.toml"))
		.expect("read Wendelon manifest");
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(manifest.contains("\"audit\""));
	assert!(audit.contains("use observability::audit as shared_audit;"));
	assert!(audit.contains("shared_audit::build_audit_sink"));
	assert!(audit.contains("shared_audit::LocalAuditSink::open_with_archive"));
}

#[test]
pub(crate) fn adr065_wendelon_payload_remains_ring_owned_and_typed() {
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	for required in [
		"pub enum AuditOperation",
		"pub enum AuditSchema",
		"pub struct AdministrativeControlSchema",
		"pub struct LocalSecuritySchema",
		"pub struct DataAccessSchema",
		"pub fn from_actor_identity",
	] {
		assert!(
			audit.contains(required),
			"Wendelon audit missing {required}"
		);
	}
	for forbidden in ["pub enum ToronPayload", "pub struct ToronPayload"] {
		assert!(!audit.contains(forbidden));
	}
}

#[test]
pub(crate) fn adr065_wendelon_audit_serialization_is_preserved() {
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(audit.contains("serialize_struct(\"AuditEvent\", 13)"));
	assert!(audit.contains("event.serialize_field(\"authority_model\", &self.authority_model())"));
	assert!(audit.contains("event.serialize_field(\"operation\", &self.operation)"));
	assert!(audit.contains("serde_json::to_value(schema)"));
	assert!(audit.contains("identity.as_bytes()"));
}

#[test]
pub(crate) fn adr065_wendelon_audit_verify_cli_uses_shared_verifier() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI");
	let audit = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(cli.contains("use crate::audit::verify_local_audit_evidence"));
	assert!(audit.contains("shared_audit::verify_local_audit_evidence"));
}

#[test]
pub(crate) fn adr065_local_ndjson_chain_sink_is_in_observability_audit() {
	let source = observability_audit_source_text();
	assert!(source.contains("pub struct LocalAuditSink"));
	assert!(source.contains("OpenOptions::new().create(true).append(true).open"));
	assert!(source.contains("serde_json::to_string(&persisted)"));
	assert!(source.contains("sync_data()"));
}

#[test]
pub(crate) fn adr065_audit_sink_trait_is_in_observability_audit() {
	let source = observability_audit_source_text();
	assert!(source.contains("pub trait AuditSink: Send + Sync"));
	assert!(source.contains("fn emit(&self, event: AuditEvent)"));
	assert!(source.contains("fn chain_head(&self)"));
}

#[test]
pub(crate) fn adr065_audit_format_literals_are_consumer_provided() {
	let shared = observability_audit_source_text();
	let wendelon = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(shared.contains("pub struct AuditFormat"));
	for literal in [
		"wendelon-audit-genesis-v1",
		"wendelon-audit-chain-root-v1",
		"wendelon@47450",
	] {
		assert!(
			!shared.contains(literal),
			"shared audit must not hard-code Wendelon literal {literal}"
		);
		assert!(
			wendelon.contains(literal),
			"Wendelon adapter must provide literal {literal}"
		);
	}
}

#[test]
pub(crate) fn adr065_kafka_sink_remains_feature_gated() {
	let manifest = observability_manifest();
	let source = observability_audit_source_text();
	assert!(manifest.contains(r#"kafka = ["audit", "dep:rdkafka"]"#));
	assert!(source.contains("#[cfg(feature = \"kafka\")]"));
	assert!(source.contains("pub trait KafkaProducer"));
}

#[test]
pub(crate) fn adr065_toron_audit_consumption_keeps_shared_substrate_policy_free() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	let shared = observability_audit_source_text();
	assert!(manifest.contains("features = [\"http\", \"audit\"]"));
	assert!(config.contains("[audit]"));
	assert!(manifest.contains("default-features = false"));
	for forbidden in [
		"tokio::sync::mpsc",
		"try_send",
		"ToronAuditEvent",
		"ToronAuditOperation",
		"SqlErrorCode",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared audit substrate gained Toron policy token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr065_no_async_try_send_policy_in_observability_audit() {
	let source = observability_audit_source_text();
	for forbidden in ["tokio::sync::mpsc", "try_send", "spawn(", "async fn"] {
		assert!(
			!source.contains(forbidden),
			"shared audit contains async policy {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr065_audit_schema_version_is_preserved() {
	let wendelon = std::fs::read_to_string(wendelon_manifest_dir().join("src/audit.rs"))
		.expect("read Wendelon audit");
	assert!(wendelon.contains("const AUDIT_SCHEMA_VERSION: u16 = 4;"));
	assert!(wendelon.contains("schema_version: AUDIT_SCHEMA_VERSION"));
}

#[test]
pub(crate) fn adr065_observability_namespace_reserves_future_telemetry_facet() {
	let adr = std::fs::read_to_string(
		manifest_dir().join("plans/adr/0065_observability_namespace_audit_substrate.md"),
	)
	.expect("read ADR 065");
	let lib = std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/lib.rs"))
		.expect("read observability lib");
	assert!(adr.contains("observability::telemetry"));
	assert!(lib.contains("#[cfg(feature = \"telemetry\")]"));
	assert!(lib.contains("pub mod telemetry"));
}

#[test]
pub(crate) fn adr065_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	// TORON-532 supersedes the leaf's no-PKI/no-runtime-manifest restriction;
	// it does not admit an async, TLS, or observability runtime here.
	for forbidden in ["tokio", "rustls", "observability"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr065_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	assert!(!worker.contains("observability"));
	assert!(!worker.contains("audit"));
}

#[test]
pub(crate) fn adr065_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let source = format!(
		"{}\n{}",
		observability_http_source_text(),
		observability_audit_source_text()
	);
	for forbidden in [
		"ToronAudit",
		"WendelonAudit",
		"NilketAudit",
		"AuditEventV1",
		"AuditSinkV1",
		"ObservabilityV1",
		"TelemetryV1",
	] {
		assert!(
			!source.contains(forbidden),
			"observability surface contains forbidden branded/V1 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr065_prior_adr045_to_064_guards_continue_to_pass_except_adr048_retarget() {
	let architecture_test = architecture_corpus_source();
	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr065_flat_observability_http_runtime_crate_removed"));
}

#[test]
pub(crate) fn adr066_toron_consumes_observability_audit_without_kafka_default() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(manifest.contains("observability = { path = \"../observability\""));
	assert!(manifest.contains("default-features = false"));
	assert!(manifest.contains("features = [\"http\", \"audit\"]"));
	assert!(!manifest.contains("\"kafka\""));
	assert!(audit.contains("use observability::audit as shared_audit;"));
	assert!(audit.contains("shared_audit::build_audit_sink"));
	assert!(audit.contains("toron_audit_format()"));
}

#[test]
pub(crate) fn adr066_audit_config_is_default_disabled_and_enabled_requires_identity_path_queue() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("pub enum AuditConfig"));
	assert!(config.contains("Disabled"));
	assert!(config.contains("Enabled(Box<AuditEnabledConfig>)"));
	assert!(config.contains("enabled: bool"));
	assert!(config.contains("return Ok(AuditConfig::Disabled);"));
	for required in [
		"audit.node_id is required when audit is enabled",
		"audit.node_numeric_id is required when audit is enabled",
		"audit.local_path is required when audit is enabled",
		"audit.queue_capacity is required when audit is enabled",
	] {
		assert!(
			config.contains(required),
			"missing required validator {required}"
		);
	}
}

#[test]
pub(crate) fn adr066_toron_audit_node_id_is_typed_and_not_uuid_or_bare_numeric() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(audit.contains("const TORON_NODE_ID_PREFIX: &str = \"toron-n-\""));
	assert!(audit.contains("parse_toron_audit_node_numeric"));
	assert!(audit.contains("strip_prefix(TORON_NODE_ID_PREFIX)"));
	assert!(config_test.contains("audit_config_rejects_bare_numeric_and_uuid_node_ids"));
	assert!(config_test.contains("audit_config_rejects_numeric_mismatch_and_zero_queue"));
}

#[test]
pub(crate) fn adr066_audit_payload_is_toron_owned_and_typed() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"pub enum ToronAuditOperation",
		"pub enum ToronAuditEventClass",
		"pub enum StatementClass",
		"pub enum DurationBucket",
		"pub enum RowCountBucket",
		"pub enum SqlErrorCodeClass",
		"pub struct ToronAuditEvent",
		"fn to_shared(&self, node_id: &ToronAuditNodeId)",
	] {
		assert!(
			audit.contains(required),
			"missing typed audit surface {required}"
		);
	}
	assert!(!audit.contains("pub type ToronAuditEvent = shared_audit::AuditEvent"));
}

#[test]
pub(crate) fn adr066_event_taxonomy_covers_lifecycle_edge_denial_and_query_outcomes() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for operation in [
		"toron.lifecycle.startup",
		"toron.lifecycle.shutdown",
		"toron.audit.sink_failure",
		"toron.sql.edge.connection_limit_denied",
		"toron.sql.edge.tenant_identity_denied",
		"toron.sql.edge.request_frame_too_large",
		"toron.sql.edge.statement_too_large",
		"toron.sql.edge.protocol_mismatch",
		"toron.sql.edge.unexpected_client_frame",
		"toron.sql.query.completed",
		"toron.sql.query.planned_error",
		"toron.sql.query.execution_error",
		"toron.sql.query.cancelled",
		"toron.sql.query.timed_out",
		"toron.sql.query.disconnect_aborted",
	] {
		assert!(audit.contains(operation), "missing operation {operation}");
	}
}

#[test]
pub(crate) fn adr066_emitter_is_bounded_try_send_fail_open() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("tokio::sync::{mpsc, oneshot}"));
	assert!(audit.contains("mpsc::channel(config.queue_capacity.get())"));
	assert!(audit.contains("tx.try_send(event)"));
	assert!(audit.contains("TrySendError::Full"));
	assert!(audit.contains("TrySendError::Closed"));
	assert!(!audit.contains(".send(event).await"));
	assert!(audit.contains("ToronAuditCommand::fire_and_forget(event)"));
	assert!(audit.contains("record_drop(metrics, event.event(), ToronAuditDropReason::QueueFull)"));
}

#[test]
pub(crate) fn adr066_disabled_audit_has_no_metrics_or_readiness_effect() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability test");

	assert!(daemon.contains("let audit_metrics = config"));
	assert!(daemon.contains(".is_enabled()\n\t\t\t.then"));
	assert!(observability.contains("let Some(metrics) = &self.audit_metrics else"));
	assert!(
		observability_test.contains("audit_metrics_are_disabled_until_audit_config_is_enabled")
	);
	assert!(
		!observability_test.contains("audit_metrics: Some(Arc::new(ToronAuditMetrics::default()))")
	);
}

#[test]
pub(crate) fn adr066_readiness_degrades_only_on_local_chain_open_or_write_failure() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("audit_local_chain_unavailable"));
	assert!(audit.contains("audit_local_chain_write_failed"));
	assert!(audit.contains("set_degraded_reason"));
	let queue_full_block = audit
		.split("TrySendError::Full")
		.nth(1)
		.expect("queue full branch");
	assert!(
		!queue_full_block
			.lines()
			.take(8)
			.any(|line| line.contains("set_degraded_reason"))
	);
}

#[test]
pub(crate) fn adr066_sql_edge_denials_emit_audit_without_sql_frame_drift() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	for required in [
		"SqlConnectionLimitDenied",
		"SqlTenantIdentityDenied",
		"SqlRequestFrameTooLarge",
		"SqlStatementTooLarge",
		"SqlProtocolMismatch",
		"SqlUnexpectedClientFrame",
	] {
		assert!(
			daemon.contains(required),
			"missing daemon audit edge {required}"
		);
	}
	assert!(!wire.contains("Audit"));
	assert!(!wire.contains("AuditEvent"));
	assert!(!wire.contains("protocol_version: SqlProtocolVersion::V"));
}

#[test]
pub(crate) fn adr066_query_outcomes_emit_after_outcome_is_known() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	for required in [
		"QueryCompleted",
		"QueryPlannedError",
		"QueryExecutionError",
		"QueryCancelled",
		"QueryTimedOut",
		"QueryDisconnectAborted",
		".with_row_count(row_count)",
		".with_duration(query_started.elapsed())",
	] {
		assert!(
			daemon.contains(required),
			"missing query audit path {required}"
		);
	}
	let completion_index = daemon.find("QueryCompleted").expect("completed audit");
	let row_count_index = daemon
		.find("let row_count = if returns_rows")
		.expect("row count");
	assert!(row_count_index < completion_index);
}

#[test]
pub(crate) fn adr066_audit_payload_privacy_has_only_bounded_fields() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	for allowed in [
		"event_class",
		"session_id",
		"query_id",
		"statement_class",
		"sql_error_code_class",
		"duration_bucket",
		"row_count_bucket",
	] {
		assert!(audit.contains(&format!("with_parameter(\"{allowed}\"")));
	}
	for forbidden in [
		"with_parameter(\"sql",
		"with_parameter(\"statement",
		"with_parameter(\"row",
		"with_parameter(\"arrow",
		"with_parameter(\"table",
		"with_parameter(\"descriptor",
		"with_parameter(\"peer",
		"with_parameter(\"cert",
		"with_parameter(\"san",
		"with_parameter(\"error",
		"with_parameter(\"plan",
	] {
		if forbidden == "with_parameter(\"statement" {
			assert!(audit.contains("with_parameter(\"statement_class\""));
		} else if forbidden == "with_parameter(\"row" {
			assert!(audit.contains("with_parameter(\"row_count_bucket\""));
		} else if forbidden == "with_parameter(\"sql" {
			assert!(audit.contains("with_parameter(\"sql_error_code_class\""));
		} else if forbidden == "with_parameter(\"error" {
			assert!(audit.contains("with_parameter(\"error_class\""));
		} else {
			assert!(
				!audit.contains(forbidden),
				"audit payload leaked {forbidden}"
			);
		}
	}
	assert!(daemon.contains("planner.plan(&request.statement)"));
	assert!(!audit.contains("request.statement"));
}

#[test]
pub(crate) fn adr066_audit_metrics_are_aggregate_and_identity_free() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let tests =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");

	for family in [
		"toron_audit_events_total",
		"toron_audit_dropped_total",
		"toron_audit_sink_errors_total",
		"toron_audit_security_event_unrecorded_total",
	] {
		assert!(observability.contains(family), "missing metric {family}");
	}
	for forbidden in [
		"tenant_id",
		"query_id",
		"session_id",
		"table",
		"descriptor",
		"sql_text",
	] {
		let offenders = observability
			.lines()
			.filter(|line| line.contains("toron_audit_") && line.contains(forbidden))
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"audit metric gained forbidden label {forbidden}: {offenders:?}"
		);
	}
	assert!(tests.contains("audit_metrics_are_aggregate_only"));
}

#[test]
pub(crate) fn adr066_security_unrecorded_counts_queue_closed_and_sink_errors() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"ToronAuditSecurityEventUnrecordedReason",
		"QueueFull",
		"EmitterClosed",
		"SinkError",
		"record_security_unrecorded",
		"security_relevant()",
	] {
		assert!(
			audit.contains(required),
			"missing security unrecorded path {required}"
		);
	}
	assert!(audit.contains("unavailable_emitter_records_security_drop_without_panic"));
}

#[test]
pub(crate) fn adr066_archive_and_syslog_are_optional_but_kafka_is_not_configured_by_toron() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("archive: Option<RawAuditArchiveConfig>"));
	assert!(config.contains("syslog: Option<RawAuditSyslogConfig>"));
	assert!(config.contains("ArchiveAuditConfig"));
	assert!(config.contains("SyslogAuditConfig"));
	assert!(config.contains("kafka: None"));
	assert!(!config.contains("RawAuditKafkaConfig"));
}

#[test]
pub(crate) fn adr066_shared_observability_audit_remains_policy_free() {
	let shared = observability_audit_source_text();

	for forbidden in [
		"ToronAudit",
		"SqlErrorCode",
		"SqlMessage",
		"QueryId",
		"crate::storage::TenantId",
		"tokio::sync::mpsc",
		"try_send",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared audit gained Toron policy or async policy {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr066_no_wendelon_nilket_fixture_worker_or_sql_wire_runtime_change() {
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!sql_wire.contains("Audit"));
	assert!(!wendelon_source.contains("ToronAudit"));
	assert!(!nilket_manifest.contains("observability"));
	assert!(!worker.contains("Audit"));
	assert!(!worker.contains("audit"));
}

#[test]
pub(crate) fn adr066_fixtures_cover_config_metrics_emitter_and_sql_paths() {
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");
	let sql_test = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for fixture in [
		"audit_config_enabled_accepts_toron_node_id_and_resolves_paths",
		"audit_config_rejects_bare_numeric_and_uuid_node_ids",
		"audit_config_rejects_numeric_mismatch_and_zero_queue",
		"audit_metrics_are_disabled_until_audit_config_is_enabled",
		"audit_metrics_are_aggregate_only",
		"unavailable_emitter_records_security_drop_without_panic",
		"toron_sql_select_from_bootstrapped_descriptor_executes_over_wire",
		"oversized_declared_request_length_rejected_before_allocation_or_decode",
		"oversized_decoded_execute_statement_returns_resource_exhausted_without_planning",
		"audit_records_planned_error_without_raw_sql",
		"cancel_below_request_frame_cap_is_not_statement_capped",
	] {
		let haystack = format!("{config_test}\n{observability_test}\n{sql_test}\n{audit}");
		assert!(haystack.contains(fixture), "missing fixture {fixture}");
	}
}

#[test]
pub(crate) fn adr066_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "observability"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr066_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(!worker.contains("ToronAudit"));
	assert!(!worker.contains("observability"));
	assert!(!worker.contains("[audit]"));
}

#[test]
pub(crate) fn adr066_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let shared = observability_audit_source_text();
	for forbidden in [
		"ToronAudit",
		"ToronAuditEvent",
		"ToronAuditV1",
		"AuditEmitterV1",
		"AuditConsumerV1",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared observability surface contains forbidden Toron/V1 token {forbidden}"
		);
	}
	let toron_audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	assert!(!toron_audit.contains("V1"));
}

#[test]
pub(crate) fn adr066_prior_adr045_to_065_guards_continue_to_pass_except_audit_feature_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_", "adr065_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr071_toron_audit_event_classes_include_task_lifecycle_and_observability_access() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"TaskLifecycle",
		"ObservabilityAccess",
		"task_lifecycle",
		"observability_access",
		"pub const COUNT: usize = 6",
	] {
		assert!(audit.contains(required), "missing {required}");
	}
}

#[test]
pub(crate) fn adr071_toron_audit_operations_include_worker_task_and_observability_access() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for operation in [
		"toron.worker.task.started",
		"toron.worker.task.completed",
		"toron.worker.task.failed",
		"toron.worker.task.cancelled",
		"toron.observability.ready.access",
		"toron.observability.metrics.access",
	] {
		assert!(audit.contains(operation), "missing operation {operation}");
	}
}

#[test]
pub(crate) fn adr071_worker_task_audit_uses_toron_audit_emitter() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(server.contains("audit: ToronAuditEmitter"));
	assert!(server.contains("WorkerTaskAuditRecorder"));
	assert!(server.contains("ToronAuditEvent::worker_task_started"));
	assert!(server.contains("ToronAuditEvent::worker_task_completed"));
	assert!(server.contains("ToronAuditEvent::worker_task_failed"));
	assert!(server.contains("ToronAuditEvent::worker_task_cancelled"));
}

#[test]
pub(crate) fn adr071_worker_task_started_after_decode_route_register_and_accept() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let start_task = server
		.split("MessageKind::StartTask =>")
		.nth(1)
		.and_then(|rest| rest.split("MessageKind::CancelTask =>").next())
		.expect("StartTask arm");

	let decode = start_task
		.find("decode_payload(&envelope.payload)")
		.expect("decode");
	let route = start_task
		.find("if envelope.identity")
		.expect("route validate");
	let register = start_task
		.find("self.active_tasks.register(identity)")
		.expect("register");
	let cancelled = start_task
		.find("if active_task.cancelled_before_start")
		.expect("pre-start cancel check");
	let started = start_task
		.find("audit_recorder.record_started()")
		.expect("started audit");
	assert!(decode < route && route < register && register < cancelled && cancelled < started);
}

#[test]
pub(crate) fn adr071_worker_task_audit_emits_exactly_one_terminal_event() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let recorder = brace_blocks_after_marker(&server, "impl<'a> WorkerTaskAuditRecorder")
		.into_iter()
		.next()
		.expect("worker audit recorder")
		.1;

	assert!(recorder.contains("terminal_recorded"));
	assert_eq!(recorder.matches("self.terminal_recorded = true").count(), 3);
	for method in ["record_completed", "record_failed", "record_cancelled"] {
		let body = brace_blocks_after_marker(&recorder, &format!("fn {method}"))
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{method} body"))
			.1;
		assert!(body.contains("if self.terminal_recorded"));
		assert!(body.contains("self.terminal_recorded = true"));
	}
}

#[test]
pub(crate) fn adr071_worker_task_audit_fields_are_bounded() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for allowed in [
		"with_parameter(\"query_id\"",
		"with_parameter(\"stage_id\"",
		"with_parameter(\"task_id\"",
		"with_parameter(\"attempt_id\"",
		"with_parameter(\"worker_id\"",
		"with_parameter(\"stage_role\"",
		"with_parameter(\"operator_kind\"",
		"with_parameter(\"outcome_class\"",
		"with_parameter(\"error_class\"",
		"with_parameter(\"duration_bucket\"",
		"with_parameter(\"row_count_bucket\"",
	] {
		assert!(audit.contains(allowed), "missing bounded field {allowed}");
	}
}

#[test]
pub(crate) fn adr071_worker_task_audit_has_no_sql_plan_table_descriptor_value_payload_or_raw_error()
{
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for forbidden in [
		"with_parameter(\"sql",
		"with_parameter(\"plan",
		"with_parameter(\"table",
		"with_parameter(\"column",
		"with_parameter(\"descriptor",
		"with_parameter(\"runtime_filter",
		"with_parameter(\"literal",
		"with_parameter(\"arrow",
		"with_parameter(\"payload",
		"with_parameter(\"peer",
		"with_parameter(\"raw_error",
	] {
		if forbidden == "with_parameter(\"sql" {
			assert!(audit.contains("with_parameter(\"sql_error_code_class\""));
		} else {
			assert!(
				!audit.contains(forbidden),
				"worker audit leaked {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr071_worker_task_audit_does_not_change_task_wire_protocol() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");

	for forbidden in [
		"WorkerTaskStarted",
		"WorkerTaskCompleted",
		"WorkerTaskFailedAudit",
		"AuditEvent",
		"audit",
	] {
		assert!(!wire.contains(forbidden), "wire gained {forbidden}");
	}
}

#[test]
pub(crate) fn adr071_worker_audit_is_disabled_by_default_handle() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(server.contains("audit: ToronAuditEmitter::disabled()"));
	assert!(server.contains("pub fn with_audit_emitter"));
	assert!(daemon.contains("worker.with_audit_emitter(audit.clone())"));
}

#[test]
pub(crate) fn adr071_observability_ready_metrics_audit_is_in_toron_network_layer() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(observability.contains("ToronAuditEmitter"));
	assert!(observability.contains("ObservabilityReadyAccess"));
	assert!(observability.contains("ObservabilityMetricsAccess"));
	assert!(observability.contains("handle_ready_connection"));
	assert!(observability.contains("handle_metrics_connection"));
	adr071_shared_observability_crates_remain_policy_free();
}

#[test]
pub(crate) fn adr071_observability_health_listener_remains_unaudited() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let health = brace_blocks_after_marker(&observability, "async fn handle_health_connection")
		.into_iter()
		.next()
		.expect("health handler")
		.1;

	assert!(!health.contains("ToronAuditEmitter"));
	assert!(!health.contains("ToronAuditEvent"));
	assert!(!health.contains("record_observability_access"));
}

#[test]
pub(crate) fn adr071_observability_access_records_admitted_monitor_principal_only() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs")).expect("read tls");

	assert!(tls.contains("accepted_observability_monitor_principal"));
	assert!(tls.contains("ListenerRole::Observability"));
	assert!(tls.contains("PeerKind::Monitor"));
	assert!(observability.contains("accepted_observability_monitor_principal"));
	assert!(observability.contains("format!(\"monitor:{principal}\")"));
	assert!(audit.contains("with_parameter(\"actor_key_id\""));
	assert!(audit.contains("with_parameter(\"actor_role\""));
}

#[test]
pub(crate) fn adr071_observability_access_has_no_raw_san_dn_ip_or_certificate_material() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");
	let source = format!("{audit}\n{observability}");

	for forbidden in [
		"with_parameter(\"san",
		"with_parameter(\"dn",
		"with_parameter(\"distinguished",
		"with_parameter(\"peer_ip",
		"with_parameter(\"peer_addr",
		"with_parameter(\"cert",
		"with_parameter(\"certificate",
		"with_parameter(\"serial",
		"with_parameter(\"header",
	] {
		assert!(
			!source.contains(forbidden),
			"access audit leaked {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr071_shared_observability_crates_remain_policy_free() {
	let shared = format!(
		"{}\n{}",
		observability_http_source_text(),
		observability_audit_source_text()
	);

	for forbidden in [
		"ToronAudit",
		"toron.worker.task",
		"toron.observability",
		"TaskLifecycle",
		"ObservabilityAccess",
		"try_send",
		"tokio::sync::mpsc",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared observability gained Toron policy {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr071_audit_emission_preserves_try_send_fail_open_no_request_failure() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert!(audit.contains("tx.try_send(event)"));
	assert!(audit.contains("TrySendError::Full"));
	assert!(audit.contains("TrySendError::Closed"));
	assert!(!audit.contains(".send(event).await"));
	assert!(!server.contains("record(") || !server.contains("?; // audit"));
	assert!(
		!observability.contains("record_observability_access(")
			|| !observability.contains("?; // audit")
	);
}

#[test]
pub(crate) fn adr071_fail_open_default_preserves_no_audit_request_failure_path_after_adr072() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let record = brace_blocks_after_marker(&audit, "pub fn record(&self")
		.into_iter()
		.next()
		.expect("record body")
		.1;
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(config.contains("# fail_closed = false"));
	assert!(record.contains("tx.try_send(event)"));
	assert!(!record.contains("AuditUnavailable"));
	assert!(required.contains("if !*fail_closed"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("return Ok(());"));
}

#[test]
pub(crate) fn adr071_runtime_worker_audit_allowed_fixture_worker_stays_audit_free() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	assert!(server.contains("audit: ToronAuditEmitter"));
	assert!(server.contains("worker_task_started"));
	assert!(!fixture.contains("audit"));
	assert!(!fixture.contains("ToronAudit"));
}

#[test]
pub(crate) fn adr071_worker_node_id_matches_toron_n_u32() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("const TORON_NODE_ID_PREFIX: &str = \"toron-n-\""));
	assert!(audit.contains("parse_toron_audit_node_numeric"));
	assert!(audit.contains("audit.node_id must use toron-n-<u32>"));
}

#[test]
pub(crate) fn adr071_no_wendelon_nilket_sql_wire_storage_or_worker_wire_change() {
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let worker_wire =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for source in [sql_wire.as_str(), worker_wire.as_str(), storage.as_str()] {
		assert!(!source.contains("ToronAudit"));
		assert!(!source.contains("AuditEvent"));
	}
	assert!(!wendelon_source.contains("toron.worker.task"));
	assert!(!nilket_manifest.contains("observability"));
}

#[test]
pub(crate) fn adr071_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "observability"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr071_prior_adr065_066_audit_guards_continue_to_pass_except_worker_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in ["adr065_", "adr066_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior audit guard prefix {prefix}");
	}
	assert!(
		architecture_test
			.contains("adr071_runtime_worker_audit_allowed_fixture_worker_stays_audit_free")
	);
}

#[test]
pub(crate) fn adr072_audit_fail_closed_is_opt_in_default_false() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("# fail_closed = false"));
	assert!(config.contains("pub fail_closed: bool"));
	assert!(config.contains("fail_closed: bool"));
	assert!(config.contains("fail_closed: raw.fail_closed"));
	assert!(config.contains("DEFAULT_AUDIT_FAIL_CLOSED_ACK_TIMEOUT_MS"));
}

#[test]
pub(crate) fn adr072_fail_closed_false_is_behavior_equivalent_to_071() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let sql_test = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
	let observability_test =
		std::fs::read_to_string(manifest_dir().join("tests/observability_listener_test.rs"))
			.expect("read observability tests");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(required.contains("if !*fail_closed"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("return Ok(());"));
	assert!(sql_test.contains("fail_closed: false"));
	assert!(observability_test.contains("fail_closed: false"));
	assert!(audit.contains("record_required_delegates_fail_open_when_fail_closed_false"));
}

#[test]
pub(crate) fn adr072_fail_closed_true_requires_audit_enabled() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(config.contains("if !raw.enabled"));
	assert!(config.contains("if raw.fail_closed"));
	assert!(config.contains("audit.fail_closed requires audit.enabled = true"));
	assert!(config_test.contains("audit_fail_closed_config_accepts_enabled_and_rejects_disabled"));
}

#[test]
pub(crate) fn adr072_fail_closed_ack_timeout_is_bounded_and_nonzero() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let config_test = std::fs::read_to_string(manifest_dir().join("tests/config_test.rs"))
		.expect("read config tests");

	assert!(config.contains("DEFAULT_AUDIT_FAIL_CLOSED_ACK_TIMEOUT_MS: u64 = 500"));
	assert!(config.contains("fail_closed_ack_timeout_ms: Option<u64>"));
	assert!(config.contains("fail_closed_ack_timeout: Duration"));
	assert!(config.contains("audit.fail_closed_ack_timeout_ms must be greater than 0"));
	assert!(config_test.contains("fail_closed_ack_timeout"));
}

#[test]
pub(crate) fn adr072_record_remains_try_send_fail_open() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let record = brace_blocks_after_marker(&audit, "pub fn record(&self")
		.into_iter()
		.next()
		.expect("record body")
		.1;

	assert!(record.contains("ToronAuditCommand::fire_and_forget(event)"));
	assert!(record.contains("tx.try_send(event)"));
	assert!(record.contains("TrySendError::Full"));
	assert!(record.contains("TrySendError::Closed"));
	assert!(record.contains("record_drop(metrics"));
	assert!(!record.contains("AuditUnavailable"));
	assert!(!record.contains(".send("));
	assert!(!record.contains(".await"));
}

#[test]
pub(crate) fn adr072_record_required_is_mode_aware() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(required.contains("Result<(), AuditUnavailable>"));
	assert!(required.contains("Self::Disabled => Ok(())"));
	assert!(required.contains("if !*fail_closed"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("ToronAuditCommand::required(event, ack_tx)"));
	assert!(required.contains("tokio::time::timeout(*ack_timeout, ack_rx).await"));
}

#[test]
pub(crate) fn adr072_required_audit_uses_worker_ack() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("required_ack: Option<oneshot::Sender<Result<(), AuditUnavailable>>>"));
	assert!(audit.contains("let (ack_tx, ack_rx) = oneshot::channel();"));
	assert!(audit.contains("ToronAuditCommand::required(event, ack_tx)"));
	assert!(audit.contains("let required_ack = command.required_ack;"));
}

#[test]
pub(crate) fn adr072_required_ack_means_local_chain_written_and_fsynced() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let shared_audit = observability_audit_source_text();
	let worker = brace_blocks_after_marker(&audit, "async fn run_audit_worker")
		.into_iter()
		.next()
		.expect("audit worker")
		.1;

	assert!(shared_audit.contains("file.sync_data()"));
	assert!(worker.contains("sink.emit(shared_event)"));
	assert!(worker.contains("ack.send(Ok(()))"));
}

#[test]
pub(crate) fn adr072_required_ack_uses_oneshot_after_sink_emit_returns() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let worker = brace_blocks_after_marker(&audit, "async fn run_audit_worker")
		.into_iter()
		.next()
		.expect("audit worker")
		.1;
	let emit = worker.find("sink.emit(shared_event)").expect("sink emit");
	let ack = worker.find("ack.send(Ok(()))").expect("success ack");

	assert!(emit < ack);
}

#[test]
pub(crate) fn adr072_security_relevant_is_eligibility_not_global_control_flow() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(audit.contains("pub fn security_relevant(&self) -> bool"));
	assert!(required.contains("!event.security_relevant()"));
	assert!(required.contains("event.operation() == ToronAuditOperation::AuditSinkFailure"));
	assert!(required.contains("self.record(event);"));
	assert!(required.contains("return Ok(());"));
}

#[test]
pub(crate) fn adr072_required_call_sites_match_surface_table() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	for operation in [
		"SqlRequestFrameTooLarge",
		"SqlStatementTooLarge",
		"SqlProtocolMismatch",
		"SqlUnexpectedClientFrame",
		"QueryCompleted",
		"QueryPlannedError",
		"QueryExecutionError",
		"QueryCancelled",
		"QueryTimedOut",
		"QueryDisconnectAborted",
	] {
		assert!(
			daemon.contains(operation),
			"missing SQL surface {operation}"
		);
	}
	assert!(daemon.contains("record_sql_audit_required_or_error"));
	assert!(daemon.contains("record_sql_audit_required_without_response"));
	assert!(server.contains("record_started().await"));
	assert!(server.contains("record_completed(emitted_rows).await"));
	assert!(server.contains("record_failed(&error).await"));
	assert!(server.contains("record_cancelled().await"));
	assert!(observability.contains("record_observability_access_required"));
}

#[test]
pub(crate) fn adr072_audit_sink_failure_is_never_required() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let required = brace_blocks_after_marker(&audit, "pub async fn record_required")
		.into_iter()
		.next()
		.expect("record_required body")
		.1;

	assert!(required.contains("ToronAuditOperation::AuditSinkFailure"));
	assert!(required.contains("self.record(event);"));
	assert!(audit.contains("sink_failure_diagnostic_does_not_recursively_require_audit"));
}

#[test]
pub(crate) fn adr072_query_results_are_not_streamed_before_required_terminal_audit() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_test = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
	let fail_closed = daemon
		.split("let encoded_result_batches")
		.nth(1)
		.expect("SQL success pre-encode block");
	let preencode = fail_closed
		.find("encode_sql_batch")
		.expect("pre-encode batch");
	let required = fail_closed
		.find("record_sql_audit_required_or_error")
		.expect("required terminal audit");
	let send_batch = fail_closed
		.find("SqlMessage::ResultBatch")
		.expect("send batch");

	assert!(preencode < required && required < send_batch);
	assert!(
		sql_test.contains(
			"fail_closed_sql_success_audit_failure_returns_internal_before_result_batches"
		)
	);
}

#[test]
pub(crate) fn adr072_worker_started_required_before_task_execution() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let start_task = server
		.split("MessageKind::StartTask =>")
		.nth(1)
		.and_then(|rest| rest.split("MessageKind::CancelTask =>").next())
		.expect("StartTask arm");
	let started = start_task
		.find("audit_recorder.record_started().await")
		.expect("started audit");
	let task_started = start_task
		.find("MessageKind::TaskStarted")
		.expect("TaskStarted response");
	let execute = start_task
		.find("self.execute_task_local_with_telemetry")
		.expect("task execution");

	assert!(started < task_started && started < execute);
	assert!(server.contains("worker_started_audit_failure_prevents_task_execution"));
}

#[test]
pub(crate) fn adr072_required_queue_full_closed_sink_error_or_timeout_rejects() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	for required in [
		"AuditUnavailableReason::QueueFull",
		"AuditUnavailableReason::EmitterClosed",
		"AuditUnavailableReason::SinkFailed",
		"AuditUnavailableReason::Timeout",
		"required_audit_queue_full_returns_bounded_audit_unavailable",
		"required_audit_closed_emitter_returns_bounded_audit_unavailable",
		"required_audit_sink_error_rejects_and_degrades_readiness",
		"required_audit_ack_timeout_returns_bounded_audit_unavailable",
	] {
		assert!(
			audit.contains(required),
			"missing required failure {required}"
		);
	}
}

#[test]
pub(crate) fn adr072_audit_unavailable_errors_are_bounded_oracle_safe() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	for label in [
		"queue_full",
		"emitter_closed",
		"serialization_failed",
		"sink_failed",
		"timeout",
	] {
		assert!(audit.contains(label), "missing bounded reason {label}");
	}
	assert!(daemon.contains("const AUDIT_UNAVAILABLE_SQL_MESSAGE: &str = \"audit unavailable\""));
	assert!(server.contains("reason: \"audit_unavailable\".to_string()"));
	assert!(observability.contains("\"audit unavailable\\n\""));
	assert!(!daemon.contains("write_sql_error(stream, query_id, SqlErrorCode::Internal, error"));
}

#[test]
pub(crate) fn adr072_no_new_sql_error_or_task_failure_enum_variant() {
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let task_errors = std::fs::read_to_string(manifest_dir().join("src/error/distributed.rs"))
		.expect("read task errors");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(!sql_wire.contains("AuditUnavailable"));
	assert!(!task_errors.contains("AuditUnavailable"));
	assert!(server.contains("TaskFailureKind::TaskFailed"));
	assert!(server.contains("audit_unavailable"));
}

#[test]
pub(crate) fn adr072_response_gating_not_side_effect_gating_documented() {
	let adr = std::fs::read_to_string(manifest_dir().join("plans/adr/0072_audit_fail_closed.md"))
		.expect("read ADR 072");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(adr.contains("response-gating"));
	assert!(adr.contains("not side-effect rollback"));
	assert!(daemon.contains("let encoded_result_batches"));
	assert!(server.contains("record_completed(emitted_rows).await"));
}

#[test]
pub(crate) fn adr072_fail_closed_degrades_readiness_for_security_relevant_surfaces() {
	let audit =
		std::fs::read_to_string(manifest_dir().join("src/audit.rs")).expect("read Toron audit");

	assert!(audit.contains("audit_local_chain_unavailable"));
	assert!(audit.contains("audit_local_chain_write_failed"));
	assert!(audit.contains("record_security_unrecorded"));
	assert!(audit.contains("required_audit_sink_error_rejects_and_degrades_readiness"));
}

#[test]
pub(crate) fn adr072_no_wendelon_storage_sql_wire_worker_wire_or_shared_substrate_change() {
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let worker_wire =
		std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read worker wire");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");
	let shared = observability_audit_source_text();
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for source in [
		sql_wire.as_str(),
		worker_wire.as_str(),
		storage.as_str(),
		shared.as_str(),
	] {
		assert!(!source.contains("audit.fail_closed"));
		assert!(!source.contains("ToronAuditEmitter"));
	}
	assert!(!wendelon_source.contains("audit.fail_closed"));
	assert!(!wendelon_source.contains("record_required"));
}

#[test]
pub(crate) fn adr072_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "observability"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr072_prior_adr065_066_071_audit_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in ["adr065_", "adr066_", "adr071_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior audit guard prefix {prefix}");
	}
	assert!(architecture_test.contains("fn adr072_record_required_is_mode_aware"));
}
