#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};

#[test]
pub(crate) fn adr067_observability_telemetry_module_exists_and_is_feature_gated() {
	let lib = std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/lib.rs"))
		.expect("read observability lib");
	assert!(lib.contains("#[cfg(feature = \"telemetry\")]"));
	assert!(lib.contains("pub mod telemetry;"));
	assert!(
		workspace_root_dir()
			.join("crates/observability/src/telemetry/mod.rs")
			.exists()
	);
}

#[test]
pub(crate) fn adr067_telemetry_feature_is_independent_of_http_and_audit() {
	let manifest = observability_manifest();
	let source = observability_telemetry_source_text();
	assert!(manifest.contains("telemetry = []"));
	assert!(manifest.contains(r#"http = ["dep:tokio"]"#));
	assert!(!manifest.contains(r#"telemetry = ["http""#));
	assert!(!manifest.contains(r#"telemetry = ["audit""#));
	for forbidden in [
		"observability::http",
		"observability::audit",
		"crate::http",
		"crate::audit",
		"super::http",
		"super::audit",
	] {
		assert!(
			!source.contains(forbidden),
			"telemetry references {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr067_otlp_http_and_grpc_features_are_non_default() {
	let manifest = observability_manifest();
	assert!(manifest.contains("default = []"));
	assert!(manifest.contains("otlp-http = ["));
	assert!(manifest.contains("otlp-grpc = ["));
	for required in [
		"dep:reqwest",
		"dep:h2",
		"dep:http",
		"dep:tokio-rustls",
		"dep:tls-pki",
	] {
		assert!(
			manifest.contains(required),
			"missing OTLP feature dep {required}"
		);
	}
}

#[test]
pub(crate) fn adr067_wendelon_otlp_features_delegate_to_observability() {
	let manifest = std::fs::read_to_string(wendelon_manifest_dir().join("Cargo.toml"))
		.expect("read Wendelon manifest");
	assert!(manifest.contains(r#"otlp-http = ["observability/otlp-http"]"#));
	assert!(manifest.contains(r#"otlp-grpc = ["observability/otlp-grpc"]"#));
	assert!(manifest.contains(r#"features = ["http", "audit", "telemetry"]"#));
	assert!(!manifest.contains(r#"otlp-http = []"#));
	assert!(!manifest.contains(r#""dep:h2""#));
	assert!(!manifest.contains(r#""dep:http""#));
}

#[test]
pub(crate) fn adr067_wendelon_build_dispatch_uses_observability_telemetry() {
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	assert!(telemetry.contains("use observability::telemetry as shared_telemetry;"));
	assert!(telemetry.contains("shared_telemetry::OtlpTraceLayer::new"));
	assert!(telemetry.contains("fn build_dispatch("));
	assert!(!telemetry.contains("mod otlp"));
	assert!(!telemetry.contains("fn encode_export_trace_service_request"));
}

#[test]
pub(crate) fn adr067_wendelon_otlp_behavior_fixtures_remain_present() {
	for fixture in [
		"tests/otlp_http_export.rs",
		"tests/otlp_grpc_export.rs",
		"tests/config_loader.rs",
	] {
		assert!(
			wendelon_manifest_dir().join(fixture).exists(),
			"missing Wendelon fixture {fixture}"
		);
	}
	let http = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_http_export.rs"))
		.expect("read OTLP HTTP test");
	let grpc = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_grpc_export.rs"))
		.expect("read OTLP gRPC test");
	assert!(http.contains("telemetry::build_dispatch(Some(&trace_cfg), 7)"));
	assert!(grpc.contains("telemetry::build_dispatch(Some(&trace_cfg), 8)"));
	assert!(grpc.contains("telemetry::build_dispatch(Some(&trace_cfg), 9)"));
}

#[test]
pub(crate) fn adr067_shared_telemetry_has_no_wendelon_or_toron_imports() {
	let source = observability_telemetry_source_text();
	for forbidden in [
		"wendelon",
		"Wendelon",
		"toron",
		"Toron",
		"crate::metrics",
		"crate::network",
		"crate::storage",
	] {
		assert!(
			!source.contains(forbidden),
			"shared telemetry contains ring token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr067_wendelon_policy_literals_remain_ring_owned() {
	let shared = observability_telemetry_source_text();
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	for literal in [
		"WENDELON_LOG",
		"wendelon.traces",
		"service.name",
		"service.version",
		"wendelon.node_id",
	] {
		assert!(
			telemetry.contains(literal),
			"Wendelon missing policy literal {literal}"
		);
		assert!(
			!shared.contains(literal),
			"shared telemetry owns Wendelon policy literal {literal}"
		);
	}
	assert!(telemetry.contains("TelemetryProfile::new"));
}

#[test]
pub(crate) fn adr067_wendelon_dropped_span_metric_name_remains_ring_owned() {
	let shared = observability_telemetry_source_text();
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	assert!(telemetry.contains("otlp_spans_dropped_total"));
	assert!(!shared.contains("otlp_spans_dropped_total"));
	assert!(telemetry.contains("WendelonDroppedSpanObserver"));
}

#[test]
pub(crate) fn adr067_w3c_trace_context_validation_is_shared() {
	let shared = observability_telemetry_source_text();
	let compute =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
			.expect("read Wendelon compute integration");
	assert!(shared.contains("pub struct TraceContext"));
	assert!(shared.contains("pub fn validate_traceparent"));
	assert!(shared.contains("pub fn validate_tracestate"));
	assert!(compute.contains("use observability::telemetry as shared_telemetry;"));
	assert!(compute.contains("shared_telemetry::TraceContext"));
	assert!(compute.contains("shared_telemetry::validate_traceparent(traceparent)"));
}

#[test]
pub(crate) fn adr067_compute_trace_context_v1_is_preserved_as_compatibility_surface() {
	let compute =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
			.expect("read Wendelon compute integration");
	assert!(compute.contains("pub struct ComputeTraceContext"));
	assert!(compute.contains("impl ComputeTraceContext"));
	assert!(compute.contains("shared_telemetry::TraceContext"));
	assert!(
		compute.contains("pub const TRACEPARENT_LEN: usize = shared_telemetry::TRACEPARENT_LEN;")
	);
}

#[test]
pub(crate) fn adr067_queryid_to_traceid_mapping_is_absent() {
	let shared = observability_telemetry_source_text();
	for forbidden in [
		"QueryId",
		"query_id",
		"tenant_id",
		"session_id",
		"trace_id_from_query",
		"TraceId::from",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared telemetry gained identity mapping token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr067_otlp_queue_remains_bounded_try_send_fail_open() {
	let shared = observability_telemetry_source_text();
	for required in [
		"mpsc::channel(cfg.max_queue_size)",
		"tx.try_send(ExportCommand::Span(completed))",
		"TrySendError::Full",
		"TrySendError::Closed",
		"record_dropped_span",
	] {
		assert!(
			shared.contains(required),
			"missing bounded queue token {required}"
		);
	}
	assert!(!shared.contains(".send(ExportCommand::Span"));
}

#[test]
pub(crate) fn adr067_otlp_shutdown_handle_remains_explicit() {
	let shared = observability_telemetry_source_text();
	let telemetry =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/telemetry.rs"))
			.expect("read Wendelon telemetry");
	assert!(shared.contains("pub struct OtlpShutdownHandle"));
	assert!(shared.contains("pub async fn shutdown(self)"));
	assert!(telemetry.contains("pub struct TelemetryHandle"));
	assert!(telemetry.contains("shutdown: Option<shared_telemetry::OtlpShutdownHandle>"));
}

#[test]
pub(crate) fn adr067_http_and_grpc_export_paths_remain_feature_gated() {
	let shared = observability_telemetry_source_text();
	for required in [
		"#[cfg(feature = \"otlp-http\")]",
		"#[cfg(feature = \"otlp-grpc\")]",
		"const OTLP_HTTP_TRACES_PATH: &str = \"/v1/traces\";",
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export",
		"application/x-protobuf",
		"application/grpc",
		"te\", \"trailers",
	] {
		assert!(
			shared.contains(required),
			"missing OTLP protocol token {required}"
		);
	}
}

#[test]
pub(crate) fn adr067_no_new_opentelemetry_sdk_or_tonic_dependency() {
	let manifests = workspace_cargo_tomls()
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read manifest"))
		.collect::<Vec<_>>()
		.join("\n");
	for forbidden in [
		"opentelemetry-otlp",
		"opentelemetry_sdk",
		"opentelemetry =",
		"tonic",
		"prost =",
	] {
		assert!(
			!manifests.contains(forbidden),
			"manifest gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr067_metrics_http_exporter_is_not_moved_as_otlp() {
	let exporter = std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/exporter.rs"))
		.expect("read Wendelon metrics exporter");
	let shared = observability_telemetry_source_text();
	assert!(exporter.contains("METRICS_PATH"));
	assert!(exporter.contains("\"/metrics\""));
	assert!(exporter.contains("MetricsExporterHandle"));
	assert!(!shared.contains("METRICS_PATH"));
	assert!(!shared.contains("MetricsExporterHandle"));
}

#[test]
pub(crate) fn adr067_no_toron_telemetry_config_or_exporter_yet() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	let source = source_files(&manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Toron source"))
		.collect::<Vec<_>>()
		.join("\n");
	assert!(source.contains("pub struct TaskTelemetry"));
	assert!(manifest.contains(r#"telemetry = ["observability/telemetry"]"#));
	assert!(config.contains("[observability.telemetry]"));
	assert!(config.contains("RawTelemetryConfig"));
	assert!(source.contains("OtlpTraceLayer"));
}

#[test]
pub(crate) fn adr067_no_audit_http_sql_wire_worker_or_storage_behavior_change() {
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read Toron storage");
	assert!(!sql_wire.contains("Otlp"));
	assert!(!sql_wire.contains("TraceExport"));
	assert!(!worker.contains("Otlp"));
	assert!(!worker.contains("TraceExport"));
	assert!(!storage.contains("Otlp"));
	assert!(!storage.contains("TraceExport"));
	adr065_observability_http_does_not_reference_audit();
	adr065_observability_audit_does_not_reference_http();
}

#[test]
pub(crate) fn adr067_prior_adr065_066_observability_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();
	for prefix in ["adr065_", "adr066_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(
			count > 0,
			"missing prior observability guard prefix {prefix}"
		);
	}
}

#[test]
pub(crate) fn adr067_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	for forbidden in [
		"tokio",
		"quinn",
		"rustls",
		"observability",
		"tls-pki",
		"reqwest",
		"h2",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr067_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");
	assert!(!worker.contains("observability"));
	assert!(!worker.contains("Otlp"));
	assert!(!worker.contains("TraceExport"));
	assert!(!worker.contains("traceparent"));
}

#[test]
pub(crate) fn adr067_new_shared_surface_is_brand_clean_and_has_no_v1_suffix() {
	let shared = observability_telemetry_source_text();
	for forbidden in [
		"Wendelon",
		"Toron",
		"Nilket",
		"TraceContextV1",
		"TelemetryProfileV1",
		"OtlpTraceLayerV1",
	] {
		assert!(
			!shared.contains(forbidden),
			"shared telemetry surface contains forbidden brand/V1 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr067_wendelon_v1_trace_context_suffix_is_compatibility_scoped() {
	let shared = observability_telemetry_source_text();
	let compute =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
			.expect("read Wendelon compute integration");
	assert!(!shared.contains("V1"));
	assert!(compute.contains("pub struct ComputeTraceContext"));
	assert!(compute.contains("shared_telemetry::TraceContext"));
}

#[test]
pub(crate) fn adr067_prior_adr045_to_066_guards_continue_to_pass_except_otlp_feature_retarget() {
	let architecture_test = architecture_corpus_source();
	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_", "adr065_", "adr066_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr067_wendelon_otlp_features_delegate_to_observability"));
}

#[test]
pub(crate) fn adr068_toron_telemetry_features_are_default_disabled() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	assert!(manifest.contains("[features]"));
	assert!(manifest.contains("default = []"));
	assert!(manifest.contains(r#"telemetry = ["observability/telemetry"]"#));
	assert!(manifest.contains(r#"otlp-http = ["telemetry", "observability/otlp-http"]"#));
	assert!(manifest.contains(r#"otlp-grpc = ["telemetry", "observability/otlp-grpc"]"#));
}

#[test]
pub(crate) fn adr068_toron_otlp_features_delegate_to_observability() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	assert!(manifest.contains(r#"observability = { path = "../observability", default-features = false, features = ["http", "audit"] }"#));
	assert!(manifest.contains(r#"otlp-http = ["telemetry", "observability/otlp-http"]"#));
	assert!(manifest.contains(r#"otlp-grpc = ["telemetry", "observability/otlp-grpc"]"#));
	assert!(!manifest.contains(r#""dep:h2""#));
	assert!(!manifest.contains(r#""dep:http""#));
}

#[test]
pub(crate) fn adr068_observability_telemetry_config_is_default_absent_noop() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	assert!(config.contains("pub telemetry: Option<TelemetryConfig>"));
	assert!(config.contains("telemetry: None"));
	assert!(config.contains("# [observability.telemetry]"));
	assert!(daemon.contains("if config.observability.telemetry.is_none()"));
}

#[test]
pub(crate) fn adr068_telemetry_config_uses_toron_raw_config_surface() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read Toron config");
	assert!(config.contains("struct RawTelemetryConfig"));
	assert!(config.contains("fn normalize_telemetry_config("));
	assert!(config.contains("fn normalize_telemetry_protocol("));
	assert!(!config.contains("TraceExportConfig"));
}

#[test]
pub(crate) fn adr068_enabled_telemetry_requires_successful_subscriber_install() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(daemon.contains("config.observability.telemetry.is_some()"));
	assert!(daemon.contains("Some(crate::telemetry::install_global("));
	assert!(daemon.contains("Arc::clone(&metrics)"));
	assert!(telemetry.contains("tracing::dispatcher::set_global_default(dispatch).map_err"));
	assert!(telemetry.contains("global tracing subscriber already installed"));
}

#[test]
pub(crate) fn adr068_toron_telemetry_wrapper_uses_observability_telemetry() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("use observability::telemetry as shared_telemetry;"));
	assert!(telemetry.contains("shared_telemetry::OtlpTraceLayer::new"));
	assert!(telemetry.contains("shared_trace_config"));
	assert!(!telemetry.contains("mod otlp"));
	assert!(!telemetry.contains("fn encode_export_trace_service_request"));
}

#[test]
pub(crate) fn adr068_toron_policy_literals_are_ring_owned() {
	let shared = observability_telemetry_source_text();
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	for literal in [
		"TORON_LOG",
		"toron.traces",
		"toron.node_id",
		"toron_otlp_spans_dropped_total",
	] {
		assert!(telemetry.contains(literal), "Toron missing {literal}");
		assert!(!shared.contains(literal), "shared telemetry owns {literal}");
	}
	assert!(telemetry.contains("\"service.name\", \"toron\""));
	assert!(telemetry.contains("\"service.version\""));
}

#[test]
pub(crate) fn adr068_toron_node_id_matches_toron_n_u32_identity() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("parse_toron_audit_node_numeric(&audit.node_id)"));
	assert!(telemetry.contains("parsed != audit.node_numeric_id"));
	assert!(telemetry.contains("\"toron.node_id\""));
}

#[test]
pub(crate) fn adr068_trace_id_is_derived_blake3_not_literal_query_id() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("const TRACE_ID_DOMAIN: &[u8] = b\"toron.trace.v1\";"));
	assert!(telemetry.contains("let mut hasher = blake3::Hasher::new();"));
	assert!(telemetry.contains("hasher.update(query_id.get().as_bytes())"));
	assert!(telemetry.contains("nonzero_trace_id(trace_id)"));
	assert!(!telemetry.contains("Uuid::from_u128"));
	assert!(!telemetry.contains("trace_id = *query_id.get().as_bytes()"));
}

#[test]
pub(crate) fn adr068_query_id_is_not_exported_as_span_attribute() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let production = telemetry.split("#[cfg(test)]").next().unwrap_or(&telemetry);
	for forbidden in ["query_id =", "toron.query_id", "query.id", "query_id = %"] {
		assert!(
			!production.contains(forbidden),
			"query id span attr marker found: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr068_sampling_is_head_based_deterministic_and_query_root_only() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let production = telemetry.split("#[cfg(test)]").next().unwrap_or(&telemetry);
	assert!(telemetry.contains("pub fn head_sample("));
	assert!(telemetry.contains("u64::from_be_bytes"));
	assert!(telemetry.contains("head % TRACE_SAMPLING_DENOMINATOR < u64::from(rate)"));
	assert_eq!(production.matches("head_sample(").count(), 2);
	assert!(telemetry.contains("pub fn query_trace_context(query_id: QueryId)"));
}

#[test]
pub(crate) fn adr068_explicit_sampled_field_is_authoritative() {
	let shared = observability_telemetry_source_text();
	assert!(shared.contains("pub const OTEL_SAMPLED_FIELD"));
	assert!(shared.contains("ExplicitSamplingContext::Unsampled"));
	assert!(shared.contains("ExplicitSamplingContext::Sampled(context)"));
	assert!(shared.contains("unwrap_or_else(|| self.should_sample_root_span())"));
	assert!(shared.contains("return ExplicitSamplingContext::Unsampled;"));
}

#[test]
pub(crate) fn adr068_worker_never_resamples_absent_trace_context() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	assert!(telemetry.contains("fn unsampled_task_span"));
	assert!(telemetry.contains("otel.sampled = false"));
	assert!(server.contains("crate::telemetry::task_trace_context(spec)"));
	assert!(!server.contains("head_sample("));
}

#[test]
pub(crate) fn adr068_external_context_fields_are_shared_ring_neutral_and_stripped() {
	let shared = observability_telemetry_source_text();
	for constant in [
		"OTEL_TRACE_ID_FIELD",
		"OTEL_SPAN_ID_FIELD",
		"OTEL_PARENT_SPAN_ID_FIELD",
		"OTEL_SAMPLED_FIELD",
	] {
		assert!(shared.contains(constant), "missing {constant}");
	}
	assert!(shared.contains("self.explicit_context.record(field.name(), &value)"));
	assert!(shared.contains("return;"));
	assert!(!shared.contains("Toron"));
}

#[test]
pub(crate) fn adr068_propagation_uses_task_execution_context_trace_context() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("read context source");
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	assert!(context.contains("pub trace_context: Option<TraceContext>"));
	assert!(context.contains("context.trace_context"));
	assert!(scheduler.contains("spec.context.trace_context = Some(trace_context);"));
	assert!(server.contains("spec.context.trace_context"));
}

#[test]
pub(crate) fn adr068_taskspec_trace_is_not_a_live_carrier() {
	let task = std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs"))
		.expect("read task source");
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	assert!(task.contains("pub trace: Option<TraceContext>"));
	assert!(!scheduler.contains(".trace ="));
	assert!(!server.contains("spec.trace"));
}

#[test]
pub(crate) fn adr068_boundary_span_ids_are_toron_derived_nonzero() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	for domain in [
		"QUERY_SPAN_DOMAIN",
		"DISPATCH_SPAN_DOMAIN",
		"TASK_SPAN_DOMAIN",
		"STORAGE_SCAN_SPAN_DOMAIN",
	] {
		assert!(telemetry.contains(domain), "missing {domain}");
	}
	assert!(telemetry.contains("fn task_identity_span_id("));
	assert!(telemetry.contains("fn nonzero_span_id("));
	assert!(telemetry.contains("span_id[7] = 1"));
}

#[test]
pub(crate) fn adr068_worker_task_spans_attach_raw_task_telemetry_with_bounded_labels() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("pub fn record_task_telemetry"));
	for field in [
		"toron.task.pulled_shuffle_bytes",
		"toron.task.build_rows",
		"toron.task.emitted_rows",
		"toron.task.task_wall_micros",
		"toron.task.framework_overhead_micros",
	] {
		assert!(telemetry.contains(field), "missing task field {field}");
	}
	assert!(telemetry.contains("fn worker_count_bucket("));
	assert!(telemetry.contains("toron.task.stage_role = field::Empty"));
	assert!(telemetry.contains("toron.task.operator_kind = field::Empty"));
}

#[test]
pub(crate) fn adr068_error_status_and_bounded_error_class_on_failed_spans() {
	let shared = observability_telemetry_source_text();
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(shared.contains("metadata.level() == &tracing::Level::ERROR"));
	assert!(shared.contains("state.status_code = SpanStatusCode::Error"));
	assert!(telemetry.contains("toron.error_class"));
	assert!(telemetry.contains("fn query_error_class("));
	assert!(telemetry.contains("fn task_failure_class("));
	assert!(!telemetry.contains("error = %"));
}

#[test]
pub(crate) fn adr068_storage_scan_spans_do_not_change_storage_results() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(exec.contains("let span = crate::telemetry::storage_scan_span(&req);"));
	assert!(exec.contains("self.storage.scan(req).instrument(span).await?"));
	assert!(telemetry.contains("\"toron.storage.scan\""));
	assert!(!telemetry.contains("projected_columns"));
	assert!(!telemetry.contains("pbc_predicate"));
}

#[test]
pub(crate) fn adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074() {
	let client =
		std::fs::read_to_string(wendelon_manifest_dir().join("crates/wendelon-client/src/lib.rs"))
			.expect("read Wendelon client");
	let table_schema = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");
	let options = client
		.split("pub struct TableQueryOptions")
		.nth(1)
		.and_then(|rest| rest.split("impl Default for TableQueryOptions").next())
		.expect("TableQueryOptions block");
	let meta = table_schema
		.split("pub struct TableQueryRequestMeta")
		.nth(1)
		.and_then(|rest| rest.split("pub struct TableQueryResponseMeta").next())
		.expect("TableQueryRequestMeta block");
	assert!(options.contains("trace_context: TableQueryTraceContext"));
	assert!(meta.contains("trace_id: [u8; 16]"));
	assert!(meta.contains("trace_parent_span_id: [u8; 8]"));
}

#[test]
pub(crate) fn adr069_wendelon_trace_consumer_is_toron_only() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(storage.contains("fn wendelon_table_query_trace_context("));
	assert!(storage.contains("wendelon_client::TableQueryTraceContext"));
	assert!(telemetry.contains("pub(crate) fn storage_scan_trace_context("));

	let mut offenders = Vec::new();
	for path in source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.chain(source_files(
			&wendelon_manifest_dir().join("crates/wendelon-client/src"),
		)) {
		let content = std::fs::read_to_string(&path).expect("read Wendelon source");
		for forbidden in [
			"wendelon_table_query_trace_context",
			"storage_scan_trace_context",
			"toron.storage.scan",
		] {
			if content.contains(forbidden) {
				offenders.push(format!("{} contains {forbidden}", path.display()));
			}
		}
	}
	assert!(
		offenders.is_empty(),
		"ADR 069 consumer hooks must stay out of Wendelon production:\n{}",
		offenders.join("\n")
	);
}

#[test]
pub(crate) fn adr069_only_storage_scan_populates_table_query_trace_context() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	assert_eq!(
		production
			.matches("fn wendelon_table_query_trace_context(")
			.count(),
		1
	);
	assert_eq!(
		production
			.matches("TableQueryTraceContext::Sampled")
			.count(),
		1
	);
	assert_eq!(
		production
			.matches("trace_context: wendelon_table_query_trace_context(&request),")
			.count(),
		1
	);
}

#[test]
pub(crate) fn adr069_trace_context_set_once_on_shared_options() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, scan_block) =
		brace_blocks_after_marker(&storage, "impl StorageScan for WendelonStorageScan")
			.into_iter()
			.next()
			.expect("WendelonStorageScan impl block");
	let options_pos = scan_block
		.find("let mut options = wendelon_client::TableQueryOptions {")
		.expect("options initialization");
	let carrier = "trace_context: wendelon_table_query_trace_context(&request),";
	let carrier_pos = scan_block.find(carrier).expect("trace context carrier");
	let default_pos = scan_block
		.find("..wendelon_client::TableQueryOptions::default()")
		.expect("default options update");
	let branch_pos = scan_block
		.find("let mut query_stream = if let Some(downstream_cancellation)")
		.expect("cancellation branch");
	assert_eq!(scan_block.matches(carrier).count(), 1);
	assert!(options_pos < carrier_pos);
	assert!(carrier_pos < default_pos);
	assert!(default_pos < branch_pos);
}

#[test]
pub(crate) fn adr069_disabled_local_telemetry_sends_absent_even_with_context() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("if !telemetry_enabled"));
	assert!(helper.contains("return wendelon_client::TableQueryTraceContext::Absent;"));
}

#[test]
pub(crate) fn adr069_enabled_unsampled_sends_unsampled_carrier() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("None => wendelon_client::TableQueryTraceContext::Unsampled"));
}

#[test]
pub(crate) fn adr069_enabled_sampled_sends_sampled_carrier() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("Some(context) => wendelon_client::TableQueryTraceContext::Sampled"));
	assert!(helper.contains("trace_id: context.trace_id"));
	assert!(helper.contains("parent_span_id: context.span_id"));
}

#[test]
pub(crate) fn adr069_parent_span_id_equals_storage_scan_trace_context_span_id() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(helper.contains("match crate::telemetry::storage_scan_trace_context(request)"));
	assert!(helper.contains("parent_span_id: context.span_id"));
}

#[test]
pub(crate) fn adr069_parent_span_id_is_not_request_trace_context_span_id() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	assert!(!helper.contains("request.trace_context"));
	assert!(!helper.contains("parent_span_id: parent.span_id"));
	assert!(!helper.contains("parent_span_id: request"));
}

#[test]
pub(crate) fn adr069_storage_scan_span_id_remains_private_to_telemetry() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(telemetry.contains("\nfn storage_scan_span_id(request: &ScanRequest) -> [u8; 8]"));
	assert!(!telemetry.contains("pub(crate) fn storage_scan_span_id"));
	assert!(!telemetry.contains("pub fn storage_scan_span_id"));
	assert!(telemetry.contains("pub(crate) fn storage_scan_trace_context(request: &ScanRequest)"));
}

#[test]
pub(crate) fn adr069_storage_rs_does_not_duplicate_storage_scan_hash_recipe() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	for forbidden in [
		"STORAGE_SCAN_SPAN_DOMAIN",
		"blake3::Hasher",
		"hasher.update(request.query_id",
	] {
		assert!(
			!production.contains(forbidden),
			"storage.rs must delegate storage-scan span id derivation, found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr069_cancellation_attempt_hook_shape_is_preserved() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, scan_block) =
		brace_blocks_after_marker(&storage, "impl StorageScan for WendelonStorageScan")
			.into_iter()
			.next()
			.expect("WendelonStorageScan impl block");
	assert!(scan_block.contains("table_query_stream_with_runtime_filter_and_attempt_hook"));
	assert!(scan_block.contains("register_wendelon_table_query_attempt"));
	assert!(scan_block.contains("WendelonTableQueryAttempt"));
	assert!(scan_block.contains("target: attempt.target"));
	assert!(scan_block.contains("tenant_id: attempt.tenant_id"));
	assert!(scan_block.contains("request_id: attempt.request_id"));
}

#[test]
pub(crate) fn adr069_no_toron_derived_wendelon_table_query_span_id() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	for forbidden in [
		"wendelon.table.query",
		"TABLE_QUERY_TRACE",
		"request_id",
		"blake3",
	] {
		assert!(
			!helper.contains(forbidden),
			"Toron must pass the parent for Wendelon's table-query span, not derive it; found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr069_no_traceparent_compute_trace_context_v1_or_taskspec_trace() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	for forbidden in ["traceparent", "ComputeTraceContext", "TaskSpec.trace"] {
		assert!(
			!production.contains(forbidden),
			"ADR 069 must use the Wendelon table-query carrier directly; found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr069_no_sql_tenant_table_descriptor_peer_queryid_attrs() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let (_, helper) = brace_blocks_after_marker(
		&storage,
		"fn wendelon_table_query_trace_context_with_enabled",
	)
	.into_iter()
	.next()
	.expect("trace context helper block");
	for forbidden in [
		"sql",
		"tenant_id",
		"table_id",
		"descriptor",
		"peer",
		"query_id",
	] {
		assert!(
			!helper.contains(forbidden),
			"trace carrier mapping must not add unbounded attributes; found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr069_no_wendelon_production_change() {
	let mut offenders = Vec::new();
	for path in source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.chain(source_files(
			&wendelon_manifest_dir().join("crates/wendelon-client/src"),
		)) {
		let content = std::fs::read_to_string(&path).expect("read Wendelon source");
		for forbidden in [
			"wendelon_table_query_trace_context",
			"storage_scan_trace_context",
			"toron.storage.scan",
		] {
			if content.contains(forbidden) {
				offenders.push(format!("{} contains {forbidden}", path.display()));
			}
		}
	}
	assert!(
		offenders.is_empty(),
		"ADR 069 must not add Toron consumer hooks to Wendelon production:\n{}",
		offenders.join("\n")
	);
}

#[test]
pub(crate) fn adr069_only_table_query_read_path_carrier_populated() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let production = storage.split("#[cfg(test)]").next().unwrap_or(&storage);
	assert_eq!(
		production
			.matches("trace_context: wendelon_table_query_trace_context")
			.count(),
		1
	);
	assert!(!production.contains("table_insert_with_trace_context"));
	assert!(!production.contains("delete_rows_with_trace_context"));
	assert!(!production.contains("drop_table_with_trace_context"));
}

#[test]
pub(crate) fn adr069_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	for forbidden in [
		"tokio",
		"quinn",
		"rustls",
		"observability",
		"tls-pki",
		"reqwest",
		"h2",
		"wendelon-client",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr069_reconciles_074_producer_only_toron_population_guards() {
	let toron_arch = architecture_corpus_source();
	let (_, adr068_block) = brace_blocks_after_marker(
		&toron_arch,
		"fn adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074",
	)
	.into_iter()
	.next()
	.expect("ADR 068 reconciliation guard block");
	assert!(!adr068_block.contains("!storage.contains(\"TableQueryTraceContext\")"));
	assert!(!adr068_block.contains("consumer ADR, not ADR 074"));

	let wendelon_arch =
		std::fs::read_to_string(wendelon_manifest_dir().join("tests/architecture_test.rs"))
			.expect("read Wendelon architecture tests");
	let (_, adr074_block) =
		brace_blocks_after_marker(&wendelon_arch, "fn adr074_no_toron_production_change")
			.into_iter()
			.next()
			.expect("ADR 074 reconciliation guard block");
	assert!(adr074_block.contains("ADR 069 owns Toron's table-query trace carrier population"));
	assert!(!adr074_block.contains("\"TableQueryTraceContext\","));
	assert!(!adr074_block.contains("\"trace_parent_span_id\""));
}

#[test]
pub(crate) fn adr069_adr068_and_adr074_guards_remain_green() {
	let toron_arch = architecture_corpus_source();
	let wendelon_arch =
		std::fs::read_to_string(wendelon_manifest_dir().join("tests/architecture_test.rs"))
			.expect("read Wendelon architecture tests");
	for required in [
		"fn adr068_wendelon_table_query_trace_carrier_is_owned_by_wendelon_adr074",
		"fn adr069_wendelon_trace_consumer_is_toron_only",
		"fn adr069_reconciles_074_producer_only_toron_population_guards",
	] {
		assert!(toron_arch.contains(required), "missing {required}");
	}
	assert!(wendelon_arch.contains("fn adr074_no_toron_production_change"));
	assert!(wendelon_arch.contains("fn adr074_toron_consumer_population_is_owned_by_adr069"));
}

#[test]
pub(crate) fn adr068_no_sql_table_tenant_session_descriptor_peer_queryid_span_attrs() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let production = telemetry.split("#[cfg(test)]").next().unwrap_or(&telemetry);
	for forbidden in [
		"sql =",
		"statement =",
		"table_name",
		"column_name",
		"tenant_id =",
		"session_id =",
		"physical_descriptor_fingerprint =",
		"peer",
		"query_id =",
	] {
		assert!(
			!production.contains(forbidden),
			"forbidden span attr marker found: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr068_toron_otlp_queue_is_bounded_fail_open() {
	let shared = observability_telemetry_source_text();
	assert!(shared.contains("mpsc::channel(cfg.max_queue_size)"));
	assert!(shared.contains("tx.try_send(ExportCommand::Span(completed))"));
	assert!(shared.contains("TrySendError::Full"));
	assert!(shared.contains("TrySendError::Closed"));
}

#[test]
pub(crate) fn adr068_toron_dropped_span_metric_is_aggregate_only() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let metrics = std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
		.expect("read metrics source");
	assert!(telemetry.contains("TORON_DROPPED_SPAN_METRIC"));
	assert!(metrics.contains("# TYPE toron_otlp_spans_dropped_total counter"));
	assert!(metrics.contains("toron_otlp_spans_dropped_total {}"));
	assert!(!metrics.contains("toron_otlp_spans_dropped_total{"));
}

#[test]
pub(crate) fn adr068_toron_shutdown_handle_is_explicit() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	assert!(daemon.contains("telemetry: Option<crate::telemetry::TelemetryHandle>"));
	assert!(daemon.contains("telemetry.shutdown()"));
	assert!(telemetry.contains("pub struct TelemetryHandle"));
	assert!(telemetry.contains("OtlpShutdownHandle"));
}

#[test]
pub(crate) fn adr068_wendelon_otlp_behavior_fixtures_remain_unchanged() {
	adr067_wendelon_otlp_behavior_fixtures_remain_present();
	let http = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_http_export.rs"))
		.expect("read Wendelon OTLP HTTP fixture");
	let grpc = std::fs::read_to_string(wendelon_manifest_dir().join("tests/otlp_grpc_export.rs"))
		.expect("read Wendelon OTLP gRPC fixture");
	assert!(!http.contains("toron"));
	assert!(!grpc.contains("toron"));
}

#[test]
pub(crate) fn adr068_nilket_root_stays_runtime_free() {
	adr067_nilket_root_stays_runtime_free();
}

#[test]
pub(crate) fn adr068_fixture_worker_source_unchanged() {
	adr067_fixture_worker_source_unchanged();
}

#[test]
pub(crate) fn adr068_toron_telemetry_surface_is_brand_clean_no_v1() {
	let telemetry = std::fs::read_to_string(manifest_dir().join("src/telemetry.rs"))
		.expect("read telemetry source");
	let shared = observability_telemetry_source_text();
	for forbidden in ["TraceContextV1", "TelemetryProfileV1", "OtlpTraceLayerV1"] {
		assert!(!telemetry.contains(forbidden), "Toron contains {forbidden}");
		assert!(!shared.contains(forbidden), "shared contains {forbidden}");
	}
	assert!(!shared.contains("Toron"));
}

#[test]
pub(crate) fn adr068_prior_adr045_to_067_guards_continue_to_pass_except_external_context_seam() {
	let architecture_test = architecture_corpus_source();
	for prefix in [
		"adr045_", "adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_",
		"adr053_", "adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_",
		"adr061_", "adr062_", "adr063_", "adr064_", "adr065_", "adr066_", "adr067_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr068_explicit_sampled_field_is_authoritative"));
}
