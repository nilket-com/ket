use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, Field, Schema};
use toron::catalog::{TableRef, WendelonDescriptorRegistry};
use toron::config::{
	DescriptorBootstrap, DescriptorBootstrapEntry, load_descriptor_bootstrap,
	load_toron_bootstrap_config, render_example_config,
};
use toron::distributed::budget::{ByteCount, QueryBudget};
use toron::ir::TableId;
use toron::storage::TenantId;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

fn fresh_dir(tag: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("system time after epoch")
		.as_nanos();
	let dir = std::env::temp_dir().join(format!(
		"toron-config-{tag}-{}-{}",
		std::process::id(),
		nanos
	));
	std::fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

fn write_toron_tls_placeholders(dir: &Path) {
	for name in ["listener.pem", "listener.key", "peer-ca.pem"] {
		std::fs::write(dir.join(name), "placeholder").expect("write TLS placeholder");
	}
}

fn toron_tls_section() -> &'static str {
	r#"
[tls]
listener_cert = "listener.pem"
listener_key = "listener.key"
peer_ca = "peer-ca.pem"
cluster_domain = "cluster.local"
"#
}

fn descriptor() -> TableSchemaDescriptor {
	descriptor_with_identity(0x42, 1, "config_test", "sales")
}

fn descriptor_with_identity(
	object_byte: u8,
	generation: u64,
	namespace: &str,
	table_name: &str,
) -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
	]);
	TableSchemaDescriptor::new(
		TableObjectId::new([object_byte; 16]),
		generation,
		namespace,
		table_name,
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

#[test]
fn toron_example_config_loads() {
	let dir = fresh_dir("example-loads");
	let path = dir.join("toron.toml");
	std::fs::write(&path, render_example_config()).expect("write example config");

	let config = load_toron_bootstrap_config(&path).expect("load example config");

	assert!(config.worker.task_listener_bind.ip().is_loopback());
	assert_eq!(config.storage.wendelon.bootstrap_peers.len(), 1);
	assert!(config.catalog.descriptor_bootstrap_path.is_none());
	assert!(
		config
			.sql
			.expect("example sql config")
			.listener_bind
			.ip()
			.is_loopback()
	);
	assert!(config.coordinator.include_self_worker);
	assert!(config.tls.is_none());
	assert_eq!(
		config.tenant_budgets.default.memory_per_task,
		QueryBudget::default().memory_per_task
	);
	assert_eq!(
		config.tenant_budgets.default.aggregate_memory,
		ByteCount::new(67_108_864)
	);
	assert_eq!(
		config.tenant_budgets.default.max_concurrent_queries.get(),
		1
	);
	assert_eq!(
		config
			.tenant_budgets
			.default
			.max_concurrent_descriptor_syncs
			.get(),
		1
	);
	assert_eq!(config.tenant_budgets.default.admission_weight.get(), 1);
	assert_eq!(config.tenant_budgets.default.max_queued_queries, 0);
	assert_eq!(
		config.tenant_budgets.default.max_borrowed_memory,
		ByteCount::ZERO
	);
	assert_eq!(
		config
			.tenant_budgets
			.default
			.max_borrowed_concurrent_queries,
		0
	);
	assert!(config.admission.is_none());
}

#[test]
fn tenant_concurrency_policy_config_round_trips() {
	let dir = fresh_dir("tenant-budget-round-trip");
	let path = dir.join("toron.toml");
	let config = format!(
		"{}\n[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nmax_concurrent_queries = 3\nmax_concurrent_descriptor_syncs = 2\n\n[[tenant_budgets.tenants]]\ntenant_id = 7\nmemory_per_task_bytes = 512\naggregate_memory_bytes = 2048\nmax_concurrent_queries = 2\nmax_concurrent_descriptor_syncs = 1\n",
		render_example_config()
	);
	std::fs::write(&path, config).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load tenant budget config");

	assert_eq!(
		config.tenant_budgets.default.memory_per_task,
		ByteCount::new(1024)
	);
	assert_eq!(
		config.tenant_budgets.default.aggregate_memory,
		ByteCount::new(4096)
	);
	assert_eq!(
		config.tenant_budgets.default.max_concurrent_queries.get(),
		3
	);
	assert_eq!(
		config
			.tenant_budgets
			.default
			.max_concurrent_descriptor_syncs
			.get(),
		2
	);
	assert_eq!(
		config
			.tenant_budgets
			.budget_for(TenantId(7))
			.memory_per_task,
		ByteCount::new(512)
	);
	assert_eq!(
		config
			.tenant_budgets
			.budget_for(TenantId(7))
			.aggregate_memory,
		ByteCount::new(2048)
	);
	assert_eq!(
		config
			.tenant_budgets
			.budget_for(TenantId(7))
			.max_concurrent_queries
			.get(),
		2
	);
	assert_eq!(
		config
			.tenant_budgets
			.budget_for(TenantId(7))
			.max_concurrent_descriptor_syncs
			.get(),
		1
	);
}

#[test]
fn tenant_admission_policy_config_round_trips() {
	let dir = fresh_dir("tenant-admission-round-trip");
	let path = dir.join("toron.toml");
	let config = format!(
		"{}\n[admission]\nmax_queued_queries = 8\nmax_queue_wait_ms = 25\nshared_memory_bytes = 4096\nshared_concurrent_queries = 2\n\n[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nadmission_weight = 3\nmax_queued_queries = 4\nmax_borrowed_memory_bytes = 512\nmax_borrowed_concurrent_queries = 1\n\n[[tenant_budgets.tenants]]\ntenant_id = 7\nmemory_per_task_bytes = 512\naggregate_memory_bytes = 2048\nadmission_weight = 2\nmax_queued_queries = 5\nmax_borrowed_memory_bytes = 256\nmax_borrowed_concurrent_queries = 1\n",
		render_example_config()
	);
	std::fs::write(&path, config).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load tenant admission config");
	let admission = config.admission.expect("admission policy");

	assert_eq!(admission.max_queued_queries, 8);
	assert_eq!(admission.max_queue_wait_ms.get(), 25);
	assert_eq!(admission.shared_memory, ByteCount::new(4096));
	assert_eq!(admission.shared_concurrent_queries, 2);
	assert_eq!(config.tenant_budgets.default.admission_weight.get(), 3);
	assert_eq!(config.tenant_budgets.default.max_queued_queries, 4);
	assert_eq!(
		config.tenant_budgets.default.max_borrowed_memory,
		ByteCount::new(512)
	);
	assert_eq!(
		config
			.tenant_budgets
			.default
			.max_borrowed_concurrent_queries,
		1
	);
	let tenant = config.tenant_budgets.budget_for(TenantId(7));
	assert_eq!(tenant.admission_weight.get(), 2);
	assert_eq!(tenant.max_queued_queries, 5);
	assert_eq!(tenant.max_borrowed_memory, ByteCount::new(256));
	assert_eq!(tenant.max_borrowed_concurrent_queries, 1);
}

#[test]
fn tenant_concurrency_omitted_config_derives_default() {
	let dir = fresh_dir("tenant-budget-derived-concurrency");
	let path = dir.join("toron.toml");
	let config = format!(
		"{}\n[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\n\n[[tenant_budgets.tenants]]\ntenant_id = 7\nmemory_per_task_bytes = 512\naggregate_memory_bytes = 2048\n",
		render_example_config()
	);
	std::fs::write(&path, config).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load tenant budget config");

	assert_eq!(
		config.tenant_budgets.default.max_concurrent_queries.get(),
		4
	);
	assert_eq!(
		config
			.tenant_budgets
			.default
			.max_concurrent_descriptor_syncs
			.get(),
		2
	);
	assert_eq!(
		config
			.tenant_budgets
			.budget_for(TenantId(7))
			.max_concurrent_queries
			.get(),
		4
	);
	assert_eq!(
		config
			.tenant_budgets
			.budget_for(TenantId(7))
			.max_concurrent_descriptor_syncs
			.get(),
		2
	);
}

#[test]
fn tenant_concurrency_config_rejects_zero() {
	let dir = fresh_dir("tenant-concurrency-zero");
	let path = dir.join("toron.toml");
	let snippet = "[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nmax_concurrent_queries = 0\n";
	std::fs::write(&path, format!("{}\n{snippet}", render_example_config())).expect("write config");

	let error = load_toron_bootstrap_config(&path).expect_err("zero concurrency must be rejected");

	assert!(
		error
			.to_string()
			.contains("tenant_budgets max_concurrent_queries must be greater than 0"),
		"{error}"
	);
}

#[test]
fn tenant_budget_config_accepts_descriptor_sync_limit() {
	let dir = fresh_dir("tenant-descriptor-sync-accepts");
	let path = dir.join("toron.toml");
	let snippet = "[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nmax_concurrent_queries = 4\nmax_concurrent_descriptor_syncs = 2\n";
	std::fs::write(&path, format!("{}\n{snippet}", render_example_config())).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load config");

	assert_eq!(
		config
			.tenant_budgets
			.default
			.max_concurrent_descriptor_syncs
			.get(),
		2
	);
}

#[test]
fn tenant_budget_config_rejects_zero_descriptor_sync_limit() {
	let dir = fresh_dir("tenant-descriptor-sync-zero");
	let path = dir.join("toron.toml");
	let snippet = "[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nmax_concurrent_descriptor_syncs = 0\n";
	std::fs::write(&path, format!("{}\n{snippet}", render_example_config())).expect("write config");

	let error =
		load_toron_bootstrap_config(&path).expect_err("zero descriptor sync must be rejected");

	assert!(
		error
			.to_string()
			.contains("tenant_budgets max_concurrent_descriptor_syncs must be greater than 0"),
		"{error}"
	);
}

#[test]
fn tenant_budget_config_derives_descriptor_sync_limit_when_omitted() {
	let dir = fresh_dir("tenant-descriptor-sync-derived");
	let path = dir.join("toron.toml");
	let snippet = "[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nmax_concurrent_queries = 4\n";
	std::fs::write(&path, format!("{}\n{snippet}", render_example_config())).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load config");

	assert_eq!(
		config
			.tenant_budgets
			.default
			.max_concurrent_descriptor_syncs
			.get(),
		2
	);
}

#[test]
fn tenant_budget_config_rejects_zero_duplicate_and_per_task_over_aggregate() {
	for (tag, snippet, expected) in [
		(
			"zero-tenant",
			"[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\n\n[[tenant_budgets.tenants]]\ntenant_id = 0\nmemory_per_task_bytes = 512\naggregate_memory_bytes = 2048\n",
			"tenant_budgets tenant_id must be greater than 0",
		),
		(
			"zero-budget",
			"[tenant_budgets.default]\nmemory_per_task_bytes = 0\naggregate_memory_bytes = 4096\n",
			"tenant_budgets memory_per_task_bytes must be greater than 0",
		),
		(
			"duplicate",
			"[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\n\n[[tenant_budgets.tenants]]\ntenant_id = 7\nmemory_per_task_bytes = 512\naggregate_memory_bytes = 2048\n\n[[tenant_budgets.tenants]]\ntenant_id = 7\nmemory_per_task_bytes = 512\naggregate_memory_bytes = 2048\n",
			"duplicate tenant_budgets entry for tenant_id 7",
		),
		(
			"per-task-over-aggregate",
			"[tenant_budgets.default]\nmemory_per_task_bytes = 4097\naggregate_memory_bytes = 4096\n",
			"tenant_budgets memory_per_task_bytes must be less than or equal to aggregate_memory_bytes",
		),
		(
			"zero-concurrency",
			"[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nmax_concurrent_queries = 0\n",
			"tenant_budgets max_concurrent_queries must be greater than 0",
		),
		(
			"zero-descriptor-sync",
			"[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nmax_concurrent_descriptor_syncs = 0\n",
			"tenant_budgets max_concurrent_descriptor_syncs must be greater than 0",
		),
		(
			"zero-admission-weight",
			"[tenant_budgets.default]\nmemory_per_task_bytes = 1024\naggregate_memory_bytes = 4096\nadmission_weight = 0\n",
			"tenant_budgets admission_weight must be greater than 0",
		),
	] {
		let dir = fresh_dir(tag);
		let path = dir.join("toron.toml");
		std::fs::write(&path, format!("{}\n{snippet}", render_example_config()))
			.expect("write config");

		let error = load_toron_bootstrap_config(&path).expect_err("invalid tenant budget");

		assert!(error.to_string().contains(expected), "{error}");
	}
}

#[test]
fn toron_sql_listener_rejects_non_loopback_bind() {
	let dir = fresh_dir("sql-non-loopback");
	let path = dir.join("toron.toml");
	let config = render_example_config().replace(
		"listener_bind = \"127.0.0.1:7742\"",
		"listener_bind = \"0.0.0.0:7742\"",
	);
	std::fs::write(&path, config).expect("write config");

	let error = load_toron_bootstrap_config(&path).expect_err("non-loopback SQL bind must fail");

	assert!(
		error.to_string().contains(
			"sql listener requires authenticated transport for non-loopback bind address"
		),
		"{error}"
	);
}

#[test]
fn toron_worker_listener_rejects_non_loopback_bind_without_tls() {
	let dir = fresh_dir("worker-non-loopback");
	let path = dir.join("toron.toml");
	let config = render_example_config().replace(
		"task_listener_bind = \"127.0.0.1:0\"",
		"task_listener_bind = \"0.0.0.0:0\"",
	);
	std::fs::write(&path, config).expect("write config");

	let error = load_toron_bootstrap_config(&path).expect_err("non-loopback worker bind must fail");

	assert!(
		error.to_string().contains(
			"worker listener requires authenticated transport for non-loopback bind address"
		),
		"{error}"
	);
}

#[test]
fn toron_worker_listener_allows_non_loopback_bind_with_tls() {
	let dir = fresh_dir("worker-non-loopback-tls");
	write_toron_tls_placeholders(&dir);
	let path = dir.join("toron.toml");
	let config = format!(
		"{}{}",
		render_example_config().replace(
			"task_listener_bind = \"127.0.0.1:0\"",
			"task_listener_bind = \"0.0.0.0:0\"",
		),
		toron_tls_section()
	);
	std::fs::write(&path, config).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load TLS remote worker config");

	assert!(!config.worker.task_listener_bind.ip().is_loopback());
	assert!(config.tls.is_some());
}

#[test]
fn toron_sql_listener_allows_non_loopback_bind_with_tls() {
	let dir = fresh_dir("sql-non-loopback-tls");
	write_toron_tls_placeholders(&dir);
	let path = dir.join("toron.toml");
	let config = format!(
		"{}{}",
		render_example_config().replace(
			"listener_bind = \"127.0.0.1:7742\"",
			"listener_bind = \"0.0.0.0:7742\"",
		),
		toron_tls_section()
	);
	std::fs::write(&path, config).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load TLS remote SQL config");

	assert!(
		!config
			.sql
			.expect("sql config")
			.listener_bind
			.ip()
			.is_loopback()
	);
	assert!(config.tls.is_some());
}

#[test]
fn toron_static_worker_rejects_non_loopback_endpoint_without_tls() {
	let dir = fresh_dir("static-worker-non-loopback");
	let path = dir.join("toron.toml");
	let config = render_example_config().replace(
		"# [[coordinator.static_workers]]\n# worker_id = \"00000000-0000-0000-0000-000000000043\"\n# endpoint = \"127.0.0.1:9101\"",
		"[[coordinator.static_workers]]\nworker_id = \"00000000-0000-0000-0000-000000000043\"\nendpoint = \"10.0.0.42:9101\"",
	);
	std::fs::write(&path, config).expect("write config");

	let error =
		load_toron_bootstrap_config(&path).expect_err("non-loopback static worker must fail");

	assert!(
		error.to_string().contains(
			"coordinator static workers require authenticated transport for non-loopback endpoints"
		),
		"{error}"
	);
}

#[test]
fn toron_static_worker_allows_non_loopback_endpoint_with_tls() {
	let dir = fresh_dir("static-worker-non-loopback-tls");
	write_toron_tls_placeholders(&dir);
	let path = dir.join("toron.toml");
	let config = format!(
		"{}{}",
		render_example_config().replace(
			"# [[coordinator.static_workers]]\n# worker_id = \"00000000-0000-0000-0000-000000000043\"\n# endpoint = \"127.0.0.1:9101\"",
			"[[coordinator.static_workers]]\nworker_id = \"00000000-0000-0000-0000-000000000043\"\nendpoint = \"10.0.0.42:9101\"",
		),
		toron_tls_section()
	);
	std::fs::write(&path, config).expect("write config");

	let config = load_toron_bootstrap_config(&path).expect("load TLS remote static worker config");

	assert_eq!(config.coordinator.static_workers.len(), 1);
	assert!(
		!config.coordinator.static_workers[0]
			.endpoint
			.ip()
			.is_loopback()
	);
	assert!(config.tls.is_some());
}

#[test]
fn toron_tls_missing_cert_file_fails_with_stable_diagnostic() {
	let dir = fresh_dir("tls-missing-cert");
	write_toron_tls_placeholders(&dir);
	std::fs::remove_file(dir.join("listener.pem")).expect("remove listener cert");
	let path = dir.join("toron.toml");
	let config = format!("{}{}", render_example_config(), toron_tls_section());
	std::fs::write(&path, config).expect("write config");

	let error = load_toron_bootstrap_config(&path).expect_err("missing cert must fail");

	assert!(
		error
			.to_string()
			.contains("tls cert file not found or unreadable"),
		"{error}"
	);
}

#[test]
fn toron_tls_paths_reject_env_interpolation() {
	let dir = fresh_dir("tls-env-interpolation");
	write_toron_tls_placeholders(&dir);
	let path = dir.join("toron.toml");
	let config = format!(
		"{}\n[tls]\nlistener_cert = \"${{TORON_CERT}}\"\nlistener_key = \"listener.key\"\npeer_ca = \"peer-ca.pem\"\n",
		render_example_config()
	);
	std::fs::write(&path, config).expect("write config");

	let error = load_toron_bootstrap_config(&path).expect_err("env interpolation must fail");

	assert!(
		error
			.to_string()
			.contains("tls config paths do not interpolate environment variables"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_requires_tenant_id() {
	let path = write_descriptor_bootstrap_json(
		"descriptor-missing-tenant-id",
		serde_json::json!({
			"tables": [
				{ "table_id": 7, "descriptor": descriptor() }
			]
		}),
	);

	let error = load_descriptor_bootstrap(&path).expect_err("missing tenant_id must fail");

	assert!(
		error
			.to_string()
			.contains("descriptor bootstrap tenant_id is required"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_requires_table_id() {
	let path = write_descriptor_bootstrap_json(
		"descriptor-missing-table-id",
		serde_json::json!({
			"tables": [
				{ "tenant_id": 7, "descriptor": descriptor() }
			]
		}),
	);

	let error = load_descriptor_bootstrap(&path).expect_err("missing table_id must fail");

	assert!(
		error
			.to_string()
			.contains("descriptor bootstrap table_id is required"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_rejects_zero_tenant() {
	let path = write_descriptor_bootstrap_json(
		"descriptor-zero-tenant-id",
		serde_json::json!({
			"tables": [
				{ "tenant_id": 0, "table_id": 7, "descriptor": descriptor() }
			]
		}),
	);

	let error = load_descriptor_bootstrap(&path).expect_err("zero tenant_id must fail");

	assert!(
		error
			.to_string()
			.contains("descriptor bootstrap tenant_id 0 is reserved"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_rejects_zero_table_id() {
	let path = write_descriptor_bootstrap_json(
		"descriptor-zero-table-id",
		serde_json::json!({
			"tables": [
				{ "tenant_id": 7, "table_id": 0, "descriptor": descriptor() }
			]
		}),
	);

	let error = load_descriptor_bootstrap(&path).expect_err("zero table_id must fail");

	assert!(
		error
			.to_string()
			.contains("descriptor bootstrap table_id 0 is reserved"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_rejects_duplicate_table_id_within_tenant() {
	let first = descriptor_with_identity(0xA1, 1, "config_test", "sales");
	let second = descriptor_with_identity(0xA2, 1, "config_test", "refunds");
	let error = registry_from_entries(vec![
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 11,
			descriptor: first,
		},
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 11,
			descriptor: second,
		},
	])
	.err()
	.expect("duplicate table_id must fail");

	assert!(
		error
			.to_string()
			.contains("duplicate descriptor bootstrap table_id 11 for tenant 7"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_rejects_duplicate_table_name_within_tenant() {
	let first = descriptor_with_identity(0xB1, 1, "config_test", "sales");
	let second = descriptor_with_identity(0xB2, 2, "config_test", "sales");
	let error = registry_from_entries(vec![
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 11,
			descriptor: first,
		},
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 12,
			descriptor: second,
		},
	])
	.err()
	.expect("duplicate table name must fail");

	assert!(
		error
			.to_string()
			.contains("duplicate descriptor bootstrap table config_test.sales for tenant 7"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_allows_same_table_id_across_tenants() {
	let registry = registry_from_entries(vec![
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 11,
			descriptor: descriptor_with_identity(0xC7, 1, "config_test", "sales"),
		},
		DescriptorBootstrapEntry {
			tenant_id: 8,
			table_id: 11,
			descriptor: descriptor_with_identity(0xC8, 1, "config_test", "refunds"),
		},
	])
	.expect("same table_id across tenants");

	assert!(
		registry
			.resolve_table(TenantId(7), &TableRef::with_schema("config_test", "sales"))
			.is_ok()
	);
	assert!(
		registry
			.resolve_table(
				TenantId(8),
				&TableRef::with_schema("config_test", "refunds")
			)
			.is_ok()
	);
}

#[test]
fn descriptor_bootstrap_allows_same_table_name_across_tenants() {
	let registry = registry_from_entries(vec![
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 11,
			descriptor: descriptor_with_identity(0xD7, 1, "config_test", "sales"),
		},
		DescriptorBootstrapEntry {
			tenant_id: 8,
			table_id: 12,
			descriptor: descriptor_with_identity(0xD8, 1, "config_test", "sales"),
		},
	])
	.expect("same table name across tenants");

	assert!(
		registry
			.resolve_table(TenantId(7), &TableRef::with_schema("config_test", "sales"))
			.is_ok()
	);
	assert!(
		registry
			.resolve_table(TenantId(8), &TableRef::with_schema("config_test", "sales"))
			.is_ok()
	);
}

#[test]
fn descriptor_bootstrap_loads_two_tenants_into_one_registry() {
	let tenant_7_descriptor = descriptor_with_identity(0xE7, 7, "config_test", "sales");
	let tenant_8_descriptor = descriptor_with_identity(0xE8, 8, "config_test", "sales");
	let registry = registry_from_entries(vec![
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 1,
			descriptor: tenant_7_descriptor.clone(),
		},
		DescriptorBootstrapEntry {
			tenant_id: 8,
			table_id: 1,
			descriptor: tenant_8_descriptor.clone(),
		},
	])
	.expect("two-tenant descriptor registry");

	let tenant_7 = registry
		.resolve_table(TenantId(7), &TableRef::with_schema("config_test", "sales"))
		.expect("tenant 7 table");
	let tenant_8 = registry
		.resolve_table(TenantId(8), &TableRef::with_schema("config_test", "sales"))
		.expect("tenant 8 table");
	assert_eq!(tenant_7.table, tenant_7_descriptor.table_object_id);
	assert_eq!(tenant_8.table, tenant_8_descriptor.table_object_id);
}

#[test]
fn descriptor_bootstrap_preserves_physical_fingerprint() {
	let descriptor = descriptor_with_identity(0xF7, 7, "config_test", "sales");
	let registry = registry_from_entries(vec![DescriptorBootstrapEntry {
		tenant_id: 7,
		table_id: 9,
		descriptor: descriptor.clone(),
	}])
	.expect("descriptor registry");

	let binding = registry
		.resolve_table(TenantId(7), &TableRef::with_schema("config_test", "sales"))
		.expect("resolve descriptor");

	assert_eq!(binding.legacy_table_id, TableId(9));
	assert_eq!(binding.table, descriptor.table_object_id);
	assert_eq!(
		binding.fingerprint,
		descriptor.physical_descriptor_fingerprint
	);
}

#[test]
fn old_single_tenant_descriptor_bootstrap_shape_is_rejected() {
	let path = write_descriptor_bootstrap_json(
		"descriptor-old-single-tenant-shape",
		serde_json::json!({
			"tables": [
				{ "table_id": 7, "descriptor": descriptor() }
			]
		}),
	);

	let error = load_descriptor_bootstrap(&path).expect_err("old shape must fail");

	assert!(
		error
			.to_string()
			.contains("descriptor bootstrap tenant_id is required"),
		"{error}"
	);
}

#[test]
fn descriptor_bootstrap_json_example_round_trips() {
	let bootstrap = DescriptorBootstrap {
		tables: vec![DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 1,
			descriptor: descriptor(),
		}],
	};

	let encoded = serde_json::to_string_pretty(&bootstrap).expect("encode bootstrap");
	let decoded: DescriptorBootstrap = serde_json::from_str(&encoded).expect("decode bootstrap");

	assert_eq!(decoded.tables.len(), 1);
	assert_eq!(decoded.tables[0].tenant_id, 7);
	assert_eq!(decoded.tables[0].table_id, 1);
}

#[test]
fn same_table_object_id_across_different_tenants_is_allowed() {
	let tenant_7 = descriptor_with_identity(0xAA, 7, "config_test", "sales");
	let tenant_8 = descriptor_with_identity(0xAA, 8, "config_test", "sales");
	let registry = registry_from_entries(vec![
		DescriptorBootstrapEntry {
			tenant_id: 7,
			table_id: 1,
			descriptor: tenant_7,
		},
		DescriptorBootstrapEntry {
			tenant_id: 8,
			table_id: 1,
			descriptor: tenant_8,
		},
	])
	.expect("same object id across tenants");

	assert!(registry.descriptor(TenantId(7), TableId(1)).is_ok());
	assert!(registry.descriptor(TenantId(8), TableId(1)).is_ok());
}

fn registry_from_entries(
	entries: Vec<DescriptorBootstrapEntry>,
) -> Result<WendelonDescriptorRegistry, toron::error::CatalogError> {
	let path = write_descriptor_bootstrap_json(
		"descriptor-registry",
		serde_json::to_value(DescriptorBootstrap { tables: entries })
			.expect("serialize descriptor bootstrap"),
	);
	let descriptors = load_descriptor_bootstrap(&path).expect("load descriptor bootstrap");
	WendelonDescriptorRegistry::from_tenant_descriptors(descriptors)
}

fn write_descriptor_bootstrap_json(tag: &str, value: serde_json::Value) -> PathBuf {
	let dir = fresh_dir(tag);
	let path = dir.join("descriptors.json");
	std::fs::write(&path, serde_json::to_string_pretty(&value).unwrap())
		.expect("write descriptor bootstrap");
	path
}
