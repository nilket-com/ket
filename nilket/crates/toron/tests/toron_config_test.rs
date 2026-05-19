use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, Field, Schema};
use toron::catalog::{TableRef, WendelonDescriptorRegistry};
use toron::config::{
	ToronDescriptorBootstrap, ToronDescriptorBootstrapTable, load_descriptor_bootstrap,
	load_toron_bootstrap_config, render_example_config,
};
use toron::ir::TableId;
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

fn descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
	]);
	TableSchemaDescriptor::new(
		TableObjectId::new([0x42; 16]),
		1,
		"config_test",
		"sales",
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
			"toron sql listener requires authenticated transport for non-loopback bind address"
		),
		"{error}"
	);
}

#[test]
fn toron_descriptor_bootstrap_requires_table_ids() {
	let dir = fresh_dir("descriptor-table-id");
	let path = dir.join("descriptors.json");
	let missing_table_id = serde_json::json!({
		"tables": [
			{ "descriptor": descriptor() }
		]
	});
	std::fs::write(
		&path,
		serde_json::to_string_pretty(&missing_table_id).unwrap(),
	)
	.expect("write descriptor bootstrap");

	let error = load_descriptor_bootstrap(&path).expect_err("missing table_id must fail");

	assert!(error.to_string().contains("missing field `table_id`"));
}

#[test]
fn toron_descriptor_bootstrap_loads_registry() {
	let dir = fresh_dir("descriptor-registry");
	let path = dir.join("descriptors.json");
	let bootstrap = ToronDescriptorBootstrap {
		tables: vec![ToronDescriptorBootstrapTable {
			table_id: 7,
			descriptor: descriptor(),
		}],
	};
	std::fs::write(&path, serde_json::to_string_pretty(&bootstrap).unwrap())
		.expect("write descriptor bootstrap");

	let descriptors = load_descriptor_bootstrap(&path).expect("load descriptor bootstrap");
	assert_eq!(descriptors.len(), 1);
	assert_eq!(descriptors[0].0, TableId(7));

	let registry =
		WendelonDescriptorRegistry::from_descriptors(descriptors).expect("descriptor registry");
	let binding = registry
		.resolve_table(&TableRef::with_schema("config_test", "sales"))
		.expect("resolve descriptor");
	assert_eq!(binding.legacy_table_id, TableId(7));
}
