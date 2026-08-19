//! PRODUCT-0134 stage 0 baselines.
//!
//! These tests pin the adapter configuration surface exactly as it stands
//! before the saved-connection-profile arc moves it. Adapters do not share one
//! input kind, so each is baselined at what it actually does with its
//! configuration: SQLite and Parquet connect end to end deterministically,
//! Postgres and MySQL are pinned at the seam where a typed string becomes
//! driver arguments, and BigQuery and Toron are configuration-only. No test
//! requires a network.

use crate::adapters::common::ExecutionResult;
use crate::adapters::driver::{
	AdapterConfiguration, AdapterSelection, AdapterState, fields_for,
};

fn all_selections() -> Vec<AdapterSelection> {
	vec![
		AdapterSelection::BigQuery,
		AdapterSelection::MySQL,
		AdapterSelection::Parquet,
		AdapterSelection::Postgres,
		AdapterSelection::SQLite,
		#[cfg(feature = "toron")]
		AdapterSelection::Toron,
	]
}

/// 0a census: every adapter declares exactly one plaintext text field, so the
/// masking branch in `adapter_configuration_view` has no consumer and a saved
/// profile could not yet separate a secret field from a plain one.
#[test]
fn product_0134_census_every_adapter_declares_one_plaintext_field() {
	let expected: &[(&str, &str, Option<&str>)] = &[
		("BigQuery", "project_id", None),
		("MySQL", "connection_string", None),
		("Parquet", "input_path", None),
		("Postgres", "connection_string", None),
		("SQLite", "connection_string", Some("memory")),
		#[cfg(feature = "toron")]
		("Toron", "endpoint", None),
	];
	let actual: Vec<(String, Vec<(&str, Option<&str>, bool)>)> = all_selections()
		.iter()
		.map(|selection| {
			(
				selection.adapter_type_str().to_owned(),
				fields_for(selection)
					.iter()
					.map(|field| (field.key, field.default_value, field.is_secure))
					.collect(),
			)
		})
		.collect();
	for ((name, key, default), (actual_name, fields)) in expected.iter().zip(&actual) {
		assert_eq!(name, actual_name);
		assert_eq!(
			fields,
			&vec![(*key, *default, false)],
			"{name} must declare exactly one plaintext field before decomposition"
		);
	}
	assert_eq!(expected.len(), actual.len());
}

/// 0a census: `connection_identity` trims the dialog Name and falls back to
/// the adapter type. PRODUCT-0134 §4 promotes this trimmed name to the profile
/// identity, so the trim itself is baselined.
#[test]
fn product_0134_census_connection_identity_trims_the_name() {
	let mut state = AdapterState::default();
	state.selection = AdapterSelection::Postgres;
	state.name = "   ".to_owned();
	assert_eq!(state.connection_identity(), "Postgres");
	state.name = "prod-warehouse".to_owned();
	assert_eq!(state.connection_identity(), "prod-warehouse");
}

/// 0b baseline: SQLite connects end to end against both accepted connection
/// strings — the `memory` sentinel and a filesystem path — and answers a
/// trivial query.
#[test]
fn product_0134_baseline_sqlite_connects_memory_and_file() {
	let runtime = tokio::runtime::Runtime::new().expect("runtime");
	let memory = runtime
		.block_on(AdapterState::connect(AdapterConfiguration::SQLite {
			connection_string: "memory".into(),
		}))
		.expect("memory sqlite connects");
	let result = runtime.block_on(async {
		memory.write().await.dispatch("select 1 as x").await
	});
	let ExecutionResult::Rows { artifact, .. } = result else {
		panic!("memory sqlite answers a trivial query");
	};
	assert_eq!(artifact.shape(), (1, 1));

	let dir = std::env::temp_dir().join(format!(
		"polariton-0134-sqlite-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_nanos()
	));
	std::fs::create_dir_all(&dir).expect("temp dir");
	let path = dir.join("baseline.db");
	let file = runtime
		.block_on(AdapterState::connect(AdapterConfiguration::SQLite {
			connection_string: path.to_str().expect("utf8 path").to_owned(),
		}))
		.expect("file sqlite connects");
	drop(file);
	let _ = std::fs::remove_dir_all(&dir);
}

/// 0b baseline: Parquet connects end to end against a locally written fixture
/// file, registers the file stem as the table name, and answers SQL over it.
#[test]
fn product_0134_baseline_parquet_connects_a_fixture_file() {
	use polars::prelude::ParquetWriter;
	let runtime = tokio::runtime::Runtime::new().expect("runtime");
	let dir = std::env::temp_dir().join(format!(
		"polariton-0134-parquet-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_nanos()
	));
	std::fs::create_dir_all(&dir).expect("temp dir");
	let path = dir.join("baseline_fixture.parquet");
	let mut frame = polars::df!("x" => [1_i64, 2, 3], "label" => ["a", "b", "c"])
		.expect("fixture frame");
	let file = std::fs::File::create(&path).expect("fixture file");
	ParquetWriter::new(file)
		.finish(&mut frame)
		.expect("fixture written");

	let adapter = runtime
		.block_on(AdapterState::connect(AdapterConfiguration::Parquet {
			input_path: path.to_str().expect("utf8 path").to_owned(),
		}))
		.expect("parquet connects");
	let result = runtime.block_on(async {
		adapter
			.write()
			.await
			.dispatch("select x, label from baseline_fixture order by x")
			.await
	});
	let ExecutionResult::Rows { artifact, .. } = result else {
		panic!("parquet fixture answers SQL over the file-stem table");
	};
	assert_eq!(artifact.shape(), (3, 2));
	let _ = std::fs::remove_dir_all(&dir);
}

/// 0b baseline: the seam where a typed Postgres connection string becomes
/// driver arguments. Stage 1's decomposed field map must assemble driver
/// arguments equal to parsing this equivalent string.
#[test]
fn product_0134_baseline_postgres_assembly_from_a_typed_string() {
	use tokio_postgres::config::Host;
	let config: tokio_postgres::Config = "postgresql://user:secret@db.example.com:6432/analytics"
		.parse()
		.expect("typed string parses");
	assert_eq!(
		config.get_hosts(),
		&[Host::Tcp("db.example.com".to_owned())]
	);
	assert_eq!(config.get_ports(), &[6432]);
	assert_eq!(config.get_dbname(), Some("analytics"));
	assert_eq!(config.get_user(), Some("user"));
	assert_eq!(config.get_password(), Some(b"secret".as_slice()));
}

/// 0b baseline: the seam where a typed MySQL connection string becomes driver
/// arguments, including percent-decoding of the password.
#[test]
fn product_0134_baseline_mysql_assembly_from_a_typed_string() {
	let opts = mysql_async::Opts::from_url("mysql://user:p%40ssword@db.example.com:3307/analytics")
		.expect("typed string parses");
	assert_eq!(opts.ip_or_hostname(), "db.example.com");
	assert_eq!(opts.tcp_port(), 3307);
	assert_eq!(opts.db_name(), Some("analytics"));
	assert_eq!(opts.user(), Some("user"));
	assert_eq!(opts.pass(), Some("p@ssword"));
}

/// 0a/0b: `configure()` assembles each adapter's configuration from the field
/// map verbatim, and the missing-field fallbacks are pinned exactly as they
/// stand. BigQuery and Toron are configuration-only: no credential, no
/// network, nothing decomposes.
#[test]
fn product_0134_baseline_configure_assembles_each_configuration_from_fields() {
	let mut state = AdapterState::default();

	state.select(AdapterSelection::Postgres);
	state
		.fields
		.insert("connection_string".into(), "postgresql://h/db".into());
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::Postgres { connection_string } if connection_string == "postgresql://h/db"
	));

	state.select(AdapterSelection::Postgres);
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::Postgres { connection_string }
			if connection_string == "postgresql://localhost"
	));

	state.select(AdapterSelection::MySQL);
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::MySQL { connection_string }
			if connection_string == "mysql://root@localhost/mydb"
	));

	state.select(AdapterSelection::SQLite);
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::SQLite { connection_string } if connection_string == "memory"
	));

	state.select(AdapterSelection::BigQuery);
	state.fields.insert("project_id".into(), "warehouse".into());
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::BigQuery { project_id } if project_id == "warehouse"
	));

	#[cfg(feature = "toron")]
	{
		state.select(AdapterSelection::Toron);
		state
			.fields
			.insert("endpoint".into(), "loopback.invalid:1".into());
		state.configure();
		assert!(matches!(
			&state.configuration,
			AdapterConfiguration::Toron { endpoint } if endpoint == "loopback.invalid:1"
		));
	}
}
