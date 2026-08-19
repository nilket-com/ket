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

/// (key, default_value, is_secure) — one declared adapter field.
type FieldRow = (&'static str, Option<&'static str>, bool);

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

/// Stage 1: the four uncredentialed adapters keep their stage 0 single-field
/// declarations byte for byte, while Postgres and MySQL decompose into the
/// PRODUCT-0134 §2 field set with exactly one secure field, `password` — the
/// masking branch's first production consumer.
#[test]
fn product_0134_fields_decompose_only_the_credentialed_adapters() {
	let decomposed = |port_default: &'static str| {
		vec![
			("host", None, false),
			("port", Some(port_default), false),
			("database", None, false),
			("user", None, false),
			("password", None, true),
		]
	};
	let expected: Vec<(&str, Vec<FieldRow>)> = vec![
		("BigQuery", vec![("project_id", None, false)]),
		("MySQL", decomposed("3306")),
		("Parquet", vec![("input_path", None, false)]),
		("Postgres", decomposed("5432")),
		("SQLite", vec![("connection_string", Some("memory"), false)]),
		#[cfg(feature = "toron")]
		("Toron", vec![("endpoint", None, false)]),
	];
	let actual: Vec<(String, Vec<FieldRow>)> = all_selections()
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
	for ((name, fields), (actual_name, actual_fields)) in expected.iter().zip(&actual) {
		assert_eq!(name, actual_name);
		assert_eq!(fields, actual_fields, "{name} field surface");
	}
	assert_eq!(expected.len(), actual.len());
	for selection in [AdapterSelection::Postgres, AdapterSelection::MySQL] {
		let secure: Vec<&str> = fields_for(&selection)
			.iter()
			.filter(|field| field.is_secure)
			.map(|field| field.key)
			.collect();
		assert_eq!(secure, ["password"]);
	}
}

/// Stage 1: the decomposed Postgres field set assembles driver arguments equal
/// to parsing the equivalent typed string — the stage 0 oracle.
#[test]
fn product_0134_postgres_fields_match_the_typed_string_oracle() {
	use tokio_postgres::config::Host;
	let assembled =
		crate::adapters::postgres::config_from_fields("db.example.com", "6432", "analytics", "user", "secret")
			.expect("fields assemble");
	let oracle: tokio_postgres::Config = "postgresql://user:secret@db.example.com:6432/analytics"
		.parse()
		.expect("oracle parses");
	assert_eq!(assembled.get_hosts(), oracle.get_hosts());
	assert_eq!(assembled.get_hosts(), &[Host::Tcp("db.example.com".into())]);
	assert_eq!(assembled.get_ports(), oracle.get_ports());
	assert_eq!(assembled.get_dbname(), oracle.get_dbname());
	assert_eq!(assembled.get_user(), oracle.get_user());
	assert_eq!(assembled.get_password(), oracle.get_password());
}

/// Stage 1: the decomposed MySQL field set assembles driver arguments equal to
/// the typed-string oracle, with the password taken verbatim — no
/// percent-encoding, where the URL form requires `p%40ssword`.
#[test]
fn product_0134_mysql_fields_match_the_typed_string_oracle() {
	let assembled = crate::adapters::mysql::opts_from_fields(
		"db.example.com",
		"3307",
		"analytics",
		"user",
		"p@ssword",
	)
	.expect("fields assemble");
	let oracle =
		mysql_async::Opts::from_url("mysql://user:p%40ssword@db.example.com:3307/analytics")
			.expect("oracle parses");
	assert_eq!(assembled.ip_or_hostname(), oracle.ip_or_hostname());
	assert_eq!(assembled.tcp_port(), oracle.tcp_port());
	assert_eq!(assembled.db_name(), oracle.db_name());
	assert_eq!(assembled.user(), oracle.user());
	assert_eq!(assembled.pass(), oracle.pass());
}

/// Stage 1: the port refuses non-numeric input at connect time; empty
/// optional fields fall to driver defaults rather than erroring.
#[test]
fn product_0134_port_refuses_non_numeric_and_optionals_default() {
	let error = crate::adapters::postgres::config_from_fields("h", "54x2", "db", "u", "")
		.expect_err("non-numeric port refuses");
	assert!(error.contains("port"), "{error}");
	let error = crate::adapters::mysql::opts_from_fields("h", "33 06", "db", "u", "")
		.expect_err("non-numeric port refuses");
	assert!(error.contains("port"), "{error}");

	let config = crate::adapters::postgres::config_from_fields("h", "", "", "u", "")
		.expect("optionals may be empty");
	assert_eq!(config.get_ports(), &[5432]);
	assert_eq!(config.get_dbname(), None);
	assert_eq!(config.get_password(), None);
	let opts = crate::adapters::mysql::opts_from_fields("h", "", "", "u", "")
		.expect("optionals may be empty");
	assert_eq!(opts.tcp_port(), 3306);
	assert_eq!(opts.db_name(), None);
	assert_eq!(opts.pass(), None);
}

/// Stage 1: the two input kinds never merge. An ad hoc string beside touched
/// decomposed fields refuses; a field still holding its seeded default is
/// untouched and does not trip the refusal.
#[test]
fn product_0134_input_kinds_refuse_when_mixed() {
	let mut state = AdapterState::default();
	state.select(AdapterSelection::Postgres);
	assert_eq!(
		state.fields.get("port").map(String::as_str),
		Some("5432"),
		"select seeds only the safe port default"
	);
	assert_eq!(state.fields.len(), 1);

	state.ad_hoc_connection_string = "postgresql://u@h/db".to_owned();
	assert!(
		state.input_kind_error().is_none(),
		"a seeded default is not a touched field"
	);

	state.fields.insert("host".into(), "h".into());
	assert!(state.input_kind_error().is_some());

	state.ad_hoc_connection_string.clear();
	assert!(state.input_kind_error().is_none());
}

/// Stage 1: `configure()` routes each input kind to its own configuration —
/// the field kind carries values verbatim, the ad hoc kind stays a string —
/// and a debug rendering of the field kind never exposes the password.
#[test]
fn product_0134_configure_routes_input_kinds_and_debug_redacts() {
	let mut state = AdapterState::default();
	state.select(AdapterSelection::Postgres);
	state.fields.insert("host".into(), "h".into());
	state.fields.insert("database".into(), "db".into());
	state.fields.insert("user".into(), "u".into());
	state.fields.insert("password".into(), "sekrit".into());
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::PostgresFields { host, port, database, user, password }
			if host == "h" && port == "5432" && database == "db" && user == "u" && password == "sekrit"
	));
	let rendered = format!("{:?}", state.configuration);
	assert!(!rendered.contains("sekrit"), "{rendered}");
	assert!(rendered.contains("<redacted>"));

	state.select(AdapterSelection::Postgres);
	state.ad_hoc_connection_string = "postgresql://u@h/db".to_owned();
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::Postgres { connection_string } if connection_string == "postgresql://u@h/db"
	));

	state.select(AdapterSelection::MySQL);
	state.fields.insert("host".into(), "h".into());
	state.fields.insert("database".into(), "db".into());
	state.fields.insert("user".into(), "u".into());
	state.configure();
	assert!(matches!(
		&state.configuration,
		AdapterConfiguration::MySQLFields { host, port, database, user, password }
			if host == "h" && port == "3306" && database == "db" && user == "u" && password.is_empty()
	));
}

/// 0a census: `connection_identity` trims the dialog Name and falls back to
/// the adapter type. PRODUCT-0134 §4 promotes this trimmed name to the profile
/// identity, so the trim itself is baselined.
#[test]
fn product_0134_census_connection_identity_trims_the_name() {
	let mut state = AdapterState {
		selection: AdapterSelection::Postgres,
		name: "   ".to_owned(),
		..AdapterState::default()
	};
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

/// 0a/0b: `configure()` assembles the uncredentialed adapters' configurations
/// from the field map verbatim, with the SQLite fallback pinned exactly as it
/// stands. BigQuery and Toron are configuration-only: no credential, no
/// network, nothing decomposes. (The stage 0 Postgres/MySQL fallback arms were
/// the deliberately moved surface; their stage 1 routing is pinned in
/// `product_0134_configure_routes_input_kinds_and_debug_redacts`.)
#[test]
fn product_0134_baseline_configure_assembles_each_configuration_from_fields() {
	let mut state = AdapterState::default();

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
