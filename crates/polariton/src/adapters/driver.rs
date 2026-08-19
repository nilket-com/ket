#[cfg(feature = "toron")]
use crate::adapters::toron;
use crate::adapters::{
	bigquery,
	common::{AdapterField, AdapterStage, CancelCapability, DatabaseAdapter},
	mysql, parquet, postgres, sqlite,
};
use gcloud_bigquery::client::{Client, ClientConfig};
use polars::prelude::{HiveOptions, LazyFrame, PlRefPath, ScanArgsParquet};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_rusqlite::Connection as AsyncConnection;

#[derive(Clone, Debug, Default)]
pub enum AdapterSelection {
	#[default]
	None,
	BigQuery,
	MySQL,
	Parquet,
	Postgres,
	SQLite,
	#[cfg(feature = "toron")]
	Toron,
}

impl AdapterSelection {
	/// The inverse of `adapter_type_str`, for rebuilding a selection from a
	/// saved profile's stored adapter type (PRODUCT-0134 §4).
	pub fn from_type_str(adapter_type: &str) -> Option<Self> {
		match adapter_type {
			"BigQuery" => Some(AdapterSelection::BigQuery),
			"MySQL" => Some(AdapterSelection::MySQL),
			"Parquet" => Some(AdapterSelection::Parquet),
			"Postgres" => Some(AdapterSelection::Postgres),
			"SQLite" => Some(AdapterSelection::SQLite),
			#[cfg(feature = "toron")]
			"Toron" => Some(AdapterSelection::Toron),
			_ => None,
		}
	}

	pub fn adapter_type_str(&self) -> &str {
		match self {
			AdapterSelection::None => "None",
			AdapterSelection::BigQuery => "BigQuery",
			AdapterSelection::MySQL => "MySQL",
			AdapterSelection::Parquet => "Parquet",
			AdapterSelection::Postgres => "Postgres",
			AdapterSelection::SQLite => "SQLite",
			#[cfg(feature = "toron")]
			AdapterSelection::Toron => "Toron",
		}
	}
}

#[derive(Clone, Default)]
pub enum AdapterConfiguration {
	#[default]
	None,
	BigQuery {
		project_id: String,
	},
	MySQL {
		connection_string: String,
	},
	/// PRODUCT-0134 §2: the decomposed MySQL field set. The port stays a
	/// string here and refuses non-numeric input at connect time.
	MySQLFields {
		host: String,
		port: String,
		database: String,
		user: String,
		password: String,
	},
	Parquet {
		input_path: String,
	},
	Postgres {
		connection_string: String,
	},
	/// PRODUCT-0134 §2: the decomposed Postgres field set.
	PostgresFields {
		host: String,
		port: String,
		database: String,
		user: String,
		password: String,
	},
	SQLite {
		connection_string: String,
	},
	#[cfg(feature = "toron")]
	Toron {
		endpoint: String,
	},
}

/// Manual so the secure password field can never reach a log line, panic
/// message, or diagnostic through a debug print (PRODUCT-0134 §5).
impl std::fmt::Debug for AdapterConfiguration {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			AdapterConfiguration::None => f.write_str("None"),
			AdapterConfiguration::BigQuery { project_id } => f
				.debug_struct("BigQuery")
				.field("project_id", project_id)
				.finish(),
			AdapterConfiguration::MySQL { connection_string } => f
				.debug_struct("MySQL")
				.field("connection_string", connection_string)
				.finish(),
			AdapterConfiguration::MySQLFields {
				host,
				port,
				database,
				user,
				password: _,
			} => f
				.debug_struct("MySQLFields")
				.field("host", host)
				.field("port", port)
				.field("database", database)
				.field("user", user)
				.field("password", &"<redacted>")
				.finish(),
			AdapterConfiguration::Parquet { input_path } => f
				.debug_struct("Parquet")
				.field("input_path", input_path)
				.finish(),
			AdapterConfiguration::Postgres { connection_string } => f
				.debug_struct("Postgres")
				.field("connection_string", connection_string)
				.finish(),
			AdapterConfiguration::PostgresFields {
				host,
				port,
				database,
				user,
				password: _,
			} => f
				.debug_struct("PostgresFields")
				.field("host", host)
				.field("port", port)
				.field("database", database)
				.field("user", user)
				.field("password", &"<redacted>")
				.finish(),
			AdapterConfiguration::SQLite { connection_string } => f
				.debug_struct("SQLite")
				.field("connection_string", connection_string)
				.finish(),
			#[cfg(feature = "toron")]
			AdapterConfiguration::Toron { endpoint } => {
				f.debug_struct("Toron").field("endpoint", endpoint).finish()
			}
		}
	}
}

#[derive(Default)]
pub struct AdapterState {
	pub stage: AdapterStage,
	pub selection: AdapterSelection,
	pub name: String,
	pub fields: BTreeMap<String, String>,
	/// PRODUCT-0134 §1: the ad hoc connection string is a separate input kind
	/// from the decomposed field set, never derived from it and never merged
	/// with it. Only adapters with an `ad_hoc_placeholder` render it.
	pub ad_hoc_connection_string: String,
	pub configuration: AdapterConfiguration,
	pub connection: Option<Arc<RwLock<dyn DatabaseAdapter>>>,
	/// PRODUCT-0135 §2: the external connection's cancellation capability,
	/// held beside the connection, never behind its lock.
	pub cancellation: CancelCapability,
	#[cfg(feature = "toron")]
	pub local_workspace: Option<nilket_local::LocalWorkspace>,
	/// Session-local in-memory SQLite scratch database. Always the SQL target
	/// when no external adapter is connected, so SQL panes and notebook cells
	/// work out of the box and share tables. Never persisted; disconnecting an
	/// external adapter falls back here instead of leaving SQL unusable. The
	/// `Arc` lives for the whole session, so scratch tables survive a round
	/// trip through an external connection.
	pub scratch: Option<Arc<RwLock<dyn DatabaseAdapter>>>,
}

impl AdapterState {
	/// Select an adapter and seed only values its descriptor declares as safe,
	/// usable defaults. Placeholder examples never become connection input.
	pub fn select(&mut self, selection: AdapterSelection) {
		self.fields.clear();
		self.ad_hoc_connection_string.clear();
		self.fields
			.extend(fields_for(&selection).iter().filter_map(|field| {
				field
					.default_value
					.map(|value| (field.key.into(), value.into()))
			}));
		self.selection = selection;
		self.stage = AdapterStage::Unconfigured;
	}

	/// PRODUCT-0134 §1/§10: a submission mixing the ad hoc connection string
	/// with touched decomposed fields refuses rather than choosing between the
	/// two input kinds. A field still holding its seeded default is untouched.
	pub fn input_kind_error(&self) -> Option<String> {
		if self.ad_hoc_connection_string.trim().is_empty() {
			return None;
		}
		let touched = fields_for(&self.selection).iter().any(|field| {
			let value = self
				.fields
				.get(field.key)
				.map(String::as_str)
				.unwrap_or_default();
			!value.trim().is_empty() && value.trim() != field.default_value.unwrap_or_default()
		});
		touched.then(|| {
			"Use either the connection fields or the ad hoc connection string, not both."
				.to_owned()
		})
	}

	/// The connection SQL executes against: the external adapter when one is
	/// connected, otherwise the session scratch database.
	pub fn sql_connection(&self) -> Option<&Arc<RwLock<dyn DatabaseAdapter>>> {
		self.connection.as_ref().or(self.scratch.as_ref())
	}

	#[cfg(feature = "toron")]
	pub fn rune_connection(&self) -> Option<&Arc<RwLock<dyn DatabaseAdapter>>> {
		matches!(self.selection, AdapterSelection::Toron)
			.then_some(())
			.and(self.connection.as_ref())
	}

	#[cfg(feature = "toron")]
	pub fn local_workspace_unavailable(&self) -> String {
		self.local_workspace.as_ref().map_or_else(
			|| "Local workspace discovery has not completed.".to_owned(),
			ToString::to_string,
		)
	}

	pub fn scratch_is_active(&self) -> bool {
		self.connection.is_none() && self.scratch.is_some()
	}

	/// The capability governing the connection SQL currently executes against.
	/// The scratch fallback is Stopped waiting, per the accepted table.
	pub fn active_cancel_capability(&self) -> CancelCapability {
		if self.connection.is_some() {
			self.cancellation.clone()
		} else {
			CancelCapability::StopWaiting
		}
	}

	/// Dispatch-time identity recorded in result provenance.
	pub fn connection_identity(&self) -> String {
		if self.scratch_is_active() {
			return "scratch".to_owned();
		}
		if !self.name.trim().is_empty() {
			return self.name.clone();
		}
		self.selection.adapter_type_str().to_owned()
	}

	pub async fn open_scratch() -> Option<Arc<RwLock<dyn DatabaseAdapter>>> {
		let aconn = AsyncConnection::open_in_memory().await.ok()?;
		Some(Arc::new(RwLock::new(sqlite::SQLiteAdapter { aconn })))
	}
}

impl AdapterConfiguration {
	/// Rebuild an adapter configuration from a decrypted saved profile's field
	/// map — the same shape the configuration editor produces, per
	/// PRODUCT-0134 §3. Keeping this mapping here prevents a frontend from
	/// needing to know the adapter-specific configuration shape, and keeps
	/// exactly one path from stored profile to live configuration.
	pub fn from_saved(adapter_type: &str, fields: &BTreeMap<String, String>) -> Self {
		let value = |key: &str| fields.get(key).cloned().unwrap_or_default();
		match adapter_type {
			"BigQuery" => AdapterConfiguration::BigQuery {
				project_id: value("project_id"),
			},
			"MySQL" => AdapterConfiguration::MySQLFields {
				host: value("host"),
				port: value("port"),
				database: value("database"),
				user: value("user"),
				password: value("password"),
			},
			"Parquet" => AdapterConfiguration::Parquet {
				input_path: value("input_path"),
			},
			"Postgres" => AdapterConfiguration::PostgresFields {
				host: value("host"),
				port: value("port"),
				database: value("database"),
				user: value("user"),
				password: value("password"),
			},
			"SQLite" => AdapterConfiguration::SQLite {
				connection_string: value("connection_string"),
			},
			#[cfg(feature = "toron")]
			"Toron" => AdapterConfiguration::Toron {
				endpoint: value("endpoint"),
			},
			_ => AdapterConfiguration::None,
		}
	}
}

impl AdapterState {
	fn field_value(&self, key: &str) -> String {
		self.fields.get(key).cloned().unwrap_or_default()
	}

	pub fn configure(&mut self) {
		match &self.selection {
			AdapterSelection::None => {
				self.configuration = AdapterConfiguration::None;
				self.stage = AdapterStage::Unconfigured;
			}
			AdapterSelection::MySQL => {
				let ad_hoc = self.ad_hoc_connection_string.trim();
				self.configuration = if ad_hoc.is_empty() {
					AdapterConfiguration::MySQLFields {
						host: self.field_value("host"),
						port: self.field_value("port"),
						database: self.field_value("database"),
						user: self.field_value("user"),
						password: self.field_value("password"),
					}
				} else {
					AdapterConfiguration::MySQL {
						connection_string: ad_hoc.to_owned(),
					}
				};
				self.stage = AdapterStage::Configured;
			}
			AdapterSelection::BigQuery => {
				let project_id = self.fields.get("project_id").cloned().unwrap_or_default();
				self.configuration = AdapterConfiguration::BigQuery { project_id };
				self.stage = AdapterStage::Configured;
			}
			AdapterSelection::Parquet => {
				let input_path = self.fields.get("input_path").unwrap().clone();
				self.configuration = AdapterConfiguration::Parquet { input_path };
				self.stage = AdapterStage::Configured;
			}
			AdapterSelection::Postgres => {
				let ad_hoc = self.ad_hoc_connection_string.trim();
				self.configuration = if ad_hoc.is_empty() {
					AdapterConfiguration::PostgresFields {
						host: self.field_value("host"),
						port: self.field_value("port"),
						database: self.field_value("database"),
						user: self.field_value("user"),
						password: self.field_value("password"),
					}
				} else {
					AdapterConfiguration::Postgres {
						connection_string: ad_hoc.to_owned(),
					}
				};
				self.stage = AdapterStage::Configured;
			}
			AdapterSelection::SQLite => {
				let connection_string = self
					.fields
					.get("connection_string")
					.unwrap_or(&"memory".to_string())
					.clone();
				self.configuration = AdapterConfiguration::SQLite { connection_string };
				self.stage = AdapterStage::Configured;
			}
			#[cfg(feature = "toron")]
			AdapterSelection::Toron => {
				let endpoint = self.fields.get("endpoint").cloned().unwrap_or_default();
				self.configuration = AdapterConfiguration::Toron { endpoint };
				self.stage = AdapterStage::Configured;
			}
		}
	}

	/// PRODUCT-0135 §2: the capability is acquired here, at connect, while
	/// nothing holds the adapter lock, and travels beside the connection.
	pub async fn connect(
		config: AdapterConfiguration,
	) -> Result<(Arc<RwLock<dyn DatabaseAdapter>>, CancelCapability), String> {
		match config {
			AdapterConfiguration::None => Err("No adapter is configured.".to_owned()),
			AdapterConfiguration::MySQL { connection_string } => {
				let opts = mysql_async::Opts::from_url(&connection_string)
					.map_err(|error| format!("MySQL URL: {error}"))?;
				let pool = mysql_async::Pool::new(opts);
				match tokio::time::timeout(Duration::from_secs(10), pool.get_conn()).await {
					Ok(Ok(_conn)) => Ok((
						Arc::new(RwLock::new(mysql::MySQLAdapter { pool })) as _,
						CancelCapability::StopWaiting,
					)),
					Ok(Err(error)) => Err(format!("MySQL connection: {error}")),
					Err(_) => Err("MySQL connection timed out after 10s".to_owned()),
				}
			}
			AdapterConfiguration::MySQLFields {
				host,
				port,
				database,
				user,
				password,
			} => {
				let opts = mysql::opts_from_fields(&host, &port, &database, &user, &password)?;
				let pool = mysql_async::Pool::new(opts);
				match tokio::time::timeout(Duration::from_secs(10), pool.get_conn()).await {
					Ok(Ok(_conn)) => Ok((
						Arc::new(RwLock::new(mysql::MySQLAdapter { pool })) as _,
						CancelCapability::StopWaiting,
					)),
					Ok(Err(error)) => Err(format!("MySQL connection: {error}")),
					Err(_) => Err("MySQL connection timed out after 10s".to_owned()),
				}
			}
			AdapterConfiguration::BigQuery { project_id } => {
				let (bq_config, _detected_project) = ClientConfig::new_with_auth()
					.await
					.map_err(|error| format!("BigQuery authentication: {error}"))?;
				match Client::new(bq_config).await {
					Ok(client) => Ok((
						Arc::new(RwLock::new(bigquery::BigQueryAdapter {
							client,
							project_id,
						})) as _,
						CancelCapability::StopWaiting,
					)),
					Err(error) => Err(format!("BigQuery client: {error}")),
				}
			}
			AdapterConfiguration::Parquet { input_path } => {
				let input_path = Path::new(&input_path);
				let input_ref_path = PlRefPath::try_from_path(input_path)
					.map_err(|error| format!("Parquet path: {error}"))?;
				let hive_options = HiveOptions {
					enabled: Some(input_path.is_dir()),
					try_parse_dates: true,
					..Default::default()
				};
				let scan_args = ScanArgsParquet {
					hive_options,
					..Default::default()
				};
				let lf = LazyFrame::scan_parquet(input_ref_path, scan_args)
					.map_err(|error| format!("Parquet scan: {error}"))?;
				let table_name = if input_path.is_file() {
					input_path
						.file_prefix()
						.and_then(|s| s.to_str())
						.unwrap_or("temp_table")
				} else {
					input_path
						.file_name()
						.and_then(|s| s.to_str())
						.unwrap_or("temp_table")
				};
				let context = polars::sql::SQLContext::new();
				context.register(table_name, lf);
				// The accepted table's Refusal row: polars executes
				// synchronously; even abandonment waits.
				Ok((
					Arc::new(RwLock::new(parquet::ParquetAdapter { context })) as _,
					CancelCapability::Refuse,
				))
			}
			AdapterConfiguration::Postgres { connection_string } => {
				let tls = tokio_postgres_rustls::MakeRustlsConnect::with_webpki_roots();
				let (client, connection) =
					tokio_postgres::connect(&connection_string, tls)
						.await
						.map_err(|error| format!("Postgres connection: {error}"))?;
				tokio::spawn(async move {
					if let Err(e) = connection.await {
						eprintln!("Postgres connection error: {}", e);
					}
				});
				let cancel = CancelCapability::Engine(Arc::new(postgres::PostgresCancel {
					token: client.cancel_token(),
				}));
				Ok((
					Arc::new(RwLock::new(postgres::PostgresAdapter { client })) as _,
					cancel,
				))
			}
			AdapterConfiguration::PostgresFields {
				host,
				port,
				database,
				user,
				password,
			} => {
				let config =
					postgres::config_from_fields(&host, &port, &database, &user, &password)?;
				let tls = tokio_postgres_rustls::MakeRustlsConnect::with_webpki_roots();
				let (client, connection) = config
					.connect(tls)
					.await
					.map_err(|error| format!("Postgres connection: {error}"))?;
				tokio::spawn(async move {
					if let Err(e) = connection.await {
						eprintln!("Postgres connection error: {}", e);
					}
				});
				let cancel = CancelCapability::Engine(Arc::new(postgres::PostgresCancel {
					token: client.cancel_token(),
				}));
				Ok((
					Arc::new(RwLock::new(postgres::PostgresAdapter { client })) as _,
					cancel,
				))
			}
			AdapterConfiguration::SQLite { connection_string } => {
				// `tokio-rusqlite` 0.7.0 exposes no interrupt handle: the
				// accepted table's Stopped waiting row.
				if connection_string == "memory" {
					AsyncConnection::open_in_memory()
						.await
						.map(|aconn| {
							(
								Arc::new(RwLock::new(sqlite::SQLiteAdapter { aconn })) as _,
								CancelCapability::StopWaiting,
							)
						})
						.map_err(|error| format!("SQLite memory connection: {error}"))
				} else {
					AsyncConnection::open(connection_string)
						.await
						.map(|aconn| {
							(
								Arc::new(RwLock::new(sqlite::SQLiteAdapter { aconn })) as _,
								CancelCapability::StopWaiting,
							)
						})
						.map_err(|error| format!("SQLite connection: {error}"))
				}
			}
			#[cfg(feature = "toron")]
			AdapterConfiguration::Toron { endpoint } => toron::ToronAdapter::connect(&endpoint)
				.await
				.map(|adapter| {
					let cancel = adapter.cancel_capability();
					(Arc::new(RwLock::new(adapter)) as _, cancel)
				})
				.map_err(|error| format!("Toron connection: {error}")),
		}
	}
}

/// Adapters that keep an ad hoc connection-string input alongside their
/// decomposed field set (PRODUCT-0134 §1). The other adapters' single field is
/// their whole configuration and needs no second kind.
pub fn ad_hoc_placeholder(selection: &AdapterSelection) -> Option<&'static str> {
	match selection {
		AdapterSelection::MySQL => Some(mysql::AD_HOC_PLACEHOLDER),
		AdapterSelection::Postgres => Some(postgres::AD_HOC_PLACEHOLDER),
		_ => None,
	}
}

pub fn fields_for(selection: &AdapterSelection) -> &'static [AdapterField] {
	match selection {
		AdapterSelection::None => &[],
		AdapterSelection::BigQuery => bigquery::FIELDS,
		AdapterSelection::MySQL => mysql::FIELDS,
		AdapterSelection::Parquet => parquet::FIELDS,
		AdapterSelection::Postgres => postgres::FIELDS,
		AdapterSelection::SQLite => sqlite::FIELDS,
		#[cfg(feature = "toron")]
		AdapterSelection::Toron => toron::FIELDS,
	}
}

#[cfg(test)]
mod tests {
	use super::{AdapterConfiguration, AdapterSelection, AdapterState};

	#[cfg(feature = "toron")]
	#[test]
	fn selecting_toron_does_not_guess_a_local_endpoint() {
		let mut state = AdapterState::default();
		state.fields.insert("stale".into(), "old adapter".into());

		state.select(AdapterSelection::Toron);

		assert!(state.fields.is_empty());
	}

	#[test]
	fn placeholder_examples_do_not_become_connection_values() {
		let mut state = AdapterState::default();
		state.select(AdapterSelection::Postgres);
		// The port's declared default seeds like SQLite's `memory`; the
		// placeholder examples (host, database, user) must not.
		assert_eq!(
			state.fields.iter().collect::<Vec<_>>(),
			[(&"port".to_owned(), &"5432".to_owned())]
		);
	}

	#[test]
	fn postgres_tls_connector_constructs_with_the_production_provider() {
		crate::ensure_rustls_provider();
		let _ = tokio_postgres_rustls::MakeRustlsConnect::with_webpki_roots();
	}

	#[test]
	fn sql_connection_prefers_external_and_falls_back_to_scratch() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let mut state = AdapterState::default();
		assert!(state.sql_connection().is_none());
		assert!(!state.scratch_is_active());

		state.scratch = runtime.block_on(AdapterState::open_scratch());
		assert!(state.scratch.is_some());
		assert!(state.scratch_is_active());
		let scratch = state.sql_connection().expect("scratch target").clone();
		assert!(std::sync::Arc::ptr_eq(
			&scratch,
			state.scratch.as_ref().unwrap()
		));

		// An external connection takes over; disconnecting falls back to the
		// same scratch instance, so its tables survive the round trip.
		let (external, _cancel) = runtime
			.block_on(AdapterState::connect(AdapterConfiguration::SQLite {
				connection_string: "memory".into(),
			}))
			.expect("external sqlite");
		state.connection = Some(external.clone());
		assert!(!state.scratch_is_active());
		assert!(std::sync::Arc::ptr_eq(
			state.sql_connection().expect("external target"),
			&external
		));
		state.connection = None;
		assert!(std::sync::Arc::ptr_eq(
			state.sql_connection().expect("scratch target"),
			&scratch
		));
	}

	#[test]
	fn saved_configuration_keeps_adapter_specific_field_names() {
		use std::collections::BTreeMap;
		let map = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
			pairs
				.iter()
				.map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
				.collect()
		};
		assert!(matches!(
			AdapterConfiguration::from_saved("BigQuery", &map(&[("project_id", "warehouse")])),
			AdapterConfiguration::BigQuery { project_id } if project_id == "warehouse"
		));
		assert!(matches!(
			AdapterConfiguration::from_saved(
				"Postgres",
				&map(&[
					("host", "h"),
					("port", "6432"),
					("database", "db"),
					("user", "u"),
					("password", "pw"),
				]),
			),
			AdapterConfiguration::PostgresFields { host, port, database, user, password }
				if host == "h" && port == "6432" && database == "db" && user == "u" && password == "pw"
		));
		assert!(matches!(
			AdapterConfiguration::from_saved("MySQL", &map(&[("host", "h"), ("user", "u")])),
			AdapterConfiguration::MySQLFields { host, port, database, user, password }
				if host == "h" && port.is_empty() && database.is_empty()
					&& user == "u" && password.is_empty()
		));
		assert!(matches!(
			AdapterConfiguration::from_saved("SQLite", &map(&[("connection_string", "memory")])),
			AdapterConfiguration::SQLite { connection_string } if connection_string == "memory"
		));
		assert!(matches!(
			AdapterConfiguration::from_saved("unknown", &map(&[("x", "ignored")])),
			AdapterConfiguration::None
		));
		#[cfg(feature = "toron")]
		assert!(matches!(
			AdapterConfiguration::from_saved("Toron", &map(&[("endpoint", "loopback.invalid:1")])),
			AdapterConfiguration::Toron { endpoint } if endpoint == "loopback.invalid:1"
		));
	}

	/// PRODUCT-0135 §3: the connectable rows of the accepted capability table,
	/// and the scratch fallback. SQLite is Stopped waiting (`tokio-rusqlite`
	/// exposes no interrupt handle); Parquet refuses (synchronous execution).
	#[test]
	fn product_0135_capabilities_match_the_accepted_table() {
		use crate::adapters::common::CancelCapability;
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let (_conn, capability) = runtime
			.block_on(AdapterState::connect(AdapterConfiguration::SQLite {
				connection_string: "memory".into(),
			}))
			.expect("sqlite connects");
		assert!(matches!(capability, CancelCapability::StopWaiting));

		let dir = std::env::temp_dir().join(format!(
			"polariton-0135-cap-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).expect("temp dir");
		let path = dir.join("cap_fixture.parquet");
		let mut frame = polars::df!("x" => [1_i64]).expect("frame");
		polars::prelude::ParquetWriter::new(std::fs::File::create(&path).expect("file"))
			.finish(&mut frame)
			.expect("written");
		let (_conn, capability) = runtime
			.block_on(AdapterState::connect(AdapterConfiguration::Parquet {
				input_path: path.to_str().expect("utf8").to_owned(),
			}))
			.expect("parquet connects");
		assert!(matches!(capability, CancelCapability::Refuse));
		let _ = std::fs::remove_dir_all(&dir);

		// The scratch fallback: no external connection means Stopped waiting,
		// whatever capability a previous connection left behind.
		let mut state = AdapterState::default();
		state.cancellation = CancelCapability::Refuse;
		assert!(matches!(
			state.active_cancel_capability(),
			CancelCapability::StopWaiting
		));
	}

	#[test]
	fn provenance_identity_distinguishes_scratch_named_and_typed_connections() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let mut state = AdapterState::default();
		state.scratch = runtime.block_on(AdapterState::open_scratch());
		assert_eq!(state.connection_identity(), "scratch");

		state.connection = Some(
			runtime
				.block_on(AdapterState::connect(AdapterConfiguration::SQLite {
					connection_string: "memory".into(),
				}))
				.expect("external sqlite")
				.0,
		);
		state.selection = super::AdapterSelection::SQLite;
		assert_eq!(state.connection_identity(), "SQLite");

		state.name = "local-dev".to_owned();
		assert_eq!(state.connection_identity(), "local-dev");
	}
}
