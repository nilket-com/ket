#[cfg(feature = "toron")]
use crate::adapters::toron;
use crate::adapters::{
	bigquery,
	common::{AdapterField, AdapterStage, DatabaseAdapter},
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

#[derive(Clone, Debug, Default)]
pub enum AdapterConfiguration {
	#[default]
	None,
	BigQuery {
		project_id: String,
	},
	MySQL {
		connection_string: String,
	},
	Parquet {
		input_path: String,
	},
	Postgres {
		connection_string: String,
	},
	SQLite {
		connection_string: String,
	},
	#[cfg(feature = "toron")]
	Toron {
		endpoint: String,
	},
}

#[derive(Default)]
pub struct AdapterState {
	pub stage: AdapterStage,
	pub selection: AdapterSelection,
	pub name: String,
	pub fields: BTreeMap<String, String>,
	pub configuration: AdapterConfiguration,
	pub connection: Option<Arc<RwLock<dyn DatabaseAdapter>>>,
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
		self.fields
			.extend(fields_for(&selection).iter().filter_map(|field| {
				field
					.default_value
					.map(|value| (field.key.into(), value.into()))
			}));
		self.selection = selection;
		self.stage = AdapterStage::Unconfigured;
	}

	/// The connection SQL executes against: the external adapter when one is
	/// connected, otherwise the session scratch database.
	pub fn sql_connection(&self) -> Option<&Arc<RwLock<dyn DatabaseAdapter>>> {
		self.connection.as_ref().or(self.scratch.as_ref())
	}

	pub fn scratch_is_active(&self) -> bool {
		self.connection.is_none() && self.scratch.is_some()
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
	/// Rebuild an adapter configuration after libaryon has decrypted an opaque
	/// saved connection profile. Keeping this mapping here prevents a frontend
	/// from needing to know the adapter-specific configuration shape.
	#[allow(dead_code)] // The encrypted-profile reader is the next consumer.
	pub fn from_saved(adapter_type: &str, config_value: &str) -> Self {
		match adapter_type {
			"BigQuery" => AdapterConfiguration::BigQuery {
				project_id: config_value.to_string(),
			},
			"MySQL" => AdapterConfiguration::MySQL {
				connection_string: config_value.to_string(),
			},
			"Parquet" => AdapterConfiguration::Parquet {
				input_path: config_value.to_string(),
			},
			"Postgres" => AdapterConfiguration::Postgres {
				connection_string: config_value.to_string(),
			},
			"SQLite" => AdapterConfiguration::SQLite {
				connection_string: config_value.to_string(),
			},
			#[cfg(feature = "toron")]
			"Toron" => AdapterConfiguration::Toron {
				endpoint: config_value.to_string(),
			},
			_ => AdapterConfiguration::None,
		}
	}
}

impl AdapterState {
	pub fn configure(&mut self) {
		match &self.selection {
			AdapterSelection::None => {
				self.configuration = AdapterConfiguration::None;
				self.stage = AdapterStage::Unconfigured;
			}
			AdapterSelection::MySQL => {
				let connection_string = self
					.fields
					.get("connection_string")
					.unwrap_or(&"mysql://root@localhost/mydb".to_string())
					.clone();
				self.configuration = AdapterConfiguration::MySQL { connection_string };
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
				let connection_string = self
					.fields
					.get("connection_string")
					.unwrap_or(&"postgresql://localhost".to_string())
					.clone();
				self.configuration = AdapterConfiguration::Postgres { connection_string };
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
				let endpoint = self
					.fields
					.get("endpoint")
					.unwrap_or(&"127.0.0.1:7742".to_string())
					.clone();
				self.configuration = AdapterConfiguration::Toron { endpoint };
				self.stage = AdapterStage::Configured;
			}
		}
	}

	pub async fn connect(config: AdapterConfiguration) -> Option<Arc<RwLock<dyn DatabaseAdapter>>> {
		match config {
			AdapterConfiguration::None => None,
			AdapterConfiguration::MySQL { connection_string } => {
				let opts = match mysql_async::Opts::from_url(&connection_string) {
					Ok(o) => o,
					Err(err) => {
						eprintln!("MySQL URL parse error: {err}");
						return None;
					}
				};
				let pool = mysql_async::Pool::new(opts);
				match tokio::time::timeout(Duration::from_secs(10), pool.get_conn()).await {
					Ok(Ok(_conn)) => Some(Arc::new(RwLock::new(mysql::MySQLAdapter { pool }))),
					Ok(Err(err)) => {
						eprintln!("MySQL connection error: {err}");
						None
					}
					Err(_elapsed) => {
						eprintln!("MySQL connection timed out after 10s");
						None
					}
				}
			}
			AdapterConfiguration::BigQuery { project_id } => {
				let (bq_config, _detected_project) = match ClientConfig::new_with_auth().await {
					Ok(res) => res,
					Err(_) => return None,
				};
				match Client::new(bq_config).await {
					Ok(client) => Some(Arc::new(RwLock::new(bigquery::BigQueryAdapter {
						client,
						project_id,
					}))),
					Err(_) => None,
				}
			}
			AdapterConfiguration::Parquet { input_path } => {
				let input_path = Path::new(&input_path);
				let input_ref_path = PlRefPath::try_from_path(input_path).unwrap();
				let hive_options = HiveOptions {
					enabled: Some(input_path.is_dir()),
					try_parse_dates: true,
					..Default::default()
				};
				let scan_args = ScanArgsParquet {
					hive_options,
					..Default::default()
				};
				let lf = LazyFrame::scan_parquet(input_ref_path, scan_args).unwrap();
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
				Some(Arc::new(RwLock::new(parquet::ParquetAdapter { context })))
			}
			AdapterConfiguration::Postgres { connection_string } => {
				let (client, connection) = match tokio_postgres::connect(
					&connection_string,
					tokio_postgres::NoTls,
				)
				.await
				{
					Ok(res) => res,
					Err(_) => return None,
				};
				tokio::spawn(async move {
					if let Err(e) = connection.await {
						eprintln!("Postgres connection error: {}", e);
					}
				});
				Some(Arc::new(RwLock::new(postgres::PostgresAdapter { client })))
			}
			AdapterConfiguration::SQLite { connection_string } => {
				if connection_string == "memory" {
					match AsyncConnection::open_in_memory().await.ok() {
						Some(aconn) => Some(Arc::new(RwLock::new(sqlite::SQLiteAdapter { aconn }))),
						None => None,
					}
				} else {
					match AsyncConnection::open(connection_string).await.ok() {
						Some(aconn) => Some(Arc::new(RwLock::new(sqlite::SQLiteAdapter { aconn }))),
						None => None,
					}
				}
			}
			#[cfg(feature = "toron")]
			AdapterConfiguration::Toron { endpoint } => {
				match toron::ToronAdapter::connect(&endpoint).await {
					Ok(adapter) => Some(Arc::new(RwLock::new(adapter))),
					Err(error) => {
						eprintln!(
							"Toron connection error: {error}\nStart local Nilket with: nilket"
						);
						None
					}
				}
			}
		}
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
	fn selecting_toron_prefills_the_canonical_local_endpoint() {
		let mut state = AdapterState::default();
		state.fields.insert("stale".into(), "old adapter".into());

		state.select(AdapterSelection::Toron);

		assert_eq!(state.fields.len(), 1);
		assert_eq!(
			state.fields.get("endpoint").map(String::as_str),
			Some("127.0.0.1:7742")
		);
	}

	#[test]
	fn placeholder_examples_do_not_become_connection_values() {
		let mut state = AdapterState::default();
		state.select(AdapterSelection::Postgres);
		assert!(state.fields.is_empty());
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
		let external = runtime
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
		assert!(matches!(
			AdapterConfiguration::from_saved("BigQuery", "warehouse"),
			AdapterConfiguration::BigQuery { project_id } if project_id == "warehouse"
		));
		assert!(matches!(
			AdapterConfiguration::from_saved("Postgres", "postgres://db"),
			AdapterConfiguration::Postgres { connection_string } if connection_string == "postgres://db"
		));
		assert!(matches!(
			AdapterConfiguration::from_saved("unknown", "ignored"),
			AdapterConfiguration::None
		));
		#[cfg(feature = "toron")]
		assert!(matches!(
			AdapterConfiguration::from_saved("Toron", "127.0.0.1:7742"),
			AdapterConfiguration::Toron { endpoint } if endpoint == "127.0.0.1:7742"
		));
	}

	#[test]
	fn provenance_identity_distinguishes_scratch_named_and_typed_connections() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let mut state = AdapterState::default();
		state.scratch = runtime.block_on(AdapterState::open_scratch());
		assert_eq!(state.connection_identity(), "scratch");

		state.connection = runtime.block_on(AdapterState::connect(AdapterConfiguration::SQLite {
			connection_string: "memory".into(),
		}));
		state.selection = super::AdapterSelection::SQLite;
		assert_eq!(state.connection_identity(), "SQLite");

		state.name = "local-dev".to_owned();
		assert_eq!(state.connection_identity(), "local-dev");
	}
}
