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
}

#[derive(Default)]
pub struct AdapterState {
	pub stage: AdapterStage,
	pub selection: AdapterSelection,
	pub name: String,
	pub fields: BTreeMap<String, String>,
	pub configuration: AdapterConfiguration,
	pub connection: Option<Arc<RwLock<dyn DatabaseAdapter>>>,
}

impl AdapterConfiguration {
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
	}
}
