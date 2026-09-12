use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use crate::adapters::sql_compound::{CompoundTail, plan};
use crate::result_artifact::ResultArtifact;
use async_trait::async_trait;
use mysql_async::{Pool, Value as MySqlValue, prelude::Queryable};
use polars::{
	datatypes::AnyValue,
	frame::{DataFrame, column::Column},
	series::Series,
};
use sqlparser::dialect::MySqlDialect;
use std::time::Duration;

pub const AD_HOC_PLACEHOLDER: &str = "mysql://user:p%40ssword@host:3306/database";

/// PRODUCT-0134 §2: connection and authentication fields only. The password is
/// the one secure field; vendor DSN options stay on the ad hoc string.
pub const FIELDS: &[AdapterField] = &[
	AdapterField {
		key: "host",
		placeholder: "db.example.com",
		default_value: None,
		field_type: &AdapterFieldType::Text,
		is_secure: false,
		is_optional: false,
	},
	AdapterField {
		key: "port",
		placeholder: "3306",
		default_value: Some("3306"),
		field_type: &AdapterFieldType::Text,
		is_secure: false,
		is_optional: false,
	},
	AdapterField {
		key: "database",
		placeholder: "mydb",
		default_value: None,
		field_type: &AdapterFieldType::Text,
		is_secure: false,
		is_optional: false,
	},
	AdapterField {
		key: "user",
		placeholder: "root",
		default_value: None,
		field_type: &AdapterFieldType::Text,
		is_secure: false,
		is_optional: false,
	},
	AdapterField {
		key: "password",
		placeholder: "",
		default_value: None,
		field_type: &AdapterFieldType::Text,
		is_secure: true,
		is_optional: false,
	},
];

/// Assemble the decomposed field set into driver arguments. This is the single
/// assembly point for the MySQL field kind (PRODUCT-0134 §3): field values are
/// passed to the driver verbatim, so a password needs no percent-encoding. The
/// port refuses non-numeric input here, at connect time.
pub fn opts_from_fields(
	host: &str,
	port: &str,
	database: &str,
	user: &str,
	password: &str,
) -> Result<mysql_async::Opts, String> {
	if host.trim().is_empty() {
		return Err("MySQL host is required".to_owned());
	}
	if user.trim().is_empty() {
		return Err("MySQL user is required".to_owned());
	}
	let port: u16 = if port.trim().is_empty() {
		3306
	} else {
		port.trim()
			.parse()
			.map_err(|_| format!("MySQL port must be a number, got: {port}"))?
	};
	let mut builder = mysql_async::OptsBuilder::default()
		.ip_or_hostname(host.trim())
		.tcp_port(port)
		.user(Some(user.trim()));
	if !database.trim().is_empty() {
		builder = builder.db_name(Some(database.trim()));
	}
	if !password.is_empty() {
		builder = builder.pass(Some(password));
	}
	Ok(mysql_async::Opts::from(builder))
}

pub struct MySQLAdapter {
	pub pool: Pool,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
const DIALECT: MySqlDialect = MySqlDialect {};

#[async_trait]
impl DatabaseAdapter for MySQLAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let planned = match plan(&DIALECT, code) {
			Ok(planned) => planned,
			Err(err) => return ExecutionResult::Err(format!("MySQL parse error: {}", err)),
		};
		let mut conn = match tokio::time::timeout(Duration::from_secs(30), self.pool.get_conn())
			.await
		{
			Ok(Ok(c)) => c,
			Ok(Err(err)) => return ExecutionResult::Err(format!("MySQL connection error: {err}")),
			Err(_) => return ExecutionResult::Err("MySQL connection timed out".to_string()),
		};
		match planned.tail {
			CompoundTail::Empty => ExecutionResult::None,
			CompoundTail::Command(code) if planned.statements > 1 => conn
				.query_drop(code)
				.await
				.map(|_| ExecutionResult::Batch {
					statements: planned.statements,
				})
				.unwrap_or_else(|err| ExecutionResult::Err(err.to_string())),
			CompoundTail::Command(code) => match conn.prep(code).await {
				Err(err) => ExecutionResult::Err(err.to_string()),
				Ok(stmt) => conn
					.exec_drop(stmt, ())
					.await
					.map(|_| ExecutionResult::Affected(conn.affected_rows()))
					.unwrap_or_else(|err| ExecutionResult::Err(err.to_string())),
			},
			CompoundTail::Query { prefix, query } => {
				if planned.statements > 1
					&& let Err(error) = conn.query_drop(prefix).await
				{
					return ExecutionResult::Err(error.to_string());
				}
				mysql_to_df(conn, query)
					.await
					.map(|frame| ExecutionResult::Rows {
						artifact: ResultArtifact::from_polars(std::sync::Arc::new(frame)),
						statements: planned.statements,
					})
					.unwrap_or_else(|err| ExecutionResult::Err(err.to_string()))
			}
		}
	}
}

async fn mysql_to_df(mut conn: mysql_async::Conn, query: &str) -> Result<DataFrame, BoxError> {
	let stmt = conn.prep(query).await?;
	let col_names: Vec<String> = stmt
		.columns()
		.iter()
		.map(|c| c.name_str().to_string())
		.collect();
	let width = col_names.len();
	let mut column_data: Vec<Vec<AnyValue>> = vec![Vec::new(); width];
	let rows: Vec<mysql_async::Row> = conn.exec(stmt, ()).await?;
	for mut row in rows {
		for (i, values) in column_data.iter_mut().enumerate().take(width) {
			let val = row.take::<MySqlValue, usize>(i).unwrap_or(MySqlValue::NULL);
			values.push(mysql_value_to_any(val));
		}
	}
	let polars_columns: Vec<Column> = col_names
		.into_iter()
		.enumerate()
		.map(|(i, name)| -> Result<Column, BoxError> {
			let series = Series::from_any_values(name.into(), &column_data[i], false)?;
			Ok(Column::from(series))
		})
		.collect::<Result<Vec<_>, _>>()?;
	let height = polars_columns.first().map(|c| c.len()).unwrap_or(0);
	Ok(DataFrame::new(height, polars_columns)?)
}

fn mysql_value_to_any(val: MySqlValue) -> AnyValue<'static> {
	match val {
		MySqlValue::NULL => AnyValue::Null,
		MySqlValue::Int(i) => AnyValue::Int64(i),
		MySqlValue::UInt(u) => AnyValue::UInt64(u),
		MySqlValue::Float(f) => AnyValue::Float32(f),
		MySqlValue::Double(d) => AnyValue::Float64(d),
		MySqlValue::Bytes(b) => match String::from_utf8(b) {
			Ok(s) => AnyValue::StringOwned(s.into()),
			Err(e) => AnyValue::BinaryOwned(e.into_bytes()),
		},
		MySqlValue::Date(year, month, day, hour, min, sec, micros) => {
			let unix_micros =
				chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
					.and_then(|d| d.and_hms_micro_opt(hour as u32, min as u32, sec as u32, micros))
					.map(|ndt| ndt.and_utc().timestamp_micros())
					.unwrap_or(0);
			AnyValue::Datetime(unix_micros, polars::datatypes::TimeUnit::Microseconds, None)
		}
		MySqlValue::Time(..) => AnyValue::Null,
	}
}
