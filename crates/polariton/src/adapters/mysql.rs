use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use crate::adapters::sql_compound::{CompoundTail, plan};
use async_trait::async_trait;
use mysql_async::{Pool, Value as MySqlValue, prelude::Queryable};
use polars::{
	datatypes::AnyValue,
	frame::{DataFrame, column::Column},
	series::Series,
};
use sqlparser::dialect::MySqlDialect;
use std::time::Duration;

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "connection_string",
	placeholder: "mysql://user:p%40ssword@host:3306/database",
	default_value: None,
	field_type: &AdapterFieldType::Text,
	is_secure: false,
}];

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
						frame,
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
