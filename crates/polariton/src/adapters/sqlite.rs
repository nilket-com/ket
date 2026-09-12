use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use crate::adapters::sql_compound::{CompoundTail, plan};
use crate::result_artifact::ResultArtifact;
use async_trait::async_trait;
use polars::{
	datatypes::AnyValue,
	frame::{DataFrame, column::Column},
	series::Series,
};
use sqlparser::dialect::SQLiteDialect;
use tokio_rusqlite::{
	Connection as AsyncConnection,
	rusqlite::{Connection as SyncConnection, types::ValueRef},
};

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "connection_string",
	placeholder: "memory",
	default_value: Some("memory"),
	field_type: &AdapterFieldType::Text,
	is_secure: false,
	is_optional: false,
}];

pub struct SQLiteAdapter {
	pub aconn: AsyncConnection,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
const DIALECT: SQLiteDialect = SQLiteDialect {};

#[async_trait]
impl DatabaseAdapter for SQLiteAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let planned = match plan(&DIALECT, code) {
			Ok(planned) => planned,
			Err(err) => {
				return ExecutionResult::Err(format!("SQLite parse error: {}", err));
			}
		};
		match planned.tail {
			CompoundTail::Empty => ExecutionResult::None,
			CompoundTail::Command(code) if planned.statements > 1 => {
				let code = code.to_owned();
				self.aconn
					.call(move |conn| conn.execute_batch(&code))
					.await
					.map(|_| ExecutionResult::Batch {
						statements: planned.statements,
					})
					.unwrap_or_else(|err| ExecutionResult::Err(err.to_string()))
			}
			CompoundTail::Command(code) => {
				let code = code.to_owned();
				self.aconn
					.call(move |conn| conn.execute(&code, []))
					.await
					.map(|affected| ExecutionResult::Affected(affected as u64))
					.unwrap_or_else(|err| ExecutionResult::Err(err.to_string()))
			}
			CompoundTail::Query { prefix, query } => {
				let prefix = prefix.to_owned();
				let query = query.to_owned();
				self.aconn
					.call(move |conn| -> Result<DataFrame, BoxError> {
						if planned.statements > 1 {
							conn.execute_batch(&prefix)?;
						}
						sqlite_to_df(conn, &query)
					})
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

pub fn sqlite_to_df(conn: &SyncConnection, code: &str) -> Result<DataFrame, BoxError> {
	let mut stmt = conn.prepare(code)?;
	let col_names: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();
	let width = col_names.len();
	// Read the tail exactly once. A preliminary `count(*) from ({tail})`
	// looks like harmless preallocation, but it evaluates user SQL twice and
	// breaks the compound execution contract for volatile functions.
	let mut column_data: Vec<Vec<AnyValue>> = vec![Vec::new(); width];
	let mut rows = stmt.query([])?;
	while let Some(row) = rows.next()? {
		for (indx, bucket) in column_data.iter_mut().enumerate() {
			let val = match row.get_ref(indx)? {
				ValueRef::Null => AnyValue::Null,
				ValueRef::Integer(num) => AnyValue::Int64(num),
				ValueRef::Real(flt) => AnyValue::Float64(flt),
				ValueRef::Text(txt) => AnyValue::StringOwned(std::str::from_utf8(txt)?.into()),
				ValueRef::Blob(blb) => AnyValue::BinaryOwned(blb.to_vec()),
			};
			bucket.push(val);
		}
	}
	let total_rows = column_data.first().map_or(0, Vec::len);
	let polars_columns: Vec<Column> = col_names
		.into_iter()
		.enumerate()
		.map(|(indx, name)| {
			let ser = Series::from_any_values(name.into(), &column_data[indx], true).unwrap();
			Column::from(ser)
		})
		.collect();
	let df = DataFrame::new(total_rows, polars_columns)?;
	Ok(df)
}

#[cfg(test)]
mod tests {
	use super::SQLiteAdapter;
	use crate::adapters::common::{DatabaseAdapter, ExecutionResult};
	use tokio_rusqlite::Connection;

	async fn adapter() -> SQLiteAdapter {
		SQLiteAdapter {
			aconn: Connection::open_in_memory()
				.await
				.expect("in-memory SQLite"),
		}
	}

	#[tokio::test]
	async fn compound_query_returns_the_tail_table_after_ordered_prefix_effects() {
		let mut adapter = adapter().await;
		let result = adapter
			.dispatch(concat!(
				"create table values_seen (value text);\n",
				"insert into values_seen values ('semi;colon'), ('tail');\n",
				"select value from values_seen order by rowid;\n",
			))
			.await;

		let ExecutionResult::Rows {
			artifact,
			statements,
		} = result
		else {
			panic!("query tail must return a DataFrame");
		};
		let frame = artifact.projection().expect("SQLite projection");
		assert_eq!(statements, 3);
		assert_eq!(frame.shape(), (2, 1));
		let values = frame
			.column("value")
			.expect("value column")
			.str()
			.expect("string values");
		assert_eq!(values.get(0), Some("semi;colon"));
		assert_eq!(values.get(1), Some("tail"));
	}

	#[tokio::test]
	async fn command_tail_returns_no_artifact() {
		let mut adapter = adapter().await;
		let result = adapter
			.dispatch("create table command_only (x integer); insert into command_only values (1);")
			.await;

		assert!(matches!(result, ExecutionResult::Batch { statements: 2 }));
	}

	#[tokio::test]
	async fn prefix_failure_short_circuits_the_tail_product() {
		let mut adapter = adapter().await;
		let result = adapter
			.dispatch(
				"create table exists_now (x integer); insert into missing values (1); select 99 as x;",
			)
			.await;

		let ExecutionResult::Err(message) = result else {
			panic!("a failed prefix must suppress the tail artifact");
		};
		assert!(message.contains("missing"), "{message}");
	}
}
