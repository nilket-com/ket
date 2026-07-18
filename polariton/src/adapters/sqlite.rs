use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use async_trait::async_trait;
use polars::{
	datatypes::AnyValue,
	frame::{column::Column, DataFrame},
	series::Series,
};
use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};
use tokio_rusqlite::{
	rusqlite::{types::ValueRef, Connection as SyncConnection},
	Connection as AsyncConnection,
};

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "connection_string",
	value: "memory",
	field_type: &AdapterFieldType::Text,
	is_secure: false,
}];

pub struct SQLiteAdapter {
	pub aconn: AsyncConnection,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
const DIALECT: SQLiteDialect = SQLiteDialect {};

#[async_trait]
impl DatabaseAdapter for SQLiteAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let code = code.to_string();
		let ast = match Parser::parse_sql(&DIALECT, &code) {
			Ok(nodes) => nodes,
			Err(err) => {
				return ExecutionResult::Err(format!("SQLite parse error: {}", err));
			}
		};
		match ast.as_slice() {
			[_, _, ..] => self
				.aconn
				.call(move |conn| conn.execute_batch(&code))
				.await
				.map(|_| {
					ExecutionResult::Batch(vec![ExecutionResult::CommandCompleted(
						"Batch complete.".to_string(),
					)])
				})
				.unwrap_or_else(|err| ExecutionResult::Err(err.to_string())),
			[Statement::Query(_)] => self
				.aconn
				.call(move |conn| sqlite_to_df(conn, &code))
				.await
				.map(ExecutionResult::Rows)
				.unwrap_or_else(|err| ExecutionResult::Err(err.to_string())),
			[Statement::Insert { .. }] | [Statement::Update { .. }] | [_] => self
				.aconn
				.call(move |conn| conn.execute(&code, []))
				.await
				.map(|x| ExecutionResult::CommandCompleted(x.to_string()))
				.unwrap_or_else(|err| ExecutionResult::Err(err.to_string())),
			[] => ExecutionResult::None,
		}
	}
}

pub fn sqlite_to_df(conn: &SyncConnection, code: &str) -> Result<DataFrame, BoxError> {
	let mut stmt = conn.prepare(code)?;
	let col_names: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();
	let width = col_names.len();
	let total_rows: usize =
		conn.query_row(&format!("select count(1) from ({})", code), [], |row| {
			row.get(0)
		})?;
	let mut column_data: Vec<Vec<AnyValue>> = vec![Vec::with_capacity(total_rows); width];
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
