use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use polars::{
	datatypes::AnyValue,
	frame::{column::Column, DataFrame},
	series::Series,
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use std::pin::pin;
use tokio_postgres::{types::Type, Client};
use tokio_stream::StreamExt;

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "connection_string",
	value: "postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&param2=value2]",
	field_type: &AdapterFieldType::Text,
	is_secure: false,
}];

pub struct PostgresAdapter {
	pub client: Client,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let code = code.to_string();
		let ast = match Parser::parse_sql(&DIALECT, &code) {
			Ok(nodes) => nodes,
			Err(err) => {
				return ExecutionResult::Err(format!("Postgres parse error: {}", err));
			}
		};
		match ast.as_slice() {
			[Statement::Query(_)] => pg_to_df(&self.client, &code)
				.await
				.map(ExecutionResult::Rows)
				.unwrap_or_else(|err| ExecutionResult::Err(err.to_string())),
			[] => ExecutionResult::None,
			_ => ExecutionResult::None,
		}
	}
}

fn bytes_needed_for_row(buffer: &BytesMut, width: usize) -> Option<usize> {
	if buffer.len() < 2 {
		return None;
	}
	let mut pos = 0;
	let field_count = i16::from_be_bytes(buffer[pos..pos + 2].try_into().ok()?);
	pos += 2;
	if field_count == -1 {
		return Some(2);
	}
	for _ in 0..width {
		if buffer.len() < pos + 4 {
			return None;
		}
		let len = i32::from_be_bytes(buffer[pos..pos + 4].try_into().ok()?);
		pos += 4;
		if len != -1 {
			let u_len = len as usize;
			if buffer.len() < pos + u_len {
				return None;
			}
			pos += u_len;
		}
	}
	Some(pos)
}

pub async fn pg_to_df(client: &Client, code: &str) -> Result<DataFrame, BoxError> {
	let stmt = client.prepare(code).await?;
	let columns = stmt.columns();
	let width = columns.len();
	let names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();
	let types: Vec<Type> = columns.iter().map(|c| c.type_().clone()).collect();
	let stream = client
		.copy_out(&format!("COPY ({}) TO STDOUT BINARY", code))
		.await?;
	let mut stream = pin!(stream);
	let mut column_data: Vec<Vec<AnyValue>> = vec![Vec::new(); width];
	let mut buffer = BytesMut::with_capacity(65536);
	let mut header_skipped = false;
	while let Some(chunk_result) = stream.next().await {
		buffer.extend_from_slice(&chunk_result?);
		if !header_skipped && buffer.len() >= 19 {
			if &buffer[0..11] == b"PGCOPY\n\xff\r\n\0" {
				buffer.advance(19);
			}
			header_skipped = true;
		}
		while let Some(needed) = bytes_needed_for_row(&buffer, width) {
			let start_len = buffer.len();
			if &buffer[0..2] == b"\xff\xff" {
				buffer.advance(2);
				break;
			}
			buffer.advance(2);
			for i in 0..width {
				let len = buffer.get_i32();
				if len == -1 {
					column_data[i].push(AnyValue::Null);
				} else {
					let val_bytes = buffer.split_to(len as usize);
					let val = match types[i] {
						Type::INT4 => {
							AnyValue::Int32(i32::from_be_bytes(val_bytes[..].try_into()?))
						}
						Type::INT8 => {
							AnyValue::Int64(i64::from_be_bytes(val_bytes[..].try_into()?))
						}
						Type::FLOAT8 => {
							AnyValue::Float64(f64::from_be_bytes(val_bytes[..].try_into()?))
						}
						Type::TEXT | Type::VARCHAR => {
							AnyValue::StringOwned(std::str::from_utf8(&val_bytes)?.into())
						}
						Type::TIMESTAMP | Type::TIMESTAMPTZ => {
							let ticks = i64::from_be_bytes(val_bytes[..].try_into()?);
							let unix_micros = ticks + (946_684_800 * 1_000_000);
							AnyValue::Datetime(
								unix_micros,
								polars::datatypes::TimeUnit::Microseconds,
								None,
							)
						}
						_ => AnyValue::Null,
					};
					column_data[i].push(val);
				}
			}
			let consumed = start_len - buffer.len();
			debug_assert_eq!(
				consumed, needed,
				"Buffer mismatch: expected to consume {} bytes, but took {}",
				needed, consumed
			);
		}
	}
	let polars_columns: Vec<Column> = names
		.into_iter()
		.enumerate()
		.map(|(i, name)| {
			Column::from(Series::from_any_values(name.into(), &column_data[i], true).unwrap())
		})
		.collect();
	let height = polars_columns.first().map(|c| c.len()).unwrap_or(0);
	Ok(DataFrame::new(height, polars_columns)?)
}
