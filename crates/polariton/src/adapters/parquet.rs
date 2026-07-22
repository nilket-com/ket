use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use async_trait::async_trait;
use polars::sql::SQLContext;

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "input_path",
	value: "",
	field_type: &AdapterFieldType::Text,
	is_secure: false,
}];

pub struct ParquetAdapter {
	pub context: SQLContext,
}

#[async_trait]
impl DatabaseAdapter for ParquetAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let code = code.to_string();
		let mut ctx = self.context.clone();
		let result = tokio::task::spawn_blocking(move || match ctx.execute(&code) {
			Ok(lazy_df) => match lazy_df.collect() {
				Ok(df) => ExecutionResult::Rows(df),
				Err(err) => ExecutionResult::Err(format!("Error: {err}")),
			},
			Err(err) => ExecutionResult::Err(format!("Error: {err}")),
		})
		.await;
		match result {
			Ok(res) => res,
			Err(_) => ExecutionResult::Err("Task panicked".into()),
		}
	}
}
