use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use crate::result_artifact::ResultArtifact;
use async_trait::async_trait;
use polars::sql::SQLContext;

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "input_path",
	placeholder: "",
	default_value: None,
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
				Ok(frame) => ExecutionResult::Rows {
					artifact: ResultArtifact::from_polars(std::sync::Arc::new(frame)),
					statements: 1,
				},
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

/// One file-opening path for the connection form and the agent action.
pub(crate) fn open_data(
	path: &std::path::Path,
) -> Result<(ParquetAdapter, FileDescription), String> {
	use polars::prelude::*;
	if !path.exists() {
		return Err(format!("Data file does not exist: {}", path.display()));
	}
	let mut frame = if path
		.extension()
		.and_then(|x| x.to_str())
		.is_some_and(|x| x.eq_ignore_ascii_case("csv"))
	{
		read_csv(std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?)?.lazy()
	} else {
		let input = PlRefPath::try_from_path(path).map_err(|e| format!("Parquet path: {e}"))?;
		LazyFrame::scan_parquet(
			input,
			ScanArgsParquet {
				hive_options: HiveOptions {
					enabled: Some(path.is_dir()),
					try_parse_dates: true,
					..Default::default()
				},
				..Default::default()
			},
		)
		.map_err(|e| format!("Parquet scan: {e}"))?
	};
	let schema = frame
		.collect_schema()
		.map_err(|e| format!("File schema: {e}"))?;
	let columns = schema
		.iter()
		.map(|(name, ty)| (name.to_string(), ty.to_string()))
		.collect();
	let table = if path.is_file() {
		path.file_prefix()
	} else {
		path.file_name()
	}
	.and_then(|s| s.to_str())
	.unwrap_or("temp_table")
	.to_owned();
	let context = SQLContext::new();
	context.register(&table, frame);
	Ok((
		ParquetAdapter { context },
		FileDescription { table, columns },
	))
}
#[derive(Clone, Debug)]
pub(crate) struct FileDescription {
	pub table: String,
	pub columns: Vec<(String, String)>,
}
fn read_csv(mut file: std::fs::File) -> Result<polars::prelude::DataFrame, String> {
	use std::io::Seek;
	let format = arrow_csv::reader::Format::default().with_header(true);
	let (schema, _) = format
		.infer_schema(&mut file, None)
		.map_err(|e| e.to_string())?;
	file.rewind().map_err(|e| e.to_string())?;
	let schema = std::sync::Arc::new(schema);
	let reader = arrow_csv::ReaderBuilder::new(schema.clone())
		.with_header(true)
		.build(file)
		.map_err(|e| e.to_string())?;
	let mut data = Vec::new();
	{
		let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut data, &schema)
			.map_err(|e| e.to_string())?;
		for batch in reader {
			writer
				.write(&batch.map_err(|e| e.to_string())?)
				.map_err(|e| e.to_string())?;
		}
		writer.finish().map_err(|e| e.to_string())?;
	}
	let artifact = ResultArtifact::from_arrow_ipc(
		vec![data],
		crate::result_artifact::ArrowNullabilityPolicy::Preserve,
	)
	.map_err(|e| e.to_string())?;
	artifact
		.projection()
		.map(|f| f.as_ref().clone())
		.map_err(|e| e.to_string())
}
