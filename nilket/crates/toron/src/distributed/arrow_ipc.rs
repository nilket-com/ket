use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use thiserror::Error;

use crate::distributed::partition_store::PartitionPayload;
use crate::distributed::task::RowCount;
use crate::error::QueryError;

#[derive(Debug, Error)]
pub enum ArrowIpcError {
	#[error("arrow IPC error during {operation}: {source}")]
	Arrow {
		operation: &'static str,
		#[source]
		source: arrow::error::ArrowError,
	},
	#[error("arrow IPC row count mismatch: decoded {decoded}, metadata {metadata}")]
	RowCountMismatch { decoded: u64, metadata: u64 },
	#[error("arrow IPC schema is required for empty partition reassembly")]
	MissingSchema,
}

impl ArrowIpcError {
	fn arrow(operation: &'static str, source: arrow::error::ArrowError) -> Self {
		Self::Arrow { operation, source }
	}
}

impl From<ArrowIpcError> for QueryError {
	fn from(error: ArrowIpcError) -> Self {
		match error {
			ArrowIpcError::Arrow { source, .. } => QueryError::Arrow(source),
			other => QueryError::Internal {
				message: other.to_string(),
			},
		}
	}
}

pub(crate) fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArrowIpcError> {
	let mut buffer = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
			.map_err(|error| ArrowIpcError::arrow("stream-writer-new", error))?;
		writer
			.write(batch)
			.map_err(|error| ArrowIpcError::arrow("stream-writer-write", error))?;
		writer
			.finish()
			.map_err(|error| ArrowIpcError::arrow("stream-writer-finish", error))?;
	}
	Ok(buffer)
}

pub(crate) fn decode_record_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, ArrowIpcError> {
	let mut reader = StreamReader::try_new(bytes, None)
		.map_err(|error| ArrowIpcError::arrow("stream-reader-new", error))?;
	let mut batches = Vec::new();
	while let Some(batch) = reader
		.next()
		.transpose()
		.map_err(|error| ArrowIpcError::arrow("stream-reader-next", error))?
	{
		batches.push(batch);
	}
	Ok(batches)
}

pub(crate) fn encode_empty_record_batch(schema: SchemaRef) -> Result<Vec<u8>, ArrowIpcError> {
	encode_record_batch(&RecordBatch::new_empty(schema))
}

pub(crate) fn reassemble_partition_chunks(
	schema: Option<SchemaRef>,
	chunks: &[PartitionPayload],
) -> Result<(Vec<u8>, RowCount), ArrowIpcError> {
	if chunks.is_empty() {
		let schema = schema.ok_or(ArrowIpcError::MissingSchema)?;
		return Ok((encode_empty_record_batch(schema)?, RowCount::new(0)));
	}

	let metadata_rows = chunks.iter().fold(0u64, |rows, chunk| {
		rows.saturating_add(chunk.row_count.get())
	});
	let mut decoded_rows = 0u64;
	let mut buffer = Vec::new();
	let mut writer = None;

	for chunk in chunks {
		let mut reader = StreamReader::try_new(chunk.bytes.as_slice(), None)
			.map_err(|error| ArrowIpcError::arrow("chunk-stream-reader-new", error))?;
		while let Some(batch) = reader
			.next()
			.transpose()
			.map_err(|error| ArrowIpcError::arrow("chunk-stream-reader-next", error))?
		{
			decoded_rows = decoded_rows.saturating_add(
				u64::try_from(batch.num_rows()).expect("record batch row count fits u64"),
			);
			if writer.is_none() {
				writer = Some(
					StreamWriter::try_new(&mut buffer, &batch.schema())
						.map_err(|error| ArrowIpcError::arrow("sealed-stream-writer-new", error))?,
				);
			}
			writer
				.as_mut()
				.expect("writer initialized before write")
				.write(&batch)
				.map_err(|error| ArrowIpcError::arrow("sealed-stream-writer-write", error))?;
		}
	}

	if decoded_rows != metadata_rows {
		return Err(ArrowIpcError::RowCountMismatch {
			decoded: decoded_rows,
			metadata: metadata_rows,
		});
	}

	if let Some(mut writer) = writer {
		writer
			.finish()
			.map_err(|error| ArrowIpcError::arrow("sealed-stream-writer-finish", error))?;
		return Ok((buffer, RowCount::new(decoded_rows)));
	}

	let schema = schema.ok_or(ArrowIpcError::MissingSchema)?;
	Ok((encode_empty_record_batch(schema)?, RowCount::new(0)))
}
