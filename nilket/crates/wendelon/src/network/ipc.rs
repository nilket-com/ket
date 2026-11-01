//! Wire-side Arrow IPC compression helpers for ADR 053.
//!
//! Writer audit exclusions:
//! - descriptor-schema: `src/schema/table.rs::encode_arrow_schema_ipc`
//!   stays on default IPC options for boot-time independence.
//! - storage: `src/storage/ipc.rs` owns slab record-body policy
//!   selection under ADR 043 and is not a wire fresh-encode path.
//! - test-or-bench-default: test and benchmark fixtures may use
//!   default options when they intentionally model pre-ADR-053 bytes.

use crate::metrics::{NetworkCompressionPath, global as metrics};
use crate::schema::RecordBodyCompressionPolicy;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use std::io::{Cursor, Error, ErrorKind};
use std::time::{Duration, Instant};

pub const WIRE_BODY_COMPRESSION: RecordBodyCompressionPolicy =
	RecordBodyCompressionPolicy::Lz4Frame;

pub fn wire_ipc_write_options() -> std::io::Result<IpcWriteOptions> {
	crate::storage::ipc::ipc_write_options_for_policy(WIRE_BODY_COMPRESSION)
}

pub fn wire_encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
	crate::storage::ipc::encode_record_batch_bytes(batch, WIRE_BODY_COMPRESSION)
}

pub fn wire_decode_batch(bytes: &[u8]) -> std::io::Result<RecordBatch> {
	let policy = classify_wire_compression_policy(bytes);
	crate::storage::ipc::validate_ipc_stream_envelope(bytes).map_err(|error| {
		observe_wire_decode_error_for_policy(policy);
		error
	})?;
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|err| {
		observe_wire_decode_error_for_policy(policy);
		Error::new(ErrorKind::InvalidData, format!("arrow stream open: {err}"))
	})?;
	let batch = match reader.next() {
		Some(Ok(batch)) => Ok(batch),
		Some(Err(err)) => {
			observe_wire_decode_error_for_policy(policy);
			Err(Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream read: {err}"),
			))
		}
		None => {
			observe_wire_decode_error_for_policy(policy);
			Err(Error::new(
				ErrorKind::InvalidData,
				"arrow stream contained no record batch",
			))
		}
	}?;
	match reader.next() {
		Some(Ok(_)) => {
			observe_wire_decode_error_for_policy(policy);
			Err(Error::new(
				ErrorKind::InvalidData,
				"arrow stream contained multiple record batches",
			))
		}
		Some(Err(err)) => {
			observe_wire_decode_error_for_policy(policy);
			Err(Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream read: {err}"),
			))
		}
		None => Ok(batch),
	}
}

pub fn wire_encode_batch_for_path(
	batch: &RecordBatch,
	path: NetworkCompressionPath,
) -> std::io::Result<Vec<u8>> {
	let started = Instant::now();
	match wire_encode_batch(batch) {
		Ok(bytes) => {
			observe_wire_encode_success(path, bytes.len() as u64, started.elapsed());
			Ok(bytes)
		}
		Err(err) => {
			observe_wire_encode_error(path);
			Err(err)
		}
	}
}

pub fn observe_wire_encode_success(
	path: NetworkCompressionPath,
	encoded_bytes: u64,
	duration: Duration,
) {
	metrics().observe_network_compression_encode(
		WIRE_BODY_COMPRESSION,
		path,
		encoded_bytes,
		duration,
		None,
	);
}

pub fn observe_wire_encode_error(path: NetworkCompressionPath) {
	metrics().observe_network_compression_encode_error(WIRE_BODY_COMPRESSION, path);
}

pub fn observe_wire_decode_error(codec: RecordBodyCompressionPolicy) {
	metrics().observe_network_compression_decode_error(codec);
}

pub fn observe_wire_decode_error_for_bytes(bytes: &[u8]) {
	observe_wire_decode_error_for_policy(classify_wire_compression_policy(bytes));
}

fn classify_wire_compression_policy(bytes: &[u8]) -> Option<RecordBodyCompressionPolicy> {
	match crate::storage::ipc::record_batch_compression_policy_from_ipc(bytes) {
		Ok(codec) => Some(codec),
		Err(err) => {
			tracing::debug!(error = %err, "unable to classify transport IPC decode error codec");
			None
		}
	}
}

fn observe_wire_decode_error_for_policy(policy: Option<RecordBodyCompressionPolicy>) {
	if let Some(codec) = policy {
		observe_wire_decode_error(codec);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::Int32Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;

	#[test]
	fn wire_encode_batch_uses_lz4_frame_policy() {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"value",
			DataType::Int32,
			false,
		)]));
		let batch =
			RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))])
				.unwrap();

		let encoded = wire_encode_batch(&batch).unwrap();
		let policy = crate::storage::ipc::record_batch_compression_policy_from_ipc(&encoded)
			.expect("wire IPC should expose a record batch compression policy");

		assert_eq!(policy, WIRE_BODY_COMPRESSION);
	}
}
