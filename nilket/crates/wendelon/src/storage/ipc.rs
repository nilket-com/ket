use crate::schema::{RecordBodyCompressionPolicy, TableSchemaDescriptor};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::{BodyCompressionMethod, CompressionType, MessageHeader, root_as_message};
use arrow::record_batch::RecordBatch;
use std::io::{Error, ErrorKind};
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedRecordBatch {
	pub bytes: Vec<u8>,
	pub plaintext_byte_len: u64,
	pub encoded_byte_len: u64,
	pub codec: RecordBodyCompressionPolicy,
}

pub fn ipc_write_options_for(
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<IpcWriteOptions> {
	ipc_write_options_for_policy(descriptor.record_body_compression_policy)
}

pub fn ipc_write_options_for_policy(
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<IpcWriteOptions> {
	match policy {
		RecordBodyCompressionPolicy::Uncompressed => Ok(IpcWriteOptions::default()),
		RecordBodyCompressionPolicy::Lz4Frame => IpcWriteOptions::default()
			.try_with_compression(Some(CompressionType::LZ4_FRAME))
			.map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("arrow IPC write options for lz4_frame: {err}"),
				)
			}),
	}
}

pub fn encode_record_batch(
	batch: &RecordBatch,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<EncodedRecordBatch> {
	let started = Instant::now();
	let plaintext = encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed)?;
	let plaintext_byte_len = plaintext.len() as u64;
	let bytes = match policy {
		RecordBodyCompressionPolicy::Uncompressed => plaintext,
		RecordBodyCompressionPolicy::Lz4Frame => encode_record_batch_bytes(batch, policy)?,
	};
	let encoded_byte_len = bytes.len() as u64;
	crate::metrics::global().observe_storage_compression_encode(
		policy,
		plaintext_byte_len,
		encoded_byte_len,
		started.elapsed(),
	);
	Ok(EncodedRecordBatch {
		bytes,
		plaintext_byte_len,
		encoded_byte_len,
		codec: policy,
	})
}

pub fn encode_record_batch_bytes(
	batch: &RecordBatch,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<Vec<u8>> {
	encode_record_batches_bytes(std::slice::from_ref(batch), policy)
}

pub fn encode_record_batches_bytes(
	batches: &[RecordBatch],
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<Vec<u8>> {
	let Some(first) = batches.first() else {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"arrow stream writer requires at least one record batch",
		));
	};
	let options = ipc_write_options_for_policy(policy)?;
	let mut buf = Vec::new();
	{
		let mut writer =
			StreamWriter::try_new_with_options(&mut buf, first.schema().as_ref(), options)
				.map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("arrow stream init for {}: {err}", policy.codec_label()),
					)
				})?;
		for batch in batches {
			if batch.schema().as_ref() != first.schema().as_ref() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"arrow stream row groups must share one schema",
				));
			}
			writer.write(batch).map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("arrow stream write for {}: {err}", policy.codec_label()),
				)
			})?;
		}
		writer.finish().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream finish for {}: {err}", policy.codec_label()),
			)
		})?;
	}
	Ok(buf)
}

pub fn record_batch_compression_policy_from_ipc(
	bytes: &[u8],
) -> std::io::Result<RecordBodyCompressionPolicy> {
	let mut offset = 0_usize;
	while offset < bytes.len() {
		let Some((message_start, metadata_len)) = read_message_prefix(bytes, &mut offset)? else {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"arrow stream ended before a record batch message",
			));
		};
		let message_end = message_start
			.checked_add(metadata_len)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "arrow metadata length overflow"))?;
		if message_end > bytes.len() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"arrow metadata extends past stream",
			));
		}
		let message = root_as_message(&bytes[message_start..message_end]).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow message flatbuffer parse: {err}"),
			)
		})?;
		let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
			Error::new(ErrorKind::InvalidData, "negative arrow message body length")
		})?;
		offset = message_end
			.checked_add(body_len)
			.and_then(|end| end.checked_add(pad_to_alignment(8, body_len)))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "arrow body length overflow"))?;
		if offset > bytes.len() {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"arrow message body extends past stream",
			));
		}
		if message.header_type() != MessageHeader::RecordBatch {
			continue;
		}
		let batch = message.header_as_record_batch().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"arrow record batch message missing header",
			)
		})?;
		let Some(compression) = batch.compression() else {
			return Ok(RecordBodyCompressionPolicy::Uncompressed);
		};
		if compression.method() != BodyCompressionMethod::BUFFER {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"unsupported arrow body compression method {:?}",
					compression.method()
				),
			));
		}
		return match compression.codec() {
			CompressionType::LZ4_FRAME => Ok(RecordBodyCompressionPolicy::Lz4Frame),
			other => Err(Error::new(
				ErrorKind::InvalidData,
				format!("unsupported arrow body compression codec {other:?}"),
			)),
		};
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"arrow stream contained no record batch message",
	))
}

fn read_message_prefix(
	bytes: &[u8],
	offset: &mut usize,
) -> std::io::Result<Option<(usize, usize)>> {
	if bytes.len().saturating_sub(*offset) < 4 {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"arrow stream truncated before message prefix",
		));
	}
	let first = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
	*offset += 4;
	let metadata_len = if first == u32::MAX {
		if bytes.len().saturating_sub(*offset) < 4 {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"arrow stream truncated before continuation length",
			));
		}
		let len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
		*offset += 4;
		len as usize
	} else {
		first as usize
	};
	if metadata_len == 0 {
		return Ok(None);
	}
	Ok(Some((*offset, metadata_len)))
}

fn pad_to_alignment(alignment: usize, len: usize) -> usize {
	let mask = alignment - 1;
	((len + mask) & !mask) - len
}
