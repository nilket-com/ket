//! ADR 0736: the backup directory's layout and its manifest.
//!
//! ```text
//! <dir>/KEY              the wrapped data key (artifact::WRAP_BYTES)
//! <dir>/chunks/<ordinal> sealed chunk `ordinal` (10 decimal digits, 1-based)
//! <dir>/MANIFEST         manifest header ++ sealed manifest; published last
//! ```
//!
//! A chunk's plaintext is one Arrow IPC stream holding one record batch of one
//! table. Its associated data is its [`chunk_header`]; the manifest's is its
//! [`manifest_header`]. The manifest pins every chunk by ordinal, table, row
//! count, sealed length and SHA-256 of the sealed bytes, and every table by row
//! count and canonical digest.

use super::artifact::{MAX_CHUNK_BYTES, MAX_MANIFEST_BYTES};
use super::roster::BackupRoster;
use crate::schema::{TableObjectId, TenantId};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

pub const KEY_FILE: &str = "KEY";
pub const MANIFEST_FILE: &str = "MANIFEST";
pub const CHUNK_DIR: &str = "chunks";
pub const MANIFEST_MAGIC: &[u8; 8] = b"KETBKMAN";
pub const FORMAT_VERSION: u16 = 1;
pub const MANIFEST_HEADER_BYTES: usize = 8 + 2 + 16;
pub const CHUNK_HEADER_BYTES: usize = 16 + 8 + 16 + 8 + 8;

/// One sealed chunk, as the manifest pins it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkEntry {
	pub ordinal: u64,
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub rows: u32,
	pub sealed_len: u64,
	pub sealed_sha256: [u8; 32],
}

/// One table's export: its chunks are `first_chunk..first_chunk + chunk_count`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableEntry {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub rows: u64,
	pub digest: [u8; 32],
	pub first_chunk: u64,
	pub chunk_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
	pub format: u16,
	pub backup_id: u128,
	/// The roster, its hold token and its cut R.
	pub roster: BackupRoster,
	/// The cut at which the same hold and an equal roster were read again,
	/// after the last table and before publication.
	pub continuity_cut: u64,
	/// In roster order.
	pub tables: Vec<TableEntry>,
	/// In ordinal order, 1..=len.
	pub chunks: Vec<ChunkEntry>,
}

impl Manifest {
	pub fn encode(&self) -> std::io::Result<Vec<u8>> {
		postcard::to_allocvec(self).map_err(|error| Error::new(ErrorKind::InvalidData, error))
	}

	/// Decodes an opened manifest and checks its own shape: format, ordinals,
	/// per-table chunk ranges and counts, and that each table is on the roster.
	pub fn decode(bytes: &[u8]) -> std::io::Result<Self> {
		let manifest: Self = postcard::from_bytes(bytes)
			.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
		manifest.check()?;
		Ok(manifest)
	}

	fn check(&self) -> std::io::Result<()> {
		let invalid = |what: &str| Err(Error::new(ErrorKind::InvalidData, what.to_owned()));
		if self.format != FORMAT_VERSION {
			return invalid("unsupported backup format");
		}
		if self.roster.tables.len() != self.tables.len() {
			return invalid("the manifest's tables are not the roster's");
		}
		for (index, chunk) in self.chunks.iter().enumerate() {
			if chunk.ordinal != index as u64 + 1 {
				return invalid("chunk ordinals are not 1..=n in order");
			}
			if chunk.sealed_len > (MAX_CHUNK_BYTES + 16) as u64 {
				return invalid("a chunk is longer than the format admits");
			}
			// Each chunk is one table write on restore.
			if chunk.rows == 0 || chunk.rows as usize > crate::schema::TABLE_WRITE_MAX_ROWS {
				return invalid("a chunk's rows are outside one table write");
			}
		}
		let mut next = 1u64;
		for (table, rostered) in self.tables.iter().zip(&self.roster.tables) {
			if (table.tenant_id, table.table_object_id)
				!= (rostered.tenant_id, rostered.descriptor.table_object_id)
			{
				return invalid("the manifest's tables are not the roster's");
			}
			if table.first_chunk != next {
				return invalid("table chunk ranges are not contiguous");
			}
			let end = table
				.first_chunk
				.checked_add(table.chunk_count)
				.filter(|end| *end <= self.chunks.len() as u64 + 1)
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "a table's chunks overrun"))?;
			let mut rows = 0u64;
			for chunk in &self.chunks[(table.first_chunk - 1) as usize..(end - 1) as usize] {
				if (
					chunk.tenant_id,
					chunk.table_object_id,
					chunk.schema_generation,
				) != (
					table.tenant_id,
					table.table_object_id,
					rostered.descriptor.schema_generation,
				) {
					return invalid("a chunk names another table");
				}
				rows += u64::from(chunk.rows);
			}
			if rows != table.rows {
				return invalid("a table's chunk rows do not sum to its count");
			}
			next = end;
		}
		if next != self.chunks.len() as u64 + 1 {
			return invalid("a chunk belongs to no table");
		}
		Ok(())
	}
}

pub fn manifest_header(backup_id: u128) -> [u8; MANIFEST_HEADER_BYTES] {
	let mut header = [0u8; MANIFEST_HEADER_BYTES];
	header[..8].copy_from_slice(MANIFEST_MAGIC);
	header[8..10].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
	header[10..].copy_from_slice(&backup_id.to_be_bytes());
	header
}

/// The backup id a manifest file names, read before anything is opened.
pub fn manifest_backup_id(file: &[u8]) -> std::io::Result<u128> {
	if file.len() < MANIFEST_HEADER_BYTES
		|| file.len() > MANIFEST_HEADER_BYTES + MAX_MANIFEST_BYTES
		|| &file[..8] != MANIFEST_MAGIC
		|| file[8..10] != FORMAT_VERSION.to_be_bytes()
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"not a backup manifest of this format",
		));
	}
	Ok(u128::from_be_bytes(
		file[10..MANIFEST_HEADER_BYTES].try_into().expect("width"),
	))
}

pub fn chunk_header(
	backup_id: u128,
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	schema_generation: u64,
	ordinal: u64,
) -> [u8; CHUNK_HEADER_BYTES] {
	let mut header = [0u8; CHUNK_HEADER_BYTES];
	header[..16].copy_from_slice(&backup_id.to_be_bytes());
	header[16..24].copy_from_slice(&tenant_id.get().to_be_bytes());
	header[24..40].copy_from_slice(table_object_id.as_bytes());
	header[40..48].copy_from_slice(&schema_generation.to_be_bytes());
	header[48..].copy_from_slice(&ordinal.to_be_bytes());
	header
}

pub fn chunk_path(dir: &Path, ordinal: u64) -> PathBuf {
	dir.join(CHUNK_DIR).join(format!("{ordinal:010}"))
}

/// One record batch as an Arrow IPC stream.
pub fn encode_batch(batch: &arrow::record_batch::RecordBatch) -> std::io::Result<Vec<u8>> {
	let mut out = Vec::new();
	let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut out, &batch.schema())
		.map_err(Error::other)?;
	writer.write(batch).map_err(Error::other)?;
	writer.finish().map_err(Error::other)?;
	drop(writer);
	Ok(out)
}

/// The record batch of a chunk's plaintext, through the wire decoder's guard:
/// every IPC metadata and body span is checked against the actual buffer before
/// Arrow allocates for it.
pub fn decode_batch(bytes: &[u8]) -> std::io::Result<arrow::record_batch::RecordBatch> {
	crate::network::ipc::wire_decode_batch(bytes)
}

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
	aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes)
		.as_ref()
		.try_into()
		.expect("SHA-256 is 32 bytes")
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;

	/// An authenticated chunk whose IPC message claims a length beyond its own
	/// bytes is refused before Arrow allocates for that length.
	#[test]
	fn a_chunk_whose_ipc_claims_more_than_it_holds_is_refused_before_allocation() {
		let batch = arrow::record_batch::RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.unwrap();
		let encoded = encode_batch(&batch).unwrap();
		assert_eq!(decode_batch(&encoded).unwrap(), batch);
		// The stream's first message: continuation marker, then its metadata length.
		assert_eq!(&encoded[..4], &[0xFF; 4]);
		let mut claims_more = encoded.clone();
		claims_more[4..8].copy_from_slice(&0x7FFF_FFF0u32.to_le_bytes());
		let error = decode_batch(&claims_more).unwrap_err();
		assert_eq!(error.kind(), ErrorKind::InvalidData, "{error}");
	}
}
