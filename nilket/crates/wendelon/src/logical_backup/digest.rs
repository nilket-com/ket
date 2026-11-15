//! ADR 0736: the per-table canonical digest, bounded and framed.
//!
//! Each row is the existing logical encoding (`storage::content_hash::encode_row_canonical`,
//! which reads the descriptor's Arrow fields and values, not table, store or parent ids). The
//! rows of one table are held in memory up to two independent caps, sorted, and hashed as
//! SHA-256 over: a domain, the canonical logical schema, the row count, then each row as a
//! 32-bit length and its bytes, in sorted order. Duplicates and nulls are kept: two equal rows
//! are two framed entries.

use aws_lc_rs::digest::{Context, SHA256};

/// The digest's domain and version.
pub const DOMAIN: &[u8] = b"ket-backup-table-digest-v1";
/// At most this many canonical row bytes per table.
pub const MAX_TABLE_BYTES: usize = 64 * 1024 * 1024;
/// At most this many rows per table, whatever their size.
pub const MAX_TABLE_ROWS: usize = 1 << 20;

/// Why a table's digest cannot be computed within the first workflow's bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestRefusal {
	TooManyRows,
	TooManyBytes,
	/// One row longer than its 32-bit length frame.
	RowTooLong,
}

/// The rows of one table, accumulated up to the caps, then digested.
pub struct TableDigest {
	schema: Vec<u8>,
	rows: Vec<Vec<u8>>,
	bytes: usize,
}

impl TableDigest {
	/// `schema` is the canonical logical schema (see [`canonical_schema`]).
	pub fn new(schema: Vec<u8>) -> Self {
		Self {
			schema,
			rows: Vec::new(),
			bytes: 0,
		}
	}

	/// Adds one canonical row. Counts and lengths are checked before the row is kept.
	pub fn push(&mut self, row: Vec<u8>) -> Result<(), DigestRefusal> {
		if self.rows.len() >= MAX_TABLE_ROWS {
			return Err(DigestRefusal::TooManyRows);
		}
		if u32::try_from(row.len()).is_err() {
			return Err(DigestRefusal::RowTooLong);
		}
		let bytes = self
			.bytes
			.checked_add(row.len())
			.filter(|bytes| *bytes <= MAX_TABLE_BYTES)
			.ok_or(DigestRefusal::TooManyBytes)?;
		self.bytes = bytes;
		self.rows.push(row);
		Ok(())
	}

	pub fn rows(&self) -> usize {
		self.rows.len()
	}

	/// The table's digest.
	pub fn finish(mut self) -> [u8; 32] {
		self.rows.sort_unstable();
		let mut context = Context::new(&SHA256);
		context.update(&(DOMAIN.len() as u32).to_be_bytes());
		context.update(DOMAIN);
		context.update(&(self.schema.len() as u64).to_be_bytes());
		context.update(&self.schema);
		context.update(&(self.rows.len() as u64).to_be_bytes());
		for row in &self.rows {
			context.update(&(row.len() as u32).to_be_bytes());
			context.update(row);
		}
		context
			.finish()
			.as_ref()
			.try_into()
			.expect("SHA-256 is 32 bytes")
	}
}

/// The canonical logical schema: for each field in order, its name, its Arrow data type's
/// display and its nullability, each length-prefixed. Physical ids are not part of it.
pub fn canonical_schema(schema: &arrow::datatypes::Schema) -> Vec<u8> {
	let mut out = Vec::new();
	out.extend((schema.fields().len() as u32).to_be_bytes());
	for field in schema.fields() {
		for part in [
			field.name().as_bytes(),
			field.data_type().to_string().as_bytes(),
		] {
			out.extend((part.len() as u32).to_be_bytes());
			out.extend(part);
		}
		out.push(u8::from(field.is_nullable()));
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	fn digest(rows: &[&[u8]]) -> [u8; 32] {
		let mut table = TableDigest::new(b"schema".to_vec());
		for row in rows {
			table.push(row.to_vec()).unwrap();
		}
		table.finish()
	}

	#[test]
	fn order_does_not_matter_and_multiplicity_and_framing_do() {
		assert_eq!(digest(&[b"a", b"b"]), digest(&[b"b", b"a"]));
		// Duplicates are kept.
		assert_ne!(digest(&[b"a"]), digest(&[b"a", b"a"]));
		// Row boundaries are framed: "ab" is not "a" then "b".
		assert_ne!(digest(&[b"ab"]), digest(&[b"a", b"b"]));
		// An empty row (all nulls encode to something; an empty frame still counts).
		assert_ne!(digest(&[]), digest(&[b""]));
		// The schema is part of it.
		let mut other = TableDigest::new(b"other".to_vec());
		other.push(b"a".to_vec()).unwrap();
		assert_ne!(other.finish(), digest(&[b"a"]));
	}

	#[test]
	fn the_caps_refuse_before_growth() {
		let mut table = TableDigest::new(Vec::new());
		table.push(vec![0; MAX_TABLE_BYTES]).unwrap();
		assert_eq!(table.push(vec![0; 1]), Err(DigestRefusal::TooManyBytes));
		assert_eq!(table.rows(), 1);
		let mut many = TableDigest::new(Vec::new());
		for _ in 0..MAX_TABLE_ROWS {
			many.push(Vec::new()).unwrap();
		}
		assert_eq!(many.push(Vec::new()), Err(DigestRefusal::TooManyRows));
	}

	#[test]
	fn the_logical_schema_is_framed_and_ignores_nothing_it_names() {
		use arrow::datatypes::{DataType, Field, Schema};
		let a = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
		]);
		let b = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, false),
		]);
		let c = Schema::new(vec![
			Field::new("idn", DataType::Int64, false),
			Field::new("ame", DataType::LargeUtf8, true),
		]);
		assert_ne!(canonical_schema(&a), canonical_schema(&b));
		assert_ne!(canonical_schema(&a), canonical_schema(&c));
		assert_eq!(canonical_schema(&a), canonical_schema(&a.clone()));
	}
}
