use super::{AdapterError, KEY_MAGIC, ProofScalarType, ROW_MAGIC, scalar::cell};
use crate::storage::mutation_visibility::{MAX_CONTENT_INPUT, MAX_KEY_INPUT, encoding::Reader};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::collections::BTreeSet;

pub(super) fn write_field(
	out: &mut Vec<u8>,
	ordinal: usize,
	f: &arrow::datatypes::Field,
	t: ProofScalarType,
	a: &dyn arrow::array::Array,
	row: usize,
) -> Result<(), AdapterError> {
	let v = cell(a, row, t)?;
	if v.is_none() && !f.is_nullable() {
		return Err(AdapterError::InvalidValue);
	}
	out.extend_from_slice(&(ordinal as u16).to_le_bytes());
	out.extend_from_slice(&(f.name().len() as u16).to_le_bytes());
	out.extend_from_slice(f.name().as_bytes());
	out.extend_from_slice(&t.descriptor());
	out.push(u8::from(f.is_nullable()));
	out.push(u8::from(v.is_some()));
	out.extend_from_slice(&(v.as_ref().map_or(0, |v| v.bytes().len()) as u32).to_le_bytes());
	if let Some(v) = v {
		out.extend_from_slice(v.bytes())
	}
	Ok(())
}

/// Immutable explicit schema. Nominal/custom metadata is refused rather than erased.
#[derive(Clone)]
pub struct ProofSchema {
	table: [u8; 16],
	generation: u64,
	schema: SchemaRef,
	types: Vec<ProofScalarType>,
	pk: usize,
}
crate::storage::mutation_visibility::redacted_debug!(ProofSchema, "ProofSchema");
impl ProofSchema {
	pub fn new(
		table: [u8; 16],
		generation: u64,
		schema: SchemaRef,
		pk: usize,
	) -> Result<Self, AdapterError> {
		if table == [0; 16]
			|| generation == 0
			|| schema.fields().is_empty()
			|| schema.fields().len() > 128
			|| pk >= schema.fields().len()
			|| !schema.metadata().is_empty()
		{
			return Err(AdapterError::SchemaMismatch);
		}
		let mut names = BTreeSet::new();
		let mut types = Vec::with_capacity(schema.fields().len());
		for f in schema.fields() {
			if f.name().is_empty()
				|| f.name().len() > 255
				|| f.name().contains('\0')
				|| !f.metadata().is_empty()
				|| !names.insert(f.name())
			{
				return Err(AdapterError::SchemaMismatch);
			}
			types.push(ProofScalarType::from_arrow(f.data_type())?);
		}
		if schema.field(pk).is_nullable() || !types[pk].pk_supported() {
			return Err(AdapterError::UnsupportedType);
		}
		Ok(Self {
			table,
			generation,
			schema,
			types,
			pk,
		})
	}
	pub fn table(&self) -> [u8; 16] {
		self.table
	}
	pub fn generation(&self) -> u64 {
		self.generation
	}
	pub fn check_batch(&self, b: &RecordBatch) -> Result<(), AdapterError> {
		if !b.schema().metadata().is_empty() || b.num_columns() != self.types.len() {
			return Err(AdapterError::SchemaMismatch);
		}
		for (i, f) in b.schema().fields().iter().enumerate() {
			let expected = self.schema.field(i);
			if f.name() != expected.name()
				|| f.is_nullable() != expected.is_nullable()
				|| !f.metadata().is_empty()
				|| ProofScalarType::from_arrow(f.data_type())? != self.types[i]
			{
				return Err(AdapterError::SchemaMismatch);
			}
		}
		Ok(())
	}
	pub fn encode_key(&self, b: &RecordBatch, row: usize) -> Result<Vec<u8>, AdapterError> {
		self.check_batch(b)?;
		let value = cell(b.column(self.pk).as_ref(), row, self.types[self.pk])?
			.ok_or(AdapterError::InvalidValue)?;
		let descriptor = self.types[self.pk].descriptor();
		let size = 12usize
			.checked_add(descriptor.len())
			.and_then(|n| n.checked_add(value.bytes().len()))
			.ok_or(AdapterError::LimitExceeded)?;
		if size > MAX_KEY_INPUT {
			return Err(AdapterError::LimitExceeded);
		}
		let mut out = Vec::with_capacity(size);
		out.extend_from_slice(KEY_MAGIC);
		out.extend_from_slice(&descriptor);
		out.extend_from_slice(&(value.bytes().len() as u32).to_le_bytes());
		out.extend_from_slice(value.bytes());
		Ok(out)
	}
	pub fn validate_key(&self, b: &[u8]) -> Result<(), AdapterError> {
		if b.len() > MAX_KEY_INPUT {
			return Err(AdapterError::LimitExceeded);
		}
		let mut r = Reader::new(b);
		if r.take(8)? != KEY_MAGIC {
			return Err(AdapterError::InvalidValue);
		}
		let t = ProofScalarType::read(&mut r)?;
		if t != self.types[self.pk] || !t.pk_supported() {
			return Err(AdapterError::SchemaMismatch);
		}
		let n = r.u32()? as usize;
		t.validate(r.take(n)?)?;
		r.finish()?;
		Ok(())
	}
	pub fn encode_row(&self, b: &RecordBatch, row: usize) -> Result<Vec<u8>, AdapterError> {
		self.check_batch(b)?;
		let mut size = 10usize;
		for (i, t) in self.types.iter().enumerate() {
			let f = self.schema.field(i);
			let v = cell(b.column(i).as_ref(), row, *t)?;
			if v.is_none() && !f.is_nullable() {
				return Err(AdapterError::InvalidValue);
			}
			size = size
				.checked_add(10 + f.name().len() + t.descriptor().len())
				.and_then(|n| n.checked_add(v.as_ref().map_or(0, |v| v.bytes().len())))
				.ok_or(AdapterError::LimitExceeded)?;
			if size > MAX_CONTENT_INPUT {
				return Err(AdapterError::LimitExceeded);
			}
		}
		let mut out = Vec::with_capacity(size);
		out.extend_from_slice(ROW_MAGIC);
		out.extend_from_slice(&(self.types.len() as u16).to_le_bytes());
		for (i, t) in self.types.iter().enumerate() {
			write_field(
				&mut out,
				i,
				self.schema.field(i),
				*t,
				b.column(i).as_ref(),
				row,
			)?;
		}
		debug_assert_eq!(out.len(), size);
		Ok(out)
	}
	pub fn validate_row(&self, b: &[u8]) -> Result<(), AdapterError> {
		if b.len() > MAX_CONTENT_INPUT {
			return Err(AdapterError::LimitExceeded);
		}
		let mut r = Reader::new(b);
		if r.take(8)? != ROW_MAGIC || r.u16()? as usize != self.types.len() {
			return Err(AdapterError::SchemaMismatch);
		}
		for (i, t) in self.types.iter().enumerate() {
			let f = self.schema.field(i);
			if r.u16()? as usize != i {
				return Err(AdapterError::SchemaMismatch);
			}
			let n = r.u16()? as usize;
			if r.take(n)? != f.name().as_bytes()
				|| ProofScalarType::read(&mut r)? != *t
				|| r.u8()? != u8::from(f.is_nullable())
			{
				return Err(AdapterError::SchemaMismatch);
			}
			let present = r.u8()?;
			let n = r.u32()? as usize;
			let bytes = r.take(n)?;
			match present {
				0 if f.is_nullable() && n == 0 => {}
				1 => t.validate(bytes)?,
				_ => return Err(AdapterError::InvalidValue),
			}
		}
		r.finish()?;
		Ok(())
	}
}
