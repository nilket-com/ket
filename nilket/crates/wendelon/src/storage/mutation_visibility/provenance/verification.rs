use super::*;
use crate::storage::mutation_visibility::adapters::ProofSchema;
use arrow::{
	array::{Array, BinaryArray},
	record_batch::RecordBatch,
};

// Intentionally unconstructable outside cfg(test) until a separately reviewed loader exists.
#[allow(dead_code)]
enum TrustedRows {
	Table(ProofSchema, RecordBatch),
	KeyValues(Option<ProofSchema>, RecordBatch),
}
/// No public constructor. The only linkage constructor is a cfg(test) round-trip adapter.
pub struct TrustedFrameRows {
	bytes: Vec<u8>,
	rows: TrustedRows,
}
redacted_debug!(TrustedFrameRows, "TrustedFrameRows");
impl TrustedFrameRows {
	#[allow(dead_code)]
	pub(in crate::storage::mutation_visibility) fn from_bounded_rows(
		_cap: &crate::storage::mutation_visibility::root2::durable::loader::LoaderCapability2,
		bytes: Vec<u8>,
		schema: Option<ProofSchema>,
		batch: RecordBatch,
		table: bool,
	) -> Result<Self, AdapterError> {
		if table {
			let schema = schema.ok_or(AdapterError::SchemaMismatch)?;
			schema.check_batch(&batch)?;
			Ok(Self {
				bytes,
				rows: TrustedRows::Table(schema, batch),
			})
		} else {
			if batch.num_columns() != 2 || !batch.schema().metadata().is_empty() {
				return Err(AdapterError::SchemaMismatch);
			}
			for (i, name) in ["key", "value"].iter().enumerate() {
				let s = batch.schema();
				let f = s.field(i);
				if f.name() != name
					|| f.data_type() != &arrow::datatypes::DataType::Binary
					|| f.is_nullable()
					|| !f.metadata().is_empty()
				{
					return Err(AdapterError::SchemaMismatch);
				}
			}
			Ok(Self {
				bytes,
				rows: TrustedRows::KeyValues(schema, batch),
			})
		}
	}

	#[cfg(test)]
	pub(crate) fn from_table_frame(
		bytes: Vec<u8>,
		schema: ProofSchema,
	) -> Result<Self, AdapterError> {
		Self::check_frame(&bytes)?;
		let batch = crate::storage::multirow::decode_batch(&bytes)
			.map_err(|_| AdapterError::InvalidProvenance)?;
		let batch = crate::storage::multirow::strip_storage_synthetic_columns(&batch)
			.map_err(|_| AdapterError::InvalidProvenance)?;
		schema.check_batch(&batch)?;
		Ok(Self {
			bytes,
			rows: TrustedRows::Table(schema, batch),
		})
	}
	#[cfg(test)]
	pub(crate) fn from_key_value_frame(
		bytes: Vec<u8>,
		schema: Option<ProofSchema>,
	) -> Result<Self, AdapterError> {
		Self::check_frame(&bytes)?;
		let b = crate::storage::multirow::decode_batch(&bytes)
			.map_err(|_| AdapterError::InvalidProvenance)?;
		if b.num_columns() != 2 || !b.schema().metadata().is_empty() {
			return Err(AdapterError::SchemaMismatch);
		}
		for (i, name) in ["key", "value"].iter().enumerate() {
			let schema = b.schema();
			let f = schema.field(i);
			if f.name() != name
				|| f.data_type() != &arrow::datatypes::DataType::Binary
				|| f.is_nullable()
				|| !f.metadata().is_empty()
			{
				return Err(AdapterError::SchemaMismatch);
			}
		}
		Ok(Self {
			bytes,
			rows: TrustedRows::KeyValues(schema, b),
		})
	}
	#[cfg(test)]
	fn check_frame(bytes: &[u8]) -> Result<(), AdapterError> {
		if bytes.is_empty() || bytes.len() > MAX_PROOF_FRAME_BYTES {
			Err(AdapterError::LimitExceeded)
		} else {
			Ok(())
		}
	}
	fn count(&self) -> usize {
		match &self.rows {
			TrustedRows::Table(_, b) | TrustedRows::KeyValues(_, b) => b.num_rows(),
		}
	}
	fn row(&self, i: usize, manifest: &ParentManifest) -> Result<(Vec<u8>, Vec<u8>), AdapterError> {
		match &self.rows {
			TrustedRows::Table(schema, b) => {
				if manifest.identity.scope != MutationScope::Table
					|| manifest.identity.table != schema.table()
					|| manifest.identity.schema_generation != schema.generation()
					|| manifest.operation.is_delete()
				{
					return Err(AdapterError::InvalidProvenance);
				}
				Ok((schema.encode_key(b, i)?, schema.encode_row(b, i)?))
			}
			TrustedRows::KeyValues(schema, b) => {
				if !matches!(
					manifest.operation,
					MutationOperation::RawPut
						| MutationOperation::RawDelete
						| MutationOperation::Delete
				) {
					return Err(AdapterError::InvalidProvenance);
				}
				let key = b
					.column(0)
					.as_any()
					.downcast_ref::<BinaryArray>()
					.ok_or(AdapterError::SchemaMismatch)?;
				let value = b
					.column(1)
					.as_any()
					.downcast_ref::<BinaryArray>()
					.ok_or(AdapterError::SchemaMismatch)?;
				if key.is_null(i) || value.is_null(i) {
					return Err(AdapterError::InvalidValue);
				}
				CanonicalKeyBytes::new(key.value(i))?;
				CanonicalContentBytes::new(value.value(i))?;
				match schema {
					Some(s)
						if manifest.operation == MutationOperation::Delete
							&& manifest.identity.table == s.table()
							&& manifest.identity.schema_generation == s.generation() =>
					{
						s.validate_key(key.value(i))?
					}
					None if manifest.identity.scope == MutationScope::Raw => {}
					_ => return Err(AdapterError::SchemaMismatch),
				}
				if manifest.operation.is_delete() && !value.value(i).is_empty() {
					return Err(AdapterError::InvalidValue);
				}
				Ok((key.value(i).to_vec(), value.value(i).to_vec()))
			}
		}
	}
}

pub struct ContentVerifiedFrameProvenance {
	provenance: UnverifiedFrameProvenance,
}
redacted_debug!(
	ContentVerifiedFrameProvenance,
	"ContentVerifiedFrameProvenance"
);
impl ContentVerifiedFrameProvenance {
	pub fn provenance(&self) -> &UnverifiedFrameProvenance {
		&self.provenance
	}
}
impl UnverifiedFrameProvenance {
	pub fn verify(
		self,
		provider: &impl ProofKeyProvider,
		rows: &TrustedFrameRows,
	) -> Result<ContentVerifiedFrameProvenance, AdapterError> {
		self.validate()?;
		if rows.bytes.len() != self.frame_len as usize
			|| blake3::hash(&rows.bytes).as_bytes() != &self.frame_digest
			|| rows.count() != self.physical_rows as usize
		{
			return Err(AdapterError::InvalidProvenance);
		}
		// Preflight aggregate canonical size, BEFORE provider calls. No collect-all buffers.
		let mut total = 0usize;
		for r in &self.references {
			let (key, content) = rows.row(
				r.physical_row as usize,
				&self.parents[r.parent as usize].manifest,
			)?;
			total = total
				.checked_add(key.len() + content.len())
				.ok_or(AdapterError::LimitExceeded)?;
			if total > MAX_PROOF_FRAME_BYTES {
				return Err(AdapterError::LimitExceeded);
			}
		}
		for p in &self.parents {
			if parent_manifest_commitment(provider, &p.manifest)? != p.commitment {
				return Err(AdapterError::InvalidProvenance);
			}
		}
		for r in &self.references {
			let p = &self.parents[r.parent as usize];
			let m = &p.manifest;
			let v = &m.children[r.child as usize].versions[r.row as usize];
			let (key, content) = rows.row(r.physical_row as usize, m)?;
			let binding = m.binding();
			let token = key_token(
				provider,
				&binding,
				&m.identity,
				CanonicalKeyBytes::new(&key)?,
			)?;
			if token != v.key_token {
				return Err(AdapterError::InvalidProvenance);
			}
			let context = RowCommitmentContext {
				identity: m.identity,
				admission: m.admission,
				operation: m.operation,
				child_ordinal: r.child,
				row_ordinal: r.row,
				key_token: token,
				tombstone: v.tombstone,
			};
			if row_content_commitment(
				provider,
				&binding,
				&context,
				CanonicalContentBytes::new(&content)?,
			)? != v.content_commitment
			{
				return Err(AdapterError::InvalidProvenance);
			}
		}
		Ok(ContentVerifiedFrameProvenance { provenance: self })
	}
}
