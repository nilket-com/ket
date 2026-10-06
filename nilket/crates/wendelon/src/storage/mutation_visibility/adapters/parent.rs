use super::{AdapterError, ProofSchema};
use crate::storage::mutation_visibility::*;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeSet;

#[derive(Clone)]
pub struct ProofChildInput {
	pub range_start: u64,
	pub range_end: u64,
	pub partition: Option<u64>,
	pub input_rows: Vec<usize>,
}
redacted_debug!(ProofChildInput, "ProofChildInput");
fn layout(children: &[ProofChildInput], count: usize) -> Result<(), AdapterError> {
	if count == 0
		|| count > MAX_LOGICAL_VERSIONS
		|| children.is_empty()
		|| children.len() > MAX_CHILDREN
	{
		return Err(AdapterError::LimitExceeded);
	}
	let mut seen = BTreeSet::new();
	for c in children {
		if c.input_rows.is_empty() || c.range_start > c.range_end || c.partition == Some(0) {
			return Err(AdapterError::InvalidValue);
		}
		for row in &c.input_rows {
			if *row >= count || !seen.insert(*row) {
				return Err(AdapterError::InvalidValue);
			}
		}
	}
	if seen.len() != count {
		return Err(AdapterError::InvalidValue);
	}
	Ok(())
}
fn build(
	provider: &impl ProofKeyProvider,
	id: MutationIdentity,
	admission: Admission,
	binding: ProofKeyBinding,
	op: MutationOperation,
	children: &[ProofChildInput],
	mut row: impl FnMut(usize) -> Result<(Vec<u8>, Vec<u8>), AdapterError>,
) -> Result<ParentManifest, AdapterError> {
	id.validate()?;
	admission.encode()?;
	if !binding.matches(&id) {
		return Err(AdapterError::InvalidValue);
	}
	binding.encode()?;
	let mut result = ParentManifest {
		identity: id,
		admission,
		key_id: binding.key_id,
		key_version: binding.key_version,
		operation: op,
		content_codec: 1,
		children: Vec::with_capacity(children.len()),
	};
	for (child, c) in children.iter().enumerate() {
		let mut versions = Vec::with_capacity(c.input_rows.len());
		for (ordinal, input) in c.input_rows.iter().enumerate() {
			let (key, content) = row(*input)?;
			let token = key_token(provider, &binding, &id, CanonicalKeyBytes::new(&key)?)?;
			let context = RowCommitmentContext {
				identity: id,
				admission,
				operation: op,
				child_ordinal: child as u32,
				row_ordinal: ordinal as u32,
				key_token: token,
				tombstone: op.is_delete(),
			};
			let commitment = row_content_commitment(
				provider,
				&binding,
				&context,
				CanonicalContentBytes::new(&content)?,
			)?;
			versions.push(LogicalVersion {
				ordinal: ordinal as u32,
				key_token: token,
				content_commitment: commitment,
				tombstone: op.is_delete(),
			});
		}
		result.children.push(ManifestChild {
			ordinal: child as u32,
			range_start: c.range_start,
			range_end: c.range_end,
			partition: c.partition,
			versions,
		});
	}
	result.validate()?;
	Ok(result)
}
pub fn build_table_parent(
	provider: &impl ProofKeyProvider,
	id: MutationIdentity,
	admission: Admission,
	binding: ProofKeyBinding,
	op: MutationOperation,
	schema: &ProofSchema,
	batch: &RecordBatch,
	original_keys: Option<&[Vec<u8>]>,
	children: &[ProofChildInput],
) -> Result<ParentManifest, AdapterError> {
	if id.scope != MutationScope::Table
		|| id.table != schema.table()
		|| id.schema_generation != schema.generation()
		|| !matches!(op, MutationOperation::Insert | MutationOperation::Update)
	{
		return Err(AdapterError::InvalidValue);
	}
	schema.check_batch(batch)?;
	layout(children, batch.num_rows())?;
	match (op, original_keys) {
		(MutationOperation::Insert, None) => {}
		(MutationOperation::Update, Some(k)) if k.len() == batch.num_rows() => {}
		_ => return Err(AdapterError::InvalidValue),
	}
	build(provider, id, admission, binding, op, children, |i| {
		let key = schema.encode_key(batch, i)?;
		if let Some(keys) = original_keys {
			schema.validate_key(&keys[i])?;
			if keys[i] != key {
				return Err(AdapterError::KeyChanged);
			}
		}
		Ok((key, schema.encode_row(batch, i)?))
	})
}
pub fn build_table_delete_parent(
	provider: &impl ProofKeyProvider,
	id: MutationIdentity,
	admission: Admission,
	binding: ProofKeyBinding,
	schema: &ProofSchema,
	keys: &[Vec<u8>],
	children: &[ProofChildInput],
) -> Result<ParentManifest, AdapterError> {
	if id.scope != MutationScope::Table
		|| id.table != schema.table()
		|| id.schema_generation != schema.generation()
	{
		return Err(AdapterError::SchemaMismatch);
	}
	layout(children, keys.len())?;
	build(
		provider,
		id,
		admission,
		binding,
		MutationOperation::Delete,
		children,
		|i| {
			schema.validate_key(&keys[i])?;
			Ok((keys[i].clone(), vec![]))
		},
	)
}
pub fn build_raw_parent(
	provider: &impl ProofKeyProvider,
	id: MutationIdentity,
	admission: Admission,
	binding: ProofKeyBinding,
	key: &[u8],
	value: Option<&[u8]>,
	child: &ProofChildInput,
) -> Result<ParentManifest, AdapterError> {
	if id.scope != MutationScope::Raw {
		return Err(AdapterError::InvalidValue);
	}
	layout(std::slice::from_ref(child), 1)?;
	CanonicalKeyBytes::new(key)?;
	CanonicalContentBytes::new(value.unwrap_or(&[]))?;
	build(
		provider,
		id,
		admission,
		binding,
		if value.is_some() {
			MutationOperation::RawPut
		} else {
			MutationOperation::RawDelete
		},
		std::slice::from_ref(child),
		|_| Ok((key.to_vec(), value.unwrap_or(&[]).to_vec())),
	)
}
pub fn append_only_key_input(
	id: &MutationIdentity,
	op: MutationOperation,
	original_input_ordinal: u32,
) -> Result<[u8; 77], AdapterError> {
	if id.scope != MutationScope::Table
		|| op != MutationOperation::Insert
		|| original_input_ordinal >= 512
	{
		return Err(AdapterError::InvalidValue);
	}
	let mut b = [0; 77];
	b[..8].copy_from_slice(b"VTAPP001");
	b[8..49].copy_from_slice(&id.key_namespace()?);
	b[49..73].copy_from_slice(&id.caller_bytes());
	b[73..].copy_from_slice(&original_input_ordinal.to_le_bytes());
	Ok(b)
}
