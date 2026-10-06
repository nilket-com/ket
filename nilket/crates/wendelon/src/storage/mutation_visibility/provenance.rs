//! Dormant per-frame metadata. Structural integrity is not acceptance or durability.
mod verification;
use super::{
	adapters::AdapterError,
	encoding::{Reader, envelope, open_envelope},
	*,
};
use std::collections::BTreeSet;
pub use verification::{ContentVerifiedFrameProvenance, TrustedFrameRows};
pub const MAX_PROVENANCE_BYTES: usize = 1048576;
pub const MAX_PROOF_FRAME_BYTES: usize = 16777216;
const MAGIC: &[u8; 8] = b"VISPROV1";

#[derive(Clone, PartialEq, Eq)]
pub struct ProvenanceParent {
	pub manifest: ParentManifest,
	pub commitment: [u8; 32],
}
redacted_debug!(ProvenanceParent, "ProvenanceParent");
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ProvenanceReference {
	pub physical_row: u32,
	pub parent: u16,
	pub child: u32,
	pub row: u32,
}
redacted_debug!(ProvenanceReference, "ProvenanceReference");
/// Kept private: mutation after validation must not bypass bounds or dictionary checks.
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedFrameProvenance {
	scope: [u8; 49],
	frame_len: u32,
	frame_digest: [u8; 32],
	physical_rows: u32,
	parents: Vec<ProvenanceParent>,
	references: Vec<ProvenanceReference>,
}
redacted_debug!(UnverifiedFrameProvenance, "UnverifiedFrameProvenance");
impl UnverifiedFrameProvenance {
	#[cfg(test)]
	pub(crate) fn owned_metadata_bytes(&self) -> usize {
		std::mem::size_of::<Self>()
			+ self.parents.capacity() * std::mem::size_of::<ProvenanceParent>()
			+ self.references.capacity() * std::mem::size_of::<ProvenanceReference>()
			+ self
				.parents
				.iter()
				.map(|p| {
					p.manifest.children.capacity() * std::mem::size_of::<ManifestChild>()
						+ p.manifest
							.children
							.iter()
							.map(|c| c.versions.capacity() * std::mem::size_of::<LogicalVersion>())
							.sum::<usize>()
				})
				.sum::<usize>()
	}
	pub fn new(
		scope: &MutationIdentity,
		frame: &[u8],
		parents: Vec<ProvenanceParent>,
		references: Vec<ProvenanceReference>,
	) -> Result<Self, AdapterError> {
		if frame.is_empty() || frame.len() > MAX_PROOF_FRAME_BYTES {
			return Err(AdapterError::LimitExceeded);
		}
		let value = Self {
			scope: scope.encode()?[..49]
				.try_into()
				.map_err(|_| AdapterError::InvalidValue)?,
			frame_len: frame.len() as u32,
			frame_digest: *blake3::hash(frame).as_bytes(),
			physical_rows: references
				.len()
				.try_into()
				.map_err(|_| AdapterError::LimitExceeded)?,
			parents,
			references,
		};
		value.validate()?;
		Ok(value)
	}
	pub fn parents(&self) -> &[ProvenanceParent] {
		&self.parents
	}
	pub fn references(&self) -> &[ProvenanceReference] {
		&self.references
	}
	fn validate(&self) -> Result<usize, AdapterError> {
		if self.frame_len == 0
			|| self.frame_len as usize > MAX_PROOF_FRAME_BYTES
			|| self.physical_rows == 0
			|| self.physical_rows as usize > MAX_LOGICAL_VERSIONS
			|| self.references.len() != self.physical_rows as usize
			|| self.parents.is_empty()
			|| self.parents.len() > 512
		{
			return Err(AdapterError::LimitExceeded);
		}
		let mut size = 95usize + 14 * self.references.len();
		let mut previous = None;
		for p in &self.parents {
			let id = p.manifest.identity.encode()?;
			if id[..49] != self.scope || previous.is_some_and(|old| old >= id) {
				return Err(AdapterError::InvalidProvenance);
			}
			previous = Some(id);
			size = size
				.checked_add(36 + p.manifest.validate()?)
				.ok_or(AdapterError::LimitExceeded)?;
		}
		if size + 46 > MAX_PROVENANCE_BYTES {
			return Err(AdapterError::LimitExceeded);
		}
		let mut used = BTreeSet::new();
		let mut coords = BTreeSet::new();
		for (i, r) in self.references.iter().enumerate() {
			if r.physical_row as usize != i || !coords.insert((r.parent, r.child, r.row)) {
				return Err(AdapterError::InvalidProvenance);
			}
			let p = self
				.parents
				.get(r.parent as usize)
				.ok_or(AdapterError::InvalidProvenance)?;
			p.manifest
				.children
				.get(r.child as usize)
				.and_then(|c| c.versions.get(r.row as usize))
				.ok_or(AdapterError::InvalidProvenance)?;
			used.insert(r.parent);
		}
		if used.len() != self.parents.len() {
			return Err(AdapterError::InvalidProvenance);
		}
		Ok(size)
	}
	pub fn encode(&self) -> Result<Vec<u8>, AdapterError> {
		let mut b = Vec::with_capacity(self.validate()?);
		b.extend_from_slice(&self.scope);
		b.extend_from_slice(&self.frame_len.to_le_bytes());
		b.extend_from_slice(&self.frame_digest);
		b.extend_from_slice(&self.physical_rows.to_le_bytes());
		b.extend_from_slice(&(self.parents.len() as u16).to_le_bytes());
		b.extend_from_slice(&(self.references.len() as u32).to_le_bytes());
		for p in &self.parents {
			let m = p.manifest.encode()?;
			b.extend_from_slice(&(m.len() as u32).to_le_bytes());
			b.extend_from_slice(&m);
			b.extend_from_slice(&p.commitment)
		}
		for r in &self.references {
			b.extend_from_slice(&r.physical_row.to_le_bytes());
			b.extend_from_slice(&r.parent.to_le_bytes());
			b.extend_from_slice(&r.child.to_le_bytes());
			b.extend_from_slice(&r.row.to_le_bytes())
		}
		Ok(envelope(MAGIC, &b, MAX_PROVENANCE_BYTES)?)
	}
	pub fn decode(bytes: &[u8]) -> Result<Self, AdapterError> {
		let payload = open_envelope(bytes, MAGIC, MAX_PROVENANCE_BYTES)?;
		let mut r = Reader::new(payload);
		let scope = r.array()?;
		let frame_len = r.u32()?;
		let frame_digest = r.array()?;
		let physical_rows = r.u32()?;
		let n = r.u16()? as usize;
		let nr = r.u32()? as usize;
		if n == 0
			|| n > 512
			|| nr == 0
			|| nr > 512
			|| nr != physical_rows as usize
			|| frame_len == 0
			|| frame_len as usize > MAX_PROOF_FRAME_BYTES
		{
			return Err(AdapterError::LimitExceeded);
		}
		// A's minimum valid manifest is 212 bytes. Check aggregate minimum BEFORE reserve.
		if payload.len() < 95 + n * 248 + nr * 14 {
			return Err(AdapterError::InvalidProvenance);
		}
		let mut parents = Vec::with_capacity(n);
		for _ in 0..n {
			let len = r.u32()? as usize;
			if !(212..=MAX_MANIFEST_BYTES).contains(&len) {
				return Err(AdapterError::LimitExceeded);
			}
			let manifest = ParentManifest::decode(r.take(len)?)?;
			let commitment = r.array()?;
			parents.push(ProvenanceParent {
				manifest,
				commitment,
			});
		}
		let mut references = Vec::with_capacity(nr);
		for _ in 0..nr {
			references.push(ProvenanceReference {
				physical_row: r.u32()?,
				parent: r.u16()?,
				child: r.u32()?,
				row: r.u32()?,
			})
		}
		r.finish()?;
		let value = Self {
			scope,
			frame_len,
			frame_digest,
			physical_rows,
			parents,
			references,
		};
		value.validate()?;
		Ok(value)
	}
}
