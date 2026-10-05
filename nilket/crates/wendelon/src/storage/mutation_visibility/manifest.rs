use super::{
	Admission, MutationIdentity, MutationScope, ProofKeyBinding, VisibilityError, encoding::Reader,
	redacted_debug,
};
use std::collections::BTreeSet;

pub const MAX_CHILDREN: usize = 256;
pub const MAX_LOGICAL_VERSIONS: usize = 512;
pub const MAX_MANIFEST_BYTES: usize = 65536;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MutationOperation {
	Insert = 1,
	Update = 2,
	Delete = 3,
	RawPut = 4,
	RawDelete = 5,
}
impl MutationOperation {
	pub(super) fn read(tag: u8) -> Result<Self, VisibilityError> {
		match tag {
			1 => Ok(Self::Insert),
			2 => Ok(Self::Update),
			3 => Ok(Self::Delete),
			4 => Ok(Self::RawPut),
			5 => Ok(Self::RawDelete),
			_ => Err(VisibilityError::InvalidManifest),
		}
	}
	pub fn is_delete(self) -> bool {
		matches!(self, Self::Delete | Self::RawDelete)
	}
}
#[derive(Clone, PartialEq, Eq)]
pub struct LogicalVersion {
	pub ordinal: u32,
	pub key_token: [u8; 32],
	pub content_commitment: [u8; 32],
	pub tombstone: bool,
}
redacted_debug!(LogicalVersion, "LogicalVersion");
#[derive(Clone, PartialEq, Eq)]
pub struct ManifestChild {
	pub ordinal: u32,
	pub range_start: u64,
	pub range_end: u64,
	pub partition: Option<u64>,
	pub versions: Vec<LogicalVersion>,
}
redacted_debug!(ManifestChild, "ManifestChild");
#[derive(Clone, PartialEq, Eq)]
pub struct ParentManifest {
	pub identity: MutationIdentity,
	pub admission: Admission,
	pub key_id: [u8; 16],
	pub key_version: u32,
	pub operation: MutationOperation,
	pub content_codec: u16,
	pub children: Vec<ManifestChild>,
}
redacted_debug!(ParentManifest, "ParentManifest");
impl ParentManifest {
	pub fn binding(&self) -> ProofKeyBinding {
		ProofKeyBinding {
			generation: self.identity.generation,
			tenant: self.identity.tenant,
			key_id: self.key_id,
			key_version: self.key_version,
		}
	}
	pub fn validate(&self) -> Result<usize, VisibilityError> {
		self.identity.validate()?;
		self.admission.encode()?;
		self.binding().encode()?;
		let correct_scope = match self.operation {
			MutationOperation::RawPut | MutationOperation::RawDelete => MutationScope::Raw,
			_ => MutationScope::Table,
		};
		if self.identity.scope != correct_scope || self.content_codec != 1 {
			return Err(VisibilityError::InvalidManifest);
		}
		if self.children.is_empty() || self.children.len() > MAX_CHILDREN {
			return Err(VisibilityError::LimitExceeded);
		}
		let mut total = 0usize;
		let mut tokens = BTreeSet::new();
		for (i, c) in self.children.iter().enumerate() {
			if c.ordinal as usize != i || c.range_start > c.range_end || c.partition == Some(0) {
				return Err(VisibilityError::InvalidManifest);
			}
			total = total
				.checked_add(c.versions.len())
				.ok_or(VisibilityError::LimitExceeded)?;
			if c.versions.is_empty() || total > MAX_LOGICAL_VERSIONS {
				return Err(VisibilityError::LimitExceeded);
			}
			for (j, v) in c.versions.iter().enumerate() {
				if v.ordinal as usize != j
					|| v.tombstone != self.operation.is_delete()
					|| !tokens.insert(v.key_token)
				{
					return Err(VisibilityError::InvalidManifest);
				}
			}
		}
		let size = 112usize
			.checked_add(
				self.children
					.len()
					.checked_mul(31)
					.ok_or(VisibilityError::LimitExceeded)?,
			)
			.and_then(|s| s.checked_add(total * 69))
			.ok_or(VisibilityError::LimitExceeded)?;
		if size > MAX_MANIFEST_BYTES {
			return Err(VisibilityError::LimitExceeded);
		}
		Ok(size)
	}
	pub fn encode(&self) -> Result<Vec<u8>, VisibilityError> {
		let mut b = Vec::with_capacity(self.validate()?);
		b.extend_from_slice(&1u16.to_le_bytes());
		b.extend_from_slice(&self.identity.encode()?);
		b.extend_from_slice(&self.admission.encode()?);
		b.extend_from_slice(&self.key_id);
		b.extend_from_slice(&self.key_version.to_le_bytes());
		b.push(self.operation as u8);
		b.extend_from_slice(&self.content_codec.to_le_bytes());
		b.extend_from_slice(&(self.children.len() as u16).to_le_bytes());
		for c in &self.children {
			b.extend_from_slice(&c.ordinal.to_le_bytes());
			b.extend_from_slice(&c.range_start.to_le_bytes());
			b.extend_from_slice(&c.range_end.to_le_bytes());
			b.push(u8::from(c.partition.is_some()));
			b.extend_from_slice(&c.partition.unwrap_or(0).to_le_bytes());
			b.extend_from_slice(&(c.versions.len() as u16).to_le_bytes());
			for v in &c.versions {
				b.extend_from_slice(&v.ordinal.to_le_bytes());
				b.extend_from_slice(&v.key_token);
				b.extend_from_slice(&v.content_commitment);
				b.push(u8::from(v.tombstone));
			}
		}
		Ok(b)
	}
	pub fn decode(b: &[u8]) -> Result<Self, VisibilityError> {
		if b.len() > MAX_MANIFEST_BYTES {
			return Err(VisibilityError::LimitExceeded);
		}
		let mut r = Reader::new(b);
		if r.u16()? != 1 {
			return Err(VisibilityError::UnknownFormat);
		}
		let identity = MutationIdentity::decode(r.take(73)?)?;
		let admission = Admission::read(&mut r)?;
		let key_id = r.array()?;
		let key_version = r.u32()?;
		let operation = MutationOperation::read(r.u8()?)?;
		let content_codec = r.u16()?;
		let count = r.u16()? as usize;
		if count == 0 || count > MAX_CHILDREN {
			return Err(VisibilityError::LimitExceeded);
		}
		// Even the child headers must be present before any count-based allocation.
		if b.len() < 112 + count * 31 {
			return Err(VisibilityError::Truncated);
		}
		let mut children = Vec::with_capacity(count);
		let mut total = 0usize;
		for _ in 0..count {
			let ordinal = r.u32()?;
			let range_start = r.u64()?;
			let range_end = r.u64()?;
			let flag = r.u8()?;
			let id = r.u64()?;
			let partition = match (flag, id) {
				(0, 0) => None,
				(1, n) if n > 0 => Some(n),
				_ => return Err(VisibilityError::InvalidManifest),
			};
			let n = r.u16()? as usize;
			total = total.checked_add(n).ok_or(VisibilityError::LimitExceeded)?;
			if n == 0 || total > MAX_LOGICAL_VERSIONS {
				return Err(VisibilityError::LimitExceeded);
			}
			let rows = r.take(n * 69)?;
			let mut rr = Reader::new(rows);
			let mut versions = Vec::with_capacity(n);
			for _ in 0..n {
				let ordinal = rr.u32()?;
				let key_token = rr.array()?;
				let content_commitment = rr.array()?;
				let tombstone = match rr.u8()? {
					0 => false,
					1 => true,
					_ => return Err(VisibilityError::InvalidManifest),
				};
				versions.push(LogicalVersion {
					ordinal,
					key_token,
					content_commitment,
					tombstone,
				});
			}
			children.push(ManifestChild {
				ordinal,
				range_start,
				range_end,
				partition,
				versions,
			});
		}
		r.finish()?;
		let m = Self {
			identity,
			admission,
			key_id,
			key_version,
			operation,
			content_codec,
			children,
		};
		m.validate()?;
		Ok(m)
	}
}
