use super::{VisibilityError, encoding::Reader, redacted_debug};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MutationScope {
	Raw = 0,
	Table = 1,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MutationIdentity {
	pub generation: [u8; 16],
	pub tenant: u64,
	pub scope: MutationScope,
	pub table: [u8; 16],
	pub schema_generation: u64,
	pub caller: [u8; 16],
	pub sequence: u64,
}
redacted_debug!(MutationIdentity, "MutationIdentity");
impl MutationIdentity {
	pub fn validate(&self) -> Result<(), VisibilityError> {
		if self.generation == [0; 16] || self.caller == [0; 16] {
			return Err(VisibilityError::InvalidIdentity);
		}
		match self.scope {
			MutationScope::Raw if self.table != [0; 16] || self.schema_generation != 0 => {
				Err(VisibilityError::InvalidIdentity)
			}
			MutationScope::Table if self.table == [0; 16] || self.schema_generation == 0 => {
				Err(VisibilityError::InvalidIdentity)
			}
			_ => Ok(()),
		}
	}
	pub fn encode(&self) -> Result<[u8; 73], VisibilityError> {
		self.validate()?;
		let mut out = [0; 73];
		out[..16].copy_from_slice(&self.generation);
		out[16..24].copy_from_slice(&self.tenant.to_le_bytes());
		out[24] = self.scope as u8;
		out[25..41].copy_from_slice(&self.table);
		out[41..49].copy_from_slice(&self.schema_generation.to_le_bytes());
		out[49..65].copy_from_slice(&self.caller);
		out[65..].copy_from_slice(&self.sequence.to_le_bytes());
		Ok(out)
	}
	pub fn decode(bytes: &[u8]) -> Result<Self, VisibilityError> {
		let mut r = Reader::new(bytes);
		let generation = r.array()?;
		let tenant = r.u64()?;
		let scope = match r.u8()? {
			0 => MutationScope::Raw,
			1 => MutationScope::Table,
			_ => return Err(VisibilityError::InvalidIdentity),
		};
		let value = Self {
			generation,
			tenant,
			scope,
			table: r.array()?,
			schema_generation: r.u64()?,
			caller: r.array()?,
			sequence: r.u64()?,
		};
		r.finish()?;
		value.validate()?;
		Ok(value)
	}
	/// UUID+sequence, deliberately excludes wire TTL and padding hints.
	pub fn caller_bytes(&self) -> [u8; 24] {
		let mut bytes = [0; 24];
		bytes[..16].copy_from_slice(&self.caller);
		bytes[16..].copy_from_slice(&self.sequence.to_le_bytes());
		bytes
	}
	pub fn key_namespace(&self) -> Result<[u8; 41], VisibilityError> {
		Ok(self.encode()?[..41]
			.try_into()
			.expect("fixed namespace length"))
	}
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Admission {
	pub epoch: u64,
	pub holder: u32,
}
redacted_debug!(Admission, "Admission");
impl Admission {
	pub fn encode(&self) -> Result<[u8; 12], VisibilityError> {
		if self.epoch == 0 || self.holder == 0 {
			return Err(VisibilityError::InvalidIdentity);
		}
		let mut bytes = [0; 12];
		bytes[..8].copy_from_slice(&self.epoch.to_le_bytes());
		bytes[8..].copy_from_slice(&self.holder.to_le_bytes());
		Ok(bytes)
	}
	pub(super) fn read(r: &mut Reader<'_>) -> Result<Self, VisibilityError> {
		let value = Self {
			epoch: r.u64()?,
			holder: r.u32()?,
		};
		value.encode()?;
		Ok(value)
	}
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ProofKeyBinding {
	pub generation: [u8; 16],
	pub tenant: u64,
	pub key_id: [u8; 16],
	pub key_version: u32,
}
redacted_debug!(ProofKeyBinding, "ProofKeyBinding");
impl ProofKeyBinding {
	pub fn encode(&self) -> Result<[u8; 44], VisibilityError> {
		if self.generation == [0; 16] || self.key_id == [0; 16] || self.key_version == 0 {
			return Err(VisibilityError::InvalidIdentity);
		}
		let mut b = [0; 44];
		b[..16].copy_from_slice(&self.generation);
		b[16..24].copy_from_slice(&self.tenant.to_le_bytes());
		b[24..40].copy_from_slice(&self.key_id);
		b[40..].copy_from_slice(&self.key_version.to_le_bytes());
		Ok(b)
	}
	pub fn matches(&self, identity: &MutationIdentity) -> bool {
		self.generation == identity.generation && self.tenant == identity.tenant
	}
}
