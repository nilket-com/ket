use super::{
	Admission, MutationIdentity, MutationOperation, ParentManifest, ProofKeyBinding,
	VisibilityError, redacted_debug,
};

pub const MAX_KEY_INPUT: usize = 4096;
pub const MAX_CONTENT_INPUT: usize = 1048576;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofKeyUnavailable {
	Unavailable,
	WrongScope,
	WrongVersion,
	NotAuthorized,
	BackendFailure,
}
/// No default implementation. Resolve protected keys before this synchronous, nonblocking call.
pub trait ProofKeyProvider {
	fn keyed_digest(
		&self,
		binding: &ProofKeyBinding,
		canonical_preimage: &[u8],
	) -> Result<[u8; 32], ProofKeyUnavailable>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitmentError {
	InvalidInput(VisibilityError),
	Key(ProofKeyUnavailable),
}
impl From<VisibilityError> for CommitmentError {
	fn from(e: VisibilityError) -> Self {
		Self::InvalidInput(e)
	}
}
impl From<ProofKeyUnavailable> for CommitmentError {
	fn from(e: ProofKeyUnavailable) -> Self {
		Self::Key(e)
	}
}
/// Trusted canonical input: construction checks lengths, not SQL/Arrow logical equivalence.
pub struct CanonicalKeyBytes<'a>(&'a [u8]);
redacted_debug!(CanonicalKeyBytes<'_>, "CanonicalKeyBytes");
impl<'a> CanonicalKeyBytes<'a> {
	pub fn new(b: &'a [u8]) -> Result<Self, VisibilityError> {
		if b.is_empty() || b.len() > MAX_KEY_INPUT {
			Err(VisibilityError::LimitExceeded)
		} else {
			Ok(Self(b))
		}
	}
}
pub struct CanonicalContentBytes<'a>(&'a [u8]);
redacted_debug!(CanonicalContentBytes<'_>, "CanonicalContentBytes");
impl<'a> CanonicalContentBytes<'a> {
	pub fn new(b: &'a [u8]) -> Result<Self, VisibilityError> {
		if b.len() > MAX_CONTENT_INPUT {
			Err(VisibilityError::LimitExceeded)
		} else {
			Ok(Self(b))
		}
	}
}
fn prefix(
	domain: &[u8],
	binding: &ProofKeyBinding,
	id: &MutationIdentity,
) -> Result<Vec<u8>, VisibilityError> {
	id.validate()?;
	if !binding.matches(id) {
		return Err(VisibilityError::InvalidIdentity);
	}
	let mut b = Vec::with_capacity(domain.len() + 44);
	b.extend_from_slice(domain);
	b.extend_from_slice(&binding.encode()?);
	Ok(b)
}
pub fn key_token(
	provider: &impl ProofKeyProvider,
	binding: &ProofKeyBinding,
	id: &MutationIdentity,
	key: CanonicalKeyBytes<'_>,
) -> Result<[u8; 32], CommitmentError> {
	let mut b = prefix(b"wendelon/adr0100/key-token/v1\0", binding, id)?;
	b.extend_from_slice(&id.key_namespace()?);
	b.extend_from_slice(&1u16.to_le_bytes());
	b.extend_from_slice(&(key.0.len() as u32).to_le_bytes());
	b.extend_from_slice(key.0);
	Ok(provider.keyed_digest(binding, &b)?)
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RowCommitmentContext {
	pub identity: MutationIdentity,
	pub admission: Admission,
	pub operation: MutationOperation,
	pub child_ordinal: u32,
	pub row_ordinal: u32,
	pub key_token: [u8; 32],
	pub tombstone: bool,
}
redacted_debug!(RowCommitmentContext, "RowCommitmentContext");
pub fn row_content_commitment(
	provider: &impl ProofKeyProvider,
	binding: &ProofKeyBinding,
	context: &RowCommitmentContext,
	content: CanonicalContentBytes<'_>,
) -> Result<[u8; 32], CommitmentError> {
	let correct_scope = match context.operation {
		MutationOperation::RawPut | MutationOperation::RawDelete => super::MutationScope::Raw,
		_ => super::MutationScope::Table,
	};
	if context.identity.scope != correct_scope
		|| context.tombstone != context.operation.is_delete()
		|| (context.tombstone && !content.0.is_empty())
		|| context.child_ordinal >= super::MAX_CHILDREN as u32
		|| context.row_ordinal >= super::MAX_LOGICAL_VERSIONS as u32
	{
		return Err(VisibilityError::InvalidManifest.into());
	}
	let mut b = prefix(
		b"wendelon/adr0100/row-content/v1\0",
		binding,
		&context.identity,
	)?;
	b.extend_from_slice(&context.identity.encode()?);
	b.extend_from_slice(&context.admission.encode()?);
	b.push(context.operation as u8);
	b.extend_from_slice(&context.child_ordinal.to_le_bytes());
	b.extend_from_slice(&context.row_ordinal.to_le_bytes());
	b.extend_from_slice(&context.key_token);
	b.push(u8::from(context.tombstone));
	b.extend_from_slice(&1u16.to_le_bytes());
	b.extend_from_slice(&(content.0.len() as u32).to_le_bytes());
	b.extend_from_slice(content.0);
	Ok(provider.keyed_digest(binding, &b)?)
}
pub fn parent_manifest_commitment(
	provider: &impl ProofKeyProvider,
	manifest: &ParentManifest,
) -> Result<[u8; 32], CommitmentError> {
	let canonical = manifest.encode()?;
	let binding = manifest.binding();
	let mut b = prefix(
		b"wendelon/adr0100/parent-manifest/v1\0",
		&binding,
		&manifest.identity,
	)?;
	b.extend_from_slice(&(canonical.len() as u32).to_le_bytes());
	b.extend_from_slice(&canonical);
	Ok(provider.keyed_digest(&binding, &b)?)
}

#[cfg(test)]
pub(super) struct TestProofKey {
	binding: ProofKeyBinding,
	secret: zeroize::Zeroizing<[u8; 32]>,
}
#[cfg(test)]
impl TestProofKey {
	pub fn new(binding: ProofKeyBinding, secret: [u8; 32]) -> Self {
		Self {
			binding,
			secret: zeroize::Zeroizing::new(secret),
		}
	}
}
#[cfg(test)]
impl ProofKeyProvider for TestProofKey {
	fn keyed_digest(
		&self,
		binding: &ProofKeyBinding,
		preimage: &[u8],
	) -> Result<[u8; 32], ProofKeyUnavailable> {
		if binding.generation != self.binding.generation || binding.tenant != self.binding.tenant {
			return Err(ProofKeyUnavailable::WrongScope);
		}
		if binding.key_id != self.binding.key_id {
			return Err(ProofKeyUnavailable::Unavailable);
		}
		if binding.key_version != self.binding.key_version {
			return Err(ProofKeyUnavailable::WrongVersion);
		}
		let mut h = blake3::Hasher::new_keyed(&self.secret);
		h.update(preimage);
		Ok(*h.finalize().as_bytes())
	}
}
