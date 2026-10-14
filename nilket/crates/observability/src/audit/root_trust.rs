//! Explicit, format-scoped audit root enrollment. No embedded-key fallback.
use super::{
	AuditChainHead, AuditFormat, AuditNodeId, SignedAuditChainRoot, signed_root_payload_bytes,
};
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signature, VerifyingKey};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditTrustError {
	DuplicateNode { node: u32 },
	InvalidTrustedKey { node: u32 },
	InvalidNode,
}
impl std::fmt::Display for AuditTrustError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "audit trust refused: {self:?}")
	}
}
impl std::error::Error for AuditTrustError {}

#[derive(Debug, Clone)]
pub struct AuditRootTrust {
	keys: BTreeMap<String, [u8; 32]>,
	// The full signature namespace, rather than a parsed numeric identity, binds enrollment.
	chain_root_tag: &'static [u8],
	root_signature_tag: &'static str,
}
impl AuditRootTrust {
	pub fn new(
		format: AuditFormat,
		entries: impl IntoIterator<Item = (u32, [u8; 32])>,
	) -> Result<Self, AuditTrustError> {
		let mut keys = BTreeMap::new();
		for (node, key) in entries {
			if node == 0 {
				return Err(AuditTrustError::InvalidNode);
			}
			let wire = (format.canonical_node_wire)(node);
			if (format.parse_node_numeric)(&wire).ok() != Some(node) {
				return Err(AuditTrustError::InvalidNode);
			}
			if keys.contains_key(&wire) {
				return Err(AuditTrustError::DuplicateNode { node });
			}
			let point = CompressedEdwardsY(key)
				.decompress()
				.ok_or(AuditTrustError::InvalidTrustedKey { node })?;
			if point.compress().to_bytes() != key
				|| !point.is_torsion_free()
				|| point.is_small_order()
			{
				return Err(AuditTrustError::InvalidTrustedKey { node });
			}
			let vk = VerifyingKey::from_bytes(&key)
				.map_err(|_| AuditTrustError::InvalidTrustedKey { node })?;
			if vk.is_weak() {
				return Err(AuditTrustError::InvalidTrustedKey { node });
			}
			keys.insert(wire, key);
		}
		Ok(Self {
			keys,
			chain_root_tag: format.chain_root_tag,
			root_signature_tag: format.root_signature_tag,
		})
	}
	fn key(&self, format: AuditFormat, wire: &str) -> Option<&[u8; 32]> {
		if self.chain_root_tag != format.chain_root_tag
			|| self.root_signature_tag != format.root_signature_tag
		{
			return None;
		}
		self.keys.get(wire)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditRootVerifyError {
	Algorithm,
	NodeParse,
	NonCanonicalNode,
	UntrustedNode { node: u32 },
	MalformedKey,
	KeyMismatch { node: u32 },
	MalformedSignature,
	Signature,
}
impl std::fmt::Display for AuditRootVerifyError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "audit root refused: {self:?}")
	}
}
impl std::error::Error for AuditRootVerifyError {}
impl AuditRootVerifyError {
	pub(super) fn issue_code(self) -> &'static str {
		match self {
			Self::Algorithm | Self::NodeParse => "invalid_signed_root",
			Self::NonCanonicalNode => "audit_root_noncanonical_node",
			Self::UntrustedNode { .. } => "audit_root_untrusted_node",
			Self::MalformedKey | Self::MalformedSignature => "audit_root_malformed",
			Self::KeyMismatch { .. } => "audit_root_key_mismatch",
			Self::Signature => "audit_root_bad_signature",
		}
	}
}
// Bounded, sanitized decoding; the historical generic decode_hex helper echoes input.
fn decode<const N: usize>(raw: &str) -> Option<[u8; N]> {
	if raw.len() != N * 2 {
		return None;
	}
	let mut out = [0; N];
	for (slot, pair) in out.iter_mut().zip(raw.as_bytes().chunks_exact(2)) {
		let nibble = |b| match b {
			b'0'..=b'9' => Some(b - b'0'),
			b'a'..=b'f' => Some(b - b'a' + 10),
			b'A'..=b'F' => Some(b - b'A' + 10),
			_ => None,
		};
		*slot = (nibble(pair[0])? << 4) | nibble(pair[1])?;
	}
	Some(out)
}
pub fn verify_signed_chain_root(
	format: AuditFormat,
	root: &SignedAuditChainRoot,
	trust: &AuditRootTrust,
) -> Result<(), AuditRootVerifyError> {
	use AuditRootVerifyError as E;
	if root.signature_algorithm_tag != format.root_signature_tag {
		return Err(E::Algorithm);
	}
	let node = (format.parse_node_numeric)(&root.node_id).map_err(|_| E::NodeParse)?;
	if (format.canonical_node_wire)(node) != root.node_id {
		return Err(E::NonCanonicalNode);
	}
	let key = trust
		.key(format, &root.node_id)
		.ok_or(E::UntrustedNode { node })?;
	let embedded = decode::<32>(&root.public_key).ok_or(E::MalformedKey)?;
	if embedded != *key {
		return Err(E::KeyMismatch { node });
	}
	let signature =
		Signature::from_bytes(&decode::<64>(&root.signature).ok_or(E::MalformedSignature)?);
	let head = AuditChainHead {
		sequence_number: root.sequence_number,
		record_hash: root.chain_head_hash.clone(),
		hash_algorithm_tag: root.hash_algorithm_tag.clone(),
	};
	let payload = signed_root_payload_bytes(format, &AuditNodeId::new(&root.node_id, node), &head);
	// Enrollment validated these exact trusted bytes. Strict verification rejects weak R/S forms.
	VerifyingKey::from_bytes(key)
		.map_err(|_| E::MalformedKey)?
		.verify_strict(&payload, &signature)
		.map_err(|_| E::Signature)
}
