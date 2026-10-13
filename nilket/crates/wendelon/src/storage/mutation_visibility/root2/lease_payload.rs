//! Dormant exact runtime lease prefix on little-endian hosts; no production caller.
//! A signature proves authorization of 80 bytes, not issuance, commitment or leadership.
//! Runtime signatures have no domain/root/cluster binding: keys must be unique per deployment
//! and sign only LeaseDescriptor payloads. Runtime non-strict acceptance can disagree with this
//! strict check (availability/compatibility risk); the runtime protocol remains unchanged.
//! Future runtime projection must reject nonzero holder/issuer reserved words before dropping
//! them; reconstruction zero-fills them and does not prove that V/A projection boundary.
use crate::storage::mutation_visibility::root2::types::SignedLease2;
use ed25519_dalek::{Signature, VerifyingKey};
pub(in crate::storage::mutation_visibility::root2) const LEASE_PAYLOAD_BYTES2: usize = 80;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum LeasePayloadError2 {
	UnsupportedEndian,
	Key,
	Signature,
}
/// Encodes supplied fields; does not validate lease semantics, enrollment or authority.
pub(in crate::storage::mutation_visibility::root2) fn lease_payload2(
	lease: &SignedLease2,
) -> Result<[u8; LEASE_PAYLOAD_BYTES2], LeasePayloadError2> {
	if cfg!(target_endian = "big") {
		return Err(LeasePayloadError2::UnsupportedEndian);
	}
	let mut bytes = [0u8; LEASE_PAYLOAD_BYTES2];
	bytes[0..8].copy_from_slice(&lease.epoch.to_le_bytes());
	bytes[8..12].copy_from_slice(&lease.holder.to_le_bytes());
	bytes[16..24].copy_from_slice(&lease.range_start.to_le_bytes());
	bytes[24..32].copy_from_slice(&lease.range_end.to_le_bytes());
	bytes[32..40].copy_from_slice(&lease.start.physical_ms.to_le_bytes());
	bytes[40..44].copy_from_slice(&lease.start.logical.to_le_bytes());
	bytes[44..48].copy_from_slice(&lease.start.node.to_le_bytes());
	bytes[48..56].copy_from_slice(&lease.expiry.physical_ms.to_le_bytes());
	bytes[56..60].copy_from_slice(&lease.expiry.logical.to_le_bytes());
	bytes[60..64].copy_from_slice(&lease.expiry.node.to_le_bytes());
	bytes[64..72].copy_from_slice(&lease.min_commit_index.to_le_bytes());
	bytes[72..76].copy_from_slice(&lease.issuer.to_le_bytes());
	Ok(bytes)
}
/// Key enrollment canonicality/subgroup policy belongs to the future issuer registry.
pub(in crate::storage::mutation_visibility::root2) fn verify_issuer_signature2(
	key: &[u8; 32],
	lease: &SignedLease2,
) -> Result<(), LeasePayloadError2> {
	let payload = lease_payload2(lease)?;
	VerifyingKey::from_bytes(key)
		.map_err(|_| LeasePayloadError2::Key)?
		.verify_strict(&payload, &Signature::from_bytes(&lease.signature))
		.map_err(|_| LeasePayloadError2::Signature)
}
