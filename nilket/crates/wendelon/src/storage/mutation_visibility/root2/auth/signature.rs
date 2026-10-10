use super::{AuthError2, AuthResult2, admission_preimage2, holder_preimage2};
use crate::storage::mutation_visibility::root2::{ClaimProjection2, HolderAttestation2};
use ed25519_dalek::{Signature, VerifyingKey};

// Fixed pure Ed25519, exact locked provider acceptance. No signing, algorithm selector,
// enrollment/purpose lookup or trusted output. ZIP-215 public parsing is not canonical enrollment.
fn verify(bytes: &[u8], public_key: &[u8; 32], signature: &[u8; 64]) -> AuthResult2<()> {
	VerifyingKey::from_bytes(public_key)
		.map_err(|_| AuthError2::CryptoInvalid)?
		.verify_strict(bytes, &Signature::from_bytes(signature))
		.map_err(|_| AuthError2::CryptoInvalid)
}
pub(in crate::storage::mutation_visibility::root2) fn verify_admission_signature2(
	claim: &ClaimProjection2,
	cluster: [u8; 16],
	public_key: &[u8; 32],
) -> AuthResult2<()> {
	verify(
		&admission_preimage2(claim, cluster)?,
		public_key,
		&claim.signature,
	)
}
pub(in crate::storage::mutation_visibility::root2) fn verify_holder_signature2(
	claim: &ClaimProjection2,
	cluster: [u8; 16],
	holder: &HolderAttestation2,
	public_key: &[u8; 32],
) -> AuthResult2<()> {
	verify(
		&holder_preimage2(claim, cluster, holder)?,
		public_key,
		&holder.signature,
	)
}
