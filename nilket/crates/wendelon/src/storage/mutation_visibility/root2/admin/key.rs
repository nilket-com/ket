use super::errors::{AdminManifestError2, AdminResult2};
use ed25519_dalek::VerifyingKey;

// Enrollment is stricter than the ZIP-215 parsing used for signature math: exactly one
// encoding per key, and only prime-order-subgroup points.
pub(in crate::storage::mutation_visibility::root2) fn check_enrollable_admin_key2(
	key: &[u8; 32],
) -> AdminResult2<()> {
	let parsed = VerifyingKey::from_bytes(key).map_err(|_| AdminManifestError2::NonCanonicalKey)?;
	let point = parsed.to_edwards();
	if point.compress().to_bytes() != *key || parsed.is_weak() || !point.is_torsion_free() {
		return Err(AdminManifestError2::NonCanonicalKey);
	}
	Ok(())
}
