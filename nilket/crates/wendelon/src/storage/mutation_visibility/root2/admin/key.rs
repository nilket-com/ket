use super::errors::{AdminManifestError2, AdminResult2};
use curve25519_dalek::edwards::CompressedEdwardsY;

// Enrollment is stricter than the ZIP-215 parsing used for signature math: exactly one
// encoding per key, and only prime-order-subgroup points.
pub(in crate::storage::mutation_visibility::root2) fn check_enrollable_admin_key2(
	key: &[u8; 32],
) -> AdminResult2<()> {
	let point = CompressedEdwardsY(*key)
		.decompress()
		.ok_or(AdminManifestError2::NonCanonicalKey)?;
	if point.compress().to_bytes() != *key || point.is_small_order() || !point.is_torsion_free() {
		return Err(AdminManifestError2::NonCanonicalKey);
	}
	Ok(())
}
