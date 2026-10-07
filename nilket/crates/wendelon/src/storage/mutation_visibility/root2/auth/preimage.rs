use super::super::{ATTESTATION_BYTES2, CLAIM_BYTES2, ClaimProjection2, HolderAttestation2};
use super::{AuthError2, AuthResult2};

// Preflight every caller-owned collection before validation, cloning or serialization.
// Existing canonical validators may allocate bounded sets; their input is already capped here.
fn claim_size(claim: &ClaimProjection2) -> AuthResult2<usize> {
	let manifest = &claim.manifest;
	if manifest.children.len() > 256 || claim.stamps.len() > 512 || claim.plans.len() > 256 {
		return Err(AuthError2::Limit);
	}
	let mut versions = 0usize;
	for child in &manifest.children {
		versions = versions
			.checked_add(child.versions.len())
			.ok_or(AuthError2::Overflow)?;
		if versions > 512 {
			return Err(AuthError2::Limit);
		}
	}
	let manifest_size = checked_size(112, manifest.children.len(), 31, 65536)?;
	let manifest_size = checked_size(manifest_size, versions, 69, 65536)?;
	let mut size = checked_size(460, 1, manifest_size, CLAIM_BYTES2)?;
	size = checked_size(size, claim.stamps.len(), 24, CLAIM_BYTES2)?;
	for plan in &claim.plans {
		if plan.nodes.len() > 3 {
			return Err(AuthError2::Limit);
		}
		size = checked_size(size, 1, 10, CLAIM_BYTES2)?;
		size = checked_size(size, plan.nodes.len(), 20, CLAIM_BYTES2)?;
	}
	Ok(size)
}
fn holder_size(holder: &HolderAttestation2) -> AuthResult2<usize> {
	if holder.children.len() > 256 {
		return Err(AuthError2::Limit);
	}
	let mut size = 212;
	for child in &holder.children {
		if child.nodes.len() > 3 {
			return Err(AuthError2::Limit);
		}
		size = checked_size(size, 1, 37, ATTESTATION_BYTES2)?;
		size = checked_size(size, child.nodes.len(), 20, ATTESTATION_BYTES2)?;
	}
	Ok(size)
}
pub(super) fn checked_size(
	base: usize,
	count: usize,
	width: usize,
	cap: usize,
) -> AuthResult2<usize> {
	let size = count
		.checked_mul(width)
		.and_then(|n| base.checked_add(n))
		.ok_or(AuthError2::Overflow)?;
	if size > cap {
		Err(AuthError2::Limit)
	} else {
		Ok(size)
	}
}
fn claim_body(claim: &ClaimProjection2, cluster: [u8; 16]) -> AuthResult2<Vec<u8>> {
	let expected = claim_size(claim)?;
	if cluster == [0; 16] || claim.manifest.identity.generation == [0; 16] {
		return Err(AuthError2::ScopeMismatch);
	}
	let body = claim.encode()?;
	if body.len() != expected {
		return Err(AuthError2::Structural);
	}
	Ok(body)
}
pub(super) fn length(size: usize) -> AuthResult2<[u8; 4]> {
	Ok(u32::try_from(size)
		.map_err(|_| AuthError2::Overflow)?
		.to_le_bytes())
}
fn preimage(
	domain: &[u8],
	claim: &ClaimProjection2,
	cluster: [u8; 16],
	extra: &[u8],
	body: &[u8],
	prefix: usize,
) -> AuthResult2<Vec<u8>> {
	let size = checked_size(prefix, 1, body.len(), CLAIM_BYTES2 + 129)?;
	let body_length = length(body.len())?;
	let mut out = Vec::with_capacity(size);
	out.extend_from_slice(domain);
	out.extend_from_slice(&claim.manifest.identity.generation);
	out.extend_from_slice(&cluster);
	out.extend_from_slice(extra);
	out.extend_from_slice(&body_length);
	out.extend_from_slice(body);
	if out.len() != size {
		return Err(AuthError2::Structural);
	}
	Ok(out)
}
/// Ordinary canonical bytes only; this function does not compute a MAC or authorize a claim.
pub(in crate::storage::mutation_visibility::root2) fn claim_mac_preimage2(
	claim: &ClaimProjection2,
	cluster: [u8; 16],
) -> AuthResult2<Vec<u8>> {
	let mut body = claim_body(claim, cluster)?;
	let tail = body.len().checked_sub(96).ok_or(AuthError2::Structural)?;
	body[tail..].fill(0);
	let binding = claim
		.manifest
		.binding()
		.encode()
		.map_err(|_| AuthError2::Structural)?;
	preimage(b"VIS-CLAIM-COMMIT-2\0", claim, cluster, &binding, &body, 99)
}
pub(in crate::storage::mutation_visibility::root2) fn admission_preimage2(
	claim: &ClaimProjection2,
	cluster: [u8; 16],
) -> AuthResult2<Vec<u8>> {
	let mut body = claim_body(claim, cluster)?;
	let tail = body.len().checked_sub(64).ok_or(AuthError2::Structural)?;
	body[tail..].fill(0);
	preimage(b"VIS-CLAIM-ADMIT-2\0", claim, cluster, &[], &body, 54)
}
/// Cross-reference/shape checks are not holder enrollment, committed provenance or quorum verification.
pub(in crate::storage::mutation_visibility::root2) fn holder_preimage2(
	claim: &ClaimProjection2,
	cluster: [u8; 16],
	holder: &HolderAttestation2,
) -> AuthResult2<Vec<u8>> {
	let expected = holder_size(holder)?;
	let _ = claim_body(claim, cluster)?;
	let mut body = holder.encode()?;
	if body.len() != expected {
		return Err(AuthError2::Structural);
	}
	if holder.grant != claim.original.reference()?
		|| holder.claim_digest != claim.digest()?
		|| holder.authority != claim.authority
	{
		return Err(AuthError2::ScopeMismatch);
	}
	let tail = body.len().checked_sub(64).ok_or(AuthError2::Structural)?;
	body[tail..].fill(0);
	let identity = claim
		.manifest
		.identity
		.encode()
		.map_err(|_| AuthError2::Structural)?;
	preimage(
		b"VIS-HOLDER-QUORUM-2\0",
		claim,
		cluster,
		&identity,
		&body,
		129,
	)
}
