const TENANT_DOMAIN: &[u8] = b"wendelon.identity.tenant.v1";

/// BLAKE3 helpers that are explicitly outside the FIPS module boundary because
/// record Identity is format-stable across build profiles.
pub fn digest(bytes: &[u8]) -> [u8; 32] {
	*blake3::hash(bytes).as_bytes()
}

pub fn tenant_scoped(tenant_id: u64, bytes: &[u8]) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(TENANT_DOMAIN);
	hasher.update(&tenant_id.to_le_bytes());
	hasher.update(bytes);
	*hasher.finalize().as_bytes()
}

pub fn keyed(key: &[u8; 32], bytes: &[u8]) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new_keyed(key);
	hasher.update(bytes);
	*hasher.finalize().as_bytes()
}
