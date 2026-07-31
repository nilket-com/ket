/// BLAKE3 helpers that are explicitly outside the FIPS module boundary because
/// they serve content-addressing style payload identity, not CMVP-bound crypto.
pub fn digest(bytes: &[u8]) -> [u8; 32] {
	*blake3::hash(bytes).as_bytes()
}

pub fn prefix_u32(bytes: &[u8]) -> u32 {
	let digest = digest(bytes);
	let prefix: [u8; 4] = digest[..4].try_into().expect("BLAKE3 digest is 32 bytes");
	u32::from_le_bytes(prefix)
}
