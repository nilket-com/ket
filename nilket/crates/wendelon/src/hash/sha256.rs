use aws_lc_rs::digest::{Context as Sha256Context, SHA256, digest as sha256_digest};

/// SHA-256 helpers used where the code wants the concrete primitive rather than
/// a boundary/profile label.
pub fn digest(bytes: &[u8]) -> [u8; 32] {
	let digest = sha256_digest(&SHA256, bytes);
	let mut out = [0u8; 32];
	out.copy_from_slice(digest.as_ref());
	out
}

pub fn digest_many(parts: &[&[u8]]) -> [u8; 32] {
	let total_len: usize = parts.iter().map(|part| part.len()).sum();
	let mut buf = Vec::with_capacity(total_len);
	for part in parts {
		buf.extend_from_slice(part);
	}
	digest(&buf)
}

pub fn prefix_u32(bytes: &[u8]) -> u32 {
	let digest = digest(bytes);
	u32::from_le_bytes(
		digest[..4]
			.try_into()
			.expect("SHA-256 digest prefix is 4 bytes"),
	)
}

pub fn prefix_u64(bytes: &[u8]) -> u64 {
	let digest = digest(bytes);
	u64::from_le_bytes(
		digest[..8]
			.try_into()
			.expect("SHA-256 digest prefix is 8 bytes"),
	)
}

pub struct Context {
	inner: Sha256Context,
}

impl Context {
	pub fn new() -> Self {
		Self {
			inner: Sha256Context::new(&SHA256),
		}
	}

	pub fn update(&mut self, bytes: &[u8]) {
		self.inner.update(bytes);
	}

	pub fn finish(self) -> [u8; 32] {
		let digest = self.inner.finish();
		let mut out = [0u8; 32];
		out.copy_from_slice(digest.as_ref());
		out
	}
}
