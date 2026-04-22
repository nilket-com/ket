/// BLAKE3 helpers that are intentionally kept outside the validated boundary.
pub fn digest(bytes: &[u8]) -> [u8; 32] {
	*::blake3::hash(bytes).as_bytes()
}

pub fn prefix_u32(bytes: &[u8]) -> u32 {
	let digest = digest(bytes);
	u32::from_le_bytes(
		digest[..4]
			.try_into()
			.expect("BLAKE3 digest prefix is 4 bytes"),
	)
}

pub struct Context {
	inner: ::blake3::Hasher,
}

impl Context {
	pub fn new() -> Self {
		Self {
			inner: ::blake3::Hasher::new(),
		}
	}

	pub fn update(&mut self, bytes: &[u8]) {
		self.inner.update(bytes);
	}

	pub fn finish(self) -> [u8; 32] {
		*self.inner.finalize().as_bytes()
	}
}
