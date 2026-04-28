use crate::hash::state_digest::StateDigestDomain;

const ELEMENT_PROJECTION_DOMAIN: &[u8] = b"state-digest.element";
const LTHASH_WORDS: usize = 1024;
const LTHASH_BYTES: usize = LTHASH_WORDS * 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LtHashBaseline {
	state: [u16; LTHASH_WORDS],
}

impl LtHashBaseline {
	pub(crate) fn new() -> Self {
		Self {
			state: [0; LTHASH_WORDS],
		}
	}

	pub(crate) fn add(&mut self, domain: StateDigestDomain, preimage: &[u8]) {
		let mut bytes = [0u8; LTHASH_BYTES];
		project_element_xof(domain, preimage, &mut bytes);
		for (slot, word) in self.state.iter_mut().zip(bytes.chunks_exact(2)) {
			let value = u16::from_le_bytes([word[0], word[1]]);
			*slot = slot.wrapping_add(value);
		}
	}

	pub(crate) fn subtract(&mut self, domain: StateDigestDomain, preimage: &[u8]) {
		let mut bytes = [0u8; LTHASH_BYTES];
		project_element_xof(domain, preimage, &mut bytes);
		for (slot, word) in self.state.iter_mut().zip(bytes.chunks_exact(2)) {
			let value = u16::from_le_bytes([word[0], word[1]]);
			*slot = slot.wrapping_sub(value);
		}
	}

	pub(crate) fn as_words(&self) -> &[u16; LTHASH_WORDS] {
		&self.state
	}
}

fn project_element_xof(domain: StateDigestDomain, preimage: &[u8], out: &mut [u8]) {
	domain
		.validate()
		.expect("state digest domain constants must be well-formed");
	let mut hasher = blake3::Hasher::new();
	hasher.update(ELEMENT_PROJECTION_DOMAIN);
	hasher.update(&(domain.tag().len() as u32).to_be_bytes());
	hasher.update(domain.tag());
	hasher.update(&(preimage.len() as u64).to_be_bytes());
	hasher.update(preimage);
	hasher.finalize_xof().fill(out);
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::hash::state_digest::DOMAIN_TABLE_ROW_VISIBLE;

	#[test]
	fn baseline_add_subtract_round_trips() {
		let mut baseline = LtHashBaseline::new();
		baseline.add(DOMAIN_TABLE_ROW_VISIBLE, b"row");
		assert_ne!(baseline.as_words(), &[0; LTHASH_WORDS]);
		baseline.subtract(DOMAIN_TABLE_ROW_VISIBLE, b"row");
		assert_eq!(baseline.as_words(), &[0; LTHASH_WORDS]);
	}
}
