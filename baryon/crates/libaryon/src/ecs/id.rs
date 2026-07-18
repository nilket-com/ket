use std::num::NonZeroU32;

/// ==========================================
/// CORE ENTITY
/// ==========================================
/// The Entity ID. A lightweight 32-bit integer.
/// We use NonZeroU32 to allow `Option<NodeId>` to fit cleanly into 4 bytes (Null-pointer optimization).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub NonZeroU32);

impl NodeId {
	#[inline(always)]
	pub fn index(self) -> usize {
		self.0.get() as usize - 1
	}

	pub fn from_index(idx: usize) -> Self {
		Self(NonZeroU32::new(idx as u32 + 1).unwrap())
	}
}
