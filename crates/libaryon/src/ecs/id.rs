use std::num::NonZeroU32;

pub(crate) const NODE_SLOT_BITS: u32 = 24;
pub(crate) const NODE_SLOT_MASK: u32 = (1 << NODE_SLOT_BITS) - 1;
pub(crate) const NODE_MAX_GENERATION: u8 = u8::MAX;

/// ==========================================
/// CORE ENTITY
/// ==========================================
/// A generation-qualified registry entity identity.
///
/// The low 24 bits encode `slot + 1`; the high 8 bits encode a generation
/// which is never repeated for that slot. `NonZeroU32` preserves the
/// `Option<NodeId>` niche.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub NonZeroU32);

impl NodeId {
	#[inline(always)]
	pub fn index(self) -> usize {
		((self.0.get() & NODE_SLOT_MASK) - 1) as usize
	}

	#[inline(always)]
	pub(crate) fn generation(self) -> u8 {
		(self.0.get() >> NODE_SLOT_BITS) as u8
	}

	#[inline(always)]
	pub(crate) fn raw(self) -> u32 {
		self.0.get()
	}

	pub(crate) fn from_parts(index: usize, generation: u8) -> Option<Self> {
		let slot_code = u32::try_from(index).ok()?.checked_add(1)?;
		if slot_code > NODE_SLOT_MASK {
			return None;
		}
		let raw = (u32::from(generation) << NODE_SLOT_BITS) | slot_code;
		NonZeroU32::new(raw).map(Self)
	}

	pub(crate) fn from_raw(raw: u32) -> Option<Self> {
		let value = NonZeroU32::new(raw)?;
		((raw & NODE_SLOT_MASK) != 0).then_some(Self(value))
	}

	/// Constructs a generation-zero identity for fixtures and fresh bump-tail
	/// allocation. Production reconstruction from a numeric slot is forbidden.
	#[cfg(test)]
	pub fn from_index(idx: usize) -> Self {
		Self::from_parts(idx, 0).expect("test NodeId index fits the 24-bit slot field")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn packed_identity_preserves_slot_and_generation() {
		let id = NodeId::from_parts(15_999_999, 237).expect("configured capacity fits");
		assert_eq!(id.index(), 15_999_999);
		assert_eq!(id.generation(), 237);
		assert_eq!(NodeId::from_raw(id.raw()), Some(id));
		assert_eq!(std::mem::size_of::<Option<NodeId>>(), 4);
	}
}
