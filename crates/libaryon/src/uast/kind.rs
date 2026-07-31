/// ==========================================
/// SEMANTIC COMPONENTS
/// ==========================================
/// The logical meaning of the node. Unifies Relational (CSV) and Logical (Rust/SQL).
#[repr(u8)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum SemanticKind {
	/// The identity element of semantic interpretation. The node may still
	/// group children structurally, but contributes no semantic meaning:
	/// interpreting it is equivalent to interpreting its children directly.
	///
	/// Complete-source grafts use this for topology-only fan-out buckets.
	/// Untouched and fully reclaimed registry slots also rest at this honest
	/// zero-state until an explicit publication assigns their semantic kind.
	#[default]
	None = 0,
	RelationalTable = 1,
	RelationalRow = 2,
	Token = 3,
}

/// Language-provider-local syntax identity carried by a live UAST node.
///
/// Zero deliberately means "no provider identity": physical extents,
/// topology-only fan-out buckets, and nodes born from generic text mutation
/// all use it. Non-zero values are interpreted by the document's active
/// language provider; there is no cross-language numeric namespace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ProviderKindId(u16);

impl ProviderKindId {
	pub const NONE: Self = Self(0);

	pub const fn new(value: u16) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u16 {
		self.0
	}
}

#[cfg(test)]
mod tests {
	use super::SemanticKind;

	#[test]
	fn semantic_kind_zero_state_and_discriminants_are_stable() {
		assert_eq!(SemanticKind::None as u8, 0);
		assert_eq!(SemanticKind::RelationalTable as u8, 1);
		assert_eq!(SemanticKind::RelationalRow as u8, 2);
		assert_eq!(SemanticKind::Token as u8, 3);
		assert_eq!(SemanticKind::default(), SemanticKind::None);
		assert_eq!(std::mem::size_of::<SemanticKind>(), 1);
	}
}
