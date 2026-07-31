/// ==========================================
/// SEMANTIC COMPONENTS
/// ==========================================
/// The provider-independent semantic role of a node.
///
/// These variants classify meaning, not topology: rows and tokens may have
/// children, while `None` nodes may provide structural fan-out. “Relational”
/// describes the relationship between semantic levels; it does not promise
/// Codd set semantics. See `plans/adr/0003-semantic-vocabulary.md`.
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
	/// Establishes a semantic context containing addressable members or nested
	/// contexts.
	RelationalTable = 1,
	/// One addressable member of a semantic context. A row may itself contain
	/// nested structure, including tables.
	RelationalRow = 2,
	/// Residual syntax not promoted to a table or row.
	///
	/// `Token` is not guaranteed to be a topology leaf: providers may assign it
	/// to unpromoted interior nodes. Consumers must determine leafness from
	/// topology (`first_child`), never from this semantic kind.
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
