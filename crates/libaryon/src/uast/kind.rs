/// ==========================================
/// SEMANTIC COMPONENTS
/// ==========================================
/// The logical meaning of the node. Unifies Relational (CSV) and Logical (Rust/SQL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum SemanticKind {
	RelationalTable,
	RelationalRow,
	/// Topology-only fan-out grouping. It carries subtree metrics but is not a
	/// semantic scope, text object, navigation target, or fold boundary.
	///
	/// Grouping is currently established only by complete-source grafts; sparse
	/// ingestion keeps its flat indexed extents and partial-knowledge model.
	/// Edits may let fan-out drift until the next graft rather than rebalancing
	/// the mutable tree online.
	Group,
	Token,
}

/// Language-provider-local syntax identity carried by a live UAST node.
///
/// Zero deliberately means "no provider identity": physical extents,
/// topology-only groups, and nodes born from generic text mutation all use it.
/// Non-zero values are interpreted by the document's active language provider;
/// there is no cross-language numeric namespace.
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
