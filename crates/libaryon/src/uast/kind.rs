/// ==========================================
/// SEMANTIC COMPONENTS
/// ==========================================
/// The logical meaning of the node. Unifies Relational (CSV) and Logical (Rust/SQL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
