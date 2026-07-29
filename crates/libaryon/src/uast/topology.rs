use crate::ecs::id::NodeId;

/// Target fan-out for bounded tree levels. SumTree nodes and topology-only
/// graft groups share this value because both trade a short linear sibling
/// scan against another tree level.
pub(crate) const TARGET_TREE_FANOUT: usize = 64;

/// The Edges of the DAG/Tree.
/// Instead of `Vec<NodeId>` which causes heap fragmentation, we use a
/// Left-Child / Right-Sibling (LCRS) tree representation.
/// This guarantees every node's topological footprint is exactly 12 bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeEdges {
	pub parent: Option<NodeId>,
	pub first_child: Option<NodeId>,
	pub next_sibling: Option<NodeId>,
}

#[cfg(test)]
mod tests {
	#[test]
	fn test_tree_edges_size() {
		assert_eq!(std::mem::size_of::<super::TreeEdges>(), 12);
	}

	#[test]
	fn test_option_node_id_size() {
		assert_eq!(std::mem::size_of::<Option<crate::ecs::NodeId>>(), 4);
	}
}
