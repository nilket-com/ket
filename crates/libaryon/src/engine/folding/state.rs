use crate::core::DocLine;
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::UastProjection;
use crate::uast::kind::SemanticKind;

use super::FoldSet;

fn node_is_foldable_boundary(registry: &UastRegistry, root: NodeId, node: NodeId) -> bool {
	if node == root {
		return false;
	}

	let idx = node.index();
	let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };
	has_children
		&& !matches!(
			unsafe { *registry.kinds[idx].get() },
			SemanticKind::Group | SemanticKind::Token
		)
}

pub(crate) fn nearest_foldable_boundary(
	registry: &UastRegistry,
	root: NodeId,
	start: NodeId,
) -> Option<NodeId> {
	let mut curr = Some(start);
	while let Some(node) = curr {
		if node_is_foldable_boundary(registry, root, node) {
			return Some(node);
		}
		curr = unsafe { (*registry.edges[node.index()].get()).parent };
	}
	None
}

pub(crate) fn set_subtree_fold_state(
	registry: &UastRegistry,
	root: NodeId,
	folds: &mut FoldSet,
	folded: bool,
) {
	let mut visit = Some(root);
	while let Some(node) = visit {
		if node_is_foldable_boundary(registry, root, node) {
			folds.set(node, folded);
		}
		visit = registry.get_next_node_in_walk(node);
	}
}

pub(crate) fn unfold_ancestor_chain(registry: &UastRegistry, folds: &mut FoldSet, node: NodeId) {
	let mut curr = unsafe { (*registry.edges[node.index()].get()).parent };
	while let Some(parent) = curr {
		folds.remove(parent);
		curr = unsafe { (*registry.edges[parent.index()].get()).parent };
	}
}

pub(super) fn visible_fold_boundary_on_line(
	registry: &UastRegistry,
	root: NodeId,
	folds: &FoldSet,
	line: DocLine,
) -> Option<NodeId> {
	registry
		.query_viewport(root, folds, line, 1)
		.into_iter()
		.find(|token| token.is_folded && token.absolute_start_line == line)
		.map(|token| token.node_id)
}

#[cfg(test)]
mod tests {
	use super::{FoldSet, nearest_foldable_boundary, set_subtree_fold_state};
	use crate::core::{DocDisplayCol, DocLine};
	use crate::engine::test_support::{
		build_document, build_grouped_semantic_document, build_root_with_rows,
	};
	use crate::uast::UastProjection;

	#[test]
	fn nearest_foldable_boundary_skips_root_node() {
		let (registry, root) = build_document("alpha\nbeta\n");
		let cursor = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::ZERO,
			DocDisplayCol::ZERO,
		);

		assert_eq!(
			nearest_foldable_boundary(&registry, root, cursor.node_id),
			None
		);
	}

	#[test]
	fn close_all_folds_keeps_root_visible() {
		let (registry, root, row_a, row_b) = build_root_with_rows();
		let mut folds = FoldSet::default();

		set_subtree_fold_state(&registry, root, &mut folds, true);

		assert!(!folds.contains(root));
		assert!(folds.contains(row_a));
		assert!(folds.contains(row_b));
	}

	#[test]
	fn topology_group_is_not_a_fold_boundary() {
		let (registry, root, grouped_leaf, _) = build_grouped_semantic_document();
		let group = unsafe { (*registry.edges[grouped_leaf.index()].get()).parent }
			.expect("grouped leaf parent");
		let mut folds = FoldSet::default();

		assert_eq!(
			nearest_foldable_boundary(&registry, root, grouped_leaf),
			None
		);

		set_subtree_fold_state(&registry, root, &mut folds, true);
		assert!(
			!folds.contains(group),
			"topology-only groups must not become user-visible folds"
		);
	}
}
