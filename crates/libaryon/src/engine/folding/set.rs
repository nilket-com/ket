use crate::core::DocByte;
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::FoldPolicy;
use crate::uast::kind::SemanticKind;

/// View-owned closed-fold state.
///
/// The representation is deliberately private. The projection walk performs a
/// membership lookup for almost every visited node; the §4.2 release benchmark
/// selected a sorted small collection over `HashSet` for the expected tens of
/// entries and remained faster through 128 entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FoldSet {
	nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FoldRecord {
	node: NodeId,
	start: DocByte,
	end: DocByte,
	kind: SemanticKind,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FoldSnapshot {
	records: Vec<FoldRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FoldEdit {
	pub(crate) start: DocByte,
	pub(crate) deleted_len: u64,
	pub(crate) inserted_len: u64,
}

impl FoldSet {
	#[inline]
	fn position(&self, node: NodeId) -> Result<usize, usize> {
		self.nodes.binary_search(&node)
	}

	#[inline]
	pub(crate) fn contains(&self, node: NodeId) -> bool {
		self.position(node).is_ok()
	}

	pub(crate) fn insert(&mut self, node: NodeId) -> bool {
		match self.position(node) {
			Ok(_) => false,
			Err(index) => {
				self.nodes.insert(index, node);
				true
			}
		}
	}

	pub(crate) fn remove(&mut self, node: NodeId) -> bool {
		let Ok(index) = self.position(node) else {
			return false;
		};
		self.nodes.remove(index);
		true
	}

	pub(crate) fn set(&mut self, node: NodeId, folded: bool) -> bool {
		if folded {
			self.insert(node)
		} else {
			self.remove(node)
		}
	}

	pub(crate) fn clear(&mut self) {
		self.nodes.clear();
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.nodes.is_empty()
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = NodeId> + '_ {
		self.nodes.iter().copied()
	}

	pub(crate) fn snapshot(&self, registry: &UastRegistry, root: NodeId) -> FoldSnapshot {
		let query = crate::engine::uast_query::UastQuery::new(registry, root);
		let records = self
			.iter()
			.filter(|&node| attached_to_root(registry, root, node))
			.filter(|&node| fold_kind(registry, root, node).is_some())
			.map(|node| {
				let (start, end) = query.node_bounds(node);
				FoldRecord {
					node,
					start,
					end,
					kind: unsafe { *registry.kinds[node.index()].get() },
				}
			})
			.collect();
		FoldSnapshot { records }
	}

	/// Restore a byte-identical topology replacement. A fold survives only when
	/// exactly one compatible interior node owns the same byte range.
	pub(crate) fn restore_exact(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		snapshot: FoldSnapshot,
	) {
		self.nodes = snapshot
			.records
			.into_iter()
			.filter_map(|record| find_exact_match(registry, root, record))
			.collect();
		self.nodes.sort_unstable();
		self.nodes.dedup();
	}

	/// Rebase folds through a text mutation. Only disjoint edits have a
	/// trustworthy range mapping; a fold touched in its interior is dropped.
	pub(crate) fn restore_after_edits(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		snapshot: FoldSnapshot,
		edits: &[FoldEdit],
	) {
		let records = snapshot
			.records
			.into_iter()
			.filter_map(|record| transform_record(record, edits))
			.collect();
		self.restore_exact(registry, root, FoldSnapshot { records });
	}
}

impl FoldPolicy for FoldSet {
	#[inline(always)]
	fn is_folded(&self, node: NodeId) -> bool {
		self.contains(node)
	}

	#[inline(always)]
	fn has_folds(&self) -> bool {
		!self.is_empty()
	}
}

fn fold_kind(registry: &UastRegistry, root: NodeId, node: NodeId) -> Option<SemanticKind> {
	if node == root {
		return None;
	}
	let idx = node.index();
	if unsafe { (*registry.edges[idx].get()).first_child.is_none() } {
		return None;
	}
	let kind = unsafe { *registry.kinds[idx].get() };
	(!matches!(kind, SemanticKind::Group | SemanticKind::Token)).then_some(kind)
}

fn attached_to_root(registry: &UastRegistry, root: NodeId, node: NodeId) -> bool {
	if !registry.is_current_node(root) || !registry.is_current_node(node) {
		return false;
	}
	let mut current = Some(node);
	while let Some(candidate) = current {
		if !registry.is_current_node(candidate) {
			return false;
		}
		if candidate == root {
			return true;
		}
		current = unsafe { (*registry.edges[candidate.index()].get()).parent };
	}
	false
}

fn find_exact_match(registry: &UastRegistry, root: NodeId, record: FoldRecord) -> Option<NodeId> {
	if attached_to_root(registry, root, record.node)
		&& fold_kind(registry, root, record.node) == Some(record.kind)
	{
		let bounds =
			crate::engine::uast_query::UastQuery::new(registry, root).node_bounds(record.node);
		if bounds == (record.start, record.end) {
			return Some(record.node);
		}
	}

	let total = registry.get_total_bytes(root);
	if record.start >= record.end || record.start.get() >= total {
		return None;
	}
	let mut candidate = Some(
		registry
			.find_node_at_doc_byte_raw(root, record.start)
			.node_id,
	);
	let query = crate::engine::uast_query::UastQuery::new(registry, root);
	let mut matched = None;
	while let Some(node) = candidate {
		if node == root {
			break;
		}
		if fold_kind(registry, root, node) == Some(record.kind)
			&& query.node_bounds(node) == (record.start, record.end)
		{
			if matched.is_some() {
				return None;
			}
			matched = Some(node);
		}
		candidate = unsafe { (*registry.edges[node.index()].get()).parent };
	}
	matched
}

fn transform_record(mut record: FoldRecord, edits: &[FoldEdit]) -> Option<FoldRecord> {
	let mut shift = 0i128;
	for edit in edits {
		let edit_start = edit.start.get();
		let edit_end = edit_start.saturating_add(edit.deleted_len);
		if edit_end <= record.start.get() {
			shift += edit.inserted_len as i128 - edit.deleted_len as i128;
		} else if edit_start < record.end.get() {
			return None;
		}
	}
	let start = (record.start.get() as i128).checked_add(shift)?;
	let end = (record.end.get() as i128).checked_add(shift)?;
	if start < 0 || end <= start {
		return None;
	}
	record.start = DocByte::new(start as u64);
	record.end = DocByte::new(end as u64);
	Some(record)
}

#[cfg(test)]
mod tests {
	use super::FoldSet;
	use crate::core::{DocByte, DocLine, StateId};
	use crate::ecs::NodeId;
	use crate::engine::document::edit::apply_deltas_to_document;
	use crate::engine::language::GreenNode;
	use crate::engine::structural_graft::apply_graft;
	use crate::engine::test_support::build_foldable_document;
	use crate::engine::undo::{TextDelta, UndoLedger};
	use crate::uast::UastProjection;
	use crate::uast::kind::SemanticKind;

	fn foldable_green(ambiguous: bool) -> GreenNode {
		let folded_leaf = GreenNode {
			kind: SemanticKind::Token,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: 8,
			children: Vec::new(),
		};
		let folded = GreenNode {
			kind: SemanticKind::RelationalTable,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: 8,
			children: vec![folded_leaf],
		};
		GreenNode {
			kind: SemanticKind::RelationalTable,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: 17,
			children: vec![
				GreenNode {
					kind: SemanticKind::Token,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: 5,
					children: Vec::new(),
				},
				if ambiguous {
					GreenNode {
						kind: SemanticKind::RelationalTable,
						provider_kind: crate::uast::ProviderKindId::NONE,
						byte_len: 8,
						children: vec![folded],
					}
				} else {
					folded
				},
				GreenNode {
					kind: SemanticKind::Token,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: 4,
					children: Vec::new(),
				},
			],
		}
	}

	fn insert_delta(at: u64, text: &str) -> TextDelta {
		TextDelta {
			global_byte_offset: DocByte::new(at),
			deleted_bytes: Vec::new(),
			inserted_bytes: text.as_bytes().to_vec(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		}
	}

	#[test]
	fn fold_set_keeps_nodes_sorted_and_unique() {
		let mut folds = FoldSet::default();
		assert!(folds.insert(NodeId::from_index(9)));
		assert!(folds.insert(NodeId::from_index(2)));
		assert!(!folds.insert(NodeId::from_index(9)));
		assert_eq!(
			folds.iter().map(NodeId::index).collect::<Vec<_>>(),
			vec![2, 9]
		);
		assert!(folds.remove(NodeId::from_index(2)));
		assert!(!folds.remove(NodeId::from_index(2)));
	}

	#[test]
	fn two_views_project_the_same_tree_with_independent_folds() {
		let (registry, root, folded) = build_foldable_document();
		let mut left = FoldSet::default();
		let right = FoldSet::default();
		left.insert(folded);

		let left_tokens = registry.query_viewport(root, &left, DocLine::ZERO, 4);
		let right_tokens = registry.query_viewport(root, &right, DocLine::ZERO, 4);

		assert!(left_tokens.iter().any(|token| token.is_folded));
		assert!(right_tokens.iter().all(|token| !token.is_folded));
		assert!(right_tokens.len() > left_tokens.len());
	}

	#[test]
	fn byte_identical_graft_restores_fold_to_unique_exact_node() {
		let (registry, root, old_fold) = build_foldable_document();
		let mut folds = FoldSet::default();
		folds.insert(old_fold);
		let snapshot = folds.snapshot(&registry, root);

		apply_graft(
			&registry,
			root,
			&foldable_green(false),
			b"head\none\ntwo\ntail",
		)
		.expect("replacement topology covers identical bytes");
		folds.restore_exact(&registry, root, snapshot);

		let restored = folds.iter().collect::<Vec<_>>();
		assert_eq!(restored.len(), 1);
		assert_ne!(restored[0], old_fold);
		assert!(
			registry
				.query_viewport(root, &folds, DocLine::ZERO, 4)
				.iter()
				.any(|token| token.is_folded && token.node_id == restored[0])
		);
	}

	#[test]
	fn byte_identical_graft_drops_ambiguous_exact_fold_match() {
		let (registry, root, old_fold) = build_foldable_document();
		let mut folds = FoldSet::default();
		folds.insert(old_fold);
		let snapshot = folds.snapshot(&registry, root);

		apply_graft(
			&registry,
			root,
			&foldable_green(true),
			b"head\none\ntwo\ntail",
		)
		.expect("nested exact ranges still cover identical bytes");
		folds.restore_exact(&registry, root, snapshot);

		assert!(folds.is_empty());
	}

	#[test]
	fn disjoint_edit_transforms_fold_range_and_preserves_it() {
		let (registry, root, folded) = build_foldable_document();
		let mut root_id = Some(root);
		let mut folds = FoldSet::default();
		folds.insert(folded);

		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut UndoLedger::new(),
			&mut Vec::new(),
			&mut Vec::new(),
			vec![insert_delta(0, "x")],
		)
		.expect("disjoint insertion applies");

		assert!(folds.contains(folded));
		let tokens = registry.query_viewport(root, &folds, DocLine::ZERO, 4);
		let marker = tokens
			.iter()
			.find(|token| token.is_folded)
			.expect("shifted fold stays closed");
		assert_eq!(marker.absolute_start_byte, DocByte::new(6));
	}

	#[test]
	fn overlapping_edit_drops_fold_instead_of_reattaching_it() {
		let (registry, root, folded) = build_foldable_document();
		let mut root_id = Some(root);
		let mut folds = FoldSet::default();
		folds.insert(folded);

		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut UndoLedger::new(),
			&mut Vec::new(),
			&mut Vec::new(),
			vec![insert_delta(9, "x")],
		)
		.expect("overlapping insertion applies");

		assert!(folds.is_empty());
	}
}
