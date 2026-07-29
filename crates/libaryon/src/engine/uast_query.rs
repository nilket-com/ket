use crate::core::DocByte;
use crate::core::NodeByteOffset;
use crate::ecs::id::NodeId;
use crate::ecs::registry::UastRegistry;
use crate::uast::UastProjection;
use crate::uast::kind::SemanticKind;

/// Lightweight spatial query API over the UastRegistry's LCRS tree.
///
/// All queries are stateless and borrow the registry immutably.
/// The engine handles arbitrary tree depth — flat token trees and
/// deep structural trees (after graft) work identically.
pub struct UastQuery<'a> {
	registry: &'a UastRegistry,
	root: NodeId,
}

impl<'a> UastQuery<'a> {
	pub fn new(registry: &'a UastRegistry, root: NodeId) -> Self {
		Self { registry, root }
	}

	/// Returns the absolute (start, end_exclusive) byte offsets for a node.
	pub fn node_bounds(&self, node: NodeId) -> (DocByte, DocByte) {
		let start = self
			.registry
			.doc_byte_for_node_offset(self.root, node, NodeByteOffset::ZERO);
		let byte_len = unsafe { (*self.registry.metrics[node.index()].get()).byte_length };
		(start, start.saturating_add(byte_len))
	}

	/// Finds the deepest node containing `offset`.
	///
	/// Descends through the LCRS tree, always preferring the deepest
	/// child whose byte range contains the target offset.
	pub fn node_at_offset(&self, offset: DocByte) -> Option<NodeId> {
		let target = self.registry.find_node_at_doc_byte_raw(self.root, offset);
		Some(target.node_id)
	}

	/// Walks up the parent chain from `start_node` until it finds a node
	/// whose SemanticKind satisfies `predicate`. Returns the first match.
	pub fn find_ancestor(
		&self,
		start_node: NodeId,
		predicate: impl Fn(SemanticKind) -> bool,
	) -> Option<NodeId> {
		let mut current = start_node;
		loop {
			let parent = unsafe { (*self.registry.edges[current.index()].get()).parent }?;
			if parent == self.root {
				// Don't return the document root — it's always a RelationalTable
				// but never represents a function.
				return None;
			}
			let kind = unsafe { *self.registry.kinds[parent.index()].get() };
			if predicate(kind) {
				return Some(parent);
			}
			current = parent;
		}
	}

	/// Finds the next node whose kind satisfies `predicate` and whose
	/// start offset is strictly greater than `current_offset`.
	///
	/// Uses a depth-first walk from the root, skipping subtrees that
	/// end before `current_offset`.
	pub fn next_of_kind(
		&self,
		current_offset: DocByte,
		predicate: impl Fn(SemanticKind) -> bool,
	) -> Option<NodeId> {
		let mut visit = Some(self.root);
		let mut byte_acc = DocByte::ZERO;

		while let Some(node) = visit {
			let idx = node.index();
			let metrics = unsafe { &*self.registry.metrics[idx].get() };
			let node_start = byte_acc;
			let node_end = node_start.saturating_add(metrics.byte_length);
			let has_children = unsafe { (*self.registry.edges[idx].get()).first_child.is_some() };

			if node_end <= current_offset {
				// Entire subtree is before the target — skip
				byte_acc = node_end;
				visit = unsafe { (*self.registry.edges[idx].get()).next_sibling };
				continue;
			}

			// Node's range overlaps or is past current_offset
			if node_start > current_offset && node != self.root {
				let kind = unsafe { *self.registry.kinds[idx].get() };
				if predicate(kind) {
					return Some(node);
				}
			}

			if has_children {
				visit = unsafe { (*self.registry.edges[idx].get()).first_child };
			} else {
				// Advance to next in DFT order
				visit = self.next_in_walk(node, &mut byte_acc);
			}
			continue;
		}

		None
	}

	/// DFT advance: next_sibling, or walk up to parent's next_sibling.
	/// Updates `byte_acc` to reflect the accumulated offset.
	fn next_in_walk(&self, node: NodeId, byte_acc: &mut DocByte) -> Option<NodeId> {
		let idx = node.index();
		let metrics = unsafe { &*self.registry.metrics[idx].get() };
		*byte_acc = byte_acc.saturating_add(metrics.byte_length);

		let next_sib = unsafe { (*self.registry.edges[idx].get()).next_sibling };
		if next_sib.is_some() {
			return next_sib;
		}

		// Walk up parent chain
		let mut curr = node;
		loop {
			let parent = unsafe { (*self.registry.edges[curr.index()].get()).parent }?;
			let uncle = unsafe { (*self.registry.edges[parent.index()].get()).next_sibling };
			if uncle.is_some() {
				return uncle;
			}
			curr = parent;
		}
	}
}

/// Predicate: matches nodes that represent function/method scope boundaries.
///
/// In the relational isomorphism, `RelationalTable` is used for scopes/blocks
/// (functions, classes, modules, impl blocks). This is the predicate for
/// `vaf`, `daf`, and `]m`.
pub fn is_function_scope(kind: SemanticKind) -> bool {
	matches!(kind, SemanticKind::RelationalTable)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ecs::registry::UastRegistry;
	use crate::engine::test_support::build_grouped_semantic_document;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;

	/// Build a tree:
	/// ```
	///  Root (RelationalTable, 100 bytes)
	///  ├─ Token (10 bytes)         [0..10]
	///  ├─ RelationalTable (50 bytes) [10..60]  ← "function"
	///  │  ├─ Token (20 bytes)      [10..30]
	///  │  └─ RelationalRow (30 bytes) [30..60]
	///  │     ├─ Token (15 bytes)   [30..45]
	///  │     └─ Token (15 bytes)   [45..60]
	///  └─ Token (40 bytes)         [60..100]
	/// ```
	fn build_deep_tree() -> (UastRegistry, NodeId) {
		let registry = UastRegistry::new(64);

		let root = registry.alloc_test_node();
		let t1 = registry.alloc_test_node();
		let func = registry.alloc_test_node();
		let func_header = registry.alloc_test_node();
		let row = registry.alloc_test_node();
		let row_a = registry.alloc_test_node();
		let row_b = registry.alloc_test_node();
		let t2 = registry.alloc_test_node();

		unsafe {
			// Root
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: 100,
				newlines: 0,
			};
			(*registry.edges[root.index()].get()).first_child = Some(t1);

			// t1: Token [0..10]
			*registry.kinds[t1.index()].get() = SemanticKind::Token;
			*registry.metrics[t1.index()].get() = SpanMetrics {
				byte_length: 10,
				newlines: 0,
			};
			(*registry.edges[t1.index()].get()).parent = Some(root);
			(*registry.edges[t1.index()].get()).next_sibling = Some(func);

			// func: RelationalTable [10..60]
			*registry.kinds[func.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[func.index()].get() = SpanMetrics {
				byte_length: 50,
				newlines: 0,
			};
			(*registry.edges[func.index()].get()).parent = Some(root);
			(*registry.edges[func.index()].get()).first_child = Some(func_header);
			(*registry.edges[func.index()].get()).next_sibling = Some(t2);

			// func_header: Token [10..30]
			*registry.kinds[func_header.index()].get() = SemanticKind::Token;
			*registry.metrics[func_header.index()].get() = SpanMetrics {
				byte_length: 20,
				newlines: 0,
			};
			(*registry.edges[func_header.index()].get()).parent = Some(func);
			(*registry.edges[func_header.index()].get()).next_sibling = Some(row);

			// row: RelationalRow [30..60]
			*registry.kinds[row.index()].get() = SemanticKind::RelationalRow;
			*registry.metrics[row.index()].get() = SpanMetrics {
				byte_length: 30,
				newlines: 0,
			};
			(*registry.edges[row.index()].get()).parent = Some(func);
			(*registry.edges[row.index()].get()).first_child = Some(row_a);

			// row_a: Token [30..45]
			*registry.kinds[row_a.index()].get() = SemanticKind::Token;
			*registry.metrics[row_a.index()].get() = SpanMetrics {
				byte_length: 15,
				newlines: 0,
			};
			(*registry.edges[row_a.index()].get()).parent = Some(row);
			(*registry.edges[row_a.index()].get()).next_sibling = Some(row_b);

			// row_b: Token [45..60]
			*registry.kinds[row_b.index()].get() = SemanticKind::Token;
			*registry.metrics[row_b.index()].get() = SpanMetrics {
				byte_length: 15,
				newlines: 0,
			};
			(*registry.edges[row_b.index()].get()).parent = Some(row);

			// t2: Token [60..100]
			*registry.kinds[t2.index()].get() = SemanticKind::Token;
			*registry.metrics[t2.index()].get() = SpanMetrics {
				byte_length: 40,
				newlines: 0,
			};
			(*registry.edges[t2.index()].get()).parent = Some(root);
		}

		(registry, root)
	}

	#[test]
	fn node_at_offset_finds_leaf_inside_function() {
		let (reg, root) = build_deep_tree();
		let q = UastQuery::new(&reg, root);

		// Offset 35 is inside row_a [30..45]
		let node = q.node_at_offset(DocByte::new(35)).unwrap();
		let (start, end) = q.node_bounds(node);
		assert_eq!(start, DocByte::new(30));
		assert_eq!(end, DocByte::new(45));
	}

	#[test]
	fn find_ancestor_finds_enclosing_function() {
		let (reg, root) = build_deep_tree();
		let q = UastQuery::new(&reg, root);

		// Cursor at byte 35 → leaf is row_a → ancestor RelationalTable is func
		let leaf = q.node_at_offset(DocByte::new(35)).unwrap();
		let func_node = q.find_ancestor(leaf, is_function_scope).unwrap();
		let (start, end) = q.node_bounds(func_node);
		assert_eq!(start, DocByte::new(10));
		assert_eq!(end, DocByte::new(60));
	}

	#[test]
	fn find_ancestor_returns_none_at_root_level() {
		let (reg, root) = build_deep_tree();
		let q = UastQuery::new(&reg, root);

		// Cursor at byte 5 → leaf is t1 → parent is root → should return None
		let leaf = q.node_at_offset(DocByte::new(5)).unwrap();
		assert!(q.find_ancestor(leaf, is_function_scope).is_none());
	}

	#[test]
	fn next_of_kind_finds_function_after_offset() {
		let (reg, root) = build_deep_tree();
		let q = UastQuery::new(&reg, root);

		// From offset 0, next RelationalTable should be func at [10..60]
		let next = q.next_of_kind(DocByte::new(0), is_function_scope).unwrap();
		let (start, end) = q.node_bounds(next);
		assert_eq!(start, DocByte::new(10));
		assert_eq!(end, DocByte::new(60));
	}

	#[test]
	fn next_of_kind_returns_none_when_no_more_functions() {
		let (reg, root) = build_deep_tree();
		let q = UastQuery::new(&reg, root);

		// From offset 60, no more RelationalTable nodes
		assert!(
			q.next_of_kind(DocByte::new(60), is_function_scope)
				.is_none()
		);
	}

	#[test]
	fn node_bounds_returns_correct_range_for_function() {
		let (reg, root) = build_deep_tree();
		let q = UastQuery::new(&reg, root);

		let leaf = q.node_at_offset(DocByte::new(15)).unwrap();
		let func = q.find_ancestor(leaf, is_function_scope).unwrap();
		let (start, end) = q.node_bounds(func);
		assert_eq!(start, DocByte::new(10));
		assert_eq!(end, DocByte::new(60));
	}

	/// Two functions: next_of_kind from inside func1 should find func2.
	///
	/// ```
	///  Root (100)
	///  ├─ Func1 (RelationalTable, 40) [0..40]
	///  │  └─ Token (40) [0..40]
	///  ├─ Token (10) [40..50]
	///  └─ Func2 (RelationalTable, 50) [50..100]
	///     └─ Token (50) [50..100]
	/// ```
	#[test]
	fn next_of_kind_skips_to_second_function() {
		let registry = UastRegistry::new(64);
		let root = registry.alloc_test_node();
		let func1 = registry.alloc_test_node();
		let f1_tok = registry.alloc_test_node();
		let gap = registry.alloc_test_node();
		let func2 = registry.alloc_test_node();
		let f2_tok = registry.alloc_test_node();

		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: 100,
				newlines: 0,
			};
			(*registry.edges[root.index()].get()).first_child = Some(func1);

			*registry.kinds[func1.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[func1.index()].get() = SpanMetrics {
				byte_length: 40,
				newlines: 0,
			};
			(*registry.edges[func1.index()].get()).parent = Some(root);
			(*registry.edges[func1.index()].get()).first_child = Some(f1_tok);
			(*registry.edges[func1.index()].get()).next_sibling = Some(gap);

			*registry.kinds[f1_tok.index()].get() = SemanticKind::Token;
			*registry.metrics[f1_tok.index()].get() = SpanMetrics {
				byte_length: 40,
				newlines: 0,
			};
			(*registry.edges[f1_tok.index()].get()).parent = Some(func1);

			*registry.kinds[gap.index()].get() = SemanticKind::Token;
			*registry.metrics[gap.index()].get() = SpanMetrics {
				byte_length: 10,
				newlines: 0,
			};
			(*registry.edges[gap.index()].get()).parent = Some(root);
			(*registry.edges[gap.index()].get()).next_sibling = Some(func2);

			*registry.kinds[func2.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[func2.index()].get() = SpanMetrics {
				byte_length: 50,
				newlines: 0,
			};
			(*registry.edges[func2.index()].get()).parent = Some(root);
			(*registry.edges[func2.index()].get()).first_child = Some(f2_tok);

			*registry.kinds[f2_tok.index()].get() = SemanticKind::Token;
			*registry.metrics[f2_tok.index()].get() = SpanMetrics {
				byte_length: 50,
				newlines: 0,
			};
			(*registry.edges[f2_tok.index()].get()).parent = Some(func2);
		}

		let q = UastQuery::new(&registry, root);

		// From inside func1, ]m should find func2
		let next = q.next_of_kind(DocByte::new(20), is_function_scope).unwrap();
		let (start, end) = q.node_bounds(next);
		assert_eq!(start, DocByte::new(50));
		assert_eq!(end, DocByte::new(100));

		// From gap between functions, ]m should also find func2
		let next = q.next_of_kind(DocByte::new(42), is_function_scope).unwrap();
		let (start, _) = q.node_bounds(next);
		assert_eq!(start, DocByte::new(50));
	}

	#[test]
	fn topology_groups_are_transparent_to_function_text_objects_and_navigation() {
		let (registry, root, grouped_leaf, function) = build_grouped_semantic_document();
		let query = UastQuery::new(&registry, root);

		assert!(
			query
				.find_ancestor(grouped_leaf, is_function_scope)
				.is_none(),
			"daf/caf must not treat a topology group as a function scope"
		);
		assert_eq!(
			query.next_of_kind(DocByte::ZERO, is_function_scope),
			Some(function),
			"]m must skip the topology group and find the real function"
		);
	}
}
