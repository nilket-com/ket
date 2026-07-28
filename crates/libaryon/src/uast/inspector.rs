use crate::ecs::{NodeId, UastRegistry};
use crate::uast::{ProviderKindId, SemanticKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UastMetricCertainty {
	Unknown,
	Partial,
	Exact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UastStorageKind {
	Branch,
	PhysicalCold,
	PhysicalPartial,
	PhysicalResident,
	Virtual,
}

/// Frontend-safe, bounded graph projection. It contains resolved labels and
/// numeric identities, never registry access or resident source payloads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UastGraphNode {
	pub id: u32,
	pub parent: Option<u32>,
	pub depth: u16,
	pub label: String,
	pub semantic_kind: SemanticKind,
	pub storage: UastStorageKind,
	pub start_byte: u64,
	pub end_byte: u64,
	pub selected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UastGraphSnapshot {
	pub nodes: Vec<UastGraphNode>,
	pub source_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UastInspectorOptions {
	pub max_nodes: usize,
	pub max_depth: u16,
	pub child_count_budget: usize,
	pub snippet_bytes: usize,
	pub cursor_byte: Option<u64>,
}

impl Default for UastInspectorOptions {
	fn default() -> Self {
		Self {
			max_nodes: 2_048,
			max_depth: 32,
			child_count_budget: 256,
			snippet_bytes: 48,
			cursor_byte: None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UastInspectorNode {
	pub node_id: NodeId,
	pub parent: Option<NodeId>,
	pub depth: u16,
	pub start_byte: u64,
	pub end_byte: u64,
	pub semantic_kind: SemanticKind,
	pub provider_kind: ProviderKindId,
	pub child_count: usize,
	pub child_count_exact: bool,
	pub newlines: u64,
	pub newline_certainty: UastMetricCertainty,
	pub storage: UastStorageKind,
	pub resident_bytes: u64,
	/// A prefix copied only from bytes already resident in memory.
	pub resident_snippet: Option<Vec<u8>>,
	pub contains_cursor: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UastInspectorSnapshot {
	pub root: NodeId,
	pub total_bytes: u64,
	pub allocated_nodes: u64,
	pub reachable_visited: usize,
	pub reachable_complete: bool,
	pub reachable_resident_bytes: u64,
	pub nodes: Vec<UastInspectorNode>,
}

#[derive(Debug, Clone, Copy)]
struct PendingNode {
	node: NodeId,
	parent: Option<NodeId>,
	depth: u16,
	start_byte: u64,
}

impl UastRegistry {
	/// Produces a bounded, metadata-only view of the live registry.
	///
	/// This method never calls a backing store, schedules hydration, or
	/// requests metric inflation. Snippets are present only when their prefix
	/// was already resident before this call.
	pub fn inspect_uast(
		&self,
		root: NodeId,
		options: UastInspectorOptions,
	) -> UastInspectorSnapshot {
		let max_nodes = options.max_nodes.max(1);
		let root_metrics_incomplete = self.inspector_root_metrics_incomplete(root);
		let mut pending = vec![PendingNode {
			node: root,
			parent: None,
			depth: 0,
			start_byte: 0,
		}];
		let mut nodes = Vec::with_capacity(max_nodes.min(4_096));
		let mut reachable_resident_bytes = 0u64;
		let mut depth_truncated = false;

		while nodes.len() < max_nodes {
			let Some(current) = pending.pop() else {
				break;
			};
			let idx = current.node.index();
			let byte_length = self.get_total_bytes(current.node);
			let end_byte = current.start_byte.saturating_add(byte_length);

			if current.parent.is_some()
				&& let Some(sibling) = self.get_next_sibling(current.node)
			{
				pending.push(PendingNode {
					node: sibling,
					parent: current.parent,
					depth: current.depth,
					start_byte: end_byte,
				});
			}

			let first_child = self.get_first_child(current.node);
			if current.depth < options.max_depth {
				if let Some(child) = first_child {
					pending.push(PendingNode {
						node: child,
						parent: Some(current.node),
						depth: current.depth.saturating_add(1),
						start_byte: current.start_byte,
					});
				}
			} else if first_child.is_some() {
				depth_truncated = true;
			}

			let (child_count, child_count_exact) =
				self.inspector_child_count(current.node, options.child_count_budget);
			let metadata = self.inspector_node_metadata(current.node, options.snippet_bytes);
			reachable_resident_bytes =
				reachable_resident_bytes.saturating_add(metadata.resident_bytes);
			let newline_certainty = if metadata.has_physical_span && !metadata.metric_inflated {
				UastMetricCertainty::Unknown
			} else if root_metrics_incomplete {
				UastMetricCertainty::Partial
			} else {
				UastMetricCertainty::Exact
			};
			let storage = if first_child.is_some() {
				UastStorageKind::Branch
			} else if !metadata.has_physical_span {
				UastStorageKind::Virtual
			} else if metadata.has_full_residency {
				UastStorageKind::PhysicalResident
			} else if metadata.has_partial_residency {
				UastStorageKind::PhysicalPartial
			} else {
				UastStorageKind::PhysicalCold
			};
			let contains_cursor = options.cursor_byte.is_some_and(|cursor| {
				if byte_length == 0 {
					cursor == current.start_byte
				} else {
					cursor >= current.start_byte && cursor < end_byte
				}
			});

			nodes.push(UastInspectorNode {
				node_id: current.node,
				parent: current.parent,
				depth: current.depth,
				start_byte: current.start_byte,
				end_byte,
				semantic_kind: unsafe { *self.kinds[idx].get() },
				provider_kind: unsafe { *self.provider_kinds[idx].get() },
				child_count,
				child_count_exact,
				newlines: self.get_total_newlines(current.node),
				newline_certainty,
				storage,
				resident_bytes: metadata.resident_bytes,
				resident_snippet: metadata.snippet,
				contains_cursor,
			});
		}

		let reachable_complete = pending.is_empty() && !depth_truncated;
		UastInspectorSnapshot {
			root,
			total_bytes: self.get_total_bytes(root),
			allocated_nodes: self.stats().allocated_nodes,
			reachable_visited: nodes.len(),
			reachable_complete,
			reachable_resident_bytes,
			nodes,
		}
	}

	fn inspector_child_count(&self, node: NodeId, budget: usize) -> (usize, bool) {
		let mut count = 0usize;
		let mut child = self.get_first_child(node);
		while let Some(current) = child {
			if count >= budget {
				return (count, false);
			}
			count += 1;
			child = self.get_next_sibling(current);
		}
		(count, true)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::pointer::SvpPointer;
	use crate::uast::SpanMetrics;

	#[test]
	fn inspecting_a_cold_leaf_does_not_hydrate_it() {
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 8,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 8,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 8,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);

		let before = registry.stats().resident_bytes;
		let snapshot = registry.inspect_uast(root, UastInspectorOptions::default());

		assert_eq!(registry.stats().resident_bytes, before);
		assert!(!registry.whole_leaf_is_resident(leaf));
		assert_eq!(snapshot.nodes[1].storage, UastStorageKind::PhysicalCold);
		assert_eq!(snapshot.nodes[1].resident_snippet, None);
		assert_eq!(
			snapshot.nodes[1].newline_certainty,
			UastMetricCertainty::Unknown
		);
	}

	#[test]
	fn snapshot_obeys_its_node_budget() {
		let registry = UastRegistry::new(4);
		let mut chunk = registry.reserve_chunk(4).expect("chunk");
		let root = chunk.spawn_node(SemanticKind::RelationalTable, None, SpanMetrics::default());
		for _ in 0..3 {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				None,
				SpanMetrics {
					byte_length: 1,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
		}

		let snapshot = registry.inspect_uast(
			root,
			UastInspectorOptions {
				max_nodes: 2,
				..UastInspectorOptions::default()
			},
		);
		assert_eq!(snapshot.nodes.len(), 2);
		assert!(!snapshot.reachable_complete);
	}
}
