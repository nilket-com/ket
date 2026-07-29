use crate::core::{BufferId, DocByte, StateId, TopologyRev};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UastGraphStamp {
	pub source_buffer_id: BufferId,
	pub root_id: u32,
	pub state_id: StateId,
	pub topology_rev: TopologyRev,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UastGraphSelection {
	pub stamp: UastGraphStamp,
	pub node_id: u32,
	pub start_byte: u64,
	pub end_byte: u64,
	pub provider_kind: ProviderKindId,
	pub ancestry: Vec<ProviderKindId>,
}

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
	pub provider_kind: ProviderKindId,
	pub ancestry: Vec<ProviderKindId>,
	pub selected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UastGraphSnapshot {
	pub stamp: UastGraphStamp,
	pub nodes: Vec<UastGraphNode>,
	pub source_truncated: bool,
}

impl UastGraphSnapshot {
	pub fn selection(&self, node: &UastGraphNode) -> UastGraphSelection {
		UastGraphSelection {
			stamp: self.stamp,
			node_id: node.id,
			start_byte: node.start_byte,
			end_byte: node.end_byte,
			provider_kind: node.provider_kind,
			ancestry: node.ancestry.clone(),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UastInspectorOptions {
	pub max_nodes: usize,
	pub max_depth: u16,
	pub child_count_budget: usize,
	/// Maximum topology nodes spent locating a cursor-centered scope.
	pub cursor_lookup_budget: usize,
	pub snippet_bytes: usize,
	pub cursor_byte: Option<u64>,
}

impl Default for UastInspectorOptions {
	fn default() -> Self {
		Self {
			max_nodes: 2_048,
			max_depth: 32,
			child_count_budget: 256,
			cursor_lookup_budget: 4_096,
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
	pub capacity: u64,
	pub retired_nodes: u64,
	pub authoritative_payload_bytes: u64,
	pub orphaned_payload_bytes: u64,
	pub orphaned_payload_bytes_reclaimed: u64,
	pub reachable_visited: usize,
	pub reachable_complete: bool,
	pub reachable_resident_bytes: u64,
	/// True when the node list is centered on the cursor rather than being a
	/// root-prefix traversal. Diagnostics remain honest lower bounds.
	pub cursor_scoped: bool,
	pub nodes: Vec<UastInspectorNode>,
}

#[derive(Debug, Clone, Copy)]
struct PendingNode {
	node: NodeId,
	parent: Option<NodeId>,
	depth: u16,
	start_byte: u64,
	follow_sibling: bool,
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
		let document_end = self.get_total_bytes(root);
		let root_metrics_incomplete = self.inspector_root_metrics_incomplete(root);
		let (prefix, mut pending, cursor_scoped) = self
			.inspector_cursor_scope(root, options)
			.unwrap_or_else(|| {
				(
					Vec::new(),
					vec![PendingNode {
						node: root,
						parent: None,
						depth: 0,
						start_byte: 0,
						follow_sibling: false,
					}],
					false,
				)
			});
		let mut nodes = Vec::with_capacity(max_nodes.min(4_096));
		let mut reachable_resident_bytes = 0u64;
		let mut depth_truncated = false;

		for current in prefix.into_iter().take(max_nodes) {
			let (node, resident_bytes, _) = self.inspector_snapshot_node(
				current,
				root_metrics_incomplete,
				document_end,
				options,
			);
			reachable_resident_bytes = reachable_resident_bytes.saturating_add(resident_bytes);
			nodes.push(node);
		}

		while nodes.len() < max_nodes {
			let Some(current) = pending.pop() else {
				break;
			};
			let (node, resident_bytes, first_child) = self.inspector_snapshot_node(
				current,
				root_metrics_incomplete,
				document_end,
				options,
			);
			let end_byte = node.end_byte;

			if current.follow_sibling
				&& let Some(sibling) = self.get_next_sibling(current.node)
			{
				pending.push(PendingNode {
					node: sibling,
					parent: current.parent,
					depth: current.depth,
					start_byte: end_byte,
					follow_sibling: true,
				});
			}

			if current.depth < options.max_depth {
				if let Some(child) = first_child {
					pending.push(PendingNode {
						node: child,
						parent: Some(current.node),
						depth: current.depth.saturating_add(1),
						start_byte: current.start_byte,
						follow_sibling: true,
					});
				}
			} else if first_child.is_some() {
				depth_truncated = true;
			}

			reachable_resident_bytes = reachable_resident_bytes.saturating_add(resident_bytes);
			nodes.push(node);
		}

		let reachable_complete = !cursor_scoped && pending.is_empty() && !depth_truncated;
		let stats = self.stats();
		UastInspectorSnapshot {
			root,
			total_bytes: self.get_total_bytes(root),
			allocated_nodes: stats.allocated_nodes,
			capacity: stats.capacity,
			retired_nodes: stats.retired_nodes,
			authoritative_payload_bytes: stats.authoritative_payload_bytes,
			orphaned_payload_bytes: stats.orphaned_payload_bytes,
			orphaned_payload_bytes_reclaimed: stats.orphaned_payload_bytes_reclaimed,
			reachable_visited: nodes.len(),
			reachable_complete,
			reachable_resident_bytes,
			cursor_scoped,
			nodes,
		}
	}

	fn inspector_snapshot_node(
		&self,
		current: PendingNode,
		root_metrics_incomplete: bool,
		document_end: u64,
		options: UastInspectorOptions,
	) -> (UastInspectorNode, u64, Option<NodeId>) {
		let idx = current.node.index();
		let byte_length = self.get_total_bytes(current.node);
		let end_byte = current.start_byte.saturating_add(byte_length);
		let first_child = self.get_first_child(current.node);
		let (child_count, child_count_exact) =
			self.inspector_child_count(current.node, options.child_count_budget);
		let metadata = self.inspector_node_metadata(current.node, options.snippet_bytes);
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
				(cursor >= current.start_byte && cursor < end_byte)
					|| (cursor == document_end && end_byte == document_end)
			}
		});
		let resident_bytes = metadata.resident_bytes;
		(
			UastInspectorNode {
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
				resident_bytes,
				resident_snippet: metadata.snippet,
				contains_cursor,
			},
			resident_bytes,
			first_child,
		)
	}

	/// Resolve a bounded cursor path, then begin traversal at the cursor's
	/// nearest useful branch. Ancestors are retained so the scope remains
	/// intelligible, while wide siblings before the cursor are never dumped.
	fn inspector_cursor_scope(
		&self,
		root: NodeId,
		options: UastInspectorOptions,
	) -> Option<(Vec<PendingNode>, Vec<PendingNode>, bool)> {
		let cursor = options.cursor_byte?;
		let (target, mut remaining) = self
			.find_node_at_doc_byte_raw_bounded(
				root,
				DocByte::new(cursor),
				options.cursor_lookup_budget.max(1),
			)
			.ok()
			.map(|(target, visited)| {
				(
					target,
					options.cursor_lookup_budget.max(1).saturating_sub(visited),
				)
			})?;

		let mut path = vec![target.node_id];
		let mut current = target.node_id;
		while current != root {
			if remaining == 0 {
				return None;
			}
			remaining -= 1;
			let parent = unsafe { (*self.edges[current.index()].get()).parent }?;
			path.push(parent);
			current = parent;
		}
		path.reverse();
		path.truncate(usize::from(options.max_depth).saturating_add(1));
		if path.is_empty() || path[0] != root {
			return None;
		}

		let mut starts = Vec::with_capacity(path.len());
		starts.push(0u64);
		for pair in path.windows(2) {
			let parent = pair[0];
			let child = pair[1];
			let parent_start = *starts.last()?;
			let indexed_start = (parent == root)
				.then(|| {
					self.root_child_line_indexes
						.read()
						.unwrap()
						.get(&root)
						.and_then(|index| index.location_for_node(child))
						.map(|location| location.start_byte.get())
				})
				.flatten();
			if let Some(start) = indexed_start {
				starts.push(start);
				continue;
			}

			let mut child_start = parent_start;
			let mut sibling = self.get_first_child(parent);
			while let Some(candidate) = sibling {
				if remaining == 0 {
					return None;
				}
				remaining -= 1;
				if candidate == child {
					break;
				}
				child_start = child_start.saturating_add(self.get_total_bytes(candidate));
				sibling = self.get_next_sibling(candidate);
			}
			if sibling != Some(child) {
				return None;
			}
			starts.push(child_start);
		}

		let focus_index = if path.len() <= 1 {
			0
		} else {
			path.len().saturating_sub(2).max(1)
		};
		let mut pending_path = path
			.into_iter()
			.zip(starts)
			.enumerate()
			.map(|(depth, (node, start_byte))| PendingNode {
				node,
				parent: (depth > 0).then(|| {
					unsafe { (*self.edges[node.index()].get()).parent }
						.expect("cursor path descendant has a parent")
				}),
				depth: depth as u16,
				start_byte,
				follow_sibling: false,
			})
			.collect::<Vec<_>>();
		let mut focus = pending_path.split_off(focus_index);
		focus.truncate(1);
		if let Some(scope_root) = focus.first_mut() {
			scope_root.follow_sibling = true;
		}
		Some((pending_path, focus, focus_index > 0))
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
		let revision_before = registry.inspector_revision();
		let snapshot = registry.inspect_uast(root, UastInspectorOptions::default());

		assert_eq!(registry.stats().resident_bytes, before);
		assert_eq!(registry.inspector_revision(), revision_before);
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

	#[test]
	fn graph_selection_carries_its_stamp_and_remap_signature() {
		let stamp = UastGraphStamp {
			source_buffer_id: BufferId::new(7),
			root_id: 11,
			state_id: StateId::new(13),
			topology_rev: TopologyRev::new(17),
		};
		let node = UastGraphNode {
			id: 19,
			parent: Some(11),
			depth: 2,
			label: "FN".to_string(),
			semantic_kind: SemanticKind::Group,
			storage: UastStorageKind::Branch,
			start_byte: 23,
			end_byte: 29,
			provider_kind: ProviderKindId::new(31),
			ancestry: vec![ProviderKindId::new(37), ProviderKindId::new(41)],
			selected: true,
		};
		let snapshot = UastGraphSnapshot {
			stamp,
			nodes: vec![node.clone()],
			source_truncated: false,
		};

		let selection = snapshot.selection(&node);

		assert_eq!(selection.stamp, stamp);
		assert_eq!(selection.node_id, node.id);
		assert_eq!(selection.start_byte, node.start_byte);
		assert_eq!(selection.end_byte, node.end_byte);
		assert_eq!(selection.provider_kind, node.provider_kind);
		assert_eq!(selection.ancestry, node.ancestry);
	}

	#[test]
	fn cursor_scope_reaches_late_nodes_without_dumping_the_root_prefix() {
		const ROWS: u32 = 100;
		let registry = UastRegistry::new(1 + ROWS * 2);
		let mut chunk = registry.reserve_chunk(1 + ROWS * 2).expect("chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: ROWS as u64,
				newlines: 0,
			},
		);
		let mut target_row = None;
		let mut target_leaf = None;
		for index in 0..ROWS {
			let row = chunk.spawn_node(
				SemanticKind::RelationalRow,
				None,
				SpanMetrics {
					byte_length: 1,
					newlines: 0,
				},
			);
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				None,
				SpanMetrics {
					byte_length: 1,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, row);
			chunk.append_local_child(row, leaf);
			if index == ROWS - 1 {
				target_row = Some(row);
				target_leaf = Some(leaf);
			}
		}

		let snapshot = registry.inspect_uast(
			root,
			UastInspectorOptions {
				max_nodes: 8,
				cursor_byte: Some(u64::from(ROWS - 1)),
				..UastInspectorOptions::default()
			},
		);

		assert!(snapshot.cursor_scoped);
		assert!(snapshot.nodes.len() <= 8);
		assert_eq!(
			snapshot
				.nodes
				.iter()
				.map(|node| node.node_id)
				.collect::<Vec<_>>(),
			vec![root, target_row.expect("row"), target_leaf.expect("leaf")]
		);
		assert!(snapshot.nodes.iter().all(|node| node.contains_cursor));
		assert_eq!(snapshot.nodes[1].start_byte, u64::from(ROWS - 1));
	}
}
