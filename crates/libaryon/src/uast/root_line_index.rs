use crate::core::{DocByte, DocLine};
use crate::ecs::NodeId;
use std::collections::HashMap;

/// An extent's line summary, known only once its metrics have been inflated.
///
/// The count and the terminal-newline flag are one value because they are only
/// ever known together: line ownership at an extent boundary cannot be decided
/// from the count alone. Keeping them as two independent `Option`s let a caller
/// build an entry that reported itself unknown while still contributing its
/// newlines to the known-prefix summary, which silently mis-seeks `seek_line`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KnownLineMetric {
	pub(crate) newline_count: u64,
	pub(crate) ends_with_newline: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RootChildLineIndexEntry {
	pub(crate) node_id: NodeId,
	pub(crate) byte_length: u64,
	pub(crate) line_metric: Option<KnownLineMetric>,
}

impl RootChildLineIndexEntry {
	pub(crate) fn new(
		node_id: NodeId,
		byte_length: u64,
		line_metric: Option<KnownLineMetric>,
	) -> Self {
		Self {
			node_id,
			byte_length,
			line_metric,
		}
	}

	pub(crate) fn newline_count(self) -> Option<u64> {
		self.line_metric.map(|metric| metric.newline_count)
	}

	pub(crate) fn ends_with_newline(self) -> Option<bool> {
		self.line_metric.map(|metric| metric.ends_with_newline)
	}
}

#[derive(Debug, Clone, Copy, Default)]
struct IndexSummary {
	len: usize,
	bytes: u64,
	newlines: u64,
	height: usize,
	all_known: bool,
	known_prefix_len: usize,
	known_prefix_bytes: u64,
	known_prefix_newlines: u64,
}

impl IndexSummary {
	fn empty() -> Self {
		Self {
			all_known: true,
			..Self::default()
		}
	}

	fn entry(entry: RootChildLineIndexEntry) -> Self {
		// Every known-prefix field is derived from the same `Option`, so an entry
		// cannot be unknown yet still contribute newlines to the known prefix.
		let known = entry.line_metric.map(|metric| metric.newline_count);
		Self {
			len: 1,
			bytes: entry.byte_length,
			newlines: known.unwrap_or(0),
			height: 1,
			all_known: known.is_some(),
			known_prefix_len: usize::from(known.is_some()),
			known_prefix_bytes: known.map_or(0, |_| entry.byte_length),
			known_prefix_newlines: known.unwrap_or(0),
		}
	}

	fn concat(self, other: Self) -> Self {
		let (known_prefix_len, known_prefix_bytes, known_prefix_newlines) = if self.all_known {
			(
				self.len.saturating_add(other.known_prefix_len),
				self.bytes.saturating_add(other.known_prefix_bytes),
				self.newlines.saturating_add(other.known_prefix_newlines),
			)
		} else {
			(
				self.known_prefix_len,
				self.known_prefix_bytes,
				self.known_prefix_newlines,
			)
		};
		Self {
			len: self.len.saturating_add(other.len),
			bytes: self.bytes.saturating_add(other.bytes),
			newlines: self.newlines.saturating_add(other.newlines),
			height: self.height.max(other.height),
			all_known: self.all_known && other.all_known,
			known_prefix_len,
			known_prefix_bytes,
			known_prefix_newlines,
		}
	}
}

#[derive(Debug, Clone)]
struct IndexNode {
	entry: RootChildLineIndexEntry,
	priority: u64,
	left: Option<usize>,
	right: Option<usize>,
	parent: Option<usize>,
	summary: IndexSummary,
}

impl IndexNode {
	fn new(entry: RootChildLineIndexEntry) -> Self {
		let mut z = u64::from(entry.node_id.0.get()).wrapping_add(0x9e37_79b9_7f4a_7c15);
		z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
		let priority = z ^ (z >> 31);
		Self {
			entry,
			priority,
			left: None,
			right: None,
			parent: None,
			summary: IndexSummary::entry(entry),
		}
	}
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RootChildLineIndexLocation {
	pub(crate) entry: RootChildLineIndexEntry,
	pub(crate) rank: usize,
	pub(crate) start_byte: DocByte,
	pub(crate) start_line: Option<DocLine>,
}

impl RootChildLineIndexLocation {
	pub(crate) fn end_byte(self) -> DocByte {
		self.start_byte.saturating_add(self.entry.byte_length)
	}

	pub(crate) fn end_line(self) -> Option<DocLine> {
		self.start_line
			.zip(self.entry.newline_count())
			.map(|(start, count)| {
				start
					.checked_add(count)
					.expect("root child line prefix overflow")
			})
	}
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RootChildLineIndexInspectorRow {
	pub(crate) rank: usize,
	pub(crate) arena: usize,
	pub(crate) node_id: NodeId,
	pub(crate) start_byte: DocByte,
	pub(crate) end_byte: DocByte,
	pub(crate) start_line: Option<DocLine>,
	pub(crate) end_line: Option<DocLine>,
	pub(crate) byte_length: u64,
	pub(crate) newline_count: Option<u64>,
	pub(crate) subtree_bytes: u64,
	pub(crate) subtree_newlines: Option<u64>,
	pub(crate) parent: Option<usize>,
	pub(crate) left: Option<usize>,
	pub(crate) right: Option<usize>,
	pub(crate) on_lookup_path: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RootChildLineIndexInspectorSnapshot {
	pub(crate) total_entries: usize,
	pub(crate) arena_slots: usize,
	pub(crate) depth: usize,
	pub(crate) known_prefix_entries: usize,
	pub(crate) known_prefix_bytes: u64,
	pub(crate) known_prefix_newlines: u64,
	pub(crate) total_bytes: u64,
	pub(crate) focus_rank: Option<usize>,
	pub(crate) lookup_path: Vec<usize>,
	pub(crate) rows: Vec<RootChildLineIndexInspectorRow>,
	pub(crate) truncated_before: bool,
	pub(crate) truncated_after: bool,
}

/// Dynamic prefix-sum index over the direct children of a flat document root.
///
/// The implicit treap keeps stable arena handles for `NodeId` lookup while
/// supporting point metric changes and local topology splices in expected
/// O(log extents). Prefix line/byte queries use subtree summaries and never
/// rewrite suffix coordinates after an edit.
#[derive(Debug, Clone)]
pub struct RootChildLineIndex {
	nodes: Vec<IndexNode>,
	root: Option<usize>,
	positions: HashMap<NodeId, usize>,
}

impl RootChildLineIndex {
	pub(crate) fn from_entries(entries: impl IntoIterator<Item = RootChildLineIndexEntry>) -> Self {
		let mut index = Self {
			nodes: Vec::new(),
			root: None,
			positions: HashMap::new(),
		};
		for entry in entries {
			let rank = index.len();
			index.insert_at(rank, entry);
		}
		index
	}

	pub(crate) fn len(&self) -> usize {
		self.summary(self.root).len
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.root.is_none()
	}

	pub(crate) fn contains(&self, node: NodeId) -> bool {
		self.positions.contains_key(&node)
	}

	pub(crate) fn known_prefix_len(&self) -> usize {
		self.summary(self.root).known_prefix_len
	}

	pub(crate) fn known_prefix_boundary(&self) -> Option<RootChildLineIndexLocation> {
		self.known_prefix_len()
			.checked_sub(1)
			.and_then(|rank| self.location_at(rank))
	}

	pub(crate) fn first(&self) -> Option<RootChildLineIndexLocation> {
		self.location_at(0)
	}

	pub(crate) fn node_at(&self, rank: usize) -> Option<NodeId> {
		self.arena_at(rank)
			.map(|arena| self.nodes[arena].entry.node_id)
	}

	pub(crate) fn location_for_node(&self, node: NodeId) -> Option<RootChildLineIndexLocation> {
		let arena = *self.positions.get(&node)?;
		let rank = self.rank_of(arena);
		self.location_at_arena(arena, rank)
	}

	pub(crate) fn successor(&self, node: NodeId) -> Option<NodeId> {
		let arena = *self.positions.get(&node)?;
		let rank = self.rank_of(arena);
		self.arena_at(rank.saturating_add(1))
			.map(|next| self.nodes[next].entry.node_id)
	}

	pub(crate) fn update(&mut self, entry: RootChildLineIndexEntry) -> bool {
		self.update_status(entry).is_some()
	}

	pub(crate) fn update_changed(&mut self, entry: RootChildLineIndexEntry) -> bool {
		self.update_status(entry) == Some(true)
	}

	fn update_status(&mut self, entry: RootChildLineIndexEntry) -> Option<bool> {
		let &arena = self.positions.get(&entry.node_id)?;
		if self.nodes[arena].entry == entry {
			return Some(false);
		}
		self.nodes[arena].entry = entry;
		self.recompute_to_root(Some(arena));
		Some(true)
	}

	pub(crate) fn insert_after(&mut self, anchor: Option<NodeId>, entry: RootChildLineIndexEntry) {
		let rank = match anchor {
			Some(node) => self
				.positions
				.get(&node)
				.map(|&arena| self.rank_of(arena).saturating_add(1))
				.unwrap_or(self.len()),
			None => 0,
		};
		self.insert_at(rank, entry);
	}

	pub(crate) fn remove(&mut self, node: NodeId) -> bool {
		let Some(&arena) = self.positions.get(&node) else {
			return false;
		};
		let rank = self.rank_of(arena);
		let root = self.root.take();
		let (left, rest) = self.split(root, rank);
		let (removed, right) = self.split(rest, 1);
		self.root = self.merge(left, right);
		if let Some(root) = self.root {
			self.nodes[root].parent = None;
		}
		if let Some(removed) = removed {
			let removed_node = self.nodes[removed].entry.node_id;
			self.positions.remove(&removed_node);
			self.nodes[removed].left = None;
			self.nodes[removed].right = None;
			self.nodes[removed].parent = None;
		}
		true
	}

	pub(crate) fn seek_byte(&self, target: DocByte) -> Option<RootChildLineIndexLocation> {
		self.seek_byte_traced(target, |_| {})
	}

	fn seek_byte_traced(
		&self,
		target: DocByte,
		mut visit: impl FnMut(usize),
	) -> Option<RootChildLineIndexLocation> {
		let mut arena = self.root?;
		let mut prefix_bytes = 0u64;
		let mut prefix_rank = 0usize;
		loop {
			visit(arena);
			let left = self.nodes[arena].left;
			let left_summary = self.summary(left);
			let entry = self.nodes[arena].entry;
			let entry_start = prefix_bytes.saturating_add(left_summary.bytes);
			let entry_rank = prefix_rank.saturating_add(left_summary.len);
			if target.get() < entry_start {
				arena = left.expect("target before entry requires a left subtree");
				continue;
			}
			let entry_end = entry_start.saturating_add(entry.byte_length);
			if target.get() < entry_end {
				return self.location_at_arena_with_prefix(arena, entry_rank, entry_start);
			}
			if let Some(right) = self.nodes[arena].right {
				prefix_bytes = entry_end;
				prefix_rank = entry_rank.saturating_add(1);
				arena = right;
			} else {
				return self.location_at(self.len().saturating_sub(1));
			}
		}
	}

	pub(crate) fn inspector_snapshot(
		&self,
		cursor_byte: Option<DocByte>,
		max_rows: usize,
	) -> RootChildLineIndexInspectorSnapshot {
		let mut path = Vec::new();
		let focus_rank = cursor_byte
			.and_then(|target| self.seek_byte_traced(target, |arena| path.push(arena)))
			.map(|location| location.rank);
		let max_rows = max_rows.max(1);
		let desired_start = focus_rank
			.unwrap_or(0)
			.saturating_sub(max_rows.saturating_div(2));
		let start_rank = desired_start.min(self.len().saturating_sub(max_rows));
		let end_rank = start_rank.saturating_add(max_rows).min(self.len());
		let mut rows = Vec::with_capacity(end_rank.saturating_sub(start_rank));
		for rank in start_rank..end_rank {
			let Some(arena) = self.arena_at(rank) else {
				break;
			};
			let Some(location) = self.location_at_arena(arena, rank) else {
				break;
			};
			let node = &self.nodes[arena];
			rows.push(RootChildLineIndexInspectorRow {
				rank,
				arena,
				node_id: location.entry.node_id,
				start_byte: location.start_byte,
				end_byte: location.end_byte(),
				start_line: location.start_line,
				end_line: location.end_line(),
				byte_length: location.entry.byte_length,
				newline_count: location.entry.newline_count(),
				subtree_bytes: node.summary.bytes,
				subtree_newlines: node.summary.all_known.then_some(node.summary.newlines),
				parent: node.parent,
				left: node.left,
				right: node.right,
				on_lookup_path: path.contains(&arena),
			});
		}
		let summary = self.summary(self.root);
		RootChildLineIndexInspectorSnapshot {
			total_entries: summary.len,
			arena_slots: self.nodes.len(),
			depth: summary.height,
			known_prefix_entries: summary.known_prefix_len,
			known_prefix_bytes: summary.known_prefix_bytes,
			known_prefix_newlines: summary.known_prefix_newlines,
			total_bytes: summary.bytes,
			focus_rank,
			lookup_path: path,
			rows,
			truncated_before: start_rank > 0,
			truncated_after: end_rank < self.len(),
		}
	}

	pub(crate) fn seek_line(&self, target: DocLine) -> Option<RootChildLineIndexLocation> {
		if target == DocLine::ZERO {
			return self.first();
		}
		let candidate = self.seek_line_from(self.root?, target.get(), 0, 0, 0)?;
		let end_line = candidate.end_line()?;
		if end_line == target
			&& candidate.entry.ends_with_newline() == Some(true)
			&& candidate.rank.saturating_add(1) < self.known_prefix_len()
		{
			self.location_at(candidate.rank.saturating_add(1))
		} else {
			Some(candidate)
		}
	}

	fn seek_line_from(
		&self,
		arena: usize,
		target: u64,
		prefix_lines: u64,
		prefix_bytes: u64,
		prefix_rank: usize,
	) -> Option<RootChildLineIndexLocation> {
		let left = self.nodes[arena].left;
		let left_summary = self.summary(left);
		if left_summary.known_prefix_len > 0
			&& target <= prefix_lines.saturating_add(left_summary.known_prefix_newlines)
		{
			return self.seek_line_from(left?, target, prefix_lines, prefix_bytes, prefix_rank);
		}
		if !left_summary.all_known {
			return None;
		}

		let entry = self.nodes[arena].entry;
		let count = entry.newline_count()?;
		let start_line = prefix_lines.saturating_add(left_summary.newlines);
		let start_byte = prefix_bytes.saturating_add(left_summary.bytes);
		let rank = prefix_rank.saturating_add(left_summary.len);
		if target <= start_line.saturating_add(count) {
			return Some(RootChildLineIndexLocation {
				entry,
				rank,
				start_byte: DocByte::new(start_byte),
				start_line: Some(DocLine::new(start_line)),
			});
		}

		self.seek_line_from(
			self.nodes[arena].right?,
			target,
			start_line.saturating_add(count),
			start_byte.saturating_add(entry.byte_length),
			rank.saturating_add(1),
		)
	}

	fn insert_at(&mut self, rank: usize, entry: RootChildLineIndexEntry) {
		if self.positions.contains_key(&entry.node_id) {
			self.update(entry);
			return;
		}
		let arena = self.nodes.len();
		self.nodes.push(IndexNode::new(entry));
		self.positions.insert(entry.node_id, arena);
		let len = self.len();
		let root = self.root.take();
		let (left, right) = self.split(root, rank.min(len));
		let with_entry = self.merge(left, Some(arena));
		self.root = self.merge(with_entry, right);
		if let Some(root) = self.root {
			self.nodes[root].parent = None;
		}
	}

	fn location_at(&self, rank: usize) -> Option<RootChildLineIndexLocation> {
		let arena = self.arena_at(rank)?;
		self.location_at_arena(arena, rank)
	}

	fn location_at_arena(&self, arena: usize, rank: usize) -> Option<RootChildLineIndexLocation> {
		let prefix = self.prefix_summary(rank);
		Some(RootChildLineIndexLocation {
			entry: self.nodes.get(arena)?.entry,
			rank,
			start_byte: DocByte::new(prefix.bytes),
			start_line: prefix.all_known.then(|| DocLine::new(prefix.newlines)),
		})
	}

	fn location_at_arena_with_prefix(
		&self,
		arena: usize,
		rank: usize,
		start_byte: u64,
	) -> Option<RootChildLineIndexLocation> {
		let prefix = self.prefix_summary(rank);
		Some(RootChildLineIndexLocation {
			entry: self.nodes.get(arena)?.entry,
			rank,
			start_byte: DocByte::new(start_byte),
			start_line: prefix.all_known.then(|| DocLine::new(prefix.newlines)),
		})
	}

	fn arena_at(&self, mut rank: usize) -> Option<usize> {
		let mut arena = self.root?;
		loop {
			let left_len = self.summary(self.nodes[arena].left).len;
			if rank < left_len {
				arena = self.nodes[arena].left?;
			} else if rank == left_len {
				return Some(arena);
			} else {
				rank = rank.saturating_sub(left_len.saturating_add(1));
				arena = self.nodes[arena].right?;
			}
		}
	}

	fn rank_of(&self, arena: usize) -> usize {
		let mut rank = self.summary(self.nodes[arena].left).len;
		let mut current = arena;
		while let Some(parent) = self.nodes[current].parent {
			if self.nodes[parent].right == Some(current) {
				rank = rank
					.saturating_add(self.summary(self.nodes[parent].left).len)
					.saturating_add(1);
			}
			current = parent;
		}
		rank
	}

	fn prefix_summary(&self, count: usize) -> IndexSummary {
		self.prefix_summary_from(self.root, count.min(self.len()))
	}

	fn prefix_summary_from(&self, arena: Option<usize>, count: usize) -> IndexSummary {
		let Some(arena) = arena else {
			return IndexSummary::empty();
		};
		if count == 0 {
			return IndexSummary::empty();
		}
		let left = self.nodes[arena].left;
		let left_summary = self.summary(left);
		if count <= left_summary.len {
			return self.prefix_summary_from(left, count);
		}
		let with_entry = left_summary.concat(IndexSummary::entry(self.nodes[arena].entry));
		if count == left_summary.len.saturating_add(1) {
			return with_entry;
		}
		with_entry.concat(self.prefix_summary_from(
			self.nodes[arena].right,
			count.saturating_sub(left_summary.len.saturating_add(1)),
		))
	}

	fn summary(&self, arena: Option<usize>) -> IndexSummary {
		arena.map_or_else(IndexSummary::empty, |arena| self.nodes[arena].summary)
	}

	fn recompute(&mut self, arena: usize) {
		let left = self.summary(self.nodes[arena].left);
		let entry = IndexSummary::entry(self.nodes[arena].entry);
		let right = self.summary(self.nodes[arena].right);
		let mut summary = left.concat(entry).concat(right);
		summary.height = left.height.max(right.height).saturating_add(1);
		self.nodes[arena].summary = summary;
	}

	fn recompute_to_root(&mut self, mut arena: Option<usize>) {
		while let Some(current) = arena {
			self.recompute(current);
			arena = self.nodes[current].parent;
		}
	}

	fn split(&mut self, root: Option<usize>, left_len: usize) -> (Option<usize>, Option<usize>) {
		let Some(root) = root else {
			return (None, None);
		};
		let root_left = self.nodes[root].left;
		let root_left_len = self.summary(root_left).len;
		if left_len <= root_left_len {
			let (left, new_left) = self.split(root_left, left_len);
			self.nodes[root].left = new_left;
			if let Some(new_left) = new_left {
				self.nodes[new_left].parent = Some(root);
			}
			self.nodes[root].parent = None;
			if let Some(left) = left {
				self.nodes[left].parent = None;
			}
			self.recompute(root);
			(left, Some(root))
		} else {
			let root_right = self.nodes[root].right;
			let (new_right, right) = self.split(
				root_right,
				left_len.saturating_sub(root_left_len.saturating_add(1)),
			);
			self.nodes[root].right = new_right;
			if let Some(new_right) = new_right {
				self.nodes[new_right].parent = Some(root);
			}
			self.nodes[root].parent = None;
			if let Some(right) = right {
				self.nodes[right].parent = None;
			}
			self.recompute(root);
			(Some(root), right)
		}
	}

	fn merge(&mut self, left: Option<usize>, right: Option<usize>) -> Option<usize> {
		match (left, right) {
			(None, other) | (other, None) => {
				if let Some(other) = other {
					self.nodes[other].parent = None;
				}
				other
			}
			(Some(left), Some(right)) => {
				if self.nodes[left].priority <= self.nodes[right].priority {
					let left_right = self.nodes[left].right;
					let merged = self.merge(left_right, Some(right));
					self.nodes[left].right = merged;
					if let Some(merged) = merged {
						self.nodes[merged].parent = Some(left);
					}
					self.nodes[left].parent = None;
					self.recompute(left);
					Some(left)
				} else {
					let right_left = self.nodes[right].left;
					let merged = self.merge(Some(left), right_left);
					self.nodes[right].left = merged;
					if let Some(merged) = merged {
						self.nodes[merged].parent = Some(right);
					}
					self.nodes[right].parent = None;
					self.recompute(right);
					Some(right)
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn entry(id: usize, bytes: u64, lines: Option<u64>) -> RootChildLineIndexEntry {
		RootChildLineIndexEntry::new(
			NodeId::from_index(id),
			bytes,
			lines.map(|newline_count| KnownLineMetric {
				newline_count,
				ends_with_newline: false,
			}),
		)
	}

	#[test]
	fn point_updates_shift_prefixes_without_rebuilding_suffix_entries() {
		let mut index = RootChildLineIndex::from_entries([
			entry(0, 10, Some(1)),
			entry(1, 20, Some(2)),
			entry(2, 30, Some(3)),
		]);
		assert_eq!(
			index
				.location_for_node(NodeId::from_index(2))
				.expect("third")
				.start_byte,
			DocByte::new(30)
		);
		index.update(entry(1, 25, Some(4)));
		let third = index
			.location_for_node(NodeId::from_index(2))
			.expect("third");
		assert_eq!(third.start_byte, DocByte::new(35));
		assert_eq!(third.start_line, Some(DocLine::new(5)));
	}

	#[test]
	fn local_splices_preserve_line_and_byte_queries() {
		let mut index =
			RootChildLineIndex::from_entries([entry(0, 4, Some(1)), entry(2, 4, Some(1))]);
		index.insert_after(Some(NodeId::from_index(0)), entry(1, 3, Some(0)));
		assert_eq!(
			index
				.seek_byte(DocByte::new(5))
				.expect("middle")
				.entry
				.node_id,
			NodeId::from_index(1)
		);
		assert_eq!(
			index
				.seek_line(DocLine::new(1))
				.expect("line one")
				.entry
				.node_id,
			NodeId::from_index(0)
		);
		assert!(index.remove(NodeId::from_index(1)));
		assert_eq!(
			index
				.location_for_node(NodeId::from_index(2))
				.expect("last")
				.start_byte,
			DocByte::new(4)
		);
	}

	#[test]
	fn unknown_entry_bounds_the_known_prefix() {
		let mut index = RootChildLineIndex::from_entries([
			entry(0, 4, Some(1)),
			entry(1, 4, None),
			entry(2, 4, Some(1)),
		]);
		assert_eq!(index.known_prefix_len(), 1);
		index.update(entry(1, 4, Some(2)));
		assert_eq!(index.known_prefix_len(), 3);
		assert_eq!(
			index
				.location_for_node(NodeId::from_index(2))
				.expect("last")
				.start_line,
			Some(DocLine::new(3))
		);
	}

	#[test]
	fn large_point_update_and_removal_sequence_matches_a_linear_model() {
		let mut model: Vec<_> = (0..2_048usize)
			.map(|id| entry(id, (id % 13 + 1) as u64, Some((id % 5) as u64)))
			.collect();
		let mut index = RootChildLineIndex::from_entries(model.iter().copied());

		for id in (0..2_048usize).step_by(7) {
			let replacement = entry(id, (id % 17 + 3) as u64, Some((id % 3 + 1) as u64));
			assert!(index.update(replacement));
			model[id] = replacement;
		}
		for id in (0..2_048usize).step_by(5) {
			assert!(index.remove(NodeId::from_index(id)));
		}
		model.retain(|entry| entry.node_id.index() % 5 != 0);

		let mut expected_bytes = 0u64;
		let mut expected_lines = 0u64;
		for entry in &model {
			let location = index
				.location_for_node(entry.node_id)
				.expect("model node remains indexed");
			assert_eq!(location.start_byte, DocByte::new(expected_bytes));
			assert_eq!(location.start_line, Some(DocLine::new(expected_lines)));
			expected_bytes = expected_bytes.saturating_add(entry.byte_length);
			expected_lines =
				expected_lines.saturating_add(entry.newline_count().expect("known model entry"));
		}
		assert_eq!(index.len(), model.len());
		assert_eq!(
			index
				.seek_byte(DocByte::new(expected_bytes.saturating_sub(1)))
				.expect("last byte")
				.entry
				.node_id,
			model.last().expect("model is non-empty").node_id
		);
	}

	#[test]
	fn inspector_window_is_bounded_ordered_and_uses_the_real_seek_path() {
		let index = RootChildLineIndex::from_entries((0..32usize).map(|id| {
			entry(
				id,
				u64::try_from(id + 1).expect("fixture"),
				Some((id % 3) as u64),
			)
		}));
		let target = DocByte::new(200);
		let expected = index.seek_byte(target).expect("real seek");
		let snapshot = index.inspector_snapshot(Some(target), 7);

		assert_eq!(snapshot.focus_rank, Some(expected.rank));
		assert_eq!(snapshot.rows.len(), 7);
		assert!(snapshot.truncated_before);
		assert!(snapshot.truncated_after);
		assert!(
			snapshot
				.rows
				.windows(2)
				.all(|pair| pair[0].rank + 1 == pair[1].rank
					&& pair[0].end_byte == pair[1].start_byte)
		);
		let focused = snapshot
			.rows
			.iter()
			.find(|row| row.rank == expected.rank)
			.expect("focused row");
		assert_eq!(focused.node_id, expected.entry.node_id);
		assert!(focused.on_lookup_path);
		assert_eq!(snapshot.lookup_path.last(), Some(&focused.arena));
		assert_eq!(snapshot.total_entries, 32);
		assert!(snapshot.depth > 0);
	}
}
