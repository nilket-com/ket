use crate::core::{DocLine, DocLineCount, DocVisualRow, VisualRowCount};
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::UastProjection;
use crate::uast::topology::TARGET_TREE_FANOUT;

const B: usize = TARGET_TREE_FANOUT;
const SENTINEL: u32 = u32::MAX;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LineMetric {
	pub bytes: u64,
	pub doc_lines: DocLineCount,
	pub visual_rows: VisualRowCount,
}

impl LineMetric {
	fn add(self, other: Self) -> Self {
		Self {
			bytes: self.bytes + other.bytes,
			doc_lines: self.doc_lines + other.doc_lines.get(),
			visual_rows: self.visual_rows + other.visual_rows.get(),
		}
	}
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum TreeNode {
	Interior {
		children: [u32; B],
		child_count: u8,
		entry_count: usize,
		summary: LineMetric,
	},
	Leaf {
		entries: [LineMetric; B],
		len: u8,
		next_leaf: u32,
		summary: LineMetric,
	},
}

impl TreeNode {
	fn summary(&self) -> LineMetric {
		match self {
			TreeNode::Interior { summary, .. } | TreeNode::Leaf { summary, .. } => *summary,
		}
	}

	fn entry_count(&self) -> usize {
		match self {
			TreeNode::Interior { entry_count, .. } => *entry_count,
			TreeNode::Leaf { len, .. } => usize::from(*len),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineQuery {
	pub byte_offset: u64,
	pub visual_row: DocVisualRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisualRowQuery {
	pub doc_line: u64,
	pub byte_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteQuery {
	pub doc_line: u64,
	pub visual_row: DocVisualRow,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SumTreeInspectorRow {
	pub(crate) rank: usize,
	pub(crate) leaf: u32,
	pub(crate) slot: usize,
	pub(crate) start_byte: u64,
	pub(crate) end_byte: u64,
	pub(crate) start_doc_line: u64,
	pub(crate) end_doc_line: u64,
	pub(crate) start_visual_row: u64,
	pub(crate) end_visual_row: u64,
	pub(crate) on_lookup_path: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SumTreeInspectorSnapshot {
	pub(crate) total_entries: usize,
	pub(crate) arena_nodes: usize,
	pub(crate) levels: usize,
	pub(crate) total: LineMetric,
	pub(crate) focus_rank: Option<usize>,
	pub(crate) lookup_path: Vec<u32>,
	pub(crate) rows: Vec<SumTreeInspectorRow>,
	pub(crate) truncated_before: bool,
	pub(crate) truncated_after: bool,
}

#[derive(Debug, Clone)]
pub struct SumTree {
	arena: Vec<TreeNode>,
	root: u32,
	first_leaf: u32,
	doc_line_count: DocLineCount,
	has_cold_extents: bool,
}

impl Default for SumTree {
	fn default() -> Self {
		Self::new()
	}
}

impl SumTree {
	pub fn new() -> Self {
		let leaf = TreeNode::Leaf {
			entries: [LineMetric::default(); B],
			len: 0,
			next_leaf: SENTINEL,
			summary: LineMetric::default(),
		};
		Self {
			arena: vec![leaf],
			root: 0,
			first_leaf: 0,
			doc_line_count: DocLineCount::ZERO,
			has_cold_extents: false,
		}
	}

	pub fn build_from_lines(metrics: &[LineMetric]) -> Self {
		if metrics.is_empty() {
			return Self::new();
		}

		let mut arena: Vec<TreeNode> = Vec::new();

		// Build leaves
		let mut leaf_indices: Vec<u32> = Vec::new();
		for chunk in metrics.chunks(B) {
			let idx = arena.len() as u32;
			let mut entries = [LineMetric::default(); B];
			entries[..chunk.len()].copy_from_slice(chunk);
			let mut summary = LineMetric::default();
			for m in chunk {
				summary = summary.add(*m);
			}
			arena.push(TreeNode::Leaf {
				entries,
				len: chunk.len() as u8,
				next_leaf: SENTINEL,
				summary,
			});
			leaf_indices.push(idx);
		}

		// Thread leaves
		for i in 0..leaf_indices.len().saturating_sub(1) {
			let next = leaf_indices[i + 1];
			if let TreeNode::Leaf { next_leaf, .. } = &mut arena[leaf_indices[i] as usize] {
				*next_leaf = next;
			}
		}

		let first_leaf = leaf_indices[0];

		// Build interior levels bottom-up
		let mut current_level = leaf_indices;
		while current_level.len() > 1 {
			let mut next_level: Vec<u32> = Vec::new();
			for chunk in current_level.chunks(B) {
				let idx = arena.len() as u32;
				let mut children = [SENTINEL; B];
				let mut summary = LineMetric::default();
				let mut entry_count = 0usize;
				for (i, &child) in chunk.iter().enumerate() {
					children[i] = child;
					summary = summary.add(arena[child as usize].summary());
					entry_count = entry_count.saturating_add(arena[child as usize].entry_count());
				}
				arena.push(TreeNode::Interior {
					children,
					child_count: chunk.len() as u8,
					entry_count,
					summary,
				});
				next_level.push(idx);
			}
			current_level = next_level;
		}

		let root = current_level[0];

		Self {
			arena,
			root,
			first_leaf,
			doc_line_count: metrics.iter().fold(DocLineCount::ZERO, |total, metric| {
				total + metric.doc_lines.get()
			}),
			has_cold_extents: false,
		}
	}

	pub fn build_from_registry(registry: &UastRegistry, root: NodeId) -> Self {
		let _resident_read = registry.resident_data_guard();
		let total_bytes = registry.get_total_bytes(root);
		if total_bytes == 0 {
			return Self::new();
		}

		// Unloaded leaves contribute aggregate entries, so reserving by physical
		// line count defeats the sparse representation and can attempt enormous
		// allocations while metrics inflate. Grow by emitted entries instead.
		let mut line_metrics = Vec::new();
		let mut current_line_bytes = 0u64;
		let mut has_cold_extents = false;

		let mut visit = registry.get_first_child(root);
		while let Some(node) = visit {
			let idx = node.index();
			let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };
			if has_children {
				visit = registry.get_first_child(node);
				continue;
			}

			let m = unsafe { &*registry.metrics[idx].get() };
			let byte_len = m.byte_length;
			let newlines = m.newlines;

			let loaded = unsafe { &*registry.virtual_data[idx].get() };
			if let Some(v_data) = loaded {
				// Node loaded — scan actual bytes for precise per-line byte counts,
				// but only emit as many lines as the metrics claim (consistency with
				// registry.get_total_newlines).
				let mut emitted = 0u64;
				for &b in v_data.as_slice() {
					current_line_bytes += 1;
					if b == b'\n' {
						emitted += 1;
						if emitted <= newlines {
							line_metrics.push(LineMetric {
								bytes: current_line_bytes,
								doc_lines: DocLineCount::new(1),
								visual_rows: VisualRowCount::new(1),
							});
							current_line_bytes = 0;
						}
					}
				}
				// If metrics claim more newlines than the bytes contain,
				// emit the remaining lines with zero-byte width.
				while emitted < newlines {
					emitted += 1;
					line_metrics.push(LineMetric {
						bytes: current_line_bytes,
						doc_lines: DocLineCount::new(1),
						visual_rows: VisualRowCount::new(1),
					});
					current_line_bytes = 0;
				}
			} else {
				has_cold_extents = true;
				// Node not loaded — emit a single aggregate entry covering all
				// lines in this node instead of one entry per line. This keeps
				// memory O(nodes) instead of O(lines) for huge unloaded files.
				if newlines == 0 {
					current_line_bytes += byte_len;
				} else {
					line_metrics.push(LineMetric {
						bytes: current_line_bytes + byte_len,
						doc_lines: DocLineCount::new(newlines),
						visual_rows: VisualRowCount::new(newlines),
					});
					current_line_bytes = 0;
				}
			}

			visit = registry.get_next_node_in_walk(node);
		}

		// Final line (no trailing newline)
		if current_line_bytes > 0 || line_metrics.is_empty() {
			line_metrics.push(LineMetric {
				bytes: current_line_bytes,
				doc_lines: DocLineCount::new(1),
				visual_rows: VisualRowCount::new(1),
			});
		}

		let mut tree = Self::build_from_lines(&line_metrics);
		tree.has_cold_extents = has_cold_extents;
		tree
	}

	pub(crate) fn has_cold_extents(&self) -> bool {
		self.has_cold_extents
	}

	pub fn doc_line_count(&self) -> DocLineCount {
		self.doc_line_count
	}

	pub fn total_summary(&self) -> LineMetric {
		self.arena[self.root as usize].summary()
	}

	// --- Queries ---

	pub fn query_by_doc_line_count(&self, target_lines: DocLineCount) -> LineQuery {
		if self.doc_line_count == DocLineCount::ZERO || target_lines == DocLineCount::ZERO {
			return LineQuery {
				byte_offset: 0,
				visual_row: DocVisualRow::ZERO,
			};
		}
		let target = target_lines.min(self.doc_line_count);
		let mut prefix = LineMetric::default();
		self.walk_prefix_by_doc_lines(self.root, target, &mut prefix);
		LineQuery {
			byte_offset: prefix.bytes,
			visual_row: prefix.visual_rows.start_position(),
		}
	}

	pub fn query_by_visual_row(&self, target_row: DocVisualRow) -> VisualRowQuery {
		if self.doc_line_count == DocLineCount::ZERO || target_row == DocVisualRow::ZERO {
			return VisualRowQuery {
				doc_line: 0,
				byte_offset: 0,
			};
		}
		let total = self.total_summary();
		let target = match total.visual_rows.last_index() {
			Some(last_row) if target_row <= last_row => target_row.boundary_before(),
			Some(_) => total.visual_rows,
			None => VisualRowCount::ZERO,
		};
		let mut prefix = LineMetric::default();
		self.walk_prefix_by_visual_rows(self.root, target, &mut prefix);
		VisualRowQuery {
			doc_line: prefix.doc_lines.get(),
			byte_offset: prefix.bytes,
		}
	}

	pub fn query_by_byte(&self, target_byte: u64) -> ByteQuery {
		self.query_by_byte_traced(target_byte, |_| {}).0
	}

	fn query_by_byte_traced(
		&self,
		target_byte: u64,
		mut visit: impl FnMut(u32),
	) -> (ByteQuery, Option<usize>) {
		if self.doc_line_count == DocLineCount::ZERO || target_byte == 0 {
			return (
				ByteQuery {
					doc_line: 0,
					visual_row: DocVisualRow::ZERO,
				},
				(self.arena[self.root as usize].entry_count() > 0).then_some(0),
			);
		}
		let total = self.total_summary();
		let target = target_byte.min(total.bytes);
		let mut prefix = LineMetric::default();
		let mut rank = 0usize;
		let mut node = self.root;
		loop {
			visit(node);
			match &self.arena[node as usize] {
				TreeNode::Interior {
					children,
					child_count,
					..
				} => {
					let mut descended = false;
					for &child in &children[..usize::from(*child_count)] {
						let child_node = &self.arena[child as usize];
						let child_summary = child_node.summary();
						if prefix.bytes.saturating_add(child_summary.bytes) > target {
							node = child;
							descended = true;
							break;
						}
						prefix = prefix.add(child_summary);
						rank = rank.saturating_add(child_node.entry_count());
					}
					if !descended {
						return (
							ByteQuery {
								doc_line: prefix.doc_lines.get(),
								visual_row: prefix.visual_rows.start_position(),
							},
							rank.checked_sub(1),
						);
					}
				}
				TreeNode::Leaf { entries, len, .. } => {
					for entry in &entries[..usize::from(*len)] {
						if prefix.bytes.saturating_add(entry.bytes) > target {
							return (
								ByteQuery {
									doc_line: prefix.doc_lines.get(),
									visual_row: prefix.visual_rows.start_position(),
								},
								Some(rank),
							);
						}
						prefix = prefix.add(*entry);
						rank = rank.saturating_add(1);
					}
					return (
						ByteQuery {
							doc_line: prefix.doc_lines.get(),
							visual_row: prefix.visual_rows.start_position(),
						},
						rank.checked_sub(1),
					);
				}
			}
		}
	}

	pub(crate) fn inspector_snapshot(
		&self,
		cursor_byte: Option<u64>,
		max_rows: usize,
	) -> SumTreeInspectorSnapshot {
		let mut path = Vec::new();
		let focus_rank =
			cursor_byte.and_then(|byte| self.query_by_byte_traced(byte, |node| path.push(node)).1);
		let total_entries = self.arena[self.root as usize].entry_count();
		let max_rows = max_rows.max(1);
		let desired_start = focus_rank
			.unwrap_or(0)
			.saturating_sub(max_rows.saturating_div(2));
		let start_rank = desired_start.min(total_entries.saturating_sub(max_rows));
		let end_rank = start_rank.saturating_add(max_rows).min(total_entries);
		let rows = (start_rank..end_rank)
			.filter_map(|rank| self.inspector_row_at(rank, &path))
			.collect();
		let mut levels = 1usize;
		let mut node = self.root;
		while let TreeNode::Interior {
			children,
			child_count,
			..
		} = &self.arena[node as usize]
		{
			let Some(&child) = children[..usize::from(*child_count)].first() else {
				break;
			};
			levels = levels.saturating_add(1);
			node = child;
		}
		SumTreeInspectorSnapshot {
			total_entries,
			arena_nodes: self.arena.len(),
			levels,
			total: self.total_summary(),
			focus_rank,
			lookup_path: path,
			rows,
			truncated_before: start_rank > 0,
			truncated_after: end_rank < total_entries,
		}
	}

	fn inspector_row_at(&self, mut rank: usize, path: &[u32]) -> Option<SumTreeInspectorRow> {
		let absolute_rank = rank;
		let mut node = self.root;
		let mut prefix = LineMetric::default();
		loop {
			match &self.arena[node as usize] {
				TreeNode::Interior {
					children,
					child_count,
					..
				} => {
					let mut descended = false;
					for &child in &children[..usize::from(*child_count)] {
						let child_node = &self.arena[child as usize];
						let child_entries = child_node.entry_count();
						if rank < child_entries {
							node = child;
							descended = true;
							break;
						}
						rank = rank.saturating_sub(child_entries);
						prefix = prefix.add(child_node.summary());
					}
					if !descended {
						return None;
					}
				}
				TreeNode::Leaf { entries, len, .. } => {
					let slot = rank;
					if slot >= usize::from(*len) {
						return None;
					}
					for entry in &entries[..slot] {
						prefix = prefix.add(*entry);
					}
					let metric = entries[slot];
					return Some(SumTreeInspectorRow {
						rank: absolute_rank,
						leaf: node,
						slot,
						start_byte: prefix.bytes,
						end_byte: prefix.bytes.saturating_add(metric.bytes),
						start_doc_line: prefix.doc_lines.get(),
						end_doc_line: prefix
							.doc_lines
							.get()
							.saturating_add(metric.doc_lines.get()),
						start_visual_row: prefix.visual_rows.get(),
						end_visual_row: prefix
							.visual_rows
							.get()
							.saturating_add(metric.visual_rows.get()),
						on_lookup_path: path.contains(&node),
					});
				}
			}
		}
	}

	fn walk_prefix_by_doc_lines(
		&self,
		node_idx: u32,
		target: DocLineCount,
		prefix: &mut LineMetric,
	) {
		match &self.arena[node_idx as usize] {
			TreeNode::Interior {
				children,
				child_count,
				..
			} => {
				for &child in &children[..*child_count as usize] {
					let child_summary = self.arena[child as usize].summary();
					if prefix.doc_lines + child_summary.doc_lines.get() > target {
						self.walk_prefix_by_doc_lines(child, target, prefix);
						return;
					}
					*prefix = prefix.add(child_summary);
				}
			}
			TreeNode::Leaf { entries, len, .. } => {
				for e in &entries[..*len as usize] {
					if prefix.doc_lines + e.doc_lines.get() > target {
						return;
					}
					*prefix = prefix.add(*e);
				}
			}
		}
	}

	fn walk_prefix_by_visual_rows(
		&self,
		node_idx: u32,
		target: VisualRowCount,
		prefix: &mut LineMetric,
	) {
		match &self.arena[node_idx as usize] {
			TreeNode::Interior {
				children,
				child_count,
				..
			} => {
				for &child in &children[..*child_count as usize] {
					let child_summary = self.arena[child as usize].summary();
					if prefix.visual_rows + child_summary.visual_rows.get() > target {
						self.walk_prefix_by_visual_rows(child, target, prefix);
						return;
					}
					*prefix = prefix.add(child_summary);
				}
			}
			TreeNode::Leaf { entries, len, .. } => {
				for e in &entries[..*len as usize] {
					if prefix.visual_rows + e.visual_rows.get() > target {
						return;
					}
					*prefix = prefix.add(*e);
				}
			}
		}
	}

	// --- Mutations ---

	pub fn update_line(&mut self, line: DocLine, metric: LineMetric) {
		if line.get() >= self.doc_line_count.get() {
			return;
		}
		self.update_line_recursive(self.root, line.boundary_before(), metric);
	}

	fn update_line_recursive(
		&mut self,
		node_idx: u32,
		mut remaining: DocLineCount,
		metric: LineMetric,
	) {
		match &self.arena[node_idx as usize] {
			TreeNode::Interior {
				children,
				child_count,
				..
			} => {
				let count = *child_count as usize;
				let children_copy: Vec<u32> = children[..count].to_vec();
				for &child in &children_copy {
					let child_lines = self.arena[child as usize].summary().doc_lines;
					if remaining < child_lines {
						self.update_line_recursive(child, remaining, metric);
						self.recompute_interior_summary(node_idx);
						return;
					}
					remaining -= child_lines.get();
				}
			}
			TreeNode::Leaf { entries, len, .. } => {
				let l = *len as usize;
				let mut target_idx = None;
				let mut line_acc = DocLineCount::ZERO;
				for (i, entry) in entries[..l].iter().enumerate() {
					if line_acc + entry.doc_lines.get() > remaining {
						target_idx = Some(i);
						break;
					}
					line_acc += entry.doc_lines.get();
				}
				if let (
					Some(i),
					TreeNode::Leaf {
						entries, summary, ..
					},
				) = (target_idx, &mut self.arena[node_idx as usize])
				{
					entries[i] = metric;
					let mut s = LineMetric::default();
					for e in &entries[..l] {
						s = s.add(*e);
					}
					*summary = s;
				}
			}
		}
	}

	pub fn splice(
		&mut self,
		start_line: DocLine,
		delete_count: DocLineCount,
		insert: &[LineMetric],
	) {
		// Collect all existing entries
		let mut all_entries = self.collect_all_entries();
		let start = (start_line.get() as usize).min(all_entries.len());
		let end = (start + delete_count.get() as usize).min(all_entries.len());
		all_entries.splice(start..end, insert.iter().copied());

		// Rebuild tree from entries
		*self = Self::build_from_lines(&all_entries);
	}

	fn collect_all_entries(&self) -> Vec<LineMetric> {
		let mut result = Vec::new();
		let mut leaf = self.first_leaf;
		while leaf != SENTINEL {
			if let TreeNode::Leaf {
				entries,
				len,
				next_leaf,
				..
			} = &self.arena[leaf as usize]
			{
				result.extend_from_slice(&entries[..*len as usize]);
				leaf = *next_leaf;
			} else {
				break;
			}
		}
		result
	}

	fn recompute_interior_summary(&mut self, node_idx: u32) {
		let (children_copy, count) = if let TreeNode::Interior {
			children,
			child_count,
			..
		} = &self.arena[node_idx as usize]
		{
			let c = *child_count as usize;
			let mut copy = [SENTINEL; B];
			copy[..c].copy_from_slice(&children[..c]);
			(copy, c)
		} else {
			return;
		};

		let mut s = LineMetric::default();
		for &child in &children_copy[..count] {
			s = s.add(self.arena[child as usize].summary());
		}
		if let TreeNode::Interior { summary, .. } = &mut self.arena[node_idx as usize] {
			*summary = s;
		}
	}

	pub fn set_line_visual_rows(&mut self, line: DocLine, rows: VisualRowCount) {
		if line.get() >= self.doc_line_count.get() {
			return;
		}
		let metric = self.get_line_metric(line);
		if let Some(mut m) = metric {
			m.visual_rows = rows;
			self.update_line(line, m);
		}
	}

	fn get_line_metric(&self, line: DocLine) -> Option<LineMetric> {
		self.get_line_metric_recursive(self.root, line.boundary_before())
	}

	fn get_line_metric_recursive(
		&self,
		node_idx: u32,
		mut remaining: DocLineCount,
	) -> Option<LineMetric> {
		match &self.arena[node_idx as usize] {
			TreeNode::Interior {
				children,
				child_count,
				..
			} => {
				for &child in &children[..*child_count as usize] {
					let child_lines = self.arena[child as usize].summary().doc_lines;
					if remaining < child_lines {
						return self.get_line_metric_recursive(child, remaining);
					}
					remaining -= child_lines.get();
				}
				None
			}
			TreeNode::Leaf { entries, len, .. } => {
				let mut line_acc = DocLineCount::ZERO;
				for entry in &entries[..*len as usize] {
					if line_acc + entry.doc_lines.get() > remaining {
						return Some(*entry);
					}
					line_acc += entry.doc_lines.get();
				}
				None
			}
		}
	}

	// --- Threaded leaf iteration ---

	pub fn iter_leaves_from(&self, start_line: DocLine) -> LeafIter<'_> {
		let (leaf_idx, offset_in_leaf) = self.seek_leaf(start_line);
		LeafIter {
			tree: self,
			current_leaf: leaf_idx,
			entry_offset: offset_in_leaf,
		}
	}

	fn seek_leaf(&self, target_line: DocLine) -> (u32, usize) {
		let Some(last_line) = self.doc_line_count.last_index() else {
			return (self.first_leaf, 0);
		};
		let target = target_line.min(last_line);
		self.seek_leaf_recursive(self.root, target.boundary_before())
	}

	fn seek_leaf_recursive(&self, node_idx: u32, mut remaining: DocLineCount) -> (u32, usize) {
		match &self.arena[node_idx as usize] {
			TreeNode::Interior {
				children,
				child_count,
				..
			} => {
				let count = *child_count as usize;
				for &child in &children[..count] {
					let child_lines = self.arena[child as usize].summary().doc_lines;
					if remaining < child_lines {
						return self.seek_leaf_recursive(child, remaining);
					}
					remaining -= child_lines.get();
				}
				if count > 0 {
					self.seek_leaf_recursive(children[count - 1], remaining)
				} else {
					(node_idx, 0)
				}
			}
			TreeNode::Leaf { entries, len, .. } => {
				let mut line_acc = DocLineCount::ZERO;
				for (i, entry) in entries[..*len as usize].iter().enumerate() {
					if line_acc + entry.doc_lines.get() > remaining {
						return (node_idx, i);
					}
					line_acc += entry.doc_lines.get();
				}
				(node_idx, (*len as usize).saturating_sub(1))
			}
		}
	}
}

pub struct LeafIter<'a> {
	tree: &'a SumTree,
	current_leaf: u32,
	entry_offset: usize,
}

impl<'a> Iterator for LeafIter<'a> {
	type Item = LineMetric;

	fn next(&mut self) -> Option<LineMetric> {
		loop {
			if self.current_leaf == SENTINEL {
				return None;
			}
			if let TreeNode::Leaf {
				entries,
				len,
				next_leaf,
				..
			} = &self.tree.arena[self.current_leaf as usize]
			{
				let l = *len as usize;
				if self.entry_offset < l {
					let metric = entries[self.entry_offset];
					self.entry_offset += 1;
					return Some(metric);
				}
				self.current_leaf = *next_leaf;
				self.entry_offset = 0;
			} else {
				return None;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::SvpPointer;
	use crate::uast::{SemanticKind, SpanMetrics};

	fn make_metric(bytes: u64, doc_lines: u64, visual_rows: u64) -> LineMetric {
		LineMetric {
			bytes,
			doc_lines: DocLineCount::new(doc_lines),
			visual_rows: VisualRowCount::new(visual_rows),
		}
	}

	fn uniform_metrics(count: usize) -> Vec<LineMetric> {
		(0..count).map(|_| make_metric(80, 1, 1)).collect()
	}

	#[test]
	fn empty_tree() {
		let tree = SumTree::new();
		assert_eq!(tree.doc_line_count(), DocLineCount::ZERO);
		assert_eq!(tree.total_summary(), LineMetric::default());
		let q = tree.query_by_doc_line_count(DocLineCount::ZERO);
		assert_eq!(q.byte_offset, 0);
		assert_eq!(q.visual_row, DocVisualRow::ZERO);
	}

	#[test]
	fn build_single_line() {
		let tree = SumTree::build_from_lines(&[make_metric(42, 1, 2)]);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(1));
		let s = tree.total_summary();
		assert_eq!(s.bytes, 42);
		assert_eq!(s.doc_lines, DocLineCount::new(1));
		assert_eq!(s.visual_rows, VisualRowCount::new(2));
	}

	#[test]
	fn unloaded_extent_aggregates_more_than_u32_lines_without_per_line_entries() {
		let newlines = u64::from(u32::MAX) + 9;
		let bytes = newlines * 2;
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: bytes,
				newlines,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: bytes,
				newlines,
			},
		);
		chunk.append_local_child(root, leaf);

		let tree = SumTree::build_from_registry(&registry, root);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(newlines));
		assert_eq!(tree.total_summary().bytes, bytes);
	}

	#[test]
	fn clean_eviction_collapses_hot_per_line_layout_entries() {
		let registry = UastRegistry::new(3);
		let mut chunk = registry.reserve_chunk(3).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 11,
				newlines: 3,
			},
		);
		let first = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 7,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 7,
				newlines: 3,
			},
		);
		let second = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 1,
				byte_length: 4,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, first);
		chunk.append_local_child(root, second);
		registry.set_resident_budget(8, 4);
		registry.publish_hydrated_data(first, b"a\nb\nc\nq".to_vec());
		let hot_entries = SumTree::build_from_registry(&registry, root)
			.iter_leaves_from(DocLine::ZERO)
			.count();

		registry.publish_hydrated_data(second, b"tail".to_vec());
		// Eviction is engine-thread work; the resolver only publishes bytes.
		registry.enforce_resident_budget(None);
		assert!(unsafe { (&*registry.virtual_data[first.index()].get()).is_none() });
		let cold_entries = SumTree::build_from_registry(&registry, root)
			.iter_leaves_from(DocLine::ZERO)
			.count();
		assert!(cold_entries < hot_entries);
	}

	#[test]
	fn build_b_minus_1() {
		let metrics = uniform_metrics(B - 1);
		let tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.doc_line_count(), DocLineCount::new((B - 1) as u64));
		let s = tree.total_summary();
		assert_eq!(s.bytes, 80 * (B as u64 - 1));
		assert_eq!(s.doc_lines, DocLineCount::new((B - 1) as u64));
	}

	#[test]
	fn build_exactly_b() {
		let metrics = uniform_metrics(B);
		let tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(B as u64));
	}

	#[test]
	fn build_b_plus_1_causes_two_leaves() {
		let metrics = uniform_metrics(B + 1);
		let tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.doc_line_count(), DocLineCount::new((B + 1) as u64));
		let s = tree.total_summary();
		assert_eq!(s.doc_lines, DocLineCount::new((B + 1) as u64));
		assert_eq!(s.bytes, 80 * (B as u64 + 1));
	}

	#[test]
	fn build_large() {
		let n = 10_000;
		let metrics: Vec<LineMetric> = (0..n).map(|i| make_metric(i as u64 + 1, 1, 1)).collect();
		let tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(n as u64));
		let s = tree.total_summary();
		assert_eq!(s.bytes, (n * (n + 1) / 2) as u64);
		assert_eq!(s.doc_lines, DocLineCount::new(n as u64));
	}

	#[test]
	fn query_doc_line_roundtrip() {
		let metrics: Vec<LineMetric> = (0..200)
			.map(|i| make_metric(10 * (i + 1), 1, i % 3 + 1))
			.collect();
		let tree = SumTree::build_from_lines(&metrics);

		// Line 0 should have offset 0
		let q0 = tree.query_by_doc_line_count(DocLineCount::ZERO);
		assert_eq!(q0.byte_offset, 0);
		assert_eq!(q0.visual_row, DocVisualRow::ZERO);

		// Line 1 should be after first entry's bytes
		let q1 = tree.query_by_doc_line_count(DocLineCount::new(1));
		assert_eq!(q1.byte_offset, 10);
		assert_eq!(q1.visual_row, DocVisualRow::new(1)); // first line has visual_rows=1

		// Line 5
		let q5 = tree.query_by_doc_line_count(DocLineCount::new(5));
		let expected_bytes: u64 = (1..=5).map(|i| 10 * i as u64).sum();
		assert_eq!(q5.byte_offset, expected_bytes);
	}

	#[test]
	fn query_visual_row_roundtrip() {
		// 3 lines, each with different visual_rows
		let metrics = vec![
			make_metric(80, 1, 2),
			make_metric(40, 1, 1),
			make_metric(120, 1, 3),
		];
		let tree = SumTree::build_from_lines(&metrics);

		// visual row 0 -> doc_line 0
		let q0 = tree.query_by_visual_row(DocVisualRow::ZERO);
		assert_eq!(q0.doc_line, 0);

		// visual row 2 -> doc_line 1 (after 2 visual rows of line 0)
		let q2 = tree.query_by_visual_row(DocVisualRow::new(2));
		assert_eq!(q2.doc_line, 1);

		// visual row 3 -> doc_line 2 (after 2+1 visual rows of lines 0,1)
		let q3 = tree.query_by_visual_row(DocVisualRow::new(3));
		assert_eq!(q3.doc_line, 2);
	}

	#[test]
	fn query_by_byte_basic() {
		let metrics = vec![
			make_metric(100, 1, 1),
			make_metric(200, 1, 2),
			make_metric(50, 1, 1),
		];
		let tree = SumTree::build_from_lines(&metrics);

		let q0 = tree.query_by_byte(0);
		assert_eq!(q0.doc_line, 0);

		// Byte 100 -> start of line 1
		let q100 = tree.query_by_byte(100);
		assert_eq!(q100.doc_line, 1);

		// Byte 150 -> still line 1
		let q150 = tree.query_by_byte(150);
		assert_eq!(q150.doc_line, 1);

		// Byte 300 -> line 2
		let q300 = tree.query_by_byte(300);
		assert_eq!(q300.doc_line, 2);
	}

	#[test]
	fn update_line_updates_summary() {
		let metrics = uniform_metrics(10);
		let mut tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.total_summary().bytes, 800);

		tree.update_line(DocLine::new(3), make_metric(200, 1, 5));
		assert_eq!(tree.total_summary().bytes, 800 - 80 + 200);
		assert_eq!(
			tree.total_summary().visual_rows,
			VisualRowCount::new(10 - 1 + 5)
		);
	}

	#[test]
	fn splice_insert() {
		let metrics = uniform_metrics(5);
		let mut tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(5));

		tree.splice(
			DocLine::new(2),
			DocLineCount::ZERO,
			&[make_metric(99, 1, 1), make_metric(100, 1, 1)],
		);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(7));
		assert_eq!(tree.total_summary().bytes, 80 * 5 + 99 + 100);
	}

	#[test]
	fn splice_delete() {
		let metrics = uniform_metrics(10);
		let mut tree = SumTree::build_from_lines(&metrics);
		tree.splice(DocLine::new(3), DocLineCount::new(4), &[]);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(6));
		assert_eq!(tree.total_summary().bytes, 80 * 6);
	}

	#[test]
	fn splice_replace() {
		let metrics = uniform_metrics(10);
		let mut tree = SumTree::build_from_lines(&metrics);
		tree.splice(
			DocLine::new(2),
			DocLineCount::new(3),
			&[make_metric(999, 1, 1)],
		);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(8));
		assert_eq!(tree.total_summary().bytes, 80 * 7 + 999);
	}

	#[test]
	fn threaded_iteration_complete() {
		let n = 200;
		let metrics: Vec<LineMetric> = (0..n).map(|i| make_metric(i as u64 + 1, 1, 1)).collect();
		let tree = SumTree::build_from_lines(&metrics);

		let collected: Vec<LineMetric> = tree.iter_leaves_from(DocLine::ZERO).collect();
		assert_eq!(collected.len(), n);
		for (i, m) in collected.iter().enumerate() {
			assert_eq!(m.bytes, i as u64 + 1);
		}
	}

	#[test]
	fn threaded_iteration_from_middle() {
		let n = 200;
		let metrics: Vec<LineMetric> = (0..n).map(|i| make_metric(i as u64 + 1, 1, 1)).collect();
		let tree = SumTree::build_from_lines(&metrics);

		let start = 150;
		let collected: Vec<LineMetric> = tree.iter_leaves_from(DocLine::new(start)).collect();
		assert_eq!(collected.len(), n - start as usize);
		assert_eq!(collected[0].bytes, start + 1);
	}

	#[test]
	fn set_line_visual_rows_fold() {
		let metrics = uniform_metrics(10);
		let mut tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.total_summary().visual_rows, VisualRowCount::new(10));

		// Fold lines 3-5 (set visual_rows to 0)
		tree.set_line_visual_rows(DocLine::new(3), VisualRowCount::ZERO);
		tree.set_line_visual_rows(DocLine::new(4), VisualRowCount::ZERO);
		tree.set_line_visual_rows(DocLine::new(5), VisualRowCount::ZERO);
		assert_eq!(tree.total_summary().visual_rows, VisualRowCount::new(7));

		// Unfold line 4
		tree.set_line_visual_rows(DocLine::new(4), VisualRowCount::new(1));
		assert_eq!(tree.total_summary().visual_rows, VisualRowCount::new(8));
	}

	#[test]
	fn build_stress_multi_level() {
		// Force 3+ levels: B^2 + 1 = 4097 lines
		let n = B * B + 1;
		let metrics = uniform_metrics(n);
		let tree = SumTree::build_from_lines(&metrics);
		assert_eq!(tree.doc_line_count(), DocLineCount::new(n as u64));
		assert_eq!(tree.total_summary().bytes, 80 * n as u64);

		// Verify iteration covers all
		let collected: Vec<LineMetric> = tree.iter_leaves_from(DocLine::ZERO).collect();
		assert_eq!(collected.len(), n);

		// Verify queries at boundaries
		let q = tree.query_by_doc_line_count(DocLineCount::new(n as u64 - 1));
		assert_eq!(q.byte_offset, 80 * (n as u64 - 1));
	}

	#[test]
	fn splice_across_leaves() {
		// Build tree with 2 leaves
		let metrics = uniform_metrics(B + 10);
		let mut tree = SumTree::build_from_lines(&metrics);

		// Splice across the leaf boundary
		let new_lines: Vec<LineMetric> = (0..5).map(|_| make_metric(99, 1, 1)).collect();
		tree.splice(DocLine::new(B as u64 - 2), DocLineCount::new(5), &new_lines);

		let expected_total = B + 10 - 5 + 5;
		assert_eq!(
			tree.doc_line_count(),
			DocLineCount::new(expected_total as u64)
		);

		// Verify threaded iteration is consistent
		let collected: Vec<LineMetric> = tree.iter_leaves_from(DocLine::ZERO).collect();
		assert_eq!(collected.len(), expected_total);
	}

	#[test]
	fn query_clamping() {
		let metrics = uniform_metrics(10);
		let tree = SumTree::build_from_lines(&metrics);

		// Query past end should clamp
		let q = tree.query_by_doc_line_count(DocLineCount::new(100));
		assert_eq!(q.byte_offset, 800);

		let q = tree.query_by_visual_row(DocVisualRow::new(100));
		assert_eq!(q.doc_line, 10);

		let q = tree.query_by_byte(10000);
		assert_eq!(q.doc_line, 10);
	}

	#[test]
	fn inspector_window_uses_the_production_byte_descent() {
		let tree = SumTree::build_from_lines(&uniform_metrics(B * 3 + 7));
		let target = 80 * (B as u64 + 5) + 17;
		let production = tree.query_by_byte(target);
		let snapshot = tree.inspector_snapshot(Some(target), 9);

		assert_eq!(snapshot.rows.len(), 9);
		assert!(snapshot.truncated_before);
		assert!(snapshot.truncated_after);
		let focus_rank = snapshot.focus_rank.expect("focus rank");
		assert_eq!(focus_rank, B + 5);
		let focused = snapshot
			.rows
			.iter()
			.find(|row| row.rank == focus_rank)
			.expect("focused row");
		assert_eq!(focused.start_byte, production.doc_line * 80);
		assert_eq!(focused.start_doc_line, production.doc_line);
		assert!(focused.on_lookup_path);
		assert_eq!(snapshot.lookup_path.last(), Some(&focused.leaf));
		assert_eq!(snapshot.total_entries, B * 3 + 7);
		assert_eq!(snapshot.total.bytes, 80 * (B * 3 + 7) as u64);
		assert!(snapshot.levels >= 2);
	}
}
