use crate::core::DocByte;
use crate::core::DocLine;
use crate::core::TAB_SIZE;
use crate::ecs::NodeId;
use crate::ecs::registry::UastRegistry;
use crate::uast::UastProjection;
use crate::uast::kind::SemanticKind;
use std::collections::HashMap;

use super::session::{DiffBlock, DiffKind, DiffRole, IntraLineDiff, IntraLineDiffKind};
use crate::core::PaneId;
use crate::engine::session::EditorSession;

#[cfg(test)]
std::thread_local! {
	static STRUCTURAL_DIFF_NODE_VISITS: std::cell::Cell<u64> = const {
		std::cell::Cell::new(0)
	};
}

pub(crate) fn recompute_all_diffs(registry: &UastRegistry, session: &mut EditorSession) {
	let mut pairs_to_recompute = Vec::new();
	for (&pane_id, pair) in &session.diff_pairs {
		// Pane ids deduplicate reciprocal entries only. Chronology comes from
		// the persistent role, never from allocation or screen order.
		if pane_id.get() < pair.partner_pane_id.get() {
			let (before_pane_id, after_pane_id) =
				canonical_pair_order(pane_id, pair.partner_pane_id, pair.role);
			pairs_to_recompute.push((before_pane_id, after_pane_id));
		}
	}

	for (before_pane_id, after_pane_id) in pairs_to_recompute {
		compute_pane_diff(registry, session, before_pane_id, after_pane_id);
	}
}

fn canonical_pair_order(
	pane_id: PaneId,
	partner_pane_id: PaneId,
	role: DiffRole,
) -> (PaneId, PaneId) {
	match role {
		DiffRole::Before => (pane_id, partner_pane_id),
		DiffRole::After => (partner_pane_id, pane_id),
	}
}

pub(crate) fn compute_pane_diff(
	registry: &UastRegistry,
	session: &mut EditorSession,
	before_pane_id: PaneId,
	after_pane_id: PaneId,
) {
	let before_doc = session
		.layout
		.find_pane(before_pane_id)
		.and_then(|pane| session.buffers.get(pane.active_tab().buffer_id));
	let after_doc = session
		.layout
		.find_pane(after_pane_id)
		.and_then(|pane| session.buffers.get(pane.active_tab().buffer_id));

	if let (Some(before), Some(after)) = (before_doc, after_doc)
		&& let (Some(before_root), Some(after_root)) = (before.root_id(), after.root_id())
	{
		let blocks = compute_diff(before_root, after_root, registry);

		let mut after_blocks = Vec::with_capacity(blocks.len());
		for block in &blocks {
			let mirrored_intra = block.intra_line_diffs.as_ref().map(|diffs| {
				diffs
					.iter()
					.map(|diff| diff.swap_sides())
					.collect::<Vec<_>>()
			});
			after_blocks.push(crate::engine::session::DiffBlock {
				range: block.partner_range,
				kind: block.kind,
				partner_range: block.range,
				intra_line_diffs: mirrored_intra,
			});
		}

		// DiffPair is symmetric one-to-one. Re-pairing a pane (e.g. a second
		// `:pd` from the same worktree pane) must detach BOTH sides of any
		// pair it replaces: overwriting only this pane's entry would leave
		// the abandoned partner holding a stale one-way pair, and scroll
		// sync / navigation would disagree depending on which pane is
		// focused. The abandoned partner stays open, just unpaired.
		for (pane, partner) in [
			(before_pane_id, after_pane_id),
			(after_pane_id, before_pane_id),
		] {
			if let Some(old) = session.diff_pairs.get(&pane)
				&& old.partner_pane_id != partner
			{
				let abandoned = old.partner_pane_id;
				if session
					.diff_pairs
					.get(&abandoned)
					.is_some_and(|pair| pair.partner_pane_id == pane)
				{
					session.diff_pairs.remove(&abandoned);
				}
			}
		}
		session.diff_pairs.insert(
			before_pane_id,
			crate::engine::session::DiffPair {
				partner_pane_id: after_pane_id,
				role: DiffRole::Before,
				blocks: blocks.clone(),
			},
		);
		session.diff_pairs.insert(
			after_pane_id,
			crate::engine::session::DiffPair {
				partner_pane_id: before_pane_id,
				role: DiffRole::After,
				blocks: after_blocks,
			},
		);
	}
}

#[derive(Clone, Copy)]
struct NodeCoords {
	start_line: DocLine,
	end_line: DocLine,
	start_byte: DocByte,
	end_byte: DocByte,
}

pub(crate) fn compute_diff(
	left_root: NodeId,
	right_root: NodeId,
	registry: &UastRegistry,
) -> Vec<DiffBlock> {
	let mut blocks = Vec::new();

	let left_bytes = registry
		.read_loaded_slice(
			left_root,
			DocByte::ZERO,
			DocByte::new(registry.get_total_bytes(left_root)),
		)
		.unwrap_or_default();
	let right_bytes = registry
		.read_loaded_slice(
			right_root,
			DocByte::ZERO,
			DocByte::new(registry.get_total_bytes(right_root)),
		)
		.unwrap_or_default();

	// Git snapshots commonly differ in registry topology before and after their
	// asynchronous semantic graft even when their bytes are identical. Avoid
	// entering the structural matcher at all in that case: discovering equality
	// after the all-pairs node walk is both redundant and extremely expensive in
	// debug builds.
	if left_bytes == right_bytes {
		return blocks;
	}

	if left_bytes.is_empty() || right_bytes.is_empty() {
		return blocks;
	}

	let l_index = LineIndex::new(&left_bytes);
	let r_index = LineIndex::new(&right_bytes);

	// 1. Extract nodes and pre-compute coordinates
	let mut l_tokens = Vec::new();
	let mut l_rows = Vec::new();
	let mut l_tables = Vec::new();
	let mut l_groups = Vec::new();
	let mut l_coords = HashMap::new();
	extract_nodes_with_coords(
		registry,
		left_root,
		&mut l_tokens,
		&mut l_rows,
		&mut l_tables,
		&mut l_groups,
		&mut l_coords,
		&l_index,
	);

	let mut r_tokens = Vec::new();
	let mut r_rows = Vec::new();
	let mut r_tables = Vec::new();
	let mut r_groups = Vec::new();
	let mut r_coords = HashMap::new();
	extract_nodes_with_coords(
		registry,
		right_root,
		&mut r_tokens,
		&mut r_rows,
		&mut r_tables,
		&mut r_groups,
		&mut r_coords,
		&r_index,
	);

	let mut l_matches = HashMap::new();
	let mut r_matches = HashMap::new();

	// Phase 1: Leaf matching. Some providers (notably CSV) use semantic rows
	// directly as terminal nodes, so structural matching cannot be seeded from
	// `SemanticKind::Token` nodes alone.
	let l_match_leaves = get_leaves(registry, left_root);
	let r_match_leaves = get_leaves(registry, right_root);
	let mut l_by_text: HashMap<&[u8], Vec<NodeId>> = HashMap::new();
	for &l in &l_match_leaves {
		let coords = &l_coords[&l];
		let start = coords.start_byte.get() as usize;
		let end = coords.end_byte.get() as usize;
		if start < left_bytes.len() && end <= left_bytes.len() {
			l_by_text
				.entry(&left_bytes[start..end])
				.or_default()
				.push(l);
		}
	}
	let mut r_by_text: HashMap<&[u8], Vec<NodeId>> = HashMap::new();
	for &r in &r_match_leaves {
		let coords = &r_coords[&r];
		let start = coords.start_byte.get() as usize;
		let end = coords.end_byte.get() as usize;
		if start < right_bytes.len() && end <= right_bytes.len() {
			r_by_text
				.entry(&right_bytes[start..end])
				.or_default()
				.push(r);
		}
	}

	for (text, l_list) in &l_by_text {
		if l_list.len() == 1
			&& let Some(r_list) = r_by_text.get(text)
			&& r_list.len() == 1
		{
			l_matches.insert(l_list[0], r_list[0]);
			r_matches.insert(r_list[0], l_list[0]);
		}
	}
	// Greedy ambiguous resolve
	for (text, l_list) in &l_by_text {
		if let Some(r_list) = r_by_text.get(text) {
			let mut r_idx = 0;
			for &l in l_list {
				if l_matches.contains_key(&l) {
					continue;
				}
				while r_idx < r_list.len() && r_matches.contains_key(&r_list[r_idx]) {
					r_idx += 1;
				}
				if r_idx < r_list.len() {
					l_matches.insert(l, r_list[r_idx]);
					r_matches.insert(r_list[r_idx], l);
					r_idx += 1;
				}
			}
		}
	}

	// Phase 2: Bottom-Up structural matching
	let mut l_leaves_map = HashMap::new();
	for &n in l_tables.iter().chain(l_rows.iter()).chain(l_groups.iter()) {
		l_leaves_map.insert(n, get_leaves(registry, n));
	}
	let mut r_leaves_map = HashMap::new();
	for &n in r_tables.iter().chain(r_rows.iter()).chain(r_groups.iter()) {
		r_leaves_map.insert(n, get_leaves(registry, n));
	}
	match_topology_groups(
		&l_groups,
		&r_groups,
		&l_leaves_map,
		&r_leaves_map,
		&mut l_matches,
		&mut r_matches,
	);

	let mut match_structural = |l_nodes: &[NodeId], r_nodes: &[NodeId]| {
		for &l in l_nodes {
			if l_matches.contains_key(&l) {
				continue;
			}
			let l_leaves = &l_leaves_map[&l];
			if l_leaves.is_empty() {
				continue;
			}

			let mut best_r = None;
			let mut best_score = 0.0;

			for &r in r_nodes {
				if r_matches.contains_key(&r) {
					continue;
				}
				let r_leaves = &r_leaves_map[&r];
				if r_leaves.is_empty() {
					continue;
				}

				let mut shared = 0;
				for &ll in l_leaves {
					if let Some(&rr) = l_matches.get(&ll)
						&& r_leaves.contains(&rr)
					{
						shared += 1;
					}
				}

				let score = (shared as f32 * 2.0) / ((l_leaves.len() + r_leaves.len()) as f32);
				if score >= 0.1 && score > best_score {
					best_score = score;
					best_r = Some(r);
				}
			}

			if let Some(r) = best_r {
				l_matches.insert(l, r);
				r_matches.insert(r, l);
			}
		}
	};

	match_structural(&l_rows, &r_rows);
	match_structural(&l_tables, &r_tables);

	// Ensure roots are matched
	l_matches.insert(left_root, right_root);
	r_matches.insert(right_root, left_root);

	// Phase 3: Block Generation
	generate_blocks(
		registry,
		left_root,
		right_root,
		&l_matches,
		&r_matches,
		&l_coords,
		&r_coords,
		&left_bytes,
		&right_bytes,
		&mut blocks,
		true,
	);

	// Combine contiguous blocks of the same kind to reduce UI noise
	let mut combined_blocks: Vec<DiffBlock> = Vec::new();
	for block in &blocks {
		let block = block.clone();
		if let Some(last) = combined_blocks.last_mut()
			&& last.kind == block.kind
		{
			let l_overlap = last.range.1 >= block.range.0;
			let r_overlap = last.partner_range.1 >= block.partner_range.0;
			if l_overlap && r_overlap {
				last.range.1 = last.range.1.max(block.range.1);
				last.partner_range.1 = last.partner_range.1.max(block.partner_range.1);
				continue;
			}
		}
		combined_blocks.push(block);
	}

	// Filter out 0-length "Modified" blocks
	combined_blocks.retain(|b| {
		!(b.kind == DiffKind::Modified
			&& b.range.0 == b.range.1
			&& b.partner_range.0 == b.partner_range.1)
	});

	// Semantic topology proposes correspondences, but exact text alignment
	// owns presentation coverage. An unmatched parent can span byte-identical
	// children (for example a rewritten Rust item's unchanged declaration);
	// painting the parent's structural range would falsely mark those lines.
	// Fence the final blocks through a whole-document histogram diff, retaining
	// only exact semantic move candidates that line alignment also recognizes
	// as a deletion paired with an insertion.
	if std::str::from_utf8(&left_bytes).is_ok() && std::str::from_utf8(&right_bytes).is_ok() {
		let semantic_moves = combined_blocks
			.iter()
			.filter(|block| block.kind == DiffKind::Moved)
			.cloned()
			.collect::<Vec<_>>();
		combined_blocks.clear();
		run_line_diff(
			&left_bytes,
			&right_bytes,
			DocLine::ZERO,
			DocLine::ZERO,
			&mut combined_blocks,
		);
		promote_exact_semantic_moves(&mut combined_blocks, &semantic_moves);
	}

	// Reclassify Modified blocks where one side is pure whitespace — a blank
	// line paired against a line of real content is really an insertion or
	// deletion, and painting it blue/Modified misleads the reader.
	reclassify_whitespace_modified_blocks(
		&mut combined_blocks,
		&left_bytes,
		&right_bytes,
		&l_index,
		&r_index,
	);

	// Per-line splitter for equal-count Modified blocks: if individual line
	// pairs inside the block have whitespace on one side and content on the
	// other, break them out as Added/Removed sub-blocks.
	split_modified_whitespace_lines(
		&mut combined_blocks,
		&left_bytes,
		&right_bytes,
		&l_index,
		&r_index,
	);

	// Line-level move detection: for any line whose normalized text occurs
	// exactly once on each side (and that both occurrences are inside some
	// diff block), extract the pair as a Moved block and shrink the source
	// blocks from their boundaries so the moved content doesn't keep its old
	// Added/Removed/Modified classification.
	detect_line_level_moves(
		&mut combined_blocks,
		&left_bytes,
		&right_bytes,
		&l_index,
		&r_index,
	);

	// Layer 3: Super-targeted precision — refine Modified blocks with
	// token-level intra-line diffs where the geometry permits a stable
	// line-by-line mapping. Falls back to None when the heuristic
	// considers the semantic match too chaotic.
	let l_tokens_by_line = build_tokens_by_line(&l_tokens, &l_coords, &l_index);
	let r_tokens_by_line = build_tokens_by_line(&r_tokens, &r_coords, &r_index);
	for block in &mut combined_blocks {
		if block.kind != DiffKind::Modified {
			continue;
		}
		block.intra_line_diffs = compute_intra_line_diffs_for_block(
			block,
			&left_bytes,
			&right_bytes,
			&l_index,
			&r_index,
			&l_tokens_by_line,
			&r_tokens_by_line,
		);
	}

	combined_blocks
}

struct LineIndex {
	line_starts: Vec<usize>,
}

impl LineIndex {
	fn new(bytes: &[u8]) -> Self {
		let mut line_starts = vec![0];
		for (i, &b) in bytes.iter().enumerate() {
			if b == b'\n' {
				line_starts.push(i + 1);
			}
		}
		Self { line_starts }
	}

	fn line_for_byte(&self, byte: usize) -> u64 {
		match self.line_starts.binary_search(&byte) {
			Ok(line) => line as u64,
			Err(line) => line.saturating_sub(1) as u64,
		}
	}

	fn line_start(&self, line: u64) -> usize {
		self.line_starts
			.get(usize::try_from(line).unwrap_or(usize::MAX))
			.copied()
			.unwrap_or_else(|| self.line_starts.last().copied().unwrap_or(0))
	}

	fn line_slice<'b>(&self, bytes: &'b [u8], line: u64) -> &'b [u8] {
		let start = self.line_start(line);
		let end = self
			.line_starts
			.get(
				usize::try_from(line)
					.unwrap_or(usize::MAX)
					.saturating_add(1),
			)
			.copied()
			.unwrap_or(bytes.len());
		let end = end.min(bytes.len()).max(start);
		let trimmed_end = if end > start && bytes.get(end - 1) == Some(&b'\n') {
			end - 1
		} else {
			end
		};
		&bytes[start..trimmed_end]
	}
}

fn extract_nodes_with_coords(
	registry: &UastRegistry,
	root: NodeId,
	tokens: &mut Vec<NodeId>,
	rows: &mut Vec<NodeId>,
	tables: &mut Vec<NodeId>,
	groups: &mut Vec<NodeId>,
	coords_map: &mut HashMap<NodeId, NodeCoords>,
	line_index: &LineIndex,
) {
	let mut stack = vec![(root, DocByte::ZERO)];
	while let Some((node, start_byte)) = stack.pop() {
		#[cfg(test)]
		STRUCTURAL_DIFF_NODE_VISITS.with(|visits| visits.set(visits.get() + 1));

		let metrics = unsafe { *registry.metrics[node.index()].get() };
		let kind = unsafe { *registry.kinds[node.index()].get() };

		let end_byte = DocByte::new(start_byte.get() + metrics.byte_length);
		let start_line = line_index.line_for_byte(start_byte.get() as usize);
		let mut end_line = line_index.line_for_byte(end_byte.get() as usize);

		// Ensure nodes cover their partial lines. If a node ends exactly at a newline
		// that starts a NEW line, we don't include that new line.
		if end_byte.get() > start_byte.get() {
			let ends_exactly_at_newline_boundary = line_index
				.line_starts
				.binary_search(&(end_byte.get() as usize))
				.is_ok();
			if !ends_exactly_at_newline_boundary {
				end_line += 1;
			}
		}

		coords_map.insert(
			node,
			NodeCoords {
				start_line: DocLine::new(start_line),
				end_line: DocLine::new(end_line),
				start_byte,
				end_byte,
			},
		);

		match kind {
			SemanticKind::RelationalTable => tables.push(node),
			SemanticKind::RelationalRow => rows.push(node),
			SemanticKind::None => groups.push(node),
			SemanticKind::Token => tokens.push(node),
		}

		let mut child_start_byte = start_byte;
		let mut children = Vec::new();
		let mut child = registry.get_first_child(node);
		while let Some(c) = child {
			children.push((c, child_start_byte));
			let c_metrics = unsafe { *registry.metrics[c.index()].get() };
			child_start_byte = DocByte::new(child_start_byte.get() + c_metrics.byte_length);
			child = registry.get_next_sibling(c);
		}
		stack.extend(children.into_iter().rev());
	}
	tables.reverse();
	rows.reverse();
	tokens.reverse();
	groups.reverse();
}

fn get_leaves(registry: &UastRegistry, root: NodeId) -> Vec<NodeId> {
	let mut leaves = Vec::new();
	let mut stack = vec![root];
	while let Some(n) = stack.pop() {
		let mut has_child = false;
		let mut child = registry.get_first_child(n);
		let mut children = Vec::new();
		while let Some(c) = child {
			has_child = true;
			children.push(c);
			child = registry.get_next_sibling(c);
		}
		if !has_child {
			leaves.push(n);
		} else {
			stack.extend(children.into_iter().rev());
		}
	}
	leaves
}

/// Match topology-only groups through the terminal-node matches they contain.
/// The reverse extraction order visits inner groups before their parents. An
/// index from right-side leaves to containing groups avoids comparing every
/// left group with every right group on wide CSV grafts.
fn match_topology_groups(
	l_groups: &[NodeId],
	r_groups: &[NodeId],
	l_leaves: &HashMap<NodeId, Vec<NodeId>>,
	r_leaves: &HashMap<NodeId, Vec<NodeId>>,
	l_matches: &mut HashMap<NodeId, NodeId>,
	r_matches: &mut HashMap<NodeId, NodeId>,
) {
	let mut r_groups_by_leaf: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
	let mut r_order = HashMap::new();
	for (order, &group) in r_groups.iter().enumerate() {
		r_order.insert(group, order);
		for &leaf in &r_leaves[&group] {
			r_groups_by_leaf.entry(leaf).or_default().push(group);
		}
	}

	for &l_group in l_groups {
		if l_matches.contains_key(&l_group) {
			continue;
		}
		let l_group_leaves = &l_leaves[&l_group];
		if l_group_leaves.is_empty() {
			continue;
		}

		let mut shared_by_candidate: HashMap<NodeId, usize> = HashMap::new();
		for l_leaf in l_group_leaves {
			let Some(r_leaf) = l_matches.get(l_leaf) else {
				continue;
			};
			let Some(containing_groups) = r_groups_by_leaf.get(r_leaf) else {
				continue;
			};
			for &candidate in containing_groups {
				if !r_matches.contains_key(&candidate) {
					*shared_by_candidate.entry(candidate).or_default() += 1;
				}
			}
		}

		let mut best: Option<(NodeId, usize, usize)> = None;
		for (candidate, shared) in shared_by_candidate {
			let total_leaves = l_group_leaves.len() + r_leaves[&candidate].len();
			if shared.saturating_mul(20) < total_leaves {
				continue;
			}
			let order = r_order[&candidate];
			let replace = best.is_none_or(|(best_candidate, best_shared, best_order)| {
				let best_total = l_group_leaves.len() + r_leaves[&best_candidate].len();
				shared.saturating_mul(best_total) > best_shared.saturating_mul(total_leaves)
					|| (shared.saturating_mul(best_total)
						== best_shared.saturating_mul(total_leaves)
						&& order < best_order)
			});
			if replace {
				best = Some((candidate, shared, order));
			}
		}

		if let Some((r_group, _, _)) = best {
			l_matches.insert(l_group, r_group);
			r_matches.insert(r_group, l_group);
		}
	}
}

fn get_children(registry: &UastRegistry, node: NodeId) -> Vec<NodeId> {
	let mut children = Vec::new();
	let mut child = registry.get_first_child(node);
	while let Some(c) = child {
		children.push(c);
		child = registry.get_next_sibling(c);
	}
	children
}

fn generate_blocks(
	registry: &UastRegistry,
	l_node: NodeId,
	r_node: NodeId,
	l_matches: &HashMap<NodeId, NodeId>,
	r_matches: &HashMap<NodeId, NodeId>,
	l_coords: &HashMap<NodeId, NodeCoords>,
	r_coords: &HashMap<NodeId, NodeCoords>,
	left_bytes: &[u8],
	right_bytes: &[u8],
	blocks: &mut Vec<DiffBlock>,
	is_root: bool,
) {
	let lc = &l_coords[&l_node];
	let rc = &r_coords[&r_node];
	let l_start = (lc.start_byte.get() as usize).min(left_bytes.len());
	let l_end = (lc.end_byte.get() as usize).min(left_bytes.len());
	let r_start = (rc.start_byte.get() as usize).min(right_bytes.len());
	let r_end = (rc.end_byte.get() as usize).min(right_bytes.len());
	let l_text = &left_bytes[l_start..l_end];
	let r_text = &right_bytes[r_start..r_end];

	if l_text == r_text {
		return;
	}

	let kind = unsafe { *registry.kinds[l_node.index()].get() };
	if kind == SemanticKind::RelationalRow || kind == SemanticKind::Token {
		blocks.push(DiffBlock {
			range: (lc.start_line, lc.end_line),
			kind: DiffKind::Modified,
			partner_range: (rc.start_line, rc.end_line),
			intra_line_diffs: None,
		});
		return;
	}

	if !is_root && kind == SemanticKind::RelationalTable {
		run_line_diff(l_text, r_text, lc.start_line, rc.start_line, blocks);
		return;
	}

	let l_children = get_children(registry, l_node);
	let r_children = get_children(registry, r_node);

	if l_children.is_empty() && r_children.is_empty() {
		run_line_diff(l_text, r_text, lc.start_line, rc.start_line, blocks);
		return;
	}

	let mut dp = vec![vec![0; r_children.len() + 1]; l_children.len() + 1];
	for i in 0..l_children.len() {
		for j in 0..r_children.len() {
			if l_matches.get(&l_children[i]) == Some(&r_children[j]) {
				dp[i + 1][j + 1] = dp[i][j] + 1;
			} else {
				dp[i + 1][j + 1] = dp[i + 1][j].max(dp[i][j + 1]);
			}
		}
	}

	let mut aligned_l = Vec::new();
	let mut aligned_r = Vec::new();
	let mut i = l_children.len();
	let mut j = r_children.len();
	while i > 0 && j > 0 {
		if l_matches.get(&l_children[i - 1]) == Some(&r_children[j - 1]) {
			aligned_l.push(l_children[i - 1]);
			aligned_r.push(r_children[j - 1]);
			i -= 1;
			j -= 1;
		} else if dp[i - 1][j] > dp[i][j - 1] {
			i -= 1;
		} else {
			j -= 1;
		}
	}
	aligned_l.reverse();
	aligned_r.reverse();

	let mut l_idx = 0;
	let mut r_idx = 0;
	let mut align_idx = 0;

	while l_idx < l_children.len() || r_idx < r_children.len() {
		let in_align = align_idx < aligned_l.len();
		let target_l = if in_align {
			Some(aligned_l[align_idx])
		} else {
			None
		};
		let target_r = if in_align {
			Some(aligned_r[align_idx])
		} else {
			None
		};

		let l_c = l_children.get(l_idx).copied();
		let r_c = r_children.get(r_idx).copied();

		if let (Some(l_node), Some(r_node)) = (l_c, r_c)
			&& Some(l_node) == target_l
			&& Some(r_node) == target_r
		{
			generate_blocks(
				registry,
				l_node,
				r_node,
				l_matches,
				r_matches,
				l_coords,
				r_coords,
				left_bytes,
				right_bytes,
				blocks,
				false,
			);
			l_idx += 1;
			r_idx += 1;
			align_idx += 1;
		} else {
			if let Some(node) = l_c
				&& Some(node) != target_l
			{
				let coords = &l_coords[&node];
				let r_anchor = if let Some(&r) = r_children.get(r_idx) {
					r_coords[&r].start_line
				} else {
					if let Some(r) = r_children.last() {
						r_coords[r].end_line
					} else {
						rc.start_line
					}
				};

				if let Some(&r_match) = l_matches.get(&node) {
					let r_c_match = &r_coords[&r_match];
					let l_start = coords.start_byte.get() as usize;
					let l_end = coords.end_byte.get() as usize;
					let r_start = r_c_match.start_byte.get() as usize;
					let r_end = r_c_match.end_byte.get() as usize;
					let unchanged = left_bytes.get(l_start..l_end).is_some_and(|left| {
						right_bytes
							.get(r_start..r_end)
							.is_some_and(|right| left == right)
					});
					blocks.push(DiffBlock {
						range: (coords.start_line, coords.end_line),
						kind: if unchanged {
							DiffKind::Moved
						} else {
							DiffKind::Modified
						},
						partner_range: (r_c_match.start_line, r_c_match.end_line),
						intra_line_diffs: None,
					});
				} else {
					blocks.push(DiffBlock {
						range: (coords.start_line, coords.end_line),
						kind: DiffKind::Removed,
						partner_range: (r_anchor, r_anchor),
						intra_line_diffs: None,
					});
				}
				l_idx += 1;
			} else if let Some(node) = r_c
				&& Some(node) != target_r
			{
				let coords = &r_coords[&node];
				let l_anchor = if let Some(&l) = l_children.get(l_idx) {
					l_coords[&l].start_line
				} else {
					if let Some(l) = l_children.last() {
						l_coords[l].end_line
					} else {
						lc.start_line
					}
				};

				if !r_matches.contains_key(&node) {
					blocks.push(DiffBlock {
						range: (l_anchor, l_anchor),
						kind: DiffKind::Added,
						partner_range: (coords.start_line, coords.end_line),
						intra_line_diffs: None,
					});
				}
				r_idx += 1;
			}
		}
	}
}

fn run_line_diff(
	l_text: &[u8],
	r_text: &[u8],
	l_start_line: DocLine,
	r_start_line: DocLine,
	blocks: &mut Vec<DiffBlock>,
) {
	use imara_diff::{Algorithm, Diff, InternedInput};

	let l_input = std::str::from_utf8(l_text).unwrap_or("");
	let r_input = std::str::from_utf8(r_text).unwrap_or("");

	let input = InternedInput::new(l_input, r_input);
	let diff = Diff::compute(Algorithm::Histogram, &input);

	for hunk in diff.hunks() {
		let before = hunk.before;
		let after = hunk.after;

		let l_range = (
			DocLine::new(l_start_line.get() + u64::from(before.start)),
			DocLine::new(l_start_line.get() + u64::from(before.end)),
		);
		let r_range = (
			DocLine::new(r_start_line.get() + u64::from(after.start)),
			DocLine::new(r_start_line.get() + u64::from(after.end)),
		);

		if !before.is_empty() && !after.is_empty() {
			blocks.push(DiffBlock {
				range: l_range,
				kind: DiffKind::Modified,
				partner_range: r_range,
				intra_line_diffs: None,
			});
		} else if !before.is_empty() {
			blocks.push(DiffBlock {
				range: l_range,
				kind: DiffKind::Removed,
				partner_range: (r_range.0, r_range.0),
				intra_line_diffs: None,
			});
		} else if !after.is_empty() {
			blocks.push(DiffBlock {
				range: (l_range.0, l_range.0),
				kind: DiffKind::Added,
				partner_range: r_range,
				intra_line_diffs: None,
			});
		}
	}
}

/// Byte ranges of single-line tokens keyed by absolute document line. Tokens
/// that straddle line boundaries are dropped here because they cannot anchor a
/// per-line semantic diff cleanly — Layer 3 only operates inside a single
/// logical line.
fn build_tokens_by_line(
	tokens: &[NodeId],
	coords: &HashMap<NodeId, NodeCoords>,
	index: &LineIndex,
) -> HashMap<u64, Vec<(u32, u32)>> {
	let mut by_line: HashMap<u64, Vec<(u32, u32)>> = HashMap::new();
	for &token in tokens {
		let Some(node_coords) = coords.get(&token) else {
			continue;
		};
		if node_coords.start_line != node_coords.end_line {
			continue;
		}
		if node_coords.end_byte.get() <= node_coords.start_byte.get() {
			continue;
		}
		let line = node_coords.start_line.get();
		let line_start = index.line_start(line) as u64;
		let start = node_coords.start_byte.get().saturating_sub(line_start) as u32;
		let end = node_coords.end_byte.get().saturating_sub(line_start) as u32;
		if end <= start {
			continue;
		}
		by_line.entry(line).or_default().push((start, end));
	}
	for list in by_line.values_mut() {
		list.sort_by_key(|entry| entry.0);
	}
	by_line
}

fn compute_intra_line_diffs_for_block(
	block: &DiffBlock,
	left_bytes: &[u8],
	right_bytes: &[u8],
	l_index: &LineIndex,
	r_index: &LineIndex,
	l_tokens_by_line: &HashMap<u64, Vec<(u32, u32)>>,
	r_tokens_by_line: &HashMap<u64, Vec<(u32, u32)>>,
) -> Option<Vec<IntraLineDiff>> {
	let l_line_count = block.range.1.get().saturating_sub(block.range.0.get());
	let r_line_count = block
		.partner_range
		.1
		.get()
		.saturating_sub(block.partner_range.0.get());
	if l_line_count == 0 || r_line_count == 0 || l_line_count != r_line_count {
		return None;
	}

	let mut collected: Vec<IntraLineDiff> = Vec::new();
	for offset in 0..l_line_count {
		let left_line = DocLine::new(block.range.0.get() + offset);
		let right_line = DocLine::new(block.partner_range.0.get() + offset);
		let l_line_bytes = l_index.line_slice(left_bytes, left_line.get());
		let r_line_bytes = r_index.line_slice(right_bytes, right_line.get());

		let diffs = intra_line_diffs_for_pair(
			left_line,
			right_line,
			l_line_bytes,
			r_line_bytes,
			l_tokens_by_line.get(&left_line.get()).map(|v| v.as_slice()),
			r_tokens_by_line
				.get(&right_line.get())
				.map(|v| v.as_slice()),
		)?;
		collected.extend(diffs);
	}

	if collected.is_empty() {
		None
	} else {
		Some(collected)
	}
}

/// Compute the intra-line diff for a single line pair. Returns `None` to signal
/// the adaptive fallback — either there weren't enough tokens to produce a
/// meaningful semantic map, or the token diff was so chaotic (>= 70% of tokens
/// changed) that we'd rather keep the plain full-line highlight.
fn intra_line_diffs_for_pair(
	left_line: DocLine,
	right_line: DocLine,
	l_line_bytes: &[u8],
	r_line_bytes: &[u8],
	l_uast_tokens: Option<&[(u32, u32)]>,
	r_uast_tokens: Option<&[(u32, u32)]>,
) -> Option<Vec<IntraLineDiff>> {
	// Prefer the UAST token partitions; fall back to a character-class
	// tokenizer if either side doesn't have enough semantic tokens to
	// discriminate changes.
	type TokenRange = (u32, u32);
	let l_owned: Vec<TokenRange>;
	let r_owned: Vec<TokenRange>;
	let (l_tokens, r_tokens): (&[TokenRange], &[TokenRange]) = match (l_uast_tokens, r_uast_tokens)
	{
		(Some(l), Some(r)) if l.len() >= 2 && r.len() >= 2 => (l, r),
		_ => {
			l_owned = character_class_tokens(l_line_bytes);
			r_owned = character_class_tokens(r_line_bytes);
			if l_owned.is_empty() || r_owned.is_empty() {
				return None;
			}
			(l_owned.as_slice(), r_owned.as_slice())
		}
	};

	let l_token_strings: Vec<String> = l_tokens
		.iter()
		.map(|(s, e)| token_signature(l_line_bytes, *s, *e))
		.collect();
	let r_token_strings: Vec<String> = r_tokens
		.iter()
		.map(|(s, e)| token_signature(r_line_bytes, *s, *e))
		.collect();

	let l_joined = l_token_strings.join("\n");
	let r_joined = r_token_strings.join("\n");

	use imara_diff::{Algorithm, Diff, InternedInput};
	let input = InternedInput::new(l_joined.as_str(), r_joined.as_str());
	let diff = Diff::compute(Algorithm::Histogram, &input);

	let mut changed_left: u32 = 0;
	let mut changed_right: u32 = 0;
	let mut result: Vec<IntraLineDiff> = Vec::new();
	for hunk in diff.hunks() {
		let before = hunk.before;
		let after = hunk.after;
		changed_left = changed_left.saturating_add(before.end.saturating_sub(before.start));
		changed_right = changed_right.saturating_add(after.end.saturating_sub(after.start));

		let (l_byte_start, l_byte_end) =
			byte_span_from_token_range(l_tokens, before.start, before.end);
		let (r_byte_start, r_byte_end) =
			byte_span_from_token_range(r_tokens, after.start, after.end);

		let l_col_start = byte_to_visual_col(l_line_bytes, l_byte_start);
		let l_col_end = byte_to_visual_col(l_line_bytes, l_byte_end);
		let r_col_start = byte_to_visual_col(r_line_bytes, r_byte_start);
		let r_col_end = byte_to_visual_col(r_line_bytes, r_byte_end);

		let kind = if before.is_empty() && !after.is_empty() {
			IntraLineDiffKind::Added
		} else if after.is_empty() && !before.is_empty() {
			IntraLineDiffKind::Removed
		} else {
			IntraLineDiffKind::Modified
		};

		result.push(IntraLineDiff {
			line: left_line,
			col_start: l_col_start,
			col_end: l_col_end,
			partner_line: right_line,
			partner_col_start: r_col_start,
			partner_col_end: r_col_end,
			kind,
		});
	}

	// Adaptive fallback: if the token diff churned through most of the line
	// on either side, the semantic signal is too weak to be useful and we
	// defer back to the full-line block highlight.
	let l_total = l_tokens.len() as u32;
	let r_total = r_tokens.len() as u32;
	let l_ratio = if l_total == 0 {
		0.0
	} else {
		changed_left as f32 / l_total as f32
	};
	let r_ratio = if r_total == 0 {
		0.0
	} else {
		changed_right as f32 / r_total as f32
	};
	if l_ratio >= 0.7 && r_ratio >= 0.7 {
		return None;
	}

	if result.is_empty() {
		None
	} else {
		Some(result)
	}
}

fn byte_span_from_token_range(tokens: &[(u32, u32)], start: u32, end: u32) -> (u32, u32) {
	if start == end {
		// Zero-length hunk → anchor at the boundary between neighboring
		// tokens so pure insertions/deletions collapse to a single column.
		let anchor = if (start as usize) < tokens.len() {
			tokens[start as usize].0
		} else if let Some(last) = tokens.last() {
			last.1
		} else {
			0
		};
		return (anchor, anchor);
	}
	let start_byte = tokens
		.get(start as usize)
		.map(|(s, _)| *s)
		.unwrap_or_else(|| tokens.last().map(|(_, e)| *e).unwrap_or(0));
	let end_byte = tokens
		.get((end as usize).saturating_sub(1))
		.map(|(_, e)| *e)
		.unwrap_or(start_byte);
	(start_byte, end_byte.max(start_byte))
}

fn byte_to_visual_col(line_bytes: &[u8], byte_offset: u32) -> u64 {
	let tab_size = u64::from(TAB_SIZE);
	let mut col: u64 = 0;
	let limit = (byte_offset as usize).min(line_bytes.len());
	for &b in &line_bytes[..limit] {
		match b {
			b'\t' => col = col.saturating_add(tab_size - (col % tab_size)),
			b'\n' => break,
			_ => col = col.saturating_add(1),
		}
	}
	col
}

/// Split a line into runs with stable classes so imara-diff has something to
/// anchor on when UAST tokens are absent. We group: word characters
/// (ascii alphanumerics + underscore), whitespace runs, and individual
/// punctuation bytes. Non-ASCII bytes are treated as word characters so we
/// don't shatter UTF-8 sequences.
fn character_class_tokens(bytes: &[u8]) -> Vec<(u32, u32)> {
	#[derive(PartialEq, Eq, Copy, Clone)]
	enum Class {
		Word,
		Space,
		Punct,
	}
	fn classify(b: u8) -> Class {
		if b == b' ' || b == b'\t' {
			Class::Space
		} else if b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii() {
			Class::Word
		} else {
			Class::Punct
		}
	}

	let mut out = Vec::new();
	let mut i = 0usize;
	while i < bytes.len() {
		if bytes[i] == b'\n' {
			break;
		}
		let class = classify(bytes[i]);
		let start = i;
		match class {
			Class::Punct => {
				i += 1;
			}
			Class::Word | Class::Space => {
				i += 1;
				while i < bytes.len() && bytes[i] != b'\n' && classify(bytes[i]) == class {
					i += 1;
				}
			}
		}
		out.push((start as u32, i as u32));
	}
	out
}

fn token_signature(bytes: &[u8], start: u32, end: u32) -> String {
	let start = (start as usize).min(bytes.len());
	let end = (end as usize).min(bytes.len()).max(start);
	String::from_utf8_lossy(&bytes[start..end]).replace(['\n', '\r'], " ")
}

/// Flip Modified blocks whose content on one side is exclusively whitespace
/// into the corresponding Added/Removed block. This keeps the diff honest
/// when a blank line is paired with a new comment or code line: the original
/// side had no content to modify, so the user should see it as pure insertion
/// or deletion rather than a modification.
fn reclassify_whitespace_modified_blocks(
	blocks: &mut [DiffBlock],
	left_bytes: &[u8],
	right_bytes: &[u8],
	l_index: &LineIndex,
	r_index: &LineIndex,
) {
	for block in blocks.iter_mut() {
		if block.kind != DiffKind::Modified {
			continue;
		}
		let l_empty = range_is_all_whitespace(left_bytes, l_index, block.range);
		let r_empty = range_is_all_whitespace(right_bytes, r_index, block.partner_range);
		if l_empty && !r_empty {
			block.kind = DiffKind::Added;
			block.range = (block.range.0, block.range.0);
		} else if r_empty && !l_empty {
			block.kind = DiffKind::Removed;
			block.partner_range = (block.partner_range.0, block.partner_range.0);
		}
	}
}

fn range_is_all_whitespace(bytes: &[u8], index: &LineIndex, range: (DocLine, DocLine)) -> bool {
	let start = range.0.get();
	let end = range.1.get();
	if end <= start {
		return false;
	}
	for line in start..end {
		let slice = index.line_slice(bytes, line);
		if !slice.iter().all(|&b| b == b' ' || b == b'\t' || b == b'\r') {
			return false;
		}
	}
	true
}

fn promote_exact_semantic_moves(blocks: &mut Vec<DiffBlock>, candidates: &[DiffBlock]) {
	for candidate in candidates {
		let Some(removed) = blocks
			.iter()
			.position(|block| block.kind == DiffKind::Removed && block.range == candidate.range)
		else {
			continue;
		};
		let Some(added) = blocks.iter().position(|block| {
			block.kind == DiffKind::Added && block.partner_range == candidate.partner_range
		}) else {
			continue;
		};
		let (first, second) = if removed < added {
			(removed, added)
		} else {
			(added, removed)
		};
		blocks.remove(second);
		blocks.remove(first);
		blocks.push(candidate.clone());
	}
	blocks.sort_by_key(|block| {
		(
			block.range.0.get(),
			block.partner_range.0.get(),
			matches!(block.kind, DiffKind::Moved),
		)
	});
}

/// Detect moves at the line level. For every line whose normalized content
/// appears exactly once in a Removed range and exactly once in an Added
/// range, emit a dedicated `Moved` block and shrink those source blocks.
/// Merely changing absolute line number inside a Modified region is not a
/// move: ordinary insertions shift every following line.
fn detect_line_level_moves(
	blocks: &mut Vec<DiffBlock>,
	left_bytes: &[u8],
	right_bytes: &[u8],
	l_index: &LineIndex,
	r_index: &LineIndex,
) {
	let mut left_positions: HashMap<String, Vec<u64>> = HashMap::new();
	let mut right_positions: HashMap<String, Vec<u64>> = HashMap::new();
	for block in blocks.iter() {
		if block.kind == DiffKind::Removed {
			for line in block.range.0.get()..block.range.1.get() {
				if let Some(content) = line_content_normalized(left_bytes, l_index, line) {
					left_positions.entry(content).or_default().push(line);
				}
			}
		}
		if block.kind == DiffKind::Added {
			for line in block.partner_range.0.get()..block.partner_range.1.get() {
				if let Some(content) = line_content_normalized(right_bytes, r_index, line) {
					right_positions.entry(content).or_default().push(line);
				}
			}
		}
	}

	let mut left_to_right: HashMap<u64, u64> = HashMap::new();
	let mut right_to_left: HashMap<u64, u64> = HashMap::new();
	for (content, left_lines) in &left_positions {
		if left_lines.len() != 1 {
			continue;
		}
		let Some(right_lines) = right_positions.get(content) else {
			continue;
		};
		if right_lines.len() != 1 {
			continue;
		}
		let l = left_lines[0];
		let r = right_lines[0];
		if l == r {
			continue;
		}
		left_to_right.insert(l, r);
		right_to_left.insert(r, l);
	}

	if left_to_right.is_empty() {
		return;
	}

	let mut next_blocks: Vec<DiffBlock> = Vec::new();
	for block in blocks.drain(..) {
		let mut l_start = block.range.0.get();
		let mut l_end = block.range.1.get();
		while l_start < l_end && left_to_right.contains_key(&l_start) {
			l_start += 1;
		}
		while l_start < l_end && left_to_right.contains_key(&(l_end - 1)) {
			l_end -= 1;
		}
		let mut r_start = block.partner_range.0.get();
		let mut r_end = block.partner_range.1.get();
		while r_start < r_end && right_to_left.contains_key(&r_start) {
			r_start += 1;
		}
		while r_start < r_end && right_to_left.contains_key(&(r_end - 1)) {
			r_end -= 1;
		}

		let shrunk = l_start != block.range.0.get()
			|| l_end != block.range.1.get()
			|| r_start != block.partner_range.0.get()
			|| r_end != block.partner_range.1.get();
		if !shrunk {
			next_blocks.push(block);
			continue;
		}

		let left_empty = l_start == l_end;
		let right_empty = r_start == r_end;
		if left_empty && right_empty {
			continue;
		}
		let new_kind = if left_empty {
			DiffKind::Added
		} else if right_empty {
			DiffKind::Removed
		} else {
			DiffKind::Modified
		};
		next_blocks.push(DiffBlock {
			range: (DocLine::new(l_start), DocLine::new(l_end)),
			kind: new_kind,
			partner_range: (DocLine::new(r_start), DocLine::new(r_end)),
			intra_line_diffs: None,
		});
	}

	for (l, r) in &left_to_right {
		next_blocks.push(DiffBlock {
			range: (DocLine::new(*l), DocLine::new(*l + 1)),
			kind: DiffKind::Moved,
			partner_range: (DocLine::new(*r), DocLine::new(*r + 1)),
			intra_line_diffs: None,
		});
	}

	next_blocks.sort_by(|a, b| {
		let a_key = (a.range.0.get(), matches!(a.kind, DiffKind::Moved));
		let b_key = (b.range.0.get(), matches!(b.kind, DiffKind::Moved));
		a_key.cmp(&b_key)
	});

	*blocks = next_blocks;
}

fn line_content_normalized(bytes: &[u8], index: &LineIndex, line: u64) -> Option<String> {
	let slice = index.line_slice(bytes, line);
	let text = std::str::from_utf8(slice).ok()?;
	let trimmed = text.trim_end_matches([' ', '\t', '\r']);
	if trimmed.is_empty() {
		return None;
	}
	Some(trimmed.to_string())
}

fn line_is_whitespace(bytes: &[u8], index: &LineIndex, line: u64) -> bool {
	let slice = index.line_slice(bytes, line);
	slice.iter().all(|&b| b == b' ' || b == b'\t' || b == b'\r')
}

/// Walk each Modified block with equal left/right line counts and split it into
/// sub-blocks along runs of line pairs that classify the same way. A pair where
/// only the left side is whitespace becomes `Added` (right gained content); a
/// pair where only the right is whitespace becomes `Removed`; otherwise the
/// pair stays `Modified`. This catches the common case where a new comment is
/// inserted next to existing lines that imara-diff merged into one big
/// Modified hunk — painting just that line green reads far more clearly than
/// painting the whole span blue.
fn split_modified_whitespace_lines(
	blocks: &mut Vec<DiffBlock>,
	left_bytes: &[u8],
	right_bytes: &[u8],
	l_index: &LineIndex,
	r_index: &LineIndex,
) {
	#[derive(Eq, PartialEq, Copy, Clone)]
	enum PairKind {
		Added,
		Removed,
		Modified,
	}

	let mut next: Vec<DiffBlock> = Vec::with_capacity(blocks.len());
	for block in blocks.drain(..) {
		let l_count = block.range.1.get().saturating_sub(block.range.0.get());
		let r_count = block
			.partner_range
			.1
			.get()
			.saturating_sub(block.partner_range.0.get());
		if block.kind != DiffKind::Modified || l_count == 0 || l_count != r_count {
			next.push(block);
			continue;
		}

		let mut classes = Vec::with_capacity(l_count as usize);
		for i in 0..l_count {
			let l_line = block.range.0.get() + i;
			let r_line = block.partner_range.0.get() + i;
			let l_ws = line_is_whitespace(left_bytes, l_index, l_line);
			let r_ws = line_is_whitespace(right_bytes, r_index, r_line);
			let kind = match (l_ws, r_ws) {
				(true, false) => PairKind::Added,
				(false, true) => PairKind::Removed,
				_ => PairKind::Modified,
			};
			classes.push(kind);
		}

		if classes.iter().all(|k| *k == PairKind::Modified) {
			next.push(block);
			continue;
		}

		let mut run_start = 0usize;
		while run_start < classes.len() {
			let kind = classes[run_start];
			let mut run_end = run_start + 1;
			while run_end < classes.len() && classes[run_end] == kind {
				run_end += 1;
			}
			let l_start = block.range.0.get() + run_start as u64;
			let l_end = block.range.0.get() + run_end as u64;
			let r_start = block.partner_range.0.get() + run_start as u64;
			let r_end = block.partner_range.0.get() + run_end as u64;
			let (new_kind, l_range, r_range) = match kind {
				PairKind::Added => (
					DiffKind::Added,
					(DocLine::new(l_start), DocLine::new(l_start)),
					(DocLine::new(r_start), DocLine::new(r_end)),
				),
				PairKind::Removed => (
					DiffKind::Removed,
					(DocLine::new(l_start), DocLine::new(l_end)),
					(DocLine::new(r_start), DocLine::new(r_start)),
				),
				PairKind::Modified => (
					DiffKind::Modified,
					(DocLine::new(l_start), DocLine::new(l_end)),
					(DocLine::new(r_start), DocLine::new(r_end)),
				),
			};
			next.push(DiffBlock {
				range: l_range,
				kind: new_kind,
				partner_range: r_range,
				intra_line_diffs: None,
			});
			run_start = run_end;
		}
	}
	*blocks = next;
}

#[cfg(test)]
mod tests {
	use super::{STRUCTURAL_DIFF_NODE_VISITS, canonical_pair_order, compute_diff};
	use crate::core::PaneId;
	use crate::ecs::{NodeId, UastRegistry};
	use crate::engine::language::{GreenNode, LanguageProvider};
	use crate::engine::session::{DiffKind, DiffRole, IntraLineDiff, IntraLineDiffKind};
	use crate::engine::structural_graft::apply_graft;
	use crate::svp::rust_provider::RustProvider;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;

	#[test]
	fn chronology_is_independent_of_pane_id_order() {
		let low = PaneId::new(1);
		let high = PaneId::new(2);
		assert_eq!(
			canonical_pair_order(low, high, DiffRole::Before),
			(low, high)
		);
		assert_eq!(
			canonical_pair_order(low, high, DiffRole::After),
			(high, low)
		);
	}

	#[test]
	fn mirrored_intra_line_insertions_keep_their_canonical_kind() {
		let insertion = IntraLineDiff {
			line: crate::core::DocLine::new(4),
			col_start: 3,
			col_end: 3,
			partner_line: crate::core::DocLine::new(5),
			partner_col_start: 3,
			partner_col_end: 9,
			kind: IntraLineDiffKind::Added,
		};
		let mirrored = insertion.swap_sides();
		assert_eq!(mirrored.kind, IntraLineDiffKind::Added);
		assert_eq!((mirrored.col_start, mirrored.col_end), (3, 9));
		assert_eq!(
			(mirrored.partner_col_start, mirrored.partner_col_end),
			(3, 3)
		);
	}

	fn green_lines(source: &[u8]) -> GreenNode {
		let children = source
			.split_inclusive(|&byte| byte == b'\n')
			.map(|line| GreenNode {
				kind: SemanticKind::RelationalRow,
				provider_kind: crate::uast::ProviderKindId::NONE,
				byte_len: line.len() as u32,
				children: Vec::new(),
			})
			.collect();
		GreenNode {
			kind: SemanticKind::RelationalTable,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: source.len() as u32,
			children,
		}
	}

	fn graft_diff_side(registry: &UastRegistry, source: &[u8]) -> NodeId {
		graft_diff_side_with_green(registry, source, &green_lines(source))
	}

	fn graft_diff_side_with_green(
		registry: &UastRegistry,
		source: &[u8],
		green: &GreenNode,
	) -> NodeId {
		let root = registry.alloc_test_node();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: source.iter().filter(|&&byte| byte == b'\n').count() as u64,
			};
		}
		apply_graft(registry, root, green, source).expect("test graft is byte-identical");
		root
	}

	#[test]
	fn identical_documents_never_enter_the_structural_matcher() {
		let source = (0..512)
			.map(|line| format!("fn item_{line:03}() {{ shared(); }}\n"))
			.collect::<String>()
			.into_bytes();
		let registry = UastRegistry::new(2_048);
		let left_root = graft_diff_side(&registry, &source);
		let right_root = graft_diff_side(&registry, &source);

		STRUCTURAL_DIFF_NODE_VISITS.with(|visits| visits.set(0));
		let blocks = compute_diff(left_root, right_root, &registry);
		let visits = STRUCTURAL_DIFF_NODE_VISITS.with(std::cell::Cell::get);

		assert!(blocks.is_empty());
		assert_eq!(
			visits, 0,
			"byte-identical documents must return before structural extraction"
		);
	}

	#[test]
	fn topology_groups_align_without_turning_a_local_change_into_bucket_churn() {
		const LINES: usize = 130;
		const CHANGED_LINE: u64 = 70;
		let left = (0..LINES)
			.map(|line| format!("fn item_{line:03}() {{ alpha(); }}\n"))
			.collect::<String>()
			.into_bytes();
		let right = (0..LINES)
			.map(|line| {
				if line as u64 == CHANGED_LINE {
					format!("fn item_{line:03}() {{ beta(); }}\n")
				} else {
					format!("fn item_{line:03}() {{ alpha(); }}\n")
				}
			})
			.collect::<String>()
			.into_bytes();
		let registry = UastRegistry::new(2_048);
		let left_root = graft_diff_side(&registry, &left);
		let right_root = graft_diff_side(&registry, &right);

		let blocks = compute_diff(left_root, right_root, &registry);

		assert!(!blocks.is_empty(), "the changed line must produce a diff");
		assert!(
			blocks.iter().any(|block| {
				(block.range.0.get() <= CHANGED_LINE && block.range.1.get() > CHANGED_LINE)
					|| (block.partner_range.0.get() <= CHANGED_LINE
						&& block.partner_range.1.get() > CHANGED_LINE)
			}),
			"the diff must retain the changed line: {blocks:?}"
		);
		assert!(
			blocks.iter().all(|block| {
				block.range.1.get().saturating_sub(block.range.0.get()) <= 1
					&& block
						.partner_range
						.1
						.get()
						.saturating_sub(block.partner_range.0.get())
						<= 1
			}),
			"a local edit must not become a 64-line fan-out removal/addition: {blocks:?}"
		);
	}

	#[test]
	fn expanded_rewrite_exposes_additions_without_calling_shifted_context_moved() {
		let before = b"use std::num::NonZeroU32;\n\
\n\
/// CORE ENTITY\n\
/// A lightweight 32-bit integer.\n\
pub struct NodeId(pub NonZeroU32);\n\
\n\
impl NodeId {\n\
\tpub fn index(self) -> usize {\n\
\t\tself.0.get() as usize - 1\n\
\t}\n\
}\n";
		let after = b"use std::num::NonZeroU32;\n\
\n\
pub(crate) const NODE_SLOT_BITS: u32 = 24;\n\
pub(crate) const NODE_SLOT_MASK: u32 = (1 << NODE_SLOT_BITS) - 1;\n\
\n\
/// CORE ENTITY\n\
/// A generation-qualified registry entity identity.\n\
pub struct NodeId(pub NonZeroU32);\n\
\n\
impl NodeId {\n\
\tpub fn index(self) -> usize {\n\
\t\t((self.0.get() & NODE_SLOT_MASK) - 1) as usize\n\
\t}\n\
\n\
\tpub(crate) fn generation(self) -> u8 {\n\
\t\t(self.0.get() >> NODE_SLOT_BITS) as u8\n\
\t}\n\
}\n";
		let registry = UastRegistry::new(1_024);
		let provider = RustProvider::new();
		let before_green = provider
			.parse_structural_topology(before)
			.expect("before Rust topology");
		let after_green = provider
			.parse_structural_topology(after)
			.expect("after Rust topology");
		let before_root = graft_diff_side_with_green(&registry, before, &before_green);
		let after_root = graft_diff_side_with_green(&registry, after, &after_green);

		let blocks = compute_diff(before_root, after_root, &registry);

		assert!(
			blocks.iter().any(|block| block.kind == DiffKind::Added),
			"the expanded after-side rewrite must expose added lines: {blocks:?}"
		);
		assert!(
			blocks.iter().all(|block| block.kind != DiffKind::Moved),
			"context shifted by preceding additions is not a move: {blocks:?}"
		);
		assert!(
			blocks.iter().all(|block| {
				block.kind != DiffKind::Modified
					|| block
						.partner_range
						.1
						.get()
						.saturating_sub(block.partner_range.0.get())
						< 10
			}),
			"a structural match must not paint the expanded rewrite as one blue slab: {blocks:?}"
		);
	}

	#[test]
	fn unchanged_subtree_reordered_across_an_anchor_is_moved() {
		let before = b"fn first() { alpha(); }\nfn second() { beta(); }\n";
		let after = b"fn second() { beta(); }\nfn first() { alpha(); }\n";
		let registry = UastRegistry::new(512);
		let provider = RustProvider::new();
		let before_green = provider
			.parse_structural_topology(before)
			.expect("before Rust topology");
		let after_green = provider
			.parse_structural_topology(after)
			.expect("after Rust topology");
		let before_root = graft_diff_side_with_green(&registry, before, &before_green);
		let after_root = graft_diff_side_with_green(&registry, after, &after_green);

		let blocks = compute_diff(before_root, after_root, &registry);

		assert!(
			blocks.iter().any(|block| block.kind == DiffKind::Moved),
			"an unchanged reordered function should retain a purple move: {blocks:?}"
		);
	}

	#[test]
	fn unchanged_node_id_declaration_is_not_colored_by_its_rewritten_parent() {
		let before = b"use std::num::NonZeroU32;\n\
\n\
/// ==========================================\n\
/// CORE ENTITY\n\
/// ==========================================\n\
/// The Entity ID. A lightweight 32-bit integer.\n\
/// We use NonZeroU32 to allow `Option<NodeId>` to fit cleanly into 4 bytes (Null-pointer optimization).\n\
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n\
pub struct NodeId(pub NonZeroU32);\n\
\n\
impl NodeId {\n\
\t#[inline(always)]\n\
\tpub fn index(self) -> usize {\n\
\t\tself.0.get() as usize - 1\n\
\t}\n\
\n\
\tpub fn from_index(idx: usize) -> Self {\n\
\t\tSelf(NonZeroU32::new(idx as u32 + 1).unwrap())\n\
\t}\n\
}\n";
		let after = include_bytes!("../ecs/id.rs");
		let registry = UastRegistry::new(2_048);
		let provider = RustProvider::new();
		let before_green = provider
			.parse_structural_topology(before)
			.expect("before Rust topology");
		let after_green = provider
			.parse_structural_topology(after)
			.expect("after Rust topology");
		let before_root = graft_diff_side_with_green(&registry, before, &before_green);
		let after_root = graft_diff_side_with_green(&registry, after, &after_green);

		let blocks = compute_diff(before_root, after_root, &registry);
		let before_declaration = 8;
		let after_declaration = 15;
		assert!(
			blocks.iter().all(|block| {
				!(block.range.0.get() <= before_declaration
					&& before_declaration < block.range.1.get())
					&& !(block.partner_range.0.get() <= after_declaration
						&& after_declaration < block.partner_range.1.get())
			}),
			"the byte-identical declaration is aligned context, not removed or added: {blocks:#?}"
		);
	}
}
