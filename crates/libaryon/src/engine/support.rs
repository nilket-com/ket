use super::index::SumTree;
use crate::core::{DocByte, DocDisplayCol, DocLine, DocLineCount, TAB_SIZE};
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::UastProjection;
use ra_ap_syntax::{Direction, Edition, SyntaxKind, SyntaxToken, TextSize};

pub(crate) fn line_byte_range(
	doc: &[u8],
	start_line: DocLine,
	end_line: DocLine,
) -> (usize, usize) {
	let mut current_line = DocLine::ZERO;
	let mut byte_start = 0usize;
	let mut found_start = start_line == DocLine::ZERO;

	for (i, &b) in doc.iter().enumerate() {
		if b == b'\n' {
			current_line += DocLineCount::new(1);
			if !found_start && current_line == start_line {
				byte_start = i + 1;
				found_start = true;
			}
			if current_line > end_line {
				return (byte_start, i + 1);
			}
		}
	}
	if !found_start {
		byte_start = doc.len();
	}
	(byte_start, doc.len())
}

pub(crate) fn line_content_slice(doc: &[u8], target_line: DocLine) -> &[u8] {
	let (start, end) = line_byte_range(doc, target_line, target_line);
	let line = &doc[start..end];
	if line.ends_with(b"\n") {
		&line[..line.len().saturating_sub(1)]
	} else {
		line
	}
}

pub(crate) fn advance_visual_col_only(col: &mut DocDisplayCol, b: u8) {
	if b == b'\t' {
		*col = col
			.checked_add(TAB_SIZE - (col.get() % TAB_SIZE))
			.expect("DocDisplayCol addition overflow");
	} else if b != b'\n' {
		*col = col.checked_add(1).expect("DocDisplayCol addition overflow");
	}
}

pub(crate) fn line_end_visual_col(doc: &[u8], target_line: DocLine) -> DocDisplayCol {
	let mut col = DocDisplayCol::ZERO;
	for &b in line_content_slice(doc, target_line) {
		advance_visual_col_only(&mut col, b);
	}
	col
}

pub(crate) fn first_non_whitespace_visual_col(doc: &[u8], target_line: DocLine) -> DocDisplayCol {
	let mut col = DocDisplayCol::ZERO;
	for &b in line_content_slice(doc, target_line) {
		match b {
			b' ' | b'\t' => advance_visual_col_only(&mut col, b),
			_ => return col,
		}
	}

	DocDisplayCol::ZERO
}

pub(crate) fn line_start_byte_sparse(
	registry: &UastRegistry,
	root: NodeId,
	target_line: DocLine,
) -> Result<DocByte, String> {
	let target = registry
		.find_node_at_line_col_raw(root, target_line, DocDisplayCol::ZERO)
		.ready()
		.ok_or_else(|| "Line index is still building".to_string())?;
	Ok(registry.doc_byte_for_node_offset(root, target.node_id, target.node_byte))
}

/// Descend from `node` to its first (leftmost) leaf. If `node` is
/// already a leaf, returns `node` unchanged.
fn first_leaf(registry: &UastRegistry, mut node: NodeId) -> NodeId {
	while let Some(child) = registry.get_first_child(node) {
		node = child;
	}
	node
}

/// Descend from `node` to its last (rightmost) leaf.
fn last_leaf(registry: &UastRegistry, mut node: NodeId) -> NodeId {
	while let Some(child) = unsafe { *registry.child_tails[node.index()].get() } {
		node = child;
	}
	node
}

/// Return the previous leaf before `node` in document order, walking up
/// through structural branches as necessary.
pub(crate) fn previous_leaf(registry: &UastRegistry, node: NodeId, root: NodeId) -> Option<NodeId> {
	let mut current = node;
	loop {
		if let Some(sibling) = registry.get_prev_sibling(current) {
			return Some(last_leaf(registry, sibling));
		}
		let parent = unsafe { (*registry.edges[current.index()].get()).parent }?;
		if parent == root {
			return None;
		}
		current = parent;
	}
}

/// Return the next leaf after `node` in document order, walking up the
/// tree as necessary. Returns `None` when `node` is the last leaf.
fn next_leaf(registry: &UastRegistry, node: NodeId, root: NodeId) -> Option<NodeId> {
	let mut current = node;
	loop {
		if let Some(sib) = registry.get_next_sibling(current) {
			return Some(first_leaf(registry, sib));
		}
		// Walk up to parent. Stop at root.
		let parent = unsafe { (*registry.edges[current.index()].get()).parent }?;
		if parent == root {
			return None;
		}
		current = parent;
	}
}

pub(crate) fn line_end_byte_sparse(
	registry: &UastRegistry,
	root: NodeId,
	target_line: DocLine,
	include_newline: bool,
) -> Result<DocByte, String> {
	let line_start_target = registry
		.find_node_at_line_col_raw(root, target_line, DocDisplayCol::ZERO)
		.ready()
		.ok_or_else(|| "Line index is still building".to_string())?;
	let mut node = first_leaf(registry, line_start_target.node_id);
	let mut node_offset = if node == line_start_target.node_id {
		line_start_target.node_byte.get() as usize
	} else {
		0
	};
	let mut absolute = registry.doc_byte_for_node_offset(
		root,
		line_start_target.node_id,
		line_start_target.node_byte,
	);
	let file_size = registry.get_total_bytes(root);

	loop {
		let text = resolve_loaded_node_text(registry, node)?;
		for &b in &text.as_bytes()[node_offset..] {
			if b == b'\n' {
				return Ok(if include_newline {
					absolute.saturating_add(1)
				} else {
					absolute
				});
			}
			absolute = absolute.saturating_add(1);
		}

		if absolute.get() >= file_size {
			return Ok(DocByte::new(file_size));
		}

		let Some(next) = next_leaf(registry, node, root) else {
			return Ok(DocByte::new(file_size));
		};
		node = next;
		node_offset = 0;
	}
}

pub(crate) fn read_line_bytes_sparse(
	registry: &UastRegistry,
	root: NodeId,
	target_line: DocLine,
	include_newline: bool,
) -> Result<Vec<u8>, String> {
	let start = line_start_byte_sparse(registry, root, target_line)?;
	let end = line_end_byte_sparse(registry, root, target_line, include_newline)?;
	registry
		.read_loaded_slice(root, start, end)
		.map_err(|msg| msg.to_string())
}

pub(crate) fn line_start_byte_from_sum_tree(tree: &SumTree, target_line: DocLine) -> DocByte {
	DocByte::new(
		tree.query_by_doc_line_count(target_line.boundary_before())
			.byte_offset,
	)
}

pub(crate) fn read_line_bytes_from_sum_tree(
	registry: &UastRegistry,
	root: NodeId,
	tree: &SumTree,
	target_line: DocLine,
	include_newline: bool,
) -> Result<Vec<u8>, String> {
	let start = line_start_byte_from_sum_tree(tree, target_line);
	let next_line_start = tree
		.query_by_doc_line_count(target_line.boundary_after())
		.byte_offset;
	let total_bytes = registry.get_total_bytes(root);
	let raw_end = if next_line_start > start.get() {
		next_line_start
	} else {
		total_bytes
	};
	let mut bytes = registry
		.read_loaded_slice(root, start, DocByte::new(raw_end))
		.map_err(|msg| msg.to_string())?;
	// Only a real terminator is stripped: the document's last line has no
	// trailing newline, and blindly dropping the final byte would chop its
	// last character (visibly desyncing cursor math from the caret).
	if !include_newline && bytes.last() == Some(&b'\n') {
		bytes.pop();
	}
	Ok(bytes)
}

fn is_structural_word_token(kind: SyntaxKind) -> bool {
	kind == SyntaxKind::IDENT || kind.is_keyword(Edition::Edition2024) || kind.is_literal()
}

fn seek_structural_token(mut token: SyntaxToken, direction: Direction) -> Option<SyntaxToken> {
	loop {
		if is_structural_word_token(token.kind()) {
			return Some(token);
		}

		if token.kind() == SyntaxKind::WHITESPACE || token.kind().is_punct() {
			token = match direction {
				Direction::Next => token.next_token()?,
				Direction::Prev => token.prev_token()?,
			};
			continue;
		}

		return None;
	}
}

pub(crate) fn select_structural_token_at_offset(
	syntax: &ra_ap_syntax::SyntaxNode,
	offset: TextSize,
) -> Option<SyntaxToken> {
	let left = syntax.token_at_offset(offset).left_biased();
	let right = syntax.token_at_offset(offset).right_biased();

	match (left, right) {
		(None, None) => None,
		(Some(token), None) | (None, Some(token)) => {
			if is_structural_word_token(token.kind()) {
				Some(token)
			} else {
				seek_structural_token(token.clone(), Direction::Next)
					.or_else(|| seek_structural_token(token, Direction::Prev))
			}
		}
		(Some(left), Some(right)) if left == right => {
			if is_structural_word_token(left.kind()) {
				Some(left)
			} else {
				seek_structural_token(left.clone(), Direction::Next)
					.or_else(|| seek_structural_token(left, Direction::Prev))
			}
		}
		(Some(left), Some(right)) => {
			if is_structural_word_token(right.kind()) {
				return Some(right);
			}

			if right.kind() == SyntaxKind::WHITESPACE
				&& let Some(found) = seek_structural_token(right.clone(), Direction::Next)
			{
				return Some(found);
			}

			if is_structural_word_token(left.kind()) {
				return Some(left);
			}

			if right.kind().is_punct()
				&& let Some(found) = seek_structural_token(right, Direction::Next)
			{
				return Some(found);
			}

			seek_structural_token(left, Direction::Prev)
		}
	}
}

pub(crate) fn resolve_loaded_node_text(
	registry: &UastRegistry,
	node: NodeId,
) -> Result<String, String> {
	let text = registry.resolve_physical_bytes(node);
	let byte_len = unsafe { (*registry.metrics[node.index()].get()).byte_length as usize };
	if text.is_empty() && byte_len > 0 {
		return Err("File still loading, cannot resolve structural token".to_string());
	}
	String::from_utf8(text).map_err(|_| "Loaded node is not valid UTF-8".to_string())
}

#[cfg(test)]
mod tests {
	use super::{first_non_whitespace_visual_col, line_end_visual_col};
	use crate::core::{DocDisplayCol, DocLine};

	#[test]
	fn line_end_visual_col_expands_tabs() {
		assert_eq!(
			line_end_visual_col(b"\tfoo\n", DocLine::ZERO),
			DocDisplayCol::new(7),
		);
		assert_eq!(
			line_end_visual_col(b"\t\tlet x = 1;\n", DocLine::ZERO),
			DocDisplayCol::new(18),
		);
	}

	#[test]
	fn first_non_whitespace_visual_col_is_tab_aware() {
		assert_eq!(
			first_non_whitespace_visual_col(b" \t\tfoo\n", DocLine::ZERO),
			DocDisplayCol::new(8),
		);
		assert_eq!(
			first_non_whitespace_visual_col(b"    \n", DocLine::ZERO),
			DocDisplayCol::ZERO,
		);
	}
}
