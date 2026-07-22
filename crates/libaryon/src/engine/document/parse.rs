use super::super::support::{resolve_loaded_node_text, select_structural_token_at_offset};
use crate::core::{DocByte, StateId};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::undo::TextDelta;
use crate::uast::NodeByteTarget;
use ra_ap_syntax::{AstNode, Direction, Edition, SourceFile, SyntaxKind, SyntaxToken, TextSize};

struct ParseWindow {
	text: String,
	global_start_byte: DocByte,
	cursor_local_byte: u32,
}

fn build_parse_window_around_leaf(
	registry: &UastRegistry,
	leaf: NodeByteTarget,
) -> Result<ParseWindow, String> {
	let mut text = String::new();
	let mut global_start_byte = leaf.node_start_byte;
	let mut cursor_local_byte = leaf.node_byte.get() as usize;

	// Find the previous leaf node in the walk
	let mut prev_leaf = None;
	let mut curr = Some(leaf.node_id);
	while let Some(node) = curr {
		if let Some(p) = registry.get_prev_sibling(node) {
			// Found a sibling branch or leaf. Find its last leaf.
			let mut last_child = Some(p);
			while let Some(child) = last_child {
				if let Some(tail) = unsafe { *registry.child_tails[child.index()].get() } {
					last_child = Some(tail);
				} else {
					prev_leaf = Some(child);
					break;
				}
			}
			break;
		}
		curr = unsafe { (*registry.edges[node.index()].get()).parent };
	}

	if let Some(prev) = prev_leaf {
		let prev_text = resolve_loaded_node_text(registry, prev)?;
		let prev_len = unsafe { (*registry.metrics[prev.index()].get()).byte_length };
		cursor_local_byte += prev_text.len();
		global_start_byte = global_start_byte.saturating_sub(prev_len);
		text.push_str(&prev_text);
	}

	let current_text = resolve_loaded_node_text(registry, leaf.node_id)?;
	text.push_str(&current_text);

	// Find the next leaf node in the walk
	let mut next_leaf = None;
	let mut curr = Some(leaf.node_id);
	while let Some(node) = curr {
		if let Some(n) = registry.get_next_sibling(node) {
			// Found a sibling branch or leaf. Find its first leaf.
			let mut first_child = Some(n);
			while let Some(child) = first_child {
				if let Some(fc) = registry.get_first_child(child) {
					first_child = Some(fc);
				} else {
					next_leaf = Some(child);
					break;
				}
			}
			break;
		}
		curr = unsafe { (*registry.edges[node.index()].get()).parent };
	}

	if let Some(next) = next_leaf {
		let next_text = resolve_loaded_node_text(registry, next)?;
		text.push_str(&next_text);
	}

	Ok(ParseWindow {
		text,
		global_start_byte,
		cursor_local_byte: cursor_local_byte as u32,
	})
}

fn is_matching_delimiter(kind: SyntaxKind) -> bool {
	matches!(
		kind,
		SyntaxKind::L_CURLY
			| SyntaxKind::R_CURLY
			| SyntaxKind::L_PAREN
			| SyntaxKind::R_PAREN
			| SyntaxKind::L_BRACK
			| SyntaxKind::R_BRACK
	)
}

fn matching_delimiter_target(kind: SyntaxKind) -> Option<(SyntaxKind, Direction)> {
	match kind {
		SyntaxKind::L_CURLY => Some((SyntaxKind::R_CURLY, Direction::Next)),
		SyntaxKind::R_CURLY => Some((SyntaxKind::L_CURLY, Direction::Prev)),
		SyntaxKind::L_PAREN => Some((SyntaxKind::R_PAREN, Direction::Next)),
		SyntaxKind::R_PAREN => Some((SyntaxKind::L_PAREN, Direction::Prev)),
		SyntaxKind::L_BRACK => Some((SyntaxKind::R_BRACK, Direction::Next)),
		SyntaxKind::R_BRACK => Some((SyntaxKind::L_BRACK, Direction::Prev)),
		_ => None,
	}
}

fn select_delimiter_token_at_offset(
	syntax: &ra_ap_syntax::SyntaxNode,
	offset: TextSize,
) -> Option<SyntaxToken> {
	let right = syntax.token_at_offset(offset).right_biased();
	let left = syntax.token_at_offset(offset).left_biased();

	right
		.filter(|token| is_matching_delimiter(token.kind()))
		.or_else(|| left.filter(|token| is_matching_delimiter(token.kind())))
}

pub(crate) fn find_matching_delimiter_byte(doc: &str, cursor_byte: usize) -> Option<usize> {
	let parse = SourceFile::parse(doc, Edition::Edition2024);
	let syntax = parse.tree().syntax().clone();
	let offset = TextSize::from(cursor_byte as u32);
	let token = select_delimiter_token_at_offset(&syntax, offset)?;
	let (target_kind, direction) = matching_delimiter_target(token.kind())?;
	let source_kind = token.kind();
	let mut depth = 1u32;
	let mut current = token;

	loop {
		current = match direction {
			Direction::Next => current.next_token()?,
			Direction::Prev => current.prev_token()?,
		};

		if current.kind() == source_kind {
			depth += 1;
		} else if current.kind() == target_kind {
			depth = depth.saturating_sub(1);
			if depth == 0 {
				return Some(u32::from(current.text_range().start()) as usize);
			}
		}
	}
}

pub(crate) fn word_object_delta_at_cursor(
	registry: &UastRegistry,
	root: NodeId,
	cursor_abs_byte_offset: DocByte,
) -> Result<Option<TextDelta>, String> {
	let leaf = registry.find_node_at_doc_byte_raw(root, cursor_abs_byte_offset);
	let parse_window = build_parse_window_around_leaf(registry, leaf)?;
	let parse = SourceFile::parse(&parse_window.text, Edition::Edition2024);
	let syntax = parse.tree().syntax().clone();
	let offset = TextSize::from(parse_window.cursor_local_byte);

	let Some(token) = select_structural_token_at_offset(&syntax, offset) else {
		return Ok(None);
	};

	let range = token.text_range();
	let start = parse_window
		.global_start_byte
		.saturating_add(u64::from(u32::from(range.start())));
	let end = parse_window
		.global_start_byte
		.saturating_add(u64::from(u32::from(range.end())));

	if end <= start {
		return Ok(None);
	}

	Ok(Some(TextDelta {
		global_byte_offset: start,
		deleted_bytes: token.text().as_bytes().to_vec(),
		inserted_bytes: Vec::new(),
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	}))
}

#[cfg(test)]
mod tests {
	use super::{find_matching_delimiter_byte, word_object_delta_at_cursor};
	use crate::core::DocByte;
	use crate::engine::test_support::build_document;

	#[test]
	fn word_object_delta_uses_ast_identifier_boundaries() {
		let (registry, root) = build_document("let alpha = beta;\n");
		let delta = word_object_delta_at_cursor(&registry, root, DocByte::new(5))
			.expect("word lookup should succeed")
			.expect("expected a word delta");

		assert_eq!(delta.global_byte_offset, DocByte::new(4));
		assert_eq!(delta.deleted_bytes, b"alpha");
		assert_eq!(delta.inserted_bytes, b"");
	}

	#[test]
	fn word_object_delta_falls_forward_from_whitespace() {
		let (registry, root) = build_document("let alpha beta\n");
		let delta = word_object_delta_at_cursor(&registry, root, DocByte::new(9))
			.expect("word lookup should succeed")
			.expect("expected a word delta");

		assert_eq!(delta.global_byte_offset, DocByte::new(10));
		assert_eq!(delta.deleted_bytes, b"beta");
	}

	#[test]
	fn find_matching_delimiter_byte_handles_nested_pairs_and_strings() {
		let doc = "fn main() { let text = \"}\"; if ready { call(); } }\n";
		let open = doc.find('{').expect("outer opening brace");
		let close = doc.rfind('}').expect("outer closing brace");

		assert_eq!(find_matching_delimiter_byte(doc, open), Some(close));
		assert_eq!(find_matching_delimiter_byte(doc, close), Some(open));
	}
}
