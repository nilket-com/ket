use super::super::support::{
	first_non_whitespace_visual_col, line_content_slice, line_start_byte_sparse,
	resolve_loaded_node_text,
};
use crate::core::{DocByte, DocDisplayCol, DocLine, TAB_SIZE};
use crate::core::{StateId, TopologyRev};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::folding::FoldSet;
use crate::engine::session::{Affinity, Caret};
use crate::uast::UastProjection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LexClass {
	Whitespace,
	Keyword,
	Punctuation,
}

fn classify_lex_char(ch: char) -> LexClass {
	match ch {
		' ' | '\t' | '\n' => LexClass::Whitespace,
		_ if ch.is_alphanumeric() || ch == '_' => LexClass::Keyword,
		_ => LexClass::Punctuation,
	}
}

fn clamp_to_char_boundary(doc: &str, byte: usize) -> usize {
	let mut byte = byte.min(doc.len());
	while byte > 0 && !doc.is_char_boundary(byte) {
		byte -= 1;
	}
	byte
}

fn char_start_before(doc: &str, byte: usize) -> Option<usize> {
	let byte = clamp_to_char_boundary(doc, byte);
	doc[..byte].char_indices().next_back().map(|(idx, _)| idx)
}

fn char_at(doc: &str, byte: usize) -> Option<char> {
	let byte = clamp_to_char_boundary(doc, byte);
	doc[byte..].char_indices().next().map(|(_, ch)| ch)
}

fn next_char_start(doc: &str, byte: usize) -> Option<usize> {
	let byte = clamp_to_char_boundary(doc, byte);
	let ch = char_at(doc, byte)?;
	Some(byte + ch.len_utf8())
}

pub(crate) fn next_word_start(doc: &str, current_byte: usize) -> usize {
	let mut byte = clamp_to_char_boundary(doc, current_byte);
	if byte >= doc.len() {
		return doc.len();
	}

	let start_class = classify_lex_char(char_at(doc, byte).expect("char at valid boundary"));
	while byte < doc.len() {
		let ch = char_at(doc, byte).expect("char at valid boundary");
		if classify_lex_char(ch) != start_class {
			break;
		}
		byte = match next_char_start(doc, byte) {
			Some(next) => next,
			None => return doc.len(),
		};
	}

	while byte < doc.len() {
		let ch = char_at(doc, byte).expect("char at valid boundary");
		if classify_lex_char(ch) != LexClass::Whitespace {
			break;
		}
		byte = match next_char_start(doc, byte) {
			Some(next) => next,
			None => return doc.len(),
		};
	}

	byte
}

pub(crate) fn prev_word_start(doc: &str, current_byte: usize) -> usize {
	let current_byte = clamp_to_char_boundary(doc, current_byte);
	let Some(mut byte) = char_start_before(doc, current_byte) else {
		return 0;
	};

	while let Some(ch) = char_at(doc, byte) {
		if classify_lex_char(ch) != LexClass::Whitespace {
			break;
		}
		let Some(prev) = char_start_before(doc, byte) else {
			return 0;
		};
		byte = prev;
	}

	let Some(ch) = char_at(doc, byte) else {
		return 0;
	};
	let target_class = classify_lex_char(ch);

	while let Some(prev) = char_start_before(doc, byte) {
		let prev_ch = char_at(doc, prev).expect("char at valid boundary");
		if classify_lex_char(prev_ch) != target_class {
			break;
		}
		byte = prev;
	}

	byte
}

pub(crate) fn next_word_end(doc: &str, current_byte: usize) -> usize {
	let current_byte = clamp_to_char_boundary(doc, current_byte);
	let Some(mut byte) = next_char_start(doc, current_byte) else {
		return current_byte.min(doc.len());
	};

	while byte < doc.len() {
		let ch = char_at(doc, byte).expect("char at valid boundary");
		if classify_lex_char(ch) != LexClass::Whitespace {
			break;
		}
		byte = match next_char_start(doc, byte) {
			Some(next) => next,
			None => return current_byte.min(doc.len()),
		};
	}

	if byte >= doc.len() {
		return current_byte.min(doc.len());
	}

	let target_class = classify_lex_char(char_at(doc, byte).expect("char at valid boundary"));
	let mut last = byte;

	while byte < doc.len() {
		let ch = char_at(doc, byte).expect("char at valid boundary");
		if classify_lex_char(ch) != target_class {
			break;
		}
		last = byte;
		byte = match next_char_start(doc, byte) {
			Some(next) => next,
			None => break,
		};
	}

	last
}

pub(crate) fn smart_home_visual_col(
	doc: &[u8],
	target_line: DocLine,
	current_col: DocDisplayCol,
) -> DocDisplayCol {
	let first_non_whitespace = first_non_whitespace_visual_col(doc, target_line);
	if current_col == first_non_whitespace {
		DocDisplayCol::ZERO
	} else {
		first_non_whitespace
	}
}

fn advance_visual_col_only(col: &mut DocDisplayCol, b: u8) {
	if b == b'\t' {
		*col = col
			.checked_add(TAB_SIZE - (col.get() % TAB_SIZE))
			.expect("DocDisplayCol addition overflow");
	} else if b != b'\n' {
		*col = col.checked_add(1).expect("DocDisplayCol addition overflow");
	}
}

pub(crate) fn step_left_visual_col(
	doc: &[u8],
	target_line: DocLine,
	current_col: DocDisplayCol,
) -> DocDisplayCol {
	let mut col = DocDisplayCol::ZERO;

	for &b in line_content_slice(doc, target_line) {
		let char_start = col;
		advance_visual_col_only(&mut col, b);
		if current_col <= col {
			return char_start;
		}
	}

	DocDisplayCol::ZERO
}

pub(crate) fn step_right_visual_col(
	doc: &[u8],
	target_line: DocLine,
	current_col: DocDisplayCol,
) -> DocDisplayCol {
	let mut col = DocDisplayCol::ZERO;

	for &b in line_content_slice(doc, target_line) {
		advance_visual_col_only(&mut col, b);
		if current_col < col {
			return col;
		}
	}

	col
}

pub(crate) fn page_motion_step(viewport_lines: u32) -> u64 {
	if viewport_lines > 1 {
		u64::from(viewport_lines - 1)
	} else {
		1
	}
}

pub(crate) fn place_cursor_at_line_col(
	registry: &UastRegistry,
	root: NodeId,
	folds: &mut FoldSet,
	target_line: DocLine,
	target_col: DocDisplayCol,
	reveal_hidden: bool,
	cache_revisions: (TopologyRev, StateId),
	caret: &mut Caret,
) -> bool {
	caret.place_at_line_col(
		registry,
		root,
		folds,
		cache_revisions.0,
		cache_revisions.1,
		target_line,
		target_col,
		reveal_hidden,
		Affinity::After,
	)
}

pub(crate) fn line_col_from_doc_byte_sparse(
	registry: &UastRegistry,
	root: NodeId,
	target_byte: DocByte,
) -> Result<(DocLine, DocDisplayCol), String> {
	// The byte descent accumulates exact newline metrics for every complete
	// left sibling it skips at each topology level. That yields the leaf and
	// its absolute starting line in one root-to-leaf pass, without a second
	// leaf-to-root walk or a dependency on the flat-root line index.
	let leaf = registry.find_node_at_doc_byte_raw(root, target_byte);

	let leaf_text = resolve_loaded_node_text(registry, leaf.node_id)?;
	let local_offset = (leaf.node_byte.get() as usize).min(leaf_text.len());
	let local_prefix = &leaf_text.as_bytes()[..local_offset];
	let local_lines = local_prefix.iter().filter(|&&b| b == b'\n').count() as u64;
	let line = leaf
		.node_start_line
		.ok_or_else(|| "Byte-to-line index is still building".to_string())?
		.saturating_add(local_lines);

	// With the line known, derive the visual column by reading the bytes
	// from the start of that line up to `target_byte`. Both helpers used
	// here descend correctly through grafted trees.
	let line_start = line_start_byte_sparse(registry, root, line)?;
	let prefix_bytes = registry
		.read_loaded_slice(root, line_start, target_byte)
		.map_err(|msg| msg.to_string())?;
	let mut col = DocDisplayCol::ZERO;
	for &b in &prefix_bytes {
		advance_visual_col_only(&mut col, b);
	}

	Ok((line, col))
}

#[cfg(test)]
mod tests {
	use super::{
		advance_visual_col_only, line_col_from_doc_byte_sparse, next_word_end, next_word_start,
		place_cursor_at_line_col, prev_word_start, smart_home_visual_col, step_left_visual_col,
		step_right_visual_col,
	};
	use crate::core::{DocByte, DocDisplayCol, DocLine, StateId, TopologyRev};
	use crate::engine::folding::{FoldSet, materialize_fold_boundary_at_cursor};
	use crate::engine::test_support::{
		build_document, build_exact_metric_physical_document, build_grafted_rust_document,
		build_split_virtual_document,
	};
	use crate::uast::UastProjection;

	fn linear_line_col(bytes: &[u8], target: usize) -> (DocLine, DocDisplayCol) {
		let target = target.min(bytes.len());
		let prefix = &bytes[..target];
		let line = DocLine::new(prefix.iter().filter(|&&byte| byte == b'\n').count() as u64);
		let line_start = prefix
			.iter()
			.rposition(|&byte| byte == b'\n')
			.map_or(0, |newline| newline + 1);
		let mut col = DocDisplayCol::ZERO;
		for &byte in &bytes[line_start..target] {
			advance_visual_col_only(&mut col, byte);
		}
		(line, col)
	}

	#[test]
	fn sparse_line_col_tracks_columns_across_leaf_boundaries() {
		let (registry, root) = build_split_virtual_document(&["ab\ncd", "ef"]);
		let (line, col) =
			line_col_from_doc_byte_sparse(&registry, root, DocByte::new(6)).expect("line/col");
		assert_eq!(line, DocLine::new(1));
		assert_eq!(col, DocDisplayCol::new(3));
	}

	#[test]
	fn sparse_line_col_matches_linear_coordinates_for_every_split_and_grafted_byte() {
		let parts = ["a\t", "b\nc", "\td\n", "tail\n"];
		let split_source = parts.concat();
		let (split_registry, split_root) = build_split_virtual_document(&parts);
		let grafted_source = "fn alpha() {\n\tlet a = 1;\n}\n\nfn beta() {\n    return a;\n}\n";
		let (grafted_registry, grafted_root) = build_grafted_rust_document(grafted_source);

		for (registry, root, source) in [
			(&split_registry, split_root, split_source.as_bytes()),
			(&grafted_registry, grafted_root, grafted_source.as_bytes()),
		] {
			for byte in 0..=source.len() {
				assert_eq!(
					line_col_from_doc_byte_sparse(registry, root, DocByte::new(byte as u64))
						.expect("hot coordinate should resolve"),
					linear_line_col(source, byte),
					"coordinate mismatch at byte {byte}"
				);
			}
		}
	}

	#[test]
	fn sparse_line_col_resolves_function_start_in_grafted_tree() {
		// Regression: line_col_from_doc_byte_sparse used to walk only root's
		// direct children. After apply_graft the tree is deep
		// (root → module → fn → tokens), so the loop always landed on the
		// lone module child and added its full newline count, returning
		// line ≈ total_newlines for any target. This pins the correct
		// behavior for a deeply nested target byte.
		let source =
			"fn alpha() {\n    let a = 1;\n}\n\nfn beta() {\n    let b = 2;\n    return b;\n}\n";
		let (registry, root) = build_grafted_rust_document(source);

		let target_byte = source.find("fn beta()").expect("beta exists") as u64;
		let (line, col) = line_col_from_doc_byte_sparse(&registry, root, DocByte::new(target_byte))
			.expect("sparse line/col");

		assert_eq!(line, DocLine::new(4));
		assert_eq!(col, DocDisplayCol::ZERO);
	}

	#[test]
	fn sparse_line_col_resolves_mid_line_position_in_grafted_tree() {
		let source =
			"fn alpha() {\n    let a = 1;\n}\n\nfn beta() {\n    let b = 2;\n    return b;\n}\n";
		let (registry, root) = build_grafted_rust_document(source);

		// Byte offset of `b` inside `    return b;` on line 6.
		let target_byte =
			(source.find("return b;").expect("return exists") + "return ".len()) as u64;
		let (line, col) = line_col_from_doc_byte_sparse(&registry, root, DocByte::new(target_byte))
			.expect("sparse line/col");

		assert_eq!(line, DocLine::new(6));
		assert_eq!(col, DocDisplayCol::new(11));
	}

	#[test]
	fn sparse_line_col_resolves_position_near_eof_in_grafted_tree() {
		// After the structural-delete bug, the cursor would jump to
		// total_newlines even when the new cursor byte was nowhere near EOF.
		// Verify a near-EOF byte still resolves to its true (line, col).
		let source = "fn one() {\n    let a = 1;\n}\n\nfn two() {\n    let b = 2;\n    return b;\n}\n\nfn three() {\n    let c = 3;\n}\n";
		let (registry, root) = build_grafted_rust_document(source);

		// Byte offset of `c` inside `    let c = 3;` on line 10.
		let target_byte = (source.find("let c = 3").expect("let c exists") + "let ".len()) as u64;
		let (line, col) = line_col_from_doc_byte_sparse(&registry, root, DocByte::new(target_byte))
			.expect("sparse line/col");

		assert_eq!(line, DocLine::new(10));
		assert_eq!(col, DocDisplayCol::new(8));
	}

	#[test]
	fn sparse_line_col_real_world_clipboard_regression() {
		let source = include_str!("../clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);

		// The user reported jumping to EOF after deleting get_text.
		// get_text starts at line 14 (index 13).
		let get_text_pos = source.find("pub fn get_text").expect("get_text exists");
		let target_byte = DocByte::new(get_text_pos as u64);

		let (line, col) = line_col_from_doc_byte_sparse(&registry, root, target_byte)
			.expect("should resolve line/col for real-world file");

		assert_eq!(line.get(), 13, "get_text start line mismatch");
		let line_start = source[..get_text_pos]
			.rfind('\n')
			.map_or(0, |offset| offset + 1);
		let mut expected_col = DocDisplayCol::ZERO;
		for &b in &source.as_bytes()[line_start..get_text_pos] {
			advance_visual_col_only(&mut expected_col, b);
		}
		assert_eq!(col, expected_col, "get_text start col mismatch");

		// Now simulate the structural bounds for SelectAroundFunction
		let q = crate::engine::uast_query::UastQuery::new(&registry, root);
		let leaf = q.node_at_offset(target_byte).expect("leaf exists");
		let func = q
			.find_ancestor(leaf, crate::engine::uast_query::is_function_scope)
			.expect("function exists");
		let (fn_start, _fn_end) = q.node_bounds(func);

		// Verify fn_start resolves correctly
		let (fn_line, _) =
			line_col_from_doc_byte_sparse(&registry, root, fn_start).expect("fn_start resolution");
		assert_eq!(fn_line.get(), 13);

		// Verify a position AFTER the function (set_text) resolves correctly
		let set_text_pos = source.find("pub fn set_text").expect("set_text exists");
		let (set_line, _) =
			line_col_from_doc_byte_sparse(&registry, root, DocByte::new(set_text_pos as u64))
				.expect("set_text resolution");
		assert_eq!(set_line.get(), 21);
	}

	#[test]
	fn smart_home_toggles_between_indent_and_line_start() {
		let doc = b"\t  foo\n";
		assert_eq!(
			smart_home_visual_col(doc, DocLine::ZERO, DocDisplayCol::new(10)),
			DocDisplayCol::new(6),
		);
		assert_eq!(
			smart_home_visual_col(doc, DocLine::ZERO, DocDisplayCol::new(6)),
			DocDisplayCol::ZERO,
		);
		assert_eq!(
			smart_home_visual_col(doc, DocLine::ZERO, DocDisplayCol::ZERO),
			DocDisplayCol::new(6),
		);
	}

	#[test]
	fn step_right_visual_col_jumps_across_tabs() {
		assert_eq!(
			step_right_visual_col(b"\tfoo\n", DocLine::ZERO, DocDisplayCol::ZERO),
			DocDisplayCol::new(4),
		);
		assert_eq!(
			step_right_visual_col(b" \tfoo\n", DocLine::ZERO, DocDisplayCol::new(1)),
			DocDisplayCol::new(4),
		);
	}

	#[test]
	fn step_left_visual_col_jumps_back_across_tabs() {
		assert_eq!(
			step_left_visual_col(b"\tfoo\n", DocLine::ZERO, DocDisplayCol::new(4)),
			DocDisplayCol::ZERO,
		);
		assert_eq!(
			step_left_visual_col(b" \tfoo\n", DocLine::ZERO, DocDisplayCol::new(4)),
			DocDisplayCol::new(1),
		);
	}

	#[test]
	fn next_word_start_handles_punctuation_boundaries() {
		let doc = "foo::bar baz";
		assert_eq!(next_word_start(doc, 0), 3);
		assert_eq!(next_word_start(doc, 3), 5);
		assert_eq!(next_word_start(doc, 5), 9);
	}

	#[test]
	fn prev_word_start_respects_utf8_boundaries() {
		let doc = "éx yz";
		assert_eq!(prev_word_start(doc, 4), 0);
		assert_eq!(prev_word_start(doc, 2), 0);
	}

	#[test]
	fn next_word_end_respects_utf8_boundaries() {
		let doc = "éx yz";
		assert_eq!(next_word_end(doc, 0), 2);
		assert_eq!(next_word_end(doc, 2), 5);
	}

	#[test]
	fn place_cursor_at_line_col_reveals_hidden_synthetic_fold_target() {
		let (registry, root) = build_document(
			"fn main() {\n    if true {\n        alpha();\n        beta();\n    }\n    gamma();\n}\n",
		);
		let fold = materialize_fold_boundary_at_cursor(
			&registry,
			root,
			DocLine::new(2),
			DocDisplayCol::new(8),
		)
		.expect("nested block fold should materialize");
		let mut folds = FoldSet::default();
		folds.insert(fold);

		let mut caret = crate::engine::session::Caret::at_start();
		let first = registry
			.find_node_at_line_col(
				root,
				&crate::uast::NoFolds,
				DocLine::new(1),
				DocDisplayCol::ZERO,
			)
			.node_id;
		caret.reset_to_start(first, TopologyRev::ZERO, StateId::ZERO);
		place_cursor_at_line_col(
			&registry,
			root,
			&mut folds,
			DocLine::new(3),
			DocDisplayCol::new(8),
			true,
			(TopologyRev::ZERO, StateId::ZERO),
			&mut caret,
		);

		assert!(!folds.contains(fold));
		assert_eq!(caret.abs_line(), DocLine::new(3));
		assert_ne!(caret.node_cache().expect("node cache").node, fold);
	}

	#[test]
	fn cold_projection_preserves_logical_target_without_fabricating_local_offset() {
		let (registry, root, path) = build_exact_metric_physical_document(&["aaa\nbbb", "ccc\n"]);
		let first = registry.get_first_child(root).unwrap();
		let mut folds = FoldSet::default();
		let mut caret = crate::engine::session::Caret::at_start();
		caret.set_byte(DocByte::new(2), crate::engine::session::Affinity::After);
		caret.resolve(
			&registry,
			root,
			&mut folds,
			TopologyRev::ZERO,
			StateId::ZERO,
		);

		assert!(!place_cursor_at_line_col(
			&registry,
			root,
			&mut folds,
			DocLine::new(1),
			DocDisplayCol::ZERO,
			false,
			(TopologyRev::ZERO, StateId::ZERO),
			&mut caret,
		));
		assert_eq!(caret.byte(), DocByte::new(2));
		assert_eq!(caret.abs_line(), DocLine::new(1));
		assert_eq!(caret.abs_col(), DocDisplayCol::ZERO);

		let bytes = registry.read_node_bytes_sync(first).unwrap();
		registry.hot_swap_virtual_data(first, bytes);
		assert!(place_cursor_at_line_col(
			&registry,
			root,
			&mut folds,
			DocLine::new(1),
			DocDisplayCol::ZERO,
			false,
			(TopologyRev::ZERO, StateId::ZERO),
			&mut caret,
		));
		assert_eq!(caret.node_cache().expect("node cache").offset, 4);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn reveal_line_lookup_near_eof_positions_cursor_correctly() {
		let chunk_count = 128usize;
		let mut chunks = vec!["x\n"; chunk_count];
		chunks[chunk_count - 1] = "x";
		let (registry, root, path) = build_exact_metric_physical_document(&chunks);
		let target_line = DocLine::new((chunk_count - 2) as u64);
		let mut folds = FoldSet::default();
		let mut caret = crate::engine::session::Caret::at_start();
		caret.reset_to_start(
			registry.get_first_child(root).expect("first leaf"),
			TopologyRev::ZERO,
			StateId::ZERO,
		);
		place_cursor_at_line_col(
			&registry,
			root,
			&mut folds,
			target_line,
			DocDisplayCol::ZERO,
			true,
			(TopologyRev::ZERO, StateId::ZERO),
			&mut caret,
		);

		assert_eq!(caret.abs_line(), target_line);
		assert_eq!(
			caret.node_cache().expect("node cache").node.index(),
			chunk_count - 1
		);

		let _ = std::fs::remove_file(path);
	}
}
