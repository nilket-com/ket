//! Structural topology for TOML — maps `[section]` headers to
//! `RelationalTable` branches and each key-value entry to a
//! `RelationalRow`. Children of every node always cover the parent's
//! byte range exactly (no gaps, no overlap), which the structural graft
//! relies on when computing absolute child offsets.

use crate::engine::language::GreenNode;
use crate::svp::toml_parse::{TomlToken, TomlTokenKind, tokenize};
use crate::uast::kind::SemanticKind;

#[derive(Debug, Clone)]
struct RangeNode {
	kind: SemanticKind,
	start: usize,
	end: usize,
	children: Vec<RangeNode>,
}

impl RangeNode {
	fn leaf(start: usize, end: usize) -> Self {
		Self {
			kind: SemanticKind::Token,
			start,
			end,
			children: Vec::new(),
		}
	}

	fn row(start: usize, end: usize) -> Self {
		Self {
			kind: SemanticKind::RelationalRow,
			start,
			end,
			children: Vec::new(),
		}
	}

	fn to_green(&self) -> GreenNode {
		GreenNode {
			kind: self.kind,
			byte_len: self.end.saturating_sub(self.start) as u32,
			children: self.children.iter().map(Self::to_green).collect(),
		}
	}
}

pub fn build_toml_topology(source: &[u8]) -> GreenNode {
	let total_len = source.len();
	if total_len == 0 {
		return GreenNode {
			kind: SemanticKind::RelationalTable,
			byte_len: 0,
			children: Vec::new(),
		};
	}

	let tokens = tokenize(source);
	let items = extract_items(&tokens);
	let sections = split_into_sections(&items);
	let section_bounds = compute_section_bounds(&sections, total_len);

	let root_children: Vec<RangeNode> = sections
		.iter()
		.zip(section_bounds.iter())
		.filter_map(|(section, &(sec_start, sec_end))| {
			if sec_end <= sec_start {
				return None;
			}
			Some(build_section_node(section, sec_start, sec_end))
		})
		.collect();

	let root_children = if root_children.is_empty() {
		vec![RangeNode::leaf(0, total_len)]
	} else {
		root_children
	};

	RangeNode {
		kind: SemanticKind::RelationalTable,
		start: 0,
		end: total_len,
		children: root_children,
	}
	.to_green()
}

pub fn flat_topology(source_len: u32) -> GreenNode {
	GreenNode {
		kind: SemanticKind::RelationalTable,
		byte_len: source_len,
		children: vec![GreenNode {
			kind: SemanticKind::Token,
			byte_len: source_len,
			children: Vec::new(),
		}],
	}
}

#[derive(Debug, Clone, Copy)]
enum Item {
	Header { start: usize, end: usize },
	Row { start: usize, end: usize },
}

#[derive(Debug, Clone)]
struct Section {
	/// Byte offset where the header begins, or `None` for the implicit
	/// top-of-file section that precedes any explicit header.
	header: Option<(usize, usize)>,
	rows: Vec<(usize, usize)>,
}

fn split_into_sections(items: &[Item]) -> Vec<Section> {
	let mut sections: Vec<Section> = vec![Section {
		header: None,
		rows: Vec::new(),
	}];
	for item in items {
		match *item {
			Item::Header { start, end } => {
				sections.push(Section {
					header: Some((start, end)),
					rows: Vec::new(),
				});
			}
			Item::Row { start, end } => {
				sections
					.last_mut()
					.expect("sections always has the implicit head")
					.rows
					.push((start, end));
			}
		}
	}
	sections
}

/// Compute absolute `[start, end)` byte bounds for every section.
/// A section starts where its header begins (or at byte 0 for the
/// implicit initial section) and ends where the next section starts
/// (or at EOF for the final section).
fn compute_section_bounds(sections: &[Section], total_len: usize) -> Vec<(usize, usize)> {
	let mut bounds = Vec::with_capacity(sections.len());
	for idx in 0..sections.len() {
		let start = match sections[idx].header {
			Some((hs, _)) => hs,
			None => 0,
		};
		let end = sections
			.get(idx + 1)
			.and_then(|next| next.header.map(|(hs, _)| hs))
			.unwrap_or(total_len);
		bounds.push((start, end));
	}
	bounds
}

fn build_section_node(section: &Section, sec_start: usize, sec_end: usize) -> RangeNode {
	let mut children = Vec::new();
	let mut cursor = sec_start;

	// Emit a Token leaf covering the header line (including `[foo]` plus
	// any trailing whitespace/newline up to the first row). This keeps
	// the section's children contiguous across the header bytes.
	if let Some((_, header_end)) = section.header {
		let clamped_end = header_end.min(sec_end);
		if clamped_end > cursor {
			children.push(RangeNode::leaf(cursor, clamped_end));
			cursor = clamped_end;
		}
	}

	for &(row_start, row_end) in &section.rows {
		let clamped_start = row_start.max(cursor).min(sec_end);
		let clamped_end = row_end.max(clamped_start).min(sec_end);
		if clamped_start > cursor {
			children.push(RangeNode::leaf(cursor, clamped_start));
		}
		if clamped_end > clamped_start {
			children.push(RangeNode::row(clamped_start, clamped_end));
			cursor = clamped_end;
		}
	}

	if cursor < sec_end {
		children.push(RangeNode::leaf(cursor, sec_end));
	}

	if children.is_empty() {
		children.push(RangeNode::leaf(sec_start, sec_end));
	}

	RangeNode {
		kind: SemanticKind::RelationalTable,
		start: sec_start,
		end: sec_end,
		children,
	}
}

fn extract_items(tokens: &[TomlToken]) -> Vec<Item> {
	let mut out = Vec::new();
	let mut i = 0usize;

	while i < tokens.len() {
		let token = tokens[i];
		match token.kind {
			TomlTokenKind::Whitespace | TomlTokenKind::Newline | TomlTokenKind::Comment => {
				i += 1;
			}
			TomlTokenKind::LBracket => {
				let start = token.start;
				let (end, next_i) = scan_header(tokens, i);
				out.push(Item::Header { start, end });
				i = next_i;
			}
			_ => {
				let start = token.start;
				let (end, next_i) = scan_row(tokens, i);
				out.push(Item::Row { start, end });
				i = next_i;
			}
		}
	}

	out
}

/// Returns (byte_end, next_token_index) for a `[header]` or `[[header]]`.
fn scan_header(tokens: &[TomlToken], from: usize) -> (usize, usize) {
	let mut depth = 0i32;
	let mut i = from;
	let mut last_end = tokens[from].end;
	while i < tokens.len() {
		let token = tokens[i];
		match token.kind {
			TomlTokenKind::LBracket => depth += 1,
			TomlTokenKind::RBracket => {
				depth -= 1;
				last_end = token.end;
				if depth <= 0 {
					// Absorb a trailing `]` for `[[foo]]`.
					if let Some(next) = peek_significant(tokens, i + 1)
						&& tokens[next].kind == TomlTokenKind::RBracket
					{
						return (tokens[next].end, next + 1);
					}
					return (last_end, i + 1);
				}
			}
			TomlTokenKind::Newline => {
				// Unterminated header — stop at the newline.
				return (token.start, i);
			}
			_ => {}
		}
		last_end = token.end;
		i += 1;
	}
	(last_end, tokens.len())
}

/// Returns (byte_end, next_token_index) for a `key = value` row.
fn scan_row(tokens: &[TomlToken], from: usize) -> (usize, usize) {
	let mut bracket_depth = 0i32;
	let mut brace_depth = 0i32;
	let mut last_end = tokens[from].end;
	let mut i = from;
	while i < tokens.len() {
		let token = tokens[i];
		match token.kind {
			TomlTokenKind::LBracket => bracket_depth += 1,
			TomlTokenKind::RBracket => bracket_depth -= 1,
			TomlTokenKind::LBrace => brace_depth += 1,
			TomlTokenKind::RBrace => brace_depth -= 1,
			TomlTokenKind::Newline if bracket_depth <= 0 && brace_depth <= 0 => {
				return (token.start, i);
			}
			_ => {}
		}
		last_end = token.end;
		i += 1;
	}
	(last_end, tokens.len())
}

fn peek_significant(tokens: &[TomlToken], from: usize) -> Option<usize> {
	let mut i = from;
	while i < tokens.len() {
		match tokens[i].kind {
			TomlTokenKind::Whitespace | TomlTokenKind::Comment | TomlTokenKind::Newline => i += 1,
			_ => return Some(i),
		}
	}
	None
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Recursive invariant: every node's children must sum to its byte_len
	/// (or be empty for a leaf). This is the contract the structural graft
	/// relies on — a violation causes it to walk past EOF.
	fn assert_children_cover_parent(node: &GreenNode, path: &str) {
		if node.children.is_empty() {
			return;
		}
		let sum: u32 = node.children.iter().map(|c| c.byte_len).sum();
		assert_eq!(
			sum, node.byte_len,
			"children of {path} sum to {sum} but parent byte_len is {}",
			node.byte_len
		);
		for (idx, child) in node.children.iter().enumerate() {
			let child_path = format!("{path}.{idx}");
			assert_children_cover_parent(child, &child_path);
		}
	}

	#[test]
	fn empty_source_yields_empty_table() {
		let tree = build_toml_topology(b"");
		assert_eq!(tree.byte_len, 0);
		assert_eq!(tree.kind, SemanticKind::RelationalTable);
	}

	#[test]
	fn children_cover_parent_for_diverse_inputs() {
		let sources: &[&[u8]] = &[
			b"name = \"x\"\n",
			b"[foo]\nbar = 1\nbaz = 2\n",
			b"# top comment\n\n[a]\nx = 1\n\n[b]\ny = 2\n",
			b"arr = [\n  1,\n  2,\n]\n",
			b"[[bin]]\nname = \"a\"\n\n[[bin]]\nname = \"b\"\n",
			b"[package]\nname = \"baryon\"\nversion = \"0.1.0\"\n\n[package.metadata.deb]\nassets = [\n    [\"a\", \"b\", \"755\"],\n]\n\n[dependencies]\nimage = { version = \"0.25\", default-features = false }\n",
			b"\n\n[section]\nk = 1\n\n\n",
			b"leading = 1\n[section]\nv = 2\n",
		];
		for src in sources {
			let tree = build_toml_topology(src);
			assert_eq!(
				tree.byte_len as usize,
				src.len(),
				"top-level byte_len should equal source length for {:?}",
				std::str::from_utf8(src).unwrap_or("<non-utf8>"),
			);
			assert_children_cover_parent(&tree, std::str::from_utf8(src).unwrap_or("<non-utf8>"));
		}
	}

	#[test]
	fn header_begins_a_new_relational_table_child() {
		let src = b"x = 1\n[foo]\ny = 2\n";
		let tree = build_toml_topology(src);
		let table_count = tree
			.children
			.iter()
			.filter(|c| c.kind == SemanticKind::RelationalTable)
			.count();
		assert!(table_count >= 2, "expected implicit + [foo] sections");
	}

	#[test]
	fn multiline_array_stays_as_single_row() {
		let src = b"deps = [\n  1,\n  2,\n]\n";
		let tree = build_toml_topology(src);
		let row_count = tree
			.children
			.iter()
			.flat_map(|c| c.children.iter())
			.filter(|n| n.kind == SemanticKind::RelationalRow)
			.count();
		assert_eq!(row_count, 1);
	}

	#[test]
	fn flat_topology_length_matches_source() {
		let tree = flat_topology(42);
		assert_eq!(tree.byte_len, 42);
		assert_eq!(tree.children.len(), 1);
	}

	#[test]
	fn real_cargo_toml_invariant_holds() {
		let src = include_bytes!("../../Cargo.toml");
		let tree = build_toml_topology(src);
		assert_eq!(tree.byte_len as usize, src.len());
		assert_children_cover_parent(&tree, "Cargo.toml");
	}
}
