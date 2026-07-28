//! Structural topology for CSV/TSV — root `RelationalTable` with
//! a `RelationalRow` per logical record. Quoted fields with embedded
//! newlines stay inside the same row because the tokenizer classifies
//! their newlines as part of the quoted-field span.
//!
//! The strict invariant `parent.byte_len == sum(children.byte_len)`
//! is enforced by tests (the structural graft walks children with
//! `child_offset += child.byte_len` and any gap mis-places downstream
//! content — this was the TOML breakage).

use crate::engine::language::GreenNode;
use crate::svp::csv_parse::{CsvTokenKind, tokenize};
use crate::uast::kind::{ProviderKindId, SemanticKind};

pub const CSV_TABLE_KIND: ProviderKindId = ProviderKindId::new(1);
pub const CSV_ROW_KIND: ProviderKindId = ProviderKindId::new(2);
pub const CSV_TOKEN_KIND: ProviderKindId = ProviderKindId::new(3);

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
			provider_kind: match self.kind {
				SemanticKind::RelationalTable => CSV_TABLE_KIND,
				SemanticKind::RelationalRow => CSV_ROW_KIND,
				SemanticKind::Token | SemanticKind::Group => CSV_TOKEN_KIND,
			},
			byte_len: self.end.saturating_sub(self.start) as u32,
			children: self.children.iter().map(Self::to_green).collect(),
		}
	}
}

pub fn build_csv_topology(source: &[u8], delimiter: u8) -> GreenNode {
	let total_len = source.len();
	if total_len == 0 {
		return GreenNode {
			kind: SemanticKind::RelationalTable,
			provider_kind: CSV_TABLE_KIND,
			byte_len: 0,
			children: Vec::new(),
		};
	}

	let tokens = tokenize(source, delimiter);
	let mut children: Vec<RangeNode> = Vec::new();
	let mut cursor = 0usize;
	let mut record_start: Option<usize> = None;

	for token in &tokens {
		match token.kind {
			CsvTokenKind::Newline => {
				if let Some(start) = record_start.take() {
					children.push(RangeNode::row(start, token.end));
					cursor = token.end;
				} else {
					// Empty line — emit as trivia so the structural tree
					// still covers it without inventing an empty row.
					if token.start > cursor {
						children.push(RangeNode::leaf(cursor, token.start));
					}
					children.push(RangeNode::leaf(token.start, token.end));
					cursor = token.end;
				}
			}
			_ => {
				if record_start.is_none() {
					if token.start > cursor {
						children.push(RangeNode::leaf(cursor, token.start));
						cursor = token.start;
					}
					record_start = Some(token.start);
				}
			}
		}
	}

	if let Some(start) = record_start
		&& total_len > start
	{
		children.push(RangeNode::row(start, total_len));
		cursor = total_len;
	}

	if cursor < total_len {
		children.push(RangeNode::leaf(cursor, total_len));
	}

	if children.is_empty() {
		children.push(RangeNode::leaf(0, total_len));
	}

	RangeNode {
		kind: SemanticKind::RelationalTable,
		start: 0,
		end: total_len,
		children,
	}
	.to_green()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::csv_parse::{CSV_DELIMITER, TSV_DELIMITER};

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
			assert_children_cover_parent(child, &format!("{path}.{idx}"));
		}
	}

	#[test]
	fn empty_source_yields_empty_table() {
		let tree = build_csv_topology(b"", CSV_DELIMITER);
		assert_eq!(tree.byte_len, 0);
		assert_eq!(tree.kind, SemanticKind::RelationalTable);
	}

	#[test]
	fn invariant_holds_for_diverse_inputs() {
		let sources: &[&[u8]] = &[
			b"name,age\ntest,38\n",
			b"a,b,c\nd,e,f\n",
			b"single line no newline",
			b"\"quoted\",plain\n",
			b"\"multi\nline\",x\ny,z\n",
			b"a,,b\n,,\n",
			b"a\tb\tc\nd\te\tf\n",
			b"\r\n\r\na,b\r\nc,d\r\n",
			b"\n\nname,age\n\ntest,38\n",
		];
		for src in sources {
			for delim in &[CSV_DELIMITER, TSV_DELIMITER] {
				let tree = build_csv_topology(src, *delim);
				assert_eq!(
					tree.byte_len as usize,
					src.len(),
					"byte_len should equal source length for {:?}",
					std::str::from_utf8(src).unwrap_or("<non-utf8>"),
				);
				assert_children_cover_parent(
					&tree,
					std::str::from_utf8(src).unwrap_or("<non-utf8>"),
				);
			}
		}
	}

	#[test]
	fn each_record_becomes_a_relational_row() {
		let src = b"a,b\nc,d\ne,f\n";
		let tree = build_csv_topology(src, CSV_DELIMITER);
		let rows = tree
			.children
			.iter()
			.filter(|n| n.kind == SemanticKind::RelationalRow)
			.count();
		assert_eq!(rows, 3);
	}

	#[test]
	fn quoted_field_with_embedded_newline_stays_one_row() {
		let src = b"\"multi\nline\",x\ny,z\n";
		let tree = build_csv_topology(src, CSV_DELIMITER);
		let rows = tree
			.children
			.iter()
			.filter(|n| n.kind == SemanticKind::RelationalRow)
			.count();
		assert_eq!(rows, 2);
	}

	#[test]
	fn trailing_record_without_newline_is_captured() {
		let src = b"a,b\nc,d";
		let tree = build_csv_topology(src, CSV_DELIMITER);
		let rows = tree
			.children
			.iter()
			.filter(|n| n.kind == SemanticKind::RelationalRow)
			.count();
		assert_eq!(rows, 2);
	}

	#[test]
	fn blank_lines_become_trivia_leaves() {
		let src = b"\n\na,b\n";
		let tree = build_csv_topology(src, CSV_DELIMITER);
		let rows = tree
			.children
			.iter()
			.filter(|n| n.kind == SemanticKind::RelationalRow)
			.count();
		assert_eq!(rows, 1);
		assert_eq!(tree.byte_len as usize, src.len());
	}
}
