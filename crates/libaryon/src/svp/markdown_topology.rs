//! Byte-exact Markdown structural topology.
//!
//! The preview parser exposes source ranges, but those ranges are not a flat
//! partition: container blocks may wholly contain nested blocks. This module
//! first reduces that laminar set to maximal top-level blocks and fills every
//! uncovered byte with explicit trivia. Only then does it fold headings into
//! sections. Keeping interval proof and hierarchy construction separate means
//! a section bug cannot reorder, duplicate, or drop source bytes.

use std::cmp::Reverse;

use crate::engine::language::GreenNode;
use crate::svp::markdown_blocks::{MarkdownBlock, parse_markdown_blocks};
use crate::uast::kind::{ProviderKindId, SemanticKind};

pub const MARKDOWN_DOCUMENT_KIND: ProviderKindId = ProviderKindId::new(1);
pub const MARKDOWN_H1_KIND: ProviderKindId = ProviderKindId::new(2);
pub const MARKDOWN_H2_KIND: ProviderKindId = ProviderKindId::new(3);
pub const MARKDOWN_H3_KIND: ProviderKindId = ProviderKindId::new(4);
pub const MARKDOWN_H4_KIND: ProviderKindId = ProviderKindId::new(5);
pub const MARKDOWN_H5_KIND: ProviderKindId = ProviderKindId::new(6);
pub const MARKDOWN_H6_KIND: ProviderKindId = ProviderKindId::new(7);
pub const MARKDOWN_HEADING_LINE_KIND: ProviderKindId = ProviderKindId::new(8);
pub const MARKDOWN_PARAGRAPH_KIND: ProviderKindId = ProviderKindId::new(9);
pub const MARKDOWN_CODE_BLOCK_KIND: ProviderKindId = ProviderKindId::new(10);
pub const MARKDOWN_QUOTE_KIND: ProviderKindId = ProviderKindId::new(11);
pub const MARKDOWN_LIST_ITEM_KIND: ProviderKindId = ProviderKindId::new(12);
pub const MARKDOWN_RULE_KIND: ProviderKindId = ProviderKindId::new(13);
pub const MARKDOWN_TABLE_KIND: ProviderKindId = ProviderKindId::new(14);
pub const MARKDOWN_HTML_KIND: ProviderKindId = ProviderKindId::new(15);
pub const MARKDOWN_GAP_KIND: ProviderKindId = ProviderKindId::new(16);
pub const MARKDOWN_CONTENT_KIND: ProviderKindId = ProviderKindId::new(17);

const MARKDOWN_KIND_NAMES: [&str; 17] = [
	"DOCUMENT",
	"H1",
	"H2",
	"H3",
	"H4",
	"H5",
	"H6",
	"HEADING_LINE",
	"PARAGRAPH",
	"CODE_BLOCK",
	"QUOTE",
	"LIST_ITEM",
	"RULE",
	"TABLE",
	"HTML",
	"GAP",
	"CONTENT",
];

pub fn markdown_kind_name(kind: ProviderKindId) -> Option<&'static str> {
	let index = usize::from(kind.get().checked_sub(1)?);
	MARKDOWN_KIND_NAMES.get(index).copied()
}

pub const fn markdown_kind_is_trivia(kind: ProviderKindId) -> bool {
	kind.get() == MARKDOWN_GAP_KIND.get()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CoveredUnit {
	start: usize,
	end: usize,
	kind: ProviderKindId,
	heading_level: Option<u8>,
}

impl CoveredUnit {
	fn len(self) -> usize {
		self.end - self.start
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntervalError {
	OutOfBounds,
	NotUtf8Boundary,
	Crossing,
}

#[derive(Debug, Clone, Copy)]
struct BlockInterval {
	start: usize,
	end: usize,
	kind: ProviderKindId,
	heading_level: Option<u8>,
	original_index: usize,
}

/// Build the provider topology. UTF-8 is guaranteed by `&str`; documents too
/// large for GreenNode's u32 extent stay on the provider-unavailable path.
pub fn build_markdown_topology(source: &str) -> Option<GreenNode> {
	let total_len = u32::try_from(source.len()).ok()?;
	let blocks = parse_markdown_blocks(source);
	Some(lower_blocks(source, &blocks, total_len))
}

fn lower_blocks(source: &str, blocks: &[MarkdownBlock], total_len: u32) -> GreenNode {
	let units = match cover_top_level_blocks(source, blocks) {
		Ok(units) => units,
		Err(_) => return flat_fallback(total_len),
	};
	fold_sections(units, total_len)
}

fn cover_top_level_blocks(
	source: &str,
	blocks: &[MarkdownBlock],
) -> Result<Vec<CoveredUnit>, IntervalError> {
	let mut intervals = Vec::with_capacity(blocks.len());
	for (original_index, block) in blocks.iter().enumerate() {
		let range = block.source_range();
		if range.start > range.end || range.end > source.len() {
			return Err(IntervalError::OutOfBounds);
		}
		if !source.is_char_boundary(range.start) || !source.is_char_boundary(range.end) {
			return Err(IntervalError::NotUtf8Boundary);
		}
		if range.is_empty() {
			continue;
		}
		let (kind, heading_level) = block_kind(block);
		intervals.push(BlockInterval {
			start: range.start,
			end: range.end,
			kind,
			heading_level,
			original_index,
		});
	}
	intervals.sort_by_key(|interval| {
		(
			interval.start,
			Reverse(interval.end),
			interval.original_index,
		)
	});

	let mut maximal: Vec<BlockInterval> = Vec::with_capacity(intervals.len());
	for interval in intervals {
		let Some(previous) = maximal.last() else {
			maximal.push(interval);
			continue;
		};
		if interval.start >= previous.end {
			maximal.push(interval);
		} else if interval.end <= previous.end {
			// Equal and nested ranges are represented by their first, maximal
			// parser block. V1 deliberately does not expose nested block shape.
		} else {
			return Err(IntervalError::Crossing);
		}
	}

	let mut units = Vec::with_capacity(maximal.len().saturating_mul(2).saturating_add(1));
	let mut cursor = 0usize;
	for interval in maximal {
		if cursor < interval.start {
			units.push(CoveredUnit {
				start: cursor,
				end: interval.start,
				kind: MARKDOWN_GAP_KIND,
				heading_level: None,
			});
		}
		units.push(CoveredUnit {
			start: interval.start,
			end: interval.end,
			kind: interval.kind,
			heading_level: interval.heading_level,
		});
		cursor = interval.end;
	}
	if cursor < source.len() {
		units.push(CoveredUnit {
			start: cursor,
			end: source.len(),
			kind: MARKDOWN_GAP_KIND,
			heading_level: None,
		});
	}
	debug_assert!(units_partition(&units, source.len()));
	Ok(units)
}

fn block_kind(block: &MarkdownBlock) -> (ProviderKindId, Option<u8>) {
	match block {
		MarkdownBlock::Heading { level, .. } => {
			(MARKDOWN_HEADING_LINE_KIND, Some((*level).clamp(1, 6)))
		}
		MarkdownBlock::Paragraph { .. } => (MARKDOWN_PARAGRAPH_KIND, None),
		MarkdownBlock::CodeBlock { .. } => (MARKDOWN_CODE_BLOCK_KIND, None),
		MarkdownBlock::Quote { .. } => (MARKDOWN_QUOTE_KIND, None),
		MarkdownBlock::ListItem { .. } => (MARKDOWN_LIST_ITEM_KIND, None),
		MarkdownBlock::Rule { .. } => (MARKDOWN_RULE_KIND, None),
		MarkdownBlock::Table { .. } => (MARKDOWN_TABLE_KIND, None),
		MarkdownBlock::Html { .. } => (MARKDOWN_HTML_KIND, None),
	}
}

#[derive(Debug)]
struct Section {
	level: u8,
	children: Vec<GreenNode>,
}

fn fold_sections(units: Vec<CoveredUnit>, total_len: u32) -> GreenNode {
	let mut root_children = Vec::new();
	let mut sections: Vec<Section> = Vec::new();

	for unit in units {
		if let Some(level) = unit.heading_level {
			while sections
				.last()
				.is_some_and(|section| section.level >= level)
			{
				close_section(&mut sections, &mut root_children);
			}
			sections.push(Section {
				level,
				children: vec![block_node(MARKDOWN_HEADING_LINE_KIND, unit.len())],
			});
			continue;
		}

		let node = if unit.kind == MARKDOWN_GAP_KIND {
			leaf_node(MARKDOWN_GAP_KIND, unit.len())
		} else {
			block_node(unit.kind, unit.len())
		};
		append_to_active(node, &mut sections, &mut root_children);
	}

	while !sections.is_empty() {
		close_section(&mut sections, &mut root_children);
	}

	let root = GreenNode {
		kind: SemanticKind::RelationalTable,
		provider_kind: MARKDOWN_DOCUMENT_KIND,
		byte_len: total_len,
		children: root_children,
	};
	debug_assert!(green_lengths_are_exact(&root));
	root
}

fn close_section(sections: &mut Vec<Section>, root_children: &mut Vec<GreenNode>) {
	let section = sections.pop().expect("caller proves an open section");
	let byte_len = section.children.iter().map(|child| child.byte_len).sum();
	let node = GreenNode {
		kind: SemanticKind::RelationalTable,
		provider_kind: heading_kind(section.level),
		byte_len,
		children: section.children,
	};
	append_to_active(node, sections, root_children);
}

fn append_to_active(node: GreenNode, sections: &mut [Section], root_children: &mut Vec<GreenNode>) {
	if let Some(section) = sections.last_mut() {
		section.children.push(node);
	} else {
		root_children.push(node);
	}
}

const fn heading_kind(level: u8) -> ProviderKindId {
	match level {
		1 => MARKDOWN_H1_KIND,
		2 => MARKDOWN_H2_KIND,
		3 => MARKDOWN_H3_KIND,
		4 => MARKDOWN_H4_KIND,
		5 => MARKDOWN_H5_KIND,
		_ => MARKDOWN_H6_KIND,
	}
}

fn block_node(kind: ProviderKindId, len: usize) -> GreenNode {
	GreenNode {
		kind: SemanticKind::RelationalRow,
		provider_kind: kind,
		byte_len: len as u32,
		children: vec![leaf_node(ProviderKindId::NONE, len)],
	}
}

fn leaf_node(kind: ProviderKindId, len: usize) -> GreenNode {
	GreenNode {
		kind: SemanticKind::Token,
		provider_kind: kind,
		byte_len: len as u32,
		children: Vec::new(),
	}
}

fn flat_fallback(total_len: u32) -> GreenNode {
	GreenNode {
		kind: SemanticKind::RelationalTable,
		provider_kind: MARKDOWN_DOCUMENT_KIND,
		byte_len: total_len,
		children: (total_len > 0)
			.then(|| block_node(MARKDOWN_CONTENT_KIND, total_len as usize))
			.into_iter()
			.collect(),
	}
}

fn units_partition(units: &[CoveredUnit], source_len: usize) -> bool {
	let mut cursor = 0usize;
	for unit in units {
		if unit.start != cursor || unit.end <= unit.start || unit.end > source_len {
			return false;
		}
		cursor = unit.end;
	}
	cursor == source_len
}

fn green_lengths_are_exact(node: &GreenNode) -> bool {
	if node.children.is_empty() {
		return true;
	}
	let Some(children_len) = node
		.children
		.iter()
		.try_fold(0u32, |total, child| total.checked_add(child.byte_len))
	else {
		return false;
	};
	children_len == node.byte_len && node.children.iter().all(green_lengths_are_exact)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::DocByte;
	use crate::ecs::registry::UastRegistry;
	use crate::engine::structural_graft::apply_graft;
	use crate::svp::markdown_blocks::{ListMarker, MarkdownInlineText};
	use crate::uast::metrics::SpanMetrics;
	use crate::uast::projection::UastProjection;
	use std::fs;
	use std::path::{Path, PathBuf};

	fn assert_exact(source: &str) -> GreenNode {
		let blocks = parse_markdown_blocks(source);
		let units = cover_top_level_blocks(source, &blocks)
			.unwrap_or_else(|error| panic!("interval lowering failed for {source:?}: {error:?}"));
		assert!(
			units_partition(&units, source.len()),
			"intervals do not partition {source:?}: {units:?}"
		);
		let tree = fold_sections(units, source.len() as u32);
		assert_eq!(tree.byte_len as usize, source.len(), "{source:?}");
		assert!(
			green_lengths_are_exact(&tree),
			"Green lengths do not cover {source:?}: {tree:#?}"
		);
		tree
	}

	fn kinds_at_depth(node: &GreenNode, depth: usize, out: &mut Vec<(usize, ProviderKindId)>) {
		out.push((depth, node.provider_kind));
		for child in &node.children {
			kinds_at_depth(child, depth + 1, out);
		}
	}

	#[test]
	fn named_byte_hazards_partition_exactly() {
		let fixtures = [
			"",
			"\n\n\t\n",
			"\u{feff}# BOM\n",
			"# no trailing newline",
			"# CRLF\r\n\r\nbody\r\n",
			"```rust\nfn main() {}\n```\n",
			"```rust\nunterminated\n",
			"# Emoji 🦀\n\n段落 with é\n",
			"  leading paragraph\n\ntrailing  ",
		];
		for source in fixtures {
			assert_exact(source);
		}
	}

	#[test]
	fn block_type_zoo_and_nested_containers_partition_exactly() {
		let source = concat!(
			"# Zoo\n\n",
			"> quote\n>\n> ```rust\n> fn nested() {}\n> ```\n\n",
			"1. parent\n   - child\n     - grandchild\n2. sibling\n\n",
			"| a | b |\n| - | - |\n| 1 | 2 |\n\n",
			"<section>\nhtml\n</section>\n\n---\n",
		);
		assert_exact(source);
	}

	#[test]
	fn seeded_compositions_partition_exactly() {
		const FRAGMENTS: [&str; 12] = [
			"# H1\n",
			"### H3\r\n",
			"paragraph with 🦀\n",
			"\n\n",
			"- item\n",
			"1. ordered\r\n",
			"> quote\n",
			"```rust\nfn f() {}\n```\n",
			"```sql\nselect 1;\n",
			"| a | b |\n| - | - |\n| 1 | 2 |\n",
			"---\n",
			"<div>html</div>\n",
		];
		let mut state = 0x6d61726b646f776eu64;
		for case in 0..512 {
			let mut source = String::new();
			let count = 1 + (next_random(&mut state) as usize % 18);
			for _ in 0..count {
				let index = next_random(&mut state) as usize % FRAGMENTS.len();
				source.push_str(FRAGMENTS[index]);
			}
			let blocks = parse_markdown_blocks(&source);
			let units = cover_top_level_blocks(&source, &blocks).unwrap_or_else(|error| {
				panic!("seeded case {case}, state={state:#x}, input={source:?}: {error:?}")
			});
			assert!(
				units_partition(&units, source.len()),
				"seeded case {case}, state={state:#x}, input={source:?}, units={units:?}"
			);
			let tree = fold_sections(units, source.len() as u32);
			assert!(
				green_lengths_are_exact(&tree),
				"seeded case {case}, state={state:#x}, input={source:?}, tree={tree:#?}"
			);
		}
	}

	fn next_random(state: &mut u64) -> u64 {
		*state = state
			.wrapping_mul(6_364_136_223_846_793_005)
			.wrapping_add(1_442_695_040_888_963_407);
		*state
	}

	#[test]
	fn contained_nested_ranges_are_suppressed_but_crossing_ranges_fail_closed() {
		let nested = "- parent\n  - child\n";
		let blocks = parse_markdown_blocks(nested);
		let units = cover_top_level_blocks(nested, &blocks).expect("nested ranges are laminar");
		assert!(units_partition(&units, nested.len()));
		assert_eq!(
			units
				.iter()
				.filter(|unit| unit.kind == MARKDOWN_LIST_ITEM_KIND)
				.count(),
			1,
			"v1 retains the maximal parent item"
		);

		let crossing = vec![
			MarkdownBlock::Paragraph {
				text: MarkdownInlineText::new(),
				source_range: 0..4,
			},
			MarkdownBlock::ListItem {
				depth: 0,
				marker: ListMarker::Unordered,
				checked: None,
				text: MarkdownInlineText::new(),
				source_range: 2..6,
			},
		];
		assert_eq!(
			cover_top_level_blocks("abcdef", &crossing),
			Err(IntervalError::Crossing)
		);
		let fallback = lower_blocks("abcdef", &crossing, 6);
		assert_eq!(fallback.children[0].provider_kind, MARKDOWN_CONTENT_KIND);
		assert!(green_lengths_are_exact(&fallback));

		let non_boundary = vec![MarkdownBlock::Paragraph {
			text: MarkdownInlineText::new(),
			source_range: 1..4,
		}];
		let fallback = lower_blocks("éab", &non_boundary, 4);
		assert_eq!(fallback.children[0].provider_kind, MARKDOWN_CONTENT_KIND);
	}

	#[test]
	fn section_stack_handles_preamble_skips_and_trailing_headings() {
		let tree = assert_exact("preamble\n\n# One\n\nbody\n\n### Three\n\n- item\n\n## Two\n");
		let mut kinds = Vec::new();
		kinds_at_depth(&tree, 0, &mut kinds);
		assert!(kinds.contains(&(0, MARKDOWN_DOCUMENT_KIND)));
		assert!(kinds.contains(&(1, MARKDOWN_PARAGRAPH_KIND)));
		assert!(kinds.contains(&(1, MARKDOWN_H1_KIND)));
		assert!(kinds.contains(&(2, MARKDOWN_H3_KIND)));
		assert!(kinds.contains(&(2, MARKDOWN_H2_KIND)));

		let heading_only = assert_exact("## Only");
		assert_eq!(heading_only.children[0].provider_kind, MARKDOWN_H2_KIND);
		assert_eq!(
			heading_only.children[0].children[0].provider_kind,
			MARKDOWN_HEADING_LINE_KIND
		);
	}

	#[test]
	fn gap_before_a_heading_stays_on_the_closing_side() {
		let tree = assert_exact("# One\n\nbody\n\n## Two\n");
		let h1 = &tree.children[0];
		let h2 = h1
			.children
			.iter()
			.find(|child| child.provider_kind == MARKDOWN_H2_KIND)
			.expect("H2 nested under H1");
		assert_eq!(h2.children[0].provider_kind, MARKDOWN_HEADING_LINE_KIND);
		let child_before_h2 = h1
			.children
			.iter()
			.position(|child| child.provider_kind == MARKDOWN_H2_KIND)
			.and_then(|index| index.checked_sub(1))
			.map(|index| &h1.children[index])
			.expect("content before H2");
		assert_eq!(child_before_h2.provider_kind, MARKDOWN_GAP_KIND);
	}

	#[test]
	fn every_libaryon_plan_partitions_and_grafts_byte_exactly() {
		let plans = Path::new(env!("CARGO_MANIFEST_DIR")).join("plans");
		let mut files = Vec::new();
		collect_markdown_files(&plans, &mut files);
		files.sort();
		assert!(!files.is_empty(), "expected Markdown plans under {plans:?}");

		for path in files {
			let source = fs::read_to_string(&path)
				.unwrap_or_else(|error| panic!("read plan {path:?}: {error}"));
			let green = build_markdown_topology(&source)
				.unwrap_or_else(|| panic!("plan exceeds GreenNode extent: {path:?}"));
			assert!(
				green_lengths_are_exact(&green),
				"Green coverage failed for {path:?}"
			);

			let registry = UastRegistry::new(100_000);
			let newline_count = source.bytes().filter(|byte| *byte == b'\n').count() as u64;
			let metrics = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: newline_count,
			};
			let mut initial = registry.reserve_chunk(2).expect("initial plan tree");
			let root = initial.spawn_node(SemanticKind::RelationalTable, None, metrics);
			let leaf = initial.spawn_node(SemanticKind::Token, None, metrics);
			initial.append_local_child(root, leaf);
			drop(initial);
			registry.replace_authoritative_data(leaf, source.as_bytes().to_vec());

			apply_graft(&registry, root, &green, source.as_bytes())
				.unwrap_or_else(|error| panic!("graft plan {path:?}: {error}"));
			let after = registry
				.read_loaded_slice(root, DocByte::ZERO, DocByte::new(source.len() as u64))
				.unwrap_or_else(|error| panic!("read grafted plan {path:?}: {error:?}"));
			assert_eq!(after, source.as_bytes(), "graft changed bytes for {path:?}");
		}
	}

	fn collect_markdown_files(directory: &Path, files: &mut Vec<PathBuf>) {
		for entry in fs::read_dir(directory)
			.unwrap_or_else(|error| panic!("read plans directory {directory:?}: {error}"))
		{
			let path = entry.expect("read plans entry").path();
			if path.is_dir() {
				collect_markdown_files(&path, files);
			} else if path.extension().is_some_and(|extension| extension == "md") {
				files.push(path);
			}
		}
	}
}
