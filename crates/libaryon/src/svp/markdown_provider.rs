use std::ops::Range;
use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{GreenNode, LanguageInjection, LanguageProvider, SemanticOutput};
use crate::svp::markdown_highlight::highlight_markdown;
use crate::svp::markdown_semantic::markdown_semantic_highlights;
use crate::svp::markdown_topology::{
	build_markdown_topology, markdown_kind_is_trivia, markdown_kind_name,
};
use crate::svp::rust_highlight::HighlightSpan;
use crate::uast::kind::ProviderKindId;
use crate::uast::projection::{
	MarkdownCellCodeVisibility, MarkdownCellOutputTarget, MarkdownCellPresentation,
};

pub struct MarkdownProvider;

/// One complete fenced block in a Markdown document. Attributes are parsed
/// once here and reused by highlighting, preview projection, and later cell
/// execution; consumers must not each invent their own fence parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FencedCodeBlock {
	pub(crate) language_id: String,
	pub(crate) body_range: Range<usize>,
	pub(crate) explicit_id: Option<String>,
	pub(crate) presentation: MarkdownCellPresentation,
	pub(crate) occurrence: u32,
}

impl LanguageProvider for MarkdownProvider {
	fn language_id(&self) -> &'static str {
		"markdown"
	}

	fn label(&self) -> &'static str {
		"Markdown"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["md", "markdown"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_markdown(viewport_bytes, offset_map)
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		// Authoritative, whole-document: this pass overrules the viewport
		// lexer's guesses, including its false emphasis inside `snake_case`.
		let highlights = markdown_semantic_highlights(&full_source, global_offset);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
		}
	}

	fn injections(&self, source: &str) -> Vec<LanguageInjection> {
		fenced_code_injections(source)
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		build_markdown_topology(std::str::from_utf8(source).ok()?)
	}

	fn structural_kind_name(&self, kind: ProviderKindId) -> Option<String> {
		markdown_kind_name(kind).map(str::to_owned)
	}

	fn structural_kind_is_trivia(&self, kind: ProviderKindId) -> bool {
		markdown_kind_is_trivia(kind)
	}
}

/// Parse fenced code blocks without interpreting their contents. The info
/// string is structured as `language` followed by attributes; `{#trips}` is
/// retained as an explicit cell identity while unknown attributes remain inert.
pub(crate) fn fenced_code_blocks(source: &str) -> Vec<FencedCodeBlock> {
	let mut blocks = Vec::new();
	let mut offset = 0usize;
	let mut open: Option<(
		u8,
		usize,
		String,
		Option<String>,
		MarkdownCellPresentation,
		usize,
	)> = None;
	for line in source.split_inclusive('\n') {
		let line_start = offset;
		offset = offset.saturating_add(line.len());
		let trimmed = line.trim_start_matches([' ', '\t']);
		let bytes = trimmed.as_bytes();
		let marker = bytes.first().copied();
		let count = marker.map_or(0, |marker| {
			bytes.iter().take_while(|byte| **byte == marker).count()
		});
		if count < 3 || !matches!(marker, Some(b'`' | b'~')) {
			continue;
		}
		let marker = marker.expect("count proves marker");
		if let Some((open_marker, open_count, language_id, explicit_id, presentation, body_start)) =
			open.take()
		{
			if marker == open_marker && count >= open_count {
				blocks.push(FencedCodeBlock {
					language_id,
					body_range: body_start..line_start,
					explicit_id,
					presentation,
					occurrence: blocks.len() as u32,
				});
			} else {
				open = Some((
					open_marker,
					open_count,
					language_id,
					explicit_id,
					presentation,
					body_start,
				));
			}
			continue;
		}

		let info = trimmed[count..].trim();
		let Some(language_id) = info.split_whitespace().next().filter(|id| !id.is_empty()) else {
			continue;
		};
		let (explicit_id, presentation) = parse_fence_attributes(&info[language_id.len()..]);
		open = Some((
			marker,
			count,
			language_id.to_owned(),
			explicit_id,
			presentation,
			offset,
		));
	}
	blocks
}

/// Parse comma-delimited attributes inside one or more `{...}` groups.
/// Unknown entries remain inert so the grammar can grow without changing the
/// highlighter or breaking documents written by newer Polariton versions.
fn parse_fence_attributes(info: &str) -> (Option<String>, MarkdownCellPresentation) {
	let mut explicit_id = None;
	let mut presentation = MarkdownCellPresentation::default();
	let mut remaining = info;
	while let Some(start) = remaining.find('{') {
		let Some(end) = remaining[start + 1..].find('}') else {
			break;
		};
		let attributes = &remaining[start + 1..start + 1 + end];
		for attribute in attributes.split(',').map(str::trim) {
			if let Some(id) = attribute.strip_prefix('#').filter(|id| !id.is_empty()) {
				explicit_id = Some(id.to_owned());
				continue;
			}
			let Some((key, value)) = attribute.split_once('=') else {
				continue;
			};
			match (key.trim(), value.trim()) {
				("output", "inline") => presentation.output = MarkdownCellOutputTarget::Inline,
				("output", "pane") => presentation.output = MarkdownCellOutputTarget::Pane,
				("output", "none") => presentation.output = MarkdownCellOutputTarget::None,
				("code", "show") => presentation.code = MarkdownCellCodeVisibility::Show,
				("code", "hide") => presentation.code = MarkdownCellCodeVisibility::Hide,
				_ => {}
			}
		}
		remaining = &remaining[start + 1 + end + 1..];
	}
	(explicit_id, presentation)
}

/// Adapt structured fenced blocks to the generic language-injection contract.
pub(crate) fn fenced_code_injections(source: &str) -> Vec<LanguageInjection> {
	fenced_code_blocks(source)
		.into_iter()
		.map(|block| LanguageInjection {
			language_id: block.language_id,
			range: block.body_range,
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::markdown_topology::{
		MARKDOWN_CODE_BLOCK_KIND, MARKDOWN_CONTENT_KIND, MARKDOWN_DOCUMENT_KIND, MARKDOWN_GAP_KIND,
		MARKDOWN_H1_KIND, MARKDOWN_H2_KIND, MARKDOWN_H3_KIND, MARKDOWN_H4_KIND, MARKDOWN_H5_KIND,
		MARKDOWN_H6_KIND, MARKDOWN_HEADING_LINE_KIND, MARKDOWN_HTML_KIND, MARKDOWN_LIST_ITEM_KIND,
		MARKDOWN_PARAGRAPH_KIND, MARKDOWN_QUOTE_KIND, MARKDOWN_RULE_KIND, MARKDOWN_TABLE_KIND,
	};
	use crate::svp::rust_highlight::TokenCategory;

	#[test]
	fn provider_identifies_markdown_extensions() {
		assert_eq!(MarkdownProvider.language_id(), "markdown");
		assert_eq!(MarkdownProvider.extensions(), &["md", "markdown"]);
	}

	#[test]
	fn lexical_baseline_highlights_markdown_constructs() {
		let provider = MarkdownProvider;
		let src = b"# Title\n\n- [Baryon](https://example.com)\n";
		let spans = provider.lexical_baseline(src, &[(0, DocByte::ZERO)]);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Type)
		);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Function)
		);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::String)
		);
	}

	#[test]
	fn semantic_reactor_applies_global_offset() {
		let provider = MarkdownProvider;
		let src = Arc::new("# Title\n".to_string());
		let output = provider.semantic_reactor(src, DocByte::new(1000), "README.md");
		assert!(output.diagnostics.is_empty());
		assert!(
			output
				.highlights
				.iter()
				.any(|span| span.start == DocByte::new(1000))
		);
	}

	#[test]
	fn structural_kinds_round_trip_and_only_gap_is_trivia() {
		let provider = MarkdownProvider;
		let kinds = [
			(MARKDOWN_DOCUMENT_KIND, "DOCUMENT"),
			(MARKDOWN_H1_KIND, "H1"),
			(MARKDOWN_H2_KIND, "H2"),
			(MARKDOWN_H3_KIND, "H3"),
			(MARKDOWN_H4_KIND, "H4"),
			(MARKDOWN_H5_KIND, "H5"),
			(MARKDOWN_H6_KIND, "H6"),
			(MARKDOWN_HEADING_LINE_KIND, "HEADING_LINE"),
			(MARKDOWN_PARAGRAPH_KIND, "PARAGRAPH"),
			(MARKDOWN_CODE_BLOCK_KIND, "CODE_BLOCK"),
			(MARKDOWN_QUOTE_KIND, "QUOTE"),
			(MARKDOWN_LIST_ITEM_KIND, "LIST_ITEM"),
			(MARKDOWN_RULE_KIND, "RULE"),
			(MARKDOWN_TABLE_KIND, "TABLE"),
			(MARKDOWN_HTML_KIND, "HTML"),
			(MARKDOWN_GAP_KIND, "GAP"),
			(MARKDOWN_CONTENT_KIND, "CONTENT"),
		];
		for (kind, name) in kinds {
			assert_eq!(provider.structural_kind_name(kind).as_deref(), Some(name));
			assert_eq!(
				provider.structural_kind_is_trivia(kind),
				kind == MARKDOWN_GAP_KIND
			);
		}
		assert_eq!(provider.structural_kind_name(ProviderKindId::NONE), None);
		assert_eq!(
			provider.structural_kind_name(ProviderKindId::new(u16::MAX)),
			None
		);
		assert!(!provider.structural_kind_is_trivia(ProviderKindId::NONE));
		assert!(!provider.structural_kind_is_trivia(ProviderKindId::new(u16::MAX)));
	}

	#[test]
	fn fenced_blocks_expose_language_and_body_byte_range() {
		let source = "# Query\n\n```sql {#trips} {.wide}\nselect 42;\n```\n\n```rune\n1 + 1\n```\n";
		let blocks = fenced_code_blocks(source);
		assert_eq!(blocks.len(), 2);
		assert_eq!(blocks[0].language_id, "sql");
		assert_eq!(blocks[0].explicit_id.as_deref(), Some("trips"));
		assert_eq!(blocks[0].occurrence, 0);
		assert_eq!(&source[blocks[0].body_range.clone()], "select 42;\n");
		assert_eq!(blocks[1].language_id, "rune");
		assert_eq!(blocks[1].explicit_id, None);
		assert_eq!(blocks[1].occurrence, 1);

		let injections = fenced_code_injections(source);
		assert_eq!(
			injections
				.iter()
				.map(|injection| injection.language_id.as_str())
				.collect::<Vec<_>>(),
			vec!["sql", "rune"]
		);
	}

	#[test]
	fn fenced_blocks_parse_comma_delimited_cell_presentation() {
		let source = "```sql {#sales, output=pane, code=hide}\nselect 42;\n```\n";
		let blocks = fenced_code_blocks(source);
		let block = blocks.first().expect("one fence");
		assert_eq!(block.explicit_id.as_deref(), Some("sales"));
		assert_eq!(block.presentation.output, MarkdownCellOutputTarget::Pane);
		assert_eq!(block.presentation.code, MarkdownCellCodeVisibility::Hide);

		// Unknown attributes are deliberately inert and bare fences retain the
		// notebook defaults rather than requiring annotations.
		let defaults = fenced_code_blocks("```sql {future=whatever}\nselect 1;\n```\n");
		assert_eq!(
			defaults[0].presentation,
			MarkdownCellPresentation::default()
		);
	}
}
