use std::ops::Range;
use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{LanguageInjection, LanguageProvider, SemanticOutput};
use crate::svp::markdown_highlight::highlight_markdown;
use crate::svp::rust_highlight::HighlightSpan;

pub struct MarkdownProvider;

/// One complete fenced block in a Markdown document. Attributes are parsed
/// once here and reused by highlighting, preview projection, and later cell
/// execution; consumers must not each invent their own fence parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FencedCodeBlock {
	pub(crate) language_id: String,
	pub(crate) body_range: Range<usize>,
	pub(crate) explicit_id: Option<String>,
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
		let offset_map = [(0usize, global_offset)];
		let highlights = highlight_markdown(full_source.as_bytes(), &offset_map);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
		}
	}

	fn injections(&self, source: &str) -> Vec<LanguageInjection> {
		fenced_code_injections(source)
	}
}

/// Parse fenced code blocks without interpreting their contents. The info
/// string is structured as `language` followed by attributes; `{#trips}` is
/// retained as an explicit cell identity while unknown attributes remain inert.
pub(crate) fn fenced_code_blocks(source: &str) -> Vec<FencedCodeBlock> {
	let mut blocks = Vec::new();
	let mut offset = 0usize;
	let mut open: Option<(u8, usize, String, Option<String>, usize)> = None;
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
		if let Some((open_marker, open_count, language_id, explicit_id, body_start)) = open.take() {
			if marker == open_marker && count >= open_count {
				blocks.push(FencedCodeBlock {
					language_id,
					body_range: body_start..line_start,
					explicit_id,
					occurrence: blocks.len() as u32,
				});
			} else {
				open = Some((
					open_marker,
					open_count,
					language_id,
					explicit_id,
					body_start,
				));
			}
			continue;
		}

		let info = trimmed[count..].trim();
		let mut fields = info.split_whitespace();
		let Some(language_id) = fields.next().filter(|id| !id.is_empty()) else {
			continue;
		};
		let explicit_id = fields.find_map(|attribute| {
			attribute
				.strip_prefix("{#")
				.and_then(|id| id.strip_suffix('}'))
				.filter(|id| !id.is_empty())
				.map(str::to_owned)
		});
		open = Some((marker, count, language_id.to_owned(), explicit_id, offset));
	}
	blocks
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
}
