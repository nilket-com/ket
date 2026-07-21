use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{LanguageInjection, LanguageProvider, SemanticOutput};
use crate::svp::markdown_highlight::highlight_markdown;
use crate::svp::rust_highlight::HighlightSpan;

pub struct MarkdownProvider;

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

/// Parse fenced code bodies without interpreting their contents. Fence info is
/// intentionally structured as `language` plus ignored trailing attributes,
/// so `sql {#trips}` works today and gives notebook cell identity a compatible
/// home later.
pub(crate) fn fenced_code_injections(source: &str) -> Vec<LanguageInjection> {
	let mut injections = Vec::new();
	let mut offset = 0usize;
	let mut open: Option<(u8, usize, String, usize)> = None;
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
		if let Some((open_marker, open_count, language_id, body_start)) = open.take() {
			if marker == open_marker && count >= open_count {
				injections.push(LanguageInjection {
					language_id,
					range: body_start..line_start,
				});
			} else {
				open = Some((open_marker, open_count, language_id, body_start));
			}
			continue;
		}

		let info = trimmed[count..].trim();
		let Some(language_id) = info.split_whitespace().next().filter(|id| !id.is_empty()) else {
			continue;
		};
		open = Some((marker, count, language_id.to_owned(), offset));
	}
	injections
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
		let source = "# Query\n\n```sql {#trips}\nselect 42;\n```\n";
		let injections = fenced_code_injections(source);
		assert_eq!(injections.len(), 1);
		assert_eq!(injections[0].language_id, "sql");
		assert_eq!(&source[injections[0].range.clone()], "select 42;\n");
	}
}
