use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{LanguageProvider, SemanticOutput};
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
		}
	}
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
}
