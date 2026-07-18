use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{LanguageProvider, SemanticOutput};
use crate::svp::html_highlight::highlight_html;
use crate::svp::rust_highlight::HighlightSpan;

pub struct HtmlProvider;

impl LanguageProvider for HtmlProvider {
	fn language_id(&self) -> &'static str {
		"html"
	}

	fn label(&self) -> &'static str {
		"HTML"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["html", "htm"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_html(viewport_bytes, offset_map)
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		let offset_map = [(0usize, global_offset)];
		let highlights = highlight_html(full_source.as_bytes(), &offset_map);
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
	fn provider_identifies_html_extensions() {
		assert_eq!(HtmlProvider.language_id(), "html");
		assert_eq!(HtmlProvider.extensions(), &["html", "htm"]);
	}

	#[test]
	fn lexical_baseline_highlights_html_constructs() {
		let provider = HtmlProvider;
		let src = br#"<!doctype html>
<main class="app">&amp;</main>
"#;
		let spans = provider.lexical_baseline(src, &[(0, DocByte::ZERO)]);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Keyword)
		);
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
				.any(|span| span.category == TokenCategory::Constant)
		);
	}

	#[test]
	fn semantic_reactor_applies_global_offset() {
		let provider = HtmlProvider;
		let src = Arc::new("<p>hello</p>".to_string());
		let output = provider.semantic_reactor(src, DocByte::new(1000), "index.html");
		assert!(output.diagnostics.is_empty());
		assert!(
			output
				.highlights
				.iter()
				.any(|span| span.start == DocByte::new(1000))
		);
	}

	#[test]
	fn structural_topology_defaults_to_none() {
		let provider = HtmlProvider;
		assert!(
			provider
				.parse_structural_topology(b"<main></main>")
				.is_none()
		);
	}
}
