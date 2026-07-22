use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{LanguageProvider, SemanticOutput};
use crate::svp::json_highlight::highlight_json;
use crate::svp::rust_highlight::HighlightSpan;

pub struct JsonProvider;

impl LanguageProvider for JsonProvider {
	fn language_id(&self) -> &'static str {
		"json"
	}

	fn label(&self) -> &'static str {
		"JSON"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["json", "ipynb"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_json(viewport_bytes, offset_map)
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		let offset_map = [(0usize, global_offset)];
		let highlights = highlight_json(full_source.as_bytes(), &offset_map);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::engine::language::LanguageRegistry;
	use crate::svp::rust_highlight::TokenCategory;

	#[test]
	fn provider_identifies_json_extension() {
		assert_eq!(JsonProvider.language_id(), "json");
		assert_eq!(JsonProvider.label(), "JSON");
		assert_eq!(JsonProvider.extensions(), &["json", "ipynb"]);
	}

	#[test]
	fn registry_resolves_ipynb_extension_to_json() {
		let mut registry = LanguageRegistry::new();
		registry.register(JsonProvider);

		assert_eq!(registry.resolve_language_id("ipynb"), Some("json"));
	}

	#[test]
	fn lexical_baseline_highlights_json_constructs() {
		let provider = JsonProvider;
		let src = br#"{"name":"baryon","count":2,"ok":true}"#;
		let spans = provider.lexical_baseline(src, &[(0, DocByte::ZERO)]);
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
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Number)
		);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Constant)
		);
	}

	#[test]
	fn semantic_reactor_applies_global_offset() {
		let provider = JsonProvider;
		let src = Arc::new(r#"{"name":"baryon"}"#.to_string());
		let output = provider.semantic_reactor(src, DocByte::new(1000), "settings.json");
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
		let provider = JsonProvider;
		assert!(
			provider
				.parse_structural_topology(br#"{"name":"baryon"}"#)
				.is_none()
		);
	}
}
