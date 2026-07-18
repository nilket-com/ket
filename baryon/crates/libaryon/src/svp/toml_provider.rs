use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{GreenNode, LanguageProvider, SemanticOutput};
use crate::svp::rust_highlight::HighlightSpan;
use crate::svp::toml_highlight::highlight_toml;
use crate::svp::toml_semantic::build_toml_topology;
use crate::svp::cargo_annotations;

pub struct TomlProvider;

impl LanguageProvider for TomlProvider {
	fn language_id(&self) -> &'static str {
		"toml"
	}

	fn label(&self) -> &'static str {
		"TOML"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["toml"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_toml(viewport_bytes, offset_map)
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		file_path: &str,
	) -> SemanticOutput {
		// The lexical pass is already context-aware (bracket/brace depth,
		// key vs. value mode), so the reactor just re-runs it over the
		// full buffer to provide absolute-span highlights.
		let offset_map = [(0usize, global_offset)];
		let highlights = highlight_toml(full_source.as_bytes(), &offset_map);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
			inline_annotations: cargo_annotations::annotations(&full_source, file_path),
		}
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		Some(build_toml_topology(source))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::rust_highlight::TokenCategory;
	use crate::uast::kind::SemanticKind;

	#[test]
	fn provider_identifies_toml_extension() {
		assert_eq!(TomlProvider.language_id(), "toml");
		assert_eq!(TomlProvider.extensions(), &["toml"]);
	}

	#[test]
	fn lexical_baseline_classifies_key_and_value() {
		let provider = TomlProvider;
		let src = b"name = \"baryon\"\n";
		let spans = provider.lexical_baseline(src, &[(0, DocByte::ZERO)]);
		assert!(
			spans
				.iter()
				.any(|s| s.category == TokenCategory::Function && s.start == DocByte::ZERO)
		);
		assert!(
			spans
				.iter()
				.any(|s| s.category == TokenCategory::String && s.start == DocByte::new(7))
		);
	}

	#[test]
	fn structural_topology_preserves_byte_length() {
		let provider = TomlProvider;
		let src = b"[package]\nname = \"baryon\"\n\n[dependencies]\nfoo = \"1.0\"\n";
		let tree = provider
			.parse_structural_topology(src)
			.expect("toml provider always returns a topology");
		assert_eq!(tree.byte_len as usize, src.len());
		assert_eq!(tree.kind, SemanticKind::RelationalTable);
	}

	#[test]
	fn semantic_reactor_applies_global_offset() {
		let provider = TomlProvider;
		let src = Arc::new("port = 8080\n".to_string());
		let output = provider.semantic_reactor(src, DocByte::new(1000), "config.toml");
		assert!(output.diagnostics.is_empty());
		assert!(
			output
				.highlights
				.iter()
				.any(|s| s.start == DocByte::new(1000))
		);
	}
}
