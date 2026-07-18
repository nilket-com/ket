use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{LanguageProvider, SemanticOutput};
use crate::svp::bash_highlight::highlight_bash;
use crate::svp::rust_highlight::HighlightSpan;

pub struct BashProvider;

impl LanguageProvider for BashProvider {
	fn language_id(&self) -> &'static str {
		"bash"
	}

	fn label(&self) -> &'static str {
		"Bash"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["sh", "bash", "bats"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_bash(viewport_bytes, offset_map)
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		let offset_map = [(0usize, global_offset)];
		SemanticOutput {
			highlights: highlight_bash(full_source.as_bytes(), &offset_map),
			diagnostics: Vec::new(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::engine::language::LanguageRegistry;
	use crate::svp::rust_highlight::TokenCategory;

	#[test]
	fn provider_identifies_bash_extensions() {
		assert_eq!(BashProvider.language_id(), "bash");
		assert_eq!(BashProvider.label(), "Bash");
		assert_eq!(BashProvider.extensions(), &["sh", "bash", "bats"]);
	}

	#[test]
	fn registry_resolves_ids_extensions_and_completion_names() {
		let mut registry = LanguageRegistry::new();
		registry.register(BashProvider);
		for name in ["bash", "sh", "bats"] {
			assert_eq!(registry.resolve_language_id(name), Some("bash"));
		}
		assert_eq!(registry.completion_candidates(), vec!["bash", "bats", "sh"]);
	}

	#[test]
	fn lexical_baseline_highlights_a_script() {
		let source = b"#!/usr/bin/env bash\nif test -f file; then echo ok; fi\n";
		let highlights = BashProvider.lexical_baseline(source, &[(0, DocByte::ZERO)]);
		for category in [
			TokenCategory::Attribute,
			TokenCategory::Keyword,
			TokenCategory::Function,
			TokenCategory::Operator,
		] {
			assert!(
				highlights.iter().any(|span| span.category == category),
				"missing {category:?}"
			);
		}
	}

	#[test]
	fn semantic_reactor_applies_global_offset_without_diagnostics() {
		let source = Arc::new("name=world\necho $name\n".to_string());
		let output = BashProvider.semantic_reactor(source, DocByte::new(1000), "script.sh");
		assert!(output.diagnostics.is_empty());
		assert!(
			output
				.highlights
				.iter()
				.all(|span| span.start >= DocByte::new(1000))
		);
	}

	#[test]
	fn structural_topology_is_deliberately_deferred() {
		assert!(
			BashProvider
				.parse_structural_topology(b"if true; then echo ok; fi\n")
				.is_none()
		);
	}
}
