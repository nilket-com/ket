use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{GreenNode, LanguageProvider, SemanticOutput};
use crate::svp::python_ast;
use crate::svp::python_highlight::highlight_python_viewport;
use crate::svp::python_parse::parse_python_fragment;
use crate::svp::python_semantic;
use crate::svp::rust_highlight::HighlightSpan;
use crate::svp::sync::{ViewportChunk, find_safe_parse_boundaries};

pub struct PythonProvider;

impl LanguageProvider for PythonProvider {
	fn language_id(&self) -> &'static str {
		"python"
	}

	fn label(&self) -> &'static str {
		"Python"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["py", "pyi"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		match find_safe_parse_boundaries(viewport_bytes) {
			Some(bounds) => {
				let chunk = ViewportChunk {
					global_offset: DocByte::ZERO,
					buffer: viewport_bytes.to_vec(),
				};
				let tree = parse_python_fragment(&chunk, bounds, offset_map.to_vec());
				highlight_python_viewport(&tree)
			}
			None => Vec::new(),
		}
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		let parsed =
			ruff_python_parser::parse_unchecked(&full_source, ruff_python_parser::Mode::Module);
		let highlights = python_semantic::extract_semantic_highlights(&parsed, global_offset);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
		}
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		let text = std::str::from_utf8(source).ok()?;
		let parsed = ruff_python_parser::parse_unchecked(text, ruff_python_parser::Mode::Module);
		let green = python_ast::build_green_tree(&parsed, source.len() as u32);
		Some(python_ast::to_trait_green_node(&green))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::rust_highlight::TokenCategory;

	#[test]
	fn lexical_baseline_highlights_def_and_import() {
		let provider = PythonProvider;
		let source = b"import os\n\ndef main():\n    pass\n";
		let highlights = provider.lexical_baseline(source, &[(0, DocByte::ZERO)]);

		// "import" at byte 0..6 should be Keyword
		assert!(highlights.iter().any(|span| {
			span.category == TokenCategory::Keyword
				&& span.start == DocByte::ZERO
				&& span.end == DocByte::new(6)
		}));

		// "def" should be Keyword
		assert!(
			highlights
				.iter()
				.any(|span| { span.category == TokenCategory::Keyword && span.start.get() == 11 })
		);

		// "os" and "main" should be Variable (lexical fallback)
		assert!(
			highlights
				.iter()
				.any(|span| { span.category == TokenCategory::Variable && span.start.get() == 7 })
		);
	}

	#[test]
	fn plaintext_extension_is_not_claimed() {
		// PythonProvider should not handle .txt files
		assert!(!PythonProvider.extensions().contains(&"txt"));
	}
}
