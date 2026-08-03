use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{GreenNode, LanguageProvider, SemanticOutput};
use crate::svp::rune_ast;
use crate::svp::rune_highlight::highlight_rune_viewport;
use crate::svp::rune_parse::parse_rune_fragment;
use crate::svp::rune_semantic;
use crate::svp::rust_highlight::HighlightSpan;
use crate::svp::sync::{ViewportChunk, find_safe_parse_boundaries};
use crate::uast::kind::ProviderKindId;

pub struct RuneProvider;

impl LanguageProvider for RuneProvider {
	fn language_id(&self) -> &'static str {
		"rune"
	}

	fn label(&self) -> &'static str {
		"Rune"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["rn"]
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
				let tree = parse_rune_fragment(&chunk, bounds, offset_map.to_vec());
				highlight_rune_viewport(&tree)
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
		let highlights = match rune_ast::try_parse_rune_document(&full_source) {
			Some(document) => rune_semantic::extract_semantic_highlights(&document, global_offset),
			None => Vec::new(),
		};
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
		}
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		let text = std::str::from_utf8(source).ok()?;
		let green = rune_ast::build_green_tree(text, source.len() as u32);
		Some(rune_ast::to_trait_green_node(&green))
	}

	fn structural_kind_name(&self, kind: ProviderKindId) -> Option<String> {
		rune_ast::RuneSyntaxKind::name(kind)
	}

	fn structural_kind_is_trivia(&self, kind: ProviderKindId) -> bool {
		kind == rune_ast::RuneSyntaxKind::Trivia.provider_kind()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::rust_highlight::TokenCategory;

	#[test]
	fn lexical_baseline_highlights_fn_and_let() {
		let provider = RuneProvider;
		let source = b"fn main() { let x = 42; }";
		let highlights = provider.lexical_baseline(source, &[(0, DocByte::ZERO)]);

		assert!(
			highlights
				.iter()
				.any(|s| s.category == TokenCategory::Keyword && s.start == DocByte::ZERO)
		);
		assert!(
			highlights
				.iter()
				.any(|s| s.category == TokenCategory::Number)
		);
		assert!(
			highlights
				.iter()
				.any(|s| s.category == TokenCategory::Variable)
		);
	}

	#[test]
	fn semantic_reactor_highlights_item_files_and_workspace_statement_buffers() {
		let provider = RuneProvider;
		let workspace = r#"workspace
	.table("test2")
	.sort([col("cat").asc()])
	.views([
		scatter("cat", "val"),
		bar("cat", "val"),
		table(),
	])"#;
		for (source, expected_category, expected_text) in [
			(
				"pub fn main() { let x = make(); }",
				TokenCategory::Function,
				"main",
			),
			(workspace, TokenCategory::Method, "views"),
		] {
			let output =
				provider.semantic_reactor(Arc::new(source.to_owned()), DocByte::ZERO, "test.rn");
			assert!(output.highlights.iter().any(|span| {
				let start = u64::from(span.start) as usize;
				let end = u64::from(span.end) as usize;
				span.category == expected_category && &source[start..end] == expected_text
			}));
		}
	}

	#[test]
	fn malformed_statement_buffer_keeps_the_lexical_fallback() {
		let provider = RuneProvider;
		let source = "workspace.table(\"test2\"";
		let semantic =
			provider.semantic_reactor(Arc::new(source.to_owned()), DocByte::ZERO, "broken.rn");
		assert!(semantic.highlights.is_empty());
		assert!(
			!provider
				.lexical_baseline(source.as_bytes(), &[(0, DocByte::ZERO)])
				.is_empty()
		);
	}
}
