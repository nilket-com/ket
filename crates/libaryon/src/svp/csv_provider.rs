use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{GreenNode, LanguageProvider, SemanticOutput};
use crate::svp::csv_highlight::highlight_csv;
use crate::svp::csv_parse::{CSV_DELIMITER, TSV_DELIMITER};
use crate::svp::csv_semantic::build_csv_topology;
use crate::svp::rust_highlight::HighlightSpan;

pub struct CsvProvider;
pub struct TsvProvider;

impl LanguageProvider for CsvProvider {
	fn language_id(&self) -> &'static str {
		"csv"
	}

	fn label(&self) -> &'static str {
		"CSV"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["csv"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_csv(viewport_bytes, CSV_DELIMITER, offset_map)
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		let offset_map = [(0usize, global_offset)];
		let highlights = highlight_csv(full_source.as_bytes(), CSV_DELIMITER, &offset_map);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
		}
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		Some(build_csv_topology(source, CSV_DELIMITER))
	}
}

impl LanguageProvider for TsvProvider {
	fn language_id(&self) -> &'static str {
		"tsv"
	}

	fn label(&self) -> &'static str {
		"TSV"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["tsv"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_csv(viewport_bytes, TSV_DELIMITER, offset_map)
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		let offset_map = [(0usize, global_offset)];
		let highlights = highlight_csv(full_source.as_bytes(), TSV_DELIMITER, &offset_map);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
		}
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		Some(build_csv_topology(source, TSV_DELIMITER))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::rust_highlight::TokenCategory;
	use crate::uast::kind::SemanticKind;

	#[test]
	fn csv_provider_identifies_csv_extension() {
		assert_eq!(CsvProvider.language_id(), "csv");
		assert_eq!(CsvProvider.extensions(), &["csv"]);
	}

	#[test]
	fn tsv_provider_identifies_tsv_extension() {
		assert_eq!(TsvProvider.language_id(), "tsv");
		assert_eq!(TsvProvider.extensions(), &["tsv"]);
	}

	#[test]
	fn csv_lexical_baseline_alternates_columns() {
		let provider = CsvProvider;
		let src = b"name,age\ntest,38\n";
		let spans = provider.lexical_baseline(src, &[(0, DocByte::ZERO)]);
		assert!(spans.iter().any(|s| s.category == TokenCategory::Function));
		assert!(spans.iter().any(|s| s.category == TokenCategory::Type));
		assert!(spans.iter().any(|s| s.category == TokenCategory::Number));
	}

	#[test]
	fn tsv_lexical_baseline_uses_tab_delimiter() {
		let provider = TsvProvider;
		let src = b"name\tage\ntest\t38\n";
		let spans = provider.lexical_baseline(src, &[(0, DocByte::ZERO)]);
		let puncts = spans
			.iter()
			.filter(|s| s.category == TokenCategory::Punctuation)
			.count();
		assert_eq!(puncts, 2, "one tab per record");
	}

	#[test]
	fn structural_topology_preserves_byte_length() {
		let src = b"a,b,c\nd,e,f\n";
		let tree = CsvProvider
			.parse_structural_topology(src)
			.expect("csv provider always returns a topology");
		assert_eq!(tree.byte_len as usize, src.len());
		assert_eq!(tree.kind, SemanticKind::RelationalTable);
	}

	#[test]
	fn semantic_reactor_applies_global_offset() {
		let provider = CsvProvider;
		let src = Arc::new("a,b\n".to_string());
		let output = provider.semantic_reactor(src, DocByte::new(500), "data.csv");
		assert!(output.diagnostics.is_empty());
		assert!(
			output
				.highlights
				.iter()
				.any(|s| s.start == DocByte::new(500))
		);
	}
}
