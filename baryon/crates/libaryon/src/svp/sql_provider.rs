use std::sync::Arc;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

use crate::core::DocByte;
use crate::engine::language::{GreenNode, LanguageProvider, SemanticOutput};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use crate::svp::sql_highlight::classify_sql_token;
use crate::svp::sql_parse::{LineIndex, overlaps_macro, shadow_mask_sql, translate_viewport_span};
use crate::svp::sql_semantic::{
	build_structural_topology, extract_semantic_highlights, flat_topology,
};

pub struct SqlProvider;

impl LanguageProvider for SqlProvider {
	fn language_id(&self) -> &'static str {
		"sql"
	}

	fn label(&self) -> &'static str {
		"SQL"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["sql", "dbt", "j2"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		let masked = shadow_mask_sql(viewport_bytes);
		let mut highlights = masked
			.macro_spans
			.iter()
			.map(|macro_span| {
				let (start, end) = translate_viewport_span(macro_span.span, offset_map);
				HighlightSpan::new(start, end, TokenCategory::Macro)
			})
			.collect::<Vec<_>>();

		let shadow = match String::from_utf8(masked.shadow_bytes.clone()) {
			Ok(shadow) => shadow,
			Err(_) => {
				highlights.sort_unstable_by_key(|span| span.start);
				return highlights;
			}
		};

		let dialect = GenericDialect {};
		let tokens = match Tokenizer::new(&dialect, &shadow).tokenize_with_location() {
			Ok(tokens) => tokens,
			Err(_) => {
				highlights.sort_unstable_by_key(|span| span.start);
				return highlights;
			}
		};

		let line_index = LineIndex::new(&shadow);

		for token in tokens {
			let category = classify_sql_token(&token.token);
			if category == TokenCategory::Unclassified {
				continue;
			}

			let Some(local_span) = line_index.span_to_byte_span(token.span) else {
				continue;
			};
			if overlaps_macro(local_span.start, local_span.end, &masked.macro_spans) {
				continue;
			}

			let (start, end) = translate_viewport_span(local_span, offset_map);
			highlights.push(HighlightSpan::new(start, end, category));
		}

		highlights.sort_unstable_by_key(|span| span.start);
		highlights
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		let masked = shadow_mask_sql(full_source.as_bytes());
		let shadow = match String::from_utf8(masked.shadow_bytes) {
			Ok(shadow) => shadow,
			Err(_) => return SemanticOutput::default(),
		};

		let dialect = GenericDialect {};
		let statements = match Parser::parse_sql(&dialect, &shadow) {
			Ok(statements) => statements,
			Err(_) => return SemanticOutput::default(),
		};

		let line_index = LineIndex::new(&shadow);
		let highlights = extract_semantic_highlights(&statements, &line_index, global_offset);
		SemanticOutput {
			highlights,
			diagnostics: Vec::new(),
		}
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		let masked = shadow_mask_sql(source);
		let shadow = String::from_utf8(masked.shadow_bytes).ok()?;
		let dialect = GenericDialect {};
		let line_index = LineIndex::new(&shadow);

		let green = match Parser::parse_sql(&dialect, &shadow) {
			Ok(statements) => {
				build_structural_topology(&statements, &line_index, source.len() as u32)
			}
			Err(_) => flat_topology(source.len() as u32),
		};

		Some(green)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lexical_baseline_highlights_sql_and_jinja_macros() {
		let provider = SqlProvider;
		let source = b"select {{ ref('orders') }} as model\nfrom users\n";
		let highlights = provider.lexical_baseline(source, &[(0, DocByte::ZERO)]);

		assert!(highlights.iter().any(|span| {
			span.category == TokenCategory::Keyword
				&& span.start == DocByte::ZERO
				&& span.end == DocByte::new(6)
		}));
		assert!(
			highlights
				.iter()
				.any(|span| span.category == TokenCategory::Macro)
		);
		assert!(
			highlights
				.iter()
				.any(|span| span.category == TokenCategory::Variable)
		);
	}

	#[test]
	fn structural_topology_falls_back_to_flat_tree_on_parse_error() {
		let provider = SqlProvider;
		let source = b"select * from {% if prod %}";
		let green = provider
			.parse_structural_topology(source)
			.expect("sql provider should always return a fallback topology");

		assert_eq!(green.byte_len, source.len() as u32);
		assert_eq!(green.children.len(), 1);
		assert_eq!(
			green.children[0].kind,
			crate::uast::kind::SemanticKind::Token
		);
	}

	#[test]
	fn sql_provider_claims_templated_extensions() {
		assert!(SqlProvider.extensions().contains(&"sql"));
		assert!(SqlProvider.extensions().contains(&"dbt"));
		assert!(SqlProvider.extensions().contains(&"j2"));
	}
}
