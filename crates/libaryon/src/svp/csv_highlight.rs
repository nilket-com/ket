//! CSV/TSV highlighter — alternating column colors so the record
//! structure reads at a glance without full grid alignment.
//!
//! The plan initially specified `Function`/`Variable` for alternating
//! columns but `Variable` collapses to foreground in most bundled
//! themes (this was the TOML key issue). `Function`/`Type` is used
//! instead: both categories are visually distinct in every theme.

use crate::core::DocByte;
use crate::svp::csv_parse::{CsvToken, CsvTokenKind, is_numeric_field, tokenize, translate_offset};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

pub fn highlight_csv(
	viewport_bytes: &[u8],
	delimiter: u8,
	offset_map: &[(usize, DocByte)],
) -> Vec<HighlightSpan> {
	let tokens = tokenize(viewport_bytes, delimiter);
	let mut out = Vec::with_capacity(tokens.len());
	let mut column = 0usize;

	for token in tokens {
		match token.kind {
			CsvTokenKind::Delimiter => {
				push(&mut out, token, TokenCategory::Punctuation, offset_map);
				column += 1;
			}
			CsvTokenKind::Newline => {
				column = 0;
			}
			CsvTokenKind::QuotedField => {
				push(&mut out, token, TokenCategory::String, offset_map);
			}
			CsvTokenKind::Field => {
				let slice = &viewport_bytes[token.start..token.end];
				let category = if is_numeric_field(slice) {
					TokenCategory::Number
				} else if column.is_multiple_of(2) {
					TokenCategory::Function
				} else {
					TokenCategory::Type
				};
				push(&mut out, token, category, offset_map);
			}
		}
	}

	out
}

fn push(
	out: &mut Vec<HighlightSpan>,
	token: CsvToken,
	category: TokenCategory,
	offset_map: &[(usize, DocByte)],
) {
	let start = translate_offset(token.start, offset_map);
	let end = translate_offset(token.end, offset_map);
	out.push(HighlightSpan::new(start, end, category));
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::csv_parse::{CSV_DELIMITER, TSV_DELIMITER};

	fn spans(src: &[u8], delim: u8) -> Vec<HighlightSpan> {
		highlight_csv(src, delim, &[(0, DocByte::ZERO)])
	}

	#[test]
	fn alternating_columns_use_function_and_type() {
		let src = b"a,b,c,d\n";
		let h = spans(src, CSV_DELIMITER);
		let functions: Vec<_> = h
			.iter()
			.filter(|s| s.category == TokenCategory::Function)
			.collect();
		let types: Vec<_> = h
			.iter()
			.filter(|s| s.category == TokenCategory::Type)
			.collect();
		assert_eq!(functions.len(), 2, "columns 0 and 2 are Function");
		assert_eq!(types.len(), 2, "columns 1 and 3 are Type");
	}

	#[test]
	fn column_counter_resets_at_newline() {
		let src = b"a,b\nc,d\n";
		let h = spans(src, CSV_DELIMITER);
		let functions = h
			.iter()
			.filter(|s| s.category == TokenCategory::Function)
			.count();
		let types = h
			.iter()
			.filter(|s| s.category == TokenCategory::Type)
			.count();
		assert_eq!(functions, 2, "columns 0 of each row");
		assert_eq!(types, 2, "columns 1 of each row");
	}

	#[test]
	fn numeric_fields_render_as_number_regardless_of_column() {
		let src = b"name,age,score\njoel,38,3.14\n";
		let h = spans(src, CSV_DELIMITER);
		let numbers = h
			.iter()
			.filter(|s| s.category == TokenCategory::Number)
			.count();
		assert_eq!(numbers, 2, "38 and 3.14 are numeric");
	}

	#[test]
	fn quoted_fields_render_as_string() {
		let src = b"\"hello, world\",42\n";
		let h = spans(src, CSV_DELIMITER);
		assert!(h.iter().any(|s| s.category == TokenCategory::String));
		assert!(h.iter().any(|s| s.category == TokenCategory::Number));
	}

	#[test]
	fn delimiters_render_as_punctuation() {
		let src = b"a,b,c\n";
		let h = spans(src, CSV_DELIMITER);
		let puncts = h
			.iter()
			.filter(|s| s.category == TokenCategory::Punctuation)
			.count();
		assert_eq!(puncts, 2);
	}

	#[test]
	fn tsv_uses_tab_delimiter() {
		let src = b"a\tb\tc\n";
		let h = spans(src, TSV_DELIMITER);
		let functions = h
			.iter()
			.filter(|s| s.category == TokenCategory::Function)
			.count();
		let types = h
			.iter()
			.filter(|s| s.category == TokenCategory::Type)
			.count();
		assert_eq!(functions, 2);
		assert_eq!(types, 1);
	}

	#[test]
	fn offset_map_applied_to_spans() {
		let src = b"a,b\n";
		let h = highlight_csv(src, CSV_DELIMITER, &[(0, DocByte::new(1000))]);
		assert!(
			h.iter()
				.any(|s| s.category == TokenCategory::Function && s.start == DocByte::new(1000))
		);
	}
}
