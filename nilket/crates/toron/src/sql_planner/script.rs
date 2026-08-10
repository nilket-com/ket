use std::fmt;

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer};

/// A lexical failure while finding statement boundaries in authored SQL source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlScriptError {
	message: String,
}

impl fmt::Display for SqlScriptError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(&self.message)
	}
}

/// Split SQL source at real semicolon tokens while retaining exact authored slices.
///
/// The tokenizer is only a boundary oracle. It does not parse or render statements, so
/// Toron-specific syntax remains available to the authoritative server frontend. The
/// returned slices borrow the single source buffer and omit comment-only/empty segments.
pub(crate) fn split_sql_script(source: &str) -> Result<Vec<&str>, SqlScriptError> {
	let dialect = GenericDialect {};
	let tokens = Tokenizer::new(&dialect, source)
		.with_unescape(false)
		.tokenize_with_location()
		.map_err(|error| SqlScriptError {
			message: error.to_string(),
		})?;

	let mut boundaries = Vec::new();
	let mut segment_has_content = false;
	for token in tokens {
		match token.token {
			Token::Whitespace(_) | Token::EOF => {}
			Token::SemiColon => {
				boundaries.push((token.span.start, segment_has_content));
				segment_has_content = false;
			}
			_ => segment_has_content = true,
		}
	}

	let boundary_offsets = byte_offsets_for_locations(source, &boundaries)?;
	let mut statements = Vec::new();
	let mut segment_start = 0usize;
	for ((_, has_content), delimiter_offset) in boundaries.iter().zip(boundary_offsets) {
		if *has_content {
			let statement = source[segment_start..delimiter_offset].trim();
			if !statement.is_empty() {
				statements.push(statement);
			}
		}
		segment_start = delimiter_offset + 1;
	}
	if segment_has_content {
		let statement = source[segment_start..].trim();
		if !statement.is_empty() {
			statements.push(statement);
		}
	}
	Ok(statements)
}

/// Convert tokenizer line/column locations to UTF-8 byte offsets with one source pass.
fn byte_offsets_for_locations(
	source: &str,
	boundaries: &[(Location, bool)],
) -> Result<Vec<usize>, SqlScriptError> {
	let mut offsets = Vec::with_capacity(boundaries.len());
	let mut boundary_index = 0usize;
	let mut line = 1u64;
	let mut column = 1u64;

	for (byte_offset, character) in source.char_indices() {
		while boundaries
			.get(boundary_index)
			.is_some_and(|(location, _)| location.line == line && location.column == column)
		{
			offsets.push(byte_offset);
			boundary_index += 1;
		}
		if character == '\n' {
			line += 1;
			column = 1;
		} else {
			column += 1;
		}
	}

	if boundary_index != boundaries.len() {
		return Err(SqlScriptError {
			message: "SQL tokenizer returned a boundary outside the source".to_string(),
		});
	}
	Ok(offsets)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lexical_boundaries_preserve_exact_authored_slices() {
		let source = "\n-- lead ; comment\nSELECT 'a;b' AS \"semi;colon\";\nSELECT E'escaped\\';semi';\n\n/* empty ; */;\nCREATE LOGICAL TYPE Percent AS Float64 CHECK (VALUE >= 0);\nSELECT '雪'; -- tail ;\n";

		let statements = split_sql_script(source).expect("split valid SQL");

		assert_eq!(
			statements,
			vec![
				"-- lead ; comment\nSELECT 'a;b' AS \"semi;colon\"",
				"SELECT E'escaped\\';semi'",
				"CREATE LOGICAL TYPE Percent AS Float64 CHECK (VALUE >= 0)",
				"SELECT '雪'",
			]
		);
	}

	#[test]
	fn adjacent_delimiters_and_comments_do_not_create_statements() {
		assert!(
			split_sql_script(" ; /* only */ ; -- only\n ; ")
				.unwrap()
				.is_empty()
		);
		assert_eq!(split_sql_script(";;; SELECT 1 ;;;").unwrap(), ["SELECT 1"]);
	}

	#[test]
	fn unterminated_later_literal_refuses_the_whole_source() {
		let error = split_sql_script("SELECT 1; SELECT 'unterminated")
			.expect_err("tokenization must fail before any execution");

		assert!(
			error
				.to_string()
				.to_ascii_lowercase()
				.contains("unterminated")
		);
	}
}
