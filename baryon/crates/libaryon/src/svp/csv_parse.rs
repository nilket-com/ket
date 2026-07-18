//! Single-pass CSV/TSV tokenizer — RFC 4180 compliant.
//!
//! Parameterized by a single-byte field delimiter so `CsvProvider`
//! (`,`) and `TsvProvider` (`\t`) share the same implementation.
//! Handles quoted fields with embedded newlines and escaped `""`.
//! Unterminated quotes are tolerated (consume to EOF) so live-typing
//! never poisons highlighting.

use crate::core::DocByte;

pub const CSV_DELIMITER: u8 = b',';
pub const TSV_DELIMITER: u8 = b'\t';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsvTokenKind {
	/// An unquoted field (may be empty — e.g. between two adjacent delimiters).
	Field,
	/// A quoted field including the surrounding `"`. May span multiple lines.
	QuotedField,
	/// The delimiter byte (`,` or `\t`).
	Delimiter,
	/// `\n` or `\r\n` — only emitted outside quoted fields.
	Newline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvToken {
	pub kind: CsvTokenKind,
	pub start: usize,
	pub end: usize,
}

pub fn tokenize(src: &[u8], delimiter: u8) -> Vec<CsvToken> {
	let mut out = Vec::new();
	let len = src.len();
	let mut pos = 0usize;

	while pos < len {
		let start = pos;
		let b = src[pos];

		if b == b'\r' {
			pos += if pos + 1 < len && src[pos + 1] == b'\n' {
				2
			} else {
				1
			};
			out.push(CsvToken {
				kind: CsvTokenKind::Newline,
				start,
				end: pos,
			});
			continue;
		}

		if b == b'\n' {
			pos += 1;
			out.push(CsvToken {
				kind: CsvTokenKind::Newline,
				start,
				end: pos,
			});
			continue;
		}

		if b == b'"' {
			pos = scan_quoted(src, pos);
			out.push(CsvToken {
				kind: CsvTokenKind::QuotedField,
				start,
				end: pos,
			});
			continue;
		}

		if b == delimiter {
			pos += 1;
			out.push(CsvToken {
				kind: CsvTokenKind::Delimiter,
				start,
				end: pos,
			});
			continue;
		}

		while pos < len {
			let c = src[pos];
			if c == b'\n' || c == b'\r' || c == delimiter {
				break;
			}
			pos += 1;
		}
		out.push(CsvToken {
			kind: CsvTokenKind::Field,
			start,
			end: pos,
		});
	}

	out
}

fn scan_quoted(src: &[u8], start: usize) -> usize {
	let len = src.len();
	let mut pos = start + 1;
	while pos < len {
		if src[pos] == b'"' {
			if pos + 1 < len && src[pos + 1] == b'"' {
				pos += 2;
				continue;
			}
			return pos + 1;
		}
		pos += 1;
	}
	len
}

pub fn is_numeric_field(slice: &[u8]) -> bool {
	if slice.is_empty() {
		return false;
	}
	let mut i = 0usize;
	if slice[0] == b'+' || slice[0] == b'-' {
		i += 1;
	}
	let mut has_digit = false;
	let mut seen_dot = false;
	let mut seen_exp = false;
	while i < slice.len() {
		let b = slice[i];
		if b.is_ascii_digit() {
			has_digit = true;
			i += 1;
		} else if b == b'.' && !seen_dot && !seen_exp {
			seen_dot = true;
			i += 1;
		} else if (b == b'e' || b == b'E') && has_digit && !seen_exp {
			seen_exp = true;
			i += 1;
			if i < slice.len() && (slice[i] == b'+' || slice[i] == b'-') {
				i += 1;
			}
			// Require at least one digit in the exponent.
			if i >= slice.len() || !slice[i].is_ascii_digit() {
				return false;
			}
		} else {
			return false;
		}
	}
	has_digit
}

pub fn translate_offset(local: usize, map: &[(usize, DocByte)]) -> DocByte {
	if map.is_empty() {
		return DocByte::new(local as u64);
	}
	let idx = map
		.partition_point(|&(viewport_idx, _)| viewport_idx <= local)
		.saturating_sub(1);
	let (viewport_idx, base) = map[idx];
	base.saturating_add(local.saturating_sub(viewport_idx) as u64)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn kinds(src: &[u8], delim: u8) -> Vec<CsvTokenKind> {
		tokenize(src, delim).into_iter().map(|t| t.kind).collect()
	}

	#[test]
	fn tokenizes_simple_csv_record() {
		let k = kinds(b"a,b,c\n", CSV_DELIMITER);
		assert_eq!(
			k,
			vec![
				CsvTokenKind::Field,
				CsvTokenKind::Delimiter,
				CsvTokenKind::Field,
				CsvTokenKind::Delimiter,
				CsvTokenKind::Field,
				CsvTokenKind::Newline,
			]
		);
	}

	#[test]
	fn tokenizes_tsv_with_tabs() {
		let k = kinds(b"a\tb\tc\n", TSV_DELIMITER);
		assert_eq!(
			k,
			vec![
				CsvTokenKind::Field,
				CsvTokenKind::Delimiter,
				CsvTokenKind::Field,
				CsvTokenKind::Delimiter,
				CsvTokenKind::Field,
				CsvTokenKind::Newline,
			]
		);
	}

	#[test]
	fn tsv_treats_comma_as_field_content() {
		let tokens = tokenize(b"a,x\tb\n", TSV_DELIMITER);
		let first = tokens
			.iter()
			.find(|t| t.kind == CsvTokenKind::Field)
			.unwrap();
		assert_eq!(first.end - first.start, 3); // "a,x"
	}

	#[test]
	fn quoted_field_with_embedded_quote_and_comma() {
		let src = b"\"hello, \"\"world\"\"\",x\n";
		let tokens = tokenize(src, CSV_DELIMITER);
		assert_eq!(tokens[0].kind, CsvTokenKind::QuotedField);
		assert_eq!(
			&src[tokens[0].start..tokens[0].end],
			b"\"hello, \"\"world\"\"\""
		);
	}

	#[test]
	fn quoted_field_with_embedded_newline_does_not_break_record() {
		let src = b"\"line1\nline2\",b\n";
		let tokens = tokenize(src, CSV_DELIMITER);
		let newline_count = tokens
			.iter()
			.filter(|t| t.kind == CsvTokenKind::Newline)
			.count();
		assert_eq!(newline_count, 1);
	}

	#[test]
	fn empty_field_between_delimiters_yields_no_token() {
		let tokens = tokenize(b"a,,b\n", CSV_DELIMITER);
		assert_eq!(
			tokens
				.iter()
				.filter(|t| t.kind == CsvTokenKind::Delimiter)
				.count(),
			2
		);
	}

	#[test]
	fn crlf_newline_absorbed_as_single_token() {
		let tokens = tokenize(b"a,b\r\nc,d\r\n", CSV_DELIMITER);
		for t in &tokens {
			if t.kind == CsvTokenKind::Newline {
				assert_eq!(t.end - t.start, 2);
			}
		}
	}

	#[test]
	fn unterminated_quote_does_not_panic() {
		let _ = tokenize(b"\"oops,no close", CSV_DELIMITER);
	}

	#[test]
	fn numeric_heuristic_accepts_common_forms() {
		assert!(is_numeric_field(b"42"));
		assert!(is_numeric_field(b"-3.14"));
		assert!(is_numeric_field(b"+0.5"));
		assert!(is_numeric_field(b"6.022e23"));
		assert!(is_numeric_field(b"1.5E-9"));
	}

	#[test]
	fn numeric_heuristic_rejects_garbage() {
		assert!(!is_numeric_field(b""));
		assert!(!is_numeric_field(b"hello"));
		assert!(!is_numeric_field(b"1.2.3"));
		assert!(!is_numeric_field(b"1e"));
		assert!(!is_numeric_field(b"1eA"));
		assert!(!is_numeric_field(b"-"));
	}
}
