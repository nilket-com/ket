//! Lightweight TOML tokenizer and structural parser.
//!
//! Error-resilient by design: unknown bytes fall through as `Unknown`
//! tokens so live-typing does not poison highlighting. No external
//! dependency — TOML's surface grammar is small enough to hand-roll.

use crate::core::DocByte;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TomlTokenKind {
	Comment,
	String,
	Number,
	Ident,
	LBracket,
	RBracket,
	LBrace,
	RBrace,
	Equals,
	Dot,
	Comma,
	Newline,
	Whitespace,
	Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TomlToken {
	pub kind: TomlTokenKind,
	pub start: usize,
	pub end: usize,
}

pub fn tokenize(src: &[u8]) -> Vec<TomlToken> {
	let mut tokens = Vec::new();
	let mut pos = 0usize;
	let len = src.len();

	while pos < len {
		let start = pos;
		let b = src[pos];

		if b == b'\n' {
			pos += 1;
			tokens.push(TomlToken {
				kind: TomlTokenKind::Newline,
				start,
				end: pos,
			});
			continue;
		}

		if b == b' ' || b == b'\t' || b == b'\r' {
			while pos < len && matches!(src[pos], b' ' | b'\t' | b'\r') {
				pos += 1;
			}
			tokens.push(TomlToken {
				kind: TomlTokenKind::Whitespace,
				start,
				end: pos,
			});
			continue;
		}

		if b == b'#' {
			while pos < len && src[pos] != b'\n' {
				pos += 1;
			}
			tokens.push(TomlToken {
				kind: TomlTokenKind::Comment,
				start,
				end: pos,
			});
			continue;
		}

		if b == b'"' {
			pos = scan_basic_string(src, pos);
			tokens.push(TomlToken {
				kind: TomlTokenKind::String,
				start,
				end: pos,
			});
			continue;
		}

		if b == b'\'' {
			pos = scan_literal_string(src, pos);
			tokens.push(TomlToken {
				kind: TomlTokenKind::String,
				start,
				end: pos,
			});
			continue;
		}

		if is_number_start(src, pos) {
			pos = scan_number_or_datetime(src, pos);
			tokens.push(TomlToken {
				kind: TomlTokenKind::Number,
				start,
				end: pos,
			});
			continue;
		}

		if is_ident_start(b) {
			pos += 1;
			while pos < len && is_ident_continue(src[pos]) {
				pos += 1;
			}
			tokens.push(TomlToken {
				kind: TomlTokenKind::Ident,
				start,
				end: pos,
			});
			continue;
		}

		let kind = match b {
			b'[' => TomlTokenKind::LBracket,
			b']' => TomlTokenKind::RBracket,
			b'{' => TomlTokenKind::LBrace,
			b'}' => TomlTokenKind::RBrace,
			b'=' => TomlTokenKind::Equals,
			b'.' => TomlTokenKind::Dot,
			b',' => TomlTokenKind::Comma,
			_ => TomlTokenKind::Unknown,
		};
		pos += 1;
		tokens.push(TomlToken {
			kind,
			start,
			end: pos,
		});
	}

	tokens
}

fn scan_basic_string(src: &[u8], start: usize) -> usize {
	let len = src.len();
	// Detect triple-quoted: """..."""
	if start + 3 <= len && src[start + 1] == b'"' && src[start + 2] == b'"' {
		let mut i = start + 3;
		while i + 2 < len {
			if src[i] == b'"' && src[i + 1] == b'"' && src[i + 2] == b'"' {
				return i + 3;
			}
			if src[i] == b'\\' && i + 1 < len {
				i += 2;
				continue;
			}
			i += 1;
		}
		return len;
	}

	let mut i = start + 1;
	while i < len {
		let b = src[i];
		if b == b'\\' && i + 1 < len {
			i += 2;
			continue;
		}
		if b == b'"' {
			return i + 1;
		}
		if b == b'\n' {
			return i;
		}
		i += 1;
	}
	len
}

fn scan_literal_string(src: &[u8], start: usize) -> usize {
	let len = src.len();
	// Triple-quoted literal '''...'''
	if start + 3 <= len && src[start + 1] == b'\'' && src[start + 2] == b'\'' {
		let mut i = start + 3;
		while i + 2 < len {
			if src[i] == b'\'' && src[i + 1] == b'\'' && src[i + 2] == b'\'' {
				return i + 3;
			}
			i += 1;
		}
		return len;
	}

	let mut i = start + 1;
	while i < len {
		let b = src[i];
		if b == b'\'' {
			return i + 1;
		}
		if b == b'\n' {
			return i;
		}
		i += 1;
	}
	len
}

fn is_number_start(src: &[u8], pos: usize) -> bool {
	let b = src[pos];
	if b.is_ascii_digit() {
		return true;
	}
	if (b == b'+' || b == b'-') && pos + 1 < src.len() {
		let next = src[pos + 1];
		return next.is_ascii_digit() || next == b'i' || next == b'n';
	}
	false
}

fn scan_number_or_datetime(src: &[u8], start: usize) -> usize {
	let len = src.len();
	let mut i = start;
	if src[i] == b'+' || src[i] == b'-' {
		i += 1;
	}
	// Consume body — permissive on characters to cover hex, dates, times.
	while i < len {
		let b = src[i];
		let is_body =
			b.is_ascii_alphanumeric()
				|| b == b'_' || b == b'.'
				|| b == b':' || b == b'-'
				|| b == b'+';
		if !is_body {
			break;
		}
		// Datetime can contain a single space before the time portion.
		// Allow "1979-05-27 07:32:00" only when the next char is a digit.
		i += 1;
	}
	// Handle the " 07:32:00" case: peek past one space if the run ended
	// mid-datetime with a trailing digit-space-digit pattern.
	if i < len && src[i] == b' ' && i + 1 < len && src[i + 1].is_ascii_digit() {
		// Only take the space if what we've consumed so far looks date-like.
		let consumed = &src[start..i];
		if consumed.contains(&b'-') && !consumed.contains(&b':') {
			i += 1;
			while i < len {
				let b = src[i];
				let is_body = b.is_ascii_alphanumeric()
					|| b == b'_' || b == b'.'
					|| b == b':' || b == b'-'
					|| b == b'+';
				if !is_body {
					break;
				}
				i += 1;
			}
		}
	}
	i
}

fn is_ident_start(b: u8) -> bool {
	b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_continue(b: u8) -> bool {
	b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
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

	fn kinds(src: &[u8]) -> Vec<TomlTokenKind> {
		tokenize(src)
			.into_iter()
			.filter(|t| !matches!(t.kind, TomlTokenKind::Whitespace))
			.map(|t| t.kind)
			.collect()
	}

	#[test]
	fn tokenizes_simple_key_value() {
		let k = kinds(b"name = \"baryon\"\n");
		assert_eq!(
			k,
			vec![
				TomlTokenKind::Ident,
				TomlTokenKind::Equals,
				TomlTokenKind::String,
				TomlTokenKind::Newline,
			]
		);
	}

	#[test]
	fn tokenizes_table_header() {
		let k = kinds(b"[dependencies]\n");
		assert_eq!(
			k,
			vec![
				TomlTokenKind::LBracket,
				TomlTokenKind::Ident,
				TomlTokenKind::RBracket,
				TomlTokenKind::Newline,
			]
		);
	}

	#[test]
	fn tokenizes_array_of_tables() {
		let k = kinds(b"[[bin]]\n");
		assert_eq!(
			k,
			vec![
				TomlTokenKind::LBracket,
				TomlTokenKind::LBracket,
				TomlTokenKind::Ident,
				TomlTokenKind::RBracket,
				TomlTokenKind::RBracket,
				TomlTokenKind::Newline,
			]
		);
	}

	#[test]
	fn tokenizes_comment_and_number() {
		let tokens = tokenize(b"# top\nport = 8080\n");
		assert_eq!(tokens[0].kind, TomlTokenKind::Comment);
		assert!(tokens.iter().any(|t| t.kind == TomlTokenKind::Number
			&& &b"port = 8080"[..][t.start - 6..t.end - 6] == b"8080"));
	}

	#[test]
	fn multiline_basic_string_consumed() {
		let src = b"msg = \"\"\"hello\nworld\"\"\"\n";
		let tokens = tokenize(src);
		let string_token = tokens
			.iter()
			.find(|t| t.kind == TomlTokenKind::String)
			.unwrap();
		assert_eq!(
			&src[string_token.start..string_token.end],
			b"\"\"\"hello\nworld\"\"\""
		);
	}

	#[test]
	fn literal_string_does_not_process_escapes() {
		let src = b"path = 'C:\\Users\\foo'\n";
		let tokens = tokenize(src);
		let string_token = tokens
			.iter()
			.find(|t| t.kind == TomlTokenKind::String)
			.unwrap();
		assert_eq!(
			&src[string_token.start..string_token.end],
			b"'C:\\Users\\foo'"
		);
	}

	#[test]
	fn datetime_with_space_separator_scans_as_one_number() {
		let src = b"d = 1979-05-27 07:32:00\n";
		let tokens = tokenize(src);
		let num = tokens
			.iter()
			.find(|t| t.kind == TomlTokenKind::Number)
			.unwrap();
		assert_eq!(&src[num.start..num.end], b"1979-05-27 07:32:00");
	}

	#[test]
	fn unterminated_string_does_not_panic() {
		let src = b"x = \"unterminated\n";
		let _ = tokenize(src);
	}

	#[test]
	fn translate_offset_handles_empty_map() {
		assert_eq!(translate_offset(5, &[]), DocByte::new(5));
	}

	#[test]
	fn translate_offset_uses_nearest_base() {
		let map = [(0, DocByte::new(100)), (10, DocByte::new(200))];
		assert_eq!(translate_offset(3, &map), DocByte::new(103));
		assert_eq!(translate_offset(15, &map), DocByte::new(205));
	}
}
