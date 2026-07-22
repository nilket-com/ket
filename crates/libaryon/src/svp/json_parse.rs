use crate::core::DocByte;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTokenKind {
	String,
	Number,
	True,
	False,
	Null,
	LBrace,
	RBrace,
	LBracket,
	RBracket,
	Colon,
	Comma,
	Newline,
	Whitespace,
	Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonToken {
	pub kind: JsonTokenKind,
	pub start: usize,
	pub end: usize,
}

pub fn tokenize(src: &[u8]) -> Vec<JsonToken> {
	let mut tokens = Vec::new();
	let mut pos = 0usize;

	while pos < src.len() {
		let start = pos;
		match src[pos] {
			b'\n' => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::Newline, start, pos);
			}
			b' ' | b'\t' | b'\r' => {
				while pos < src.len() && matches!(src[pos], b' ' | b'\t' | b'\r') {
					pos += 1;
				}
				push(&mut tokens, JsonTokenKind::Whitespace, start, pos);
			}
			b'"' => {
				pos = scan_string(src, pos);
				push(&mut tokens, JsonTokenKind::String, start, pos);
			}
			b'-' | b'0'..=b'9' if is_number_start(src, pos) => {
				pos = scan_number(src, pos);
				push(&mut tokens, JsonTokenKind::Number, start, pos);
			}
			b't' if starts_with(src, pos, b"true") => {
				pos += 4;
				push(&mut tokens, JsonTokenKind::True, start, pos);
			}
			b'f' if starts_with(src, pos, b"false") => {
				pos += 5;
				push(&mut tokens, JsonTokenKind::False, start, pos);
			}
			b'n' if starts_with(src, pos, b"null") => {
				pos += 4;
				push(&mut tokens, JsonTokenKind::Null, start, pos);
			}
			b'{' => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::LBrace, start, pos);
			}
			b'}' => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::RBrace, start, pos);
			}
			b'[' => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::LBracket, start, pos);
			}
			b']' => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::RBracket, start, pos);
			}
			b':' => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::Colon, start, pos);
			}
			b',' => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::Comma, start, pos);
			}
			_ => {
				pos += 1;
				push(&mut tokens, JsonTokenKind::Unknown, start, pos);
			}
		}
	}

	tokens
}

fn scan_string(src: &[u8], start: usize) -> usize {
	let mut pos = start + 1;
	while pos < src.len() {
		match src[pos] {
			b'"' => return pos + 1,
			b'\n' => return pos,
			b'\\' if pos + 1 < src.len() => {
				if src[pos + 1] == b'\n' {
					return pos;
				}
				pos += 2;
			}
			_ => pos += 1,
		}
	}
	src.len()
}

fn is_number_start(src: &[u8], pos: usize) -> bool {
	let byte = src[pos];
	byte.is_ascii_digit() || (byte == b'-' && pos + 1 < src.len() && src[pos + 1].is_ascii_digit())
}

fn scan_number(src: &[u8], start: usize) -> usize {
	let mut pos = start;
	if pos < src.len() && src[pos] == b'-' {
		pos += 1;
	}

	while pos < src.len() && src[pos].is_ascii_digit() {
		pos += 1;
	}

	if pos < src.len() && src[pos] == b'.' {
		pos += 1;
		while pos < src.len() && src[pos].is_ascii_digit() {
			pos += 1;
		}
	}

	if pos < src.len() && matches!(src[pos], b'e' | b'E') {
		pos += 1;
		if pos < src.len() && matches!(src[pos], b'+' | b'-') {
			pos += 1;
		}
		while pos < src.len() && src[pos].is_ascii_digit() {
			pos += 1;
		}
	}

	pos
}

fn starts_with(src: &[u8], pos: usize, needle: &[u8]) -> bool {
	pos + needle.len() <= src.len() && &src[pos..pos + needle.len()] == needle
}

fn push(out: &mut Vec<JsonToken>, kind: JsonTokenKind, start: usize, end: usize) {
	if start < end {
		out.push(JsonToken { kind, start, end });
	}
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

	fn kinds(src: &[u8]) -> Vec<JsonTokenKind> {
		tokenize(src).into_iter().map(|token| token.kind).collect()
	}

	#[test]
	fn tokenizes_object_punctuation_and_literals() {
		let src = br#"{"ok":true,"missing":null,"enabled":false}"#;
		let kinds = kinds(src);
		assert!(kinds.contains(&JsonTokenKind::LBrace));
		assert!(kinds.contains(&JsonTokenKind::RBrace));
		assert!(kinds.contains(&JsonTokenKind::Colon));
		assert!(kinds.contains(&JsonTokenKind::Comma));
		assert!(kinds.contains(&JsonTokenKind::True));
		assert!(kinds.contains(&JsonTokenKind::False));
		assert!(kinds.contains(&JsonTokenKind::Null));
	}

	#[test]
	fn tokenizes_strings_with_escapes() {
		let src = br#"{"quote":"a\"b","slash":"c\\d"}"#;
		let tokens = tokenize(src);
		assert_eq!(
			tokens
				.iter()
				.filter(|token| token.kind == JsonTokenKind::String)
				.count(),
			4
		);
		assert!(tokens.iter().any(|token| {
			token.kind == JsonTokenKind::String && &src[token.start..token.end] == br#""a\"b""#
		}));
	}

	#[test]
	fn unterminated_string_stops_at_newline() {
		let src = b"\"name\n\"next\"";
		let tokens = tokenize(src);
		assert_eq!(tokens[0].kind, JsonTokenKind::String);
		assert_eq!(&src[tokens[0].start..tokens[0].end], b"\"name");
		assert!(
			tokens
				.iter()
				.any(|token| token.kind == JsonTokenKind::Newline)
		);
		assert_eq!(
			tokens
				.iter()
				.filter(|token| token.kind == JsonTokenKind::String)
				.count(),
			2
		);
	}

	#[test]
	fn tokenizes_numbers_with_fraction_and_exponent() {
		let src = b"[-12.34e+5,0,6E-2]";
		let tokens = tokenize(src);
		let numbers: Vec<_> = tokens
			.iter()
			.filter(|token| token.kind == JsonTokenKind::Number)
			.collect();
		assert_eq!(numbers.len(), 3);
		assert_eq!(&src[numbers[0].start..numbers[0].end], b"-12.34e+5");
		assert_eq!(&src[numbers[2].start..numbers[2].end], b"6E-2");
	}

	#[test]
	fn malformed_input_uses_unknown_tokens_without_panicking() {
		let src = b"{foo: @}";
		let kinds = kinds(src);
		assert!(kinds.contains(&JsonTokenKind::Unknown));
	}

	#[test]
	fn translate_offset_uses_nearest_base() {
		let map = [(0usize, DocByte::new(100)), (10, DocByte::new(200))];
		assert_eq!(translate_offset(3, &map), DocByte::new(103));
		assert_eq!(translate_offset(15, &map), DocByte::new(205));
	}
}
