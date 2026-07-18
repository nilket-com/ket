use crate::core::DocByte;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HtmlTokenKind {
	Comment,
	Doctype,
	TagDelimiter,
	TagName,
	AttrName,
	Equals,
	AttrValue,
	Entity,
	RawText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HtmlToken {
	pub kind: HtmlTokenKind,
	pub start: usize,
	pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawTextTag {
	Script,
	Style,
}

#[derive(Debug, Clone, Copy)]
struct TagScan {
	end: usize,
	raw_text: Option<RawTextTag>,
	closing: bool,
	self_closing: bool,
}

pub fn tokenize(src: &[u8]) -> Vec<HtmlToken> {
	let mut out = Vec::new();
	let mut pos = 0usize;

	while pos < src.len() {
		if starts_with(src, pos, b"<!--") {
			pos = scan_comment(src, pos, &mut out);
			continue;
		}

		if src[pos] == b'<' {
			if starts_with_ci(src, pos, b"<!doctype") || starts_with(src, pos, b"<!") {
				pos = scan_declaration(src, pos, &mut out);
				continue;
			}

			if starts_with(src, pos, b"<?") {
				pos = scan_declaration(src, pos, &mut out);
				continue;
			}

			if is_tag_start(src, pos) {
				let scan = scan_tag(src, pos, &mut out);
				pos = scan.end;

				if !scan.closing
					&& !scan.self_closing
					&& let Some(raw_text) = scan.raw_text
				{
					let raw_start = pos;
					if let Some(close_start) = find_raw_text_close(src, raw_start, raw_text) {
						push(&mut out, HtmlTokenKind::RawText, raw_start, close_start);
						pos = close_start;
					} else {
						push(&mut out, HtmlTokenKind::RawText, raw_start, src.len());
						pos = src.len();
					}
				}

				continue;
			}
		}

		if src[pos] == b'&'
			&& let Some(entity_end) = scan_entity(src, pos)
		{
			push(&mut out, HtmlTokenKind::Entity, pos, entity_end);
			pos = entity_end;
			continue;
		}

		pos += 1;
	}

	out
}

fn scan_comment(src: &[u8], start: usize, out: &mut Vec<HtmlToken>) -> usize {
	let end = find_bytes(src, start + 4, b"-->")
		.map(|idx| idx + 3)
		.unwrap_or(src.len());
	push(out, HtmlTokenKind::Comment, start, end);
	end
}

fn scan_declaration(src: &[u8], start: usize, out: &mut Vec<HtmlToken>) -> usize {
	let end = find_byte(src, start + 2, b'>')
		.map(|idx| idx + 1)
		.unwrap_or(src.len());
	push(out, HtmlTokenKind::Doctype, start, end);
	end
}

fn scan_tag(src: &[u8], start: usize, out: &mut Vec<HtmlToken>) -> TagScan {
	let mut pos = start;
	let mut closing = false;
	let mut self_closing = false;
	let mut raw_text = None;

	if starts_with(src, pos, b"</") {
		push(out, HtmlTokenKind::TagDelimiter, pos, pos + 2);
		pos += 2;
		closing = true;
	} else {
		push(out, HtmlTokenKind::TagDelimiter, pos, pos + 1);
		pos += 1;
	}

	pos = skip_ascii_space(src, pos);
	let name_start = pos;
	while pos < src.len() && is_tag_name_byte(src[pos]) {
		pos += 1;
	}
	if name_start < pos {
		push(out, HtmlTokenKind::TagName, name_start, pos);
		raw_text = classify_raw_text_tag(&src[name_start..pos]);
	}

	while pos < src.len() {
		pos = skip_ascii_space(src, pos);
		if pos >= src.len() {
			break;
		}

		match src[pos] {
			b'>' => {
				push(out, HtmlTokenKind::TagDelimiter, pos, pos + 1);
				pos += 1;
				break;
			}
			b'/' if pos + 1 < src.len() && src[pos + 1] == b'>' => {
				push(out, HtmlTokenKind::TagDelimiter, pos, pos + 2);
				pos += 2;
				self_closing = true;
				break;
			}
			b'/' => {
				push(out, HtmlTokenKind::TagDelimiter, pos, pos + 1);
				pos += 1;
			}
			b'=' => {
				push(out, HtmlTokenKind::Equals, pos, pos + 1);
				pos += 1;
			}
			b'\'' | b'"' => {
				pos = scan_quoted_value(src, pos, out);
			}
			_ => {
				if is_attr_name_byte(src[pos]) {
					pos = scan_attribute(src, pos, out);
				} else {
					pos += 1;
				}
			}
		}
	}

	TagScan {
		end: pos,
		raw_text,
		closing,
		self_closing,
	}
}

fn scan_attribute(src: &[u8], start: usize, out: &mut Vec<HtmlToken>) -> usize {
	let mut pos = start;
	while pos < src.len() && is_attr_name_byte(src[pos]) {
		pos += 1;
	}
	push(out, HtmlTokenKind::AttrName, start, pos);

	pos = skip_ascii_space(src, pos);
	if pos < src.len() && src[pos] == b'=' {
		push(out, HtmlTokenKind::Equals, pos, pos + 1);
		pos += 1;
		pos = skip_ascii_space(src, pos);
		pos = scan_attr_value(src, pos, out);
	}

	pos
}

fn scan_attr_value(src: &[u8], start: usize, out: &mut Vec<HtmlToken>) -> usize {
	if start >= src.len() {
		return start;
	}

	match src[start] {
		b'\'' | b'"' => scan_quoted_value(src, start, out),
		b'>' => start,
		_ => {
			let mut pos = start;
			while pos < src.len()
				&& !src[pos].is_ascii_whitespace()
				&& src[pos] != b'>'
				&& !(src[pos] == b'/' && pos + 1 < src.len() && src[pos + 1] == b'>')
			{
				pos += 1;
			}
			push(out, HtmlTokenKind::AttrValue, start, pos);
			pos
		}
	}
}

fn scan_quoted_value(src: &[u8], start: usize, out: &mut Vec<HtmlToken>) -> usize {
	let quote = src[start];
	let mut pos = start + 1;
	while pos < src.len() {
		if src[pos] == quote {
			pos += 1;
			break;
		}
		pos += 1;
	}
	push(out, HtmlTokenKind::AttrValue, start, pos);
	pos
}

fn scan_entity(src: &[u8], start: usize) -> Option<usize> {
	let mut pos = start + 1;
	if pos >= src.len() {
		return None;
	}

	if src[pos] == b'#' {
		pos += 1;
		if pos < src.len() && matches!(src[pos], b'x' | b'X') {
			pos += 1;
			let digits_start = pos;
			while pos < src.len() && src[pos].is_ascii_hexdigit() {
				pos += 1;
			}
			return entity_terminator(src, pos, digits_start);
		}

		let digits_start = pos;
		while pos < src.len() && src[pos].is_ascii_digit() {
			pos += 1;
		}
		return entity_terminator(src, pos, digits_start);
	}

	let name_start = pos;
	while pos < src.len() && src[pos].is_ascii_alphanumeric() {
		pos += 1;
	}
	entity_terminator(src, pos, name_start)
}

fn entity_terminator(src: &[u8], pos: usize, body_start: usize) -> Option<usize> {
	if pos > body_start && pos < src.len() && src[pos] == b';' && pos - body_start <= 32 {
		Some(pos + 1)
	} else {
		None
	}
}

fn find_raw_text_close(src: &[u8], start: usize, tag: RawTextTag) -> Option<usize> {
	let name = match tag {
		RawTextTag::Script => b"script".as_slice(),
		RawTextTag::Style => b"style".as_slice(),
	};

	let mut pos = start;
	while pos + name.len() + 2 <= src.len() {
		if starts_with(src, pos, b"</")
			&& ascii_eq_ignore_case_slice(src, pos + 2, name)
			&& raw_close_boundary(src, pos + 2 + name.len())
		{
			return Some(pos);
		}
		pos += 1;
	}
	None
}

fn raw_close_boundary(src: &[u8], pos: usize) -> bool {
	pos >= src.len() || matches!(src[pos], b'>' | b'/' | b' ' | b'\t' | b'\r' | b'\n')
}

fn classify_raw_text_tag(name: &[u8]) -> Option<RawTextTag> {
	if ascii_eq_ignore_case(name, b"script") {
		Some(RawTextTag::Script)
	} else if ascii_eq_ignore_case(name, b"style") {
		Some(RawTextTag::Style)
	} else {
		None
	}
}

fn is_tag_start(src: &[u8], pos: usize) -> bool {
	if pos + 1 >= src.len() || src[pos] != b'<' {
		return false;
	}
	let next = src[pos + 1];
	next.is_ascii_alphabetic()
		|| (next == b'/' && pos + 2 < src.len() && src[pos + 2].is_ascii_alphabetic())
}

fn is_tag_name_byte(b: u8) -> bool {
	b.is_ascii_alphanumeric() || matches!(b, b'-' | b':' | b'_')
}

fn is_attr_name_byte(b: u8) -> bool {
	!b.is_ascii_whitespace() && !matches!(b, b'=' | b'/' | b'>' | b'<' | b'\'' | b'"' | b'`')
}

fn skip_ascii_space(src: &[u8], mut pos: usize) -> usize {
	while pos < src.len() && src[pos].is_ascii_whitespace() {
		pos += 1;
	}
	pos
}

fn find_byte(src: &[u8], start: usize, needle: u8) -> Option<usize> {
	let mut pos = start;
	while pos < src.len() {
		if src[pos] == needle {
			return Some(pos);
		}
		pos += 1;
	}
	None
}

fn find_bytes(src: &[u8], start: usize, needle: &[u8]) -> Option<usize> {
	if needle.is_empty() || start >= src.len() || needle.len() > src.len() {
		return None;
	}

	let last_start = src.len().saturating_sub(needle.len());
	let mut pos = start;
	while pos <= last_start {
		if starts_with(src, pos, needle) {
			return Some(pos);
		}
		pos += 1;
	}
	None
}

fn starts_with(src: &[u8], pos: usize, needle: &[u8]) -> bool {
	pos + needle.len() <= src.len() && &src[pos..pos + needle.len()] == needle
}

fn starts_with_ci(src: &[u8], pos: usize, needle: &[u8]) -> bool {
	pos + needle.len() <= src.len() && ascii_eq_ignore_case(&src[pos..pos + needle.len()], needle)
}

fn ascii_eq_ignore_case_slice(src: &[u8], pos: usize, needle: &[u8]) -> bool {
	pos + needle.len() <= src.len() && ascii_eq_ignore_case(&src[pos..pos + needle.len()], needle)
}

fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
	a.len() == b.len()
		&& a.iter()
			.zip(b.iter())
			.all(|(&left, &right)| left.eq_ignore_ascii_case(&right))
}

fn push(out: &mut Vec<HtmlToken>, kind: HtmlTokenKind, start: usize, end: usize) {
	if start < end {
		out.push(HtmlToken { kind, start, end });
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

	fn kinds(src: &[u8]) -> Vec<HtmlTokenKind> {
		tokenize(src).into_iter().map(|token| token.kind).collect()
	}

	#[test]
	fn tokenizes_comments_and_doctype() {
		let src = b"<!doctype html>\n<!-- note -->\n";
		let tokens = tokenize(src);
		assert_eq!(tokens[0].kind, HtmlTokenKind::Doctype);
		assert!(
			tokens
				.iter()
				.any(|token| token.kind == HtmlTokenKind::Comment)
		);
	}

	#[test]
	fn tokenizes_tag_names_and_attributes() {
		let src = br#"<main class="app" data-count=3 disabled></main>"#;
		let kinds = kinds(src);
		assert!(kinds.contains(&HtmlTokenKind::TagName));
		assert_eq!(
			kinds
				.iter()
				.filter(|kind| **kind == HtmlTokenKind::AttrName)
				.count(),
			3
		);
		assert_eq!(
			kinds
				.iter()
				.filter(|kind| **kind == HtmlTokenKind::AttrValue)
				.count(),
			2
		);
	}

	#[test]
	fn emits_raw_text_for_script_body_and_keeps_closing_tag() {
		let src = b"<script>let x = '<tag>';</script>";
		let tokens = tokenize(src);
		let raw = tokens
			.iter()
			.find(|token| token.kind == HtmlTokenKind::RawText)
			.expect("script body should be raw text");
		assert_eq!(&src[raw.start..raw.end], b"let x = '<tag>';");
		assert!(tokens.iter().any(|token| {
			token.kind == HtmlTokenKind::TagName && &src[token.start..token.end] == b"script"
		}));
	}

	#[test]
	fn detects_entities_in_text_content() {
		let src = b"<p>Baryon &amp; HTML &#x3c;</p>";
		let tokens = tokenize(src);
		assert_eq!(
			tokens
				.iter()
				.filter(|token| token.kind == HtmlTokenKind::Entity)
				.count(),
			2
		);
	}

	#[test]
	fn translate_offset_uses_nearest_base() {
		let map = [(0usize, DocByte::new(100)), (10, DocByte::new(200))];
		assert_eq!(translate_offset(3, &map), DocByte::new(103));
		assert_eq!(translate_offset(15, &map), DocByte::new(205));
	}
}
