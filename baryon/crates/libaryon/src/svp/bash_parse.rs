use crate::core::DocByte;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BashTokenKind {
	Shebang,
	Comment,
	String,
	Variable,
	Word,
	Operator,
	Punctuation,
	HereDocBody,
	HereDocDelimiter,
	Newline,
	Whitespace,
	Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BashToken {
	pub kind: BashTokenKind,
	pub start: usize,
	pub end: usize,
}

#[derive(Debug)]
struct HereDoc {
	delimiter: Vec<u8>,
	strip_tabs: bool,
}

pub fn tokenize(src: &[u8]) -> Vec<BashToken> {
	let mut tokens = Vec::new();
	let mut pos = 0usize;
	let mut awaiting_heredoc = None;
	let mut heredocs = VecDeque::new();

	while pos < src.len() {
		let start = pos;
		match src[pos] {
			b'#' if pos == 0 && src.get(1) == Some(&b'!') => {
				pos = scan_to_newline(src, pos);
				push(&mut tokens, BashTokenKind::Shebang, start, pos);
			}
			b'#' => {
				pos = scan_to_newline(src, pos);
				push(&mut tokens, BashTokenKind::Comment, start, pos);
			}
			b'\n' => {
				pos += 1;
				push(&mut tokens, BashTokenKind::Newline, start, pos);
				awaiting_heredoc = None;
				if let Some(heredoc) = heredocs.pop_front() {
					pos = scan_heredoc(src, pos, &heredoc, &mut tokens);
				}
			}
			b' ' | b'\t' | b'\r' => {
				while pos < src.len() && matches!(src[pos], b' ' | b'\t' | b'\r') {
					pos += 1;
				}
				push(&mut tokens, BashTokenKind::Whitespace, start, pos);
			}
			b'\'' => {
				pos = scan_quoted(src, pos, b'\'', false);
				push(&mut tokens, BashTokenKind::String, start, pos);
				capture_heredoc(
					src,
					start,
					pos,
					BashTokenKind::String,
					&mut awaiting_heredoc,
					&mut heredocs,
				);
			}
			b'"' => {
				pos = scan_quoted(src, pos, b'"', true);
				push(&mut tokens, BashTokenKind::String, start, pos);
				capture_heredoc(
					src,
					start,
					pos,
					BashTokenKind::String,
					&mut awaiting_heredoc,
					&mut heredocs,
				);
			}
			b'`' => {
				pos = scan_quoted(src, pos, b'`', true);
				push(&mut tokens, BashTokenKind::String, start, pos);
			}
			b'$' if matches!(src.get(pos + 1), Some(b'\'' | b'"')) => {
				let quote = src[pos + 1];
				pos = scan_quoted(src, pos + 1, quote, true);
				push(&mut tokens, BashTokenKind::String, start, pos);
				capture_heredoc(
					src,
					start,
					pos,
					BashTokenKind::String,
					&mut awaiting_heredoc,
					&mut heredocs,
				);
			}
			b'$' => {
				pos = scan_variable(src, pos);
				push(&mut tokens, BashTokenKind::Variable, start, pos);
			}
			byte if punctuation(byte) => {
				if let Some(end) = scan_operator(src, pos) {
					pos = end;
					push(&mut tokens, BashTokenKind::Operator, start, pos);
				} else {
					pos += 1;
					push(&mut tokens, BashTokenKind::Punctuation, start, pos);
				}
			}
			_ => {
				if let Some(end) = scan_operator(src, pos) {
					pos = end;
					push(&mut tokens, BashTokenKind::Operator, start, pos);
					awaiting_heredoc = match &src[start..pos] {
						b"<<" => Some(false),
						b"<<-" => Some(true),
						_ => awaiting_heredoc,
					};
				} else {
					pos = scan_word(src, pos);
					let kind = if pos == start {
						pos += 1;
						BashTokenKind::Unknown
					} else {
						BashTokenKind::Word
					};
					push(&mut tokens, kind, start, pos);
					capture_heredoc(src, start, pos, kind, &mut awaiting_heredoc, &mut heredocs);
				}
			}
		}
	}

	tokens
}

fn capture_heredoc(
	src: &[u8],
	start: usize,
	end: usize,
	kind: BashTokenKind,
	awaiting: &mut Option<bool>,
	heredocs: &mut VecDeque<HereDoc>,
) {
	let Some(strip_tabs) = awaiting.take() else {
		return;
	};
	if !matches!(kind, BashTokenKind::Word | BashTokenKind::String) {
		return;
	}
	let delimiter = normalize_heredoc_delimiter(&src[start..end]);
	if !delimiter.is_empty() {
		heredocs.push_back(HereDoc {
			delimiter,
			strip_tabs,
		});
	}
}

fn normalize_heredoc_delimiter(raw: &[u8]) -> Vec<u8> {
	let raw = if raw.len() >= 2 && matches!(raw[0], b'\'' | b'"') && raw.last() == Some(&raw[0]) {
		&raw[1..raw.len() - 1]
	} else if raw.len() >= 3
		&& raw[0] == b'$'
		&& matches!(raw[1], b'\'' | b'"')
		&& raw.last() == Some(&raw[1])
	{
		&raw[2..raw.len() - 1]
	} else {
		raw
	};

	let mut delimiter = Vec::with_capacity(raw.len());
	let mut pos = 0usize;
	while pos < raw.len() {
		if raw[pos] == b'\\' && pos + 1 < raw.len() {
			pos += 1;
		}
		delimiter.push(raw[pos]);
		pos += 1;
	}
	delimiter
}

fn scan_heredoc(src: &[u8], start: usize, heredoc: &HereDoc, out: &mut Vec<BashToken>) -> usize {
	let mut line_start = start;
	while line_start < src.len() {
		let line_end = src[line_start..]
			.iter()
			.position(|&byte| byte == b'\n')
			.map(|offset| line_start + offset)
			.unwrap_or(src.len());
		let compare_start = if heredoc.strip_tabs {
			let mut pos = line_start;
			while pos < line_end && src[pos] == b'\t' {
				pos += 1;
			}
			pos
		} else {
			line_start
		};
		if src[compare_start..line_end] == heredoc.delimiter {
			push(out, BashTokenKind::HereDocBody, start, line_start);
			push(out, BashTokenKind::HereDocDelimiter, line_start, line_end);
			return line_end;
		}
		if line_end == src.len() {
			push(out, BashTokenKind::HereDocBody, start, src.len());
			return src.len();
		}
		line_start = line_end + 1;
	}
	start
}

fn scan_to_newline(src: &[u8], start: usize) -> usize {
	src[start..]
		.iter()
		.position(|&byte| byte == b'\n')
		.map(|offset| start + offset)
		.unwrap_or(src.len())
}

fn scan_quoted(src: &[u8], start: usize, quote: u8, escaped: bool) -> usize {
	let mut pos = start + 1;
	while pos < src.len() {
		if escaped && src[pos] == b'\\' && pos + 1 < src.len() {
			pos += 2;
			continue;
		}
		if src[pos] == quote {
			return pos + 1;
		}
		pos += 1;
	}
	src.len()
}

fn scan_variable(src: &[u8], start: usize) -> usize {
	let Some(&next) = src.get(start + 1) else {
		return start + 1;
	};
	match next {
		b'{' => scan_balanced(src, start + 1, b'{', b'}'),
		b'(' => scan_balanced(src, start + 1, b'(', b')'),
		b'0'..=b'9' | b'?' | b'*' | b'@' | b'#' | b'$' | b'!' | b'-' => start + 2,
		byte if is_name_start(byte) => {
			let mut pos = start + 2;
			while pos < src.len() && is_name_continue(src[pos]) {
				pos += 1;
			}
			pos
		}
		_ => start + 1,
	}
}

fn scan_balanced(src: &[u8], start: usize, open: u8, close: u8) -> usize {
	let mut depth = 0usize;
	let mut pos = start;
	while pos < src.len() {
		match src[pos] {
			b'\\' if pos + 1 < src.len() => pos += 2,
			b'\'' => pos = scan_quoted(src, pos, b'\'', false),
			b'"' => pos = scan_quoted(src, pos, b'"', true),
			b'`' => pos = scan_quoted(src, pos, b'`', true),
			byte if byte == open => {
				depth += 1;
				pos += 1;
			}
			byte if byte == close => {
				depth = depth.saturating_sub(1);
				pos += 1;
				if depth == 0 {
					return pos;
				}
			}
			_ => pos += 1,
		}
	}
	src.len()
}

fn scan_word(src: &[u8], start: usize) -> usize {
	let mut pos = start;
	while pos < src.len() {
		if src[pos] == b'\\' && pos + 1 < src.len() {
			pos += 2;
			continue;
		}
		if word_boundary(src[pos]) || scan_operator(src, pos).is_some() {
			break;
		}
		pos += 1;
	}
	pos
}

fn word_boundary(byte: u8) -> bool {
	matches!(
		byte,
		b' ' | b'\t'
			| b'\r' | b'\n'
			| b'\'' | b'"'
			| b'`' | b'$'
			| b'(' | b')'
			| b'{' | b'}'
			| b'[' | b']'
	)
}

fn punctuation(byte: u8) -> bool {
	matches!(byte, b'(' | b')' | b'{' | b'}' | b'[' | b']')
}

fn scan_operator(src: &[u8], start: usize) -> Option<usize> {
	const OPERATORS: &[&[u8]] = &[
		b"&>>", b";;&", b"<<<", b"<<-", b"&&", b"||", b";;", b";&", b"|&", b">>", b"<<", b"<>",
		b">|", b"&>", b"<&", b">&", b"==", b"!=", b"=~", b"<=", b">=", b"+=", b"-=", b"*=", b"/=",
		b"%=", b"[[", b"]]", b"((", b"))", b";", b"|", b"&", b">", b"<", b"=", b"!",
	];
	OPERATORS
		.iter()
		.find(|operator| src[start..].starts_with(operator))
		.map(|operator| start + operator.len())
}

fn is_name_start(byte: u8) -> bool {
	byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_name_continue(byte: u8) -> bool {
	byte.is_ascii_alphanumeric() || byte == b'_'
}

fn push(out: &mut Vec<BashToken>, kind: BashTokenKind, start: usize, end: usize) {
	if start < end {
		out.push(BashToken { kind, start, end });
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

	fn text<'a>(src: &'a [u8], token: &BashToken) -> &'a [u8] {
		&src[token.start..token.end]
	}

	#[test]
	fn tokenizes_comments_without_confusing_parameter_hashes() {
		let src = b"echo foo#bar # note\nprintf '%s' $# ${value#prefix}\n";
		let tokens = tokenize(src);
		let comments = tokens
			.iter()
			.filter(|token| token.kind == BashTokenKind::Comment)
			.collect::<Vec<_>>();
		assert_eq!(comments.len(), 1);
		assert_eq!(text(src, comments[0]), b"# note");
		assert!(
			tokens.iter().any(|token| {
				token.kind == BashTokenKind::Variable && text(src, token) == b"$#"
			})
		);
		assert!(tokens.iter().any(|token| {
			token.kind == BashTokenKind::Variable && text(src, token) == b"${value#prefix}"
		}));
	}

	#[test]
	fn tokenizes_quotes_variables_and_command_substitutions() {
		let src = b"name=$USER; one='literal'; two=\"double $USER\"; out=$(printf x)\n";
		let tokens = tokenize(src);
		assert!(tokens.iter().any(|token| {
			token.kind == BashTokenKind::Variable && text(src, token) == b"$USER"
		}));
		assert!(tokens.iter().any(|token| {
			token.kind == BashTokenKind::String && text(src, token) == b"'literal'"
		}));
		assert!(tokens.iter().any(|token| {
			token.kind == BashTokenKind::String && text(src, token) == b"\"double $USER\""
		}));
		assert!(tokens.iter().any(|token| {
			token.kind == BashTokenKind::Variable && text(src, token) == b"$(printf x)"
		}));
	}

	#[test]
	fn tokenizes_control_and_redirection_operators() {
		let src = b"build && test -f out || exit 1; cat <in >>out\n";
		let operators = tokenize(src)
			.into_iter()
			.filter(|token| token.kind == BashTokenKind::Operator)
			.map(|token| text(src, &token).to_vec())
			.collect::<Vec<_>>();
		for expected in [b"&&".as_slice(), b"||", b";", b"<", b">>"] {
			assert!(operators.iter().any(|operator| operator == expected));
		}
	}

	#[test]
	fn tokenizes_simple_and_tab_stripped_heredocs() {
		let src = b"cat <<EOF\nhello $USER\nEOF\ncat <<-'END'\n\tbody\n\tEND\n";
		let tokens = tokenize(src);
		let bodies = tokens
			.iter()
			.filter(|token| token.kind == BashTokenKind::HereDocBody)
			.map(|token| text(src, token))
			.collect::<Vec<_>>();
		assert_eq!(bodies, vec![b"hello $USER\n".as_slice(), b"\tbody\n"]);
		let delimiters = tokens
			.iter()
			.filter(|token| token.kind == BashTokenKind::HereDocDelimiter)
			.map(|token| text(src, token))
			.collect::<Vec<_>>();
		assert_eq!(delimiters, vec![b"EOF".as_slice(), b"\tEND"]);
	}

	#[test]
	fn malformed_constructs_are_bounded_and_non_overlapping() {
		for src in [
			b"echo 'unterminated".as_slice(),
			b"echo ${unterminated".as_slice(),
			b"cat <<EOF\nunterminated\n".as_slice(),
			b"echo \\".as_slice(),
		] {
			let tokens = tokenize(src);
			let mut end = 0usize;
			for token in tokens {
				assert!(token.start >= end);
				assert!(token.start < token.end);
				assert!(token.end <= src.len());
				end = token.end;
			}
		}
	}

	#[test]
	fn translate_offset_uses_nearest_mapping_base() {
		let map = [(0, DocByte::new(100)), (10, DocByte::new(500))];
		assert_eq!(translate_offset(4, &map), DocByte::new(104));
		assert_eq!(translate_offset(13, &map), DocByte::new(503));
	}
}
