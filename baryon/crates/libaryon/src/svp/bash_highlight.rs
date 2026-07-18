use crate::core::DocByte;
use crate::svp::bash_parse::{BashToken, BashTokenKind, tokenize, translate_offset};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

#[derive(Debug)]
struct HighlightState {
	expect_command: bool,
	expect_function_name: bool,
	expect_variable: bool,
	declaration_arguments: bool,
	assignment_value: bool,
}

impl Default for HighlightState {
	fn default() -> Self {
		Self {
			expect_command: true,
			expect_function_name: false,
			expect_variable: false,
			declaration_arguments: false,
			assignment_value: false,
		}
	}
}

impl HighlightState {
	fn reset_command(&mut self) {
		self.expect_command = true;
		self.expect_function_name = false;
		self.expect_variable = false;
		self.declaration_arguments = false;
		self.assignment_value = false;
	}
}

pub fn highlight_bash(
	viewport_bytes: &[u8],
	offset_map: &[(usize, DocByte)],
) -> Vec<HighlightSpan> {
	let tokens = tokenize(viewport_bytes);
	let mut out = Vec::new();
	let mut state = HighlightState::default();

	for (idx, token) in tokens.iter().copied().enumerate() {
		let text = &viewport_bytes[token.start..token.end];
		match token.kind {
			BashTokenKind::Whitespace => {
				state.assignment_value = false;
			}
			BashTokenKind::Newline => state.reset_command(),
			BashTokenKind::Shebang => push(&mut out, token, TokenCategory::Attribute, offset_map),
			BashTokenKind::Comment => push(&mut out, token, TokenCategory::Comment, offset_map),
			BashTokenKind::String
			| BashTokenKind::HereDocBody
			| BashTokenKind::HereDocDelimiter => {
				push(&mut out, token, TokenCategory::String, offset_map);
				if state.expect_command && !state.assignment_value {
					state.expect_command = false;
				}
			}
			BashTokenKind::Variable => {
				push(&mut out, token, TokenCategory::Variable, offset_map);
				if state.expect_command && !state.assignment_value {
					state.expect_command = false;
				}
			}
			BashTokenKind::Operator => {
				let category = if matches!(text, b"[[" | b"]]") {
					TokenCategory::Keyword
				} else {
					TokenCategory::Operator
				};
				push(&mut out, token, category, offset_map);
				if command_separator(text) {
					state.reset_command();
				} else if matches!(text, b"[[" | b"((") {
					state.expect_command = false;
					state.declaration_arguments = false;
				}
			}
			BashTokenKind::Punctuation => {
				push(&mut out, token, TokenCategory::Punctuation, offset_map);
				if text == b"{" || (text == b"(" && state.expect_command) {
					state.reset_command();
				}
			}
			BashTokenKind::Word => {
				classify_word(
					viewport_bytes,
					&tokens,
					idx,
					token,
					text,
					offset_map,
					&mut state,
					&mut out,
				);
			}
			BashTokenKind::Unknown => {}
		}
	}

	out.sort_unstable_by_key(|span| span.start);
	out
}

fn classify_word(
	src: &[u8],
	tokens: &[BashToken],
	idx: usize,
	token: BashToken,
	text: &[u8],
	offset_map: &[(usize, DocByte)],
	state: &mut HighlightState,
	out: &mut Vec<HighlightSpan>,
) {
	if state.assignment_value {
		if text.iter().all(u8::is_ascii_digit) {
			push(out, token, TokenCategory::Number, offset_map);
		}
		return;
	}

	if state.expect_function_name {
		push(out, token, TokenCategory::Function, offset_map);
		state.expect_function_name = false;
		state.expect_command = false;
		return;
	}

	if is_keyword(text) {
		push(out, token, TokenCategory::Keyword, offset_map);
		apply_keyword_state(text, state);
		return;
	}

	if state.expect_variable && valid_name(text) {
		push(out, token, TokenCategory::Variable, offset_map);
		state.expect_variable = false;
		return;
	}

	let assignment = assignment_operator_after(src, tokens, idx).is_some() && valid_name(text);
	if state.expect_command && assignment {
		push(out, token, TokenCategory::Variable, offset_map);
		state.assignment_value = true;
		return;
	}

	if state.declaration_arguments && valid_name(text) {
		push(out, token, TokenCategory::Variable, offset_map);
		state.assignment_value = assignment;
		return;
	}

	if state.expect_command {
		push(out, token, TokenCategory::Function, offset_map);
		state.expect_command = false;
		state.declaration_arguments = declaration_command(text);
		if function_definition_after(src, tokens, idx) {
			state.declaration_arguments = false;
		}
		return;
	}

	if text.iter().all(u8::is_ascii_digit) {
		push(out, token, TokenCategory::Number, offset_map);
	}
}

fn assignment_operator_after<'a>(
	src: &'a [u8],
	tokens: &'a [BashToken],
	idx: usize,
) -> Option<&'a [u8]> {
	let current = tokens.get(idx)?;
	let next = tokens.get(idx + 1)?;
	if next.start != current.end || next.kind != BashTokenKind::Operator {
		return None;
	}
	let operator = &src[next.start..next.end];
	matches!(operator, b"=" | b"+=").then_some(operator)
}

fn function_definition_after(src: &[u8], tokens: &[BashToken], idx: usize) -> bool {
	let Some(open_idx) = next_significant(tokens, idx + 1) else {
		return false;
	};
	let Some(close_idx) = next_significant(tokens, open_idx + 1) else {
		return false;
	};
	tokens[open_idx].kind == BashTokenKind::Punctuation
		&& &src[tokens[open_idx].start..tokens[open_idx].end] == b"("
		&& tokens[close_idx].kind == BashTokenKind::Punctuation
		&& &src[tokens[close_idx].start..tokens[close_idx].end] == b")"
}

fn next_significant(tokens: &[BashToken], mut idx: usize) -> Option<usize> {
	while idx < tokens.len() {
		if tokens[idx].kind != BashTokenKind::Whitespace {
			return Some(idx);
		}
		idx += 1;
	}
	None
}

fn is_keyword(word: &[u8]) -> bool {
	matches!(
		word,
		b"if"
			| b"then" | b"elif"
			| b"else" | b"fi"
			| b"for" | b"select"
			| b"while"
			| b"until"
			| b"do" | b"done"
			| b"case" | b"in"
			| b"esac" | b"function"
			| b"time" | b"coproc"
			| b"break"
			| b"continue"
			| b"return"
	)
}

fn apply_keyword_state(word: &[u8], state: &mut HighlightState) {
	state.assignment_value = false;
	state.declaration_arguments = false;
	match word {
		b"function" => {
			state.expect_function_name = true;
			state.expect_command = false;
		}
		b"for" | b"select" => {
			state.expect_variable = true;
			state.expect_command = false;
		}
		b"if" | b"then" | b"elif" | b"else" | b"while" | b"until" | b"do" | b"time" | b"coproc" => {
			state.expect_command = true;
		}
		_ => state.expect_command = false,
	}
}

fn declaration_command(word: &[u8]) -> bool {
	matches!(
		word,
		b"declare" | b"export" | b"local" | b"readonly" | b"typeset"
	)
}

fn command_separator(operator: &[u8]) -> bool {
	matches!(
		operator,
		b";" | b";;" | b";&" | b";;&" | b"|" | b"|&" | b"&" | b"&&" | b"||"
	)
}

fn valid_name(word: &[u8]) -> bool {
	word.first()
		.is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
		&& word
			.iter()
			.skip(1)
			.all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

fn push(
	out: &mut Vec<HighlightSpan>,
	token: BashToken,
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

	fn spans(src: &[u8]) -> Vec<HighlightSpan> {
		highlight_bash(src, &[(0, DocByte::ZERO)])
	}

	fn has(src: &[u8], spans: &[HighlightSpan], needle: &[u8], category: TokenCategory) -> bool {
		let Some(start) = src
			.windows(needle.len())
			.position(|window| window == needle)
		else {
			return false;
		};
		spans.iter().any(|span| {
			span.start == DocByte::new(start as u64)
				&& span.end == DocByte::new((start + needle.len()) as u64)
				&& span.category == category
		})
	}

	#[test]
	fn highlights_control_flow_commands_variables_and_strings() {
		let src = b"#!/usr/bin/env bash\nname=world\nif [[ -n $name ]]; then\n  printf '%s\\n' \"$name\"\nfi\n";
		let highlights = spans(src);
		assert!(has(
			src,
			&highlights,
			b"#!/usr/bin/env bash",
			TokenCategory::Attribute
		));
		assert!(has(src, &highlights, b"name", TokenCategory::Variable));
		assert!(has(src, &highlights, b"if", TokenCategory::Keyword));
		assert!(has(src, &highlights, b"then", TokenCategory::Keyword));
		assert!(has(src, &highlights, b"fi", TokenCategory::Keyword));
		assert!(has(src, &highlights, b"printf", TokenCategory::Function));
		assert!(has(src, &highlights, b"$name", TokenCategory::Variable));
		assert!(has(src, &highlights, b"'%s\\n'", TokenCategory::String));
		assert!(!has(src, &highlights, b"-n", TokenCategory::Function));
	}

	#[test]
	fn assignments_do_not_consume_the_command_position() {
		let src = b"FOO=bar BAR=$HOME env printenv\n";
		let highlights = spans(src);
		assert!(has(src, &highlights, b"FOO", TokenCategory::Variable));
		assert!(has(src, &highlights, b"BAR", TokenCategory::Variable));
		assert!(has(src, &highlights, b"$HOME", TokenCategory::Variable));
		assert!(has(src, &highlights, b"env", TokenCategory::Function));
		assert!(!has(src, &highlights, b"bar", TokenCategory::Function));
	}

	#[test]
	fn highlights_function_and_declaration_names() {
		let src = b"greet() {\n  local name=$1\n  echo \"hello\"\n}\n";
		let highlights = spans(src);
		assert!(has(src, &highlights, b"greet", TokenCategory::Function));
		assert!(has(src, &highlights, b"local", TokenCategory::Function));
		assert!(has(src, &highlights, b"name", TokenCategory::Variable));
		assert!(has(src, &highlights, b"echo", TokenCategory::Function));
	}

	#[test]
	fn heredoc_body_and_delimiter_are_strings() {
		let src = b"cat <<EOF\nhello\nEOF\n";
		let highlights = spans(src);
		assert!(has(src, &highlights, b"hello\n", TokenCategory::String));
		let delimiter_start = src.windows(3).rposition(|window| window == b"EOF").unwrap();
		assert!(highlights.iter().any(|span| {
			span.start == DocByte::new(delimiter_start as u64)
				&& span.category == TokenCategory::String
		}));
	}

	#[test]
	fn applies_global_offsets_and_emits_ordered_spans() {
		let src = b"if echo $value; then printf x; fi\n";
		let highlights = highlight_bash(src, &[(0, DocByte::new(700))]);
		assert!(
			highlights
				.iter()
				.all(|span| span.start >= DocByte::new(700))
		);
		assert!(
			highlights
				.windows(2)
				.all(|pair| pair[0].end <= pair[1].start)
		);
	}
}
