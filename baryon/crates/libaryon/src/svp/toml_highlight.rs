//! TOML highlighting — walks the raw token stream with a small state
//! machine and assigns `TokenCategory` to each meaningful span.
//!
//! Category choices optimize for visual contrast across every bundled
//! theme. In particular, keys are classified as `Function` (not `Variable`)
//! because `Variable` collapses to foreground in most themes and would
//! render `name = "x"` as plain text.

use crate::core::DocByte;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use crate::svp::toml_parse::{TomlToken, TomlTokenKind, tokenize, translate_offset};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
	TopKey,
	TopValue,
	Header,
	Array,
	InlineKey,
	InlineValue,
}

pub fn highlight_toml(
	viewport_bytes: &[u8],
	offset_map: &[(usize, DocByte)],
) -> Vec<HighlightSpan> {
	let tokens = tokenize(viewport_bytes);
	let mut out = Vec::with_capacity(tokens.len());
	let mut stack: Vec<Mode> = vec![Mode::TopKey];

	let mut i = 0usize;
	while i < tokens.len() {
		let token = tokens[i];
		let mode = *stack.last().unwrap_or(&Mode::TopKey);

		match token.kind {
			TomlTokenKind::Whitespace => {
				i += 1;
				continue;
			}
			TomlTokenKind::Newline => {
				// At top-level, a newline ends the current value and returns
				// us to key mode. Inside a bracketed array or inline table
				// (stack depth > 1), newlines are whitespace.
				if stack.len() == 1 && matches!(mode, Mode::TopValue) {
					stack[0] = Mode::TopKey;
				}
				i += 1;
				continue;
			}
			TomlTokenKind::Comment => {
				push(&mut out, token, TokenCategory::Comment, offset_map);
				i += 1;
				continue;
			}
			_ => {}
		}

		match mode {
			Mode::TopKey => {
				match token.kind {
					TomlTokenKind::LBracket => {
						push(&mut out, token, TokenCategory::Type, offset_map);
						// Consume a second `[` for `[[array.of.tables]]`.
						if let Some(next) = peek_significant(&tokens, i + 1)
							&& tokens[next].kind == TomlTokenKind::LBracket
						{
							push(&mut out, tokens[next], TokenCategory::Type, offset_map);
							i = next;
						}
						stack.push(Mode::Header);
					}
					TomlTokenKind::Ident | TomlTokenKind::String => {
						push(&mut out, token, TokenCategory::Function, offset_map);
					}
					TomlTokenKind::Dot => {
						push(&mut out, token, TokenCategory::Punctuation, offset_map);
					}
					TomlTokenKind::Equals => {
						push(&mut out, token, TokenCategory::Operator, offset_map);
						stack.pop();
						stack.push(Mode::TopValue);
					}
					_ => {}
				}
			}
			Mode::Header => match token.kind {
				TomlTokenKind::Ident | TomlTokenKind::String => {
					push(&mut out, token, TokenCategory::Type, offset_map);
				}
				TomlTokenKind::Dot => {
					push(&mut out, token, TokenCategory::Type, offset_map);
				}
				TomlTokenKind::RBracket => {
					push(&mut out, token, TokenCategory::Type, offset_map);
					if let Some(next) = peek_significant(&tokens, i + 1)
						&& tokens[next].kind == TomlTokenKind::RBracket
					{
						push(&mut out, tokens[next], TokenCategory::Type, offset_map);
						i = next;
					}
					stack.pop();
				}
				_ => {}
			},
			Mode::TopValue | Mode::InlineValue | Mode::Array => {
				match token.kind {
					TomlTokenKind::String => {
						push(&mut out, token, TokenCategory::String, offset_map);
					}
					TomlTokenKind::Number => {
						push(&mut out, token, TokenCategory::Number, offset_map);
					}
					TomlTokenKind::Ident => {
						let category = classify_ident_value(viewport_bytes, token);
						push(&mut out, token, category, offset_map);
					}
					TomlTokenKind::Dot => {
						push(&mut out, token, TokenCategory::Punctuation, offset_map);
					}
					TomlTokenKind::LBracket => {
						push(&mut out, token, TokenCategory::Punctuation, offset_map);
						stack.push(Mode::Array);
					}
					TomlTokenKind::RBracket => {
						push(&mut out, token, TokenCategory::Punctuation, offset_map);
						if matches!(mode, Mode::Array) {
							stack.pop();
						}
					}
					TomlTokenKind::LBrace => {
						push(&mut out, token, TokenCategory::Punctuation, offset_map);
						stack.push(Mode::InlineKey);
					}
					TomlTokenKind::RBrace => {
						push(&mut out, token, TokenCategory::Punctuation, offset_map);
						// Walk upward past any value mode until the matching InlineKey pops.
						while let Some(top) = stack.last() {
							match top {
								Mode::InlineKey | Mode::InlineValue => {
									stack.pop();
									break;
								}
								_ => {
									stack.pop();
								}
							}
						}
					}
					TomlTokenKind::Comma => {
						push(&mut out, token, TokenCategory::Punctuation, offset_map);
						if matches!(mode, Mode::InlineValue) {
							let depth = stack.len();
							stack[depth - 1] = Mode::InlineKey;
						}
					}
					_ => {}
				}
			}
			Mode::InlineKey => match token.kind {
				TomlTokenKind::Ident | TomlTokenKind::String => {
					push(&mut out, token, TokenCategory::Function, offset_map);
				}
				TomlTokenKind::Dot => {
					push(&mut out, token, TokenCategory::Punctuation, offset_map);
				}
				TomlTokenKind::Equals => {
					push(&mut out, token, TokenCategory::Operator, offset_map);
					let depth = stack.len();
					stack[depth - 1] = Mode::InlineValue;
				}
				TomlTokenKind::RBrace => {
					push(&mut out, token, TokenCategory::Punctuation, offset_map);
					stack.pop();
				}
				_ => {}
			},
		}

		i += 1;
	}

	out.sort_unstable_by_key(|span| span.start);
	out
}

fn peek_significant(tokens: &[TomlToken], from: usize) -> Option<usize> {
	let mut i = from;
	while i < tokens.len() {
		match tokens[i].kind {
			TomlTokenKind::Whitespace | TomlTokenKind::Newline | TomlTokenKind::Comment => {
				i += 1;
			}
			_ => return Some(i),
		}
	}
	None
}

fn classify_ident_value(src: &[u8], token: TomlToken) -> TokenCategory {
	let slice = &src[token.start..token.end];
	match slice {
		b"true" | b"false" => TokenCategory::Constant,
		b"inf" | b"nan" | b"+inf" | b"-inf" | b"+nan" | b"-nan" => TokenCategory::Number,
		_ => TokenCategory::Variable,
	}
}

fn push(
	out: &mut Vec<HighlightSpan>,
	token: TomlToken,
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
		highlight_toml(src, &[(0, DocByte::ZERO)])
	}

	#[test]
	fn key_value_pair_is_classified() {
		let src = b"name = \"baryon\"\n";
		let h = spans(src);

		assert!(
			h.iter()
				.any(|s| s.category == TokenCategory::Function && s.start == DocByte::ZERO)
		);
		assert!(
			h.iter()
				.any(|s| s.category == TokenCategory::Operator && s.start == DocByte::new(5))
		);
		assert!(
			h.iter()
				.any(|s| s.category == TokenCategory::String && s.start == DocByte::new(7))
		);
	}

	#[test]
	fn table_header_name_is_type() {
		let src = b"[dependencies]\n";
		let h = spans(src);
		assert!(h.iter().any(|s| s.category == TokenCategory::Type
			&& s.start == DocByte::new(1)
			&& s.end == DocByte::new(13)));
	}

	#[test]
	fn array_of_tables_header_tokens_all_render_as_type() {
		let src = b"[[bin]]\n";
		let h = spans(src);
		let type_count = h
			.iter()
			.filter(|s| s.category == TokenCategory::Type)
			.count();
		// `[`, `[`, `bin`, `]`, `]` → 5 Type spans
		assert_eq!(type_count, 5);
	}

	#[test]
	fn booleans_and_numbers_in_value_position() {
		let src = b"ready = true\ncount = 42\n";
		let h = spans(src);
		assert!(h.iter().any(|s| s.category == TokenCategory::Constant));
		assert!(h.iter().any(|s| s.category == TokenCategory::Number));
	}

	#[test]
	fn comment_is_comment_in_any_mode() {
		let src = b"# hello\nname = \"x\" # trailing\n";
		let h = spans(src);
		let comment_count = h
			.iter()
			.filter(|s| s.category == TokenCategory::Comment)
			.count();
		assert_eq!(comment_count, 2);
	}

	#[test]
	fn inline_table_keys_and_values() {
		let src = b"point = { x = 1, y = 2 }\n";
		let h = spans(src);
		// `point`, `x`, `y` are all keys → Function
		let key_count = h
			.iter()
			.filter(|s| s.category == TokenCategory::Function)
			.count();
		assert_eq!(key_count, 3);
		let num_count = h
			.iter()
			.filter(|s| s.category == TokenCategory::Number)
			.count();
		assert_eq!(num_count, 2);
	}

	#[test]
	fn multiline_array_stays_in_value_mode() {
		let src = b"deps = [\n  \"a\",\n  \"b\",\n]\n";
		let h = spans(src);
		let str_count = h
			.iter()
			.filter(|s| s.category == TokenCategory::String)
			.count();
		assert_eq!(str_count, 2);
	}

	#[test]
	fn dotted_key_both_parts_are_functions() {
		let src = b"package.name = \"baryon\"\n";
		let h = spans(src);
		let key_count = h
			.iter()
			.filter(|s| s.category == TokenCategory::Function)
			.count();
		assert_eq!(key_count, 2);
	}

	#[test]
	fn offset_map_applied_to_spans() {
		let src = b"x = 1\n";
		let h = highlight_toml(src, &[(0, DocByte::new(500))]);
		assert!(
			h.iter()
				.any(|s| s.category == TokenCategory::Function && s.start == DocByte::new(500))
		);
	}
}
