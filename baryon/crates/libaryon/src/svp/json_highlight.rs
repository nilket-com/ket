use crate::core::DocByte;
use crate::svp::json_parse::{JsonToken, JsonTokenKind, tokenize, translate_offset};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectExpect {
	KeyOrEnd,
	Colon,
	Value,
	CommaOrEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrayExpect {
	ValueOrEnd,
	CommaOrEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Context {
	Object(ObjectExpect),
	Array(ArrayExpect),
}

pub fn highlight_json(
	viewport_bytes: &[u8],
	offset_map: &[(usize, DocByte)],
) -> Vec<HighlightSpan> {
	let tokens = tokenize(viewport_bytes);
	let mut out = Vec::with_capacity(tokens.len());
	let mut stack = Vec::new();

	for token in tokens {
		match token.kind {
			JsonTokenKind::Whitespace | JsonTokenKind::Newline | JsonTokenKind::Unknown => {}
			JsonTokenKind::LBrace => {
				push(&mut out, token, TokenCategory::Punctuation, offset_map);
				stack.push(Context::Object(ObjectExpect::KeyOrEnd));
			}
			JsonTokenKind::RBrace => {
				push(&mut out, token, TokenCategory::Punctuation, offset_map);
				if matches!(stack.last(), Some(Context::Object(_))) {
					stack.pop();
					mark_value_consumed(&mut stack);
				}
			}
			JsonTokenKind::LBracket => {
				push(&mut out, token, TokenCategory::Punctuation, offset_map);
				stack.push(Context::Array(ArrayExpect::ValueOrEnd));
			}
			JsonTokenKind::RBracket => {
				push(&mut out, token, TokenCategory::Punctuation, offset_map);
				if matches!(stack.last(), Some(Context::Array(_))) {
					stack.pop();
					mark_value_consumed(&mut stack);
				}
			}
			JsonTokenKind::Colon => {
				push(&mut out, token, TokenCategory::Operator, offset_map);
				if let Some(Context::Object(expect)) = stack.last_mut()
					&& matches!(expect, ObjectExpect::Colon)
				{
					*expect = ObjectExpect::Value;
				}
			}
			JsonTokenKind::Comma => {
				push(&mut out, token, TokenCategory::Punctuation, offset_map);
				match stack.last_mut() {
					Some(Context::Object(expect)) if matches!(expect, ObjectExpect::CommaOrEnd) => {
						*expect = ObjectExpect::KeyOrEnd;
					}
					Some(Context::Array(expect)) if matches!(expect, ArrayExpect::CommaOrEnd) => {
						*expect = ArrayExpect::ValueOrEnd;
					}
					_ => {}
				}
			}
			JsonTokenKind::String => {
				let category = if object_is_expecting_key(&stack) {
					if let Some(Context::Object(expect)) = stack.last_mut() {
						*expect = ObjectExpect::Colon;
					}
					TokenCategory::Function
				} else {
					mark_value_consumed(&mut stack);
					TokenCategory::String
				};
				push(&mut out, token, category, offset_map);
			}
			JsonTokenKind::Number => {
				push(&mut out, token, TokenCategory::Number, offset_map);
				mark_value_consumed(&mut stack);
			}
			JsonTokenKind::True | JsonTokenKind::False | JsonTokenKind::Null => {
				push(&mut out, token, TokenCategory::Constant, offset_map);
				mark_value_consumed(&mut stack);
			}
		}
	}

	out.sort_unstable_by_key(|span| span.start);
	out
}

fn object_is_expecting_key(stack: &[Context]) -> bool {
	matches!(stack.last(), Some(Context::Object(ObjectExpect::KeyOrEnd)))
}

fn mark_value_consumed(stack: &mut [Context]) {
	match stack.last_mut() {
		Some(Context::Object(expect)) if matches!(expect, ObjectExpect::Value) => {
			*expect = ObjectExpect::CommaOrEnd;
		}
		Some(Context::Array(expect)) if matches!(expect, ArrayExpect::ValueOrEnd) => {
			*expect = ArrayExpect::CommaOrEnd;
		}
		_ => {}
	}
}

fn push(
	out: &mut Vec<HighlightSpan>,
	token: JsonToken,
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
		highlight_json(src, &[(0, DocByte::ZERO)])
	}

	fn pos(src: &[u8], needle: &[u8]) -> DocByte {
		let offset = src
			.windows(needle.len())
			.position(|window| window == needle)
			.expect("needle should exist");
		DocByte::new(offset as u64)
	}

	#[test]
	fn object_keys_and_string_values_use_distinct_categories() {
		let src = br#"{"name":"baryon","nested":{"ok":"yes"}}"#;
		let h = spans(src);
		assert!(
			h.iter().any(|span| span.category == TokenCategory::Function
				&& span.start == pos(src, br#""name""#))
		);
		assert!(
			h.iter().any(|span| span.category == TokenCategory::String
				&& span.start == pos(src, br#""baryon""#))
		);
		assert!(
			h.iter().any(|span| span.category == TokenCategory::Function
				&& span.start == pos(src, br#""ok""#))
		);
		assert!(
			h.iter().any(|span| span.category == TokenCategory::String
				&& span.start == pos(src, br#""yes""#))
		);
	}

	#[test]
	fn array_strings_are_values_not_keys() {
		let src = br#"{"items":["one","two"]}"#;
		let h = spans(src);
		assert!(h.iter().any(|span| span.category == TokenCategory::Function
			&& span.start == pos(src, br#""items""#)));
		assert!(
			h.iter().any(|span| span.category == TokenCategory::String
				&& span.start == pos(src, br#""one""#))
		);
		assert!(
			h.iter().any(|span| span.category == TokenCategory::String
				&& span.start == pos(src, br#""two""#))
		);
	}

	#[test]
	fn primitives_use_number_and_constant_categories() {
		let src = br#"{"count":12.5,"ok":true,"missing":null,"off":false}"#;
		let h = spans(src);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Number
					&& span.start == pos(src, b"12.5"))
		);
		assert!(h.iter().any(
			|span| span.category == TokenCategory::Constant && span.start == pos(src, b"true")
		));
		assert!(h.iter().any(
			|span| span.category == TokenCategory::Constant && span.start == pos(src, b"null")
		));
		assert!(h.iter().any(
			|span| span.category == TokenCategory::Constant && span.start == pos(src, b"false")
		));
	}

	#[test]
	fn punctuation_and_colons_are_highlighted() {
		let src = br#"{"a":[1,2]}"#;
		let h = spans(src);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Punctuation
					&& span.start == DocByte::ZERO)
		);
		assert!(
			h.iter().any(
				|span| span.category == TokenCategory::Operator && span.start == pos(src, b":")
			)
		);
		assert!(h.iter().any(
			|span| span.category == TokenCategory::Punctuation && span.start == pos(src, b"[")
		));
	}

	#[test]
	fn offset_map_is_applied() {
		let h = highlight_json(br#"{"a":1}"#, &[(0, DocByte::new(500))]);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Punctuation
					&& span.start == DocByte::new(500))
		);
	}
}
