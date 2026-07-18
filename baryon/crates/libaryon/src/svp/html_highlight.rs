use crate::core::DocByte;
use crate::svp::html_parse::{HtmlToken, HtmlTokenKind, tokenize, translate_offset};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

pub fn highlight_html(
	viewport_bytes: &[u8],
	offset_map: &[(usize, DocByte)],
) -> Vec<HighlightSpan> {
	let mut out = Vec::new();
	for token in tokenize(viewport_bytes) {
		push(&mut out, token, classify(token), offset_map);
	}
	out.sort_unstable_by_key(|span| span.start);
	out
}

fn classify(token: HtmlToken) -> TokenCategory {
	match token.kind {
		HtmlTokenKind::Comment => TokenCategory::Comment,
		HtmlTokenKind::Doctype => TokenCategory::Keyword,
		HtmlTokenKind::TagDelimiter => TokenCategory::Punctuation,
		HtmlTokenKind::TagName => TokenCategory::Type,
		HtmlTokenKind::AttrName => TokenCategory::Function,
		HtmlTokenKind::Equals => TokenCategory::Operator,
		HtmlTokenKind::AttrValue | HtmlTokenKind::RawText => TokenCategory::String,
		HtmlTokenKind::Entity => TokenCategory::Constant,
	}
}

fn push(
	out: &mut Vec<HighlightSpan>,
	token: HtmlToken,
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
		highlight_html(src, &[(0, DocByte::ZERO)])
	}

	#[test]
	fn highlights_tags_attributes_and_values() {
		let src = br#"<main class="app" data-count=3></main>"#;
		let h = spans(src);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Type && span.start == DocByte::new(1))
		);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Function
					&& span.start == DocByte::new(6))
		);
		assert!(
			h.iter().any(
				|span| span.category == TokenCategory::String && span.start == DocByte::new(12)
			)
		);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Operator
					&& span.start == DocByte::new(11))
		);
	}

	#[test]
	fn highlights_comments_doctype_and_entities() {
		let src = b"<!doctype html>\n<!-- note -->\n<p>&amp;</p>";
		let h = spans(src);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Keyword && span.start == DocByte::ZERO)
		);
		assert!(h.iter().any(|span| span.category == TokenCategory::Comment));
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Constant)
		);
	}

	#[test]
	fn highlights_script_body_as_string() {
		let src = b"<script>let x = 1;</script>";
		let h = spans(src);
		assert!(h.iter().any(|span| span.category == TokenCategory::String
			&& span.start == DocByte::new(8)
			&& span.end == DocByte::new(18)));
	}

	#[test]
	fn offset_map_is_applied() {
		let h = highlight_html(b"<p>x</p>", &[(0, DocByte::new(500))]);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Punctuation
					&& span.start == DocByte::new(500))
		);
	}
}
