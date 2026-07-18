use crate::core::DocByte;
use crate::svp::markdown_parse::{MarkdownToken, MarkdownTokenKind, tokenize, translate_offset};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

pub fn highlight_markdown(
	viewport_bytes: &[u8],
	offset_map: &[(usize, DocByte)],
) -> Vec<HighlightSpan> {
	let mut out = Vec::new();
	for token in tokenize(viewport_bytes) {
		push(&mut out, token, classify(token, viewport_bytes), offset_map);
	}
	out.sort_unstable_by_key(|span| span.start);
	out
}

fn classify(token: MarkdownToken, src: &[u8]) -> TokenCategory {
	match token.kind {
		MarkdownTokenKind::HeadingMarker | MarkdownTokenKind::HeadingText => TokenCategory::Type,
		MarkdownTokenKind::Fence
		| MarkdownTokenKind::HorizontalRule
		| MarkdownTokenKind::Punctuation => TokenCategory::Punctuation,
		MarkdownTokenKind::ListMarker if src[token.start].is_ascii_digit() => TokenCategory::Number,
		MarkdownTokenKind::ListMarker => TokenCategory::Operator,
		MarkdownTokenKind::FenceInfo | MarkdownTokenKind::LinkLabel => TokenCategory::Function,
		MarkdownTokenKind::CodeBlockText
		| MarkdownTokenKind::InlineCode
		| MarkdownTokenKind::LinkDestination => TokenCategory::String,
		MarkdownTokenKind::BlockQuoteMarker | MarkdownTokenKind::HtmlComment => {
			TokenCategory::Comment
		}
		MarkdownTokenKind::Emphasis => TokenCategory::Operator,
	}
}

fn push(
	out: &mut Vec<HighlightSpan>,
	token: MarkdownToken,
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
		highlight_markdown(src, &[(0, DocByte::ZERO)])
	}

	#[test]
	fn headings_render_as_type() {
		let h = spans(b"# Title\n");
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Type && span.start == DocByte::ZERO)
		);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Type && span.start == DocByte::new(2))
		);
	}

	#[test]
	fn links_render_label_and_destination() {
		let h = spans(b"[Baryon](https://example.com)\n");
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Function
					&& span.start == DocByte::new(1))
		);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::String && span.start == DocByte::new(9))
		);
	}

	#[test]
	fn fenced_code_body_renders_as_string() {
		let h = spans(b"```rust\nlet x = 1;\n```\n");
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::String && span.start == DocByte::new(8))
		);
	}

	#[test]
	fn list_markers_render_with_visible_categories() {
		let h = spans(b"- item\n12. item\n");
		assert!(h.iter().any(|span| span.category == TokenCategory::Operator
			&& span.start == DocByte::ZERO
			&& span.end == DocByte::new(1)));
		assert!(h.iter().any(|span| span.category == TokenCategory::Number
			&& span.start == DocByte::new(7)
			&& span.end == DocByte::new(10)));
	}

	#[test]
	fn offset_map_is_applied() {
		let h = highlight_markdown(b"# Title\n", &[(0, DocByte::new(500))]);
		assert!(
			h.iter()
				.any(|span| span.category == TokenCategory::Type && span.start == DocByte::new(500))
		);
	}
}
