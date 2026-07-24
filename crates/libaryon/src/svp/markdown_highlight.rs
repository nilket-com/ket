use crate::core::{DocByte, Rgb};
use crate::svp::markdown_parse::{MarkdownToken, MarkdownTokenKind, tokenize, translate_offset};
use crate::svp::rust_highlight::{CATEGORY_COUNT, HighlightSpan, TokenCategory};

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

/// A Markdown construct, named independently of how it appears in source.
///
/// A preview renders a heading with no `#` and a bold run with no `**`, so it
/// cannot classify by token. Naming the construct lets every surface ask the
/// same question — what color is a heading? — and get one answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkdownConstruct {
	/// Prose. Colored by `BodyText`, which is also what a pane paints text no
	/// highlighter claimed — so themed prose reaches the document and its
	/// preview through the same value.
	Body,
	Heading,
	InlineCode,
	CodeText,
	/// The `*`/`**` runs, and by extension the text they wrap.
	Emphasis,
	BulletMarker,
	OrderedMarker,
	Quote,
	Rule,
	FenceInfo,
	Link,
}

/// The one mapping from Markdown construct to theme category.
///
/// Both the source highlighter below and the preview renderers in each head
/// read this, so a construct cannot end up one color in a document and another
/// in its preview, or one color in Baryon and another in Polariton. `None`
/// means the surface's default foreground.
pub const fn markdown_category(construct: MarkdownConstruct) -> Option<TokenCategory> {
	match construct {
		MarkdownConstruct::Body => Some(TokenCategory::BodyText),
		MarkdownConstruct::Heading => Some(TokenCategory::Type),
		MarkdownConstruct::InlineCode | MarkdownConstruct::CodeText => Some(TokenCategory::String),
		MarkdownConstruct::Emphasis | MarkdownConstruct::BulletMarker => {
			Some(TokenCategory::Operator)
		}
		MarkdownConstruct::OrderedMarker => Some(TokenCategory::Number),
		MarkdownConstruct::Quote => Some(TokenCategory::Comment),
		MarkdownConstruct::Rule => Some(TokenCategory::Punctuation),
		MarkdownConstruct::FenceInfo | MarkdownConstruct::Link => Some(TokenCategory::Function),
	}
}

/// Resolve a construct against a theme, falling back to a default foreground.
pub fn markdown_construct_color(
	construct: MarkdownConstruct,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	default_foreground: Rgb,
) -> Rgb {
	markdown_category(construct)
		.and_then(|category| theme_colors[category as usize])
		.unwrap_or(default_foreground)
}

fn classify(token: MarkdownToken, src: &[u8]) -> TokenCategory {
	let construct = match token.kind {
		MarkdownTokenKind::HeadingMarker | MarkdownTokenKind::HeadingText => {
			MarkdownConstruct::Heading
		}
		MarkdownTokenKind::Fence
		| MarkdownTokenKind::HorizontalRule
		| MarkdownTokenKind::Punctuation => MarkdownConstruct::Rule,
		MarkdownTokenKind::ListMarker if src[token.start].is_ascii_digit() => {
			MarkdownConstruct::OrderedMarker
		}
		MarkdownTokenKind::ListMarker => MarkdownConstruct::BulletMarker,
		MarkdownTokenKind::FenceInfo => MarkdownConstruct::FenceInfo,
		MarkdownTokenKind::LinkLabel => MarkdownConstruct::Link,
		MarkdownTokenKind::CodeBlockText => MarkdownConstruct::CodeText,
		MarkdownTokenKind::InlineCode => MarkdownConstruct::InlineCode,
		MarkdownTokenKind::LinkDestination => MarkdownConstruct::CodeText,
		MarkdownTokenKind::BlockQuoteMarker | MarkdownTokenKind::HtmlComment => {
			MarkdownConstruct::Quote
		}
		MarkdownTokenKind::Emphasis => MarkdownConstruct::Emphasis,
	};
	// Every token kind names a construct, so body text — which has no token —
	// is the only case the map leaves uncolored.
	markdown_category(construct).unwrap_or(TokenCategory::Unclassified)
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

	/// The table and the highlighter must not drift apart: what the source pane
	/// paints a construct is what a preview asks the table for.
	#[test]
	fn highlighter_categories_agree_with_the_construct_table() {
		let cases: &[(&[u8], MarkdownConstruct, DocByte)] = &[
			(b"# Title\n", MarkdownConstruct::Heading, DocByte::new(2)),
			(b"- item\n", MarkdownConstruct::BulletMarker, DocByte::ZERO),
			(
				b"12. item\n",
				MarkdownConstruct::OrderedMarker,
				DocByte::ZERO,
			),
			(b"> quote\n", MarkdownConstruct::Quote, DocByte::ZERO),
			(b"---\n", MarkdownConstruct::Rule, DocByte::ZERO),
			(
				b"a `code` b\n",
				MarkdownConstruct::InlineCode,
				DocByte::new(2),
			),
			(
				b"a *emph* b\n",
				MarkdownConstruct::Emphasis,
				DocByte::new(2),
			),
		];

		for (source, construct, at) in cases {
			let expected = markdown_category(*construct).expect("construct is colored");
			assert!(
				spans(source)
					.iter()
					.any(|span| span.start == *at && span.category == expected),
				"{construct:?} in {:?} must be {expected:?}",
				std::str::from_utf8(source).unwrap()
			);
		}
	}

	/// Prose reaches a pane as its default foreground rather than as a span, so
	/// the construct and that default must resolve to the same color — else a
	/// themed body color would appear in the preview but not in the document.
	#[test]
	fn prose_and_unclaimed_text_resolve_to_one_color() {
		use crate::svp::rust_highlight::default_text_color;

		let fallback = Rgb::new(1, 2, 3);
		let prose = Rgb::new(9, 9, 9);
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::BodyText as usize] = Some(prose);

		assert_eq!(
			markdown_construct_color(MarkdownConstruct::Body, &theme_colors, fallback),
			default_text_color(&theme_colors, fallback),
		);
		assert_eq!(
			markdown_construct_color(MarkdownConstruct::Body, &theme_colors, fallback),
			prose,
		);

		let unset = [None; CATEGORY_COUNT];
		assert_eq!(
			markdown_construct_color(MarkdownConstruct::Body, &unset, fallback),
			default_text_color(&unset, fallback),
			"a theme that sets no body color leaves both at the pane default"
		);

		// The tokenizer emits nothing for prose: the color arrives as the
		// pane's default foreground, never as a highlight span.
		assert!(spans(b"just prose\n").is_empty());
	}

	/// The text inside `*emphasis*` is prose too — only the markers are spans —
	/// so it takes the body color on both sides.
	#[test]
	fn emphasized_text_is_colored_as_prose() {
		let emphasized = spans(b"a *emph* b\n");

		assert!(
			!emphasized
				.iter()
				.any(|span| span.start <= DocByte::new(4) && span.end > DocByte::new(4)),
			"the text between the markers carries no span of its own"
		);
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
