//! Authoritative Markdown highlighting, parsed from the whole document.
//!
//! The lexical baseline is a viewport lexer: it sees a window, guesses, and is
//! sometimes wrong — `snake_case_word` reads as emphasis, `\*escaped\*` reads as
//! emphasis, setext headings and tables read as nothing. This pass sees the
//! entire document through the same CommonMark parser the previews use, so its
//! spans are the truth.
//!
//! Which is why prose is spanned here rather than left bare. Semantic spans
//! *replace* the lexical spans they overlap, so a `BodyText` span over ordinary
//! text is what erases a lexical guess; text left unspanned would keep it. Body
//! text is a classification, not an absence.
//!
//! Spans for languages embedded in fences are appended after these by the
//! reactor, which is what keeps them winning inside a fence body.

use std::ops::Range;

use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};

use crate::core::DocByte;
use crate::svp::markdown_highlight::{MarkdownConstruct, markdown_category};
use crate::svp::rust_highlight::HighlightSpan;

/// Where a text run sits, which is what decides its category.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TextContext {
	Body,
	Heading,
	Emphasis,
	Code,
	Link,
	Quote,
	TableHeader,
}

impl TextContext {
	fn construct(self) -> MarkdownConstruct {
		match self {
			Self::Body => MarkdownConstruct::Body,
			// A table's header cells read as headings on both sides.
			Self::Heading | Self::TableHeader => MarkdownConstruct::Heading,
			Self::Emphasis => MarkdownConstruct::Emphasis,
			Self::Code => MarkdownConstruct::CodeText,
			Self::Link => MarkdownConstruct::Link,
			Self::Quote => MarkdownConstruct::Quote,
		}
	}
}

pub fn markdown_semantic_highlights(source: &str, global_offset: DocByte) -> Vec<HighlightSpan> {
	let mut options = Options::empty();
	options.insert(Options::ENABLE_TABLES);
	options.insert(Options::ENABLE_TASKLISTS);
	options.insert(Options::ENABLE_STRIKETHROUGH);
	options.insert(Options::ENABLE_GFM);

	let mut out = Vec::new();
	let mut context: Vec<TextContext> = Vec::new();
	let mut emit = |range: Range<usize>, construct: MarkdownConstruct| {
		if range.start >= range.end {
			return;
		}
		let Some(category) = markdown_category(construct) else {
			return;
		};
		out.push(HighlightSpan::new(
			global_offset.saturating_add(range.start as u64),
			global_offset.saturating_add(range.end as u64),
			category,
		));
	};

	for (event, range) in Parser::new_ext(source, options).into_offset_iter() {
		match event {
			Event::Start(Tag::Heading { .. }) => {
				// Markers and text alike: `#` is part of the heading.
				emit(range, MarkdownConstruct::Heading);
				context.push(TextContext::Heading);
			}
			Event::End(TagEnd::Heading(_)) => {
				context.pop();
			}
			Event::Start(Tag::CodeBlock(kind)) => {
				emit_code_block(source, &range, &kind, &mut emit);
				context.push(TextContext::Code);
			}
			Event::End(TagEnd::CodeBlock) => {
				context.pop();
			}
			Event::Start(Tag::Emphasis | Tag::Strong | Tag::Strikethrough) => {
				emit(range, MarkdownConstruct::Emphasis);
				context.push(TextContext::Emphasis);
			}
			Event::End(TagEnd::Emphasis | TagEnd::Strong | TagEnd::Strikethrough) => {
				context.pop();
			}
			Event::Start(Tag::Link { .. } | Tag::Image { .. }) => {
				emit(range, MarkdownConstruct::Link);
				context.push(TextContext::Link);
			}
			Event::End(TagEnd::Link | TagEnd::Image) => {
				context.pop();
			}
			Event::Start(Tag::BlockQuote(_)) => {
				for marker in quote_markers(source, &range) {
					emit(marker, MarkdownConstruct::Quote);
				}
				// The quote's prose is quoted too, not just its `>`: both
				// previews color the whole block, and the document must agree.
				context.push(TextContext::Quote);
			}
			Event::End(TagEnd::BlockQuote(_)) => {
				context.pop();
			}
			Event::Start(Tag::Item) => {
				if let Some((marker, construct)) = list_marker(source, &range) {
					emit(marker, construct);
				}
			}
			Event::Start(Tag::TableHead) => context.push(TextContext::TableHeader),
			Event::End(TagEnd::TableHead) => {
				context.pop();
			}
			Event::Rule => emit(range, MarkdownConstruct::Rule),
			Event::TaskListMarker(_) => emit(range, MarkdownConstruct::BulletMarker),
			Event::Code(_) => emit(range, MarkdownConstruct::InlineCode),
			Event::Html(_) | Event::InlineHtml(_) => emit(range, MarkdownConstruct::Quote),
			Event::Text(_) | Event::InlineMath(_) | Event::DisplayMath(_) => {
				let construct = context
					.last()
					.copied()
					.unwrap_or(TextContext::Body)
					.construct();
				emit(range, construct);
			}
			_ => {}
		}
	}
	out.sort_unstable_by_key(|span| span.start);
	out
}

/// A fence contributes three things: its markers, its info string, and its
/// body. Only the body may be overwritten by an embedded language.
fn emit_code_block(
	source: &str,
	range: &Range<usize>,
	kind: &CodeBlockKind<'_>,
	emit: &mut impl FnMut(Range<usize>, MarkdownConstruct),
) {
	if matches!(kind, CodeBlockKind::Indented) {
		emit(range.clone(), MarkdownConstruct::CodeText);
		return;
	}

	let block = &source[range.clone()];
	let first_line_len = block.find('\n').unwrap_or(block.len());
	let opening = &block[..first_line_len];
	let indent = opening.len() - opening.trim_start_matches([' ', '\t']).len();
	let fence_char = opening[indent..].chars().next().unwrap_or('`');
	let fence_len = opening[indent..]
		.chars()
		.take_while(|ch| *ch == fence_char)
		.count();
	let marker_start = range.start + indent;
	emit(
		marker_start..marker_start + fence_len,
		MarkdownConstruct::Rule,
	);

	let info = &opening[indent + fence_len..];
	let info_trimmed = info.trim();
	if !info_trimmed.is_empty() {
		let info_start = marker_start + fence_len + (info.len() - info.trim_start().len());
		emit(
			info_start..info_start + info_trimmed.len(),
			MarkdownConstruct::FenceInfo,
		);
	}

	// The closing fence is the last line made only of fence characters; without
	// one the block runs to the end of its range.
	let body_start = (range.start + first_line_len + 1).min(range.end);
	let mut body_end = range.end;
	for (offset, line) in line_starts(block) {
		let start = range.start + offset;
		if start <= body_start {
			continue;
		}
		let trimmed = line.trim();
		if !trimmed.is_empty() && trimmed.chars().all(|ch| ch == fence_char) {
			body_end = start;
			let closing_start = start + (line.len() - line.trim_start().len());
			emit(
				closing_start..closing_start + trimmed.len(),
				MarkdownConstruct::Rule,
			);
			break;
		}
	}
	emit(body_start..body_end, MarkdownConstruct::CodeText);
}

fn line_starts(text: &str) -> impl Iterator<Item = (usize, &str)> {
	let mut offset = 0;
	text.split_inclusive('\n').map(move |line| {
		let start = offset;
		offset += line.len();
		(start, line.trim_end_matches('\n'))
	})
}

/// The `>` at the head of each quoted line.
fn quote_markers(source: &str, range: &Range<usize>) -> Vec<Range<usize>> {
	let block = &source[range.clone()];
	line_starts(block)
		.filter_map(|(offset, line)| {
			let indent = line.len() - line.trim_start().len();
			line.trim_start().starts_with('>').then(|| {
				let start = range.start + offset + indent;
				start..start + 1
			})
		})
		.collect()
}

/// The `-` or `1.` that opens a list item.
fn list_marker(source: &str, range: &Range<usize>) -> Option<(Range<usize>, MarkdownConstruct)> {
	let item = source.get(range.clone())?;
	let indent = item.len() - item.trim_start_matches([' ', '\t']).len();
	let rest = &item[indent..];
	let start = range.start + indent;
	let first = rest.chars().next()?;
	if matches!(first, '-' | '*' | '+') {
		return Some((start..start + 1, MarkdownConstruct::BulletMarker));
	}
	if first.is_ascii_digit() {
		let digits = rest.chars().take_while(char::is_ascii_digit).count();
		let delimiter = usize::from(matches!(rest[digits..].chars().next(), Some('.' | ')')));
		return Some((
			start..start + digits + delimiter,
			MarkdownConstruct::OrderedMarker,
		));
	}
	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::markdown_highlight::highlight_markdown;
	use crate::svp::rust_highlight::{HighlightSpanIndex, TokenCategory};

	fn semantic(source: &str) -> Vec<HighlightSpan> {
		markdown_semantic_highlights(source, DocByte::ZERO)
	}

	/// What the panes actually paint: the lexical guess merged under the
	/// authoritative pass.
	fn merged_categories(source: &str) -> Vec<TokenCategory> {
		// The same merge the render path performs: semantic spans replace every
		// lexical span they overlap.
		let lexical = highlight_markdown(source.as_bytes(), &[(0, DocByte::ZERO)]);
		let index = HighlightSpanIndex::merged(&lexical, &semantic(source));
		(0..source.len())
			.map(|byte| index.category_at(DocByte::new(byte as u64)))
			.collect()
	}

	fn category_at(source: &str, byte: usize) -> TokenCategory {
		merged_categories(source)[byte]
	}

	/// The lexical pass reads `snake_case_word`'s underscores as emphasis. The
	/// semantic pass must erase that, which it can only do by spanning prose.
	#[test]
	fn snake_case_words_are_body_text_end_to_end() {
		let source = "a snake_case_word here\n";
		let word = source.find("snake").expect("the word")..source.find(" here").expect("the tail");

		let lexical = highlight_markdown(source.as_bytes(), &[(0, DocByte::ZERO)]);
		assert!(
			lexical
				.iter()
				.any(|span| span.category == TokenCategory::Operator),
			"the lexical guess is wrong, which is the premise of this test"
		);

		let categories = merged_categories(source);
		assert!(
			categories[word.clone()]
				.iter()
				.all(|category| *category == TokenCategory::BodyText),
			"the whole word is body text after merging: {:?}",
			&categories[word]
		);
		assert!(
			!categories.contains(&TokenCategory::Operator),
			"no byte of the line keeps the lexical emphasis guess: {categories:?}"
		);
	}

	/// Newlines and consumed escape characters carry no span of their own; they
	/// land on the pane's default foreground, which `BodyText` also resolves to.
	#[test]
	fn escaped_markers_stay_body_text() {
		let source = "escaped \\*not emphasis\\* here\n";
		let categories = merged_categories(source);

		for marker in [
			source.find('*').expect("first"),
			source.rfind('*').expect("last"),
		] {
			assert_eq!(
				categories[marker],
				TokenCategory::BodyText,
				"an escaped marker is prose, not emphasis"
			);
		}
		assert!(
			!categories.contains(&TokenCategory::Operator),
			"no emphasis survives anywhere on the line: {categories:?}"
		);
	}

	#[test]
	fn real_emphasis_is_categorized_as_emphasis() {
		let source = "a *emph* and **strong** b\n";
		let emphasis = markdown_category(MarkdownConstruct::Emphasis).expect("emphasis is colored");

		assert_eq!(category_at(source, 2), emphasis, "the opening marker");
		assert_eq!(category_at(source, 4), emphasis, "the emphasized text");
		assert_eq!(category_at(source, 15), emphasis, "the strong text");
		assert_eq!(
			category_at(source, 0),
			TokenCategory::BodyText,
			"the prose around it is untouched"
		);
	}

	/// A quote colors its prose, not only its `>`. Inline styling nested inside
	/// keeps its own category, since it pushes over the quote's context.
	#[test]
	fn blockquote_prose_is_quoted_and_nested_styles_survive() {
		let source = "> quoted `code` and *emph*\n\nafter\n";
		let quote = markdown_category(MarkdownConstruct::Quote).expect("quotes are colored");
		let code = markdown_category(MarkdownConstruct::InlineCode).expect("code is colored");
		let emphasis = markdown_category(MarkdownConstruct::Emphasis).expect("emphasis is colored");

		assert_eq!(category_at(source, 0), quote, "the marker");
		assert_eq!(
			category_at(source, source.find("quoted").expect("prose")),
			quote,
			"the quoted prose"
		);
		assert_eq!(
			category_at(source, source.find("code").expect("inline code")),
			code,
			"inline code inside a quote is still code"
		);
		assert_eq!(
			category_at(source, source.find("emph").expect("emphasis")),
			emphasis,
			"emphasis inside a quote is still emphasis"
		);
		assert_eq!(
			category_at(source, source.find("after").expect("paragraph")),
			TokenCategory::BodyText,
			"and the quote's context does not leak past its end"
		);
	}

	#[test]
	fn setext_headings_are_recognized() {
		let source = "Setext Title\n============\n\nbody\n";
		let heading = markdown_category(MarkdownConstruct::Heading).expect("headings are colored");

		assert_eq!(category_at(source, 0), heading, "the title line");
		assert_eq!(category_at(source, 13), heading, "the underline");
		assert_eq!(
			category_at(source, source.find("body").expect("the paragraph")),
			TokenCategory::BodyText,
			"the paragraph after it is prose"
		);
	}

	#[test]
	fn tables_are_recognized() {
		let source = "| head | b |\n| --- | --- |\n| snake_case | y |\n";
		let heading = markdown_category(MarkdownConstruct::Heading).expect("headings are colored");

		assert_eq!(
			category_at(source, 2),
			heading,
			"header cells read as headings"
		);
		assert_eq!(
			category_at(source, 30),
			TokenCategory::BodyText,
			"a body cell is prose, underscores included"
		);
	}

	#[test]
	fn indented_code_blocks_are_recognized() {
		let source = "text\n\n    indented code\n";
		let code = markdown_category(MarkdownConstruct::CodeText).expect("code is colored");

		assert_eq!(category_at(source, 10), code);
	}

	#[test]
	fn fenced_blocks_split_markers_info_and_body() {
		let source = "```rust\nlet x = 1;\n```\n";
		let rule = markdown_category(MarkdownConstruct::Rule).expect("rules are colored");
		let info = markdown_category(MarkdownConstruct::FenceInfo).expect("info is colored");
		let code = markdown_category(MarkdownConstruct::CodeText).expect("code is colored");

		assert_eq!(category_at(source, 0), rule, "opening fence");
		assert_eq!(category_at(source, 3), info, "info string");
		assert_eq!(category_at(source, 8), code, "body");
		assert_eq!(category_at(source, 19), rule, "closing fence");
	}

	/// A fence's body is classified as code, but the language inside it has the
	/// final word: the reactor appends embedded spans after these.
	#[test]
	fn embedded_language_spans_win_inside_a_fence() {
		let source = "```rust\nlet x = 1;\n```\n";
		let mut spans = semantic(source);
		let embedded =
			HighlightSpan::new(DocByte::new(8), DocByte::new(11), TokenCategory::Keyword);
		spans.push(embedded);

		let index = HighlightSpanIndex::new(spans);
		assert_eq!(
			index.category_at(DocByte::new(9)),
			TokenCategory::Keyword,
			"the embedded language decides inside the fence"
		);
		assert_eq!(
			index.category_at(DocByte::new(12)),
			markdown_category(MarkdownConstruct::CodeText).expect("code is colored"),
			"bytes the embedded pass did not claim stay code"
		);
	}

	#[test]
	fn a_nonzero_global_offset_shifts_every_span_exactly() {
		let source = "# Title\n\nbody *emph*\n";
		let base = markdown_semantic_highlights(source, DocByte::ZERO);
		let shifted = markdown_semantic_highlights(source, DocByte::new(1000));

		assert_eq!(base.len(), shifted.len());
		for (base, shifted) in base.iter().zip(shifted.iter()) {
			assert_eq!(shifted.start.get(), base.start.get() + 1000);
			assert_eq!(shifted.end.get(), base.end.get() + 1000);
			assert_eq!(shifted.category, base.category);
		}
	}
}
