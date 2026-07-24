//! Head-agnostic Markdown block model.
//!
//! Markdown structure is a property of the document, not of the surface that
//! draws it: a terminal rasterizer and a GUI widget tree disagree about pixels
//! but must agree about what a document *is*. Parsing lives here so both heads
//! read the same blocks, and neither reimplements CommonMark.
//!
//! Blocks carry the byte range that produced them, which is what lets a head
//! align its preview with the source viewport it mirrors.

use std::ops::Range;

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};

/// Inline styling for a run of text, as a set rather than one label: Markdown
/// nests emphasis freely, so `**bold `code`**` is a single run that is both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct MarkdownInlineStyle {
	pub code: bool,
	pub strong: bool,
	pub emphasis: bool,
}

impl MarkdownInlineStyle {
	/// Unstyled body text.
	pub const TEXT: Self = Self {
		code: false,
		strong: false,
		emphasis: false,
	};

	/// Inline code, carrying no emphasis.
	pub const CODE: Self = Self {
		code: true,
		strong: false,
		emphasis: false,
	};
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkdownInlineSpan {
	pub style: MarkdownInlineStyle,
	pub text: String,
}

/// A run of inline text, stored as style-homogeneous spans.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarkdownInlineText {
	spans: Vec<MarkdownInlineSpan>,
}

impl MarkdownInlineText {
	pub fn new() -> Self {
		Self { spans: Vec::new() }
	}

	pub fn spans(&self) -> &[MarkdownInlineSpan] {
		&self.spans
	}

	/// Append text, merging into the previous span when the style matches so
	/// consumers never see an arbitrary number of spans for one styled run.
	pub fn push(&mut self, style: MarkdownInlineStyle, text: &str) {
		if text.is_empty() {
			return;
		}
		if let Some(last) = self.spans.last_mut().filter(|span| span.style == style) {
			last.text.push_str(text);
			return;
		}
		self.spans.push(MarkdownInlineSpan {
			style,
			text: text.to_string(),
		});
	}

	/// Fold a nested block's text into this one, separated by a newline. Used
	/// where a container (a quote, a list item) owns several child blocks but
	/// the model presents it as one text run.
	pub fn append_block_text(&mut self, text: &MarkdownInlineText) {
		let text = text.trimmed();
		if text.is_empty() {
			return;
		}
		if !self.is_empty() {
			self.push(MarkdownInlineStyle::TEXT, "\n");
		}
		for span in text.spans {
			self.push(span.style, &span.text);
		}
	}

	pub fn trimmed(&self) -> Self {
		let mut trimmed = self.clone();
		trimmed.trim();
		trimmed
	}

	pub fn trim(&mut self) {
		while let Some(first) = self.spans.first_mut() {
			let trimmed = first.text.trim_start();
			if trimmed.len() == first.text.len() {
				break;
			}
			first.text = trimmed.to_string();
			if !first.text.is_empty() {
				break;
			}
			self.spans.remove(0);
		}
		while let Some(last) = self.spans.last_mut() {
			let trimmed = last.text.trim_end();
			if trimmed.len() == last.text.len() {
				break;
			}
			last.text = trimmed.to_string();
			if !last.text.is_empty() {
				break;
			}
			self.spans.pop();
		}
	}

	pub fn is_empty(&self) -> bool {
		self.spans.iter().all(|span| span.text.is_empty())
	}

	pub fn plain_text(&self) -> String {
		self.spans
			.iter()
			.map(|span| span.text.as_str())
			.collect::<String>()
	}

	pub fn char_count(&self) -> usize {
		self.spans
			.iter()
			.map(|span| span.text.chars().count())
			.sum()
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkdownBlock {
	Heading {
		level: u8,
		text: String,
		source_range: Range<usize>,
	},
	Paragraph {
		text: MarkdownInlineText,
		source_range: Range<usize>,
	},
	CodeBlock {
		info: Option<String>,
		language: Option<String>,
		text: String,
		source_range: Range<usize>,
	},
	Quote {
		text: MarkdownInlineText,
		source_range: Range<usize>,
	},
	ListItem {
		depth: u16,
		marker: ListMarker,
		checked: Option<bool>,
		text: MarkdownInlineText,
		source_range: Range<usize>,
	},
	Rule {
		source_range: Range<usize>,
	},
	Table {
		rows: Vec<Vec<MarkdownInlineText>>,
		source_range: Range<usize>,
	},
	Html {
		text: String,
		source_range: Range<usize>,
	},
}

impl MarkdownBlock {
	pub fn source_range(&self) -> &Range<usize> {
		match self {
			Self::Heading { source_range, .. }
			| Self::Paragraph { source_range, .. }
			| Self::CodeBlock { source_range, .. }
			| Self::Quote { source_range, .. }
			| Self::ListItem { source_range, .. }
			| Self::Rule { source_range }
			| Self::Table { source_range, .. }
			| Self::Html { source_range, .. } => source_range,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListMarker {
	Unordered,
	Ordered(u64),
}

enum ActiveBlock {
	Paragraph {
		text: MarkdownInlineText,
		source_range: Range<usize>,
	},
	Heading {
		level: u8,
		text: String,
		source_range: Range<usize>,
	},
	CodeBlock {
		info: Option<String>,
		language: Option<String>,
		text: String,
		source_range: Range<usize>,
	},
	Quote {
		text: MarkdownInlineText,
		source_range: Range<usize>,
	},
	List {
		next_number: Option<u64>,
	},
	Item {
		depth: u16,
		marker: ListMarker,
		checked: Option<bool>,
		text: MarkdownInlineText,
		source_range: Range<usize>,
	},
	Table {
		rows: Vec<Vec<MarkdownInlineText>>,
		source_range: Range<usize>,
	},
	TableRow {
		cells: Vec<MarkdownInlineText>,
		source_range: Range<usize>,
	},
	TableCell {
		text: MarkdownInlineText,
		source_range: Range<usize>,
	},
	Html {
		text: String,
		source_range: Range<usize>,
	},
}

/// Emphasis nesting depth. Markdown permits `**a *b* a**`, so styling is a
/// depth count rather than a flag: the inner close must not clear the outer.
#[derive(Default)]
struct InlineStyleStack {
	strong: u32,
	emphasis: u32,
}

impl InlineStyleStack {
	fn style(&self, code: bool) -> MarkdownInlineStyle {
		MarkdownInlineStyle {
			code,
			strong: self.strong > 0,
			emphasis: self.emphasis > 0,
		}
	}
}

pub fn parse_markdown_blocks(source: &str) -> Vec<MarkdownBlock> {
	let mut options = Options::empty();
	options.insert(Options::ENABLE_TABLES);
	options.insert(Options::ENABLE_TASKLISTS);
	options.insert(Options::ENABLE_STRIKETHROUGH);
	options.insert(Options::ENABLE_GFM);

	let mut stack: Vec<ActiveBlock> = Vec::new();
	let mut style = InlineStyleStack::default();
	let mut blocks = Vec::new();
	for (event, range) in Parser::new_ext(source, options).into_offset_iter() {
		match event {
			Event::Start(Tag::Paragraph) => stack.push(ActiveBlock::Paragraph {
				text: MarkdownInlineText::new(),
				source_range: range,
			}),
			Event::End(TagEnd::Paragraph) => {
				let Some(ActiveBlock::Paragraph { text, source_range }) = stack.pop() else {
					continue;
				};
				let text = text.trimmed();
				if text.is_empty() {
					continue;
				}
				let source_range = source_range.start..range.end.max(source_range.end);
				if !append_inline_text_to_parent(&mut stack, &text) {
					blocks.push(MarkdownBlock::Paragraph { text, source_range });
				}
			}
			Event::Start(Tag::Heading { level, .. }) => stack.push(ActiveBlock::Heading {
				level: heading_level_u8(level),
				text: String::new(),
				source_range: range,
			}),
			Event::End(TagEnd::Heading(_)) => {
				let Some(ActiveBlock::Heading {
					level,
					text,
					source_range,
				}) = stack.pop()
				else {
					continue;
				};
				let text = text.trim().to_string();
				if !text.is_empty() {
					blocks.push(MarkdownBlock::Heading {
						level,
						text,
						source_range: source_range.start..range.end.max(source_range.end),
					});
				}
			}
			Event::Start(Tag::CodeBlock(kind)) => {
				let info = code_block_info(kind);
				stack.push(ActiveBlock::CodeBlock {
					language: code_block_language(info.as_deref()),
					info,
					text: String::new(),
					source_range: range,
				});
			}
			Event::End(TagEnd::CodeBlock) => {
				let Some(ActiveBlock::CodeBlock {
					info,
					language,
					text,
					source_range,
				}) = stack.pop()
				else {
					continue;
				};
				blocks.push(MarkdownBlock::CodeBlock {
					info,
					language,
					text: text.trim_end_matches('\n').to_string(),
					source_range: source_range.start..range.end.max(source_range.end),
				});
			}
			Event::Start(Tag::BlockQuote(_)) => stack.push(ActiveBlock::Quote {
				text: MarkdownInlineText::new(),
				source_range: range,
			}),
			Event::End(TagEnd::BlockQuote(_)) => {
				let Some(ActiveBlock::Quote { text, source_range }) = stack.pop() else {
					continue;
				};
				let text = text.trimmed();
				if !text.is_empty() {
					blocks.push(MarkdownBlock::Quote {
						text,
						source_range: source_range.start..range.end.max(source_range.end),
					});
				}
			}
			Event::Start(Tag::List(start)) => stack.push(ActiveBlock::List { next_number: start }),
			Event::End(TagEnd::List(_)) => {
				if matches!(stack.last(), Some(ActiveBlock::List { .. })) {
					let _ = stack.pop();
				}
			}
			Event::Start(Tag::Item) => {
				let marker = next_list_marker(&mut stack);
				let depth = current_list_depth(&stack);
				stack.push(ActiveBlock::Item {
					depth,
					marker,
					checked: None,
					text: MarkdownInlineText::new(),
					source_range: range,
				});
			}
			Event::End(TagEnd::Item) => {
				let Some(ActiveBlock::Item {
					depth,
					marker,
					checked,
					text,
					source_range,
				}) = stack.pop()
				else {
					continue;
				};
				let text = text.trimmed();
				blocks.push(MarkdownBlock::ListItem {
					depth,
					marker,
					checked,
					text,
					source_range: source_range.start..range.end.max(source_range.end),
				});
			}
			Event::Start(Tag::Table(_)) => stack.push(ActiveBlock::Table {
				rows: Vec::new(),
				source_range: range,
			}),
			Event::End(TagEnd::Table) => {
				let Some(ActiveBlock::Table { rows, source_range }) = stack.pop() else {
					continue;
				};
				if !rows.is_empty() {
					blocks.push(MarkdownBlock::Table {
						rows,
						source_range: source_range.start..range.end.max(source_range.end),
					});
				}
			}
			Event::Start(Tag::TableHead) => stack.push(ActiveBlock::TableRow {
				cells: Vec::new(),
				source_range: range,
			}),
			Event::End(TagEnd::TableHead) => {
				let Some(ActiveBlock::TableRow {
					cells,
					source_range,
				}) = stack.pop()
				else {
					continue;
				};
				if let Some(ActiveBlock::Table {
					rows,
					source_range: table_range,
				}) = stack
					.iter_mut()
					.rev()
					.find(|ctx| matches!(ctx, ActiveBlock::Table { .. }))
				{
					table_range.end = table_range.end.max(source_range.end).max(range.end);
					rows.push(cells);
				}
			}
			Event::Start(Tag::TableRow) => stack.push(ActiveBlock::TableRow {
				cells: Vec::new(),
				source_range: range,
			}),
			Event::End(TagEnd::TableRow) => {
				let Some(ActiveBlock::TableRow {
					cells,
					source_range,
				}) = stack.pop()
				else {
					continue;
				};
				if let Some(ActiveBlock::Table {
					rows,
					source_range: table_range,
				}) = stack
					.iter_mut()
					.rev()
					.find(|ctx| matches!(ctx, ActiveBlock::Table { .. }))
				{
					table_range.end = table_range.end.max(source_range.end).max(range.end);
					rows.push(cells);
				}
			}
			Event::Start(Tag::TableCell) => stack.push(ActiveBlock::TableCell {
				text: MarkdownInlineText::new(),
				source_range: range,
			}),
			Event::End(TagEnd::TableCell) => {
				let Some(ActiveBlock::TableCell { text, source_range }) = stack.pop() else {
					continue;
				};
				if let Some(ActiveBlock::TableRow {
					cells,
					source_range: row_range,
				}) = stack
					.iter_mut()
					.rev()
					.find(|ctx| matches!(ctx, ActiveBlock::TableRow { .. }))
				{
					row_range.end = row_range.end.max(source_range.end).max(range.end);
					cells.push(text.trimmed());
				}
			}
			Event::Start(Tag::HtmlBlock) => stack.push(ActiveBlock::Html {
				text: String::new(),
				source_range: range,
			}),
			Event::End(TagEnd::HtmlBlock) => {
				let Some(ActiveBlock::Html { text, source_range }) = stack.pop() else {
					continue;
				};
				let text = text.trim().to_string();
				if !text.is_empty() {
					blocks.push(MarkdownBlock::Html {
						text,
						source_range: source_range.start..range.end.max(source_range.end),
					});
				}
			}
			Event::Text(text) | Event::InlineMath(text) => {
				append_text(&mut stack, style.style(false), text.as_ref());
			}
			Event::Code(text) => {
				append_text(&mut stack, style.style(true), text.as_ref());
			}
			Event::Html(text) | Event::InlineHtml(text) => {
				append_text(&mut stack, style.style(false), text.as_ref());
			}
			Event::DisplayMath(text) => {
				append_text(&mut stack, style.style(false), text.as_ref());
			}
			Event::SoftBreak => append_text(&mut stack, style.style(false), " "),
			Event::HardBreak => append_text(&mut stack, style.style(false), "\n"),
			Event::Rule => blocks.push(MarkdownBlock::Rule {
				source_range: range,
			}),
			Event::TaskListMarker(checked) => {
				if !set_task_marker(&mut stack, checked) {
					append_text(
						&mut stack,
						style.style(false),
						if checked { "[x] " } else { "[ ] " },
					);
				}
			}
			Event::FootnoteReference(label) => {
				append_text(&mut stack, style.style(false), &format!("[{}]", label));
			}
			Event::Start(Tag::Strong) => style.strong = style.strong.saturating_add(1),
			Event::End(TagEnd::Strong) => style.strong = style.strong.saturating_sub(1),
			Event::Start(Tag::Emphasis) => style.emphasis = style.emphasis.saturating_add(1),
			Event::End(TagEnd::Emphasis) => style.emphasis = style.emphasis.saturating_sub(1),
			Event::Start(
				Tag::Strikethrough
				| Tag::Superscript
				| Tag::Subscript
				| Tag::Link { .. }
				| Tag::Image { .. }
				| Tag::MetadataBlock(_)
				| Tag::FootnoteDefinition(_)
				| Tag::DefinitionList
				| Tag::DefinitionListTitle
				| Tag::DefinitionListDefinition,
			)
			| Event::End(
				TagEnd::Strikethrough
				| TagEnd::Superscript
				| TagEnd::Subscript
				| TagEnd::Link
				| TagEnd::Image
				| TagEnd::MetadataBlock(_)
				| TagEnd::FootnoteDefinition
				| TagEnd::DefinitionList
				| TagEnd::DefinitionListTitle
				| TagEnd::DefinitionListDefinition,
			) => {}
		}
	}
	blocks.sort_by_key(|block| block.source_range().start);
	blocks
}

fn append_text(stack: &mut [ActiveBlock], style: MarkdownInlineStyle, text: &str) {
	if text.is_empty() {
		return;
	}
	for ctx in stack.iter_mut().rev() {
		match ctx {
			ActiveBlock::Paragraph { text: target, .. }
			| ActiveBlock::Quote { text: target, .. }
			| ActiveBlock::Item { text: target, .. }
			| ActiveBlock::TableCell { text: target, .. } => {
				target.push(style, text);
				return;
			}
			ActiveBlock::Heading { text: target, .. }
			| ActiveBlock::CodeBlock { text: target, .. }
			| ActiveBlock::Html { text: target, .. } => {
				target.push_str(text);
				return;
			}
			ActiveBlock::List { .. } | ActiveBlock::Table { .. } | ActiveBlock::TableRow { .. } => {
			}
		}
	}
}

fn append_inline_text_to_parent(stack: &mut [ActiveBlock], text: &MarkdownInlineText) -> bool {
	for ctx in stack.iter_mut().rev() {
		match ctx {
			ActiveBlock::Quote { text: target, .. }
			| ActiveBlock::Item { text: target, .. }
			| ActiveBlock::TableCell { text: target, .. } => {
				target.append_block_text(text);
				return true;
			}
			_ => {}
		}
	}
	false
}

fn next_list_marker(stack: &mut [ActiveBlock]) -> ListMarker {
	for ctx in stack.iter_mut().rev() {
		if let ActiveBlock::List { next_number } = ctx {
			if let Some(number) = next_number {
				let marker = ListMarker::Ordered(*number);
				*number = number.saturating_add(1);
				return marker;
			}
			return ListMarker::Unordered;
		}
	}
	ListMarker::Unordered
}

fn current_list_depth(stack: &[ActiveBlock]) -> u16 {
	stack
		.iter()
		.filter(|ctx| matches!(ctx, ActiveBlock::List { .. }))
		.count()
		.saturating_sub(1)
		.min(u16::MAX as usize) as u16
}

fn set_task_marker(stack: &mut [ActiveBlock], checked: bool) -> bool {
	for ctx in stack.iter_mut().rev() {
		if let ActiveBlock::Item { checked: state, .. } = ctx {
			*state = Some(checked);
			return true;
		}
	}
	false
}

fn heading_level_u8(level: HeadingLevel) -> u8 {
	match level {
		HeadingLevel::H1 => 1,
		HeadingLevel::H2 => 2,
		HeadingLevel::H3 => 3,
		HeadingLevel::H4 => 4,
		HeadingLevel::H5 => 5,
		HeadingLevel::H6 => 6,
	}
}

fn code_block_info(kind: CodeBlockKind<'_>) -> Option<String> {
	match kind {
		CodeBlockKind::Fenced(info) => {
			let info = info.trim();
			(!info.is_empty()).then(|| info.to_string())
		}
		CodeBlockKind::Indented => None,
	}
}

fn code_block_language(info: Option<&str>) -> Option<String> {
	let info = info?.trim();
	if info.is_empty() {
		return None;
	}
	code_block_attribute_language(info)
		.or_else(|| info.split_whitespace().next().map(str::to_string))
}

fn code_block_attribute_language(info: &str) -> Option<String> {
	let attrs = info.strip_prefix('{')?;
	let attrs = attrs.strip_suffix('}').unwrap_or(attrs);
	attrs.split_whitespace().find_map(|attr| {
		let attr = attr.trim_matches(|ch| ch == '{' || ch == '}');
		let language = attr
			.strip_prefix(".language-")
			.or_else(|| attr.strip_prefix('.'))?;
		(!language.is_empty()).then(|| language.to_string())
	})
}

/// Byte offset where `target_line` begins, clamped to the end of `source`.
pub fn line_start_byte(source: &str, target_line: u64) -> usize {
	if target_line == 0 {
		return 0;
	}
	let mut current_line = 0u64;
	for (idx, ch) in source.char_indices() {
		if ch == '\n' {
			current_line = current_line.saturating_add(1);
			if current_line == target_line {
				return idx.saturating_add(1);
			}
		}
	}
	source.len()
}

/// Zero-based line containing `byte`.
pub fn source_line_for_byte(source: &str, byte: usize) -> u64 {
	source
		.as_bytes()
		.iter()
		.take(byte.min(source.len()))
		.filter(|byte| **byte == b'\n')
		.count() as u64
}

/// Lines in `source`, counted the way an editor numbers them: a trailing
/// newline opens a final empty line, and an empty document still has line 1.
pub fn source_line_count(source: &str) -> u64 {
	if source.is_empty() {
		return 1;
	}
	source
		.as_bytes()
		.iter()
		.filter(|byte| **byte == b'\n')
		.count()
		.saturating_add(1) as u64
}

#[cfg(test)]
mod tests {
	use super::*;

	fn inline_parts(text: &MarkdownInlineText) -> Vec<(MarkdownInlineStyle, String)> {
		text.spans()
			.iter()
			.map(|span| (span.style, span.text.clone()))
			.collect()
	}

	fn paragraph(blocks: &[MarkdownBlock], index: usize) -> &MarkdownInlineText {
		match &blocks[index] {
			MarkdownBlock::Paragraph { text, .. } => text,
			other => panic!("expected a paragraph, got {other:?}"),
		}
	}

	#[test]
	fn inline_code_is_distinguished_from_body_text() {
		let blocks = parse_markdown_blocks("plain `code` text\n");

		assert_eq!(
			inline_parts(paragraph(&blocks, 0)),
			vec![
				(MarkdownInlineStyle::TEXT, "plain ".to_string()),
				(MarkdownInlineStyle::CODE, "code".to_string()),
				(MarkdownInlineStyle::TEXT, " text".to_string()),
			]
		);
	}

	#[test]
	fn strong_and_emphasis_are_carried_as_style() {
		let blocks = parse_markdown_blocks("a **bold** and *italic* end\n");

		assert_eq!(
			inline_parts(paragraph(&blocks, 0)),
			vec![
				(MarkdownInlineStyle::TEXT, "a ".to_string()),
				(
					MarkdownInlineStyle {
						strong: true,
						..MarkdownInlineStyle::TEXT
					},
					"bold".to_string()
				),
				(MarkdownInlineStyle::TEXT, " and ".to_string()),
				(
					MarkdownInlineStyle {
						emphasis: true,
						..MarkdownInlineStyle::TEXT
					},
					"italic".to_string()
				),
				(MarkdownInlineStyle::TEXT, " end".to_string()),
			],
			"emphasis markers are consumed, not left in the text"
		);
	}

	/// Markdown nests inline styles, so a run can be several at once and an
	/// inner close must not cancel the style the outer span still owns.
	#[test]
	fn nested_styles_combine_and_unwind_independently() {
		let blocks = parse_markdown_blocks("**bold *both* `mix`** after\n");

		assert_eq!(
			inline_parts(paragraph(&blocks, 0)),
			vec![
				(
					MarkdownInlineStyle {
						strong: true,
						..MarkdownInlineStyle::TEXT
					},
					"bold ".to_string()
				),
				(
					MarkdownInlineStyle {
						strong: true,
						emphasis: true,
						..MarkdownInlineStyle::TEXT
					},
					"both".to_string()
				),
				(
					MarkdownInlineStyle {
						strong: true,
						..MarkdownInlineStyle::TEXT
					},
					" ".to_string()
				),
				(
					MarkdownInlineStyle {
						code: true,
						strong: true,
						emphasis: false,
					},
					"mix".to_string()
				),
				(MarkdownInlineStyle::TEXT, " after".to_string()),
			]
		);
	}

	/// Inline styling survives every container that can hold text, not just
	/// paragraphs.
	#[test]
	fn inline_code_spans_survive_lists_tables_and_quotes() {
		let blocks = parse_markdown_blocks(
			"- item `code`\n\n| A | B |\n| --- | --- |\n| `x` | y |\n\n> quote `code`\n",
		);

		let list_item = blocks
			.iter()
			.find_map(|block| match block {
				MarkdownBlock::ListItem { text, .. } => Some(text),
				_ => None,
			})
			.expect("list item");
		assert_eq!(
			inline_parts(list_item),
			vec![
				(MarkdownInlineStyle::TEXT, "item ".to_string()),
				(MarkdownInlineStyle::CODE, "code".to_string()),
			]
		);

		let table_cell = blocks
			.iter()
			.find_map(|block| match block {
				MarkdownBlock::Table { rows, .. } => rows.get(1).and_then(|row| row.first()),
				_ => None,
			})
			.expect("table code cell");
		assert_eq!(
			inline_parts(table_cell),
			vec![(MarkdownInlineStyle::CODE, "x".to_string())]
		);

		let quote = blocks
			.iter()
			.find_map(|block| match block {
				MarkdownBlock::Quote { text, .. } => Some(text),
				_ => None,
			})
			.expect("quote");
		assert_eq!(
			inline_parts(quote),
			vec![
				(MarkdownInlineStyle::TEXT, "quote ".to_string()),
				(MarkdownInlineStyle::CODE, "code".to_string()),
			]
		);
	}

	#[test]
	fn deeply_nested_lists_keep_depth_and_source_order() {
		let blocks = parse_markdown_blocks("- parent\n  - child\n    - grandchild\n- sibling\n");

		let items: Vec<(String, u16)> = blocks
			.iter()
			.filter_map(|block| match block {
				MarkdownBlock::ListItem { depth, text, .. } => Some((text.plain_text(), *depth)),
				_ => None,
			})
			.collect();
		assert_eq!(
			items,
			vec![
				("parent".to_string(), 0),
				("child".to_string(), 1),
				("grandchild".to_string(), 2),
				("sibling".to_string(), 0),
			]
		);
	}

	/// Pandoc-style fence attributes name the language without being the
	/// language, so the raw info string stays available alongside it.
	#[test]
	fn attribute_form_fences_keep_both_info_and_language() {
		let blocks = parse_markdown_blocks("```{#example .rust .numberLines}\nfn main() {}\n```\n");

		assert!(blocks.iter().any(|block| matches!(
			block,
			MarkdownBlock::CodeBlock {
				info: Some(info),
				language: Some(language),
				text,
				..
			} if info == "{#example .rust .numberLines}"
				&& language == "rust"
				&& text.contains("fn main")
		)));
	}

	#[test]
	fn headings_carry_their_level_and_source_range() {
		let source = "# One\n\n### Three\n";
		let blocks = parse_markdown_blocks(source);

		let levels: Vec<u8> = blocks
			.iter()
			.filter_map(|block| match block {
				MarkdownBlock::Heading { level, .. } => Some(*level),
				_ => None,
			})
			.collect();
		assert_eq!(levels, vec![1, 3]);
		assert_eq!(&source[blocks[0].source_range().clone()], "# One\n");
	}

	#[test]
	fn nested_list_items_keep_depth_and_markers() {
		let blocks = parse_markdown_blocks("1. first\n2. second\n   - nested\n");

		let items: Vec<(u16, ListMarker, String)> = blocks
			.iter()
			.filter_map(|block| match block {
				MarkdownBlock::ListItem {
					depth,
					marker,
					text,
					..
				} => Some((*depth, *marker, text.plain_text())),
				_ => None,
			})
			.collect();
		// A nested list stays its own item rather than folding into the parent's
		// text: depth is what a head indents by.
		assert_eq!(
			items,
			vec![
				(0, ListMarker::Ordered(1), "first".to_string()),
				(0, ListMarker::Ordered(2), "second".to_string()),
				(1, ListMarker::Unordered, "nested".to_string()),
			]
		);
	}

	#[test]
	fn task_markers_are_lifted_out_of_the_item_text() {
		let blocks = parse_markdown_blocks("- [x] done\n- [ ] todo\n");

		let states: Vec<(Option<bool>, String)> = blocks
			.iter()
			.filter_map(|block| match block {
				MarkdownBlock::ListItem { checked, text, .. } => {
					Some((*checked, text.plain_text()))
				}
				_ => None,
			})
			.collect();
		assert_eq!(
			states,
			vec![
				(Some(true), "done".to_string()),
				(Some(false), "todo".to_string()),
			]
		);
	}

	#[test]
	fn fenced_blocks_report_their_language_including_attribute_form() {
		let blocks =
			parse_markdown_blocks("```rust\nfn main() {}\n```\n\n```{.python}\nprint(1)\n```\n");

		let languages: Vec<Option<String>> = blocks
			.iter()
			.filter_map(|block| match block {
				MarkdownBlock::CodeBlock { language, .. } => Some(language.clone()),
				_ => None,
			})
			.collect();
		assert_eq!(
			languages,
			vec![Some("rust".to_string()), Some("python".to_string())]
		);
	}

	#[test]
	fn tables_keep_their_rows_and_cells() {
		let blocks = parse_markdown_blocks("| a | b |\n| - | - |\n| 1 | 2 |\n");

		let MarkdownBlock::Table { rows, .. } = &blocks[0] else {
			panic!("expected a table, got {:?}", blocks[0]);
		};
		let cells: Vec<Vec<String>> = rows
			.iter()
			.map(|row| row.iter().map(MarkdownInlineText::plain_text).collect())
			.collect();
		assert_eq!(
			cells,
			vec![
				vec!["a".to_string(), "b".to_string()],
				vec!["1".to_string(), "2".to_string()],
			]
		);
	}

	/// Blocks are ordered by where they start, which is what lets a head bisect
	/// them against a source line to find its anchor.
	#[test]
	fn blocks_are_ordered_by_source_position() {
		let source = "# Title\n\ntext\n\n- item\n\n> quote\n";
		let blocks = parse_markdown_blocks(source);

		let starts: Vec<usize> = blocks
			.iter()
			.map(|block| block.source_range().start)
			.collect();
		let mut sorted = starts.clone();
		sorted.sort_unstable();
		assert_eq!(starts, sorted);
	}

	#[test]
	fn line_lookups_round_trip_against_the_source() {
		let source = "one\ntwo\nthree\n";

		assert_eq!(line_start_byte(source, 0), 0);
		assert_eq!(line_start_byte(source, 2), 8);
		assert_eq!(line_start_byte(source, 99), source.len());
		assert_eq!(source_line_for_byte(source, 8), 2);
		// The trailing newline opens a fourth, empty line, exactly as the
		// source pane numbers it.
		assert_eq!(source_line_count(source), 4);
		assert_eq!(source_line_count("no trailing newline"), 1);
		assert_eq!(source_line_count(""), 1);
	}
}
