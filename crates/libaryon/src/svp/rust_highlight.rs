use crate::core::DocByte;
use crate::svp::rust_parse::ViewportTree;
use ra_ap_syntax::SyntaxKind;

#[allow(dead_code)]
#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenCategory {
	Keyword,
	String,
	Comment,
	Type,
	Function,
	Number,
	Punctuation,
	Variable,
	Constant,
	Macro,
	Module,
	Lifetime,
	Attribute,
	Operator,
	SelfKeyword,
	BuiltinType,
	MutableVariable,
	Method,
	Crate,
	Whitespace,
	Unclassified,
	GitStaged,
	GitUnstaged,
	GitUntracked,
	GitRename,
}

pub const CATEGORY_COUNT: usize = TokenCategory::GitRename as usize + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HighlightSpan {
	pub start: DocByte,
	pub end: DocByte,
	pub category: TokenCategory,
}

impl HighlightSpan {
	pub const fn new(start: DocByte, end: DocByte, category: TokenCategory) -> Self {
		Self {
			start,
			end,
			category,
		}
	}
}

/// Start-ordered highlight intervals with exact nested-overlap queries.
///
/// Semantic providers can emit a parent-language span together with nested
/// injected-language spans. Starts are monotonic after sorting, but ends are
/// not. Prefix maxima make range-overlap checks exact in logarithmic time; the
/// max-end tree finds the rightmost covering span so the most specific active
/// injection wins without losing an enclosing parent after an inner span ends.
#[derive(Debug, Clone)]
pub(crate) struct HighlightSpanIndex {
	spans: Vec<HighlightSpan>,
	prefix_max_end: Vec<DocByte>,
	tree_leaf_count: usize,
	max_end_tree: Vec<DocByte>,
}

impl HighlightSpanIndex {
	pub(crate) fn new(mut spans: Vec<HighlightSpan>) -> Self {
		// Stable ordering preserves provider layer priority for tied starts.
		spans.sort_by_key(|span| span.start);

		let mut prefix_max_end = Vec::with_capacity(spans.len());
		let mut max_end = DocByte::ZERO;
		for span in &spans {
			max_end = max_end.max(span.end);
			prefix_max_end.push(max_end);
		}

		let tree_leaf_count = spans.len().max(1).next_power_of_two();
		let mut max_end_tree = vec![DocByte::ZERO; tree_leaf_count * 2];
		for (index, span) in spans.iter().enumerate() {
			max_end_tree[tree_leaf_count + index] = span.end;
		}
		for node in (1..tree_leaf_count).rev() {
			max_end_tree[node] = max_end_tree[node * 2].max(max_end_tree[node * 2 + 1]);
		}

		Self {
			spans,
			prefix_max_end,
			tree_leaf_count,
			max_end_tree,
		}
	}

	pub(crate) fn merged(lexical: &[HighlightSpan], semantic: &[HighlightSpan]) -> Self {
		if semantic.is_empty() {
			return Self::new(lexical.to_vec());
		}

		let semantic_index = Self::new(semantic.to_vec());
		let mut merged = Vec::with_capacity(lexical.len() + semantic.len());
		merged.extend(
			lexical
				.iter()
				.copied()
				.filter(|span| !semantic_index.overlaps(span.start, span.end)),
		);
		merged.extend_from_slice(semantic);
		Self::new(merged)
	}

	pub(crate) fn into_spans(self) -> Vec<HighlightSpan> {
		self.spans
	}

	pub(crate) fn overlaps(&self, start: DocByte, end: DocByte) -> bool {
		if start >= end {
			return false;
		}
		let count = self.spans.partition_point(|span| span.start < end);
		count > 0 && self.prefix_max_end[count - 1] > start
	}

	pub(crate) fn category_at(&self, byte: DocByte) -> TokenCategory {
		let before = self.spans.partition_point(|span| span.start <= byte);
		self.rightmost_covering(1, 0, self.tree_leaf_count, before, byte)
			.map_or(TokenCategory::Unclassified, |index| {
				self.spans[index].category
			})
	}

	fn rightmost_covering(
		&self,
		node: usize,
		start: usize,
		end: usize,
		before: usize,
		byte: DocByte,
	) -> Option<usize> {
		if start >= before || self.max_end_tree[node] <= byte {
			return None;
		}
		if end - start == 1 {
			return (start < self.spans.len()).then_some(start);
		}

		let midpoint = start + (end - start) / 2;
		self.rightmost_covering(node * 2 + 1, midpoint, end, before, byte)
			.or_else(|| self.rightmost_covering(node * 2, start, midpoint, before, byte))
	}
}

fn classify_token(kind: SyntaxKind) -> TokenCategory {
	match kind {
		SyntaxKind::FN_KW
		| SyntaxKind::LET_KW
		| SyntaxKind::MUT_KW
		| SyntaxKind::IF_KW
		| SyntaxKind::ELSE_KW
		| SyntaxKind::MATCH_KW
		| SyntaxKind::WHILE_KW
		| SyntaxKind::FOR_KW
		| SyntaxKind::LOOP_KW
		| SyntaxKind::BREAK_KW
		| SyntaxKind::CONTINUE_KW
		| SyntaxKind::RETURN_KW
		| SyntaxKind::STRUCT_KW
		| SyntaxKind::ENUM_KW
		| SyntaxKind::TRAIT_KW
		| SyntaxKind::IMPL_KW
		| SyntaxKind::PUB_KW
		| SyntaxKind::USE_KW
		| SyntaxKind::MOD_KW
		| SyntaxKind::CONST_KW
		| SyntaxKind::STATIC_KW
		| SyntaxKind::TYPE_KW
		| SyntaxKind::ASYNC_KW
		| SyntaxKind::AWAIT_KW
		| SyntaxKind::DYN_KW => TokenCategory::Keyword,

		SyntaxKind::STRING | SyntaxKind::BYTE_STRING => TokenCategory::String,

		SyntaxKind::COMMENT => TokenCategory::Comment,

		SyntaxKind::INT_NUMBER | SyntaxKind::FLOAT_NUMBER => TokenCategory::Number,

		SyntaxKind::L_CURLY
		| SyntaxKind::R_CURLY
		| SyntaxKind::L_PAREN
		| SyntaxKind::R_PAREN
		| SyntaxKind::L_BRACK
		| SyntaxKind::R_BRACK
		| SyntaxKind::COMMA
		| SyntaxKind::SEMICOLON
		| SyntaxKind::COLON
		| SyntaxKind::DOT
		| SyntaxKind::AT
		| SyntaxKind::UNDERSCORE
		| SyntaxKind::QUESTION => TokenCategory::Punctuation,

		SyntaxKind::EQ
		| SyntaxKind::FAT_ARROW
		| SyntaxKind::THIN_ARROW
		| SyntaxKind::BANG
		| SyntaxKind::MINUS
		| SyntaxKind::MINUSEQ
		| SyntaxKind::PLUS
		| SyntaxKind::PLUSEQ
		| SyntaxKind::STAR
		| SyntaxKind::STAREQ
		| SyntaxKind::SLASH
		| SyntaxKind::SLASHEQ
		| SyntaxKind::PERCENT
		| SyntaxKind::PERCENTEQ
		| SyntaxKind::CARET
		| SyntaxKind::CARETEQ
		| SyntaxKind::PIPE
		| SyntaxKind::PIPEEQ
		| SyntaxKind::AMP
		| SyntaxKind::AMPEQ
		| SyntaxKind::L_ANGLE
		| SyntaxKind::R_ANGLE
		| SyntaxKind::LTEQ
		| SyntaxKind::GTEQ
		| SyntaxKind::SHL
		| SyntaxKind::SHLEQ
		| SyntaxKind::SHR
		| SyntaxKind::SHREQ => TokenCategory::Operator,

		// Lexical fallback for names keeps edit-boundary characters from flashing
		// white while semantic highlighting catches up asynchronously.
		SyntaxKind::IDENT => TokenCategory::Variable,

		_ => TokenCategory::Unclassified,
	}
}

pub fn highlight_viewport(viewport: &ViewportTree) -> Vec<HighlightSpan> {
	let mut highlights = Vec::new();

	for token in viewport
		.tree
		.descendants_with_tokens()
		.filter_map(|element| element.into_token())
	{
		let category = classify_token(token.kind());

		if category != TokenCategory::Unclassified {
			let (start, end) = viewport.local_to_global(token.text_range());
			highlights.push(HighlightSpan::new(start, end, category));
		}
	}

	highlights
}

#[cfg(test)]
mod tests {
	use super::{HighlightSpan, HighlightSpanIndex, TokenCategory, classify_token};
	use crate::core::DocByte;
	use ra_ap_syntax::SyntaxKind;

	#[test]
	fn ident_tokens_have_a_lexical_fallback_category() {
		assert_eq!(classify_token(SyntaxKind::IDENT), TokenCategory::Variable);
	}

	#[test]
	fn nested_intervals_keep_the_rightmost_active_category() {
		let span = |start, end, category| {
			HighlightSpan::new(DocByte::new(start), DocByte::new(end), category)
		};
		let index = HighlightSpanIndex::new(vec![
			span(0, 100, TokenCategory::String),
			span(10, 40, TokenCategory::Keyword),
			span(20, 30, TokenCategory::Number),
		]);

		assert_eq!(index.category_at(DocByte::new(25)), TokenCategory::Number);
		assert_eq!(index.category_at(DocByte::new(35)), TokenCategory::Keyword);
		assert_eq!(index.category_at(DocByte::new(60)), TokenCategory::String);
		assert_eq!(
			index.category_at(DocByte::new(100)),
			TokenCategory::Unclassified
		);
	}

	#[test]
	fn nested_intervals_report_half_open_overlap_exactly() {
		let span = |start, end| {
			HighlightSpan::new(
				DocByte::new(start),
				DocByte::new(end),
				TokenCategory::String,
			)
		};
		let index = HighlightSpanIndex::new(vec![span(0, 100), span(10, 20), span(30, 40)]);

		assert!(index.overlaps(DocByte::new(50), DocByte::new(60)));
		assert!(index.overlaps(DocByte::new(15), DocByte::new(16)));
		assert!(!index.overlaps(DocByte::new(100), DocByte::new(110)));
		assert!(!index.overlaps(DocByte::new(110), DocByte::new(120)));
	}
}
