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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
	use super::{TokenCategory, classify_token};
	use ra_ap_syntax::SyntaxKind;

	#[test]
	fn ident_tokens_have_a_lexical_fallback_category() {
		assert_eq!(classify_token(SyntaxKind::IDENT), TokenCategory::Variable);
	}
}
