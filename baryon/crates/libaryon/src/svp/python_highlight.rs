use crate::svp::python_parse::PythonViewportTree;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use ruff_python_parser::TokenKind;

fn classify_python_token(kind: TokenKind) -> TokenCategory {
	match kind {
		// Keywords
		TokenKind::And
		| TokenKind::As
		| TokenKind::Assert
		| TokenKind::Async
		| TokenKind::Await
		| TokenKind::Break
		| TokenKind::Class
		| TokenKind::Continue
		| TokenKind::Def
		| TokenKind::Del
		| TokenKind::Elif
		| TokenKind::Else
		| TokenKind::Except
		| TokenKind::Finally
		| TokenKind::For
		| TokenKind::From
		| TokenKind::Global
		| TokenKind::If
		| TokenKind::Import
		| TokenKind::In
		| TokenKind::Is
		| TokenKind::Lambda
		| TokenKind::Nonlocal
		| TokenKind::Not
		| TokenKind::Or
		| TokenKind::Pass
		| TokenKind::Raise
		| TokenKind::Return
		| TokenKind::Try
		| TokenKind::While
		| TokenKind::With
		| TokenKind::Yield
		| TokenKind::Match
		| TokenKind::Case
		| TokenKind::Type => TokenCategory::Keyword,

		// Constants
		TokenKind::True | TokenKind::False | TokenKind::None => TokenCategory::Constant,

		// Strings
		TokenKind::String
		| TokenKind::FStringStart
		| TokenKind::FStringMiddle
		| TokenKind::FStringEnd => TokenCategory::String,

		// Comments
		TokenKind::Comment => TokenCategory::Comment,

		// Numbers
		TokenKind::Int | TokenKind::Float | TokenKind::Complex => TokenCategory::Number,

		// Punctuation
		TokenKind::Lpar
		| TokenKind::Rpar
		| TokenKind::Lsqb
		| TokenKind::Rsqb
		| TokenKind::Lbrace
		| TokenKind::Rbrace
		| TokenKind::Comma
		| TokenKind::Semi
		| TokenKind::Colon
		| TokenKind::Dot
		| TokenKind::Ellipsis
		| TokenKind::At
		| TokenKind::Rarrow => TokenCategory::Punctuation,

		// Operators
		TokenKind::Plus
		| TokenKind::Minus
		| TokenKind::Star
		| TokenKind::Slash
		| TokenKind::Vbar
		| TokenKind::Amper
		| TokenKind::Less
		| TokenKind::Greater
		| TokenKind::Equal
		| TokenKind::Percent
		| TokenKind::Tilde
		| TokenKind::CircumFlex
		| TokenKind::LeftShift
		| TokenKind::RightShift
		| TokenKind::DoubleStar
		| TokenKind::DoubleSlash
		| TokenKind::EqEqual
		| TokenKind::NotEqual
		| TokenKind::LessEqual
		| TokenKind::GreaterEqual
		| TokenKind::PlusEqual
		| TokenKind::MinusEqual
		| TokenKind::StarEqual
		| TokenKind::SlashEqual
		| TokenKind::PercentEqual
		| TokenKind::AmperEqual
		| TokenKind::VbarEqual
		| TokenKind::CircumflexEqual
		| TokenKind::LeftShiftEqual
		| TokenKind::RightShiftEqual
		| TokenKind::DoubleStarEqual
		| TokenKind::DoubleSlashEqual
		| TokenKind::AtEqual
		| TokenKind::ColonEqual => TokenCategory::Operator,

		// Names — lexical fallback, semantic highlighting will refine
		TokenKind::Name => TokenCategory::Variable,

		_ => TokenCategory::Unclassified,
	}
}

pub fn highlight_python_viewport(viewport: &PythonViewportTree) -> Vec<HighlightSpan> {
	let mut highlights = Vec::new();

	for token in viewport.parsed.tokens().iter() {
		if token.kind() == TokenKind::Unknown {
			continue;
		}
		let category = classify_python_token(token.kind());

		if category != TokenCategory::Unclassified {
			let (start, end) = viewport.token_to_global(token);
			highlights.push(HighlightSpan::new(start, end, category));
		}
	}

	highlights
}

#[cfg(test)]
mod tests {
	use super::{TokenCategory, classify_python_token};
	use ruff_python_parser::TokenKind;

	#[test]
	fn python_name_tokens_have_a_lexical_fallback_category() {
		assert_eq!(
			classify_python_token(TokenKind::Name),
			TokenCategory::Variable
		);
	}

	#[test]
	fn python_def_is_keyword() {
		assert_eq!(
			classify_python_token(TokenKind::Def),
			TokenCategory::Keyword
		);
	}

	#[test]
	fn python_none_is_constant() {
		assert_eq!(
			classify_python_token(TokenKind::None),
			TokenCategory::Constant
		);
	}
}
