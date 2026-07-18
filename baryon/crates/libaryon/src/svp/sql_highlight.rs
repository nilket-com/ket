use crate::svp::rust_highlight::TokenCategory;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Whitespace};

pub fn classify_sql_token(token: &Token) -> TokenCategory {
	match token {
		Token::Word(word) if word.keyword != Keyword::NoKeyword => TokenCategory::Keyword,
		Token::Word(_) => TokenCategory::Variable,
		Token::Number(..) => TokenCategory::Number,

		Token::SingleQuotedString(_)
		| Token::DoubleQuotedString(_)
		| Token::TripleSingleQuotedString(_)
		| Token::TripleDoubleQuotedString(_)
		| Token::DollarQuotedString(_)
		| Token::SingleQuotedByteStringLiteral(_)
		| Token::DoubleQuotedByteStringLiteral(_)
		| Token::TripleSingleQuotedByteStringLiteral(_)
		| Token::TripleDoubleQuotedByteStringLiteral(_)
		| Token::SingleQuotedRawStringLiteral(_)
		| Token::DoubleQuotedRawStringLiteral(_)
		| Token::TripleSingleQuotedRawStringLiteral(_)
		| Token::TripleDoubleQuotedRawStringLiteral(_)
		| Token::NationalStringLiteral(_)
		| Token::QuoteDelimitedStringLiteral(_)
		| Token::NationalQuoteDelimitedStringLiteral(_)
		| Token::EscapedStringLiteral(_)
		| Token::UnicodeStringLiteral(_)
		| Token::HexStringLiteral(_) => TokenCategory::String,

		Token::Whitespace(Whitespace::SingleLineComment { .. })
		| Token::Whitespace(Whitespace::MultiLineComment(_)) => TokenCategory::Comment,

		Token::Whitespace(_) | Token::EOF => TokenCategory::Unclassified,

		Token::Comma
		| Token::LParen
		| Token::RParen
		| Token::Period
		| Token::Colon
		| Token::DoubleColon
		| Token::SemiColon
		| Token::Backslash
		| Token::LBracket
		| Token::RBracket
		| Token::LBrace
		| Token::RBrace
		| Token::Char(_) => TokenCategory::Punctuation,

		Token::DoubleEq
		| Token::Eq
		| Token::Neq
		| Token::Lt
		| Token::Gt
		| Token::LtEq
		| Token::GtEq
		| Token::Spaceship
		| Token::Plus
		| Token::Minus
		| Token::Mul
		| Token::Div
		| Token::DuckIntDiv
		| Token::Mod
		| Token::StringConcat
		| Token::Assignment
		| Token::Ampersand
		| Token::Pipe
		| Token::Caret
		| Token::RArrow
		| Token::Sharp
		| Token::DoubleSharp
		| Token::Tilde
		| Token::TildeAsterisk
		| Token::ExclamationMarkTilde
		| Token::ExclamationMarkTildeAsterisk
		| Token::DoubleTilde
		| Token::DoubleTildeAsterisk
		| Token::ExclamationMarkDoubleTilde
		| Token::ExclamationMarkDoubleTildeAsterisk
		| Token::ShiftLeft
		| Token::ShiftRight
		| Token::Overlap
		| Token::ExclamationMark
		| Token::DoubleExclamationMark
		| Token::AtSign
		| Token::CaretAt
		| Token::PGSquareRoot
		| Token::PGCubeRoot
		| Token::Placeholder(_)
		| Token::Arrow
		| Token::LongArrow
		| Token::HashArrow
		| Token::AtDashAt
		| Token::QuestionMarkDash
		| Token::AmpersandLeftAngleBracket
		| Token::AmpersandRightAngleBracket
		| Token::AmpersandLeftAngleBracketVerticalBar
		| Token::VerticalBarAmpersandRightAngleBracket
		| Token::TwoWayArrow
		| Token::LeftAngleBracketCaret
		| Token::RightAngleBracketCaret
		| Token::QuestionMarkSharp
		| Token::QuestionMarkDashVerticalBar
		| Token::QuestionMarkDoubleVerticalBar
		| Token::TildeEqual
		| Token::ShiftLeftVerticalBar
		| Token::VerticalBarShiftRight
		| Token::VerticalBarRightAngleBracket => TokenCategory::Operator,

		_ => TokenCategory::Unclassified,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use sqlparser::tokenizer::{Token, Tokenizer};

	#[test]
	fn sql_keyword_tokens_are_classified() {
		let dialect = sqlparser::dialect::GenericDialect {};
		let tokens = Tokenizer::new(&dialect, "select").tokenize().unwrap();
		assert_eq!(classify_sql_token(&tokens[0]), TokenCategory::Keyword);
	}

	#[test]
	fn sql_comment_tokens_are_classified() {
		let token = Token::Whitespace(Whitespace::SingleLineComment {
			prefix: "--".to_string(),
			comment: " note".to_string(),
		});
		assert_eq!(classify_sql_token(&token), TokenCategory::Comment);
	}
}
