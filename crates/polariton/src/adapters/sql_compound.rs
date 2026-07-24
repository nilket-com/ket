//! Exact-source planning for an ordered SQL cell.
//!
//! Parsing decides whether the final statement is a query. Source spans only
//! choose byte boundaries: adapters execute the user's original SQL rather
//! than an AST reserialization, and the tail is never executed twice.

use sqlparser::ast::{Spanned, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Token, Tokenizer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompoundTail<'a> {
	Empty,
	Command(&'a str),
	Query { prefix: &'a str, query: &'a str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompoundPlan<'a> {
	pub(crate) statements: usize,
	pub(crate) tail: CompoundTail<'a>,
}

pub(crate) fn plan<'a>(
	dialect: &dyn Dialect,
	source: &'a str,
) -> Result<CompoundPlan<'a>, ParserError> {
	let statements = Parser::parse_sql(dialect, source)?;
	let count = statements.len();
	let Some(Statement::Query(query)) = statements.last() else {
		return Ok(CompoundPlan {
			statements: count,
			tail: if count == 0 {
				CompoundTail::Empty
			} else {
				CompoundTail::Command(source)
			},
		});
	};

	let query_start = location_byte(source, query.span().start).ok_or_else(|| {
		ParserError::ParserError("tail query has no usable source location".to_owned())
	})?;
	let tokens = Tokenizer::new(dialect, source).tokenize_with_location()?;
	let start = tokens
		.iter()
		.filter(|token| token.token == Token::SemiColon)
		.filter_map(|token| location_byte(source, token.span.end))
		.take_while(|byte| *byte <= query_start)
		.last()
		.unwrap_or(0);
	let mut significant = tokens
		.iter()
		.rev()
		.filter(|token| !matches!(token.token, Token::Whitespace(_)));
	let last = significant
		.find(|token| token.token != Token::SemiColon)
		.ok_or_else(|| ParserError::ParserError("tail query has no source tokens".to_owned()))?;
	let end = location_byte(source, last.span.end).ok_or_else(|| {
		ParserError::ParserError("tail query has no usable end location".to_owned())
	})?;
	if start >= end {
		return Err(ParserError::ParserError(
			"tail query source range is empty".to_owned(),
		));
	}

	Ok(CompoundPlan {
		statements: count,
		tail: CompoundTail::Query {
			prefix: &source[..start],
			query: &source[start..end],
		},
	})
}

fn location_byte(source: &str, target: Location) -> Option<usize> {
	if target.line == 0 || target.column == 0 {
		return None;
	}
	let mut line = 1_u64;
	let mut column = 1_u64;
	for (byte, character) in source.char_indices() {
		if line == target.line && column == target.column {
			return Some(byte);
		}
		if character == '\n' {
			line = line.saturating_add(1);
			column = 1;
		} else {
			column = column.saturating_add(1);
		}
	}
	(line == target.line && column == target.column).then_some(source.len())
}

#[cfg(test)]
mod tests {
	use super::{CompoundTail, plan};
	use sqlparser::dialect::SQLiteDialect;

	const DIALECT: SQLiteDialect = SQLiteDialect {};

	#[test]
	fn query_tail_preserves_exact_prefix_and_query_source() {
		let source = concat!(
			"create table t (value text);\n",
			"insert into t values ('semi;colon');\n",
			"-- the product\n",
			"select value from t;\n",
		);
		let planned = plan(&DIALECT, source).expect("compound plan");

		assert_eq!(planned.statements, 3);
		assert_eq!(
			planned.tail,
			CompoundTail::Query {
				prefix: concat!(
					"create table t (value text);\n",
					"insert into t values ('semi;colon');",
				),
				query: "\n-- the product\nselect value from t",
			}
		);
	}

	#[test]
	fn a_cte_belongs_to_the_tail_query() {
		let source =
			"create table t (x int);\nwith answer as (select 42 as x)\nselect x from answer";
		let planned = plan(&DIALECT, source).expect("compound plan");
		let CompoundTail::Query { prefix, query } = planned.tail else {
			panic!("query tail");
		};
		assert_eq!(prefix, "create table t (x int);");
		assert_eq!(
			query,
			"\nwith answer as (select 42 as x)\nselect x from answer"
		);
	}

	#[test]
	fn leading_comments_and_repeated_terminal_delimiters_do_not_change_the_query() {
		let source = "-- keep this hint-shaped source\nselect 'λ' as value;;; -- done";
		let planned = plan(&DIALECT, source).expect("compound plan");
		assert_eq!(
			planned.tail,
			CompoundTail::Query {
				prefix: "",
				query: "-- keep this hint-shaped source\nselect 'λ' as value",
			}
		);
	}

	#[test]
	fn command_tail_keeps_the_whole_compound() {
		let source = "create table t (x int); insert into t values (1);";
		let planned = plan(&DIALECT, source).expect("compound plan");
		assert_eq!(planned.statements, 2);
		assert_eq!(planned.tail, CompoundTail::Command(source));
	}

	#[test]
	fn an_empty_source_has_no_tail() {
		let planned = plan(&DIALECT, " \n -- nothing\n").expect("empty plan");
		assert_eq!(planned.statements, 0);
		assert_eq!(planned.tail, CompoundTail::Empty);
	}
}
