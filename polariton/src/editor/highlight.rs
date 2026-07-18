use std::ops::Range;
use tree_sitter::{Language, Node, Parser, Tree};

// ─── Token kinds (shared across languages) ────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
	Keyword,
	Type,
	String,
	Number,
	Comment,
	Operator,
	Punctuation,
	Identifier,
	Function,
	Macro,
	Attribute,
	Lifetime,
	Error,
	Plain,
}

#[derive(Debug, Clone)]
pub struct SyntaxToken {
	pub byte_range: Range<usize>,
	pub kind: TokenKind,
}

// ─── Supported languages ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyntaxLanguage {
	Sql,
	Rust,
	Txt,
}

impl SyntaxLanguage {
	pub fn display_name(&self) -> &'static str {
		match self {
			Self::Sql => "SQL",
			Self::Rust => "Rust",
			Self::Txt => "Plain Text",
		}
	}
}

// ─── Highlighter ──────────────────────────────────────────────────────────────

pub struct Highlighter {
	parser: Option<Parser>,
	tree: Option<Tree>,
	pub tokens: Vec<SyntaxToken>,
	pub language: SyntaxLanguage,
}

impl Highlighter {
	pub fn new(language: SyntaxLanguage) -> Self {
		let parser = match language {
			SyntaxLanguage::Sql | SyntaxLanguage::Txt => None,
			SyntaxLanguage::Rust => {
				let mut p = Parser::new();
				let ts_lang = Language::from(tree_sitter_rust::LANGUAGE);
				p.set_language(&ts_lang)
					.expect("failed to set Rust language");
				Some(p)
			}
		};
		Self {
			parser,
			tree: None,
			tokens: Vec::new(),
			language,
		}
	}

	pub fn parse(&mut self, text: &str) {
		match self.language {
			SyntaxLanguage::Txt => {
				self.tree = None;
				self.tokens = Vec::new();
			}
			SyntaxLanguage::Sql => {
				self.tree = None;
				self.tokens = tokenize_sql(text);
			}
			SyntaxLanguage::Rust => {
				if let Some(ref mut p) = self.parser {
					self.tree = p.parse(text, None);
				}
				let language = self.language;
				let mut tokens = Vec::new();
				if let Some(ref tree) = self.tree {
					walk_tokens(tree.root_node(), language, &mut tokens);
				}
				self.tokens = tokens;
			}
		}
	}

	pub fn tree(&self) -> Option<&Tree> {
		self.tree.as_ref()
	}
}

// ─── Rust tree-sitter walker ──────────────────────────────────────────────────

fn walk_tokens(node: Node, language: SyntaxLanguage, tokens: &mut Vec<SyntaxToken>) {
	if node.child_count() == 0 {
		let kind = classify_rust(&node);
		tokens.push(SyntaxToken {
			byte_range: node.byte_range(),
			kind,
		});
	} else {
		for i in 0..node.child_count() {
			if let Some(child) = node.child(i as u32) {
				walk_tokens(child, language, tokens);
			}
		}
	}
}

// ─── SQL manual tokenizer ─────────────────────────────────────────────────────

fn tokenize_sql(text: &str) -> Vec<SyntaxToken> {
	let mut tokens = Vec::new();
	let bytes = text.as_bytes();
	let mut i = 0;

	while i < bytes.len() {
		// Whitespace
		if bytes[i].is_ascii_whitespace() {
			i += 1;
			continue;
		}

		// Line comment: --
		if bytes[i..].starts_with(b"--") {
			let start = i;
			while i < bytes.len() && bytes[i] != b'\n' {
				i += 1;
			}
			tokens.push(SyntaxToken {
				byte_range: start..i,
				kind: TokenKind::Comment,
			});
			continue;
		}

		// Block comment: /* */
		if bytes[i..].starts_with(b"/*") {
			let start = i;
			i += 2;
			loop {
				if i + 1 >= bytes.len() {
					i = bytes.len();
					break;
				}
				if bytes[i] == b'*' && bytes[i + 1] == b'/' {
					i += 2;
					break;
				}
				i += 1;
			}
			tokens.push(SyntaxToken {
				byte_range: start..i,
				kind: TokenKind::Comment,
			});
			continue;
		}

		// Single-quoted string: '...'
		if bytes[i] == b'\'' {
			let start = i;
			i += 1;
			while i < bytes.len() {
				if bytes[i] == b'\\' {
					i += 2;
					continue;
				}
				if bytes[i] == b'\'' {
					i += 1;
					break;
				}
				i += 1;
			}
			tokens.push(SyntaxToken {
				byte_range: start..i,
				kind: TokenKind::String,
			});
			continue;
		}

		// Double-quoted identifier: "..."
		if bytes[i] == b'"' {
			let start = i;
			i += 1;
			while i < bytes.len() && bytes[i] != b'"' {
				i += 1;
			}
			if i < bytes.len() {
				i += 1;
			}
			tokens.push(SyntaxToken {
				byte_range: start..i,
				kind: TokenKind::Identifier,
			});
			continue;
		}

		// Number
		if bytes[i].is_ascii_digit()
			|| (bytes[i] == b'.' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
		{
			let start = i;
			while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
				i += 1;
			}
			tokens.push(SyntaxToken {
				byte_range: start..i,
				kind: TokenKind::Number,
			});
			continue;
		}

		// Identifier or keyword
		if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
			let start = i;
			while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
				i += 1;
			}
			let word = &text[start..i];
			let kind = classify_sql_word(word);
			tokens.push(SyntaxToken {
				byte_range: start..i,
				kind,
			});
			continue;
		}

		// Two-char and three-char operators
		if i + 1 < bytes.len() {
			let slice = &bytes[i..];
			if slice.starts_with(b"->>") {
				tokens.push(SyntaxToken {
					byte_range: i..i + 3,
					kind: TokenKind::Operator,
				});
				i += 3;
				continue;
			}
			let two = &bytes[i..i + 2];
			if matches!(
				two,
				b"!=" | b"<>" | b"<=" | b">=" | b"||" | b"->" | b"::" | b"@>"
			) {
				tokens.push(SyntaxToken {
					byte_range: i..i + 2,
					kind: TokenKind::Operator,
				});
				i += 2;
				continue;
			}
		}

		// Punctuation
		if matches!(
			bytes[i],
			b'(' | b')' | b',' | b';' | b'[' | b']' | b'{' | b'}'
		) {
			tokens.push(SyntaxToken {
				byte_range: i..i + 1,
				kind: TokenKind::Punctuation,
			});
			i += 1;
			continue;
		}

		// Single-char operators
		if matches!(
			bytes[i],
			b'=' | b'<' | b'>' | b'+' | b'-' | b'*' | b'/' | b'%' | b'~' | b'&' | b'|' | b'^'
		) {
			tokens.push(SyntaxToken {
				byte_range: i..i + 1,
				kind: TokenKind::Operator,
			});
			i += 1;
			continue;
		}

		// Skip anything else (e.g. dot used as punctuation)
		if bytes[i] == b'.' {
			tokens.push(SyntaxToken {
				byte_range: i..i + 1,
				kind: TokenKind::Punctuation,
			});
		}
		i += 1;
	}

	tokens
}

fn classify_sql_word(word: &str) -> TokenKind {
	match word.to_uppercase().as_str() {
		"SELECT" | "FROM" | "WHERE" | "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "DROP"
		| "ALTER" | "TABLE" | "INDEX" | "INTO" | "VALUES" | "SET" | "JOIN" | "LEFT" | "RIGHT"
		| "INNER" | "OUTER" | "CROSS" | "ON" | "AND" | "OR" | "NOT" | "IN" | "IS" | "NULL"
		| "AS" | "ORDER" | "BY" | "GROUP" | "HAVING" | "LIMIT" | "OFFSET" | "UNION" | "EXCEPT"
		| "INTERSECT" | "ALL" | "DISTINCT" | "EXISTS" | "BETWEEN" | "LIKE" | "CASE" | "WHEN"
		| "THEN" | "ELSE" | "END" | "BEGIN" | "COMMIT" | "ROLLBACK" | "TRANSACTION" | "IF"
		| "REPLACE" | "WITH" | "RECURSIVE" | "ASC" | "DESC" | "PRIMARY" | "KEY" | "FOREIGN"
		| "REFERENCES" | "CONSTRAINT" | "DEFAULT" | "UNIQUE" | "CHECK" | "CASCADE"
		| "RETURNING" | "USING" | "OVER" | "PARTITION" | "WINDOW" | "ROWS" | "RANGE"
		| "UNBOUNDED" | "PRECEDING" | "FOLLOWING" | "CURRENT" | "ROW" | "GRANT" | "REVOKE"
		| "TRUE" | "FALSE" | "VIEW" | "TRIGGER" | "FUNCTION" | "PROCEDURE" | "SCHEMA"
		| "DATABASE" | "USE" | "SHOW" | "DESCRIBE" | "EXPLAIN" | "ANALYZE" | "VACUUM"
		| "TRUNCATE" | "RENAME" | "TO" | "ADD" | "COLUMN" | "TEMP" | "TEMPORARY"
		| "MATERIALIZED" | "LATERAL" | "NATURAL" | "FULL" | "ILIKE" | "SIMILAR" | "ANY"
		| "SOME" | "COALESCE" | "NULLIF" | "CAST" | "EXTRACT" | "POSITION" | "SUBSTRING"
		| "TRIM" | "OVERLAY" | "PLACING" | "COLLATE" => TokenKind::Keyword,

		"INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "FLOAT" | "DOUBLE" | "REAL"
		| "DECIMAL" | "NUMERIC" | "BOOLEAN" | "BOOL" | "CHAR" | "VARCHAR" | "TEXT" | "BLOB"
		| "DATE" | "TIME" | "TIMESTAMP" | "DATETIME" | "INTERVAL" | "UUID" | "JSON" | "JSONB"
		| "SERIAL" | "BIGSERIAL" | "BYTEA" | "ARRAY" | "MONEY" | "INET" | "TIMESTAMPTZ"
		| "TIMETZ" | "INT2" | "INT4" | "INT8" | "FLOAT4" | "FLOAT8" => TokenKind::Type,

		_ => TokenKind::Identifier,
	}
}

// ─── Rust classifier ──────────────────────────────────────────────────────────

fn classify_rust(node: &Node) -> TokenKind {
	let kind = node.kind();
	if node.is_error() || node.is_missing() {
		return TokenKind::Error;
	}

	match kind {
		// Keywords
		"as" | "async" | "await" | "break" | "const" | "continue" | "crate" | "dyn" | "else"
		| "enum" | "extern" | "fn" | "for" | "if" | "impl" | "in" | "let" | "loop" | "match"
		| "mod" | "move" | "mut" | "pub" | "ref" | "return" | "self" | "Self" | "static"
		| "struct" | "super" | "trait" | "type" | "unsafe" | "use" | "where" | "while"
		| "yield" | "true" | "false" => TokenKind::Keyword,

		// Literals
		"string_literal" | "raw_string_literal" | "string_content" | "char_literal"
		| "escape_sequence" => TokenKind::String,

		"integer_literal" | "float_literal" => TokenKind::Number,

		"line_comment" | "block_comment" => TokenKind::Comment,

		// Attributes
		"attribute_item" | "inner_attribute_item" => TokenKind::Attribute,

		// Lifetimes
		"lifetime" => TokenKind::Lifetime,

		// Macros
		"macro_invocation" | "macro_definition" => TokenKind::Macro,
		"!" if node.parent().map(|p| p.kind()) == Some("macro_invocation") => TokenKind::Macro,

		// Types
		"type_identifier" | "primitive_type" | "generic_type" | "scoped_type_identifier" => {
			TokenKind::Type
		}

		// Identifiers — disambiguate by parent
		"identifier" => {
			if let Some(p) = node.parent() {
				match p.kind() {
					"function_item" | "call_expression" => TokenKind::Function,
					"macro_invocation" | "macro_definition" => TokenKind::Macro,
					"type_identifier" | "struct_item" | "enum_item" | "trait_item"
					| "type_item" | "impl_item" | "use_declaration" => TokenKind::Type,
					"attribute_item" | "inner_attribute_item" => TokenKind::Attribute,
					_ => TokenKind::Identifier,
				}
			} else {
				TokenKind::Identifier
			}
		}

		// Punctuation
		"(" | ")" | "[" | "]" | "{" | "}" | "," | ";" | "." | "::" | ":" | "->" | "=>" | ".."
		| "..=" => TokenKind::Punctuation,

		// Operators
		"=" | "==" | "!=" | "<" | ">" | "<=" | ">=" | "+" | "-" | "*" | "/" | "%" | "&" | "|"
		| "^" | "!" | "~" | "<<" | ">>" | "&&" | "||" | "+=" | "-=" | "*=" | "/=" | "%=" | "&="
		| "|=" | "^=" | "<<=" | ">>=" | "?" => TokenKind::Operator,

		"mutable_specifier" => TokenKind::Keyword,
		"field_identifier" => TokenKind::Identifier,

		"ERROR" => TokenKind::Error,

		_ => {
			if let Some(p) = node.parent() {
				match p.kind() {
					"string_literal" | "raw_string_literal" | "char_literal" => TokenKind::String,
					"line_comment" | "block_comment" => TokenKind::Comment,
					"attribute_item" | "inner_attribute_item" => TokenKind::Attribute,
					"macro_invocation" | "macro_definition" => TokenKind::Macro,
					_ => TokenKind::Plain,
				}
			} else {
				TokenKind::Plain
			}
		}
	}
}
