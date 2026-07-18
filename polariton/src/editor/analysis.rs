use std::collections::BTreeMap;

use ropey::Rope;

use super::coords::{CharIdx, LineIdx, document};
use super::folding::{FoldRegion, FoldState};
use super::highlight::{Highlighter, SyntaxLanguage, SyntaxToken, TokenKind};

#[derive(Debug, Clone)]
pub struct Diagnostic {
	pub line: LineIdx,
	pub col_start: CharIdx,
	pub col_end: CharIdx,
	pub message: String,
}

#[derive(Debug, Clone)]
pub struct AnalysisSnapshot {
	pub version: u64,
	pub language: SyntaxLanguage,
	pub tokens: Vec<SyntaxToken>,
	pub diagnostics: Vec<Diagnostic>,
	pub fold_regions: BTreeMap<LineIdx, FoldRegion>,
}

pub fn analyze(version: u64, language: SyntaxLanguage, text: String) -> AnalysisSnapshot {
	let mut highlighter = Highlighter::new(language);
	highlighter.parse(&text);

	let tokens = highlighter.tokens.clone();
	let diagnostics = collect_diagnostics(language, &text, &tokens, highlighter.tree());
	let fold_regions = detect_fold_regions(language, &text, highlighter.tree());

	AnalysisSnapshot {
		version,
		language,
		tokens,
		diagnostics,
		fold_regions,
	}
}

fn detect_fold_regions(
	language: SyntaxLanguage,
	text: &str,
	tree: Option<&tree_sitter::Tree>,
) -> BTreeMap<LineIdx, FoldRegion> {
	let rope = Rope::from_str(text);
	let line_count = LineIdx(rope.len_lines().max(1));
	let mut folds = FoldState::new();
	let mut line_text = |line: LineIdx| {
		if *line >= rope.len_lines() {
			return String::new();
		}
		let s: String = rope.line(*line).chars().collect();
		s.trim_end_matches('\n').trim_end_matches('\r').to_string()
	};
	folds.detect_regions(tree, language, line_count, &mut line_text);
	folds.regions
}

fn collect_diagnostics(
	language: SyntaxLanguage,
	text: &str,
	tokens: &[SyntaxToken],
	tree: Option<&tree_sitter::Tree>,
) -> Vec<Diagnostic> {
	match language {
		SyntaxLanguage::Txt => Vec::new(),
		SyntaxLanguage::Rust => {
			let rope = Rope::from_str(text);
			let mut diagnostics = Vec::new();
			if let Some(tree) = tree {
				walk_errors(tree.root_node(), &rope, &mut diagnostics);
			}
			diagnostics
		}
		SyntaxLanguage::Sql => collect_sql_diagnostics(text, tokens),
	}
}

fn collect_sql_diagnostics(text: &str, tokens: &[SyntaxToken]) -> Vec<Diagnostic> {
	let rope = Rope::from_str(text);
	let mut diagnostics = Vec::new();
	let mut paren_stack: Vec<(LineIdx, CharIdx)> = Vec::new();
	let mut at_stmt_start = true;

	for tok in tokens {
		if tok.byte_range.start >= text.len() {
			continue;
		}
		let slice = match text.get(tok.byte_range.clone()) {
			Some(s) => s,
			None => continue,
		};
		match tok.kind {
			TokenKind::Comment => {}
			TokenKind::Punctuation => match slice {
				"(" => {
					let (line, col) = document::byte_to_char_col(&rope, tok.byte_range.start);
					paren_stack.push((line, col));
					at_stmt_start = false;
				}
				")" => {
					at_stmt_start = false;
					if paren_stack.pop().is_none() {
						let (line, col) = document::byte_to_char_col(&rope, tok.byte_range.start);
						diagnostics.push(Diagnostic {
							line,
							col_start: col,
							col_end: col + 1,
							message: "Unmatched `)`".into(),
						});
					}
				}
				";" => at_stmt_start = true,
				_ => at_stmt_start = false,
			},
			TokenKind::Keyword => at_stmt_start = false,
			TokenKind::Identifier if at_stmt_start => {
				let (line, col) = document::byte_to_char_col(&rope, tok.byte_range.start);
				let message = match sql_keyword_near_miss(slice) {
					Some(kw) => format!(
						"Unrecognized SQL command `{}`, did you mean `{}`?",
						slice, kw
					),
					None => format!("Unrecognized SQL command `{}`", slice),
				};
				diagnostics.push(Diagnostic {
					line,
					col_start: col,
					col_end: col + slice.chars().count(),
					message,
				});
				at_stmt_start = false;
			}
			TokenKind::Identifier => {
				if let Some(kw) = sql_keyword_near_miss(slice) {
					let (line, col) = document::byte_to_char_col(&rope, tok.byte_range.start);
					diagnostics.push(Diagnostic {
						line,
						col_start: col,
						col_end: col + slice.chars().count(),
						message: format!("Did you mean `{}`?", kw),
					});
				}
				at_stmt_start = false;
			}
			_ => at_stmt_start = false,
		}
	}

	for (line, col) in paren_stack {
		diagnostics.push(Diagnostic {
			line,
			col_start: col,
			col_end: col + 1,
			message: "Unclosed `(`".into(),
		});
	}

	diagnostics
}

fn walk_errors<'t>(node: tree_sitter::Node<'t>, rope: &Rope, diagnostics: &mut Vec<Diagnostic>) {
	if node.is_error() || node.is_missing() {
		let start =
			document::point_to_char_pos(rope, node.start_position(), |line| line_text(rope, line));
		let end =
			document::point_to_char_pos(rope, node.end_position(), |line| line_text(rope, line));
		let snippet = if *start.line < rope.len_lines().max(1) {
			let end_col = if start.line == end.line {
				end.col
			} else {
				line_len(rope, start.line)
			};
			let snippet = line_slice(rope, start.line, start.col, end_col);
			if snippet.is_empty() {
				format!("`{}`", node.kind())
			} else {
				format!("`{}`", snippet)
			}
		} else {
			format!("`{}`", node.kind())
		};
		let message = if node.is_missing() {
			format!("Missing token near {}", snippet)
		} else {
			format!("Unexpected {}", snippet)
		};
		diagnostics.push(Diagnostic {
			line: start.line,
			col_start: start.col,
			col_end: if start.line == end.line {
				end.col.max(start.col + 1)
			} else {
				line_len(rope, start.line).max(start.col + 1)
			},
			message,
		});
	}
	for i in 0..node.child_count() {
		if let Some(child) = node.child(i as u32) {
			walk_errors(child, rope, diagnostics);
		}
	}
}

fn line_text(rope: &Rope, line: LineIdx) -> String {
	if *line >= rope.len_lines() {
		return String::new();
	}
	let s: String = rope.line(*line).chars().collect();
	s.trim_end_matches('\n').trim_end_matches('\r').to_string()
}

fn line_len(rope: &Rope, line: LineIdx) -> CharIdx {
	CharIdx(line_text(rope, line).chars().count())
}

fn line_slice(rope: &Rope, line: LineIdx, start_col: CharIdx, end_col: CharIdx) -> String {
	line_text(rope, line)
		.chars()
		.skip(*start_col)
		.take((*end_col).saturating_sub(*start_col))
		.collect()
}

fn sql_keyword_near_miss(word: &str) -> Option<&'static str> {
	if word.is_empty()
		|| !word.chars().all(|c| c.is_ascii_alphabetic() || c == '_')
		|| !word.chars().any(|c| c.is_ascii_uppercase())
	{
		return None;
	}

	const SQL_KEYWORDS: &[&str] = &[
		"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "FROM",
		"WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "OFFSET", "JOIN", "LEFT", "RIGHT",
		"INNER", "OUTER", "ON", "AS", "WITH", "UNION", "ALL", "DISTINCT", "VALUES", "INTO", "SET",
		"CASE", "WHEN", "THEN", "ELSE", "END", "BEGIN",
	];

	let up = word.to_ascii_uppercase();
	if SQL_KEYWORDS.contains(&up.as_str()) {
		return None;
	}

	SQL_KEYWORDS
		.iter()
		.copied()
		.find(|kw| edit_distance_leq_one(&up, kw))
}

fn edit_distance_leq_one(a: &str, b: &str) -> bool {
	if a == b {
		return true;
	}
	let ac: Vec<char> = a.chars().collect();
	let bc: Vec<char> = b.chars().collect();
	let al = ac.len();
	let bl = bc.len();
	if al.abs_diff(bl) > 1 {
		return false;
	}
	if al == bl {
		let diffs: Vec<usize> = (0..al).filter(|&i| ac[i] != bc[i]).collect();
		return diffs.len() == 1
			|| (diffs.len() == 2
				&& diffs[1] == diffs[0] + 1
				&& ac[diffs[0]] == bc[diffs[1]]
				&& ac[diffs[1]] == bc[diffs[0]]);
	}
	let (short, long) = if al < bl { (&ac, &bc) } else { (&bc, &ac) };
	let mut i = 0;
	let mut j = 0;
	let mut edits = 0;
	while i < short.len() && j < long.len() {
		if short[i] == long[j] {
			i += 1;
			j += 1;
		} else {
			edits += 1;
			if edits > 1 {
				return false;
			}
			j += 1;
		}
	}
	true
}
