//! Phase 7 — Rune Lexical Beachhead.
//!
//! Rune 0.14.1 exposes a high-level `Parser` that yields `ast::Token`s via
//! `parser.parse::<ast::Token>()`, but its native `Lexer` is `pub(crate)` and
//! its parser path drops comments/whitespace and cannot recover from lexer
//! errors mid-stream — which is incompatible with the Fold-Placeholder
//! Constraint inherited from Phase 6 (virtual buffers contain the synthetic
//! `❯❯❯` marker, which is not valid Rune source).
//!
//! This module therefore provides a small hand-rolled byte-level lexer that
//! mirrors Rune's token taxonomy (keywords sourced from `rune::ast::Kind`).
//! It is resilient by construction: any byte we don't recognize is emitted as
//! an `Unknown` token (or silently skipped for whitespace) and lexing
//! continues with the next byte — so placeholder runes, stray UTF-8, or
//! unterminated literals never halt the scan.

use crate::core::DocByte;
use crate::svp::sync::ViewportChunk;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuneTokKind {
	Keyword,
	Ident,
	Label,
	Number,
	String,
	Char,
	LineComment,
	BlockComment,
	Punct,
	Op,
	Unknown,
}

#[derive(Debug, Clone, Copy)]
pub struct RuneToken {
	/// Start offset within the fragment buffer (fragment-local, not global).
	pub start: u32,
	/// End offset within the fragment buffer.
	pub end: u32,
	pub kind: RuneTokKind,
}

pub struct RuneViewportTokens {
	pub offset_map: Vec<(usize, DocByte)>,
	pub fragment_start: usize,
	pub tokens: Vec<RuneToken>,
}

pub fn parse_rune_fragment(
	chunk: &ViewportChunk,
	bounds: (usize, usize),
	offset_map: Vec<(usize, DocByte)>,
) -> RuneViewportTokens {
	let (start, end) = bounds;
	let bytes = &chunk.buffer[start..end];
	let tokens = lex(bytes);
	RuneViewportTokens {
		offset_map,
		fragment_start: start,
		tokens,
	}
}

fn translate(local: u32, map: &[(usize, DocByte)]) -> DocByte {
	let idx = map
		.partition_point(|&(v_idx, _)| v_idx <= local as usize)
		.saturating_sub(1);
	let (v_idx, global_base) = map[idx];
	global_base.saturating_add((local as usize - v_idx) as u64)
}

impl RuneViewportTokens {
	pub fn token_to_global(&self, token: &RuneToken) -> (DocByte, DocByte) {
		let local_start = self.fragment_start as u32 + token.start;
		let local_end = self.fragment_start as u32 + token.end;
		(
			translate(local_start, &self.offset_map),
			translate(local_end, &self.offset_map),
		)
	}
}

/// Rune reserved words. Kept in sync with `rune::ast::Kind` keyword variants
/// in rune 0.14.1.
fn is_rune_keyword(word: &[u8]) -> bool {
	matches!(
		word,
		b"abstract"
			| b"alignof"
			| b"as" | b"async"
			| b"await"
			| b"become"
			| b"break"
			| b"const"
			| b"continue"
			| b"crate"
			| b"default"
			| b"do" | b"else"
			| b"enum" | b"extern"
			| b"false"
			| b"final"
			| b"fn" | b"for"
			| b"if" | b"impl"
			| b"in" | b"is"
			| b"let" | b"loop"
			| b"macro"
			| b"match"
			| b"mod" | b"move"
			| b"mut" | b"not"
			| b"offsetof"
			| b"override"
			| b"priv" | b"proc"
			| b"pub" | b"pure"
			| b"ref" | b"return"
			| b"select"
			| b"Self" | b"self"
			| b"sizeof"
			| b"static"
			| b"struct"
			| b"super"
			| b"true" | b"typeof"
			| b"unsafe"
			| b"use" | b"virtual"
			| b"while"
			| b"yield"
	)
}

fn lex(bytes: &[u8]) -> Vec<RuneToken> {
	let mut out = Vec::new();
	let mut i: usize = 0;
	let len = bytes.len();

	while i < len {
		let b = bytes[i];

		// Whitespace: consumed silently.
		if b.is_ascii_whitespace() {
			i += 1;
			continue;
		}

		// Line comment: `// ...`
		if b == b'/' && i + 1 < len && bytes[i + 1] == b'/' {
			let s = i;
			i += 2;
			while i < len && bytes[i] != b'\n' {
				i += 1;
			}
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind: RuneTokKind::LineComment,
			});
			continue;
		}

		// Block comment: `/* ... */`
		if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
			let s = i;
			i += 2;
			while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
				i += 1;
			}
			if i + 1 < len {
				i += 2;
			} else {
				i = len;
			}
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind: RuneTokKind::BlockComment,
			});
			continue;
		}

		// String literal: `"..."` with backslash escapes.
		if b == b'"' {
			let s = i;
			i += 1;
			while i < len && bytes[i] != b'"' {
				if bytes[i] == b'\\' && i + 1 < len {
					i += 2;
					continue;
				}
				if bytes[i] == b'\n' {
					break;
				}
				i += 1;
			}
			if i < len && bytes[i] == b'"' {
				i += 1;
			}
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind: RuneTokKind::String,
			});
			continue;
		}

		// Byte string literal: `b"..."`
		if b == b'b' && i + 1 < len && bytes[i + 1] == b'"' {
			let s = i;
			i += 2;
			while i < len && bytes[i] != b'"' {
				if bytes[i] == b'\\' && i + 1 < len {
					i += 2;
					continue;
				}
				if bytes[i] == b'\n' {
					break;
				}
				i += 1;
			}
			if i < len && bytes[i] == b'"' {
				i += 1;
			}
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind: RuneTokKind::String,
			});
			continue;
		}

		// Char literal OR label (`'ident`).
		if b == b'\'' {
			let s = i;
			i += 1;
			// Label candidate: `'ident` followed by a non-closing-quote.
			if i < len && (bytes[i] == b'_' || bytes[i].is_ascii_alphabetic()) {
				let name_start = i;
				while i < len && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
					i += 1;
				}
				if i < len && bytes[i] == b'\'' {
					// It was actually a char literal wrapping an alphabetic.
					i += 1;
					out.push(RuneToken {
						start: s as u32,
						end: i as u32,
						kind: RuneTokKind::Char,
					});
				} else {
					out.push(RuneToken {
						start: s as u32,
						end: i as u32,
						kind: RuneTokKind::Label,
					});
					let _ = name_start;
				}
				continue;
			}
			// Regular char literal.
			while i < len && bytes[i] != b'\'' {
				if bytes[i] == b'\\' && i + 1 < len {
					i += 2;
					continue;
				}
				if bytes[i] == b'\n' {
					break;
				}
				i += 1;
			}
			if i < len && bytes[i] == b'\'' {
				i += 1;
			}
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind: RuneTokKind::Char,
			});
			continue;
		}

		// Numeric literal: decimal, hex, octal, binary, float.
		if b.is_ascii_digit() {
			let s = i;
			i += 1;
			while i < len {
				let c = bytes[i];
				if c.is_ascii_alphanumeric() || c == b'_' || c == b'.' {
					i += 1;
				} else {
					break;
				}
			}
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind: RuneTokKind::Number,
			});
			continue;
		}

		// Identifier or keyword (ASCII-only; non-ASCII identifiers fall into
		// the Unknown branch below, preserving resilience).
		if b == b'_' || b.is_ascii_alphabetic() {
			let s = i;
			i += 1;
			while i < len && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
				i += 1;
			}
			let kind = if is_rune_keyword(&bytes[s..i]) {
				RuneTokKind::Keyword
			} else {
				RuneTokKind::Ident
			};
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind,
			});
			continue;
		}

		// Single-byte punctuation.
		if matches!(
			b,
			b'(' | b')'
				| b'{' | b'}'
				| b'[' | b']'
				| b',' | b';'
				| b':' | b'.'
				| b'@' | b'?'
				| b'#' | b'$'
		) {
			out.push(RuneToken {
				start: i as u32,
				end: (i + 1) as u32,
				kind: RuneTokKind::Punct,
			});
			i += 1;
			continue;
		}

		// Operator characters. Greedily consume a small run so that `->`,
		// `==`, `<<=`, etc. collapse into one token.
		if matches!(
			b,
			b'+' | b'-'
				| b'*' | b'/'
				| b'%' | b'&'
				| b'|' | b'^'
				| b'~' | b'!'
				| b'=' | b'<'
				| b'>'
		) {
			let s = i;
			i += 1;
			while i < len
				&& matches!(
					bytes[i],
					b'=' | b'&' | b'|' | b'<' | b'>' | b'+' | b'-' | b'*' | b'/' | b'%' | b'^'
				) && (i - s) < 3
			{
				i += 1;
			}
			out.push(RuneToken {
				start: s as u32,
				end: i as u32,
				kind: RuneTokKind::Op,
			});
			continue;
		}

		// Anything else — high-byte UTF-8 (including the `❯❯❯` fold
		// placeholder), stray control characters, etc. — is reported as a
		// single-byte Unknown token and we keep going. This is the heart of
		// the Fold-Placeholder resilience guarantee.
		out.push(RuneToken {
			start: i as u32,
			end: (i + 1) as u32,
			kind: RuneTokKind::Unknown,
		});
		i += 1;
	}

	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lexer_classifies_basic_tokens() {
		let src = b"fn main() { let x = 42; }";
		let toks = lex(src);
		// fn -> Keyword
		assert!(
			toks.iter().any(|t| t.kind == RuneTokKind::Keyword
				&& &src[t.start as usize..t.end as usize] == b"fn")
		);
		// let -> Keyword
		assert!(
			toks.iter().any(|t| t.kind == RuneTokKind::Keyword
				&& &src[t.start as usize..t.end as usize] == b"let")
		);
		// main -> Ident
		assert!(
			toks.iter().any(|t| t.kind == RuneTokKind::Ident
				&& &src[t.start as usize..t.end as usize] == b"main")
		);
		// 42 -> Number
		assert!(
			toks.iter().any(|t| t.kind == RuneTokKind::Number
				&& &src[t.start as usize..t.end as usize] == b"42")
		);
	}

	#[test]
	fn lexer_handles_strings_and_comments() {
		let src = b"// hi\nlet s = \"hello\";\n/* block */";
		let toks = lex(src);
		assert!(toks.iter().any(|t| t.kind == RuneTokKind::LineComment));
		assert!(toks.iter().any(|t| t.kind == RuneTokKind::String));
		assert!(toks.iter().any(|t| t.kind == RuneTokKind::BlockComment));
	}

	#[test]
	fn lexer_survives_fold_placeholder() {
		// ❯❯❯ is U+276F * 3 = 9 bytes of high-byte UTF-8. The lexer must not
		// halt or panic; it should keep classifying the surrounding code.
		let mut src: Vec<u8> = Vec::new();
		src.extend_from_slice(b"fn a() {}\n");
		src.extend_from_slice("❯❯❯".as_bytes());
		src.extend_from_slice(b"\nfn b() {}\n");
		let toks = lex(&src);
		// Both fn tokens survived.
		let fn_count = toks
			.iter()
			.filter(|t| {
				t.kind == RuneTokKind::Keyword && &src[t.start as usize..t.end as usize] == b"fn"
			})
			.count();
		assert_eq!(fn_count, 2, "lexer halted on fold placeholder");
	}

	#[test]
	fn lexer_survives_unterminated_string() {
		let src = b"let s = \"oops\nfn b() {}\n";
		let toks = lex(src);
		// We must still see `fn` after the broken string.
		assert!(
			toks.iter().any(|t| t.kind == RuneTokKind::Keyword
				&& &src[t.start as usize..t.end as usize] == b"fn")
		);
	}

	#[test]
	fn lexer_recognizes_label() {
		let src = b"'loop: while true {}";
		let toks = lex(src);
		assert!(toks.iter().any(|t| t.kind == RuneTokKind::Label));
	}
}
