//! Phase 7 — Rune lexical highlighter.
//!
//! Consumes the resilient token stream produced by [`crate::svp::rune_parse`]
//! and maps it into [`HighlightSpan`]s, translating fragment-local offsets to
//! global document offsets via the viewport's offset map.

use crate::svp::rune_parse::{RuneTokKind, RuneViewportTokens};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

fn classify(kind: RuneTokKind) -> TokenCategory {
	match kind {
		RuneTokKind::Keyword => TokenCategory::Keyword,
		RuneTokKind::Ident => TokenCategory::Variable,
		RuneTokKind::Label => TokenCategory::Lifetime,
		RuneTokKind::Number => TokenCategory::Number,
		RuneTokKind::String | RuneTokKind::Char => TokenCategory::String,
		RuneTokKind::LineComment | RuneTokKind::BlockComment => TokenCategory::Comment,
		RuneTokKind::Punct => TokenCategory::Punctuation,
		RuneTokKind::Op => TokenCategory::Operator,
		RuneTokKind::Unknown => TokenCategory::Unclassified,
	}
}

pub fn highlight_rune_viewport(viewport: &RuneViewportTokens) -> Vec<HighlightSpan> {
	let mut highlights = Vec::with_capacity(viewport.tokens.len());
	for token in &viewport.tokens {
		let category = classify(token.kind);
		if category == TokenCategory::Unclassified {
			continue;
		}
		let (start, end) = viewport.token_to_global(token);
		highlights.push(HighlightSpan::new(start, end, category));
	}
	highlights
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::DocByte;
	use crate::svp::rune_parse::parse_rune_fragment;
	use crate::svp::sync::ViewportChunk;

	fn highlight_all(src: &[u8]) -> Vec<HighlightSpan> {
		let chunk = ViewportChunk {
			global_offset: DocByte::ZERO,
			buffer: src.to_vec(),
		};
		let tree = parse_rune_fragment(&chunk, (0, src.len()), vec![(0, DocByte::ZERO)]);
		highlight_rune_viewport(&tree)
	}

	#[test]
	fn rune_highlight_classifies_fn_and_let() {
		let src = b"fn main() { let x = 42; }";
		let spans = highlight_all(src);
		assert!(
			spans
				.iter()
				.any(|s| s.category == TokenCategory::Keyword && s.start == DocByte::ZERO)
		);
		assert!(spans.iter().any(|s| s.category == TokenCategory::Number));
		assert!(spans.iter().any(|s| s.category == TokenCategory::Variable));
	}

	#[test]
	fn rune_highlight_survives_placeholder() {
		let mut src: Vec<u8> = Vec::new();
		src.extend_from_slice(b"fn a() {}\n");
		src.extend_from_slice("❯❯❯".as_bytes());
		src.extend_from_slice(b"\nfn b() {}\n");
		let spans = highlight_all(&src);
		// At minimum, two `fn` keyword spans must survive.
		let kw_count = spans
			.iter()
			.filter(|s| s.category == TokenCategory::Keyword)
			.count();
		assert!(kw_count >= 2, "expected both fn keywords to highlight");
	}
}
