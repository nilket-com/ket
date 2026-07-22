use crate::core::DocByte;
use crate::svp::sync::ViewportChunk;
use ra_ap_syntax::{AstNode, SourceFile, SyntaxNode, TextRange};

pub struct ViewportTree {
	pub offset_map: Vec<(usize, DocByte)>,
	pub fragment_start: usize,
	pub tree: SyntaxNode,
}

pub fn parse_fragment(
	chunk: &ViewportChunk,
	bounds: (usize, usize),
	offset_map: Vec<(usize, DocByte)>,
) -> ViewportTree {
	let (start, end) = bounds;

	// Psycho-optimization: bypass secondary validation.
	// The bounds were already verified as valid UTF-8 in find_safe_parse_boundaries.
	let text = unsafe { std::str::from_utf8_unchecked(&chunk.buffer[start..end]) };

	let parse = SourceFile::parse(text, ra_ap_syntax::Edition::Edition2024);

	ViewportTree {
		offset_map,
		fragment_start: start,
		tree: parse.tree().syntax().clone(),
	}
}

fn translate(local: u32, map: &[(usize, DocByte)]) -> DocByte {
	let idx = map
		.partition_point(|&(v_idx, _)| v_idx <= local as usize)
		.saturating_sub(1);
	let (v_idx, global_base) = map[idx];
	global_base.saturating_add((local as usize - v_idx) as u64)
}

impl ViewportTree {
	pub fn local_to_global(&self, local_range: TextRange) -> (DocByte, DocByte) {
		let start: u32 = local_range.start().into();
		let end: u32 = local_range.end().into();

		let local_start = self.fragment_start as u32 + start;
		let local_end = self.fragment_start as u32 + end;

		(
			translate(local_start, &self.offset_map),
			translate(local_end, &self.offset_map),
		)
	}
}
