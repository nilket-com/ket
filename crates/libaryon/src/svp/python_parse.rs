use crate::core::DocByte;
use crate::svp::sync::ViewportChunk;
use ruff_python_ast::Mod;
use ruff_python_parser::{Mode, Parsed, Token, parse_unchecked};
use ruff_text_size::Ranged;

pub struct PythonViewportTree {
	pub offset_map: Vec<(usize, DocByte)>,
	pub fragment_start: usize,
	pub parsed: Parsed<Mod>,
}

pub fn parse_python_fragment(
	chunk: &ViewportChunk,
	bounds: (usize, usize),
	offset_map: Vec<(usize, DocByte)>,
) -> PythonViewportTree {
	let (start, end) = bounds;

	let text = unsafe { std::str::from_utf8_unchecked(&chunk.buffer[start..end]) };

	let parsed = parse_unchecked(text, Mode::Module);

	PythonViewportTree {
		offset_map,
		fragment_start: start,
		parsed,
	}
}

fn translate(local: u32, map: &[(usize, DocByte)]) -> DocByte {
	let idx = map
		.partition_point(|&(v_idx, _)| v_idx <= local as usize)
		.saturating_sub(1);
	let (v_idx, global_base) = map[idx];
	global_base.saturating_add((local as usize - v_idx) as u64)
}

impl PythonViewportTree {
	pub fn token_to_global(&self, token: &Token) -> (DocByte, DocByte) {
		let range = token.range();
		let start: u32 = range.start().into();
		let end: u32 = range.end().into();

		let local_start = self.fragment_start as u32 + start;
		let local_end = self.fragment_start as u32 + end;

		(
			translate(local_start, &self.offset_map),
			translate(local_end, &self.offset_map),
		)
	}
}
