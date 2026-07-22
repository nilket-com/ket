use crate::core::DocByte;
use sqlparser::tokenizer::{Location, Span};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacroKind {
	Expression,
	Block,
	Comment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSpan {
	pub start: usize,
	pub end: usize,
}

impl ByteSpan {
	pub const fn new(start: usize, end: usize) -> Self {
		Self { start, end }
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MacroSpan {
	pub span: ByteSpan,
	pub kind: MacroKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowMask {
	pub shadow_bytes: Vec<u8>,
	pub macro_spans: Vec<MacroSpan>,
}

pub fn shadow_mask_sql(bytes: &[u8]) -> ShadowMask {
	let mut shadow_bytes = bytes.to_vec();
	let mut macro_spans = Vec::new();
	let mut i = 0usize;

	while i + 1 < bytes.len() {
		if bytes[i] != b'{' {
			i += 1;
			continue;
		}

		let (kind, close) = match bytes[i + 1] {
			b'{' => (MacroKind::Expression, [b'}', b'}']),
			b'%' => (MacroKind::Block, [b'%', b'}']),
			b'#' => (MacroKind::Comment, [b'#', b'}']),
			_ => {
				i += 1;
				continue;
			}
		};

		let end = find_closing_tag(bytes, i + 2, close).unwrap_or(bytes.len());
		let span = ByteSpan::new(i, end);
		macro_spans.push(MacroSpan { span, kind });

		match kind {
			MacroKind::Expression => {
				replace_expression_macro(&mut shadow_bytes[span.start..span.end])
			}
			MacroKind::Block | MacroKind::Comment => {
				shadow_bytes[span.start..span.end].fill(b' ');
			}
		}

		i = end.max(i + 2);
	}

	ShadowMask {
		shadow_bytes,
		macro_spans,
	}
}

fn find_closing_tag(bytes: &[u8], start: usize, close: [u8; 2]) -> Option<usize> {
	let mut i = start;
	while i + 1 < bytes.len() {
		if bytes[i] == close[0] && bytes[i + 1] == close[1] {
			return Some(i + 2);
		}
		i += 1;
	}
	None
}

fn replace_expression_macro(buf: &mut [u8]) {
	if buf.is_empty() {
		return;
	}
	if buf.len() == 1 {
		buf[0] = b'_';
		return;
	}

	buf[0] = b'"';
	if buf.len() > 2 {
		let len = buf.len();
		buf[1..len - 1].fill(b'_');
	}
	buf[buf.len() - 1] = b'"';
}

pub fn overlaps_macro(start: usize, end: usize, macro_spans: &[MacroSpan]) -> bool {
	if start >= end || macro_spans.is_empty() {
		return false;
	}

	let idx = macro_spans.partition_point(|macro_span| macro_span.span.end <= start);
	for macro_span in &macro_spans[idx..] {
		if macro_span.span.start >= end {
			break;
		}
		if macro_span.span.end > start {
			return true;
		}
	}

	false
}

pub fn translate_viewport_offset(local: usize, map: &[(usize, DocByte)]) -> DocByte {
	if map.is_empty() {
		return DocByte::new(local as u64);
	}

	let idx = map
		.partition_point(|&(viewport_idx, _)| viewport_idx <= local)
		.saturating_sub(1);
	let (viewport_idx, global_base) = map[idx];
	global_base.saturating_add(local.saturating_sub(viewport_idx) as u64)
}

pub fn translate_viewport_span(span: ByteSpan, map: &[(usize, DocByte)]) -> (DocByte, DocByte) {
	(
		translate_viewport_offset(span.start, map),
		translate_viewport_offset(span.end, map),
	)
}

#[derive(Debug, Clone)]
pub struct LineIndex {
	line_starts: Vec<usize>,
	columns: Vec<Vec<usize>>,
}

impl LineIndex {
	pub fn new(text: &str) -> Self {
		let mut line_starts = vec![0usize];
		let mut columns = vec![vec![0usize]];
		let mut current_line_start = 0usize;

		for (idx, ch) in text.char_indices() {
			if ch == '\n' {
				let next_start = idx + ch.len_utf8();
				line_starts.push(next_start);
				columns.push(vec![0usize]);
				current_line_start = next_start;
			} else {
				let rel = idx + ch.len_utf8() - current_line_start;
				columns
					.last_mut()
					.expect("line index must always contain the current line")
					.push(rel);
			}
		}

		Self {
			line_starts,
			columns,
		}
	}

	pub fn location_to_byte_offset(&self, location: Location) -> Option<usize> {
		if location.line == 0 {
			return Some(0);
		}

		let line_idx = location.line.saturating_sub(1) as usize;
		let col_idx = location.column.saturating_sub(1) as usize;
		let line_start = *self.line_starts.get(line_idx)?;
		let rel = *self.columns.get(line_idx)?.get(col_idx)?;
		Some(line_start + rel)
	}

	pub fn span_to_byte_span(&self, span: Span) -> Option<ByteSpan> {
		let start = self.location_to_byte_offset(span.start)?;
		let end = self.location_to_byte_offset(span.end)?;
		Some(ByteSpan::new(start, end))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn shadow_mask_preserves_lengths_and_macro_ranges() {
		let src = b"select {{ ref('orders') }}\n{% if prod %}\nfrom users\n{% endif %}\n";
		let masked = shadow_mask_sql(src);

		assert_eq!(masked.shadow_bytes.len(), src.len());
		assert_eq!(masked.macro_spans.len(), 3);
		assert_eq!(masked.macro_spans[0].kind, MacroKind::Expression);
		assert_eq!(masked.macro_spans[1].kind, MacroKind::Block);
		assert_eq!(masked.macro_spans[2].kind, MacroKind::Block);

		let expr =
			&masked.shadow_bytes[masked.macro_spans[0].span.start..masked.macro_spans[0].span.end];
		assert_eq!(expr[0], b'"');
		assert_eq!(expr[expr.len() - 1], b'"');
		assert!(expr[1..expr.len() - 1].iter().all(|&b| b == b'_'));

		let block =
			&masked.shadow_bytes[masked.macro_spans[1].span.start..masked.macro_spans[1].span.end];
		assert!(block.iter().all(|&b| b == b' '));
	}

	#[test]
	fn line_index_maps_utf8_locations_to_byte_offsets() {
		let text = "éx\nrow";
		let index = LineIndex::new(text);

		assert_eq!(index.location_to_byte_offset(Location::new(1, 1)), Some(0));
		assert_eq!(index.location_to_byte_offset(Location::new(1, 2)), Some(2));
		assert_eq!(index.location_to_byte_offset(Location::new(1, 3)), Some(3));
		assert_eq!(index.location_to_byte_offset(Location::new(2, 1)), Some(4));
	}
}
