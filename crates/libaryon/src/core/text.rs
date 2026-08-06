//! Unicode-aware display cells over the engine's byte-authoritative text.
//!
//! Storage may contain arbitrary bytes and may split UTF-8 at leaf boundaries.
//! Callers provide the contiguous bytes they can observe; valid UTF-8 runs are
//! segmented into extended grapheme clusters, while each genuinely invalid
//! byte remains one visible fallback cell.

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisplayCellKind {
	Text,
	Tab,
	Newline,
	InvalidByte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DisplayCell {
	pub byte_start: usize,
	pub byte_end: usize,
	pub width: u32,
	pub kind: DisplayCellKind,
}

fn visit_text_graphemes(
	base: usize,
	text: &str,
	visit: &mut impl FnMut(DisplayCell) -> bool,
) -> bool {
	for (offset, grapheme) in text.grapheme_indices(true) {
		let byte_start = base + offset;
		let byte_end = byte_start + grapheme.len();
		let width = u32::try_from(UnicodeWidthStr::width(grapheme))
			.unwrap_or(u32::MAX)
			.max(1);
		if !visit(DisplayCell {
			byte_start,
			byte_end,
			width,
			kind: DisplayCellKind::Text,
		}) {
			return false;
		}
	}
	true
}

fn visit_valid_cells(base: usize, text: &str, visit: &mut impl FnMut(DisplayCell) -> bool) -> bool {
	if text.is_ascii() {
		for (offset, byte) in text.bytes().enumerate() {
			let (kind, width) = match byte {
				b'\n' => (DisplayCellKind::Newline, 0),
				b'\t' => (DisplayCellKind::Tab, 0),
				_ => (DisplayCellKind::Text, 1),
			};
			if !visit(DisplayCell {
				byte_start: base + offset,
				byte_end: base + offset + 1,
				width,
				kind,
			}) {
				return false;
			}
		}
		return true;
	}

	let mut run_start = 0usize;
	for (offset, character) in text.char_indices() {
		let kind = match character {
			'\n' => DisplayCellKind::Newline,
			'\t' => DisplayCellKind::Tab,
			_ => continue,
		};
		if !visit_text_graphemes(base + run_start, &text[run_start..offset], visit) {
			return false;
		}
		if !visit(DisplayCell {
			byte_start: base + offset,
			byte_end: base + offset + 1,
			width: 0,
			kind,
		}) {
			return false;
		}
		run_start = offset + 1;
	}
	visit_text_graphemes(base + run_start, &text[run_start..], visit)
}

/// Visit display cells without allocating one record per grapheme. Invalid
/// bytes remain individually addressable; valid text never exposes an
/// interior scalar or grapheme boundary. Return `false` from the visitor to
/// stop early.
pub fn visit_display_cells(bytes: &[u8], mut visit: impl FnMut(DisplayCell) -> bool) {
	let mut offset = 0usize;
	while offset < bytes.len() {
		match std::str::from_utf8(&bytes[offset..]) {
			Ok(text) => {
				visit_valid_cells(offset, text, &mut visit);
				break;
			}
			Err(error) if error.valid_up_to() > 0 => {
				let valid_end = offset + error.valid_up_to();
				let text = std::str::from_utf8(&bytes[offset..valid_end])
					.expect("the UTF-8 validator identified a valid prefix");
				if !visit_valid_cells(offset, text, &mut visit) {
					break;
				}
				offset = valid_end;
			}
			Err(_) => {
				if !visit(DisplayCell {
					byte_start: offset,
					byte_end: offset + 1,
					width: 1,
					kind: DisplayCellKind::InvalidByte,
				}) {
					break;
				}
				offset += 1;
			}
		}
	}
}

/// Collect display cells when a caller genuinely needs random access.
pub fn display_cells(bytes: &[u8]) -> Vec<DisplayCell> {
	let mut cells = Vec::new();
	visit_display_cells(bytes, |cell| {
		cells.push(cell);
		true
	});
	cells
}

pub fn advance_display_col(column: u32, cell: DisplayCell, tab_size: u32) -> u32 {
	match cell.kind {
		DisplayCellKind::Newline => 0,
		DisplayCellKind::Tab => column.saturating_add(tab_size - column % tab_size),
		DisplayCellKind::Text | DisplayCellKind::InvalidByte => column.saturating_add(cell.width),
	}
}

pub fn display_width(bytes: &[u8], initial_column: u32, tab_size: u32) -> u32 {
	let mut column = initial_column;
	visit_display_cells(bytes, |cell| {
		if cell.kind == DisplayCellKind::Newline {
			return false;
		}
		column = advance_display_col(column, cell, tab_size);
		true
	});
	column
}

/// Map a display column to a byte boundary. A target inside a wide cell lands
/// after that cell, matching the editor's existing tab behavior.
pub fn byte_offset_for_display_col(
	bytes: &[u8],
	target_column: u32,
	initial_column: u32,
	tab_size: u32,
) -> (usize, u32) {
	if target_column <= initial_column {
		return (0, initial_column);
	}
	let mut column = initial_column;
	let mut byte_offset = 0usize;
	visit_display_cells(bytes, |cell| {
		if cell.kind == DisplayCellKind::Newline || column >= target_column {
			return false;
		}
		let next_column = advance_display_col(column, cell, tab_size);
		byte_offset = cell.byte_end;
		column = next_column;
		if target_column <= next_column {
			return false;
		}
		true
	});
	(byte_offset, column)
}

/// Map a byte position to a display column without manufacturing an interior
/// grapheme position. An interior byte offset snaps to the cell's start.
pub fn display_col_for_byte_offset(
	bytes: &[u8],
	byte_offset: usize,
	initial_column: u32,
	tab_size: u32,
) -> u32 {
	let target = byte_offset.min(bytes.len());
	if target == 0 {
		return initial_column;
	}
	let mut column = initial_column;
	visit_display_cells(bytes, |cell| {
		if cell.kind == DisplayCellKind::Newline || cell.byte_end > target {
			return false;
		}
		column = advance_display_col(column, cell, tab_size);
		true
	});
	column
}

/// Return the byte span of the display cell containing `byte_offset`. A byte
/// at a cell boundary belongs to the following cell. The returned end is
/// exclusive.
pub fn display_cell_span_for_byte_offset(
	bytes: &[u8],
	byte_offset: usize,
) -> Option<(usize, usize)> {
	if bytes.is_empty() {
		return None;
	}
	let target = byte_offset.min(bytes.len().saturating_sub(1));
	let mut span = None;
	visit_display_cells(bytes, |cell| {
		if target < cell.byte_end {
			span = Some((cell.byte_start, cell.byte_end));
			return false;
		}
		true
	});
	span
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn emoji_sequences_are_single_wide_display_cells() {
		let cells = display_cells("☠️".as_bytes());
		assert_eq!(cells.len(), 1);
		assert_eq!(cells[0].byte_end, "☠️".len());
		assert_eq!(cells[0].width, 2);
	}

	#[test]
	fn invalid_bytes_remain_visible_individual_cells() {
		let cells = display_cells(b"a\xffb");
		assert_eq!(cells.len(), 3);
		assert_eq!(cells[1].kind, DisplayCellKind::InvalidByte);
		assert_eq!(display_width(b"a\xffb", 0, 4), 3);
	}

	#[test]
	fn crlf_preserves_the_byte_authoritative_newline_boundary() {
		let cells = display_cells(b"a\r\nb");
		assert_eq!(cells.len(), 4);
		assert_eq!(cells[1].kind, DisplayCellKind::Text);
		assert_eq!(cells[2].kind, DisplayCellKind::Newline);
	}

	#[test]
	fn wide_cell_coordinate_mapping_never_returns_an_interior_byte() {
		let text = "a☠️b";
		let skull_end = 1 + "☠️".len();
		assert_eq!(
			byte_offset_for_display_col(text.as_bytes(), 2, 0, 4),
			(skull_end, 3)
		);
		assert_eq!(display_col_for_byte_offset(text.as_bytes(), 2, 0, 4), 1);
		assert_eq!(
			display_cell_span_for_byte_offset(text.as_bytes(), 3),
			Some((1, skull_end))
		);
	}
}
