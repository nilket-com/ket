use super::super::protocol::VisualKind;
use super::super::support::{line_byte_range, line_end_byte_sparse, line_start_byte_sparse};
use super::cursor::line_col_from_doc_byte_sparse;
use crate::core::{DocByte, DocLine};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::folding::FoldSet;
use crate::engine::undo::{byte_offset_from_line_col, line_col_from_byte_offset};
use crate::uast::UastProjection;

fn clamp_visual_byte(byte: DocByte, buffer: &[u8]) -> Option<DocByte> {
	if buffer.is_empty() {
		None
	} else {
		Some(DocByte::new(
			byte.get().min(buffer.len().saturating_sub(1) as u64),
		))
	}
}

fn clamp_existing_doc_byte(byte: DocByte, file_size: u64) -> Option<DocByte> {
	if file_size == 0 {
		None
	} else {
		Some(DocByte::new(byte.get().min(file_size.saturating_sub(1))))
	}
}

fn display_cell_span(buffer: &[u8], byte: DocByte) -> (DocByte, DocByte) {
	let offset = usize::try_from(byte.get())
		.unwrap_or(usize::MAX)
		.min(buffer.len().saturating_sub(1));
	let line_start = buffer[..offset]
		.iter()
		.rposition(|&value| value == b'\n')
		.map_or(0, |newline| newline + 1);
	let line_end = buffer[offset..]
		.iter()
		.position(|&value| value == b'\n')
		.map_or(buffer.len(), |newline| offset + newline + 1);
	let (start, end_exclusive) = crate::core::text::display_cell_span_for_byte_offset(
		&buffer[line_start..line_end],
		offset - line_start,
	)
	.expect("a clamped byte in a non-empty buffer has a display cell");
	(
		DocByte::new((line_start + start) as u64),
		DocByte::new((line_start + end_exclusive.saturating_sub(1)) as u64),
	)
}

fn display_cell_span_sparse(
	registry: &UastRegistry,
	root: NodeId,
	byte: DocByte,
) -> Result<(DocByte, DocByte), String> {
	let (line, _) = line_col_from_doc_byte_sparse(registry, root, byte)?;
	let line_start = line_start_byte_sparse(registry, root, line)?;
	let line_end = line_end_byte_sparse(registry, root, line, true)?;
	let bytes = registry
		.read_loaded_slice(root, line_start, line_end)
		.map_err(|message| message.to_string())?;
	let relative = byte.saturating_sub(line_start.get()).get() as usize;
	let (start, end_exclusive) =
		crate::core::text::display_cell_span_for_byte_offset(&bytes, relative)
			.ok_or_else(|| "display cell is unavailable".to_string())?;
	Ok((
		line_start.saturating_add(start as u64),
		line_start.saturating_add(end_exclusive.saturating_sub(1) as u64),
	))
}

pub fn resolve_visual_ranges(
	anchor: DocByte,
	cursor: DocByte,
	kind: VisualKind,
	buffer: &[u8],
) -> Vec<(DocByte, DocByte)> {
	let Some(anchor) = clamp_visual_byte(anchor, buffer) else {
		return Vec::new();
	};
	let Some(cursor) = clamp_visual_byte(cursor, buffer) else {
		return Vec::new();
	};

	let start = anchor.min(cursor);
	let end = anchor.max(cursor);

	match kind {
		VisualKind::Char => {
			let (start, _) = display_cell_span(buffer, start);
			let (_, end) = display_cell_span(buffer, end);
			vec![(start, end)]
		}
		VisualKind::Line => {
			let mut expanded_start = start.get() as usize;
			while expanded_start > 0 && buffer[expanded_start - 1] != b'\n' {
				expanded_start -= 1;
			}

			let mut expanded_end = end.get() as usize;
			while expanded_end + 1 < buffer.len() && buffer[expanded_end] != b'\n' {
				expanded_end += 1;
			}

			vec![(
				DocByte::new(expanded_start as u64),
				DocByte::new(expanded_end as u64),
			)]
		}
		VisualKind::Block => {
			let (anchor_line, anchor_col) = line_col_from_byte_offset(buffer, anchor);
			let (cursor_line, cursor_col) = line_col_from_byte_offset(buffer, cursor);
			let min_line = anchor_line.get().min(cursor_line.get());
			let max_line = anchor_line.get().max(cursor_line.get());
			let min_col = anchor_col.min(cursor_col);
			let max_col = anchor_col.max(cursor_col);
			let mut ranges = Vec::new();

			for line in min_line..=max_line {
				let line = DocLine::new(line);
				let (line_start, line_end_with_break) = line_byte_range(buffer, line, line);
				let line_content_end = if line_end_with_break > line_start
					&& buffer[line_end_with_break - 1] == b'\n'
				{
					line_end_with_break - 1
				} else {
					line_end_with_break
				};

				if line_content_end <= line_start {
					continue;
				}

				let start_byte = byte_offset_from_line_col(buffer, line, min_col).get() as usize;
				if start_byte >= line_content_end {
					continue;
				}

				let raw_end_byte = byte_offset_from_line_col(buffer, line, max_col).get() as usize;
				let end_byte = if raw_end_byte >= line_content_end {
					line_content_end - 1
				} else {
					raw_end_byte
				};

				if start_byte <= end_byte {
					ranges.push((
						DocByte::new(start_byte as u64),
						DocByte::new(end_byte as u64),
					));
				}
			}

			ranges
		}
	}
}

pub(crate) fn resolve_visual_ranges_sparse(
	registry: &UastRegistry,
	root: NodeId,
	folds: &FoldSet,
	anchor: DocByte,
	cursor: DocByte,
	kind: VisualKind,
) -> Result<Vec<(DocByte, DocByte)>, String> {
	let file_size = registry.get_total_bytes(root);
	let Some(anchor) = clamp_existing_doc_byte(anchor, file_size) else {
		return Ok(Vec::new());
	};
	let Some(cursor) = clamp_existing_doc_byte(cursor, file_size) else {
		return Ok(Vec::new());
	};

	let start = anchor.min(cursor);
	let end = anchor.max(cursor);

	match kind {
		VisualKind::Char => {
			let (start, _) = display_cell_span_sparse(registry, root, start)?;
			let (_, end) = display_cell_span_sparse(registry, root, end)?;
			Ok(vec![(start, end)])
		}
		VisualKind::Line => {
			let (start_line, _) = line_col_from_doc_byte_sparse(registry, root, start)?;
			let (end_line, _) = line_col_from_doc_byte_sparse(registry, root, end)?;
			let expanded_start = line_start_byte_sparse(registry, root, start_line)?;
			let expanded_end_exclusive = line_end_byte_sparse(registry, root, end_line, true)?;
			if expanded_end_exclusive <= expanded_start {
				Ok(Vec::new())
			} else {
				Ok(vec![(
					expanded_start,
					expanded_end_exclusive.saturating_sub(1),
				)])
			}
		}
		VisualKind::Block => {
			let (anchor_line, anchor_col) = line_col_from_doc_byte_sparse(registry, root, anchor)?;
			let (cursor_line, cursor_col) = line_col_from_doc_byte_sparse(registry, root, cursor)?;
			let min_line = anchor_line.get().min(cursor_line.get());
			let max_line = anchor_line.get().max(cursor_line.get());
			let min_col = anchor_col.min(cursor_col);
			let max_col = anchor_col.max(cursor_col);
			let mut ranges = Vec::new();

			for line in min_line..=max_line {
				let line = DocLine::new(line);
				let start_target = registry
					.find_node_at_line_col(root, folds, line, min_col)
					.ready()
					.ok_or_else(|| "Line index is still building".to_string())?;
				let start_byte = registry.doc_byte_for_node_offset(
					root,
					start_target.node_id,
					start_target.node_byte,
				);
				let line_content_end = line_end_byte_sparse(registry, root, line, false)?;
				if start_byte >= line_content_end {
					continue;
				}

				let end_target = registry
					.find_node_at_line_col(root, folds, line, max_col)
					.ready()
					.ok_or_else(|| "Line index is still building".to_string())?;
				let raw_end = registry.doc_byte_for_node_offset(
					root,
					end_target.node_id,
					end_target.node_byte,
				);
				let end_byte = if raw_end >= line_content_end {
					line_content_end.saturating_sub(1)
				} else {
					raw_end
				};

				if start_byte <= end_byte {
					ranges.push((start_byte, end_byte));
				}
			}

			Ok(ranges)
		}
	}
}

pub(crate) fn extract_visual_text_sparse(
	registry: &UastRegistry,
	root: NodeId,
	ranges: &[(DocByte, DocByte)],
) -> Result<String, String> {
	let mut text = String::new();

	for (idx, (start, end)) in ranges.iter().enumerate() {
		let bytes = registry
			.read_loaded_slice(root, *start, end.saturating_add(1))
			.map_err(|msg| msg.to_string())?;
		if bytes.is_empty() {
			continue;
		}

		if idx > 0 && !text.ends_with('\n') {
			text.push('\n');
		}

		text.push_str(&String::from_utf8_lossy(&bytes));
	}

	Ok(text)
}

#[cfg(test)]
mod tests {
	use super::resolve_visual_ranges;
	use crate::core::DocByte;
	use crate::engine::VisualKind;

	#[test]
	fn resolve_visual_ranges_returns_disjoint_char_range() {
		assert_eq!(
			resolve_visual_ranges(
				DocByte::new(8),
				DocByte::new(3),
				VisualKind::Char,
				b"alpha beta\n",
			),
			vec![(DocByte::new(3), DocByte::new(8))]
		);
	}

	#[test]
	fn char_ranges_expand_to_complete_graphemes() {
		let text = "a☠️b";
		assert_eq!(
			resolve_visual_ranges(
				DocByte::new(3),
				DocByte::new(3),
				VisualKind::Char,
				text.as_bytes(),
			),
			vec![(DocByte::new(1), DocByte::new(6))]
		);
	}

	#[test]
	fn resolve_visual_ranges_expands_line_mode_to_full_lines() {
		assert_eq!(
			resolve_visual_ranges(
				DocByte::new(4),
				DocByte::new(7),
				VisualKind::Line,
				b"aa\nbb\ncc\n",
			),
			vec![(DocByte::new(3), DocByte::new(8))]
		);
	}

	#[test]
	fn resolve_visual_ranges_handles_line_mode_at_eof() {
		assert_eq!(
			resolve_visual_ranges(
				DocByte::new(6),
				DocByte::new(9),
				VisualKind::Line,
				b"alpha\nbeta",
			),
			vec![(DocByte::new(6), DocByte::new(9))]
		);
	}

	#[test]
	fn resolve_visual_ranges_builds_block_matrix_across_lines() {
		assert_eq!(
			resolve_visual_ranges(
				DocByte::new(1),
				DocByte::new(12),
				VisualKind::Block,
				b"abcd\nefgh\nijkl\n",
			),
			vec![
				(DocByte::new(1), DocByte::new(2)),
				(DocByte::new(6), DocByte::new(7)),
				(DocByte::new(11), DocByte::new(12)),
			]
		);
	}

	#[test]
	fn resolve_visual_ranges_clamps_and_skips_short_lines_in_block_mode() {
		assert_eq!(
			resolve_visual_ranges(
				DocByte::new(2),
				DocByte::new(17),
				VisualKind::Block,
				b"abcdef\nabc\nz\nabcdef\n",
			),
			vec![
				(DocByte::new(2), DocByte::new(4)),
				(DocByte::new(9), DocByte::new(9)),
				(DocByte::new(15), DocByte::new(17)),
			]
		);
	}
}
