//! Pixel widths for a Markdown table's columns in the preview (polariton ADR
//! 0001).
//!
//! libaryon chooses the widths in characters
//! ([`table_content_widths`]); this module converts the pane to characters and
//! the choice back to pixels. Prose in a table renders in the bundled
//! Polariton Plot Sans and inline code in DejaVu Sans Mono, so a cell's width
//! can be estimated from budgets measured against those faces. The estimate
//! bounds printable ASCII in the regular faces; other text is best effort.

use libaryon::svp::markdown_blocks::MarkdownInlineText;
use libaryon::svp::markdown_table_layout::table_content_widths;

use crate::plot::text_estimate::estimate_ascii_label_width;

/// Horizontal padding on each side of a table cell's text.
pub(super) const TABLE_CELL_PADDING_X: u32 = 8;
/// DejaVu Sans Mono advances 0.602 em per character; rounded up.
const CODE_ADVANCE_EM: f32 = 0.61;
/// The advance assumed for a table with no characters at all.
const EMPTY_TABLE_ADVANCE_EM: f32 = 0.55;

/// A cell's estimated width in pixels: prose by Polariton Plot Sans' censused
/// character budgets, inline code at DejaVu Sans Mono's advance.
pub(super) fn cell_estimate_px(cell: &MarkdownInlineText, font_size: f32) -> f32 {
	cell.spans()
		.iter()
		.map(|span| {
			if span.style.code {
				span.text.chars().count() as f32 * CODE_ADVANCE_EM * font_size
			} else {
				estimate_ascii_label_width(&span.text, font_size)
			}
		})
		.sum()
}

/// Text widths in pixels, one per column of the widest row, and which columns
/// were reserved their widest cell's estimate (the preserved columns).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TableColumnPixels {
	pub(super) text: Vec<u32>,
	pub(super) preserved: Vec<bool>,
}

/// Converts a pane `pane_width` pixels wide into text widths for each column.
///
/// The text space is the pane less each cell's padding, `T`. The table's
/// average estimated advance turns `T` into characters for
/// [`table_content_widths`]; then, so the reservation never overspends:
/// - every column kept whole and their widest estimates `R <= T`: each gets
///   its estimate plus an even share of `T - R`;
/// - some kept whole and `R` leaves every other column at least one em: those
///   are reserved their estimates and the others share `T - R` by character
///   width;
/// - otherwise no column is reserved and all share `T` by character width.
///
/// Shares round down and the remainder goes to the widest receiving column, so
/// the text widths always sum to `T`.
pub(super) fn table_column_pixels(
	rows: &[Vec<MarkdownInlineText>],
	pane_width: f32,
	font_size: f32,
) -> TableColumnPixels {
	let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
	if columns == 0 {
		return TableColumnPixels {
			text: Vec::new(),
			preserved: Vec::new(),
		};
	}
	let mut estimate = vec![0u32; columns];
	let mut longest = vec![0usize; columns];
	let mut total_estimate = 0.0f32;
	let mut total_chars = 0usize;
	for row in rows {
		for (column, cell) in row.iter().enumerate() {
			let px = cell_estimate_px(cell, font_size);
			let chars = cell.char_count();
			estimate[column] = estimate[column].max(px.ceil() as u32);
			longest[column] = longest[column].max(chars);
			total_estimate += px;
			total_chars += chars;
		}
	}
	let padding = 2 * TABLE_CELL_PADDING_X * columns as u32;
	let text_space = if pane_width.is_finite() {
		(pane_width.max(0.0).floor() as u32).saturating_sub(padding)
	} else {
		// An unbounded pane: every column fits at its widest estimate.
		estimate.iter().sum()
	};
	let advance = if total_chars == 0 {
		EMPTY_TABLE_ADVANCE_EM * font_size
	} else {
		total_estimate / total_chars as f32
	}
	.max(f32::MIN_POSITIVE);
	let content_chars = (text_space as f32 / advance).floor() as usize;
	let widths = table_content_widths(rows, content_chars);

	let whole = (0..columns)
		.map(|column| widths[column] >= longest[column])
		.collect::<Vec<_>>();
	let reserved = (0..columns)
		.filter(|&column| whole[column])
		.map(|column| estimate[column])
		.sum::<u32>();
	let open = (0..columns)
		.filter(|&column| !whole[column])
		.collect::<Vec<_>>();
	let em = font_size.ceil().max(1.0) as u32;
	let all = (0..columns).collect::<Vec<_>>();

	if open.is_empty() && reserved <= text_space {
		let mut text = estimate;
		share(&mut text, text_space - reserved, &all, None, &widths);
		return TableColumnPixels {
			text,
			preserved: vec![true; columns],
		};
	}
	if !open.is_empty()
		&& reserved.saturating_add((open.len() as u32).saturating_mul(em)) <= text_space
	{
		let mut text = (0..columns)
			.map(|column| if whole[column] { estimate[column] } else { 0 })
			.collect::<Vec<_>>();
		share(
			&mut text,
			text_space - reserved,
			&open,
			Some(&widths),
			&widths,
		);
		return TableColumnPixels {
			text,
			preserved: whole,
		};
	}
	let mut text = vec![0u32; columns];
	share(&mut text, text_space, &all, Some(&widths), &widths);
	TableColumnPixels {
		text,
		preserved: vec![false; columns],
	}
}

/// Adds `amount` pixels across `receivers`, by `weights` (evenly without
/// them, or when every weight is zero), rounding down; the remainder goes to
/// the receiver with the widest character width (ties to the earlier column).
fn share(
	text: &mut [u32],
	amount: u32,
	receivers: &[usize],
	weights: Option<&[usize]>,
	widths: &[usize],
) {
	let Some(&widest) = receivers
		.iter()
		.max_by_key(|&&column| (widths[column], std::cmp::Reverse(column)))
	else {
		return;
	};
	let weight = |column: usize| weights.map_or(1u64, |weights| weights[column] as u64);
	let total = receivers.iter().map(|&column| weight(column)).sum::<u64>();
	let mut given = 0u64;
	for &column in receivers {
		let part = (u64::from(amount) * weight(column))
			.checked_div(total)
			.unwrap_or(u64::from(amount) / receivers.len() as u64);
		text[column] += part as u32;
		given += part;
	}
	text[widest] += (u64::from(amount) - given) as u32;
}

#[cfg(test)]
mod tests {
	use super::*;
	use libaryon::svp::markdown_blocks::{
		MarkdownBlock, MarkdownInlineStyle, parse_markdown_blocks,
	};

	pub(crate) const ATOMIC_OPS_TABLE: &str = "| Atomic op | Returns | Does |\n|---|---|---|\n| `update(cells, id, f)` | `Ok(result)` or `Err(reason)` | Applies `f` to the named cells at one instant, or changes nothing if `f` refuses |\n| `merge(cell, id, delta)` | nothing | Folds a commutative delta into one cell; never refuses |\n| `read(cells, at)` | states | Returns one or many cells as of one version; latest by default |\n| `await(cell, pred)` | state | Returns once `pred` holds for the cell |\n| `send` (returned by `f`) | n/a | Delivers a message exactly once, as an `update` or `merge` at another cell |\n";

	pub(crate) fn table_rows(source: &str) -> Vec<Vec<MarkdownInlineText>> {
		parse_markdown_blocks(source)
			.into_iter()
			.find_map(|block| match block {
				MarkdownBlock::Table { rows, .. } => Some(rows),
				_ => None,
			})
			.expect("a table")
	}

	fn text(value: &str) -> MarkdownInlineText {
		let mut text = MarkdownInlineText::new();
		text.push(MarkdownInlineStyle::TEXT, value);
		text
	}

	fn text_space(rows: &[Vec<MarkdownInlineText>], pane: u32) -> u32 {
		let columns = rows.iter().map(Vec::len).max().unwrap_or(0) as u32;
		pane.saturating_sub(2 * TABLE_CELL_PADDING_X * columns)
	}

	#[test]
	fn text_widths_sum_to_the_text_space_and_preserved_columns_fit_their_estimate() {
		let rows = table_rows(ATOMIC_OPS_TABLE);
		for pane in (0..=2000).step_by(7) {
			let pixels = table_column_pixels(&rows, pane as f32, 17.0);
			assert_eq!(pixels.text.len(), 3);
			assert_eq!(
				pixels.text.iter().sum::<u32>(),
				text_space(&rows, pane),
				"at {pane}"
			);
			for (column, &preserved) in pixels.preserved.iter().enumerate() {
				if preserved {
					let widest = rows
						.iter()
						.filter_map(|row| row.get(column))
						.map(|cell| cell_estimate_px(cell, 17.0).ceil() as u32)
						.max()
						.unwrap_or(0);
					assert!(pixels.text[column] >= widest, "column {column} at {pane}");
				}
			}
		}
	}

	#[test]
	fn the_operator_table_keeps_its_short_columns_at_a_wide_enough_pane() {
		let rows = table_rows(ATOMIC_OPS_TABLE);
		let pixels = table_column_pixels(&rows, 900.0, 17.0);
		assert_eq!(pixels.preserved, vec![true, true, false]);
	}

	#[test]
	fn an_overspent_reservation_falls_back_to_shares() {
		// Narrow characters dominate the average, so the character choice
		// keeps the wide column whole at a width its pixels cannot have.
		let mut rows = vec![vec![text("WWWWWWWWWWWWWWWWWWWW"), text("x")]];
		for _ in 0..200 {
			rows.push(vec![text("iiii"), text("iiii iiii iiii iiii iiii iiii")]);
		}
		let pane = 2 * 2 * TABLE_CELL_PADDING_X + 200;
		let (estimate, chars) = rows
			.iter()
			.flatten()
			.fold((0.0f32, 0usize), |(px, n), cell| {
				(px + cell_estimate_px(cell, 17.0), n + cell.char_count())
			});
		let content_chars = (200.0 / (estimate / chars as f32)).floor() as usize;
		assert!(
			table_content_widths(&rows, content_chars)[0] >= 20,
			"the wide column is kept whole"
		);
		assert!(
			cell_estimate_px(&rows[0][0], 17.0) > 200.0,
			"and its pixels exceed the space"
		);
		let pixels = table_column_pixels(&rows, pane as f32, 17.0);
		assert_eq!(pixels.preserved, vec![false, false]);
		assert_eq!(pixels.text.iter().sum::<u32>(), text_space(&rows, pane));
	}

	#[test]
	fn empty_ragged_and_tiny_tables_give_finite_widths() {
		assert!(table_column_pixels(&[], 500.0, 17.0).text.is_empty());
		let empty = vec![vec![MarkdownInlineText::new(), MarkdownInlineText::new()]];
		let pixels = table_column_pixels(&empty, 500.0, 17.0);
		assert_eq!(pixels.text.iter().sum::<u32>(), text_space(&empty, 500));
		let ragged = vec![
			vec![text("name"), text("status"), text("note")],
			vec![text("alpha")],
		];
		for pane in [0.0, 1.0, 47.0, 48.0, 49.0, 300.0, f32::INFINITY] {
			let pixels = table_column_pixels(&ragged, pane, 17.0);
			assert_eq!(pixels.text.len(), 3);
			assert_eq!(pixels.preserved.len(), 3);
		}
		assert_eq!(table_column_pixels(&ragged, 0.0, 17.0).text, vec![0, 0, 0]);
	}

	// Real shaping and real layout: the cell's own spans through iced's
	// rich-text paragraph (Advanced shaping, word-or-glyph wrapping) and the
	// real table element on a headless tiny-skia renderer. No GPU is needed.
	mod shaping {
		use super::super::super::{
			EditorMsg, MARKDOWN_BODY_LINE_HEIGHT, MARKDOWN_BODY_SIZE, MarkdownPreviewColors,
			markdown_inline_spans, markdown_table_surface,
		};
		use super::*;
		use crate::fonts::{DEJAVU_SANS_MONO, PLOT_TEXT_FONT, POLARITON_PLOT_SANS_REGULAR};
		use iced::advanced::graphics::text::{Paragraph as IcedParagraph, font_system};
		use iced::advanced::layout::{Limits, Node};
		use iced::advanced::renderer::Headless as _;
		use iced::advanced::text::{LineHeight, Paragraph as _, Shaping, Text, Wrapping};
		use iced::advanced::widget::Tree;
		use iced::{Font, Pixels, Size, alignment};
		use libaryon::engine::UiPalette;
		use libaryon::svp::rust_highlight::CATEGORY_COUNT;
		use std::borrow::Cow;

		fn load_fonts() {
			let mut fonts = font_system().write().expect("global font system lock");
			fonts.load_font(Cow::Borrowed(POLARITON_PLOT_SANS_REGULAR));
			fonts.load_font(Cow::Borrowed(DEJAVU_SANS_MONO));
		}

		fn colors() -> MarkdownPreviewColors {
			MarkdownPreviewColors::new([None; CATEGORY_COUNT], UiPalette::default())
		}

		/// Lines and widest line of one cell shaped as the table draws it.
		fn shaped(cell: &MarkdownInlineText, text_px: u32) -> (usize, f32) {
			load_fonts();
			let colors = colors();
			let spans = markdown_inline_spans(cell, colors, colors.body, PLOT_TEXT_FONT);
			let paragraph = IcedParagraph::with_spans::<EditorMsg>(Text {
				content: &spans[..],
				bounds: Size::new(text_px as f32, f32::INFINITY),
				size: Pixels(MARKDOWN_BODY_SIZE),
				line_height: LineHeight::Absolute(Pixels(MARKDOWN_BODY_LINE_HEIGHT)),
				font: PLOT_TEXT_FONT,
				align_x: alignment::Horizontal::Left.into(),
				align_y: alignment::Vertical::Top,
				shaping: Shaping::Advanced,
				wrapping: Wrapping::WordOrGlyph,
			});
			let bounds = paragraph.min_bounds();
			(
				(bounds.height / MARKDOWN_BODY_LINE_HEIGHT).round() as usize,
				bounds.width,
			)
		}

		fn measured_ascii_regular(cell: &MarkdownInlineText) -> bool {
			cell.spans().iter().all(|span| {
				!span.style.strong
					&& !span.style.emphasis
					&& span.text.chars().all(|ch| (' '..='~').contains(&ch))
			})
		}

		const MIXED_TABLE: &str = "| **Name** | Kind | Notes |\n|---|---|---|\n| `wal_v21_entries.txt` | golden | Replayed at startup; the snapshot and the log must agree on every entry |\n| `snapshot_v21.txt` | golden | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |\n| `PD envelope` | format | Version 16 since takeover |\n";

		#[test]
		fn the_operator_table_keeps_atomic_op_and_returns_on_one_line_while_does_wraps() {
			let rows = table_rows(ATOMIC_OPS_TABLE);
			let pixels = table_column_pixels(&rows, 900.0, MARKDOWN_BODY_SIZE);
			assert_eq!(pixels.preserved, vec![true, true, false]);
			for row in &rows {
				for (column, cell) in row.iter().enumerate().take(2) {
					assert_eq!(shaped(cell, pixels.text[column]).0, 1, "{cell:?}");
				}
			}
			assert!(rows.iter().any(|row| shaped(&row[2], pixels.text[2]).0 > 1));
		}

		#[test]
		fn preserved_columns_shape_to_one_line_at_every_width() {
			for table in [ATOMIC_OPS_TABLE, MIXED_TABLE] {
				let rows = table_rows(table);
				for pane in (300..=1600).step_by(50) {
					let pixels = table_column_pixels(&rows, pane as f32, MARKDOWN_BODY_SIZE);
					for (column, &preserved) in pixels.preserved.iter().enumerate() {
						if !preserved {
							continue;
						}
						for (index, row) in rows.iter().enumerate() {
							let Some(cell) = row.get(column) else {
								continue;
							};
							let (lines, _) = shaped(cell, pixels.text[column]);
							if measured_ascii_regular(cell) {
								assert_eq!(lines, 1, "row {index} column {column} at {pane} px");
							} else if lines != 1 {
								// Outside the measured bound: reported, not asserted.
								println!(
									"best effort: row {index} column {column} at {pane} px shaped to {lines} lines"
								);
							}
						}
					}
				}
			}
		}

		#[test]
		fn an_unbreakable_token_breaks_inside_its_column() {
			let rows = table_rows(MIXED_TABLE);
			let pixels = table_column_pixels(&rows, 520.0, MARKDOWN_BODY_SIZE);
			let (lines, width) = shaped(&rows[2][2], pixels.text[2]);
			assert!(lines > 1, "the 64-character token wraps");
			assert!(
				width <= pixels.text[2] as f32 + 0.5,
				"{width} px spills past {} px",
				pixels.text[2]
			);
		}

		fn headless_renderer() -> iced::Renderer {
			load_fonts();
			iced::futures::executor::block_on(iced::Renderer::new(
				Font::DEFAULT,
				Pixels(16.0),
				Some("tiny-skia"),
			))
			.expect("a tiny-skia headless renderer")
		}

		/// Each row's cell nodes: (x, width) in the row, from the real layout.
		fn laid_out(rows: &[Vec<MarkdownInlineText>], width: f32) -> (Node, Vec<Vec<(f32, f32)>>) {
			let renderer = headless_renderer();
			let mut element = markdown_table_surface(rows, colors());
			let mut tree = Tree::new(&element);
			let node = element.as_widget_mut().layout(
				&mut tree,
				&renderer,
				&Limits::new(Size::ZERO, Size::new(width, f32::INFINITY)),
			);
			let table = &node.children()[0];
			let cells = table
				.children()
				.iter()
				// Banded rows hold a row of cells; the header rule holds only a space.
				.filter(|child| {
					child
						.children()
						.first()
						.is_some_and(|row| !row.children().is_empty())
				})
				.map(|banded| {
					banded.children()[0]
						.children()
						.iter()
						.map(|cell| (cell.bounds().x, cell.bounds().width))
						.collect::<Vec<_>>()
				})
				.collect::<Vec<_>>();
			(node, cells)
		}

		#[test]
		fn the_real_table_lays_out_aligned_and_resizes() {
			let mut rows = table_rows(MIXED_TABLE);
			rows[3].truncate(1); // a ragged row
			let mut previous = None;
			for width in [320.0, 700.0, 1400.0] {
				let (node, cells) = laid_out(&rows, width);
				let height = node.bounds().height;
				assert!(
					height.is_finite() && height > 0.0,
					"height {height} at {width}"
				);
				assert_eq!(cells.len(), rows.len());
				let pixels = table_column_pixels(&rows, width, MARKDOWN_BODY_SIZE);
				let expected = pixels
					.text
					.iter()
					.map(|&text| (text + 2 * TABLE_CELL_PADDING_X) as f32)
					.collect::<Vec<_>>();
				for row in &cells {
					assert_eq!(
						row.iter().map(|cell| cell.1).collect::<Vec<_>>(),
						expected,
						"at {width}"
					);
					assert_eq!(row, &cells[0], "columns align at {width}");
				}
				assert!(
					previous.as_ref() != Some(&expected),
					"widths change on resize"
				);
				previous = Some(expected);
			}
		}
	}
}
