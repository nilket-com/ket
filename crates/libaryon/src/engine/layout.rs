use super::folding::{
	doc_line_for_visual_index, snap_line_to_visible_boundary, visual_line_index_for_doc_line,
};
use super::index::SumTree;
use super::support::{read_line_bytes_from_sum_tree, read_line_bytes_sparse};
use crate::core::{
	DisplayRow, DocByte, DocDisplayCol, DocLine, DocLineCount, DocVisualRow, PaneLocalCol,
	PaneLocalRow, TAB_SIZE, VisualRowCount,
};
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::{FoldPolicy, UastProjection};
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

const MINIMAP_MIN_SCREEN_WIDTH: u16 = 40;
const MINIMAP_RESERVED_COLUMNS: u16 = 24;
const MINIMAP_MAX_WIDTH: u16 = 14;
const WRAP_CACHE_CAPACITY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibleRow {
	pub line: DocLine,
	pub start_col: DocDisplayCol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GutterNumbering {
	Absolute,
	Relative,
	Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewportAnchor {
	pub line: DocLine,
	pub row_offset: DisplayRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewportGeometry {
	pub gutter_width: u16,
	pub text_width: u16,
	pub minimap_width: u16,
	pub separator_width: u16,
}

pub fn viewport_max_line_on_screen(
	viewport_start_line: DocLine,
	viewport_line_count: u32,
	max_doc_line: DocLine,
) -> DocLine {
	(viewport_start_line + DocLineCount::new(u64::from(viewport_line_count.saturating_sub(1))))
		.min(max_doc_line)
}

pub fn gutter_width_for_max_line(max_line_on_screen: u64) -> u16 {
	// Reserve one column for 1-based numbering and one extra column for
	// symmetric left/right gutter padding around the digits.
	let display_number = max_line_on_screen.saturating_add(1).max(1);
	let digits = display_number.ilog10() + 1;
	digits as u16 + 2
}

pub fn viewport_geometry(
	max_line_on_screen: u64,
	screen_width: u16,
	show_minimap: bool,
) -> ViewportGeometry {
	viewport_geometry_with_options(max_line_on_screen, screen_width, show_minimap, true)
}

pub fn viewport_geometry_with_options(
	max_line_on_screen: u64,
	screen_width: u16,
	show_minimap: bool,
	show_gutter: bool,
) -> ViewportGeometry {
	let minimap_width = if show_minimap && screen_width > MINIMAP_MIN_SCREEN_WIDTH {
		MINIMAP_MAX_WIDTH.min(screen_width.saturating_sub(MINIMAP_RESERVED_COLUMNS))
	} else {
		0
	};
	let separator_width = if minimap_width > 0 { 1 } else { 0 };
	let gutter_width = if show_gutter {
		gutter_width_for_max_line(max_line_on_screen)
	} else {
		0
	};
	let text_width = screen_width.saturating_sub(gutter_width + minimap_width + separator_width);

	ViewportGeometry {
		gutter_width,
		text_width,
		minimap_width,
		separator_width,
	}
}

pub fn viewport_geometry_for_viewport(
	viewport_start_line: DocLine,
	viewport_line_count: u32,
	max_doc_line: DocLine,
	screen_width: u16,
	show_minimap: bool,
) -> ViewportGeometry {
	viewport_geometry_for_viewport_with_options(
		viewport_start_line,
		viewport_line_count,
		max_doc_line,
		screen_width,
		show_minimap,
		true,
	)
}

pub fn viewport_geometry_for_viewport_with_options(
	viewport_start_line: DocLine,
	viewport_line_count: u32,
	max_doc_line: DocLine,
	screen_width: u16,
	show_minimap: bool,
	show_gutter: bool,
) -> ViewportGeometry {
	viewport_geometry_with_options(
		viewport_max_line_on_screen(viewport_start_line, viewport_line_count, max_doc_line).get(),
		screen_width,
		show_minimap,
		show_gutter,
	)
}

pub fn viewport_geometry_for_viewport_with_numbering(
	viewport_start_line: DocLine,
	viewport_line_count: u32,
	max_doc_line: DocLine,
	screen_width: u16,
	show_minimap: bool,
	gutter_numbering: Option<GutterNumbering>,
) -> ViewportGeometry {
	let minimap_width = if show_minimap && screen_width > MINIMAP_MIN_SCREEN_WIDTH {
		MINIMAP_MAX_WIDTH.min(screen_width.saturating_sub(MINIMAP_RESERVED_COLUMNS))
	} else {
		0
	};
	let separator_width = if minimap_width > 0 { 1 } else { 0 };
	let gutter_width = gutter_numbering.map_or(0, |numbering| {
		let max_display_number = match numbering {
			GutterNumbering::Absolute | GutterNumbering::Hybrid => {
				viewport_max_line_on_screen(viewport_start_line, viewport_line_count, max_doc_line)
					.get()
					.saturating_add(1)
			}
			GutterNumbering::Relative => u64::from(viewport_line_count.saturating_sub(1)),
		};
		let digits = max_display_number.max(1).ilog10() + 1;
		digits as u16 + 2
	});
	let text_width = screen_width.saturating_sub(gutter_width + minimap_width + separator_width);

	ViewportGeometry {
		gutter_width,
		text_width,
		minimap_width,
		separator_width,
	}
}

#[derive(Clone)]
struct WrappedLine {
	starts: Rc<[DocDisplayCol]>,
	visual_width: u32,
}

fn push_word_wraps(
	starts: &mut Vec<DocDisplayCol>,
	row_start: &mut u32,
	word_start: u32,
	word_end: u32,
	width: u32,
) {
	if word_end.saturating_sub(*row_start) <= width {
		return;
	}
	if word_start > *row_start {
		starts.push(DocDisplayCol::new(word_start));
		*row_start = word_start;
	}
	while word_end.saturating_sub(*row_start) > width {
		*row_start = row_start.saturating_add(width);
		starts.push(DocDisplayCol::new(*row_start));
	}
}

fn compute_line_wrap(line: &[u8], text_width: u16) -> WrappedLine {
	let mut starts = vec![DocDisplayCol::ZERO];
	if text_width == 0 {
		return WrappedLine {
			starts: starts.into(),
			visual_width: 0,
		};
	}

	// Keep this visual-cell model aligned with the editor's document columns:
	// tabs expand to fixed tab stops and valid text advances by grapheme width.
	// Invalid bytes remain one visible fallback cell.
	let width = u32::from(text_width);
	let mut visual_col = 0u32;
	let mut row_start = 0u32;
	let mut word_start = None;
	crate::core::text::visit_display_cells(line, |cell| {
		if cell.kind == crate::core::text::DisplayCellKind::Newline {
			return false;
		}
		let cell_bytes = &line[cell.byte_start..cell.byte_end];
		let is_whitespace = cell.kind == crate::core::text::DisplayCellKind::Tab
			|| std::str::from_utf8(cell_bytes)
				.ok()
				.is_some_and(|text| text.chars().all(char::is_whitespace));
		if is_whitespace {
			if let Some(start) = word_start.take() {
				push_word_wraps(&mut starts, &mut row_start, start, visual_col, width);
			}
		} else if word_start.is_none() {
			word_start = Some(visual_col);
		}
		visual_col = crate::core::text::advance_display_col(visual_col, cell, TAB_SIZE);
		true
	});
	if let Some(start) = word_start {
		push_word_wraps(&mut starts, &mut row_start, start, visual_col, width);
	}

	// Whitespace-only lines and long trailing whitespace still need reachable
	// continuation rows, even though separator whitespace at a word break may
	// be clipped at the right edge.
	while visual_col.saturating_sub(row_start) > width {
		row_start = row_start.saturating_add(width);
		starts.push(DocDisplayCol::new(row_start));
	}

	WrappedLine {
		starts: starts.into(),
		visual_width: visual_col,
	}
}

#[cfg(test)]
fn wrap_row_starts(line: &[u8], text_width: u16) -> Vec<DocDisplayCol> {
	compute_line_wrap(line, text_width).starts.as_ref().to_vec()
}

fn visible_line_for_layout<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	line: DocLine,
	max_doc_line: DocLine,
) -> DocLine {
	let line = line.min(max_doc_line);
	if folds.has_folds() {
		snap_line_to_visible_boundary(registry, root, folds, line)
	} else {
		line
	}
}

fn visual_index_for_layout<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	line: DocLine,
	max_doc_line: DocLine,
) -> DocVisualRow {
	if folds.has_folds() {
		visual_line_index_for_doc_line(registry, root, folds, line)
	} else {
		DocVisualRow::new(line.get().min(max_doc_line.get()))
	}
}

fn line_for_visual_index_for_layout<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	target_visual_index: DocVisualRow,
	max_doc_line: DocLine,
) -> DocLine {
	if folds.has_folds() {
		doc_line_for_visual_index(registry, root, folds, target_visual_index, max_doc_line)
	} else {
		DocLine::new(target_visual_index.get().min(max_doc_line.get()))
	}
}

#[derive(Clone, Copy)]
enum LineByteSource<'a> {
	Sparse,
	SumTree(&'a SumTree),
}

struct LayoutContext<'a, F: FoldPolicy + ?Sized> {
	registry: &'a UastRegistry,
	root: NodeId,
	folds: &'a F,
	max_doc_line: DocLine,
	wrap_enabled: bool,
	text_width: u16,
	line_byte_source: LineByteSource<'a>,
	wrap_cache: RefCell<VecDeque<(DocLine, WrappedLine)>>,
}

impl<'a, F: FoldPolicy + ?Sized> LayoutContext<'a, F> {
	fn sparse(
		registry: &'a UastRegistry,
		root: NodeId,
		folds: &'a F,
		max_doc_line: DocLine,
		wrap_enabled: bool,
		text_width: u16,
	) -> Self {
		Self {
			registry,
			root,
			folds,
			max_doc_line,
			wrap_enabled,
			text_width,
			line_byte_source: LineByteSource::Sparse,
			wrap_cache: RefCell::new(VecDeque::new()),
		}
	}

	fn with_sum_tree(
		registry: &'a UastRegistry,
		root: NodeId,
		folds: &'a F,
		max_doc_line: DocLine,
		wrap_enabled: bool,
		text_width: u16,
		tree: &'a SumTree,
	) -> Self {
		Self {
			registry,
			root,
			folds,
			max_doc_line,
			wrap_enabled,
			text_width,
			line_byte_source: if tree.has_cold_extents() {
				LineByteSource::Sparse
			} else {
				LineByteSource::SumTree(tree)
			},
			wrap_cache: RefCell::new(VecDeque::new()),
		}
	}

	fn visible_line(&self, line: DocLine) -> DocLine {
		visible_line_for_layout(
			self.registry,
			self.root,
			self.folds,
			line,
			self.max_doc_line,
		)
	}

	fn visual_index(&self, line: DocLine) -> DocVisualRow {
		visual_index_for_layout(
			self.registry,
			self.root,
			self.folds,
			line,
			self.max_doc_line,
		)
	}

	fn line_for_visual_index(&self, target_visual_index: DocVisualRow) -> DocLine {
		line_for_visual_index_for_layout(
			self.registry,
			self.root,
			self.folds,
			target_visual_index,
			self.max_doc_line,
		)
	}

	fn node_starting_line(&self, line: DocLine) -> Option<NodeId> {
		match self.line_byte_source {
			LineByteSource::Sparse => self
				.registry
				.find_node_at_line_col(self.root, self.folds, line, DocDisplayCol::ZERO)
				.ready()
				.map(|target| target.node_id),
			LineByteSource::SumTree(tree) => {
				let byte_offset = DocByte::new(
					tree.query_by_doc_line_count(line.boundary_before())
						.byte_offset,
				);
				Some(
					self.registry
						.find_node_at_doc_byte_raw(self.root, byte_offset)
						.node_id,
				)
			}
		}
	}

	fn is_folded_visible_line(&self, line: DocLine) -> bool {
		if !self.folds.has_folds() {
			return false;
		}
		let Some(node) = self.node_starting_line(line) else {
			return false;
		};
		let idx = node.index();
		let has_children = unsafe { (*self.registry.edges[idx].get()).first_child.is_some() };
		has_children && self.folds.is_folded(node)
	}

	fn read_line_bytes(&self, line: DocLine) -> Result<Vec<u8>, String> {
		match self.line_byte_source {
			LineByteSource::Sparse => read_line_bytes_sparse(self.registry, self.root, line, false),
			LineByteSource::SumTree(tree) => {
				read_line_bytes_from_sum_tree(self.registry, self.root, tree, line, false)
			}
		}
	}

	fn line_row_count(&self, line: DocLine) -> u32 {
		if !self.wrap_enabled {
			return 1;
		}
		self.line_wrap(line).starts.len() as u32
	}

	fn line_wrap(&self, line: DocLine) -> WrappedLine {
		if let Some((_, cached)) = self
			.wrap_cache
			.borrow()
			.iter()
			.find(|(cached_line, _)| *cached_line == line)
		{
			return cached.clone();
		}
		let wrapped = if !self.wrap_enabled {
			compute_line_wrap(b"", 0)
		} else if self.is_folded_visible_line(line) {
			compute_line_wrap(b"...", self.text_width)
		} else {
			self.read_line_bytes(line)
				.map(|bytes| compute_line_wrap(&bytes, self.text_width))
				.unwrap_or_else(|_| compute_line_wrap(b"", self.text_width))
		};
		let mut cache = self.wrap_cache.borrow_mut();
		if cache.len() == WRAP_CACHE_CAPACITY {
			cache.pop_front();
		}
		cache.push_back((line, wrapped.clone()));
		wrapped
	}

	fn line_row_starts(&self, line: DocLine) -> Rc<[DocDisplayCol]> {
		self.line_wrap(line).starts
	}

	fn row_start_col(&self, line: DocLine, row_offset: DisplayRow) -> DocDisplayCol {
		self.line_row_starts(line)
			.get(row_offset.get() as usize)
			.copied()
			.unwrap_or(DocDisplayCol::ZERO)
	}

	fn cursor_row_and_pane_col(&self, line: DocLine, col: DocDisplayCol) -> (u32, PaneLocalCol) {
		if !self.wrap_enabled || self.text_width == 0 || self.is_folded_visible_line(line) {
			return (0, PaneLocalCol::new(col.get()));
		}

		let wrapped = self.line_wrap(line);
		let line_end = wrapped.visual_width;
		let clamped_col = col.get().min(line_end);
		let starts = wrapped.starts;
		let row = starts
			.partition_point(|start| start.get() <= clamped_col)
			.saturating_sub(1);
		let start_col = starts[row].get();
		let pane_col = clamped_col
			.saturating_sub(start_col)
			.min(u32::from(self.text_width).saturating_sub(1));
		(row as u32, PaneLocalCol::new(pane_col))
	}

	fn next_visible_line(&self, line: DocLine) -> Option<DocLine> {
		let line = self.visible_line(line);
		if !self.folds.has_folds() {
			return (line < self.max_doc_line).then(|| line + DocLineCount::new(1));
		}
		let current_visual = self.visual_index(line);
		let next = self.line_for_visual_index(current_visual + VisualRowCount::new(1));
		(next != line).then_some(next)
	}

	fn previous_visible_line(&self, line: DocLine) -> Option<DocLine> {
		let line = self.visible_line(line);
		let current_visual = self.visual_index(line);
		if current_visual == DocVisualRow::ZERO {
			return None;
		}
		let prev = self.line_for_visual_index(current_visual - VisualRowCount::new(1));
		(prev != line).then_some(prev)
	}

	fn normalize_anchor(&self, anchor: ViewportAnchor) -> ViewportAnchor {
		let line = self.visible_line(anchor.line);
		let max_row_offset = self.line_row_count(line).saturating_sub(1);
		ViewportAnchor {
			line,
			row_offset: if self.wrap_enabled {
				DisplayRow::new(anchor.row_offset.get().min(max_row_offset))
			} else {
				DisplayRow::ZERO
			},
		}
	}

	fn rows_from_anchor_to_position(
		&self,
		anchor: ViewportAnchor,
		target_line: DocLine,
		target_row: u32,
	) -> u32 {
		if anchor.line == target_line {
			return target_row.saturating_sub(anchor.row_offset.get());
		}

		let mut rows = self
			.line_row_count(anchor.line)
			.saturating_sub(anchor.row_offset.get());
		let mut line = anchor.line;
		while let Some(next_line) = self.next_visible_line(line) {
			line = next_line;
			if line == target_line {
				return rows.saturating_add(target_row);
			}
			rows = rows.saturating_add(self.line_row_count(line));
		}
		rows
	}

	fn scroll_viewport_anchor(&self, anchor: ViewportAnchor, delta_rows: i32) -> ViewportAnchor {
		let mut anchor = self.normalize_anchor(anchor);
		if !self.wrap_enabled {
			anchor.row_offset = DisplayRow::ZERO;
		}

		let mut remaining = delta_rows;
		while remaining > 0 {
			let line_rows = self.line_row_count(anchor.line);
			let remaining_in_line = line_rows
				.saturating_sub(anchor.row_offset.get())
				.saturating_sub(1);
			if remaining as u32 <= remaining_in_line {
				anchor.row_offset =
					DisplayRow::new(anchor.row_offset.get().saturating_add(remaining as u32));
				return anchor;
			}
			remaining -= remaining_in_line as i32 + 1;
			let Some(next_line) = self.next_visible_line(anchor.line) else {
				anchor.row_offset = DisplayRow::new(line_rows.saturating_sub(1));
				return anchor;
			};
			anchor.line = next_line;
			anchor.row_offset = DisplayRow::ZERO;
		}

		while remaining < 0 {
			if (-remaining) as u32 <= anchor.row_offset.get() {
				anchor.row_offset =
					DisplayRow::new(anchor.row_offset.get().saturating_sub((-remaining) as u32));
				return anchor;
			}
			remaining += anchor.row_offset.get() as i32 + 1;
			let Some(prev_line) = self.previous_visible_line(anchor.line) else {
				return ViewportAnchor {
					line: DocLine::ZERO,
					row_offset: DisplayRow::ZERO,
				};
			};
			anchor.line = prev_line;
			anchor.row_offset = DisplayRow::new(self.line_row_count(anchor.line).saturating_sub(1));
		}

		anchor
	}

	fn pan_viewport_anchor_to_keep_cursor_visible(
		&self,
		anchor: ViewportAnchor,
		cursor_line: DocLine,
		cursor_col: DocDisplayCol,
		viewport_rows: u32,
	) -> ViewportAnchor {
		let viewport_rows = viewport_rows.max(1);
		let anchor = self.normalize_anchor(anchor);
		let cursor_line = self.visible_line(cursor_line);
		let (cursor_row, _) = self.cursor_row_and_pane_col(cursor_line, cursor_col);
		let anchor_visual = self.visual_index(anchor.line);
		let cursor_visual = self.visual_index(cursor_line);
		if cursor_visual < anchor_visual
			|| (cursor_visual == anchor_visual && cursor_row < anchor.row_offset.get())
		{
			return ViewportAnchor {
				line: cursor_line,
				row_offset: if self.wrap_enabled {
					DisplayRow::new(cursor_row)
				} else {
					DisplayRow::ZERO
				},
			};
		}

		let cursor_offset = self.rows_from_anchor_to_position(anchor, cursor_line, cursor_row);
		if cursor_offset < viewport_rows {
			anchor
		} else {
			self.scroll_viewport_anchor(
				anchor,
				cursor_offset.saturating_sub(viewport_rows.saturating_sub(1)) as i32,
			)
		}
	}

	fn clamp_cursor_to_viewport_anchor(
		&self,
		anchor: ViewportAnchor,
		cursor_line: DocLine,
		cursor_col: DocDisplayCol,
		viewport_rows: u32,
	) -> (DocLine, DocDisplayCol) {
		let viewport_rows = viewport_rows.max(1);
		let anchor = self.normalize_anchor(anchor);
		let cursor_line = self.visible_line(cursor_line);
		let (cursor_row, _) = self.cursor_row_and_pane_col(cursor_line, cursor_col);
		let anchor_visual = self.visual_index(anchor.line);
		let cursor_visual = self.visual_index(cursor_line);
		if cursor_visual < anchor_visual
			|| (cursor_visual == anchor_visual && cursor_row < anchor.row_offset.get())
		{
			return (
				anchor.line,
				self.row_start_col(anchor.line, anchor.row_offset),
			);
		}

		let cursor_offset = self.rows_from_anchor_to_position(anchor, cursor_line, cursor_row);
		if cursor_offset < viewport_rows {
			(cursor_line, cursor_col)
		} else {
			let bottom =
				self.scroll_viewport_anchor(anchor, viewport_rows.saturating_sub(1) as i32);
			(
				bottom.line,
				self.row_start_col(bottom.line, bottom.row_offset),
			)
		}
	}

	fn cursor_view_metrics(
		&self,
		anchor: ViewportAnchor,
		cursor_line: DocLine,
		cursor_col: DocDisplayCol,
	) -> (PaneLocalRow, PaneLocalCol) {
		let anchor = self.normalize_anchor(anchor);
		let cursor_line = self.visible_line(cursor_line);
		let (cursor_row, pane_col) = self.cursor_row_and_pane_col(cursor_line, cursor_col);
		let row = self.rows_from_anchor_to_position(anchor, cursor_line, cursor_row);
		(PaneLocalRow::new(row), pane_col)
	}

	fn collect_visible_rows(&self, anchor: ViewportAnchor, viewport_rows: u32) -> Vec<VisibleRow> {
		let mut rows = Vec::new();
		let viewport_rows = viewport_rows.max(1);
		let mut anchor = self.normalize_anchor(anchor);
		loop {
			let row_starts = self.line_row_starts(anchor.line);
			let line_rows = row_starts.len() as u32;
			for row_offset in anchor.row_offset.get()..line_rows {
				rows.push(VisibleRow {
					line: anchor.line,
					start_col: row_starts[row_offset as usize],
				});
				if rows.len() as u32 >= viewport_rows {
					return rows;
				}
			}
			let Some(next_line) = self.next_visible_line(anchor.line) else {
				return rows;
			};
			anchor.line = next_line;
			anchor.row_offset = DisplayRow::ZERO;
		}
	}
}

pub fn scroll_viewport_anchor<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	delta_rows: i32,
	max_doc_line: DocLine,
	wrap_enabled: bool,
	text_width: u16,
) -> ViewportAnchor {
	LayoutContext::sparse(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
	)
	.scroll_viewport_anchor(anchor, delta_rows)
}

pub fn pan_viewport_anchor_to_keep_cursor_visible<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	cursor_line: DocLine,
	cursor_col: DocDisplayCol,
	max_doc_line: DocLine,
	viewport_rows: u32,
	wrap_enabled: bool,
	text_width: u16,
) -> ViewportAnchor {
	LayoutContext::sparse(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
	)
	.pan_viewport_anchor_to_keep_cursor_visible(anchor, cursor_line, cursor_col, viewport_rows)
}

pub fn clamp_cursor_to_viewport_anchor<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	cursor_line: DocLine,
	cursor_col: DocDisplayCol,
	max_doc_line: DocLine,
	viewport_rows: u32,
	wrap_enabled: bool,
	text_width: u16,
) -> (DocLine, DocDisplayCol) {
	LayoutContext::sparse(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
	)
	.clamp_cursor_to_viewport_anchor(anchor, cursor_line, cursor_col, viewport_rows)
}

pub fn cursor_view_metrics<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	cursor_line: DocLine,
	cursor_col: DocDisplayCol,
	max_doc_line: DocLine,
	wrap_enabled: bool,
	text_width: u16,
) -> (PaneLocalRow, PaneLocalCol) {
	LayoutContext::sparse(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
	)
	.cursor_view_metrics(anchor, cursor_line, cursor_col)
}

pub fn collect_visible_rows<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	max_doc_line: DocLine,
	viewport_rows: u32,
	wrap_enabled: bool,
	text_width: u16,
) -> Vec<VisibleRow> {
	LayoutContext::sparse(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
	)
	.collect_visible_rows(anchor, viewport_rows)
}

pub(crate) fn scroll_viewport_anchor_with_sum_tree<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	delta_rows: i32,
	max_doc_line: DocLine,
	wrap_enabled: bool,
	text_width: u16,
	tree: &SumTree,
) -> ViewportAnchor {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
	)
	.scroll_viewport_anchor(anchor, delta_rows)
}

pub(crate) fn pan_viewport_anchor_to_keep_cursor_visible_with_sum_tree<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	cursor_line: DocLine,
	cursor_col: DocDisplayCol,
	max_doc_line: DocLine,
	viewport_rows: u32,
	wrap_enabled: bool,
	text_width: u16,
	tree: &SumTree,
) -> ViewportAnchor {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
	)
	.pan_viewport_anchor_to_keep_cursor_visible(anchor, cursor_line, cursor_col, viewport_rows)
}

pub(crate) fn clamp_cursor_to_viewport_anchor_with_sum_tree<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	cursor_line: DocLine,
	cursor_col: DocDisplayCol,
	max_doc_line: DocLine,
	viewport_rows: u32,
	wrap_enabled: bool,
	text_width: u16,
	tree: &SumTree,
) -> (DocLine, DocDisplayCol) {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
	)
	.clamp_cursor_to_viewport_anchor(anchor, cursor_line, cursor_col, viewport_rows)
}

pub(crate) fn cursor_view_metrics_with_sum_tree<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	cursor_line: DocLine,
	cursor_col: DocDisplayCol,
	max_doc_line: DocLine,
	wrap_enabled: bool,
	text_width: u16,
	tree: &SumTree,
) -> (PaneLocalRow, PaneLocalCol) {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
	)
	.cursor_view_metrics(anchor, cursor_line, cursor_col)
}

pub(crate) fn collect_visible_rows_with_sum_tree<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	anchor: ViewportAnchor,
	max_doc_line: DocLine,
	viewport_rows: u32,
	wrap_enabled: bool,
	text_width: u16,
	tree: &SumTree,
) -> Vec<VisibleRow> {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
	)
	.collect_visible_rows(anchor, viewport_rows)
}

#[cfg(test)]
mod tests {
	use super::{
		GutterNumbering, ViewportAnchor, VisibleRow, clamp_cursor_to_viewport_anchor,
		collect_visible_rows, cursor_view_metrics, pan_viewport_anchor_to_keep_cursor_visible,
		viewport_geometry, viewport_geometry_for_viewport,
		viewport_geometry_for_viewport_with_numbering, viewport_max_line_on_screen,
		wrap_row_starts,
	};
	use crate::core::{DisplayRow, DocDisplayCol, DocLine, PaneLocalCol, PaneLocalRow};
	use crate::ecs::UastRegistry;
	use crate::svp::pointer::SvpPointer;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;
	use std::time::{SystemTime, UNIX_EPOCH};

	fn build_document(text: &str) -> (UastRegistry, crate::ecs::NodeId) {
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(2).expect("OOM");
		let newlines = text.bytes().filter(|&b| b == b'\n').count() as u64;
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: text.len() as u64,
				newlines,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: text.len() as u64,
				newlines,
			},
		);
		chunk.append_local_child(root, leaf);
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some(text.as_bytes().to_vec());
		}
		drop(chunk);
		(registry, root)
	}

	fn temp_test_path(name: &str) -> std::path::PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("time should move forward")
			.as_nanos();
		std::env::temp_dir().join(format!("baryon-{}-{}-{}", name, std::process::id(), nanos))
	}

	fn build_source_backed_line_document(
		line_count: u32,
	) -> (UastRegistry, crate::ecs::NodeId, std::path::PathBuf) {
		let path = temp_test_path("layout-source-backed");
		let total_bytes = line_count
			.saturating_sub(1)
			.saturating_mul(2)
			.saturating_add(1);
		let registry = UastRegistry::new(line_count + 4);
		let mut chunk = registry.reserve_chunk(line_count + 1).expect("OOM");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: u64::from(total_bytes),
				newlines: u64::from(line_count.saturating_sub(1)),
			},
		);

		let mut bytes = Vec::with_capacity(total_bytes as usize);
		let mut byte_offset = 0u64;
		for idx in 0..line_count {
			let part: &[u8] = if idx + 1 == line_count { b"x" } else { b"x\n" };
			bytes.extend_from_slice(part);
			let byte_length = part.len() as u32;
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba: byte_offset / 512,
					byte_length,
					device_id: 77,
					head_trim: (byte_offset % 512) as u16,
				}),
				SpanMetrics {
					byte_length: u64::from(byte_length),
					newlines: u64::from(part.ends_with(b"\n")),
				},
			);
			chunk.append_local_child(root, leaf);
			byte_offset += byte_length as u64;
		}

		std::fs::write(&path, bytes).expect("write temp file");
		registry
			.register_device_path(77, &path)
			.expect("register backing");
		drop(chunk);
		(registry, root, path)
	}

	#[test]
	fn viewport_geometry_matches_draw_constraints() {
		let geometry = viewport_geometry(999, 120, true);
		assert_eq!(geometry.gutter_width, 6);
		assert_eq!(geometry.minimap_width, 14);
		assert_eq!(geometry.separator_width, 1);
		assert_eq!(geometry.text_width, 99);
	}

	#[test]
	fn wrap_rows_prefer_word_boundaries_and_fall_back_for_long_words() {
		assert_eq!(
			wrap_row_starts(b"hello world", 8),
			vec![DocDisplayCol::ZERO, DocDisplayCol::new(6)]
		);
		assert_eq!(
			wrap_row_starts(b"hello world", 5),
			vec![DocDisplayCol::ZERO, DocDisplayCol::new(6)]
		);
		assert_eq!(
			wrap_row_starts(b"hi abcdefgh", 4),
			vec![
				DocDisplayCol::ZERO,
				DocDisplayCol::new(3),
				DocDisplayCol::new(7),
			]
		);
	}

	#[test]
	fn wrap_rows_treat_tabs_as_whitespace_with_visual_width() {
		assert_eq!(
			wrap_row_starts(b"one\ttwo", 5),
			vec![DocDisplayCol::ZERO, DocDisplayCol::new(4)]
		);
	}

	#[test]
	fn cursor_metrics_follow_word_boundary_row_starts() {
		let (registry, root) = build_document("hello world\n");
		let (row, col) = cursor_view_metrics(
			&registry,
			root,
			&crate::uast::NoFolds,
			ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::ZERO,
			},
			DocLine::ZERO,
			DocDisplayCol::new(8),
			DocLine::new(1),
			true,
			5,
		);

		assert_eq!(row, PaneLocalRow::new(1));
		assert_eq!(col, PaneLocalCol::new(2));
	}

	#[test]
	fn viewport_geometry_shrinks_gutter_when_returning_to_top_of_huge_file() {
		let max_doc_line = DocLine::new(208_999_999);
		let top_geometry =
			viewport_geometry_for_viewport(DocLine::ZERO, 25, max_doc_line, 120, true);
		let bottom_geometry = viewport_geometry_for_viewport(
			max_doc_line.saturating_sub(24),
			25,
			max_doc_line,
			120,
			true,
		);

		assert_eq!(
			viewport_max_line_on_screen(DocLine::ZERO, 25, max_doc_line),
			DocLine::new(24)
		);
		assert_eq!(top_geometry.gutter_width, 4);
		assert_eq!(top_geometry.text_width, 101);
		assert_eq!(bottom_geometry.gutter_width, 11);
		assert_eq!(bottom_geometry.text_width, 94);
	}

	#[test]
	fn pure_relative_numbering_sizes_gutter_from_viewport_distance() {
		let max_doc_line = DocLine::new(208_999_999);
		let viewport_start = max_doc_line.saturating_sub(24);
		let relative = viewport_geometry_for_viewport_with_numbering(
			viewport_start,
			25,
			max_doc_line,
			120,
			false,
			Some(GutterNumbering::Relative),
		);
		let hybrid = viewport_geometry_for_viewport_with_numbering(
			viewport_start,
			25,
			max_doc_line,
			120,
			false,
			Some(GutterNumbering::Hybrid),
		);

		assert_eq!(relative.gutter_width, 4);
		assert_eq!(hybrid.gutter_width, 11);
	}

	#[test]
	fn collect_visible_rows_can_start_mid_wrapped_line() {
		let (registry, root) = build_document("abcdefghij\nz\n");
		let rows = collect_visible_rows(
			&registry,
			root,
			&crate::uast::NoFolds,
			ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::new(1),
			},
			DocLine::new(registry.get_total_newlines(root)),
			4,
			true,
			4,
		);

		assert_eq!(
			rows,
			vec![
				VisibleRow {
					line: DocLine::ZERO,
					start_col: DocDisplayCol::new(4),
				},
				VisibleRow {
					line: DocLine::ZERO,
					start_col: DocDisplayCol::new(8),
				},
				VisibleRow {
					line: DocLine::new(1),
					start_col: DocDisplayCol::ZERO,
				},
				VisibleRow {
					line: DocLine::new(2),
					start_col: DocDisplayCol::ZERO,
				},
			]
		);
	}

	#[test]
	fn cursor_metrics_and_clamp_respect_wrapped_offsets() {
		let (registry, root) = build_document("abcdefghij\nz\n");
		let max_doc_line = DocLine::new(registry.get_total_newlines(root));
		let anchor = ViewportAnchor {
			line: DocLine::ZERO,
			row_offset: DisplayRow::new(1),
		};

		let (cursor_row, screen_col) = cursor_view_metrics(
			&registry,
			root,
			&crate::uast::NoFolds,
			anchor,
			DocLine::ZERO,
			DocDisplayCol::new(9),
			max_doc_line,
			true,
			4,
		);
		assert_eq!(cursor_row, PaneLocalRow::new(1));
		assert_eq!(screen_col, PaneLocalCol::new(1));

		let clamped = clamp_cursor_to_viewport_anchor(
			&registry,
			root,
			&crate::uast::NoFolds,
			anchor,
			DocLine::ZERO,
			DocDisplayCol::ZERO,
			max_doc_line,
			2,
			true,
			4,
		);
		assert_eq!(clamped, (DocLine::ZERO, DocDisplayCol::new(4)));

		let panned = pan_viewport_anchor_to_keep_cursor_visible(
			&registry,
			root,
			&crate::uast::NoFolds,
			ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::ZERO,
			},
			DocLine::ZERO,
			DocDisplayCol::new(9),
			max_doc_line,
			2,
			true,
			4,
		);
		assert_eq!(
			panned,
			ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::new(1),
			}
		);
	}

	#[test]
	fn wrapped_tail_queries_only_inflate_visible_tail_chunks() {
		let viewport_rows = 20u32;
		let (registry, root, path) = build_source_backed_line_document(256);
		let max_doc_line = DocLine::new(registry.get_total_newlines(root));
		let cursor_line = max_doc_line;
		let expected_top = max_doc_line.saturating_sub(u64::from(viewport_rows.saturating_sub(1)));
		let geometry =
			viewport_geometry_for_viewport(expected_top, viewport_rows, max_doc_line, 120, true);
		let anchor = pan_viewport_anchor_to_keep_cursor_visible(
			&registry,
			root,
			&crate::uast::NoFolds,
			ViewportAnchor {
				line: expected_top,
				row_offset: DisplayRow::ZERO,
			},
			cursor_line,
			DocDisplayCol::ZERO,
			max_doc_line,
			viewport_rows,
			true,
			geometry.text_width,
		);
		let rows = collect_visible_rows(
			&registry,
			root,
			&crate::uast::NoFolds,
			anchor,
			max_doc_line,
			viewport_rows,
			true,
			geometry.text_width,
		);
		let (cursor_row, screen_col) = cursor_view_metrics(
			&registry,
			root,
			&crate::uast::NoFolds,
			anchor,
			cursor_line,
			DocDisplayCol::ZERO,
			max_doc_line,
			true,
			geometry.text_width,
		);

		assert_eq!(rows.len(), viewport_rows as usize);
		assert_eq!(rows.first().map(|row| row.line), Some(expected_top));
		assert_eq!(rows.last().map(|row| row.line), Some(cursor_line));
		assert_eq!(
			cursor_row,
			PaneLocalRow::new(viewport_rows.saturating_sub(1))
		);
		assert_eq!(screen_col, PaneLocalCol::ZERO);

		let _ = std::fs::remove_file(path);
	}
}
