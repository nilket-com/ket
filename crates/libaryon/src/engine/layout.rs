use super::folding::{
	doc_line_for_visual_index, snap_line_to_visible_boundary, visual_line_index_for_doc_line,
};
use super::index::SumTree;
use super::support::{
	line_start_byte_from_sum_tree, read_line_bytes_from_sum_tree, read_line_bytes_sparse,
};
use super::wrap_index::{
	ByteLookup, CellLookup, ColLookup, LineRead, LineSource, RowCount, RowLookup, WrapIndex,
	WrapKey,
};
use crate::core::{
	DisplayRow, DocByte, DocDisplayCol, DocLine, DocLineCount, DocVisualRow, PaneLocalCol,
	PaneLocalRow, StateId, TAB_SIZE, TopologyRev, VisualRowCount,
};
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::projection::NodeLineLookup;
use crate::uast::{FoldPolicy, UastProjection};
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

const MINIMAP_MIN_SCREEN_WIDTH: u16 = 40;
const MINIMAP_RESERVED_COLUMNS: u16 = 24;
const MINIMAP_MAX_WIDTH: u16 = 14;
const WRAP_CACHE_CAPACITY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibleRow {
	pub line: DocLine,
	/// Absolute document column of the row's start.
	pub start_col: DocDisplayCol,
	/// For rows of a line laid out through the wrap index (ADR 0009): the
	/// absolute byte of the row's first cell, `None` for a row of trailing
	/// whitespace...
	pub start_byte: Option<DocByte>,
	/// ...and its absolute display offset, which does not saturate.
	pub start_offset: Option<DocDisplayCol>,
}

impl VisibleRow {
	/// A row of a line laid out whole, from its first byte.
	pub fn whole(line: DocLine, start_col: DocDisplayCol) -> Self {
		Self {
			line,
			start_col,
			start_byte: None,
			start_offset: None,
		}
	}
}

/// Where a pane's tokens begin when its view starts inside a line laid out
/// through the wrap index (ADR 0009). The frontend lays that line out from
/// `start_offset`, not from column 0; a row's position in the window is its
/// `start_offset` minus this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowWindow {
	pub line: DocLine,
	pub start_byte: DocByte,
	pub start_offset: DocDisplayCol,
	/// `start_offset` modulo the tab size: the phase of the first tab stop.
	pub tab_phase: u8,
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
	visual_width: u64,
}

fn push_word_wraps(
	starts: &mut Vec<DocDisplayCol>,
	row_start: &mut u64,
	word_start: u64,
	word_end: u64,
	width: u64,
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

/// A `u64` row count in layout's `u32` rows.
fn saturate(rows: u64) -> u32 {
	u32::try_from(rows).unwrap_or(u32::MAX)
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
	let width = u64::from(text_width);
	let mut visual_col = 0u64;
	let mut row_start = 0u64;
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

/// Whole-line row starts and visual width, the reference `WrapScanner` must
/// reproduce.
#[cfg(test)]
pub(crate) fn wrap_starts_for_test(line: &[u8], text_width: u16) -> (Vec<u64>, u64) {
	let wrapped = compute_line_wrap(line, text_width);
	let starts = wrapped.starts.iter().map(|start| start.get()).collect();
	(starts, wrapped.visual_width)
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

/// A wrapped line at least this long goes through the wrap index (ADR 0009).
pub(crate) const WRAP_INDEX_MIN_LINE_BYTES: u64 =
	crate::svp::ingest::COLD_INGEST_EXTENT_SIZE as u64;

/// What layout could not finish this frame: nodes to hydrate, and whether a
/// later frame should continue scanning. Render acts on it; other callers may
/// drop it, since the next render asks again.
#[derive(Debug, Default)]
pub(crate) struct LayoutDemand {
	pub(crate) pending: Vec<NodeId>,
	pub(crate) more: bool,
}

impl LayoutDemand {
	pub(crate) fn note(&mut self, pending: Option<NodeId>) {
		self.more = true;
		if let Some(node) = pending
			&& !self.pending.contains(&node)
		{
			self.pending.push(node);
		}
	}
}

/// Where a scroll moves the cursor. `byte` is the row's start when the row is
/// on an indexed line: the caret is then placed from it without reading the
/// line from its start (ADR 0009).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CursorTarget {
	pub(crate) line: DocLine,
	pub(crate) col: DocDisplayCol,
	pub(crate) byte: Option<DocByte>,
}

/// A view's wrap index and what its lookups are keyed by.
#[derive(Clone, Copy)]
pub(crate) struct WrapLayout<'a> {
	pub(crate) index: &'a RefCell<WrapIndex>,
	pub(crate) state: StateId,
	pub(crate) topology: TopologyRev,
	pub(crate) demand: &'a RefCell<LayoutDemand>,
	/// [`WRAP_INDEX_MIN_LINE_BYTES`] outside tests.
	pub(crate) min_line_bytes: u64,
}

impl<'a> WrapLayout<'a> {
	pub(crate) fn new(
		index: &'a RefCell<WrapIndex>,
		state: StateId,
		topology: TopologyRev,
		demand: &'a RefCell<LayoutDemand>,
	) -> Self {
		Self {
			index,
			state,
			topology,
			demand,
			min_line_bytes: WRAP_INDEX_MIN_LINE_BYTES,
		}
	}
}

/// What a caret lookup through the index found: an indexed line's answer, or
/// that the line is indexed but not reachable this frame (ADR 0010).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaretLookup<T> {
	Known(T),
	/// The demand asks for more; the caller keeps its target and must not
	/// read the line from its start instead.
	Pending,
}

/// What carried work is valid for: the document, its state and topology, and
/// the width the index keys the line at (ADR 0010).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkKey {
	pub(crate) root: NodeId,
	pub(crate) state: StateId,
	pub(crate) topology: TopologyRev,
	pub(crate) key_width: u16,
}

/// Work a command could not finish and render carries on (ADR 0010): nodes to
/// hydrate, whether to refresh again, and a line to scan to its end. It is a
/// request for data, never a caret intent: the caret moves only on a later
/// command. Work for another key is discarded.
#[derive(Debug, Default)]
pub(crate) struct CarriedWork {
	key: Option<WorkKey>,
	demand: LayoutDemand,
	scan_to_end: Option<DocLine>,
}

impl CarriedWork {
	fn for_key(&mut self, key: WorkKey) -> &mut Self {
		if self.key != Some(key) {
			*self = Self {
				key: Some(key),
				..Self::default()
			};
		}
		self
	}

	/// Carry what a command's lookups asked for.
	pub(crate) fn carry(&mut self, key: WorkKey, demand: LayoutDemand) {
		let work = self.for_key(key);
		work.demand.more |= demand.more;
		for node in demand.pending {
			if !work.demand.pending.contains(&node) {
				work.demand.pending.push(node);
			}
		}
	}

	pub(crate) fn scan_to_end(&mut self, key: WorkKey, line: DocLine) {
		self.for_key(key).scan_to_end = Some(line);
	}

	/// Everything carried for `key`; work for any other key is dropped.
	pub(crate) fn take(&mut self, key: WorkKey) -> (LayoutDemand, Option<DocLine>) {
		let work = std::mem::take(self);
		if work.key == Some(key) {
			(work.demand, work.scan_to_end)
		} else {
			(LayoutDemand::default(), None)
		}
	}

	#[cfg(test)]
	pub(crate) fn pending_scan(&self) -> Option<DocLine> {
		self.scan_to_end
	}
}

/// Carry on the work a command left for `index`'s document (ADR 0010):
/// advance a requested line scan by this frame's budget, asking again while
/// it is unfinished, and return the nodes and refresh it asked for. Work for
/// another key is dropped.
pub(crate) fn carry_on(
	carried: &RefCell<CarriedWork>,
	index: CaretIndex<'_>,
	registry: &UastRegistry,
	root: NodeId,
) -> LayoutDemand {
	let key = index.work_key(root);
	let (demand, scan_line) = carried.borrow_mut().take(key);
	if let Some(line) = scan_line
		&& let Some(CaretLookup::Pending) = index.scan_to_end(registry, root, line)
	{
		carried.borrow_mut().scan_to_end(key, line);
	}
	demand
}

/// The wrap index as caret placement uses it (ADR 0010). It answers only for
/// lines on the indexed path: `None` from a lookup means the line takes
/// today's code, byte for byte.
#[derive(Clone, Copy)]
pub(crate) struct CaretIndex<'a> {
	wrap: WrapLayout<'a>,
	key_width: u16,
}

impl<'a> CaretIndex<'a> {
	/// With wrap on, `text_width` is the render's, so the caret shares its
	/// checkpoints. With wrap off the lookups still work, under a fixed width:
	/// cells do not depend on it.
	pub(crate) fn new(wrap: WrapLayout<'a>, wrap_enabled: bool, text_width: u16) -> Self {
		let key_width = if wrap_enabled && text_width > 0 {
			text_width
		} else {
			u16::MAX
		};
		Self { wrap, key_width }
	}

	fn key(&self) -> WrapKey {
		WrapKey {
			state: self.wrap.state,
			topology: self.wrap.topology,
			text_width: self.key_width,
		}
	}

	/// Ask render for `node`, or for another frame when `None` (ADR 0010).
	pub(crate) fn request(&self, node: Option<NodeId>) {
		self.wrap.demand.borrow_mut().note(node);
	}

	/// Grant a read outside the index from this frame's budget.
	pub(crate) fn reserve_read(&self, max: usize) -> usize {
		self.wrap.index.borrow_mut().reserve_read(max)
	}

	/// The key carried work for this index is valid under.
	pub(crate) fn work_key(&self, root: NodeId) -> WorkKey {
		WorkKey {
			root,
			state: self.wrap.state,
			topology: self.wrap.topology,
			key_width: self.key_width,
		}
	}

	/// The end of an indexed line, known only once its scan has finished: the
	/// caret then moves there without a pending intent (ADR 0010).
	pub(crate) fn line_end(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		line: DocLine,
	) -> Option<CaretLookup<(DocByte, DocDisplayCol)>> {
		match self.scan_to_end(registry, root, line)? {
			CaretLookup::Known(()) => {
				self.byte_for_line_col(registry, root, line, DocDisplayCol::new(u64::MAX))
			}
			CaretLookup::Pending => Some(CaretLookup::Pending),
		}
	}

	/// Advance an indexed line's scan by this frame's budget. Known once the
	/// scan has reached the line's end.
	pub(crate) fn scan_to_end(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		line: DocLine,
	) -> Option<CaretLookup<()>> {
		let (start, end) = match self.indexed_line(registry, root, line)? {
			CaretLookup::Known(range) => range,
			CaretLookup::Pending => return Some(CaretLookup::Pending),
		};
		let source = RegistryLine {
			registry,
			root,
			start: start.get(),
			end: end.get(),
		};
		let count = self
			.wrap
			.index
			.borrow_mut()
			.row_count(self.key(), line, &source);
		Some(match count {
			RowCount::Exact(_) => CaretLookup::Known(()),
			RowCount::AtLeast { pending, .. } => self.pending(pending),
		})
	}

	fn pending<T>(&self, node: Option<NodeId>) -> CaretLookup<T> {
		self.wrap.demand.borrow_mut().note(node);
		CaretLookup::Pending
	}

	/// The line and column of `byte`, when its line is indexed. The line comes
	/// from metrics and the one leaf holding `byte`.
	pub(crate) fn line_col_for_byte(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		byte: DocByte,
	) -> Option<CaretLookup<(DocLine, DocDisplayCol)>> {
		Some(match self.cell_at_byte(registry, root, byte)? {
			CaretLookup::Known((line, _, col)) => CaretLookup::Known((line, col)),
			CaretLookup::Pending => CaretLookup::Pending,
		})
	}

	/// The line of `byte` and the start byte and column of the cell holding
	/// it, when its line is indexed.
	pub(crate) fn cell_at_byte(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		byte: DocByte,
	) -> Option<CaretLookup<(DocLine, DocByte, DocDisplayCol)>> {
		let line = match self.line_of_byte(registry, root, byte) {
			CaretLookup::Known(line) => line,
			CaretLookup::Pending => return Some(CaretLookup::Pending),
		};
		let (start, end) = match self.indexed_line(registry, root, line)? {
			CaretLookup::Known(range) => range,
			CaretLookup::Pending => return Some(CaretLookup::Pending),
		};
		let offset = byte
			.get()
			.saturating_sub(start.get())
			.min(end.get() - start.get());
		let source = RegistryLine {
			registry,
			root,
			start: start.get(),
			end: end.get(),
		};
		let lookup = self
			.wrap
			.index
			.borrow_mut()
			.cell_for_byte(self.key(), line, offset, &source);
		Some(match lookup {
			CellLookup::Known(cell) => CaretLookup::Known((
				line,
				DocByte::new(start.get().saturating_add(cell.byte)),
				cell.col,
			)),
			CellLookup::Unknown { pending } => self.pending(pending),
		})
	}

	/// Where column `col` of `line` resolves, and the column there, when the
	/// line is indexed.
	pub(crate) fn byte_for_line_col(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		line: DocLine,
		col: DocDisplayCol,
	) -> Option<CaretLookup<(DocByte, DocDisplayCol)>> {
		let (start, end) = match self.indexed_line(registry, root, line)? {
			CaretLookup::Known(range) => range,
			CaretLookup::Pending => return Some(CaretLookup::Pending),
		};
		let source = RegistryLine {
			registry,
			root,
			start: start.get(),
			end: end.get(),
		};
		let lookup = self
			.wrap
			.index
			.borrow_mut()
			.cell_for_col(self.key(), line, col, &source);
		Some(match lookup {
			CellLookup::Known(cell) => CaretLookup::Known((
				DocByte::new(start.get().saturating_add(cell.byte)),
				cell.col,
			)),
			CellLookup::Unknown { pending } => self.pending(pending),
		})
	}

	fn line_of_byte(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		byte: DocByte,
	) -> CaretLookup<DocLine> {
		match crate::engine::document::cursor::line_from_doc_byte_raw(registry, root, byte) {
			Ok(line) => CaretLookup::Known(line),
			Err(node) => self.pending(node),
		}
	}

	/// `line`'s absolute bytes, newline excluded, if it is on the indexed path:
	/// at least [`WrapLayout::min_line_bytes`] long, or not all resident.
	pub(crate) fn indexed_line(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		line: DocLine,
	) -> Option<CaretLookup<(DocByte, DocByte)>> {
		let start = match self.line_start(registry, root, line) {
			CaretLookup::Known(start) => start,
			CaretLookup::Pending => return Some(CaretLookup::Pending),
		};
		let end = if line.get() >= registry.get_total_newlines(root) {
			DocByte::new(registry.get_total_bytes(root))
		} else {
			match self.line_start(registry, root, line + DocLineCount::new(1)) {
				// The newline ending this line is not part of it.
				CaretLookup::Known(next) => {
					DocByte::new(next.get().saturating_sub(1).max(start.get()))
				}
				CaretLookup::Pending => return Some(CaretLookup::Pending),
			}
		};
		let long = end.get() - start.get() >= self.wrap.min_line_bytes;
		let resident = || {
			registry
				.for_each_loaded_slice_fragment(root, start, end, |_| {})
				.is_ok()
		};
		(long || !resident()).then_some(CaretLookup::Known((start, end)))
	}

	fn line_start(
		&self,
		registry: &UastRegistry,
		root: NodeId,
		line: DocLine,
	) -> CaretLookup<DocByte> {
		match registry.find_node_at_line_col_raw(root, line, DocDisplayCol::ZERO) {
			NodeLineLookup::Ready(target) => CaretLookup::Known(registry.doc_byte_for_node_offset(
				root,
				target.node_id,
				target.node_byte,
			)),
			NodeLineLookup::Pending { required_node, .. } => self.pending(required_node),
		}
	}
}

/// How a line wraps: whole, from its resident bytes, or through the index.
#[derive(Clone)]
enum LineLayout {
	Whole(WrappedLine),
	Indexed { start: DocByte, end: DocByte },
}

/// A line's bytes, read for the wrap index from resident leaves only.
struct RegistryLine<'a> {
	registry: &'a UastRegistry,
	root: NodeId,
	start: u64,
	end: u64,
}

impl LineSource for RegistryLine<'_> {
	fn read(&self, offset: u64, max: usize) -> LineRead {
		let from = self.start.saturating_add(offset);
		if from >= self.end {
			return LineRead::End;
		}
		let to = self.end.min(from.saturating_add(max as u64));
		let mut bytes = Vec::with_capacity((to - from) as usize);
		let read = self.registry.for_each_loaded_slice_fragment(
			self.root,
			DocByte::new(from),
			DocByte::new(to),
			|fragment| bytes.extend_from_slice(fragment),
		);
		match read {
			// Fragments arrive in order, so what came before a cold leaf is valid.
			Ok(()) | Err(_) if !bytes.is_empty() => LineRead::Bytes(bytes),
			Ok(()) => LineRead::End,
			Err(_) => {
				let at = from.saturating_add(bytes.len() as u64);
				LineRead::Pending(
					self.registry
						.find_node_at_doc_byte_raw(self.root, DocByte::new(at))
						.node_id,
				)
			}
		}
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
	wrap_cache: RefCell<VecDeque<(DocLine, LineLayout)>>,
	wrap: Option<WrapLayout<'a>>,
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
			wrap: None,
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn with_sum_tree(
		registry: &'a UastRegistry,
		root: NodeId,
		folds: &'a F,
		max_doc_line: DocLine,
		wrap_enabled: bool,
		text_width: u16,
		tree: &'a SumTree,
		wrap: Option<WrapLayout<'a>>,
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
			wrap,
		}
	}

	#[cfg(test)]
	fn with_wrap(mut self, wrap: WrapLayout<'a>) -> Self {
		self.wrap = Some(wrap);
		self
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
		self.line_rows(line).0
	}

	/// A line's wrapped rows as far as known, and whether that count is exact.
	/// An inexact count is a scanned prefix: the line has at least that many.
	fn line_rows(&self, line: DocLine) -> (u32, bool) {
		if !self.wrap_enabled {
			return (1, true);
		}
		match self.line_layout(line) {
			LineLayout::Whole(wrapped) => (wrapped.starts.len() as u32, true),
			LineLayout::Indexed { start, end } => {
				let wrap = self.wrap.expect("an indexed line has a wrap index");
				let source = self.line_source(start, end);
				match wrap
					.index
					.borrow_mut()
					.row_count(self.wrap_key(wrap), line, &source)
				{
					RowCount::Exact(rows) => (saturate(rows), true),
					RowCount::AtLeast { rows, pending } => {
						wrap.demand.borrow_mut().note(pending);
						(saturate(rows), false)
					}
				}
			}
		}
	}

	fn line_layout(&self, line: DocLine) -> LineLayout {
		if let Some((_, cached)) = self
			.wrap_cache
			.borrow()
			.iter()
			.find(|(cached_line, _)| *cached_line == line)
		{
			return cached.clone();
		}
		let layout = if !self.wrap_enabled {
			LineLayout::Whole(compute_line_wrap(b"", 0))
		} else if self.is_folded_visible_line(line) {
			LineLayout::Whole(compute_line_wrap(b"...", self.text_width))
		} else {
			self.unfolded_line_layout(line)
		};
		let mut cache = self.wrap_cache.borrow_mut();
		if cache.len() == WRAP_CACHE_CAPACITY {
			cache.pop_front();
		}
		cache.push_back((line, layout.clone()));
		layout
	}

	/// A line past the index threshold, or one whose bytes are not all
	/// resident, wraps through the index; any other wraps whole, as before.
	fn unfolded_line_layout(&self, line: DocLine) -> LineLayout {
		let range = self.wrap.and_then(|wrap| self.line_byte_range(line, wrap));
		if let (Some(wrap), Some((start, end))) = (self.wrap, range)
			&& end.get().saturating_sub(start.get()) >= wrap.min_line_bytes
		{
			return LineLayout::Indexed { start, end };
		}
		match self.read_line_bytes(line) {
			Ok(bytes) => LineLayout::Whole(compute_line_wrap(&bytes, self.text_width)),
			Err(_) => match range {
				Some((start, end)) => LineLayout::Indexed { start, end },
				None => LineLayout::Whole(compute_line_wrap(b"", self.text_width)),
			},
		}
	}

	/// A line's absolute bytes, newline excluded, found without reading them.
	fn line_byte_range(&self, line: DocLine, wrap: WrapLayout<'_>) -> Option<(DocByte, DocByte)> {
		let total = self.registry.get_total_bytes(self.root);
		let start = match self.line_byte_source {
			LineByteSource::SumTree(tree) => line_start_byte_from_sum_tree(tree, line),
			LineByteSource::Sparse => self.sparse_line_start(line, wrap)?,
		};
		let end = if line >= self.max_doc_line {
			DocByte::new(total)
		} else {
			let next = match self.line_byte_source {
				LineByteSource::SumTree(tree) => {
					line_start_byte_from_sum_tree(tree, line + DocLineCount::new(1))
				}
				LineByteSource::Sparse => {
					self.sparse_line_start(line + DocLineCount::new(1), wrap)?
				}
			};
			// The newline ending this line is not part of it.
			DocByte::new(next.get().saturating_sub(1).max(start.get()))
		};
		Some((start, end))
	}

	fn sparse_line_start(&self, line: DocLine, wrap: WrapLayout<'_>) -> Option<DocByte> {
		match self
			.registry
			.find_node_at_line_col_raw(self.root, line, DocDisplayCol::ZERO)
		{
			NodeLineLookup::Ready(target) => Some(self.registry.doc_byte_for_node_offset(
				self.root,
				target.node_id,
				target.node_byte,
			)),
			NodeLineLookup::Pending { required_node, .. } => {
				if required_node.is_some() {
					wrap.demand.borrow_mut().note(required_node);
				}
				None
			}
		}
	}

	fn line_source(&self, start: DocByte, end: DocByte) -> RegistryLine<'a> {
		RegistryLine {
			registry: self.registry,
			root: self.root,
			start: start.get(),
			end: end.get(),
		}
	}

	fn wrap_key(&self, wrap: WrapLayout<'_>) -> WrapKey {
		WrapKey {
			state: wrap.state,
			topology: wrap.topology,
			text_width: self.text_width,
		}
	}

	/// A row's start column, or `None` when the index cannot reach it this
	/// frame; the demand then asks for a continuation.
	#[cfg(test)]
	fn try_row_start_col(&self, line: DocLine, row_offset: DisplayRow) -> Option<DocDisplayCol> {
		self.try_visible_row(line, row_offset)
			.map(|row| row.start_col)
	}

	/// A row as published, or `None` when the index cannot reach its start this
	/// frame; the demand then asks for a continuation.
	fn try_visible_row(&self, line: DocLine, row_offset: DisplayRow) -> Option<VisibleRow> {
		match self.line_layout(line) {
			LineLayout::Whole(wrapped) => Some(VisibleRow::whole(
				line,
				wrapped
					.starts
					.get(row_offset.get() as usize)
					.copied()
					.unwrap_or(DocDisplayCol::ZERO),
			)),
			LineLayout::Indexed {
				start: line_start,
				end,
			} => {
				let wrap = self.wrap.expect("an indexed line has a wrap index");
				let source = self.line_source(line_start, end);
				let key = self.wrap_key(wrap);
				let lookup = wrap.index.borrow_mut().row_start(
					key,
					line,
					u64::from(row_offset.get()),
					&source,
				);
				match lookup {
					RowLookup::Known(start) => Some(VisibleRow {
						line,
						start_col: start.col,
						start_byte: start
							.byte
							.map(|byte| DocByte::new(line_start.get().saturating_add(byte))),
						start_offset: Some(start.col),
					}),
					RowLookup::PastEnd { .. } => Some(VisibleRow::whole(line, DocDisplayCol::ZERO)),
					// Settles on a later frame; the demand asks for it.
					RowLookup::Unknown { pending, .. } => {
						wrap.demand.borrow_mut().note(pending);
						None
					}
				}
			}
		}
	}

	/// The byte and display offset a window must start at to show `row`, a row
	/// of an indexed line; `None` for a row of a line laid out whole.
	fn window_origin(&self, row: VisibleRow) -> Option<(DocByte, DocDisplayCol)> {
		let start_offset = row.start_offset?;
		if let Some(start_byte) = row.start_byte {
			return Some((start_byte, start_offset));
		}
		// A row of trailing whitespace: the window starts where the run does.
		let LineLayout::Indexed {
			start: line_start, ..
		} = self.line_layout(row.line)
		else {
			return None;
		};
		let wrap = self.wrap?;
		let run = wrap
			.index
			.borrow()
			.trailing_whitespace_origin(self.wrap_key(wrap), row.line)?;
		Some((
			DocByte::new(line_start.get().saturating_add(run.byte)),
			run.col,
		))
	}

	/// Where the cursor sits. On an indexed line whose row the index cannot
	/// reach this frame this falls back to row 0, which is only fit for drawing
	/// the cursor for one frame; a caller that moves the view uses
	/// [`Self::try_cursor_row_and_pane_col`].
	fn cursor_row_and_pane_col(&self, line: DocLine, col: DocDisplayCol) -> (u32, PaneLocalCol) {
		self.try_cursor_row_and_pane_col(line, col)
			.unwrap_or_else(|| {
				let last_pane_col = u32::from(self.text_width).saturating_sub(1);
				(0, PaneLocalCol::new(saturate(col.get()).min(last_pane_col)))
			})
	}

	/// Where the cursor sits, or `None` when its row is on an indexed line the
	/// index cannot reach this frame; the demand then asks for a continuation.
	fn try_cursor_row_and_pane_col(
		&self,
		line: DocLine,
		col: DocDisplayCol,
	) -> Option<(u32, PaneLocalCol)> {
		if !self.wrap_enabled || self.text_width == 0 || self.is_folded_visible_line(line) {
			return Some((0, PaneLocalCol::new(saturate(col.get()))));
		}
		let last_pane_col = u32::from(self.text_width).saturating_sub(1);
		let wrapped = match self.line_layout(line) {
			LineLayout::Whole(wrapped) => wrapped,
			LineLayout::Indexed { start, end } => {
				let wrap = self.wrap.expect("an indexed line has a wrap index");
				let source = self.line_source(start, end);
				let key = self.wrap_key(wrap);
				let lookup = wrap.index.borrow_mut().row_for_col(key, line, col, &source);
				return match lookup {
					ColLookup::Known { row, start, col } => {
						let pane_col = col
							.get()
							.saturating_sub(start.col.get())
							.min(u64::from(last_pane_col));
						Some((saturate(row), PaneLocalCol::new(pane_col as u32)))
					}
					ColLookup::Unknown { pending } => {
						wrap.demand.borrow_mut().note(pending);
						None
					}
				};
			}
		};
		let line_end = wrapped.visual_width;
		let clamped_col = col.get().min(line_end);
		let starts = wrapped.starts;
		let row = starts
			.partition_point(|start| start.get() <= clamped_col)
			.saturating_sub(1);
		let start_col = starts[row].get();
		let pane_col = saturate(clamped_col.saturating_sub(start_col)).min(last_pane_col);
		Some((row as u32, PaneLocalCol::new(pane_col)))
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
		let (rows, exact) = self.line_rows(line);
		let max_row_offset = rows.saturating_sub(1);
		ViewportAnchor {
			line,
			// Past a scanned prefix the row may still exist: keep it until the
			// scan reaches it rather than pulling the view back.
			row_offset: if !self.wrap_enabled {
				DisplayRow::ZERO
			} else if exact {
				DisplayRow::new(anchor.row_offset.get().min(max_row_offset))
			} else {
				anchor.row_offset
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
			let (line_rows, exact) = self.line_rows(anchor.line);
			let remaining_in_line = line_rows
				.saturating_sub(anchor.row_offset.get())
				.saturating_sub(1);
			if remaining as u32 <= remaining_in_line {
				anchor.row_offset =
					DisplayRow::new(anchor.row_offset.get().saturating_add(remaining as u32));
				return anchor;
			}
			if !exact {
				// More rows remain unscanned: stop at the last known one and let
				// the continuation refresh resume, rather than leaving the line.
				let last_known = DisplayRow::new(line_rows.saturating_sub(1));
				anchor.row_offset = anchor.row_offset.max(last_known);
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
		// A cursor row the index cannot reach this frame gives no reason to
		// move the view; the continuation settles it.
		let Some((cursor_row, _)) = self.try_cursor_row_and_pane_col(cursor_line, cursor_col)
		else {
			return anchor;
		};
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

	/// Where the cursor must move to stay in the view: `None` when its row or
	/// its new place cannot be reached this frame, or lies past the caret's
	/// `u32` column range, and a caller then keeps both the cursor and the view
	/// where they were.
	fn clamp_cursor_to_viewport_anchor(
		&self,
		anchor: ViewportAnchor,
		cursor_line: DocLine,
		cursor_col: DocDisplayCol,
		viewport_rows: u32,
	) -> Option<CursorTarget> {
		let viewport_rows = viewport_rows.max(1);
		let anchor = self.normalize_anchor(anchor);
		let cursor_line = self.visible_line(cursor_line);
		let (cursor_row, _) = self.try_cursor_row_and_pane_col(cursor_line, cursor_col)?;
		let anchor_visual = self.visual_index(anchor.line);
		let cursor_visual = self.visual_index(cursor_line);
		if cursor_visual < anchor_visual
			|| (cursor_visual == anchor_visual && cursor_row < anchor.row_offset.get())
		{
			return self.row_target(anchor);
		}

		let cursor_offset = self.rows_from_anchor_to_position(anchor, cursor_line, cursor_row);
		if cursor_offset < viewport_rows {
			Some(CursorTarget {
				line: cursor_line,
				col: cursor_col,
				byte: None,
			})
		} else {
			let bottom =
				self.scroll_viewport_anchor(anchor, viewport_rows.saturating_sub(1) as i32);
			self.row_target(bottom)
		}
	}

	/// The start of the row at `anchor` as a cursor target.
	fn row_target(&self, anchor: ViewportAnchor) -> Option<CursorTarget> {
		let row = self.try_visible_row(anchor.line, anchor.row_offset)?;
		let Some(offset) = row.start_offset else {
			// A row of a line laid out whole: the caret is placed by column.
			return Some(CursorTarget {
				line: row.line,
				col: row.start_col,
				byte: None,
			});
		};
		let col = offset;
		let byte = match row.start_byte {
			Some(byte) => byte,
			// A row of trailing whitespace has no byte of its own: find the cell
			// holding its start, walking the run, across frames if it is long.
			None => self.trailing_whitespace_byte(row.line, offset)?,
		};
		Some(CursorTarget {
			line: row.line,
			col,
			byte: Some(byte),
		})
	}

	fn trailing_whitespace_byte(&self, line: DocLine, offset: DocDisplayCol) -> Option<DocByte> {
		let LineLayout::Indexed { start, end } = self.line_layout(line) else {
			return None;
		};
		let wrap = self.wrap?;
		let source = self.line_source(start, end);
		let lookup = wrap.index.borrow_mut().trailing_whitespace_byte(
			self.wrap_key(wrap),
			line,
			offset,
			&source,
		);
		match lookup {
			ByteLookup::Known(byte) => Some(DocByte::new(start.get().saturating_add(byte))),
			ByteLookup::Unknown { pending } => {
				wrap.demand.borrow_mut().note(pending);
				None
			}
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
			let (line_rows, exact) = self.line_rows(anchor.line);
			// An anchor past a scanned prefix shows the last known row until the
			// scan reaches it.
			let first = anchor.row_offset.get().min(line_rows.saturating_sub(1));
			for row_offset in first..line_rows {
				// A row whose start the index cannot reach this frame ends the
				// list: fewer rows now, the continuation refresh adds the rest.
				let Some(row) = self.try_visible_row(anchor.line, DisplayRow::new(row_offset))
				else {
					return rows;
				};
				rows.push(row);
				if rows.len() as u32 >= viewport_rows {
					return rows;
				}
			}
			// Rows of this line are still unscanned; the next line cannot start.
			if !exact {
				return rows;
			}
			let Some(next_line) = self.next_visible_line(anchor.line) else {
				return rows;
			};
			anchor.line = next_line;
			anchor.row_offset = DisplayRow::ZERO;
		}
	}
}

/// The window a pane must publish to show `first_row` (ADR 0009): `None`
/// when the row is on a line laid out whole, or starts its line.
#[allow(clippy::too_many_arguments)]
pub(crate) fn window_origin_with_sum_tree<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	max_doc_line: DocLine,
	wrap_enabled: bool,
	text_width: u16,
	tree: &SumTree,
	wrap: Option<WrapLayout<'_>>,
	first_row: VisibleRow,
) -> Option<RowWindow> {
	if first_row
		.start_offset
		.is_none_or(|offset| offset == DocDisplayCol::ZERO)
	{
		return None;
	}
	let (start_byte, start_offset) = LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
		wrap,
	)
	.window_origin(first_row)?;
	Some(RowWindow {
		line: first_row.line,
		start_byte,
		start_offset,
		tab_phase: (start_offset.get() % u64::from(TAB_SIZE)) as u8,
	})
}

/// Whether `line` is laid out through the wrap index (ADR 0009).
#[allow(clippy::too_many_arguments)]
pub(crate) fn line_is_indexed_with_sum_tree<F: FoldPolicy + ?Sized>(
	registry: &UastRegistry,
	root: NodeId,
	folds: &F,
	max_doc_line: DocLine,
	wrap_enabled: bool,
	text_width: u16,
	tree: &SumTree,
	wrap: Option<WrapLayout<'_>>,
	line: DocLine,
) -> bool {
	wrap_enabled
		&& matches!(
			LayoutContext::with_sum_tree(
				registry,
				root,
				folds,
				max_doc_line,
				wrap_enabled,
				text_width,
				tree,
				wrap,
			)
			.line_layout(line),
			LineLayout::Indexed { .. }
		)
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
	.map(|target| (target.line, target.col))
	.expect("without a wrap index the clamp always places the cursor")
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
	wrap: Option<WrapLayout<'_>>,
) -> ViewportAnchor {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
		wrap,
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
	wrap: Option<WrapLayout<'_>>,
) -> ViewportAnchor {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
		wrap,
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
	wrap: Option<WrapLayout<'_>>,
) -> Option<CursorTarget> {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
		wrap,
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
	wrap: Option<WrapLayout<'_>>,
) -> (PaneLocalRow, PaneLocalCol) {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
		wrap,
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
	wrap: Option<WrapLayout<'_>>,
) -> Vec<VisibleRow> {
	LayoutContext::with_sum_tree(
		registry,
		root,
		folds,
		max_doc_line,
		wrap_enabled,
		text_width,
		tree,
		wrap,
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
				VisibleRow::whole(DocLine::ZERO, DocDisplayCol::new(4)),
				VisibleRow::whole(DocLine::ZERO, DocDisplayCol::new(8)),
				VisibleRow::whole(DocLine::new(1), DocDisplayCol::ZERO),
				VisibleRow::whole(DocLine::new(2), DocDisplayCol::ZERO),
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

	mod wrap_index_layout {
		use super::super::{LayoutContext, LayoutDemand, ViewportAnchor, WrapLayout};
		use super::{build_document, build_source_backed_line_document};
		use crate::core::{DisplayRow, DocDisplayCol, DocLine, StateId, TopologyRev};
		use crate::ecs::{NodeId, UastRegistry};
		use crate::engine::wrap_index::{WrapIndex, WrapIndexLimits};
		use std::cell::RefCell;

		const WIDTH: u16 = 13;
		/// Lines this long take the index in these tests.
		const THRESHOLD: u64 = 64;

		fn long_line(target: usize) -> String {
			let words = [
				"alpha ",
				"beta\t",
				"gamma ",
				"日本 ",
				"delta  ",
				"e\u{301}psilon ",
				"x",
			];
			let mut line = String::new();
			let mut at = 0;
			while line.len() < target {
				line.push_str(words[at % words.len()]);
				at += 1;
			}
			line
		}

		/// Rows as both paths publish them; the index path adds byte origins.
		fn placed(rows: &[super::super::VisibleRow]) -> Vec<(DocLine, DocDisplayCol)> {
			rows.iter().map(|row| (row.line, row.start_col)).collect()
		}

		/// An indexed row's origin names the cell holding its start column.
		fn assert_origin(text: &str, row: &super::super::VisibleRow) {
			let offset = row.start_offset.expect("an indexed row has an offset");
			assert_eq!(offset.get(), u64::from(row.start_col.get()));
			let Some(byte) = row.start_byte else { return };
			let line_start = text
				.split_inclusive('\n')
				.take(row.line.get() as usize)
				.map(str::len)
				.sum::<usize>();
			let line_bytes = text[line_start..].split('\n').next().unwrap().as_bytes();
			let in_line = byte.get() as usize - line_start;
			let at = crate::core::text::display_col_for_byte_offset(
				line_bytes,
				in_line,
				0,
				crate::core::TAB_SIZE,
			);
			let (_, after) = crate::core::text::byte_offset_for_display_col(
				&line_bytes[in_line..],
				1,
				at,
				crate::core::TAB_SIZE,
			);
			assert!(
				u64::from(at) <= offset.get()
					&& (u64::from(at) == offset.get() || offset.get() < u64::from(after)),
				"row {row:?} names a cell at {at}..{after}"
			);
		}

		struct Fixture {
			registry: UastRegistry,
			root: NodeId,
			index: RefCell<WrapIndex>,
			demand: RefCell<LayoutDemand>,
		}

		impl Fixture {
			fn new(text: &str, limits: WrapIndexLimits) -> Self {
				let (registry, root) = build_document(text);
				Self {
					registry,
					root,
					index: RefCell::new(WrapIndex::with_limits(limits)),
					demand: RefCell::new(LayoutDemand::default()),
				}
			}

			fn max_line(&self) -> DocLine {
				DocLine::new(self.registry.get_total_newlines(self.root))
			}

			fn whole(&self) -> LayoutContext<'_, crate::uast::NoFolds> {
				LayoutContext::sparse(
					&self.registry,
					self.root,
					&crate::uast::NoFolds,
					self.max_line(),
					true,
					WIDTH,
				)
			}

			/// A context through the index, at the start of a frame.
			fn indexed(&self) -> LayoutContext<'_, crate::uast::NoFolds> {
				self.index.borrow_mut().begin_frame();
				let mut wrap = WrapLayout::new(
					&self.index,
					StateId::new(1),
					TopologyRev::new(1),
					&self.demand,
				);
				wrap.min_line_bytes = THRESHOLD;
				self.whole().with_wrap(wrap)
			}

			fn take_demand(&self) -> LayoutDemand {
				std::mem::take(&mut *self.demand.borrow_mut())
			}
		}

		#[test]
		fn the_index_path_lays_out_exactly_like_the_whole_line() {
			let text = format!("{}\nshort line\n{}", long_line(3_000), long_line(1_500));
			let fixture = Fixture::new(&text, WrapIndexLimits::default());
			let whole = fixture.whole();
			let mut anchors = Vec::new();
			for line in 0..=fixture.max_line().get() {
				let line = DocLine::new(line);
				let (rows, exact) = whole.line_rows(line);
				assert!(exact);
				assert_eq!(
					fixture.indexed().line_rows(line),
					(rows, true),
					"line {line}"
				);
				for row in 0..rows {
					anchors.push(ViewportAnchor {
						line,
						row_offset: DisplayRow::new(row),
					});
				}
			}
			assert!(anchors.len() > 300, "long lines wrap into many rows");
			for (at, &anchor) in anchors.iter().enumerate().step_by(7) {
				let indexed_rows = fixture.indexed().collect_visible_rows(anchor, 9);
				assert_eq!(
					placed(&indexed_rows),
					placed(&whole.collect_visible_rows(anchor, 9)),
					"anchor {anchor:?}"
				);
				for row in indexed_rows
					.iter()
					.filter(|row| row.line != DocLine::new(1))
				{
					assert_origin(&text, row);
				}
				for delta in [-40, -9, -1, 1, 5, 40, 10_000] {
					assert_eq!(
						fixture.indexed().scroll_viewport_anchor(anchor, delta),
						whole.scroll_viewport_anchor(anchor, delta),
						"anchor {anchor:?} delta {delta}"
					);
				}
				let col = DocDisplayCol::new((at as u64 * 37) % 4_000);
				assert_eq!(
					fixture
						.indexed()
						.cursor_view_metrics(anchor, anchor.line, col),
					whole.cursor_view_metrics(anchor, anchor.line, col),
					"anchor {anchor:?} col {col}"
				);
			}
			assert!(
				!fixture.take_demand().more,
				"a resident line finishes in one frame"
			);
		}

		fn tight() -> WrapIndexLimits {
			WrapIndexLimits {
				read_budget: 512,
				checkpoint_bytes: 128,
				line_bytes: 1 << 20,
				max_lines: 4,
			}
		}

		#[test]
		fn scrolling_stops_at_the_scanned_prefix_and_resumes_on_later_frames() {
			let text = format!("head\n{}\ntail", long_line(20_000));
			let fixture = Fixture::new(&text, tight());
			let long = DocLine::new(1);
			let (rows, exact) = fixture.whole().line_rows(long);
			assert!(exact && rows > 1_000);

			// From the line above, a long scroll enters the long line and stops
			// at its last known row: it must not skip to the tail line.
			let mut anchor = ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::ZERO,
			};
			anchor = fixture.indexed().scroll_viewport_anchor(anchor, 100_000);
			assert_eq!(
				anchor.line, long,
				"the scroll stopped inside the unscanned line"
			);
			let first_stop = anchor.row_offset;
			assert!(
				fixture.take_demand().more,
				"the stop asks for a continuation"
			);

			// Each frame scans one budget further, and the same scroll goes on.
			let mut frames = 0;
			while anchor.line == long {
				anchor = fixture.indexed().scroll_viewport_anchor(anchor, 100_000);
				frames += 1;
				assert!(frames < 10_000, "the scroll never left the long line");
			}
			assert_eq!(anchor.line, DocLine::new(2), "the scroll reached the tail");
			assert!(frames > 1, "the scan took several frames");
			assert!(first_stop.get() < rows - 1);

			// An anchor past a scanned prefix is kept, not pulled back.
			let fresh = Fixture::new(&text, tight());
			let deep = ViewportAnchor {
				line: long,
				row_offset: DisplayRow::new(rows - 1),
			};
			assert_eq!(fresh.indexed().normalize_anchor(deep), deep);
		}

		#[test]
		fn one_frame_of_layout_passes_reads_one_budget() {
			let text = format!("{}\n", long_line(50_000));
			let fixture = Fixture::new(&text, tight());
			let context = fixture.indexed();
			let anchor = ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::ZERO,
			};
			// What render does in one frame: pan, visible rows, cursor metrics.
			let _ = context.pan_viewport_anchor_to_keep_cursor_visible(
				anchor,
				DocLine::ZERO,
				DocDisplayCol::new(9_000),
				30,
			);
			let _ = context.collect_visible_rows(anchor, 30);
			let _ = context.cursor_view_metrics(anchor, DocLine::ZERO, DocDisplayCol::new(9_000));
			let _ = context.scroll_viewport_anchor(anchor, 5_000);
			let fed = fixture
				.index
				.borrow()
				.fed_bytes(DocLine::ZERO, WIDTH)
				.expect("the line is indexed");
			assert!(
				fed <= tight().read_budget as u64,
				"one frame read {fed} bytes"
			);
		}

		#[test]
		fn visible_rows_never_guess_a_start_the_index_cannot_reach() {
			// Checkpoints far apart and a tiny frame budget: a replay to an old
			// row outside the recent ring takes many frames.
			let limits = WrapIndexLimits {
				read_budget: 256,
				checkpoint_bytes: 8 * 1024,
				line_bytes: 1 << 20,
				max_lines: 4,
			};
			let text = format!("{}\n", long_line(60_000));
			let fixture = Fixture::new(&text, limits);
			let whole = fixture.whole();
			let (rows, _) = whole.line_rows(DocLine::ZERO);
			assert!(rows > 5_000, "more rows than the recent ring holds");
			for _ in 0..100_000 {
				if fixture.indexed().line_rows(DocLine::ZERO).1 {
					break;
				}
			}
			fixture.take_demand();
			let anchor = ViewportAnchor {
				line: DocLine::ZERO,
				// The ring keeps the latest rows, so row 200 is outside it.
				row_offset: DisplayRow::new(200),
			};
			let expected = placed(&whole.collect_visible_rows(anchor, 20));
			let mut frames = 0;
			loop {
				let got = placed(&fixture.indexed().collect_visible_rows(anchor, 20));
				// Every emitted row is a true one; a short frame is allowed.
				assert_eq!(got[..], expected[..got.len()], "frame {frames}");
				if got.len() == expected.len() {
					break;
				}
				assert!(fixture.take_demand().more, "a short frame asks to continue");
				frames += 1;
				assert!(frames < 10_000, "the rows never settled");
			}
			assert!(frames > 0, "the replay outlasted at least one frame");
		}

		#[test]
		fn an_unplaced_cursor_does_not_move_the_view() {
			// As in the visible-rows case: a replay to an old row outlasts a frame.
			let limits = WrapIndexLimits {
				read_budget: 256,
				checkpoint_bytes: 8 * 1024,
				line_bytes: 1 << 20,
				max_lines: 4,
			};
			let text = format!("{}\n", long_line(60_000));
			let fixture = Fixture::new(&text, limits);
			let whole = fixture.whole();
			for _ in 0..100_000 {
				if fixture.indexed().line_rows(DocLine::ZERO).1 {
					break;
				}
			}
			fixture.take_demand();
			let cursor_col = whole
				.try_row_start_col(DocLine::ZERO, DisplayRow::new(200))
				.expect("the whole path places every row");
			let anchor = ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::new(210),
			};
			// The cursor's row is unknown this frame: the view stays put rather
			// than jumping to row 0.
			let first = fixture
				.indexed()
				.pan_viewport_anchor_to_keep_cursor_visible(anchor, DocLine::ZERO, cursor_col, 5);
			assert_eq!(first, anchor);
			assert!(fixture.take_demand().more);
			// Later frames place the cursor and pan to its real row.
			let expected = whole.pan_viewport_anchor_to_keep_cursor_visible(
				anchor,
				DocLine::ZERO,
				cursor_col,
				5,
			);
			let mut frames = 0;
			let mut panned = first;
			while panned != expected {
				panned = fixture
					.indexed()
					.pan_viewport_anchor_to_keep_cursor_visible(
						anchor,
						DocLine::ZERO,
						cursor_col,
						5,
					);
				frames += 1;
				assert!(frames < 10_000, "the pan never settled");
				assert!(
					panned == anchor || panned == expected,
					"frame {frames}: {panned:?}"
				);
			}
		}

		#[test]
		fn a_trailing_whitespace_window_starts_at_the_run_and_rows_keep_their_offsets() {
			let text = format!("{}{}", long_line(200), " ".repeat(60));
			let fixture = Fixture::new(&text, WrapIndexLimits::default());
			let whole = fixture.whole();
			let (rows, _) = whole.line_rows(DocLine::ZERO);
			let top = ViewportAnchor {
				line: DocLine::ZERO,
				row_offset: DisplayRow::ZERO,
			};
			let context = fixture.indexed();
			let all = context.collect_visible_rows(top, rows);
			assert_eq!(placed(&all), placed(&whole.collect_visible_rows(top, rows)));
			let trailing: Vec<_> = all.iter().filter(|row| row.start_byte.is_none()).collect();
			assert!(trailing.len() >= 2, "several rows of trailing whitespace");

			let words = text.trim_end();
			let run_byte = words.len() as u64;
			let run_offset = u64::from(crate::core::text::display_width(
				words.as_bytes(),
				0,
				crate::core::TAB_SIZE,
			));
			for row in trailing {
				let (byte, offset) = context.window_origin(*row).expect("the run is recorded");
				assert_eq!(
					(byte.get(), offset.get()),
					(run_byte, run_offset),
					"row {row:?}"
				);
				// The row keeps its own place; only the window's origin moves.
				assert!(offset.get() < row.start_offset.expect("an offset").get());
			}
		}

		fn caret_index(fixture: &Fixture, wrap_enabled: bool) -> super::super::CaretIndex<'_> {
			fixture.index.borrow_mut().begin_frame();
			let mut wrap = WrapLayout::new(
				&fixture.index,
				StateId::new(1),
				TopologyRev::new(1),
				&fixture.demand,
			);
			wrap.min_line_bytes = THRESHOLD;
			super::super::CaretIndex::new(wrap, wrap_enabled, WIDTH)
		}

		/// Call `lookup` once per frame until it is no longer pending.
		fn settle_caret<T>(
			fixture: &Fixture,
			wrap_enabled: bool,
			lookup: impl Fn(super::super::CaretIndex<'_>) -> Option<super::super::CaretLookup<T>>,
		) -> Option<T> {
			for _ in 0..10_000 {
				match lookup(caret_index(fixture, wrap_enabled)) {
					Some(super::super::CaretLookup::Known(found)) => return Some(found),
					Some(super::super::CaretLookup::Pending) => {
						assert!(
							fixture.take_demand().pending.is_empty(),
							"a resident line names nothing to hydrate"
						);
					}
					None => return None,
				}
			}
			panic!("the caret lookup never settled");
		}

		#[test]
		fn caret_lookups_match_the_whole_line_helpers_with_wrap_on_and_off() {
			let long = long_line(3_000);
			let text = format!("head\n{long}\nshort line");
			let line_start = "head\n".len() as u64;
			let bytes = long.as_bytes();
			let width = crate::core::text::display_width(bytes, 0, crate::core::TAB_SIZE);
			let line = DocLine::new(1);
			for wrap_enabled in [true, false] {
				let fixture = Fixture::new(&text, tight());
				for step in 0..40u64 {
					let byte = (step * 211) % (bytes.len() as u64 + 3);
					let found = settle_caret(&fixture, wrap_enabled, |caret| {
						caret.line_col_for_byte(
							&fixture.registry,
							fixture.root,
							crate::core::DocByte::new(line_start + byte),
						)
					})
					.expect("the long line is indexed");
					let col = crate::core::text::display_col_for_byte_offset(
						bytes,
						byte as usize,
						0,
						crate::core::TAB_SIZE,
					);
					assert_eq!(found, (line, DocDisplayCol::new(col)), "byte {byte}");
					let target = (step * 97) % (width + 5);
					let found = settle_caret(&fixture, wrap_enabled, |caret| {
						caret.byte_for_line_col(
							&fixture.registry,
							fixture.root,
							line,
							DocDisplayCol::new(target),
						)
					})
					.expect("the long line is indexed");
					let (at, col) = crate::core::text::byte_offset_for_display_col(
						bytes,
						target,
						0,
						crate::core::TAB_SIZE,
					);
					assert_eq!(
						found,
						(
							crate::core::DocByte::new(line_start + at as u64),
							DocDisplayCol::new(col)
						),
						"column {target}"
					);
				}
				// With wrap off the caret keys the line at the fixed width.
				let keyed = if wrap_enabled { WIDTH } else { u16::MAX };
				assert!(fixture.index.borrow().fed_bytes(line, keyed).is_some());
			}
		}

		#[test]
		fn an_ordinary_line_takes_todays_caret_path() {
			let fixture = Fixture::new(&format!("short line\n{}", long_line(500)), tight());
			let caret = caret_index(&fixture, true);
			let line = DocLine::ZERO;
			assert!(
				caret
					.line_col_for_byte(
						&fixture.registry,
						fixture.root,
						crate::core::DocByte::new(3)
					)
					.is_none()
			);
			assert!(
				caret
					.byte_for_line_col(&fixture.registry, fixture.root, line, DocDisplayCol::new(3))
					.is_none()
			);
			assert!(fixture.index.borrow().fed_bytes(line, WIDTH).is_none());
		}

		#[test]
		fn a_cold_indexed_line_keeps_the_caret_and_names_its_node() {
			let (registry, root, path) = build_source_backed_line_document(3);
			let fixture = Fixture {
				registry,
				root,
				index: RefCell::new(WrapIndex::default()),
				demand: RefCell::new(LayoutDemand::default()),
			};
			let caret_index = caret_index(&fixture, true);
			let line = DocLine::new(1);
			let lookup = caret_index.byte_for_line_col(
				&fixture.registry,
				fixture.root,
				line,
				DocDisplayCol::new(1),
			);
			assert_eq!(lookup, Some(super::super::CaretLookup::Pending));
			assert!(
				!fixture.take_demand().pending.is_empty(),
				"the cold leaf is named"
			);
			let mut caret = crate::engine::session::Caret::at_start();
			let mut folds = crate::engine::folding::FoldSet::default();
			let placed = caret.place_at_line_col_indexed(
				&fixture.registry,
				fixture.root,
				&mut folds,
				TopologyRev::new(1),
				StateId::new(1),
				line,
				DocDisplayCol::new(1),
				false,
				crate::engine::session::Affinity::After,
				Some(caret_index),
			);
			assert!(!placed, "a pending indexed line does not place the caret");
			assert_eq!(
				caret.byte(),
				crate::core::DocByte::ZERO,
				"the caret did not move"
			);
			let target = caret.pending_target().expect("the target is kept");
			assert_eq!((target.line, target.col), (line, DocDisplayCol::new(1)));
			let _ = std::fs::remove_file(path);
		}

		#[test]
		fn a_pending_indexed_line_never_falls_back_to_a_prefix_read() {
			// The line is resident, so today's path could place the caret by
			// reading it; the index needs several frames to reach the column.
			let fixture = Fixture::new(&format!("head\n{}", long_line(6_000)), tight());
			let line = DocLine::new(1);
			let target = DocDisplayCol::new(4_000);
			let mut caret = crate::engine::session::Caret::at_start();
			let mut folds = crate::engine::folding::FoldSet::default();
			let place = |caret: &mut crate::engine::session::Caret,
			             folds: &mut crate::engine::folding::FoldSet| {
				caret.place_at_line_col_indexed(
					&fixture.registry,
					fixture.root,
					folds,
					TopologyRev::new(1),
					StateId::new(1),
					line,
					target,
					false,
					crate::engine::session::Affinity::After,
					Some(caret_index(&fixture, true)),
				)
			};
			assert!(
				!place(&mut caret, &mut folds),
				"one frame cannot reach the column"
			);
			assert_eq!(
				caret.byte(),
				crate::core::DocByte::ZERO,
				"the caret did not move"
			);
			assert!(
				fixture.take_demand().more,
				"the demand asks for another frame"
			);
			let mut frames = 1;
			while !caret.resolve_indexed(
				&fixture.registry,
				fixture.root,
				&mut folds,
				TopologyRev::new(1),
				StateId::new(1),
				Some(caret_index(&fixture, true)),
			) {
				frames += 1;
				assert!(frames < 1_000, "the target never resolved");
			}
			assert!(frames > 2, "the walk spanned frames");
			assert_eq!(caret.abs_line(), line);
			assert!(
				caret.abs_col().get() >= target.get() && caret.abs_col().get() < target.get() + 4
			);
		}

		#[test]
		fn carried_work_for_another_key_is_dropped() {
			let key = super::super::WorkKey {
				root: NodeId::from_index(1),
				state: StateId::new(1),
				topology: TopologyRev::new(1),
				key_width: WIDTH,
			};
			let later = super::super::WorkKey {
				state: StateId::new(2),
				..key
			};
			let mut work = super::super::CarriedWork::default();
			let mut demand = LayoutDemand::default();
			demand.note(Some(NodeId::from_index(5)));
			work.carry(key, demand);
			work.scan_to_end(key, DocLine::new(3));
			let (demand, scan) = work.take(later);
			assert!(demand.pending.is_empty() && !demand.more && scan.is_none());
			// And taking drops it: nothing is left for the old key either.
			let (demand, scan) = work.take(key);
			assert!(demand.pending.is_empty() && scan.is_none());
			// Work for one key replaces work for another.
			work.scan_to_end(key, DocLine::new(3));
			work.scan_to_end(later, DocLine::new(4));
			assert_eq!(work.take(later).1, Some(DocLine::new(4)));
		}

		#[test]
		fn a_line_without_resident_bytes_asks_for_its_node() {
			let (registry, root, path) = build_source_backed_line_document(3);
			let index = RefCell::new(WrapIndex::default());
			let demand = RefCell::new(LayoutDemand::default());
			let context = LayoutContext::sparse(
				&registry,
				root,
				&crate::uast::NoFolds,
				DocLine::new(registry.get_total_newlines(root)),
				true,
				WIDTH,
			)
			.with_wrap(WrapLayout::new(
				&index,
				StateId::new(1),
				TopologyRev::new(1),
				&demand,
			));
			let rows = context.collect_visible_rows(
				ViewportAnchor {
					line: DocLine::new(1),
					row_offset: DisplayRow::ZERO,
				},
				4,
			);
			assert!(!rows.is_empty());
			let demand = demand.into_inner();
			assert!(demand.more);
			assert!(
				!demand.pending.is_empty(),
				"the cold leaf is named for hydration"
			);
			let _ = std::fs::remove_file(path);
		}
	}
}
