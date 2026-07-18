use iced::advanced::layout::{self, Layout};
use iced::advanced::renderer;
use iced::advanced::text::{Paragraph, Renderer as TextRenderer};
use iced::advanced::widget::{self, Widget};
use iced::advanced::{Clipboard, Renderer as _, Shell};
use iced::keyboard;
use iced::mouse;
use iced::{Color, Element, Event, Length, Pixels, Point, Rectangle, Renderer, Size, Theme};
use std::sync::OnceLock;

use super::buffer::{Buffer, TokenSpan};
use super::coords::{CharIdx, CursorPos, LineIdx, VisualCol, TAB_WIDTH, line};
use super::highlight::TokenKind;
use super::theme::EditorTheme;
use super::wrap::VisualLine;

// ─── Constants ────────────────────────────────────────────────────────────────

pub const CHAR_W: f32 = 9.6;

pub const EDITOR_FONT: iced::Font = iced::Font {
	family: iced::font::Family::Name("DejaVu Sans Mono"),
	weight: iced::font::Weight::Normal,
	stretch: iced::font::Stretch::Normal,
	style: iced::font::Style::Normal,
};
const LINE_H: f32 = 22.0;
const GUTTER_PAD: f32 = 16.0;
const FOLD_COL_W: f32 = 16.0;
const LEFT_PAD: f32 = 12.0;
const TOP_PAD: f32 = 8.0;
const FONT_SZ: f32 = 15.0;
const CURSOR_W: f32 = 2.0;
const ERR_THICK: f32 = 2.0;
const SCROLL_W: f32 = 10.0;
const INDENT_W: f32 = 1.0;
const BRACKET_BW: f32 = 1.5;
const MINIMAP_W: f32 = 80.0;
const MINIMAP_LINE_H: f32 = 2.5;
const MINIMAP_CHAR_W: f32 = 1.2;
const SEARCH_PANEL_H: f32 = 40.0;

// ─── Actions ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum EditorAction {
	Edit,
	CursorMoved,
	MouseDown(iced::Point),
	DoubleClick(iced::Point),
	AddCaret(iced::Point),
	/// Fired when the widget's pixel bounds change (e.g. window resize).
	Resize(f32, f32),
	/// Toggle the fold at the given document line.
	ToggleFold(usize),
}

// ─── Widget ───────────────────────────────────────────────────────────────────

pub struct EditorWidget<'a, Message> {
	buffer: &'a Buffer,
	theme: &'a EditorTheme,
	on_action: Box<dyn Fn(EditorAction) -> Message + 'a>,
	scroll_y: f32,
	scroll_x: f32,
	show_minimap: bool,
	block_cursor: bool,
	show_whitespace: bool,
	/// Visual block selection: (top_line, bottom_line, left_col, right_col inclusive)
	visual_block: Option<(LineIdx, LineIdx, CharIdx, CharIdx)>,
}

impl<'a, Message> EditorWidget<'a, Message> {
	pub fn new(
		buffer: &'a Buffer,
		theme: &'a EditorTheme,
		on_action: impl Fn(EditorAction) -> Message + 'a,
	) -> Self {
		Self {
			buffer,
			theme,
			on_action: Box::new(on_action),
			scroll_y: 0.0,
			scroll_x: 0.0,
			show_minimap: true,
			block_cursor: false,
			show_whitespace: true,
			visual_block: None,
		}
	}

	pub fn scroll_y(mut self, v: f32) -> Self {
		self.scroll_y = v;
		self
	}
	pub fn scroll_x(mut self, v: f32) -> Self {
		self.scroll_x = v;
		self
	}
	pub fn show_minimap(mut self, v: bool) -> Self {
		self.show_minimap = v;
		self
	}
	pub fn block_cursor(mut self, v: bool) -> Self {
		self.block_cursor = v;
		self
	}
	pub fn show_whitespace(mut self, v: bool) -> Self {
		self.show_whitespace = v;
		self
	}
	pub fn visual_block(mut self, v: Option<(LineIdx, LineIdx, CharIdx, CharIdx)>) -> Self {
		self.visual_block = v;
		self
	}

	fn gutter_w(&self) -> f32 {
		let d = format!("{}", self.buffer.line_count()).len().max(3) as f32;
		d * char_width() + GUTTER_PAD * 2.0 + FOLD_COL_W
	}

	fn text_x(&self) -> f32 {
		self.gutter_w() + LEFT_PAD
	}
	fn minimap_x(&self, bounds: &Rectangle) -> f32 {
		bounds.x + bounds.width
			- if self.show_minimap {
				MINIMAP_W + SCROLL_W
			} else {
				SCROLL_W
			}
	}

	fn pixel_to_pos(&self, bounds: &Rectangle, px: f32, py: f32) -> CursorPos {
		let ry = py - bounds.y - TOP_PAD + self.scroll_y;
		let vl_idx = ((ry / LINE_H).floor().max(0.0) as usize)
			.min(self.buffer.document.visual_lines.len().saturating_sub(1usize));
		if let Some(vl) = self.buffer.document.visual_lines.get(vl_idx) {
			let lt = self.buffer.line_text(vl.doc_line);
			let vl_vcol_off = line::visual_col_of(&lt, vl.col_start);
			let char_w = char_width();
			let rx = px - bounds.x - self.text_x() + self.scroll_x;
			let vcol = (rx / char_w).round().max(0.0) as usize + *vl_vcol_off;
			let logical = line::logical_col_of(&lt, VisualCol(vcol));
			self.buffer.click_to_pos(vl.doc_line, logical)
		} else {
			CursorPos::new(self.buffer.line_count().saturating_sub(1usize), CharIdx(0))
		}
	}
}

// ─── State ────────────────────────────────────────────────────────────────────

pub struct EditorState {
	pub is_focused: bool,
	is_dragging: bool,
	last_click: std::time::Instant,
	click_count: u32,
	hover_diag: Option<usize>,
	last_bounds: Rectangle,
}

impl Default for EditorState {
	fn default() -> Self {
		Self {
			is_focused: false,
			is_dragging: false,
			last_click: std::time::Instant::now(),
			click_count: 0,
			hover_diag: None,
			last_bounds: Rectangle::default(),
		}
	}
}

// ─── Private draw helpers ─────────────────────────────────────────────────────

impl<'a, Message> EditorWidget<'a, Message> {
	fn draw_visual_lines(
		&self,
		renderer: &mut Renderer,
		b: Rectangle,
		gw: f32,
		tx: f32,
		editor_h: f32,
		st: &EditorState,
	) {
		let vls = &self.buffer.document.visual_lines;
		let first = (self.scroll_y / LINE_H).floor() as usize;
		let last = (first + (editor_h / LINE_H).ceil() as usize + 2).min(vls.len());
		let active = self.buffer.session.selection.head.line;

		for vi in first..last {
			if let Some(vl) = vls.get(vi) {
				let y = b.y + TOP_PAD + (vi as f32 * LINE_H) - self.scroll_y;
				if y + LINE_H < b.y || y > b.y + editor_h {
					continue;
				}
				self.draw_line_gutter(renderer, b, gw, *vl.doc_line, y, *active, vl.is_first);
			}
		}

		// Clip text content to the region left of the minimap/scrollbar.
		let mm_x = self.minimap_x(&b);
		let content_clip = Rectangle {
			x: b.x + gw,
			y: b.y,
			width: mm_x - (b.x + gw),
			height: editor_h,
		};
		renderer.start_layer(content_clip);
		{
			for vi in first..last {
				if let Some(vl) = vls.get(vi) {
					let y = b.y + TOP_PAD + (vi as f32 * LINE_H) - self.scroll_y;
					if y + LINE_H < b.y || y > b.y + editor_h {
						continue;
					}
					self.draw_line_highlights(renderer, b, gw, tx, *vl.doc_line, y, *active, st, vl);
					self.draw_line_tokens(renderer, b, tx, *vl.doc_line, y, vl);
				}
			}
		}
		renderer.end_layer();
	}

	fn draw_ui_layers(
		&self,
		renderer: &mut Renderer,
		b: Rectangle,
		tx: f32,
		editor_h: f32,
		st: &EditorState,
	) {
		self.draw_cursor(renderer, b, tx, editor_h, st);
		self.draw_tooltip(renderer, b, tx, st);

		if self.show_minimap {
			self.draw_minimap(renderer, b, editor_h);
		}
		self.draw_scrollbar(renderer, b, editor_h);
		if self.buffer.session.search.is_open {
			self.draw_search_panel(renderer, b);
		}
	}

	fn draw_background(&self, renderer: &mut Renderer, b: Rectangle, gw: f32) {
		let th = self.theme;
		fill(renderer, b, th.background);
		fill(
			renderer,
			Rectangle {
				x: b.x,
				y: b.y,
				width: gw,
				height: b.height,
			},
			th.gutter_bg,
		);
		fill(
			renderer,
			Rectangle {
				x: b.x + gw - 1.0,
				y: b.y,
				width: 1.0,
				height: b.height,
			},
			th.gutter_border,
		);
	}

	/// Draw the line gutter for one visual line.
	/// `is_first`: true for the first visual line of a doc line; continuation lines get a blank gutter.
	fn draw_line_gutter(
		&self,
		renderer: &mut Renderer,
		b: Rectangle,
		gw: f32,
		li: usize,
		y: f32,
		active: usize,
		is_first: bool,
	) {
		if !is_first {
			return;
		}
		let th = self.theme;
		let char_w = char_width();
		let num = format!("{}", li + 1);
		let nc = if li == active {
			th.gutter_active_text
		} else {
			th.gutter_text
		};
		draw_text(
			renderer,
			&num,
			b.x + gw - FOLD_COL_W - GUTTER_PAD - (num.len() as f32 * char_w),
			y,
			nc,
			gw,
		);
		if self.buffer.document.folds.is_foldable(LineIdx(li)) {
			let collapsed = self.buffer.document.folds.is_collapsed_start(LineIdx(li));
			draw_text(
				renderer,
				if collapsed { "▶" } else { "▼" },
				b.x + gw - FOLD_COL_W + 2.0,
				y,
				th.fold_indicator,
				FOLD_COL_W,
			);
		}
		if self.buffer.document.folds.is_collapsed_start(LineIdx(li)) {
			fill(
				renderer,
				Rectangle {
					x: b.x + gw,
					y,
					width: b.width - gw,
					height: LINE_H,
				},
				th.fold_collapsed_bg,
			);
		}
		if self
			.buffer
			.document
			.diagnostics
			.iter()
			.any(|d| *d.line == li)
		{
			fill_r(
				renderer,
				Rectangle {
					x: b.x + 4.0,
					y: y + LINE_H / 2.0 - 3.0,
					width: 6.0,
					height: 6.0,
				},
				th.error_gutter_marker,
				3.0,
			);
		}
	}

	fn draw_line_highlights(
		&self,
		renderer: &mut Renderer,
		b: Rectangle,
		gw: f32,
		tx: f32,
		li: usize,
		y: f32,
		active: usize,
		st: &EditorState,
		vl: &VisualLine,
	) {
		let th = self.theme;
		let char_w = char_width();
		let lt = self.buffer.line_text(LineIdx(li));
		// Visual column offset of the start of this visual line within the doc line.
		let vl_vcol_off = line::visual_col_of(&lt, vl.col_start);

		if li == active && st.is_focused {
			fill(
				renderer,
				Rectangle {
					x: b.x + gw,
					y,
					width: b.width
						- gw - SCROLL_W - if self.show_minimap { MINIMAP_W } else { 0.0 },
					height: LINE_H,
				},
				th.current_line_bg,
			);
		}

		// Indent guides: only relevant on first visual line of a doc line.
		if vl.is_first {
			for &vcol in &self.buffer.indent_guides(LineIdx(li)) {
				let guide_abs = vcol.saturating_sub(TAB_WIDTH);
				if guide_abs >= *vl_vcol_off {
					let gx = b.x + tx + ((guide_abs - *vl_vcol_off) as f32 * char_w) - self.scroll_x;
					let c = if li == active {
						th.indent_guide_active
					} else {
						th.indent_guide
					};
					fill(
						renderer,
						Rectangle {
							x: gx,
							y,
							width: INDENT_W,
							height: LINE_H,
						},
						c,
					);
				}
			}
		}

		// Search matches clipped to this visual line's byte range.
		if self.buffer.session.search.is_open {
			let line_len = lt.chars().count();
			for (i, m) in self.buffer.session.search.matches.iter().enumerate() {
				if *m.line == li && m.col_start < vl.col_end && m.col_end > vl.col_start {
					let ms = m.col_start.max(vl.col_start).min(CharIdx(line_len));
					let me = m.col_end.min(vl.col_end).min(CharIdx(line_len));
					let mvs = line::visual_col_of(&lt, ms).saturating_sub(*vl_vcol_off);
					let mve = line::visual_col_of(&lt, me).saturating_sub(*vl_vcol_off);
					let mx = b.x + tx + (*mvs as f32 * char_w) - self.scroll_x;
					let mw = ((*mve - *mvs) as f32 * char_w).max(char_w);
					let c = if i == self.buffer.session.search.current_match {
						th.search_current_bg
					} else {
						th.search_match_bg
					};
					fill(
						renderer,
						Rectangle {
							x: mx,
							y,
							width: mw,
							height: LINE_H,
						},
						c,
					);
				}
			}
		}

		if let Some((top, bottom, left_col, right_col)) = self.visual_block {
			if LineIdx(li) >= top && LineIdx(li) <= bottom {
				let line_len = lt.chars().count();
				if left_col < vl.col_end && (right_col + 1) > vl.col_start {
					let vcs = line::visual_col_of(&lt, left_col.max(vl.col_start).min(CharIdx(line_len)))
						.saturating_sub(*vl_vcol_off);
					let vce =
						line::visual_col_of(&lt, (right_col + 1).min(vl.col_end).min(CharIdx(line_len)))
							.saturating_sub(*vl_vcol_off);
					let sx = b.x + tx + (*vcs as f32 * char_w) - self.scroll_x;
					let sw = ((*vce - *vcs) as f32 * char_w).max(char_w * 0.5);
					fill(
						renderer,
						Rectangle {
							x: sx,
							y,
							width: sw,
							height: LINE_H,
						},
						th.selection,
					);
				}
			}
		} else {
			for sel in std::iter::once(&self.buffer.session.selection)
				.chain(self.buffer.secondary_selections().iter())
			{
				if sel.is_caret() {
					continue;
				}
				let (ss, se) = sel.ordered();
				if *ss.line > li || *se.line < li {
					continue;
				}
				let line_len = lt.chars().count();
				let raw_start = if *ss.line == li { ss.col } else { CharIdx(0) };
				let raw_end = if *se.line == li { se.col } else { CharIdx(line_len) };
				if raw_start < vl.col_end && raw_end > vl.col_start {
					let clip_start = raw_start.max(vl.col_start).min(CharIdx(line_len));
					let clip_end = raw_end.min(vl.col_end).min(CharIdx(line_len));
					let vcs = line::visual_col_of(&lt, clip_start).saturating_sub(*vl_vcol_off);
					let vce = if raw_end > vl.col_end {
						let end_abs = line::visual_col_of(&lt, vl.col_end.min(CharIdx(line_len)))
							.saturating_sub(*vl_vcol_off);
						*end_abs + if *se.line > li { 1 } else { 0 }
					} else {
						*line::visual_col_of(&lt, clip_end).saturating_sub(*vl_vcol_off)
					};
					if vce > *vcs {
						let sx = b.x + tx + (*vcs as f32 * char_w) - self.scroll_x;
						let sw = ((vce - *vcs) as f32 * char_w).max(char_w * 0.5);
						fill(
							renderer,
							Rectangle {
								x: sx,
								y,
								width: sw,
								height: LINE_H,
							},
							th.selection,
						);
					}
				}
			}
		}

		// Bracket matching: only when bracket is within this visual line's byte range.
		if let Some(ref bm) = self.buffer.session.matched_bracket {
			for &(bl, bc) in &[(bm.open_line, bm.open_col), (bm.close_line, bm.close_col)] {
				if bl == LineIdx(li) && bc >= vl.col_start && bc < vl.col_end {
					let blt = self.buffer.line_text(bl);
					let bvcol = line::visual_col_of(&blt, bc).saturating_sub(*vl_vcol_off);
					let bx = b.x + tx + (*bvcol as f32 * char_w) - self.scroll_x;
					fill(
						renderer,
						Rectangle {
							x: bx,
							y,
							width: char_w,
							height: LINE_H,
						},
						th.bracket_match_bg,
					);
					for rect in [
						Rectangle {
							x: bx,
							y,
							width: char_w,
							height: BRACKET_BW,
						},
						Rectangle {
							x: bx,
							y: y + LINE_H - BRACKET_BW,
							width: char_w,
							height: BRACKET_BW,
						},
						Rectangle {
							x: bx,
							y,
							width: BRACKET_BW,
							height: LINE_H,
						},
						Rectangle {
							x: bx + char_w - BRACKET_BW,
							y,
							width: BRACKET_BW,
							height: LINE_H,
						},
					] {
						fill(renderer, rect, th.bracket_match_border);
					}
				}
			}
		}
	}

	fn draw_line_tokens(
		&self,
		renderer: &mut Renderer,
		b: Rectangle,
		tx: f32,
		li: usize,
		y: f32,
		vl: &VisualLine,
	) {
		let th = self.theme;
		let char_w = char_width();
		let lt = self.buffer.line_text(LineIdx(li));
		let line_len = lt.chars().count();
		let vl_vcol_off = line::visual_col_of(&lt, vl.col_start);
		let render_start = vl.col_start;
		let render_end = vl.col_end.min(CharIdx(line_len));

		let spans = self
			.buffer
			.token_spans_for_line(LineIdx(li), render_start, render_end);
		let mut render: Vec<(CharIdx, CharIdx, TokenKind)> = Vec::new();
		let mut cur = render_start;
		for &TokenSpan {
			col_start: s,
			col_end: e,
			kind: k,
		} in &spans
		{
			// If there's a gap before this token, fill it with Plain text.
			if s > cur {
				render.push((cur, s, TokenKind::Plain));
				cur = s;
			}
			// Clip token start to 'cur' to prevent double-drawing overlapping tokens
			// (common when tokens are stale after a text edit but before re-analysis).
			let s = s.max(cur);
			if e > s {
				render.push((s, e, k));
				cur = e;
			}
		}
		if cur < render_end {
			render.push((cur, render_end, TokenKind::Plain));
		}
		let mut merged: Vec<(CharIdx, CharIdx, TokenKind)> = Vec::with_capacity(render.len());
		for (start, end, kind) in render {
			if let Some((_, last_end, last_kind)) = merged.last_mut()
				&& *last_kind == kind
				&& *last_end == start
			{
				*last_end = end;
			} else {
				merged.push((start, end, kind));
			}
		}

		let ws_color = Color {
			a: 0.35,
			..th.gutter_text
		};
		let trail_start = lt.trim_end().chars().count();

		for &(start, end, kind) in &merged {
			if start >= render_end {
				break;
			}
			let sl = self.buffer.line_slice(LineIdx(li), start, end);
			if sl.is_empty() {
				continue;
			}
			let color = token_color(&kind, th);
			let mut vcol: VisualCol = line::visual_col_of(&lt, start);
			let mut seg = String::new();
			let mut seg_vcol = vcol;
			let mut char_pos = start;
			for ch in sl.chars() {
				if ch == '\t' {
					if !seg.is_empty() {
						draw_text(
							renderer,
							&seg,
							b.x + tx + ((*seg_vcol.saturating_sub(*vl_vcol_off)) as f32 * char_w)
								- self.scroll_x,
							y,
							color,
							b.width,
						);
						seg.clear();
					}
					if self.show_whitespace {
						draw_text(
							renderer,
							"▸",
							b.x + tx + ((*vcol.saturating_sub(*vl_vcol_off)) as f32 * char_w)
								- self.scroll_x,
							y,
							ws_color,
							char_w,
						);
					}
					vcol = VisualCol((*vcol / TAB_WIDTH + 1) * TAB_WIDTH);
					seg_vcol = vcol;
				} else if self.show_whitespace && ch == ' ' {
					if !seg.is_empty() {
						draw_text(
							renderer,
							&seg,
							b.x + tx + ((*seg_vcol.saturating_sub(*vl_vcol_off)) as f32 * char_w)
								- self.scroll_x,
							y,
							color,
							b.width,
						);
						seg.clear();
					}
					let glyph = if *char_pos >= trail_start { "~" } else { "␣" };
					draw_text(
						renderer,
						glyph,
						b.x + tx + ((*vcol.saturating_sub(*vl_vcol_off)) as f32 * char_w)
							- self.scroll_x,
						y,
						ws_color,
						char_w,
					);
					vcol += 1;
					seg_vcol = vcol;
				} else {
					seg.push(ch);
					vcol += 1;
				}
				char_pos += 1;
			}
			if !seg.is_empty() {
				draw_text(
					renderer,
					&seg,
					b.x + tx + ((*seg_vcol.saturating_sub(*vl_vcol_off)) as f32 * char_w)
						- self.scroll_x,
					y,
					color,
					b.width,
				);
			}
		}

		// EOL marker, fold indicator, and diagnostics only on the last visual line for this doc line.
		let is_last_vl = vl.col_end >= CharIdx(line_len);
		if is_last_vl {
			if self.show_whitespace {
				let eol_vcol: VisualCol = line::chars_with_vcols(&lt)
					.last()
					.map_or(VisualCol(0), |(_, vc)| vc + 1);
				draw_text(
					renderer,
					"¬",
					b.x + tx + ((*eol_vcol.saturating_sub(*vl_vcol_off)) as f32 * char_w)
						- self.scroll_x,
					y,
					Color {
						a: 0.18,
						..th.gutter_text
					},
					char_w,
				);
			}
			if self.buffer.document.folds.is_collapsed_start(LineIdx(li)) {
				let hc = self.buffer.document.folds.hidden_count(LineIdx(li));
				if hc > 0 {
					let eol_vcol: VisualCol = line::chars_with_vcols(&lt)
						.last()
						.map_or(VisualCol(0), |(_, vc)| vc + 1);
					draw_text(
						renderer,
						&format!(" ⋯ {} lines", hc),
						b.x + tx + ((*eol_vcol.saturating_sub(*vl_vcol_off)) as f32 * char_w) + 8.0
							- self.scroll_x,
						y,
						th.comment,
						200.0,
					);
				}
			}
		}

		for diag in &self.buffer.document.diagnostics {
			if *diag.line == li && diag.col_start < vl.col_end && diag.col_end > vl.col_start {
				let ds = diag.col_start.max(vl.col_start).min(CharIdx(line_len));
				let de = diag.col_end.min(render_end).min(CharIdx(line_len));
				if ds < de {
					let uvs = line::visual_col_of(&lt, ds).saturating_sub(*vl_vcol_off);
					let uve = line::visual_col_of(&lt, de).saturating_sub(*vl_vcol_off);
					let ux = b.x + tx + (*uvs as f32 * char_w) - self.scroll_x;
					let uw = ((*uve - *uvs) as f32 * char_w).max(char_w);
					let uy = y + LINE_H - ERR_THICK - 1.0;
					let seg: f32 = 4.0;
					let mut sx = ux;
					let mut up = true;
					while sx < ux + uw {
						fill(
							renderer,
							Rectangle {
								x: sx,
								y: if up { uy - 1.0 } else { uy + 1.0 },
								width: seg.min(ux + uw - sx),
								height: ERR_THICK,
							},
							th.error_underline,
						);
						sx += seg;
						up = !up;
					}
				}
			}
		}
	}

	fn draw_cursor(
		&self,
		renderer: &mut Renderer,
		b: Rectangle,
		tx: f32,
		editor_h: f32,
		st: &EditorState,
	) {
		if !st.is_focused {
			return;
		}
		let th = self.theme;
		let char_w = char_width();
		let draw_one = |renderer: &mut Renderer, caret: CursorPos, primary: bool| {
			let clt = self.buffer.line_text(caret.line);
			let cvcol_abs = line::visual_col_of(&clt, caret.col);
			let vl_idx = self
				.buffer
				.document
				.visual_lines
				.iter()
				.position(|vl| {
					vl.doc_line == caret.line
						&& vl.col_start <= caret.col
						&& caret.col <= vl.col_end
				})
				.or_else(|| {
					self.buffer
						.document
						.visual_lines
						.iter()
						.position(|vl| vl.doc_line == caret.line)
				})
				.unwrap_or(*caret.line);
			let vl_vcol_off = self
				.buffer
				.document
				.visual_lines
				.get(vl_idx)
				.map(|vl| line::visual_col_of(&clt, vl.col_start))
				.unwrap_or(VisualCol(0));

			let cy = b.y + TOP_PAD + (vl_idx as f32 * LINE_H) - self.scroll_y;
			let cx = b.x + tx + ((*cvcol_abs.saturating_sub(*vl_vcol_off)) as f32 * char_w)
				- self.scroll_x;
			if cy <= b.y - LINE_H || cy >= b.y + editor_h {
				return;
			}
			if primary && self.block_cursor {
				fill(
					renderer,
					Rectangle {
						x: cx,
						y: cy,
						width: char_w,
						height: LINE_H,
					},
					Color {
						a: 0.55,
						..th.cursor
					},
				);
			} else {
				fill(
					renderer,
					Rectangle {
						x: cx,
						y: cy,
						width: CURSOR_W,
						height: LINE_H,
					},
					if primary {
						th.cursor
					} else {
						Color {
							a: 0.75,
							..th.cursor
						}
					},
				);
			}
		};

		draw_one(renderer, self.buffer.session.selection.head, true);
		for sel in self.buffer.secondary_selections() {
			draw_one(renderer, sel.head, false);
		}
	}

	fn draw_minimap(&self, renderer: &mut Renderer, b: Rectangle, editor_h: f32) {
		let th = self.theme;
		let mx = self.minimap_x(&b);
		fill(
			renderer,
			Rectangle {
				x: mx,
				y: b.y,
				width: MINIMAP_W,
				height: editor_h,
			},
			th.minimap_bg,
		);
		let total_h = *self.buffer.line_count() as f32 * MINIMAP_LINE_H;
		if total_h > 0.0 {
			let scale = MINIMAP_LINE_H / LINE_H;
			let vp_h = (editor_h * scale).min(editor_h).max(20.0);
			let vp_y = b.y + self.scroll_y * scale;
			fill(
				renderer,
				Rectangle {
					x: mx,
					y: vp_y,
					width: MINIMAP_W,
					height: vp_h,
				},
				th.minimap_viewport,
			);
		}
		for li_raw in 0..*self.buffer.line_count() {
			let li = LineIdx(li_raw);
			if self.buffer.document.folds.is_hidden(li) {
				continue;
			}
			let my = b.y + li_raw as f32 * MINIMAP_LINE_H;
			if my > b.y + editor_h {
				break;
			}
			let lt = self.buffer.line_text(li);
			if lt.trim().is_empty() {
				continue;
			}
			for TokenSpan {
				col_start: s,
				col_end: e,
				kind,
			} in self.buffer.token_spans_for_line(li, CharIdx(0), CharIdx(lt.chars().count()))
			{
				let tw = (*e - *s) as f32 * MINIMAP_CHAR_W;
				if tw > 0.5 {
					let c = token_color(&kind, th);
					fill(
						renderer,
						Rectangle {
							x: mx + 4.0 + *s as f32 * MINIMAP_CHAR_W,
							y: my,
							width: tw.min(MINIMAP_W - 8.0),
							height: MINIMAP_LINE_H,
						},
						Color::from_rgba(c.r, c.g, c.b, 0.35),
					);
				}
			}
		}
	}

	fn draw_scrollbar(&self, renderer: &mut Renderer, b: Rectangle, editor_h: f32) {
		let th = self.theme;
		let sb_x = b.x + b.width - SCROLL_W;
		fill(
			renderer,
			Rectangle {
				x: sb_x,
				y: b.y,
				width: SCROLL_W,
				height: editor_h,
			},
			th.scrollbar_track,
		);
		let total = self.buffer.document.visual_lines.len() as f32 * LINE_H + TOP_PAD * 2.0;
		if total > editor_h {
			let th_h = ((editor_h / total) * editor_h).max(24.0);
			let th_y = b.y + (self.scroll_y / (total - editor_h)) * (editor_h - th_h);
			fill_r(
				renderer,
				Rectangle {
					x: sb_x + 2.0,
					y: th_y,
					width: SCROLL_W - 4.0,
					height: th_h,
				},
				th.scrollbar_thumb,
				3.0,
			);
		}
	}

	fn draw_search_panel(&self, renderer: &mut Renderer, b: Rectangle) {
		let th = self.theme;
		let sp_y = b.y + b.height - SEARCH_PANEL_H;
		fill(
			renderer,
			Rectangle {
				x: b.x,
				y: sp_y,
				width: b.width,
				height: SEARCH_PANEL_H,
			},
			th.search_panel_bg,
		);
		fill(
			renderer,
			Rectangle {
				x: b.x,
				y: sp_y,
				width: b.width,
				height: 1.0,
			},
			th.gutter_border,
		);
		let info = format!(
			"Find: \"{}\"  {} of {}   Replace: \"{}\"   [Enter=next] [Shift+Enter=prev] [Ctrl+Shift+H=replace] [Ctrl+Shift+Enter=all]",
			self.buffer.session.search.query,
			if self.buffer.session.search.matches.is_empty() {
				0
			} else {
				self.buffer.session.search.current_match + 1
			},
			self.buffer.session.search.match_count(),
			self.buffer.session.search.replacement,
		);
		draw_text(
			renderer,
			&info,
			b.x + 12.0,
			sp_y + 4.0,
			th.tooltip_text,
			b.width - 24.0,
		);
	}

	fn draw_tooltip(&self, renderer: &mut Renderer, b: Rectangle, tx: f32, st: &EditorState) {
		let th = self.theme;
		let char_w = char_width();
		if let Some(di) = st.hover_diag {
			if let Some(diag) = self.buffer.document.diagnostics.get(di) {
				let diag_vl_idx = self
					.buffer
					.document
					.visual_lines
					.iter()
					.position(|vl| {
						vl.doc_line == diag.line
							&& vl.col_start <= diag.col_start
							&& diag.col_start <= vl.col_end
					})
					.unwrap_or(*diag.line);
				let dy =
					b.y + TOP_PAD + (diag_vl_idx as f32 * LINE_H) - self.scroll_y + LINE_H + 4.0;
				let dx = b.x + tx + (*diag.col_start as f32 * char_w) - self.scroll_x;
				let tw = (diag.message.len() as f32 * char_w * 0.62)
					.min(400.0)
					.max(150.0);
				let th2 = 28.0;
				fill_r(
					renderer,
					Rectangle {
						x: dx + 1.0,
						y: dy + 1.0,
						width: tw,
						height: th2,
					},
					th.tooltip_shadow,
					4.0,
				);
				fill_r(
					renderer,
					Rectangle {
						x: dx,
						y: dy,
						width: tw,
						height: th2,
					},
					th.tooltip_bg,
					4.0,
				);
				for rect in [
					Rectangle {
						x: dx,
						y: dy,
						width: tw,
						height: 1.0,
					},
					Rectangle {
						x: dx,
						y: dy + th2 - 1.0,
						width: tw,
						height: 1.0,
					},
					Rectangle {
						x: dx,
						y: dy,
						width: 1.0,
						height: th2,
					},
					Rectangle {
						x: dx + tw - 1.0,
						y: dy,
						width: 1.0,
						height: th2,
					},
				] {
					fill(renderer, rect, th.tooltip_border);
				}
				let msg = if diag.message.len() > 55 {
					format!("{}…", &diag.message[..54])
				} else {
					diag.message.clone()
				};
				draw_text(renderer, &msg, dx + 8.0, dy, th.tooltip_text, tw - 16.0);
			}
		}
	}
}

// ─── Widget impl ──────────────────────────────────────────────────────────────

impl<'a, Message: Clone> Widget<Message, Theme, Renderer> for EditorWidget<'a, Message> {
	fn tag(&self) -> widget::tree::Tag {
		widget::tree::Tag::of::<EditorState>()
	}
	fn state(&self) -> widget::tree::State {
		widget::tree::State::new(EditorState::default())
	}
	fn size(&self) -> Size<Length> {
		Size {
			width: Length::Fill,
			height: Length::Fill,
		}
	}

	fn layout(
		&mut self,
		_t: &mut widget::Tree,
		_r: &Renderer,
		lim: &layout::Limits,
	) -> layout::Node {
		layout::Node::new(lim.width(Length::Fill).height(Length::Fill).max())
	}

	fn draw(
		&self,
		tree: &widget::Tree,
		renderer: &mut Renderer,
		_theme: &Theme,
		_style: &renderer::Style,
		layout: Layout<'_>,
		_cursor: mouse::Cursor,
		_vp: &Rectangle,
	) {
		let b = layout.bounds();
		let st = tree.state.downcast_ref::<EditorState>();
		let gw = self.gutter_w();
		let tx = self.text_x();
		let editor_h = b.height
			- if self.buffer.session.search.is_open {
				SEARCH_PANEL_H
			} else {
				0.0
			};

		renderer.start_layer(b);
		{
			self.draw_background(renderer, b, gw);
			self.draw_visual_lines(renderer, b, gw, tx, editor_h, st);
			self.draw_ui_layers(renderer, b, tx, editor_h, st);
		}
		renderer.end_layer();
	}

	fn update(
		&mut self,
		tree: &mut widget::Tree,
		event: &Event,
		layout: Layout<'_>,
		cursor: mouse::Cursor,
		_r: &Renderer,
		_clip: &mut dyn Clipboard,
		shell: &mut Shell<'_, Message>,
		_vp: &Rectangle,
	) {
		let b = layout.bounds();
		let st = tree.state.downcast_mut::<EditorState>();

		match event {
			Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)) => {
				if let Some(pos) = cursor.position_over(b) {
					st.is_focused = true;
					let now = std::time::Instant::now();
					if now.duration_since(st.last_click).as_millis() < 400 {
						st.click_count += 1;
					} else {
						st.click_count = 1;
					}
					st.last_click = now;

					let gw = self.gutter_w();
					if pos.x >= b.x + gw - FOLD_COL_W && pos.x <= b.x + gw {
						let ry = pos.y - b.y - TOP_PAD + self.scroll_y;
						let vl_idx = ((ry / LINE_H).floor().max(0.0) as usize)
							.min(self.buffer.document.visual_lines.len().saturating_sub(1usize));
						if let Some(vl) = self.buffer.document.visual_lines.get(vl_idx) {
							let doc_line = vl.doc_line;
							if self.buffer.document.folds.is_foldable(doc_line) {
								shell.publish((self.on_action)(EditorAction::ToggleFold(*doc_line)));
							}
						}
						shell.capture_event();
						return;
					}

					st.is_dragging = true;
					let action = if st.click_count >= 2 {
						EditorAction::DoubleClick(pos)
					} else {
						EditorAction::MouseDown(pos)
					};
					shell.publish((self.on_action)(action));
					shell.capture_event();
					return;
				} else {
					st.is_focused = false;
				}
			}
			Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Middle)) => {
				if let Some(pos) = cursor.position_over(b) {
					st.is_focused = true;
					shell.publish((self.on_action)(EditorAction::AddCaret(pos)));
					shell.capture_event();
					return;
				}
			}
			Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)) => {
				st.is_dragging = false;
			}
			Event::Mouse(mouse::Event::CursorMoved { position }) => {
				st.hover_diag = None;
				if cursor.is_over(b) {
					let hp = self.pixel_to_pos(&b, position.x, position.y);
					for (i, d) in self.buffer.document.diagnostics.iter().enumerate() {
						if d.line == hp.line && hp.col >= d.col_start && hp.col < d.col_end {
							st.hover_diag = Some(i);
							break;
						}
					}
				}
				if st.is_dragging {
					shell.capture_event();
					return;
				}
			}
			Event::Keyboard(keyboard::Event::KeyPressed { .. }) if st.is_focused => {
				shell.capture_event();
				return;
			}
			_ => {}
		}

		// Detect widget resize and notify CodeEditor so it can recompute wrap_col.
		if b.width != st.last_bounds.width || b.height != st.last_bounds.height {
			st.last_bounds = b;
			shell.publish((self.on_action)(EditorAction::Resize(b.width, b.height)));
		}
	}

	fn mouse_interaction(
		&self,
		_t: &widget::Tree,
		layout: Layout<'_>,
		cursor: mouse::Cursor,
		_vp: &Rectangle,
		_r: &Renderer,
	) -> mouse::Interaction {
		let b = layout.bounds();
		if cursor.is_over(b) {
			if let Some(pos) = cursor.position() {
				let gw = self.gutter_w();
				if pos.x > b.x + gw && pos.x < self.minimap_x(&b) {
					return mouse::Interaction::Text;
				}
			}
		}
		mouse::Interaction::default()
	}
}

impl<'a, Message: Clone + 'a> From<EditorWidget<'a, Message>> for Element<'a, Message> {
	fn from(e: EditorWidget<'a, Message>) -> Self {
		Self::new(e)
	}
}

// ─── Drawing helpers ──────────────────────────────────────────────────────────

fn fill(r: &mut Renderer, rect: Rectangle, color: Color) {
	r.fill_quad(
		renderer::Quad {
			bounds: rect,
			border: iced::Border::default(),
			shadow: iced::Shadow::default(),
			snap: false,
		},
		color,
	);
}

fn fill_r(r: &mut Renderer, rect: Rectangle, color: Color, radius: f32) {
	r.fill_quad(
		renderer::Quad {
			bounds: rect,
			border: iced::Border {
				radius: radius.into(),
				..Default::default()
			},
			shadow: iced::Shadow::default(),
			snap: false,
		},
		color,
	);
}

fn draw_text(r: &mut Renderer, content: &str, x: f32, y: f32, color: Color, max_w: f32) {
	let text_y = y + (LINE_H - FONT_SZ) / 2.0;
	r.fill_text(
		iced::advanced::text::Text {
			content: content.to_string().into(),
			bounds: Size::new(max_w, FONT_SZ),
			size: Pixels(FONT_SZ),
			line_height: iced::advanced::text::LineHeight::Relative(1.0),
			font: EDITOR_FONT,
			align_x: iced::advanced::text::Alignment::Left,
			align_y: iced::alignment::Vertical::Top,
			shaping: iced::advanced::text::Shaping::Basic,
			wrapping: iced::advanced::text::Wrapping::None,
		},
		Point::new(x, text_y),
		color,
		Rectangle {
			x,
			y,
			width: max_w,
			height: LINE_H,
		},
	);
}

fn token_color(kind: &TokenKind, th: &EditorTheme) -> Color {
	match kind {
		TokenKind::Keyword => th.keyword,
		TokenKind::Type => th.type_name,
		TokenKind::String => th.string,
		TokenKind::Number => th.number,
		TokenKind::Comment => th.comment,
		TokenKind::Operator => th.operator,
		TokenKind::Punctuation => th.punctuation,
		TokenKind::Identifier => th.identifier,
		TokenKind::Function => th.function,
		TokenKind::Macro => th.macro_color,
		TokenKind::Attribute => th.attribute,
		TokenKind::Lifetime => th.lifetime,
		TokenKind::Error => th.error_underline,
		TokenKind::Plain => th.plain,
	}
}

// ─── Public helpers ───────────────────────────────────────────────────────────

pub fn pixel_to_pos(
	buf: &Buffer,
	bounds: &Rectangle,
	gutter_w: f32,
	scroll_x: f32,
	scroll_y: f32,
	px: f32,
	py: f32,
) -> CursorPos {
	let ry = py - bounds.y - TOP_PAD + scroll_y;
	let vl_idx = ((ry / LINE_H).floor().max(0.0) as usize)
		.min(buf.document.visual_lines.len().saturating_sub(1usize));
	if let Some(vl) = buf.document.visual_lines.get(vl_idx) {
		let lt = buf.line_text(vl.doc_line);
		let vl_vcol_off = line::visual_col_of(&lt, vl.col_start);
		let char_w = char_width();
		let rx = px - bounds.x - gutter_w - LEFT_PAD + scroll_x;
		let vcol = (rx / char_w).round().max(0.0) as usize + *vl_vcol_off;
		let logical = line::logical_col_of(&lt, VisualCol(vcol));
		buf.click_to_pos(vl.doc_line, logical)
	} else {
		CursorPos::new(buf.line_count().saturating_sub(1usize), CharIdx(0))
	}
}

fn measure_text_width(content: &str) -> f32 {
	let text = iced::advanced::text::Text {
		content,
		bounds: Size::new(f32::INFINITY, f32::INFINITY),
		size: Pixels(FONT_SZ),
		line_height: iced::advanced::text::LineHeight::Relative(1.0),
		font: EDITOR_FONT,
		align_x: iced::advanced::text::Alignment::Left,
		align_y: iced::alignment::Vertical::Top,
		shaping: iced::advanced::text::Shaping::Basic,
		wrapping: iced::advanced::text::Wrapping::None,
	};

	<Renderer as TextRenderer>::Paragraph::with_text(text).min_width()
}

pub fn char_width() -> f32 {
	static CHAR_WIDTH: OnceLock<f32> = OnceLock::new();

	*CHAR_WIDTH.get_or_init(|| {
		let width = measure_text_width("0");
		let width = if width.is_finite() && width > 0.0 {
			width
		} else {
			CHAR_W
		};

		(width * 64.0).round() / 64.0
	})
}

pub fn gutter_width(line_count: usize) -> f32 {
	format!("{}", line_count).len().max(3) as f32 * char_width() + GUTTER_PAD * 2.0 + FOLD_COL_W
}

pub fn visible_line_count(h: f32) -> usize {
	((h - TOP_PAD) / LINE_H).ceil() as usize
}
pub const fn line_height() -> f32 {
	LINE_H
}
pub const fn top_pad() -> f32 {
	TOP_PAD
}
pub const fn left_pad() -> f32 {
	LEFT_PAD
}
pub const fn scrollbar_width() -> f32 {
	SCROLL_W
}
pub const fn minimap_width() -> f32 {
	MINIMAP_W
}
pub const fn search_panel_height() -> f32 {
	SEARCH_PANEL_H
}
