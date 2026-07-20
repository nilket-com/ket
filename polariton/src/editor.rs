//! Thin Iced host for the headless libaryon engine.
//!
//! This deliberately owns no document state. The engine is authoritative; this
//! module only forwards portable input, coalesces projected viewports, and
//! renders the focused pane's projection: syntax, gutter, cursor, selection,
//! diagnostics, folds, engine-owned wrap rows and scrolling, and the minimap.

use crate::gui::table::Table;
use iced::keyboard::{self, Key};
use iced::mouse;
use iced::widget::{
	Space, button, column, container, mouse_area, rich_text, row, span, stack, text,
};
use iced::{Background, Color, Element, Event, Length, Subscription, Task, event};
use libaryon::core::{CursorPosition, DocDisplayCol, DocLine, Rgb};
use libaryon::engine::{
	EditorCommand, EditorMode, EngineConfig, EngineHandle, InputEvent, KeyCode, Modifiers,
};
use libaryon::engine::{
	HostEvent, HostPanelId, MinimapSnapshot, PanelRevision, SplitDirection, UiPalette, VisualKind,
};
use libaryon::uast::Viewport;
use polars::frame::DataFrame;
use std::sync::mpsc::Receiver;

/// The single query-results surface. One stable identity reused for every run,
/// so results replace in place instead of opening a pane per query.
const RESULT_PANEL: HostPanelId = HostPanelId::new(1);

const INITIAL_COLUMNS: u16 = 120;
const INITIAL_ROWS: u16 = 40;

/// The registered application monospace font. Named explicitly (rather than
/// `Font::MONOSPACE`, which resolves to the platform default) so
/// `metrics::CELL_WIDTH` describes the same glyphs on every machine.
const EDITOR_FONT: iced::Font = iced::Font::with_name("DejaVu Sans Mono");

/// Shared text geometry. Layout (`view`), click mapping, scroll conversion,
/// and cell-grid sizing must all agree on these values, so they live in one
/// place. `CELL_WIDTH` is an estimate of `EDITOR_FONT`'s advance at
/// `TEXT_SIZE`; exact per-glyph metrics arrive with the custom
/// `advanced::Widget`, until then click columns can drift on very long lines.
mod metrics {
	pub const TEXT_SIZE: f32 = 15.0;
	pub const CELL_WIDTH: f32 = 9.6;
	pub const LINE_HEIGHT: f32 = 22.0;
	pub const GUTTER_WIDTH: f32 = 54.0;
	pub const VERTICAL_PADDING: f32 = 8.0;
}

#[derive(Debug, Clone)]
pub enum EditorMsg {
	Input(InputEvent),
	Paste(String),
	Scroll(i32),
	MinimapGoto(DocLine),
	PointerMoved(iced::Point),
	Click,
	/// Pointer released; ends a divider drag.
	Release,
}

/// Long-lived owner of Polariton's libaryon session.
pub struct CodeEditor {
	engine: Option<EngineHandle>,
	commands: libaryon::engine::CommandSender,
	input: libaryon::engine::InputSender,
	viewports: Receiver<Viewport>,
	viewport: Option<Viewport>,
	document_text: String,
	text_query_pending: bool,
	/// Input was forwarded after the pending text query was issued, so the
	/// in-flight reply is already stale and another query must follow it.
	content_dirty: bool,
	last_pointer: Option<iced::Point>,
	/// Latest engine status line (`:w` outcomes, search wraps, command
	/// errors). Retained until the next forwarded input, vim-style: each
	/// command's outcome replaces the previous one.
	status_message: Option<String>,
	/// The engine accepted `:q` and has emitted its final viewport. Further
	/// editor traffic must stay local while the application closes its window.
	quit_requested: bool,
	quit_notified: bool,
	/// Ordered engine events; drained every poll, never coalesced.
	host_events: Option<Receiver<HostEvent>>,
	panels: PanelRegistry,
	/// Monotonic revision for the results panel, so each run supersedes the
	/// last.
	result_revision: u64,
	/// Divider drag in progress, if any.
	drag: Option<DividerDrag>,
}

#[derive(Clone, Copy)]
struct DividerDrag {
	before: libaryon::core::PaneId,
	after: libaryon::core::PaneId,
	axis: SplitDirection,
	/// Distance from the grabbed point to the boundary, so the divider tracks
	/// the cursor instead of jumping to it.
	grab_offset: u32,
}

impl CodeEditor {
	pub fn new(text: &str) -> Self {
		let mut engine = EngineHandle::spawn(EngineConfig::default());
		let commands = engine.command_sender();
		let input = engine.input_sender();
		let viewports = engine
			.take_viewports()
			.expect("new engine owns viewport receiver");
		let host_events = engine.take_host_events();
		let mut editor = Self {
			engine: Some(engine),
			commands,
			input,
			viewports,
			viewport: None,
			document_text: String::new(),
			text_query_pending: false,
			content_dirty: false,
			last_pointer: None,
			status_message: None,
			quit_requested: false,
			quit_notified: false,
			host_events,
			panels: PanelRegistry::default(),
			result_revision: 0,
			drag: None,
		};
		editor.send(EditorCommand::Resize(INITIAL_COLUMNS, INITIAL_ROWS));
		editor.send(EditorCommand::ReplaceDocument {
			text: text.to_owned(),
			language: Some("sql".to_owned()),
			// File-path-shaped identity so libaryon's language services
			// (semantic highlighting, diagnostics) run for this unsaved
			// in-memory document.
			virtual_path: Some("statement.sql".to_owned()),
		});
		editor.request_content();
		editor
	}

	/// Last complete document text received from the engine.
	pub fn cached_content(&self) -> &str {
		&self.document_text
	}

	/// SQL to run from the focused editor context: an active visual selection,
	/// or the cursor's logical line when no selection exists.
	pub fn selected_or_current_line(&self) -> Option<String> {
		let pane = self.viewport.as_ref()?.focused_pane()?;
		let source = self.document_text.as_bytes();
		if !pane.selection_ranges.is_empty() {
			let mut fragments = Vec::with_capacity(pane.selection_ranges.len());
			for (start, end) in &pane.selection_ranges {
				let start = usize::try_from(start.get()).ok()?;
				let end = usize::try_from(end.get()).ok()?.saturating_add(1);
				let fragment = std::str::from_utf8(source.get(start..end)?).ok()?;
				fragments.push(fragment);
			}
			return Some(fragments.join("\n"));
		}

		let start = usize::try_from(pane.cursor_line_start_byte.get()).ok()?;
		let tail = std::str::from_utf8(source.get(start..)?).ok()?;
		Some(
			tail.split_once('\n')
				.map_or(tail, |(line, _)| line)
				.to_owned(),
		)
	}

	/// Announce that a query started: open the results panel on first run, and
	/// on later runs reuse the same identity with a fresh revision so results
	/// replace in place rather than multiplying panes.
	pub fn begin_query(&mut self) {
		// A panel awaiting disposal is already gone from the engine even though
		// the registry still holds it until a projection agrees, so refreshing
		// it would address a pane that no longer exists. Reopening is correct
		// in that window; unconditional reopening is not, because the engine
		// focuses an existing panel and every rerun would steal focus.
		let known = self.panels.panels.contains_key(&RESULT_PANEL)
			&& !self.panels.pending_disposal.contains(&RESULT_PANEL);
		let revision = PanelRevision::new(self.result_revision.saturating_add(1));
		self.result_revision = revision.get();
		if known {
			self.send(EditorCommand::RefreshHostPanel {
				panel_id: RESULT_PANEL,
				revision,
			});
		} else {
			self.send(EditorCommand::OpenHostPanel {
				panel_id: RESULT_PANEL,
				kind: "result-table".to_owned(),
				title: "Results".to_owned(),
			});
		}
		self.panels.set_content(RESULT_PANEL, PanelContent::Running);
	}

	/// Attach a completed result set to the panel.
	pub fn set_query_result(&mut self, frame: DataFrame) {
		self.panels
			.set_content(RESULT_PANEL, PanelContent::Table(Box::new(frame)));
	}

	/// A failed query keeps its panel: the error belongs where the results
	/// would have been, not only in the status bar.
	pub fn set_query_error(&mut self, message: String) {
		self.panels
			.set_content(RESULT_PANEL, PanelContent::Failed(message));
	}

	/// Latest engine status line, if one arrived since the last input.
	pub fn status_message(&self) -> Option<&str> {
		self.status_message.as_deref()
	}

	/// Returns the engine's one-shot `:q` request exactly once.
	pub fn take_quit_request(&mut self) -> bool {
		if !self.quit_requested || self.quit_notified {
			return false;
		}
		self.quit_notified = true;
		true
	}

	/// Request a fresh document snapshot. The next projected `document_text`
	/// updates [`Self::cached_content`]. Calls coalesce while a request is
	/// live, but a request issued while one is pending marks the reply stale so
	/// [`Self::poll`] immediately re-queries — the cache always converges on
	/// the state after the last forwarded input.
	pub fn request_content(&mut self) {
		if self.text_query_pending {
			self.content_dirty = true;
		} else if self.send(EditorCommand::QueryDocumentText) {
			self.text_query_pending = true;
			self.content_dirty = false;
		} else {
			// The query never left; retry from the next request or reply.
			self.content_dirty = true;
		}
	}

	pub fn replace_content(&mut self, text: &str) {
		// Optimistic: the engine confirms via the queued query, but Run/Save
		// must never observe a cleared document in the interim.
		self.document_text = text.to_owned();
		self.send(EditorCommand::ReplaceDocument {
			text: text.to_owned(),
			language: Some("sql".to_owned()),
			// File-path-shaped identity so libaryon's language services
			// (semantic highlighting, diagnostics) run for this unsaved
			// in-memory document.
			virtual_path: Some("statement.sql".to_owned()),
		});
		self.request_content();
	}

	/// Translate the code pane's pixel size into the engine's cell grid.
	/// Callers pass the actual pane rectangle (see `sync_code_editor_viewport`).
	pub fn resize(&self, width: f32, height: f32) {
		let columns = (width / metrics::CELL_WIDTH)
			.floor()
			.clamp(1.0, u16::MAX as f32) as u16;
		let rows = (height / metrics::LINE_HEIGHT)
			.floor()
			.clamp(1.0, u16::MAX as f32) as u16;
		self.send(EditorCommand::Resize(columns, rows));
	}

	pub fn subscription(&self) -> Subscription<EditorMsg> {
		event::listen_with(|event, status, _window| match (event, status) {
			// Only keys no other widget consumed: a captured key was typed
			// into a text_input (connection name, statement title, …) and must
			// not also edit the SQL document.
			(
				// `modified_key`, not `key`: iced's `key` is the unshifted
				// layout key (Shift+; arrives as ';'), while the engine's
				// keymap expects the produced character (':', 'A', '"', …)
				// with Ctrl kept as a modifier flag — which is exactly
				// `modified_key`'s contract.
				Event::Keyboard(keyboard::Event::KeyPressed {
					modified_key,
					modifiers,
					..
				}),
				status,
			) => {
				if std::env::var_os("POLARITON_INPUT_TRACE").is_some() {
					eprintln!(
						"[input-trace] modified_key={modified_key:?} modifiers={modifiers:?} status={status:?}"
					);
				}
				if status != event::Status::Ignored {
					return None;
				}
				// The application owns these execution shortcuts. Do not also send
				// their Enter key through libaryon's insert-mode keymap.
				if matches!(
					modified_key.as_ref(),
					Key::Named(keyboard::key::Named::Enter)
				) && (modifiers.shift() || modifiers.control())
				{
					return None;
				}
				key_code(&modified_key).map(|code| {
					EditorMsg::Input(InputEvent::Key {
						code,
						modifiers: map_modifiers(modifiers),
					})
				})
			}
			_ => None,
		})
	}

	/// Drain every available projection and retain only the latest one.
	/// This is deliberately called from Iced's update path, never blocking it.
	pub fn poll(&mut self) -> bool {
		let mut changed = false;
		while let Ok(viewport) = self.viewports.try_recv() {
			self.quit_requested |= viewport.should_quit;
			if let Some(message) = viewport.status_message.as_ref() {
				self.status_message = Some(message.clone());
			}
			if let Some(document_text) = viewport.document_text.as_ref() {
				self.document_text = document_text.clone();
				self.text_query_pending = false;
				if self.content_dirty {
					// Input landed after this query was issued; chase
					// it so the cache converges on the latest state.
					self.request_content();
				}
			}
			self.viewport = Some(viewport);
			changed = true;
		}
		// Ordered causality, drained whole every poll: unlike projections these
		// must never be coalesced. Reconciliation runs after the viewport above
		// is current, so a disposal that outruns its projection cannot blank a
		// pane that is still on screen.
		let events = self
			.host_events
			.as_ref()
			.map(|events| events.try_iter().collect::<Vec<_>>())
			.unwrap_or_default();
		changed |= !events.is_empty();
		self.panels.reconcile(&events, self.viewport.as_ref());
		changed
	}

	pub fn update(&mut self, message: EditorMsg) -> Task<EditorMsg> {
		if self.quit_requested {
			return Task::none();
		}
		match message {
			EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char('v'),
				modifiers,
			}) if modifiers.contains(Modifiers::CONTROL) && self.is_insert_mode() => {
				return iced::clipboard::read()
					.map(|text| EditorMsg::Paste(text.unwrap_or_default()));
			}
			EditorMsg::Input(input) => {
				if std::env::var_os("POLARITON_INPUT_TRACE").is_some() {
					eprintln!("[input-trace] forwarding to engine: {input:?}");
				}
				match self.input.try_send(input) {
					Ok(()) => {}
					Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return Task::none(),
					Err(error) => {
						eprintln!("Editor engine rejected input: {error}");
						return Task::none();
					}
				}
				self.status_message = None;
				self.request_content();
			}
			EditorMsg::Paste(text) => {
				match self.input.try_send(InputEvent::Paste(text)) {
					Ok(()) => {}
					Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return Task::none(),
					Err(error) => {
						eprintln!("Editor engine rejected paste: {error}");
						return Task::none();
					}
				}
				self.status_message = None;
				self.request_content();
			}
			EditorMsg::Scroll(delta) => {
				self.send(EditorCommand::ScrollViewport(delta));
			}
			EditorMsg::MinimapGoto(line) => {
				self.send(EditorCommand::ScrollViewportToLine(line));
			}
			EditorMsg::PointerMoved(position) => {
				self.last_pointer = Some(position);
				// Continuous resize: every sample while a divider is held
				// becomes a boundary target, so the split tracks the cursor.
				if let Some(command) = self.divider_drag_command() {
					self.send(command);
				}
			}
			EditorMsg::Click => {
				if !self.begin_divider_drag() {
					self.click_at_pointer();
				}
			}
			EditorMsg::Release => self.drag = None,
		}
		Task::none()
	}

	pub fn view(&self) -> Element<'_, EditorMsg> {
		let Some(viewport) = self.viewport.as_ref() else {
			return container(
				text("Starting editor…")
					.font(EDITOR_FONT)
					.size(metrics::TEXT_SIZE),
			)
			.padding(12)
			.width(Length::Fill)
			.height(Length::Fill)
			.into();
		};
		let palette = viewport.ui_palette;
		let editor_background = color(palette.editor_background);
		// Every projected pane is drawn at its engine-assigned rect: layout
		// topology is Baryon's, pixels are Polariton's.
		let mut layers: Vec<Element<'_, EditorMsg>> = Vec::with_capacity(viewport.panes.len() + 2);
		layers.push(
			container(Space::new())
				.width(Length::Fill)
				.height(Length::Fill)
				.style(move |_| container::Style {
					background: Some(Background::Color(editor_background)),
					..Default::default()
				})
				.into(),
		);
		for pane in &viewport.panes {
			let focused = pane.pane_id == viewport.focused_pane_id;
			let content = match pane.host_panel.as_ref() {
				Some(panel) => self.panels.surface(panel, focused, palette),
				None => text_pane_surface(pane, palette),
			};
			layers.push(positioned(
				pane.rect.screen_col.get() as f32 * metrics::CELL_WIDTH,
				pane.rect.screen_row.get() as f32 * metrics::LINE_HEIGHT,
				pane.rect.width.get() as f32 * metrics::CELL_WIDTH,
				pane.rect.height.get() as f32 * metrics::LINE_HEIGHT,
				content,
			));
		}
		let panes = mouse_area(stack(layers).width(Length::Fill).height(Length::Fill))
			.on_move(EditorMsg::PointerMoved)
			.on_press(EditorMsg::Click)
			.on_release(EditorMsg::Release)
			.on_scroll(|delta| {
				let lines = match delta {
					mouse::ScrollDelta::Lines { y, .. } => (y * 3.0).round() as i32,
					mouse::ScrollDelta::Pixels { y, .. } => {
						(y / metrics::LINE_HEIGHT).round() as i32
					}
				};
				EditorMsg::Scroll(-lines)
			});
		let content: Element<'_, EditorMsg> = match prompt_line(viewport, self.status_message()) {
			Some(prompt) => stack![
				panes,
				container(command_bar(prompt, palette))
					.width(Length::Fill)
					.height(Length::Fill)
					.align_y(iced::alignment::Vertical::Bottom),
			]
			.into(),
			None => panes.into(),
		};
		container(content)
			.width(Length::Fill)
			.height(Length::Fill)
			.into()
	}

	pub fn shutdown(&mut self) {
		if let Some(engine) = self.engine.take() {
			let _ = engine.shutdown();
		}
	}

	fn send(&self, command: EditorCommand) -> bool {
		if self.quit_requested {
			return false;
		}
		match self.commands.try_send(command) {
			Ok(()) => true,
			Err(std::sync::mpsc::TrySendError::Disconnected(_)) => false,
			Err(error) => {
				eprintln!("Editor engine rejected command: {error}");
				false
			}
		}
	}

	/// Pointer pixel position -> engine cell coordinates.
	fn cell_at(point: iced::Point) -> (u32, u32) {
		(
			(point.x.max(0.0) / metrics::CELL_WIDTH) as u32,
			(point.y.max(0.0) / metrics::LINE_HEIGHT) as u32,
		)
	}

	/// Start a drag when the press lands in an engine-published divider hit
	/// rectangle. Returns false when it did not, so the press falls through to
	/// cursor placement.
	fn begin_divider_drag(&mut self) -> bool {
		let (Some(viewport), Some(point)) = (self.viewport.as_ref(), self.last_pointer) else {
			return false;
		};
		let (column, row) = Self::cell_at(point);
		let Some(divider) = viewport.dividers.iter().find(|divider| {
			let rect = divider.hit_rect;
			column >= rect.screen_col.get()
				&& column < rect.right().get()
				&& row >= rect.screen_row.get()
				&& row < rect.bottom().get()
		}) else {
			return false;
		};
		let boundary = match divider.axis {
			SplitDirection::Vertical => divider.visual_rect.screen_col.get(),
			SplitDirection::Horizontal => divider.visual_rect.screen_row.get(),
		};
		let position = match divider.axis {
			SplitDirection::Vertical => column,
			SplitDirection::Horizontal => row,
		};
		self.drag = Some(DividerDrag {
			before: divider.before,
			after: divider.after,
			axis: divider.axis,
			grab_offset: position.saturating_sub(boundary),
		});
		true
	}

	fn divider_drag_command(&self) -> Option<EditorCommand> {
		let drag = self.drag?;
		let (column, row) = Self::cell_at(self.last_pointer?);
		let position = match drag.axis {
			SplitDirection::Vertical => column,
			SplitDirection::Horizontal => row,
		};
		Some(EditorCommand::ResizePaneBoundary {
			before: drag.before,
			after: drag.after,
			axis: drag.axis,
			target: position.saturating_sub(drag.grab_offset),
		})
	}

	fn click_at_pointer(&self) {
		let Some(pointer) = self.last_pointer else {
			return;
		};
		let Some(pane) = self.viewport.as_ref().and_then(Viewport::focused_pane) else {
			return;
		};
		let row = ((pointer.y - metrics::VERTICAL_PADDING).max(0.0) / metrics::LINE_HEIGHT).floor()
			as usize;
		// `visible_rows` is `render_rows` minus the wrapped rows scrolled off
		// above the viewport, matching what `view` actually laid out.
		let Some(visible_row) = pane.visible_rows.get(row) else {
			return;
		};
		let column =
			((pointer.x - metrics::GUTTER_WIDTH).max(0.0) / metrics::CELL_WIDTH).floor() as u32;
		let target = CursorPosition::new(
			visible_row.line,
			DocDisplayCol::new(visible_row.start_col.get().saturating_add(column)),
		);
		self.send(EditorCommand::ClickCursor(target));
	}

	fn is_insert_mode(&self) -> bool {
		self.viewport
			.as_ref()
			.is_some_and(|viewport| viewport.input.mode == EditorMode::Insert)
	}
}

impl Drop for CodeEditor {
	fn drop(&mut self) {
		self.shutdown();
	}
}

struct ProjectedLine {
	doc_line: u64,
	fragments: Vec<StyledFragment>,
	show_gutter: bool,
}

#[derive(Clone)]
struct StyledFragment {
	text: String,
	foreground: Color,
	background: Option<Color>,
	underline: bool,
}

fn project_lines(
	pane: &libaryon::uast::projection::PaneViewport,
	default_foreground: Rgb,
	cursor_color: Rgb,
) -> Vec<ProjectedLine> {
	let mut lines = Vec::new();
	let mut current: Option<ProjectedLine> = None;
	let mut cursor_rendered = false;
	for token in &pane.tokens {
		if token.text.is_empty() {
			continue;
		}
		let token_line = token.absolute_start_line.get();
		if current
			.as_ref()
			.is_none_or(|line| line.doc_line != token_line)
		{
			if let Some(line) = current.take() {
				lines.push(line);
			}
			current = Some(ProjectedLine {
				doc_line: token_line,
				fragments: Vec::new(),
				show_gutter: true,
			});
		}
		let mut byte = token.absolute_start_byte.get();
		let mut line_number = token_line;
		for ch in String::from_utf8_lossy(&token.text).chars() {
			if ch == '\n' {
				if let Some(line) = current.take() {
					lines.push(line);
				}
				line_number = line_number.saturating_add(1);
				current = Some(ProjectedLine {
					doc_line: line_number,
					fragments: Vec::new(),
					show_gutter: true,
				});
				byte = byte.saturating_add(1);
				continue;
			}
			let byte_len = ch.len_utf8() as u64;
			let foreground = pane
				.highlights
				.iter()
				.rev()
				.find(|highlight| highlight.start.get() <= byte && byte < highlight.end.get())
				.and_then(|highlight| pane.theme_colors[highlight.category as usize])
				.unwrap_or(default_foreground);
			let foreground = if token.is_folded {
				cursor_color
			} else {
				foreground
			};
			let selected = pane.selection_ranges.iter().any(|(start, end)| {
				start.get() <= byte.saturating_add(byte_len).saturating_sub(1) && end.get() >= byte
			});
			let cursor = pane.cursor_abs_byte.get() == byte;
			cursor_rendered |= cursor;
			let underline = pane.diagnostics.iter().any(|diagnostic| {
				diagnostic.start.get() < byte.saturating_add(byte_len)
					&& diagnostic.end.get() > byte
			});
			let background = if cursor {
				Some(color(cursor_color))
			} else if selected {
				Some(Color::from_rgb8(55, 78, 104))
			} else if token.is_folded {
				Some(Color {
					a: 0.22,
					..color(cursor_color)
				})
			} else {
				None
			};
			push_fragment(
				current.as_mut().expect("line initialized"),
				ch.to_string(),
				color(foreground),
				background,
				underline,
			);
			byte = byte.saturating_add(byte_len);
		}
	}
	if let Some(line) = current {
		lines.push(line);
	}
	// The placeholder must exist before the cursor fallback runs: an empty
	// document has no tokens, and the cursor still needs a line to land on.
	if lines.is_empty() {
		lines.push(ProjectedLine {
			doc_line: pane.viewport_start_line.get(),
			fragments: Vec::new(),
			show_gutter: true,
		});
	}
	if !cursor_rendered {
		let cursor_line = pane.cursor_abs_pos.line.get();
		if let Some(line) = lines.iter_mut().find(|line| line.doc_line == cursor_line) {
			push_fragment(
				line,
				" ".to_owned(),
				color(default_foreground),
				Some(color(cursor_color)),
				false,
			);
		}
	}
	lines
}

/// Split physical lines using the engine's visual-row projection. This keeps
/// Iced's text output aligned with libaryon's wrapping and scroll anchor rather
/// than letting the frontend independently decide where a line wraps.
///
/// `visible_rows` (not `render_rows`) drives the split: when the viewport
/// starts partway through a wrapped line, that line's scrolled-off rows are
/// absent and their characters must be dropped, not rendered.
fn wrap_projected_lines(
	lines: Vec<ProjectedLine>,
	pane: &libaryon::uast::projection::PaneViewport,
) -> Vec<ProjectedLine> {
	lines
		.into_iter()
		.flat_map(|line| {
			let mut starts = pane
				.visible_rows
				.iter()
				.filter(|row| row.line.get() == line.doc_line)
				.map(|row| row.start_col.get())
				.collect::<Vec<_>>();
			starts.sort_unstable();
			starts.dedup();
			wrap_line_at_starts(line, &starts)
		})
		.collect()
}

fn wrap_line_at_starts(line: ProjectedLine, starts: &[u32]) -> Vec<ProjectedLine> {
	if starts.is_empty() {
		return vec![line];
	}
	let mut wrapped = starts
		.iter()
		.map(|start| ProjectedLine {
			doc_line: line.doc_line,
			fragments: Vec::new(),
			// A row starting mid-line is a wrap continuation (or a scrolled
			// partial line); only the physical line start carries the number.
			show_gutter: line.show_gutter && *start == 0,
		})
		.collect::<Vec<_>>();
	let mut column = 0u32;
	for fragment in line.fragments {
		for ch in fragment.text.chars() {
			let index = starts.partition_point(|start| *start <= column);
			if let Some(index) = index.checked_sub(1) {
				push_fragment(
					&mut wrapped[index],
					ch.to_string(),
					fragment.foreground,
					fragment.background,
					fragment.underline,
				);
			}
			// Columns before the first visible row are scrolled off above the
			// viewport; skip them without emitting anything.
			column = advance_display_column(column, ch);
		}
	}
	wrapped
}

fn advance_display_column(column: u32, ch: char) -> u32 {
	use libaryon::core::TAB_SIZE;
	if ch == '\t' {
		column.saturating_add(TAB_SIZE - (column % TAB_SIZE))
	} else {
		column.saturating_add(1)
	}
}

fn push_fragment(
	line: &mut ProjectedLine,
	text: String,
	foreground: Color,
	background: Option<Color>,
	underline: bool,
) {
	if let Some(previous) = line.fragments.last_mut()
		&& previous.foreground == foreground
		&& previous.background == background
		&& previous.underline == underline
	{
		previous.text.push_str(&text);
	} else {
		line.fragments.push(StyledFragment {
			text,
			foreground,
			background,
			underline,
		});
	}
}

fn gutter_number(pane: &libaryon::uast::projection::PaneViewport, line: u64) -> String {
	pane.gutter_line_numbers
		.iter()
		.find(|entry| entry.line.get() == line)
		.map(|entry| entry.number.to_string())
		.unwrap_or_else(|| (line + 1).to_string())
}

fn color(color: Rgb) -> Color {
	Color::from_rgb8(color.r, color.g, color.b)
}

/// Polariton's side of the host-panel seam.
///
/// Two inputs with different guarantees are reconciled here. The viewport is
/// *state*: `panes[*].host_panel` is the current set of visible references, and
/// is authoritative for what exists. The host-event stream is *ordered
/// causality*: it says what happened, exactly once, and is never coalesced.
///
/// Backing objects (DataFrames, plot kernels) live here and never enter
/// libaryon.
#[derive(Default)]
pub struct PanelRegistry {
	panels: std::collections::HashMap<HostPanelId, PanelEntry>,
	/// Disposals observed but not yet confirmed by a projection. Releasing
	/// immediately would blank the pane for a frame whenever the event arrives
	/// before the viewport that stops referencing it.
	pending_disposal: std::collections::HashSet<HostPanelId>,
}

struct PanelEntry {
	revision: PanelRevision,
	content: PanelContent,
}

/// Host-owned backing state for a panel. None of this is representable in
/// libaryon: the engine only knows the panel exists.
enum PanelContent {
	/// Projected or restored, but the frontend has no content for it — either
	/// a panel from a persisted layout or one seen before its object exists.
	Unresolved,
	Running,
	Table(Box<DataFrame>),
	Failed(String),
}

impl PanelRegistry {
	/// Apply ordered events, then reconcile against the latest projection.
	/// Called from `poll`, after the viewport has been updated.
	fn reconcile(&mut self, events: &[HostEvent], viewport: Option<&Viewport>) {
		for event in events {
			match event {
				HostEvent::PanelRebound { panel_id, revision } => {
					// Only the revision syncs here. Polariton is the sole
					// producer of RefreshHostPanel and sets the new content
					// itself, so clobbering it would race its own request.
					self.panels
						.entry(*panel_id)
						.or_insert(PanelEntry {
							revision: *revision,
							content: PanelContent::Unresolved,
						})
						.revision = *revision;
				}
				HostEvent::PanelDisposed { panel_id } => {
					self.pending_disposal.insert(*panel_id);
				}
				HostEvent::PanelRestored {
					panel_id,
					descriptor: _,
				} => {
					self.panels.insert(
						*panel_id,
						PanelEntry {
							revision: PanelRevision::new(0),
							content: PanelContent::Unresolved,
						},
					);
				}
				HostEvent::UnhandledInput { .. } => {}
			}
		}

		let Some(viewport) = viewport else {
			return;
		};
		let visible = viewport
			.panes
			.iter()
			.filter_map(|pane| pane.host_panel.as_ref())
			.map(|panel| panel.panel_id)
			.collect::<std::collections::HashSet<_>>();

		// Release only once the projection agrees the panel is gone, so a
		// disposal that outruns its viewport never blanks a live pane.
		self.pending_disposal.retain(|panel_id| {
			if visible.contains(panel_id) {
				return true;
			}
			self.panels.remove(panel_id);
			false
		});

		// A projected id we have never seen is restore-like: render a
		// placeholder rather than nothing. An explicit `PanelRestored` can make
		// this deliberate later.
		for pane in &viewport.panes {
			if let Some(panel) = pane.host_panel.as_ref() {
				self.panels.entry(panel.panel_id).or_insert(PanelEntry {
					revision: panel.revision,
					content: PanelContent::Unresolved,
				});
			}
		}
	}

	fn set_content(&mut self, panel_id: HostPanelId, content: PanelContent) {
		let entry = self.panels.entry(panel_id).or_insert(PanelEntry {
			revision: PanelRevision::new(0),
			content: PanelContent::Unresolved,
		});
		entry.content = content;
	}

	/// Native surface for a host panel. Backing objects will replace the
	/// placeholder without touching the topology seam.
	fn surface<'a>(
		&'a self,
		panel: &'a libaryon::uast::projection::HostPanelViewport,
		focused: bool,
		palette: UiPalette,
	) -> Element<'a, EditorMsg> {
		let content = self
			.panels
			.get(&panel.panel_id)
			.map(|entry| &entry.content)
			.unwrap_or(&PanelContent::Unresolved);

		let inner: Element<'a, EditorMsg> = match content {
			PanelContent::Table(frame) => Table::new(frame, 0).into(),
			PanelContent::Running => panel_message(
				format!("{}\n\nrunning…", panel.title),
				color(palette.preview_text),
			),
			PanelContent::Failed(message) => panel_message(
				format!("{}\n\n{message}", panel.title),
				color(palette.status_text),
			),
			PanelContent::Unresolved => panel_message(
				format!(
					"{}\n\n{} unavailable (revision {})\nrerun to populate",
					panel.title,
					panel.kind,
					panel.revision.get()
				),
				color(palette.preview_text),
			),
		};
		let surface = color(palette.preview_surface);
		let border = if focused {
			color(palette.accent)
		} else {
			color(palette.divider_surface)
		};
		container(inner)
			.padding(4)
			.width(Length::Fill)
			.height(Length::Fill)
			.style(move |_| container::Style {
				background: Some(Background::Color(surface)),
				border: iced::Border {
					color: border,
					width: if focused { 2.0 } else { 1.0 },
					radius: 0.0.into(),
				},
				..Default::default()
			})
			.into()
	}
}

fn panel_message<'a>(body: String, tint: Color) -> Element<'a, EditorMsg> {
	container(
		text(body)
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.color(tint),
	)
	.padding(8)
	.into()
}

/// One text pane's surface: gutter, styled rows, and minimap. Positioned by the
/// caller at the engine's rect.
fn text_pane_surface<'a>(
	pane: &'a libaryon::uast::projection::PaneViewport,
	palette: UiPalette,
) -> Element<'a, EditorMsg> {
	let lines = wrap_projected_lines(
		project_lines(pane, palette.editor_text, palette.accent),
		pane,
	);
	let gutter_background = color(palette.gutter_surface);
	let gutter_text = color(palette.gutter_text);
	let cursor_lines = &pane.cursor_lines;
	let rows = column(lines.into_iter().map(|line| {
		let number = if line.show_gutter {
			gutter_number(pane, line.doc_line)
		} else {
			String::new()
		};
		let number_color = if cursor_lines
			.iter()
			.any(|cursor| cursor.get() == line.doc_line)
		{
			color(palette.gutter_accent)
		} else {
			gutter_text
		};
		let fragments = line
			.fragments
			.into_iter()
			.map(|fragment| {
				let StyledFragment {
					text,
					foreground,
					background,
					underline,
				} = fragment;
				let fragment = span::<(), iced::Font>(text)
					.color(foreground)
					.underline(underline);
				match background {
					Some(background) => fragment.background(Background::Color(background)),
					None => fragment,
				}
			})
			.collect::<Vec<_>>();
		row![
			container(
				text(number)
					.font(EDITOR_FONT)
					.size(metrics::TEXT_SIZE)
					.color(number_color)
			)
			.width(Length::Fixed(metrics::GUTTER_WIDTH))
			// `container` makes its child fluid by default. Pinning this
			// prevents the enclosing column from distributing the pane's
			// spare height across every projected editor row.
			.height(Length::Fixed(metrics::LINE_HEIGHT))
			.padding([0, 8])
			.style(move |_| container::Style {
				background: Some(Background::Color(gutter_background)),
				..Default::default()
			}),
			rich_text(fragments)
				.font(EDITOR_FONT)
				.size(metrics::TEXT_SIZE)
				.height(Length::Fixed(metrics::LINE_HEIGHT))
				// `Pixels`, not a bare f32: `From<f32> for LineHeight` is
				// Relative (a multiplier of the font size), which made
				// every line 22× tall and clipped all glyphs out of the
				// fixed-height row.
				.line_height(iced::Pixels(metrics::LINE_HEIGHT)),
		]
		.spacing(0)
		.height(Length::Fixed(metrics::LINE_HEIGHT))
		.into()
	}))
	.width(Length::Fill)
	.height(Length::Shrink);
	let editor_rows = mouse_area(
		container(rows)
			.padding([metrics::VERTICAL_PADDING, 0.0])
			.width(Length::Fill)
			.height(Length::Fill),
	)
	.on_move(EditorMsg::PointerMoved)
	.on_press(EditorMsg::Click)
	.on_scroll(|delta| {
		let lines = match delta {
			mouse::ScrollDelta::Lines { y, .. } => (y * 3.0).round() as i32,
			mouse::ScrollDelta::Pixels { y, .. } => (y / metrics::LINE_HEIGHT).round() as i32,
		};
		EditorMsg::Scroll(-lines)
	});
	let editor_content: Element<'_, EditorMsg> = if let Some(minimap) = pane.minimap.as_ref() {
		row![editor_rows, minimap_view(minimap, palette)]
			.height(Length::Fill)
			.into()
	} else {
		editor_rows.into()
	};
	editor_content
}

/// Place a fixed-size element at a pixel offset within the workspace.
fn positioned<'a>(
	x: f32,
	y: f32,
	width: f32,
	height: f32,
	content: Element<'a, EditorMsg>,
) -> Element<'a, EditorMsg> {
	column![
		Space::new().height(Length::Fixed(y)),
		row![
			Space::new().width(Length::Fixed(x)),
			container(content)
				.width(Length::Fixed(width))
				.height(Length::Fixed(height)),
		],
	]
	.into()
}

/// The bottom-bar text for the current engine state: `:` command, `/` search,
/// and pending confirm questions all use the same line. Non-Normal modes
/// without a prompt still announce themselves vim-style, since the block cursor
/// is otherwise the only modal feedback. An active prompt outranks the engine's
/// latest status line, which outranks the mode indicator — the vim cmdline
/// ordering.
fn prompt_line(viewport: &Viewport, status_message: Option<&str>) -> Option<String> {
	match viewport.input.mode {
		EditorMode::Command => Some(format!(":{}", viewport.input.prompt_buffer)),
		EditorMode::Search => Some(format!("/{}", viewport.input.prompt_buffer)),
		EditorMode::Confirm => Some(
			viewport
				.confirm_prompt
				.clone()
				.unwrap_or_else(|| "Confirm? [y/n]".to_owned()),
		),
		EditorMode::Insert | EditorMode::Visual { .. } | EditorMode::Normal
			if status_message.is_some() =>
		{
			status_message.map(str::to_owned)
		}
		EditorMode::Insert => Some("-- INSERT --".to_owned()),
		EditorMode::Visual { kind, .. } => Some(
			match kind {
				VisualKind::Char => "-- VISUAL --",
				VisualKind::Line => "-- VISUAL LINE --",
				VisualKind::Block => "-- VISUAL BLOCK --",
			}
			.to_owned(),
		),
		EditorMode::Normal => None,
	}
}

/// Project libaryon's prompt line (`:` command, `/` search, or a confirm
/// question). The engine remains responsible for editing and executing the
/// buffer; this is intentionally display-only so its modal key handling
/// continues to flow through the normal subscription.
fn command_bar(prompt: String, palette: UiPalette) -> Element<'static, EditorMsg> {
	container(
		text(prompt)
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.color(color(palette.editor_text)),
	)
	.width(Length::Fill)
	.height(Length::Fixed(metrics::LINE_HEIGHT))
	.padding([0, 8])
	.style(move |_| container::Style {
		background: Some(Background::Color(color(palette.gutter_surface))),
		..Default::default()
	})
	.into()
}

const MINIMAP_BANDS: usize = 96;

fn minimap_view(snapshot: &MinimapSnapshot, palette: UiPalette) -> Element<'static, EditorMsg> {
	let overlay = snapshot.overlay();
	let density = minimap_density(snapshot);
	let document_lines = overlay.doc_line_count.get().max(1);
	let viewport_start = overlay.viewport_start_line.get();
	let viewport_end = overlay.viewport_end_line.get();
	let search_bands = &overlay.search_bands;
	let rows = column(density.into_iter().enumerate().map(|(index, density)| {
		let line = ((index as u128 * document_lines as u128) / MINIMAP_BANDS as u128) as u64;
		let next_line =
			(((index + 1) as u128 * document_lines as u128) / MINIMAP_BANDS as u128) as u64;
		let in_viewport = line < viewport_end && next_line.max(line + 1) > viewport_start;
		let search_index = ((index * search_bands.len()) / MINIMAP_BANDS)
			.min(search_bands.len().saturating_sub(1));
		let has_search = search_bands.get(search_index).copied().unwrap_or_default() > 0;
		let shade = if has_search {
			palette.search_highlight
		} else {
			palette.minimap_accent
		};
		let alpha = if density == 0 {
			0.0
		} else {
			0.15 + (density as f32 / 255.0) * 0.85
		};
		let background = if in_viewport {
			Color {
				a: 0.45,
				..color(palette.accent)
			}
		} else {
			Color {
				a: alpha,
				..color(shade)
			}
		};
		button(Space::new().height(Length::Fixed(2.0)))
			.padding(0)
			.width(Length::Fixed(72.0))
			.on_press(EditorMsg::MinimapGoto(DocLine::new(line)))
			.style(move |_, _| button::Style {
				background: Some(Background::Color(background)),
				..Default::default()
			})
			.into()
	}))
	.spacing(0);
	container(rows)
		.padding([8, 4])
		.width(Length::Fixed(80.0))
		.height(Length::Fill)
		.style(move |_| container::Style {
			background: Some(Background::Color(color(palette.minimap_surface))),
			..Default::default()
		})
		.into()
}

fn minimap_density(snapshot: &MinimapSnapshot) -> Vec<u8> {
	let source = match snapshot {
		MinimapSnapshot::Preview(preview) => preview
			.rows
			.iter()
			.map(|row| {
				((u32::from(row.logical_width) * 255) / u32::from(preview.max_columns.max(1))) as u8
			})
			.collect::<Vec<_>>(),
		MinimapSnapshot::Overview(overview) | MinimapSnapshot::ByteFallback(overview) => {
			overview.bands.clone()
		}
	};
	(0..MINIMAP_BANDS)
		.map(|index| {
			let start = (index * source.len()) / MINIMAP_BANDS;
			let end = (((index + 1) * source.len()) / MINIMAP_BANDS)
				.max(start + 1)
				.min(source.len());
			source
				.get(start..end)
				.unwrap_or_default()
				.iter()
				.copied()
				.max()
				.unwrap_or(0)
		})
		.collect()
}

fn map_modifiers(modifiers: keyboard::Modifiers) -> Modifiers {
	let mut mapped = Modifiers::NONE;
	if modifiers.shift() {
		mapped |= Modifiers::SHIFT;
	}
	if modifiers.control() {
		mapped |= Modifiers::CONTROL;
	}
	if modifiers.alt() {
		mapped |= Modifiers::ALT;
	}
	if modifiers.logo() {
		mapped |= Modifiers::SUPER;
	}
	mapped
}

fn key_code(key: &Key) -> Option<KeyCode> {
	use iced::keyboard::key::Named;
	Some(match key.as_ref() {
		Key::Character(value) => KeyCode::Char(value.chars().next()?),
		Key::Named(Named::Space) => KeyCode::Char(' '),
		Key::Named(Named::Backspace) => KeyCode::Backspace,
		Key::Named(Named::Enter) => KeyCode::Enter,
		Key::Named(Named::ArrowLeft) => KeyCode::Left,
		Key::Named(Named::ArrowRight) => KeyCode::Right,
		Key::Named(Named::ArrowUp) => KeyCode::Up,
		Key::Named(Named::ArrowDown) => KeyCode::Down,
		Key::Named(Named::Home) => KeyCode::Home,
		Key::Named(Named::End) => KeyCode::End,
		Key::Named(Named::PageUp) => KeyCode::PageUp,
		Key::Named(Named::PageDown) => KeyCode::PageDown,
		Key::Named(Named::Tab) => KeyCode::Tab,
		Key::Named(Named::Delete) => KeyCode::Delete,
		Key::Named(Named::Insert) => KeyCode::Insert,
		Key::Named(Named::Escape) => KeyCode::Esc,
		_ => return None,
	})
}

#[cfg(test)]
mod tests {
	use super::{CodeEditor, EditorCommand, EditorMsg, InputEvent, KeyCode, Modifiers, metrics};
	use std::thread;
	use std::time::{Duration, Instant};

	fn wait_for_content(editor: &mut CodeEditor, expected: &str) {
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.cached_content() != expected && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.cached_content(), expected);
	}

	fn send_keys(editor: &mut CodeEditor, keys: impl IntoIterator<Item = char>) {
		for key in keys {
			let _ = editor.update(EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char(key),
				modifiers: Modifiers::NONE,
			}));
		}
	}

	#[test]
	fn prompt_line_orders_prompt_above_status_above_mode() {
		use super::prompt_line;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		let viewport = editor.viewport.as_ref().expect("viewport");

		// Normal mode, no status: nothing to show.
		assert_eq!(prompt_line(viewport, None), None);
		// A status line outranks the (absent) mode indicator.
		assert_eq!(
			prompt_line(viewport, Some("No file name")).as_deref(),
			Some("No file name")
		);

		// An active `:` prompt outranks a pending status message.
		send_keys(&mut editor, [':', 'w']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|v| prompt_line(v, Some("stale")).as_deref() != Some(":w"))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let viewport = editor.viewport.as_ref().expect("viewport");
		assert_eq!(prompt_line(viewport, Some("stale")).as_deref(), Some(":w"));
	}

	fn wait_for_host_panel(editor: &mut CodeEditor) {
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|v| !v.panes.iter().any(|pane| pane.host_panel.is_some()))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
	}

	#[test]
	fn host_panels_render_as_unresolved_until_content_arrives() {
		use super::PanelContent;
		use libaryon::engine::HostPanelId;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.send(EditorCommand::OpenHostPanel {
			panel_id: HostPanelId::new(1),
			kind: "result-table".to_string(),
			title: "Results".to_string(),
		});
		wait_for_host_panel(&mut editor);

		// A panel projected before its backing object exists is restore-like:
		// the registry adopts it and renders a placeholder rather than nothing.
		// Assert adoption explicitly — `is_unresolved` alone is true for ids
		// the registry has never seen, so it would pass vacuously.
		assert!(
			editor.panels.panels.contains_key(&HostPanelId::new(1)),
			"the registry adopts an unknown projected panel"
		);
		assert!(matches!(
			editor.panels.panels[&HostPanelId::new(1)].content,
			PanelContent::Unresolved
		));
		let viewport = editor.viewport.as_ref().expect("viewport");
		let panel = viewport
			.panes
			.iter()
			.find_map(|pane| pane.host_panel.as_ref())
			.expect("panel projected");
		assert_eq!(panel.kind, "result-table");
	}

	#[test]
	fn disposal_releases_only_after_the_projection_agrees() {
		use libaryon::engine::{HostEvent, HostPanelId};

		let panel_id = HostPanelId::new(2);
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.send(EditorCommand::OpenHostPanel {
			panel_id,
			kind: "dashboard".to_string(),
			title: "Dashboard".to_string(),
		});
		wait_for_host_panel(&mut editor);
		assert!(editor.panels.panels.contains_key(&panel_id));

		// A disposal that outruns its projection must not release yet: the
		// pane is still on screen, and dropping backing state here would blank
		// it for a frame.
		let viewport = editor.viewport.take().expect("viewport");
		editor
			.panels
			.reconcile(&[HostEvent::PanelDisposed { panel_id }], Some(&viewport));
		assert!(
			editor.panels.panels.contains_key(&panel_id),
			"release must wait for the projection to stop referencing the panel"
		);
		editor.viewport = Some(viewport);

		// Once the projection no longer shows it, the backing state goes.
		editor.send(EditorCommand::ClosePane);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.panels.panels.contains_key(&panel_id) && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			!editor.panels.panels.contains_key(&panel_id),
			"the panel is released once no pane references it"
		);
	}

	#[test]
	fn a_rerun_during_pending_disposal_reopens_rather_than_refreshing() {
		use super::{PanelContent, RESULT_PANEL};
		use libaryon::engine::HostEvent;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.begin_query();
		wait_for_host_panel(&mut editor);

		// Reproduce the window the deferred-release rule opens: the disposal
		// event has been drained, but the projection still shows the panel, so
		// the registry still holds it.
		let viewport = editor.viewport.take().expect("viewport");
		editor.panels.reconcile(
			&[HostEvent::PanelDisposed {
				panel_id: RESULT_PANEL,
			}],
			Some(&viewport),
		);
		editor.viewport = Some(viewport);
		assert!(editor.panels.panels.contains_key(&RESULT_PANEL));
		assert!(editor.panels.pending_disposal.contains(&RESULT_PANEL));

		// A rerun here must reopen, not refresh a pane the engine has dropped.
		editor.begin_query();
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|v| !v.panes.iter().any(|pane| pane.host_panel.is_some()))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.is_some_and(|v| v.panes.iter().any(|pane| pane.host_panel.is_some())),
			"the results panel exists after a rerun during pending disposal"
		);
		assert!(matches!(
			editor.panels.panels[&RESULT_PANEL].content,
			PanelContent::Running
		));
	}

	#[test]
	fn divider_drag_uses_engine_published_hit_rects() {
		use libaryon::engine::HostPanelId;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.send(EditorCommand::OpenHostPanel {
			panel_id: HostPanelId::new(3),
			kind: "result-table".to_string(),
			title: "Results".to_string(),
		});
		wait_for_host_panel(&mut editor);

		let (before, after, column, row) = {
			let viewport = editor.viewport.as_ref().expect("viewport");
			let divider = viewport.dividers.first().expect("split projects a divider");
			(
				divider.before,
				divider.after,
				divider.hit_rect.screen_col.get(),
				divider.hit_rect.screen_row.get() + 1,
			)
		};
		let point = |column: u32, row: u32| {
			iced::Point::new(
				column as f32 * metrics::CELL_WIDTH,
				row as f32 * metrics::LINE_HEIGHT,
			)
		};
		editor.last_pointer = Some(point(column, row));
		assert!(
			editor.begin_divider_drag(),
			"a press inside the published hit rect starts a drag"
		);
		let EditorCommand::ResizePaneBoundary {
			before: dragged_before,
			after: dragged_after,
			target,
			..
		} = editor.divider_drag_command().expect("dragging")
		else {
			panic!("divider drag must produce a boundary resize");
		};
		assert_eq!((dragged_before, dragged_after), (before, after));

		editor.last_pointer = Some(point(column - 8, row));
		let EditorCommand::ResizePaneBoundary { target: moved, .. } =
			editor.divider_drag_command().expect("still dragging")
		else {
			panic!("divider drag must produce a boundary resize");
		};
		assert_eq!(moved, target - 8);

		// Away from any divider, a press falls through to cursor placement.
		editor.drag = None;
		editor.last_pointer = Some(point(2, row));
		assert!(!editor.begin_divider_drag());
	}

	#[test]
	fn repeated_runs_reuse_one_result_panel_and_replace_its_contents() {
		use super::{PanelContent, RESULT_PANEL};

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");

		// First run opens the panel and shows it running.
		editor.begin_query();
		wait_for_host_panel(&mut editor);
		let panes_after_first = editor
			.viewport
			.as_ref()
			.map(|v| v.panes.len())
			.expect("viewport");
		assert!(matches!(
			editor.panels.panels[&RESULT_PANEL].content,
			PanelContent::Running
		));

		// Completion attaches the table.
		let frame = polars::df!("n" => [1i64, 2, 3]).expect("frame");
		editor.set_query_result(frame);
		assert!(matches!(
			editor.panels.panels[&RESULT_PANEL].content,
			PanelContent::Table(_)
		));

		// A second run reuses the identity: revision advances, no pane
		// explosion, and the surface returns to running.
		let first_revision = editor.result_revision;
		editor.begin_query();
		for _ in 0..40 {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(editor.result_revision > first_revision);
		assert_eq!(
			editor.viewport.as_ref().map(|v| v.panes.len()),
			Some(panes_after_first),
			"reusing the panel identity must not open another pane"
		);
		assert!(matches!(
			editor.panels.panels[&RESULT_PANEL].content,
			PanelContent::Running
		));

		// A failure keeps the panel and shows the error in place.
		editor.set_query_error("syntax error at or near \"slect\"".to_owned());
		let PanelContent::Failed(message) = &editor.panels.panels[&RESULT_PANEL].content else {
			panic!("a failed query retains its panel");
		};
		assert!(message.contains("slect"));
	}

	#[test]
	fn running_again_after_closing_the_result_pane_recreates_it() {
		use super::{PanelContent, RESULT_PANEL};

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.begin_query();
		wait_for_host_panel(&mut editor);
		editor.set_query_result(polars::df!("n" => [1i64]).expect("frame"));

		// Close the results pane specifically — `ClosePane` acts on the focused
		// pane, and opening a panel does not steal focus, so focus it first or
		// this closes the editor instead and proves nothing.
		let panel_pane = editor
			.viewport
			.as_ref()
			.and_then(|v| {
				v.panes
					.iter()
					.find(|pane| pane.host_panel.is_some())
					.map(|pane| pane.pane_id)
			})
			.expect("panel pane");
		editor.send(EditorCommand::FocusPane(panel_pane));
		editor.send(EditorCommand::ClosePane);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|v| v.panes.iter().any(|pane| pane.host_panel.is_some()))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.is_some_and(|v| !v.panes.iter().any(|pane| pane.host_panel.is_some())),
			"the results pane is actually closed"
		);

		// Disposal is deferred until a projection agrees, so a rerun issued
		// before reconciliation may still see the panel in the registry — the
		// case where a naive "refresh if known" would address a pane the engine
		// has already dropped.
		editor.begin_query();

		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|v| !v.panes.iter().any(|pane| pane.host_panel.is_some()))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.is_some_and(|v| v.panes.iter().any(|pane| pane.host_panel.is_some())),
			"a rerun after closing the pane must recreate it"
		);
		assert!(matches!(
			editor.panels.panels[&RESULT_PANEL].content,
			PanelContent::Running
		));
	}

	#[test]
	fn engine_status_messages_surface_and_clear_on_the_next_input() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");

		// `:w` on an in-memory statement is refused by the engine; the refusal
		// must reach the bridge's status line.
		send_keys(&mut editor, [':', 'w']);
		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Enter,
			modifiers: Modifiers::NONE,
		}));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.status_message() != Some("No file name") && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.status_message(), Some("No file name"));

		// The next input retires the message, vim-style.
		send_keys(&mut editor, ['i']);
		assert_eq!(editor.status_message(), None);
	}

	#[test]
	fn quit_request_is_observable_once_and_stops_editor_input() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		send_keys(&mut editor, [':', 'q']);
		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Enter,
			modifiers: Modifiers::NONE,
		}));

		let deadline = Instant::now() + Duration::from_secs(10);
		while !editor.take_quit_request() && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor.quit_requested,
			"`:q` must reach the Polariton bridge"
		);
		assert!(
			editor.quit_notified,
			"the close request must be consumed once"
		);
		assert!(!editor.take_quit_request(), "quit requests are one-shot");

		// The engine has already exited, so this must be ignored locally rather
		// than attempting to send on its closed input channel.
		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Char('x'),
			modifiers: Modifiers::NONE,
		}));
	}

	#[test]
	fn empty_document_still_renders_a_cursor_block() {
		use super::project_lines;

		let mut editor = CodeEditor::new("");
		wait_for_content(&mut editor, "");
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none()
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let viewport = editor.viewport.as_ref().expect("viewport");
		let pane = viewport.focused_pane().expect("focused pane");
		let palette = viewport.ui_palette;
		let lines = project_lines(pane, palette.editor_text, palette.accent);
		assert!(
			lines
				.iter()
				.any(|line| line.fragments.iter().any(|f| f.background.is_some())),
			"an empty document must still project a cursor block"
		);
	}

	/// Replicates `view`'s projection pipeline (project + wrap) and returns the
	/// concatenated visible text per line, so tests can assert what the widget
	/// tree would actually display.
	fn rendered_lines(editor: &CodeEditor) -> Vec<String> {
		use super::{project_lines, wrap_projected_lines};
		let viewport = editor.viewport.as_ref().expect("viewport");
		let pane = viewport.focused_pane().expect("focused pane");
		let palette = viewport.ui_palette;
		wrap_projected_lines(
			project_lines(pane, palette.editor_text, palette.accent),
			pane,
		)
		.into_iter()
		.map(|line| {
			line.fragments
				.into_iter()
				.map(|fragment| fragment.text)
				.collect::<String>()
		})
		.collect()
	}

	#[test]
	fn typed_text_survives_the_full_render_pipeline() {
		let mut editor = CodeEditor::new("");
		wait_for_content(&mut editor, "");
		send_keys(&mut editor, ['i', 'a', 'b']);
		wait_for_content(&mut editor, "ab");
		let deadline = Instant::now() + Duration::from_secs(10);
		while !rendered_lines(&editor)
			.iter()
			.any(|line| line.contains("ab"))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let lines = rendered_lines(&editor);
		assert!(
			lines.iter().any(|line| line.contains("ab")),
			"typed text must reach the rendered projection, got: {lines:?}"
		);
	}

	#[test]
	fn backspacing_an_in_memory_document_to_empty_keeps_it_editable() {
		let mut editor = CodeEditor::new("");
		wait_for_content(&mut editor, "");
		send_keys(&mut editor, ['i', 's', 'e', 'l', 'e', 'c', 't']);
		wait_for_content(&mut editor, "select");

		for _ in 0..6 {
			let _ = editor.update(EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Backspace,
				modifiers: Modifiers::NONE,
			}));
		}
		wait_for_content(&mut editor, "");
		assert_ne!(editor.status_message(), Some("Loading cursor extent"));

		send_keys(&mut editor, ['x']);
		wait_for_content(&mut editor, "x");
	}

	#[test]
	fn in_memory_content_round_trips_through_the_engine() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.replace_content("select 2;");
		wait_for_content(&mut editor, "select 2;");
	}

	#[test]
	fn rapid_input_still_converges_the_cached_content() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		// Enter insert mode, then type without polling in between: the second
		// key lands while the first key's text query is in flight, so the
		// cache only converges if the stale reply is chased with a re-query.
		for ch in ['i', 'x'] {
			let _ = editor.update(EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char(ch),
				modifiers: Modifiers::NONE,
			}));
		}
		wait_for_content(&mut editor, "xselect 1;");
	}

	#[test]
	fn immediate_typing_after_replacing_an_in_memory_document_never_waits_for_extent() {
		let mut editor = CodeEditor::new("");
		for index in 0..32 {
			let statement = format!("select {index};");
			editor.replace_content(&statement);
			send_keys(&mut editor, ['i', 'x']);
			wait_for_content(&mut editor, &format!("x{statement}"));
			let _ = editor.update(EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Esc,
				modifiers: Modifiers::NONE,
			}));
			assert_ne!(
				editor.status_message(),
				Some("Loading cursor extent"),
				"replacement {index} left the in-memory cursor unresolved"
			);
		}
	}

	#[test]
	fn viewport_scroll_and_minimap_navigation_stay_engine_owned() {
		use super::EditorMsg;
		use libaryon::core::DocLine;

		let text = (0..80)
			.map(|line| format!("select {line};\n"))
			.collect::<String>();
		let mut editor = CodeEditor::new(&text);
		editor.resize(960.0, 110.0);
		wait_for_content(&mut editor, &text);

		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.minimap.is_none())
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.and_then(|viewport| viewport.focused_pane())
				.is_some_and(|pane| pane.minimap.is_some())
		);

		let _ = editor.update(EditorMsg::Scroll(3));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.viewport_start_line == DocLine::ZERO)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.and_then(|viewport| viewport.focused_pane())
				.is_some_and(|pane| pane.viewport_start_line > DocLine::ZERO)
		);

		let _ = editor.update(EditorMsg::MinimapGoto(DocLine::new(40)));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.viewport_start_line.get() < 35)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.and_then(|viewport| viewport.focused_pane())
				.is_some_and(|pane| pane.viewport_start_line.get() >= 35)
		);
	}

	#[test]
	fn engine_visual_rows_split_wrapped_text_without_repeating_the_gutter() {
		use super::{Color, ProjectedLine, StyledFragment, wrap_line_at_starts};

		let line = ProjectedLine {
			doc_line: 8,
			fragments: vec![StyledFragment {
				text: "select 123".to_owned(),
				foreground: Color::WHITE,
				background: None,
				underline: false,
			}],
			show_gutter: true,
		};
		let wrapped = wrap_line_at_starts(line, &[0, 4, 8]);
		assert_eq!(wrapped.len(), 3);
		assert_eq!(wrapped[0].fragments[0].text, "sele");
		assert_eq!(wrapped[1].fragments[0].text, "ct 1");
		assert_eq!(wrapped[2].fragments[0].text, "23");
		assert!(wrapped[0].show_gutter);
		assert!(!wrapped[1].show_gutter);
		assert!(!wrapped[2].show_gutter);
	}

	#[test]
	fn columns_scrolled_off_above_the_viewport_are_dropped_not_prepended() {
		use super::{Color, ProjectedLine, StyledFragment, wrap_line_at_starts};

		// The viewport starts partway through this wrapped line: its first two
		// visual rows (cols 0..4) are scrolled off, so `visible_rows` only
		// carries starts 4 and 8.
		let line = ProjectedLine {
			doc_line: 8,
			fragments: vec![StyledFragment {
				text: "select 123".to_owned(),
				foreground: Color::WHITE,
				background: None,
				underline: false,
			}],
			show_gutter: true,
		};
		let wrapped = wrap_line_at_starts(line, &[4, 8]);
		assert_eq!(wrapped.len(), 2);
		assert_eq!(wrapped[0].fragments[0].text, "ct 1");
		assert_eq!(wrapped[1].fragments[0].text, "23");
		// A mid-line row is never the physical line start, so no gutter number.
		assert!(!wrapped[0].show_gutter);
		assert!(!wrapped[1].show_gutter);
	}

	#[test]
	fn local_mouse_coordinates_become_engine_cursor_positions() {
		use super::EditorMsg;
		use iced::Point;

		let mut editor = CodeEditor::new("select 1;\nselect 2;\nselect 3;");
		wait_for_content(&mut editor, "select 1;\nselect 2;\nselect 3;");
		let _ = editor.update(EditorMsg::PointerMoved(Point::new(
			54.0 + 9.6 * 3.0,
			8.0 + 22.0,
		)));
		let _ = editor.update(EditorMsg::Click);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| {
				pane.cursor_abs_pos.line.get() != 1 || pane.cursor_abs_pos.col.get() < 3
			}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.and_then(|viewport| viewport.focused_pane())
				.is_some_and(|pane| pane.cursor_abs_pos.line.get() == 1
					&& pane.cursor_abs_pos.col.get() >= 3)
		);
	}

	#[test]
	fn execution_text_uses_the_current_line_or_visual_selection() {
		let mut editor = CodeEditor::new("select 1;\nselect 2;");
		wait_for_content(&mut editor, "select 1;\nselect 2;");
		assert_eq!(
			editor.selected_or_current_line().as_deref(),
			Some("select 1;")
		);

		send_keys(&mut editor, ['j']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.cursor_abs_pos.line.get() != 1)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(
			editor.selected_or_current_line().as_deref(),
			Some("select 2;")
		);

		send_keys(&mut editor, ['v', 'l']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.selection_ranges.is_empty())
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.selected_or_current_line().as_deref(), Some("se"));
	}

	#[test]
	fn insert_mode_paste_uses_the_engine_paste_protocol() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Char('i'),
			modifiers: Modifiers::NONE,
		}));
		let _ = editor.update(EditorMsg::Paste("-- pasted\n".to_owned()));
		wait_for_content(&mut editor, "-- pasted\nselect 1;");
	}

	#[test]
	fn modal_sql_editing_preserves_operator_undo_and_linewise_put_behavior() {
		let original = "one two\nthree\n";
		let mut editor = CodeEditor::new(original);
		wait_for_content(&mut editor, original);

		send_keys(&mut editor, ['d', 'w']);
		wait_for_content(&mut editor, "two\nthree\n");

		send_keys(&mut editor, ['u']);
		wait_for_content(&mut editor, original);

		send_keys(&mut editor, ['y', 'y', 'p']);
		wait_for_content(&mut editor, "one two\none two\nthree\n");
	}

	#[test]
	fn ctrl_v_keeps_normal_mode_visual_block_binding_while_insert_paste_is_separate() {
		use libaryon::engine::EditorMode;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Char('v'),
			modifiers: Modifiers::CONTROL,
		}));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|viewport| !matches!(viewport.input.mode, EditorMode::Visual { .. }))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.is_some_and(|viewport| matches!(viewport.input.mode, EditorMode::Visual { .. }))
		);
		assert_eq!(editor.cached_content(), "select 1;");
	}
}
