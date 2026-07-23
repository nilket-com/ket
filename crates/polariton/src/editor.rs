//! Thin Iced host for the headless libaryon engine.
//!
//! This deliberately owns no document state. The engine is authoritative; this
//! module only forwards portable input, coalesces projected viewports, and
//! renders the focused pane's projection: syntax, gutter, cursor, selection,
//! diagnostics, folds, engine-owned wrap rows and scrolling, and the minimap.

use crate::gui::{
	dashboard::{self, DashboardMsg, DashboardState},
	table::{Table, TableScroll},
};
use crate::plot::common::{PlotKernel, PlotRenderLayer, PlotSettings, PlotWidget};
use crate::plot::composition::PlotComposition;
use iced::keyboard::{self, Key};
use iced::mouse;
use iced::widget::{
	Space, canvas, column, container, mouse_area, rich_text, row, span, stack, text,
};
use iced::{Background, Color, Element, Event, Length, Subscription, Task, event};
use libaryon::core::{CursorPosition, DocDisplayCol, DocLine, PaneRect, Rgb};
use libaryon::engine::{
	EditorCommand, EditorMode, EngineConfig, EngineHandle, InputEvent, KeyCode, LanguageId,
	Modifiers,
};
use libaryon::engine::{
	HostEvent, HostPanelId, MinimapSnapshot, PaneDropEdge, PanelRevision, SplitDirection,
	UiPalette, VisualKind,
};
use libaryon::uast::Viewport;
use libaryon::uast::projection::{
	MarkdownCellCodeVisibility, MarkdownCellIdentity, MarkdownCellPresentation,
};
use polars::frame::DataFrame;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, mpsc::Receiver};

/// The single query-results surface. One stable identity reused for every run,
/// so results replace in place instead of opening a pane per query.
const RESULT_PANEL: HostPanelId = HostPanelId::new(1);
const DASHBOARD_PANEL: HostPanelId = HostPanelId::new(2);

const INITIAL_COLUMNS: u16 = 120;
const INITIAL_ROWS: u16 = 40;
/// A visible, low-friction workspace drag handle. It also establishes the
/// natural home for pane titles and close buttons; those can grow here without
/// leaking pane chrome back into Polariton's old outer grid.
const PANE_HEADER_HEIGHT: f32 = 22.0;
const PANE_DRAG_DEADBAND: f32 = 8.0;
/// A plot needs more room than a tabular result, but remains a document block
/// rather than a workspace pane. Width stays responsive to the preview.
const NOTEBOOK_PLOT_HEIGHT: f32 = 360.0;

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

#[derive(Clone)]
pub enum EditorMsg {
	EngineReady,
	Dashboard(DashboardMsg),
	Input(InputEvent),
	Paste(String),
	Scroll(i32),
	MinimapGoto(DocLine),
	/// Editor-local pointer motion from the workspace mouse_area.
	PointerMoved(iced::Point),
	/// Raw window-space pointer motion from the root subscription. Reaches us
	/// even while a table/dashboard widget captures its own move events, but in
	/// window coordinates — converted to editor-local via `pointer_offset`.
	RawCursorMoved(iced::Point),
	Click,
	/// Pointer released; ends a divider drag.
	Release,
	/// A pane header captured the press. Header-local handling is important:
	/// native table/dashboard widgets may capture pointer events before the
	/// workspace-wide mouse area sees them.
	PaneDragStarted(libaryon::core::PaneId),
	/// A message from one inline notebook plot. Identity keeps interactive
	/// state local to its cell rather than leaking through the dashboard.
	NotebookPlot {
		identity: MarkdownCellIdentity,
		message: crate::gui::messages::PlotMessage,
	},
}

enum EngineOutput {
	Viewport(Box<Viewport>),
	HostEvent(HostEvent),
}

#[derive(Clone)]
struct EngineWake {
	id: usize,
	receiver: Arc<Mutex<Option<iced::futures::channel::mpsc::UnboundedReceiver<()>>>>,
}

impl PartialEq for EngineWake {
	fn eq(&self, other: &Self) -> bool {
		self.id == other.id
	}
}

impl Eq for EngineWake {}

impl Hash for EngineWake {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.id.hash(state);
	}
}

fn engine_wake_stream(wake: &EngineWake) -> iced::futures::stream::BoxStream<'static, ()> {
	use iced::futures::StreamExt;
	wake.receiver
		.lock()
		.expect("engine wake receiver mutex poisoned")
		.take()
		.map_or_else(
			|| iced::futures::stream::pending().boxed(),
			iced::futures::StreamExt::boxed,
		)
}

/// Long-lived owner of Polariton's libaryon session.
pub struct CodeEditor {
	engine: Option<EngineHandle>,
	commands: libaryon::engine::CommandSender,
	input: libaryon::engine::InputSender,
	engine_outputs: Receiver<EngineOutput>,
	engine_wake: EngineWake,
	viewport: Option<Viewport>,
	document_text: String,
	text_query_pending: bool,
	/// Input was forwarded after the pending text query was issued, so the
	/// in-flight reply is already stale and another query must follow it.
	content_dirty: bool,
	last_pointer: Option<iced::Point>,
	/// Latest raw window-space cursor, from the root subscription.
	last_raw_window: Option<iced::Point>,
	/// window_coord - editor_local_coord, learned whenever both the mouse_area
	/// and the subscription report the same move. Lets subscription-only motion
	/// (over a capturing widget) be converted back to editor-local space, so
	/// `last_pointer` is always one coordinate system.
	pointer_offset: Option<iced::Vector>,
	/// Latest engine status line (`:w` outcomes, search wraps, command
	/// errors). Retained until the next forwarded input, vim-style: each
	/// command's outcome replaces the previous one.
	status_message: Option<String>,
	/// The engine accepted `:q` and has emitted its final viewport. Further
	/// editor traffic must stay local while the application closes its window.
	quit_requested: bool,
	quit_notified: bool,
	/// Table presentation remains Polariton-owned even when the table lives in
	/// a Baryon-managed HostPanel.
	show_column_types: bool,
	panels: PanelRegistry,
	/// Monotonic revision for the results panel, so each run supersedes the
	/// last.
	result_revision: u64,
	/// Divider drag in progress, if any.
	drag: Option<DividerDrag>,
	/// Workspace pane rearrangement in progress. The engine receives one drop
	/// command on release; it remains the sole owner of the split tree.
	pane_drag: Option<PaneDrag>,
	/// Output state belongs to the native notebook preview, keyed by the
	/// engine-projected identity of its Markdown fence.
	notebook_outputs: HashMap<MarkdownCellIdentity, NotebookCellOutput>,
	/// Successful SQL results are execution state, not presentation state.
	/// A cell with `output=none` must still be addressable by a later Rune cell.
	notebook_cell_data: HashMap<MarkdownCellIdentity, Arc<DataFrame>>,
}

/// A runnable fenced SQL cell selected by the engine's cursor projection.
pub struct MarkdownCellExecution {
	pub identity: MarkdownCellIdentity,
	pub presentation: MarkdownCellPresentation,
	pub language_id: String,
	pub code: String,
}

enum NotebookCellOutput {
	Running,
	Table(Box<DataFrame>),
	Plot { surface: Box<NotebookPlotSurface> },
	Failed(String),
	Message(String),
}

/// Native interaction state for a plot embedded in the Markdown preview.
/// It deliberately shares the existing plot kernel and settings model with
/// dashboard plots, but has its own identity and hover state.
struct NotebookPlotSurface {
	composition: PlotComposition,
	kernel: Arc<dyn PlotKernel + Send + Sync>,
	settings: PlotSettings,
	hovered_info: Option<String>,
	render_revision: u64,
}

impl NotebookPlotSurface {
	fn new(composition: PlotComposition, kernel: Arc<dyn PlotKernel + Send + Sync>) -> Self {
		Self {
			composition,
			kernel,
			settings: PlotSettings::default(),
			hovered_info: None,
			render_revision: 0,
		}
	}

	fn update(&mut self, message: crate::gui::messages::PlotMessage) {
		if let crate::gui::messages::PlotMessage::UpdateHover(hover) = message {
			self.hovered_info = hover;
		}
	}
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

#[derive(Clone, Copy)]
struct PaneDrag {
	source: libaryon::core::PaneId,
	origin: iced::Point,
	target: Option<(libaryon::core::PaneId, PaneDropEdge)>,
}

impl CodeEditor {
	pub fn new(text: &str) -> Self {
		let mut engine = EngineHandle::spawn(EngineConfig::default());
		let commands = engine.command_sender();
		let input = engine.input_sender();
		let viewports = engine
			.take_viewports()
			.expect("new engine owns viewport receiver");
		let host_events = engine
			.take_host_events()
			.expect("new engine owns host event receiver");
		let (output_tx, engine_outputs) = std::sync::mpsc::channel();
		let (wake_tx, wake_rx) = iced::futures::channel::mpsc::unbounded();
		let viewport_output_tx = output_tx.clone();
		let viewport_wake_tx = wake_tx.clone();
		std::thread::spawn(move || {
			while let Ok(viewport) = viewports.recv() {
				if viewport_output_tx
					.send(EngineOutput::Viewport(Box::new(viewport)))
					.is_err()
				{
					break;
				}
				let _ = viewport_wake_tx.unbounded_send(());
			}
		});
		std::thread::spawn(move || {
			while let Ok(event) = host_events.recv() {
				if output_tx.send(EngineOutput::HostEvent(event)).is_err() {
					break;
				}
				let _ = wake_tx.unbounded_send(());
			}
		});
		let wake_receiver = Arc::new(Mutex::new(Some(wake_rx)));
		let engine_wake = EngineWake {
			id: Arc::as_ptr(&wake_receiver) as usize,
			receiver: wake_receiver,
		};
		let mut editor = Self {
			engine: Some(engine),
			commands,
			input,
			engine_outputs,
			engine_wake,
			viewport: None,
			document_text: String::new(),
			text_query_pending: false,
			content_dirty: false,
			last_pointer: None,
			last_raw_window: None,
			pointer_offset: None,
			status_message: None,
			quit_requested: false,
			quit_notified: false,
			show_column_types: false,
			panels: PanelRegistry::default(),
			result_revision: 0,
			drag: None,
			pane_drag: None,
			notebook_outputs: HashMap::new(),
			notebook_cell_data: HashMap::new(),
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

	/// Stable execution language of the focused text buffer.
	pub fn active_language_id(&self) -> Option<LanguageId> {
		self.viewport.as_ref()?.focused_pane()?.language_id
	}

	/// Stable identity of the focused document, independent of which view is
	/// showing it. Async SQL publications and Rune plot surfaces capture this
	/// before dispatch so later focus changes cannot reroute their output.
	pub fn active_buffer_id(&self) -> Option<libaryon::core::BufferId> {
		Some(self.viewport.as_ref()?.focused_pane()?.buffer_id)
	}

	/// Code to run from the focused editor context: an active visual selection,
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

	/// The executable fenced cell containing the focused source cursor, if that source has a
	/// paired Markdown preview. The fence ranges and identities are projected by
	/// libaryon; Polariton only slices the authoritative source bytes.
	pub fn selected_markdown_cell(&self) -> Option<MarkdownCellExecution> {
		// A snippet run wants exactly the selected text, so a live selection
		// suppresses whole-cell dispatch. Cursor-in-cell with no selection runs
		// the cell.
		if self
			.viewport
			.as_ref()
			.and_then(Viewport::focused_pane)
			.is_some_and(|pane| !pane.selection_ranges.is_empty())
		{
			return None;
		}
		self.markdown_cell_at_cursor()
	}

	/// The SQL fence containing the focused cursor, ignoring any selection.
	/// `Run` uses this so pressing it mid-selection still runs the cell rather
	/// than falling through to dispatching the whole Markdown document as SQL.
	pub fn markdown_cell_at_cursor(&self) -> Option<MarkdownCellExecution> {
		let viewport = self.viewport.as_ref()?;
		let source = self.notebook_source(viewport)?;
		let cursor = source.cursor_abs_byte;
		let cell = source.markdown_cells.iter().find(|cell| {
			matches!(cell.language_id.as_str(), "sql" | "rune")
				&& cell.body_range.start <= cursor
				&& cursor < cell.body_range.end
		})?;
		let code = self
			.document_text
			.get(cell.body_range.start.get() as usize..cell.body_range.end.get() as usize)?
			.to_owned();
		Some(MarkdownCellExecution {
			identity: cell.identity.clone(),
			presentation: cell.presentation,
			language_id: cell.language_id.clone(),
			code,
		})
	}

	/// Whether the focused source is a notebook — i.e. it has a paired Markdown
	/// preview. `Run` uses this to refuse dispatching raw Markdown as SQL when
	/// the cursor is not inside a cell.
	pub fn is_notebook(&self) -> bool {
		self.viewport
			.as_ref()
			.and_then(|viewport| self.notebook_source(viewport))
			.is_some()
	}

	/// Resolve the Markdown source that owns notebook execution.
	/// Workspace focus can be on a result or dashboard host panel while the
	/// source retains the cursor that identifies the executable cell.
	fn notebook_source<'a>(
		&self,
		viewport: &'a Viewport,
	) -> Option<&'a libaryon::uast::projection::PaneViewport> {
		let focused = viewport.focused_pane()?;
		if !focused.markdown_cells.is_empty() {
			return Some(focused);
		}
		if let Some(preview) = focused.markdown_preview.as_ref() {
			let source = viewport
				.panes
				.iter()
				.find(|pane| pane.view_id == preview.source_view_id)?;
			return (!source.markdown_cells.is_empty()).then_some(source);
		}

		// Native output panes have no document identity. The Markdown source
		// remains the execution context even when its preview is closed.
		viewport
			.panes
			.iter()
			.find(|pane| !pane.markdown_cells.is_empty())
	}

	pub fn begin_markdown_cell(&mut self, identity: MarkdownCellIdentity) {
		self.notebook_cell_data.remove(&identity);
		self.notebook_outputs
			.insert(identity, NotebookCellOutput::Running);
	}

	pub fn set_markdown_cell_table(&mut self, identity: MarkdownCellIdentity, frame: DataFrame) {
		self.retain_markdown_cell_table(identity.clone(), &frame);
		self.notebook_outputs
			.insert(identity, NotebookCellOutput::Table(Box::new(frame)));
	}

	pub fn retain_markdown_cell_table(
		&mut self,
		identity: MarkdownCellIdentity,
		frame: &DataFrame,
	) {
		self.notebook_cell_data
			.insert(identity, Arc::new(frame.clone()));
	}

	pub fn set_markdown_cell_error(&mut self, identity: MarkdownCellIdentity, message: String) {
		self.notebook_cell_data.remove(&identity);
		self.notebook_outputs
			.insert(identity, NotebookCellOutput::Failed(message));
	}

	pub fn set_markdown_cell_message(&mut self, identity: MarkdownCellIdentity, message: String) {
		self.notebook_outputs
			.insert(identity, NotebookCellOutput::Message(message));
	}

	pub fn set_markdown_cell_plot(
		&mut self,
		identity: MarkdownCellIdentity,
		composition: PlotComposition,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	) {
		self.notebook_outputs.insert(
			identity,
			NotebookCellOutput::Plot {
				surface: Box::new(NotebookPlotSurface::new(composition, kernel)),
			},
		);
	}

	/// Explicit SQL-cell snapshots available to a Rune notebook cell.
	///
	/// Values are owned `Arc`s, so the Rune VM never borrows editor state. An
	/// unnamed SQL fence remains useful for presentation, but cannot be named
	/// from Rune; that is precisely the purpose of `{#cell-name}`.
	pub fn named_notebook_cell_data(&self) -> HashMap<String, Arc<DataFrame>> {
		self.notebook_cell_data
			.iter()
			.filter_map(|(identity, frame)| match identity {
				MarkdownCellIdentity::Explicit { id } => Some((id.clone(), frame.clone())),
				MarkdownCellIdentity::Implicit { .. } => None,
			})
			.collect()
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
				split_direction: SplitDirection::Horizontal,
				focus: false,
			});
		}
		self.panels.set_content(RESULT_PANEL, PanelContent::Running);
	}

	/// Open (or focus) Polariton's nested dashboard inside the Baryon workspace.
	pub fn open_dashboard(&mut self) {
		self.send(EditorCommand::OpenHostPanel {
			panel_id: DASHBOARD_PANEL,
			kind: "dashboard".to_owned(),
			title: "Dashboard".to_owned(),
			split_direction: SplitDirection::Vertical,
			focus: true,
		});
	}

	/// Pixel bounds relative to the Baryon workspace, for dashboard export.
	pub fn dashboard_bounds(&self) -> Option<iced::Rectangle> {
		let pane = self.viewport.as_ref()?.panes.iter().find(|pane| {
			pane.host_panel
				.as_ref()
				.is_some_and(|panel| panel.panel_id == DASHBOARD_PANEL)
		})?;
		Some(iced::Rectangle {
			x: pane.rect.screen_col.get() as f32 * metrics::CELL_WIDTH,
			y: pane.rect.screen_row.get() as f32 * metrics::LINE_HEIGHT,
			width: pane.rect.width.get() as f32 * metrics::CELL_WIDTH,
			height: pane.rect.height.get() as f32 * metrics::LINE_HEIGHT,
		})
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

	/// Complete a non-row SQL command in the results pane. A pane-targeted
	/// `CREATE` or `INSERT` has no table to attach, but it must still replace
	/// the panel's transient `running…` state.
	pub fn set_query_message(&mut self, message: String) {
		self.panels
			.set_content(RESULT_PANEL, PanelContent::Message(message));
	}

	pub fn set_show_column_types(&mut self, show_column_types: bool) {
		self.show_column_types = show_column_types;
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

	#[cfg(test)]
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
	/// Callers pass the actual workspace rectangle (see `sync_workspace_viewport`).
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
		Subscription::batch([
			event::listen_with(|event, status, _window| editor_input_for(&event, status)),
			Subscription::run_with(self.engine_wake.clone(), engine_wake_stream)
				.map(|()| EditorMsg::EngineReady),
		])
	}

	/// Drain every available projection and retain only the latest one.
	/// This is deliberately called from Iced's update path, never blocking it.
	pub fn poll(&mut self) -> bool {
		let mut changed = false;
		let mut events = Vec::new();
		while let Ok(output) = self.engine_outputs.try_recv() {
			let EngineOutput::Viewport(viewport) = output else {
				if let EngineOutput::HostEvent(event) = output {
					events.push(event);
				}
				continue;
			};
			let viewport = *viewport;
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
			self.reconcile_notebook_outputs(&viewport);
			self.viewport = Some(viewport);
			changed = true;
		}
		// Ordered causality, drained whole every poll: unlike projections these
		// must never be coalesced. Reconciliation runs after the viewport above
		// is current, so a disposal that outruns its projection cannot blank a
		// pane that is still on screen.
		changed |= !events.is_empty();
		self.panels.reconcile(&events, self.viewport.as_ref());
		changed
	}

	fn reconcile_notebook_outputs(&mut self, viewport: &Viewport) {
		let preview_cells = viewport
			.panes
			.iter()
			.filter_map(|pane| pane.markdown_preview.as_ref())
			.flat_map(|preview| preview.cells.iter().map(|cell| cell.identity.clone()))
			.collect::<std::collections::HashSet<_>>();
		self.notebook_outputs
			.retain(|identity, _| preview_cells.contains(identity));
		let source_cells = viewport
			.panes
			.iter()
			.flat_map(|pane| pane.markdown_cells.iter().map(|cell| cell.identity.clone()))
			.collect::<std::collections::HashSet<_>>();
		self.notebook_cell_data
			.retain(|identity, _| source_cells.contains(identity));
	}

	pub fn update(&mut self, message: EditorMsg) -> Task<EditorMsg> {
		if self.quit_requested {
			return Task::none();
		}
		match message {
			EditorMsg::EngineReady => {
				self.poll();
			}
			EditorMsg::Dashboard(_) => {}
			EditorMsg::NotebookPlot { identity, message } => {
				if let Some(NotebookCellOutput::Plot { surface }) =
					self.notebook_outputs.get_mut(&identity)
				{
					surface.update(message);
				}
			}
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
				// The mouse_area is anchored at the editor origin, so this is
				// authoritative editor-local space. Learn the window offset from
				// it while we can.
				if let Some(raw) = self.last_raw_window {
					self.pointer_offset =
						Some(iced::Vector::new(raw.x - position.x, raw.y - position.y));
				}
				self.apply_pointer(position);
			}
			EditorMsg::RawCursorMoved(window) => {
				self.last_raw_window = Some(window);
				// Only useful once the offset is known; until then the
				// mouse_area is the source of truth.
				if let Some(offset) = self.pointer_offset {
					self.apply_pointer(iced::Point::new(window.x - offset.x, window.y - offset.y));
				}
			}
			EditorMsg::Click => {
				// A pane header may publish its more specific press before the
				// workspace-wide mouse area publishes this generic click. Do not let
				// the divider hit band steal a drag the header already owns.
				if self.pane_drag.is_none() && !self.begin_divider_drag() {
					self.click_at_pointer();
					self.begin_pane_drag();
				}
			}
			EditorMsg::PaneDragStarted(pane_id) => {
				self.drag = None;
				self.send(EditorCommand::FocusPane(pane_id));
				self.begin_pane_drag_for(pane_id);
			}
			EditorMsg::Release => {
				self.drag = None;
				self.finish_pane_drag();
			}
		}
		Task::none()
	}

	pub fn view<'a>(&'a self, dashboard: &'a DashboardState) -> Element<'a, EditorMsg> {
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
			let content = if let Some(preview) = pane.markdown_preview.as_ref() {
				markdown_preview_surface(
					preview,
					palette,
					&self.notebook_outputs,
					self.show_column_types,
				)
			} else {
				match pane.host_panel.as_ref() {
					Some(panel) => self.panels.surface(
						panel,
						focused,
						palette,
						self.show_column_types,
						dashboard,
					),
					None => text_pane_surface(pane, palette, focused),
				}
			};
			let title = pane_title(pane);
			let content = pane_surface(pane.pane_id, title, focused, palette, content);
			layers.push(positioned(
				pane.rect.screen_col.get() as f32 * metrics::CELL_WIDTH,
				pane.rect.screen_row.get() as f32 * metrics::LINE_HEIGHT,
				pane.rect.width.get() as f32 * metrics::CELL_WIDTH,
				pane.rect.height.get() as f32 * metrics::LINE_HEIGHT,
				content,
			));
		}
		// The engine deliberately publishes horizontal divider hit rects wider
		// than their one-row visual gap. Put those rectangles above pane content:
		// otherwise a child editor/table captures presses in the expanded rows
		// and makes the promised grab area unreachable.
		for divider in &viewport.dividers {
			let hit = pane_pixel_rect(divider.hit_rect);
			layers.push(positioned(
				hit.x,
				hit.y,
				hit.width,
				hit.height,
				// Capture the press only. `mouse_area::on_move` reports
				// coordinates relative to *this* overlay's bounds, which sit at
				// the divider's offset — publishing them would corrupt
				// `last_pointer` into the wrong coordinate space. The outer
				// mouse_area, anchored at the editor origin, owns pointer
				// tracking and reports correct cell-space coordinates.
				mouse_area(
					container(Space::new())
						.width(Length::Fill)
						.height(Length::Fill),
				)
				.on_press(EditorMsg::Click)
				.into(),
			));
		}
		if let Some(drag) = self.pane_drag
			&& let Some((target, edge)) = drag.target
			&& let Some(pane) = viewport.panes.iter().find(|pane| pane.pane_id == target)
		{
			let bounds = pane_pixel_rect(pane.rect);
			let overlay = drop_overlay_rect(bounds, edge);
			let accent = color(palette.accent);
			layers.push(positioned(
				overlay.x,
				overlay.y,
				overlay.width,
				overlay.height,
				container(Space::new())
					.width(Length::Fill)
					.height(Length::Fill)
					.style(move |_| container::Style {
						background: Some(Background::Color(Color { a: 0.18, ..accent })),
						border: iced::Border {
							color: accent,
							width: 2.0,
							radius: 0.0.into(),
						},
						..Default::default()
					})
					.into(),
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
	/// Route an editor-local pointer position into any active drag. Shared by
	/// the mouse_area (editor-local) and the subscription (window, converted),
	/// so `last_pointer` only ever holds editor-local coordinates.
	fn apply_pointer(&mut self, position: iced::Point) {
		self.last_pointer = Some(position);
		if self.update_pane_drag() {
			return;
		}
		// Continuous resize: every sample while a divider is held becomes a
		// boundary target, so the split tracks the cursor.
		if let Some(command) = self.divider_drag_command() {
			self.send(command);
		}
	}

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

	fn begin_pane_drag(&mut self) {
		let (Some(viewport), Some(pointer)) = (self.viewport.as_ref(), self.last_pointer) else {
			return;
		};
		let pane = viewport.panes.iter().find(|pane| {
			let rect = pane_pixel_rect(pane.rect);
			rect.contains(pointer) && pointer.y < rect.y + PANE_HEADER_HEIGHT
		});
		if let Some(pane) = pane {
			self.begin_pane_drag_for(pane.pane_id);
		}
	}

	fn begin_pane_drag_for(&mut self, source: libaryon::core::PaneId) {
		let Some(origin) = self.last_pointer else {
			return;
		};
		self.pane_drag = Some(PaneDrag {
			source,
			origin,
			target: None,
		});
	}

	/// Recompute the drop target from the current projected pane rectangles.
	/// Polariton describes only a geometric intention; the engine owns the
	/// topology mutation that happens when the mouse is released.
	fn update_pane_drag(&mut self) -> bool {
		let Some(mut drag) = self.pane_drag else {
			return false;
		};
		let Some(pointer) = self.last_pointer else {
			return true;
		};
		if (pointer.x - drag.origin.x).hypot(pointer.y - drag.origin.y) < PANE_DRAG_DEADBAND {
			return true;
		}
		let Some(viewport) = self.viewport.as_ref() else {
			return true;
		};
		drag.target = viewport
			.panes
			.iter()
			.filter(|pane| pane.pane_id != drag.source)
			.find(|pane| pane_pixel_rect(pane.rect).contains(pointer))
			.map(|pane| (pane.pane_id, drop_edge(pane_pixel_rect(pane.rect), pointer)));
		self.pane_drag = Some(drag);
		true
	}

	fn finish_pane_drag(&mut self) {
		let Some(drag) = self.pane_drag.take() else {
			return;
		};
		let Some(pointer) = self.last_pointer else {
			return;
		};
		if (pointer.x - drag.origin.x).hypot(pointer.y - drag.origin.y) < PANE_DRAG_DEADBAND {
			return;
		}
		if let Some((target, edge)) = drag.target {
			self.send(EditorCommand::MovePane {
				source: drag.source,
				target,
				edge,
			});
		}
	}

	fn click_at_pointer(&self) {
		let Some(pointer) = self.last_pointer else {
			return;
		};
		let Some(viewport) = self.viewport.as_ref() else {
			return;
		};
		let (column, row) = Self::cell_at(pointer);
		let Some(pane) = viewport.panes.iter().find(|pane| {
			column >= pane.rect.screen_col.get()
				&& column < pane.rect.right().get()
				&& row >= pane.rect.screen_row.get()
				&& row < pane.rect.bottom().get()
		}) else {
			return;
		};
		self.send(EditorCommand::FocusPane(pane.pane_id));
		let pane_top = pane.rect.screen_row.get() as f32 * metrics::LINE_HEIGHT;
		if pane.host_panel.is_some() || pointer.y < pane_top + PANE_HEADER_HEIGHT {
			return;
		}
		let local_y = pointer.y - pane_top - PANE_HEADER_HEIGHT;
		let local_x = pointer.x - pane.rect.screen_col.get() as f32 * metrics::CELL_WIDTH;
		let row = ((local_y - metrics::VERTICAL_PADDING).max(0.0) / metrics::LINE_HEIGHT).floor()
			as usize;
		// `visible_rows` is `render_rows` minus the wrapped rows scrolled off
		// above the viewport, matching what `view` actually laid out.
		let Some(visible_row) = pane.visible_rows.get(row) else {
			return;
		};
		let column =
			((local_x - metrics::GUTTER_WIDTH).max(0.0) / metrics::CELL_WIDTH).floor() as u32;
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
	show_cursor: bool,
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
			let cursor = show_cursor && pane.cursor_abs_byte.get() == byte;
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
	if show_cursor && !cursor_rendered {
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
	/// Monotonic across all panels, so each declined key applies exactly once.
	scroll_sequence: u64,
}

struct PanelEntry {
	revision: PanelRevision,
	content: PanelContent,
	/// Latest keyboard-driven scroll, with a sequence the widget uses to apply
	/// it exactly once rather than on every frame.
	scroll: Option<(u64, TableScroll)>,
}

/// Host-owned backing state for a panel. None of this is representable in
/// libaryon: the engine only knows the panel exists.
enum PanelContent {
	/// Projected or restored, but the frontend has no content for it — either
	/// a panel from a persisted layout or one seen before its object exists.
	Unresolved,
	Running,
	Table(Box<DataFrame>),
	Message(String),
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
							scroll: None,
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
							scroll: None,
						},
					);
				}
				HostEvent::UnhandledInput { panel_id, input } => {
					// Baryon already decided this is not workspace control, so
					// the panel interprets it however its content sees fit.
					self.handle_panel_input(*panel_id, input);
				}
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
					scroll: None,
				});
			}
		}
	}

	/// Route input the engine declined to the panel that has workspace focus.
	/// What a key means is Polariton's business — the engine has only said it
	/// is not a workspace command.
	fn handle_panel_input(&mut self, panel_id: HostPanelId, input: &InputEvent) {
		let InputEvent::Key { code, modifiers } = input else {
			return;
		};
		if !modifiers.is_empty() {
			return;
		}
		let Some(entry) = self.panels.get_mut(&panel_id) else {
			return;
		};
		if !matches!(entry.content, PanelContent::Table(_)) {
			return;
		}
		let action = match code {
			KeyCode::Char('j') | KeyCode::Down => TableScroll::LineDown,
			KeyCode::Char('k') | KeyCode::Up => TableScroll::LineUp,
			KeyCode::PageDown => TableScroll::PageDown,
			KeyCode::PageUp => TableScroll::PageUp,
			KeyCode::Home => TableScroll::Top,
			KeyCode::End => TableScroll::Bottom,
			_ => return,
		};
		self.scroll_sequence = self.scroll_sequence.saturating_add(1);
		let sequence = self.scroll_sequence;
		if let Some(entry) = self.panels.get_mut(&panel_id) {
			entry.scroll = Some((sequence, action));
		}
	}

	fn set_content(&mut self, panel_id: HostPanelId, content: PanelContent) {
		let entry = self.panels.entry(panel_id).or_insert(PanelEntry {
			revision: PanelRevision::new(0),
			content: PanelContent::Unresolved,
			scroll: None,
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
		show_column_types: bool,
		dashboard: &'a DashboardState,
	) -> Element<'a, EditorMsg> {
		let content = self
			.panels
			.get(&panel.panel_id)
			.map(|entry| &entry.content)
			.unwrap_or(&PanelContent::Unresolved);

		let inner: Element<'a, EditorMsg> = if panel.panel_id == DASHBOARD_PANEL {
			dashboard::view(dashboard).map(EditorMsg::Dashboard)
		} else {
			match content {
				PanelContent::Table(frame) => Table::new(frame, 0)
					.show_column_types(show_column_types)
					.scroll_request(
						self.panels
							.get(&panel.panel_id)
							.and_then(|entry| entry.scroll),
					)
					.into(),
				PanelContent::Running => {
					panel_message("running…".to_owned(), color(palette.preview_text))
				}
				PanelContent::Message(message) => {
					panel_message(message.clone(), color(palette.preview_text))
				}
				PanelContent::Failed(message) => {
					panel_message(message.clone(), color(palette.status_text))
				}
				PanelContent::Unresolved => panel_message(
					format!(
						"{} unavailable (revision {})\nrerun to populate",
						panel.kind,
						panel.revision.get()
					),
					color(palette.preview_text),
				),
			}
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

fn pane_title(pane: &libaryon::uast::projection::PaneViewport) -> &str {
	pane.markdown_preview.as_ref().map_or_else(
		|| {
			pane.host_panel
				.as_ref()
				.map_or(pane.language_label, |panel| panel.title.as_str())
		},
		|preview| preview.source_path.as_deref().unwrap_or("Markdown Preview"),
	)
}

/// Common workspace chrome. Its header is intentionally simple: a stable drag
/// handle now, and the future home of pane tabs and close controls.
fn pane_surface<'a>(
	pane_id: libaryon::core::PaneId,
	title: &str,
	focused: bool,
	palette: UiPalette,
	content: Element<'a, EditorMsg>,
) -> Element<'a, EditorMsg> {
	let header_background = color(if focused {
		palette.tab_active_surface
	} else {
		palette.tab_surface
	});
	let border = color(if focused {
		palette.accent
	} else {
		palette.divider_surface
	});
	container(
		column![
			mouse_area(
				container(
					text(format!("⠿⠿  {title}"))
						.font(EDITOR_FONT)
						.size(13.0)
						.color(color(palette.tab_text)),
				)
				.padding([2, 7])
				.width(Length::Fill)
				.height(Length::Fixed(PANE_HEADER_HEIGHT))
				.style(move |_| container::Style {
					background: Some(Background::Color(header_background)),
					..Default::default()
				}),
			)
			.on_press(EditorMsg::PaneDragStarted(pane_id))
			.on_release(EditorMsg::Release),
			content,
		]
		.width(Length::Fill)
		.height(Length::Fill),
	)
	.width(Length::Fill)
	.height(Length::Fill)
	.style(move |_| container::Style {
		border: iced::Border {
			color: border,
			width: if focused { 2.0 } else { 1.0 },
			radius: 0.0.into(),
		},
		..Default::default()
	})
	.into()
}

fn pane_pixel_rect(rect: PaneRect) -> iced::Rectangle {
	iced::Rectangle {
		x: rect.screen_col.get() as f32 * metrics::CELL_WIDTH,
		y: rect.screen_row.get() as f32 * metrics::LINE_HEIGHT,
		width: rect.width.get() as f32 * metrics::CELL_WIDTH,
		height: rect.height.get() as f32 * metrics::LINE_HEIGHT,
	}
}

fn drop_edge(bounds: iced::Rectangle, pointer: iced::Point) -> PaneDropEdge {
	let x = (pointer.x - bounds.x) / bounds.width;
	let y = (pointer.y - bounds.y) / bounds.height;
	if (0.3..0.7).contains(&x) && (0.3..0.7).contains(&y) {
		return PaneDropEdge::Center;
	}
	let left = x;
	let right = 1.0 - x;
	let top = y;
	let bottom = 1.0 - y;
	let nearest = left.min(right).min(top).min(bottom);
	if nearest == left {
		PaneDropEdge::Left
	} else if nearest == right {
		PaneDropEdge::Right
	} else if nearest == top {
		PaneDropEdge::Top
	} else {
		PaneDropEdge::Bottom
	}
}

fn drop_overlay_rect(bounds: iced::Rectangle, edge: PaneDropEdge) -> iced::Rectangle {
	match edge {
		PaneDropEdge::Center => bounds,
		PaneDropEdge::Left => iced::Rectangle {
			width: bounds.width / 2.0,
			..bounds
		},
		PaneDropEdge::Right => iced::Rectangle {
			x: bounds.x + bounds.width / 2.0,
			width: bounds.width / 2.0,
			..bounds
		},
		PaneDropEdge::Top => iced::Rectangle {
			height: bounds.height / 2.0,
			..bounds
		},
		PaneDropEdge::Bottom => iced::Rectangle {
			y: bounds.y + bounds.height / 2.0,
			height: bounds.height / 2.0,
			..bounds
		},
	}
}

/// One text pane's surface: gutter, styled rows, and minimap. Positioned by the
/// caller at the engine's rect.
fn text_pane_surface<'a>(
	pane: &'a libaryon::uast::projection::PaneViewport,
	palette: UiPalette,
	focused: bool,
) -> Element<'a, EditorMsg> {
	let lines = wrap_projected_lines(
		project_lines(pane, palette.editor_text, palette.accent, focused),
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
	// No `on_move` here: pointer tracking must have exactly one owner. A
	// per-pane mouse_area reports coordinates relative to *its* bounds (the
	// pane's offset), so publishing PointerMoved from here would corrupt
	// `last_pointer` into pane-local space whenever the cursor is over pane
	// content — which is precisely the horizontal-divider grab band. The outer
	// wrapper, anchored at the editor origin, owns pointer tracking; this pane
	// mouse_area only claims clicks and scrolls over its own content.
	let editor_rows = mouse_area(
		container(rows)
			.padding([metrics::VERTICAL_PADDING, 0.0])
			.width(Length::Fill)
			.height(Length::Fill),
	)
	.on_press(EditorMsg::Click)
	.on_scroll(|delta| {
		let lines = match delta {
			mouse::ScrollDelta::Lines { y, .. } => (y * 3.0).round() as i32,
			mouse::ScrollDelta::Pixels { y, .. } => (y / metrics::LINE_HEIGHT).round() as i32,
		};
		EditorMsg::Scroll(-lines)
	});
	let editor_content: Element<'_, EditorMsg> = if let Some(minimap) = pane.minimap.as_ref() {
		row![
			editor_rows,
			minimap_view(minimap, pane.theme_colors, palette)
		]
		.height(Length::Fill)
		.into()
	} else {
		editor_rows.into()
	};
	editor_content
}

/// Native, read-only Markdown projection. The engine owns both parsing and
/// highlighting: this surface receives source-coordinate categories plus the
/// same palette a text pane receives, then only paints those pixels.
fn markdown_preview_surface<'a>(
	preview: &'a libaryon::uast::projection::MarkdownPreviewSnapshot,
	palette: UiPalette,
	outputs: &'a HashMap<MarkdownCellIdentity, NotebookCellOutput>,
	show_column_types: bool,
) -> Element<'a, EditorMsg> {
	let mut rows: Vec<Element<'_, EditorMsg>> = Vec::new();
	let mut byte_offset = 0u64;
	// Every fenced block retains Markdown's normal code rendering. A cell is
	// the subset of a fence with an engine-projected identity and presentation.
	let mut in_code_block = false;
	let mut active_code_cell: Option<&libaryon::uast::projection::MarkdownCellSnapshot> = None;
	let first_line = preview.source_scroll_line.get();

	for (line_number, source_line) in preview.source_text.split_inclusive('\n').enumerate() {
		let content = source_line.strip_suffix('\n').unwrap_or(source_line);
		let trimmed = content.trim_start();
		let fence = trimmed.starts_with("```") || trimmed.starts_with("~~~");
		if fence {
			if in_code_block {
				in_code_block = false;
				if let Some(cell) = active_code_cell.take() {
					let body_end = byte_offset;
					if let Some(cell) = preview.cells.iter().find(|candidate| {
						candidate.identity == cell.identity
							&& candidate.body_range.end.get() == body_end
					}) && let Some(output) = outputs.get(&cell.identity)
						&& cell.presentation.output
							== libaryon::uast::projection::MarkdownCellOutputTarget::Inline
					{
						rows.push(markdown_cell_output_surface(
							cell.identity.clone(),
							output,
							palette,
							show_column_types,
						));
					}
				}
			} else {
				in_code_block = true;
				let body_start = byte_offset.saturating_add(source_line.len() as u64);
				active_code_cell = preview
					.cells
					.iter()
					.find(|cell| cell.body_range.start.get() == body_start);
			}
			byte_offset = byte_offset.saturating_add(source_line.len() as u64);
			continue;
		}
		if active_code_cell
			.is_some_and(|cell| cell.presentation.code == MarkdownCellCodeVisibility::Hide)
		{
			byte_offset = byte_offset.saturating_add(source_line.len() as u64);
			continue;
		}
		if line_number as u64 >= first_line {
			let row: Element<'_, EditorMsg> = if in_code_block {
				container(
					rich_text(markdown_code_fragments(
						content,
						byte_offset,
						&preview.code_highlights,
						preview.theme_colors,
						palette.editor_text,
					))
					.font(EDITOR_FONT)
					.size(metrics::TEXT_SIZE)
					.line_height(iced::Pixels(metrics::LINE_HEIGHT)),
				)
				.padding([0, 12])
				.width(Length::Fill)
				.style(move |_| container::Style {
					background: Some(Background::Color(color(palette.gutter_surface))),
					..Default::default()
				})
				.into()
			} else if let Some(heading) = trimmed.strip_prefix("# ") {
				text(heading)
					.font(iced::Font::DEFAULT)
					.size(28)
					.color(color(palette.editor_text))
					.into()
			} else if let Some(heading) = trimmed.strip_prefix("## ") {
				text(heading)
					.font(iced::Font::DEFAULT)
					.size(22)
					.color(color(palette.editor_text))
					.into()
			} else if is_markdown_thematic_break(content) {
				container(Space::new().height(Length::Fixed(1.0)))
					.width(Length::Fill)
					.style(move |_| container::Style {
						background: Some(Background::Color(Color {
							a: 0.35,
							..color(palette.editor_text)
						})),
						..Default::default()
					})
					.into()
			} else if content.is_empty() {
				Space::new()
					.height(Length::Fixed(metrics::LINE_HEIGHT / 2.0))
					.into()
			} else {
				text(content)
					.font(iced::Font::DEFAULT)
					.size(17)
					.color(color(palette.editor_text))
					.into()
			};
			rows.push(row);
		}
		byte_offset = byte_offset.saturating_add(source_line.len() as u64);
	}

	container(column(rows).spacing(8))
		.padding(20)
		.width(Length::Fill)
		.height(Length::Fill)
		.into()
}

/// Markdown's thematic-break syntax: at least three matching `-`, `_`, or
/// `*` characters, with optional whitespace between them.
fn is_markdown_thematic_break(line: &str) -> bool {
	let marks: Vec<char> = line
		.chars()
		.filter(|character| !character.is_whitespace())
		.collect();
	matches!(marks.as_slice(), [first, rest @ ..] if marks.len() >= 3
		&& matches!(first, '-' | '_' | '*')
		&& rest.iter().all(|character| character == first))
}

fn markdown_cell_output_surface<'a>(
	identity: MarkdownCellIdentity,
	output: &'a NotebookCellOutput,
	palette: UiPalette,
	show_column_types: bool,
) -> Element<'a, EditorMsg> {
	let content: Element<'a, EditorMsg> = match output {
		NotebookCellOutput::Running => text("running…")
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.color(color(palette.preview_text))
			.into(),
		NotebookCellOutput::Table(frame) => Table::new(frame, 0)
			.show_column_types(show_column_types)
			.into(),
		NotebookCellOutput::Plot { surface } => {
			let plot: Element<'_, crate::gui::messages::PlotMessage> = canvas(PlotWidget {
				kernel: surface.kernel.as_ref(),
				title: surface.composition.title(),
				padding: 20.0,
				settings: surface.settings.clone(),
				render_revision: surface.render_revision,
				resize_render_suspended: false,
				layer: PlotRenderLayer::Full,
			})
			.width(Length::Fill)
			.height(Length::Fill)
			.into();
			let mut layers = stack![plot.map({
				let identity = identity.clone();
				move |message| EditorMsg::NotebookPlot {
					identity: identity.clone(),
					message,
				}
			})];
			if let Some(info) = surface.hovered_info.as_deref() {
				layers = layers.push(
					container(text(info).size(metrics::TEXT_SIZE))
						.padding(6)
						.style(move |_| container::Style {
							background: Some(Background::Color(Color {
								a: 0.85,
								..surface.settings.background_color
							})),
							border: iced::Border {
								color: surface.settings.decoration_color,
								width: 1.0,
								radius: 2.0.into(),
							},
							text_color: Some(surface.settings.decoration_color),
							..Default::default()
						}),
				);
			}
			container(layers)
				.width(Length::Fill)
				.height(Length::Fixed(NOTEBOOK_PLOT_HEIGHT))
				.into()
		}
		NotebookCellOutput::Failed(message) => text(message)
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.color(color(palette.status_text))
			.into(),
		NotebookCellOutput::Message(message) => text(message)
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.color(color(palette.preview_text))
			.into(),
	};
	container(content)
		.padding(8)
		.width(Length::Fill)
		.height(if matches!(output, NotebookCellOutput::Plot { .. }) {
			Length::Fixed(NOTEBOOK_PLOT_HEIGHT)
		} else {
			Length::Fixed(260.0)
		})
		.style(move |_| container::Style {
			background: Some(Background::Color(color(palette.preview_surface))),
			border: iced::Border {
				color: color(palette.divider_surface),
				width: 1.0,
				radius: 0.0.into(),
			},
			..Default::default()
		})
		.into()
}

fn markdown_code_fragments(
	line: &str,
	line_start: u64,
	highlights: &[libaryon::svp::rust_highlight::HighlightSpan],
	theme_colors: [Option<Rgb>; libaryon::svp::rust_highlight::CATEGORY_COUNT],
	default_foreground: Rgb,
) -> Vec<iced::widget::text::Span<'static, EditorMsg, iced::Font>> {
	let mut fragments = Vec::new();
	let mut byte = line_start;
	for ch in line.chars() {
		let foreground = highlights
			.iter()
			.rev()
			.find(|highlight| highlight.start.get() <= byte && byte < highlight.end.get())
			.and_then(|highlight| theme_colors[highlight.category as usize])
			.unwrap_or(default_foreground);
		fragments.push(span::<EditorMsg, iced::Font>(ch.to_string()).color(color(foreground)));
		byte = byte.saturating_add(ch.len_utf8() as u64);
	}
	fragments
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

/// Which window events become editor input.
///
/// **Ordering invariant.** Iced dispatches events to the widget tree first and
/// only then broadcasts them to subscriptions, tagged with whether a widget
/// consumed them. Captured keys are dropped here so typing into app chrome — a
/// connection name, a statement title — does not also edit the SQL document.
///
/// The consequence, which is a real constraint rather than an accident: a
/// native host panel **must not capture keyboard events**, or it would swallow
/// `:` and pane navigation before Baryon ever sees them and the workspace would
/// stop responding while that panel had focus. The result table is safe because
/// it handles only pointer input; keyboard scrolling reaches it the correct way,
/// as `HostEvent::UnhandledInput` that the engine declined.
///
/// A panel that genuinely needs keyboard capture — a Rune console, a native text
/// input — cannot simply capture: it needs a root-level arbiter that forwards to
/// Baryon before children see the key, which is a deliberate architectural
/// change and not a local one. See `plans/wip/notebook-with-panes.md`.
fn editor_input_for(event: &Event, status: event::Status) -> Option<EditorMsg> {
	// Pointer motion/release is observed at the root so a workspace pane drag
	// begun in a header continues while the cursor crosses native table or
	// dashboard widgets that capture their own pointer events. These messages
	// are inert unless the editor has an active drag.
	match event {
		Event::Mouse(mouse::Event::CursorMoved { position }) => {
			return Some(EditorMsg::RawCursorMoved(*position));
		}
		Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)) => {
			return Some(EditorMsg::Release);
		}
		_ => {}
	}
	let Event::Keyboard(keyboard::Event::KeyPressed {
		modified_key,
		modifiers,
		..
	}) = event
	else {
		return None;
	};
	if std::env::var_os("POLARITON_INPUT_TRACE").is_some() {
		eprintln!(
			"[input-trace] modified_key={modified_key:?} modifiers={modifiers:?} status={status:?}"
		);
	}
	if status != event::Status::Ignored {
		return None;
	}
	// `modified_key`, not `key`: iced's `key` is the unshifted layout key
	// (Shift+; arrives as ';'), while the engine's keymap expects the produced
	// character (':', 'A', '"', …) with Ctrl kept as a modifier flag — which is
	// exactly `modified_key`'s contract.
	key_code(modified_key).map(|code| {
		EditorMsg::Input(InputEvent::Key {
			code,
			modifiers: map_modifiers(*modifiers),
		})
	})
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
		EditorMode::Password => Some(format!(
			"{} {}",
			viewport
				.input
				.prompt_label
				.as_deref()
				.unwrap_or("Encryption password:"),
			viewport.input.prompt_buffer
		)),
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

const MINIMAP_WIDTH: f32 = 80.0;
/// Search-marker gutter on the minimap's right edge, in logical pixels.
const MINIMAP_MARKER_WIDTH: u32 = 3;
/// Oversampling factor: the ink image is rendered at this multiple of the
/// canvas's logical size so hidpi surfaces get sharp per-column detail.
const MINIMAP_SCALE: u32 = 2;

/// Canvas-backed minimap. The engine's `PreviewSnapshot` carries per-column
/// token bins for every source row; this widget projects them through the same
/// shared samplers baryon uses (`sample_preview_bin_range` /
/// `dominant_preview_bin_for_rows`) into a syntax-colored pixel image, then
/// draws the viewport frame, cursor line, and search markers on top.
struct MinimapWidget<'a> {
	snapshot: &'a MinimapSnapshot,
	theme_colors: [Option<Rgb>; libaryon::svp::rust_highlight::CATEGORY_COUNT],
	palette: UiPalette,
}

fn minimap_view<'a>(
	snapshot: &'a MinimapSnapshot,
	theme_colors: [Option<Rgb>; libaryon::svp::rust_highlight::CATEGORY_COUNT],
	palette: UiPalette,
) -> Element<'a, EditorMsg> {
	canvas(MinimapWidget {
		snapshot,
		theme_colors,
		palette,
	})
	.width(Length::Fixed(MINIMAP_WIDTH))
	.height(Length::Fill)
	.into()
}

fn minimap_blend(base: Rgb, accent: Rgb, accent_weight: u32, total_weight: u32) -> Rgb {
	let base_weight = total_weight - accent_weight;
	let channel = |base: u8, accent: u8| {
		((u32::from(base) * base_weight + u32::from(accent) * accent_weight) / total_weight) as u8
	};
	Rgb::new(
		channel(base.r, accent.r),
		channel(base.g, accent.g),
		channel(base.b, accent.b),
	)
}

struct MinimapPixels {
	width: u32,
	height: u32,
	rgba: Vec<u8>,
}

impl MinimapPixels {
	fn new(width: u32, height: u32, base: Rgb) -> Self {
		let mut rgba = Vec::with_capacity((width * height * 4) as usize);
		for _ in 0..width * height {
			rgba.extend_from_slice(&[base.r, base.g, base.b, 255]);
		}
		Self {
			width,
			height,
			rgba,
		}
	}

	fn put(&mut self, x: u32, y: u32, color: Rgb) {
		if x >= self.width || y >= self.height {
			return;
		}
		let idx = ((y * self.width + x) * 4) as usize;
		self.rgba[idx..idx + 3].copy_from_slice(&[color.r, color.g, color.b]);
	}

	fn fill(&mut self, x0: u32, y0: u32, x1: u32, y1: u32, color: Rgb) {
		for y in y0..y1.min(self.height) {
			for x in x0..x1.min(self.width) {
				self.put(x, y, color);
			}
		}
	}
}

impl MinimapWidget<'_> {
	fn token_color(&self, bin: libaryon::core::TokenBin, base: Rgb) -> Option<Rgb> {
		use libaryon::engine::EMPTY_PREVIEW_BIN;
		if bin == EMPTY_PREVIEW_BIN {
			return None;
		}
		let accent = self
			.theme_colors
			.get(bin.index())
			.and_then(|color| *color)
			.unwrap_or(self.palette.editor_text);
		Some(minimap_blend(base, accent, 5, 6))
	}

	/// Vertical pixels of image actually covered by document content. Short
	/// files top-align at 2px per line instead of stretching to fill.
	fn content_height(&self, image_height: u32) -> u32 {
		let total_rows = self.total_rows();
		if total_rows == 0 {
			return 0;
		}
		let line_px = self.sparse_line_px();
		(total_rows as u32)
			.saturating_mul(line_px)
			.min(image_height)
	}

	fn total_rows(&self) -> usize {
		match self.snapshot {
			MinimapSnapshot::Preview(preview) => preview.rows.len(),
			MinimapSnapshot::Overview(_) | MinimapSnapshot::ByteFallback(_) => {
				usize::try_from(self.snapshot.overlay().doc_line_count.get()).unwrap_or(usize::MAX)
			}
		}
	}

	fn sparse_line_px(&self) -> u32 {
		if self.total_rows() <= 64 {
			2 * MINIMAP_SCALE
		} else {
			MINIMAP_SCALE
		}
	}

	fn render_ink(&self, image: &mut MinimapPixels, content_width: u32, content_height: u32) {
		let base = self.palette.minimap_surface;
		match self.snapshot {
			MinimapSnapshot::Preview(preview) => {
				use libaryon::engine::{dominant_preview_bin_for_rows, sample_preview_bin_range};
				let total_rows = preview.rows.len();
				if total_rows == 0 {
					return;
				}
				let line_px = self.sparse_line_px();
				if (total_rows as u32).saturating_mul(line_px) <= image.height {
					// Sparse: one band per source line, width from the line's
					// logical width, per-column dominant token color.
					let max_columns = u32::from(preview.max_columns.max(1));
					for (row_idx, row) in preview.rows.iter().enumerate() {
						if row.logical_width == 0 {
							continue;
						}
						let row_width = (u32::from(row.logical_width)
							.saturating_mul(content_width))
						.div_ceil(max_columns)
						.clamp(1, content_width);
						let y0 = row_idx as u32 * line_px;
						let y1 = (y0 + line_px).min(image.height);
						let ink_top = if line_px > 2 { y0 + 1 } else { y0 };
						let ink_bottom = if line_px > 2 {
							y1.saturating_sub(1)
						} else {
							y1
						};
						for x in 0..row_width {
							let bin = sample_preview_bin_range(
								row,
								x.saturating_sub(1) as usize,
								(x + 2).min(row_width) as usize,
								row_width as usize,
							);
							let Some(color) = self.token_color(bin, base) else {
								continue;
							};
							image.fill(x, ink_top, x + 1, ink_bottom.max(ink_top + 1), color);
						}
					}
				} else {
					// Dense: each pixel row aggregates a slice of source rows;
					// coverage modulates how far the token color leans off the
					// surface so sparse regions read lighter.
					for y in 0..content_height {
						let row_start = (y as usize * total_rows) / content_height.max(1) as usize;
						let row_end = ((y as usize + 1) * total_rows)
							.div_ceil(content_height.max(1) as usize)
							.max(row_start + 1)
							.min(total_rows);
						let rows = &preview.rows[row_start.min(total_rows - 1)..row_end];
						for x in 0..content_width {
							let Some((bin, non_empty, sampled)) = dominant_preview_bin_for_rows(
								rows,
								x as usize,
								x as usize + 1,
								content_width as usize,
							) else {
								continue;
							};
							let Some(token) = self.token_color(bin, base) else {
								continue;
							};
							let coverage =
								(2 + (u32::from(non_empty) * 6) / u32::from(sampled.max(1))).min(8);
							image.put(x, y, minimap_blend(base, token, coverage, 8));
						}
					}
				}
			}
			MinimapSnapshot::Overview(overview) | MinimapSnapshot::ByteFallback(overview) => {
				if overview.bands.is_empty() {
					return;
				}
				let accent = self.palette.minimap_accent;
				for y in 0..content_height {
					let band = (y as usize * overview.bands.len()) / content_height.max(1) as usize;
					let density = overview.bands[band.min(overview.bands.len() - 1)];
					if density == 0 {
						continue;
					}
					let line_width = (u32::from(density) * content_width / 255).max(1);
					image.fill(0, y, line_width.min(content_width), y + 1, accent);
				}
			}
		}
	}

	fn render_search_markers(&self, image: &mut MinimapPixels, content_height: u32) {
		let overlay = self.snapshot.overlay();
		if content_height == 0 || overlay.search_bands.is_empty() {
			return;
		}
		let active = self.palette.search_highlight;
		let inactive = minimap_blend(self.palette.minimap_surface, active, 3, 5);
		let marker_x = image
			.width
			.saturating_sub(MINIMAP_MARKER_WIDTH * MINIMAP_SCALE);
		let bands = overlay.search_bands.len() as u32;
		for (band_idx, intensity) in overlay.search_bands.iter().copied().enumerate() {
			if intensity == 0 {
				continue;
			}
			let y0 = (band_idx as u32 * content_height) / bands;
			let y1 = (((band_idx as u32 + 1) * content_height) / bands).max(y0 + 1);
			let color = if overlay.active_search_band == Some(band_idx) {
				active
			} else {
				inactive
			};
			image.fill(marker_x, y0, image.width, y1, color);
		}
	}
}

/// Cached ink image. The app polls the engine on a 16ms tick, so `draw` runs
/// at frame rate; regenerating the pixel buffer (and minting a fresh
/// `image::Handle`, which defeats iced's texture cache) every frame pins a
/// core. The ink only changes on edits, theme swaps, or resizes — everything
/// else (viewport frame, cursor, hover) is drawn as vector overlays — so the
/// handle is reused until the fingerprint of its actual inputs changes.
#[derive(Default)]
struct MinimapInkCache(std::cell::RefCell<Option<(u64, iced::advanced::image::Handle)>>);

impl MinimapWidget<'_> {
	/// Hash of everything `render_ink` + `render_search_markers` read. Preview
	/// rows are immutable for the lifetime of a projected viewport, so their
	/// allocation identity is the content revision here. A replacement
	/// projection is allocated before the current one is dropped, which keeps
	/// the identity unambiguous while avoiding an O(document size) scan on every
	/// 16ms Iced redraw.
	///
	/// The overlay's viewport/cursor fields are deliberately excluded: they
	/// change on every scroll and keystroke but never touch the ink image.
	fn ink_fingerprint(&self, width: u32, height: u32) -> u64 {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::collections::hash_map::DefaultHasher::new();
		width.hash(&mut hasher);
		height.hash(&mut hasher);
		match self.snapshot {
			MinimapSnapshot::Preview(preview) => {
				0u8.hash(&mut hasher);
				preview.max_columns.hash(&mut hasher);
				preview.rows.len().hash(&mut hasher);
				(preview.rows.as_ptr() as usize).hash(&mut hasher);
			}
			MinimapSnapshot::Overview(overview) | MinimapSnapshot::ByteFallback(overview) => {
				1u8.hash(&mut hasher);
				overview.bands.hash(&mut hasher);
			}
		}
		let overlay = self.snapshot.overlay();
		overlay.search_bands.hash(&mut hasher);
		overlay.active_search_band.hash(&mut hasher);
		for color in self.theme_colors.iter().flatten().chain([
			&self.palette.minimap_surface,
			&self.palette.minimap_accent,
			&self.palette.search_highlight,
			&self.palette.editor_text,
		]) {
			(color.r, color.g, color.b).hash(&mut hasher);
		}
		hasher.finish()
	}

	fn ink_handle(
		&self,
		cache: &MinimapInkCache,
		width: u32,
		height: u32,
	) -> iced::advanced::image::Handle {
		let fingerprint = self.ink_fingerprint(width, height);
		let mut slot = cache.0.borrow_mut();
		if let Some((cached_fingerprint, handle)) = slot.as_ref()
			&& *cached_fingerprint == fingerprint
		{
			return handle.clone();
		}
		let mut image = MinimapPixels::new(width, height, self.palette.minimap_surface);
		let content_width = width.saturating_sub(MINIMAP_MARKER_WIDTH * MINIMAP_SCALE);
		let content_height = self.content_height(height);
		self.render_ink(&mut image, content_width, content_height);
		self.render_search_markers(&mut image, content_height);
		let handle =
			iced::advanced::image::Handle::from_rgba(image.width, image.height, image.rgba);
		*slot = Some((fingerprint, handle.clone()));
		handle
	}
}

impl canvas::Program<EditorMsg> for MinimapWidget<'_> {
	type State = MinimapInkCache;

	fn draw(
		&self,
		state: &Self::State,
		renderer: &iced::Renderer,
		_theme: &iced::Theme,
		bounds: iced::Rectangle,
		_cursor: mouse::Cursor,
	) -> Vec<canvas::Geometry> {
		let mut frame = canvas::Frame::new(renderer, bounds.size());
		let width = (bounds.width.max(1.0) as u32) * MINIMAP_SCALE;
		let height = (bounds.height.max(1.0) as u32) * MINIMAP_SCALE;
		let content_height = self.content_height(height);
		frame.draw_image(
			iced::Rectangle::new(iced::Point::ORIGIN, bounds.size()),
			iced::advanced::image::Image::new(self.ink_handle(state, width, height)),
		);

		// Viewport frame and cursor line as vector overlays on top of the ink.
		let overlay = self.snapshot.overlay();
		let doc_lines = overlay.doc_line_count.get().max(1);
		let content_height_logical = content_height as f32 / MINIMAP_SCALE as f32;
		let y_of = |line: u64| (line as f32 / doc_lines as f32) * content_height_logical;
		if content_height > 0 {
			let top = y_of(overlay.viewport_start_line.get());
			let bottom = y_of(overlay.viewport_end_line.get()).max(top + 1.0);
			let accent = color(self.palette.minimap_accent);
			let frame_color = Color { a: 0.85, ..accent };
			let size = bounds.size();
			frame.fill_rectangle(
				iced::Point::new(0.0, top),
				iced::Size::new(size.width, 1.0),
				frame_color,
			);
			frame.fill_rectangle(
				iced::Point::new(0.0, bottom - 1.0),
				iced::Size::new(size.width, 1.0),
				frame_color,
			);
			frame.fill_rectangle(
				iced::Point::new(0.0, top),
				iced::Size::new(1.0, bottom - top),
				frame_color,
			);
			frame.fill_rectangle(
				iced::Point::new(size.width - 1.0, top),
				iced::Size::new(1.0, bottom - top),
				frame_color,
			);
			let cursor_y = y_of(overlay.cursor_line.get());
			frame.fill_rectangle(
				iced::Point::new(0.0, cursor_y),
				iced::Size::new(size.width, 1.0),
				accent,
			);
		}
		vec![frame.into_geometry()]
	}

	fn update(
		&self,
		_state: &mut Self::State,
		event: &Event,
		bounds: iced::Rectangle,
		cursor: mouse::Cursor,
	) -> Option<canvas::Action<EditorMsg>> {
		let Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)) = event else {
			return None;
		};
		let position = cursor.position_in(bounds)?;
		let overlay = self.snapshot.overlay();
		let content_height = self.content_height((bounds.height.max(1.0) as u32) * MINIMAP_SCALE)
			as f32 / MINIMAP_SCALE as f32;
		if content_height <= 0.0 {
			return None;
		}
		let fraction = (position.y / content_height).clamp(0.0, 1.0);
		let line = ((fraction as f64) * overlay.doc_line_count.get().max(1) as f64) as u64;
		let line = line.min(overlay.doc_line_count.get().saturating_sub(1));
		Some(canvas::Action::publish(EditorMsg::MinimapGoto(
			DocLine::new(line),
		)))
	}

	fn mouse_interaction(
		&self,
		_state: &Self::State,
		bounds: iced::Rectangle,
		cursor: mouse::Cursor,
	) -> mouse::Interaction {
		if cursor.is_over(bounds) {
			mouse::Interaction::Pointer
		} else {
			mouse::Interaction::default()
		}
	}
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
	use super::{
		CodeEditor, DASHBOARD_PANEL, EditorCommand, EditorMsg, InputEvent, KeyCode, LanguageId,
		Modifiers, PANE_HEADER_HEIGHT, SplitDirection, is_markdown_thematic_break,
		markdown_code_fragments, metrics, pane_title,
	};
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
	fn minimap_ink_handle_is_cached_until_content_changes() {
		use super::{MinimapInkCache, MinimapWidget};
		use libaryon::core::{DocLine, TokenBin};
		use libaryon::engine::{
			MinimapOverlay, MinimapSnapshot, PREVIEW_BIN_COLUMNS, PreviewRow, PreviewSnapshot,
			UiPalette,
		};
		use libaryon::svp::rust_highlight::{CATEGORY_COUNT, TokenCategory};

		let make_snapshot = |category: TokenCategory| {
			MinimapSnapshot::Preview(PreviewSnapshot {
				overlay: MinimapOverlay::new(DocLine::ZERO, 2, DocLine::ZERO),
				max_columns: 8,
				rows: vec![PreviewRow {
					logical_width: 8,
					bins: vec![TokenBin::new(category as u8); PREVIEW_BIN_COLUMNS]
						.into_boxed_slice(),
				}],
			})
		};
		let snapshot = make_snapshot(TokenCategory::Keyword);
		let widget = MinimapWidget {
			snapshot: &snapshot,
			theme_colors: [None; CATEGORY_COUNT],
			palette: UiPalette::default(),
		};
		let cache = MinimapInkCache::default();
		let first = widget.ink_handle(&cache, 64, 64);
		let second = widget.ink_handle(&cache, 64, 64);
		assert_eq!(
			first.id(),
			second.id(),
			"unchanged content must reuse the handle"
		);

		let edited = make_snapshot(TokenCategory::Function);
		let widget = MinimapWidget {
			snapshot: &edited,
			..widget
		};
		let third = widget.ink_handle(&cache, 64, 64);
		assert_ne!(first.id(), third.id(), "edits must rebuild the ink image");
	}

	#[test]
	fn minimap_ink_uses_per_column_token_colors() {
		use super::{MinimapPixels, MinimapWidget};
		use libaryon::core::DocLine;
		use libaryon::core::Rgb;
		use libaryon::engine::{
			EMPTY_PREVIEW_BIN, MinimapOverlay, MinimapSnapshot, PREVIEW_BIN_COLUMNS, PreviewRow,
			PreviewSnapshot, UiPalette,
		};
		use libaryon::svp::rust_highlight::{CATEGORY_COUNT, TokenCategory};

		let keyword = libaryon::core::TokenBin::new(TokenCategory::Keyword as u8);
		let function = libaryon::core::TokenBin::new(TokenCategory::Function as u8);
		// Row 0: left half keywords, right half functions. Row 1: empty.
		let mut bins = vec![keyword; PREVIEW_BIN_COLUMNS];
		for bin in bins.iter_mut().skip(PREVIEW_BIN_COLUMNS / 2) {
			*bin = function;
		}
		let rows = vec![
			PreviewRow {
				logical_width: 8,
				bins: bins.into_boxed_slice(),
			},
			PreviewRow {
				logical_width: 0,
				bins: vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS].into_boxed_slice(),
			},
		];
		let snapshot = MinimapSnapshot::Preview(PreviewSnapshot {
			overlay: MinimapOverlay::new(DocLine::ZERO, 2, DocLine::ZERO),
			max_columns: 8,
			rows,
		});
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));
		theme_colors[TokenCategory::Function as usize] = Some(Rgb::new(0, 255, 0));
		let widget = MinimapWidget {
			snapshot: &snapshot,
			theme_colors,
			palette: UiPalette::default(),
		};

		let base = widget.palette.minimap_surface;
		let mut image = MinimapPixels::new(64, 64, base);
		let content_height = widget.content_height(64);
		widget.render_ink(&mut image, 64, content_height);

		let pixel = |x: u32, y: u32| {
			let idx = ((y * 64 + x) * 4) as usize;
			(image.rgba[idx], image.rgba[idx + 1], image.rgba[idx + 2])
		};
		// Row 0 spans the top band: left ink leans red, right ink leans green.
		let (lr, lg, _) = pixel(8, 1);
		let (rr, rg, _) = pixel(56, 1);
		assert!(lr > lg, "left half should carry the keyword color");
		assert!(rg > rr, "right half should carry the function color");
		// The empty row leaves the surface untouched.
		let empty_y = widget.sparse_line_px() + 1;
		assert_eq!(pixel(8, empty_y), (base.r, base.g, base.b));
	}

	#[test]
	fn cached_content_converges_via_engine_wake_alone() {
		let mut editor = CodeEditor::new("");
		// Simulate the GUI: typing forwards input + requests content; the wake
		// subscription then delivers EngineReady, which polls.
		send_keys(&mut editor, "i".chars());
		send_keys(&mut editor, "select 1 as x".chars());
		let deadline = std::time::Instant::now() + Duration::from_secs(10);
		while std::time::Instant::now() < deadline {
			let _ = editor.update(EditorMsg::EngineReady);
			if editor.cached_content().contains("select 1 as x") {
				break;
			}
			thread::sleep(Duration::from_millis(10));
		}
		assert!(
			editor.cached_content().contains("select 1 as x"),
			"cached_content: {:?}",
			editor.cached_content()
		);
	}
	#[test]
	fn cached_content_tracks_typing_in_new_independent_pane() {
		let mut editor = CodeEditor::new("");
		let pump = |editor: &mut CodeEditor| {
			let deadline = std::time::Instant::now() + Duration::from_millis(500);
			while std::time::Instant::now() < deadline {
				let _ = editor.update(EditorMsg::EngineReady);
				thread::sleep(Duration::from_millis(5));
			}
		};
		// Open an independent vertical pane, then type into it.
		send_keys(&mut editor, ":pv".chars());
		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Enter,
			modifiers: Modifiers::NONE,
		}));
		pump(&mut editor);
		send_keys(&mut editor, "i".chars());
		send_keys(&mut editor, "select 1 as x".chars());
		let deadline = std::time::Instant::now() + Duration::from_secs(10);
		while std::time::Instant::now() < deadline {
			let _ = editor.update(EditorMsg::EngineReady);
			if editor.cached_content().contains("select 1 as x") {
				break;
			}
			thread::sleep(Duration::from_millis(10));
		}
		assert!(
			editor.cached_content().contains("select 1 as x"),
			"cached_content after :pv: {:?}",
			editor.cached_content()
		);
		assert_eq!(
			editor.selected_or_current_line().as_deref(),
			Some("select 1 as x")
		);
	}

	#[test]
	fn text_pane_title_tracks_the_active_language() {
		let mut editor = CodeEditor::new("# Markdown\n");
		wait_for_content(&mut editor, "# Markdown\n");
		editor.send(EditorCommand::SetLanguage(Some("markdown".to_owned())));
		let deadline = Instant::now() + Duration::from_secs(5);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			viewport
				.panes
				.iter()
				.find(|pane| pane.pane_id == viewport.focused_pane_id)
				.is_none_or(|pane| pane.language_label != "Markdown")
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}

		let viewport = editor.viewport.as_ref().expect("viewport");
		let pane = viewport
			.panes
			.iter()
			.find(|pane| pane.pane_id == viewport.focused_pane_id)
			.expect("focused pane");
		assert_eq!(pane_title(pane), "Markdown");
		assert_eq!(
			editor.active_language_id().map(LanguageId::as_str),
			Some("markdown")
		);
	}

	#[test]
	fn markdown_thematic_breaks_accept_standard_markers() {
		for line in ["---", "***", "___", "- - -", " *  * * "] {
			assert!(is_markdown_thematic_break(line), "{line:?}");
		}
		for line in ["--", "--- text", "-_*", "```"] {
			assert!(!is_markdown_thematic_break(line), "{line:?}");
		}
	}

	#[test]
	fn markdown_fence_fragments_resolve_engine_categories_through_its_palette() {
		use libaryon::core::{DocByte, Rgb};
		use libaryon::svp::rust_highlight::{CATEGORY_COUNT, HighlightSpan, TokenCategory};

		let mut palette = [None; CATEGORY_COUNT];
		palette[TokenCategory::Keyword as usize] = Some(Rgb::new(153, 0, 255));
		let fragments = markdown_code_fragments(
			"select 1",
			100,
			&[HighlightSpan::new(
				DocByte::new(100),
				DocByte::new(106),
				TokenCategory::Keyword,
			)],
			palette,
			Rgb::new(255, 255, 255),
		);

		assert_eq!(fragments.len(), 8);
		assert_eq!(
			fragments[0].color,
			Some(iced::Color::from_rgb8(153, 0, 255))
		);
		assert_eq!(
			fragments[6].color,
			Some(iced::Color::from_rgb8(255, 255, 255))
		);
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

	#[test]
	fn pane_drop_edges_make_center_a_swap_and_edges_a_split_intent() {
		use super::{PaneDropEdge, drop_edge};
		let bounds = iced::Rectangle {
			x: 100.0,
			y: 200.0,
			width: 400.0,
			height: 300.0,
		};
		assert_eq!(
			drop_edge(bounds, iced::Point::new(300.0, 350.0)),
			PaneDropEdge::Center
		);
		assert_eq!(
			drop_edge(bounds, iced::Point::new(105.0, 350.0)),
			PaneDropEdge::Left
		);
		assert_eq!(
			drop_edge(bounds, iced::Point::new(495.0, 350.0)),
			PaneDropEdge::Right
		);
		assert_eq!(
			drop_edge(bounds, iced::Point::new(300.0, 205.0)),
			PaneDropEdge::Top
		);
		assert_eq!(
			drop_edge(bounds, iced::Point::new(300.0, 495.0)),
			PaneDropEdge::Bottom
		);
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
			split_direction: SplitDirection::Vertical,
			focus: false,
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
			split_direction: SplitDirection::Vertical,
			focus: true,
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
	fn opening_query_results_keeps_workspace_focus_in_the_source_pane() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		let source_pane = editor
			.viewport
			.as_ref()
			.expect("initial viewport")
			.focused_pane_id;

		editor.begin_query();
		wait_for_host_panel(&mut editor);

		assert_eq!(
			editor
				.viewport
				.as_ref()
				.expect("result viewport")
				.focused_pane_id,
			source_pane,
			"running a query must not make the source editor stop accepting text"
		);
		let viewport = editor.viewport.as_ref().expect("result viewport");
		let source = viewport.pane(source_pane).expect("source pane");
		let results = viewport
			.panes
			.iter()
			.find(|pane| pane.host_panel.is_some())
			.expect("result pane");
		assert_eq!(
			results.rect.screen_col, source.rect.screen_col,
			"results retain the source column"
		);
		assert!(
			results.rect.screen_row > source.rect.screen_row,
			"results open below their source rather than in a right-hand split"
		);
	}

	#[test]
	fn clicking_a_host_panel_moves_workspace_focus_to_its_pane() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.begin_query();
		wait_for_host_panel(&mut editor);
		let panel = editor
			.viewport
			.as_ref()
			.expect("result viewport")
			.panes
			.iter()
			.find(|pane| pane.host_panel.is_some())
			.expect("result pane");
		let panel_id = panel.pane_id;
		let point = iced::Point::new(
			(panel.rect.screen_col.get() + 1) as f32 * metrics::CELL_WIDTH,
			(panel.rect.screen_row.get() + 1) as f32 * metrics::LINE_HEIGHT,
		);
		let _ = editor.update(EditorMsg::PointerMoved(point));
		let _ = editor.update(EditorMsg::Click);

		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|viewport| viewport.focused_pane_id != panel_id)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(
			editor
				.viewport
				.as_ref()
				.expect("focused viewport")
				.focused_pane_id,
			panel_id,
			"a host-panel click must update Baryon's workspace focus"
		);
	}

	#[test]
	fn opening_dashboard_creates_and_focuses_its_host_panel() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.open_dashboard();

		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			!viewport.panes.iter().any(|pane| {
				pane.host_panel
					.as_ref()
					.is_some_and(|panel| panel.panel_id == DASHBOARD_PANEL)
			})
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let viewport = editor.viewport.as_ref().expect("dashboard viewport");
		assert_eq!(
			viewport.focused_pane_id,
			viewport
				.panes
				.iter()
				.find(|pane| {
					pane.host_panel
						.as_ref()
						.is_some_and(|panel| panel.panel_id == DASHBOARD_PANEL)
				})
				.expect("dashboard pane")
				.pane_id,
			"opening the dashboard intentionally transfers workspace focus"
		);
	}

	#[test]
	fn subscription_window_coords_convert_to_editor_local() {
		use iced::Point;
		use libaryon::engine::SplitDirection;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.resize(1200.0, 800.0);
		editor.send(EditorCommand::SplitPane(SplitDirection::Horizontal));
		let deadline = Instant::now() + Duration::from_secs(5);
		while editor.viewport.as_ref().is_none_or(|v| {
			!v.dividers
				.iter()
				.any(|d| d.axis == SplitDirection::Horizontal)
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let divider = editor
			.viewport
			.as_ref()
			.unwrap()
			.dividers
			.iter()
			.find(|d| d.axis == SplitDirection::Horizontal)
			.expect("horizontal divider")
			.to_owned();
		// A point squarely inside the hit band, in editor-local pixels.
		let band_row = divider.hit_rect.screen_row.get();
		let local_y = (band_row as f32 + 0.5) * metrics::LINE_HEIGHT;
		let local = Point::new(300.0, local_y);

		// Simulate the workspace chrome: the subscription reports window
		// coordinates offset from editor-local by 69px (menu bar + header).
		const CHROME: f32 = 69.0;
		let window = Point::new(local.x, local.y + CHROME);

		// The offset is learned when both sources report the same move.
		let _ = editor.update(EditorMsg::RawCursorMoved(window));
		let _ = editor.update(EditorMsg::PointerMoved(local));
		assert_eq!(editor.last_pointer, Some(local));

		// A subscription-only move (as when the cursor is over a capturing
		// table) must convert back to editor-local, not corrupt last_pointer
		// with the window value.
		editor.drag = None;
		let _ = editor.update(EditorMsg::RawCursorMoved(window));
		let lp = editor.last_pointer.expect("last pointer");
		assert!(
			(lp.y - local.y).abs() < 1.0,
			"subscription window coords convert to editor-local: got {lp:?}, want ~{local:?}"
		);
		assert!(
			editor.begin_divider_drag(),
			"the converted point still resolves the divider"
		);
		assert_eq!(
			editor.drag.map(|d| d.axis),
			Some(SplitDirection::Horizontal)
		);
	}

	#[test]
	fn horizontal_divider_grab_band_spans_beyond_its_one_row_gap() {
		use libaryon::engine::SplitDirection;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.resize(1200.0, 800.0);
		editor.send(EditorCommand::SplitPane(SplitDirection::Horizontal));
		let deadline = Instant::now() + Duration::from_secs(5);
		while editor.viewport.as_ref().is_none_or(|v| {
			!v.dividers
				.iter()
				.any(|d| d.axis == SplitDirection::Horizontal)
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}

		let divider = editor
			.viewport
			.as_ref()
			.unwrap()
			.dividers
			.iter()
			.find(|d| d.axis == SplitDirection::Horizontal)
			.expect("a horizontal split projects a horizontal divider")
			.to_owned();

		// The one-row visual gap would be a ~22px grab target. The engine widens
		// it upward, while leaving the lower pane's title bar exclusively for
		// pane reordering.
		assert_eq!(divider.visual_rect.height.get(), 1);
		assert!(
			divider.hit_rect.height.get() >= 2,
			"horizontal grab band must extend past its single-row gap, got {}",
			divider.hit_rect.height.get()
		);
		assert!(divider.hit_rect.screen_row.get() < divider.visual_rect.screen_row.get());
		assert_eq!(divider.hit_rect.bottom(), divider.visual_rect.bottom());

		// Every pixel row across the widened band resolves to the drag — the
		// gap itself and the padding row above it, but not the bottom pane header.
		let top = divider.hit_rect.screen_row.get();
		let bottom = divider.hit_rect.bottom().get();
		for row in top..bottom {
			let y = (row as f32 + 0.5) * metrics::LINE_HEIGHT;
			editor.last_pointer = Some(iced::Point::new(300.0, y));
			editor.drag = None;
			assert!(
				editor.begin_divider_drag(),
				"row {row} inside the hit band must start a drag"
			);
			assert_eq!(
				editor.drag.map(|d| d.axis),
				Some(SplitDirection::Horizontal)
			);
		}

		let lower_pane = editor
			.viewport
			.as_ref()
			.unwrap()
			.panes
			.iter()
			.find(|pane| pane.pane_id == divider.after)
			.expect("lower pane");
		let header_point = iced::Point::new(
			300.0,
			lower_pane.rect.screen_row.get() as f32 * metrics::LINE_HEIGHT
				+ PANE_HEADER_HEIGHT / 2.0,
		);
		editor.last_pointer = Some(header_point);
		editor.drag = None;
		assert!(
			!editor.begin_divider_drag(),
			"the lower pane header must not start divider resizing"
		);
		editor.begin_pane_drag();
		assert_eq!(
			editor.pane_drag.map(|drag| drag.source),
			Some(divider.after),
			"the lower pane header starts pane reordering"
		);
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
			split_direction: SplitDirection::Vertical,
			focus: false,
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

		// Non-row commands must complete the pane too; otherwise a notebook
		// `output=pane` CREATE/INSERT would leave it stuck on `running…`.
		editor.begin_query();
		editor.set_query_message("Rows affected: 3".to_owned());
		let PanelContent::Message(message) = &editor.panels.panels[&RESULT_PANEL].content else {
			panic!("a completed non-row query replaces the running panel state");
		};
		assert_eq!(message, "Rows affected: 3");
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
	fn declined_keys_scroll_the_focused_result_table() {
		use super::{PanelContent, RESULT_PANEL};
		use crate::gui::table::TableScroll;
		use libaryon::engine::HostEvent;

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.begin_query();
		wait_for_host_panel(&mut editor);
		editor.set_query_result(polars::df!("n" => [1i64, 2, 3]).expect("frame"));

		let key = |code| HostEvent::UnhandledInput {
			panel_id: RESULT_PANEL,
			input: InputEvent::Key {
				code,
				modifiers: Modifiers::NONE,
			},
		};
		// Baryon has already decided these are not workspace commands; what
		// they mean inside the panel is Polariton's business.
		for (code, expected) in [
			(KeyCode::Char('j'), TableScroll::LineDown),
			(KeyCode::Char('k'), TableScroll::LineUp),
			(KeyCode::Down, TableScroll::LineDown),
			(KeyCode::PageDown, TableScroll::PageDown),
			(KeyCode::Home, TableScroll::Top),
			(KeyCode::End, TableScroll::Bottom),
		] {
			editor.panels.reconcile(&[key(code)], None);
			let (_, action) = editor.panels.panels[&RESULT_PANEL]
				.scroll
				.expect("a declined key produces a scroll action");
			assert_eq!(action, expected, "{code:?} scrolls the table");
		}

		// Each key applies once: the sequence advances so the widget cannot
		// re-apply a stale request every frame.
		let (first, _) = editor.panels.panels[&RESULT_PANEL].scroll.expect("scroll");
		editor.panels.reconcile(&[key(KeyCode::Char('j'))], None);
		let (second, _) = editor.panels.panels[&RESULT_PANEL].scroll.expect("scroll");
		assert!(second > first, "each declined key gets a fresh sequence");

		// Keys the table has no meaning for are ignored rather than guessed at.
		editor.panels.reconcile(&[key(KeyCode::Char('z'))], None);
		let (third, _) = editor.panels.panels[&RESULT_PANEL].scroll.expect("scroll");
		assert_eq!(third, second, "an unmapped key leaves the panel untouched");

		// A panel with no table ignores input entirely.
		editor
			.panels
			.set_content(RESULT_PANEL, PanelContent::Running);
		editor.panels.reconcile(&[key(KeyCode::Char('j'))], None);
		let (fourth, _) = editor.panels.panels[&RESULT_PANEL].scroll.expect("scroll");
		assert_eq!(fourth, third, "input is inert while the panel has no table");
	}

	#[test]
	fn a_capturing_widget_would_hide_workspace_keys_from_baryon() {
		use super::editor_input_for;
		use iced::keyboard::key::Named;
		use iced::{Event, event, keyboard};

		let colon = Event::Keyboard(keyboard::Event::KeyPressed {
			key: keyboard::Key::Character(";".into()),
			modified_key: keyboard::Key::Character(":".into()),
			physical_key: keyboard::key::Physical::Unidentified(
				keyboard::key::NativeCode::Unidentified,
			),
			location: keyboard::Location::Standard,
			modifiers: keyboard::Modifiers::SHIFT,
			text: Some(":".into()),
			repeat: false,
		});

		// Unconsumed keys reach the engine, which is how `:` opens the prompt.
		assert!(
			editor_input_for(&colon, event::Status::Ignored).is_some(),
			"an unconsumed key reaches Baryon"
		);

		// Consumed keys are dropped so app chrome (connection name, statement
		// title) does not also edit the SQL document. The same rule means a
		// host panel that captured keyboard input would swallow `:` and pane
		// navigation before Baryon saw them — so native panels must not
		// capture, and adding one that must needs a root-level arbiter rather
		// than a local change. This assertion exists to make that trade-off
		// impossible to change silently.
		assert!(
			editor_input_for(&colon, event::Status::Captured).is_none(),
			"a captured key never reaches Baryon: host panels must not capture"
		);

		let arrow = Event::Keyboard(keyboard::Event::KeyPressed {
			key: keyboard::Key::Named(Named::ArrowDown),
			modified_key: keyboard::Key::Named(Named::ArrowDown),
			physical_key: keyboard::key::Physical::Unidentified(
				keyboard::key::NativeCode::Unidentified,
			),
			location: keyboard::Location::Standard,
			modifiers: keyboard::Modifiers::empty(),
			text: None,
			repeat: false,
		});
		assert!(editor_input_for(&arrow, event::Status::Ignored).is_some());
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
		let lines = project_lines(pane, palette.editor_text, palette.accent, true);
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
			project_lines(pane, palette.editor_text, palette.accent, true),
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
		use super::{EditorMsg, PANE_HEADER_HEIGHT};
		use iced::Point;

		let mut editor = CodeEditor::new("select 1;\nselect 2;\nselect 3;");
		wait_for_content(&mut editor, "select 1;\nselect 2;\nselect 3;");
		let _ = editor.update(EditorMsg::PointerMoved(Point::new(
			54.0 + 9.6 * 3.0,
			PANE_HEADER_HEIGHT + 8.0 + 22.0,
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
			.is_none_or(|pane| {
				pane.selection_ranges
					.first()
					.is_none_or(|(start, end)| end.get().saturating_sub(start.get()) < 1)
			}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.selected_or_current_line().as_deref(), Some("se"));
	}

	/// Build a notebook editor with a Markdown preview and wait for its cells.
	fn notebook_with_preview(markdown: &str) -> CodeEditor {
		let mut editor = CodeEditor::new("");
		wait_for_content(&mut editor, "");
		editor.send(EditorCommand::ReplaceDocument {
			text: markdown.to_owned(),
			language: Some("markdown".to_owned()),
			virtual_path: Some("notebook.md".to_owned()),
		});
		editor.request_content();
		wait_for_content(&mut editor, markdown);
		editor.send(EditorCommand::MarkdownPreview { serif: false });
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|v| {
			!v.panes.iter().any(|p| {
				p.markdown_preview
					.as_ref()
					.is_some_and(|preview| !preview.cells.is_empty())
			})
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		editor
	}

	#[test]
	fn cell_extraction_is_exact_across_implicit_and_explicit_fences() {
		// The last cell reproduces the shape that once looked like a `test` ->
		// `tes` truncation. Extraction must be byte-exact, never dropping the
		// final character of the fence body, on both identity tiers.
		let markdown = concat!(
			"# Notebook\n\n",
			"```sql\n",
			"create table if not exists test (x int32);\n",
			"```\n\n",
			"```sql {#trips, output=pane, code=hide}\n",
			"select y, z from test;\n",
			"```\n",
		);
		let editor = notebook_with_preview(markdown);
		let preview = editor
			.viewport
			.as_ref()
			.unwrap()
			.panes
			.iter()
			.find_map(|p| p.markdown_preview.as_ref())
			.expect("preview");
		let codes: Vec<String> = preview
			.cells
			.iter()
			.map(|cell| {
				editor
					.document_text
					.get(cell.body_range.start.get() as usize..cell.body_range.end.get() as usize)
					.expect("cell range in bounds")
					.to_owned()
			})
			.collect();
		assert_eq!(
			codes,
			vec![
				"create table if not exists test (x int32);\n".to_owned(),
				"select y, z from test;\n".to_owned(),
			],
			"fence bodies extract byte-exact on both identity tiers"
		);
		// Identity tiers: bare fence is Implicit, `{#trips}` is Explicit.
		use libaryon::uast::projection::MarkdownCellIdentity;
		assert!(matches!(
			preview.cells[0].identity,
			MarkdownCellIdentity::Implicit { .. }
		));
		assert!(matches!(
			&preview.cells[1].identity,
			MarkdownCellIdentity::Explicit { id } if id == "trips"
		));
		assert_eq!(
			preview.cells[1].presentation.output,
			libaryon::uast::projection::MarkdownCellOutputTarget::Pane
		);
		assert_eq!(
			preview.cells[1].presentation.code,
			libaryon::uast::projection::MarkdownCellCodeVisibility::Hide
		);
	}

	#[test]
	fn rune_cells_dispatch_and_named_sql_results_ignore_presentation() {
		let markdown = concat!(
			"```sql {#trips, output=none}\n",
			"select x, y from trips;\n",
			"```\n\n",
			"```rune {#trips-plot, output=inline, code=hide}\n",
			"viz.plot(workspace.cell(\"trips\")?).scatter().line().show()?;\n",
			"```\n",
		);
		let mut editor = notebook_with_preview(markdown);
		let source = editor
			.viewport
			.as_ref()
			.and_then(|viewport| editor.notebook_source(viewport))
			.expect("Markdown source projection");
		let sql = source
			.markdown_cells
			.iter()
			.find(|cell| cell.language_id == "sql")
			.expect("SQL cell");
		let rune = source
			.markdown_cells
			.iter()
			.find(|cell| cell.language_id == "rune")
			.expect("Rune cell");
		assert!(matches!(
			sql.presentation.output,
			libaryon::uast::projection::MarkdownCellOutputTarget::None
		));
		assert!(matches!(
			rune.presentation.output,
			libaryon::uast::projection::MarkdownCellOutputTarget::Inline
		));
		let sql_identity = sql.identity.clone();

		let frame = polars::df!("x" => [1_i32], "y" => [2_i32]).expect("frame");
		editor.retain_markdown_cell_table(sql_identity.clone(), &frame);
		assert_eq!(
			editor
				.named_notebook_cell_data()
				.get("trips")
				.map(|snapshot| snapshot.height()),
			Some(1),
			"output=none controls presentation, not Rune data availability"
		);
		editor.begin_markdown_cell(sql_identity);
		assert!(
			!editor.named_notebook_cell_data().contains_key("trips"),
			"rerunning a SQL cell retires its prior snapshot before Rune can read stale data"
		);

		// Ctrl+Enter dispatch resolves the language structurally from the
		// engine-projected fence, never by re-parsing Markdown in Polariton.
		send_keys(&mut editor, ['g', 'g', 'j', 'j', 'j', 'j', 'j']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.markdown_cell_at_cursor()
			.is_none_or(|cell| cell.language_id != "rune")
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(
			editor
				.markdown_cell_at_cursor()
				.map(|cell| cell.language_id),
			Some("rune".to_owned())
		);
	}

	#[test]
	fn inline_plot_hover_is_scoped_to_its_cell_identity() {
		use crate::gui::messages::PlotMessage;
		use crate::plot::{composition::PlotComposition, core::PlotType, factory::create_plot};
		use libaryon::uast::projection::MarkdownCellIdentity;

		let frame = polars::df!("x" => [1_i32, 2], "y" => [2_i32, 4]).expect("frame");
		let composition = PlotComposition::single(PlotType::Scatter);
		let kernel = create_plot(&composition, &frame, 600, 360);
		let identity = MarkdownCellIdentity::Explicit {
			id: "trips-plot".to_owned(),
		};
		let mut editor = CodeEditor::new("");
		editor.set_markdown_cell_plot(identity.clone(), composition, kernel);
		let _ = editor.update(EditorMsg::NotebookPlot {
			identity: identity.clone(),
			message: PlotMessage::UpdateHover(Some("x: 1, y: 2".to_owned())),
		});

		let Some(super::NotebookCellOutput::Plot { surface }) =
			editor.notebook_outputs.get(&identity)
		else {
			panic!("the inline plot remains attached to its source cell");
		};
		assert_eq!(surface.hovered_info.as_deref(), Some("x: 1, y: 2"));
	}

	#[test]
	fn run_targets_the_cell_at_the_cursor_even_mid_selection() {
		let markdown = concat!(
			"# Notebook\n\n",
			"```sql\n",
			"select y, z from test;\n",
			"```\n",
		);
		let mut editor = notebook_with_preview(markdown);

		// Cursor into the fence body: down past heading, blank, and fence open.
		send_keys(&mut editor, ['g', 'g', 'j', 'j', 'j']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.markdown_cell_at_cursor().is_none() && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let cell = editor
			.markdown_cell_at_cursor()
			.expect("cursor is inside the SQL fence");
		assert_eq!(cell.code, "select y, z from test;\n");

		// A visual-line selection suppresses whole-cell dispatch for a snippet
		// run, but Run (markdown_cell_at_cursor) must still resolve the cell —
		// this is the fix for Run sending the whole Markdown document as SQL.
		send_keys(&mut editor, ['V']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|v| v.focused_pane())
			.is_none_or(|pane| pane.selection_ranges.is_empty())
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor.selected_markdown_cell().is_none(),
			"a live selection suppresses whole-cell snippet dispatch"
		);
		assert_eq!(
			editor.selected_or_current_line().as_deref(),
			Some("select y, z from test;\n"),
			"a visual-line snippet inside a cell excludes the fence metadata"
		);
		assert_eq!(
			editor
				.markdown_cell_at_cursor()
				.map(|cell| cell.code)
				.as_deref(),
			Some("select y, z from test;\n"),
			"Run resolves the cell at the cursor regardless of selection"
		);
		assert!(
			editor.is_notebook(),
			"a document with a preview is a notebook"
		);
	}

	#[test]
	fn notebook_run_keeps_the_source_context_when_an_output_pane_is_focused() {
		use libaryon::engine::HostPanelId;

		let markdown = "```sql\nselect y, z from test;\n```\n";
		let mut editor = notebook_with_preview(markdown);
		send_keys(&mut editor, ['j']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.markdown_cell_at_cursor().is_none() && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor.markdown_cell_at_cursor().is_some(),
			"the source cursor starts inside the SQL cell"
		);

		editor.send(EditorCommand::OpenHostPanel {
			panel_id: HostPanelId::new(99),
			kind: "result-table".to_owned(),
			title: "Results".to_owned(),
			split_direction: SplitDirection::Vertical,
			focus: true,
		});
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			viewport
				.focused_pane()
				.and_then(|pane| pane.host_panel.as_ref())
				.is_none()
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}

		assert_eq!(
			editor.markdown_cell_at_cursor().map(|cell| cell.code),
			Some("select y, z from test;\n".to_owned()),
			"an output pane cannot make Run fall back to the raw Markdown document"
		);
	}

	#[test]
	fn markdown_cells_remain_runnable_without_a_preview_pane() {
		let markdown = "```sql {#create, output=none}\ncreate table test (x int32);\n```\n";
		let mut editor = CodeEditor::new("");
		wait_for_content(&mut editor, "");
		editor.send(EditorCommand::ReplaceDocument {
			text: markdown.to_owned(),
			language: Some("markdown".to_owned()),
			virtual_path: Some("notebook.md".to_owned()),
		});
		editor.request_content();
		wait_for_content(&mut editor, markdown);
		send_keys(&mut editor, ['j']);

		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.markdown_cell_at_cursor().is_none() && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let cell = editor
			.markdown_cell_at_cursor()
			.expect("fenced SQL remains executable without a preview pane");
		assert_eq!(cell.code, "create table test (x int32);\n");
		assert_eq!(
			cell.presentation.output,
			libaryon::uast::projection::MarkdownCellOutputTarget::None
		);
	}

	#[test]
	fn markdown_sql_fence_runs_as_one_cell() {
		let markdown = "# Notebook\n\n```sql {#trips}\nselect id\nfrom trips;\n```\n";
		let mut editor = CodeEditor::new("");
		wait_for_content(&mut editor, "");
		editor.send(EditorCommand::ReplaceDocument {
			text: markdown.to_owned(),
			language: Some("markdown".to_owned()),
			virtual_path: Some("notebook.md".to_owned()),
		});
		editor.request_content();
		wait_for_content(&mut editor, markdown);
		editor.send(EditorCommand::MarkdownPreview { serif: false });

		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			!viewport
				.panes
				.iter()
				.any(|pane| pane.markdown_preview.is_some())
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(editor.viewport.as_ref().is_some_and(|viewport| {
			viewport
				.panes
				.iter()
				.any(|pane| pane.markdown_preview.is_some())
		}));

		send_keys(&mut editor, ['j', 'j', 'j']);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.selected_markdown_cell().is_none() && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let cell = editor
			.selected_markdown_cell()
			.expect("cursor is in SQL fence");
		assert_eq!(cell.code, "select id\nfrom trips;\n");
		assert!(matches!(
			cell.identity,
			libaryon::uast::projection::MarkdownCellIdentity::Explicit { ref id } if id == "trips"
		));
		let identity = cell.identity.clone();
		editor.begin_markdown_cell(identity.clone());
		assert!(matches!(
			editor.notebook_outputs.get(&identity),
			Some(super::NotebookCellOutput::Running)
		));
		editor.set_markdown_cell_message(identity.clone(), "Rows affected: 3".to_owned());
		assert!(matches!(
			editor.notebook_outputs.get(&identity),
			Some(super::NotebookCellOutput::Message(message)) if message == "Rows affected: 3"
		));
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
