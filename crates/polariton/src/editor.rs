//! Thin Iced host for the headless libaryon engine.
//!
//! This deliberately owns no document state. The engine is authoritative; this
//! module only forwards portable input, coalesces projected viewports, and
//! renders the focused pane's projection: syntax, gutter, cursor, selection,
//! diagnostics, folds, engine-owned wrap rows and scrolling, and the minimap.

#[cfg(feature = "toron")]
use crate::gui::plot_pane::{self, HostPlotState, PlotPaneMsg};
use crate::gui::{
	dashboard::{self, DashboardMsg, DashboardState},
	table::{Table, TableScroll},
	table_skin::{TableSkin, TableSkinCascade, TableSkinSpec},
};
use crate::plot::common::{PlotKernel, PlotRenderLayer, PlotWidget, ResolvedPlotSettings};
use crate::plot::composition::PlotComposition;
#[cfg(feature = "toron")]
use crate::plot::spec::Presence;
#[cfg(feature = "toron")]
use crate::plot::theme::PlotSettingsLayer;
#[cfg(feature = "toron")]
use crate::rune_relation_envelope::{LocalViewIdentity, ViewContentKind, ViewPresentation};
use iced::keyboard::{self, Key};
use iced::mouse;
#[cfg(feature = "toron")]
use iced::widget::tooltip;
use iced::widget::{
	Space, button, canvas, column, container, mouse_area, rich_text, row, span, stack, text,
};
use iced::{Background, Color, Element, Event, Length, Subscription, Task, Theme, event, time};
use libaryon::core::{CursorPosition, DocDisplayCol, DocLine, PaneRect, Rgb, ViewId};
use libaryon::engine::{
	EditorCommand, EditorMode, EngineConfig, EngineHandle, InputEvent, KeyCode, LanguageId,
	Modifiers,
};
use libaryon::engine::{
	HostEvent, HostPanelId, HostPanelPlacement, MinimapSnapshot, PaneDropEdge, PanelRevision,
	SplitDirection, UiPalette, VisualKind, minimap_click_viewport_start_line,
};
#[cfg(feature = "toron")]
use libaryon::engine::{
	HostPanelGroupId, HostPanelGroupLayout, HostPanelGroupMember, HostPanelGroupShape,
};
use libaryon::svp::markdown_blocks::{
	ListMarker, MarkdownBlock, MarkdownInlineText, line_start_byte, parse_markdown_blocks,
};
use libaryon::svp::markdown_highlight::{MarkdownConstruct, markdown_construct_color};
use libaryon::svp::rust_highlight::{HighlightSpan, default_text_color};
use libaryon::uast::Viewport;
use libaryon::uast::projection::{
	MarkdownCellCodeVisibility, MarkdownCellIdentity, MarkdownCellPresentation,
};
use polars::frame::DataFrame;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, mpsc::Receiver};
use std::time::Duration;

/// The single query-results surface. One stable identity reused for every run,
/// so results replace in place instead of opening a pane per query.
const RESULT_PANEL: HostPanelId = HostPanelId::new(1);
const DASHBOARD_PANEL: HostPanelId = HostPanelId::new(2);
#[cfg(feature = "toron")]
const DYNAMIC_VIEW_PANEL_BASE: u64 = 1_u64 << 63;

const INITIAL_COLUMNS: u16 = 120;
const INITIAL_ROWS: u16 = 40;
/// A visible, low-friction workspace drag handle. It also establishes the
/// natural home for pane titles and close buttons; those can grow here without
/// leaking pane chrome back into Polariton's old outer grid.
const PANE_HEADER_HEIGHT: f32 = 22.0;
/// Keep the leading drag target comfortably clickable without painting a grip
/// glyph. The tab-strip surface already distinguishes workspace chrome from
/// document content.
const PANE_DRAG_HANDLE_WIDTH: f32 = 30.0;
const PANE_DRAG_DEADBAND: f32 = 8.0;
const DOUBLE_CLICK_INTERVAL: Duration = Duration::from_millis(500);
/// A plot needs more room than a tabular result, but remains a document block
/// rather than a workspace pane. Width stays responsive to the preview.
const NOTEBOOK_PLOT_HEIGHT: f32 = 360.0;
/// Proportional face for prose. Markdown preview is the one surface that is not
/// a grid of cells, so it does not use the editor's monospace font.
const HEADING_FONT: iced::Font = iced::Font {
	weight: iced::font::Weight::Bold,
	..iced::Font::DEFAULT
};
const MARKDOWN_BODY_SIZE: f32 = 17.0;
const MARKDOWN_BODY_LINE_HEIGHT: f32 = 24.0;
/// Horizontal step per list nesting level.
const MARKDOWN_LIST_INDENT: f32 = 20.0;

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
}

fn viewport_cells(size: iced::Size) -> (u16, u16) {
	let columns = (size.width / metrics::CELL_WIDTH)
		.floor()
		.clamp(1.0, u16::MAX as f32) as u16;
	let rows = (size.height / metrics::LINE_HEIGHT)
		.floor()
		.clamp(1.0, u16::MAX as f32) as u16;
	(columns, rows)
}

#[derive(Clone, Copy)]
pub enum PaneTabAction {
	NewPane(SplitDirection),
	NewTab,
}

#[derive(Clone)]
pub enum EditorMsg {
	EngineReady,
	CursorBlinkTick(time::Instant),
	BusySpinnerTick(time::Instant),
	WindowFocused(bool),
	ActivateDocTab {
		pane_id: libaryon::core::PaneId,
		view_id: ViewId,
	},
	CloseDocTab {
		pane_id: libaryon::core::PaneId,
		view_id: ViewId,
	},
	PaneTabAction {
		pane_id: libaryon::core::PaneId,
		action: PaneTabAction,
	},
	#[cfg(feature = "toron")]
	RetileOutputs {
		pane_id: libaryon::core::PaneId,
	},
	Dashboard(DashboardMsg),
	#[cfg(feature = "toron")]
	HostPlot {
		panel_id: HostPanelId,
		message: PlotPaneMsg,
	},
	Input(InputEvent),
	Paste(String),
	Scroll(i32),
	MinimapGoto {
		pane_id: libaryon::core::PaneId,
		line: DocLine,
	},
	UastGraphNode {
		inspector_view_id: ViewId,
		selection: libaryon::uast::UastGraphSelection,
	},
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

/// An authoritative document snapshot requested specifically for execution.
///
/// Unlike [`CodeEditor::cached_content`], this text was read by the engine
/// after every input event that preceded the user's Run command.
pub struct ExecutionSnapshot {
	pub buffer_id: libaryon::core::BufferId,
	pub text: String,
}

#[derive(Clone, Copy)]
enum TextQuery {
	Cache,
	Execution(u64),
}

const CURSOR_BLINK_INTERVAL: time::Duration = time::Duration::from_millis(500);
const CURSOR_BLINK_TICK: time::Duration = time::Duration::from_millis(125);
const BUSY_SPINNER_INTERVAL: time::Duration = time::Duration::from_millis(80);

struct CursorBlink {
	last_activity: time::Instant,
	now: time::Instant,
	window_focused: bool,
}

impl CursorBlink {
	fn new() -> Self {
		let now = time::Instant::now();
		Self {
			last_activity: now,
			now,
			window_focused: true,
		}
	}

	fn note_activity(&mut self, now: time::Instant) {
		self.last_activity = now;
		self.now = now;
	}

	fn advance(&mut self, now: time::Instant) {
		self.now = now;
	}

	fn set_window_focused(&mut self, focused: bool, now: time::Instant) {
		self.window_focused = focused;
		self.note_activity(now);
	}

	fn visible(&self) -> bool {
		if !self.window_focused {
			return true;
		}
		let elapsed = self
			.now
			.checked_duration_since(self.last_activity)
			.unwrap_or_default();
		(elapsed.as_millis() / CURSOR_BLINK_INTERVAL.as_millis()).is_multiple_of(2)
	}
}

struct BusySpinner {
	phase: usize,
	last_tick: time::Instant,
}

impl BusySpinner {
	fn new() -> Self {
		Self {
			phase: 0,
			last_tick: time::Instant::now(),
		}
	}

	fn restart(&mut self, now: time::Instant) {
		self.phase = 0;
		self.last_tick = now;
	}

	fn advance(&mut self, now: time::Instant) {
		let elapsed = now
			.checked_duration_since(self.last_tick)
			.unwrap_or_default();
		let steps = elapsed.as_millis() / BUSY_SPINNER_INTERVAL.as_millis();
		if steps > 0 {
			self.phase = self.phase.wrapping_add(steps as usize);
			self.last_tick = now;
		}
	}
}

/// Long-lived owner of Polariton's libaryon session.
pub struct CodeEditor {
	engine: Option<EngineHandle>,
	commands: libaryon::engine::CommandSender,
	input: libaryon::engine::InputSender,
	engine_outputs: Receiver<EngineOutput>,
	engine_wake: EngineWake,
	viewport: Option<Viewport>,
	/// Latest complete text snapshot for each buffer the host has observed.
	/// Keeping these attributed prevents a focus projection for one pane from
	/// being paired with a still-in-flight text reply from another.
	document_texts: HashMap<libaryon::core::BufferId, String>,
	/// Queries and input share one engine FIFO. Retaining the matching host
	/// intent lets a Run-specific reply bypass the eventually-consistent cache.
	text_queries: VecDeque<TextQuery>,
	next_execution_query: u64,
	execution_snapshots: HashMap<u64, ExecutionSnapshot>,
	cursor_blink: CursorBlink,
	busy_spinner: BusySpinner,
	/// Input was forwarded after the pending text query was issued, so the
	/// in-flight reply is already stale and another query must follow it.
	content_dirty: bool,
	last_pointer: Option<iced::Point>,
	/// Last file-tree or Git-status row pressed, used to reproduce Baryon's
	/// deliberate same-pane, same-row double-click contract.
	last_navigation_click: Option<(libaryon::core::PaneId, DocLine, time::Instant)>,
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
	/// Session-local realization of semantic view identities. Dynamic ids live
	/// in the upper half of the namespace; fixed application panels stay below.
	#[cfg(feature = "toron")]
	view_panel_ids: HashMap<(libaryon::core::BufferId, LocalViewIdentity), HostPanelId>,
	#[cfg(feature = "toron")]
	next_view_panel_id: u64,
	/// Placement identity is producer-scoped and intentionally separate from
	/// semantic view-panel identity.
	#[cfg(feature = "toron")]
	view_group_ids: HashMap<libaryon::core::BufferId, HostPanelGroupId>,
	#[cfg(feature = "toron")]
	next_view_group_id: u64,
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
	/// Parsed Markdown structure per previewed source buffer. `view` runs on
	/// every frame while the source text only changes with a projection, so
	/// parsing happens when a viewport arrives and the result is reused.
	preview_blocks: HashMap<libaryon::core::BufferId, CachedPreviewBlocks>,
}

struct CachedPreviewBlocks {
	source_digest: u64,
	blocks: Vec<MarkdownBlock>,
}

/// A runnable fenced SQL cell selected by the engine's cursor projection.
pub struct MarkdownCellExecution {
	pub identity: MarkdownCellIdentity,
	pub presentation: MarkdownCellPresentation,
	pub language_id: String,
	pub code: String,
	/// One-based source-document line where `code` begins.
	pub source_start_line: usize,
}

enum NotebookCellOutput {
	Running,
	Table(TableArtifact),
	Plot { surface: Box<NotebookPlotSurface> },
	Failed(String),
}

/// Native interaction state for a plot embedded in the Markdown preview.
/// It deliberately shares the existing plot kernel and settings model with
/// dashboard plots, but has its own identity and hover state.
struct NotebookPlotSurface {
	composition: PlotComposition,
	kernel: Arc<dyn PlotKernel + Send + Sync>,
	settings: ResolvedPlotSettings,
	hovered_info: Option<String>,
	render_revision: u64,
}

impl NotebookPlotSurface {
	fn new(composition: PlotComposition, kernel: Arc<dyn PlotKernel + Send + Sync>) -> Self {
		Self {
			composition,
			kernel,
			settings: ResolvedPlotSettings::default(),
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
	#[cfg(test)]
	pub fn new(text: &str) -> Self {
		Self::spawn(EngineConfig::default(), Some(text))
	}

	/// Start an embedded editor whose durable settings live alongside the
	/// Polariton application configuration.
	pub fn new_with_init(
		text: &str,
		init_script: Option<std::path::PathBuf>,
		viewport: iced::Size,
	) -> Self {
		Self::spawn(
			polariton_engine_config(None, init_script, Some(viewport_cells(viewport))),
			Some(text),
		)
	}

	/// Start directly on a real file. Unlike [`Self::new`], this leaves
	/// Libaryon's startup document intact so it retains path identity,
	/// language detection, reload, save, and recovery behavior.
	#[cfg(test)]
	pub fn open_initial_file(path: String) -> Self {
		Self::spawn(
			EngineConfig {
				initial_file: Some(path),
				..EngineConfig::default()
			},
			None,
		)
	}

	pub fn open_initial_file_with_init(
		path: String,
		init_script: Option<std::path::PathBuf>,
		viewport: iced::Size,
	) -> Self {
		Self::spawn(
			polariton_engine_config(Some(path), init_script, Some(viewport_cells(viewport))),
			None,
		)
	}

	fn spawn(mut config: EngineConfig, replacement: Option<&str>) -> Self {
		config
			.initial_viewport
			.get_or_insert((INITIAL_COLUMNS, INITIAL_ROWS));
		let mut engine = EngineHandle::spawn(config);
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
			document_texts: HashMap::new(),
			text_queries: VecDeque::new(),
			next_execution_query: 0,
			execution_snapshots: HashMap::new(),
			cursor_blink: CursorBlink::new(),
			busy_spinner: BusySpinner::new(),
			content_dirty: false,
			last_pointer: None,
			last_navigation_click: None,
			last_raw_window: None,
			pointer_offset: None,
			status_message: None,
			quit_requested: false,
			quit_notified: false,
			show_column_types: false,
			panels: PanelRegistry::default(),
			#[cfg(feature = "toron")]
			view_panel_ids: HashMap::new(),
			#[cfg(feature = "toron")]
			next_view_panel_id: DYNAMIC_VIEW_PANEL_BASE,
			#[cfg(feature = "toron")]
			view_group_ids: HashMap::new(),
			#[cfg(feature = "toron")]
			next_view_group_id: 1,
			result_revision: 0,
			drag: None,
			pane_drag: None,
			notebook_outputs: HashMap::new(),
			notebook_cell_data: HashMap::new(),
			preview_blocks: HashMap::new(),
		};
		if let Some(text) = replacement {
			editor.send(EditorCommand::ReplaceDocument {
				text: text.to_owned(),
				language: Some("sql".to_owned()),
				// File-path-shaped identity so libaryon's language services
				// (semantic highlighting, diagnostics) run for this unsaved
				// in-memory document.
				virtual_path: Some("statement.sql".to_owned()),
			});
		}
		editor.request_content();
		editor
	}

	/// Last complete document text received from the engine.
	pub fn cached_content(&self) -> &str {
		self.active_buffer_id()
			.and_then(|buffer_id| self.document_texts.get(&buffer_id))
			.map_or("", String::as_str)
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
		let source = self.cached_content().as_bytes();
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
			.document_texts
			.get(&source.buffer_id)?
			.get(cell.body_range.start.get() as usize..cell.body_range.end.get() as usize)?
			.to_owned();
		let source_start_line = self
			.document_texts
			.get(&source.buffer_id)?
			.get(..cell.body_range.start.get() as usize)?
			.bytes()
			.filter(|byte| *byte == b'\n')
			.count() + 1;
		Some(MarkdownCellExecution {
			identity: cell.identity.clone(),
			presentation: cell.presentation,
			language_id: cell.language_id.clone(),
			code,
			source_start_line,
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

	/// Retire a SQL cell's prior data product without replacing it with
	/// operational UI. A later table may claim the output surface; completion
	/// messages and diagnostics belong to the application status channel.
	pub fn clear_markdown_cell_artifact(&mut self, identity: &MarkdownCellIdentity) {
		self.notebook_cell_data.remove(identity);
		self.notebook_outputs.remove(identity);
	}

	pub fn set_markdown_cell_table(&mut self, identity: MarkdownCellIdentity, frame: DataFrame) {
		self.set_markdown_cell_table_with_skin(identity, frame, TableSkinCascade::library_floor());
	}

	fn set_markdown_cell_table_with_skin(
		&mut self,
		identity: MarkdownCellIdentity,
		frame: DataFrame,
		skin: TableSkin,
	) {
		self.retain_markdown_cell_table(identity.clone(), &frame);
		self.notebook_outputs.insert(
			identity,
			NotebookCellOutput::Table(TableArtifact::new(frame, skin)),
		);
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
				placement: HostPanelPlacement::Split {
					direction: SplitDirection::Horizontal,
				},
				focus: false,
			});
		}
		self.panels.set_content(RESULT_PANEL, PanelContent::Running);
	}

	/// Resolve one buffer revision's semantic view identities and establish its
	/// ordinary host panels in presentation order before any query completes.
	#[cfg(feature = "toron")]
	pub fn prepare_rune_view_panels<P>(
		&mut self,
		producer: libaryon::core::BufferId,
		views: &[(LocalViewIdentity, P)],
		revision: u64,
	) where
		P: Clone + Into<ViewPresentation>,
	{
		let active = views
			.iter()
			.map(|(identity, _)| identity.clone())
			.collect::<std::collections::HashSet<_>>();
		let orphaned = self
			.view_panel_ids
			.iter()
			.filter_map(|((buffer, identity), panel_id)| {
				(*buffer == producer && !active.contains(identity)).then_some(*panel_id)
			})
			.collect::<Vec<_>>();
		for panel_id in orphaned {
			self.panels.mark_orphaned(panel_id);
		}

		let resolved = views
			.iter()
			.map(|(identity, presentation)| {
				(
					identity,
					presentation.clone().into(),
					self.resolve_rune_view_panel(producer, identity.clone()),
				)
			})
			.collect::<Vec<_>>();
		let anchor = self.viewport.as_ref().and_then(|viewport| {
			viewport
				.panes
				.iter()
				.find(|pane| pane.buffer_id == producer)
				.map(|pane| pane.pane_id)
		});
		let group_id = self.resolve_rune_view_group(producer);
		let mut group_members = Vec::with_capacity(resolved.len());
		for (identity, presentation, panel_id) in resolved {
			let known = self.panels.panels.contains_key(&panel_id)
				&& !self.panels.pending_disposal.contains(&panel_id);
			if known {
				self.send(EditorCommand::RefreshHostPanel {
					panel_id,
					revision: PanelRevision::new(revision),
				});
			}
			let title = match identity {
				LocalViewIdentity::Named(name) => name.clone(),
				LocalViewIdentity::Anonymous(position) => {
					let label = match &presentation.authored_title {
						Presence::Show(title) => title.as_str(),
						Presence::Inherit | Presence::Hide => &presentation.display_label,
					};
					format!("{label} {}", position + 1)
				}
			};
			group_members.push(HostPanelGroupMember {
				panel_id,
				kind: match presentation.content_kind {
					ViewContentKind::Table => "rune-view-table",
					ViewContentKind::Plot => "rune-view-plot",
				}
				.to_owned(),
				title,
				shape: match presentation.content_kind {
					ViewContentKind::Table => HostPanelGroupShape::Compact,
					ViewContentKind::Plot => HostPanelGroupShape::Expansive,
				},
			});
			self.panels.mark_running(panel_id);
		}
		self.send(EditorCommand::EnsureHostPanelGroup {
			group_id,
			anchor,
			members: group_members,
			layout: HostPanelGroupLayout::RowMajorTile,
		});
	}

	#[cfg(feature = "toron")]
	fn resolve_rune_view_group(&mut self, producer: libaryon::core::BufferId) -> HostPanelGroupId {
		if let Some(group_id) = self.view_group_ids.get(&producer) {
			return *group_id;
		}
		let group_id = HostPanelGroupId::new(self.next_view_group_id);
		self.next_view_group_id = self
			.next_view_group_id
			.checked_add(1)
			.expect("HostPanelGroupId namespace exhausted");
		self.view_group_ids.insert(producer, group_id);
		group_id
	}

	#[cfg(feature = "toron")]
	fn rune_view_group_for_pane(
		&self,
		pane_id: libaryon::core::PaneId,
	) -> Option<HostPanelGroupId> {
		let pane = self
			.viewport
			.as_ref()?
			.panes
			.iter()
			.find(|pane| pane.pane_id == pane_id)?;
		if let Some(group_id) = self.view_group_ids.get(&pane.buffer_id) {
			return Some(*group_id);
		}
		let panel_id = pane.host_panel.as_ref()?.panel_id;
		let producer = self
			.view_panel_ids
			.iter()
			.find_map(|((producer, _), candidate)| (*candidate == panel_id).then_some(*producer))?;
		self.view_group_ids.get(&producer).copied()
	}

	#[cfg(feature = "toron")]
	fn resolve_rune_view_panel(
		&mut self,
		producer: libaryon::core::BufferId,
		identity: LocalViewIdentity,
	) -> HostPanelId {
		let key = (producer, identity);
		if let Some(panel_id) = self.view_panel_ids.get(&key) {
			return *panel_id;
		}
		let value = self.next_view_panel_id;
		self.next_view_panel_id = self
			.next_view_panel_id
			.checked_add(1)
			.expect("dynamic HostPanelId namespace exhausted");
		let panel_id = HostPanelId::new(value);
		self.view_panel_ids.insert(key, panel_id);
		panel_id
	}

	#[cfg(feature = "toron")]
	pub fn rune_view_panel_id(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<HostPanelId> {
		self.view_panel_ids
			.get(&(producer, identity.clone()))
			.copied()
	}

	#[cfg(all(test, feature = "toron"))]
	pub fn set_rune_view_table(&mut self, panel_id: HostPanelId, frame: DataFrame) {
		self.set_rune_view_table_with_skin(panel_id, frame, TableSkinCascade::library_floor());
	}

	#[cfg(feature = "toron")]
	pub fn set_rune_view_table_with_skin(
		&mut self,
		panel_id: HostPanelId,
		frame: DataFrame,
		skin: TableSkin,
	) {
		self.panels.set_content(
			panel_id,
			PanelContent::Table(TableArtifact::new(frame, skin)),
		);
	}

	#[cfg(feature = "toron")]
	pub fn set_rune_view_plot(
		&mut self,
		panel_id: HostPanelId,
		display_label: String,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		authored_settings: PlotSettingsLayer,
	) {
		self.panels
			.set_plot(panel_id, display_label, kernel, authored_settings);
	}

	#[cfg(feature = "toron")]
	pub fn fail_rune_view(&mut self, panel_id: HostPanelId) {
		self.panels.fail_or_stale(panel_id);
	}

	#[cfg(feature = "toron")]
	pub fn mark_rune_view_producer_stale(&mut self, producer: libaryon::core::BufferId) {
		let panels = self
			.view_panel_ids
			.iter()
			.filter_map(|((buffer, _), panel_id)| (*buffer == producer).then_some(*panel_id))
			.collect::<Vec<_>>();
		for panel_id in panels {
			self.panels.fail_or_stale(panel_id);
		}
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_is_table(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> bool {
		self.rune_view_panel_id(producer, identity)
			.and_then(|panel_id| self.panels.panels.get(&panel_id))
			.is_some_and(|entry| matches!(entry.content, PanelContent::Table(_)))
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_is_plot(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> bool {
		self.rune_view_panel_id(producer, identity)
			.and_then(|panel_id| self.panels.panels.get(&panel_id))
			.is_some_and(|entry| matches!(entry.content, PanelContent::Plot(_)))
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_display_label(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<&str> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let entry = self.panels.panels.get(&panel_id)?;
		let PanelContent::Plot(plot) = &entry.content else {
			return None;
		};
		Some(plot.display_label.as_str())
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_projected_title(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<&str> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		self.viewport
			.as_ref()?
			.panes
			.iter()
			.filter_map(|pane| pane.host_panel.as_ref())
			.find(|panel| panel.panel_id == panel_id)
			.map(|panel| panel.title.as_str())
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_plot_svg(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<String> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let entry = self.panels.panels.get(&panel_id)?;
		let PanelContent::Plot(plot) = &entry.content else {
			return None;
		};
		let widget = PlotWidget {
			kernel: plot.kernel.as_ref(),
			title: plot.display_label.clone(),
			padding: 0.0,
			settings: plot.plot_settings.clone(),
			settings_model: plot.plot_settings_model.clone(),
			render_revision: plot.render_revision,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = crate::plot::export::SvgBackend::new(800.0, 480.0);
		widget.render(
			&mut backend,
			iced::Rectangle::new([0.0, 0.0].into(), [800.0, 480.0].into()),
		);
		Some(backend.finish())
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_plot_hover(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
		bounds: iced::Rectangle,
		point: iced::Point,
	) -> Option<String> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let entry = self.panels.panels.get(&panel_id)?;
		let PanelContent::Plot(plot) = &entry.content else {
			return None;
		};
		let layout = plot.kernel.layout(plot.plot_settings.clone());
		let transform = crate::plot::common::CoordinateTransformer::new(&layout, bounds);
		plot.kernel.hover_with_settings(
			&transform,
			iced::advanced::mouse::Cursor::Available(point),
			&plot.plot_settings_model,
		)
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_layout_signature(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<(
		HostPanelGroupId,
		libaryon::core::PaneId,
		PaneRect,
		Vec<ViewId>,
		usize,
		libaryon::core::PaneId,
	)> {
		let group_id = self.view_group_ids.get(&producer).copied()?;
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let viewport = self.viewport.as_ref()?;
		let pane = viewport.panes.iter().find(|pane| {
			pane.host_panel
				.as_ref()
				.is_some_and(|panel| panel.panel_id == panel_id)
		})?;
		Some((
			group_id,
			pane.pane_id,
			pane.rect,
			pane.tabs.iter().map(|tab| tab.view_id).collect(),
			pane.active_doctab_index,
			viewport.focused_pane_id,
		))
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_count(&self, producer: libaryon::core::BufferId) -> usize {
		self.view_panel_ids
			.keys()
			.filter(|(buffer, _)| *buffer == producer)
			.count()
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_is_failed(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> bool {
		self.rune_view_panel_id(producer, identity)
			.and_then(|panel_id| self.panels.panels.get(&panel_id))
			.is_some_and(|entry| matches!(entry.content, PanelContent::Failed))
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_status(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<&'static str> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let entry = self.panels.panels.get(&panel_id)?;
		Some(match entry.status {
			PanelStatus::Fresh => "fresh",
			PanelStatus::Running => "running",
			PanelStatus::Stale => "stale",
			PanelStatus::Orphaned => "orphaned",
		})
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_table(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<&DataFrame> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let entry = self.panels.panels.get(&panel_id)?;
		match &entry.content {
			PanelContent::Table(artifact) => Some(artifact.frame.as_ref()),
			_ => None,
		}
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_table_skin(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<TableSkin> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let entry = self.panels.panels.get(&panel_id)?;
		match &entry.content {
			PanelContent::Table(artifact) => Some(artifact.skin),
			_ => None,
		}
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn rune_view_panel_table_draw_skin(
		&self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) -> Option<TableSkin> {
		let panel_id = self.rune_view_panel_id(producer, identity)?;
		let entry = self.panels.panels.get(&panel_id)?;
		match &entry.content {
			PanelContent::Table(artifact) => {
				Some(self.panels.table_skin_at_draw(panel_id, artifact.skin))
			}
			_ => None,
		}
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn set_test_rune_view_pane_table_skin(
		&mut self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
		skin: TableSkinSpec,
	) {
		let panel_id = self
			.rune_view_panel_id(producer, identity)
			.expect("prepared Rune table panel");
		self.panels.set_test_pane_table_skin(panel_id, skin);
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn clear_test_rune_view_pane_table_skin(
		&mut self,
		producer: libaryon::core::BufferId,
		identity: &LocalViewIdentity,
	) {
		let panel_id = self
			.rune_view_panel_id(producer, identity)
			.expect("prepared Rune table panel");
		self.panels.clear_test_pane_table_skin(panel_id);
	}

	#[cfg(test)]
	pub(crate) fn has_query_surface(&self) -> bool {
		self.panels.panels.contains_key(&RESULT_PANEL)
	}

	/// Open (or focus) Polariton's nested dashboard inside the Baryon workspace.
	pub fn open_dashboard(&mut self) {
		self.send(EditorCommand::OpenHostPanel {
			panel_id: DASHBOARD_PANEL,
			kind: "dashboard".to_owned(),
			title: "Dashboard".to_owned(),
			placement: HostPanelPlacement::Split {
				direction: SplitDirection::Vertical,
			},
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
		self.set_query_result_with_skin(frame, TableSkinCascade::library_floor());
	}

	fn set_query_result_with_skin(&mut self, frame: DataFrame, skin: TableSkin) {
		self.panels.set_content(
			RESULT_PANEL,
			PanelContent::Table(TableArtifact::new(frame, skin)),
		);
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

	/// Animation phase while libaryon is performing foreground I/O or semantic
	/// analysis. `None` keeps the application status bar completely idle.
	pub fn busy_spinner_phase(&self) -> Option<usize> {
		self.viewport
			.as_ref()
			.is_some_and(|viewport| viewport.busy)
			.then_some(self.busy_spinner.phase)
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
		if !self.text_queries.is_empty() {
			self.content_dirty = true;
		} else if self.send(EditorCommand::QueryDocumentText) {
			self.text_queries.push_back(TextQuery::Cache);
			self.content_dirty = false;
		} else {
			// The query never left; retry from the next request or reply.
			self.content_dirty = true;
		}
	}

	/// Queue an execution barrier after all input already forwarded to the
	/// engine. This deliberately does not coalesce with ordinary cache queries:
	/// its own reply is the proof that Run has crossed the last edit.
	pub fn request_execution_snapshot(&mut self) -> Option<u64> {
		self.active_buffer_id()?;
		self.next_execution_query = self.next_execution_query.saturating_add(1);
		let request = self.next_execution_query;
		if !self.send(EditorCommand::QueryDocumentText) {
			return None;
		}
		self.text_queries.push_back(TextQuery::Execution(request));
		Some(request)
	}

	/// Take a completed execution snapshot. Until this returns `Some`, Run must
	/// remain queued rather than falling back to [`Self::cached_content`].
	pub fn take_execution_snapshot(&mut self, request: u64) -> Option<ExecutionSnapshot> {
		self.execution_snapshots.remove(&request)
	}

	#[cfg(test)]
	pub fn replace_content(&mut self, text: &str) {
		// Optimistic: the engine confirms via the queued query, but Run/Save
		// must never observe a cleared document in the interim.
		if let Some(buffer_id) = self.active_buffer_id() {
			self.document_texts.insert(buffer_id, text.to_owned());
		}
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
		let (columns, rows) = viewport_cells(iced::Size::new(width, height));
		self.send(EditorCommand::Resize(columns, rows));
	}

	pub fn subscription(&self) -> Subscription<EditorMsg> {
		let blink = if self.cursor_blink.window_focused
			&& self.viewport.as_ref().is_some_and(|viewport| {
				prompt_has_cursor(&viewport.input.mode)
					|| viewport.focused_pane().is_some_and(|pane| {
						pane.host_panel.is_none() && pane.markdown_preview.is_none()
					})
			}) {
			time::every(CURSOR_BLINK_TICK).map(EditorMsg::CursorBlinkTick)
		} else {
			Subscription::none()
		};
		let busy_spinner = if self.busy_spinner_phase().is_some() {
			time::every(BUSY_SPINNER_INTERVAL).map(EditorMsg::BusySpinnerTick)
		} else {
			Subscription::none()
		};
		Subscription::batch([
			event::listen_with(|event, status, _window| editor_input_for(&event, status)),
			Subscription::run_with(self.engine_wake.clone(), engine_wake_stream)
				.map(|()| EditorMsg::EngineReady),
			blink,
			busy_spinner,
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
			let was_busy = self.viewport.as_ref().is_some_and(|viewport| viewport.busy);
			if viewport.busy && !was_busy {
				self.busy_spinner.restart(time::Instant::now());
			}
			self.quit_requested |= viewport.should_quit;
			if let Some(message) = viewport.status_message.as_ref() {
				self.status_message = Some(message.clone());
			}
			if let (Some(document_text), Some(buffer_id)) = (
				viewport.document_text.as_ref(),
				viewport.document_text_buffer_id,
			) {
				self.document_texts.insert(buffer_id, document_text.clone());
				if let Some(TextQuery::Execution(request)) = self.text_queries.pop_front() {
					self.execution_snapshots.insert(
						request,
						ExecutionSnapshot {
							buffer_id,
							text: document_text.clone(),
						},
					);
				}
				if self.text_queries.is_empty() && self.content_dirty {
					// Input landed after this query was issued; chase
					// it so the cache converges on the latest state.
					self.request_content();
				}
			}
			let live_buffers = viewport
				.panes
				.iter()
				.flat_map(|pane| pane.tabs.iter().map(|tab| tab.buffer_id))
				.collect::<std::collections::HashSet<_>>();
			self.document_texts
				.retain(|buffer_id, _| live_buffers.contains(buffer_id));
			self.reconcile_notebook_outputs(&viewport);
			self.reconcile_preview_blocks(&viewport);
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

	/// Reparse each previewed document whose text actually changed, and forget
	/// the previews that closed.
	fn reconcile_preview_blocks(&mut self, viewport: &Viewport) {
		let previews: Vec<&libaryon::uast::projection::MarkdownPreviewSnapshot> = viewport
			.panes
			.iter()
			.filter_map(|pane| pane.markdown_preview.as_ref())
			.collect();
		let live: std::collections::HashSet<libaryon::core::BufferId> = previews
			.iter()
			.map(|preview| preview.source_buffer_id)
			.collect();
		self.preview_blocks
			.retain(|buffer_id, _| live.contains(buffer_id));
		for preview in previews {
			let digest = {
				let mut hasher = std::collections::hash_map::DefaultHasher::new();
				preview.source_text.hash(&mut hasher);
				hasher.finish()
			};
			let entry = self.preview_blocks.get(&preview.source_buffer_id);
			if entry.is_some_and(|cached| cached.source_digest == digest) {
				continue;
			}
			self.preview_blocks.insert(
				preview.source_buffer_id,
				CachedPreviewBlocks {
					source_digest: digest,
					blocks: parse_markdown_blocks(&preview.source_text),
				},
			);
		}
	}

	pub fn update(&mut self, message: EditorMsg) -> Task<EditorMsg> {
		if self.quit_requested {
			return Task::none();
		}
		match message {
			EditorMsg::EngineReady => {
				self.poll();
			}
			EditorMsg::CursorBlinkTick(now) => {
				self.cursor_blink.advance(now);
			}
			EditorMsg::BusySpinnerTick(now) => {
				self.busy_spinner.advance(now);
			}
			EditorMsg::WindowFocused(focused) => {
				self.cursor_blink
					.set_window_focused(focused, time::Instant::now());
			}
			EditorMsg::ActivateDocTab { pane_id, view_id } => {
				self.cursor_blink.note_activity(time::Instant::now());
				self.send(EditorCommand::ActivateDocTab { pane_id, view_id });
				self.request_content();
			}
			EditorMsg::CloseDocTab { pane_id, view_id } => {
				self.cursor_blink.note_activity(time::Instant::now());
				self.send(EditorCommand::CloseDocTab { pane_id, view_id });
				self.request_content();
			}
			EditorMsg::PaneTabAction { pane_id, action } => {
				self.cursor_blink.note_activity(time::Instant::now());
				self.send(EditorCommand::FocusPane(pane_id));
				self.send(match action {
					PaneTabAction::NewPane(direction) => EditorCommand::NewPane {
						direction,
						language: None,
					},
					PaneTabAction::NewTab => EditorCommand::DocTabNew,
				});
				self.request_content();
			}
			#[cfg(feature = "toron")]
			EditorMsg::RetileOutputs { pane_id } => {
				if let Some(group_id) = self.rune_view_group_for_pane(pane_id) {
					self.send(EditorCommand::ResetHostPanelGroup { group_id });
					self.request_content();
				}
			}
			EditorMsg::Dashboard(_) => {}
			#[cfg(feature = "toron")]
			EditorMsg::HostPlot { panel_id, message } => {
				self.panels.update_plot(panel_id, message);
			}
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
				self.cursor_blink.note_activity(time::Instant::now());
				return iced::clipboard::read()
					.map(|text| EditorMsg::Paste(text.unwrap_or_default()));
			}
			EditorMsg::Input(input) => {
				self.cursor_blink.note_activity(time::Instant::now());
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
				self.cursor_blink.note_activity(time::Instant::now());
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
			EditorMsg::MinimapGoto { pane_id, line } => {
				self.send(EditorCommand::FocusPane(pane_id));
				self.send(EditorCommand::ScrollViewportToLine(line));
			}
			EditorMsg::UastGraphNode {
				inspector_view_id,
				selection,
			} => {
				self.send(EditorCommand::ActivateUastGraphNode {
					inspector_view_id,
					selection,
				});
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
				self.cursor_blink.note_activity(time::Instant::now());
				// A pane header may publish its more specific press before the
				// workspace-wide mouse area publishes this generic click. Do not let
				// the divider hit band steal a drag the header already owns.
				if self.pane_drag.is_none() && !self.begin_divider_drag() {
					self.click_at_pointer();
					self.begin_pane_drag();
				}
			}
			EditorMsg::PaneDragStarted(pane_id) => {
				self.cursor_blink.note_activity(time::Instant::now());
				self.drag = None;
				self.send(EditorCommand::FocusPane(pane_id));
				self.request_content();
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
			let content = if let Some(graph) = pane.uast_graph.as_ref() {
				uast_graph_surface(graph, pane.view_id, palette)
			} else if let Some(preview) = pane.markdown_preview.as_ref() {
				markdown_preview_surface(
					preview,
					self.preview_blocks
						.get(&preview.source_buffer_id)
						.map_or(&[][..], |cached| cached.blocks.as_slice()),
					palette,
					&self.notebook_outputs,
					self.show_column_types,
				)
			} else {
				match pane.host_panel.as_ref() {
					Some(panel) => {
						self.panels
							.surface(panel, palette, self.show_column_types, dashboard)
					}
					None => {
						text_pane_surface(pane, palette, focused && self.cursor_blink.visible())
					}
				}
			};
			#[cfg(feature = "toron")]
			let retile_outputs = self.rune_view_group_for_pane(pane.pane_id).is_some();
			#[cfg(not(feature = "toron"))]
			let retile_outputs = false;
			let content = pane_surface(pane, focused, palette, content, retile_outputs);
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
				container(command_bar(
					prompt,
					prompt_cursor_byte(viewport),
					self.cursor_blink.visible(),
					palette,
				))
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

	/// Relinquish the engine when the owning GUI process is already exiting.
	///
	/// Joining here can make language-worker teardown part of the user's
	/// perceived close latency. Baryon's frontend deliberately uses the same
	/// process-owned lifetime: the engine has either accepted `:q`, or process
	/// termination is about to reclaim it. Ordinary `CodeEditor` drops still
	/// use [`Self::shutdown`] so tests and non-process owners remain tidy.
	pub fn detach_for_process_exit(&mut self) {
		if let Some(engine) = self.engine.take() {
			engine.detach();
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

	fn click_at_pointer(&mut self) {
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
			self.request_content();
			return;
		}
		let local_y = pointer.y - pane_top - PANE_HEADER_HEIGHT;
		let local_x = pointer.x - pane.rect.screen_col.get() as f32 * metrics::CELL_WIDTH;
		let row = (local_y.max(0.0) / metrics::LINE_HEIGHT).floor() as usize;
		// `visible_rows` is `render_rows` minus the wrapped rows scrolled off
		// above the viewport, matching what `view` actually laid out.
		let Some(visible_row) = pane.visible_rows.get(row) else {
			return;
		};
		let column =
			((local_x - pane_gutter_width(pane)).max(0.0) / metrics::CELL_WIDTH).floor() as u32;
		let target = CursorPosition::new(
			visible_row.line,
			DocDisplayCol::new(visible_row.start_col.get().saturating_add(column)),
		);
		if pane.is_file_browser || pane.is_git_status_panel {
			let now = time::Instant::now();
			let double_click = self.last_navigation_click.is_some_and(
				|(previous_pane, previous_line, previous_at)| {
					previous_pane == pane.pane_id
						&& previous_line == visible_row.line
						&& now.saturating_duration_since(previous_at) <= DOUBLE_CLICK_INTERVAL
				},
			);
			if !double_click {
				self.last_navigation_click = Some((pane.pane_id, visible_row.line, now));
			}
			if pane.is_file_browser {
				self.send(EditorCommand::ClickBrowserEntry {
					line: visible_row.line,
					double_click,
				});
			} else if double_click {
				self.send(EditorCommand::OpenGitStatusEntry(visible_row.line));
			} else {
				self.send(EditorCommand::ClickCursor(target));
			}
			self.request_content();
			return;
		}
		self.send(EditorCommand::ClickCursor(target));
		self.request_content();
	}

	fn is_insert_mode(&self) -> bool {
		self.viewport
			.as_ref()
			.is_some_and(|viewport| viewport.input.mode == EditorMode::Insert)
	}
}

fn polariton_engine_config(
	initial_file: Option<String>,
	init_script: Option<std::path::PathBuf>,
	initial_viewport: Option<(u16, u16)>,
) -> EngineConfig {
	EngineConfig {
		initial_file,
		initial_viewport,
		init_script,
		init_entrypoint: "editor".to_string(),
		..EngineConfig::default()
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

/// Match Baryon's deliberately theme-independent whitespace ink. Whitespace
/// must remain visible even when a syntax theme maps its whitespace token to
/// the editor background.
const VISIBLE_WHITESPACE_COLOR: Rgb = Rgb::new(76, 86, 106);

fn highlight_span_at(
	spans: &[HighlightSpan],
	byte: libaryon::core::DocByte,
) -> Option<HighlightSpan> {
	let before = spans.partition_point(|span| span.start <= byte);
	spans[..before]
		.iter()
		.rev()
		.find(|span| byte < span.end)
		.copied()
}

fn project_lines(
	pane: &libaryon::uast::projection::PaneViewport,
	default_foreground: Rgb,
	cursor_color: Rgb,
	show_cursor: bool,
) -> Vec<ProjectedLine> {
	use libaryon::core::TAB_SIZE;

	let mut lines = Vec::new();
	let mut current: Option<ProjectedLine> = None;
	let mut cursor_rendered = false;
	let mut display_column = 0u32;
	let mut line_has_content = false;
	let fallback_first_line = pane.viewport_start_line.get();
	let fallback_last_line = fallback_first_line
		.saturating_add(u64::from(pane.viewport_line_count.max(1)))
		.saturating_sub(1);
	let last_visible_line = pane
		.visible_rows
		.iter()
		.map(|row| row.line.get())
		.max()
		.unwrap_or(fallback_last_line);
	let line_is_visible = |line: u64| {
		if pane.visible_rows.is_empty() {
			(fallback_first_line..=fallback_last_line).contains(&line)
		} else {
			pane.visible_rows.iter().any(|row| row.line.get() == line)
		}
	};
	'tokens: for token in &pane.tokens {
		if token.text.is_empty() {
			continue;
		}
		let token_line = token.absolute_start_line.get();
		if token_line > last_visible_line {
			break;
		}
		if current
			.as_ref()
			.is_none_or(|line| line.doc_line != token_line)
		{
			if let Some(line) = current.take() {
				lines.push(line);
			}
			current = line_is_visible(token_line).then_some(ProjectedLine {
				doc_line: token_line,
				fragments: Vec::new(),
				show_gutter: true,
			});
		}
		let mut byte = token.absolute_start_byte.get();
		let mut line_number = token_line;
		let mut line_visible = line_is_visible(line_number);
		let token_text = String::from_utf8_lossy(&token.text);
		for cell in libaryon::core::text::display_cells(token_text.as_bytes()) {
			let grapheme = &token_text[cell.byte_start..cell.byte_end];
			let byte_len = (cell.byte_end - cell.byte_start) as u64;
			if line_number > last_visible_line {
				break 'tokens;
			}
			if !line_visible {
				if grapheme == "\n" {
					line_number = line_number.saturating_add(1);
					line_visible = line_is_visible(line_number);
					current = line_visible.then_some(ProjectedLine {
						doc_line: line_number,
						fragments: Vec::new(),
						show_gutter: true,
					});
					display_column = 0;
					line_has_content = false;
				}
				byte = byte.saturating_add(byte_len);
				continue;
			}
			let foreground =
				highlight_span_at(&pane.highlights, libaryon::core::DocByte::new(byte))
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
			let content_background = if selected {
				Some(Color::from_rgb8(55, 78, 104))
			} else if token.is_folded {
				Some(Color {
					a: 0.22,
					..color(cursor_color)
				})
			} else {
				None
			};
			let background = if cursor {
				Some(color(cursor_color))
			} else {
				content_background
			};
			if grapheme == "\n" {
				if pane.visible_whitespace {
					push_fragment(
						current.as_mut().expect("line initialized"),
						"¬".to_owned(),
						color(VISIBLE_WHITESPACE_COLOR),
						background,
						underline,
					);
				}
				if let Some(line) = current.take() {
					lines.push(line);
				}
				line_number = line_number.saturating_add(1);
				line_visible = line_is_visible(line_number);
				current = line_visible.then_some(ProjectedLine {
					doc_line: line_number,
					fragments: Vec::new(),
					show_gutter: true,
				});
				byte = byte.saturating_add(1);
				display_column = 0;
				line_has_content = false;
				continue;
			}

			let trailing = line_has_content
				&& token_text[cell.byte_end..]
					.split_once('\n')
					.is_some_and(|(rest, _)| {
						rest.chars()
							.all(|character| matches!(character, ' ' | '\t'))
					});
			let (text, foreground) = match grapheme {
				"\t" => {
					let width = TAB_SIZE - (display_column % TAB_SIZE);
					let head = if pane.visible_whitespace {
						if trailing { '~' } else { '▸' }
					} else {
						' '
					};
					let mut text = String::with_capacity(width as usize);
					text.push(head);
					text.extend(std::iter::repeat_n(' ', width.saturating_sub(1) as usize));
					(
						text,
						if pane.visible_whitespace {
							VISIBLE_WHITESPACE_COLOR
						} else {
							foreground
						},
					)
				}
				" " if pane.visible_whitespace => (
					if trailing { "~" } else { "␣" }.to_owned(),
					VISIBLE_WHITESPACE_COLOR,
				),
				"\u{00a0}" if pane.visible_whitespace => ("⍽".to_owned(), VISIBLE_WHITESPACE_COLOR),
				text if pane.visible_whitespace && text.chars().all(char::is_whitespace) => {
					("␣".to_owned(), VISIBLE_WHITESPACE_COLOR)
				}
				_ => (grapheme.to_owned(), foreground),
			};
			if !grapheme.chars().all(char::is_whitespace) {
				line_has_content = true;
			}
			let line = current.as_mut().expect("line initialized");
			if cursor && grapheme == "\t" {
				let first_end = text
					.char_indices()
					.nth(1)
					.map_or(text.len(), |(offset, _)| offset);
				push_fragment(
					line,
					text[..first_end].to_owned(),
					color(foreground),
					background,
					underline,
				);
				if first_end < text.len() {
					push_fragment(
						line,
						text[first_end..].to_owned(),
						color(foreground),
						content_background,
						underline,
					);
				}
			} else {
				push_fragment(line, text, color(foreground), background, underline);
			}
			display_column =
				libaryon::core::text::advance_display_col(display_column, cell, TAB_SIZE);
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
		for cell in libaryon::core::text::display_cells(fragment.text.as_bytes()) {
			let grapheme = &fragment.text[cell.byte_start..cell.byte_end];
			let index = starts.partition_point(|start| *start <= column);
			if let Some(index) = index.checked_sub(1) {
				push_fragment(
					&mut wrapped[index],
					grapheme.to_owned(),
					fragment.foreground,
					fragment.background,
					fragment.underline,
				);
			}
			// Columns before the first visible row are scrolled off above the
			// viewport; skip them without emitting anything.
			column =
				libaryon::core::text::advance_display_col(column, cell, libaryon::core::TAB_SIZE);
		}
	}
	wrapped
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

fn pane_gutter_width(pane: &libaryon::uast::projection::PaneViewport) -> f32 {
	if pane.gutter_numbering.is_some() {
		metrics::GUTTER_WIDTH
	} else {
		0.0
	}
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
	/// PRODUCT-0045's draw-time tier. No production surface writes it yet;
	/// retaining the intent here makes a future pane producer a local change.
	pane_table_skins: std::collections::HashMap<HostPanelId, TableSkinSpec>,
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
	status: PanelStatus,
	/// Latest keyboard-driven scroll, with a sequence the widget uses to apply
	/// it exactly once rather than on every frame.
	scroll: Option<(u64, TableScroll)>,
	/// User-resized column widths, surviving the widget-tree state that dies
	/// whenever panel content swaps variants (every rerun passes `Running`).
	col_widths: std::cell::RefCell<Option<Vec<f32>>>,
}

struct TableArtifact {
	frame: Box<DataFrame>,
	skin: TableSkin,
}

impl TableArtifact {
	fn new(frame: DataFrame, skin: TableSkin) -> Self {
		Self {
			frame: Box::new(frame),
			skin,
		}
	}
}

/// Host-owned backing state for a panel. None of this is representable in
/// libaryon: the engine only knows the panel exists.
enum PanelContent {
	/// Projected or restored, but the frontend has no content for it — either
	/// a panel from a persisted layout or one seen before its object exists.
	Unresolved,
	Running,
	Table(TableArtifact),
	#[cfg(feature = "toron")]
	Plot(Box<HostPlotState>),
	Message(String),
	#[cfg(feature = "toron")]
	Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum PanelStatus {
	#[default]
	Fresh,
	#[cfg(feature = "toron")]
	Running,
	#[cfg(feature = "toron")]
	Stale,
	#[cfg(feature = "toron")]
	Orphaned,
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
							status: PanelStatus::Fresh,
							scroll: None,
							col_widths: std::cell::RefCell::new(None),
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
							status: PanelStatus::Fresh,
							scroll: None,
							col_widths: std::cell::RefCell::new(None),
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
			self.pane_table_skins.remove(panel_id);
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
					status: PanelStatus::Fresh,
					scroll: None,
					col_widths: std::cell::RefCell::new(None),
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
			status: PanelStatus::Fresh,
			scroll: None,
			col_widths: std::cell::RefCell::new(None),
		});
		entry.content = content;
		entry.status = PanelStatus::Fresh;
	}

	fn table_skin_at_draw(&self, panel_id: HostPanelId, baked: TableSkin) -> TableSkin {
		self.pane_table_skins
			.get(&panel_id)
			.map(|spec| {
				spec.resolve()
					.expect("pane table-skin intents are validated by their producer")
			})
			.unwrap_or(baked)
	}

	#[cfg(test)]
	fn set_test_pane_table_skin(&mut self, panel_id: HostPanelId, skin: TableSkinSpec) {
		self.pane_table_skins.insert(panel_id, skin);
	}

	#[cfg(test)]
	fn clear_test_pane_table_skin(&mut self, panel_id: HostPanelId) {
		self.pane_table_skins.remove(&panel_id);
	}

	#[cfg(feature = "toron")]
	fn mark_running(&mut self, panel_id: HostPanelId) {
		let entry = self.panels.entry(panel_id).or_insert(PanelEntry {
			revision: PanelRevision::new(0),
			content: PanelContent::Unresolved,
			status: PanelStatus::Fresh,
			scroll: None,
			col_widths: std::cell::RefCell::new(None),
		});
		if matches!(
			entry.content,
			PanelContent::Unresolved | PanelContent::Running | PanelContent::Failed
		) {
			entry.content = PanelContent::Running;
		}
		entry.status = PanelStatus::Running;
	}

	#[cfg(feature = "toron")]
	fn fail_or_stale(&mut self, panel_id: HostPanelId) {
		let entry = self.panels.entry(panel_id).or_insert(PanelEntry {
			revision: PanelRevision::new(0),
			content: PanelContent::Unresolved,
			status: PanelStatus::Fresh,
			scroll: None,
			col_widths: std::cell::RefCell::new(None),
		});
		if matches!(
			entry.content,
			PanelContent::Table(_) | PanelContent::Plot(_) | PanelContent::Message(_)
		) {
			entry.status = PanelStatus::Stale;
		} else {
			entry.content = PanelContent::Failed;
			entry.status = PanelStatus::Fresh;
		}
	}

	#[cfg(feature = "toron")]
	fn mark_orphaned(&mut self, panel_id: HostPanelId) {
		if let Some(entry) = self.panels.get_mut(&panel_id) {
			entry.status = PanelStatus::Orphaned;
		}
	}

	#[cfg(feature = "toron")]
	fn update_plot(&mut self, panel_id: HostPanelId, message: PlotPaneMsg) {
		if let Some(PanelEntry {
			content: PanelContent::Plot(plot),
			..
		}) = self.panels.get_mut(&panel_id)
		{
			plot.update(message);
		}
	}

	#[cfg(feature = "toron")]
	fn set_plot(
		&mut self,
		panel_id: HostPanelId,
		display_label: String,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		authored_settings: PlotSettingsLayer,
	) {
		if let Some(PanelEntry {
			content: PanelContent::Plot(plot),
			status,
			..
		}) = self.panels.get_mut(&panel_id)
		{
			plot.replace(display_label, kernel, authored_settings);
			*status = PanelStatus::Fresh;
			return;
		}
		self.set_content(
			panel_id,
			PanelContent::Plot(Box::new(HostPlotState::new(
				display_label,
				kernel,
				authored_settings,
			))),
		);
	}

	/// Native surface for a host panel. Backing objects will replace the
	/// placeholder without touching the topology seam.
	fn surface<'a>(
		&'a self,
		panel: &'a libaryon::uast::projection::HostPanelViewport,
		palette: UiPalette,
		show_column_types: bool,
		dashboard: &'a DashboardState,
	) -> Element<'a, EditorMsg> {
		let entry = self.panels.get(&panel.panel_id);
		let content = entry
			.map(|entry| &entry.content)
			.unwrap_or(&PanelContent::Unresolved);

		let inner: Element<'a, EditorMsg> = if panel.panel_id == DASHBOARD_PANEL {
			dashboard::view(dashboard).map(EditorMsg::Dashboard)
		} else {
			match content {
				PanelContent::Table(artifact) => Table::new(
					artifact.frame.as_ref(),
					0,
					self.table_skin_at_draw(panel.panel_id, artifact.skin),
				)
				.show_column_types(show_column_types)
				.scroll_request(
					self.panels
						.get(&panel.panel_id)
						.and_then(|entry| entry.scroll),
				)
				.width_store(
					self.panels
						.get(&panel.panel_id)
						.map(|entry| &entry.col_widths),
				)
				.into(),
				#[cfg(feature = "toron")]
				PanelContent::Plot(plot) => {
					plot_pane::host_view(plot).map(move |message| EditorMsg::HostPlot {
						panel_id: panel.panel_id,
						message,
					})
				}
				PanelContent::Running => {
					panel_message("running…".to_owned(), color(palette.preview_text))
				}
				PanelContent::Message(message) => {
					panel_message(message.clone(), color(palette.preview_text))
				}
				#[cfg(feature = "toron")]
				PanelContent::Failed => panel_message(
					"failed · see status bar".to_owned(),
					color(palette.status_text),
				),
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
		#[cfg(feature = "toron")]
		let marker = entry.and_then(|entry| match &entry.status {
			PanelStatus::Fresh => None,
			#[cfg(feature = "toron")]
			PanelStatus::Running => Some(("refreshing…".to_owned(), palette.preview_text)),
			#[cfg(feature = "toron")]
			PanelStatus::Stale => Some(("stale".to_owned(), palette.status_text)),
			#[cfg(feature = "toron")]
			PanelStatus::Orphaned => Some(("orphaned".to_owned(), palette.status_text)),
		});
		#[cfg(feature = "toron")]
		let inner = if let Some((label, tint)) = marker {
			stack![
				inner,
				container(
					text(label)
						.font(EDITOR_FONT)
						.size(metrics::TEXT_SIZE - 2.0)
						.color(color(tint)),
				)
				.padding(4)
				.align_x(iced::alignment::Horizontal::Right)
			]
			.into()
		} else {
			inner
		};
		let surface = color(palette.preview_surface);
		container(inner)
			.padding(4)
			.width(Length::Fill)
			.height(Length::Fill)
			.style(move |_| container::Style {
				background: Some(Background::Color(surface)),
				border: iced::Border {
					color: color(palette.divider_surface),
					width: 1.0,
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

#[cfg(test)]
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

fn tab_label(tab: &libaryon::uast::projection::DocTabSummary) -> String {
	format!("{}{}", tab.title, if tab.is_dirty { " +" } else { "" })
}

fn tab_button_style(
	active: bool,
	pane_focused: bool,
	palette: UiPalette,
	status: button::Status,
) -> button::Style {
	let base_surface = if pane_focused {
		palette.tab_surface
	} else {
		palette.tab_active_surface
	};
	let active_surface = if pane_focused {
		palette.tab_active_surface
	} else {
		palette.tab_surface
	};
	let background =
		if active || matches!(status, button::Status::Hovered | button::Status::Pressed) {
			active_surface
		} else {
			base_surface
		};
	button::Style {
		background: Some(Background::Color(color(background))),
		text_color: color(if active {
			palette.tab_accent
		} else {
			palette.tab_text
		}),
		..button::Style::default()
	}
}

fn pane_tab_bar<'a>(
	pane: &'a libaryon::uast::projection::PaneViewport,
	focused: bool,
	palette: UiPalette,
	retile_outputs: bool,
) -> Element<'a, EditorMsg> {
	let header_background = color(if focused {
		palette.tab_surface
	} else {
		palette.tab_active_surface
	});
	let drag_handle = mouse_area(
		Space::new()
			.width(Length::Fixed(PANE_DRAG_HANDLE_WIDTH))
			.height(Length::Fixed(PANE_HEADER_HEIGHT)),
	)
	.on_press(EditorMsg::PaneDragStarted(pane.pane_id))
	.on_release(EditorMsg::Release);

	let mut tabs = row![drag_handle]
		.spacing(1)
		.height(Length::Fixed(PANE_HEADER_HEIGHT));
	for (index, tab) in pane.tabs.iter().enumerate() {
		let active = index == pane.active_doctab_index;
		let label = tab_label(tab);
		let font = iced::Font {
			weight: if active {
				iced::font::Weight::Bold
			} else {
				iced::font::Weight::Normal
			},
			..EDITOR_FONT
		};
		let pane_id = pane.pane_id;
		let view_id = tab.view_id;
		let title = button(text(label).font(font).size(13.0))
			.padding([2, 6])
			.height(Length::Fixed(PANE_HEADER_HEIGHT))
			.style(move |_theme: &Theme, status| tab_button_style(active, focused, palette, status))
			.on_press(EditorMsg::ActivateDocTab { pane_id, view_id });
		let previous_view_id = pane.tabs[(index + pane.tabs.len() - 1) % pane.tabs.len()].view_id;
		let next_view_id = pane.tabs[(index + 1) % pane.tabs.len()].view_id;
		let control = |label: &'static str, message| {
			button(text(label).font(font).size(13.0))
				.padding([2, 4])
				.height(Length::Fixed(PANE_HEADER_HEIGHT))
				.style(move |_theme: &Theme, status| {
					tab_button_style(active, focused, palette, status)
				})
				.on_press(message)
		};
		let controls = row![
			control(
				"❮",
				EditorMsg::ActivateDocTab {
					pane_id,
					view_id: previous_view_id,
				}
			),
			control(
				"—",
				EditorMsg::PaneTabAction {
					pane_id,
					action: PaneTabAction::NewPane(SplitDirection::Horizontal),
				}
			),
			control(
				"|",
				EditorMsg::PaneTabAction {
					pane_id,
					action: PaneTabAction::NewPane(SplitDirection::Vertical),
				}
			),
			control(
				"+",
				EditorMsg::PaneTabAction {
					pane_id,
					action: PaneTabAction::NewTab,
				}
			),
			control("✕", EditorMsg::CloseDocTab { pane_id, view_id }),
			control(
				"❯",
				EditorMsg::ActivateDocTab {
					pane_id,
					view_id: next_view_id,
				}
			),
		]
		.spacing(0);
		tabs = tabs.push(row![title, controls].spacing(0));
	}
	#[cfg(feature = "toron")]
	if retile_outputs {
		let retile = button(text("▦").font(EDITOR_FONT).size(13.0))
			.padding([2, 6])
			.height(Length::Fixed(PANE_HEADER_HEIGHT))
			.style(move |_theme: &Theme, status| tab_button_style(false, focused, palette, status))
			.on_press(EditorMsg::RetileOutputs {
				pane_id: pane.pane_id,
			});
		tabs = tabs.push(tooltip(
			retile,
			text("Retile outputs").font(EDITOR_FONT).size(13.0),
			tooltip::Position::Bottom,
		));
	}
	#[cfg(not(feature = "toron"))]
	let _ = retile_outputs;
	tabs = tabs.push(
		mouse_area(Space::new().width(Length::Fill).height(Length::Fill))
			.on_press(EditorMsg::PaneDragStarted(pane.pane_id))
			.on_release(EditorMsg::Release),
	);

	container(tabs)
		.width(Length::Fill)
		.height(Length::Fixed(PANE_HEADER_HEIGHT))
		.style(move |_| container::Style {
			background: Some(Background::Color(header_background)),
			..Default::default()
		})
		.into()
}

/// Common workspace chrome. Libaryon owns tab order, active identity, titles,
/// and dirty state; Polariton renders those projections as compact controls.
fn pane_surface<'a>(
	pane: &'a libaryon::uast::projection::PaneViewport,
	focused: bool,
	palette: UiPalette,
	content: Element<'a, EditorMsg>,
	retile_outputs: bool,
) -> Element<'a, EditorMsg> {
	let surface: Element<'a, EditorMsg> = container(
		column![
			pane_tab_bar(pane, focused, palette, retile_outputs),
			content,
		]
		.width(Length::Fill)
		.height(Length::Fill),
	)
	.width(Length::Fill)
	.height(Length::Fill)
	.style(move |_| container::Style {
		border: iced::Border {
			color: color(palette.divider_surface),
			width: 1.0,
			radius: 0.0.into(),
		},
		..Default::default()
	})
	.into();
	pane_focus_frame(surface, focused, palette)
}

const FOCUSED_PANE_BORDER_WIDTH: f32 = 1.0;

fn focused_pane_border(palette: UiPalette) -> iced::Border {
	iced::Border {
		color: color(palette.accent),
		width: FOCUSED_PANE_BORDER_WIDTH,
		radius: 0.0.into(),
	}
}

/// Paint focus over the complete pane, after its header and body. Keeping the
/// overlay at this fixed-size boundary prevents a shrink-sized editor body
/// from placing its bottom edge through the final rendered text row.
fn pane_focus_frame<'a>(
	pane: Element<'a, EditorMsg>,
	focused: bool,
	palette: UiPalette,
) -> Element<'a, EditorMsg> {
	if !focused {
		return pane;
	}
	stack![
		pane,
		container(Space::new())
			.width(Length::Fill)
			.height(Length::Fill)
			.style(move |_| container::Style {
				border: focused_pane_border(palette),
				..Default::default()
			}),
	]
	.width(Length::Fill)
	.height(Length::Fill)
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

struct UastGraphWidget<'a> {
	snapshot: &'a libaryon::uast::UastGraphSnapshot,
	inspector_view_id: ViewId,
	palette: UiPalette,
}

fn uast_graph_surface<'a>(
	snapshot: &'a libaryon::uast::UastGraphSnapshot,
	inspector_view_id: ViewId,
	palette: UiPalette,
) -> Element<'a, EditorMsg> {
	container(
		canvas(UastGraphWidget {
			snapshot,
			inspector_view_id,
			palette,
		})
		.width(Length::Fill)
		.height(Length::Fill),
	)
	.width(Length::Fill)
	.height(Length::Fill)
	.style(move |_| container::Style {
		background: Some(Background::Color(color(palette.editor_background))),
		..Default::default()
	})
	.into()
}

fn uast_graph_layout(
	snapshot: &libaryon::uast::UastGraphSnapshot,
	size: iced::Size,
) -> HashMap<u32, iced::Rectangle> {
	if snapshot.nodes.is_empty() {
		return HashMap::new();
	}
	let min_depth = snapshot
		.nodes
		.iter()
		.map(|node| node.depth)
		.min()
		.unwrap_or(0);
	let max_depth = snapshot
		.nodes
		.iter()
		.map(|node| node.depth)
		.max()
		.unwrap_or(min_depth);
	let level_count = f32::from(max_depth.saturating_sub(min_depth)) + 1.0;
	let mut levels = HashMap::<u16, Vec<u32>>::new();
	for node in &snapshot.nodes {
		levels.entry(node.depth).or_default().push(node.id);
	}
	let mut layout = HashMap::new();
	for (depth, ids) in levels {
		let count = ids.len().max(1) as f32;
		let slot_width = size.width / count;
		let box_width = (slot_width - 12.0).clamp(58.0, 180.0);
		let slot_height = size.height / level_count.max(1.0);
		let box_height = (slot_height - 14.0).clamp(34.0, 58.0);
		let y = f32::from(depth.saturating_sub(min_depth)) * slot_height
			+ (slot_height - box_height) / 2.0;
		for (index, id) in ids.into_iter().enumerate() {
			let x = index as f32 * slot_width + (slot_width - box_width) / 2.0;
			layout.insert(
				id,
				iced::Rectangle::new(
					iced::Point::new(x.max(0.0), y.max(0.0)),
					iced::Size::new(box_width.min(size.width), box_height.min(size.height)),
				),
			);
		}
	}
	layout
}

impl canvas::Program<EditorMsg> for UastGraphWidget<'_> {
	type State = ();

	fn draw(
		&self,
		_state: &Self::State,
		renderer: &iced::Renderer,
		_theme: &iced::Theme,
		bounds: iced::Rectangle,
		_cursor: mouse::Cursor,
	) -> Vec<canvas::Geometry> {
		let mut frame = canvas::Frame::new(renderer, bounds.size());
		let layout = uast_graph_layout(self.snapshot, bounds.size());
		for node in &self.snapshot.nodes {
			let (Some(parent), Some(child)) = (
				node.parent.and_then(|parent| layout.get(&parent)),
				layout.get(&node.id),
			) else {
				continue;
			};
			frame.stroke(
				&canvas::Path::line(
					iced::Point::new(parent.center_x(), parent.y + parent.height),
					iced::Point::new(child.center_x(), child.y),
				),
				canvas::Stroke::default()
					.with_width(1.5)
					.with_color(color(self.palette.divider_surface)),
			);
		}
		for node in &self.snapshot.nodes {
			let Some(rect) = layout.get(&node.id).copied() else {
				continue;
			};
			let fill = match node.semantic_kind {
				libaryon::uast::SemanticKind::RelationalTable => self.palette.tab_active_surface,
				libaryon::uast::SemanticKind::RelationalRow => self.palette.gutter_surface,
				libaryon::uast::SemanticKind::None => self.palette.tab_surface,
				libaryon::uast::SemanticKind::Token => self.palette.editor_background,
			};
			let path = canvas::Path::rectangle(rect.position(), rect.size());
			frame.fill(&path, color(fill));
			frame.stroke(
				&path,
				canvas::Stroke::default()
					.with_width(if node.selected { 2.5 } else { 1.0 })
					.with_color(color(if node.selected {
						self.palette.accent
					} else {
						self.palette.tab_text
					})),
			);
			frame.fill_text(canvas::Text {
				content: node.label.chars().take(22).collect(),
				position: iced::Point::new(rect.x + 7.0, rect.y + 8.0),
				color: color(self.palette.editor_text),
				size: iced::Pixels(13.0),
				font: EDITOR_FONT,
				..Default::default()
			});
			frame.fill_text(canvas::Text {
				content: format!("#{} [{}..{})", node.id, node.start_byte, node.end_byte),
				position: iced::Point::new(rect.x + 7.0, rect.y + 25.0),
				color: color(self.palette.gutter_text),
				size: iced::Pixels(10.0),
				font: EDITOR_FONT,
				..Default::default()
			});
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
		let layout = uast_graph_layout(self.snapshot, bounds.size());
		let node = self.snapshot.nodes.iter().rev().find(|node| {
			layout
				.get(&node.id)
				.is_some_and(|rect| rect.contains(position))
		})?;
		Some(
			canvas::Action::publish(EditorMsg::UastGraphNode {
				inspector_view_id: self.inspector_view_id,
				selection: self.snapshot.selection(node),
			})
			.and_capture(),
		)
	}

	fn mouse_interaction(
		&self,
		_state: &Self::State,
		bounds: iced::Rectangle,
		cursor: mouse::Cursor,
	) -> mouse::Interaction {
		let Some(position) = cursor.position_in(bounds) else {
			return mouse::Interaction::default();
		};
		if uast_graph_layout(self.snapshot, bounds.size())
			.values()
			.any(|rect| rect.contains(position))
		{
			mouse::Interaction::Pointer
		} else {
			mouse::Interaction::default()
		}
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
	show_cursor: bool,
) -> Element<'a, EditorMsg> {
	// Text no highlighter claimed is `BodyText`, the same value the Markdown
	// preview paints prose with, so a themed body color reaches both panes.
	let lines = wrap_projected_lines(
		project_lines(
			pane,
			default_text_color(&pane.theme_colors, palette.editor_text),
			palette.accent,
			show_cursor,
		),
		pane,
	);
	let gutter_background = color(palette.gutter_surface);
	let gutter_text = color(palette.gutter_text);
	let cursor_lines = &pane.cursor_lines;
	let show_gutter = pane.gutter_numbering.is_some();
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
		let editor_text = rich_text(fragments)
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.height(Length::Fixed(metrics::LINE_HEIGHT))
			// `Pixels`, not a bare f32: `From<f32> for LineHeight` is
			// Relative (a multiplier of the font size), which made every
			// line 22× tall and clipped all glyphs out of the fixed row.
			.line_height(iced::Pixels(metrics::LINE_HEIGHT));
		if show_gutter {
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
				editor_text,
			]
			.spacing(0)
			.height(Length::Fixed(metrics::LINE_HEIGHT))
			.into()
		} else {
			row![editor_text]
				.height(Length::Fixed(metrics::LINE_HEIGHT))
				.into()
		}
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
	let editor_rows = mouse_area(container(rows).width(Length::Fill).height(Length::Fill))
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
			minimap_view(pane.pane_id, minimap, pane.theme_colors, palette)
		]
		.height(Length::Fill)
		.into()
	} else {
		editor_rows.into()
	};
	editor_content
}

/// Native, read-only Markdown projection. The engine owns parsing, cell
/// identity, and code highlighting: this surface receives blocks plus the same
/// palette a text pane receives, then only paints those pixels.
///
/// The window is anchored on the source line the preview mirrors and bounded by
/// [`preview_row_budget`], so a long document costs the same as a short one.
fn markdown_preview_surface<'a>(
	preview: &'a libaryon::uast::projection::MarkdownPreviewSnapshot,
	blocks: &'a [MarkdownBlock],
	palette: UiPalette,
	outputs: &'a HashMap<MarkdownCellIdentity, NotebookCellOutput>,
	show_column_types: bool,
) -> Element<'a, EditorMsg> {
	let source = preview.source_text.as_str();
	let colors = MarkdownPreviewColors::new(preview.theme_colors, palette);
	let window = preview_source_window(
		source,
		preview.source_scroll_line.get(),
		preview.source_viewport_line_count,
	);

	let mut rows: Vec<Element<'_, EditorMsg>> = Vec::new();
	for block in visible_blocks(blocks, window) {
		let range = block.source_range();
		if let MarkdownBlock::CodeBlock { text, .. } = block {
			rows.extend(markdown_code_block_rows(
				preview,
				source,
				range,
				text,
				palette,
				outputs,
				show_column_types,
			));
			continue;
		}
		rows.push(preview_row(markdown_block_surface(block, colors)));
	}

	container(column(rows))
		.padding(20)
		.width(Length::Fill)
		.height(Length::Fill)
		.into()
}

/// The colors the preview draws Markdown in.
///
/// Every entry comes from [`markdown_construct_color`], the same table the
/// source highlighter classifies its tokens with. A construct therefore cannot
/// be one color in a document and another in its preview — and Baryon, reading
/// the same table, cannot disagree with either.
///
/// Bold and italic are not in the table's gift: on the left only the `*`
/// markers take the emphasis color, and the preview draws no markers, so an
/// emphasized run stays prose-colored and lets weight and slant carry it.
/// Coloring the run here would show more than the document says.
#[derive(Clone, Copy)]
struct MarkdownPreviewColors {
	body: Rgb,
	heading: Rgb,
	inline_code: Rgb,
	bullet: Rgb,
	ordered_marker: Rgb,
	quote: Rgb,
	rule: Rgb,
	row_alt: Rgb,
}

impl MarkdownPreviewColors {
	fn new(
		theme_colors: [Option<Rgb>; libaryon::svp::rust_highlight::CATEGORY_COUNT],
		palette: UiPalette,
	) -> Self {
		let resolved = |construct: MarkdownConstruct| -> Rgb {
			markdown_construct_color(construct, &theme_colors, palette.editor_text)
		};
		Self {
			body: resolved(MarkdownConstruct::Body),
			heading: resolved(MarkdownConstruct::Heading),
			inline_code: resolved(MarkdownConstruct::InlineCode),
			bullet: resolved(MarkdownConstruct::BulletMarker),
			ordered_marker: resolved(MarkdownConstruct::OrderedMarker),
			quote: resolved(MarkdownConstruct::Quote),
			rule: resolved(MarkdownConstruct::Rule),
			// Banding is chrome, not syntax: no construct produces it.
			row_alt: palette.preview_surface,
		}
	}
}

/// Byte range of the source the preview draws: the window its source pane
/// shows, anchored on the line the two panes share.
fn preview_source_window(
	source: &str,
	first_line: u64,
	source_viewport_line_count: u32,
) -> std::ops::Range<usize> {
	let last_line = first_line.saturating_add(preview_row_budget(source_viewport_line_count));
	line_start_byte(source, first_line)..line_start_byte(source, last_line)
}

/// Blocks overlapping the window. A block that merely straddles an edge is
/// drawn: a paragraph half in view is still half in view.
fn visible_blocks(
	blocks: &[MarkdownBlock],
	window: std::ops::Range<usize>,
) -> impl Iterator<Item = &MarkdownBlock> {
	blocks.iter().filter(move |block| {
		let range = block.source_range();
		range.end > window.start && range.start < window.end
	})
}

/// Vertical gap between preview rows, in pixels.
const PREVIEW_ROW_GAP: f32 = 8.0;

/// How many source lines the preview renders below its anchor line.
///
/// The preview mirrors the source pane's window, but its rows are not a fixed
/// height — a blank line is half a row, a heading or an inline cell output is
/// several — so the count that fills the pane is not the source's line count.
/// Overshooting is what keeps the pane full when short rows dominate; blocks
/// past the bottom edge cost a measure and draw nothing. It stays proportional
/// to the pane so the budget cannot outgrow the space that has to absorb it.
fn preview_row_budget(source_viewport_line_count: u32) -> u64 {
	u64::from(source_viewport_line_count).saturating_mul(2)
}

/// Wrap a preview row in its own bottom padding.
///
/// The gap between rows is deliberately not column spacing: iced subtracts a
/// column's entire spacing budget from the available height *before* laying out
/// any child, so with enough rows the first child is measured against a
/// negative height and every row collapses to nothing. Padding is measured per
/// row, so rows that do fit always render.
fn preview_row<'a>(content: Element<'a, EditorMsg>) -> Element<'a, EditorMsg> {
	container(content)
		.padding(iced::padding::bottom(PREVIEW_ROW_GAP))
		.width(Length::Fill)
		.into()
}

fn markdown_block_surface<'a>(
	block: &'a MarkdownBlock,
	colors: MarkdownPreviewColors,
) -> Element<'a, EditorMsg> {
	match block {
		MarkdownBlock::Heading {
			level, text: title, ..
		} => {
			let heading = text(title.as_str())
				.font(HEADING_FONT)
				.size(heading_text_size(*level))
				.color(color(colors.heading));
			// A rule under the top two levels is the document's own sectioning,
			// which is why it is drawn for those levels and no others.
			if *level <= 2 {
				column![heading, markdown_rule(colors.rule, 0.45)]
					.spacing(4)
					.into()
			} else {
				heading.into()
			}
		}
		MarkdownBlock::Paragraph { text: body, .. } => {
			markdown_inline_surface(body, colors, colors.body)
		}
		MarkdownBlock::Quote { text: body, .. } => {
			container(markdown_inline_surface(body, colors, colors.quote))
				.padding([2, 12])
				.width(Length::Fill)
				.into()
		}
		MarkdownBlock::ListItem {
			depth,
			marker,
			checked,
			text: body,
			..
		} => {
			let marker_color = match marker {
				ListMarker::Ordered(_) => colors.ordered_marker,
				ListMarker::Unordered => colors.bullet,
			};
			row![
				text(list_item_marker(*marker, *checked))
					.font(iced::Font::DEFAULT)
					.size(MARKDOWN_BODY_SIZE)
					.color(color(marker_color)),
				markdown_inline_surface(body, colors, colors.body),
			]
			.spacing(8)
			.padding(iced::padding::left(
				MARKDOWN_LIST_INDENT * f32::from(*depth),
			))
			.into()
		}
		MarkdownBlock::Rule { .. } => markdown_rule(colors.rule, 0.35),
		MarkdownBlock::Table { rows: cells, .. } => markdown_table_surface(cells, colors),
		MarkdownBlock::Html { text: raw, .. } => text(raw.as_str())
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.color(color(colors.quote))
			.into(),
		// Fences are rendered by `markdown_code_block_rows`, which needs source
		// coordinates this function does not carry.
		MarkdownBlock::CodeBlock { text: code, .. } => text(code.as_str())
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.color(color(colors.body))
			.into(),
	}
}

/// Inline runs, styled by the shared model: bold, italic, and inline code are
/// document semantics, so they are honored rather than shown as their markers.
///
/// Emphasis carries weight and slant only, matching what the source pane shows
/// for the same text; see [`MarkdownPreviewColors`]. Inline code keeps its
/// color when a run is both — a bold `identifier` is still an identifier.
fn markdown_inline_surface<'a>(
	inline: &'a MarkdownInlineText,
	colors: MarkdownPreviewColors,
	foreground: Rgb,
) -> Element<'a, EditorMsg> {
	let spans = inline
		.spans()
		.iter()
		.map(|inline_span| {
			let style = inline_span.style;
			let mut font = if style.code {
				EDITOR_FONT
			} else {
				iced::Font::DEFAULT
			};
			if style.strong {
				font.weight = iced::font::Weight::Bold;
			}
			if style.emphasis {
				font.style = iced::font::Style::Italic;
			}
			let span_color = if style.code {
				colors.inline_code
			} else {
				foreground
			};
			span::<EditorMsg, iced::Font>(inline_span.text.as_str())
				.font(font)
				.color(color(span_color))
		})
		.collect::<Vec<_>>();
	rich_text(spans)
		.size(MARKDOWN_BODY_SIZE)
		.line_height(iced::Pixels(MARKDOWN_BODY_LINE_HEIGHT))
		.into()
}

fn markdown_table_surface<'a>(
	rows: &'a [Vec<MarkdownInlineText>],
	colors: MarkdownPreviewColors,
) -> Element<'a, EditorMsg> {
	let mut laid_out: Vec<Element<'_, EditorMsg>> = Vec::with_capacity(rows.len());
	for (index, cells) in rows.iter().enumerate() {
		let foreground = if index == 0 {
			colors.heading
		} else {
			colors.body
		};
		let row_cells = cells
			.iter()
			.map(|cell| {
				container(markdown_inline_surface(cell, colors, foreground))
					.padding([2, 8])
					.width(Length::Fill)
					.into()
			})
			.collect::<Vec<Element<'_, EditorMsg>>>();
		// Banding is what makes a wide row readable across; it is chrome, so it
		// comes from the UI palette rather than a syntax category.
		let row_alt = colors.row_alt;
		laid_out.push(
			container(row(row_cells))
				.width(Length::Fill)
				.style(move |_| container::Style {
					background: (index % 2 == 1).then(|| Background::Color(color(row_alt))),
					..Default::default()
				})
				.into(),
		);
		if index == 0 {
			laid_out.push(markdown_rule(colors.rule, 0.45));
		}
	}
	column(laid_out).into()
}

fn markdown_rule<'a>(rule_color: Rgb, alpha: f32) -> Element<'a, EditorMsg> {
	container(Space::new().height(Length::Fixed(1.0)))
		.width(Length::Fill)
		.style(move |_| container::Style {
			background: Some(Background::Color(Color {
				a: alpha,
				..color(rule_color)
			})),
			..Default::default()
		})
		.into()
}

fn list_item_marker(marker: ListMarker, checked: Option<bool>) -> String {
	match (checked, marker) {
		(Some(true), _) => "\u{2611}".to_string(),
		(Some(false), _) => "\u{2610}".to_string(),
		(None, ListMarker::Unordered) => "\u{25aa}".to_string(),
		(None, ListMarker::Ordered(number)) => format!("{number}."),
	}
}

fn heading_text_size(level: u8) -> f32 {
	match level {
		1 => 28.0,
		2 => 22.0,
		3 => 18.0,
		_ => MARKDOWN_BODY_SIZE,
	}
}

/// Render one fence: its highlighted body, then its output when the engine
/// gave the fence a cell identity and the host has run it.
///
/// Highlight spans are in source coordinates, so the body's own byte offset is
/// recovered from the fence's source range rather than tracked line by line.
fn markdown_code_block_rows<'a>(
	preview: &'a libaryon::uast::projection::MarkdownPreviewSnapshot,
	source: &'a str,
	range: &std::ops::Range<usize>,
	body: &'a str,
	palette: UiPalette,
	outputs: &'a HashMap<MarkdownCellIdentity, NotebookCellOutput>,
	show_column_types: bool,
) -> Vec<Element<'a, EditorMsg>> {
	let mut produced = Vec::new();
	let body_start = fence_body_start(source, range);
	let cell = preview
		.cells
		.iter()
		.find(|cell| cell.body_range.start.get() == body_start as u64);
	let hidden =
		cell.is_some_and(|cell| cell.presentation.code == MarkdownCellCodeVisibility::Hide);

	if !hidden {
		let mut lines: Vec<Element<'_, EditorMsg>> = Vec::new();
		let mut byte = body_start as u64;
		for line in body.split('\n') {
			lines.push(
				rich_text(markdown_code_fragments(
					line,
					byte,
					&preview.code_highlights,
					preview.theme_colors,
					palette.editor_text,
				))
				.font(EDITOR_FONT)
				.size(metrics::TEXT_SIZE)
				.line_height(iced::Pixels(metrics::LINE_HEIGHT))
				.into(),
			);
			byte = byte.saturating_add(line.len() as u64).saturating_add(1);
		}
		produced.push(preview_row(
			container(column(lines))
				.padding([6, 12])
				.width(Length::Fill)
				.style(move |_| container::Style {
					background: Some(Background::Color(color(palette.gutter_surface))),
					..Default::default()
				})
				.into(),
		));
	}

	if let Some(cell) = cell
		&& let Some(output) = outputs.get(&cell.identity)
		&& cell.presentation.output == libaryon::uast::projection::MarkdownCellOutputTarget::Inline
	{
		produced.push(preview_row(markdown_cell_output_surface(
			cell.identity.clone(),
			output,
			palette,
			show_column_types,
		)));
	}
	produced
}

/// Byte offset of a fence body: the line after the opening fence. An indented
/// code block has no info line, so its body starts where the block does.
fn fence_body_start(source: &str, range: &std::ops::Range<usize>) -> usize {
	let opening = &source[range.clone()];
	if !opening.starts_with("```") && !opening.starts_with("~~~") {
		return range.start;
	}
	opening
		.find('\n')
		.map_or(range.end, |newline| range.start + newline + 1)
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
		NotebookCellOutput::Table(artifact) => {
			Table::new(artifact.frame.as_ref(), 0, artifact.skin)
				.show_column_types(show_column_types)
				.into()
		}
		NotebookCellOutput::Plot { surface } => {
			let plot: Element<'_, crate::gui::messages::PlotMessage> = canvas(PlotWidget {
				kernel: surface.kernel.as_ref(),
				title: surface.composition.title(),
				padding: 20.0,
				settings: surface.settings.clone(),
				settings_model: crate::plot::theme::PlotSettings::from_resolved(
					surface.settings.clone(),
				),
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
								..surface.settings.surrounding_background_color
							})),
							border: iced::Border {
								color: surface.settings.surrounding_decoration_color,
								width: 1.0,
								radius: 2.0.into(),
							},
							text_color: Some(surface.settings.surrounding_decoration_color),
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
		Event::Window(iced::window::Event::Focused) => {
			return Some(EditorMsg::WindowFocused(true));
		}
		Event::Window(iced::window::Event::Unfocused) => {
			return Some(EditorMsg::WindowFocused(false));
		}
		Event::Mouse(mouse::Event::CursorMoved { position }) => {
			return Some(EditorMsg::RawCursorMoved(*position));
		}
		Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)) => {
			return Some(EditorMsg::Release);
		}
		_ => {}
	}
	if status != event::Status::Ignored {
		return None;
	}
	if let Event::InputMethod(iced::advanced::input_method::Event::Commit(committed)) = event
		&& !committed.is_empty()
	{
		return Some(EditorMsg::Input(InputEvent::Paste(committed.clone())));
	}
	let Event::Keyboard(keyboard::Event::KeyPressed {
		modified_key,
		modifiers,
		text,
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
	// `text` is the compositor's committed-text payload. Emoji pickers, input
	// methods, and composed graphemes can produce text that has no portable key
	// identity, or more than one Unicode scalar. Turning `modified_key` into a
	// `KeyCode::Char` would either drop that input or keep only its first scalar.
	// Route those commits through the engine's existing modal-safe paste path.
	// Ordinary single-character keys stay key events so Normal-mode mappings and
	// command modifiers retain their exact behavior.
	let command_modified = modifiers.control() || modifiers.alt() || modifiers.logo();
	if !command_modified
		&& let Some(committed) = text.as_ref().filter(|value| !value.is_empty())
		&& (committed.chars().nth(1).is_some()
			|| key_code(modified_key).is_none()
			|| matches!(modified_key, Key::Character(value) if value != committed))
	{
		return Some(EditorMsg::Input(InputEvent::Paste(committed.to_string())));
	}
	if !command_modified
		&& let Key::Character(committed) = modified_key
		&& committed.chars().nth(1).is_some()
	{
		return Some(EditorMsg::Input(InputEvent::Paste(committed.to_string())));
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

fn prompt_has_cursor(mode: &EditorMode) -> bool {
	matches!(
		mode,
		EditorMode::Command | EditorMode::Search | EditorMode::Password
	)
}

fn clamp_utf8_boundary(text: &str, byte: usize) -> usize {
	let mut byte = byte.min(text.len());
	while !text.is_char_boundary(byte) {
		byte = byte.saturating_sub(1);
	}
	byte
}

/// Translate libaryon's prompt-local insertion index into the displayed
/// command-bar string, whose `:`/`/`/password label is frontend decoration.
fn prompt_cursor_byte(viewport: &Viewport) -> Option<usize> {
	let input = &viewport.input;
	match input.mode {
		EditorMode::Command | EditorMode::Search => {
			Some(1 + clamp_utf8_boundary(&input.prompt_buffer, input.prompt_cursor))
		}
		EditorMode::Password => {
			let label = input
				.prompt_label
				.as_deref()
				.unwrap_or("Encryption password:");
			let buffer_byte = input
				.prompt_buffer
				.char_indices()
				.nth(input.prompt_cursor)
				.map_or(input.prompt_buffer.len(), |(byte, _)| byte);
			Some(label.len() + 1 + buffer_byte)
		}
		_ => None,
	}
}

fn command_bar_fragments(
	prompt: String,
	cursor_byte: Option<usize>,
	cursor_visible: bool,
	palette: UiPalette,
) -> Vec<StyledFragment> {
	let foreground = color(palette.editor_text);
	let Some(cursor_byte) = cursor_byte else {
		return vec![StyledFragment {
			text: prompt,
			foreground,
			background: None,
			underline: false,
		}];
	};
	let cursor_byte = clamp_utf8_boundary(&prompt, cursor_byte);
	let cursor_end = prompt[cursor_byte..]
		.chars()
		.next()
		.map_or(cursor_byte, |character| cursor_byte + character.len_utf8());
	let mut fragments = Vec::with_capacity(3);
	let mut push = |text: String, cursor: bool| {
		if text.is_empty() {
			return;
		}
		fragments.push(StyledFragment {
			text,
			foreground: if cursor && cursor_visible {
				color(palette.editor_background)
			} else {
				foreground
			},
			background: (cursor && cursor_visible).then_some(color(palette.accent)),
			underline: false,
		});
	};
	push(prompt[..cursor_byte].to_owned(), false);
	push(
		if cursor_end == cursor_byte {
			" ".to_owned()
		} else {
			prompt[cursor_byte..cursor_end].to_owned()
		},
		true,
	);
	push(prompt[cursor_end..].to_owned(), false);
	fragments
}

/// Project libaryon's prompt line (`:` command, `/` search, or a confirm
/// question). The engine remains responsible for editing and executing the
/// buffer; this is intentionally display-only so its modal key handling
/// continues to flow through the normal subscription.
fn command_bar(
	prompt: String,
	cursor_byte: Option<usize>,
	cursor_visible: bool,
	palette: UiPalette,
) -> Element<'static, EditorMsg> {
	let fragments = command_bar_fragments(prompt, cursor_byte, cursor_visible, palette)
		.into_iter()
		.map(|fragment| {
			let StyledFragment {
				text,
				foreground,
				background,
				..
			} = fragment;
			let text_span = span::<(), iced::Font>(text).color(foreground);
			match background {
				Some(background) => text_span.background(Background::Color(background)),
				None => text_span,
			}
		})
		.collect::<Vec<_>>();
	container(
		rich_text(fragments)
			.font(EDITOR_FONT)
			.size(metrics::TEXT_SIZE)
			.line_height(iced::Pixels(metrics::LINE_HEIGHT)),
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
	pane_id: libaryon::core::PaneId,
	snapshot: &'a MinimapSnapshot,
	theme_colors: [Option<Rgb>; libaryon::svp::rust_highlight::CATEGORY_COUNT],
	palette: UiPalette,
}

fn minimap_view<'a>(
	pane_id: libaryon::core::PaneId,
	snapshot: &'a MinimapSnapshot,
	theme_colors: [Option<Rgb>; libaryon::svp::rust_highlight::CATEGORY_COUNT],
	palette: UiPalette,
) -> Element<'a, EditorMsg> {
	canvas(MinimapWidget {
		pane_id,
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
		let rgba = [base.r, base.g, base.b, 255].repeat((width * height) as usize);
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

	fn click_target(&self, position_y: f32, bounds_height: f32) -> Option<DocLine> {
		let image_height = (bounds_height.max(1.0) as u32) * MINIMAP_SCALE;
		let content_height = self.content_height(image_height);
		let local_row = (position_y.max(0.0) * MINIMAP_SCALE as f32) as u32;
		let overlay = self.snapshot.overlay();
		let max_doc_line = DocLine::new(overlay.doc_line_count.get().saturating_sub(1));
		minimap_click_viewport_start_line(
			local_row,
			max_doc_line,
			content_height,
			overlay.viewport_line_count,
		)
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
								preview.max_columns,
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
struct MinimapInkCache {
	ink: std::cell::RefCell<Option<(u64, iced::advanced::image::Handle)>>,
	swatches: std::cell::RefCell<
		Option<(
			Rgb,
			iced::advanced::image::Handle,
			iced::advanced::image::Handle,
		)>,
	>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MinimapOverlayTone {
	Frame,
	Cursor,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct MinimapOverlayImage {
	bounds: iced::Rectangle,
	tone: MinimapOverlayTone,
}

fn minimap_live_overlays(
	overlay: &libaryon::engine::MinimapOverlay,
	content_height: f32,
	size: iced::Size,
) -> Vec<MinimapOverlayImage> {
	if content_height <= 0.0 {
		return Vec::new();
	}
	let doc_lines = overlay.doc_line_count.get().max(1);
	let y_of = |line: u64| (line as f32 / doc_lines as f32) * content_height;
	let top = y_of(overlay.viewport_start_line.get());
	let bottom = y_of(overlay.viewport_end_line.get()).max(top + 1.0);
	let cursor_y = y_of(overlay.cursor_line.get());
	vec![
		MinimapOverlayImage {
			bounds: iced::Rectangle::new(
				iced::Point::new(0.0, top),
				iced::Size::new(size.width, 1.0),
			),
			tone: MinimapOverlayTone::Frame,
		},
		MinimapOverlayImage {
			bounds: iced::Rectangle::new(
				iced::Point::new(0.0, bottom - 1.0),
				iced::Size::new(size.width, 1.0),
			),
			tone: MinimapOverlayTone::Frame,
		},
		MinimapOverlayImage {
			bounds: iced::Rectangle::new(
				iced::Point::new(0.0, top),
				iced::Size::new(1.0, bottom - top),
			),
			tone: MinimapOverlayTone::Frame,
		},
		MinimapOverlayImage {
			bounds: iced::Rectangle::new(
				iced::Point::new(size.width - 1.0, top),
				iced::Size::new(1.0, bottom - top),
			),
			tone: MinimapOverlayTone::Frame,
		},
		// Cursor comes last so it remains the final pixel at intersections with
		// the viewport frame.
		MinimapOverlayImage {
			bounds: iced::Rectangle::new(
				iced::Point::new(0.0, cursor_y),
				iced::Size::new(size.width, 1.0),
			),
			tone: MinimapOverlayTone::Cursor,
		},
	]
}

impl MinimapWidget<'_> {
	/// Hash of everything `render_ink` + `render_search_markers` read. The
	/// engine's content revision names the shared immutable rows/bands without
	/// rescanning them on every 16ms Iced redraw.
	///
	/// The overlay's viewport/cursor fields are deliberately excluded: they
	/// change on every scroll and keystroke but never touch the ink image.
	fn ink_fingerprint(&self, width: u32, height: u32) -> u64 {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::collections::hash_map::DefaultHasher::new();
		width.hash(&mut hasher);
		height.hash(&mut hasher);
		match self.snapshot {
			MinimapSnapshot::Preview(_) => 0u8.hash(&mut hasher),
			MinimapSnapshot::Overview(_) => 1u8.hash(&mut hasher),
			MinimapSnapshot::ByteFallback(_) => 2u8.hash(&mut hasher),
		}
		self.snapshot.revision().hash(&mut hasher);
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
		let mut slot = cache.ink.borrow_mut();
		if let Some((cached_fingerprint, handle)) = slot.as_ref()
			&& *cached_fingerprint == fingerprint
		{
			return handle.clone();
		}
		let trace_started = std::env::var_os("KET_MINIMAP_TRACE").map(|_| time::Instant::now());
		let mut image = MinimapPixels::new(width, height, self.palette.minimap_surface);
		let content_width = width.saturating_sub(MINIMAP_MARKER_WIDTH * MINIMAP_SCALE);
		let content_height = self.content_height(height);
		self.render_ink(&mut image, content_width, content_height);
		self.render_search_markers(&mut image, content_height);
		let handle =
			iced::advanced::image::Handle::from_rgba(image.width, image.height, image.rgba);
		if let Some(started) = trace_started {
			eprintln!(
				"[minimap] polariton-raster width={width} height={height} revision={:?} elapsed={:?}",
				self.snapshot.revision(),
				started.elapsed(),
			);
		}
		*slot = Some((fingerprint, handle.clone()));
		handle
	}

	fn overlay_swatches(
		&self,
		cache: &MinimapInkCache,
	) -> (iced::advanced::image::Handle, iced::advanced::image::Handle) {
		let accent = self.palette.minimap_accent;
		let mut slot = cache.swatches.borrow_mut();
		if let Some((cached_accent, frame, cursor)) = slot.as_ref()
			&& *cached_accent == accent
		{
			return (frame.clone(), cursor.clone());
		}
		let pixel = |alpha| {
			iced::advanced::image::Handle::from_rgba(
				1,
				1,
				vec![accent.r, accent.g, accent.b, alpha],
			)
		};
		let frame = pixel(217);
		let cursor = pixel(255);
		*slot = Some((accent, frame.clone(), cursor.clone()));
		(frame, cursor)
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

		// Iced's WGPU canvas batches meshes before images regardless of call
		// order. These live marks therefore use cached 1×1 raster swatches:
		// ink and overlays stay in one image batch, whose insertion order is
		// preserved in both debug and release builds.
		let overlay = self.snapshot.overlay();
		let content_height_logical = content_height as f32 / MINIMAP_SCALE as f32;
		let (frame_swatch, cursor_swatch) = self.overlay_swatches(state);
		for live in minimap_live_overlays(overlay, content_height_logical, bounds.size()) {
			let swatch = match live.tone {
				MinimapOverlayTone::Frame => frame_swatch.clone(),
				MinimapOverlayTone::Cursor => cursor_swatch.clone(),
			};
			frame.draw_image(live.bounds, iced::advanced::image::Image::new(swatch));
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
		Some(
			canvas::Action::publish(EditorMsg::MinimapGoto {
				pane_id: self.pane_id,
				line: self.click_target(position.y, bounds.height)?,
			})
			.and_capture(),
		)
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
		BUSY_SPINNER_INTERVAL, BusySpinner, CodeEditor, DASHBOARD_PANEL, EditorCommand, EditorMsg,
		HostPanelPlacement, INITIAL_ROWS, InputEvent, KeyCode, LanguageId, MarkdownBlock,
		MarkdownPreviewColors, Modifiers, NotebookCellOutput, PANE_HEADER_HEIGHT, PREVIEW_ROW_GAP,
		PaneTabAction, PanelContent, PanelStatus, Rgb, SplitDirection, UiPalette,
		command_bar_fragments, fence_body_start, highlight_span_at, markdown_code_fragments,
		metrics, pane_gutter_width, pane_title, parse_markdown_blocks, preview_row_budget,
		preview_source_window, prompt_cursor_byte, viewport_cells, visible_blocks,
	};
	use crate::gui::table_skin::{TableRgba, TableSkin};
	use libaryon::svp::markdown_highlight::MarkdownConstruct;
	use libaryon::svp::rust_highlight::TokenCategory;
	use std::thread;
	use std::time::{Duration, Instant};

	#[test]
	fn highlight_lookup_preserves_nested_span_priority() {
		use libaryon::core::DocByte;
		use libaryon::svp::rust_highlight::{HighlightSpan, TokenCategory};

		let spans = [
			HighlightSpan::new(DocByte::new(0), DocByte::new(100), TokenCategory::BodyText),
			HighlightSpan::new(DocByte::new(20), DocByte::new(30), TokenCategory::Keyword),
		];
		assert_eq!(
			highlight_span_at(&spans, DocByte::new(25)).map(|span| span.category),
			Some(TokenCategory::Keyword)
		);
		assert_eq!(
			highlight_span_at(&spans, DocByte::new(35)).map(|span| span.category),
			Some(TokenCategory::BodyText)
		);
		assert_eq!(highlight_span_at(&spans, DocByte::new(100)), None);
	}

	#[test]
	fn startup_viewport_uses_the_actual_workspace_pixels() {
		assert_eq!(viewport_cells(iced::Size::new(1900.0, 967.0)), (197, 43));
	}

	#[test]
	fn busy_spinner_advances_at_baryons_cadence_and_restarts() {
		let start = Instant::now();
		let mut spinner = BusySpinner::new();
		spinner.restart(start);

		spinner.advance(start + BUSY_SPINNER_INTERVAL - Duration::from_millis(1));
		assert_eq!(spinner.phase, 0);
		spinner.advance(start + BUSY_SPINNER_INTERVAL * 3);
		assert_eq!(spinner.phase, 3);

		spinner.restart(start + Duration::from_secs(1));
		assert_eq!(spinner.phase, 0);
	}

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

	fn navigation_fixture(label: &str) -> std::path::PathBuf {
		let nonce = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("time should move forward")
			.as_nanos();
		std::env::temp_dir().join(format!(
			"polariton-file-browser-{label}-{}-{nonce}",
			std::process::id()
		))
	}

	fn wait_for_file_browser(editor: &mut CodeEditor) {
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|viewport| !viewport.panes.iter().any(|pane| pane.is_file_browser))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			editor
				.viewport
				.as_ref()
				.is_some_and(|viewport| viewport.panes.iter().any(|pane| pane.is_file_browser)),
			"file browser should be projected"
		);
	}

	#[test]
	fn minimap_ink_handle_is_cached_until_content_changes() {
		use super::{MinimapInkCache, MinimapWidget};
		use libaryon::core::{DocLine, PaneId, TokenBin};
		use libaryon::engine::{
			MinimapContentRevision, MinimapOverlay, MinimapSnapshot, PREVIEW_BIN_COLUMNS,
			PreviewRow, PreviewSnapshot, UiPalette,
		};
		use libaryon::svp::rust_highlight::{CATEGORY_COUNT, TokenCategory};

		let make_snapshot = |category: TokenCategory| {
			MinimapSnapshot::Preview(PreviewSnapshot::new(
				MinimapOverlay::new(DocLine::ZERO, 2, DocLine::ZERO),
				MinimapContentRevision::new(
					libaryon::core::BufferId::ZERO,
					libaryon::core::StateId::ZERO,
					category as u64,
				),
				8,
				vec![PreviewRow {
					logical_width: 8,
					bins: vec![TokenBin::new(category as u8); PREVIEW_BIN_COLUMNS]
						.into_boxed_slice(),
				}],
			))
		};
		let snapshot = make_snapshot(TokenCategory::Keyword);
		let widget = MinimapWidget {
			pane_id: PaneId::new(1),
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

		let mut searched = snapshot.clone();
		searched.overlay_mut().search_bands[0] = 255;
		let searched_widget = MinimapWidget {
			pane_id: PaneId::new(1),
			snapshot: &searched,
			theme_colors: [None; CATEGORY_COUNT],
			palette: UiPalette::default(),
		};
		let searched_handle = searched_widget.ink_handle(&cache, 64, 64);
		assert_ne!(
			first.id(),
			searched_handle.id(),
			"search bands must invalidate ink independently of content revision"
		);

		let edited = make_snapshot(TokenCategory::Function);
		let widget = MinimapWidget {
			pane_id: PaneId::new(1),
			snapshot: &edited,
			theme_colors: [None; CATEGORY_COUNT],
			palette: UiPalette::default(),
		};
		let third = widget.ink_handle(&cache, 64, 64);
		assert_ne!(first.id(), third.id(), "edits must rebuild the ink image");
	}

	#[test]
	fn minimap_click_centers_the_hit_with_leading_context() {
		use super::{EditorMsg, MinimapInkCache, MinimapWidget};
		use iced::widget::canvas::{self, Program as _};
		use iced::{Point, Rectangle, Size, event, mouse};
		use libaryon::core::{DocLine, DocLineCount, PaneId};
		use libaryon::engine::{
			MinimapContentRevision, MinimapOverlay, MinimapSnapshot, OverviewSnapshot, UiPalette,
		};
		use libaryon::svp::rust_highlight::CATEGORY_COUNT;

		let mut overlay = MinimapOverlay::new(DocLine::ZERO, 40, DocLine::ZERO);
		overlay.doc_line_count = DocLineCount::new(10_001);
		let snapshot = MinimapSnapshot::Overview(OverviewSnapshot::new(
			overlay,
			MinimapContentRevision::ZERO,
			vec![1],
		));
		let pane_id = PaneId::new(1);
		let widget = MinimapWidget {
			pane_id,
			snapshot: &snapshot,
			theme_colors: [None; CATEGORY_COUNT],
			palette: UiPalette::default(),
		};

		assert_eq!(widget.click_target(0.0, 80.0), Some(DocLine::ZERO));
		assert_eq!(
			widget.click_target(40.0, 80.0),
			Some(DocLine::new(5_011)),
			"the interpolated line is shifted up by half the 40-line viewport"
		);
		assert_eq!(
			widget.click_target(79.5, 80.0),
			Some(DocLine::new(9_980)),
			"the bottom of a 10k-line minimap must request the document tail"
		);

		let bounds = Rectangle::new(Point::new(100.0, 200.0), Size::new(80.0, 80.0));
		let action = widget
			.update(
				&mut MinimapInkCache::default(),
				&canvas::Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)),
				bounds,
				mouse::Cursor::Available(Point::new(140.0, 279.5)),
			)
			.expect("minimap press action");
		let (message, _, status) = action.into_inner();
		assert_eq!(
			status,
			event::Status::Captured,
			"the editor surface must not reinterpret the minimap press as a text click"
		);
		assert!(matches!(
			message,
			Some(EditorMsg::MinimapGoto {
				pane_id: clicked_pane,
				line,
			}) if clicked_pane == pane_id && line == DocLine::new(9_980)
		));
	}

	#[test]
	fn minimap_live_images_finish_with_cursor_over_viewport_frame() {
		use super::{MinimapInkCache, MinimapOverlayTone, MinimapWidget, minimap_live_overlays};
		use libaryon::core::{DocLine, DocLineCount};
		use libaryon::engine::{
			MinimapContentRevision, MinimapOverlay, MinimapSnapshot, OverviewSnapshot, UiPalette,
		};
		use libaryon::svp::rust_highlight::CATEGORY_COUNT;

		let mut overlay = MinimapOverlay::new(DocLine::new(20), 10, DocLine::new(20));
		overlay.doc_line_count = DocLineCount::new(100);
		let layers = minimap_live_overlays(&overlay, 100.0, iced::Size::new(80.0, 100.0));
		assert_eq!(layers.len(), 5);
		assert!(
			layers[..4]
				.iter()
				.all(|layer| layer.tone == MinimapOverlayTone::Frame)
		);
		assert_eq!(layers[4].tone, MinimapOverlayTone::Cursor);
		let intersection = iced::Point::new(40.0, 20.5);
		assert_eq!(
			layers
				.iter()
				.filter(|layer| layer.bounds.contains(intersection))
				.next_back()
				.map(|layer| layer.tone),
			Some(MinimapOverlayTone::Cursor),
			"the cursor must own the final pixel where it meets the frame"
		);

		let snapshot = MinimapSnapshot::Overview(OverviewSnapshot::new(
			overlay,
			MinimapContentRevision::ZERO,
			vec![1],
		));
		let widget = MinimapWidget {
			pane_id: libaryon::core::PaneId::new(1),
			snapshot: &snapshot,
			theme_colors: [None; CATEGORY_COUNT],
			palette: UiPalette::default(),
		};
		let cache = MinimapInkCache::default();
		let first = widget.overlay_swatches(&cache);
		let second = widget.overlay_swatches(&cache);
		assert_eq!(first.0.id(), second.0.id());
		assert_eq!(first.1.id(), second.1.id());
	}

	#[test]
	fn focused_pane_frame_is_a_single_accent_pixel() {
		use super::focused_pane_border;

		let palette = libaryon::engine::UiPalette::default();
		let border = focused_pane_border(palette);
		assert_eq!(border.width, 1.0);
		assert_eq!(border.color, super::color(palette.accent));
	}

	#[test]
	fn cursor_blink_stays_solid_after_activity_and_while_unfocused() {
		use super::{CURSOR_BLINK_INTERVAL, CursorBlink};

		let start = iced::time::Instant::now();
		let mut blink = CursorBlink {
			last_activity: start,
			now: start,
			window_focused: true,
		};
		assert!(blink.visible());

		blink.advance(start + CURSOR_BLINK_INTERVAL);
		assert!(!blink.visible(), "the first idle phase hides the cursor");
		blink.advance(start + CURSOR_BLINK_INTERVAL * 2);
		assert!(blink.visible(), "the next phase restores the cursor");

		let activity = start + CURSOR_BLINK_INTERVAL * 3;
		blink.note_activity(activity);
		assert!(
			blink.visible(),
			"activity immediately restores a solid cursor"
		);
		blink.advance(activity + CURSOR_BLINK_INTERVAL - iced::time::Duration::from_millis(1));
		assert!(
			blink.visible(),
			"the cursor stays solid for a complete interval after activity"
		);

		blink.set_window_focused(false, activity + CURSOR_BLINK_INTERVAL);
		blink.advance(activity + CURSOR_BLINK_INTERVAL * 2);
		assert!(
			blink.visible(),
			"an unfocused window keeps a steady location marker"
		);
		blink.set_window_focused(true, activity + CURSOR_BLINK_INTERVAL * 2);
		assert!(blink.visible(), "refocusing restarts the solid phase");
	}

	#[test]
	fn minimap_ink_uses_per_column_token_colors() {
		use super::{MinimapPixels, MinimapWidget};
		use libaryon::core::DocLine;
		use libaryon::core::Rgb;
		use libaryon::engine::{
			EMPTY_PREVIEW_BIN, MinimapContentRevision, MinimapOverlay, MinimapSnapshot,
			PREVIEW_BIN_COLUMNS, PreviewRow, PreviewSnapshot, UiPalette,
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
		let snapshot = MinimapSnapshot::Preview(PreviewSnapshot::new(
			MinimapOverlay::new(DocLine::ZERO, 2, DocLine::ZERO),
			MinimapContentRevision::ZERO,
			8,
			rows,
		));
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));
		theme_colors[TokenCategory::Function as usize] = Some(Rgb::new(0, 255, 0));
		let widget = MinimapWidget {
			pane_id: libaryon::core::PaneId::new(1),
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
	fn minimap_dense_ink_preserves_short_line_widths() {
		use super::{MinimapPixels, MinimapWidget};
		use libaryon::core::{DocLine, Rgb};
		use libaryon::engine::{
			MinimapContentRevision, MinimapOverlay, MinimapSnapshot, PREVIEW_BIN_COLUMNS,
			PreviewRow, PreviewSnapshot, UiPalette,
		};
		use libaryon::svp::rust_highlight::{CATEGORY_COUNT, TokenCategory};

		let keyword = libaryon::core::TokenBin::new(TokenCategory::Keyword as u8);
		let rows = (0..400)
			.map(|_| PreviewRow {
				logical_width: 8,
				bins: vec![keyword; PREVIEW_BIN_COLUMNS].into_boxed_slice(),
			})
			.collect();
		let snapshot = MinimapSnapshot::Preview(PreviewSnapshot::new(
			MinimapOverlay::new(DocLine::ZERO, 20, DocLine::ZERO),
			MinimapContentRevision::ZERO,
			32,
			rows,
		));
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));
		let widget = MinimapWidget {
			pane_id: libaryon::core::PaneId::new(1),
			snapshot: &snapshot,
			theme_colors,
			palette: UiPalette::default(),
		};

		let base = widget.palette.minimap_surface;
		let mut image = MinimapPixels::new(64, 64, base);
		widget.render_ink(&mut image, 64, widget.content_height(64));
		let pixel = |x: u32, y: u32| {
			let idx = ((y * 64 + x) * 4) as usize;
			(image.rgba[idx], image.rgba[idx + 1], image.rgba[idx + 2])
		};

		assert_ne!(pixel(8, 32), (base.r, base.g, base.b));
		assert_eq!(
			pixel(32, 32),
			(base.r, base.g, base.b),
			"quarter-width rows must not become full-width rainbow bands"
		);
	}

	#[test]
	fn minimap_realistic_dense_raster_reuses_its_handle() {
		use super::{MinimapInkCache, MinimapWidget};
		use libaryon::core::{DocLine, Rgb};
		use libaryon::engine::{
			MinimapContentRevision, MinimapOverlay, MinimapSnapshot, PREVIEW_BIN_COLUMNS,
			PreviewRow, PreviewSnapshot, UiPalette,
		};
		use libaryon::svp::rust_highlight::{CATEGORY_COUNT, TokenCategory};

		let keyword = libaryon::core::TokenBin::new(TokenCategory::Keyword as u8);
		let rows = (0..5_900)
			.map(|_| PreviewRow {
				logical_width: 80,
				bins: vec![keyword; PREVIEW_BIN_COLUMNS].into_boxed_slice(),
			})
			.collect();
		let snapshot = MinimapSnapshot::Preview(PreviewSnapshot::new(
			MinimapOverlay::new(DocLine::ZERO, 40, DocLine::ZERO),
			MinimapContentRevision::ZERO,
			120,
			rows,
		));
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));
		let widget = MinimapWidget {
			pane_id: libaryon::core::PaneId::new(1),
			snapshot: &snapshot,
			theme_colors,
			palette: UiPalette::default(),
		};
		let cache = MinimapInkCache::default();

		let first = widget.ink_handle(&cache, 160, 4_000);
		let second = widget.ink_handle(&cache, 160, 4_000);
		assert_eq!(first.id(), second.id());
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
	fn execution_snapshot_crosses_edits_still_waiting_on_the_display_cache() {
		let original = "select 1 as x";
		let expected = "select 1 as x union all select 2";
		let mut editor = CodeEditor::new(original);
		wait_for_content(&mut editor, original);

		// Each inserted character asks for an eventually-consistent display
		// snapshot. Request Run immediately, before pumping any of those
		// replies, to reproduce edit-then-Ctrl-Enter.
		send_keys(&mut editor, "A union all select 2".chars());
		let request = editor
			.request_execution_snapshot()
			.expect("execution snapshot request");
		let deadline = Instant::now() + Duration::from_secs(10);
		let snapshot = loop {
			editor.poll();
			if let Some(snapshot) = editor.take_execution_snapshot(request) {
				break snapshot;
			}
			assert!(
				Instant::now() < deadline,
				"execution snapshot did not complete"
			);
			thread::sleep(Duration::from_millis(5));
		};

		assert_eq!(snapshot.buffer_id, editor.active_buffer_id().unwrap());
		assert_eq!(snapshot.text, expected);
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
	fn cached_content_stays_attributed_when_focus_changes_between_languages() {
		let sql = "select 1 as x, 2 as y";
		let mut editor = CodeEditor::new(sql);
		wait_for_content(&mut editor, sql);
		let sql_pane = editor.viewport.as_ref().unwrap().focused_pane_id;
		let sql_buffer = editor.active_buffer_id().expect("SQL buffer");

		send_keys(&mut editor, ":pv rune".chars());
		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Enter,
			modifiers: Modifiers::NONE,
		}));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.active_language_id().map(LanguageId::as_str) != Some("rune")
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let rune_pane = editor.viewport.as_ref().unwrap().focused_pane_id;
		let rune_buffer = editor.active_buffer_id().expect("Rune buffer");
		assert_ne!(sql_buffer, rune_buffer);

		send_keys(&mut editor, "i40 + 2".chars());
		wait_for_content(&mut editor, "40 + 2");

		// Reproduce the GUI race: focus changes and its viewport is consumed,
		// but no new document-text query has completed yet. Language and text
		// must still come from the same SQL buffer.
		editor.send(EditorCommand::FocusPane(sql_pane));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.active_buffer_id() != Some(sql_buffer) && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(
			editor.active_language_id().map(LanguageId::as_str),
			Some("sql")
		);
		assert_eq!(editor.cached_content(), sql);

		editor.send(EditorCommand::FocusPane(rune_pane));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.active_buffer_id() != Some(rune_buffer) && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(
			editor.active_language_id().map(LanguageId::as_str),
			Some("rune")
		);
		assert_eq!(editor.cached_content(), "40 + 2");
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
	fn projected_tabs_keep_compact_labels_and_distinct_active_style() {
		use super::{tab_button_style, tab_label};
		use iced::widget::button;
		use libaryon::core::{BufferId, ViewId};
		use libaryon::uast::projection::DocTabSummary;

		let clean = DocTabSummary {
			buffer_id: BufferId::new(1),
			view_id: ViewId::new(1),
			title: "query.sql".to_owned(),
			is_dirty: false,
			snapshot: None,
		};
		let dirty = DocTabSummary {
			is_dirty: true,
			..clean.clone()
		};
		let unnamed = DocTabSummary {
			title: "_".to_owned(),
			..clean.clone()
		};
		assert_eq!(tab_label(&clean), "query.sql");
		assert_eq!(tab_label(&dirty), "query.sql +");
		assert_eq!(tab_label(&unnamed), "_");

		let palette = libaryon::engine::UiPalette::default();
		let active = tab_button_style(true, true, palette, button::Status::Active);
		let inactive = tab_button_style(false, true, palette, button::Status::Active);
		assert_eq!(active.text_color, super::color(palette.tab_accent));
		assert_eq!(
			active.background,
			Some(iced::Background::Color(super::color(
				palette.tab_active_surface
			)))
		);
		assert_eq!(inactive.text_color, super::color(palette.tab_text));
		assert_eq!(
			inactive.background,
			Some(iced::Background::Color(super::color(palette.tab_surface)))
		);
	}

	#[test]
	fn projected_tab_controls_activate_and_close_exact_views() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		let pane_id = editor.viewport.as_ref().unwrap().focused_pane_id;
		let first_view = editor
			.viewport
			.as_ref()
			.unwrap()
			.focused_pane()
			.unwrap()
			.tabs[0]
			.view_id;

		let _ = editor.update(EditorMsg::PaneTabAction {
			pane_id,
			action: PaneTabAction::NewTab,
		});
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.tabs.len() != 2)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let second_view = {
			let pane = editor.viewport.as_ref().unwrap().focused_pane().unwrap();
			assert_eq!(pane.active_doctab_index, 1);
			pane.tabs[1].view_id
		};

		let _ = editor.update(EditorMsg::ActivateDocTab {
			pane_id,
			view_id: first_view,
		});
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.tabs[pane.active_doctab_index].view_id != first_view)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let pane = editor.viewport.as_ref().unwrap().focused_pane().unwrap();
		assert_eq!(pane.tabs[pane.active_doctab_index].view_id, first_view);

		let _ = editor.update(EditorMsg::CloseDocTab {
			pane_id,
			view_id: second_view,
		});
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.is_none_or(|pane| pane.tabs.len() != 1)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let pane = editor.viewport.as_ref().unwrap().focused_pane().unwrap();
		assert_eq!(pane.tabs.len(), 1);
		assert_eq!(pane.tabs[0].view_id, first_view);
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
		assert_eq!(
			prompt_cursor_byte(viewport),
			Some(2),
			"the frontend prefix shifts libaryon's command-local insertion index"
		);

		let _ = editor.update(EditorMsg::Input(InputEvent::Key {
			code: KeyCode::Left,
			modifiers: Modifiers::NONE,
		}));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|viewport| viewport.input.prompt_cursor != 0)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let viewport = editor.viewport.as_ref().expect("viewport");
		assert_eq!(prompt_cursor_byte(viewport), Some(1));
	}

	#[test]
	fn command_bar_cursor_marks_unicode_and_preserves_end_width_while_blinking() {
		let palette = UiPalette::default();
		let prompt = ":edit café.rs";
		let cursor_byte = ":edit caf".len();
		let visible = command_bar_fragments(prompt.to_owned(), Some(cursor_byte), true, palette);
		assert_eq!(
			visible
				.iter()
				.map(|fragment| fragment.text.as_str())
				.collect::<String>(),
			prompt
		);
		let cursor = visible
			.iter()
			.find(|fragment| fragment.background.is_some())
			.expect("visible prompt cursor");
		assert_eq!(cursor.text, "é");
		assert_eq!(cursor.background, Some(super::color(palette.accent)));

		let hidden = command_bar_fragments(prompt.to_owned(), Some(prompt.len()), false, palette);
		assert!(
			hidden.iter().all(|fragment| fragment.background.is_none()),
			"the hidden blink phase must not paint a cursor"
		);
		assert_eq!(
			hidden
				.iter()
				.map(|fragment| fragment.text.as_str())
				.collect::<String>(),
			format!("{prompt} "),
			"an end cursor reserves one cell in both blink phases"
		);
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
			placement: HostPanelPlacement::Split {
				direction: SplitDirection::Vertical,
			},
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

	#[cfg(feature = "toron")]
	#[test]
	fn rune_view_collection_tiles_one_forward_producer_group_without_tabs() {
		use crate::rune_relation_envelope::{LocalViewIdentity, ViewContentKind, ViewPresentation};

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		let source_pane = editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.expect("source pane")
			.pane_id;
		let producer = editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.expect("source pane")
			.buffer_id;
		let views = [
			(
				LocalViewIdentity::Named("first-data".to_owned()),
				ViewPresentation {
					content_kind: ViewContentKind::Table,
					display_label: "table".to_owned(),
					authored_title: crate::plot::spec::Presence::Inherit,
				},
			),
			(
				LocalViewIdentity::Named("overlay".to_owned()),
				ViewPresentation {
					content_kind: ViewContentKind::Plot,
					display_label: "Line + Scatter".to_owned(),
					authored_title: crate::plot::spec::Presence::Inherit,
				},
			),
			(
				LocalViewIdentity::Named("second-data".to_owned()),
				ViewPresentation {
					content_kind: ViewContentKind::Table,
					display_label: "table".to_owned(),
					authored_title: crate::plot::spec::Presence::Inherit,
				},
			),
			(
				LocalViewIdentity::Named("totals".to_owned()),
				ViewPresentation {
					content_kind: ViewContentKind::Plot,
					display_label: "plot.bar".to_owned(),
					authored_title: crate::plot::spec::Presence::Inherit,
				},
			),
		];
		editor.prepare_rune_view_panels(producer, &views, 1);

		let expected = ["first-data", "overlay", "second-data", "totals"];
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			viewport
				.panes
				.iter()
				.filter(|pane| pane.host_panel.is_some())
				.count() < expected.len()
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}

		let viewport = editor.viewport.as_ref().expect("view collection projected");
		assert_eq!(viewport.panes.len(), 5, "four views require four new panes");
		assert_eq!(
			viewport
				.focused_pane()
				.expect("source focus retained")
				.pane_id,
			source_pane
		);
		let view_panes = viewport
			.panes
			.iter()
			.filter(|pane| pane.host_panel.is_some())
			.collect::<Vec<_>>();
		assert_eq!(
			view_panes
				.iter()
				.filter_map(|pane| pane.host_panel.as_ref())
				.map(|panel| panel.title.as_str())
				.collect::<Vec<_>>(),
			expected
		);
		assert!(
			view_panes.iter().all(|pane| pane.tabs.len() == 1),
			"independent views must not be hidden in inferred tab stacks"
		);
		assert_eq!(
			view_panes
				.iter()
				.map(|pane| pane.rect.screen_row.get())
				.collect::<std::collections::HashSet<_>>()
				.len(),
			2,
			"four members tile as two visible rows"
		);
		assert!(view_panes[1].rect.width.get() > view_panes[0].rect.width.get());
		assert!(view_panes[3].rect.width.get() > view_panes[2].rect.width.get());
		let first_projection = view_panes
			.iter()
			.map(|pane| {
				(
					pane.pane_id,
					pane.rect,
					pane.tabs.iter().map(|tab| tab.view_id).collect::<Vec<_>>(),
					pane.active_doctab_index,
				)
			})
			.collect::<Vec<_>>();
		assert!(editor.view_group_ids.contains_key(&producer));
		assert_eq!(
			editor.rune_view_group_for_pane(source_pane),
			editor.view_group_ids.get(&producer).copied()
		);
		let first_panel = editor
			.rune_view_panel_id(producer, &views[0].0)
			.expect("first view panel");
		editor.fail_rune_view(first_panel);
		editor.mark_rune_view_producer_stale(producer);
		assert_eq!(
			editor
				.viewport
				.as_ref()
				.expect("completion-local projection")
				.panes
				.iter()
				.filter(|pane| pane.host_panel.is_some())
				.map(|pane| (
					pane.pane_id,
					pane.rect,
					pane.tabs.iter().map(|tab| tab.view_id).collect::<Vec<_>>(),
					pane.active_doctab_index,
				))
				.collect::<Vec<_>>(),
			first_projection,
			"failure and stale completion state emit no placement"
		);
		let _ = editor.update(EditorMsg::RetileOutputs {
			pane_id: source_pane,
		});
		for _ in 0..20 {
			editor.poll();
			thread::sleep(Duration::from_millis(2));
		}
		assert_eq!(
			editor
				.viewport
				.as_ref()
				.expect("retile no-op projection")
				.panes
				.iter()
				.filter(|pane| pane.host_panel.is_some())
				.map(|pane| (
					pane.pane_id,
					pane.rect,
					pane.tabs.iter().map(|tab| tab.view_id).collect::<Vec<_>>(),
					pane.active_doctab_index,
				))
				.collect::<Vec<_>>(),
			first_projection,
			"Retile outputs is exact on an already-current Automatic group"
		);

		editor.prepare_rune_view_panels(producer, &views, 2);
		let deadline = Instant::now() + Duration::from_secs(10);
		while Instant::now() < deadline {
			editor.poll();
			let current = editor
				.viewport
				.as_ref()
				.expect("rerun projection")
				.panes
				.iter()
				.filter(|pane| pane.host_panel.is_some())
				.map(|pane| {
					(
						pane.pane_id,
						pane.rect,
						pane.tabs.iter().map(|tab| tab.view_id).collect::<Vec<_>>(),
						pane.active_doctab_index,
					)
				})
				.collect::<Vec<_>>();
			if current == first_projection {
				break;
			}
			thread::sleep(Duration::from_millis(5));
		}
		let viewport = editor.viewport.as_ref().expect("stable rerun");
		assert_eq!(viewport.focused_pane_id, source_pane);
		assert_eq!(
			viewport
				.panes
				.iter()
				.filter(|pane| pane.host_panel.is_some())
				.map(|pane| (
					pane.pane_id,
					pane.rect,
					pane.tabs.iter().map(|tab| tab.view_id).collect::<Vec<_>>(),
					pane.active_doctab_index,
				))
				.collect::<Vec<_>>(),
			first_projection,
			"unchanged reruns preserve identity, geometry, tabs, and activation"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn rune_view_groups_are_distinct_and_one_producer_does_not_retile_another() {
		use crate::rune_relation_envelope::{LocalViewIdentity, ViewContentKind, ViewPresentation};

		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		let first_producer = editor.active_buffer_id().expect("first producer");
		let first_views = [
			(
				LocalViewIdentity::Named("a-table".to_owned()),
				ViewPresentation {
					content_kind: ViewContentKind::Table,
					display_label: "table".to_owned(),
					authored_title: crate::plot::spec::Presence::Inherit,
				},
			),
			(
				LocalViewIdentity::Named("a-plot".to_owned()),
				ViewPresentation {
					content_kind: ViewContentKind::Plot,
					display_label: "plot.line".to_owned(),
					authored_title: crate::plot::spec::Presence::Inherit,
				},
			),
		];
		editor.prepare_rune_view_panels(first_producer, &first_views, 1);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			viewport
				.panes
				.iter()
				.filter(|pane| pane.host_panel.is_some())
				.count() < 2
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let first_panels = first_views
			.iter()
			.map(|(identity, _)| editor.rune_view_panel_id(first_producer, identity).unwrap())
			.collect::<std::collections::HashSet<_>>();
		let first_layout = editor
			.viewport
			.as_ref()
			.unwrap()
			.panes
			.iter()
			.filter_map(|pane| {
				pane.host_panel
					.as_ref()
					.filter(|panel| first_panels.contains(&panel.panel_id))
					.map(|_| (pane.pane_id, pane.rect))
			})
			.collect::<Vec<_>>();

		editor.send(EditorCommand::NewPane {
			direction: SplitDirection::Horizontal,
			language: Some("rune".to_owned()),
		});
		let deadline = Instant::now() + Duration::from_secs(10);
		let mut second_producer = None;
		while Instant::now() < deadline {
			editor.poll();
			if let Some(candidate) = editor.active_buffer_id()
				&& candidate != first_producer
			{
				second_producer = Some(candidate);
				break;
			}
			thread::sleep(Duration::from_millis(5));
		}
		let second_producer = second_producer.expect("second producer");
		let second_views = [(
			LocalViewIdentity::Named("b-table".to_owned()),
			ViewPresentation {
				content_kind: ViewContentKind::Table,
				display_label: "table".to_owned(),
				authored_title: crate::plot::spec::Presence::Inherit,
			},
		)];
		editor.prepare_rune_view_panels(second_producer, &second_views, 1);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			viewport
				.panes
				.iter()
				.filter(|pane| pane.host_panel.is_some())
				.count() < 3
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_ne!(
			editor.view_group_ids[&first_producer],
			editor.view_group_ids[&second_producer]
		);
		assert_eq!(
			editor
				.viewport
				.as_ref()
				.unwrap()
				.panes
				.iter()
				.filter_map(|pane| {
					pane.host_panel
						.as_ref()
						.filter(|panel| first_panels.contains(&panel.panel_id))
						.map(|_| (pane.pane_id, pane.rect))
				})
				.collect::<Vec<_>>(),
			first_layout,
			"establishing a sibling producer preserves the first group"
		);
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
			placement: HostPanelPlacement::Split {
				direction: SplitDirection::Vertical,
			},
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
			placement: HostPanelPlacement::Split {
				direction: SplitDirection::Vertical,
			},
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

		// Non-row commands must complete the pane too; otherwise a notebook
		// `output=pane` CREATE/INSERT would leave it stuck on `running…`.
		editor.begin_query();
		editor.set_query_message("Rows affected: 3".to_owned());
		let PanelContent::Message(message) = &editor.panels.panels[&RESULT_PANEL].content else {
			panic!("a completed non-row query replaces the running panel state");
		};
		assert_eq!(message, "Rows affected: 3");
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0023_completed_table_artifacts_replace_and_stale_as_atomic_pairs() {
		use libaryon::engine::HostPanelId;
		use libaryon::uast::projection::MarkdownCellIdentity;

		let alternate_skin = TableSkin {
			body_background: TableRgba::opaque(9, 8, 7),
			..TableSkin::DEFAULT
		};
		let first = polars::df!("n" => [1_i64]).expect("first frame");
		let second = polars::df!("n" => [2_i64, 3]).expect("second frame");
		let assert_pair = |content: &PanelContent, first_value, height, skin| {
			let PanelContent::Table(artifact) = content else {
				panic!("completed table content must retain one table artifact")
			};
			assert_eq!(artifact.frame.height(), height);
			assert_eq!(
				artifact
					.frame
					.column("n")
					.expect("n column")
					.i64()
					.expect("i64 column")
					.get(0),
				Some(first_value)
			);
			assert_eq!(artifact.skin, skin);
		};

		let mut editor = CodeEditor::new("select 1;");
		editor.set_query_result_with_skin(first.clone(), TableSkin::DEFAULT);
		assert_pair(
			&editor.panels.panels[&super::RESULT_PANEL].content,
			1,
			1,
			TableSkin::DEFAULT,
		);
		editor.set_query_result_with_skin(second.clone(), alternate_skin);
		assert_pair(
			&editor.panels.panels[&super::RESULT_PANEL].content,
			2,
			2,
			alternate_skin,
		);
		editor.panels.fail_or_stale(super::RESULT_PANEL);
		assert_pair(
			&editor.panels.panels[&super::RESULT_PANEL].content,
			2,
			2,
			alternate_skin,
		);
		assert_eq!(
			editor.panels.panels[&super::RESULT_PANEL].status,
			PanelStatus::Stale
		);

		let rune_panel = HostPanelId::new(99);
		editor.set_rune_view_table_with_skin(rune_panel, first.clone(), TableSkin::DEFAULT);
		editor.set_rune_view_table_with_skin(rune_panel, second.clone(), alternate_skin);
		editor.panels.fail_or_stale(rune_panel);
		assert_pair(
			&editor.panels.panels[&rune_panel].content,
			2,
			2,
			alternate_skin,
		);
		assert_eq!(editor.panels.panels[&rune_panel].status, PanelStatus::Stale);

		let identity = MarkdownCellIdentity::Explicit {
			id: "table-skin".to_owned(),
		};
		editor.set_markdown_cell_table_with_skin(identity.clone(), first, TableSkin::DEFAULT);
		editor.set_markdown_cell_table_with_skin(identity.clone(), second, alternate_skin);
		let Some(NotebookCellOutput::Table(artifact)) = editor.notebook_outputs.get(&identity)
		else {
			panic!("markdown completion must retain one table artifact")
		};
		assert_eq!(artifact.frame.height(), 2);
		assert_eq!(artifact.skin, alternate_skin);
	}

	#[test]
	fn product_0023_skin_only_replacement_does_not_mutate_host_panel_layout() {
		let mut editor = CodeEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.begin_query();
		wait_for_host_panel(&mut editor);
		let frame = polars::df!("n" => [1_i64]).expect("frame");
		editor.set_query_result_with_skin(frame.clone(), TableSkin::DEFAULT);

		let panel_snapshot = |editor: &CodeEditor| {
			let viewport = editor.viewport.as_ref().expect("viewport");
			let pane = viewport
				.panes
				.iter()
				.find(|pane| {
					pane.host_panel
						.as_ref()
						.is_some_and(|panel| panel.panel_id == super::RESULT_PANEL)
				})
				.expect("result panel");
			(
				viewport.focused_pane_id,
				pane.pane_id,
				pane.rect,
				pane.host_panel.clone(),
				pane.tabs.clone(),
				pane.active_doctab_index,
			)
		};
		let before = panel_snapshot(&editor);
		let alternate_skin = TableSkin {
			body_background: TableRgba::opaque(9, 8, 7),
			..TableSkin::DEFAULT
		};
		editor.set_query_result_with_skin(frame, alternate_skin);
		let after = panel_snapshot(&editor);
		assert_eq!(before, after);
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
			matches!(
				editor_input_for(&colon, event::Status::Ignored),
				Some(EditorMsg::Input(InputEvent::Key {
					code: KeyCode::Char(':'),
					modifiers: Modifiers::SHIFT,
				}))
			),
			"an ordinary unconsumed key retains its exact mapping"
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
	fn committed_unicode_text_uses_the_modal_safe_paste_path() {
		use super::editor_input_for;
		use iced::{Event, event, keyboard};

		fn key_event(modified_key: keyboard::Key, text: Option<&str>) -> Event {
			Event::Keyboard(keyboard::Event::KeyPressed {
				key: modified_key.clone(),
				modified_key,
				physical_key: keyboard::key::Physical::Unidentified(
					keyboard::key::NativeCode::Unidentified,
				),
				location: keyboard::Location::Standard,
				modifiers: keyboard::Modifiers::empty(),
				text: text.map(Into::into),
				repeat: false,
			})
		}

		let emoji = key_event(keyboard::Key::Unidentified, Some("👩‍💻"));
		assert!(matches!(
			editor_input_for(&emoji, event::Status::Ignored),
			Some(EditorMsg::Input(InputEvent::Paste(text))) if text == "👩‍💻"
		));

		let replacement_key = key_event(keyboard::Key::Character("�".into()), Some("🚀"));
		assert!(matches!(
			editor_input_for(&replacement_key, event::Status::Ignored),
			Some(EditorMsg::Input(InputEvent::Paste(text))) if text == "🚀"
		));

		let composed = key_event(keyboard::Key::Character("👩‍💻".into()), None);
		assert!(matches!(
			editor_input_for(&composed, event::Status::Ignored),
			Some(EditorMsg::Input(InputEvent::Paste(text))) if text == "👩‍💻"
		));

		let ime_commit = Event::InputMethod(iced::advanced::input_method::Event::Commit(
			"📈🚀".to_owned(),
		));
		assert!(matches!(
			editor_input_for(&ime_commit, event::Status::Ignored),
			Some(EditorMsg::Input(InputEvent::Paste(text))) if text == "📈🚀"
		));
		assert!(
			editor_input_for(&ime_commit, event::Status::Captured).is_none(),
			"IME commits captured by application chrome must not edit the document"
		);
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
	fn process_exit_detach_never_joins_the_engine() {
		let mut editor = CodeEditor::new("select 1;");
		let commands = editor.commands.clone();
		let started = Instant::now();

		editor.detach_for_process_exit();

		assert!(
			started.elapsed() < Duration::from_millis(100),
			"process-final teardown must not wait for engine workers"
		);
		assert!(
			editor.engine.is_none(),
			"the ordinary Drop path must have no engine left to join"
		);

		// This test process continues after the GUI-style detach, unlike the
		// real application process. Retire its engine asynchronously so it does
		// not leave a live worker behind for the rest of the suite.
		let _ = commands.send(EditorCommand::Quit);
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
	fn viz_and_noviz_project_distinct_whitespace_without_changing_text() {
		let source = "\tlet value = 1; \t\n\u{00a0}";
		let mut editor = CodeEditor::new(source);
		wait_for_content(&mut editor, source);

		editor.send(EditorCommand::SetVisibleWhitespace(false));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.status_message() != Some("Whitespace hidden") && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.status_message(), Some("Whitespace hidden"));
		let hidden = rendered_lines(&editor);
		assert!(hidden[0].starts_with("    let value = 1;"));
		assert!(
			hidden
				.iter()
				.all(|line| !line.contains(['▸', '␣', '~', '¬', '⍽']))
		);

		editor.send(EditorCommand::SetVisibleWhitespace(true));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.status_message() != Some("Whitespace visible") && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.status_message(), Some("Whitespace visible"));
		let visible = rendered_lines(&editor);
		assert!(
			visible[0].starts_with("▸   let␣value␣=␣1;"),
			"leading tabs and interior spaces should be distinct: {visible:?}"
		);
		assert!(
			visible[0].contains('~'),
			"trailing spaces and tabs should be unmistakable: {visible:?}"
		);
		assert!(visible[0].ends_with('¬'), "line endings should be visible");
		assert!(
			visible[1].contains('⍽'),
			"NBSP must not masquerade as an ordinary space"
		);
		let viewport = editor.viewport.as_ref().expect("viewport");
		let pane = viewport.focused_pane().expect("focused pane");
		let cursor_color = super::color(viewport.ui_palette.accent);
		let cursor_cells = super::project_lines(
			pane,
			viewport.ui_palette.editor_text,
			viewport.ui_palette.accent,
			true,
		)
		.iter()
		.flat_map(|line| &line.fragments)
		.filter(|fragment| fragment.background == Some(cursor_color))
		.map(|fragment| fragment.text.chars().count())
		.sum::<usize>();
		assert_eq!(
			cursor_cells, 1,
			"a tab keeps its visual width but the cursor paints one cell"
		);
		assert_eq!(
			editor.cached_content(),
			source,
			"projection is display-only"
		);
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
	fn committed_emoji_survive_the_full_render_pipeline() {
		let mut editor = CodeEditor::new("");
		wait_for_content(&mut editor, "");
		send_keys(&mut editor, ['i']);
		let _ = editor.update(EditorMsg::Input(InputEvent::Paste("👩‍💻📈".to_owned())));
		wait_for_content(&mut editor, "👩‍💻📈");
		let deadline = Instant::now() + Duration::from_secs(10);
		while !rendered_lines(&editor)
			.iter()
			.any(|line| line.contains("👩‍💻📈"))
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let lines = rendered_lines(&editor);
		assert!(
			lines.iter().any(|line| line.contains("👩‍💻📈")),
			"committed emoji must reach the rendered projection intact, got: {lines:?}"
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
	fn initial_file_keeps_real_path_content_and_detected_language() {
		let path = std::env::temp_dir().join(format!(
			"polariton-initial-file-{}-{}.rs",
			std::process::id(),
			std::thread::current().name().unwrap_or("test")
		));
		let source = "pub fn opened_from_cli() -> bool { true }\n";
		std::fs::write(&path, source).expect("write initial file fixture");

		let mut editor = CodeEditor::open_initial_file(path.to_string_lossy().into_owned());
		wait_for_content(&mut editor, source);
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.active_language_id().map(LanguageId::as_str) != Some("rust")
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.cached_content(), source);
		assert_eq!(
			editor.active_language_id().map(LanguageId::as_str),
			Some("rust")
		);
		let pane = editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.expect("focused initial-file pane");
		assert_eq!(
			pane.file_name.as_deref(),
			Some(path.to_string_lossy().as_ref())
		);

		editor.shutdown();
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn large_extent_projects_only_the_viewports_lines() {
		let path = std::env::temp_dir().join(format!(
			"polariton-large-projection-{}-{}.txt",
			std::process::id(),
			std::thread::current().name().unwrap_or("test")
		));
		let source = (0..500)
			.map(|line| format!("line{line:03}\n"))
			.collect::<String>();
		std::fs::write(&path, &source).expect("write large projection fixture");

		let mut editor = CodeEditor::open_initial_file(path.to_string_lossy().into_owned());
		wait_for_content(&mut editor, &source);
		let deadline = Instant::now() + Duration::from_secs(10);
		let rendered = loop {
			let rendered = rendered_lines(&editor);
			if rendered
				.first()
				.is_some_and(|line| line.contains("line000"))
				|| Instant::now() >= deadline
			{
				break rendered;
			}
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		};
		assert!(
			rendered.len() <= INITIAL_ROWS as usize,
			"a viewport must not build widgets for all {} document lines",
			source.lines().count()
		);
		assert!(
			rendered
				.first()
				.is_some_and(|line| line.contains("line000")),
			"unexpected projected viewport: {rendered:?}"
		);
		assert!(!rendered.iter().any(|line| line.contains("line499")));

		editor.shutdown();
		let _ = std::fs::remove_file(path);
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

		let pane_id = editor
			.viewport
			.as_ref()
			.and_then(|viewport| viewport.focused_pane())
			.map(|pane| pane.pane_id)
			.expect("focused minimap pane");
		let _ = editor.update(EditorMsg::MinimapGoto {
			pane_id,
			line: DocLine::new(40),
		});
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

		editor.send(EditorCommand::SplitPane(SplitDirection::Horizontal));
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor
			.viewport
			.as_ref()
			.is_none_or(|viewport| viewport.panes.len() < 2 || viewport.focused_pane_id == pane_id)
			&& Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_ne!(
			editor
				.viewport
				.as_ref()
				.expect("split viewport")
				.focused_pane_id,
			pane_id,
			"the split should leave another pane focused before the minimap click"
		);

		let _ = editor.update(EditorMsg::MinimapGoto {
			pane_id,
			line: DocLine::new(10),
		});
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.viewport.as_ref().is_none_or(|viewport| {
			viewport.focused_pane_id != pane_id
				|| viewport
					.panes
					.iter()
					.find(|pane| pane.pane_id == pane_id)
					.is_none_or(|pane| pane.viewport_start_line.get() > 15)
		}) && Instant::now() < deadline
		{
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		let viewport = editor.viewport.as_ref().expect("minimap viewport");
		assert_eq!(
			viewport.focused_pane_id, pane_id,
			"a minimap click must focus the pane that owns the minimap"
		);
		assert!(
			viewport
				.panes
				.iter()
				.find(|pane| pane.pane_id == pane_id)
				.is_some_and(|pane| pane.viewport_start_line.get() <= 15),
			"the scroll command must be applied to the clicked pane"
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
	fn wrapped_text_keeps_emoji_sequences_atomic_and_uses_their_width() {
		use super::{Color, ProjectedLine, StyledFragment, wrap_line_at_starts};

		let line = ProjectedLine {
			doc_line: 8,
			fragments: vec![StyledFragment {
				text: "a☠️b".to_owned(),
				foreground: Color::WHITE,
				background: None,
				underline: false,
			}],
			show_gutter: true,
		};
		let wrapped = wrap_line_at_starts(line, &[0, 3]);
		assert_eq!(wrapped.len(), 2);
		assert_eq!(wrapped[0].fragments[0].text, "a☠️");
		assert_eq!(wrapped[1].fragments[0].text, "b");
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
	fn file_browser_omits_editor_gutter_and_minimap() {
		let root = navigation_fixture("file-browser-chrome");
		std::fs::create_dir_all(&root).expect("create browser fixture");
		std::fs::write(root.join("main.rs"), "fn main() {}\n").expect("write browser fixture");

		{
			let mut editor = CodeEditor::new("select 1;");
			wait_for_content(&mut editor, "select 1;");
			editor.send(EditorCommand::OpenFileBrowserAt(
				root.to_string_lossy().into_owned(),
			));
			wait_for_file_browser(&mut editor);

			let browser = editor
				.viewport
				.as_ref()
				.expect("browser viewport")
				.panes
				.iter()
				.find(|pane| pane.is_file_browser)
				.expect("browser pane");
			assert!(browser.gutter_numbering.is_none());
			assert!(browser.gutter_line_numbers.is_empty());
			assert_eq!(pane_gutter_width(browser), 0.0);
			assert!(
				browser.minimap.is_none(),
				"file tree keeps its width for paths even when editor minimaps are enabled"
			);
		}

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn double_clicking_file_browser_row_opens_the_file() {
		let root = navigation_fixture("file-browser-double-click");
		std::fs::create_dir_all(&root).expect("create browser fixture");
		let file = root.join("main.rs");
		std::fs::write(&file, "fn main() {}\n").expect("write browser fixture");
		let expected = file.to_string_lossy().into_owned();

		{
			let mut editor = CodeEditor::new("select 1;");
			wait_for_content(&mut editor, "select 1;");
			editor.send(EditorCommand::OpenFileBrowserAt(
				root.to_string_lossy().into_owned(),
			));
			wait_for_file_browser(&mut editor);

			let (browser_pane, point) = {
				let browser = editor
					.viewport
					.as_ref()
					.expect("browser viewport")
					.panes
					.iter()
					.find(|pane| pane.is_file_browser)
					.expect("browser pane");
				(
					browser.pane_id,
					iced::Point::new(
						browser.rect.screen_col.get() as f32 * metrics::CELL_WIDTH
							+ metrics::CELL_WIDTH / 2.0,
						browser.rect.screen_row.get() as f32 * metrics::LINE_HEIGHT
							+ PANE_HEADER_HEIGHT + metrics::LINE_HEIGHT / 2.0,
					),
				)
			};
			let _ = editor.update(EditorMsg::PointerMoved(point));
			let _ = editor.update(EditorMsg::Click);
			let _ = editor.update(EditorMsg::Click);

			let deadline = Instant::now() + Duration::from_secs(10);
			while editor.viewport.as_ref().is_none_or(|viewport| {
				let focused = viewport.focused_pane();
				focused.is_none_or(|pane| {
					pane.is_file_browser || pane.file_name.as_deref() != Some(expected.as_str())
				})
			}) && Instant::now() < deadline
			{
				editor.poll();
				thread::sleep(Duration::from_millis(5));
			}
			let viewport = editor.viewport.as_ref().expect("opened file viewport");
			let focused = viewport.focused_pane().expect("focused editor pane");
			assert!(!focused.is_file_browser);
			assert_eq!(focused.file_name.as_deref(), Some(expected.as_str()));
			assert!(
				viewport
					.pane(browser_pane)
					.is_some_and(|pane| pane.is_file_browser),
				"opening a tree entry keeps the browser sidebar alive"
			);
		}

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn git_status_omits_editor_chrome_and_double_click_opens_the_file() {
		let repo = navigation_fixture("git-status");
		std::fs::create_dir_all(&repo).expect("create Git fixture");
		let initialized = std::process::Command::new("git")
			.args(["init", "--quiet"])
			.current_dir(&repo)
			.status()
			.expect("run git init");
		assert!(initialized.success(), "git init failed");
		let file = repo.join("untracked.txt");
		std::fs::write(&file, "untracked\n").expect("write Git fixture");
		let source = repo.join("main.rs");
		std::fs::write(&source, "fn main() {}\n").expect("write source fixture");
		let expected = file.to_string_lossy().into_owned();

		{
			let mut editor = CodeEditor::open_initial_file(source.to_string_lossy().into_owned());
			wait_for_content(&mut editor, "fn main() {}\n");
			editor.send(EditorCommand::GitStatus);

			let deadline = Instant::now() + Duration::from_secs(10);
			while editor.viewport.as_ref().is_none_or(|viewport| {
				!viewport.panes.iter().any(|pane| {
					pane.is_git_status_panel
						&& String::from_utf8_lossy(
							&pane
								.tokens
								.iter()
								.flat_map(|token| token.text.iter().copied())
								.collect::<Vec<_>>(),
						)
						.contains("?? untracked.txt")
				})
			}) && Instant::now() < deadline
			{
				editor.poll();
				thread::sleep(Duration::from_millis(5));
			}

			let (git_pane, point) = {
				let pane = editor
					.viewport
					.as_ref()
					.expect("Git status viewport")
					.panes
					.iter()
					.find(|pane| pane.is_git_status_panel)
					.expect("Git status pane");
				let text = String::from_utf8_lossy(
					&pane
						.tokens
						.iter()
						.flat_map(|token| token.text.iter().copied())
						.collect::<Vec<_>>(),
				)
				.into_owned();
				let line = text
					.lines()
					.position(|line| line == "?? untracked.txt")
					.expect("untracked Git row") as u64;
				let visible_row = pane
					.visible_rows
					.iter()
					.position(|row| row.line.get() == line)
					.expect("visible untracked Git row");
				assert!(pane.gutter_numbering.is_none());
				assert_eq!(pane_gutter_width(pane), 0.0);
				assert!(pane.minimap.is_none());
				(
					pane.pane_id,
					iced::Point::new(
						pane.rect.screen_col.get() as f32 * metrics::CELL_WIDTH
							+ metrics::CELL_WIDTH / 2.0,
						pane.rect.screen_row.get() as f32 * metrics::LINE_HEIGHT
							+ PANE_HEADER_HEIGHT + (visible_row as f32 + 0.5) * metrics::LINE_HEIGHT,
					),
				)
			};
			let _ = editor.update(EditorMsg::PointerMoved(point));
			let _ = editor.update(EditorMsg::Click);
			let _ = editor.update(EditorMsg::Click);

			let deadline = Instant::now() + Duration::from_secs(10);
			while editor.viewport.as_ref().is_none_or(|viewport| {
				viewport.focused_pane().is_none_or(|pane| {
					pane.is_git_status_panel || pane.file_name.as_deref() != Some(expected.as_str())
				})
			}) && Instant::now() < deadline
			{
				editor.poll();
				thread::sleep(Duration::from_millis(5));
			}
			let viewport = editor.viewport.as_ref().expect("opened Git entry viewport");
			assert_eq!(
				viewport
					.focused_pane()
					.expect("focused editor pane")
					.file_name
					.as_deref(),
				Some(expected.as_str())
			);
			assert!(
				viewport
					.pane(git_pane)
					.is_some_and(|pane| pane.is_git_status_panel),
				"opening a Git entry keeps the status pane alive"
			);
		}

		let _ = std::fs::remove_dir_all(repo);
	}

	#[test]
	fn git_observer_updates_an_open_status_pane() {
		let repo = navigation_fixture("git-refresh");
		std::fs::create_dir_all(&repo).expect("create Git fixture");
		let initialized = std::process::Command::new("git")
			.args(["init", "--quiet"])
			.current_dir(&repo)
			.status()
			.expect("run git init");
		assert!(initialized.success(), "git init failed");
		let source = repo.join("main.rs");
		std::fs::write(&source, "fn main() {}\n").expect("write source fixture");
		std::fs::create_dir_all(repo.join("logs")).expect("create ignored-content directory");
		std::fs::write(repo.join(".gitignore"), "*.log\n").expect("write ignore fixture");
		std::fs::write(repo.join("logs/current.log"), "ignored\n").expect("write ignored fixture");

		{
			let mut editor = CodeEditor::open_initial_file(source.to_string_lossy().into_owned());
			wait_for_content(&mut editor, "fn main() {}\n");
			editor.send(EditorCommand::GitStatus);

			let pane_text = |editor: &CodeEditor| {
				editor
					.viewport
					.as_ref()
					.and_then(|viewport| {
						viewport.panes.iter().find(|pane| pane.is_git_status_panel)
					})
					.map(|pane| {
						String::from_utf8_lossy(
							&pane
								.tokens
								.iter()
								.flat_map(|token| token.text.iter().copied())
								.collect::<Vec<_>>(),
						)
						.into_owned()
					})
			};
			let deadline = Instant::now() + Duration::from_secs(10);
			while pane_text(&editor).is_none_or(|text| !text.contains("?? main.rs"))
				&& Instant::now() < deadline
			{
				editor.poll();
				thread::sleep(Duration::from_millis(5));
			}
			editor.send(EditorCommand::SetGitRefresh(
				libaryon::engine::GitRefreshMode::Watch,
			));
			let deadline = Instant::now() + Duration::from_secs(10);
			while editor.viewport.as_ref().is_none_or(|viewport| {
				viewport.git_refresh_mode != libaryon::engine::GitRefreshMode::Watch
			}) && Instant::now() < deadline
			{
				editor.poll();
				thread::sleep(Duration::from_millis(5));
			}

			std::fs::write(repo.join("logs/README.md"), "dirty\n").expect("dirty Git fixture");
			let deadline = Instant::now() + Duration::from_secs(10);
			while pane_text(&editor).is_none_or(|text| !text.contains("logs/"))
				&& Instant::now() < deadline
			{
				editor.poll();
				thread::sleep(Duration::from_millis(5));
			}
			assert!(
				pane_text(&editor).is_some_and(|text| text.contains("logs/")),
				"an all-ignored-content directory must observe its first relevant file"
			);
		}

		let _ = std::fs::remove_dir_all(repo);
	}

	#[test]
	fn local_mouse_coordinates_become_engine_cursor_positions() {
		use super::{EditorMsg, PANE_HEADER_HEIGHT};
		use iced::Point;

		let mut editor = CodeEditor::new("select 1;\nselect 2;\nselect 3;");
		wait_for_content(&mut editor, "select 1;\nselect 2;\nselect 3;");
		let _ = editor.update(EditorMsg::PointerMoved(Point::new(
			54.0 + 9.6 * 3.0,
			PANE_HEADER_HEIGHT + 22.0,
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

	/// Iced charges a column's whole inter-row gap budget against the available
	/// height before it measures a single row, so a budget that can reach the
	/// pane's height leaves nothing to lay out in and the preview renders
	/// empty. Row gaps are per-row padding now, but the row budget must stay
	/// proportional to the pane for the same reason: gap total below pane
	/// height, at every pane size.
	#[test]
	fn preview_row_gaps_never_consume_the_pane_height() {
		for viewport_lines in [1_u32, 8, 40, 120, 400] {
			let gap_total = preview_row_budget(viewport_lines) as f32 * PREVIEW_ROW_GAP;
			let pane_height = viewport_lines as f32 * metrics::LINE_HEIGHT;
			assert!(
				gap_total < pane_height,
				"{viewport_lines} lines: {gap_total}px of gaps in a {pane_height}px pane"
			);
		}
	}

	/// A long document must still render its visible window: the budget is
	/// counted from the anchor line, not from the top of the document.
	#[test]
	fn preview_row_budget_covers_more_than_the_source_window() {
		let viewport_lines = 40_u32;
		let budget = preview_row_budget(viewport_lines);
		assert!(
			budget >= u64::from(viewport_lines),
			"the preview must not show fewer lines than the source pane"
		);
		assert_eq!(preview_row_budget(0), 0, "an empty pane renders no rows");
	}

	/// The preview draws the window its source pane shows. A block that begins
	/// above the anchor but reaches into view is part of that window.
	#[test]
	fn the_visible_window_follows_the_source_anchor() {
		let source = "# One\n\npara one\n\n# Two\n\npara two\n\n# Three\n\npara three\n";
		let blocks = parse_markdown_blocks(source);
		let headings = |window: std::ops::Range<usize>| -> Vec<String> {
			visible_blocks(&blocks, window)
				.filter_map(|block| match block {
					MarkdownBlock::Heading { text, .. } => Some(text.clone()),
					_ => None,
				})
				.collect()
		};

		// Two source lines of budget: one viewport line, doubled.
		assert_eq!(headings(preview_source_window(source, 0, 1)), vec!["One"]);
		assert_eq!(headings(preview_source_window(source, 4, 1)), vec!["Two"]);
		assert_eq!(
			headings(preview_source_window(source, 0, 100)),
			vec!["One", "Two", "Three"],
			"a budget past the end of the document still stops at the end"
		);
	}

	/// Scrolling into the middle of a long paragraph must not blank it: the
	/// block starts above the anchor but is still on screen.
	#[test]
	fn a_block_straddling_the_window_start_is_drawn() {
		let source = "first line of a paragraph\nsecond line of it\nthird line of it\n\nlater\n";
		let blocks = parse_markdown_blocks(source);

		let visible: Vec<String> = visible_blocks(&blocks, preview_source_window(source, 1, 1))
			.map(|block| match block {
				MarkdownBlock::Paragraph { text, .. } => text.plain_text(),
				other => panic!("expected paragraphs, got {other:?}"),
			})
			.collect();
		assert_eq!(
			visible,
			vec!["first line of a paragraph second line of it third line of it".to_string()],
			"the paragraph the anchor sits inside must still be drawn"
		);
	}

	/// The preview and the source pane must color the same construct the same
	/// way, which they do by reading the same theme category rather than by
	/// keeping a second palette in sync.
	#[test]
	fn preview_colors_come_from_the_panes_own_theme_categories() {
		let mut theme_colors = [None; libaryon::svp::rust_highlight::CATEGORY_COUNT];
		let heading = Rgb::new(10, 20, 30);
		let string = Rgb::new(40, 50, 60);
		let operator = Rgb::new(70, 80, 90);
		let number = Rgb::new(100, 110, 120);
		let comment = Rgb::new(130, 140, 150);
		let punctuation = Rgb::new(160, 170, 180);
		theme_colors[TokenCategory::Type as usize] = Some(heading);
		theme_colors[TokenCategory::String as usize] = Some(string);
		theme_colors[TokenCategory::Operator as usize] = Some(operator);
		theme_colors[TokenCategory::Number as usize] = Some(number);
		theme_colors[TokenCategory::Comment as usize] = Some(comment);
		theme_colors[TokenCategory::Punctuation as usize] = Some(punctuation);
		let palette = UiPalette::default();

		let colors = MarkdownPreviewColors::new(theme_colors, palette);

		assert_eq!(colors.heading, heading, "headings are Type, as on the left");
		assert_eq!(colors.inline_code, string);
		assert_eq!(colors.bullet, operator);
		assert_eq!(colors.ordered_marker, number);
		assert_eq!(colors.quote, comment);
		assert_eq!(colors.rule, punctuation);
		// Prose has no category on either side, so it stays at the default
		// foreground rather than picking up a color the document lacks.
		assert_eq!(colors.body, palette.editor_text);
	}

	/// The preview must not invent a color the source pane does not show for
	/// the same construct, in either direction.
	#[test]
	fn preview_colors_match_the_source_highlighter_category_for_category() {
		let mut theme_colors = [None; libaryon::svp::rust_highlight::CATEGORY_COUNT];
		for (index, slot) in theme_colors.iter_mut().enumerate() {
			*slot = Some(Rgb::new(index as u8, 0, 0));
		}
		let palette = UiPalette::default();

		let colors = MarkdownPreviewColors::new(theme_colors, palette);
		let source_color = |construct| {
			libaryon::svp::markdown_highlight::markdown_construct_color(
				construct,
				&theme_colors,
				palette.editor_text,
			)
		};

		assert_eq!(colors.heading, source_color(MarkdownConstruct::Heading));
		assert_eq!(
			colors.inline_code,
			source_color(MarkdownConstruct::InlineCode)
		);
		assert_eq!(colors.bullet, source_color(MarkdownConstruct::BulletMarker));
		assert_eq!(
			colors.ordered_marker,
			source_color(MarkdownConstruct::OrderedMarker)
		);
		assert_eq!(colors.quote, source_color(MarkdownConstruct::Quote));
		assert_eq!(colors.rule, source_color(MarkdownConstruct::Rule));
		assert_eq!(colors.body, source_color(MarkdownConstruct::Body));
	}

	/// A theme that leaves a category unset must not black out the text that
	/// would have used it. Everything falls back to the one default foreground
	/// the source pane also falls back to.
	#[test]
	fn preview_colors_fall_back_to_the_default_foreground() {
		let palette = UiPalette::default();

		let colors = MarkdownPreviewColors::new(
			[None; libaryon::svp::rust_highlight::CATEGORY_COUNT],
			palette,
		);

		for resolved in [
			colors.heading,
			colors.inline_code,
			colors.bullet,
			colors.ordered_marker,
			colors.quote,
			colors.rule,
			colors.body,
		] {
			assert_eq!(resolved, palette.editor_text);
		}
	}

	#[test]
	fn fence_bodies_start_after_their_info_line() {
		let backticks = "```rust\nfn main() {}\n```\n";
		assert_eq!(fence_body_start(backticks, &(0..backticks.len())), 8);

		let tildes = "~~~\ncode\n~~~\n";
		assert_eq!(fence_body_start(tildes, &(0..tildes.len())), 4);

		// An indented block has no info line, so its body is where it starts.
		let indented = "    code\n";
		assert_eq!(fence_body_start(indented, &(0..indented.len())), 0);
	}

	/// Parsing happens when a projection arrives, not per frame, so the cache
	/// must actually be populated for the preview to have anything to draw.
	#[test]
	fn preview_blocks_are_parsed_when_the_projection_arrives() {
		let markdown = "# Title\n\nsome **bold** text\n";
		let editor = notebook_with_preview(markdown);
		let preview = editor
			.viewport
			.as_ref()
			.unwrap()
			.panes
			.iter()
			.find_map(|pane| pane.markdown_preview.as_ref())
			.expect("preview");

		let cached = editor
			.preview_blocks
			.get(&preview.source_buffer_id)
			.expect("blocks parsed for the previewed buffer");
		assert!(matches!(
			cached.blocks.first(),
			Some(MarkdownBlock::Heading { level: 1, .. })
		));
		assert!(
			cached.blocks.iter().any(|block| matches!(
				block,
				MarkdownBlock::Paragraph { text, .. }
					if text.spans().iter().any(|span| span.style.strong)
			)),
			"emphasis reaches the host as style, not as literal asterisks"
		);
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
					.document_texts
					.get(&preview.source_buffer_id)
					.expect("source text cached")
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
		use crate::plot::{composition::PlotComposition, core::PlotType};
		use libaryon::uast::projection::MarkdownCellIdentity;

		let frame = polars::df!("x" => [1_i32, 2], "y" => [2_i32, 4]).expect("frame");
		let composition = PlotComposition::single(PlotType::Scatter);
		let kernel =
			crate::plot::factory::create_automatic_plot(composition.clone(), &frame, 600, 360)
				.expect("resolved plot");
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
			placement: HostPanelPlacement::Split {
				direction: SplitDirection::Vertical,
			},
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
		assert_eq!(cell.source_start_line, 4);
		assert!(matches!(
			cell.identity,
			libaryon::uast::projection::MarkdownCellIdentity::Explicit { ref id } if id == "trips"
		));
		let identity = cell.identity.clone();
		editor.clear_markdown_cell_artifact(&identity);
		assert!(
			!editor.notebook_outputs.contains_key(&identity),
			"SQL operational state belongs to the status bar, not inline output"
		);
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
