#[cfg(feature = "toron")]
use crate::adapters::common::DatabaseAdapter;
use crate::adapters::{
	common::{AdapterStage, ExecutionResult},
	driver::AdapterState,
};
use crate::configuration::{self, StartupData};
use crate::editor::{CodeEditor, MarkdownCellExecution};
use crate::fonts::DEJAVU_SANS_MONO;
#[cfg(all(test, feature = "toron"))]
use crate::gui::table_skin::TableSkin;
#[cfg(all(test, feature = "toron"))]
use crate::gui::table_skin::TableSkinProjectionError;
#[cfg(feature = "toron")]
use crate::gui::table_skin::{TableSkinCascade, TableSkinProjection, TableSkinSpec};
use crate::gui::{
	components,
	menu::MenuState,
	messages::{ExportFormat, Message},
};
#[cfg(feature = "toron")]
#[cfg(feature = "toron")]
use crate::plot::common::{PlotLayout, TickPlacementError};
#[cfg(feature = "toron")]
use crate::plot::core::PlotType;
use crate::plot::export::{AvifBackend, PngBackend, SvgBackend};
use crate::plot::factory::create_plot;
#[cfg(feature = "toron")]
use crate::plot::factory::{create_layered_plot, create_styled_plot};
#[cfg(feature = "toron")]
use crate::plot::grid::{PlotGrid, PlotGridKernel};
#[cfg(feature = "toron")]
use crate::plot::request::plan_with_categorical_palette;
#[cfg(feature = "toron")]
use crate::plot::request::{PlotRequest, PlotSource};
use crate::plot::request::{ResolvedPlot, plan};
#[cfg(feature = "toron")]
use crate::plot::spec::AuthoredPlotSpec;
#[cfg(feature = "toron")]
use crate::plot::spec::{
	CategoricalPaletteSelection, Channel, ColorIdentity, LayeredPlotSpec, Override, ScaleFamily,
	resolve_channels, resolve_layered_channels,
};
#[cfg(feature = "toron")]
use crate::plot::theme::PlotSettings;
use crate::plot::theme::PlotSettingsLayer;
use crate::result_registry::{ResultRegistry, parse_result_annotation};
use crate::rune_host::{self, RuneCapabilities};
#[cfg(feature = "toron")]
use crate::rune_relation_envelope::{
	DisposableRunePlotSink, PendingRunePresentation, PendingRuneView, RuneEnvelopeError,
	RunePlotBody, RunePlotDomainSpans, RunePlotMinorTickSpans, RunePlotTickStepSpans,
	RuneTableSink, RuneTableSkinIntent, RuneTableSkinProjection, RuneViewIntent, RuneViewScheduler,
	SpannedRuneViewIntent, StyledRunePlotSink, TransformedRuneRelation, TransformedRuneView,
};
use crate::rune_view::{LocalViewIdentity, ViewContentKind, ViewPresentation};
use iced::{Element, Size, Subscription, Task, application, event, keyboard, window};
use libaryon::core::{BufferId, SessionId};
#[cfg(feature = "toron")]
use libaryon::core::{SavedSession, SessionStore, Writable};
use libaryon::uast::projection::MarkdownCellOutputTarget;
#[cfg(feature = "toron")]
use polars::frame::DataFrame;
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "toron")]
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "toron")]
use tokio::sync::RwLock;

struct AppState {
	menu_state: MenuState,
	code_editor: CodeEditor,
	status_msg: String,
	status_error: String,
	status_df_size: Option<(usize, usize)>,
	status_time_elapsed: Option<Duration>,
	status_detail: Option<StatusDetail>,
	status_detail_open: bool,
	adapter_state: AdapterState,
	code_started: Instant,
	is_maximized: bool,
	showing_settings: bool,
	show_column_types: bool,
	window_size: Size,
	result_registry: ResultRegistry,
	rune_buffer_revisions: HashMap<BufferId, u64>,
	#[cfg(feature = "toron")]
	rune_view_scheduler: RuneViewScheduler,
	/// Session-local categorical continuation keyed by producer, semantic view,
	/// and the natural length of the selected ordered palette.
	#[cfg(feature = "toron")]
	rune_categorical_identities: HashMap<(BufferId, LocalViewIdentity, usize), ColorIdentity>,
	#[cfg(feature = "toron")]
	local_creation: crate::local_workspace::CreationGrant,
	#[cfg(feature = "toron")]
	local_workspace_owner: Option<crate::local_workspace::LocalWorkspaceOwner>,
	#[cfg(feature = "toron")]
	closing_local_workspace: bool,
	pending_runs: VecDeque<PendingRun>,
	session_id: Option<SessionId>,
	saved_close_state: SavedCloseState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SavedCloseState {
	Open,
	Failed,
	Frozen,
}

struct StatusDetail {
	summary: String,
	full: String,
	content: iced::widget::text_editor::Content,
}

impl StatusDetail {
	fn new(summary: String, full: String) -> Self {
		let content = iced::widget::text_editor::Content::with_text(&full);
		Self {
			summary,
			full,
			content,
		}
	}
}

impl Drop for AppState {
	fn drop(&mut self) {
		// The application state is process-final. Do not turn teardown of
		// rust-analyzer or another language worker into visible shutdown
		// latency; Baryon's terminal frontend follows the same ownership rule.
		self.code_editor.detach_for_process_exit();
	}
}

#[derive(Clone, Copy)]
enum PendingRunKind {
	Buffer,
	Snippet,
}

struct PendingRun {
	snapshot_request: u64,
	source_buffer: BufferId,
	language_id: libaryon::engine::LanguageId,
	kind: PendingRunKind,
}

#[cfg(feature = "toron")]
#[derive(Clone)]
enum PreparedRuneCategoricalPalette {
	Ready {
		selection: CategoricalPaletteSelection,
		identity: ColorIdentity,
	},
	Failed(String),
}

pub type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartupTarget {
	Ephemeral { initial_file: Option<String> },
	CreateSaved,
	OpenSaved(SessionId),
}

enum StartupAuthority {
	Ephemeral {
		initial_file: Option<String>,
	},
	#[cfg(feature = "toron")]
	Saved {
		session_id: SessionId,
		saved: SavedSession<Writable>,
	},
}

fn resolve_startup(target: StartupTarget) -> std::result::Result<StartupAuthority, String> {
	match target {
		StartupTarget::Ephemeral { initial_file } => {
			Ok(StartupAuthority::Ephemeral { initial_file })
		}
		#[cfg(feature = "toron")]
		StartupTarget::CreateSaved => {
			let saved = SessionStore::platform_default()
				.map_err(|error| format!("cannot locate the saved-session store: {error}"))?
				.create()
				.map_err(|error| format!("cannot create a saved session: {error}"))?;
			let session_id = saved.snapshot().session_id();
			Ok(StartupAuthority::Saved { session_id, saved })
		}
		#[cfg(feature = "toron")]
		StartupTarget::OpenSaved(session_id) => {
			let saved = SessionStore::platform_default()
				.map_err(|error| format!("cannot locate saved session {session_id}: {error}"))?
				.open_writable(session_id)
				.map_err(|error| format!("cannot open saved session {session_id}: {error}"))?;
			Ok(StartupAuthority::Saved { session_id, saved })
		}
		#[cfg(not(feature = "toron"))]
		StartupTarget::CreateSaved | StartupTarget::OpenSaved(_) => {
			Err("saved sessions require Polariton's `toron` feature".to_owned())
		}
	}
}

/// Build a planned Rune plot without blocking Iced's update loop. Keeping the
/// resolved product intact through the task preserves its composition and
/// owned snapshot until the destination surface receives it.
fn build_resolved_plot_task(
	resolved: ResolvedPlot,
	height: u32,
	on_ready: impl FnOnce(
		ResolvedPlot,
		std::sync::Arc<dyn crate::plot::common::PlotKernel + Send + Sync>,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			let spec = resolved.resolved_spec.clone();
			let data = resolved.data.as_ref().clone();
			let kernel =
				tokio::task::spawn_blocking(move || create_plot(&spec, &data, 1200, height))
					.await
					.expect("Rune plot build task panicked");
			(resolved, kernel)
		},
		move |(resolved, kernel)| on_ready(resolved, kernel),
	)
}

enum RenderedPlotExport {
	Svg(String),
	Png(PngBackend),
	Avif(AvifBackend),
}

struct PlotExportJob {
	filename: String,
	artifact: RenderedPlotExport,
}

impl PlotExportJob {
	fn write_to(self, root: &std::path::Path) {
		let path = root.join(self.filename);
		match self.artifact {
			RenderedPlotExport::Svg(svg) => {
				let _ = std::fs::write(path, svg);
			}
			RenderedPlotExport::Png(backend) => backend.save(&path),
			RenderedPlotExport::Avif(backend) => backend.save(&path),
		}
	}
}

fn render_plot_export(
	snapshot: crate::editor::HostPlotExport,
	format: ExportFormat,
	ordinal: usize,
) -> PlotExportJob {
	let width = snapshot.width;
	let height = snapshot.height;
	let bounds = iced::Rectangle {
		x: 0.0,
		y: 0.0,
		width,
		height,
	};
	let widget = crate::plot::common::PlotWidget {
		kernel: snapshot.kernel.as_ref(),
		title: snapshot.title,
		padding: 20.0,
		settings: snapshot.settings,
		settings_model: snapshot.settings_model,
		render_revision: snapshot.render_revision,
		resize_render_suspended: snapshot.resize_render_suspended,
		layer: crate::plot::common::PlotRenderLayer::Full,
	};
	match format {
		ExportFormat::SVG => {
			let mut backend = SvgBackend::new(width, height);
			widget.render(&mut backend, bounds);
			PlotExportJob {
				filename: format!("plot_{ordinal}.svg"),
				artifact: RenderedPlotExport::Svg(backend.finish()),
			}
		}
		ExportFormat::PNG => {
			let mut backend = PngBackend::new(width as u32, height as u32);
			widget.render(&mut backend, bounds);
			PlotExportJob {
				filename: format!("plot_{ordinal}.png"),
				artifact: RenderedPlotExport::Png(backend),
			}
		}
		ExportFormat::AVIF => {
			let mut backend = AvifBackend::new(width as u32, height as u32);
			widget.render(&mut backend, bounds);
			PlotExportJob {
				filename: format!("plot_{ordinal}.avif"),
				artifact: RenderedPlotExport::Avif(backend),
			}
		}
	}
}

#[cfg(feature = "toron")]
fn build_facet_resolved_plot_task(
	resolved: ResolvedPlot,
	source: Arc<crate::result_artifact::ResultArtifact>,
	facet: Option<String>,
	figure_grid: crate::plot::grid::FigureGridLayoutSettings,
	layer_settings: Option<crate::plot::spec::LayerSettings>,
	on_ready: impl FnOnce(
		ResolvedPlot,
		std::result::Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String>,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			let frame = match source.projection() {
				Ok(frame) => frame,
				Err(error) => return (resolved, Err(error.to_string())),
			};
			let stamped = crate::plot::spec::StampedResult {
				data: source,
				provenance: resolved.resolved_spec.provenance.clone(),
			};
			let spec = Arc::new(resolved.resolved_spec.clone());
			let result = tokio::task::spawn_blocking(move || {
				let grid = match facet {
					Some(facet) => PlotGrid::value_facet_derived(spec, &stamped, facet)
						.map_err(|error| error.to_string())?,
					None => PlotGrid::single(spec, frame),
				};
				match layer_settings {
					Some(settings) => PlotGridKernel::from_styled_grid_with_figure_grid(
						grid,
						figure_grid,
						settings,
						1200,
						720,
					),
					None => Ok(PlotGridKernel::from_grid_with_figure_grid(
						grid,
						figure_grid,
						1200,
						720,
					)),
				}
			})
			.await
			.map_err(|error| format!("facet plot build task panicked: {error}"))
			.and_then(|result| result)
			.map(|kernel| {
				Arc::new(kernel) as Arc<dyn crate::plot::common::PlotKernel + Send + Sync>
			});
			(resolved, result)
		},
		move |(resolved, result)| on_ready(resolved, result),
	)
}

#[cfg(feature = "toron")]
fn build_facet_layered_plot_task(
	specification: crate::plot::spec::LayeredPlotSpec,
	source: Arc<crate::result_artifact::ResultArtifact>,
	facet: Option<String>,
	figure_grid: crate::plot::grid::FigureGridLayoutSettings,
	on_ready: impl FnOnce(
		std::result::Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String>,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			let frame = match source.projection() {
				Ok(frame) => frame,
				Err(error) => return Err(error.to_string()),
			};
			tokio::task::spawn_blocking(move || {
				let stamped = crate::plot::spec::StampedResult {
					data: source,
					provenance: crate::plot::spec::PlotProvenance {
						source_name: "workspace relation".to_owned(),
						revision: None,
					},
				};
				let binding = resolve_layered_channels(&specification, frame.schema().as_ref())
					.map_err(|error| error.to_string())?;
				let resolved = crate::plot::spec::resolve_layered_domains(binding, &stamped)
					.map_err(|error| error.to_string())?;
				let body = Arc::new(resolved.layers.first().spec.clone());
				let grid = match facet {
					Some(facet) => PlotGrid::value_facet_derived(body, &stamped, facet)
						.map_err(|error| error.to_string())?,
					None => PlotGrid::single(body, Arc::clone(&frame)),
				};
				PlotGridKernel::from_grid_with_factory_and_figure_grid(
					grid,
					figure_grid,
					1200,
					720,
					|_, data| create_layered_plot(&specification, data, 1200, 720),
				)
				.map(|kernel| {
					Arc::new(kernel) as Arc<dyn crate::plot::common::PlotKernel + Send + Sync>
				})
			})
			.await
			.map_err(|error| format!("facet layered plot build task panicked: {error}"))
			.and_then(|result| result)
		},
		on_ready,
	)
}

fn subscription(state: &AppState) -> Subscription<Message> {
	let events = event::listen_with(|ev, _status, _window| match ev {
		event::Event::Keyboard(keyboard::Event::KeyPressed {
			key: keyboard::Key::Named(keyboard::key::Named::Enter),
			modifiers,
			..
		}) if modifiers.shift() || modifiers.control() => {
			if modifiers.shift() {
				Some(Message::RunSnippet)
			} else {
				Some(Message::Run)
			}
		}
		_ => None,
	});
	Subscription::batch([
		events,
		state.code_editor.subscription().map(Message::CodeEditEvent),
		window::resize_events().map(|(_, size)| Message::CodeEditResize(size)),
	])
}

pub fn run(startup_data: StartupData, target: StartupTarget) -> Result {
	let size = Size::new(startup_data.window_size.0, startup_data.window_size.1);
	let authority = resolve_startup(target)
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
	let initial = new_with_authority(startup_data, authority)
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
	let initial = std::sync::Mutex::new(Some(initial));
	let initialize = move || {
		initial
			.lock()
			.expect("Polariton boot state lock poisoned")
			.take()
			.expect("Polariton boot state may be consumed exactly once")
	};
	application(initialize, update, view)
		.subscription(subscription)
		.theme(components::theme())
		.title("Polariton")
		// Canvas geometry (plot marks, gridlines, legend swatches) is
		// tessellated triangles; without MSAA every curved edge is aliased
		// hard enough to read as rasterization. Text is unaffected either way.
		.antialiasing(true)
		.font(DEJAVU_SANS_MONO)
		.window(window::Settings {
			decorations: false,
			maximized: false,
			min_size: Some(Size::new(1280.0, 720.0)),
			position: window::Position::Centered,
			resizable: true,
			size,
			transparent: false,
			..Default::default()
		})
		.run()?;
	Ok(())
}

#[cfg(test)]
fn new(startup_data: StartupData, initial_file: Option<String>) -> (AppState, Task<Message>) {
	new_with_authority(startup_data, StartupAuthority::Ephemeral { initial_file })
		.expect("ephemeral startup cannot refuse")
}

fn new_with_authority(
	startup_data: StartupData,
	authority: StartupAuthority,
) -> std::result::Result<(AppState, Task<Message>), String> {
	let window_size = Size::new(startup_data.window_size.0, startup_data.window_size.1);
	let init_error = startup_data.init_error.clone();
	let init_summary = init_error
		.as_ref()
		.map(|_| "init.rn · configuration failed".to_owned())
		.unwrap_or_default();
	let init_detail = init_error.map(|error| StatusDetail::new(init_summary.clone(), error));
	let init_script = configuration::default_init_path();
	let pane_divider_reservation = startup_data.pane_divider_reservation;
	let workspace_size = workspace_bounds(window_size).size();
	let (mut code_editor, session_id) = match authority {
		StartupAuthority::Ephemeral { initial_file } => (
			match initial_file {
				Some(path) => CodeEditor::open_initial_file_with_init(
					path,
					pane_divider_reservation,
					init_script,
					workspace_size,
				),
				None => CodeEditor::new_with_init(
					"",
					pane_divider_reservation,
					init_script,
					workspace_size,
				),
			},
			None,
		),
		#[cfg(feature = "toron")]
		StartupAuthority::Saved { session_id, saved } => (
			CodeEditor::open_saved_with_init(
				saved,
				pane_divider_reservation,
				init_script,
				workspace_size,
			)
			.map_err(|error| format!("cannot restore saved session {session_id}: {error}"))?,
			Some(session_id),
		),
	};
	code_editor.set_show_column_types(startup_data.show_column_types);
	let mut state = AppState {
		menu_state: MenuState::default(),
		code_editor,
		status_msg: "".to_string(),
		status_error: init_summary,
		status_df_size: None,
		status_time_elapsed: None,
		status_detail: init_detail,
		status_detail_open: false,
		adapter_state: AdapterState::default(),
		code_started: Instant::now(),
		is_maximized: false,
		showing_settings: false,
		show_column_types: startup_data.show_column_types,
		window_size,
		result_registry: ResultRegistry::default(),
		rune_buffer_revisions: HashMap::new(),
		#[cfg(feature = "toron")]
		rune_view_scheduler: RuneViewScheduler::default(),
		#[cfg(feature = "toron")]
		rune_categorical_identities: HashMap::new(),
		#[cfg(feature = "toron")]
		local_creation: crate::local_workspace::CreationGrant::default(),
		#[cfg(feature = "toron")]
		local_workspace_owner: None,
		#[cfg(feature = "toron")]
		closing_local_workspace: false,
		pending_runs: VecDeque::new(),
		session_id,
		saved_close_state: SavedCloseState::Open,
	};
	sync_workspace_viewport(&mut state);
	// Eagerly open the session scratch database so SQL always has a target;
	// an in-memory SQLite open is effectively instant.
	let scratch = Task::perform(AdapterState::open_scratch(), Message::ScratchReady);
	#[cfg(feature = "toron")]
	{
		let discovery = Task::perform(
			nilket_local::discover(None),
			Message::LocalWorkspaceDiscovered,
		);
		Ok((state, Task::batch([scratch, discovery])))
	}
	#[cfg(not(feature = "toron"))]
	{
		Ok((state, scratch))
	}
}

fn view(app_state: &AppState) -> Element<'_, Message> {
	let status_detail = app_state.status_detail.as_ref().and_then(|detail| {
		let visible_summary = if app_state.status_error.is_empty() {
			&app_state.status_msg
		} else {
			&app_state.status_error
		};
		(detail.summary == *visible_summary).then_some(detail)
	});
	components::main_screen(
		&app_state.menu_state,
		&app_state.code_editor,
		&app_state.status_msg,
		&app_state.status_error,
		app_state.status_df_size,
		app_state.status_time_elapsed,
		app_state.code_editor.busy_spinner_phase(),
		&app_state.adapter_state,
		app_state.showing_settings,
		app_state.show_column_types,
		status_detail.map(|detail| (detail.full.as_str(), &detail.content)),
		app_state.status_detail_open,
		#[cfg(feature = "toron")]
		app_state.local_creation.offer(),
		#[cfg(not(feature = "toron"))]
		None,
		app_state.session_id.is_some(),
		app_state.saved_close_state == SavedCloseState::Failed,
	)
}

fn session_identity_text(session_id: SessionId) -> String {
	session_id.to_string()
}

fn compact_sql_diagnostic(message: &str, source_start_line: Option<usize>) -> (String, String) {
	let mut body = message.trim();
	if let Some((prefix, rest)) = body.split_once(" parse error: ")
		&& !prefix.contains('\n')
	{
		body = rest;
	}
	if let Some(rest) = body.strip_prefix("sql parser error: ") {
		body = rest;
	}

	let mut parser_line = None;
	let mut parser_column = None;
	let mut core = body;
	if let Some((before, location)) = body.rsplit_once(" at Line: ")
		&& let Some((line, column)) = location.split_once(", Column: ")
	{
		parser_line = line.trim().parse::<usize>().ok();
		parser_column = column
			.split_whitespace()
			.next()
			.and_then(|column| column.parse::<usize>().ok());
		core = before;
	}

	let core = core.split_whitespace().collect::<Vec<_>>().join(" ");
	let core = if let Some(rest) = core.strip_prefix("Expected: ") {
		if let Some((expected, found)) = rest.split_once(", found: ") {
			format!("expected {expected}, found '{}'", found.trim_matches('\''))
		} else {
			format!("expected {rest}")
		}
	} else {
		core
	};
	let location = match (parser_line, parser_column) {
		(Some(line), Some(column)) => {
			let line = source_start_line
				.map(|source| source.saturating_add(line.saturating_sub(1)))
				.unwrap_or(line);
			format!("line {line}, col {column} · ")
		}
		(Some(line), None) => {
			let line = source_start_line
				.map(|source| source.saturating_add(line.saturating_sub(1)))
				.unwrap_or(line);
			format!("line {line} · ")
		}
		_ => String::new(),
	};
	let summary = format!("SQL · {location}{core}");
	let full = if source_start_line.is_some() && parser_line.is_some() {
		format!("{summary}\n\nAdapter diagnostic:\n{message}")
	} else {
		message.to_owned()
	};
	(summary, full)
}

fn set_sql_status(app_state: &mut AppState, summary: impl Into<String>) {
	app_state.status_msg = summary.into();
	app_state.status_error.clear();
	app_state.status_detail = None;
	app_state.status_detail_open = false;
}

fn set_sql_error(app_state: &mut AppState, message: String, source_start_line: Option<usize>) {
	let (summary, full) = compact_sql_diagnostic(&message, source_start_line);
	app_state.status_msg.clear();
	app_state.status_error = summary.clone();
	app_state.status_detail = Some(StatusDetail::new(summary, full));
	app_state.status_detail_open = false;
}

fn compact_rune_diagnostic(message: &str) -> (String, String) {
	let lines = message
		.lines()
		.map(str::trim)
		.filter(|line| !line.is_empty())
		.collect::<Vec<_>>();
	let first = lines.first().copied().unwrap_or("unknown failure");
	let first = first.split_whitespace().collect::<Vec<_>>().join(" ");
	let first = first.strip_prefix("Error: ").unwrap_or(&first);
	let first = if let Some(rest) = first.strip_prefix("Rune view syntax at bytes ")
		&& let Some((span, diagnostic)) = rest.split_once(": ")
	{
		format!("bytes {span} · {diagnostic}")
	} else if let Some(rest) = first.strip_prefix("Rune view syntax: ") {
		rest.to_owned()
	} else {
		first.to_owned()
	};
	let more = lines.len().saturating_sub(1);
	let summary = if more == 0 {
		format!("Rune · {first}")
	} else {
		format!(
			"Rune · {first} · {more} more diagnostic{}",
			if more == 1 { "" } else { "s" }
		)
	};
	(summary, message.to_owned())
}

fn set_rune_error(app_state: &mut AppState, message: String) {
	let (summary, full) = compact_rune_diagnostic(&message);
	app_state.status_msg.clear();
	app_state.status_error = summary.clone();
	app_state.status_detail = Some(StatusDetail::new(summary, full));
	app_state.status_detail_open = false;
}

fn clear_transient_execution_status_on_editor_activity(
	app_state: &mut AppState,
	message: &crate::editor::EditorMsg,
) {
	let is_activity = matches!(
		message,
		crate::editor::EditorMsg::ActivateDocTab { .. }
			| crate::editor::EditorMsg::Click
			| crate::editor::EditorMsg::CloseDocTab { .. }
			| crate::editor::EditorMsg::Input(_)
			| crate::editor::EditorMsg::MinimapGoto { .. }
			| crate::editor::EditorMsg::Paste(_)
			| crate::editor::EditorMsg::Scroll(_)
	);
	if is_activity
		&& (app_state.status_msg.starts_with("SQL · ")
			|| app_state.status_error.starts_with("SQL · ")
			|| app_state.status_msg.starts_with("Rune · ")
			|| app_state.status_error.starts_with("Rune · "))
	{
		app_state.status_msg.clear();
		app_state.status_error.clear();
		app_state.status_detail = None;
		app_state.status_detail_open = false;
	}
}

fn run_code(
	app_state: &mut AppState,
	source_buffer: BufferId,
	code: String,
	annotation_source: &str,
) -> Task<Message> {
	let Some(db) = app_state.adapter_state.sql_connection() else {
		set_sql_error(
			app_state,
			"No SQL target: scratch database unavailable.".to_owned(),
			None,
		);
		return Task::none();
	};
	if code.trim().is_empty() {
		set_sql_status(app_state, "SQL · nothing to run on the current line");
		return Task::none();
	}
	let publish_name = match parse_result_annotation(annotation_source) {
		Ok(name) => name,
		Err(message) => {
			set_sql_error(app_state, message, None);
			return Task::none();
		}
	};
	let db = db.clone();
	let connection = app_state.adapter_state.connection_identity();
	let publish = publish_name.map(|name| {
		app_state
			.result_registry
			.begin(&name, source_buffer, connection)
	});
	set_sql_status(app_state, "SQL · running…");
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	Task::perform(
		async move {
			let mut guard = db.write().await;
			guard.dispatch(&code).await
		},
		move |result| Message::RunResult { result, publish },
	)
}

fn run_language_code(
	app_state: &mut AppState,
	source_buffer: BufferId,
	language_id: libaryon::engine::LanguageId,
	code: String,
	annotation_source: &str,
) -> Task<Message> {
	match language_id.as_str() {
		"sql" => run_code(app_state, source_buffer, code, annotation_source),
		"rune" => run_rune_code(app_state, source_buffer, code),
		language => {
			app_state.status_msg = format!("No executor is registered for language: {language}");
			Task::none()
		}
	}
}

fn run_rune_code(app_state: &mut AppState, source_buffer: BufferId, code: String) -> Task<Message> {
	if code.trim().is_empty() {
		app_state.status_msg = "Nothing to run in the current Rune buffer.".into();
		return Task::none();
	}
	let revision = *app_state
		.rune_buffer_revisions
		.entry(source_buffer)
		.and_modify(|revision| *revision = revision.saturating_add(1))
		.or_insert(1);
	#[cfg(feature = "toron")]
	{
		let work = PendingRunePresentation::new(source_buffer, revision, code);
		let Some(work) = app_state.rune_view_scheduler.submit_presentation(work) else {
			app_state.status_msg = "Rune presentation queued newest revision…".into();
			return Task::none();
		};
		return start_rune_presentation_task(app_state, work);
	}
	#[cfg(not(feature = "toron"))]
	return run_plain_rune_code(app_state, source_buffer, revision, code);
}

fn run_plain_rune_code(
	app_state: &mut AppState,
	source_buffer: BufferId,
	revision: u64,
	code: String,
) -> Task<Message> {
	let capabilities = RuneCapabilities {
		results: match app_state.result_registry.snapshot() {
			Ok(results) => results,
			Err(error) => {
				app_state.status_error = error.to_string();
				return Task::none();
			}
		},
		..RuneCapabilities::default()
	};
	app_state.status_msg = "Rune running…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				let mut execution = rune_host::execute_body(&code, capabilities)?;
				let plot = match execution.requests.len() {
					0 => None,
					1 => Some(plan(execution.requests.remove(0))?),
					count => {
						return Err(format!(
							"Rune buffer published {count} plots; one buffer currently owns one plot pane."
						));
					}
				};
				Ok(crate::gui::messages::RuneBufferOutcome {
					display: execution.display,
					plot,
				})
			})
			.await
			.map_err(|error| format!("Rune task panicked: {error}"))?
		},
		move |result| Message::RunRuneBufferResult {
			source_buffer,
			revision,
			result,
		},
	)
}

#[cfg(feature = "toron")]
fn start_rune_presentation_task(
	app_state: &mut AppState,
	work: PendingRunePresentation,
) -> Task<Message> {
	let source_buffer = work.source_buffer;
	let revision = work.revision;
	let source = work.source.clone();
	app_state.status_msg = "Rune presentation compiling…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				work.is_current()
					.then(|| crate::rune_view_compiler::compile(&work.source))
			})
			.await
			.unwrap_or_else(|error| {
				Some(Err(RuneEnvelopeError::Invalid(format!(
					"Rune presentation task panicked: {error}"
				))))
			})
		},
		move |result| Message::RunRunePresentationResult {
			source_buffer,
			revision,
			source,
			result,
		},
	)
}

#[cfg(feature = "toron")]
fn run_rune_relation_view_at_revision(
	app_state: &mut AppState,
	source_buffer: BufferId,
	revision: u64,
	view: TransformedRuneView,
) -> Task<Message> {
	let work = PendingRuneView {
		source_buffer,
		revision,
		view,
	};
	let presentation = work
		.view
		.presentation_views()
		.into_iter()
		.map(|view| (view.identity.clone(), view.intent.presentation()))
		.collect::<Vec<_>>();
	app_state
		.code_editor
		.prepare_rune_view_panels(source_buffer, &presentation, revision);
	let Some(work) = app_state.rune_view_scheduler.submit(work) else {
		app_state.status_msg = "Rune workspace queued newest view…".into();
		return Task::none();
	};
	start_rune_relation_task(app_state, work)
}

#[cfg(feature = "toron")]
fn start_rune_relation_task(app_state: &mut AppState, work: PendingRuneView) -> Task<Message> {
	let Some(connection) = app_state.adapter_state.rune_connection().cloned() else {
		let message = app_state.adapter_state.local_workspace_unavailable();
		app_state
			.code_editor
			.mark_rune_view_producer_stale(work.source_buffer);
		set_rune_error(app_state, message);
		app_state.rune_view_scheduler.cancel(work.source_buffer);
		return Task::none();
	};
	let PendingRuneView {
		source_buffer,
		revision,
		view,
	} = work;
	let relation_count = view.relation_count();
	app_state.status_msg = format!(
		"Rune workspace running {relation_count} independent quer{}…",
		if relation_count == 1 { "y" } else { "ies" }
	);
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	let tasks = view.into_relations().map(|view| {
		let connection = connection.clone();
		let origin = view.origin.clone();
		let palettes = prepare_rune_categorical_palettes(app_state, source_buffer, &view);
		let views = view
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		Task::perform(
			dispatch_rune_relation_with_palettes(connection, view, palettes),
			move |result| Message::RunRuneViewSetResult {
				source_buffer,
				revision,
				origin,
				views,
				result,
			},
		)
	});
	Task::batch(tasks)
}

#[cfg(feature = "toron")]
fn resolved_rune_categorical_selection(
	authored_settings: &PlotSettingsLayer,
) -> std::result::Result<CategoricalPaletteSelection, String> {
	let settings = PlotSettings {
		authored_spec: authored_settings.clone(),
		..PlotSettings::default()
	};
	let identity = settings.resolve_categorical_palette_identity();
	CategoricalPaletteSelection::from_theme_name(identity.as_str())
		.map_err(|error| error.to_string())
}

#[cfg(feature = "toron")]
fn resolve_rune_plot_skin(
	mut authored_settings: PlotSettingsLayer,
) -> std::result::Result<PlotSettingsLayer, String> {
	let settings = PlotSettings {
		authored_spec: authored_settings.clone(),
		..PlotSettings::default()
	};
	if let Some(selection) = settings.resolve_plot_skin() {
		authored_settings.resolved_skin = Override::Set(
			crate::plot::chrome::ResolvedPlotSkin::from_theme_name(
				selection.name.as_str(),
				selection.projection,
			)
			.map_err(|error| error.to_string())?,
		);
	}
	Ok(authored_settings)
}

#[cfg(feature = "toron")]
fn authored_rune_plot_spec(sink: &DisposableRunePlotSink) -> AuthoredPlotSpec {
	if let Some(fields) = sink.mark_fields.as_ref() {
		return match fields.to_mark_fields() {
			crate::plot::spec::MarkFields::BoxPlot(fields) => AuthoredPlotSpec::box_plot(fields),
			crate::plot::spec::MarkFields::Heatmap(fields) => AuthoredPlotSpec::heatmap(fields),
			crate::plot::spec::MarkFields::Bubble(fields) => AuthoredPlotSpec::bubble(fields),
			crate::plot::spec::MarkFields::Funnel(fields) => AuthoredPlotSpec::funnel(fields),
			crate::plot::spec::MarkFields::RadialDial(fields) => {
				AuthoredPlotSpec::radial_dial(fields)
			}
			crate::plot::spec::MarkFields::Pie(fields) => AuthoredPlotSpec::pie(fields),
			crate::plot::spec::MarkFields::Violin(fields) => AuthoredPlotSpec::violin(fields),
			crate::plot::spec::MarkFields::Radar(fields) => AuthoredPlotSpec::radar(fields),
			crate::plot::spec::MarkFields::Parallel(fields) => AuthoredPlotSpec::parallel(fields),
		};
	}
	if sink.composition.primary_plot_type() == PlotType::Histogram {
		return AuthoredPlotSpec::histogram(
			sink.x.clone(),
			sink.interval_end
				.clone()
				.expect("Rune histogram carries its explicit bin_end"),
			sink.y.clone(),
		);
	}
	AuthoredPlotSpec::xy_color(
		sink.composition.clone(),
		sink.x.clone(),
		sink.y.clone(),
		sink.category.clone(),
	)
}

#[cfg(feature = "toron")]
fn with_rune_continuous_intent(
	spec: AuthoredPlotSpec,
	intent: Option<crate::plot::continuous_color::ContinuousScaleIntent>,
) -> AuthoredPlotSpec {
	match intent {
		Some(intent) => spec.with_continuous_intent(intent),
		None => spec,
	}
}

#[cfg(feature = "toron")]
fn box_plot_failure_role(
	data: &DataFrame,
	fields: &crate::plot::spec::BoxPlotFields,
) -> Option<crate::plot::kernels::boxplot::BoxPlotFieldRole> {
	use crate::plot::kernels::boxplot::BoxPlotFieldRole;
	let authored = AuthoredPlotSpec::box_plot(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		for (role, field) in [
			(BoxPlotFieldRole::Category, &fields.category),
			(BoxPlotFieldRole::WhiskerLow, &fields.whisker_low),
			(BoxPlotFieldRole::Q1, &fields.q1),
			(BoxPlotFieldRole::Median, &fields.median),
			(BoxPlotFieldRole::Q3, &fields.q3),
			(BoxPlotFieldRole::WhiskerHigh, &fields.whisker_high),
			(BoxPlotFieldRole::Outlier, &fields.outlier),
		] {
			if error.field == *field || error.reason.contains(role.name()) {
				return Some(role);
			}
		}
		return None;
	}
	crate::plot::kernels::boxplot::prepare_box_plot_data(data, fields)
		.err()
		.and_then(|error| error.field_role())
}

#[cfg(feature = "toron")]
fn heatmap_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::HeatmapFields,
	origins: &crate::rune_relation_envelope::HeatmapFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::heatmap(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if error.field == fields.x {
			origins.x.clone()
		} else if error.field == fields.y {
			origins.y.clone()
		} else if error.field == fields.value {
			origins.value.clone()
		} else {
			mark.clone()
		};
	}
	match crate::plot::kernels::heatmap::prepare_heatmap_data(
		data,
		&fields.x,
		&fields.y,
		&fields.value,
	) {
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::NonFiniteValue { .. }) => {
			origins.value.clone()
		}
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::NullX { .. }) => {
			origins.x.clone()
		}
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::NullY { .. }) => {
			origins.y.clone()
		}
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::DuplicateCell { .. }) => {
			origins.keys()
		}
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn bubble_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::BubbleFields,
	origins: &crate::rune_relation_envelope::BubbleFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	use crate::plot::kernels::bubble::BubblePreparationError;
	use crate::plot::kernels::bubble_observations::BubbleObservationsError;
	use crate::plot::spec::{BubbleNumericField, BubblePreparationChannel};

	let role_for_field = |field: &str| {
		[
			(
				BubblePreparationChannel::X,
				fields.numeric.get(BubbleNumericField::X),
			),
			(
				BubblePreparationChannel::Y,
				fields.numeric.get(BubbleNumericField::Y),
			),
			(
				BubblePreparationChannel::Size,
				fields.numeric.get(BubbleNumericField::Size),
			),
			(
				BubblePreparationChannel::Color,
				fields.numeric.get(BubbleNumericField::Color),
			),
		]
		.into_iter()
		.find_map(|(role, candidate)| (candidate == field).then_some(role))
		.or_else(|| {
			fields
				.label
				.as_deref()
				.filter(|candidate| *candidate == field)
				.map(|_| BubblePreparationChannel::Label)
		})
	};
	let authored = AuthoredPlotSpec::bubble(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return role_for_field(&error.field)
			.and_then(|role| origins.for_role(role))
			.unwrap_or_else(|| mark.clone());
	}
	let result = crate::plot::kernels::bubble::prepare_bubble_data(data, fields);
	let role = match result {
		Err(
			BubblePreparationError::MissingColumn { channel, .. }
			| BubblePreparationError::UncastableColumn { channel, .. },
		) => Some(channel),
		Err(BubblePreparationError::Observations(
			BubbleObservationsError::NullValue { channel, .. }
			| BubbleObservationsError::NonFiniteValue { channel, .. },
		)) => Some(channel.into()),
		Ok(_) => None,
	};
	role.and_then(|role| origins.for_role(role))
		.unwrap_or_else(|| mark.clone())
}

#[cfg(feature = "toron")]
fn funnel_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::FunnelFields,
	origins: &crate::rune_relation_envelope::FunnelFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::funnel(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if error.field == fields.stage {
			origins.stage.clone()
		} else if error.field == fields.value {
			origins.value.clone()
		} else {
			mark.clone()
		};
	}
	match crate::plot::kernels::funnel::prepare_funnel_data(data, &fields.stage, &fields.value) {
		Err(
			crate::plot::kernels::funnel::FunnelPreparationError::MissingColumn {
				channel: crate::plot::kernels::funnel::FunnelPreparationChannel::Stage,
				..
			}
			| crate::plot::kernels::funnel::FunnelPreparationError::UncastableColumn {
				channel: crate::plot::kernels::funnel::FunnelPreparationChannel::Stage,
				..
			},
		) => origins.stage.clone(),
		Err(crate::plot::kernels::funnel::FunnelPreparationError::Stages(
			crate::plot::kernels::funnel_stages::FunnelStagesError::NullStage { .. }
			| crate::plot::kernels::funnel_stages::FunnelStagesError::DuplicateStage { .. },
		)) => origins.stage.clone(),
		Err(
			crate::plot::kernels::funnel::FunnelPreparationError::MissingColumn { .. }
			| crate::plot::kernels::funnel::FunnelPreparationError::UncastableColumn { .. }
			| crate::plot::kernels::funnel::FunnelPreparationError::Stages(_),
		) => origins.value.clone(),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn radial_dial_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::RadialDialFields,
	origins: &crate::rune_relation_envelope::RadialDialFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::radial_dial(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if fields.label.as_deref() == Some(error.field.as_str()) {
			origins.label.clone().unwrap_or_else(|| mark.clone())
		} else if error.field == fields.value {
			origins.value.clone()
		} else if error.field == fields.maximum {
			origins.maximum.clone()
		} else {
			mark.clone()
		};
	}
	use crate::plot::kernels::radial_dial::{
		RadialDialPreparationError, RadialDialPreparationField,
	};
	use crate::plot::kernels::radial_dial_readings::RadialDialReadingsError;
	match crate::plot::kernels::radial_dial::prepare_radial_dial_data(
		data,
		fields.label.as_deref(),
		&fields.value,
		&fields.maximum,
	) {
		Err(
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Category,
				..
			}
			| RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Category,
				..
			}
			| RadialDialPreparationError::Readings(
				RadialDialReadingsError::NullCategory { .. }
				| RadialDialReadingsError::DuplicateCategory { .. },
			),
		) => origins.label.clone().unwrap_or_else(|| mark.clone()),
		Err(
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Value,
				..
			}
			| RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Value,
				..
			}
			| RadialDialPreparationError::Readings(
				RadialDialReadingsError::NullValue { .. }
				| RadialDialReadingsError::InexactValue { .. }
				| RadialDialReadingsError::NonFiniteValue { .. }
				| RadialDialReadingsError::NegativeValue { .. }
				| RadialDialReadingsError::ValueExceedsMaximum { .. },
			),
		) => origins.value.clone(),
		Err(
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Maximum,
				..
			}
			| RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Maximum,
				..
			}
			| RadialDialPreparationError::Readings(
				RadialDialReadingsError::NullMaximum { .. }
				| RadialDialReadingsError::InexactMaximum { .. }
				| RadialDialReadingsError::NonFiniteMaximum { .. }
				| RadialDialReadingsError::NonPositiveMaximum { .. },
			),
		) => origins.maximum.clone(),
		Err(RadialDialPreparationError::Readings(
			RadialDialReadingsError::UngroupedMultipleRows { .. },
		))
		| Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn pie_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::PieFields,
	origins: &crate::rune_relation_envelope::PieFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::pie(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if error.field == fields.category {
			origins.category.clone()
		} else if error.field == fields.value {
			origins.value.clone()
		} else {
			mark.clone()
		};
	}
	use crate::plot::kernels::pie::{PiePreparationError, PiePreparationField};
	use crate::plot::kernels::pie_slices::PieSlicesError;
	match crate::plot::kernels::pie::prepare_pie_data(data, Some(&fields.category), &fields.value) {
		Err(
			PiePreparationError::MissingColumn {
				field_role: PiePreparationField::Category,
				..
			}
			| PiePreparationError::UnsupportedColumn {
				field_role: PiePreparationField::Category,
				..
			}
			| PiePreparationError::Slices(
				PieSlicesError::NullCategory { .. } | PieSlicesError::DuplicateCategory { .. },
			),
		) => origins.category.clone(),
		Err(
			PiePreparationError::MissingColumn {
				field_role: PiePreparationField::Value,
				..
			}
			| PiePreparationError::UnsupportedColumn {
				field_role: PiePreparationField::Value,
				..
			}
			| PiePreparationError::Slices(
				PieSlicesError::NullValue { .. }
				| PieSlicesError::InexactValue { .. }
				| PieSlicesError::NonFiniteValue { .. }
				| PieSlicesError::NegativeValue { .. }
				| PieSlicesError::NonFiniteTotal { .. }
				| PieSlicesError::NonPositiveTotal { .. },
			),
		) => origins.value.clone(),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn violin_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::ViolinFields,
	origins: &crate::rune_relation_envelope::ViolinFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::violin(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return match error.attribution {
			crate::plot::spec::FieldAttribution::Role("group") => origins.group.clone(),
			crate::plot::spec::FieldAttribution::Role("evaluation_value") => {
				origins.evaluation_value.clone()
			}
			crate::plot::spec::FieldAttribution::Role("density") => origins.density.clone(),
			crate::plot::spec::FieldAttribution::Role("median") => origins.median.clone(),
			_ => mark.clone(),
		};
	}
	use crate::plot::kernels::violin_density::ViolinDensityError;
	match crate::plot::kernels::violin::prepare_violin_density_data(
		data,
		&fields.group,
		&fields.evaluation_value,
		&fields.density,
		&fields.median,
	) {
		Err(ViolinDensityError::NullGroup { .. }) => origins.group.clone(),
		Err(
			ViolinDensityError::NullValue { field, .. }
			| ViolinDensityError::InexactValue { field, .. },
		) => match field.as_str() {
			"evaluation_value" => origins.evaluation_value.clone(),
			"density" => origins.density.clone(),
			"median" => origins.median.clone(),
			_ => mark.clone(),
		},
		Err(ViolinDensityError::GridMismatch { .. } | ViolinDensityError::TooFewPoints { .. }) => {
			origins.evaluation_value.clone()
		}
		Err(ViolinDensityError::MedianDisagreement { .. }) => origins.median.clone(),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn multivariate_failure_span(
	data: &DataFrame,
	plot_type: PlotType,
	fields: &crate::plot::spec::MultivariateFields,
	origins: &crate::rune_relation_envelope::MultivariateFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = match plot_type {
		PlotType::Radar => AuthoredPlotSpec::radar(fields.clone()),
		PlotType::Parallel => AuthoredPlotSpec::parallel(fields.clone()),
		_ => unreachable!("multivariate diagnostic admitted {plot_type:?}"),
	};
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return origins
			.dimension(fields, &error.field)
			.or_else(|| {
				(fields.category() == Some(error.field.as_str()))
					.then(|| origins.category.clone())
					.flatten()
			})
			.unwrap_or_else(|| mark.clone());
	}
	use crate::plot::kernels::multivariate_rows::{
		MultivariatePreparationError, MultivariateRowsError, prepare_multivariate_rows,
	};
	match prepare_multivariate_rows(data, fields.dimensions(), fields.category()) {
		Err(
			MultivariatePreparationError::MissingDimension { field }
			| MultivariatePreparationError::UnsupportedDimension { field }
			| MultivariatePreparationError::Rows(
				MultivariateRowsError::NullValue { field, .. }
				| MultivariateRowsError::InexactValue { field, .. }
				| MultivariateRowsError::NonFiniteValue { field, .. },
			),
		) => origins
			.dimension(fields, &field)
			.unwrap_or_else(|| mark.clone()),
		Err(
			MultivariatePreparationError::MissingCategory { .. }
			| MultivariatePreparationError::UnsupportedCategory { .. }
			| MultivariatePreparationError::Rows(MultivariateRowsError::NullCategory { .. }),
		) => origins.category.clone().unwrap_or_else(|| mark.clone()),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn resolved_rune_axis_scales(
	body: &RunePlotBody,
	schema: &polars::prelude::Schema,
) -> std::result::Result<(ScaleFamily, ScaleFamily), String> {
	let scales = |channels: &[crate::plot::spec::BoundChannel]| {
		[Channel::X, Channel::Y].map(|channel| {
			channels
				.iter()
				.find(|binding| binding.channel == channel)
				.map(|binding| binding.scale)
				.ok_or_else(|| format!("plot has no resolved {channel} axis"))
		})
	};
	let resolved = match body {
		RunePlotBody::Single(sink) => {
			let authored = authored_rune_plot_spec(sink);
			let binding = resolve_channels(&authored, schema).map_err(|error| error.to_string())?;
			scales(binding.channels())
		}
		RunePlotBody::StyledSingle(styled) => {
			let sink = &styled.sink;
			let authored = authored_rune_plot_spec(sink);
			let authored = if styled.stacked {
				authored.with_stacked_bar()
			} else {
				authored
			};
			let authored = if styled.percent {
				authored.with_percent()
			} else {
				authored
			};
			let binding = resolve_channels(&authored, schema).map_err(|error| error.to_string())?;
			scales(binding.channels())
		}
		RunePlotBody::Layered(specification) => {
			let binding = resolve_layered_channels(specification, schema)
				.map_err(|error| error.to_string())?;
			scales(binding.layers.first().channels())
		}
	};
	Ok((resolved[0].clone()?, resolved[1].clone()?))
}

#[cfg(feature = "toron")]
fn validate_rune_numeric_domain_applicability(
	body: &RunePlotBody,
	domain_spans: &RunePlotDomainSpans,
	tick_step_spans: &RunePlotTickStepSpans,
	minor_tick_spans: &RunePlotMinorTickSpans,
	schema: &polars::prelude::Schema,
) -> std::result::Result<(), (std::ops::Range<u32>, String)> {
	if domain_spans.x.is_none()
		&& domain_spans.y.is_none()
		&& tick_step_spans.x.is_none()
		&& tick_step_spans.y.is_none()
		&& minor_tick_spans.x.is_none()
		&& minor_tick_spans.y.is_none()
	{
		return Ok(());
	}
	let Ok((x_scale, y_scale)) = resolved_rune_axis_scales(body, schema) else {
		// Established planning owns channel-resolution failures. This check only
		// rejects a successfully resolved axis that is not numeric.
		return Ok(());
	};
	let percent_value_axis = match body {
		RunePlotBody::StyledSingle(styled) if styled.percent => {
			match styled.sink.composition.primary_plot_type() {
				PlotType::StackedArea => Some("y"),
				PlotType::Bar if matches!(x_scale, ScaleFamily::Linear) => Some("x"),
				PlotType::Bar => Some("y"),
				_ => None,
			}
		}
		_ => None,
	};
	if percent_value_axis == Some("x")
		&& let Some(span) = domain_spans.x.as_ref()
	{
		return Err((
			span.clone(),
			"x_bounds(...) cannot override the fixed 0..100 percent domain".to_owned(),
		));
	}
	if percent_value_axis == Some("y")
		&& let Some(span) = domain_spans.y.as_ref()
	{
		return Err((
			span.clone(),
			"y_bounds(...) cannot override the fixed 0..100 percent domain".to_owned(),
		));
	}
	let numeric = |scale| matches!(scale, ScaleFamily::Linear | ScaleFamily::Log);
	if let Some(span) = domain_spans.x.as_ref() {
		if !numeric(x_scale) {
			return Err((
				span.clone(),
				format!(
					"x_bounds(...) requires a numeric x axis; this figure resolves x as {x_scale:?}"
				),
			));
		}
	}
	if let Some(span) = tick_step_spans.x.as_ref() {
		if !matches!(x_scale, ScaleFamily::Linear) {
			return Err((
				span.clone(),
				format!(
					"x_tick_step(...) requires a linear numeric x axis; this figure resolves x as {x_scale:?}"
				),
			));
		}
	}
	if let Some(span) = domain_spans.y.as_ref() {
		if !numeric(y_scale) {
			return Err((
				span.clone(),
				format!(
					"y_bounds(...) requires a numeric y axis; this figure resolves y as {y_scale:?}"
				),
			));
		}
	}
	if let Some(span) = tick_step_spans.y.as_ref() {
		if !matches!(y_scale, ScaleFamily::Linear) {
			return Err((
				span.clone(),
				format!(
					"y_tick_step(...) requires a linear numeric y axis; this figure resolves y as {y_scale:?}"
				),
			));
		}
	}
	if let Some(span) = minor_tick_spans.x.as_ref() {
		if !matches!(x_scale, ScaleFamily::Linear) {
			return Err((
				span.clone(),
				format!(
					"x_minor_ticks(...) requires a linear numeric x axis; this figure resolves x as {x_scale:?}"
				),
			));
		}
	}
	if let Some(span) = minor_tick_spans.y.as_ref() {
		if !matches!(y_scale, ScaleFamily::Linear) {
			return Err((
				span.clone(),
				format!(
					"y_minor_ticks(...) requires a linear numeric y axis; this figure resolves y as {y_scale:?}"
				),
			));
		}
	}
	Ok(())
}

#[cfg(feature = "toron")]
fn validate_rune_tick_capacity(
	kernel: &dyn crate::plot::common::PlotKernel,
	settings: &crate::plot::common::ResolvedPlotSettings,
	tick_step_spans: &RunePlotTickStepSpans,
	minor_tick_spans: &RunePlotMinorTickSpans,
) -> std::result::Result<(), (std::ops::Range<u32>, String)> {
	if tick_step_spans.x.is_none()
		&& tick_step_spans.y.is_none()
		&& minor_tick_spans.x.is_none()
		&& minor_tick_spans.y.is_none()
	{
		return Ok(());
	}
	let layout = kernel.layout(settings.clone());
	let check = |placement: crate::plot::common::TickPlacement,
	             axis: &str,
	             range: (f64, f64),
	             span: &Option<std::ops::Range<u32>>|
	 -> std::result::Result<(), (std::ops::Range<u32>, String)> {
		if span.is_none() {
			return Ok(());
		}
		if let Err(error) = placement.positions(range) {
			let message = match error {
				TickPlacementError::Capacity {
					step,
					range,
					requested,
					limit,
				} => format!(
					"{axis}_tick_step({step}) over {range:?} generates {requested} positions, exceeding the {limit}-position capacity"
				),
				TickPlacementError::InvalidDomain { range } => {
					format!("{axis}_tick_step has no valid resolved domain {range:?}")
				}
				TickPlacementError::UnsupportedAxis { axis_type } => {
					format!("{axis}_tick_step does not apply to {axis_type:?} axes")
				}
				TickPlacementError::MinorCapacity { .. } => unreachable!(),
			};
			return Err((span.clone().unwrap(), message));
		}
		Ok(())
	};
	let check_minor = |placement: crate::plot::common::TickPlacement,
	                   axis: &str,
	                   range: (f64, f64),
	                   minor_ticks: u32,
	                   span: &Option<std::ops::Range<u32>>|
	 -> std::result::Result<(), (std::ops::Range<u32>, String)> {
		let Some(span) = span else {
			return Ok(());
		};
		let major = placement.positions(range).map_err(|error| {
			let message = match error {
				TickPlacementError::Capacity {
					step: _,
					range,
					requested,
					limit,
				} => format!(
					"{axis}_minor_ticks({minor_ticks}) with {placement:?} over {range:?} generates {requested} major positions, exceeding the {limit}-position capacity"
				),
				TickPlacementError::InvalidDomain { range } => {
					format!("{axis}_minor_ticks({minor_ticks}) has no valid resolved domain {range:?}")
				}
				TickPlacementError::UnsupportedAxis { axis_type } => {
					format!("{axis}_minor_ticks({minor_ticks}) does not apply to {axis_type:?} axes")
				}
				TickPlacementError::MinorCapacity { .. } => unreachable!(),
			};
			(span.clone(), message)
		})?;
		placement
			.minor_positions(&major, minor_ticks)
			.map_err(|error| {
				let TickPlacementError::MinorCapacity {
					placement,
					minor_ticks,
					range,
					requested,
					limit,
				} = error
				else {
					unreachable!()
				};
				(
					span.clone(),
					format!(
						"{axis}_minor_ticks({minor_ticks}) with {placement:?} over {range:?} generates {requested} total positions, exceeding the {limit}-position capacity"
					),
				)
			})
			.map(|_| ())
	};
	match layout {
		PlotLayout::Cartesian {
			x_range, y_range, ..
		} => {
			check(settings.x_tick_placement, "x", x_range, &tick_step_spans.x)?;
			check(settings.y_tick_placement, "y", y_range, &tick_step_spans.y)?;
			check_minor(
				settings.x_tick_placement,
				"x",
				x_range,
				settings.x_minor_ticks,
				&minor_tick_spans.x,
			)?;
			check_minor(
				settings.y_tick_placement,
				"y",
				y_range,
				settings.y_minor_ticks,
				&minor_tick_spans.y,
			)
		}
		PlotLayout::CategoricalX { y_range, .. } => {
			check(settings.y_tick_placement, "y", y_range, &tick_step_spans.y)?;
			check_minor(
				settings.y_tick_placement,
				"y",
				y_range,
				settings.y_minor_ticks,
				&minor_tick_spans.y,
			)
		}
		PlotLayout::CategoricalY { x_range, .. } => {
			check(settings.x_tick_placement, "x", x_range, &tick_step_spans.x)?;
			check_minor(
				settings.x_tick_placement,
				"x",
				x_range,
				settings.x_minor_ticks,
				&minor_tick_spans.x,
			)
		}
		_ => Ok(()),
	}
}

#[cfg(feature = "toron")]
fn prepare_rune_categorical_palettes(
	app_state: &AppState,
	source_buffer: BufferId,
	view: &TransformedRuneRelation,
) -> HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette> {
	view.views
		.iter()
		.filter_map(|view| {
			let RuneViewIntent::Plot(plot) = &view.intent else {
				return None;
			};
			let prepared = match resolved_rune_categorical_selection(&plot.authored_settings) {
				Ok(selection) => {
					let identity = app_state
						.rune_categorical_identities
						.get(&(source_buffer, view.identity.clone(), selection.len()))
						.cloned()
						.unwrap_or_default();
					PreparedRuneCategoricalPalette::Ready {
						selection,
						identity,
					}
				}
				Err(error) => PreparedRuneCategoricalPalette::Failed(error),
			};
			Some((view.identity.clone(), prepared))
		})
		.collect()
}

#[cfg(all(test, feature = "toron"))]
fn default_rune_categorical_palettes(
	views: &[SpannedRuneViewIntent],
) -> HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette> {
	views
		.iter()
		.filter_map(|view| {
			let RuneViewIntent::Plot(plot) = &view.intent else {
				return None;
			};
			let prepared = match resolved_rune_categorical_selection(&plot.authored_settings) {
				Ok(selection) => PreparedRuneCategoricalPalette::Ready {
					selection,
					identity: ColorIdentity::default(),
				},
				Err(error) => PreparedRuneCategoricalPalette::Failed(error),
			};
			Some((view.identity.clone(), prepared))
		})
		.collect()
}

#[cfg(feature = "toron")]
async fn dispatch_rune_relation_with_palettes(
	connection: Arc<RwLock<dyn DatabaseAdapter>>,
	view: TransformedRuneRelation,
	palettes: HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette>,
) -> std::result::Result<crate::gui::messages::RuneRelationViewSetOutcome, String> {
	let result = connection.write().await.dispatch_rune(&view.source).await;
	match result {
		Ok(artifact) => rune_view_outcome_with_palettes(artifact, view.views, palettes),
		Err(failure) => {
			let location = failure
				.span
				.and_then(|span| {
					view.source_map.map_span(toron_client::RuneSourceSpan {
						start: span.start,
						end: span.end,
					})
				})
				.unwrap_or_else(|| view.source_map.diagnostic_fallback());
			Err(format!(
				"Rune {:?} at bytes {}..{}: {}",
				failure.kind, location.start, location.end, failure.message
			))
		}
	}
}

#[cfg(all(test, feature = "toron"))]
async fn dispatch_rune_relation(
	connection: Arc<RwLock<dyn DatabaseAdapter>>,
	view: TransformedRuneRelation,
) -> std::result::Result<crate::gui::messages::RuneRelationViewSetOutcome, String> {
	let palettes = default_rune_categorical_palettes(&view.views);
	dispatch_rune_relation_with_palettes(connection, view, palettes).await
}

#[cfg(feature = "toron")]
fn rune_table_skin_authored(
	intent: RuneTableSkinIntent,
) -> crate::plot::spec::Override<TableSkinSpec> {
	match intent {
		RuneTableSkinIntent::Default => crate::plot::spec::Override::Inherit,
		RuneTableSkinIntent::NamedTheme {
			name,
			projection: RuneTableSkinProjection::PaletteForward,
		} => crate::plot::spec::Override::Set(TableSkinSpec::new(
			name,
			TableSkinProjection::PaletteForward,
		)),
		RuneTableSkinIntent::NamedTheme {
			name,
			projection: RuneTableSkinProjection::Duotone,
		} => {
			crate::plot::spec::Override::Set(TableSkinSpec::new(name, TableSkinProjection::Duotone))
		}
	}
}

#[cfg(feature = "toron")]
fn resolve_rune_table(
	intent: RuneTableSink,
) -> std::result::Result<crate::gui::messages::RuneTablePresentation, String> {
	let completion = TableSkinCascade {
		authored: rune_table_skin_authored(intent.skin),
		..TableSkinCascade::default()
	}
	.resolve_through_authored()
	.map_err(|error| error.to_string())?;
	Ok(crate::gui::messages::RuneTablePresentation {
		skin: completion.skin,
		authored_show_column_types: intent.show_column_types,
	})
}

#[cfg(all(test, feature = "toron"))]
fn resolve_rune_table_skin_with(
	intent: RuneTableSkinIntent,
	mut resolve: impl FnMut(
		&str,
		TableSkinProjection,
	) -> std::result::Result<TableSkin, TableSkinProjectionError>,
) -> std::result::Result<TableSkin, String> {
	TableSkinCascade {
		authored: rune_table_skin_authored(intent),
		..TableSkinCascade::default()
	}
	.resolve_through_authored_with(|spec| resolve(&spec.identity, spec.projection))
	.map(|completion| completion.skin)
	.map_err(|error| error.to_string())
}

#[cfg(feature = "toron")]
fn rune_view_outcome_with_palettes(
	result: Arc<crate::result_artifact::ResultArtifact>,
	views: Vec<SpannedRuneViewIntent>,
	mut palettes: HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette>,
) -> std::result::Result<crate::gui::messages::RuneRelationViewSetOutcome, String> {
	use crate::gui::messages::{
		RuneCategoricalContinuation, RuneRelationSinkOutcome, RuneRelationSinkOutput,
		RuneRelationViewSetOutcome,
	};
	let frame = result.projection().map_err(|error| error.to_string())?;
	let sinks = views
		.into_iter()
		.map(|view| {
			let presentation = view.intent.presentation();
			let identity = view.identity;
			let figure_grid = match &view.intent {
				RuneViewIntent::Plot(plot) => {
					crate::plot::grid::FigureGridLayoutSettings::resolve(plot.facet_gutter)
				}
				RuneViewIntent::Table(_) => crate::plot::grid::FigureGridLayoutSettings::default(),
			};
			let (facet, facet_span) = match &view.intent {
				RuneViewIntent::Plot(plot) => (plot.facet.clone(), plot.facet_span.clone()),
				RuneViewIntent::Table(_) => (None, None),
			};
			let diagnostic_sink = match &view.intent {
				RuneViewIntent::Plot(plot) => match &plot.body {
					RunePlotBody::Single(sink) => Some(sink),
					RunePlotBody::StyledSingle(styled) => Some(&styled.sink),
					RunePlotBody::Layered(_) => None,
				},
				RuneViewIntent::Table(_) => None,
			};
			let box_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.box_plot()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let heatmap_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.heatmap()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let bubble_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.bubble()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let funnel_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.funnel()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let radial_dial_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.radial_dial()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let pie_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.pie()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let violin_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.violin()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let multivariate_diagnostic = diagnostic_sink.and_then(|sink| {
				let (plot_type, fields, origins) = sink.mark_fields.as_ref()?.multivariate()?;
				Some((
					plot_type,
					fields.clone(),
					origins.clone(),
					sink.origin.clone(),
				))
			});
			let body_output_span = match &view.intent {
				RuneViewIntent::Plot(plot) => match &plot.body {
					RunePlotBody::Single(sink)
						if sink.composition.primary_plot_type() == PlotType::Histogram =>
					{
						Some(sink.origin.clone())
					}
					RunePlotBody::StyledSingle(styled)
						if styled.sink.composition.primary_plot_type() == PlotType::Histogram =>
					{
						Some(styled.sink.origin.clone())
					}
					_ => None,
				},
				RuneViewIntent::Table(_) => None,
			};
			let (
				output,
				tick_step_output_span,
				minor_tick_output_span,
				continuous_intent_output_span,
			) = match view.intent {
				RuneViewIntent::Table(table) => (
					resolve_rune_table(table).map(RuneRelationSinkOutput::Table),
					None,
					RunePlotMinorTickSpans::default(),
					None,
				),
				RuneViewIntent::Plot(plot) => {
					let crate::rune_relation_envelope::RunePlotIntent {
						body,
						facet: plot_facet,
						facet_gutter: _,
						authored_settings,
						plot_skin_span,
						facet_span: _,
						domain_spans,
						tick_step_spans,
						minor_tick_spans,
						continuous_intent,
						continuous_intent_span,
					} = plot;
					let authored_settings = match resolve_rune_plot_skin(authored_settings) {
						Ok(settings) => settings,
						Err(error) => {
							return RuneRelationSinkOutcome {
								identity,
								presentation,
								span: plot_skin_span.unwrap_or(view.span),
								facet: plot_facet.clone(),
								figure_grid,
								source: std::sync::Arc::clone(&result),
								output: Err(error),
							};
						}
					};
					if let Err((span, error)) = validate_rune_numeric_domain_applicability(
						&body,
						&domain_spans,
						&tick_step_spans,
						&minor_tick_spans,
						frame.schema().as_ref(),
					) {
						return RuneRelationSinkOutcome {
							identity,
							presentation,
							span,
							facet: plot_facet.clone(),
							figure_grid,
							source: std::sync::Arc::clone(&result),
							output: Err(error),
						};
					}
					let prepared = palettes.remove(&identity).unwrap_or_else(|| {
						PreparedRuneCategoricalPalette::Failed(
							"plot destination has no prepared categorical palette".to_owned(),
						)
					});
					let PreparedRuneCategoricalPalette::Ready {
						selection,
						identity: prior_identity,
					} = prepared
					else {
						let PreparedRuneCategoricalPalette::Failed(error) = prepared else {
							unreachable!()
						};
						return RuneRelationSinkOutcome {
							identity,
							presentation,
							span: view.span,
							facet: plot_facet.clone(),
							figure_grid,
							source: std::sync::Arc::clone(&result),
							output: Err(error),
						};
					};
					let palette_len = selection.len();
					let resolved_tick_settings = PlotSettings {
						authored_spec: authored_settings.clone(),
						..PlotSettings::default()
					}
					.resolve();
					let tick_step_output_span = tick_step_spans
						.x
						.clone()
						.or_else(|| tick_step_spans.y.clone())
						.or_else(|| minor_tick_spans.x.clone())
						.or_else(|| minor_tick_spans.y.clone());
					let has_authored_minor_ticks =
						minor_tick_spans.x.is_some() || minor_tick_spans.y.is_some();
					let output = match body {
						RunePlotBody::Single(plot) => {
							let authored_spec = with_rune_continuous_intent(
								authored_rune_plot_spec(&plot),
								continuous_intent,
							);
							plan_with_categorical_palette(
								PlotRequest {
									source: PlotSource::Materialized {
										name: "workspace relation".to_owned(),
										data: std::sync::Arc::clone(&result),
									},
									authored_spec,
								},
								&selection,
								&prior_identity,
							)
							.and_then(|(resolved, next_identity)| {
								if tick_step_spans.x.is_none()
									&& tick_step_spans.y.is_none()
									&& !has_authored_minor_ticks
								{
									return Ok(RuneRelationSinkOutput::Plot(
										resolved,
										authored_settings,
										RuneCategoricalContinuation {
											palette_len,
											identity: next_identity,
										},
									));
								}
								let kernel =
									create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
								validate_rune_tick_capacity(
									kernel.as_ref(),
									&resolved_tick_settings,
									&tick_step_spans,
									&minor_tick_spans,
								)
								.map_err(|(_, error)| error)?;
								Ok(RuneRelationSinkOutput::Plot(
									resolved,
									authored_settings,
									RuneCategoricalContinuation {
										palette_len,
										identity: next_identity,
									},
								))
							})
						}
						RunePlotBody::StyledSingle(styled) => match styled.into_layer() {
							Ok(layer) => {
								let specification = LayeredPlotSpec::new(layer);
								if tick_step_spans.x.is_none()
									&& tick_step_spans.y.is_none()
									&& !has_authored_minor_ticks
								{
									Ok(RuneRelationSinkOutput::LayeredPlot(
										specification,
										authored_settings,
									))
								} else {
									create_layered_plot(&specification, frame.as_ref(), 1200, 720)
										.map_err(|error| error.to_string())
										.and_then(|kernel| {
											validate_rune_tick_capacity(
												kernel.as_ref(),
												&resolved_tick_settings,
												&tick_step_spans,
												&minor_tick_spans,
											)
											.map_err(|(_, error)| error)
											.map(|_| {
												RuneRelationSinkOutput::LayeredPlot(
													specification,
													authored_settings,
												)
											})
										})
								}
							}
							Err(styled) => {
								let StyledRunePlotSink {
									sink,
									settings,
									stacked,
									percent,
								} = styled;
								let authored_spec = with_rune_continuous_intent(
									authored_rune_plot_spec(&sink),
									continuous_intent,
								);
								let authored_spec = if stacked {
									authored_spec.with_stacked_bar()
								} else {
									authored_spec
								};
								let authored_spec = if percent {
									authored_spec.with_percent()
								} else {
									authored_spec
								};
								plan_with_categorical_palette(
									PlotRequest {
										source: PlotSource::Materialized {
											name: "workspace relation".to_owned(),
											data: std::sync::Arc::clone(&result),
										},
										authored_spec,
									},
									&selection,
									&prior_identity,
								)
								.and_then(|(resolved, next_identity)| {
									if tick_step_spans.x.is_none()
										&& tick_step_spans.y.is_none() && !has_authored_minor_ticks
									{
										return Ok(RuneRelationSinkOutput::StyledPlot(
											resolved,
											settings,
											authored_settings,
											RuneCategoricalContinuation {
												palette_len,
												identity: next_identity,
											},
										));
									}
									create_styled_plot(
										&resolved.resolved_spec,
										&resolved.data,
										settings.clone(),
									)
									.map_err(|error| error.to_string())
									.and_then(|kernel| {
										validate_rune_tick_capacity(
											kernel.as_ref(),
											&resolved_tick_settings,
											&tick_step_spans,
											&minor_tick_spans,
										)
										.map_err(|(_, error)| error)
										.map(|_| {
											RuneRelationSinkOutput::StyledPlot(
												resolved,
												settings,
												authored_settings,
												RuneCategoricalContinuation {
													palette_len,
													identity: next_identity,
												},
											)
										})
									})
								})
							}
						},
						RunePlotBody::Layered(specification) => {
							if tick_step_spans.x.is_none()
								&& tick_step_spans.y.is_none()
								&& !has_authored_minor_ticks
							{
								Ok(RuneRelationSinkOutput::LayeredPlot(
									specification,
									authored_settings,
								))
							} else {
								create_layered_plot(&specification, frame.as_ref(), 1200, 720)
									.map_err(|error| error.to_string())
									.and_then(|kernel| {
										validate_rune_tick_capacity(
											kernel.as_ref(),
											&resolved_tick_settings,
											&tick_step_spans,
											&minor_tick_spans,
										)
										.map_err(|(_, error)| error)
										.map(|_| {
											RuneRelationSinkOutput::LayeredPlot(
												specification,
												authored_settings,
											)
										})
									})
							}
						}
					};
					(
						output,
						tick_step_output_span,
						minor_tick_spans,
						continuous_intent_span,
					)
				}
			};
			let continuous_output_span = output.as_ref().err().and_then(|error| {
				(error.contains("authored continuous colour anchor"))
					.then(|| continuous_intent_output_span.clone())
					.flatten()
			});
			let output_span = output.as_ref().err().and_then(|error| {
				if error.contains("_minor_ticks") {
					minor_tick_output_span
						.x
						.clone()
						.or_else(|| minor_tick_output_span.y.clone())
						.or(tick_step_output_span)
				} else {
					tick_step_output_span
						.or_else(|| minor_tick_output_span.x.clone())
						.or_else(|| minor_tick_output_span.y.clone())
				}
			});
			let box_output_span = output.as_ref().err().and_then(|_| {
				box_diagnostic.as_ref().map(|(fields, origins, mark)| {
					box_plot_failure_role(frame.as_ref(), fields)
						.map(|role| origins.for_role(role))
						.unwrap_or_else(|| mark.clone())
				})
			});
			let heatmap_output_span = output.as_ref().err().and_then(|_| {
				heatmap_diagnostic.as_ref().map(|(fields, origins, mark)| {
					heatmap_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let bubble_output_span = output.as_ref().err().and_then(|_| {
				bubble_diagnostic.as_ref().map(|(fields, origins, mark)| {
					bubble_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let funnel_output_span = output.as_ref().err().and_then(|_| {
				funnel_diagnostic.as_ref().map(|(fields, origins, mark)| {
					funnel_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let radial_dial_output_span = output.as_ref().err().and_then(|_| {
				radial_dial_diagnostic
					.as_ref()
					.map(|(fields, origins, mark)| {
						radial_dial_failure_span(frame.as_ref(), fields, origins, mark)
					})
			});
			let pie_output_span = output.as_ref().err().and_then(|_| {
				pie_diagnostic.as_ref().map(|(fields, origins, mark)| {
					pie_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let violin_output_span = output.as_ref().err().and_then(|_| {
				violin_diagnostic.as_ref().map(|(fields, origins, mark)| {
					violin_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let multivariate_output_span = output.as_ref().err().and_then(|_| {
				multivariate_diagnostic
					.as_ref()
					.map(|(plot_type, fields, origins, mark)| {
						multivariate_failure_span(frame.as_ref(), *plot_type, fields, origins, mark)
					})
			});
			RuneRelationSinkOutcome {
				identity,
				presentation,
				span: continuous_output_span
					.or(output_span)
					.or(box_output_span)
					.or(heatmap_output_span)
					.or(bubble_output_span)
					.or(funnel_output_span)
					.or(radial_dial_output_span)
					.or(pie_output_span)
					.or(violin_output_span)
					.or(multivariate_output_span)
					.or(body_output_span)
					.or(facet_span)
					.unwrap_or(view.span),
				facet,
				figure_grid,
				source: std::sync::Arc::clone(&result),
				output,
			}
		})
		.collect();
	Ok(RuneRelationViewSetOutcome { result, sinks })
}

#[cfg(all(test, feature = "toron"))]
fn rune_view_outcome(
	frame: DataFrame,
	views: Vec<SpannedRuneViewIntent>,
) -> crate::gui::messages::RuneRelationViewSetOutcome {
	let palettes = default_rune_categorical_palettes(&views);
	rune_view_outcome_with_palettes(
		crate::result_artifact::ResultArtifact::from_polars(Arc::new(frame)),
		views,
		palettes,
	)
	.expect("test frame projects")
}

fn queue_plain_run(
	app_state: &mut AppState,
	language_id: libaryon::engine::LanguageId,
	kind: PendingRunKind,
) -> Task<Message> {
	let Some(source_buffer) = app_state.code_editor.active_buffer_id() else {
		app_state.status_error = "The focused source has no buffer identity.".to_owned();
		return Task::none();
	};
	let Some(snapshot_request) = app_state.code_editor.request_execution_snapshot() else {
		app_state.status_error = "The editor could not snapshot the focused source.".to_owned();
		return Task::none();
	};
	app_state.pending_runs.push_back(PendingRun {
		snapshot_request,
		source_buffer,
		language_id,
		kind,
	});
	app_state.status_msg = "Preparing code…".into();
	Task::none()
}

fn finish_pending_runs(app_state: &mut AppState) -> Task<Message> {
	let mut tasks = Vec::new();
	while let Some(snapshot_request) = app_state
		.pending_runs
		.front()
		.map(|pending| pending.snapshot_request)
	{
		let Some(snapshot) = app_state
			.code_editor
			.take_execution_snapshot(snapshot_request)
		else {
			break;
		};
		let pending = app_state
			.pending_runs
			.pop_front()
			.expect("pending run disappeared");
		if snapshot.buffer_id != pending.source_buffer {
			app_state.status_error =
				"The source pane changed before its execution snapshot completed.".to_owned();
			continue;
		}
		let code = match pending.kind {
			PendingRunKind::Buffer => snapshot.text.clone(),
			PendingRunKind::Snippet => {
				let Some(code) = app_state.code_editor.selected_or_current_line() else {
					app_state.status_msg = "No editor text is available to run.".into();
					continue;
				};
				code
			}
		};
		tasks.push(run_language_code(
			app_state,
			pending.source_buffer,
			pending.language_id,
			code,
			&snapshot.text,
		));
	}
	Task::batch(tasks)
}

fn run_markdown_cell(app_state: &mut AppState, cell: MarkdownCellExecution) -> Task<Message> {
	match cell.language_id.as_str() {
		"sql" => run_sql_markdown_cell(app_state, cell),
		"rune" => run_rune_markdown_cell(app_state, cell),
		language => {
			app_state.status_msg = format!("Unsupported notebook cell language: {language}");
			Task::none()
		}
	}
}

fn run_sql_markdown_cell(app_state: &mut AppState, cell: MarkdownCellExecution) -> Task<Message> {
	let Some(db) = app_state.adapter_state.sql_connection() else {
		set_sql_error(
			app_state,
			"No SQL target: scratch database unavailable.".to_owned(),
			Some(cell.source_start_line),
		);
		return Task::none();
	};
	if cell.code.trim().is_empty() {
		set_sql_status(app_state, "SQL · nothing to run in this cell");
		return Task::none();
	}
	let db = db.clone();
	let identity = cell.identity.clone();
	let presentation = cell.presentation;
	let source_start_line = cell.source_start_line;
	set_sql_status(app_state, "SQL · running…");
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	// Starting any SQL cell retires its prior named snapshot immediately.
	// Presentation only decides where the new result appears; it must not let a
	// Rune cell observe stale data while this query is in flight.
	app_state
		.code_editor
		.clear_markdown_cell_artifact(&cell.identity);
	Task::perform(
		async move {
			let mut guard = db.write().await;
			guard.dispatch(&cell.code).await
		},
		move |result| Message::RunMarkdownCellResult {
			identity,
			presentation,
			source_start_line,
			result,
		},
	)
}

fn run_rune_markdown_cell(app_state: &mut AppState, cell: MarkdownCellExecution) -> Task<Message> {
	if cell.code.trim().is_empty() {
		app_state.status_msg = "Nothing to run in this Rune cell.".into();
		return Task::none();
	}
	let snapshots = app_state.code_editor.named_notebook_cell_data();
	let identity = cell.identity.clone();
	let presentation = cell.presentation;
	app_state.status_msg = "Building notebook plot…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	if presentation.output == MarkdownCellOutputTarget::Inline {
		app_state.code_editor.begin_markdown_cell(identity.clone());
	}
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				let mut execution = rune_host::execute_body(
					&cell.code,
					RuneCapabilities {
						cells: snapshots,
						..RuneCapabilities::default()
					},
				)?;
				match execution.requests.len() {
					1 => plan(execution.requests.remove(0)),
					0 => Err("Rune cell did not publish a plot; call `.show()?`.".to_owned()),
					_ => Err(
						"Rune cell published multiple plots; one cell currently owns one output."
							.to_owned(),
					),
				}
			})
			.await
			.map_err(|error| format!("Rune notebook task panicked: {error}"))?
		},
		move |result| Message::RunRuneCellResult {
			identity,
			presentation,
			result,
		},
	)
}

fn begin_window_shutdown(_app_state: &mut AppState) -> Task<Message> {
	#[cfg(feature = "toron")]
	{
		if _app_state.closing_local_workspace {
			return Task::none();
		}
		if let Some(owner) = _app_state.local_workspace_owner.take() {
			_app_state.closing_local_workspace = true;
			_app_state.status_error.clear();
			_app_state.status_msg =
				"Stopping the local workspace: draining Toron before Wendelon…".into();
			return Task::perform(
				async move {
					tokio::task::spawn_blocking(move || {
						owner.stop(crate::local_workspace::STOP_DEADLINE)
					})
					.await
					.map_or_else(
						|error| format!("local workspace stop task failed: {error}"),
						|outcome| format!("{outcome:?}"),
					)
				},
				Message::LocalWorkspaceStopped,
			);
		}
	}
	window::latest().and_then(window::close)
}

fn update(app_state: &mut AppState, message: Message) -> Task<Message> {
	#[cfg(feature = "toron")]
	if app_state.saved_close_state == SavedCloseState::Frozen
		&& !matches!(&message, Message::LocalWorkspaceStopped(_))
	{
		return Task::none();
	}
	match message {
		Message::ToggleStatusDetail => {
			if app_state.status_detail.is_some() {
				app_state.status_detail_open = !app_state.status_detail_open;
			}
		}
		Message::CloseStatusDetail => {
			app_state.status_detail_open = false;
		}
		Message::CheckpointSession => {
			#[cfg(feature = "toron")]
			{
				if app_state.session_id.is_none() {
					app_state.status_error = "This editor is not a saved session.".to_owned();
					return Task::none();
				}
				let colours = crate::session_persistence::RuntimeColours::new(
					app_state.rune_categorical_identities.clone(),
				);
				match app_state.code_editor.checkpoint_session(&colours) {
					Ok(()) => {
						app_state.saved_close_state = SavedCloseState::Open;
						app_state.status_error.clear();
						app_state.status_msg = "Session checkpointed".to_owned();
					}
					Err(error) => {
						app_state.status_msg.clear();
						app_state.status_error = format!("Session checkpoint failed: {error}");
					}
				}
			}
		}
		Message::CopySessionId => {
			if let Some(session_id) = app_state.session_id {
				return iced::clipboard::write(session_identity_text(session_id));
			}
		}
		Message::CloseWithoutCheckpoint => {
			if app_state.session_id.is_some()
				&& app_state.saved_close_state == SavedCloseState::Failed
			{
				app_state.saved_close_state = SavedCloseState::Frozen;
				app_state.pending_runs.clear();
				return begin_window_shutdown(app_state);
			}
		}
		Message::CopyStatusDetail => {
			if let Some(detail) = app_state.status_detail.as_ref() {
				return iced::clipboard::write(detail.full.clone());
			}
		}
		Message::StatusDetailAction(action) => {
			if !action.is_edit()
				&& let Some(detail) = app_state.status_detail.as_mut()
			{
				detail.content.perform(action);
			}
		}
		Message::CloseWindow => {
			#[cfg(feature = "toron")]
			{
				if app_state.closing_local_workspace {
					return Task::none();
				}
				if app_state.session_id.is_some() {
					let colours = crate::session_persistence::RuntimeColours::new(
						app_state.rune_categorical_identities.clone(),
					);
					match app_state.code_editor.checkpoint_session(&colours) {
						Ok(()) => {
							app_state.saved_close_state = SavedCloseState::Frozen;
							app_state.pending_runs.clear();
							return begin_window_shutdown(app_state);
						}
						Err(error) => {
							app_state.saved_close_state = SavedCloseState::Failed;
							app_state.status_msg.clear();
							app_state.status_error = if error.is_stale() {
								format!(
									"Session close checkpoint failed because the workspace changed before the FIFO head; close again to retry: {error}"
								)
							} else {
								format!("Session close checkpoint failed: {error}")
							};
							return Task::none();
						}
					}
				}
			}
			return begin_window_shutdown(app_state);
		}
		Message::Menu(menu_message) => {
			if let Some(action) = app_state.menu_state.update(menu_message)
				&& let Some(message) = menu_action_to_message(&action)
			{
				return update(app_state, message);
			}
		}
		Message::DragWindow => {
			return window::latest().and_then(window::drag);
		}
		Message::CodeEditEvent(edit_event) => {
			clear_transient_execution_status_on_editor_activity(app_state, &edit_event);
			let task = app_state
				.code_editor
				.update(edit_event)
				.map(Message::CodeEditEvent);
			#[cfg(feature = "toron")]
			if let Some(colours) = app_state.code_editor.take_restored_colour_meanings() {
				app_state.rune_categorical_identities = colours.into_map();
			}
			#[cfg(feature = "toron")]
			if let Some(error) = app_state.code_editor.take_session_restore_error() {
				app_state.status_error = error;
			}
			if app_state.code_editor.take_quit_request() {
				return update(app_state, Message::CloseWindow);
			}
			let run_task = finish_pending_runs(app_state);
			return Task::batch([task, run_task]);
		}
		Message::CodeEditResize(size) => {
			app_state.window_size = size;
			sync_workspace_viewport(app_state);
		}
		Message::MaximizeWindow => {
			app_state.is_maximized = !app_state.is_maximized;
			let is_maximized = app_state.is_maximized;
			return window::latest().and_then(move |id| window::maximize(id, is_maximized));
		}
		Message::MinimizeWindow => {
			return window::latest().and_then(move |id| window::minimize(id, true));
		}
		Message::ResizeWindow(direction) => {
			return window::latest().and_then(move |id| window::drag_resize(id, direction));
		}
		Message::Connect => match app_state.adapter_state.stage {
			AdapterStage::None => {
				app_state.adapter_state.stage = AdapterStage::Unselected;
				app_state.status_msg = "Select an adapter.".into();
			}
			_ => {
				app_state.adapter_state.stage = AdapterStage::None;
				app_state.status_msg = "No adapter selected.".into();
			}
		},
		Message::AdapterSelected(adapter_selection) => {
			app_state.status_msg = format!("Adapter selected: {:?}", adapter_selection);
			app_state.adapter_state.select(adapter_selection);
		}
		Message::AdapterConfigurationChanged(key, value) => {
			app_state.adapter_state.fields.insert(key, value);
			app_state.status_msg = "Configure adapter.".into();
		}
		Message::AdapterConfigurationSubmitted => {
			let has_empty = crate::adapters::driver::fields_for(&app_state.adapter_state.selection)
				.iter()
				.any(|f| {
					app_state
						.adapter_state
						.fields
						.get(f.key)
						.is_none_or(|v| v.trim().is_empty())
				});
			if has_empty {
				app_state.status_error = "All connection fields are required.".to_string();
				return Task::none();
			}
			app_state.adapter_state.configure();
			app_state.status_msg = "Adapter configured.".into();
			app_state.adapter_state.stage = AdapterStage::Configured;
			let config = app_state.adapter_state.configuration.clone();
			return Task::perform(
				async move { AdapterState::connect(config).await },
				Message::AdapterConnected,
			);
		}
		Message::AdapterConnected(result) => match result {
			Ok(connection) => {
				app_state.adapter_state.connection = Some(connection);
				app_state.adapter_state.stage = AdapterStage::Connected;
				app_state.status_error.clear();
				app_state.status_msg = "Adapter connected.".into();
			}
			Err(error) => {
				app_state.adapter_state.connection = None;
				// Scratch remains explicitly available for SQL, while the
				// attempted adapter remains visibly unattached.
				app_state.adapter_state.stage = AdapterStage::Configured;
				app_state.status_error = error;
				app_state.status_msg = "Adapter unavailable; SQL scratch remains active.".into();
			}
		},
		#[cfg(feature = "toron")]
		Message::LocalWorkspaceDiscovered(workspace) => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			app_state.adapter_state.local_workspace = Some(workspace.clone());
			app_state.local_creation.observe(&workspace);
			let nilket_local::LocalWorkspace::Ready(ready) = workspace else {
				app_state.status_error = workspace.to_string();
				app_state.status_msg = app_state.local_creation.offer().map_or_else(
					|| "SQL scratch remains active.".to_owned(),
					|root| {
						format!(
							"Local workspace is absent. Start a session-bound workspace at {}?",
							root.display()
						)
					},
				);
				return Task::none();
			};
			app_state.status_error.clear();
			app_state.status_msg = "Attaching Toron workspace…".to_owned();
			let endpoint = ready.sql_endpoint.to_string();
			let message_workspace = ready.clone();
			return Task::perform(
				async move {
					crate::adapters::toron::ToronAdapter::connect(&endpoint)
						.await
						.map(|adapter| {
							Arc::new(RwLock::new(adapter))
								as Arc<RwLock<dyn crate::adapters::common::DatabaseAdapter>>
						})
						.map_err(|error| error.to_string())
				},
				move |connection| Message::LocalWorkspaceAttached {
					workspace: message_workspace,
					connection,
				},
			);
		}
		#[cfg(feature = "toron")]
		Message::LocalWorkspaceAttached {
			workspace,
			connection,
		} => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			match connection {
				Ok(connection) => {
					app_state.adapter_state.selection =
						crate::adapters::driver::AdapterSelection::Toron;
					app_state.adapter_state.configuration =
						crate::adapters::driver::AdapterConfiguration::Toron {
							endpoint: workspace.sql_endpoint.to_string(),
						};
					app_state.adapter_state.name = "local workspace".to_owned();
					app_state.adapter_state.connection = Some(connection);
					app_state.adapter_state.stage = AdapterStage::Connected;
					app_state.adapter_state.local_workspace =
						Some(nilket_local::LocalWorkspace::Ready(workspace.clone()));
					app_state.status_error.clear();
					app_state.status_msg = "Joined Toron workspace.".to_owned();
				}
				Err(cause) => {
					let unhealthy = nilket_local::LocalWorkspace::Unhealthy {
						identity: workspace.identity,
						cause,
					};
					app_state.status_error = unhealthy.to_string();
					app_state.status_msg = "SQL scratch remains active.".into();
					app_state.adapter_state.connection = None;
					app_state.adapter_state.local_workspace = Some(unhealthy);
				}
			}
		}
		#[cfg(feature = "toron")]
		Message::StartLocalWorkspace => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			let Some(profile_root) = app_state.local_creation.accept() else {
				return Task::none();
			};
			app_state.status_error.clear();
			app_state.status_msg = format!(
				"Starting local workspace at {} under this Polariton session…",
				profile_root.display()
			);
			let owner = crate::local_workspace::LocalWorkspaceOwner::launch(profile_root.clone());
			let monitor = owner.monitor();
			app_state.local_workspace_owner = Some(owner);
			return Task::perform(
				crate::local_workspace::watch_until_ready(
					profile_root,
					monitor,
					crate::local_workspace::STARTUP_DEADLINE,
				),
				|outcome| match outcome {
					crate::local_workspace::StartupOutcome::Ready(ready) => {
						Message::LocalWorkspaceStartupSettled(Ok(ready))
					}
					crate::local_workspace::StartupOutcome::Failed { observed, cause } => {
						Message::LocalWorkspaceStartupSettled(Err((observed, cause)))
					}
				},
			);
		}
		#[cfg(feature = "toron")]
		Message::LocalWorkspaceStartupSettled(result) => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			match result {
				Ok(ready) => {
					return update(
						app_state,
						Message::LocalWorkspaceDiscovered(nilket_local::LocalWorkspace::Ready(
							ready,
						)),
					);
				}
				Err((observed, cause)) => {
					app_state.adapter_state.local_workspace = Some(observed);
					app_state.status_error = cause;
					app_state.status_msg =
					"Local workspace creation failed; the one-shot grant was spent. SQL scratch remains active."
						.into();
				}
			}
		}
		#[cfg(feature = "toron")]
		Message::LocalWorkspaceStopped(outcome) => {
			if !app_state.closing_local_workspace {
				return Task::none();
			}
			app_state.status_msg = format!("Local workspace stopped ({outcome}).");
			return window::latest().and_then(window::close);
		}
		Message::ScratchReady(scratch) => {
			if scratch.is_none() {
				app_state.status_error = "Scratch database failed to open.".to_string();
			}
			app_state.adapter_state.scratch = scratch;
		}
		Message::Run => {
			// In a notebook, Run always means "run the cell at the cursor",
			// even mid-selection — otherwise a selection would fall through to
			// dispatching the whole Markdown document as one SQL string.
			if let Some(cell) = app_state.code_editor.markdown_cell_at_cursor() {
				return run_markdown_cell(app_state, cell);
			}
			if app_state.code_editor.is_notebook() {
				app_state.status_msg = "Place the cursor in a SQL cell to run it.".into();
				return Task::none();
			}
			let Some(language_id) = app_state.code_editor.active_language_id() else {
				app_state.status_msg = "The focused pane has no executable language.".into();
				return Task::none();
			};
			return queue_plain_run(app_state, language_id, PendingRunKind::Buffer);
		}
		Message::RunSnippet => {
			if let Some(cell) = app_state.code_editor.selected_markdown_cell() {
				return run_markdown_cell(app_state, cell);
			}
			let Some(language_id) = app_state.code_editor.active_language_id() else {
				app_state.status_msg = "The focused pane has no executable language.".into();
				return Task::none();
			};
			return queue_plain_run(app_state, language_id, PendingRunKind::Snippet);
		}
		Message::RunResult {
			result: er,
			publish,
		} => {
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			if let Some(claim) = publish.as_ref() {
				match &er {
					ExecutionResult::Rows { artifact, .. } => {
						let _ = app_state
							.result_registry
							.publish(claim, std::sync::Arc::clone(artifact));
					}
					_ => {
						let _ = app_state.result_registry.conclude_unbound(claim);
					}
				}
			}
			match er {
				ExecutionResult::Affected(rows_affected) => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {rows_affected} row{} affected",
							if rows_affected == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::Batch { statements } => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {statements} statement{} completed",
							if statements == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::CommandCompleted(_) => {
					set_sql_status(app_state, "SQL · command completed");
				}
				ExecutionResult::Err(msg) => {
					set_sql_error(app_state, msg, None);
				}
				ExecutionResult::Rows {
					artifact,
					statements,
				} => {
					let (rows, cols) = artifact.shape();
					let df = match artifact.projection() {
						Ok(frame) => frame,
						Err(error) => {
							set_sql_error(app_state, error.to_string(), None);
							return Task::none();
						}
					};
					app_state.code_editor.begin_query();
					app_state.code_editor.set_query_result(df.as_ref().clone());
					app_state.status_df_size = Some((rows, cols));
					set_sql_status(
						app_state,
						if statements > 1 {
							format!("SQL · {statements} statements completed")
						} else {
							"SQL · query completed".to_owned()
						},
					);
				}
				ExecutionResult::None => {
					set_sql_status(app_state, "SQL · no statements executed");
				}
			}
		}
		Message::RunRuneBufferResult {
			source_buffer,
			revision,
			result,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				Ok(outcome) => {
					app_state.status_error.clear();
					app_state.code_editor.begin_query();
					app_state.code_editor.set_query_message(
						outcome
							.display
							.unwrap_or_else(|| "Rune finished.".to_owned()),
					);
					if let Some(resolved) = outcome.plot {
						let (panel_id, display_label) = {
							let identity = LocalViewIdentity::Anonymous(0);
							let display_label = resolved.resolved_spec.composition.title();
							let presentation = ViewPresentation {
								content_kind: ViewContentKind::Plot,
								display_label: display_label.clone(),
								authored_title: crate::plot::spec::Presence::Inherit,
							};
							app_state.code_editor.prepare_rune_view_panels(
								source_buffer,
								&[(identity.clone(), presentation)],
								revision,
							);
							let panel_id = app_state
								.code_editor
								.rune_view_panel_id(source_buffer, &identity)
								.expect("plain Rune output has a prepared anonymous host panel");
							(panel_id, display_label)
						};
						app_state.status_msg =
							format!("Building plot from {}…", resolved.source_name);
						return build_resolved_plot_task(
							resolved,
							1200,
							move |resolved, kernel| Message::RuneBufferPlotReady {
								source_buffer,
								revision,
								panel_id,
								display_label,
								resolved,
								kernel,
							},
						);
					}
					app_state.code_editor.prepare_rune_view_panels(
						source_buffer,
						&[] as &[(LocalViewIdentity, ViewPresentation)],
						revision,
					);
					app_state.status_msg = "Rune finished.".to_owned();
				}
				Err(message) => {
					app_state
						.code_editor
						.mark_rune_view_producer_stale(source_buffer);
					set_rune_error(app_state, message);
				}
			}
		}
		#[cfg(feature = "toron")]
		Message::RunRunePresentationResult {
			source_buffer,
			revision,
			source,
			result,
		} => {
			let completion = app_state
				.rune_view_scheduler
				.complete_presentation(source_buffer, revision);
			if let Some(next) = completion.next {
				return start_rune_presentation_task(app_state, next);
			}
			if !completion.accepted
				|| app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision)
			{
				return Task::none();
			}
			let Some(result) = result else {
				panic!("an accepted presentation revision must have invoked the local VM");
			};
			match result {
				Ok(view) => {
					return run_rune_relation_view_at_revision(
						app_state,
						source_buffer,
						revision,
						view,
					);
				}
				Err(
					error @ (RuneEnvelopeError::Invalid(_)
					| RuneEnvelopeError::InvalidView { .. }
					| RuneEnvelopeError::DuplicateName { .. }),
				) => {
					app_state
						.code_editor
						.mark_rune_view_producer_stale(source_buffer);
					set_rune_error(app_state, error.to_string());
				}
				Err(RuneEnvelopeError::RecognizedNoOutput) => {
					app_state.code_editor.prepare_rune_view_panels(
						source_buffer,
						&[] as &[(LocalViewIdentity, ViewPresentation)],
						revision,
					);
					app_state.status_msg = "Rune view set is bound; no output root emitted.".into();
					app_state.status_error.clear();
				}
				Err(RuneEnvelopeError::NoTerminalView | RuneEnvelopeError::Parse(_)) => {
					return run_plain_rune_code(app_state, source_buffer, revision, source);
				}
			}
		}
		#[cfg(feature = "toron")]
		Message::RunRuneViewSetResult {
			source_buffer,
			revision,
			origin,
			views,
			result,
		} => {
			if let Some(next) = app_state
				.rune_view_scheduler
				.complete(source_buffer, &origin)
			{
				return start_rune_relation_task(app_state, next);
			}
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				Ok(outcome) => {
					use crate::gui::messages::RuneRelationSinkOutput;
					let sink_count = outcome.sinks.len();
					let mut errors = Vec::new();
					let mut plot_tasks = Vec::new();
					for sink in outcome.sinks {
						assert!(
							std::sync::Arc::ptr_eq(&outcome.result, &sink.source),
							"Rune view sinks must share one stamped result identity"
						);
						let panel_id = app_state
							.code_editor
							.rune_view_panel_id(source_buffer, &sink.identity)
							.expect("a dispatched view has a prepared host panel");
						let sink_facet = sink.facet.clone();
						let figure_grid = sink.figure_grid;
						match sink.output {
							Ok(RuneRelationSinkOutput::Table(presentation)) => {
								let frame = match sink.source.projection() {
									Ok(frame) => frame,
									Err(error) => {
										errors.push(error.to_string());
										continue;
									}
								};
								let rows = frame.height();
								let cols = frame.width();
								app_state.code_editor.set_rune_view_table_with_presentation(
									panel_id,
									frame.as_ref().clone(),
									presentation.skin,
									presentation.authored_show_column_types,
								);
								app_state.status_df_size = Some((rows, cols));
							}
							Ok(RuneRelationSinkOutput::Plot(
								resolved,
								authored_settings,
								continuation,
							)) => {
								let identity = sink.identity.clone();
								let presentation = sink.presentation;
								if let Some(facet) = sink_facet {
									let span = sink.span;
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										Some(facet),
										figure_grid,
										None,
										move |resolved, result| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result: Err(error),
											},
										},
									));
								} else {
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										None,
										figure_grid,
										None,
										move |resolved, result| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span: sink.span,
												result: Err(error),
											},
										},
									));
								}
							}
							Ok(RuneRelationSinkOutput::StyledPlot(
								resolved,
								layer_settings,
								authored_settings,
								continuation,
							)) => {
								let identity = sink.identity.clone();
								let presentation = sink.presentation;
								let span = sink.span;
								if let Some(facet) = sink_facet {
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										Some(facet),
										figure_grid,
										Some(layer_settings),
										move |resolved, result| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result: Err(error),
											},
										},
									));
								} else {
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										None,
										figure_grid,
										Some(layer_settings),
										move |resolved, result| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result: Err(error),
											},
										},
									));
								}
							}
							Ok(RuneRelationSinkOutput::LayeredPlot(
								specification,
								authored_settings,
							)) => {
								let presentation = sink.presentation;
								let span = sink.span;
								if let Some(facet) = sink_facet {
									plot_tasks.push(build_facet_layered_plot_task(
										specification,
										sink.source,
										Some(facet),
										figure_grid,
										move |result| Message::RuneRelationLayeredPlotReady {
											source_buffer,
											revision,
											panel_id,
											presentation,
											authored_settings,
											span,
											result,
										},
									));
								} else {
									plot_tasks.push(build_facet_layered_plot_task(
										specification,
										sink.source,
										None,
										figure_grid,
										move |result| Message::RuneRelationLayeredPlotReady {
											source_buffer,
											revision,
											panel_id,
											presentation,
											authored_settings,
											span,
											result,
										},
									));
								}
							}
							Err(message) => {
								let message = format!(
									"{} at bytes {}..{}: {message}",
									sink.presentation.display_label, sink.span.start, sink.span.end
								);
								app_state.code_editor.fail_rune_view(panel_id);
								errors.push(message);
							}
						}
					}
					app_state.status_msg = format!(
						"Rune workspace rendered {sink_count} shared-result view{}.",
						if sink_count == 1 { "" } else { "s" }
					);
					if errors.is_empty() {
						app_state.status_error.clear();
					} else {
						set_rune_error(app_state, errors.join("\n"));
					}
					return Task::batch(plot_tasks);
				}
				Err(message) => {
					for identity in views {
						if let Some(panel_id) = app_state
							.code_editor
							.rune_view_panel_id(source_buffer, &identity)
						{
							app_state.code_editor.fail_rune_view(panel_id);
						}
					}
					set_rune_error(app_state, message);
				}
			}
		}
		Message::RuneBufferPlotReady {
			source_buffer,
			revision,
			panel_id,
			display_label,
			resolved,
			kernel,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			app_state.code_editor.set_rune_view_plot(
				panel_id,
				display_label,
				kernel,
				PlotSettingsLayer::default(),
			);
			app_state.status_error.clear();
			app_state.status_msg = format!("Plotted {}.", resolved.source_name);
		}
		#[cfg(feature = "toron")]
		Message::RuneRelationPlotReady {
			source_buffer,
			revision,
			panel_id,
			identity,
			presentation,
			authored_settings,
			continuation,
			resolved,
			kernel,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			app_state.code_editor.set_rune_view_plot(
				panel_id,
				presentation.display_label,
				kernel,
				authored_settings,
			);
			app_state.rune_categorical_identities.insert(
				(source_buffer, identity, continuation.palette_len),
				continuation.identity,
			);
			app_state.status_msg = format!("Plotted {}.", resolved.source_name);
		}
		#[cfg(feature = "toron")]
		Message::RuneRelationLayeredPlotReady {
			source_buffer,
			revision,
			panel_id,
			presentation,
			authored_settings,
			span,
			result,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			match result {
				Ok(kernel) => {
					app_state.code_editor.set_rune_view_plot(
						panel_id,
						presentation.display_label.clone(),
						kernel,
						authored_settings,
					);
					app_state.status_msg = format!("Built {}.", presentation.display_label);
				}
				Err(error) => {
					app_state.code_editor.fail_rune_view(panel_id);
					set_rune_error(
						app_state,
						format!(
							"{} at bytes {}..{}: {error}",
							presentation.display_label, span.start, span.end
						),
					);
				}
			}
		}
		Message::RunMarkdownCellResult {
			identity,
			presentation,
			source_start_line,
			result,
		} => {
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				ExecutionResult::Rows {
					artifact,
					statements,
				} => {
					let (rows, cols) = artifact.shape();
					let df = match artifact.projection() {
						Ok(frame) => frame,
						Err(error) => {
							set_sql_error(app_state, error.to_string(), Some(source_start_line));
							return Task::none();
						}
					};
					// Presentation never decides data availability: a named SQL cell
					// remains a Rune input even when it renders in a pane or nowhere.
					app_state
						.code_editor
						.retain_markdown_cell_table(identity.clone(), df.as_ref());
					match presentation.output {
						MarkdownCellOutputTarget::Inline => app_state
							.code_editor
							.set_markdown_cell_table(identity, df.as_ref().clone()),
						MarkdownCellOutputTarget::Pane => {
							app_state.code_editor.begin_query();
							app_state.code_editor.set_query_result(df.as_ref().clone())
						}
						MarkdownCellOutputTarget::None => {}
					}
					app_state.status_df_size = Some((rows, cols));
					set_sql_status(
						app_state,
						if statements > 1 {
							format!("SQL · {statements} statements completed")
						} else {
							"SQL · cell query completed".to_owned()
						},
					);
				}
				ExecutionResult::Err(message) => {
					set_sql_error(app_state, message, Some(source_start_line));
				}
				ExecutionResult::Affected(rows) => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {rows} row{} affected",
							if rows == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::CommandCompleted(_) => {
					set_sql_status(app_state, "SQL · command completed");
				}
				ExecutionResult::Batch { statements } => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {statements} statement{} completed",
							if statements == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::None => {
					set_sql_status(app_state, "SQL · no statements executed");
				}
			}
		}
		Message::RunRuneCellResult {
			identity,
			presentation,
			result,
		} => {
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				Ok(resolved) => {
					app_state.status_error.clear();
					app_state.status_msg = format!("Building plot from {}…", resolved.source_name);
					return build_resolved_plot_task(resolved, 720, move |resolved, kernel| {
						Message::NotebookPlotReady {
							identity,
							presentation,
							resolved,
							kernel,
						}
					});
				}
				Err(message) => {
					set_rune_error(app_state, message.clone());
					if presentation.output == MarkdownCellOutputTarget::Inline {
						app_state.code_editor.set_markdown_cell_error(
							identity,
							"failed · see status bar".to_owned(),
						);
					}
				}
			}
		}
		Message::NotebookPlotReady {
			identity,
			presentation,
			resolved,
			kernel,
		} => {
			let composition = resolved.resolved_spec.composition.clone();
			match presentation.output {
				MarkdownCellOutputTarget::Inline => {
					app_state
						.code_editor
						.set_markdown_cell_plot(identity, composition, kernel)
				}
				MarkdownCellOutputTarget::Pane => {
					app_state.code_editor.begin_query();
					app_state
						.code_editor
						.set_query_plot(composition.title(), kernel);
				}
				MarkdownCellOutputTarget::None => {}
			}
			app_state.status_error.clear();
			app_state.status_msg = "Plot cell finished.".to_owned();
		}
		Message::Export(format) => {
			let snapshots = app_state.code_editor.host_plot_exports();
			if snapshots.is_empty() {
				app_state.status_msg = "No live plots to export.".to_owned();
				return Task::none();
			}
			let export_jobs = snapshots
				.into_iter()
				.enumerate()
				.map(|(index, snapshot)| render_plot_export(snapshot, format, index + 1))
				.collect::<Vec<_>>();
			let count = export_jobs.len();
			app_state.status_msg = format!("Exporting {count} plots as {format}...");
			return Task::perform(
				async move {
					tokio::task::spawn_blocking(move || {
						for job in export_jobs {
							job.write_to(std::path::Path::new("."));
						}
					})
					.await
					.unwrap();
				},
				move |()| Message::ExportDone(count, format),
			);
		}
		Message::ExportDone(count, format) => {
			app_state.status_msg = format!("Exported {count} plots as {format}.");
		}
		Message::ConnectionNameChanged(name) => {
			app_state.adapter_state.name = name;
		}
		Message::OpenSettings => {
			app_state.showing_settings = true;
		}
		Message::CloseSettings => {
			app_state.showing_settings = false;
		}
		Message::ToggleShowColumnTypes(val) => {
			app_state.show_column_types = val;
			app_state.code_editor.set_show_column_types(val);
			app_state.status_msg = format!(
				"Settings · column types {} for this session",
				if val { "shown" } else { "hidden" },
			);
		}
	}
	Task::none()
}

fn menu_action_to_message(action: &str) -> Option<Message> {
	match action {
		"connect:new" => Some(Message::Connect),
		"code:run" => Some(Message::Run),
		"code:run-snippet" => Some(Message::RunSnippet),
		"settings:preferences" => Some(Message::OpenSettings),
		"session:checkpoint" => Some(Message::CheckpointSession),
		"session:copy-id" => Some(Message::CopySessionId),
		"session:close-without-checkpoint" => Some(Message::CloseWithoutCheckpoint),
		_ => {
			if let Some(format) = action.strip_prefix("plot:export:") {
				return parse_export_format(format).map(Message::Export);
			}
			None
		}
	}
}

fn parse_export_format(format: &str) -> Option<ExportFormat> {
	match format {
		"SVG" => Some(ExportFormat::SVG),
		"PNG" => Some(ExportFormat::PNG),
		"AVIF" => Some(ExportFormat::AVIF),
		_ => None,
	}
}

/// The window rectangle occupied by Baryon's workspace: the window minus the
/// application chrome and status bar.
fn workspace_bounds(size: Size) -> iced::Rectangle {
	iced::Rectangle {
		x: 10.0,
		y: 71.0,
		width: (size.width - 20.0).max(1.0),
		height: (size.height - 113.0).max(1.0),
	}
}

/// Keep libaryon's terminal-cell viewport aligned with its direct workspace
/// surface. There is no Polariton-owned outer split layout to reconcile.
fn sync_workspace_viewport(app_state: &mut AppState) {
	let bounds = workspace_bounds(app_state.window_size);
	app_state.code_editor.resize(bounds.width, bounds.height);
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "toron")]
	use super::{AdapterState, view};
	#[cfg(feature = "toron")]
	use super::{
		SavedCloseState, StartupAuthority, dispatch_rune_relation, new_with_authority,
		resolve_rune_table_skin_with, rune_view_outcome,
	};
	use super::{
		clear_transient_execution_status_on_editor_activity, compact_rune_diagnostic,
		compact_sql_diagnostic, new, resolve_startup, session_identity_text, set_rune_error,
		set_sql_error, update,
	};
	use crate::adapters::common::ExecutionResult;
	#[cfg(feature = "toron")]
	use crate::adapters::common::{
		AdapterStage, DatabaseAdapter, RuneDispatchFailure, RuneDispatchFailureKind,
	};
	use crate::configuration::StartupData;
	use crate::gui::messages::Message;
	#[cfg(feature = "toron")]
	use crate::gui::table_skin::{TableSkin, TableSkinProjection, TableSkinSpec};
	#[cfg(feature = "toron")]
	use crate::plot::core::PlotType;
	#[cfg(feature = "toron")]
	use crate::plot::factory::{create_layered_plot, create_plot, create_styled_plot};
	#[cfg(feature = "toron")]
	use crate::plot::grid::PlotGrid;
	#[cfg(feature = "toron")]
	use crate::plot::{
		common::{CoordinateTransformer, PlotKernel, PlotLayout, ResolvedPlotSettings},
		export::SvgBackend,
	};
	#[cfg(feature = "toron")]
	use crate::rune_relation_envelope::{
		LocalViewIdentity, PendingRuneView, RuneTableSkinIntent, RuneTableSkinProjection,
		RuneViewForm, TransformedRuneRelation, TransformedRuneView,
		transform as transform_rune_view,
	};
	#[cfg(feature = "toron")]
	use libaryon::core::BufferId;
	#[cfg(feature = "toron")]
	use nilket_local::{
		Artifact, Evidence, LocalWorkspace, ObservationContext, ReadyWorkspace, StaleCause,
		VersionedArtifact, WorkspaceIdentity,
	};
	#[cfg(feature = "toron")]
	use std::collections::HashMap;
	#[cfg(feature = "toron")]
	use std::sync::{Arc, Mutex};
	#[cfg(feature = "toron")]
	use tokio::sync::RwLock;

	#[cfg(feature = "toron")]
	fn complete_rune_presentation(
		state: &mut super::AppState,
		source_buffer: BufferId,
		source: &str,
	) -> iced::Task<Message> {
		let revision = *state
			.rune_buffer_revisions
			.get(&source_buffer)
			.expect("run assigned a revision");
		update(
			state,
			Message::RunRunePresentationResult {
				source_buffer,
				revision,
				source: source.to_owned(),
				result: Some(crate::rune_view_compiler::compile(source)),
			},
		)
	}

	#[cfg(feature = "toron")]
	fn render_marks(kernel: &dyn PlotKernel) -> String {
		render_marks_with_settings(kernel, ResolvedPlotSettings::default())
	}

	#[test]
	fn product_0071_copied_identity_remains_the_exact_cli_authority() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let id = "1234567890abcdef1234567890abcdef".parse().unwrap();
		state.session_id = Some(id);
		let copied = session_identity_text(id);
		assert_eq!(copied.as_bytes().len(), 32);
		assert_eq!(copied, "1234567890abcdef1234567890abcdef");
		assert_eq!(
			copied.parse(),
			Ok(id),
			"copied identity selects the same session"
		);
		state.code_editor.shutdown();
	}

	#[cfg(not(feature = "toron"))]
	#[test]
	fn product_0063_no_toron_build_refuses_saved_authority_explicitly() {
		let Err(error) = resolve_startup(super::StartupTarget::CreateSaved) else {
			panic!("a no-toron build must refuse saved authority")
		};
		assert_eq!(error, "saved sessions require Polariton's `toron` feature");
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0063_saved_startup_checkpoint_close_and_reopen_use_production_authority() {
		const CHILD: &str = "POLARITON_PRODUCT_0063_SURFACE_CHILD";
		if std::env::var_os(CHILD).is_none() {
			let state_home = std::env::temp_dir().join(format!(
				"polariton-product-0063-{}-{}",
				std::process::id(),
				std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_nanos()
			));
			let status = std::process::Command::new(std::env::current_exe().unwrap())
				.args([
					"--exact",
					"gui::state::tests::product_0063_saved_startup_checkpoint_close_and_reopen_use_production_authority",
					"--nocapture",
				])
				.env(CHILD, "1")
				.env("XDG_STATE_HOME", &state_home)
				.status()
				.expect("spawn isolated session-surface fixture");
			let _ = std::fs::remove_dir_all(&state_home);
			assert!(status.success(), "isolated session-surface fixture failed");
			return;
		}

		let authority = resolve_startup(super::StartupTarget::CreateSaved).unwrap();
		let session_id = match &authority {
			StartupAuthority::Saved { session_id, .. } => *session_id,
			StartupAuthority::Ephemeral { .. } => panic!("create must resolve saved authority"),
		};
		let (mut state, startup) = new_with_authority(StartupData::default(), authority).unwrap();
		drop(startup);
		assert_eq!(state.session_id, Some(session_id));

		let contention = resolve_startup(super::StartupTarget::OpenSaved(session_id));
		let Err(contention) = contention else {
			panic!("the production resolver must not switch authority on contention")
		};
		assert!(contention.contains(&session_id.to_string()));
		assert!(contention.contains("already open incompatibly"));
		let ordinary = resolve_startup(super::StartupTarget::Ephemeral { initial_file: None })
			.expect("saved sessions in the store never change default authority");
		assert!(matches!(
			ordinary,
			StartupAuthority::Ephemeral { initial_file: None }
		));

		drop(update(&mut state, Message::CheckpointSession));
		assert_eq!(state.saved_close_state, SavedCloseState::Open);
		assert!(state.status_error.contains("viewport is not ready"));
		assert_eq!(
			update(&mut state, Message::CloseWithoutCheckpoint).units(),
			0,
			"a failed manual checkpoint never arms destructive close"
		);

		let first_close = update(&mut state, Message::CloseWindow);
		assert_eq!(
			first_close.units(),
			0,
			"unready checkpoint keeps the window open"
		);
		assert_eq!(state.saved_close_state, SavedCloseState::Failed);
		assert!(state.status_error.contains("viewport is not ready"));

		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state.status_msg != "Session checkpointed" && std::time::Instant::now() < deadline {
			state.code_editor.poll();
			drop(update(&mut state, Message::CheckpointSession));
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(state.status_msg, "Session checkpointed");
		assert_eq!(state.saved_close_state, SavedCloseState::Open);
		drop(update(
			&mut state,
			Message::CodeEditEvent(crate::editor::EditorMsg::EngineReady),
		));
		assert!(state.rune_categorical_identities.is_empty());
		let producer = state.code_editor.active_buffer_id().unwrap();
		let stale_view = LocalViewIdentity::Named("stale-witness".to_owned());
		let companion_view = LocalViewIdentity::Named("companion-table".to_owned());
		state.code_editor.prepare_rune_view_panels(
			producer,
			&[
				(stale_view.clone(), RuneViewForm::Bar),
				(companion_view.clone(), RuneViewForm::Table),
			],
			1,
		);
		let durable_panel = state
			.code_editor
			.rune_view_panel_id(producer, &stale_view)
			.unwrap();
		let companion_panel = state
			.code_editor
			.rune_view_panel_id(producer, &companion_view)
			.unwrap();
		use crate::plot::spec::{
			AllocationSpaceSnapshot, ColorAssignmentSnapshot, ColorIdentity, ColorIdentitySnapshot,
			ColorLayerIdentity, ColorSpaceSnapshot, LayerIdentity,
		};
		let authored_colour = ColorIdentity::from_persistence_snapshot(ColorIdentitySnapshot {
			assignments: vec![ColorAssignmentSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::HorizontalBar,
				}),
				category: "Atlas".to_owned(),
				position: 2,
			}],
			spaces: vec![ColorSpaceSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::HorizontalBar,
				}),
				space: AllocationSpaceSnapshot::CategoricalPalette { len: 4 },
			}],
		})
		.unwrap();
		state
			.rune_categorical_identities
			.insert((producer, stale_view.clone(), 4), authored_colour);
		let stale_close = update(&mut state, Message::CloseWindow);
		assert_eq!(stale_close.units(), 0);
		assert_eq!(state.saved_close_state, SavedCloseState::Failed);
		assert!(state.status_error.contains("close again to retry"));
		assert!(state.status_error.contains("stale application snapshot"));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while (state
			.code_editor
			.rune_view_panel_layout_signature(producer, &stale_view)
			.is_none()
			|| state
				.code_editor
				.rune_view_panel_layout_signature(producer, &companion_view)
				.is_none())
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert!(
			state
				.code_editor
				.rune_view_panel_layout_signature(producer, &stale_view)
				.is_some(),
			"next projected census contains the new panel before deliberate retry"
		);
		let durable_layout = state
			.code_editor
			.rune_view_panel_persistence_signature(producer, &stale_view)
			.unwrap();
		let companion_layout = state
			.code_editor
			.rune_view_panel_persistence_signature(producer, &companion_view)
			.unwrap();
		assert_eq!(
			durable_layout.0, companion_layout.0,
			"one Rune producer owns one durable panel group"
		);

		use libaryon::engine::{InputEvent, KeyCode, Modifiers};
		for key in ":pv rune".chars() {
			drop(update(
				&mut state,
				Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
					code: KeyCode::Char(key),
					modifiers: Modifiers::NONE,
				})),
			));
		}
		drop(update(
			&mut state,
			Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Enter,
				modifiers: Modifiers::NONE,
			})),
		));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.active_language_id()
			.is_none_or(|language| language.as_str() != "rune")
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state.code_editor.active_language_id().map(|id| id.as_str()),
			Some("rune")
		);
		const DIRTY_CLOSE_BYTES: &str = "// dirty close checkpoint";
		for key in std::iter::once('i').chain(DIRTY_CLOSE_BYTES.chars()) {
			drop(update(
				&mut state,
				Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
					code: KeyCode::Char(key),
					modifiers: Modifiers::NONE,
				})),
			));
		}
		drop(update(
			&mut state,
			Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Esc,
				modifiers: Modifiers::NONE,
			})),
		));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while !state
			.code_editor
			.cached_content()
			.contains(DIRTY_CLOSE_BYTES)
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert!(
			state
				.code_editor
				.cached_content()
				.contains(DIRTY_CLOSE_BYTES)
		);
		drop(update(&mut state, Message::Run));
		assert_eq!(state.pending_runs.len(), 1, "run waits on its snapshot");

		let owner = crate::local_workspace::LocalWorkspaceOwner::launch_command(
			std::process::Command::new("/bin/true"),
			nix::sys::signal::Signal::SIGINT,
		);
		state.local_workspace_owner = Some(owner);
		let close = update(&mut state, Message::CloseWindow);
		assert_eq!(close.units(), 1, "saved close emits one bounded drain task");
		assert_eq!(state.saved_close_state, SavedCloseState::Frozen);
		assert!(state.closing_local_workspace);
		assert!(
			state.pending_runs.is_empty(),
			"undispatched run is abandoned"
		);
		let status = state.status_msg.clone();
		assert_eq!(update(&mut state, Message::Run).units(), 0);
		assert_eq!(
			update(
				&mut state,
				Message::RunResult {
					result: ExecutionResult::Err("late completion".to_owned()),
					publish: None,
				},
			)
			.units(),
			0
		);
		assert_eq!(update(&mut state, Message::ScratchReady(None)).units(), 0);
		assert_eq!(
			state.status_msg, status,
			"run initiation and asynchronous completions are discarded"
		);
		assert!(state.status_error.is_empty());
		let finish = update(
			&mut state,
			Message::LocalWorkspaceStopped("Stopped".to_owned()),
		);
		assert_eq!(
			finish.units(),
			1,
			"drain completion alone closes the window"
		);
		assert!(state.status_msg.contains("Local workspace stopped"));
		drop(finish);
		drop(close);
		state.code_editor.shutdown();
		drop(state);

		let reopened = resolve_startup(super::StartupTarget::OpenSaved(session_id)).unwrap();
		let (mut state, startup) = new_with_authority(StartupData::default(), reopened).unwrap();
		drop(startup);
		assert_eq!(state.session_id, Some(session_id));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while (state
			.code_editor
			.rune_view_producer_for_panel(durable_panel, &stale_view)
			.is_none()
			|| state
				.code_editor
				.rune_view_producer_for_panel(companion_panel, &companion_view)
				.is_none()
			|| state.rune_categorical_identities.is_empty())
			&& std::time::Instant::now() < deadline
		{
			drop(update(
				&mut state,
				Message::CodeEditEvent(crate::editor::EditorMsg::EngineReady),
			));
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		let reopened_producer = state
			.code_editor
			.rune_view_producer_for_panel(durable_panel, &stale_view)
			.expect("restored panel meaning identifies its producer");
		assert!(
			state
				.code_editor
				.cached_content()
				.contains(DIRTY_CLOSE_BYTES),
			"successful close persists the dirty focused scratch before teardown"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_producer_for_panel(companion_panel, &companion_view),
			Some(reopened_producer),
			"both restored named panels join to the same durable producer"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &stale_view),
			Some(durable_panel),
			"production reopen installs the exact durable named-panel meaning"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(reopened_producer, &stale_view),
			Some("fresh"),
			"startup restores placement and meaning without executing Rune"
		);
		assert!(
			!state
				.code_editor
				.rune_view_panel_is_plot(reopened_producer, &stale_view)
				&& !state
					.code_editor
					.rune_view_panel_is_table(reopened_producer, &stale_view),
			"fresh is the registry spelling for an unresolved restored placeholder"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &companion_view),
			Some(companion_panel)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(reopened_producer, &companion_view),
			Some("fresh")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_persistence_signature(reopened_producer, &stale_view),
			Some(durable_layout),
			"the named plot returns to its exact group and logical placement"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_persistence_signature(reopened_producer, &companion_view),
			Some(companion_layout),
			"the named table returns to its exact group and logical placement"
		);
		let canonical_colour = ColorIdentity::from_persistence_snapshot(ColorIdentitySnapshot {
			assignments: vec![ColorAssignmentSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::Bar,
				}),
				category: "Atlas".to_owned(),
				position: 2,
			}],
			spaces: vec![ColorSpaceSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::Bar,
				}),
				space: AllocationSpaceSnapshot::CategoricalPalette { len: 4 },
			}],
		})
		.unwrap();
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(reopened_producer, stale_view.clone(), 4)),
			Some(&canonical_colour),
			"orientation changes geometry, never durable colour identity; restored keys: {:?}",
			state.rune_categorical_identities.keys().collect::<Vec<_>>()
		);
		state.code_editor.prepare_rune_view_panels(
			reopened_producer,
			&[
				(stale_view.clone(), RuneViewForm::Bar),
				(companion_view.clone(), RuneViewForm::Table),
			],
			2,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &stale_view),
			Some(durable_panel),
			"explicit rerun resolves in the restored panel"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &companion_view),
			Some(companion_panel),
			"both explicit rerun outputs resolve in place"
		);
		let discard_only_view = LocalViewIdentity::Named("discard-only".to_owned());
		state.code_editor.prepare_rune_view_panels(
			reopened_producer,
			&[(discard_only_view, RuneViewForm::Table)],
			3,
		);
		assert_eq!(update(&mut state, Message::CloseWindow).units(), 0);
		assert_eq!(state.saved_close_state, SavedCloseState::Failed);
		let discard = update(&mut state, Message::CloseWithoutCheckpoint);
		assert_eq!(discard.units(), 1, "explicit discard emits the close task");
		assert_eq!(state.saved_close_state, SavedCloseState::Frozen);
		drop(discard);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0063_sigkill_is_not_a_close_checkpoint() {
		const MODE: &str = "POLARITON_PRODUCT_0063_CRASH_MODE";
		const SESSION_FILE: &str = "POLARITON_PRODUCT_0063_SESSION_FILE";
		const SESSION_ID: &str = "POLARITON_PRODUCT_0063_SESSION_ID";
		const UNCOMMITTED: &str = "not checkpointed before sigkill";

		match std::env::var(MODE).as_deref() {
			Ok("holder") => {
				let authority = resolve_startup(super::StartupTarget::CreateSaved).unwrap();
				let session_id = match &authority {
					StartupAuthority::Saved { session_id, .. } => *session_id,
					StartupAuthority::Ephemeral { .. } => panic!("holder must be saved"),
				};
				let (mut state, startup) =
					new_with_authority(StartupData::default(), authority).unwrap();
				drop(startup);
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while state.status_msg != "Session checkpointed"
					&& std::time::Instant::now() < deadline
				{
					state.code_editor.poll();
					drop(update(&mut state, Message::CheckpointSession));
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				assert_eq!(state.status_msg, "Session checkpointed");

				use libaryon::engine::{InputEvent, KeyCode, Modifiers};
				for key in std::iter::once('i').chain(UNCOMMITTED.chars()) {
					drop(update(
						&mut state,
						Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
							code: KeyCode::Char(key),
							modifiers: Modifiers::NONE,
						})),
					));
				}
				drop(update(
					&mut state,
					Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
						code: KeyCode::Esc,
						modifiers: Modifiers::NONE,
					})),
				));
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while !state.code_editor.cached_content().contains(UNCOMMITTED)
					&& std::time::Instant::now() < deadline
				{
					state.code_editor.poll();
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				assert!(state.code_editor.cached_content().contains(UNCOMMITTED));
				std::fs::write(
					std::env::var_os(SESSION_FILE).unwrap(),
					session_id.to_string(),
				)
				.unwrap();
				loop {
					std::thread::park();
				}
			}
			Ok("verify") => {
				let session_id = std::env::var(SESSION_ID).unwrap().parse().unwrap();
				let authority =
					resolve_startup(super::StartupTarget::OpenSaved(session_id)).unwrap();
				let (mut state, startup) =
					new_with_authority(StartupData::default(), authority).unwrap();
				drop(startup);
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while state.code_editor.cached_content().contains(UNCOMMITTED)
					|| state.code_editor.active_buffer_id().is_none()
				{
					assert!(
						std::time::Instant::now() < deadline,
						"reopen must project the predecessor"
					);
					state.code_editor.poll();
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				assert!(
					!state.code_editor.cached_content().contains(UNCOMMITTED),
					"SIGKILL and Drop publish no edits"
				);
				state.code_editor.shutdown();
			}
			Ok(other) => panic!("unknown crash fixture mode {other}"),
			Err(_) => {
				let root = std::env::temp_dir().join(format!(
					"polariton-product-0063-crash-{}-{}",
					std::process::id(),
					std::time::SystemTime::now()
						.duration_since(std::time::UNIX_EPOCH)
						.unwrap()
						.as_nanos()
				));
				std::fs::create_dir_all(&root).unwrap();
				let session_file = root.join("session-id");
				let exact = "gui::state::tests::product_0063_sigkill_is_not_a_close_checkpoint";
				let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
					.args(["--exact", exact, "--nocapture"])
					.env(MODE, "holder")
					.env(SESSION_FILE, &session_file)
					.env("XDG_STATE_HOME", &root)
					.spawn()
					.unwrap();
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while !session_file.exists() && std::time::Instant::now() < deadline {
					assert!(
						holder.try_wait().unwrap().is_none(),
						"holder exited before signal"
					);
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				let session_id = std::fs::read_to_string(&session_file).unwrap();
				nix::sys::signal::kill(
					nix::unistd::Pid::from_raw(holder.id() as i32),
					nix::sys::signal::Signal::SIGKILL,
				)
				.unwrap();
				let status = holder.wait().unwrap();
				assert!(!status.success(), "holder must die by SIGKILL");

				let verifier = std::process::Command::new(std::env::current_exe().unwrap())
					.args(["--exact", exact, "--nocapture"])
					.env(MODE, "verify")
					.env(SESSION_ID, session_id.trim())
					.env("XDG_STATE_HOME", &root)
					.status()
					.unwrap();
				let _ = std::fs::remove_dir_all(&root);
				assert!(
					verifier.success(),
					"the lock releases and the predecessor reopens"
				);
			}
		}
	}

	#[cfg(feature = "toron")]
	fn render_marks_with_settings(
		kernel: &dyn PlotKernel,
		settings: ResolvedPlotSettings,
	) -> String {
		let bounds = iced::Rectangle::new(iced::Point::ORIGIN, iced::Size::new(100.0, 100.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(100.0, 100.0);
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			iced::advanced::mouse::Cursor::Unavailable,
			settings,
		);
		backend.finish()
	}
	#[cfg(feature = "toron")]
	fn local_identity() -> WorkspaceIdentity {
		WorkspaceIdentity {
			profile_root: "/tmp/polariton-product-0005-fixture".into(),
			profile_uuid: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
			session_nonce: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
			owner_pid: 42,
			started_unix_ms: 7,
		}
	}

	#[cfg(feature = "toron")]
	struct RecordingRuneAdapter {
		calls: Arc<Mutex<Vec<String>>>,
		fail_table: Option<&'static str>,
	}

	#[cfg(feature = "toron")]
	#[async_trait::async_trait]
	impl DatabaseAdapter for RecordingRuneAdapter {
		async fn dispatch(&mut self, _code: &str) -> ExecutionResult {
			ExecutionResult::Err("SQL dispatch is unused".to_owned())
		}

		async fn dispatch_rune(
			&mut self,
			source: &str,
		) -> crate::adapters::common::RuneDispatchResult {
			self.calls
				.lock()
				.expect("recording lock")
				.push(source.to_owned());
			if self
				.fail_table
				.is_some_and(|table| source.contains(&format!("table(\"{table}\")")))
			{
				return Err(RuneDispatchFailure {
					kind: RuneDispatchFailureKind::Execution,
					message: "fixture origin failed".to_owned(),
					span: None,
				});
			}
			Ok(crate::result_artifact::ResultArtifact::test_frame(
				polars::df!("x" => [1_i64], "y" => [2_i64]).expect("fixture frame"),
			))
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0084_rune_transport_preserves_one_artifact_allocation() {
		let relation =
			transform_rune_view("workspace.table(\"t\").views([table(), scatter(\"x\", \"y\")])")
				.expect("Rune relation fixture")
				.into_relations()
				.next()
				.expect("one relation");
		let artifact = crate::result_artifact::ResultArtifact::test_frame(
			polars::df!("x" => [1_i64], "y" => [2_i64]).expect("fixture frame"),
		);
		let transport: crate::adapters::common::RuneDispatchResult = Ok(Arc::clone(&artifact));
		let transported = transport.expect("Rune transport accepts the artifact");
		assert!(Arc::ptr_eq(&artifact, &transported));
		let palettes = super::default_rune_categorical_palettes(&relation.views);
		let outcome = super::rune_view_outcome_with_palettes(transported, relation.views, palettes)
			.expect("views resolve");
		assert!(Arc::ptr_eq(&artifact, &outcome.result));
		assert!(
			outcome
				.sinks
				.iter()
				.all(|sink| Arc::ptr_eq(&artifact, &sink.source))
		);
	}

	#[cfg(feature = "toron")]
	fn complete_relation_in_head(
		state: &mut super::AppState,
		source_buffer: libaryon::core::BufferId,
		revision: u64,
		relation: TransformedRuneRelation,
		frame: polars::frame::DataFrame,
	) -> Vec<(LocalViewIdentity, crate::plot::request::ResolvedPlot)> {
		enum ReadyPlot {
			Single(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::request::ResolvedPlot,
				crate::plot::theme::PlotSettingsLayer,
				crate::gui::messages::RuneCategoricalContinuation,
			),
			Styled(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::request::ResolvedPlot,
				crate::plot::spec::LayerSettings,
				crate::plot::theme::PlotSettingsLayer,
				crate::gui::messages::RuneCategoricalContinuation,
			),
			Layered(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::spec::LayeredPlotSpec,
				crate::plot::theme::PlotSettingsLayer,
				Arc<crate::result_artifact::ResultArtifact>,
				std::ops::Range<u32>,
			),
		}
		let origin = relation.origin.clone();
		let views = relation
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		let palettes = super::prepare_rune_categorical_palettes(state, source_buffer, &relation);
		let outcome = super::rune_view_outcome_with_palettes(
			crate::result_artifact::ResultArtifact::test_frame(frame),
			relation.views,
			palettes,
		)
		.expect("fixture projects");
		let plots = outcome
			.sinks
			.iter()
			.filter_map(|sink| match &sink.output {
				Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
					resolved,
					authored_settings,
					continuation,
				)) => Some((
					sink.identity.clone(),
					ReadyPlot::Single(
						sink.presentation.clone(),
						resolved.clone(),
						authored_settings.clone(),
						continuation.clone(),
					),
				)),
				Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
					resolved,
					layer_settings,
					authored_settings,
					continuation,
				)) => Some((
					sink.identity.clone(),
					ReadyPlot::Styled(
						sink.presentation.clone(),
						resolved.clone(),
						layer_settings.clone(),
						authored_settings.clone(),
						continuation.clone(),
					),
				)),
				Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(
					specification,
					authored_settings,
				)) => Some((
					sink.identity.clone(),
					ReadyPlot::Layered(
						sink.presentation.clone(),
						specification.clone(),
						authored_settings.clone(),
						Arc::clone(&sink.source),
						sink.span.clone(),
					),
				)),
				_ => None,
			})
			.collect::<Vec<_>>();
		let resolved_plots = plots
			.iter()
			.filter_map(|(identity, plot)| match plot {
				ReadyPlot::Single(_, resolved, _, _) | ReadyPlot::Styled(_, resolved, _, _, _) => {
					Some((identity.clone(), resolved.clone()))
				}
				ReadyPlot::Layered(..) => None,
			})
			.collect::<Vec<_>>();

		drop(update(
			state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision,
				origin,
				views,
				result: Ok(outcome),
			},
		));
		for (identity, plot) in plots {
			let panel_id = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("prepared plot panel");
			match plot {
				ReadyPlot::Single(presentation, resolved, authored_settings, continuation) => {
					let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
					drop(update(
						state,
						Message::RuneRelationPlotReady {
							source_buffer,
							revision,
							panel_id,
							identity,
							presentation,
							authored_settings,
							continuation,
							resolved,
							kernel,
						},
					));
				}
				ReadyPlot::Styled(
					presentation,
					resolved,
					layer_settings,
					authored_settings,
					continuation,
				) => {
					let kernel =
						create_styled_plot(&resolved.resolved_spec, &resolved.data, layer_settings)
							.expect("styled plot kernel");
					drop(update(
						state,
						Message::RuneRelationPlotReady {
							source_buffer,
							revision,
							panel_id,
							identity,
							presentation,
							authored_settings,
							continuation,
							resolved,
							kernel,
						},
					));
				}
				ReadyPlot::Layered(
					presentation,
					specification,
					authored_settings,
					source,
					span,
				) => {
					let result = source
						.projection()
						.map_err(|error| error.to_string())
						.and_then(|frame| {
							create_layered_plot(&specification, frame.as_ref(), 1200, 720)
						});
					drop(update(
						state,
						Message::RuneRelationLayeredPlotReady {
							source_buffer,
							revision,
							panel_id,
							presentation,
							authored_settings,
							span,
							result,
						},
					));
				}
			}
		}
		resolved_plots
	}

	#[cfg(feature = "toron")]
	fn run_one_relation_program(
		state: &mut super::AppState,
		source_buffer: BufferId,
		revision: u64,
		source: &str,
		frame: polars::frame::DataFrame,
	) -> Vec<(LocalViewIdentity, crate::plot::request::ResolvedPlot)> {
		let program = transform_rune_view(source).expect("Rune relation program");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, revision);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program.clone(),
				})
				.is_some(),
			"revision must be schedulable"
		);
		complete_relation_in_head(
			state,
			source_buffer,
			revision,
			program.into_relations().next().expect("one relation"),
			frame,
		)
	}

	#[cfg(feature = "toron")]
	fn band_mapping(
		resolved: &crate::plot::request::ResolvedPlot,
	) -> Vec<(String, usize, (u8, u8, u8))> {
		resolved
			.resolved_spec
			.band_fills
			.iter()
			.map(|assignment| {
				(
					assignment.category.clone(),
					assignment.position.index(),
					(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
					),
				)
			})
			.collect()
	}

	#[cfg(feature = "toron")]
	fn categorical_bar_frame(categories: &[&str]) -> polars::frame::DataFrame {
		polars::frame::DataFrame::new(
			categories.len(),
			vec![
				polars::prelude::Column::new(
					"cat".into(),
					categories.iter().copied().collect::<Vec<_>>(),
				),
				polars::prelude::Column::new(
					"val".into(),
					(1..=categories.len() as i64).collect::<Vec<_>>(),
				),
			],
		)
		.expect("categorical bar frame")
	}

	#[cfg(feature = "toron")]
	fn non_ready_local_workspaces() -> Vec<LocalWorkspace> {
		let identity = local_identity();
		let root = identity.profile_root.clone();
		let runtime_context = ObservationContext::Runtime(identity.clone());
		vec![
			LocalWorkspace::Absent {
				profile_root: root.clone(),
			},
			LocalWorkspace::Starting {
				profile_root: root,
				owner_pid: identity.owner_pid,
			},
			LocalWorkspace::Unhealthy {
				identity: identity.clone(),
				cause: "fixture health failure".to_owned(),
			},
			LocalWorkspace::Stale {
				identity: identity.clone(),
				cause: StaleCause::PidRecycled,
			},
			LocalWorkspace::Corrupt {
				context: runtime_context.clone(),
				artifact: Artifact::RuntimeManifest,
				cause: "fixture corruption".to_owned(),
			},
			LocalWorkspace::Incompatible {
				context: runtime_context.clone(),
				artifact: VersionedArtifact::RuntimeManifest,
				found: 2,
				supported: 1,
			},
			LocalWorkspace::Unverifiable {
				context: runtime_context,
				evidence: Evidence::StableIdentity,
				cause: "fixture churn".to_owned(),
			},
		]
	}

	#[cfg(feature = "toron")]
	#[test]
	fn local_discovery_attaches_only_ready_workspaces() {
		for workspace in non_ready_local_workspaces() {
			let is_absent = matches!(workspace, LocalWorkspace::Absent { .. });
			let expected_status = workspace.to_string();
			let (mut state, startup) = new(StartupData::default(), None);
			drop(startup);
			let attach = update(
				&mut state,
				Message::LocalWorkspaceDiscovered(workspace.clone()),
			);
			assert_eq!(
				attach.units(),
				0,
				"non-Ready discovery must construct no connection client: {workspace}"
			);
			assert!(matches!(state.adapter_state.stage, AdapterStage::None));
			assert!(state.adapter_state.connection.is_none());
			assert_eq!(state.adapter_state.local_workspace, Some(workspace));
			assert_eq!(state.status_error, expected_status);
			assert_eq!(state.local_creation.offer().is_some(), is_absent);
			if is_absent {
				assert!(state.status_msg.contains("Start a session-bound workspace"));
				let offer_frame = view(&state);
				drop(offer_frame);
			} else {
				assert_eq!(state.status_msg, "SQL scratch remains active.");
			}
			state.code_editor.shutdown();
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let ready = ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity: local_identity(),
		};
		let attach = update(
			&mut state,
			Message::LocalWorkspaceDiscovered(LocalWorkspace::Ready(ready.clone())),
		);
		assert_eq!(
			attach.units(),
			1,
			"Ready is the positive control for connection construction"
		);
		assert_eq!(state.status_msg, "Attaching Toron workspace…");
		assert!(
			!state
				.status_msg
				.contains(&ready.identity.session_nonce.to_string()),
			"ordinary chrome must not expose the Toron workspace nonce"
		);
		assert!(state.local_creation.offer().is_none());
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn first_frame_does_not_wait_for_local_discovery() {
		let (mut state, startup) = new(StartupData::default(), None);
		assert_eq!(
			startup.units(),
			2,
			"scratch open and local discovery must remain independent async work"
		);
		assert!(state.adapter_state.local_workspace.is_none());
		let first_frame = view(&state);
		drop(first_frame);
		assert!(
			state.adapter_state.local_workspace.is_none(),
			"rendering must not synchronously await discovery"
		);
		drop(startup);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0071_joined_toron_workspace_chrome_omits_the_nonce() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let workspace = ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity: local_identity(),
		};
		let nonce = workspace.identity.session_nonce.to_string();
		let connection: Arc<RwLock<dyn DatabaseAdapter>> =
			Arc::new(RwLock::new(RecordingRuneAdapter {
				calls: Arc::new(Mutex::new(Vec::new())),
				fail_table: None,
			}));

		assert_eq!(
			update(
				&mut state,
				Message::LocalWorkspaceAttached {
					workspace,
					connection: Ok(connection),
				},
			)
			.units(),
			0
		);
		assert_eq!(state.status_msg, "Joined Toron workspace.");
		assert!(
			!state.status_msg.contains(&nonce),
			"ordinary chrome must not expose the Toron workspace nonce"
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn unavailable_workspace_keeps_scratch_explicitly_attributed() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.adapter_state.scratch = runtime.block_on(AdapterState::open_scratch());
		let workspace = LocalWorkspace::Absent {
			profile_root: "/tmp/polariton-product-0005-absent".into(),
		};
		let expected_status = workspace.to_string();
		let attempt = update(&mut state, Message::LocalWorkspaceDiscovered(workspace));
		assert_eq!(attempt.units(), 0);
		assert!(state.adapter_state.sql_connection().is_some());
		assert!(state.adapter_state.scratch_is_active());
		assert_eq!(state.adapter_state.connection_identity(), "scratch");
		assert_eq!(state.status_error, expected_status);
		assert!(state.status_msg.contains("Start a session-bound workspace"));
		assert!(state.local_creation.offer().is_some());
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn spent_creation_failure_does_not_reoffer_or_retry() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let root = std::path::PathBuf::from("/tmp/polariton-product-0006-spent");
		let absent = LocalWorkspace::Absent {
			profile_root: root.clone(),
		};
		drop(update(
			&mut state,
			Message::LocalWorkspaceDiscovered(absent.clone()),
		));
		assert_eq!(state.local_creation.accept(), Some(root));
		let settled = update(
			&mut state,
			Message::LocalWorkspaceStartupSettled(Err((
				absent,
				"controlled startup failure".to_owned(),
			))),
		);
		assert_eq!(settled.units(), 0, "failure must arm no retry or timer");
		assert!(state.local_creation.offer().is_none());
		assert_eq!(state.status_error, "controlled startup failure");
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn matching_ready_observation_without_a_handle_confers_no_stop_authority() {
		let root = std::env::temp_dir().join(format!(
			"polariton-product-0006-attached-{}",
			std::process::id()
		));
		std::fs::create_dir_all(&root).unwrap();
		let signal = root.join("signal");
		let mut child = std::process::Command::new("/bin/sh")
			.arg("-c")
			.arg("trap 'printf signaled > \"$1\"; exit 0' INT; while :; do sleep 0.02; done")
			.arg("attached-fixture")
			.arg(&signal)
			.spawn()
			.unwrap();
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let mut identity = local_identity();
		identity.owner_pid = child.id();
		state.adapter_state.local_workspace = Some(LocalWorkspace::Ready(ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity,
		}));
		assert!(state.local_workspace_owner.is_none());
		let close = update(&mut state, Message::CloseWindow);
		assert_eq!(
			close.units(),
			1,
			"ephemeral close without an owned workspace must emit the window-close task immediately"
		);
		assert!(!state.closing_local_workspace);
		drop(close);
		std::thread::sleep(std::time::Duration::from_millis(50));
		assert!(child.try_wait().unwrap().is_none());
		assert!(!signal.exists());
		child.kill().unwrap();
		child.wait().unwrap();
		std::fs::remove_dir_all(root).unwrap();
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn close_with_an_owned_workspace_enters_visible_bounded_shutdown() {
		let command = std::process::Command::new("/bin/true");
		let owner = crate::local_workspace::LocalWorkspaceOwner::launch_command(
			command,
			nix::sys::signal::Signal::SIGINT,
		);
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.local_workspace_owner = Some(owner);
		let close = update(&mut state, Message::CloseWindow);
		assert_eq!(close.units(), 1, "owned close must wait before closing");
		assert!(state.closing_local_workspace);
		assert!(state.local_workspace_owner.is_none());
		assert!(state.status_error.is_empty());
		assert!(state.status_msg.contains("draining Toron before Wendelon"));
		let stopping = state.status_msg.clone();
		let absent = LocalWorkspace::Absent {
			profile_root: "/tmp/polariton-product-0006-closing".into(),
		};
		state.local_creation.observe(&absent);
		assert_eq!(update(&mut state, Message::StartLocalWorkspace).units(), 0);
		assert!(state.local_creation.offer().is_some());
		assert_eq!(
			update(&mut state, Message::LocalWorkspaceDiscovered(absent)).units(),
			0
		);
		assert_eq!(
			update(
				&mut state,
				Message::LocalWorkspaceStartupSettled(Err((
					LocalWorkspace::Absent {
						profile_root: "/tmp/polariton-product-0006-closing".into(),
					},
					"late failure".to_owned(),
				))),
			)
			.units(),
			0
		);
		assert_eq!(
			update(
				&mut state,
				Message::LocalWorkspaceAttached {
					workspace: ReadyWorkspace {
						sql_endpoint: "127.0.0.1:9".parse().unwrap(),
						identity: local_identity(),
					},
					connection: Err("late attach".to_owned()),
				},
			)
			.units(),
			0
		);
		assert_eq!(state.status_msg, stopping);
		assert!(state.status_error.is_empty());
		assert_eq!(
			update(&mut state, Message::CloseWindow).units(),
			0,
			"a second close must not start another stop"
		);
		let finish = update(
			&mut state,
			Message::LocalWorkspaceStopped("Stopped".to_owned()),
		);
		assert_eq!(
			finish.units(),
			1,
			"the ephemeral post-drain exit must emit the window-close task"
		);
		assert!(state.status_msg.contains("Local workspace stopped"));
		drop(close);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn startup_success_enters_the_existing_attach_path_only_after_ready() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let ready = ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity: local_identity(),
		};
		assert!(state.adapter_state.local_workspace.is_none());
		let attach = update(
			&mut state,
			Message::LocalWorkspaceStartupSettled(Ok(ready.clone())),
		);
		assert_eq!(attach.units(), 1);
		assert_eq!(
			state.adapter_state.local_workspace,
			Some(LocalWorkspace::Ready(ready))
		);
		assert!(state.adapter_state.connection.is_none());
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn rune_relation_results_feed_the_existing_table_and_plot_surfaces() {
		let frame = polars::df!(
			"cat" => ["a", "b"],
			"x" => [1_i64, 2],
			"y" => [3_i64, 5],
		)
		.expect("fixture frame");
		let source = "let r = workspace.table(\"t\"); table(r); let marker = 1; plot.bar(r, \"cat\", \"y\"); let tail = marker + 1; plot.scatter(r, \"x\", \"y\", \"cat\"); plot.line(r, \"x\", \"y\")";
		let views = transform_rune_view(source)
			.expect("view envelope")
			.into_relations()
			.next()
			.expect("one relation")
			.views;
		let outcome = rune_view_outcome(frame, views);
		assert_eq!(outcome.sinks.len(), 4);
		for sink in &outcome.sinks {
			assert!(std::sync::Arc::ptr_eq(&outcome.result, &sink.source));
		}
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(table_skin)) =
			&outcome.sinks[0].output
		else {
			panic!("first sink must remain the default table");
		};
		assert_eq!(table_skin.skin, TableSkin::DEFAULT);
		assert_eq!(
			table_skin.authored_show_column_types,
			crate::plot::spec::Override::Inherit
		);
		for (sink, expected) in
			outcome.sinks[1..]
				.iter()
				.zip([PlotType::Bar, PlotType::Scatter, PlotType::Line])
		{
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(plot, _, _)) = &sink.output
			else {
				panic!("expected resolved plot");
			};
			assert_eq!(plot.data.height(), 2);
			assert_eq!(
				plot.resolved_spec.composition.primary_plot_type(),
				expected,
				"the Toron producer must carry the resolved specification"
			);
			assert!(
				plot.resolved_spec
					.channel(crate::plot::spec::Channel::X)
					.is_some()
			);
			assert!(
				plot.resolved_spec
					.channel(crate::plot::spec::Channel::Y)
					.is_some()
			);
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0052_completion_retains_authored_type_intent_without_resolving_application_state() {
		let relation = transform_rune_view(
			"workspace.table(\"t\").views([table().show_types(), table().skin(\"nord\").show_types(), table()])",
		)
		.expect("three table presentations")
		.into_relations()
		.next()
		.expect("one relation");
		let outcome = rune_view_outcome(
			polars::df!("value" => [1_i64]).expect("table frame"),
			relation.views,
		);
		let presentations = outcome
			.sinks
			.iter()
			.map(|sink| {
				let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(presentation)) =
					&sink.output
				else {
					panic!("every sink must complete as a table");
				};
				presentation
			})
			.collect::<Vec<_>>();
		assert_eq!(
			presentations[0].authored_show_column_types,
			crate::plot::spec::Override::Set(true)
		);
		assert_eq!(presentations[0].skin, TableSkin::DEFAULT);
		assert_eq!(
			presentations[1].authored_show_column_types,
			crate::plot::spec::Override::Set(true)
		);
		assert_eq!(
			presentations[1].skin,
			TableSkin::from_theme_name("nord", TableSkinProjection::PaletteForward).unwrap()
		);
		assert_eq!(
			presentations[2].authored_show_column_types,
			crate::plot::spec::Override::Inherit
		);
		assert_eq!(presentations[2].skin, TableSkin::DEFAULT);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn multi_origin_dispatch_is_independent_while_each_origin_keeps_shared_identity() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let calls = Arc::new(Mutex::new(Vec::new()));
		let connection: Arc<RwLock<dyn DatabaseAdapter>> =
			Arc::new(RwLock::new(RecordingRuneAdapter {
				calls: calls.clone(),
				fail_table: None,
			}));

		let same_origin = transform_rune_view(
			"let r = workspace.table(\"a\"); table(r); plot.bar(r, \"x\", \"y\")",
		)
		.expect("same-origin fan-out");
		assert_eq!(same_origin.relation_count(), 1);
		let same_outcome = runtime
			.block_on(dispatch_rune_relation(
				connection.clone(),
				same_origin.into_relations().next().expect("one origin"),
			))
			.expect("same origin succeeds");
		assert_eq!(same_outcome.sinks.len(), 2);
		assert!(
			same_outcome
				.sinks
				.iter()
				.all(|sink| Arc::ptr_eq(&same_outcome.result, &sink.source))
		);

		let multi = transform_rune_view(
			"workspace.table(\"a\").views([bar(\"x\", \"y\")]); workspace.table(\"b\").views([bar(\"x\", \"y\")])",
		)
		.expect("two anonymous bars over two origins");
		assert_eq!(multi.relation_count(), 2);
		let mut relations = multi.into_relations().collect::<Vec<_>>();
		let second = relations.pop().expect("second origin");
		let first = relations.pop().expect("first origin");
		let second_outcome = runtime
			.block_on(dispatch_rune_relation(connection.clone(), second))
			.expect("second origin succeeds first");
		let first_outcome = runtime
			.block_on(dispatch_rune_relation(connection.clone(), first))
			.expect("first origin succeeds second");
		assert_eq!(
			(
				&second_outcome.sinks[0].identity,
				&first_outcome.sinks[0].identity,
			),
			(
				&LocalViewIdentity::Anonymous(1),
				&LocalViewIdentity::Anonymous(0),
			)
		);
		assert_eq!(calls.lock().expect("recording lock").len(), 3);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn one_origin_failure_does_not_cancel_its_sibling_dispatch() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let calls = Arc::new(Mutex::new(Vec::new()));
		let connection: Arc<RwLock<dyn DatabaseAdapter>> =
			Arc::new(RwLock::new(RecordingRuneAdapter {
				calls: calls.clone(),
				fail_table: Some("bad"),
			}));
		let program = transform_rune_view(
			"workspace.table(\"good\").views([table()]); workspace.table(\"bad\").views([table()])",
		)
		.expect("two table origins");
		let mut results = program
			.into_relations()
			.map(|relation| runtime.block_on(dispatch_rune_relation(connection.clone(), relation)))
			.collect::<Vec<_>>();

		assert!(results.remove(0).is_ok());
		assert!(results.remove(0).is_err());
		assert_eq!(calls.lock().expect("recording lock").len(), 2);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn bound_view_set_stops_without_dispatch_while_view_free_rune_still_runs_locally() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(74);
		let bound_source = "let v = workspace.table(\"t\").views([table()]);";
		let bound = super::run_rune_code(&mut state, source_buffer, bound_source.to_owned());
		assert_eq!(bound.units(), 1);
		assert_eq!(
			complete_rune_presentation(&mut state, source_buffer, bound_source).units(),
			0
		);
		assert_eq!(state.rune_view_scheduler.submitted, 0);
		assert_eq!(state.rune_view_scheduler.presentation_submitted, 1);
		assert_eq!(
			state.status_msg,
			"Rune view set is bound; no output root emitted."
		);

		let local_source = "let answer = 42; answer";
		let local = super::run_rune_code(&mut state, source_buffer, local_source.into());
		assert_eq!(
			local.units(),
			1,
			"plain Rune first enters presentation preflight"
		);
		let local = complete_rune_presentation(&mut state, source_buffer, local_source);
		assert_eq!(
			local.units(),
			1,
			"view-free Rune still reaches the local host"
		);
		assert_eq!(state.rune_buffer_revisions.get(&source_buffer), Some(&2));
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn duplicate_view_names_refuse_through_the_head_with_both_spans() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(75);
		let source = "workspace.table(\"a\").views([view(\"same\", table())]); workspace.table(\"b\").views([view(\"same\", bar(\"x\", \"y\"))])";
		let first = source.find("\"same\"").expect("first name");
		let second = source.rfind("\"same\"").expect("second name");

		let task = super::run_rune_code(&mut state, source_buffer, source.to_owned());

		assert_eq!(task.units(), 1);
		assert_eq!(
			complete_rune_presentation(&mut state, source_buffer, source).units(),
			0
		);
		assert_eq!(state.rune_view_scheduler.submitted, 0);
		assert_eq!(state.rune_view_scheduler.presentation_submitted, 1);
		assert!(
			state
				.status_error
				.contains(&format!("{first}..{}", first + 6))
		);
		assert!(
			state
				.status_error
				.contains(&format!("{second}..{}", second + 6))
		);
		assert!(state.status_error.starts_with("Rune · "));
		let detail = state
			.status_detail
			.as_ref()
			.expect("Rune diagnostic detail");
		assert_eq!(detail.summary, state.status_error);
		assert!(detail.full.contains(&format!("{first}..{}", first + 6)));
		assert!(detail.full.contains(&format!("{second}..{}", second + 6)));
		assert!(
			!state.code_editor.has_query_surface(),
			"a Rune envelope error must not manufacture a result pane"
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn bad_plot_field_fails_only_its_spanned_sink_while_table_survives() {
		let source = "let r = workspace.table(\"t\"); table(r); plot.bar(r, \"missing\", \"y\")";
		let transformed = transform_rune_view(source).expect("view envelope");
		let origin = transformed.origin.clone();
		let views = transformed
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		let outcome = rune_view_outcome(
			polars::df!("y" => [1_i64, 2]).expect("fixture frame"),
			transformed.views.clone(),
		);
		assert_eq!(outcome.sinks.len(), 2);
		assert_eq!(outcome.sinks[0].presentation, RuneViewForm::Table.into());
		assert!(outcome.sinks[0].output.is_ok());
		assert_eq!(outcome.sinks[1].presentation, RuneViewForm::Bar.into());
		let Err(error) = &outcome.sinks[1].output else {
			panic!("bad field must fail only this sink");
		};
		assert!(
			error.contains("missing"),
			"field attribution was lost: {error}"
		);
		let span = outcome.sinks[1].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"plot.bar(r, \"missing\", \"y\")"
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(73);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		let presentation = transformed
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: transformed,
				})
				.is_some()
		);
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 1,
				origin,
				views: views.clone(),
				result: Ok(outcome),
			},
		));
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &views[0]),
			"the plot failure must not replace the sibling table"
		);
		assert!(state.status_error.contains("plot.bar at bytes"));
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0035_authored_table_skin_resolution_is_per_sink_and_keeps_shared_identity() {
		let source = r#"workspace.table("t").views([
			table().skin("terrain_rs10"),
			table(),
			table().skin("dark2_s1"),
			line("x", "y")
		])"#;
		let relation = transform_rune_view(source)
			.expect("theme-projected table sinks")
			.into_relations()
			.next()
			.expect("one shared origin");
		let outcome = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [3_i64, 4]).expect("fixture frame"),
			relation.views,
		);
		assert_eq!(outcome.sinks.len(), 4);
		for sink in &outcome.sinks {
			assert!(Arc::ptr_eq(&outcome.result, &sink.source));
		}

		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(styled)) =
			&outcome.sinks[0].output
		else {
			panic!("styled table must resolve");
		};
		assert_eq!(
			styled.skin,
			TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
				.unwrap()
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(default)) =
			&outcome.sinks[1].output
		else {
			panic!("bare table must resolve");
		};
		assert_eq!(default.skin, TableSkin::DEFAULT);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(authored)) =
			&outcome.sinks[2].output
		else {
			panic!("defined authored palette must render");
		};
		assert_eq!(
			authored.skin,
			TableSkin::from_theme_name("dark2_s1", TableSkinProjection::PaletteForward).unwrap()
		);
		assert!(!authored.skin.passes_contrast());
		assert!(matches!(
			outcome.sinks[3].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(..))
		));
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0035_authored_projection_resolves_once_without_advice_or_fallback() {
		fn themed(name: &str, projection: RuneTableSkinProjection) -> RuneTableSkinIntent {
			RuneTableSkinIntent::NamedTheme {
				name: name.to_owned(),
				projection,
			}
		}

		fn resolve_traced(
			intent: RuneTableSkinIntent,
		) -> (
			std::result::Result<TableSkin, String>,
			Vec<(String, TableSkinProjection)>,
		) {
			let mut calls = Vec::new();
			let result = resolve_rune_table_skin_with(intent, |name, projection| {
				calls.push((name.to_owned(), projection));
				TableSkin::from_theme_name(name, projection)
			});
			(result, calls)
		}

		let (default, calls) = resolve_traced(RuneTableSkinIntent::Default);
		assert_eq!(default, Ok(TableSkin::DEFAULT));
		assert!(calls.is_empty());

		let (palette_forward, calls) =
			resolve_traced(themed("yellows", RuneTableSkinProjection::PaletteForward));
		assert_eq!(
			palette_forward,
			TableSkin::from_theme_name("yellows", TableSkinProjection::PaletteForward)
				.map_err(|error| error.to_string())
		);
		assert_eq!(
			calls,
			[("yellows".to_owned(), TableSkinProjection::PaletteForward)]
		);

		let (duotone, calls) = resolve_traced(themed("yellows", RuneTableSkinProjection::Duotone));
		assert_eq!(
			duotone,
			TableSkin::from_theme_name("yellows", TableSkinProjection::Duotone)
				.map_err(|error| error.to_string())
		);
		assert!(!duotone.unwrap().passes_contrast());
		assert_eq!(
			calls,
			[("yellows".to_owned(), TableSkinProjection::Duotone)]
		);

		let (unknown, calls) = resolve_traced(themed(
			"definitely-not-a-theme",
			RuneTableSkinProjection::Duotone,
		));
		assert_eq!(
			unknown,
			Err("unknown table skin theme \"definitely-not-a-theme\"".to_owned())
		);
		assert_eq!(
			calls,
			[(
				"definitely-not-a-theme".to_owned(),
				TableSkinProjection::Duotone
			)]
		);
		let (unknown, calls) = resolve_traced(themed(
			"definitely-not-a-theme",
			RuneTableSkinProjection::PaletteForward,
		));
		assert_eq!(
			unknown,
			Err("unknown table skin theme \"definitely-not-a-theme\"".to_owned())
		);
		assert_eq!(
			calls,
			[(
				"definitely-not-a-theme".to_owned(),
				TableSkinProjection::PaletteForward
			)]
		);

		let (duotone_success, calls) =
			resolve_traced(themed("terrain_rs10", RuneTableSkinProjection::Duotone));
		assert_eq!(
			duotone_success,
			TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::Duotone)
				.map_err(|error| error.to_string())
		);
		assert_eq!(
			calls,
			[("terrain_rs10".to_owned(), TableSkinProjection::Duotone)]
		);

		let (duotone_below, calls) =
			resolve_traced(themed("dark2_s1", RuneTableSkinProjection::Duotone));
		assert_eq!(
			duotone_below,
			TableSkin::from_theme_name("dark2_s1", TableSkinProjection::Duotone)
				.map_err(|error| error.to_string())
		);
		assert!(!duotone_below.unwrap().passes_contrast());
		assert_eq!(
			calls,
			[("dark2_s1".to_owned(), TableSkinProjection::Duotone)]
		);

		let (palette_forward_below, calls) =
			resolve_traced(themed("dark2_s1", RuneTableSkinProjection::PaletteForward));
		assert_eq!(
			palette_forward_below,
			TableSkin::from_theme_name("dark2_s1", TableSkinProjection::PaletteForward)
				.map_err(|error| error.to_string())
		);
		assert!(!palette_forward_below.unwrap().passes_contrast());
		assert_eq!(
			calls,
			[("dark2_s1".to_owned(), TableSkinProjection::PaletteForward)]
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0045_pane_table_skin_restyles_only_the_completed_draw() {
		let source_buffer = BufferId::new(111);
		let identity = LocalViewIdentity::Named("table".to_owned());
		let program = transform_rune_view(
			"workspace.table(\"t\").views([view(\"table\", table().skin(\"terrain_rs10\"))])",
		)
		.expect("named authored table");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		let frame = polars::df!("value" => [1_i64, 2]).expect("table frame");
		let baked = TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
			.expect("authored baked skin");
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.expect("prepared table panel");
		state
			.code_editor
			.set_rune_view_table_with_skin(panel_id, frame.clone(), baked);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &identity),
			Some(baked)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_draw_skin(source_buffer, &identity),
			Some(baked)
		);

		let pane = TableSkinSpec::new("yellows", TableSkinProjection::Duotone);
		let pane_skin = pane.resolve().expect("valid pane test producer");
		state
			.code_editor
			.set_test_rune_view_pane_table_skin(source_buffer, &identity, pane);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &identity),
			Some(baked),
			"the pane tier must not mutate the completed artifact"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_draw_skin(source_buffer, &identity),
			Some(pane_skin)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table(source_buffer, &identity),
			Some(&frame),
			"the pane tier must not replace the completed frame"
		);
		state
			.code_editor
			.clear_test_rune_view_pane_table_skin(source_buffer, &identity);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_draw_skin(source_buffer, &identity),
			Some(baked),
			"removing the pane tier restores the baked artifact without a rerun"
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0027_focused_high_density_shape_parses_and_completes_every_sink() {
		let source = r#"ket.table("test1")
	.sort([col("x").asc()])
	.views([
		table().skin("terrain_rs10"),
		figure([
			title("Plot Layer Test"),
			subtitle("Toron was here"),
			layers([
				line("x", "y")
					.stroke_width(8.0),
				scatter("x", "y")
					.point_size(8.0)])])])
ket.table("test2") // Comment Test1.
	.sort([col("cat").asc()])
	.views([
		table(), // Comment Test2.
		figure([
			title("SQL = 💩"),
			subtitle("Wendelon was here"),
			bar("cat", "val")
				.bar_width(0.98)])])"#;
		let relations = transform_rune_view(source)
			.expect("focused accepted high-density shape")
			.into_relations()
			.collect::<Vec<_>>();
		assert_eq!(relations.len(), 2);

		let first = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [3_i64, 4]).expect("test1 frame"),
			relations[0].views.clone(),
		);
		assert_eq!(first.sinks.len(), 2);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(skin)) = &first.sinks[0].output
		else {
			panic!("test1 table must complete with its skin");
		};
		assert_eq!(
			skin.skin,
			TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
				.unwrap()
		);
		assert!(matches!(
			first.sinks[1].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(
				_,
				_
			))
		));

		let second = rune_view_outcome(
			polars::df!("cat" => ["a", "b"], "val" => [3_i64, 4]).expect("test2 frame"),
			relations[1].views.clone(),
		);
		assert_eq!(second.sinks.len(), 2);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(skin)) = &second.sinks[0].output
		else {
			panic!("test2 table must remain default-skinned");
		};
		assert_eq!(skin.skin, TableSkin::DEFAULT);
		assert!(matches!(
			second.sinks[1].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(..))
		));
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0035_projection_changes_reuse_panels_and_below_threshold_skin_replaces_stale() {
		fn program(theme: &str, duotone: bool) -> TransformedRuneView {
			let projection = if duotone { ".duotone()" } else { "" };
			transform_rune_view(&format!(
				"workspace.table(\"t\").views([view(\"styled\", table().skin(\"{theme}\"){projection}), view(\"plain\", table()), view(\"bad\", table().skin(\"dark2_s1\"))])"
			))
			.expect("three named tables")
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(91);
		let styled = LocalViewIdentity::Named("styled".to_owned());
		let plain = LocalViewIdentity::Named("plain".to_owned());
		let bad = LocalViewIdentity::Named("bad".to_owned());
		let previous_bad_skin =
			TableSkin::from_theme_name("terrain_r", TableSkinProjection::Duotone).unwrap();

		let first = program("terrain_rs10", true);
		let presentation = first
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		let panel_ids = [&styled, &plain, &bad].map(|identity| {
			state
				.code_editor
				.rune_view_panel_id(source_buffer, identity)
				.expect("prepared named panel")
		});
		state.code_editor.set_rune_view_table_with_skin(
			panel_ids[2],
			polars::df!("previous" => [1_i64]).expect("previous bad frame"),
			previous_bad_skin,
		);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: first.clone(),
				})
				.is_some()
		);
		let relation = first.into_relations().next().unwrap();
		let outcome = rune_view_outcome(
			polars::df!("value" => [2_i64]).expect("first frame"),
			relation.views,
		);
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 1,
				origin: relation.origin,
				views: vec![styled.clone(), plain.clone(), bad.clone()],
				result: Ok(outcome),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &styled),
			Some(TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::Duotone).unwrap())
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &plain),
			Some(TableSkin::DEFAULT)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &bad),
			Some(
				TableSkin::from_theme_name("dark2_s1", TableSkinProjection::PaletteForward)
					.unwrap()
			),
			"defined authored projection must replace the prior artifact"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &bad),
			Some("fresh")
		);
		assert!(!state.status_error.contains("table at bytes"));

		let second = program("terrain_rs10", false);
		let presentation = second
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 2);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 2);
		assert_eq!(
			[&styled, &plain, &bad].map(|identity| state
				.code_editor
				.rune_view_panel_id(source_buffer, identity)
				.unwrap()),
			panel_ids,
			"skin changes must not replace destinations"
		);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 2,
					view: second.clone(),
				})
				.is_some()
		);
		let relation = second.into_relations().next().unwrap();
		let outcome = rune_view_outcome(
			polars::df!("value" => [3_i64]).expect("second frame"),
			relation.views,
		);
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 2,
				origin: relation.origin,
				views: vec![styled.clone(), plain, bad],
				result: Ok(outcome),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &styled),
			Some(
				TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
					.unwrap()
			)
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &styled),
			Some(panel_ids[0])
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 3);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0014_compatibility_crosses_the_factory_and_returns_to_the_outer_span() {
		let source = "workspace.table(\"t\").views([layers([line(\"x\", \"y\"), scatter(\"other_x\", \"y\")])])";
		let relation = transform_rune_view(source)
			.expect("surface grammar accepts differing fields")
			.into_relations()
			.next()
			.expect("one relation");
		let span = relation.views[0].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"layers([line(\"x\", \"y\"), scatter(\"other_x\", \"y\")])"
		);
		let outcome = rune_view_outcome(
			polars::df!(
				"x" => [1.0_f64, 2.0],
				"other_x" => [3.0_f64, 4.0],
				"y" => [5.0_f64, 6.0],
			)
			.expect("fixture frame"),
			relation.views,
		);
		let [sink] = outcome.sinks.as_slice() else {
			panic!("one layers sink must remain one outcome")
		};
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
			&sink.output
		else {
			panic!("the envelope must hand a layered specification to the head")
		};
		let projected = sink.source.projection().expect("fixture projection");
		let Err(error) = create_layered_plot(specification, projected.as_ref(), 1200, 720) else {
			panic!("PRODUCT-0013 must own incompatible field refusal")
		};
		assert!(error.contains("layer 2"));
		assert!(error.contains("x"));
		assert!(error.contains("other_x"));

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(74);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		let identity = sink.identity.clone();
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(identity.clone(), sink.presentation.clone())],
			revision,
		);
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.expect("prepared layered destination");
		drop(update(
			&mut state,
			Message::RuneRelationLayeredPlotReady {
				source_buffer,
				revision,
				panel_id,
				presentation: sink.presentation.clone(),
				authored_settings: crate::plot::theme::PlotSettingsLayer::default(),
				span: span.clone(),
				result: Err(error.clone()),
			},
		));
		let detail = state.status_detail.as_ref().expect("full Rune diagnostic");
		assert!(
			detail
				.full
				.contains(&format!("at bytes {}..{}: {error}", span.start, span.end))
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_failed(source_buffer, &identity)
		);
		assert!(
			!state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &identity)
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0014_layered_view_completes_in_a_label_only_host_panel() {
		let source = r#"
workspace.table("t").views([
    table(),
    bar("cat", "val"),
    line("x", "y"),
    scatter("x", "y"),
    view("trend", layers([line("x", "y"), scatter("x", "y")]))])
"#;
		let program = transform_rune_view(source).expect("layered host program");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		assert_eq!(
			presentation
				.iter()
				.map(|(_, presentation)| presentation.display_label.as_str())
				.collect::<Vec<_>>(),
			[
				"table",
				"plot.bar",
				"plot.line",
				"plot.scatter",
				"Line + Scatter"
			]
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(75);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, revision);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program.clone(),
				})
				.is_some()
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			revision,
			program.into_relations().next().expect("one relation"),
			polars::df!(
				"cat" => ["a", "b"],
				"val" => [3_i64, 5],
				"x" => [1.0_f64, 2.0],
				"y" => [2.0_f64, 4.0],
			)
			.expect("host fixture"),
		);

		for (identity, expected) in [
			(LocalViewIdentity::Anonymous(1), "plot.bar"),
			(LocalViewIdentity::Anonymous(2), "plot.line"),
			(LocalViewIdentity::Anonymous(3), "plot.scatter"),
			(
				LocalViewIdentity::Named("trend".to_owned()),
				"Line + Scatter",
			),
		] {
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_display_label(source_buffer, &identity),
				Some(expected)
			);
		}
		assert!(
			state.code_editor.rune_view_panel_is_plot(
				source_buffer,
				&LocalViewIdentity::Named("trend".to_owned())
			),
			"the layered kernel must reach ordinary host plot content"
		);

		let trend = LocalViewIdentity::Named("trend".to_owned());
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.rune_view_panel_projected_title(source_buffer, &trend)
			.is_none()
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &trend),
			Some("trend"),
			"panel chrome follows identity while the canvas follows display_label"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &trend),
			Some("Line + Scatter")
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0014_named_layer_edits_refresh_one_identity_and_destination() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(76);
		let identity = LocalViewIdentity::Named("trend".to_owned());
		let mut first_panel = None;
		for (revision, source, expected_label) in [
			(
				1,
				"workspace.table(\"t\").views([view(\"trend\", layers([line(\"x\", \"y\"), scatter(\"x\", \"y\")]))])",
				"Line + Scatter",
			),
			(
				2,
				"workspace.table(\"t\").views([view(\"trend\", layers([scatter(\"x\", \"y\"), line(\"x\", \"y\")]))])",
				"Scatter + Line",
			),
			(
				3,
				"workspace.table(\"t\").views([view(\"trend\", layers([line(\"x\", \"y\")]))])",
				"Line",
			),
		] {
			let program = transform_rune_view(source).expect("named layered revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation.len(), 1, "layers are not views");
			assert_eq!(presentation[0].0, identity);
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let panel_id = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("named layered panel");
			if let Some(first) = first_panel {
				assert_eq!(panel_id, first, "layer edits retain the destination");
			} else {
				first_panel = Some(panel_id);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				polars::df!("x" => [1.0_f64, 2.0], "y" => [2.0_f64, 4.0]).expect("layer frame"),
			);
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_display_label(source_buffer, &identity),
				Some(expected_label)
			);
		}
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0014_one_layer_spelling_keeps_anonymous_identity_and_svg() {
		let bare_source = "workspace.table(\"t\").views([line(\"x\", \"y\")])";
		let layered_source = "workspace.table(\"t\").views([layers([line(\"x\", \"y\")])])";
		let figure_source = "workspace.table(\"t\").views([figure([line(\"x\", \"y\")])])";
		let frame = polars::df!("x" => [1.0_f64, 2.0], "y" => [2.0_f64, 4.0]).expect("line frame");

		let bare_relation = transform_rune_view(bare_source)
			.expect("bare line")
			.into_relations()
			.next()
			.expect("one relation");
		let bare_outcome = rune_view_outcome(frame.clone(), bare_relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _)) =
			&bare_outcome.sinks[0].output
		else {
			panic!("bare line must use the established plot request")
		};
		let bare_kernel = create_plot(&resolved.resolved_spec, &resolved.data, 100, 100);

		let layered_relation = transform_rune_view(layered_source)
			.expect("one-layer surface")
			.into_relations()
			.next()
			.expect("one relation");
		let layered_outcome = rune_view_outcome(frame.clone(), layered_relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
			&layered_outcome.sinks[0].output
		else {
			panic!("layers spelling must retain the layered request")
		};
		let layered_kernel = create_layered_plot(specification, &frame, 100, 100)
			.expect("one-layer PRODUCT-0013 path");
		let figure_relation = transform_rune_view(figure_source)
			.expect("one-body figure")
			.into_relations()
			.next()
			.expect("one relation");
		let figure_outcome = rune_view_outcome(frame.clone(), figure_relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(figure, _, _)) =
			&figure_outcome.sinks[0].output
		else {
			panic!("one-body figure must retain the single plot path")
		};
		let figure_kernel = create_plot(&figure.resolved_spec, &figure.data, 100, 100);
		assert_eq!(
			render_marks(bare_kernel.as_ref()),
			render_marks(layered_kernel.as_ref())
		);
		assert_eq!(
			render_marks(bare_kernel.as_ref()),
			render_marks(figure_kernel.as_ref())
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(77);
		let identity = LocalViewIdentity::Anonymous(0);
		let mut panel = None;
		for (revision, source) in [(1, bare_source), (2, layered_source), (3, figure_source)] {
			let program = transform_rune_view(source).expect("line spelling revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("anonymous line panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "spelling is not identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
		}
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.line")
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0018_point_size_reaches_every_host_path_and_preserves_the_view() {
		let frame = polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
			.expect("scatter frame");
		let every_position = transform_rune_view(
			r#"workspace.table("t").views([
				layers([scatter("x", "y").point_size(2.0)]),
				scatter("x", "y").point_size(2.0),
				figure([title("Sized"), scatter("x", "y").point_size(2.0)]),
			])"#,
		)
		.expect("all proven modified-mark positions")
		.into_relations()
		.next()
		.expect("one relation");
		let every_outcome = rune_view_outcome(frame.clone(), every_position.views);
		assert_eq!(every_outcome.sinks.len(), 3);
		for sink in &every_outcome.sinks {
			assert!(Arc::ptr_eq(&every_outcome.result, &sink.source));
			let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
				&sink.output
			else {
				panic!("every modified mark must use the proved layer-aware output")
			};
			let mut layers = specification.layers.iter();
			let layer = layers.next().expect("one authored layer");
			assert!(
				layers.next().is_none(),
				"each spelling must lower to exactly one authored layer"
			);
			assert_eq!(
				layer.settings.point_size,
				crate::plot::spec::Override::Set(crate::plot::spec::PointSize::new(2.0).unwrap())
			);
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(84);
		let identity = LocalViewIdentity::Named("points".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"points\", scatter(\"x\", \"y\"))])",
			"workspace.table(\"t\").views([view(\"points\", scatter(\"x\", \"y\").point_size(1.0))])",
			"workspace.table(\"t\").views([view(\"points\", scatter(\"x\", \"y\").point_size(2.0))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("point-size lifecycle revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			assert_eq!(presentation[0].1.display_label, "plot.scatter");
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable point-size panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "point size is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed point-size canvas"),
			);
		}
		assert_eq!(svgs[0], svgs[1], "factor 1.0 must preserve bare output");
		assert!(
			svgs[2].contains("A 6.00 6.00"),
			"factor 2.0 must reach the six-pixel host rendering: {}",
			svgs[2]
		);
		assert_ne!(svgs[1], svgs[2]);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.scatter")
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0046_category_scatter_point_size_uses_the_standalone_carrier() {
		let frame = polars::df!(
			"units" => [10.0_f64, 20.0, 30.0],
			"revenue" => [40.0_f64, 80.0, 120.0],
			"product" => ["A", "B", "C"],
		)
		.expect("categorical scatter frame");
		let program = transform_rune_view(
			r#"workspace.table("t").views([figure([
				scatter("units", "revenue", "product").point_size(2.0),
			])])"#,
		)
		.expect("styled categorical scatter");
		let relation = program
			.clone()
			.into_relations()
			.next()
			.expect("one relation");
		let outcome = rune_view_outcome(frame.clone(), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
			resolved,
			layer_settings,
			_,
			_,
		)) = &outcome.sinks[0].output
		else {
			panic!("a styled categorical scatter must use the standalone carrier")
		};
		assert_eq!(
			resolved.resolved_spec.composition.primary_plot_type(),
			crate::plot::core::PlotType::Scatter
		);
		assert_eq!(
			layer_settings.point_size,
			crate::plot::spec::Override::Set(crate::plot::spec::PointSize::new(2.0).unwrap())
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(110);
		let identity = LocalViewIdentity::Named("points".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"points\", scatter(\"units\", \"revenue\", \"product\").point_size(1.0))])",
			"workspace.table(\"t\").views([view(\"points\", scatter(\"units\", \"revenue\", \"product\").point_size(2.0))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("categorical point-size revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable categorical point-size panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "point size is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed categorical point-size canvas"),
			);
		}
		assert!(svgs[1].contains("A 6.00 6.00"));
		assert_ne!(svgs[0], svgs[1]);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0020_stroke_width_reaches_host_and_combines_with_point_size() {
		let frame = polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
			.expect("line frame");
		let positions = transform_rune_view(
			r#"workspace.table("t").views([
				layers([line("x", "y").stroke_width(2.0)]),
				line("x", "y").stroke_width(2.0),
				figure([title("Wide"), line("x", "y").stroke_width(2.0)]),
			])"#,
		)
		.expect("all proven stroke-width positions")
		.into_relations()
		.next()
		.expect("one relation");
		let position_outcome = rune_view_outcome(frame.clone(), positions.views);
		assert_eq!(position_outcome.sinks.len(), 3);
		for sink in &position_outcome.sinks {
			assert!(Arc::ptr_eq(&position_outcome.result, &sink.source));
			let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
				&sink.output
			else {
				panic!("every styled line must use the layer-aware output")
			};
			let mut layers = specification.layers.iter();
			let layer = layers.next().expect("one line layer");
			assert!(layers.next().is_none());
			assert_eq!(layer.mark, crate::plot::spec::LayerMark::Line);
			assert_eq!(
				layer.settings.stroke_width,
				crate::plot::spec::Override::Set(crate::plot::spec::StrokeWidth::new(2.0).unwrap())
			);
		}

		let combo = transform_rune_view(
			r#"workspace.table("t").views([figure([
				title("High density"),
				subtitle("Two independent Layer settings"),
				layers([
					line("x", "y").stroke_width(2.0),
					scatter("x", "y").point_size(2.0),
				]),
			])])"#,
		)
		.expect("high-density combo")
		.into_relations()
		.next()
		.expect("one relation");
		let combo_outcome = rune_view_outcome(frame.clone(), combo.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, settings)) =
			&combo_outcome.sinks[0].output
		else {
			panic!("combo remains one layered output")
		};
		let layers = specification.layers.iter().collect::<Vec<_>>();
		assert_eq!(
			layers.iter().map(|layer| layer.mark).collect::<Vec<_>>(),
			[
				crate::plot::spec::LayerMark::Line,
				crate::plot::spec::LayerMark::Scatter
			]
		);
		assert!(matches!(
			layers[0].settings.stroke_width,
			crate::plot::spec::Override::Set(_)
		));
		assert!(matches!(
			layers[1].settings.point_size,
			crate::plot::spec::Override::Set(_)
		));
		assert!(matches!(
			settings.title,
			crate::plot::spec::Presence::Show(_)
		));
		assert!(matches!(
			settings.subtitle,
			crate::plot::spec::Presence::Show(_)
		));

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(85);
		let identity = LocalViewIdentity::Named("line".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"line\", line(\"x\", \"y\"))])",
			"workspace.table(\"t\").views([view(\"line\", line(\"x\", \"y\").stroke_width(1.0))])",
			"workspace.table(\"t\").views([view(\"line\", line(\"x\", \"y\").stroke_width(2.0))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("stroke-width lifecycle revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			assert_eq!(presentation[0].1.display_label, "plot.line");
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable line panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "stroke width is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed line canvas"),
			);
		}
		assert_eq!(svgs[0], svgs[1], "factor 1.0 must preserve bare line");
		assert!(svgs[2].contains("stroke-width=\"4\""));
		assert_ne!(svgs[1], svgs[2]);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.line")
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0022_bar_width_reaches_the_shared_result_and_real_host_panel() {
		let frame = polars::df!("cat" => ["A", "B"], "val" => [1.0_f64, 2.0]).expect("bar frame");
		let positions = transform_rune_view(
			r#"workspace.table("t").views([
				bar("cat", "val").bar_width(0.5),
				figure([title("Narrow bars"), bar("cat", "val").bar_width(0.5)]),
			])"#,
		)
		.expect("direct and figure bar width")
		.into_relations()
		.next()
		.expect("one relation");
		let outcome = rune_view_outcome(frame.clone(), positions.views);
		assert_eq!(outcome.sinks.len(), 2);
		for (index, sink) in outcome.sinks.iter().enumerate() {
			assert!(Arc::ptr_eq(&outcome.result, &sink.source));
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
				resolved,
				settings,
				authored,
				_,
			)) = &sink.output
			else {
				panic!("styled bar must use the standalone styled-plot path")
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				crate::plot::core::PlotType::Bar
			);
			assert_eq!(
				settings.bar_width,
				crate::plot::spec::Override::Set(crate::plot::spec::BarWidth::new(0.5).unwrap())
			);
			assert_eq!(
				matches!(authored.title, crate::plot::spec::Presence::Show(_)),
				index == 1
			);
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(86);
		let identity = LocalViewIdentity::Named("bars".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"bars\", bar(\"cat\", \"val\"))])",
			"workspace.table(\"t\").views([view(\"bars\", bar(\"cat\", \"val\").bar_width(0.9))])",
			"workspace.table(\"t\").views([view(\"bars\", bar(\"cat\", \"val\").bar_width(0.5))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("bar-width lifecycle revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			assert_eq!(presentation[0].1.display_label, "plot.bar");
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable bar panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "bar width is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed bar canvas"),
			);
		}
		assert_eq!(svgs[0], svgs[1], "0.9 must preserve the bare bar");
		assert_ne!(svgs[1], svgs[2], "0.5 must visibly narrow the bars");
		let bounds = iced::Rectangle::new(iced::Point::ORIGIN, iced::Size::new(100.0, 100.0));
		assert!(
			state
				.code_editor
				.rune_view_panel_plot_hover(
					source_buffer,
					&identity,
					bounds,
					iced::Point::new(20.0, 75.0),
				)
				.is_some()
		);
		assert_eq!(
			state.code_editor.rune_view_panel_plot_hover(
				source_buffer,
				&identity,
				bounds,
				iced::Point::new(10.0, 75.0),
			),
			None
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.bar")
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0016_figure_headings_reach_canvas_and_source_only_chrome() {
		let source = r#"workspace.table("t").views([
			view("stable", figure([title("Named canvas"), line("x", "y")])),
			figure([title("Revenue 📈"), subtitle("Private detail"), line("x", "y")]),
			figure([subtitle("Subtitle only"), line("x", "y")]),
			figure([title("Revenue"), line("x", "y")]),
			figure([title("Overlay"), subtitle("Both marks"), layers([line("x", "y"), scatter("x", "y")])]),
		])"#;
		let program = transform_rune_view(source).expect("figure surface");
		let relation = program.clone().into_relations().next().expect("one origin");
		let identities = relation
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		let presentations = relation
			.views
			.iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(82);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations, revision);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program,
				})
				.is_some()
		);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while identities.iter().any(|identity| {
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, identity)
				.is_none()
		}) && std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		let chrome = identities
			.iter()
			.map(|identity| {
				state
					.code_editor
					.rune_view_panel_projected_title(source_buffer, identity)
					.expect("projected figure panel")
					.to_owned()
			})
			.collect::<Vec<_>>();
		assert_eq!(
			chrome,
			[
				"stable",
				"Revenue 📈 2",
				"plot.line 3",
				"Revenue 4",
				"Overlay 5"
			]
		);
		assert!(chrome.iter().all(|title| {
			!title.contains("Private detail")
				&& !title.contains("Subtitle only")
				&& !title.contains("Both marks")
		}));

		complete_relation_in_head(
			&mut state,
			source_buffer,
			revision,
			relation,
			polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
				.expect("figure frame"),
		);
		let canvases = identities
			.iter()
			.map(|identity| {
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, identity)
					.expect("completed figure canvas")
			})
			.collect::<Vec<_>>();
		for (svg, expected) in canvases.iter().zip([
			"Named canvas",
			"Revenue 📈",
			"plot.line",
			"Revenue",
			"Overlay",
		]) {
			assert!(svg.contains(expected), "canvas must contain {expected}");
		}
		assert!(canvases[1].contains("Private detail"));
		assert!(canvases[2].contains("Subtitle only"));
		assert!(canvases[4].contains("Both marks"));
		assert!(!canvases[1].contains("plot.line"));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identities[4]),
			Some("Line + Scatter")
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0016_heading_lifecycle_keeps_destination_layout_and_atomic_canvas() {
		let anonymous = LocalViewIdentity::Anonymous(0);
		let named = LocalViewIdentity::Named("stable".to_owned());
		let identities = [anonymous.clone(), named.clone()];
		let frame = polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
			.expect("lifecycle frame");
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		let source_buffer = loop {
			state.code_editor.poll();
			if let Some(buffer_id) = state.code_editor.active_buffer_id() {
				break buffer_id;
			}
			assert!(
				std::time::Instant::now() < deadline,
				"source buffer must project before the lifecycle assay"
			);
			std::thread::sleep(std::time::Duration::from_millis(5));
		};

		let program_a = transform_rune_view(
			r#"workspace.table("t").views([
				figure([title("A"), subtitle("A detail"), line("x", "y")]),
				view("stable", figure([title("Named A"), subtitle("Named detail"), line("x", "y")]))
			])"#,
		)
		.expect("revision A");
		let presentations_a = program_a
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations_a, 1);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: program_a.clone(),
				})
				.is_some()
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			1,
			program_a.into_relations().next().expect("one A relation"),
			frame.clone(),
		);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while identities.iter().any(|identity| {
			state
				.code_editor
				.rune_view_panel_layout_signature(source_buffer, identity)
				.is_none()
		}) && std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		let panels = identities.clone().map(|identity| {
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable panel")
		});
		let layout = identities.clone().map(|identity| {
			state
				.code_editor
				.rune_view_panel_layout_signature(source_buffer, &identity)
				.expect("stable layout")
		});
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &anonymous),
			Some("A 1")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &named),
			Some("stable")
		);
		for (identity, title, subtitle) in [
			(&anonymous, "A", "A detail"),
			(&named, "Named A", "Named detail"),
		] {
			let svg = state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, identity)
				.expect("A canvas");
			assert!(svg.contains(title));
			assert!(svg.contains(subtitle));
		}

		let program_b = transform_rune_view(
			r#"workspace.table("t").views([
				figure([title("B"), subtitle("B detail"), line("x", "y")]),
				view("stable", figure([title("Named B"), subtitle("Changed"), line("x", "y")]))
			])"#,
		)
		.expect("revision B");
		let presentations_b = program_b
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 2);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations_b, 2);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 2,
					view: program_b.clone(),
				})
				.is_some()
		);
		let relation_b = program_b.into_relations().next().expect("one B relation");
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 2,
				origin: relation_b.origin,
				views: relation_b
					.views
					.into_iter()
					.map(|view| view.identity)
					.collect(),
				result: Err("revision B failed".to_owned()),
			},
		));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.rune_view_panel_projected_title(source_buffer, &anonymous)
			!= Some("B 1")
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &anonymous),
			Some("B 1"),
			"chrome follows successfully analyzed source"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &named),
			Some("stable")
		);
		for (identity, title, subtitle) in [
			(&anonymous, "A", "A detail"),
			(&named, "Named A", "Named detail"),
		] {
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_status(source_buffer, identity),
				Some("stale")
			);
			let svg = state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, identity)
				.expect("stale A canvas");
			assert!(svg.contains(title));
			assert!(svg.contains(subtitle));
			assert!(!svg.contains("B detail"));
			assert!(!svg.contains("Named B"));
		}

		let program_c = transform_rune_view(
			"workspace.table(\"t\").views([line(\"x\", \"y\"), view(\"stable\", line(\"x\", \"y\"))])",
		)
		.expect("revision C");
		let presentations_c = program_c
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 3);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations_c, 3);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 3,
					view: program_c.clone(),
				})
				.is_some()
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			3,
			program_c.into_relations().next().expect("one C relation"),
			frame,
		);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.rune_view_panel_projected_title(source_buffer, &anonymous)
			!= Some("plot.line 1")
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &anonymous),
			Some("plot.line 1")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &named),
			Some("stable")
		);
		for (index, identity) in identities.iter().enumerate() {
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_id(source_buffer, identity),
				Some(panels[index])
			);
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_layout_signature(source_buffer, identity),
				Some(layout[index].clone()),
				"descriptor refresh and completion preserve the output-group layout"
			);
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_status(source_buffer, identity),
				Some("fresh")
			);
			let svg = state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, identity)
				.expect("C canvas");
			assert!(svg.contains("plot.line"));
			assert!(!svg.contains("A detail"));
			assert!(!svg.contains("B detail"));
		}
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn dogfood_multi_origin_views_populate_four_host_panels_without_a_dashboard() {
		let source = r#"
ket.table("test1")
	.sort([col("x").asc()])
	.views([
		line("x", "y"),
		table()])
ket.table("test2")
	.sort([col("cat").asc()])
	.views([
		bar("cat", "val"),
		table()])
"#;
		let program = transform_rune_view(source).expect("dogfood view program");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		assert_eq!(
			presentation,
			vec![
				(LocalViewIdentity::Anonymous(0), RuneViewForm::Line),
				(LocalViewIdentity::Anonymous(1), RuneViewForm::Table),
				(LocalViewIdentity::Anonymous(2), RuneViewForm::Bar),
				(LocalViewIdentity::Anonymous(3), RuneViewForm::Table),
			]
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(80);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, revision);
		let panel_ids = presentation
			.iter()
			.map(|(identity, _)| {
				state
					.code_editor
					.rune_view_panel_id(source_buffer, identity)
					.expect("view panel")
			})
			.collect::<Vec<_>>();
		assert!(panel_ids.windows(2).all(|ids| ids[0] < ids[1]));
		assert!(panel_ids[0].get() >= (1_u64 << 63));
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program.clone(),
				})
				.is_some()
		);

		let mut relations = program.into_relations().collect::<Vec<_>>();
		relations.reverse();
		for relation in relations {
			let frame = if relation.origin.contains("test1") {
				polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).expect("test1 frame")
			} else {
				polars::df!("cat" => ["a", "b"], "val" => [3_i64, 5]).expect("test2 frame")
			};
			complete_relation_in_head(&mut state, source_buffer, revision, relation, frame);
		}

		let first_table = state
			.code_editor
			.rune_view_panel_table(source_buffer, &LocalViewIdentity::Anonymous(1))
			.expect("first table panel");
		let second_table = state
			.code_editor
			.rune_view_panel_table(source_buffer, &LocalViewIdentity::Anonymous(3))
			.expect("second table panel");
		assert!(first_table.column("x").is_ok());
		assert!(first_table.column("cat").is_err());
		assert!(second_table.column("cat").is_ok());
		assert!(second_table.column("x").is_err());
		assert_ne!(panel_ids[1], panel_ids[3]);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(0))
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &LocalViewIdentity::Anonymous(0)),
			Some("plot.line"),
			"the first dogfood view must reach the existing line renderer"
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(2)),
			"bar panel did not resolve: {}",
			state.status_error
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn execution_grouping_cannot_reorder_interleaved_host_panels() {
		let source = r#"
let a = workspace.table("a");
let b = workspace.table("b");
table(a);
plot.scatter(b, "x", "y");
plot.bar(a, "x", "y");
"#;
		let program = transform_rune_view(source).expect("interleaved origins");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		assert_eq!(
			presentation
				.iter()
				.map(|(_, form)| *form)
				.collect::<Vec<_>>(),
			vec![
				RuneViewForm::Table,
				RuneViewForm::Scatter,
				RuneViewForm::Bar
			]
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(81);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		let ids = presentation
			.iter()
			.map(|(identity, _)| {
				state
					.code_editor
					.rune_view_panel_id(source_buffer, identity)
					.expect("interleaved panel")
			})
			.collect::<Vec<_>>();
		assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: program.clone(),
				})
				.is_some()
		);

		let mut relations = program.into_relations().collect::<Vec<_>>();
		let b = relations
			.iter()
			.position(|relation| relation.origin.contains("table(\"b\")"))
			.map(|index| relations.remove(index))
			.expect("b relation");
		complete_relation_in_head(
			&mut state,
			source_buffer,
			1,
			b,
			polars::df!("x" => [1_i64, 2], "y" => [3_i64, 4]).expect("b frame"),
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(1))
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			1,
			relations.pop().expect("a relation"),
			polars::df!("x" => ["left", "right"], "y" => [7_i64, 8]).expect("a frame"),
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &LocalViewIdentity::Anonymous(0))
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(2)),
			"bar panel did not resolve: {}",
			state.status_error
		);
		assert_eq!(
			ids,
			(0..3)
				.map(|position| {
					state
						.code_editor
						.rune_view_panel_id(source_buffer, &LocalViewIdentity::Anonymous(position))
						.expect("stable presentation panel")
				})
				.collect::<Vec<_>>()
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn view_identity_reuses_retargets_or_orphans_one_panel_as_declared() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(82);
		let stable = LocalViewIdentity::Named("stable".to_owned());
		let anonymous = LocalViewIdentity::Anonymous(0);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(stable.clone(), RuneViewForm::Table)],
			1,
		);
		let stable_panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &stable)
			.expect("stable panel");
		state.code_editor.set_rune_view_table(
			stable_panel,
			polars::df!("x" => [1_i64], "y" => [2_i64]).expect("initial table"),
		);
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(stable.clone(), RuneViewForm::Scatter)],
			2,
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &stable),
			Some(stable_panel),
			"a named form change keeps the destination"
		);
		let changed = transform_rune_view(
			"workspace.table(\"t\").views([view(\"stable\", scatter(\"x\", \"y\"))])",
		)
		.expect("named scatter")
		.into_relations()
		.next()
		.expect("one relation");
		let changed_outcome = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).expect("plot frame"),
			changed.views,
		);
		let resolved = changed_outcome
			.sinks
			.into_iter()
			.find_map(|sink| match sink.output {
				Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _)) => {
					Some(resolved)
				}
				_ => None,
			})
			.expect("resolved scatter");
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		state.code_editor.set_rune_view_plot(
			stable_panel,
			"plot.scatter".to_owned(),
			kernel,
			crate::plot::theme::PlotSettingsLayer::default(),
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &stable)
		);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[
				(stable.clone(), RuneViewForm::Scatter),
				(anonymous.clone(), RuneViewForm::Table),
			],
			3,
		);
		let anonymous_panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &anonymous)
			.expect("anonymous panel");
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[
				(stable.clone(), RuneViewForm::Scatter),
				(anonymous.clone(), RuneViewForm::Bar),
			],
			4,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &anonymous),
			Some(anonymous_panel),
			"anonymous identity is its presentation slot"
		);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(anonymous.clone(), RuneViewForm::Bar)],
			5,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &stable),
			Some("orphaned")
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &stable),
			Some(stable_panel),
			"orphaning retains the destination"
		);
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(stable.clone(), RuneViewForm::Table)],
			6,
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &stable),
			Some(stable_panel),
		);
		state.code_editor.set_rune_view_table(
			stable_panel,
			polars::df!("restored" => [true]).expect("restored table"),
		);

		let invalid =
			"workspace.table(\"t\").views([view(\"same\", table()), view(\"same\", table())])";
		assert_eq!(
			super::run_rune_code(&mut state, source_buffer, invalid.to_owned()).units(),
			1
		);
		assert_eq!(
			complete_rune_presentation(&mut state, source_buffer, invalid).units(),
			0
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &stable),
			Some("stale"),
			"an envelope failure is not deletion"
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &stable),
			"the diagnostic belongs to the toolbar, not over prior panel content"
		);
		assert!(state.status_error.starts_with("Rune · "));
		assert!(state.status_detail.is_some());

		let first_failure = LocalViewIdentity::Named("first-failure".to_owned());
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(first_failure.clone(), RuneViewForm::Table)],
			7,
		);
		let failed_panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &first_failure)
			.expect("first-failure panel");
		state.code_editor.fail_rune_view(failed_panel);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_failed(source_buffer, &first_failure)
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn failed_origin_keeps_prior_content_stale_while_its_sibling_refreshes() {
		let source = r#"
workspace.table("good").views([view("good", table())]);
workspace.table("bad").views([view("bad", table())]);
"#;
		let program = transform_rune_view(source).expect("isolated named origins");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		let good = LocalViewIdentity::Named("good".to_owned());
		let bad = LocalViewIdentity::Named("bad".to_owned());
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(83);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		for identity in [&good, &bad] {
			let panel = state
				.code_editor
				.rune_view_panel_id(source_buffer, identity)
				.expect("named panel");
			state.code_editor.set_rune_view_table(
				panel,
				polars::df!("previous" => [1_i64]).expect("previous content"),
			);
		}
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 2);
		state.rune_buffer_revisions.insert(source_buffer, 2);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 2,
					view: program.clone(),
				})
				.is_some()
		);
		let mut relations = program.into_relations().collect::<Vec<_>>();
		let bad_relation = relations
			.iter()
			.position(|relation| relation.origin.contains("table(\"bad\")"))
			.map(|index| relations.remove(index))
			.expect("bad relation");
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 2,
				origin: bad_relation.origin,
				views: bad_relation
					.views
					.into_iter()
					.map(|view| view.identity)
					.collect(),
				result: Err("bad origin failed".to_owned()),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &bad),
			Some("stale")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &good),
			Some("running")
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &bad),
			"stale preserves the prior frame"
		);

		complete_relation_in_head(
			&mut state,
			source_buffer,
			2,
			relations.pop().expect("good relation"),
			polars::df!("current" => [2_i64]).expect("fresh sibling content"),
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &good),
			Some("fresh")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &bad),
			Some("stale")
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_table(source_buffer, &good)
				.expect("fresh good table")
				.column("current")
				.is_ok()
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn fenced_sql_diagnostics_report_document_coordinates() {
		let raw = "SQLite parse error: sql parser error: Expected: column name or constraint definition, found: ) at Line: 4, Column: 1";
		let (summary, full) = compact_sql_diagnostic(raw, Some(6));

		assert_eq!(
			summary,
			"SQL · line 9, col 1 · expected column name or constraint definition, found ')'"
		);
		assert!(full.starts_with(&summary));
		assert!(full.contains(raw));
	}

	#[test]
	fn rune_diagnostics_compact_the_toolbar_and_keep_full_hover_detail() {
		let raw = "Rune view syntax at bytes 67..81: unknown view sink; expected scatter(...), bar(...), line(...), or table()";
		let (summary, full) = compact_rune_diagnostic(raw);

		assert_eq!(
			summary,
			"Rune · bytes 67..81 · unknown view sink; expected scatter(...), bar(...), line(...), or table()"
		);
		assert_eq!(full, raw);
	}

	#[test]
	fn product_0075_plain_plot_owns_anonymous_zero_and_orphans_when_removed() {
		use crate::gui::messages::RuneBufferOutcome;
		use crate::plot::composition::PlotComposition;
		use crate::plot::core::PlotType;
		use crate::plot::factory::create_plot;
		use crate::plot::request::{PlotRequest, PlotSource, plan};
		use crate::plot::spec::AuthoredPlotSpec;
		use crate::rune_view::{LocalViewIdentity, ViewContentKind, ViewPresentation};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(7500);
		let identity = LocalViewIdentity::Anonymous(0);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "plain".to_owned(),
				data: crate::result_artifact::ResultArtifact::test_frame(
					polars::df!("x" => [1.0_f64, 2.0], "y" => [3.0_f64, 5.0]).expect("plain frame"),
				),
			},
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
			),
		})
		.expect("plain plot resolves");
		let ready = resolved.clone();
		let display_label = ready.resolved_spec.composition.title();
		let kernel = create_plot(&ready.resolved_spec, &ready.data, 1200, 720);

		state.rune_buffer_revisions.insert(source_buffer, 1);
		assert_eq!(
			update(
				&mut state,
				Message::RunRuneBufferResult {
					source_buffer,
					revision: 1,
					result: Ok(RuneBufferOutcome {
						display: None,
						plot: Some(resolved),
					}),
				},
			)
			.units(),
			1,
			"one plain plot schedules one kernel build"
		);
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.expect("plain Anonymous(0) panel");
		assert_ne!(
			panel_id.get(),
			2,
			"plain output never reuses the retired role"
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &identity),
			Some("running")
		);

		drop(update(
			&mut state,
			Message::RuneBufferPlotReady {
				source_buffer,
				revision: 1,
				panel_id,
				display_label,
				resolved: ready,
				kernel,
			},
		));
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &identity)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &identity),
			Some("fresh")
		);

		state.rune_buffer_revisions.insert(source_buffer, 2);
		drop(update(
			&mut state,
			Message::RunRuneBufferResult {
				source_buffer,
				revision: 2,
				result: Ok(RuneBufferOutcome {
					display: Some("plain value".to_owned()),
					plot: None,
				}),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &identity),
			Some("orphaned"),
			"removing the sole plot reconciles an empty view set"
		);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[
				(
					identity.clone(),
					ViewPresentation {
						content_kind: ViewContentKind::Plot,
						display_label: "plot.scatter".to_owned(),
						authored_title: crate::plot::spec::Presence::Inherit,
					},
				),
				(
					LocalViewIdentity::Named("named".to_owned()),
					ViewPresentation {
						content_kind: ViewContentKind::Plot,
						display_label: "plot.scatter".to_owned(),
						authored_title: crate::plot::spec::Presence::Inherit,
					},
				),
			],
			3,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity),
			Some(panel_id),
			"a relation's first anonymous sink reuses the plain output identity"
		);
		assert_ne!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &LocalViewIdentity::Named("named".to_owned())),
			Some(panel_id),
			"a named sink remains a distinct semantic identity"
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn product_0075_markdown_pane_plot_uses_results_while_inline_and_none_do_not() {
		use crate::gui::messages::ExportFormat;
		use crate::plot::composition::PlotComposition;
		use crate::plot::core::PlotType;
		use crate::plot::factory::create_plot;
		use crate::plot::request::{PlotRequest, PlotSource, plan};
		use crate::plot::spec::AuthoredPlotSpec;
		use libaryon::uast::projection::{
			MarkdownCellCodeVisibility, MarkdownCellIdentity, MarkdownCellOutputTarget,
			MarkdownCellPresentation,
		};
		use std::sync::Arc;

		fn fixture() -> (
			crate::plot::request::ResolvedPlot,
			Arc<dyn crate::plot::common::PlotKernel + Send + Sync>,
		) {
			let resolved = plan(PlotRequest {
				source: PlotSource::Materialized {
					name: "markdown".to_owned(),
					data: crate::result_artifact::ResultArtifact::test_frame(
						polars::df!("x" => [1.0_f64, 2.0], "y" => [2.0_f64, 3.0])
							.expect("markdown frame"),
					),
				},
				authored_spec: AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::Line),
					"x",
					"y",
				),
			})
			.expect("markdown plot resolves");
			let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 720, 720);
			(resolved, kernel)
		}

		let identity = MarkdownCellIdentity::Explicit {
			id: "chart".to_owned(),
		};
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let (resolved, kernel) = fixture();
		drop(update(
			&mut state,
			Message::NotebookPlotReady {
				identity: identity.clone(),
				presentation: MarkdownCellPresentation {
					output: MarkdownCellOutputTarget::Pane,
					code: MarkdownCellCodeVisibility::Show,
				},
				resolved,
				kernel,
			},
		));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state.code_editor.host_plot_exports().is_empty()
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state.code_editor.host_plot_exports().len(),
			1,
			"pane-targeted Markdown plot occupies the shared visible Results panel"
		);
		let export_root = std::env::temp_dir().join(format!(
			"polariton-product-0075-export-{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&export_root);
		std::fs::create_dir(&export_root).expect("create export fixture directory");
		for (ordinal, format, extension) in [
			(1, ExportFormat::SVG, "svg"),
			(2, ExportFormat::PNG, "png"),
			(3, ExportFormat::AVIF, "avif"),
		] {
			let mut snapshot = state.code_editor.host_plot_exports().remove(0);
			snapshot.width = 160.0;
			snapshot.height = 100.0;
			super::render_plot_export(snapshot, format, ordinal).write_to(&export_root);
			let path = export_root.join(format!("plot_{ordinal}.{extension}"));
			let bytes = std::fs::read(&path).expect("rendered export artifact");
			assert!(bytes.len() > 32, "{format} export is non-empty");
			match format {
				ExportFormat::SVG => assert!(
					String::from_utf8(bytes).unwrap().contains("<svg"),
					"SVG export carries the rendered plot document"
				),
				ExportFormat::PNG => assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n"),
				ExportFormat::AVIF => assert!(
					bytes.windows(8).any(|window| window == b"ftypavif"),
					"AVIF export carries the expected container brand"
				),
			}
		}
		std::fs::remove_dir_all(&export_root).expect("remove export fixture directory");
		state.code_editor.shutdown();

		for output in [
			MarkdownCellOutputTarget::Inline,
			MarkdownCellOutputTarget::None,
		] {
			let (mut control, startup) = new(StartupData::default(), None);
			drop(startup);
			let (resolved, kernel) = fixture();
			drop(update(
				&mut control,
				Message::NotebookPlotReady {
					identity: identity.clone(),
					presentation: MarkdownCellPresentation {
						output,
						code: MarkdownCellCodeVisibility::Show,
					},
					resolved,
					kernel,
				},
			));
			assert!(
				!control.code_editor.has_query_surface(),
				"{output:?} must not manufacture the shared Results panel"
			);
			assert_eq!(
				control.code_editor.markdown_cell_is_plot(&identity),
				output == MarkdownCellOutputTarget::Inline,
				"inline retains native content while none retains no output"
			);
			control.code_editor.shutdown();
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn rune_execution_error_uses_toolbar_without_manufacturing_a_result_pane() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(97);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		let raw = "execution failed at row 2";

		let _ = update(
			&mut state,
			Message::RunRuneBufferResult {
				source_buffer,
				revision: 1,
				result: Err(raw.to_owned()),
			},
		);

		assert_eq!(state.status_error, "Rune · execution failed at row 2");
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.full.as_str()),
			Some(raw)
		);
		assert!(!state.code_editor.has_query_surface());
		state.code_editor.shutdown();
	}

	#[test]
	fn rune_configuration_failure_is_projected_without_blocking_startup() {
		let startup_data = StartupData {
			init_error: Some("init.rn compile error: broken fixture".to_owned()),
			..StartupData::default()
		};
		let (mut state, startup) = new(startup_data, None);
		drop(startup);

		assert_eq!(state.status_error, "init.rn · configuration failed");
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.full.as_str()),
			Some("init.rn compile error: broken fixture"),
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0030_rune_default_authored_palette_and_unused_settings_are_exact() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1030);
		let categories = ["A", "B", "C", "D", "E", "F"];

		let default = run_one_relation_program(
			&mut state,
			source_buffer,
			1,
			"workspace.table(\"t\").views([view(\"plot\", bar(\"cat\", \"val\"))])",
			categorical_bar_frame(&categories),
		);
		assert!(
			default[0]
				.1
				.resolved_spec
				.channel(crate::plot::spec::Channel::Color)
				.is_none()
		);
		assert_eq!(
			band_mapping(&default[0].1),
			[
				("A".to_owned(), 0, (68, 1, 83)),
				("B".to_owned(), 4, (60, 69, 130)),
				("C".to_owned(), 8, (41, 125, 140)),
				("D".to_owned(), 11, (52, 162, 127)),
				("E".to_owned(), 15, (119, 205, 88)),
				("F".to_owned(), 19, (253, 231, 36)),
			]
		);

		let tab20c = run_one_relation_program(
			&mut state,
			source_buffer,
			2,
			r#"workspace.table("t").views([view("plot", figure([
				title("Categories"), palette("tab20c"), bar("cat", "val")
			]))])"#,
			categorical_bar_frame(&categories),
		);
		assert_eq!(
			band_mapping(&tab20c[0].1),
			[
				("A".to_owned(), 0, (49, 130, 189)),
				("B".to_owned(), 4, (230, 85, 13)),
				("C".to_owned(), 8, (49, 163, 84)),
				("D".to_owned(), 11, (199, 233, 192)),
				("E".to_owned(), 15, (218, 218, 235)),
				("F".to_owned(), 19, (150, 150, 150)),
			]
		);
		let panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &LocalViewIdentity::Named("plot".to_owned()))
			.expect("stable palette panel");

		let yellows = run_one_relation_program(
			&mut state,
			source_buffer,
			3,
			r#"workspace.table("t").views([view("plot", figure([
				palette("yellows"), bar("cat", "val")
			]))])"#,
			categorical_bar_frame(&categories),
		);
		assert_eq!(
			band_mapping(&yellows[0].1),
			[
				("A".to_owned(), 0, (252, 252, 204)),
				("B".to_owned(), 4, (252, 237, 139)),
				("C".to_owned(), 8, (252, 222, 75)),
				("D".to_owned(), 11, (236, 198, 42)),
				("E".to_owned(), 15, (194, 150, 21)),
				("F".to_owned(), 19, (153, 102, 0)),
			],
			"a weak authored choice reaches the artifact without aesthetic admission"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &LocalViewIdentity::Named("plot".to_owned())),
			Some(panel)
		);

		for (revision, body) in [(4, "line(\"x\", \"y\")"), (5, "scatter(\"x\", \"y\")")] {
			let source = format!(
				"workspace.table(\"t\").views([view(\"plot\", figure([palette(\"tab20c\"), {body}]))])"
			);
			let plots = run_one_relation_program(
				&mut state,
				source_buffer,
				revision,
				&source,
				polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap(),
			);
			assert!(plots[0].1.resolved_spec.band_fills.is_empty());
			assert!(
				plots[0]
					.1
					.resolved_spec
					.channel(crate::plot::spec::Channel::Color)
					.is_none()
			);
		}

		let scatter = run_one_relation_program(
			&mut state,
			source_buffer,
			6,
			r#"workspace.table("t").views([view("plot", figure([
				palette("tab20c"), scatter("x", "y", "cat")
			]))])"#,
			polars::df!(
				"x" => [1_i64, 2, 3, 4, 5, 6],
				"y" => [6_i64, 5, 4, 3, 2, 1],
				"cat" => categories,
			)
			.unwrap(),
		);
		assert!(scatter[0].1.resolved_spec.band_fills.is_empty());
		assert_eq!(
			scatter[0]
				.1
				.resolved_spec
				.channel(crate::plot::spec::Channel::Color)
				.expect("authored categorical color channel")
				.colors
				.iter()
				.map(|assignment| (
					assignment.category.as_str(),
					assignment.position.index(),
					(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
					),
				))
				.collect::<Vec<_>>(),
			[
				("A", 0, (49, 130, 189)),
				("B", 4, (230, 85, 13)),
				("C", 8, (49, 163, 84)),
				("D", 11, (199, 233, 192)),
				("E", 15, (218, 218, 235)),
				("F", 19, (150, 150, 150)),
			]
		);

		let layered = run_one_relation_program(
			&mut state,
			source_buffer,
			7,
			r#"workspace.table("t").views([view("plot", figure([
				palette("tab20c"), layers([line("x", "y"), scatter("x", "y")])
			]))])"#,
			polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap(),
		);
		assert!(
			layered.is_empty(),
			"layered content has no categorical proposal"
		);
		assert!(
			state.code_editor.rune_view_panel_is_plot(
				source_buffer,
				&LocalViewIdentity::Named("plot".to_owned())
			),
			"an unused valid palette cannot block layered completion"
		);

		for (palette, expected) in [("not-a-theme", "not-a-theme"), ("nord", "categorical")] {
			let source = format!(
				"workspace.table(\"t\").views([figure([palette(\"{palette}\"), line(\"x\", \"y\")])])"
			);
			let relation = transform_rune_view(&source)
				.expect("selection failure is below the envelope")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!("x" => [1_i64], "y" => [2_i64]).unwrap(),
				relation.views,
			);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("structurally impossible selection must refuse")
			};
			assert!(error.contains(expected), "unhelpful error: {error}");
		}

		for (palette, cardinality, palette_limit) in
			[("dark2", 9, 8), ("tab20c", 13, 20), ("dark2", 13, 8)]
		{
			let source = format!(
				"workspace.table(\"t\").views([figure([palette(\"{palette}\"), bar(\"cat\", \"val\")])])"
			);
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let categories = (0..cardinality)
				.map(|index| format!("c{index}"))
				.collect::<Vec<_>>();
			let category_refs = categories.iter().map(String::as_str).collect::<Vec<_>>();
			let outcome = rune_view_outcome(categorical_bar_frame(&category_refs), relation.views);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("capacity failure cannot fall back or truncate")
			};
			assert!(error.contains(&format!("requests {cardinality} identities")));
			assert!(error.contains("geometry limit 12"));
			assert!(error.contains(&format!("palette `{palette}` limit {palette_limit}")));
		}
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0036_numeric_bounds_apply_per_axis_and_discrete_bounds_never_disappear() {
		let y_source =
			"workspace.table(\"t\").views([figure([y_bounds(-1.0, 1.0), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(y_source)
			.expect("numeric y bound")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B", "C"]), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("numeric bar y bound must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(settings.x_min, None);
		assert_eq!(settings.x_max, None);
		assert_eq!((settings.y_min, settings.y_max), (Some(-1.0), Some(1.0)));
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::CategoricalX { y_range, .. } if y_range == (-1.0, 1.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		let x_source =
			"workspace.table(\"t\").views([figure([x_bounds(0.0, 1.0), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(x_source)
			.expect("structurally valid x bound")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical x bound must refuse");
		};
		assert!(error.contains("requires a numeric x axis"), "{error}");
		assert!(error.contains("Band"), "{error}");
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&x_source[span.start as usize..span.end as usize],
			"x_bounds(0.0, 1.0)"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0055_horizontal_bar_keeps_physical_axes_and_family_label_through_completion() {
		let source = r#"workspace.table("t").views([figure([
			x_bounds(0, 10), x_tick_step(1), x_minor_ticks(2),
			bar("value", "category", "series")
		])])"#;
		let relation = transform_rune_view(source)
			.expect("horizontal bar remains the bar surface")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!(
				"category" => ["East", "East", "West", "West"],
				"value" => [2.0, 4.0, 6.0, 8.0],
				"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			)
			.unwrap(),
			relation.views,
		);
		let sink = &outcome.sinks[0];
		assert_eq!(
			sink.presentation.display_label, "plot.bar",
			"label names authored family"
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&sink.output
		else {
			panic!("pair-resolved horizontal bar must complete");
		};
		assert_eq!(
			resolved.resolved_spec.composition.primary_plot_type(),
			PlotType::HorizontalBar
		);
		assert_eq!(
			resolved.resolved_spec.composition.title(),
			"Horizontal Bar Plot",
			"composition names resolved geometry"
		);
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!((settings.x_min, settings.x_max), (Some(0.0), Some(10.0)));
		assert_eq!(
			settings.x_tick_placement,
			crate::plot::common::TickPlacement::step(1.0).unwrap()
		);
		assert_eq!(settings.x_minor_ticks, 2);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::CategoricalY { x_range, .. } if x_range == (0.0, 10.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		for modifier in ["y_bounds(0, 10)", "y_tick_step(1)", "y_minor_ticks(2)"] {
			let refusing = format!(
				"workspace.table(\"t\").views([figure([{modifier}, bar(\"value\", \"category\")])])"
			);
			let relation = transform_rune_view(&refusing)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!("category" => ["East"], "value" => [2.0]).unwrap(),
				relation.views,
			);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("categorical y modifier must refuse: {modifier}");
			};
			assert!(error.contains("Band"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&refusing[span.start as usize..span.end as usize], modifier);
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0064_stacked_members_keep_family_label_and_physical_axis_rules() {
		let frame = polars::df!(
			"category" => ["East", "East", "West", "West"],
			"value" => [2.0, 4.0, 6.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();
		for (source, expected, title, expected_layout) in [
			(
				r#"workspace.table("t").views([figure([
					y_bounds(0, 20), y_tick_step(2), y_minor_ticks(2),
					bar("category", "value", "series").stacked()
				])])"#,
				PlotType::StackedBar,
				"Stacked Bar Plot",
				"vertical",
			),
			(
				r#"workspace.table("t").views([figure([
					x_bounds(0, 20), x_tick_step(2), x_minor_ticks(2),
					bar("value", "category", "series").stacked()
				])])"#,
				PlotType::HorizontalStackedBar,
				"Horizontal Stacked Bar Plot",
				"horizontal",
			),
		] {
			let relation = transform_rune_view(source)
				.expect("stacked bar stays inside the authored Bar family")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let sink = &outcome.sinks[0];
			assert_eq!(
				sink.presentation.display_label, "plot.bar",
				"the label names authored intent and must survive stacking"
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
				resolved,
				layer,
				authored,
				_,
			)) = &sink.output
			else {
				panic!("stacked bar must complete through the standalone styled path")
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				expected
			);
			assert_eq!(
				resolved.resolved_spec.composition.title(),
				title,
				"composition names resolved geometry and must differ from the family label"
			);
			let settings = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			};
			let resolved_settings = settings.resolve_for_layer(layer);
			let kernel = create_styled_plot(&resolved.resolved_spec, &resolved.data, layer.clone())
				.expect("canonical stacked member reaches the styled factory");
			match (expected_layout, kernel.layout(resolved_settings.clone())) {
				("vertical", PlotLayout::CategoricalX { y_range, .. }) => {
					assert_eq!(y_range, (0.0, 20.0));
					assert_eq!(
						resolved_settings.y_tick_placement,
						crate::plot::common::TickPlacement::step(2.0).unwrap()
					);
					assert_eq!(resolved_settings.y_minor_ticks, 2);
				}
				("horizontal", PlotLayout::CategoricalY { x_range, .. }) => {
					assert_eq!(x_range, (0.0, 20.0));
					assert_eq!(
						resolved_settings.x_tick_placement,
						crate::plot::common::TickPlacement::step(2.0).unwrap()
					);
					assert_eq!(resolved_settings.x_minor_ticks, 2);
				}
				_ => panic!("wrong stacked physical layout"),
			}
			assert!(
				render_marks_with_settings(kernel.as_ref(), resolved_settings).contains("<svg")
			);
		}

		for (modifier, bar) in [
			("x_bounds(0, 20)", "bar(\"category\", \"value\").stacked()"),
			("y_bounds(0, 20)", "bar(\"value\", \"category\").stacked()"),
		] {
			let source = format!("workspace.table(\"t\").views([figure([{modifier}, {bar}])])");
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("category-axis stacked setting must refuse")
			};
			assert!(error.contains("Band"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], modifier);
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0072_percent_completion_fixes_value_domain_and_refuses_bounds() {
		let frame = polars::df!(
			"category" => ["East", "East", "West", "West"],
			"value" => [30.0, 90.0, 20.0, 20.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"x" => [0.0, 0.0, 1.0, 1.0],
		)
		.unwrap();
		for (source, expected, axis) in [
			(
				r#"workspace.table("t").views([figure([
					y_tick_step(25), bar("category", "value", "series").stacked().percent()
				])])"#,
				PlotType::StackedBar,
				"y",
			),
			(
				r#"workspace.table("t").views([figure([
					x_tick_step(25), bar("value", "category", "series").percent().stacked()
				])])"#,
				PlotType::HorizontalStackedBar,
				"x",
			),
			(
				r#"workspace.table("t").views([figure([
					y_tick_step(25), area("x", "value", "series").percent()
				])])"#,
				PlotType::StackedArea,
				"y",
			),
		] {
			let relation = transform_rune_view(source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
				resolved,
				layer,
				authored,
				_,
			)) = &outcome.sinks[0].output
			else {
				panic!("percent mark must complete through the styled path")
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				expected
			);
			assert_eq!(
				resolved.resolved_spec.stack_normalization,
				crate::plot::spec::StackNormalization::Percent
			);
			let model = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			};
			let settings = model.resolve_for_layer(layer);
			let kernel = create_styled_plot(&resolved.resolved_spec, &resolved.data, layer.clone())
				.expect("both percent families reach the styled factory");
			match (axis, kernel.layout(settings.clone())) {
				(
					"y",
					PlotLayout::CategoricalX {
						y_range,
						y_axis_type,
						..
					},
				) => {
					assert_eq!(y_range, (0.0, 100.0));
					assert_eq!(y_axis_type, crate::plot::common::AxisType::Percent);
				}
				(
					"x",
					PlotLayout::CategoricalY {
						x_range,
						x_axis_type,
						..
					},
				) => {
					assert_eq!(x_range, (0.0, 100.0));
					assert_eq!(x_axis_type, crate::plot::common::AxisType::Percent);
				}
				(
					"y",
					PlotLayout::Cartesian {
						y_range,
						y_axis_type,
						..
					},
				) => {
					assert_eq!(y_range, (0.0, 100.0));
					assert_eq!(y_axis_type, crate::plot::common::AxisType::Percent);
				}
				_ => panic!("wrong percent layout"),
			}
		}

		for (modifier, mark) in [
			(
				"y_bounds(0, 100)",
				"bar(\"category\", \"value\", \"series\").stacked().percent()",
			),
			(
				"x_bounds(0, 100)",
				"bar(\"value\", \"category\", \"series\").stacked().percent()",
			),
			(
				"y_bounds(0, 100)",
				"area(\"x\", \"value\", \"series\").percent()",
			),
		] {
			let source = format!("workspace.table(\"t\").views([figure([{modifier}, {mark}])])");
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("percent value bounds must refuse")
			};
			assert!(error.contains("fixed 0..100 percent domain"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], modifier);
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0070_area_reaches_completion_with_cartesian_guides_and_fixed_orientation() {
		let source = r#"workspace.table("t").views([figure([
			y_bounds(0, 20), y_tick_step(2), y_minor_ticks(2),
			area("x", "value", "series")
		])])"#;
		let relation = transform_rune_view(source)
			.expect("area surface envelope")
			.into_relations()
			.next()
			.unwrap();
		let frame = polars::df!(
			"x" => [1.0_f64, 1.0, 2.0, 2.0],
			"value" => [2.0_f64, 4.0, 6.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();
		let outcome = rune_view_outcome(frame, relation.views);
		let sink = &outcome.sinks[0];
		assert_eq!(sink.presentation.display_label, "plot.area");
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&sink.output
		else {
			panic!("area must complete through the standalone plot path")
		};
		assert_eq!(
			resolved.resolved_spec.composition.primary_plot_type(),
			PlotType::StackedArea
		);
		assert_eq!(
			resolved.resolved_spec.composition.title(),
			"Stacked Area Plot"
		);
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!((settings.y_min, settings.y_max), (Some(0.0), Some(20.0)));
		assert_eq!(
			settings.y_tick_placement,
			crate::plot::common::TickPlacement::step(2.0).unwrap()
		);
		assert_eq!(settings.y_minor_ticks, 2);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::Cartesian { y_range, .. } if y_range == (0.0, 20.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		let reversed = "workspace.table(\"t\").views([area(\"category\", \"value\")])";
		let relation = transform_rune_view(reversed)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("category" => ["East"], "value" => [2.0_f64]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical area x must refuse rather than rotate")
		};
		assert!(
			error.contains("category") && error.contains("incompatible"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&reversed[span.start as usize..span.end as usize],
			"area(\"category\", \"value\")",
			"the area sink span carries the resolved-field refusal"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0055_pair_refusal_is_attributed_to_the_complete_bar_sink() {
		let source = "workspace.table(\"t\").views([bar(\"revenue\", \"profit\")])";
		let relation = transform_rune_view(source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("revenue" => [1.0], "profit" => [2.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("two quantitative bar axes must refuse as a pair");
		};
		assert!(error.contains(
			"bar requires exactly one quantitative value axis and one non-quantitative category axis; x `revenue` resolved as Quantitative, y `profit` resolved as Quantitative"
		));
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"bar(\"revenue\", \"profit\")"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0058_bound_fields_resolve_before_bar_pair_and_channel_resolution() {
		let bound_source = r#"
let x_field = "value";
let y_field = "category";
let series_field = "series";
workspace.table("t").views([bar(x_field, y_field, series_field)])
"#;
		let inline_source =
			"workspace.table(\"t\").views([bar(\"value\", \"category\", \"series\")])";
		let frame = polars::df!(
			"category" => ["East", "East", "West", "West"],
			"value" => [2.0, 4.0, 6.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();

		let complete = |source: &str| {
			let relation = transform_rune_view(source)
				.expect("static fields lower before dispatch")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame.clone(), relation.views)
		};
		let bound = complete(bound_source);
		let inline = complete(inline_source);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(bound_plot, _, _)) =
			&bound.sinks[0].output
		else {
			panic!("bound horizontal bar must complete")
		};
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(inline_plot, _, _)) =
			&inline.sinks[0].output
		else {
			panic!("inline horizontal bar must complete")
		};

		use crate::plot::spec::{AuthoredChannel, Channel, OptionalAuthoredChannel};
		assert_eq!(
			bound_plot.authored_spec.x.field,
			AuthoredChannel::Named("value".to_owned())
		);
		assert_eq!(
			bound_plot.authored_spec.y.field,
			AuthoredChannel::Named("category".to_owned())
		);
		assert_eq!(
			bound_plot.authored_spec.color.field,
			OptionalAuthoredChannel::Named("series".to_owned())
		);
		assert_eq!(
			bound_plot.resolved_spec.composition.primary_plot_type(),
			PlotType::HorizontalBar
		);
		assert_eq!(
			bound_plot
				.resolved_spec
				.channel(Channel::X)
				.unwrap()
				.binding
				.field,
			"value"
		);
		assert_eq!(
			bound_plot
				.resolved_spec
				.channel(Channel::Y)
				.unwrap()
				.binding
				.field,
			"category"
		);
		assert_eq!(
			bound_plot
				.resolved_spec
				.channel(Channel::Color)
				.unwrap()
				.binding
				.field,
			"series"
		);
		assert_eq!(
			bound_plot.resolved_spec, inline_plot.resolved_spec,
			"binding identifiers must not enter the authored or resolved plot"
		);

		let invalid_source = r#"
let x_field = "revenue";
let y_field = "profit";
workspace.table("t").views([bar(x_field, y_field)])
"#;
		let relation = transform_rune_view(invalid_source)
			.expect("both bindings resolve statically")
			.into_relations()
			.next()
			.unwrap();
		let invalid = rune_view_outcome(
			polars::df!("revenue" => [1.0], "profit" => [2.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &invalid.sinks[0].output else {
			panic!("two quantitative bound fields must refuse as a pair")
		};
		assert!(
			error.contains(
				"x `revenue` resolved as Quantitative, y `profit` resolved as Quantitative"
			),
			"{error}"
		);
		let span = invalid.sinks[0].span.clone();
		assert_eq!(
			&invalid_source[span.start as usize..span.end as usize],
			"bar(x_field, y_field)"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0058_bound_third_fields_share_inline_color_resolution_for_every_mark() {
		let frame = polars::df!(
			"cat" => ["East", "East", "West", "West"],
			"value" => [2.0, 4.0, 6.0, 8.0],
			"x" => [1.0, 2.0, 1.0, 2.0],
			"y" => [3.0, 4.0, 7.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();
		for (mark, x, y) in [
			("bar", "cat", "value"),
			("line", "x", "y"),
			("scatter", "x", "y"),
		] {
			let inline_source =
				format!("workspace.table(\"t\").views([{mark}(\"{x}\", \"{y}\", \"series\")])");
			let bound_source = format!(
				r#"
let first_field = "{x}";
let second_field = "{y}";
let color_field = "series";
workspace.table("t").views([{mark}(first_field, second_field, color_field)])
"#
			);
			let complete = |source: &str| {
				let relation = transform_rune_view(source)
					.expect("bound categorical field")
					.into_relations()
					.next()
					.unwrap();
				rune_view_outcome(frame.clone(), relation.views)
			};
			let inline = complete(&inline_source);
			let bound = complete(&bound_source);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(inline_plot, _, _)) =
				&inline.sinks[0].output
			else {
				panic!("inline {mark} must complete")
			};
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(bound_plot, _, _)) =
				&bound.sinks[0].output
			else {
				panic!("bound {mark} must complete")
			};

			use crate::plot::spec::{Channel, OptionalAuthoredChannel};
			assert_eq!(
				bound_plot.authored_spec.color.field,
				OptionalAuthoredChannel::Named("series".to_owned())
			);
			let bound_color = bound_plot
				.resolved_spec
				.channel(Channel::Color)
				.expect("bound third field becomes a resolved color channel");
			assert_eq!(bound_color.binding.field, "series");
			assert_eq!(bound_color.colors.len(), 2);
			assert_eq!(
				bound_plot.resolved_spec, inline_plot.resolved_spec,
				"bound {mark} must inherit exact inline colors and domains"
			);
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0055_horizontal_pair_reaches_every_bar_spelling_and_its_register() {
		let sources = [
			"plot.bar(workspace.table(\"t\"), \"value\", \"category\", \"series\")",
			"workspace.table(\"t\").bar(\"value\", \"category\", \"series\")",
			"workspace.table(\"t\").views([bar(\"value\", \"category\", \"series\")])",
			"workspace.table(\"t\").views([view(\"bars\", bar(\"value\", \"category\", \"series\"))])",
			"workspace.table(\"t\").views([figure([bar(\"value\", \"category\", \"series\")])])",
		];
		for source in sources {
			let relation = transform_rune_view(source)
				.expect("every established bar spelling admits the horizontal pair")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!(
					"category" => ["East"],
					"value" => [2.0],
					"series" => ["Atlas"],
				)
				.unwrap(),
				relation.views,
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _)) =
				&outcome.sinks[0].output
			else {
				panic!("horizontal spelling did not complete: {source}");
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				PlotType::HorizontalBar
			);
			assert_eq!(outcome.sinks[0].presentation.display_label, "plot.bar");
		}

		for source in [
			"workspace.table(\"t\").views([figure([bar(\"value\", \"category\").bar_width(0.5).category_gap(0.2)])])",
			"workspace.table(\"t\").views([figure([bar(\"value\", \"category\", \"series\").category_gap(0.2).bar_width(0.5)])])",
		] {
			let relation = transform_rune_view(source)
				.expect("the whole bar register is orientation-independent")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!(
					"category" => ["East"],
					"value" => [2.0],
					"series" => ["Atlas"],
				)
				.unwrap(),
				relation.views,
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(resolved, layer, _, _)) =
				&outcome.sinks[0].output
			else {
				panic!("modified horizontal bar did not complete: {source}");
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				PlotType::HorizontalBar
			);
			assert_eq!(
				layer.bar_width,
				crate::plot::spec::Override::Set(crate::plot::spec::BarWidth::new(0.5).unwrap())
			);
			assert_eq!(
				layer.category_gap,
				crate::plot::spec::Override::Set(crate::plot::spec::CategoryGap::new(0.2).unwrap())
			);
			let kernel = create_styled_plot(&resolved.resolved_spec, &resolved.data, layer.clone())
				.expect("resolved horizontal member reaches the styled bar factory");
			assert!(matches!(
				kernel.layout(ResolvedPlotSettings::default()),
				PlotLayout::CategoricalY { .. }
			));
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0036_exclusion_and_arbitrarily_small_positive_spans_render_exactly() {
		for (minimum, maximum) in [(100.0, 200.0), (0.0, 1e-300)] {
			let source = format!(
				"workspace.table(\"t\").views([figure([x_bounds({minimum}, {maximum}), line(\"x\", \"y\")])])"
			);
			let relation = transform_rune_view(&source)
				.expect("strictly ordered finite bounds")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!("x" => [1.0_f64, 6.0], "y" => [1.0_f64, 6.0]).unwrap(),
				relation.views,
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
				&outcome.sinks[0].output
			else {
				panic!("defined finite bounds must complete: {minimum}..{maximum}");
			};
			let settings = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			}
			.resolve();
			assert_eq!(
				(settings.x_min, settings.x_max),
				(Some(minimum), Some(maximum))
			);
			let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
			assert!(matches!(
				kernel.layout(settings.clone()),
				PlotLayout::Cartesian { x_range, .. } if x_range == (minimum, maximum)
			));
			let svg = render_marks_with_settings(kernel.as_ref(), settings);
			assert!(svg.contains("<svg"));
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0036_layered_bounds_reach_one_shared_cartesian_domain() {
		let source = r#"workspace.table("t").views([figure([
			x_bounds(-2.0, 2.0), y_bounds(0.0, 8.0),
			layers([line("x", "y"), scatter("x", "y")])
		])])"#;
		let relation = transform_rune_view(source)
			.expect("bounded layered figure")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [-1.0_f64, 1.0], "y" => [2.0_f64, 6.0]).unwrap(),
			relation.views,
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, authored)) =
			&outcome.sinks[0].output
		else {
			panic!("bounded layered plot must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		let projected = outcome.sinks[0]
			.source
			.projection()
			.expect("fixture projection");
		let kernel = create_layered_plot(specification, projected.as_ref(), 1200, 720).unwrap();
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::Cartesian { x_range, y_range, .. }
				if x_range == (-2.0, 2.0) && y_range == (0.0, 8.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0038_tick_step_reaches_the_existing_renderer_with_and_without_bounds() {
		let bounded_source = "workspace.table(\"t\").views([figure([y_bounds(0, 7), y_tick_step(1), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(bounded_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B", "C"]), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("bounded tick step must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(
			settings.y_tick_placement,
			crate::plot::common::TickPlacement::step(1.0).unwrap()
		);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::CategoricalX { y_range, .. } if y_range == (0.0, 7.0)
		));
		assert_eq!(
			settings.y_tick_placement.positions((0.0, 7.0)).unwrap(),
			(0..=7).map(f64::from).collect::<Vec<_>>()
		);
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		let unbounded_source =
			"workspace.table(\"t\").views([figure([y_tick_step(1), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(unbounded_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [1.0_f64, 6.0], "y" => [1.4_f64, 8.6]).unwrap(),
			relation.views,
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(_, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("data-derived tick step must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(
			settings.y_tick_placement.positions((1.4, 8.6)).unwrap(),
			(2..=8).map(f64::from).collect::<Vec<_>>()
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0038_tick_step_refuses_discrete_and_dense_requests_at_their_spans() {
		use polars::prelude::{IntoColumn, NamedFrom};

		let discrete_source =
			"workspace.table(\"t\").views([figure([x_tick_step(1), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(discrete_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical tick step must refuse");
		};
		assert!(
			error.contains("x_tick_step(...)") && error.contains("Band"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&discrete_source[span.start as usize..span.end as usize],
			"x_tick_step(1)"
		);

		let dense_source =
			"workspace.table(\"t\").views([figure([y_tick_step(1e-9), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(dense_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("dense tick step must refuse");
		};
		assert!(
			error.contains("0.000000001") && error.contains("capacity"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&dense_source[span.start as usize..span.end as usize],
			"y_tick_step(1e-9)"
		);

		let temporal_source =
			"workspace.table(\"t\").views([figure([x_tick_step(1), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(temporal_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let temporal_x = polars::prelude::Series::new("x".into(), &[0_i32, 1_i32])
			.cast(&polars::prelude::DataType::Date)
			.unwrap();
		let frame = polars::prelude::DataFrame::new(
			2,
			vec![
				temporal_x.into_column(),
				polars::prelude::Series::new("y".into(), &[0.0_f64, 1.0]).into_column(),
			],
		)
		.unwrap();
		let outcome = rune_view_outcome(frame, relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("temporal tick step must refuse");
		};
		assert!(
			error.contains("x_tick_step(...)") && error.contains("Time"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&temporal_source[span.start as usize..span.end as usize],
			"x_tick_step(1)"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0039_minor_ticks_toggle_and_combined_capacity_are_attributed() {
		use polars::prelude::{IntoColumn, NamedFrom};

		let off_source =
			"workspace.table(\"t\").views([figure([y_minor_ticks(0), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(off_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(_, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("zero minor count must render");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(settings.y_minor_ticks, 0);
		assert_eq!(settings.x_minor_ticks, 4);

		let discrete_source =
			"workspace.table(\"t\").views([figure([x_minor_ticks(1), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(discrete_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical minor ticks must refuse");
		};
		assert!(error.contains("x_minor_ticks(...)") && error.contains("Band"));
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&discrete_source[span.start as usize..span.end as usize],
			"x_minor_ticks(1)"
		);

		let temporal_source =
			"workspace.table(\"t\").views([figure([x_minor_ticks(1), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(temporal_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let temporal_x = polars::prelude::Series::new("x".into(), &[0_i32, 1_i32])
			.cast(&polars::prelude::DataType::Date)
			.unwrap();
		let frame = polars::prelude::DataFrame::new(
			2,
			vec![
				temporal_x.into_column(),
				polars::prelude::Series::new("y".into(), &[0.0_f64, 1.0]).into_column(),
			],
		)
		.unwrap();
		let outcome = rune_view_outcome(frame, relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("temporal minor ticks must refuse");
		};
		assert!(error.contains("x_minor_ticks(...)") && error.contains("Time"));

		let major_alone = "workspace.table(\"t\").views([figure([y_bounds(0, 7), y_tick_step(0.001), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(major_alone)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		assert!(outcome.sinks[0].output.is_ok());

		let minor_alone = "workspace.table(\"t\").views([figure([y_bounds(0, 7), y_minor_ticks(9), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(minor_alone)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		assert!(outcome.sinks[0].output.is_ok());

		let combined = "workspace.table(\"t\").views([figure([y_bounds(0, 7), y_tick_step(0.001), y_minor_ticks(9), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(combined)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("combined minor lattice must refuse");
		};
		assert!(
			error.contains("y_minor_ticks(9)")
				&& error.contains("Step")
				&& error.contains("70001")
				&& error.contains("capacity"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&combined[span.start as usize..span.end as usize],
			"y_minor_ticks(9)"
		);

		let huge = "workspace.table(\"t\").views([figure([y_minor_ticks(4294967295), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(huge)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("huge minor count must refuse before allocation");
		};
		assert!(
			error.contains("4294967295") && error.contains("capacity"),
			"{error}"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0042_facet_reaches_the_head_as_structural_intent() {
		let source = "workspace.table(\"t\").views([figure([facet(\"cat\"), title(\"Revenue\"), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B", "A"]), relation.views);
		assert_eq!(outcome.sinks[0].facet.as_deref(), Some("cat"));
		let crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _) =
			outcome.sinks[0].output.clone().expect("facet resolves")
		else {
			panic!("facet bar should use the regular resolved plot path");
		};
		let stamped = crate::plot::spec::StampedResult {
			data: Arc::clone(&outcome.result),
			provenance: resolved.resolved_spec.provenance.clone(),
		};
		let grid = PlotGrid::value_facet_derived(
			Arc::new(resolved.resolved_spec.clone()),
			&stamped,
			"cat",
		)
		.expect("facet grid");
		assert_eq!(grid.cells.len(), 2);
		assert_eq!(
			grid.arrangement,
			crate::plot::grid::GridArrangement {
				rows: 1,
				columns: 2
			}
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0080_product_0033_ordered_skin_installs_chrome_and_mark_style_atomically() {
		fn completed(
			skin: Option<&str>,
			duotone: bool,
			palette: &str,
		) -> (crate::plot::request::ResolvedPlot, ResolvedPlotSettings) {
			let skin = skin
				.map(|name| {
					format!(
						"skin(\"{name}\"){}, ",
						if duotone { ".duotone()" } else { "" }
					)
				})
				.unwrap_or_default();
			let source = format!(
				"workspace.table(\"t\").views([figure([{skin}palette(\"{palette}\"), bar(\"cat\", \"val\")])])"
			);
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome =
				rune_view_outcome(categorical_bar_frame(&["A", "B", "C"]), relation.views);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
				&outcome.sinks[0].output
			else {
				panic!("expected completed plot")
			};
			let settings = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			}
			.resolve();
			(resolved.clone(), settings)
		}

		let (reversed, reversed_settings) = completed(Some("viridis_r"), false, "viridis_r");
		let (shifted, shifted_settings) = completed(Some("viridis_s5"), false, "viridis_s5");
		let reversed_bands = band_mapping(&reversed);
		let shifted_bands = band_mapping(&shifted);
		assert_eq!(
			reversed_bands
				.iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			shifted_bands
				.iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>()
		);
		assert_ne!(
			reversed_bands, shifted_bands,
			"ordered data swatches recolor"
		);
		assert_ne!(
			reversed_settings.chrome, shifted_settings.chrome,
			"ordinary skin preserves resolved ordering"
		);
		let (_, reversed_duotone) = completed(Some("viridis_r"), true, "viridis_r");
		let (_, shifted_duotone) = completed(Some("viridis_s5"), true, "viridis_s5");
		assert_eq!(
			reversed_duotone.chrome, shifted_duotone.chrome,
			"explicit Duotone intentionally consumes the unchanged set"
		);
		assert_ne!(
			reversed_settings.mark_style, shifted_settings.mark_style,
			"authored skins now resolve marks with furniture"
		);
		assert_eq!(
			reversed_duotone.mark_style, shifted_duotone.mark_style,
			"Duotone projects one atomic skin value"
		);

		let (_, nord_tab20c) = completed(Some("nord"), false, "tab20c");
		let (yellows, nord_yellows) = completed(Some("nord"), false, "yellows");
		assert_eq!(nord_tab20c.chrome, nord_yellows.chrome);
		assert_eq!(
			band_mapping(&yellows)
				.into_iter()
				.map(|(_, _, color)| color)
				.collect::<Vec<_>>(),
			[(252, 252, 204), (247, 210, 48), (153, 102, 0)],
			"awkward data colors are accepted without a chrome/data contrast gate"
		);

		let (_, omitted) = completed(None, false, "tab20c");
		assert_eq!(omitted.chrome, crate::plot::chrome::PlotChrome::DEFAULT);

		let (_, below_threshold) = completed(Some("tab20c_s9"), false, "tab20c");
		assert_eq!(
			below_threshold.chrome.weakest_contrast().role(),
			"title text"
		);
		assert!(!below_threshold.chrome.passes_contrast());
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0032_skin_failures_are_destination_local() {
		let source = r#"workspace.table("t").views([
			view("bad", figure([skin("not-a-theme"), line("x", "y")])),
			view("good", figure([skin("nord"), line("x", "y")]))
		])"#;
		let relation = transform_rune_view(source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("invalid skin must fail its destination")
		};
		assert!(error.contains("unknown plot skin"));
		let span = &outcome.sinks[0].span;
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"skin(\"not-a-theme\")"
		);
		assert!(outcome.sinks[1].output.is_ok());
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0032_skin_lifecycle_reuses_and_atomically_stales_one_host_panel() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1032);
		let identity = LocalViewIdentity::Named("plot".to_owned());
		let source = |skin: Option<&str>| {
			let skin = skin
				.map(|name| format!("skin(\"{name}\"), "))
				.unwrap_or_default();
			format!(
				"workspace.table(\"t\").views([view(\"plot\", figure([{skin}line(\"x\", \"y\")]))])"
			)
		};
		let frame = || polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap();

		run_one_relation_program(&mut state, source_buffer, 1, &source(Some("nord")), frame());
		let panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.unwrap();
		let nord_svg = state
			.code_editor
			.rune_view_panel_plot_svg(source_buffer, &identity)
			.unwrap();

		let failed = run_one_relation_program(
			&mut state,
			source_buffer,
			2,
			&source(Some("not-a-theme")),
			frame(),
		);
		assert!(failed.is_empty());
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, &identity)
				.unwrap(),
			nord_svg,
			"failed projection retains content and chrome as one stale pair"
		);

		run_one_relation_program(
			&mut state,
			source_buffer,
			3,
			&source(Some("viridis")),
			frame(),
		);
		let viridis_svg = state
			.code_editor
			.rune_view_panel_plot_svg(source_buffer, &identity)
			.unwrap();
		assert_ne!(viridis_svg, nord_svg);

		run_one_relation_program(&mut state, source_buffer, 4, &source(None), frame());
		let default_svg = state
			.code_editor
			.rune_view_panel_plot_svg(source_buffer, &identity)
			.unwrap();
		assert_ne!(default_svg, viridis_svg);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity),
			Some(panel)
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0030_live_identity_commits_with_content_and_retargets_by_length() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1031);
		let source = |palette: &str| {
			format!(
				"workspace.table(\"t\").views([view(\"bars\", figure([palette(\"{palette}\"), bar(\"cat\", \"val\")]))])"
			)
		};

		for (revision, categories, expected) in [
			(1, vec!["A"], vec![("A", 0)]),
			(2, vec!["A", "B"], vec![("A", 0), ("B", 19)]),
			(3, vec!["A", "B", "C"], vec![("A", 0), ("B", 19), ("C", 9)]),
		] {
			let plots = run_one_relation_program(
				&mut state,
				source_buffer,
				revision,
				&source("tab20c"),
				categorical_bar_frame(&categories),
			);
			assert_eq!(
				band_mapping(&plots[0].1)
					.into_iter()
					.map(|(category, position, _)| (category, position))
					.collect::<Vec<_>>(),
				expected
					.into_iter()
					.map(|(category, position)| (category.to_owned(), position))
					.collect::<Vec<_>>()
			);
		}

		let identity = LocalViewIdentity::Named("bars".to_owned());
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 20))
				.expect("twenty-position bank")
				.len(),
			3
		);
		let filtered = run_one_relation_program(
			&mut state,
			source_buffer,
			4,
			&source("tab20c"),
			categorical_bar_frame(&["A", "C"]),
		);
		assert_eq!(
			band_mapping(&filtered[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[("A".to_owned(), 0), ("C".to_owned(), 9)]
		);
		let returned = run_one_relation_program(
			&mut state,
			source_buffer,
			5,
			&source("tab20c"),
			categorical_bar_frame(&["B"]),
		);
		assert_eq!(band_mapping(&returned[0].1)[0].1, 19);

		for (revision, palette, expected) in [
			(
				6,
				"tab20c_r",
				[
					(0, (150, 150, 150)),
					(19, (49, 130, 189)),
					(9, (161, 217, 155)),
				],
			),
			(
				7,
				"tab20c_s1",
				[(0, (150, 150, 150)), (19, (63, 0, 125)), (9, (49, 163, 84))],
			),
			(
				8,
				"viridis",
				[(0, (68, 1, 83)), (10, (39, 151, 136)), (19, (253, 231, 36))],
			),
		] {
			let plots = run_one_relation_program(
				&mut state,
				source_buffer,
				revision,
				&source(palette),
				categorical_bar_frame(&["A", "B", "C"]),
			);
			assert_eq!(
				band_mapping(&plots[0].1),
				[
					("A".to_owned(), expected[0].0, expected[0].1),
					("B".to_owned(), expected[1].0, expected[1].1),
					("C".to_owned(), expected[2].0, expected[2].1),
				]
			);
		}

		let dark2 = run_one_relation_program(
			&mut state,
			source_buffer,
			9,
			&source("dark2"),
			categorical_bar_frame(&["A", "B", "C"]),
		);
		assert_eq!(
			band_mapping(&dark2[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[
				("A".to_owned(), 0),
				("B".to_owned(), 4),
				("C".to_owned(), 7)
			]
		);
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 8))
				.unwrap()
				.len(),
			3
		);

		let resumed = run_one_relation_program(
			&mut state,
			source_buffer,
			10,
			&source("tab20c"),
			categorical_bar_frame(&["A", "B", "C"]),
		);
		assert_eq!(
			band_mapping(&resumed[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[
				("A".to_owned(), 0),
				("B".to_owned(), 10),
				("C".to_owned(), 19)
			]
		);

		let failed = run_one_relation_program(
			&mut state,
			source_buffer,
			11,
			&source("dark2"),
			categorical_bar_frame(&["A", "B", "C", "D", "E", "F", "G", "H", "I"]),
		);
		assert!(failed.is_empty(), "capacity failure cannot install content");
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 8))
				.unwrap()
				.len(),
			3,
			"failed resolution cannot advance continuation"
		);

		let stale_source = source("tab20c");
		let relation = transform_rune_view(&stale_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let presentation = relation.views[0].intent.presentation();
		let stale_identity = relation.views[0].identity.clone();
		let palettes = super::prepare_rune_categorical_palettes(&state, source_buffer, &relation);
		let outcome = super::rune_view_outcome_with_palettes(
			crate::result_artifact::ResultArtifact::test_frame(categorical_bar_frame(&[
				"A", "B", "C", "D",
			])),
			relation.views,
			palettes,
		)
		.expect("fixture projects");
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			stale_resolved,
			stale_settings,
			stale_continuation,
		)) = outcome.sinks[0].output.clone()
		else {
			panic!("stale proposal")
		};
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &stale_identity)
			.unwrap();
		state.rune_buffer_revisions.insert(source_buffer, 13);
		let kernel = create_plot(
			&stale_resolved.resolved_spec,
			&stale_resolved.data,
			1200,
			720,
		);
		drop(update(
			&mut state,
			Message::RuneRelationPlotReady {
				source_buffer,
				revision: 12,
				panel_id,
				identity: stale_identity,
				presentation,
				authored_settings: stale_settings,
				continuation: stale_continuation,
				resolved: stale_resolved,
				kernel,
			},
		));
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 20))
				.unwrap()
				.len(),
			3,
			"superseded content cannot reserve a position"
		);

		let after_stale = run_one_relation_program(
			&mut state,
			source_buffer,
			13,
			&source("tab20c"),
			categorical_bar_frame(&["A", "B", "C", "E"]),
		);
		assert_eq!(
			band_mapping(&after_stale[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[
				("A".to_owned(), 0),
				("B".to_owned(), 10),
				("C".to_owned(), 19),
				("E".to_owned(), 5),
			]
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0030_origins_and_views_keep_independent_banks_across_completion_order() {
		let source = r#"
			workspace.table("left").views([view("left-bars", figure([
				palette("tab20c"), bar("cat", "val")
			]))])
			workspace.table("right").views([view("right-bars", figure([
				palette("tab20c"), bar("cat", "val")
			]))])
		"#;
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1032);

		let mut run = |revision: u64, reverse: bool, left: &[&str], right: &[&str]| {
			let program = transform_rune_view(source).expect("two-origin categorical program");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			let mut relations = program.into_relations().collect::<Vec<_>>();
			if reverse {
				relations.reverse();
			}
			let mut mappings = HashMap::new();
			for relation in relations {
				let is_left = relation.origin.contains("table(\"left\")");
				let frame = categorical_bar_frame(if is_left { left } else { right });
				for (identity, resolved) in
					complete_relation_in_head(&mut state, source_buffer, revision, relation, frame)
				{
					mappings.insert(identity, band_mapping(&resolved));
				}
			}
			mappings
		};

		let first = run(1, true, &["L"], &["R"]);
		assert_eq!(
			first[&LocalViewIdentity::Named("left-bars".to_owned())][0].1,
			0
		);
		assert_eq!(
			first[&LocalViewIdentity::Named("right-bars".to_owned())][0].1,
			0
		);
		let second = run(2, false, &["L", "M", "N"], &["R", "S"]);
		assert_eq!(
			second[&LocalViewIdentity::Named("left-bars".to_owned())]
				.iter()
				.map(|(_, position, _)| *position)
				.collect::<Vec<_>>(),
			[0, 19, 9]
		);
		assert_eq!(
			second[&LocalViewIdentity::Named("right-bars".to_owned())]
				.iter()
				.map(|(_, position, _)| *position)
				.collect::<Vec<_>>(),
			[0, 19]
		);
		drop(run);
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(
					source_buffer,
					LocalViewIdentity::Named("left-bars".to_owned()),
					20,
				))
				.unwrap()
				.len(),
			3
		);
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(
					source_buffer,
					LocalViewIdentity::Named("right-bars".to_owned()),
					20,
				))
				.unwrap()
				.len(),
			2
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn non_tabular_sql_completion_stays_out_of_result_surfaces() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);

		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Batch { statements: 3 },
				publish: None,
			},
		);

		assert_eq!(state.status_msg, "SQL · 3 statements completed");
		assert!(
			!state.code_editor.has_query_surface(),
			"a successful command batch must not manufacture a result pane"
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn tabular_sql_completion_claims_the_result_surface() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let frame = polars::df!("x" => [1_i64, 2]).expect("frame");

		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Rows {
					artifact: crate::result_artifact::ResultArtifact::test_frame(frame),
					statements: 3,
				},
				publish: None,
			},
		);

		assert_eq!(state.status_msg, "SQL · 3 statements completed");
		assert!(
			state.code_editor.has_query_surface(),
			"a DataFrame is an artifact and must own a result surface"
		);
		state.code_editor.shutdown();
	}

	#[test]
	#[cfg(feature = "toron")]
	fn product_0079_histogram_resolves_values_spans_order_and_skin_without_color_identity() {
		use crate::plot::chrome::ResolvedPlotSkin;
		use crate::plot::spec::Channel;
		use crate::plot::theme::PlotSettings;
		use polars::frame::DataFrame;
		let source = r#"workspace.table("bins").views([figure([
    skin("nord"),
    palette("tab20c"),
    histogram("bin_start", "bin_end", "count")
])])"#;
		let complete = |source: &str, frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("histogram source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"bin_start" => [0.0, 2.0],
			"bin_end" => [1.0, 3.0],
			"count" => [4_i64, 7_i64],
		)
		.unwrap();
		let outcome = complete(source, valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			resolved,
			authored,
			continuation,
		)) = &outcome.sinks[0].output
		else {
			panic!("valid histogram must resolve");
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Histogram { bin_start, bin_end, count }
				if bin_start == "bin_start" && bin_end == "bin_end" && count == "count"
		));
		assert!(resolved.resolved_spec.band_fills.is_empty());
		assert!(resolved.resolved_spec.channel(Channel::Color).is_none());
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let settings = PlotSettings {
			authored_spec: authored.clone(),
			..PlotSettings::default()
		}
		.resolve();
		assert_eq!(
			settings.mark_style.fill,
			ResolvedPlotSkin::from_theme_name(
				"nord",
				crate::plot::chrome::PlotChromeProjection::Ordered,
			)
			.unwrap()
			.mark_style
			.fill
		);
		let without_palette = source.replace("    palette(\"tab20c\"),\n", "");
		let no_palette = complete(&without_palette, valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			no_palette_resolved,
			no_palette_authored,
			_,
		)) = &no_palette.sinks[0].output
		else {
			panic!("histogram without palette must resolve");
		};
		assert!(no_palette_resolved.resolved_spec.band_fills.is_empty());
		assert_eq!(
			PlotSettings {
				authored_spec: no_palette_authored.clone(),
				..PlotSettings::default()
			}
			.resolve()
			.mark_style
			.fill,
			settings.mark_style.fill,
			"palette metadata cannot affect an unencoded histogram fill"
		);

		for (frame, expected) in [
			(
				polars::df!("bin_start" => [0.0], "bin_end" => [1.0], "count" => [1.5]).unwrap(),
				"integer dtype",
			),
			(
				polars::df!("bin_start" => [0.0], "bin_end" => [1.0], "count" => [-1_i64]).unwrap(),
				"negative at row 0: -1",
			),
			(
				polars::df!("bin_start" => ["bad"], "bin_end" => [1.0], "count" => [1_i64])
					.unwrap(),
				"`bin_start` could not resolve: Nominal is incompatible with Histogram",
			),
			(
				polars::df!("bin_start" => [0.0], "bin_end" => [1.0]).unwrap(),
				"field is absent",
			),
			(
				polars::df!(
					"bin_start" => [Some(0.0), None],
					"bin_end" => [1.0, 2.0],
					"count" => [1_i64, 1_i64],
				)
				.unwrap(),
				"bin_start `bin_start` is null at row 1",
			),
			(
				polars::df!(
					"bin_start" => [0.0, 1.0],
					"bin_end" => [Some(1.0), None],
					"count" => [1_i64, 1_i64],
				)
				.unwrap(),
				"bin_end `bin_end` is null at row 1",
			),
			(
				polars::df!(
					"bin_start" => [0.0, 1.0],
					"bin_end" => [1.0, 2.0],
					"count" => [Some(1_i64), None],
				)
				.unwrap(),
				"count `count` is null at row 1",
			),
		] {
			let outcome = complete(source, frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid histogram must refuse");
			};
			assert!(error.contains(expected), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				"histogram(\"bin_start\", \"bin_end\", \"count\")"
			);
			assert!(complete(source, valid.clone()).sinks[0].output.is_ok());
		}

		let descending = polars::df!(
			"bin_start" => [2.0, 0.0],
			"bin_end" => [3.0, 1.0],
			"count" => [7_i64, 4_i64],
		)
		.unwrap();
		let outcome = complete(source, descending);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("descending histogram must refuse");
		};
		assert!(error.contains("unordered interval") && error.contains("sort the relation"));
		let ascending = complete(source, valid);
		assert!(ascending.sinks[0].output.is_ok());
	}

	#[test]
	fn product_0082_box_resolves_aliases_values_and_failure_scope() {
		use crate::plot::common::AxisType;
		use crate::plot::theme::PlotSettings;
		use polars::frame::DataFrame;
		let source = r#"workspace.table("box_probe").views([box("group", "lo", "p25", "mid", "p75", "hi", "point")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("box source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"group" => ["none", "all"],
			"lo" => [0_i32, 10],
			"p25" => [1.0_f32, 11.0],
			"mid" => [2_i64, 12],
			"p75" => [3.0_f64, 13.0],
			"hi" => [4_u32, 14],
			"point" => [None, Some(20_i64)],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid box must resolve");
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::BoxPlot { category, whisker_low, q1, median, q3, whisker_high, outlier }
				if (category, whisker_low, q1, median, q3, whisker_high, outlier)
					== (&"group".to_owned(), &"lo".to_owned(), &"p25".to_owned(), &"mid".to_owned(), &"p75".to_owned(), &"hi".to_owned(), &"point".to_owned())
		));
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 600, 400);
		assert_eq!(kernel.x_label(), "group");
		assert_eq!(kernel.y_label(), "");
		assert!(matches!(
			kernel.layout(PlotSettings::default().resolve()),
			PlotLayout::CategoricalX {
				y_axis_type: AxisType::Linear,
				..
			}
		));

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid box rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "mid" => [2.0], "p75" => [3.0],
				"hi" => [4.0], "point" => [None::<f64>],
			)
			.unwrap(),
			"\"p25\"",
			"absent",
		);
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "p25" => ["bad"], "mid" => [2.0],
				"p75" => [3.0], "hi" => [4.0], "point" => [None::<f64>],
			)
			.unwrap(),
			"\"p25\"",
			"quantitative",
		);
		assert_span(
			polars::df!(
				"group" => [None::<&str>], "lo" => [0.0], "p25" => [1.0], "mid" => [2.0],
				"p75" => [3.0], "hi" => [4.0], "point" => [None::<f64>],
			)
			.unwrap(),
			"\"group\"",
			"non-null",
		);
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0_i64], "p25" => [1_i64], "mid" => [2_i64],
				"p75" => [3_i64], "hi" => [4_i64], "point" => [Some(9_007_199_254_740_993_i64)],
			)
			.unwrap(),
			"\"point\"",
			"9007199254740993",
		);
		let mark = "box(\"group\", \"lo\", \"p25\", \"mid\", \"p75\", \"hi\", \"point\")";
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "p25" => [3.0], "mid" => [2.0],
				"p75" => [4.0], "hi" => [5.0], "point" => [None::<f64>],
			)
			.unwrap(),
			mark,
			"does not satisfy",
		);
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "p25" => [1.0], "mid" => [2.0],
				"p75" => [3.0], "hi" => [4.0], "point" => [Some(2.5)],
			)
			.unwrap(),
			mark,
			"inside the whisker span",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	fn product_0092_heatmap_resolves_and_attributes_binding_and_cell_refusals() {
		use polars::frame::DataFrame;
		let source =
			r#"workspace.table("heat_probe").views([heatmap("region", "quarter", "margin")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("heatmap source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"region" => ["north", "north", "south"],
			"quarter" => ["q1", "q2", "q1"],
			"margin" => [-10_i64, 5_i64, 20_i64],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid heatmap must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Heatmap { x, y, value }
				if (x.as_str(), y.as_str(), value.as_str()) == ("region", "quarter", "margin")
		));
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid heatmap rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!(
				"region" => [1_i64], "quarter" => ["q1"], "margin" => [1.0],
			)
			.unwrap(),
			"\"region\"",
			"incompatible with Heatmap",
		);
		assert_span(
			polars::df!(
				"region" => ["north"], "quarter" => ["q1"], "margin" => ["bad"],
			)
			.unwrap(),
			"\"margin\"",
			"quantitative",
		);
		assert_span(
			polars::df!(
				"region" => ["north"], "quarter" => ["q1"], "margin" => [f64::NAN],
			)
			.unwrap(),
			"\"margin\"",
			"non-finite",
		);
		assert_span(
			polars::df!(
				"region" => [None::<&str>], "quarter" => ["q1"], "margin" => [1.0],
			)
			.unwrap(),
			"\"region\"",
			"x key is null",
		);
		assert_span(
			polars::df!(
				"region" => ["north"], "quarter" => [None::<&str>], "margin" => [1.0],
			)
			.unwrap(),
			"\"quarter\"",
			"y key is null",
		);
		assert_span(
			polars::df!(
				"region" => ["north", "north"], "quarter" => ["q1", "q1"],
				"margin" => [1.0, 2.0],
			)
			.unwrap(),
			"\"region\", \"quarter\"",
			"duplicated at rows 1 and 2",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	fn product_0098_funnel_resolves_identity_and_attributes_every_refusal() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("funnel_probe").views([funnel("stage", "value")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("funnel source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"stage" => ["Visited", "Trial", "Paid"],
			"value" => [100.0_f64, 60.0, 30.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid funnel must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Funnel { stage: Some(stage), value }
				if stage == "stage" && value == "value"
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 600, 400);
		assert_eq!(
			kernel
				.legend_content(&crate::plot::common::ResolvedPlotSettings::default())
				.unwrap()
				.sections()
				.count(),
			1
		);

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid funnel rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!("stage" => [None::<&str>], "value" => [1.0]).unwrap(),
			"\"stage\"",
			"null at row 1",
		);
		assert_span(
			polars::df!("stage" => ["A", "A"], "value" => [2.0, 1.0]).unwrap(),
			"\"stage\"",
			"duplicated at rows 1 and 2",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [None::<f64>]).unwrap(),
			"\"value\"",
			"null at row 1",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [f64::NAN]).unwrap(),
			"\"value\"",
			"non-finite",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [-1.0]).unwrap(),
			"\"value\"",
			"negative",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [9_007_199_254_740_993_i64]).unwrap(),
			"\"value\"",
			"cannot round-trip",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [0.0]).unwrap(),
			"\"value\"",
			"must be positive at row 1",
		);
		assert_span(
			polars::df!("stage" => ["A", "B"], "value" => [1.0, 2.0]).unwrap(),
			"\"value\"",
			"rises between rows 1 and 2",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	fn product_0100_radial_dial_resolves_identity_and_attributes_every_refusal() {
		use polars::frame::DataFrame;
		let grouped = r#"workspace.table("radial_dial_probe").views([radial_dial("value", "maximum", "label")])"#;
		let ungrouped =
			r#"workspace.table("radial_dial_probe").views([radial_dial("value", "maximum")])"#;
		let complete = |source: &str, frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("radial-dial source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"label" => ["Zulu", "Alpha"],
			"value" => [4.0_f64, 2.0],
			"maximum" => [8.0_f64, 10.0],
		)
		.unwrap();
		let outcome = complete(grouped, valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid radial dial must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::RadialDial {
				label: Some(label), value, maximum
			} if (label.as_str(), value.as_str(), maximum.as_str()) == ("label", "value", "maximum")
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());

		let assert_span = |source: &str, frame: DataFrame, expected: &str, fragment: &str| {
			let outcome = complete(source, frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid radial dial rendered")
			};
			assert!(error.contains(fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		for (frame, expected, fragment) in [
			(
				polars::df!("value" => [1.0], "maximum" => [2.0]).unwrap(),
				"\"label\"",
				"absent",
			),
			(
				polars::df!("label" => [1_i64], "value" => [1.0], "maximum" => [2.0]).unwrap(),
				"\"label\"",
				"incompatible",
			),
			(
				polars::df!("label" => ["A"], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"absent",
			),
			(
				polars::df!("label" => ["A"], "value" => ["bad"], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"incompatible with RadialDial",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0]).unwrap(),
				"\"maximum\"",
				"absent",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => ["bad"]).unwrap(),
				"\"maximum\"",
				"Quantitative",
			),
			(
				polars::df!("label" => [None::<&str>], "value" => [1.0], "maximum" => [2.0])
					.unwrap(),
				"\"label\"",
				"null at row 1",
			),
			(
				polars::df!("label" => ["A"], "value" => [None::<f64>], "maximum" => [2.0])
					.unwrap(),
				"\"value\"",
				"null at row 1",
			),
			(
				polars::df!("label" => ["A"], "value" => [f64::NAN], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"non-finite",
			),
			(
				polars::df!("label" => ["A"], "value" => [-1.0], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"negative",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => [None::<f64>])
					.unwrap(),
				"\"maximum\"",
				"null at row 1",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => [f64::INFINITY])
					.unwrap(),
				"\"maximum\"",
				"non-finite",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => [0.0]).unwrap(),
				"\"maximum\"",
				"not positive",
			),
			(
				polars::df!("label" => ["A"], "value" => [3.0], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"exceeds its maximum",
			),
			(
				polars::df!("label" => ["A", "A"], "value" => [1.0, 1.0], "maximum" => [2.0, 2.0])
					.unwrap(),
				"\"label\"",
				"duplicated at rows 1 and 2",
			),
		] {
			assert_span(grouped, frame, expected, fragment);
		}
		assert_span(
			grouped,
			polars::df!(
				"label" => ["A"],
				"value" => [9_007_199_254_740_993_i64],
				"maximum" => [9_007_199_254_740_994_i64],
			)
			.unwrap(),
			"\"value\"",
			"exactly as f64",
		);
		assert_span(
			grouped,
			polars::df!(
				"label" => ["A"],
				"value" => [1_i64],
				"maximum" => [9_007_199_254_740_993_i64],
			)
			.unwrap(),
			"\"maximum\"",
			"exactly as f64",
		);
		assert_span(
			ungrouped,
			polars::df!("value" => [1.0, 1.0], "maximum" => [2.0, 2.0]).unwrap(),
			"radial_dial(\"value\", \"maximum\")",
			"more than one reading at row 2",
		);
		assert!(complete(grouped, valid).sinks[0].output.is_ok());
	}

	#[test]
	fn product_0103_pie_resolves_identity_and_attributes_slice_refusals() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("pie_probe").views([pie("category", "value")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("pie source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"category" => ["Zulu", "Alpha"],
			"value" => [4.0_f64, 2.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid pie must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Pie { category: Some(category), value }
				if category == "category" && value == "value"
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());

		let assert_span = |frame: DataFrame, expected: &str, fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid pie rendered")
			};
			assert!(error.contains(fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};
		assert_span(
			polars::df!("category" => [1_i64], "value" => [1.0]).unwrap(),
			"\"category\"",
			"incompatible with Pie",
		);
		assert_span(
			polars::df!("category" => ["A", "A"], "value" => [2.0, 1.0]).unwrap(),
			"\"category\"",
			".aggregate(",
		);
		assert_span(
			polars::df!("category" => ["A"], "value" => [f64::NAN]).unwrap(),
			"\"value\"",
			"non-finite",
		);
		assert_span(
			polars::df!("category" => ["A"], "value" => [0.0]).unwrap(),
			"\"value\"",
			"zero total",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	fn product_0105_multivariate_resolution_keeps_dimension_and_modifier_attribution() {
		use polars::frame::DataFrame;
		let source =
			r#"workspace.table("radar_probe").views([radar("a", "b", "c").category("group")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("radar source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"group" => ["B", "A"],
			"a" => [1.0_f64, 2.0],
			"b" => [2.0_f64, 3.0],
			"c" => [3.0_f64, 4.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid radar must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Radar { dimensions, category }
				if dimensions == &["a", "b", "c"] && category.as_deref() == Some("group")
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());

		for (frame, expected, fragment) in [
			(
				polars::df!(
					"group" => ["A"], "a" => [1.0], "b" => [2.0], "c" => [None::<f64>]
				)
				.unwrap(),
				"\"c\"",
				"field `c` is null at row 1",
			),
			(
				polars::df!(
					"group" => [None::<&str>], "a" => [1.0], "b" => [2.0], "c" => [3.0]
				)
				.unwrap(),
				".category(\"group\")",
				"category field `group` is null at row 1",
			),
		] {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid radar rendered")
			};
			assert!(error.contains(fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		}
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	fn product_0095_bubble_resolves_and_attributes_all_five_roles() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("bubble_probe").views([bubble("x", "y", "size", "color", "label")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("bubble source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"x" => [1.0, 2.0, 3.0],
			"y" => [2_i64, 3, 4],
			"size" => [10.0, 100.0, 50.0],
			"color" => [-30.0, 40.0, 0.0],
			"label" => [Some("first"), None, Some("third")],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid bubble must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Bubble { x, y, size, color, label }
				if (x.as_str(), y.as_str(), size.as_str(), color.as_str(), label.as_deref())
					== ("x", "y", "size", "color", Some("label"))
		));
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let kernel =
			crate::plot::factory::create_plot(&resolved.resolved_spec, &resolved.data, 800, 480);
		let settings = crate::plot::common::ResolvedPlotSettings::default();
		let legend = kernel
			.legend_content(&settings)
			.expect("authored bubble has its combined legend");
		let sections = legend.sections().collect::<Vec<_>>();
		assert!(matches!(
			sections.as_slice(),
			[
				crate::plot::legend::LegendSection::Gradient { stops, .. },
				crate::plot::legend::LegendSection::SizeSample { low: 10.0, high: 100.0, .. }
			] if stops.first_value() == -30.0 && stops.last_value() == 40.0
		));
		assert!(crate::plot::legend::measure_legend(&legend, &settings).width > 250.0);

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid bubble rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!(
				"x" => ["bad"], "y" => [2.0], "size" => [10.0], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"x\"",
			"incompatible with Bubble",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => ["bad"], "size" => [10.0], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"y\"",
			"incompatible with Bubble",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => ["bad"], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"size\"",
			"plot size field",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => ["bad"],
				"label" => ["first"],
			)
			.unwrap(),
			"\"color\"",
			"plot color field",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => [1.0],
				"label" => [7_i64],
			)
			.unwrap(),
			"\"label\"",
			"plot label field",
		);

		assert_span(
			polars::df!(
				"x" => [Some(1.0), None], "y" => [2.0, 3.0], "size" => [10.0, 20.0],
				"color" => [1.0, 2.0], "label" => [Some("first"), None],
			)
			.unwrap(),
			"\"x\"",
			"null at row 2",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [f64::NAN], "size" => [10.0], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"y\"",
			"non-finite at row 1",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [None::<f64>], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"size\"",
			"null at row 1",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => [f64::INFINITY],
				"label" => ["first"],
			)
			.unwrap(),
			"\"color\"",
			"non-finite at row 1",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => [1.0],
			)
			.unwrap(),
			"\"label\"",
			"absent",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	fn product_0091_rune_continuous_intent_reaches_planning_and_modifier_diagnostics() {
		use crate::plot::colors::ColorTheme;
		use crate::plot::continuous_color::{ContinuousAnchor, ContinuousScaleIntent};
		use crate::plot::spec::ColorJob;

		let frame = polars::df!(
			"x" => ["a", "b"], "y" => ["q", "q"], "value" => [-50.0, 100.0],
		)
		.unwrap();
		let complete = |source: &str| {
			let relation = transform_rune_view(source)
				.expect("continuous colour source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame.clone(), relation.views)
		};
		let valid = r#"workspace.table("heat_probe").views([figure([
			diverging_palette("green_purples"),
			heatmap("x", "y", "value").diverging()
		])])"#;
		let outcome = complete(valid);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			resolved,
			authored_settings,
			continuation,
		)) = &outcome.sinks[0].output
		else {
			panic!("diverging heatmap must resolve")
		};
		assert_eq!(
			resolved.resolved_spec.continuous_intent,
			Some(ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor: ContinuousAnchor::DefaultZero,
			})
		);
		assert_eq!(
			authored_settings.diverging_ramp,
			crate::plot::spec::Override::Set(ColorTheme::GreenPurples)
		);
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored_settings.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 800, 480);
		let legend = kernel.legend_content(&settings).expect("diverging guide");
		let crate::plot::legend::LegendSection::Gradient { stops, .. } =
			legend.sections().next().expect("gradient section")
		else {
			panic!("heatmap guide is not a gradient")
		};
		assert_eq!(stops.values().collect::<Vec<_>>(), [-50.0, 0.0, 100.0]);
		assert_eq!(stops.sample_normalized(0.5), iced::Color::WHITE);

		for (modifier, fragment) in [
			("diverging_at(500.0)", "strictly inside"),
			("diverging_at(-50.0)", "strictly inside"),
		] {
			let source = format!(
				r#"workspace.table("heat_probe").views([heatmap("x", "y", "value").{modifier}])"#
			);
			let outcome = complete(&source);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("out-of-domain anchor rendered")
			};
			assert!(error.contains(fragment), "{error}");
			assert!(!error.contains("plot anchor field"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!(".{modifier}")
			);
		}

		let constant = polars::df!("x" => ["a"], "y" => ["q"], "value" => [0.0]).unwrap();
		let complete_constant = |modifier: &str| {
			let source = format!(
				r#"workspace.table("heat_probe").views([heatmap("x", "y", "value").{modifier}])"#
			);
			let relation = transform_rune_view(&source)
				.expect("constant source compiles")
				.into_relations()
				.next()
				.unwrap();
			(source, rune_view_outcome(constant.clone(), relation.views))
		};
		assert!(complete_constant("diverging()").1.sinks[0].output.is_ok());
		let (source, outcome) = complete_constant("diverging_at(0.0)");
		let Err(error) = &outcome.sinks[0].output else {
			panic!("authored anchor on a degenerate domain rendered")
		};
		assert!(error.contains("degenerate domain"), "{error}");
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			".diverging_at(0.0)"
		);
	}

	#[test]
	fn sql_error_keeps_full_detail_without_manufacturing_a_result_pane() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let raw = "SQLite parse error: sql parser error: Expected: an SQL statement, found: viz at Line: 1, Column: 1";

		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Err(raw.to_owned()),
				publish: None,
			},
		);

		assert_eq!(
			state.status_error,
			"SQL · line 1, col 1 · expected an SQL statement, found 'viz'"
		);
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.full.as_str()),
			Some(raw)
		);
		assert!(!state.code_editor.has_query_surface());
		state.code_editor.shutdown();
	}

	#[test]
	fn diagnostic_popover_content_is_selectable_but_read_only() {
		use iced::widget::text_editor::{Action, Edit};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let raw = "database refused the statement";
		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Err(raw.to_owned()),
				publish: None,
			},
		);
		let _ = update(&mut state, Message::ToggleStatusDetail);
		assert!(state.status_detail_open);

		let _ = update(&mut state, Message::StatusDetailAction(Action::SelectAll));
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.and_then(|detail| detail.content.selection()),
			Some(raw.to_owned())
		);
		let _ = update(
			&mut state,
			Message::StatusDetailAction(Action::Edit(Edit::Insert('x'))),
		);
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.content.text()),
			Some(raw.to_owned())
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn editor_activity_retires_transient_execution_feedback() {
		use crate::editor::EditorMsg;
		use libaryon::engine::{InputEvent, KeyCode, Modifiers};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		set_sql_error(&mut state, "syntax error".to_owned(), None);

		clear_transient_execution_status_on_editor_activity(
			&mut state,
			&EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char('j'),
				modifiers: Modifiers::NONE,
			}),
		);

		assert!(state.status_error.is_empty());
		assert!(state.status_detail.is_none());

		set_rune_error(&mut state, "Rune failed".to_owned());
		clear_transient_execution_status_on_editor_activity(
			&mut state,
			&EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char('k'),
				modifiers: Modifiers::NONE,
			}),
		);

		assert!(state.status_error.is_empty());
		assert!(state.status_detail.is_none());
		state.code_editor.shutdown();
	}
}
