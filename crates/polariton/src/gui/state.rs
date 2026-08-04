#[cfg(feature = "toron")]
use crate::adapters::common::DatabaseAdapter;
use crate::adapters::{
	common::{AdapterStage, ExecutionResult},
	driver::AdapterState,
};
use crate::configuration::{self, StartupData};
use crate::editor::{CodeEditor, MarkdownCellExecution};
use crate::fonts::DEJAVU_SANS_MONO;
use crate::gui::{
	components,
	dashboard::{DashboardEffect, DashboardMsg, DashboardState},
	menu::MenuState,
	messages::{ExportFormat, Message},
	pane_grid::{self, Axis, PaneId, State as PaneState},
};
use crate::plot::composition::PlotComposition;
use crate::plot::export::{AvifBackend, PngBackend, SvgBackend};
#[cfg(feature = "toron")]
use crate::plot::factory::create_layered_plot;
use crate::plot::factory::create_plot;
use crate::plot::request::{PlotRequest, PlotSource};
use crate::plot::request::{ResolvedPlot, plan};
use crate::plot::spec::AuthoredPlotSpec;
use crate::result_registry::{ResultRegistry, parse_result_annotation};
use crate::rune_host::{self, RuneCapabilities};
#[cfg(feature = "toron")]
use crate::rune_relation_envelope::{
	LocalViewIdentity, PendingRuneView, RuneEnvelopeError, RuneViewIntent, RuneViewScheduler,
	SpannedRuneViewIntent, TransformedRuneRelation, TransformedRuneView, ViewPresentation,
	transform as transform_rune_view,
};
use iced::{Element, Size, Subscription, Task, application, event, keyboard, time, window};
use libaryon::core::BufferId;
use libaryon::uast::projection::MarkdownCellOutputTarget;
use polars::frame::DataFrame;
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "toron")]
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "toron")]
use tokio::sync::RwLock;

struct AppState {
	dashboard: DashboardState,
	menu_state: MenuState,
	code_editor: CodeEditor,
	data_frame: DataFrame,
	status_msg: String,
	status_error: String,
	status_df_size: Option<(usize, usize)>,
	status_time_elapsed: Option<f64>,
	status_detail: Option<StatusDetail>,
	status_detail_open: bool,
	adapter_state: AdapterState,
	code_started: Instant,
	is_maximized: bool,
	showing_settings: bool,
	show_column_types: bool,
	window_size: Size,
	result_registry: ResultRegistry,
	rune_plot_panes: HashMap<BufferId, PaneId>,
	rune_buffer_revisions: HashMap<BufferId, u64>,
	#[cfg(feature = "toron")]
	rune_view_scheduler: RuneViewScheduler,
	#[cfg(feature = "toron")]
	local_creation: crate::local_workspace::CreationGrant,
	#[cfg(feature = "toron")]
	local_workspace_owner: Option<crate::local_workspace::LocalWorkspaceOwner>,
	#[cfg(feature = "toron")]
	closing_local_workspace: bool,
	pending_runs: VecDeque<PendingRun>,
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

pub type Result = iced::Result;

fn build_plot_task(
	df: DataFrame,
	composition: PlotComposition,
	on_ready: impl FnOnce(
		std::result::Result<
			std::sync::Arc<dyn crate::plot::common::PlotKernel + Send + Sync>,
			String,
		>,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				let resolved = plan(PlotRequest {
					source: PlotSource::Materialized {
						name: "current result".to_owned(),
						data: std::sync::Arc::new(df),
					},
					authored_spec: AuthoredPlotSpec::automatic(composition),
				})?;
				Ok(create_plot(
					&resolved.resolved_spec,
					&resolved.data,
					1200,
					1200,
				))
			})
			.await
			.map_err(|error| format!("plot build task panicked: {error}"))?
		},
		on_ready,
	)
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

#[cfg(feature = "toron")]
fn build_layered_plot_task(
	specification: crate::plot::spec::LayeredPlotSpec,
	source: Arc<DataFrame>,
	on_ready: impl FnOnce(
		std::result::Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String>,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				create_layered_plot(&specification, source.as_ref(), 1200, 720)
			})
			.await
			.map_err(|error| format!("layered plot build task panicked: {error}"))
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
	let resize_tick = if state.dashboard.resize_freeze_until().is_some() {
		time::every(Duration::from_millis(75))
			.map(|_| Message::Dashboard(DashboardMsg::ResizeSettled))
	} else {
		Subscription::none()
	};
	Subscription::batch([
		events,
		resize_tick,
		state.code_editor.subscription().map(Message::CodeEditEvent),
		window::resize_events().map(|(_, size)| Message::CodeEditResize(size)),
	])
}

fn run_dashboard_message(app_state: &mut AppState, message: DashboardMsg) -> Task<Message> {
	if matches!(&message, DashboardMsg::AddPlot(_)) {
		// Plot creation is the user-facing entry point for the nested dashboard.
		// Its pixels remain Polariton's, while Baryon owns the workspace pane.
		app_state.code_editor.open_dashboard();
	}
	match app_state.dashboard.update(message) {
		DashboardEffect::None => Task::none(),
		DashboardEffect::BuildNewPlot { composition } => {
			app_state.status_msg =
				format!("Generating {} plot...", composition.primary_plot_type());
			build_plot_task(
				app_state.data_frame.clone(),
				composition.clone(),
				move |result| Message::DashboardPlotBuilt {
					pane: None,
					composition,
					result,
				},
			)
		}
		DashboardEffect::RebuildPlot { pane, composition } => {
			app_state.status_msg =
				format!("Generating {} plot...", composition.primary_plot_type());
			build_plot_task(
				app_state.data_frame.clone(),
				composition.clone(),
				move |result| Message::DashboardPlotBuilt {
					pane: Some(pane),
					composition,
					result,
				},
			)
		}
	}
}

pub fn run(startup_data: StartupData, initial_file: Option<String>) -> Result {
	let size = Size::new(startup_data.window_size.0, startup_data.window_size.1);
	let initialize = move || new(startup_data.clone(), initial_file.clone());
	application(initialize, update, view)
		.subscription(subscription)
		.theme(components::theme())
		.title("Polariton")
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
		.run()
}

fn new(startup_data: StartupData, initial_file: Option<String>) -> (AppState, Task<Message>) {
	let window_size = Size::new(startup_data.window_size.0, startup_data.window_size.1);
	let init_error = startup_data.init_error.clone();
	let init_summary = init_error
		.as_ref()
		.map(|_| "init.rn · configuration failed".to_owned())
		.unwrap_or_default();
	let init_detail = init_error.map(|error| StatusDetail::new(init_summary.clone(), error));
	let data_frame = DataFrame::default();
	let init_script = configuration::default_init_path();
	let workspace_size = workspace_bounds(window_size).size();
	let mut code_editor = match initial_file {
		Some(path) => CodeEditor::open_initial_file_with_init(path, init_script, workspace_size),
		None => CodeEditor::new_with_init("", init_script, workspace_size),
	};
	code_editor.set_show_column_types(startup_data.show_column_types);
	let mut state = AppState {
		dashboard: DashboardState::default(),
		menu_state: MenuState::default(),
		code_editor,
		data_frame,
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
		rune_plot_panes: HashMap::new(),
		rune_buffer_revisions: HashMap::new(),
		#[cfg(feature = "toron")]
		rune_view_scheduler: RuneViewScheduler::default(),
		#[cfg(feature = "toron")]
		local_creation: crate::local_workspace::CreationGrant::default(),
		#[cfg(feature = "toron")]
		local_workspace_owner: None,
		#[cfg(feature = "toron")]
		closing_local_workspace: false,
		pending_runs: VecDeque::new(),
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
		(state, Task::batch([scratch, discovery]))
	}
	#[cfg(not(feature = "toron"))]
	{
		(state, scratch)
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
		&app_state.dashboard,
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
	)
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
	#[cfg(feature = "toron")]
	match transform_rune_view(&code) {
		Ok(view) => return run_rune_relation_view(app_state, source_buffer, view),
		Err(
			error @ (RuneEnvelopeError::Invalid(_)
			| RuneEnvelopeError::InvalidView { .. }
			| RuneEnvelopeError::DuplicateName { .. }),
		) => {
			let message = error.to_string();
			app_state
				.code_editor
				.mark_rune_view_producer_stale(source_buffer);
			set_rune_error(app_state, message);
			return Task::none();
		}
		Err(RuneEnvelopeError::RecognizedNoOutput) => {
			app_state.code_editor.prepare_rune_view_panels(
				source_buffer,
				&[] as &[(LocalViewIdentity, ViewPresentation)],
				0,
			);
			app_state.status_msg = "Rune view set is bound; no output root emitted.".into();
			app_state.status_error.clear();
			return Task::none();
		}
		Err(RuneEnvelopeError::NoTerminalView | RuneEnvelopeError::Parse(_)) => {}
	}
	let capabilities = RuneCapabilities {
		results: app_state.result_registry.snapshot(),
		..RuneCapabilities::default()
	};
	let revision = *app_state
		.rune_buffer_revisions
		.entry(source_buffer)
		.and_modify(|revision| *revision = revision.saturating_add(1))
		.or_insert(1);
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
fn run_rune_relation_view(
	app_state: &mut AppState,
	source_buffer: BufferId,
	view: TransformedRuneView,
) -> Task<Message> {
	let revision = *app_state
		.rune_buffer_revisions
		.entry(source_buffer)
		.and_modify(|revision| *revision = revision.saturating_add(1))
		.or_insert(1);
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
	Task::batch(view.into_relations().map(|view| {
		let connection = connection.clone();
		let origin = view.origin.clone();
		let views = view
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		Task::perform(dispatch_rune_relation(connection, view), move |result| {
			Message::RunRuneViewSetResult {
				source_buffer,
				revision,
				origin,
				views,
				result,
			}
		})
	}))
}

#[cfg(feature = "toron")]
async fn dispatch_rune_relation(
	connection: Arc<RwLock<dyn DatabaseAdapter>>,
	view: TransformedRuneRelation,
) -> std::result::Result<crate::gui::messages::RuneRelationViewSetOutcome, String> {
	let result = connection.write().await.dispatch_rune(&view.source).await;
	match result {
		Ok(frame) => Ok(rune_view_outcome(frame, view.views)),
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

#[cfg(feature = "toron")]
fn rune_view_outcome(
	frame: DataFrame,
	views: Vec<SpannedRuneViewIntent>,
) -> crate::gui::messages::RuneRelationViewSetOutcome {
	use crate::gui::messages::{
		RuneRelationSinkOutcome, RuneRelationSinkOutput, RuneRelationViewSetOutcome,
	};
	let result = std::sync::Arc::new(frame);
	let sinks = views
		.into_iter()
		.map(|view| {
			let presentation = view.intent.presentation();
			let output = match view.intent {
				RuneViewIntent::Table => Ok(RuneRelationSinkOutput::Table),
				RuneViewIntent::Plot(crate::rune_relation_envelope::RunePlotIntent::Single(
					plot,
				)) => {
					let authored_spec =
						AuthoredPlotSpec::xy_color(plot.composition, plot.x, plot.y, plot.category);
					plan(PlotRequest {
						source: PlotSource::Materialized {
							name: "workspace relation".to_owned(),
							data: std::sync::Arc::clone(&result),
						},
						authored_spec,
					})
					.map(RuneRelationSinkOutput::Plot)
				}
				RuneViewIntent::Plot(crate::rune_relation_envelope::RunePlotIntent::Layered(
					specification,
				)) => Ok(RuneRelationSinkOutput::LayeredPlot(specification)),
			};
			RuneRelationSinkOutcome {
				identity: view.identity,
				presentation,
				span: view.span,
				source: std::sync::Arc::clone(&result),
				output,
			}
		})
		.collect();
	RuneRelationViewSetOutcome { result, sinks }
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

fn update(app_state: &mut AppState, message: Message) -> Task<Message> {
	match message {
		Message::Dashboard(message) => {
			return run_dashboard_message(app_state, message);
		}
		Message::DashboardPlotBuilt {
			pane,
			composition,
			result,
		} => match result {
			Ok(kernel) => {
				app_state.status_error.clear();
				app_state.status_msg = format!("{} plot ready", composition.title());
				let _ = app_state.dashboard.update(DashboardMsg::PlotKernelReady {
					pane,
					composition,
					kernel,
				});
			}
			Err(error) => {
				app_state.status_msg.clear();
				app_state.status_error = format!("Plot specification failed: {error}");
			}
		},
		Message::ToggleStatusDetail => {
			if app_state.status_detail.is_some() {
				app_state.status_detail_open = !app_state.status_detail_open;
			}
		}
		Message::CloseStatusDetail => {
			app_state.status_detail_open = false;
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
				if let Some(owner) = app_state.local_workspace_owner.take() {
					app_state.closing_local_workspace = true;
					app_state.status_error.clear();
					app_state.status_msg =
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
			return window::latest().and_then(window::close);
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
		Message::CodeEditEvent(crate::editor::EditorMsg::Dashboard(message)) => {
			return run_dashboard_message(app_state, message);
		}
		Message::CodeEditEvent(edit_event) => {
			clear_transient_execution_status_on_editor_activity(app_state, &edit_event);
			let task = app_state
				.code_editor
				.update(edit_event)
				.map(Message::CodeEditEvent);
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
			app_state.status_msg = format!(
				"Attaching local workspace session {}…",
				ready.identity.session_nonce
			);
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
					app_state.status_msg = format!(
						"Joined local workspace session {}.",
						workspace.identity.session_nonce
					);
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
			let time_elapsed = (app_state.code_started.elapsed().as_millis() as f64) / 1000.0;
			app_state.status_time_elapsed = Some(time_elapsed);
			if let Some(claim) = publish.as_ref() {
				match &er {
					ExecutionResult::Rows { frame, .. } => {
						let _ = app_state
							.result_registry
							.publish(claim, std::sync::Arc::new(frame.clone()));
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
					frame: df,
					statements,
				} => {
					let rows = df.height();
					let cols = df.width();
					app_state.code_editor.begin_query();
					app_state.code_editor.set_query_result(df.clone());
					app_state.data_frame = df;
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
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed().as_secs_f64());
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
						app_state.status_msg =
							format!("Building plot from {}…", resolved.source_name);
						return build_resolved_plot_task(
							resolved,
							1200,
							move |resolved, kernel| Message::RuneBufferPlotReady {
								source_buffer,
								revision,
								resolved,
								kernel,
							},
						);
					}
					app_state.status_msg = "Rune finished.".to_owned();
				}
				Err(message) => {
					set_rune_error(app_state, message);
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
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed().as_secs_f64());
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
						match sink.output {
							Ok(RuneRelationSinkOutput::Table) => {
								let frame = sink.source.as_ref().clone();
								let rows = frame.height();
								let cols = frame.width();
								app_state
									.code_editor
									.set_rune_view_table(panel_id, frame.clone());
								app_state.data_frame = frame;
								app_state.status_df_size = Some((rows, cols));
							}
							Ok(RuneRelationSinkOutput::Plot(resolved)) => {
								let presentation = sink.presentation;
								plot_tasks.push(build_resolved_plot_task(
									resolved,
									1200,
									move |resolved, kernel| Message::RuneRelationPlotReady {
										source_buffer,
										revision,
										panel_id,
										presentation,
										resolved,
										kernel,
									},
								));
							}
							Ok(RuneRelationSinkOutput::LayeredPlot(specification)) => {
								let presentation = sink.presentation;
								let span = sink.span;
								plot_tasks.push(build_layered_plot_task(
									specification,
									sink.source,
									move |result| Message::RuneRelationLayeredPlotReady {
										source_buffer,
										revision,
										panel_id,
										presentation,
										span,
										result,
									},
								));
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
			resolved,
			kernel,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			app_state.code_editor.open_dashboard();
			let previous = app_state.rune_plot_panes.get(&source_buffer).copied();
			let composition = resolved.resolved_spec.composition.clone();
			let pane = app_state
				.dashboard
				.apply_plot_replacing(previous, composition, kernel);
			app_state.rune_plot_panes.insert(source_buffer, pane);
			app_state.status_error.clear();
			app_state.status_msg = format!("Plotted {}.", resolved.source_name);
		}
		#[cfg(feature = "toron")]
		Message::RuneRelationPlotReady {
			source_buffer,
			revision,
			panel_id,
			presentation,
			resolved,
			kernel,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			app_state
				.code_editor
				.set_rune_view_plot(panel_id, presentation.display_label, kernel);
			app_state.status_msg = format!("Plotted {}.", resolved.source_name);
		}
		#[cfg(feature = "toron")]
		Message::RuneRelationLayeredPlotReady {
			source_buffer,
			revision,
			panel_id,
			presentation,
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
			let time_elapsed = (app_state.code_started.elapsed().as_millis() as f64) / 1000.0;
			app_state.status_time_elapsed = Some(time_elapsed);
			match result {
				ExecutionResult::Rows {
					frame: df,
					statements,
				} => {
					let rows = df.height();
					let cols = df.width();
					// Presentation never decides data availability: a named SQL cell
					// remains a Rune input even when it renders in a pane or nowhere.
					app_state
						.code_editor
						.retain_markdown_cell_table(identity.clone(), &df);
					match presentation.output {
						MarkdownCellOutputTarget::Inline => app_state
							.code_editor
							.set_markdown_cell_table(identity, df.clone()),
						MarkdownCellOutputTarget::Pane => {
							app_state.code_editor.begin_query();
							app_state.code_editor.set_query_result(df.clone())
						}
						MarkdownCellOutputTarget::None => {}
					}
					app_state.data_frame = df;
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
			let time_elapsed = (app_state.code_started.elapsed().as_millis() as f64) / 1000.0;
			app_state.status_time_elapsed = Some(time_elapsed);
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
					app_state.code_editor.open_dashboard();
					let _ = app_state.dashboard.update(DashboardMsg::PlotKernelReady {
						pane: None,
						composition,
						kernel,
					});
				}
				MarkdownCellOutputTarget::None => {}
			}
			app_state.status_error.clear();
			app_state.status_msg = "Plot cell finished.".to_owned();
		}
		Message::Export(format) => {
			return window::latest().and_then(move |id| {
				window::size(id).map(move |size| Message::ExportWithWindowSize(format, Some(size)))
			});
		}
		Message::ExportWithWindowSize(format, window_size) => {
			if let Some(dashboard) = app_state.dashboard.panes.as_ref() {
				let size = window_size.unwrap_or(Size::new(1920.0, 1080.0));
				let workspace_bounds = workspace_bounds(size);
				let dashboard_pane_bounds = app_state
					.code_editor
					.dashboard_bounds()
					.map(|rect| iced::Rectangle {
						x: 0.0,
						y: 0.0,
						width: rect.width,
						height: rect.height,
					})
					.unwrap_or(workspace_bounds);
				let mut export_jobs: Vec<Box<dyn FnOnce() + Send + 'static>> = Vec::new();
				for (id, rect) in get_pane_rects(dashboard, dashboard_pane_bounds) {
					if let Some(plot_state) = app_state.dashboard.content.get(&id) {
						let width = rect.width;
						let height = rect.height - 30.0;
						let bounds = iced::Rectangle {
							x: 0.0,
							y: 0.0,
							width,
							height,
						};
						let settings = plot_state.plot_settings.clone();
						let widget = crate::plot::common::PlotWidget {
							kernel: plot_state.kernel.as_ref(),
							title: plot_state.composition.title(),
							padding: 20.0,
							settings: settings.clone(),
							render_revision: plot_state.render_revision,
							resize_render_suspended: plot_state.resize_render_suspended,
							layer: crate::plot::common::PlotRenderLayer::Full,
						};
						match format {
							ExportFormat::SVG => {
								let mut backend = SvgBackend::new(width, height);
								widget.render(&mut backend, bounds);
								let svg_content = backend.finish();
								let filename = format!("plot_{}.svg", id.0);
								export_jobs.push(Box::new(move || {
									let _ = std::fs::write(filename, svg_content);
								}));
							}
							ExportFormat::PNG => {
								let mut backend = PngBackend::new(width as u32, height as u32);
								widget.render(&mut backend, bounds);
								let filename = format!("plot_{}.png", id.0);
								export_jobs.push(Box::new(move || {
									backend.save(std::path::Path::new(&filename));
								}));
							}
							ExportFormat::AVIF => {
								let mut backend = AvifBackend::new(width as u32, height as u32);
								widget.render(&mut backend, bounds);
								let filename = format!("plot_{}.avif", id.0);
								export_jobs.push(Box::new(move || {
									backend.save(std::path::Path::new(&filename));
								}));
							}
						}
					}
				}
				let count = export_jobs.len();
				app_state.status_msg = format!("Exporting {count} plots as {format}...");
				return Task::perform(
					async move {
						tokio::task::spawn_blocking(move || {
							for job in export_jobs {
								job();
							}
						})
						.await
						.unwrap();
					},
					move |()| Message::ExportDone(count, format),
				);
			}
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
		_ => {
			if let Some(kind) = action.strip_prefix("plot:new:") {
				return parse_plot_type(kind)
					.map(|plot_type| Message::Dashboard(DashboardMsg::AddPlot(plot_type)));
			}
			if let Some(format) = action.strip_prefix("plot:export:") {
				return parse_export_format(format).map(Message::Export);
			}
			None
		}
	}
}

fn parse_plot_type(kind: &str) -> Option<crate::plot::core::PlotType> {
	use crate::plot::core::PlotType;

	match kind {
		"Bar" => Some(PlotType::Bar),
		"BoxPlot" => Some(PlotType::BoxPlot),
		"Bubble" => Some(PlotType::Bubble),
		"Candlestick" => Some(PlotType::Candlestick),
		"FillBetween" => Some(PlotType::FillBetween),
		"Funnel" => Some(PlotType::Funnel),
		"Heatmap" => Some(PlotType::Heatmap),
		"Hexbin" => Some(PlotType::Hexbin),
		"Histogram" => Some(PlotType::Histogram),
		"HorizontalBar" => Some(PlotType::HorizontalBar),
		"HorizontalStackedBar" => Some(PlotType::HorizontalStackedBar),
		"Line" => Some(PlotType::Line),
		"Parallel" => Some(PlotType::Parallel),
		"Pie" => Some(PlotType::Pie),
		"Radar" => Some(PlotType::Radar),
		"RadialDial" => Some(PlotType::RadialDial),
		"Scatter" => Some(PlotType::Scatter),
		"StackedArea" => Some(PlotType::StackedArea),
		"StackedBar" => Some(PlotType::StackedBar),
		"Violin" => Some(PlotType::Violin),
		_ => None,
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

fn get_pane_rects(state: &PaneState, bounds: iced::Rectangle) -> Vec<(PaneId, iced::Rectangle)> {
	let Some(root) = state.root() else {
		return vec![];
	};
	get_node_rects(state, root, bounds)
}

/// The window rectangle occupied by Baryon's workspace: the window minus the
/// application chrome and status bar. Shared by dashboard export hit-testing
/// and editor viewport sizing so the two cannot drift.
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

fn get_node_rects(
	state: &PaneState,
	node: pane_grid::state::NodeId,
	bounds: iced::Rectangle,
) -> Vec<(PaneId, iced::Rectangle)> {
	use pane_grid::state::NodeId;

	match node {
		NodeId::Pane(pane) => vec![(pane, bounds)],
		NodeId::Split(split_id) => {
			let Some(split) = state.get_split(split_id) else {
				return vec![];
			};
			let axis = split.axis;
			let ratio = split.ratio;
			let first = split.first;
			let second = split.second;
			let spacing = 7.0; // handle thickness
			let (rect_a, rect_b) = match axis {
				Axis::Horizontal => {
					let width_a = (bounds.width - spacing) * ratio;
					let width_b = bounds.width - spacing - width_a;
					(
						iced::Rectangle {
							width: width_a,
							..bounds
						},
						iced::Rectangle {
							x: bounds.x + width_a + spacing,
							width: width_b,
							..bounds
						},
					)
				}
				Axis::Vertical => {
					let height_a = (bounds.height - spacing) * ratio;
					let height_b = bounds.height - spacing - height_a;
					(
						iced::Rectangle {
							height: height_a,
							..bounds
						},
						iced::Rectangle {
							y: bounds.y + height_a + spacing,
							height: height_b,
							..bounds
						},
					)
				}
			};
			let mut rects = get_node_rects(state, first, rect_a);
			rects.extend(get_node_rects(state, second, rect_b));
			rects
		}
	}
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "toron")]
	use super::{AdapterState, view};
	use super::{
		clear_transient_execution_status_on_editor_activity, compact_rune_diagnostic,
		compact_sql_diagnostic, new, set_rune_error, set_sql_error, update,
	};
	#[cfg(feature = "toron")]
	use super::{dispatch_rune_relation, rune_view_outcome};
	use crate::adapters::common::ExecutionResult;
	#[cfg(feature = "toron")]
	use crate::adapters::common::{
		AdapterStage, DatabaseAdapter, RuneDispatchFailure, RuneDispatchFailureKind,
	};
	use crate::configuration::StartupData;
	use crate::gui::messages::Message;
	#[cfg(feature = "toron")]
	use crate::plot::core::PlotType;
	#[cfg(feature = "toron")]
	use crate::plot::factory::{create_layered_plot, create_plot};
	#[cfg(feature = "toron")]
	use crate::rune_relation_envelope::{
		LocalViewIdentity, PendingRuneView, RuneViewForm, TransformedRuneRelation,
		transform as transform_rune_view,
	};
	#[cfg(feature = "toron")]
	use nilket_local::{
		Artifact, Evidence, LocalWorkspace, ObservationContext, ReadyWorkspace, StaleCause,
		VersionedArtifact, WorkspaceIdentity,
	};
	#[cfg(feature = "toron")]
	use std::sync::{Arc, Mutex};
	#[cfg(feature = "toron")]
	use tokio::sync::RwLock;
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
			Ok(polars::df!("x" => [1_i64], "y" => [2_i64]).expect("fixture frame"))
		}
	}

	#[cfg(feature = "toron")]
	fn complete_relation_in_head(
		state: &mut super::AppState,
		source_buffer: libaryon::core::BufferId,
		revision: u64,
		relation: TransformedRuneRelation,
		frame: polars::frame::DataFrame,
	) {
		enum ReadyPlot {
			Single(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::request::ResolvedPlot,
			),
			Layered(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::spec::LayeredPlotSpec,
				Arc<polars::frame::DataFrame>,
				std::ops::Range<u32>,
			),
		}
		let origin = relation.origin.clone();
		let views = relation
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		let outcome = rune_view_outcome(frame, relation.views);
		let plots = outcome
			.sinks
			.iter()
			.filter_map(|sink| match &sink.output {
				Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved)) => Some((
					sink.identity.clone(),
					ReadyPlot::Single(sink.presentation.clone(), resolved.clone()),
				)),
				Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification)) => {
					Some((
						sink.identity.clone(),
						ReadyPlot::Layered(
							sink.presentation.clone(),
							specification.clone(),
							Arc::clone(&sink.source),
							sink.span.clone(),
						),
					))
				}
				_ => None,
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
				ReadyPlot::Single(presentation, resolved) => {
					let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
					drop(update(
						state,
						Message::RuneRelationPlotReady {
							source_buffer,
							revision,
							panel_id,
							presentation,
							resolved,
							kernel,
						},
					));
				}
				ReadyPlot::Layered(presentation, specification, source, span) => {
					let result = create_layered_plot(&specification, source.as_ref(), 1200, 720);
					drop(update(
						state,
						Message::RuneRelationLayeredPlotReady {
							source_buffer,
							revision,
							panel_id,
							presentation,
							span,
							result,
						},
					));
				}
			}
		}
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
		assert!(
			state
				.status_msg
				.contains(&ready.identity.session_nonce.to_string())
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
		assert!(matches!(
			outcome.sinks[0].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::Table)
		));
		for (sink, expected) in
			outcome.sinks[1..]
				.iter()
				.zip([PlotType::Bar, PlotType::Scatter, PlotType::Line])
		{
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(plot)) = &sink.output else {
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
		let bound = super::run_rune_code(
			&mut state,
			source_buffer,
			"let v = workspace.table(\"t\").views([table()]);".to_owned(),
		);
		assert_eq!(bound.units(), 0);
		assert_eq!(state.rune_view_scheduler.submitted, 0);
		assert!(!state.rune_buffer_revisions.contains_key(&source_buffer));
		assert_eq!(
			state.status_msg,
			"Rune view set is bound; no output root emitted."
		);

		let local =
			super::run_rune_code(&mut state, source_buffer, "let answer = 42; answer".into());
		assert_eq!(
			local.units(),
			1,
			"view-free Rune still reaches the local host"
		);
		assert_eq!(state.rune_buffer_revisions.get(&source_buffer), Some(&1));
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

		assert_eq!(task.units(), 0);
		assert_eq!(state.rune_view_scheduler.submitted, 0);
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
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification)) =
			&sink.output
		else {
			panic!("the envelope must hand a layered specification to the head")
		};
		let Err(error) = create_layered_plot(specification, sink.source.as_ref(), 1200, 720) else {
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
		assert!(state.dashboard.panes.is_none());
		assert!(state.dashboard.content.is_empty());

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
		assert!(state.dashboard.panes.is_none());
		assert!(state.dashboard.content.is_empty());
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
				Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved)) => Some(resolved),
				_ => None,
			})
			.expect("resolved scatter");
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		state
			.code_editor
			.set_rune_view_plot(stable_panel, "plot.scatter".to_owned(), kernel);
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
					frame,
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
