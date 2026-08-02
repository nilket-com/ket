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
use crate::plot::factory::create_plot;
use crate::plot::request::{ResolvedPlot, plan};
use crate::result_registry::{ResultRegistry, parse_result_annotation};
use crate::rune_host::{self, RuneCapabilities};
#[cfg(feature = "toron")]
use crate::rune_relation_envelope::{
	PendingRuneView, RuneEnvelopeError, RuneViewIntent, RuneViewScheduler, TransformedRuneView,
	transform as transform_rune_view,
};
use iced::{Element, Size, Subscription, Task, application, event, keyboard, time, window};
use libaryon::core::BufferId;
use libaryon::uast::projection::MarkdownCellOutputTarget;
use polars::frame::DataFrame;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

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
	on_ready: impl FnOnce(std::sync::Arc<dyn crate::plot::common::PlotKernel + Send + Sync>) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || create_plot(&composition, &df, 1200, 1200))
				.await
				.expect("plot build task panicked")
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
			let composition = resolved.composition.clone();
			let data = resolved.data.as_ref().clone();
			let kernel =
				tokio::task::spawn_blocking(move || create_plot(&composition, &data, 1200, height))
					.await
					.expect("Rune plot build task panicked");
			(resolved, kernel)
		},
		move |(resolved, kernel)| on_ready(resolved, kernel),
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
				move |kernel| {
					Message::Dashboard(DashboardMsg::PlotKernelReady {
						pane: None,
						composition,
						kernel,
					})
				},
			)
		}
		DashboardEffect::RebuildPlot { pane, composition } => {
			app_state.status_msg =
				format!("Generating {} plot...", composition.primary_plot_type());
			build_plot_task(
				app_state.data_frame.clone(),
				composition.clone(),
				move |kernel| {
					Message::Dashboard(DashboardMsg::PlotKernelReady {
						pane: Some(pane),
						composition,
						kernel,
					})
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
		pending_runs: VecDeque::new(),
	};
	sync_workspace_viewport(&mut state);
	// Eagerly open the session scratch database so SQL always has a target;
	// an in-memory SQLite open is effectively instant.
	let scratch = Task::perform(AdapterState::open_scratch(), Message::ScratchReady);
	(state, scratch)
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

fn clear_transient_sql_status_on_editor_activity(
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
			|| app_state.status_error.starts_with("SQL · "))
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
		"rune" => {
			if code.trim().is_empty() {
				app_state.status_msg = "Nothing to run in the current Rune buffer.".into();
				return Task::none();
			}
			#[cfg(feature = "toron")]
			match transform_rune_view(&code) {
				Ok(view) => return run_rune_relation_view(app_state, source_buffer, view),
				Err(RuneEnvelopeError::Invalid(message)) => {
					app_state.status_error = format!("Error: Rune view syntax: {message}");
					app_state
						.code_editor
						.set_query_error(format!("Rune view syntax: {message}"));
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
			app_state.code_editor.begin_query();
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
							table: None,
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
		language => {
			app_state.status_msg = format!("No executor is registered for language: {language}");
			Task::none()
		}
	}
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
	let Some(work) = app_state.rune_view_scheduler.submit(work) else {
		app_state.status_msg = "Rune workspace queued newest view…".into();
		return Task::none();
	};
	start_rune_relation_task(app_state, work)
}

#[cfg(feature = "toron")]
fn start_rune_relation_task(app_state: &mut AppState, work: PendingRuneView) -> Task<Message> {
	let Some(connection) = app_state.adapter_state.sql_connection().cloned() else {
		let message = "Local Rune workspace unavailable. Start or attach with `nilket`.".to_owned();
		app_state.status_error = format!("Error: {message}");
		app_state.code_editor.set_query_error(message);
		app_state.rune_view_scheduler.cancel(work.source_buffer);
		return Task::none();
	};
	let PendingRuneView {
		source_buffer,
		revision,
		view,
	} = work;
	app_state.status_msg = "Rune workspace running…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	app_state.code_editor.begin_query();
	Task::perform(
		async move {
			let result = connection.write().await.dispatch_rune(&view.source).await;
			match result {
				Ok(frame) => rune_view_outcome(frame, view.intent),
				Err(failure) => {
					let location = failure
						.span
						.and_then(|span| {
							view.source_map.map_span(toron_client::RuneSourceSpan {
								start: span.start,
								end: span.end,
							})
						})
						.unwrap_or_else(|| view.source_map.terminal_view());
					Err(format!(
						"Rune {:?} at bytes {}..{}: {}",
						failure.kind, location.start, location.end, failure.message
					))
				}
			}
		},
		move |result| Message::RunRuneBufferResult {
			source_buffer,
			revision,
			result,
		},
	)
}

#[cfg(feature = "toron")]
fn rune_view_outcome(
	frame: DataFrame,
	intent: RuneViewIntent,
) -> std::result::Result<crate::gui::messages::RuneBufferOutcome, String> {
	match intent {
		RuneViewIntent::Table => Ok(crate::gui::messages::RuneBufferOutcome {
			display: None,
			table: Some(frame),
			plot: None,
		}),
		RuneViewIntent::Plot(plot) => {
			let mut fields = vec![plot.x.as_str(), plot.y.as_str()];
			if let Some(category) = plot.category.as_deref() {
				fields.push(category);
			}
			let projected = frame
				.select(fields)
				.map_err(|error| format!("Rune plot fields could not be projected: {error}"))?;
			Ok(crate::gui::messages::RuneBufferOutcome {
				display: None,
				table: None,
				plot: Some(ResolvedPlot {
					source_name: "workspace relation".to_owned(),
					composition: plot.composition,
					data: std::sync::Arc::new(projected),
				}),
			})
		}
	}
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
			clear_transient_sql_status_on_editor_activity(app_state, &edit_event);
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
			app_state.adapter_state.stage = AdapterStage::Unconfigured;
			app_state.status_msg = format!("Adapter selected: {:?}", adapter_selection);
			app_state.adapter_state.selection = adapter_selection;
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
		Message::AdapterConnected(dba) => {
			let ok = dba.is_some();
			app_state.adapter_state.connection = dba;
			app_state.adapter_state.stage = if ok {
				AdapterStage::Connected
			} else {
				// A failed connect keeps the scratch database as the SQL
				// target rather than reporting a connection that isn't there.
				AdapterStage::Configured
			};
			app_state.status_msg = if ok {
				"Adapter connected.".into()
			} else {
				"Adapter connection failed.".into()
			};
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
			#[cfg(feature = "toron")]
			if app_state.rune_view_scheduler.is_in_flight(source_buffer)
				&& let Some(next) = app_state.rune_view_scheduler.complete(source_buffer)
			{
				return start_rune_relation_task(app_state, next);
			}
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed().as_secs_f64());
			match result {
				Ok(outcome) => {
					app_state.status_error.clear();
					app_state.code_editor.set_query_message(
						outcome
							.display
							.unwrap_or_else(|| "Rune finished.".to_owned()),
					);
					if let Some(frame) = outcome.table {
						let rows = frame.height();
						let cols = frame.width();
						app_state.code_editor.set_query_result(frame.clone());
						app_state.data_frame = frame;
						app_state.status_df_size = Some((rows, cols));
						app_state.status_msg = "Rune workspace query completed".to_owned();
					}
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
					app_state.status_error = format!("Error: {message}");
					app_state.status_msg.clear();
					app_state.code_editor.set_query_error(message);
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
			let pane =
				app_state
					.dashboard
					.apply_plot_replacing(previous, resolved.composition, kernel);
			app_state.rune_plot_panes.insert(source_buffer, pane);
			app_state.status_error.clear();
			app_state.status_msg = format!("Plotted {}.", resolved.source_name);
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
					app_state.status_error = format!("Error: {message}");
					app_state.status_msg.clear();
					if presentation.output == MarkdownCellOutputTarget::Inline {
						app_state
							.code_editor
							.set_markdown_cell_error(identity, message);
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
			match presentation.output {
				MarkdownCellOutputTarget::Inline => app_state.code_editor.set_markdown_cell_plot(
					identity,
					resolved.composition,
					kernel,
				),
				MarkdownCellOutputTarget::Pane => {
					app_state.code_editor.open_dashboard();
					let _ = app_state.dashboard.update(DashboardMsg::PlotKernelReady {
						pane: None,
						composition: resolved.composition,
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
	use super::rune_view_outcome;
	use super::{
		clear_transient_sql_status_on_editor_activity, compact_sql_diagnostic, new, set_sql_error,
		update,
	};
	use crate::adapters::common::ExecutionResult;
	use crate::configuration::StartupData;
	use crate::gui::messages::Message;
	#[cfg(feature = "toron")]
	use crate::plot::core::PlotType;
	#[cfg(feature = "toron")]
	use crate::rune_relation_envelope::{RuneViewIntent, transform as transform_rune_view};

	#[cfg(feature = "toron")]
	#[test]
	fn rune_relation_results_feed_the_existing_table_and_plot_surfaces() {
		let frame = polars::df!(
			"cat" => ["a", "b"],
			"x" => [1_i64, 2],
			"y" => [3_i64, 5],
		)
		.expect("fixture frame");
		let table = rune_view_outcome(frame.clone(), RuneViewIntent::Table).expect("table view");
		assert_eq!(table.table.expect("table frame").shape(), (2, 3));

		for (source, expected) in [
			(
				"plot.bar(workspace.table(\"t\"), \"cat\", \"y\")",
				PlotType::Bar,
			),
			(
				"plot.scatter(workspace.table(\"t\"), \"x\", \"y\", \"cat\")",
				PlotType::Scatter,
			),
		] {
			let intent = transform_rune_view(source).expect("view envelope").intent;
			let outcome = rune_view_outcome(frame.clone(), intent).expect("plot view");
			let plot = outcome.plot.expect("resolved plot");
			assert_eq!(plot.composition.primary_plot_type(), expected);
			assert_eq!(plot.data.height(), 2);
		}
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
	fn editor_activity_retires_transient_sql_feedback() {
		use crate::editor::EditorMsg;
		use libaryon::engine::{InputEvent, KeyCode, Modifiers};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		set_sql_error(&mut state, "syntax error".to_owned(), None);

		clear_transient_sql_status_on_editor_activity(
			&mut state,
			&EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char('j'),
				modifiers: Modifiers::NONE,
			}),
		);

		assert!(state.status_error.is_empty());
		assert!(state.status_detail.is_none());
		state.code_editor.shutdown();
	}
}
