use crate::adapters::{
	common::{AdapterStage, ExecutionResult},
	driver::{AdapterConfiguration, AdapterSelection, AdapterState},
};
use crate::editor::{CodeEditor, MarkdownCellExecution};
use crate::fonts::DEJAVU_SANS_MONO;
use crate::gui::{
	components,
	dashboard::{DashboardEffect, DashboardMsg, DashboardState},
	menu::MenuState,
	messages::{ExportFormat, Message},
	pane_grid::{self, Axis, PaneId, State as PaneState},
};
use crate::notebook_rune::{self, PlotEffect};
use crate::persistence::{self, SavedConnection, SavedStatement, StartupData};
use crate::plot::composition::PlotComposition;
use crate::plot::export::{AvifBackend, PngBackend, SvgBackend};
use crate::plot::factory::create_plot;
use iced::{Element, Size, Subscription, Task, application, event, keyboard, time, window};
use libaryon::uast::projection::MarkdownCellOutputTarget;
use polars::frame::DataFrame;
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
	adapter_state: AdapterState,
	code_started: Instant,
	is_maximized: bool,
	saved_connections: Vec<SavedConnection>,
	editing_connection_id: Option<i64>,
	saved_statements: Vec<SavedStatement>,
	showing_save_statement_dialog: bool,
	save_statement_name: String,
	editing_statement_id: Option<i64>,
	private_db: Option<persistence::PrivateDb>,
	salt: Vec<u8>,
	is_password_protected: bool,
	showing_password_prompt: bool,
	password_entry: String,
	password_entry_error: String,
	showing_settings: bool,
	settings_new_password: String,
	settings_confirm_password: String,
	settings_error: String,
	show_column_types: bool,
	window_size: Size,
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

/// Build a Rune-published plot without blocking Iced's update loop. Keeping
/// the effect intact through the task lets the ready message attach the exact
/// composition and source snapshot that the VM produced.
fn build_notebook_plot_task(
	effect: PlotEffect,
	on_ready: impl FnOnce(
		PlotEffect,
		std::sync::Arc<dyn crate::plot::common::PlotKernel + Send + Sync>,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			let composition = effect.composition.clone();
			let data = effect.data.as_ref().clone();
			let kernel =
				tokio::task::spawn_blocking(move || create_plot(&composition, &data, 1200, 720))
					.await
					.expect("notebook plot build task panicked");
			(effect, kernel)
		},
		move |(effect, kernel)| on_ready(effect, kernel),
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
		time::every(Duration::from_millis(16)).map(|_| Message::CodeEditPoll),
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

pub fn run(startup_data: StartupData) -> Result {
	let size = startup_data
		.window_size
		.map(|(w, h)| Size::new(w, h))
		.unwrap_or(Size::new(1920.0, 1080.0));
	application(move || new(startup_data.clone()), update, view)
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

fn new(startup_data: StartupData) -> (AppState, Task<Message>) {
	let window_size = startup_data
		.window_size
		.map(|(width, height)| Size::new(width, height))
		.unwrap_or(Size::new(1920.0, 1080.0));
	let data_frame = DataFrame::default();
	let mut code_editor = CodeEditor::new("");
	code_editor.set_show_column_types(startup_data.show_column_types);
	let is_password_protected = startup_data.is_password_protected;
	let salt = startup_data.salt.clone();
	let mut state = AppState {
		dashboard: DashboardState::default(),
		menu_state: MenuState::default(),
		code_editor,
		data_frame,
		status_msg: "".to_string(),
		status_error: "".to_string(),
		status_df_size: None,
		status_time_elapsed: None,
		adapter_state: AdapterState::default(),
		code_started: Instant::now(),
		is_maximized: false,
		saved_connections: vec![],
		editing_connection_id: None,
		saved_statements: vec![],
		showing_save_statement_dialog: false,
		save_statement_name: String::new(),
		editing_statement_id: None,
		private_db: None,
		salt: startup_data.salt,
		is_password_protected,
		showing_password_prompt: is_password_protected,
		password_entry: String::new(),
		password_entry_error: String::new(),
		showing_settings: false,
		settings_new_password: String::new(),
		settings_confirm_password: String::new(),
		settings_error: String::new(),
		show_column_types: startup_data.show_column_types,
		window_size,
	};
	sync_workspace_viewport(&mut state);
	let task = if !is_password_protected {
		Task::perform(
			async move { persistence::PrivateDb::open(&salt, "").await },
			|result| match result {
				Ok(db) => Message::PrivateDbReady(db),
				Err(e) => Message::PrivateDbError(e),
			},
		)
	} else {
		Task::none()
	};
	(state, task)
}

fn view(app_state: &AppState) -> Element<'_, Message> {
	components::main_screen(
		&app_state.dashboard,
		&app_state.menu_state,
		&app_state.code_editor,
		&app_state.status_msg,
		&app_state.status_error,
		app_state.status_df_size,
		app_state.status_time_elapsed,
		&app_state.adapter_state,
		&app_state.saved_connections,
		&app_state.saved_statements,
		app_state.showing_password_prompt,
		&app_state.password_entry,
		&app_state.password_entry_error,
		app_state.showing_settings,
		&app_state.settings_new_password,
		&app_state.settings_confirm_password,
		&app_state.settings_error,
		app_state.is_password_protected,
		app_state.show_column_types,
		app_state.showing_save_statement_dialog,
		&app_state.save_statement_name,
		app_state.editing_statement_id,
	)
}

fn run_code(app_state: &mut AppState, code: String) -> Task<Message> {
	let Some(db) = app_state.adapter_state.connection.as_ref() else {
		return Task::none();
	};
	if code.trim().is_empty() {
		app_state.status_msg = "Nothing to run on the current line.".into();
		return Task::none();
	}
	let db = db.clone();
	app_state.status_msg = "Code running...".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	// The results surface is a Baryon-owned pane whose contents Polariton
	// draws: open or refresh it now so the run is visible while it executes.
	app_state.code_editor.begin_query();
	Task::perform(
		async move {
			let mut guard = db.write().await;
			guard.dispatch(&code).await
		},
		Message::RunResult,
	)
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
	let Some(db) = app_state.adapter_state.connection.as_ref() else {
		return Task::none();
	};
	if cell.code.trim().is_empty() {
		app_state.status_msg = "Nothing to run in this cell.".into();
		return Task::none();
	}
	let db = db.clone();
	let identity = cell.identity.clone();
	let presentation = cell.presentation;
	app_state.status_msg = "Code running…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	// Starting any SQL cell retires its prior named snapshot immediately.
	// Presentation only decides where the new result appears; it must not let a
	// Rune cell observe stale data while this query is in flight.
	app_state.code_editor.begin_markdown_cell(cell.identity);
	match presentation.output {
		MarkdownCellOutputTarget::Inline => {}
		MarkdownCellOutputTarget::Pane => app_state.code_editor.begin_query(),
		MarkdownCellOutputTarget::None => {}
	}
	Task::perform(
		async move {
			let mut guard = db.write().await;
			guard.dispatch(&cell.code).await
		},
		move |result| Message::RunMarkdownCellResult {
			identity,
			presentation,
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
				let mut effects = notebook_rune::execute_cell(&cell.code, snapshots)?;
				match effects.len() {
					1 => Ok(effects.remove(0)),
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
		Message::CloseWindow => {
			return window::latest()
				.and_then(|id| window::size(id).map(Message::SaveWindowSizeAndClose));
		}
		Message::SaveWindowSizeAndClose(size) => {
			return Task::perform(
				async move { persistence::save_window_size(size.width, size.height).await },
				|()| Message::DoCloseWindow,
			);
		}
		Message::DoCloseWindow => {
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
			return app_state
				.code_editor
				.update(edit_event)
				.map(Message::CodeEditEvent);
		}
		Message::CodeEditPoll => {
			app_state.code_editor.poll();
			if app_state.code_editor.take_quit_request() {
				return update(app_state, Message::CloseWindow);
			}
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
			app_state.adapter_state.stage = AdapterStage::Connected;
			app_state.status_msg = if ok {
				"Adapter connected.".into()
			} else {
				"Adapter connection failed.".into()
			};
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
			return run_code(app_state, app_state.code_editor.cached_content().to_owned());
		}
		Message::RunSnippet => {
			if let Some(cell) = app_state.code_editor.selected_markdown_cell() {
				return run_markdown_cell(app_state, cell);
			}
			let Some(code) = app_state.code_editor.selected_or_current_line() else {
				app_state.status_msg = "No editor text is available to run.".into();
				return Task::none();
			};
			return run_code(app_state, code);
		}
		Message::RunResult(er) => {
			let time_elapsed = (app_state.code_started.elapsed().as_millis() as f64) / 1000.0;
			app_state.status_time_elapsed = Some(time_elapsed);
			app_state.status_error = "".to_string();
			match er {
				ExecutionResult::Affected(rows_affected) => {
					app_state.status_msg = format!("Rows affected: {rows_affected}");
				}
				ExecutionResult::Batch(ver) => {
					let msg = ver
						.into_iter()
						.map(|er| format!("{:?}", er))
						.collect::<String>();
					app_state.status_msg = format!("Batch complete: {msg}");
				}
				ExecutionResult::CommandCompleted(msg) => {
					app_state.status_msg = format!("Command complete: {msg}");
				}
				ExecutionResult::Err(msg) => {
					app_state.status_error = format!("Error: {msg}");
					app_state.status_msg = "".to_string();
					app_state.code_editor.set_query_error(msg);
				}
				ExecutionResult::Rows(df) => {
					let rows = df.height();
					let cols = df.width();
					app_state.code_editor.set_query_result(df.clone());
					app_state.data_frame = df;
					app_state.status_df_size = Some((rows, cols));
					app_state.status_msg = "Code finished.".to_string();
				}
				ExecutionResult::None => {
					app_state.status_msg = "Noop finished.".to_string();
				}
			}
		}
		Message::RunMarkdownCellResult {
			identity,
			presentation,
			result,
		} => {
			let time_elapsed = (app_state.code_started.elapsed().as_millis() as f64) / 1000.0;
			app_state.status_time_elapsed = Some(time_elapsed);
			app_state.status_error.clear();
			match result {
				ExecutionResult::Rows(df) => {
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
							app_state.code_editor.set_query_result(df.clone())
						}
						MarkdownCellOutputTarget::None => {}
					}
					app_state.data_frame = df;
					app_state.status_df_size = Some((rows, cols));
					app_state.status_msg = "Cell finished.".to_owned();
				}
				ExecutionResult::Err(message) => {
					app_state.status_error = format!("Error: {message}");
					app_state.status_msg.clear();
					match presentation.output {
						MarkdownCellOutputTarget::Inline => app_state
							.code_editor
							.set_markdown_cell_error(identity, message),
						MarkdownCellOutputTarget::Pane => {
							app_state.code_editor.set_query_error(message)
						}
						MarkdownCellOutputTarget::None => {}
					}
				}
				ExecutionResult::Affected(rows) => {
					if presentation.output == MarkdownCellOutputTarget::Inline {
						app_state
							.code_editor
							.set_markdown_cell_message(identity, format!("Rows affected: {rows}"));
					}
					app_state.status_msg = format!("Rows affected: {rows}");
				}
				ExecutionResult::CommandCompleted(message) => {
					if presentation.output == MarkdownCellOutputTarget::Inline {
						app_state
							.code_editor
							.set_markdown_cell_message(identity, message.clone());
					}
					app_state.status_msg = message;
				}
				ExecutionResult::Batch(results) => {
					let message = format!("Batch complete: {results:?}");
					if presentation.output == MarkdownCellOutputTarget::Inline {
						app_state
							.code_editor
							.set_markdown_cell_message(identity, message.clone());
					}
					app_state.status_msg = message;
				}
				ExecutionResult::None => {
					if presentation.output == MarkdownCellOutputTarget::Inline {
						app_state
							.code_editor
							.set_markdown_cell_message(identity, "Done.".to_owned());
					}
					app_state.status_msg = "Cell finished.".to_owned();
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
				Ok(effect) => {
					app_state.status_error.clear();
					app_state.status_msg = format!("Building plot from {}…", effect.source_cell);
					return build_notebook_plot_task(effect, move |effect, kernel| {
						Message::NotebookPlotReady {
							identity,
							presentation,
							effect,
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
			effect,
			kernel,
		} => {
			match presentation.output {
				MarkdownCellOutputTarget::Inline => app_state.code_editor.set_markdown_cell_plot(
					identity,
					effect.composition,
					kernel,
				),
				MarkdownCellOutputTarget::Pane => {
					app_state.code_editor.open_dashboard();
					let _ = app_state.dashboard.update(DashboardMsg::PlotKernelReady {
						pane: None,
						composition: effect.composition,
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
			if app_state.dashboard.panes.is_some() {
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
				let dashboard = app_state.dashboard.panes.as_ref().unwrap();
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
		Message::SaveConnection => {
			let name = app_state.adapter_state.name.trim().to_string();
			if name.is_empty() {
				app_state.status_error = "Connection name is required.".to_string();
				return Task::none();
			}
			let fields = crate::adapters::driver::fields_for(&app_state.adapter_state.selection);
			let has_empty = fields.iter().any(|f| {
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
			let config_value = fields
				.first()
				.and_then(|f| app_state.adapter_state.fields.get(f.key))
				.cloned()
				.unwrap_or_default();
			let Some(db) = app_state.private_db.clone() else {
				app_state.status_error = "Database not unlocked.".to_string();
				return Task::none();
			};
			if let Some(edit_id) = app_state.editing_connection_id.take() {
				return Task::perform(
					async move { db.update_connection(edit_id, name, config_value).await },
					Message::ConnectionSaved,
				);
			}
			let adapter_type = app_state
				.adapter_state
				.selection
				.adapter_type_str()
				.to_string();
			return Task::perform(
				async move { db.save_connection(name, adapter_type, config_value).await },
				Message::ConnectionSaved,
			);
		}
		Message::ConnectionSaved(connections) => {
			app_state.saved_connections = connections;
			app_state.status_msg = "Connection saved.".to_string();
		}
		Message::SavedConnectionsLoaded(connections) => {
			app_state.saved_connections = connections;
		}
		Message::LoadSavedConnection(id) => {
			if let Some(saved) = app_state.saved_connections.iter().find(|c| c.id == id) {
				let config =
					AdapterConfiguration::from_saved(&saved.adapter_type, &saved.config_value);
				app_state.status_msg = format!("Connecting to {}...", saved.name);
				app_state.adapter_state.stage = crate::adapters::common::AdapterStage::Configured;
				return Task::perform(
					async move { AdapterState::connect(config).await },
					Message::AdapterConnected,
				);
			}
		}
		Message::EditConnection(id) => {
			if let Some(saved) = app_state.saved_connections.iter().find(|c| c.id == id) {
				let selection = match saved.adapter_type.as_str() {
					"Parquet" => AdapterSelection::Parquet,
					"Postgres" => AdapterSelection::Postgres,
					"SQLite" => AdapterSelection::SQLite,
					_ => AdapterSelection::None,
				};
				let field_key = match saved.adapter_type.as_str() {
					"Parquet" => "input_path",
					_ => "connection_string",
				};
				app_state.adapter_state.name = saved.name.clone();
				app_state.adapter_state.selection = selection;
				app_state.adapter_state.fields.clear();
				app_state
					.adapter_state
					.fields
					.insert(field_key.to_string(), saved.config_value.clone());
				app_state.adapter_state.stage = crate::adapters::common::AdapterStage::Unconfigured;
				app_state.editing_connection_id = Some(id);
			}
		}
		Message::DeleteConnection(id) => {
			let Some(db) = app_state.private_db.clone() else {
				return Task::none();
			};
			return Task::perform(
				async move { db.delete_connection(id).await },
				Message::SavedConnectionsLoaded,
			);
		}
		Message::PrivateDbReady(db) => {
			let db_clone = db.clone();
			let db_clone2 = db.clone();
			app_state.private_db = Some(db);
			app_state.showing_password_prompt = false;
			app_state.password_entry_error.clear();
			return Task::batch([
				Task::perform(
					async move { db_clone.load_connections().await },
					Message::SavedConnectionsLoaded,
				),
				Task::perform(
					async move { db_clone2.load_statements().await },
					Message::SavedStatementsLoaded,
				),
			]);
		}
		Message::OpenSaveStatementDialog => {
			app_state.showing_save_statement_dialog = true;
			app_state.save_statement_name.clear();
			app_state.editing_statement_id = None;
		}
		Message::CloseSaveStatementDialog => {
			app_state.showing_save_statement_dialog = false;
			app_state.save_statement_name.clear();
			app_state.editing_statement_id = None;
		}
		Message::SaveStatementNameChanged(val) => {
			app_state.save_statement_name = val;
		}
		Message::SaveStatement => {
			let name = app_state.save_statement_name.trim().to_string();
			if name.is_empty() {
				return Task::none();
			}
			let code = app_state.code_editor.cached_content().to_owned();
			let Some(db) = app_state.private_db.clone() else {
				app_state.status_error = "Database not unlocked.".to_string();
				return Task::none();
			};
			app_state.showing_save_statement_dialog = false;
			if let Some(id) = app_state.editing_statement_id.take() {
				return Task::perform(
					async move { db.update_statement(id, name, code).await },
					Message::StatementSaved,
				);
			}
			return Task::perform(
				async move { db.save_statement(name, code).await },
				Message::StatementSaved,
			);
		}
		Message::StatementSaved(statements) => {
			app_state.saved_statements = statements;
			app_state.save_statement_name.clear();
			app_state.status_msg = "Statement saved.".to_string();
		}
		Message::SavedStatementsLoaded(statements) => {
			app_state.saved_statements = statements;
		}
		Message::LoadSavedStatement(id) => {
			if let Some(stmt) = app_state.saved_statements.iter().find(|s| s.id == id) {
				app_state.code_editor.replace_content(&stmt.code);
				app_state.status_msg = format!("Statement '{}' loaded.", stmt.name);
			}
		}
		Message::EditStatement(id) => {
			if let Some(stmt) = app_state.saved_statements.iter().find(|s| s.id == id) {
				app_state.editing_statement_id = Some(id);
				app_state.save_statement_name = stmt.name.clone();
				app_state.showing_save_statement_dialog = true;
			}
		}
		Message::DeleteStatement(id) => {
			let Some(db) = app_state.private_db.clone() else {
				return Task::none();
			};
			return Task::perform(
				async move { db.delete_statement(id).await },
				Message::SavedStatementsLoaded,
			);
		}
		Message::PrivateDbError(e) => {
			app_state.status_error = format!("Database error: {e}");
		}
		Message::PasswordEntryChanged(val) => {
			app_state.password_entry = val;
			app_state.password_entry_error.clear();
		}
		Message::PasswordEntrySubmit => {
			let salt = app_state.salt.clone();
			let password = app_state.password_entry.clone();
			return Task::perform(
				async move { persistence::PrivateDb::open(&salt, &password).await },
				|result| match result {
					Ok(db) => Message::PrivateDbReady(db),
					Err(_) => Message::PasswordDecryptFailed,
				},
			);
		}
		Message::PasswordDecryptFailed => {
			app_state.password_entry_error = "Incorrect password.".to_string();
			app_state.password_entry.clear();
		}
		Message::OpenSettings => {
			app_state.showing_settings = true;
			app_state.settings_new_password.clear();
			app_state.settings_confirm_password.clear();
			app_state.settings_error.clear();
		}
		Message::CloseSettings => {
			app_state.showing_settings = false;
			app_state.settings_new_password.clear();
			app_state.settings_confirm_password.clear();
			app_state.settings_error.clear();
		}
		Message::SettingsNewPasswordChanged(val) => {
			app_state.settings_new_password = val;
			app_state.settings_error.clear();
		}
		Message::SettingsConfirmPasswordChanged(val) => {
			app_state.settings_confirm_password = val;
			app_state.settings_error.clear();
		}
		Message::SettingsApplyPassword => {
			if app_state.settings_new_password != app_state.settings_confirm_password {
				app_state.settings_error = "Passwords do not match.".to_string();
				return Task::none();
			}
			let Some(db) = app_state.private_db.take() else {
				app_state.settings_error = "Database not available.".to_string();
				return Task::none();
			};
			let new_key =
				persistence::derive_key(&app_state.settings_new_password, &app_state.salt);
			return Task::perform(
				async move { db.rekey(new_key).await },
				|result| match result {
					Ok(new_db) => Message::PrivateDbRekeyed(new_db),
					Err(e) => Message::PrivateDbError(e),
				},
			);
		}
		Message::SettingsRemovePassword => {
			let Some(db) = app_state.private_db.take() else {
				app_state.settings_error = "Database not available.".to_string();
				return Task::none();
			};
			let empty_key = persistence::derive_key("", &app_state.salt);
			return Task::perform(
				async move { db.rekey(empty_key).await },
				|result| match result {
					Ok(new_db) => Message::PrivateDbRekeyed(new_db),
					Err(e) => Message::PrivateDbError(e),
				},
			);
		}
		Message::PrivateDbRekeyed(new_db) => {
			let was_remove = app_state.settings_new_password.is_empty()
				&& app_state.settings_confirm_password.is_empty();
			let new_is_protected = !was_remove;
			app_state.private_db = Some(new_db);
			app_state.is_password_protected = new_is_protected;
			app_state.showing_settings = false;
			app_state.settings_new_password.clear();
			app_state.settings_confirm_password.clear();
			app_state.settings_error.clear();
			return Task::perform(
				async move { persistence::save_is_password_protected(new_is_protected).await },
				|()| Message::SettingsPasswordSaved,
			);
		}
		Message::SettingsPasswordSaved => {
			app_state.status_msg = "Password settings saved.".to_string();
		}
		Message::ToggleShowColumnTypes(val) => {
			app_state.show_column_types = val;
			app_state.code_editor.set_show_column_types(val);
			return Task::perform(
				async move { persistence::save_show_column_types(val).await },
				|()| Message::ShowColumnTypesSaved,
			);
		}
		Message::ShowColumnTypesSaved => {}
	}
	Task::none()
}

fn menu_action_to_message(action: &str) -> Option<Message> {
	match action {
		"noop" => None,
		"connect:new" => Some(Message::Connect),
		"code:run" => Some(Message::Run),
		"code:run-snippet" => Some(Message::RunSnippet),
		"code:save" => Some(Message::OpenSaveStatementDialog),
		"settings:preferences" => Some(Message::OpenSettings),
		_ => {
			if let Some(id) = action.strip_prefix("connect:load:") {
				return id.parse().ok().map(Message::LoadSavedConnection);
			}
			if let Some(id) = action.strip_prefix("connect:edit:") {
				return id.parse().ok().map(Message::EditConnection);
			}
			if let Some(id) = action.strip_prefix("connect:delete:") {
				return id.parse().ok().map(Message::DeleteConnection);
			}
			if let Some(id) = action.strip_prefix("code:load:") {
				return id.parse().ok().map(Message::LoadSavedStatement);
			}
			if let Some(id) = action.strip_prefix("code:edit:") {
				return id.parse().ok().map(Message::EditStatement);
			}
			if let Some(id) = action.strip_prefix("code:delete:") {
				return id.parse().ok().map(Message::DeleteStatement);
			}
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
