use crate::adapters::{
	common::{AdapterStage, ExecutionResult},
	driver::AdapterState,
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
use crate::persistence::{self, StartupData};
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
	showing_settings: bool,
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
		showing_settings: false,
		show_column_types: startup_data.show_column_types,
		window_size,
	};
	sync_workspace_viewport(&mut state);
	(state, Task::none())
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
		app_state.showing_settings,
		app_state.show_column_types,
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

/// Deliver a successful non-tabular cell result to its declared surface.
///
/// `output=pane` still owns a native panel even when there is no DataFrame to
/// display. Replacing `running…` with the completion message keeps schema and
/// setup cells honest instead of making the panel look hung.
fn finish_markdown_cell_message(
	app_state: &mut AppState,
	identity: libaryon::uast::projection::MarkdownCellIdentity,
	presentation: libaryon::uast::projection::MarkdownCellPresentation,
	message: String,
) {
	match presentation.output {
		MarkdownCellOutputTarget::Inline => app_state
			.code_editor
			.set_markdown_cell_message(identity, message.clone()),
		MarkdownCellOutputTarget::Pane => app_state.code_editor.set_query_message(message.clone()),
		MarkdownCellOutputTarget::None => {}
	}
	app_state.status_msg = message;
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
			let task = app_state
				.code_editor
				.update(edit_event)
				.map(Message::CodeEditEvent);
			if app_state.code_editor.take_quit_request() {
				return update(app_state, Message::CloseWindow);
			}
			return task;
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
					finish_markdown_cell_message(
						app_state,
						identity,
						presentation,
						format!("Rows affected: {rows}"),
					);
				}
				ExecutionResult::CommandCompleted(message) => {
					finish_markdown_cell_message(app_state, identity, presentation, message);
				}
				ExecutionResult::Batch(results) => {
					finish_markdown_cell_message(
						app_state,
						identity,
						presentation,
						format!("Batch complete: {results:?}"),
					);
				}
				ExecutionResult::None => {
					finish_markdown_cell_message(
						app_state,
						identity,
						presentation,
						"Cell finished.".to_owned(),
					);
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
