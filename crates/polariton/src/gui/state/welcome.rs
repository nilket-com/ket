//! PRODUCT-0161: welcome uses the same cell dispatch, kernels and publisher.
use super::*;
use crate::reporting::{Reason, Step};
use iced::widget::{button, column, container, row, text, text_input};

use crate::driver::{CellRef, Command, CommandReceipt, NotebookDescription, Outcome};
pub(super) struct Pending {
	ticket: notebook_actions::Ticket,
	receipt: CommandReceipt,
	phase: Phase,
}
enum Phase {
	Open(CellRef),
	Replace,
	Sql,
	Plot,
}
fn target(notebook: &NotebookDescription, name: &str) -> std::result::Result<CellRef, String> {
	let cell = notebook
		.cells
		.iter()
		.find(|c| c.name.as_deref() == Some(name))
		.ok_or("welcome cell unavailable")?;
	Ok(CellRef {
		buffer: notebook.buffer.clone(),
		generation: notebook.generation.clone(),
		cell: cell.cell,
	})
}

impl AppState {
	pub(super) fn advance_welcome(&mut self) -> Task<Message> {
		if self.welcome_action.is_some() {
			return self.advance_welcome_action();
		}
		if let Some(detail) = self.reporting.detail() {
			if self.status_error.is_empty() {
				self.status_detail = Some(StatusDetail::new(self.status_msg.clone(), detail));
			}
		}
		if !self.welcome.active || self.adapter_state.sql_connection().is_none() {
			return Task::none();
		}
		if self.code_editor.cached_content() != self.welcome.original_source
			&& self.welcome.stage == 0
		{
			return Task::none();
		}
		#[cfg(toron_local)]
		if self.welcome.workspace_requested && self.local_creation.offer().is_some() {
			return update_inner(self, Message::StartLocalWorkspace);
		}
		#[cfg(toron_local)]
		if self.welcome.workspace_requested
			&& !self.welcome.workspace_seeded
			&& matches!(
				self.adapter_state.local_workspace,
				Some(nilket_local::LocalWorkspace::Ready(_))
			) && matches!(self.adapter_state.stage, AdapterStage::Connected)
		{
			self.welcome.workspace_seeded = true;
			self.reporting.emit(Step::WorkspaceStarted, None, false);
			let connection = self.adapter_state.connection.clone().unwrap();
			return Task::perform(
				async move {
					let mut adapter = connection.write().await;
					for sql in [
						"CREATE TABLE IF NOT EXISTS polariton_first_run (x BIGINT NOT NULL PRIMARY KEY, y BIGINT)",
						"INSERT INTO polariton_first_run VALUES (1,12),(2,18),(3,15),(4,27)",
					] {
						if let ExecutionResult::Err(e) = adapter.dispatch(sql).await {
							return Err(e);
						}
					}
					Ok(())
				},
				Message::WelcomeSeeded,
			);
		}
		#[cfg(feature = "toron")]
		if self.welcome.stage == 4 && self.code_editor.cached_content() == WORKSPACE_SOURCE {
			self.welcome.stage = 5;
			return update_inner(self, Message::Run);
		}
		match self.welcome.stage {
			0 => {
				if let Some(cell) = self.code_editor.welcome_cell("first-data") {
					self.welcome.stage = 1;
					if let Err(e) = self.welcome.mark_shown() {
						eprintln!("Welcome marker: {e}");
					}
					if self.welcome.own_kind.is_none() {
						self.reporting.emit(Step::FirstRunShown, None, false);
						#[cfg(feature = "toron")]
						self.reporting.emit(Step::WorkspaceOffered, None, false);
					}
					return run_markdown_cell(self, cell);
				}
			}
			1 => {
				if self
					.code_editor
					.named_notebook_cell_data()
					.iter()
					.any(|(name, _)| name == "first-data")
				{
					if let Some(cell) = self.code_editor.welcome_cell("first-plot") {
						self.welcome.stage = 2;
						return run_markdown_cell(self, cell);
					}
				}
			}
			_ => (),
		}
		if self.welcome.rendered && self.welcome.own_kind.is_some() {
			self.welcome.changed |= self.code_editor.cached_content()
				!= self.welcome.original_source
				|| self
					.describe_surface()
					.iter()
					.any(|f| !f.live.hidden.is_empty());
		}
		Task::none()
	}
	pub(super) fn observe_welcome_message(&mut self, message: &Message) {
		if !self.welcome.active {
			return;
		}
		if let Message::NotebookTask { message, .. } = message {
			self.observe_welcome_message(message);
			return;
		}
		match message {
			#[cfg(toron_local)]
			Message::LocalWorkspaceStartupSettled(Err(_)) => {
				self.welcome.workspace_requested = false;
				self.reporting.emit(
					Step::WorkspaceFailed,
					Some(
						self.local_workspace_owner
							.as_ref()
							.map(|o| o.monitor().report_reason())
							.unwrap_or(Reason::WorkspaceExited),
					),
					true,
				);
			}
			Message::RunMarkdownCellResult {
				identity,
				result,
				generation,
				..
			} if self.welcome.own_kind.is_some()
				&& matches!(identity, libaryon::uast::projection::MarkdownCellIdentity::Explicit { id } if id == "first-data") =>
			{
				self.welcome.own_data_ready = *generation == self.current_sql_generation()
					&& matches!(result, ExecutionResult::Rows { .. });
			}
			Message::NotebookPlotReady { .. }
				if self.welcome.own_kind.is_some()
					&& self.welcome.own_data_ready
					&& !self.welcome.rendered =>
			{
				self.welcome.rendered = true;
				self.welcome.original_source = self.code_editor.cached_content().to_owned();
				self.welcome.changed = false;
			}
			Message::NotebookPlotReady { .. } if self.welcome.stage == 2 => {
				self.welcome.rendered = true;
				self.welcome.stage = 3;
				if self.welcome.own_kind.is_none() {
					self.reporting.emit(Step::ExampleRan, None, false);
				}
			}
			Message::RunMarkdownCellResult {
				result: ExecutionResult::Err(_),
				..
			} if self.welcome.stage == 1 => {
				self.welcome.stage = 3;
				self.reporting
					.emit(Step::ExampleRan, Some(Reason::ExampleSeedFailed), true);
			}

			_ => (),
		}
	}

	fn welcome_command(&mut self, command: Command, phase: Phase) -> Task<Message> {
		let started = self.start_command(&command);
		match started.pending {
			Some(actions::Pending::Notebook(ticket)) => {
				self.welcome_action = Some(Pending {
					ticket,
					receipt: started.receipt,
					phase,
				});
			}
			_ => {
				self.status_error =
					format!("First-run action refused: {:?}", started.receipt.refusal)
			}
		}
		started.task
	}
	pub(super) fn start_welcome_open(&mut self) -> Task<Message> {
		if self.welcome_action.is_some() {
			return Task::none();
		}
		let target = self
			.code_editor
			.notebook_description(None)
			.and_then(|n| target(&n, "first-data"));
		match target {
			Ok(target) => self.welcome_command(
				Command::OpenData {
					path: self.welcome.path.clone(),
				},
				Phase::Open(target),
			),
			Err(e) => {
				self.status_error = e;
				Task::none()
			}
		}
	}
	fn advance_welcome_action(&mut self) -> Task<Message> {
		let Some(mut pending) = self.welcome_action.take() else {
			return Task::none();
		};
		let Some(result) = pending.ticket.result() else {
			self.welcome_action = Some(pending);
			return Task::none();
		};
		result.apply(&mut pending.receipt);
		if pending.receipt.outcome != Outcome::Applied {
			self.status_error = format!(
				"First-run action stopped: {:?} {:?}",
				pending.receipt.refusal, pending.receipt.failure
			);
			return Task::none();
		}
		let next: std::result::Result<Option<(Command, Phase)>, String> =
			(|| match pending.phase {
				Phase::Open(target) => {
					let data = pending.receipt.data.ok_or("file schema unavailable")?;
					let numeric: Vec<_> = data
						.columns
						.iter()
						.filter(|(_, kind)| {
							[
								"i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64",
								"f32", "f64", "decimal",
							]
							.iter()
							.any(|prefix| kind.starts_with(prefix))
						})
						.map(|(name, _)| name.as_str())
						.collect();
					if numeric.len() < 2 {
						self.reporting.emit(
							Step::OwnFileOpened,
							Some(Reason::ColumnNotPlottable),
							true,
						);
						return Err("Choose a file with at least two numeric columns for this guided example.".into());
					}
					Ok(Some((
						Command::ReplaceCell {
							target,
							source: crate::first_run::own_sql(&data.table, numeric[0], numeric[1]),
						},
						Phase::Replace,
					)))
				}
				Phase::Replace | Phase::Sql => {
					let notebook = pending
						.receipt
						.notebook
						.ok_or("notebook receipt unavailable")?;
					let (name, phase) = if matches!(pending.phase, Phase::Replace) {
						("first-data", Phase::Sql)
					} else {
						("first-plot", Phase::Plot)
					};
					Ok(Some((
						Command::RunCell {
							target: target(&notebook, name)?,
						},
						phase,
					)))
				}
				Phase::Plot => Ok(None),
			})();
		match next {
			Ok(Some((command, phase))) => self.welcome_command(command, phase),
			Ok(None) => Task::none(),
			Err(e) => {
				self.status_error = e;
				Task::none()
			}
		}
	}
	pub(super) fn welcome_data_opened(&mut self, path: &str) {
		if !self.welcome.active {
			return;
		}
		self.welcome.own_kind = Some(
			if std::path::Path::new(path)
				.extension()
				.is_some_and(|s| s.eq_ignore_ascii_case("csv"))
			{
				Reason::Csv
			} else {
				Reason::Parquet
			},
		);
		self.welcome.stage = 3;
		self.welcome.rendered = false;
		self.welcome.own_data_ready = false;
		self.welcome.changed = false;
		self.welcome.kept = Default::default();
		self.reporting.emit(Step::OwnFileOpened, None, false);
	}
}
pub(super) fn view<'a>(state: &'a AppState, screen: Element<'a, Message>) -> Element<'a, Message> {
	if !state.welcome.active {
		return screen;
	}
	let notice = if state.welcome.reporting_on {
		"Reporting enabled to your configured endpoint. Change init.rn and restart to disable."
	} else {
		"Reporting is off. No analysis activity is collected or sent. Disabling attempts one final settings notice. Settings: init.rn (read at startup)."
	};
	let controls = column![
		text(notice).size(13),
		row![
			text_input("Path to your Parquet or CSV file", &state.welcome.path)
				.on_input(Message::WelcomePath)
				.on_submit(Message::WelcomeOpen),
			button("Open my data").on_press(Message::WelcomeOpen),
		]
		.spacing(8)
	]
	.spacing(6);
	#[cfg(toron_local)]
	let controls =
		controls.push(button("Start workspace (optional)").on_press(Message::WelcomeWorkspace));
	column![screen, container(controls).padding(10)].into()
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn product_0161_offline_welcome_runs_real_cells_and_own_file_publishes() {
		let mut app = super::super::headless::HeadlessApplication::open_welcome().unwrap();
		app.settle_until(std::time::Duration::from_secs(20), |s| {
			!s.describe_surface().is_empty()
		})
		.expect("offline notebook produces plot");
		assert!(app.state_mut().welcome.rendered);
		assert!(app.state_mut().reporting.detail().is_none());
		#[cfg(toron_local)]
		assert!(app.state_mut().adapter_state.local_workspace.is_none());
		let scratch =
			std::env::temp_dir().join(format!("polariton-first-run-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir_all(&scratch).unwrap();
		let csv = scratch.join("own.csv");
		std::fs::write(&csv, "day,value\n1,3\n2,8\n3,5\n").unwrap();
		app.dispatch(Message::WelcomePath(csv.display().to_string()));
		app.dispatch(Message::WelcomeOpen);
		app.settle_until(std::time::Duration::from_secs(20), |s| {
			s.welcome.rendered && s.welcome.own_kind.is_some()
		})
		.expect("own file produces plot");
		let edited = app
			.state_mut()
			.welcome
			.original_source
			.replace(" AS y FROM", " * 2 AS y FROM");
		app.state_mut().code_editor.open_welcome_notebook(&edited);
		app.settle_until(std::time::Duration::from_secs(10), |s| {
			s.code_editor.cached_content() == edited
				&& s.code_editor.welcome_cell("first-data").is_some()
		})
		.unwrap();
		app.run_welcome_cell("first-data");
		app.settle_until(std::time::Duration::from_millis(250), |_| false)
			.ok();
		app.run_welcome_cell("first-plot");
		app.settle_until(std::time::Duration::from_millis(500), |_| false)
			.ok();
		assert!(app.state_mut().welcome.changed);
		let published = app.apply_command(&crate::driver::Command::Publish {
			directory: scratch.join("package").display().to_string(),
		});
		assert_eq!(
			published.outcome,
			crate::driver::Outcome::Applied,
			"{published:?}"
		);
		assert!(published.publish.is_some());
		std::fs::remove_dir_all(scratch).unwrap();
	}
	#[cfg(toron_local)]
	#[test]
	fn product_0161_workspace_source_preserves_the_notebook_buffer() {
		let mut app = super::super::headless::HeadlessApplication::open_welcome().unwrap();
		app.settle_until(Duration::from_secs(10), |s| {
			!s.describe_surface().is_empty()
		})
		.unwrap();
		let before = app.state().code_editor.notebook_description(None).unwrap();
		app.dispatch(Message::WelcomeSeeded(Ok(())));
		app.settle_until(Duration::from_secs(10), |s| {
			s.code_editor.cached_content() == WORKSPACE_SOURCE
		})
		.unwrap();
		assert!(
			!app.state().code_editor.is_notebook(),
			"the focused Rune source owns Run"
		);
		let after = app
			.state()
			.code_editor
			.notebook_description(Some(&before.buffer))
			.unwrap();
		assert_eq!(before, after);
	}
	#[cfg(toron_local)]
	#[test]
	#[ignore = "requires an isolated XDG profile; set POLARITON_0161_WORKSPACE_EXPECT to ready or missing"]
	fn product_0161_workspace_path_against_an_isolated_profile() {
		let expected = std::env::var("POLARITON_0161_WORKSPACE_EXPECT").unwrap();
		let mut app = super::super::headless::HeadlessApplication::open_welcome().unwrap();
		app.settle_until(Duration::from_secs(10), |s| {
			!s.describe_surface().is_empty()
		})
		.unwrap();
		let before = app.state().code_editor.notebook_description(None).unwrap();
		app.dispatch(Message::WelcomeWorkspace);
		if expected == "ready" {
			app.settle_until(Duration::from_secs(70), |s| {
				s.describe_surface().iter().any(|f| f.title == "plot.line")
			})
			.unwrap();
		} else {
			app.settle_until(Duration::from_secs(10), |s| !s.welcome.workspace_requested)
				.unwrap();
			assert_eq!(
				app.state()
					.local_workspace_owner
					.as_ref()
					.unwrap()
					.monitor()
					.report_reason(),
				Reason::WorkspaceLauncherMissing
			);
			assert!(!app.state().describe_surface().is_empty());
		}
		let after = app
			.state()
			.code_editor
			.notebook_description(Some(&before.buffer))
			.unwrap();
		assert_eq!(before, after);
	}
}

#[cfg(feature = "toron")]
pub(super) const WORKSPACE_SOURCE: &str = r#"// The example table is polariton_first_run. Remove it with DROP TABLE polariton_first_run.
workspace.table("polariton_first_run").views([view("first-run", figure([
    title("Your local workspace"), line("x", "y").stroke_width(2),
]))]);
"#;
