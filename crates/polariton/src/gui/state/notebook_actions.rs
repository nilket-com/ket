//! PRODUCT-0162: one notebook/data action owner for batch and live clients.
use super::{AppState, Message, run_markdown_cell, update_inner};
use crate::adapters::common::{CancelCapability, DatabaseAdapter};
use crate::driver::{
	Command, CommandReceipt, ConnectionDescription, DataDescription, Failure, NotebookDescription,
	Refusal,
};
use crate::editor::MarkdownCellExecution;
use iced::Task;
use libaryon::core::{BufferId, RequestId, StateId};
use libaryon::engine::{DocumentEditOutcome, DocumentEditResult};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, watch};

#[derive(Clone)]
pub(crate) struct LoadedData {
	connection: Arc<RwLock<dyn DatabaseAdapter>>,
	description: DataDescription,
}
#[derive(Clone)]
pub(super) enum Completion {
	Done {
		notebook: Option<NotebookDescription>,
		data: Option<DataDescription>,
	},
	Refused(String),
	Failed(String),
	/// ADR 0166: the connection operation settled.
	Connected(ConnectionDescription),
	ConnectionFailed {
		description: ConnectionDescription,
		message: String,
	},
}
impl Completion {
	pub(super) fn apply(self, receipt: &mut CommandReceipt) {
		match self {
			Self::Done { notebook, data } => {
				receipt.notebook = notebook;
				receipt.data = data;
			}
			Self::Refused(message) => receipt.refuse(Refusal::Notebook { message }),
			Self::Failed(message) => receipt.fail(Failure::Notebook { message }),
			Self::Connected(description) => receipt.connection = Some(description),
			Self::ConnectionFailed {
				description,
				message,
			} => {
				receipt.connection = Some(description);
				receipt.fail(Failure::Connection { message });
			}
		}
	}
}
pub(super) struct Ticket {
	rx: watch::Receiver<Option<Completion>>,
	deadline: Instant,
}
impl Ticket {
	pub(super) fn result(&self) -> Option<Completion> {
		self.rx.borrow().clone().or_else(|| {
			(Instant::now() >= self.deadline).then(|| {
				Completion::Failed(
					"notebook deadline: outcome unknown; do not replay automatically".into(),
				)
			})
		})
	}
}
#[derive(Default)]
pub(super) struct NotebookActions {
	work: Option<Work>,
}
struct Work {
	id: RequestId,
	deadline: Instant,
	done: watch::Sender<Option<Completion>>,
	kind: Kind,
}
enum Kind {
	Inspect(NotebookDescription),
	Replace {
		before: NotebookDescription,
		target: crate::driver::CellRef,
		source: String,
	},
	Run {
		description: NotebookDescription,
		cell: Option<MarkdownCellExecution>,
		render_token: Option<u64>,
	},
	Data {
		path: String,
		generation: u64,
	},
	/// ADR 0166: a connect attempt, settled by `connect_settled`.
	Connect {
		generation: u64,
	},
}
impl AppState {
	pub(super) fn start_notebook_command(
		&mut self,
		command: &Command,
	) -> Result<Option<(Ticket, Task<Message>)>, Refusal> {
		if !matches!(
			command,
			Command::DescribeNotebook { .. }
				| Command::OpenData { .. }
				| Command::ReplaceCell { .. }
				| Command::RunCell { .. }
				| Command::Connect { .. }
		) {
			return Ok(None);
		}
		let refuse = |message: String| Refusal::Notebook { message };
		if self.notebook_actions.work.is_some() {
			return Err(refuse("notebook action busy".into()));
		}
		let mut guard = None;
		let mut kind = match command {
			Command::DescribeNotebook { buffer } => {
				let description = self
					.code_editor
					.notebook_description(buffer.as_deref())
					.map_err(refuse)?;
				let b = BufferId::new(
					description
						.buffer
						.parse()
						.map_err(|_| refuse("invalid buffer".into()))?,
				);
				let g = StateId::new(
					description
						.generation
						.parse()
						.map_err(|_| refuse("invalid generation".into()))?,
				);
				guard = Some((b, g, vec![]));
				Kind::Inspect(description)
			}
			Command::ReplaceCell { target, source } => {
				if source.len() > 32 * 1024
					|| (!source.is_empty() && !source.ends_with('\n'))
					|| source.lines().any(|l| {
						l.trim_start().starts_with("```") || l.trim_start().starts_with("~~~")
					}) {
					return Err(refuse("replacement must be a cell body of at most 32 KiB, ending in a newline, without fence lines".into()));
				}
				let snapshot = self
					.code_editor
					.notebook_cell_snapshot(target)
					.map_err(refuse)?;
				let before = self
					.code_editor
					.notebook_description(Some(&target.buffer))
					.map_err(refuse)?;
				let total: usize = before.cells.iter().map(|c| c.source.len()).sum();
				if total - snapshot.cell.code.len() + source.len() > 32 * 1024 {
					return Err(refuse("notebook cell bodies exceed 32 KiB".into()));
				}
				let mut edit = snapshot.edit;
				edit.replacement = source.clone();
				guard = Some((snapshot.buffer, snapshot.generation, vec![edit]));
				Kind::Replace {
					before,
					target: target.clone(),
					source: source.clone(),
				}
			}
			Command::RunCell { target } => {
				// Notebook result names are currently application-scoped.
				// Selecting source must not claim to isolate those results.
				self.code_editor
					.notebook_description(None)
					.map_err(refuse)?;
				let snapshot = self
					.code_editor
					.notebook_cell_snapshot(target)
					.map_err(refuse)?;
				if !matches!(snapshot.cell.language_id.as_str(), "sql" | "rune")
					|| snapshot.cell.code.trim().is_empty()
				{
					return Err(refuse("cell must contain executable SQL or Rune".into()));
				}
				if self.adapter_state.sql_connection().is_none() {
					return Err(refuse("SQL target unavailable".into()));
				}
				let description = self
					.code_editor
					.notebook_description(Some(&target.buffer))
					.map_err(refuse)?;
				guard = Some((snapshot.buffer, snapshot.generation, vec![]));
				Kind::Run {
					description,
					cell: Some(snapshot.cell),
					render_token: None,
				}
			}
			Command::OpenData { path } => {
				if path.is_empty() {
					return Err(refuse("data path is required".into()));
				}
				Kind::Data {
					path: path.clone(),
					generation: self.connection_generation,
				}
			}
			Command::Connect {
				adapter,
				name,
				fields,
			} => {
				let refuse = |message: String| Refusal::ConnectionRefused { message };
				let selection = crate::adapters::driver::AdapterSelection::from_type_str(adapter)
					.ok_or_else(|| refuse(format!("unknown adapter {adapter:?}")))?;
				if self.connect_in_flight.is_some() {
					return Err(refuse("a connection attempt is already in flight".into()));
				}
				// ADR 0166 §2: the form's judgement on a candidate first; only an
				// admitted candidate becomes the form's state, so a refusal leaves
				// the installed connection and the reported adapter untouched.
				let mut candidate: std::collections::BTreeMap<String, String> =
					crate::adapters::driver::fields_for(&selection)
						.iter()
						.filter_map(|field| {
							field
								.default_value
								.map(|value| (field.key.to_owned(), value.to_owned()))
						})
						.collect();
				candidate.extend(fields.iter().map(|(k, v)| (k.clone(), v.clone())));
				Self::validate_candidate(&selection, &candidate, false).map_err(refuse)?;
				self.adapter_state.select(selection);
				self.adapter_state.name = name.clone();
				self.adapter_state.fields = candidate;
				Kind::Connect { generation: 0 }
			}
			_ => unreachable!(),
		};
		self.document_request_sequence = self
			.document_request_sequence
			.checked_add(1)
			.ok_or_else(|| refuse("document request ids exhausted".into()))?;
		let id = RequestId::new(self.document_request_sequence);
		let (done, rx) = watch::channel(None);
		let deadline = Instant::now() + Duration::from_secs(60);
		let task = if let Some((buffer, generation, edits)) = guard {
			if !self
				.code_editor
				.apply_document_edits(id, buffer, generation, edits)
			{
				return Err(refuse("editor refused the request".into()));
			}
			Task::none()
		} else if let Kind::Data { path, .. } = &kind {
			let path = path.clone();
			Task::perform(
				async move {
					tokio::task::spawn_blocking(move || {
						let (adapter, info) =
							crate::adapters::parquet::open_data(std::path::Path::new(&path))?;
						Ok(LoadedData {
							connection: Arc::new(RwLock::new(adapter)),
							description: DataDescription {
								table: info.table,
								columns: info.columns,
							},
						})
					})
					.await
					.unwrap_or_else(|e| Err(format!("file task: {e}")))
				},
				move |result| Message::NotebookDataLoaded { id, result },
			)
		} else if matches!(kind, Kind::Connect { .. }) {
			let task = self.begin_connect();
			kind = Kind::Connect {
				generation: self.connect_in_flight.unwrap_or(self.connection_generation),
			};
			task
		} else {
			unreachable!()
		};
		self.notebook_actions.work = Some(Work {
			id,
			deadline,
			done,
			kind,
		});
		Ok(Some((Ticket { rx, deadline }, task)))
	}
	/// ADR 0166: a connect attempt tagged `generation` settled; answer the
	/// command that started it, if that is what is pending.
	pub(super) fn complete_connect_command(
		&mut self,
		generation: u64,
		outcome: Result<(), String>,
	) {
		let pending = matches!(
			self.notebook_actions.work.as_ref().map(|work| &work.kind),
			Some(Kind::Connect { generation: started }) if *started == generation
		);
		if !pending {
			return;
		}
		let description = self.connection_description();
		self.complete_notebook(match outcome {
			Ok(()) => Completion::Connected(description),
			Err(message) => Completion::ConnectionFailed {
				description,
				message,
			},
		});
	}
	fn complete_notebook(&mut self, result: Completion) {
		if let Some(work) = self.notebook_actions.work.take() {
			work.done.send_replace(Some(result));
		}
	}
	pub(super) fn expire_notebook_action(&mut self) {
		if let Some(Work {
			kind: Kind::Run {
				description,
				render_token: Some(token),
				..
			},
			..
		}) = &self.notebook_actions.work
		{
			if self
				.code_editor
				.host_plot_exports_with_identity()
				.iter()
				.any(|(_, incarnation, _)| incarnation.token() == *token)
			{
				let description = description.clone();
				self.complete_notebook(Completion::Done {
					notebook: Some(description),
					data: None,
				});
			} else if !self.code_editor.host_plot_incarnations().contains(token) {
				self.complete_notebook(Completion::Failed(
					"the requested cell's plot was superseded before projection".into(),
				));
			}
		}
		if self
			.notebook_actions
			.work
			.as_ref()
			.is_some_and(|w| Instant::now() >= w.deadline)
		{
			self.complete_notebook(Completion::Failed(
				"notebook deadline: outcome unknown; do not replay automatically".into(),
			));
		}
	}
	pub(super) fn notebook_edit_ack(
		&mut self,
		result: &DocumentEditResult,
	) -> Option<Task<Message>> {
		let work = self.notebook_actions.work.as_mut()?;
		if work.id != result.request_id {
			return None;
		}
		let new_generation = match &result.outcome {
			DocumentEditOutcome::Applied { new_generation } => *new_generation,
			DocumentEditOutcome::Refused { reason } => {
				self.complete_notebook(Completion::Refused(reason.to_string()));
				return Some(Task::none());
			}
		};
		match &mut work.kind {
			Kind::Inspect(description) => {
				let description = description.clone();
				self.complete_notebook(Completion::Done {
					notebook: Some(description),
					data: None,
				});
			}
			Kind::Replace {
				before,
				target,
				source,
			} => {
				let mut description = before.clone();
				self.code_editor
					.acknowledge_notebook_edit(target, source, new_generation);
				description.generation = new_generation.get().to_string();
				description.cells[target.cell as usize].source = source.clone();
				self.complete_notebook(Completion::Done {
					notebook: Some(description),
					data: None,
				});
			}
			Kind::Run { cell, .. } => {
				let id = work.id;
				let cell = cell.take().expect("guard acknowledgement occurs once");
				self.status_error.clear();
				let task = run_markdown_cell(self, cell);
				if !self.status_error.is_empty() {
					self.complete_notebook(Completion::Failed(self.status_error.clone()));
				}
				return Some(task.map(move |message| Message::NotebookTask {
					id,
					message: Box::new(message),
				}));
			}
			Kind::Data { .. } | Kind::Connect { .. } => unreachable!(),
		}
		Some(Task::none())
	}
	pub(super) fn notebook_data_loaded(
		&mut self,
		id: RequestId,
		result: Result<LoadedData, String>,
	) -> Task<Message> {
		let Some(work) = self.notebook_actions.work.as_ref().filter(|w| w.id == id) else {
			return Task::none();
		};
		let Kind::Data { path, generation } = &work.kind else {
			return Task::none();
		};
		let same = *generation == self.connection_generation;
		if !same {
			self.complete_notebook(Completion::Failed(
				"connection changed while the file was loading".into(),
			));
			return Task::none();
		}
		match result {
			Ok(loaded) => {
				let opened_path = path.clone();
				self.adapter_state.selection = crate::adapters::driver::AdapterSelection::Parquet;
				self.adapter_state.configuration =
					crate::adapters::driver::AdapterConfiguration::Parquet {
						input_path: path.clone(),
					};
				self.adapter_state.name.clear();
				// ADR 0166 §3: an opened file is a new connection generation.
				self.next_connection_generation();
				self.install_connection(loaded.connection, CancelCapability::Refuse);
				let task = Task::none();
				self.welcome_data_opened(&opened_path);
				self.complete_notebook(Completion::Done {
					notebook: None,
					data: Some(loaded.description),
				});
				return task;
			}
			Err(message) => {
				eprintln!("Open data: {message}");
				if self.welcome.active {
					self.reporting.emit(
						crate::reporting::Step::OwnFileOpened,
						Some(crate::reporting::Reason::FileUnsupported),
						true,
					);
				}
				self.status_error = message.clone();
				self.complete_notebook(Completion::Failed(message));
			}
		}
		Task::none()
	}
	pub(super) fn notebook_task(&mut self, id: RequestId, message: Message) -> Task<Message> {
		let Some(work) = self.notebook_actions.work.as_ref().filter(|w| w.id == id) else {
			return if matches!(&message, Message::CodeEditEvent(_)) {
				update_inner(self, message)
			} else {
				Task::none()
			};
		};
		let Kind::Run { description, .. } = &work.kind else {
			return Task::none();
		};
		let description = description.clone();
		let before = self.code_editor.host_plot_incarnations();
		let plot_ready = matches!(&message, Message::NotebookPlotReady { .. });
		let mut terminal = match &message {
			Message::RunMarkdownCellResult {
				result, generation, ..
			} => Some(if *generation != self.current_sql_generation() {
				Err("cell execution was superseded".into())
			} else {
				match result {
					crate::adapters::common::ExecutionResult::Err(e) => Err(e.clone()),
					_ => Ok(()),
				}
			}),
			Message::RunRuneCellResult { result, .. } => match result {
				Ok(result) if result.plot.is_some() => None,
				Ok(_) => Some(Ok(())),
				Err(e) => Some(Err(e.message.clone())),
			},
			Message::NotebookPlotReady { .. } => Some(Ok(())),
			_ => None,
		};
		let task = update_inner(self, message);
		if matches!(terminal, Some(Ok(()))) && !self.status_error.is_empty() {
			terminal = Some(Err(self.status_error.clone()));
		}
		if plot_ready {
			let after = self.code_editor.host_plot_incarnations();
			if let Some(token) = after.difference(&before).next().copied() {
				if let Some(Work {
					kind: Kind::Run { render_token, .. },
					..
				}) = &mut self.notebook_actions.work
				{
					*render_token = Some(token);
				}
				terminal = None;
			}
		}
		if let Some(result) = terminal {
			match result {
				Ok(()) => self.complete_notebook(Completion::Done {
					notebook: Some(description),
					data: None,
				}),
				Err(e) => self.complete_notebook(Completion::Failed(e)),
			}
		}
		task.map(move |message| Message::NotebookTask {
			id,
			message: Box::new(message),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::super::headless::HeadlessApplication;
	use super::*;
	use crate::driver::{CellRef, Outcome};
	struct Fixture {
		root: std::path::PathBuf,
		app: HeadlessApplication,
	}
	const SOURCE: &str = "# Agent notebook\n\n```sql {#data, output=none}\nSELECT 1 AS x, 2 AS y;\n```\n\n```rune {#plot, output=pane}\nviz.plot(workspace.cell(\"data\")?).scatter().line().show()?;\n```\n";
	impl Fixture {
		fn new() -> Self {
			Self::with_source(SOURCE)
		}
		fn with_source(body: &str) -> Self {
			let mut f = Self::open_source(body);
			f.app.run_source(Duration::from_secs(10)).unwrap();
			f
		}
		fn open_source(body: &str) -> Self {
			let root = std::env::temp_dir().join(format!(
				"polariton-0162-{}-{}",
				std::process::id(),
				crate::selection::SurfaceIncarnation::mint().token()
			));
			std::fs::create_dir_all(&root).unwrap();
			let source = root.join("notebook.md");
			std::fs::write(&source, body).unwrap();
			let mut app = HeadlessApplication::open_target(
				super::super::StartupTarget::Ephemeral {
					initial_file: Some(source.display().to_string()),
				},
				crate::configuration::StartupData::default(),
			)
			.unwrap();
			app.settle_until(Duration::from_secs(10), |s| {
				s.code_editor.cached_content() == body && s.adapter_state.sql_connection().is_some()
			})
			.unwrap();
			Self { root, app }
		}
		fn describe(&mut self) -> NotebookDescription {
			let receipt = self
				.app
				.apply_command(&Command::DescribeNotebook { buffer: None });
			assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
			receipt.notebook.unwrap()
		}
		fn target(&mut self, cell: u32) -> CellRef {
			let n = self.describe();
			CellRef {
				buffer: n.buffer,
				generation: n.generation,
				cell,
			}
		}
		fn run(&mut self, cell: u32) -> CommandReceipt {
			let target = self.target(cell);
			self.app.apply_command(&Command::RunCell { target })
		}
		fn replace(&mut self, cell: u32, source: &str) -> CommandReceipt {
			let target = self.target(cell);
			self.app.apply_command(&Command::ReplaceCell {
				target,
				source: source.into(),
			})
		}
	}
	impl Drop for Fixture {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.root);
		}
	}
	#[test]
	fn product_0162_headless_file_edit_run_and_publish_share_the_application() {
		let mut f = Fixture::new();
		#[cfg(feature = "toron-local")]
		assert!(f.app.state().adapter_state.local_workspace.is_none());
		let csv = f.root.join("own.csv");
		std::fs::write(&csv, "x,y\n1,3\n2,6\n3,4\n").unwrap();
		let opened = f.app.apply_command(&Command::OpenData {
			path: csv.display().to_string(),
		});
		assert_eq!(opened.outcome, Outcome::Applied, "{opened:?}");
		assert_eq!(opened.data.unwrap().table, "own");
		assert_eq!(
			f.replace(0, "SELECT x, y FROM own;\n").outcome,
			Outcome::Applied
		);
		assert_eq!(f.run(0).outcome, Outcome::Applied);
		let plot = f.run(1);
		assert_eq!(plot.outcome, Outcome::Applied, "{plot:?}");
		assert_eq!(
			plot.figures.as_ref().unwrap().len(),
			1,
			"completion includes the requested plot's projection"
		);
		let first = plot.figures.unwrap()[0].figure.clone();
		assert_eq!(
			f.replace(0, "SELECT x, y * 2 AS y FROM own;\n").outcome,
			Outcome::Applied
		);
		assert_eq!(f.run(0).outcome, Outcome::Applied);
		let second = f.run(1);
		assert_eq!(second.outcome, Outcome::Applied, "{second:?}");
		assert_ne!(first, second.figures.unwrap()[0].figure);
		let published = f.app.apply_command(&Command::Publish {
			directory: f.root.join("package").display().to_string(),
		});
		assert_eq!(published.outcome, Outcome::Applied, "{published:?}");
		assert!(published.publish.is_some());
	}
	#[test]
	fn product_0162_queued_typing_wins_over_a_stale_agent_edit_at_the_engine() {
		let mut f = Fixture::new();
		let target = f.target(0);
		let snapshot = f
			.app
			.state()
			.code_editor
			.notebook_cell_snapshot(&target)
			.unwrap();
		let mut human_edit = snapshot.edit;
		human_edit.replacement = "SELECT 7 AS x, 8 AS y;\n".into();
		assert!(f.app.state().code_editor.apply_document_edits(
			RequestId::new(u64::MAX),
			snapshot.buffer,
			snapshot.generation,
			vec![human_edit]
		));
		let rejected = f.app.apply_command(&Command::ReplaceCell {
			target,
			source: "SELECT 99 AS x;\n".into(),
		});
		assert_eq!(rejected.outcome, Outcome::Refused, "{rejected:?}");
		f.app.state_mut().code_editor.request_content();
		f.app
			.settle_until(Duration::from_secs(5), |s| {
				s.code_editor
					.notebook_description(None)
					.is_ok_and(|n| n.cells[0].source.contains("SELECT 7"))
			})
			.unwrap();
		assert!(f.describe().cells[0].source.contains("SELECT 7"));
	}
	#[test]
	fn product_0162_failures_and_preflight_refusals_are_distinct() {
		let mut f = Fixture::new();
		let missing = f.app.apply_command(&Command::OpenData {
			path: f.root.join("private-missing.parquet").display().to_string(),
		});
		assert_eq!(missing.outcome, Outcome::Failed);
		assert!(missing.failure.is_some());
		assert_eq!(
			f.replace(0, "SELECT missing_column FROM nowhere;\n")
				.outcome,
			Outcome::Applied
		);
		let sql = f.run(0);
		assert_eq!(sql.outcome, Outcome::Failed, "{sql:?}");
		assert_eq!(
			f.replace(1, "this is not Rune;\n").outcome,
			Outcome::Applied
		);
		let rune = f.run(1);
		assert_eq!(rune.outcome, Outcome::Failed, "{rune:?}");
		assert_eq!(f.replace(0, "").outcome, Outcome::Applied);
		let empty = f.run(0);
		assert_eq!(empty.outcome, Outcome::Refused);
		let target = f.target(1);
		let fence = f.app.apply_command(&Command::ReplaceCell {
			target,
			source: "```\n".into(),
		});
		assert_eq!(fence.outcome, Outcome::Refused);
	}
	#[test]
	fn product_0162_duplicate_cell_names_and_oversized_notebooks_refuse_inspection() {
		for body in [
			SOURCE.replace("#plot", "#data"),
			format!("```sql {{#data}}\n{}\n```\n", "x".repeat(32 * 1024)),
		] {
			let mut f = Fixture::open_source(&body);
			let receipt = f
				.app
				.apply_command(&Command::DescribeNotebook { buffer: None });
			assert_eq!(receipt.outcome, Outcome::Refused, "{receipt:?}");
			assert!(receipt.notebook.is_none());
		}
	}

	#[test]
	fn product_0162_unsupported_language_refuses_execution() {
		let mut f = Fixture::with_source("```python {#data}\nprint('hello')\n```\n");
		let receipt = f.run(0);
		assert_eq!(receipt.outcome, Outcome::Refused);
	}

	#[test]
	fn product_0162_invalid_targets_and_body_bounds_refuse_without_edits() {
		let mut f = Fixture::new();
		let before = f.describe();
		let valid = f.target(0);
		for target in [
			CellRef {
				buffer: "18446744073709551615".into(),
				..valid.clone()
			},
			CellRef {
				cell: u32::MAX,
				..valid.clone()
			},
			CellRef {
				generation: "invalid".into(),
				..valid.clone()
			},
		] {
			let receipt = f.app.apply_command(&Command::RunCell { target });
			assert_eq!(receipt.outcome, Outcome::Refused, "{receipt:?}");
		}
		let receipt = f.app.apply_command(&Command::ReplaceCell {
			target: valid,
			source: format!("{}\n", "x".repeat(32 * 1024)),
		});
		assert_eq!(receipt.outcome, Outcome::Refused);
		let after = f.describe();
		assert_eq!(before.generation, after.generation);
		assert_eq!(before.cells[0].source, after.cells[0].source);
	}
	#[test]
	fn product_0162_deadline_has_unknown_outcome_and_late_load_cannot_install() {
		let mut f = Fixture::new();
		let runtime = tokio::runtime::Runtime::new().unwrap();
		let _entered = runtime.enter();
		let started = f.app.state_mut().start_command(&Command::OpenData {
			path: "pending.csv".into(),
		});
		let work = f.app.state_mut().notebook_actions.work.as_mut().unwrap();
		let id = work.id;
		work.deadline = Instant::now() - Duration::from_secs(1);
		f.app.state_mut().expire_notebook_action();
		let super::super::actions::Pending::Notebook(ticket) = started.pending.unwrap() else {
			panic!()
		};
		let mut receipt = started.receipt;
		ticket.result().unwrap().apply(&mut receipt);
		assert_eq!(receipt.outcome, Outcome::Failed);
		let Some(Failure::Notebook { message }) = receipt.failure else {
			panic!()
		};
		assert!(message.contains("unknown"));
		let generation = f.app.state().connection_generation;
		drop(
			f.app
				.state_mut()
				.notebook_data_loaded(id, Err("late".into())),
		);
		assert_eq!(generation, f.app.state().connection_generation);
		assert!(f.app.state().notebook_actions.work.is_none());
	}

	#[test]
	fn product_0162_busy_and_connection_replacement_do_not_clobber_new_state() {
		let mut f = Fixture::new();
		let runtime = tokio::runtime::Runtime::new().unwrap();
		let _entered = runtime.enter();
		let started = f.app.state_mut().start_command(&Command::OpenData {
			path: f.root.join("old.csv").display().to_string(),
		});
		assert!(started.pending.is_some());
		let busy = f
			.app
			.state_mut()
			.start_command(&Command::DescribeNotebook { buffer: None });
		assert_eq!(busy.receipt.outcome, Outcome::Refused);
		let id = f.app.state().notebook_actions.work.as_ref().unwrap().id;
		f.app.dispatch(Message::AdapterSelected(
			crate::adapters::driver::AdapterSelection::SQLite,
		));
		let task = f
			.app
			.state_mut()
			.notebook_data_loaded(id, Err("load was overtaken".into()));
		drop(task);
		let super::super::actions::Pending::Notebook(ticket) = started.pending.unwrap() else {
			panic!()
		};
		let mut receipt = started.receipt;
		ticket.result().unwrap().apply(&mut receipt);
		assert_eq!(receipt.outcome, Outcome::Failed);
		assert!(matches!(
			f.app.state().adapter_state.selection,
			crate::adapters::driver::AdapterSelection::SQLite
		));
	}
}
