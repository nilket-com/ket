//! PRODUCT-0162: one notebook/data action owner for batch and live clients.
use super::{AppState, Message, run_markdown_cell, update_inner};
use crate::adapters::common::{CancelCapability, DatabaseAdapter};
use crate::driver::{
	Command, CommandReceipt, ConnectionDescription, DataDescription, ExecutionDescription, Failure,
	NotebookDescription, Refusal,
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
		/// ADR 0168: the answer of the SQL cell this run executed.
		result: Option<crate::driver::ResultSummary>,
	},
	Refused(String),
	Failed(String),
	/// ADR 0166: the connection operation settled.
	Connected(ConnectionDescription),
	ConnectionFailed {
		description: ConnectionDescription,
		message: String,
	},
	/// ADR 0169 §3: one identified execution settled, on its own terms.
	Execution {
		description: ExecutionDescription,
		outcome: Result<(), String>,
	},
}
impl Completion {
	pub(super) fn apply(self, receipt: &mut CommandReceipt) {
		match self {
			Self::Done {
				notebook,
				data,
				result,
			} => {
				receipt.notebook = notebook;
				receipt.data = data;
				receipt.result = result;
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
			Self::Execution {
				description,
				outcome,
			} => {
				receipt.execution = Some(description);
				if let Err(message) = outcome {
					receipt.fail(Failure::Notebook { message });
				}
			}
		}
	}
}
pub(super) struct Ticket {
	rx: watch::Receiver<Option<Completion>>,
	deadline: Instant,
	deadline_message: String,
}
impl Ticket {
	pub(super) fn result(&self) -> Option<Completion> {
		self.rx.borrow().clone().or_else(|| {
			(Instant::now() >= self.deadline)
				.then(|| Completion::Failed(self.deadline_message.clone()))
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
	/// ADR 0169: one execution of the open Rune source; `revision` is the
	/// window's execution id once the run has started.
	RunSource {
		buffer: BufferId,
		revision: u64,
		description: ExecutionDescription,
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
				| Command::RunSource {}
		) {
			return Ok(None);
		}
		let refuse = |message: String| Refusal::Notebook { message };
		if let Some(Work {
			kind: Kind::RunSource { revision, .. },
			..
		}) = &self.notebook_actions.work
		{
			// ADR 0169 §3: an overlapping request names the execution in flight.
			return Err(refuse(format!(
				"an execution is in flight: {revision}; wait for its receipt"
			)));
		}
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
			Command::RunSource {} => {
				if self.code_editor.is_notebook() {
					return Err(refuse(
						"the open source is a notebook; run its cells with run_cell".into(),
					));
				}
				let language = self
					.code_editor
					.active_language_id()
					.ok_or_else(|| refuse("no source is open".into()))?;
				if language.as_str() != "rune" {
					return Err(refuse(format!(
						"the open source is {language}, not a Rune source"
					)));
				}
				let buffer = self
					.code_editor
					.active_buffer_id()
					.ok_or_else(|| refuse("the open source has no buffer identity".into()))?;
				let (_, generation) = self
					.code_editor
					.document_snapshot(buffer)
					.ok_or_else(|| refuse("the source text is not yet projected".into()))?;
				// A source with its own `main` runs locally and needs no Toron.
				let mode = self
					.code_editor
					.document_snapshot(buffer)
					.map(|(text, _)| crate::rune_local::mode(text));
				let local = mode.as_ref().is_some_and(crate::rune_local::Mode::is_local);
				#[cfg(not(feature = "toron"))]
				let unavailable = !local;
				#[cfg(feature = "toron")]
				let unavailable = !local && self.adapter_state.rune_connection().is_none();
				if unavailable {
					// A source that does not parse is refused for that, not for a
					// connection it may never have needed.
					if let Some(crate::rune_local::Mode::Unparsed(error)) = mode {
						return Err(refuse(format!("the source does not parse: {error}")));
					}
					#[cfg(not(feature = "toron"))]
					let reason = "running a source requires Polariton's toron feature";
					#[cfg(feature = "toron")]
					let reason = "no Rune connection: connect to a Toron before run_source";
					return Err(refuse(reason.into()));
				}
				guard = Some((buffer, generation, vec![]));
				Kind::RunSource {
					buffer,
					revision: 0,
					description: ExecutionDescription {
						buffer: buffer.get().to_string(),
						generation: generation.get().to_string(),
						connection_generation: self.connection_generation,
						id: 0,
					},
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
		let (deadline, deadline_message) = if matches!(kind, Kind::RunSource { .. }) {
			(
				Instant::now() + Duration::from_secs(600),
				"the execution did not settle within 600 s; its late outcome is dropped".to_owned(),
			)
		} else {
			(
				Instant::now() + Duration::from_secs(60),
				"notebook deadline: outcome unknown; do not replay automatically".to_owned(),
			)
		};
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
		Ok(Some((
			Ticket {
				rx,
				deadline,
				deadline_message,
			},
			task,
		)))
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
	/// ADR 0169 §3: the execution's own verdict. Superseded, edited under,
	/// reconnected under, or errored is a failure naming the cause; done is
	/// the scheduler idle for it with every retained relation complete.
	fn settle_execution(
		&mut self,
		buffer: BufferId,
		revision: u64,
		plain: Option<Result<(), String>>,
	) {
		let Some(Work {
			kind: Kind::RunSource { description, .. },
			..
		}) = &self.notebook_actions.work
		else {
			return;
		};
		let description = description.clone();
		if self.rune_buffer_revisions.get(&buffer) != Some(&revision) {
			return self.fail_execution(format!(
				"execution {revision} was superseded by a newer run of the source"
			));
		}
		let generation = self
			.code_editor
			.document_snapshot(buffer)
			.map(|(_, generation)| generation.get().to_string());
		if generation.as_deref() != Some(description.generation.as_str()) {
			return self.fail_execution(format!("the source changed during execution {revision}"));
		}
		if self.connection_generation != description.connection_generation {
			return self.fail_execution(format!(
				"the connection changed during execution {revision}: generation {} is now {}",
				description.connection_generation, self.connection_generation
			));
		}
		if !self.status_error.is_empty() {
			let message = self.status_error.clone();
			return self.fail_execution(format!("execution {revision} failed: {message}"));
		}
		if let Some(outcome) = plain {
			match outcome {
				Ok(()) => self.complete_execution(),
				Err(message) => {
					self.fail_execution(format!("execution {revision} failed: {message}"))
				}
			}
		}
		#[cfg(feature = "toron")]
		{
			let retained = self
				.retained_rune_relations
				.get(&buffer)
				.is_some_and(|retained| {
					retained.revision == revision && retained.artifacts.iter().all(Option::is_some)
				});
			if retained
				&& self.rune_view_scheduler.is_idle()
				&& self.code_editor.host_panels_running() == 0
			{
				self.complete_execution();
			}
		}
	}
	/// Settle a `run_source` whose local batch has committed, once every one
	/// of its panes is exported: a receipt then describes all its figures.
	pub(super) fn settle_local_batch(&mut self) {
		let Some(Work {
			kind: Kind::RunSource {
				buffer, revision, ..
			},
			..
		}) = &self.notebook_actions.work
		else {
			return;
		};
		let (buffer, revision) = (*buffer, *revision);
		let Some((committed, figures)) = self.local_rune_commits.get(&buffer) else {
			return;
		};
		if *committed != revision || figures.is_empty() {
			return;
		}
		let exported = self
			.code_editor
			.host_plot_exports_with_identity()
			.into_iter()
			.map(|(panel, incarnation, _)| (panel, incarnation.token()))
			.collect::<std::collections::HashSet<_>>();
		let visible = figures.iter().all(|figure| exported.contains(figure));
		if visible {
			self.settle_execution(buffer, revision, Some(Ok(())));
		}
	}
	fn complete_execution(&mut self) {
		if let Some(Work {
			kind: Kind::RunSource { description, .. },
			..
		}) = &self.notebook_actions.work
		{
			let description = description.clone();
			self.complete_notebook(Completion::Execution {
				description,
				outcome: Ok(()),
			});
		}
	}
	fn fail_execution(&mut self, message: String) {
		if let Some(Work {
			kind: Kind::RunSource { description, .. },
			..
		}) = &self.notebook_actions.work
		{
			let description = description.clone();
			self.complete_notebook(Completion::Execution {
				description,
				outcome: Err(message),
			});
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
					result: None,
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
					result: None,
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
					result: None,
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
			Kind::RunSource { buffer, .. } => {
				let id = work.id;
				let buffer = *buffer;
				self.status_error.clear();
				let Some((code, generation)) = self
					.code_editor
					.document_snapshot(buffer)
					.map(|(text, generation)| (text.to_owned(), generation))
				else {
					self.fail_execution("the source text is not yet projected".into());
					return Some(Task::none());
				};
				// The run is started for this buffer directly, never through the
				// focused pane: a person's focus elsewhere must not redirect it.
				let task = super::run_rune_code_at_generation(self, buffer, code, generation);
				let started = self
					.rune_buffer_revisions
					.get(&buffer)
					.copied()
					.unwrap_or(0);
				if let Some(Work {
					kind: Kind::RunSource {
						revision,
						description,
						..
					},
					..
				}) = &mut self.notebook_actions.work
				{
					*revision = started;
					description.id = started;
				}
				if !self.status_error.is_empty() {
					let message = self.status_error.clone();
					self.fail_execution(format!("execution {started} failed: {message}"));
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
					result: None,
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
		if let Kind::RunSource {
			buffer, revision, ..
		} = &work.kind
		{
			let (buffer, revision) = (*buffer, *revision);
			// A plain or local Rune buffer settles in one message, or, when it
			// published a plot, once that pane is committed: a receipt must
			// not report an execution whose figure does not exist yet.
			let plain = match &message {
				Message::RunRuneBufferResult {
					source_buffer,
					revision: settled,
					result,
				} if *source_buffer == buffer && *settled == revision => match result {
					Ok(outcome) if outcome.plot.is_some() => None,
					// A local batch settles once its panes are on the surface.
					Ok(outcome) if outcome.panes.as_ref().is_some_and(|p| !p.is_empty()) => None,
					result => Some(result.as_ref().map(|_| ()).map_err(Clone::clone)),
				},
				Message::RuneBufferPlotReady {
					source_buffer,
					revision: settled,
					..
				} if *source_buffer == buffer && *settled == revision => Some(Ok(())),
				_ => None,
			};
			let task = update_inner(self, message);
			self.settle_execution(buffer, revision, plain);
			return task.map(move |message| Message::NotebookTask {
				id,
				message: Box::new(message),
			});
		}
		let Kind::Run { description, .. } = &work.kind else {
			return Task::none();
		};
		let description = description.clone();
		let before = self.code_editor.host_plot_incarnations();
		let plot_ready = matches!(&message, Message::NotebookPlotReady { .. });
		let mut executed: Option<std::sync::Arc<crate::result_artifact::ResultArtifact>> = None;
		let mut terminal = match &message {
			Message::RunMarkdownCellResult {
				result, generation, ..
			} => Some(if *generation != self.current_sql_generation() {
				Err("cell execution was superseded".into())
			} else {
				match result {
					crate::adapters::common::ExecutionResult::Err(e) => Err(e.clone()),
					crate::adapters::common::ExecutionResult::Rows { artifact, .. } => {
						executed = Some(std::sync::Arc::clone(artifact));
						Ok(())
					}
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
					// ADR 0168 §1: the artifact this run bound, never the cell's
					// retained one, so an earlier answer is never reported.
					result: executed
						.as_deref()
						.and_then(crate::result_summary::summarize),
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
		fn describe_receipt(&mut self) -> CommandReceipt {
			self.app
				.apply_command(&Command::DescribeNotebook { buffer: None })
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
		#[cfg(toron_local)]
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

	/// ADR 0168 §1 and §3: a SQL cell's receipt carries this run's answer with
	/// nullness as data, an empty relation keeps its columns, and every other
	/// receipt carries no answer.
	#[test]
	fn adr0168_a_sql_cells_receipt_carries_its_answer() {
		let mut f = Fixture::with_source(
			"# 0168\n\n```sql {#q, output=none}\nSELECT 1 AS x, NULL AS n, 'null' AS s\n```\n",
		);
		let receipt = f.run(0);
		assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
		let result = receipt.result.expect("a SQL cell's answer");
		assert_eq!(
			result
				.columns
				.iter()
				.map(|c| c.name.as_str())
				.collect::<Vec<_>>(),
			["x", "n", "s"]
		);
		assert!(result.columns.iter().all(|c| !c.dtype.is_empty()));
		assert_eq!(result.rows, 1);
		assert_eq!(
			result.preview,
			vec![vec![Some("1".to_owned()), None, Some("null".to_owned())]]
		);
		assert!(!result.truncated && result.truncation_reasons.is_empty());
		assert!(f.describe_receipt().result.is_none());

		let receipt = f.replace(
			0,
			"SELECT 1 AS x WHERE 0
",
		);
		assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
		let result = f.run(0).result.expect("an empty relation is an answer");
		assert_eq!(result.columns.len(), 1);
		assert_eq!(result.rows, 0);
		assert!(result.preview.is_empty() && !result.truncated);

		let receipt = f.replace(
			0,
			"WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 1000) SELECT n FROM t
",
		);
		assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
		let result = f.run(0).result.expect("a long relation is an answer");
		assert_eq!(result.rows, 1000);
		assert_eq!(result.preview.len(), crate::result_summary::PREVIEW_ROWS);
		assert_eq!(
			result.truncation_reasons,
			vec![crate::driver::TruncationReason::Rows]
		);
	}

	/// ADR 0168 §1: the answer is this execution's. After a success, a
	/// statement and then a failure in the same cell report no rows.
	#[test]
	fn adr0168_an_earlier_answer_is_never_reported() {
		let mut f =
			Fixture::with_source("# 0168\n\n```sql {#q, output=none}\nSELECT 7 AS seven\n```\n");
		assert_eq!(
			f.run(0).result.unwrap().preview,
			vec![vec![Some("7".to_owned())]]
		);
		f.replace(
			0,
			"CREATE TABLE t0168 (a INTEGER)
",
		);
		let statement = f.run(0);
		assert_eq!(statement.outcome, Outcome::Applied, "{statement:?}");
		assert!(statement.result.is_none(), "a statement has no relation");
		f.replace(
			0,
			"SELECT * FROM no_such_table_0168
",
		);
		let failed = f.run(0);
		assert_eq!(failed.outcome, Outcome::Failed, "{failed:?}");
		assert!(failed.result.is_none(), "a failure reports no earlier rows");
	}

	/// ADR 0168 §2: a wide answer stays under the live line limit, and the
	/// event that carries it encodes within bounds.
	#[test]
	fn adr0168_a_wide_answer_keeps_the_live_event_under_its_line_limit() {
		let columns: Vec<String> = (0..300)
			.map(|i| format!("'{}' AS wide_column_{i}", "x".repeat(60)))
			.collect();
		let source = format!(
			"# 0168\n\n```sql {{#q, output=none}}\nSELECT\n{}\n```\n",
			columns.join(",\n")
		);
		let mut f = Fixture::with_source(&source);
		let receipt = f.run(0);
		assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
		let result = receipt.result.clone().expect("answer");
		assert_eq!(result.columns.len(), 300);
		assert!(
			result
				.truncation_reasons
				.contains(&crate::driver::TruncationReason::Bytes),
			"{:?}",
			result.truncation_reasons
		);
		let event = crate::live::Event::Completed { id: 1, receipt };
		let line = crate::live::encode(&event).unwrap();
		assert!(line.len() < crate::live::MAX_LINE, "{} bytes", line.len());
	}

	/// Regression probe for a refusal seen once on Linux on 2026-09-12: one SQL
	/// cell on a single ~22 KB line made every guarded action on the buffer
	/// answer `MutationFailed`, then opaque. It did not recur once the cause
	/// was surfaced in the message; the probe stays so a recurrence names it.
	#[test]
	fn probe_a_single_long_line_cell_is_still_editable() {
		let columns: Vec<String> = (0..300)
			.map(|i| format!("'{}' AS wide_column_{i}", "x".repeat(60)))
			.collect();
		let source = format!(
			"# probe\n\n```sql {{#q, output=none}}\nSELECT {}\n```\n",
			columns.join(", ")
		);
		assert!(source.len() > 20 * 1024);
		let mut f = Fixture::with_source(&source);
		let described = f.describe_receipt();
		assert_eq!(described.outcome, Outcome::Applied, "{described:?}");
		let ran = f.run(0);
		assert_eq!(ran.outcome, Outcome::Applied, "{ran:?}");
		assert_eq!(ran.result.map(|r| r.columns.len()), Some(300));
	}

	/// ADR 0169 fixture: a Rune source (not a notebook) open in a headless
	/// window, with the scratch adapter installed as the "Toron" connection
	/// so admission passes. `let x = 1;` is a valid source that yields no
	/// views, so its execution completes with no figures.
	#[cfg(feature = "toron")]
	fn rune_fixture() -> Fixture {
		rune_fixture_with("let x = 1;\n")
	}

	#[cfg(feature = "toron")]
	fn rune_fixture_with(source: &str) -> Fixture {
		let mut f = Fixture::open_source(source);
		let _ = std::fs::rename(f.root.join("notebook.md"), f.root.join("source.rn"));
		f = Fixture::open_rune(&f.root);
		let scratch = f.app.state().adapter_state.scratch.clone().unwrap();
		let state = f.app.state_mut();
		state
			.adapter_state
			.select(crate::adapters::driver::AdapterSelection::Toron);
		state.install_connection(scratch, crate::adapters::common::CancelCapability::Refuse);
		f
	}

	#[cfg(feature = "toron")]
	impl Fixture {
		fn open_rune(root: &std::path::Path) -> Self {
			let source = root.join("source.rn");
			let mut app = HeadlessApplication::open_target(
				super::super::StartupTarget::Ephemeral {
					initial_file: Some(source.display().to_string()),
				},
				crate::configuration::StartupData::default(),
			)
			.unwrap();
			// run_source reads the projected text, so wait for it as well.
			app.settle_until(Duration::from_secs(10), |s| {
				s.code_editor
					.active_language_id()
					.is_some_and(|l| l.as_str() == "rune")
					&& s.adapter_state.scratch.is_some()
					&& s.code_editor
						.active_buffer_id()
						.is_some_and(|buffer| s.code_editor.document_snapshot(buffer).is_some())
			})
			.unwrap();
			Self {
				root: root.to_path_buf(),
				app,
			}
		}
	}

	/// ADR 0169 acceptance 2: no Rune connection is refused before any
	/// execution; a notebook is refused toward `run_cell`.
	#[cfg(feature = "toron")]
	#[test]
	fn adr0169_run_source_is_refused_without_a_rune_connection_or_on_a_notebook() {
		let mut f = Fixture::new();
		let receipt = f.app.apply_command(&Command::RunSource {});
		assert_eq!(receipt.outcome, Outcome::Refused, "{receipt:?}");
		assert!(format!("{receipt:?}").contains("notebook"));

		let mut f = Fixture::open_source("let x = 1;\n");
		let _ = std::fs::rename(f.root.join("notebook.md"), f.root.join("source.rn"));
		f = Fixture::open_rune(&f.root);
		let receipt = f.app.apply_command(&Command::RunSource {});
		assert_eq!(receipt.outcome, Outcome::Refused, "{receipt:?}");
		assert!(
			format!("{receipt:?}").contains("no Rune connection"),
			"{receipt:?}"
		);
		assert!(receipt.execution.is_none() && receipt.figures.is_none());
	}

	/// ADR 0169 acceptance 2: a source that yields nothing completes as an
	/// execution with no figures; a source that does not compile reports its
	/// error, its identity, and no figures.
	#[cfg(feature = "toron")]
	#[test]
	fn adr0169_a_failed_execution_reports_its_error_and_no_figures() {
		let mut quiet = rune_fixture();
		let receipt = quiet.app.apply_command(&Command::RunSource {});
		assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
		assert_eq!(receipt.execution.as_ref().map(|e| e.id), Some(1));
		assert_eq!(receipt.figures.as_deref(), Some(&[][..]));

		let mut f = rune_fixture_with("let x = ;\n");
		let receipt = f.app.apply_command(&Command::RunSource {});
		assert_eq!(receipt.outcome, Outcome::Failed, "{receipt:?}");
		let execution = receipt.execution.clone().expect("execution identity");
		assert!(execution.id >= 1);
		assert_eq!(
			execution.connection_generation,
			f.app.state().connection_generation
		);
		assert!(receipt.figures.is_none(), "{receipt:?}");
		let message = format!("{:?}", receipt.failure);
		assert!(
			message.contains(&format!("execution {}", execution.id)),
			"{message}"
		);
	}

	/// ADR 0169 §3: an execution is settled only on its own terms. A newer
	/// run, a source edit, or a connection change makes it stale, naming the
	/// cause; an overlapping request names the execution in flight; a late
	/// settle of a stale execution changes nothing.
	#[cfg(feature = "toron")]
	#[test]
	fn adr0169_stale_executions_are_named_and_never_report_figures() {
		for case in ["superseded", "source", "connection"] {
			let mut f = rune_fixture();
			let (ticket, task) = f
				.app
				.state_mut()
				.start_notebook_command(&Command::RunSource {})
				.unwrap()
				.unwrap();
			drop(task);
			// Overlap while the request is in flight names it.
			let overlap = match f
				.app
				.state_mut()
				.start_notebook_command(&Command::RunSource {})
			{
				Err(refusal) => refusal,
				Ok(_) => panic!("an overlapping run_source must be refused"),
			};
			assert!(
				format!("{overlap:?}").contains("an execution is in flight"),
				"{overlap:?}"
			);
			let buffer = f.app.state().code_editor.active_buffer_id().unwrap();
			let revision = 1;
			let state = f.app.state_mut();
			match case {
				"superseded" => {
					state.rune_buffer_revisions.insert(buffer, revision + 1);
				}
				"source" => {
					// The recorded generation no longer matches the document's.
					if let Some(Work {
						kind: Kind::RunSource { description, .. },
						..
					}) = &mut state.notebook_actions.work
					{
						description.generation = "stale".into();
					}
					state.rune_buffer_revisions.insert(buffer, revision);
				}
				_ => {
					state.rune_buffer_revisions.insert(buffer, revision);
					state.next_connection_generation();
				}
			}
			if let Some(Work {
				kind: Kind::RunSource { revision: r, .. },
				..
			}) = &mut state.notebook_actions.work
			{
				*r = revision;
			}
			state.settle_execution(buffer, revision, None);
			let Some(Completion::Execution { outcome, .. }) = ticket.result() else {
				panic!("{case}: the execution must have settled");
			};
			let message = outcome.unwrap_err();
			let expected = match case {
				"superseded" => "superseded by a newer run",
				"source" => "the source changed during execution",
				_ => "the connection changed during execution",
			};
			assert!(message.contains(expected), "{case}: {message}");
			// A late settle finds no work and changes nothing.
			assert!(f.app.state().notebook_actions.work.is_none());
			f.app
				.state_mut()
				.settle_execution(buffer, revision, Some(Ok(())));
			assert!(f.app.state().notebook_actions.work.is_none());
			let later = f
				.app
				.apply_command(&Command::DescribeNotebook { buffer: None });
			assert!(later.execution.is_none());
		}
	}

	/// ADR 0169 §3: a run past its deadline reports the timeout and the
	/// dropping of its late outcome.
	#[test]
	fn adr0169_a_timed_out_execution_says_its_late_outcome_is_dropped() {
		let (_tx, rx) = watch::channel(None);
		let ticket = Ticket {
			rx,
			deadline: Instant::now() - Duration::from_millis(1),
			deadline_message:
				"the execution did not settle within 600 s; its late outcome is dropped".into(),
		};
		let Some(Completion::Failed(message)) = ticket.result() else {
			panic!("deadline");
		};
		assert!(message.contains("late outcome is dropped"));
	}
}
