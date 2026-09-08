//! PRODUCT-0158 §2 and §6: the application loaded without a window.
//!
//! A headless application is the same `AppState` the window drives, built by
//! the same startup, fed the same messages, and advanced by pumping the same
//! tasks its `update` returns. Nothing here interprets a source, builds a
//! kernel, or assembles a package: it opens the application, runs the focused
//! buffer as the Run action does, dispatches pane messages as the legend
//! does, and publishes as the menu does. The driver on top of it is a client
//! of this surface exactly as the window is.

use super::{AppState, Message, StartupTarget, new_with_discovery, resolve_startup, update};
use crate::configuration::StartupData;
use crate::editor::{EditorMsg, HostPlotExport};
use iced::Task;
use iced::futures::StreamExt;
use iced_runtime::Action;
use libaryon::engine::HostPanelId;
use std::time::{Duration, Instant};

/// How long the engine and the tasks may stay quiet before a settle reports
/// the application has stopped changing.
const QUIET_PERIOD: Duration = Duration::from_millis(250);

pub(crate) struct HeadlessApplication {
	state: AppState,
	runtime: tokio::runtime::Runtime,
	/// Where every running task sends its output messages, and where the
	/// application reads them back in arrival order.
	outputs: tokio::sync::mpsc::UnboundedSender<Message>,
	inbox: tokio::sync::mpsc::UnboundedReceiver<Message>,
	/// The tasks still running on the runtime.
	in_flight: Vec<tokio::task::JoinHandle<()>>,
}

impl HeadlessApplication {
	pub(crate) fn describe_surface(&self) -> Vec<crate::driver::FigureDescription> {
		self.state.describe_surface()
	}

	/// Open the application on any startup target the window accepts - an
	/// ephemeral file or a saved session - through the same resolver.
	pub(crate) fn open_target(target: StartupTarget, startup: StartupData) -> Result<Self, String> {
		let authority = resolve_startup(target)?;
		// The window's runtime is implicit for everything `update` spawns; here
		// the application owns one and enters it around every call it makes.
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.map_err(|error| format!("cannot start the application runtime: {error}"))?;
		let (state, task) = {
			let _context = runtime.enter();
			new_with_discovery(startup, authority, false)?
		};
		let (outputs, inbox) = tokio::sync::mpsc::unbounded_channel();
		let mut application = Self {
			state,
			runtime,
			outputs,
			inbox,
			in_flight: Vec::new(),
		};
		application.spawn(task);
		application.drain();
		Ok(application)
	}

	/// PRODUCT-0158 §3: resize one host pane through the engine's own divider
	/// drag and wait for the projection to carry the new size.
	#[cfg(test)]
	pub(crate) fn resize_pane(
		&mut self,
		panel_id: HostPanelId,
		width: f32,
		height: f32,
		_deadline: Duration,
	) -> Result<(), String> {
		let token = self
			.exports_with_identity()
			.iter()
			.find(|(id, _, _)| *id == panel_id)
			.map(|(_, incarnation, _)| incarnation.token().to_string())
			.ok_or("pane is not projected")?;
		let receipt = self.apply_command(&crate::driver::Command::Resize {
			figure: crate::driver::FigureRef::Incarnation(token),
			width,
			height,
		});
		match receipt.refusal {
			Some(refusal) => Err(format!("{refusal:?}")),
			None => Ok(()),
		}
	}

	pub(crate) fn apply_command(
		&mut self,
		command: &crate::driver::Command,
	) -> crate::driver::CommandReceipt {
		use super::actions::{Pending, ResizeResult};
		use crate::driver::{Failure, PublishRefusal, published_receipt};
		let started = {
			let _entered = self.runtime.enter();
			self.state.start_command(command)
		};
		let mut receipt = started.receipt;
		self.spawn(started.task);
		self.state.capture_surface_baselines();
		self.drain();
		match started.pending {
			None => {}
			Some(Pending::Notebook(ticket)) => loop {
				self.tick();
				if let Some(result) = ticket.result() {
					result.apply(&mut receipt);
					receipt.figures = Some(self.state.describe_surface());
					break;
				}
				std::thread::sleep(Duration::from_millis(5));
			},
			Some(Pending::Publish(job)) => {
				let root = job.root.clone();
				match job.run() {
					Ok(package) => receipt.publish = Some(published_receipt(&root, &package)),
					// The job ran; whatever it wrote stays where it is.
					Err(error) => receipt.fail(Failure::Publish {
						reason: PublishRefusal::from(&error),
						root: root.display().to_string(),
					}),
				}
			}
			Some(Pending::Resize(resize)) => loop {
				self.tick();
				match self.state.resize_result(&resize, Instant::now()) {
					ResizeResult::Waiting => std::thread::sleep(Duration::from_millis(10)),
					ResizeResult::Completed(state) => {
						receipt.state = Some(state);
						break;
					}
					ResizeResult::Superseded(current) => {
						receipt.fail(Failure::ResizeSuperseded {
							figure: resize.incarnation.token().to_string(),
							current,
						});
						break;
					}
					ResizeResult::Failed(state) => {
						receipt.state = Some(state);
						receipt.fail(Failure::ResizeDeadline {
							figure: resize.incarnation.token().to_string(),
						});
						break;
					}
				}
			},
		}
		receipt
	}

	/// Apply one message through the application's `update` and pump the
	/// tasks it returns, exactly as the window's runtime would.
	/// Wrap an application a test built with the same startup the window uses,
	/// so the driver can be exercised on host panels the test filled itself.
	#[cfg(test)]
	pub(crate) fn from_state(mut state: AppState) -> Result<Self, String> {
		state.capture_surface_baselines();
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.map_err(|error| format!("cannot start the application runtime: {error}"))?;
		let (outputs, inbox) = tokio::sync::mpsc::unbounded_channel();
		Ok(Self {
			state,
			runtime,
			outputs,
			inbox,
			in_flight: Vec::new(),
		})
	}

	#[cfg(test)]
	pub(crate) fn state(&self) -> &AppState {
		&self.state
	}

	#[cfg(test)]
	pub(crate) fn state_mut(&mut self) -> &mut AppState {
		&mut self.state
	}

	pub(crate) fn dispatch(&mut self, message: Message) {
		let task = {
			let _context = self.runtime.enter();
			update(&mut self.state, message)
		};
		self.spawn(task);
		self.drain();
	}

	/// PRODUCT-0158 §2: run the focused buffer as the Run action does, then
	/// wait until the application has finished: the workspace attached, every
	/// submitted relation completed, and the host panels stopped changing.
	#[cfg(feature = "toron")]
	pub(crate) fn run_source(&mut self, deadline: Duration) -> Result<(), String> {
		let started = Instant::now();
		self.settle_until(deadline, |s| s.code_editor.active_language_id().is_some())?;
		if self
			.state
			.code_editor
			.active_language_id()
			.is_some_and(|l| l.as_str() == "markdown")
		{
			return self.settle_until(deadline, |s| {
				s.code_editor.notebook_description(None).is_ok()
			});
		}
		self.spawn(Task::perform(
			nilket_local::discover(None),
			Message::LocalWorkspaceDiscovered,
		));
		self.settle_until(deadline, |state| {
			state.adapter_state.rune_connection().is_some()
		})
		.map_err(|_| {
			format!(
				"no local workspace attached within {:?}: {}",
				deadline,
				self.state.adapter_state.local_workspace_unavailable()
			)
		})?;
		self.dispatch(Message::Run);
		let remaining = deadline.saturating_sub(started.elapsed());
		// Done when every relation the run retained has produced its artifact
		// and the scheduler has nothing waiting, or the run reported an error.
		// The exports follow the artifacts by an engine projection, which the
		// quiet period after this covers.
		self.settle_quiet(remaining, |state| {
			!state.status_error.is_empty()
				|| state.rune_view_scheduler.is_idle()
					&& state.code_editor.host_panels_running() == 0
					&& !state.retained_rune_relations.is_empty()
					&& state
						.retained_rune_relations
						.values()
						.all(|retained| retained.artifacts.iter().all(Option::is_some))
		})?;
		if !self.state.status_error.is_empty() {
			return Err(format!(
				"the run reported an error: {}",
				self.state.status_error
			));
		}
		Ok(())
	}

	/// Without the `toron` feature there is no workspace to run a source in.
	#[cfg(not(feature = "toron"))]
	pub(crate) fn run_source(&mut self, _deadline: Duration) -> Result<(), String> {
		self.settle_until(_deadline, |s| s.code_editor.active_language_id().is_some())?;
		if self
			.state
			.code_editor
			.active_language_id()
			.is_some_and(|l| l.as_str() == "markdown")
		{
			return self.settle_until(_deadline, |s| {
				s.code_editor.notebook_description(None).is_ok()
			});
		}
		Err("running a source requires Polariton's `toron` feature".to_owned())
	}

	/// Advance the engine and the in-flight tasks until `done` holds, or the
	/// deadline passes.
	pub(crate) fn settle_until(
		&mut self,
		deadline: Duration,
		mut done: impl FnMut(&AppState) -> bool,
	) -> Result<(), String> {
		let end = Instant::now() + deadline;
		#[cfg(feature = "toron")]
		let trace = std::env::var_os("POLARITON_DRIVE_TRACE").is_some();
		#[cfg(feature = "toron")]
		let mut last_trace = Instant::now();
		loop {
			self.tick();
			#[cfg(feature = "toron")]
			if trace && last_trace.elapsed() >= Duration::from_secs(1) {
				last_trace = Instant::now();
				eprintln!(
					"POLARITON_DRIVE_TRACE status={:?} error={:?} workspace={} language={:?} exports={} running={} in_flight={} relations={:?}",
					self.state.status_msg,
					self.state.status_error,
					self.state.adapter_state.rune_connection().is_some(),
					self.state.code_editor.active_language_id(),
					self.state.code_editor.host_plot_exports().len(),
					self.state.code_editor.host_panels_running(),
					self.in_flight.len(),
					self.state
						.retained_rune_relations
						.values()
						.map(|retained| {
							(
								retained.artifacts.len(),
								retained.artifacts.iter().filter(|a| a.is_some()).count(),
							)
						})
						.collect::<Vec<_>>(),
				);
			}
			if done(&self.state) {
				return Ok(());
			}
			if Instant::now() >= end {
				return Err(format!(
					"the application did not settle within {deadline:?}"
				));
			}
			std::thread::sleep(Duration::from_millis(10));
		}
	}

	/// Like `settle_until`, then keep ticking until nothing has changed for a
	/// quiet period, so a condition that becomes true mid-flight - the first
	/// export of several - does not end the wait early.
	pub(crate) fn settle_quiet(
		&mut self,
		deadline: Duration,
		done: impl FnMut(&AppState) -> bool,
	) -> Result<(), String> {
		let started = Instant::now();
		self.settle_until(deadline, done)?;
		let mut fingerprint = self.fingerprint();
		let mut quiet_since = Instant::now();
		while quiet_since.elapsed() < QUIET_PERIOD {
			if started.elapsed() >= deadline {
				break;
			}
			std::thread::sleep(Duration::from_millis(10));
			self.tick();
			let next = self.fingerprint();
			if next != fingerprint {
				fingerprint = next;
				quiet_since = Instant::now();
			}
		}
		Ok(())
	}

	pub(crate) fn exports_with_identity(
		&self,
	) -> Vec<(
		HostPanelId,
		crate::selection::SurfaceIncarnation,
		HostPlotExport,
	)> {
		self.state.code_editor.host_plot_exports_with_identity()
	}

	/// One engine wake, as the subscription would deliver it, then every
	/// message the in-flight tasks have produced since.
	fn tick(&mut self) {
		self.dispatch(Message::CodeEditEvent(EditorMsg::EngineReady));
	}

	/// Something that changes when the application does: the exports and
	/// their revisions, the status line, and the pane layout.
	fn fingerprint(&self) -> (Vec<(u64, u64, u32, u32)>, String, String, usize) {
		(
			self.state
				.code_editor
				.host_plot_exports_with_identity()
				.into_iter()
				.map(|(panel, incarnation, export)| {
					(
						panel.get(),
						incarnation.token(),
						export.width as u32,
						export.height as u32,
					)
				})
				.collect(),
			self.state.status_msg.clone(),
			self.state.status_error.clone(),
			self.in_flight.len(),
		)
	}

	/// Run `task` on the application's runtime, as the window's executor
	/// would: its stream is driven on a worker, its output messages are
	/// queued for `update`, and window, widget, clipboard, and system actions
	/// have no window to act on and are dropped. A blocking future inside a
	/// task holds one worker, never the application.
	fn spawn(&mut self, task: Task<Message>) {
		let Some(mut stream) = iced_runtime::task::into_stream(task) else {
			return;
		};
		let outputs = self.outputs.clone();
		self.in_flight.push(self.runtime.spawn(async move {
			while let Some(action) = stream.next().await {
				if let Action::Output(message) = action
					&& outputs.send(message).is_err()
				{
					return;
				}
			}
		}));
	}

	/// Apply every message the tasks have produced, in arrival order, and
	/// run the tasks those updates return.
	fn drain(&mut self) {
		while let Ok(message) = self.inbox.try_recv() {
			let task = {
				let _context = self.runtime.enter();
				update(&mut self.state, message)
			};
			self.spawn(task);
		}
		self.in_flight.retain(|task| !task.is_finished());
	}
}
