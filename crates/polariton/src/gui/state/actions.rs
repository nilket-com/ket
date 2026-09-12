//! Shared command resolution and admission. Every mutation uses the GUI path.

use super::{AppState, Message, publish::PublishJob, update_inner};
use crate::driver::{
	Command, CommandReceipt, FigureRef, LiveState, PublishRefusal, Refusal, live_state_of,
	series_table_of,
};
use crate::editor::{EditorMsg, HostPlotExport};
use crate::gui::plot_pane::PlotPaneMsg;
use crate::plot::series::SeriesVisibilityAction;
use crate::publication::widget_for;
use crate::selection::SurfaceIncarnation;
use iced::Task;
use libaryon::engine::HostPanelId;
use std::time::{Duration, Instant};

pub(super) struct Started {
	pub(super) receipt: CommandReceipt,
	pub(super) task: Task<Message>,
	pub(super) pending: Option<Pending>,
}

pub(super) enum Pending {
	Notebook(super::notebook_actions::Ticket),
	Resize(Resize),
	Publish(PublishJob),
}

pub(super) struct Resize {
	pub(super) panel: HostPanelId,
	pub(super) incarnation: SurfaceIncarnation,
	pub(super) before: crate::driver::Size,
	pub(super) deadline: Instant,
}

pub(super) enum ResizeResult {
	Waiting,
	Completed(LiveState),
	Superseded(Option<String>),
	Failed(LiveState),
}

pub(super) struct Resolved {
	panel: HostPanelId,
	incarnation: SurfaceIncarnation,
	export: HostPlotExport,
}

impl AppState {
	pub(super) fn start_command(&mut self, command: &Command) -> Started {
		let mut started = Started {
			receipt: CommandReceipt::new(command),
			task: Task::none(),
			pending: None,
		};
		if let Err(refusal) = self.admit_command(command, &mut started) {
			started.receipt.refuse(refusal);
		}
		started
	}

	fn admit_command(&mut self, command: &Command, started: &mut Started) -> Result<(), Refusal> {
		if let Some((ticket, task)) = self.start_notebook_command(command)? {
			started.pending = Some(Pending::Notebook(ticket));
			started.task = task;
			return Ok(());
		}
		if let Command::Describe {} = command {
			started.receipt.figures = Some(self.describe_surface());
			return Ok(());
		}
		// ADR 0166 §3: the generation gate is the whole of a disconnect's admission.
		if let Command::Disconnect { generation } = command {
			let description = self
				.disconnect_external(*generation)
				.map_err(|message| Refusal::ConnectionRefused { message })?;
			started.receipt.connection = Some(description);
			return Ok(());
		}
		if let Command::ConnectionStatus {} = command {
			started.receipt.connection = Some(self.connection_description());
			return Ok(());
		}
		if let Command::Publish { directory } = command {
			let job = self
				.prepare_publish(std::path::Path::new(directory))
				.map_err(|error| Refusal::PublishRefused {
					reason: PublishRefusal::from(&error),
				})?;
			// The captured incarnations correlate this asynchronous package with
			// the figures it was asked to publish even if a rerun follows.
			started.receipt.figures = Some(self.describe_surface());
			started.pending = Some(Pending::Publish(job));
			return Ok(());
		}
		let figure = match command {
			Command::Hide { figure, .. }
			| Command::Show { figure, .. }
			| Command::Isolate { figure, .. }
			| Command::Restore { figure }
			| Command::Resize { figure, .. } => figure,
			_ => unreachable!(),
		};
		let resolved = self.resolve(figure)?;
		let token = resolved.incarnation.token().to_string();
		match command {
			Command::Resize { width, height, .. } => {
				if !width.is_finite() || !height.is_finite() || *width <= 0.0 || *height <= 0.0 {
					return Err(Refusal::ResizeRefused {
						figure: token,
						message: "size must be finite and positive".into(),
					});
				}
				let moved = self
					.code_editor
					.resize_host_pane(resolved.panel, *width, *height)
					.map_err(|message| Refusal::ResizeRefused {
						figure: token.clone(),
						message,
					})?;
				if moved {
					started.pending = Some(Pending::Resize(Resize {
						panel: resolved.panel,
						incarnation: resolved.incarnation,
						before: live_state_of(&resolved.export).size,
						deadline: Instant::now() + Duration::from_secs(5),
					}));
				}
			}
			_ => {
				let action = match command {
					Command::Restore { .. } => SeriesVisibilityAction::RestoreAll,
					Command::Hide { series, .. }
					| Command::Show { series, .. }
					| Command::Isolate { series, .. } => {
						series_table_of(&resolved.export).map_err(|reason| {
							Refusal::NoSeriesTable {
								figure: token.clone(),
								reason,
							}
						})?;
						let widget = widget_for(&resolved.export, &resolved.export.settings_model);
						let identities = widget.series_registry().identities().to_vec();
						let identity =
							identities.get(*series as usize).cloned().ok_or_else(|| {
								Refusal::UnknownSeries {
									figure: token.clone(),
									series: *series,
									series_count: identities.len() as u32,
								}
							})?;
						match command {
							Command::Hide { .. } => SeriesVisibilityAction::Hide(identity),
							Command::Show { .. } => SeriesVisibilityAction::Show(identity),
							_ => SeriesVisibilityAction::Isolate(identity),
						}
					}
					_ => unreachable!(),
				};
				started.task = update_inner(
					self,
					Message::CodeEditEvent(EditorMsg::HostPlot {
						panel_id: resolved.panel,
						message: PlotPaneMsg::SeriesVisibility(action),
					}),
				);
			}
		}
		started.receipt.figure = Some(token);
		started.receipt.state = Some(self.surface_live_state(resolved.panel));
		Ok(())
	}

	pub(super) fn surface_live_state(&self, panel: HostPanelId) -> LiveState {
		self.code_editor
			.host_plot_exports_with_identity()
			.iter()
			.find(|(id, _, _)| *id == panel)
			.map(|(_, _, export)| live_state_of(export))
			.unwrap_or_default()
	}

	pub(super) fn resize_result(&self, resize: &Resize, now: Instant) -> ResizeResult {
		let exports = self.code_editor.host_plot_exports_with_identity();
		let Some((_, incarnation, export)) = exports.iter().find(|(id, _, _)| *id == resize.panel)
		else {
			return ResizeResult::Superseded(None);
		};
		if *incarnation != resize.incarnation {
			return ResizeResult::Superseded(Some(incarnation.token().to_string()));
		}
		let state = live_state_of(export);
		if state.size != resize.before {
			ResizeResult::Completed(state)
		} else if now >= resize.deadline {
			ResizeResult::Failed(state)
		} else {
			ResizeResult::Waiting
		}
	}
}

impl AppState {
	fn resolve(&self, figure: &FigureRef) -> Result<Resolved, Refusal> {
		let exports = self.code_editor.host_plot_exports_with_identity();
		resolve_figure(exports, &self.surface.known, figure)
	}
}

pub(super) fn resolve_figure(
	mut exports: Vec<(HostPanelId, SurfaceIncarnation, HostPlotExport)>,
	known: &std::collections::HashMap<u64, u64>,
	figure: &FigureRef,
) -> Result<Resolved, Refusal> {
	let position = match figure {
		FigureRef::Incarnation(token) => {
			let parsed: u64 = token.parse().map_err(|_| Refusal::UnknownFigure {
				figure: figure.clone(),
			})?;
			let live = exports
				.iter()
				.position(|(_, incarnation, _)| incarnation.token() == parsed);
			if live.is_none()
				&& let Some(previous_panel) = known.get(&parsed)
				&& let Some((_, current, _)) = exports
					.iter()
					.find(|(panel, _, _)| panel.get() == *previous_panel)
			{
				return Err(Refusal::StaleFigure {
					figure: figure.clone(),
					current: current.token().to_string(),
				});
			}
			live
		}
		FigureRef::View(name) => {
			let matches = exports
				.iter()
				.enumerate()
				.filter(|(_, (_, _, export))| export.view_name.as_deref() == Some(name))
				.collect::<Vec<_>>();
			if matches.len() > 1 {
				return Err(Refusal::AmbiguousFigure {
					figure: figure.clone(),
					candidates: matches
						.iter()
						.map(|(_, (_, incarnation, _))| incarnation.token().to_string())
						.collect(),
				});
			}
			matches.first().map(|(position, _)| *position)
		}
	};
	let Some(position) = position else {
		return Err(Refusal::UnknownFigure {
			figure: figure.clone(),
		});
	};
	let (panel, incarnation, export) = exports.swap_remove(position);
	Ok(Resolved {
		panel,
		incarnation,
		export,
	})
}
