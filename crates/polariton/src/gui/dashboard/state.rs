use super::message::DashboardMsg;
use crate::gui::pane_grid::{Action, Axis, PaneId, State as PaneState};
use crate::gui::plot_pane::{PlotPaneMsg, PlotPaneState};
use crate::plot::common::PlotKernel;
use crate::plot::composition::PlotComposition;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct DashboardState {
	pub panes: Option<PaneState>,
	pub content: HashMap<PaneId, PlotPaneState>,
	resize_freeze_until: Option<Instant>,
}

pub enum DashboardEffect {
	None,
	BuildNewPlot {
		composition: PlotComposition,
	},
	RebuildPlot {
		pane: PaneId,
		composition: PlotComposition,
	},
}

impl DashboardState {
	pub fn resize_freeze_until(&self) -> Option<Instant> {
		self.resize_freeze_until
	}

	pub fn update(&mut self, message: DashboardMsg) -> DashboardEffect {
		match message {
			DashboardMsg::Pane(action) => {
				let is_resize = matches!(action, Action::DragMoved(..));
				if let Some(panes) = &mut self.panes {
					panes.apply(action);
					if is_resize {
						for plot_state in self.content.values_mut() {
							plot_state.resize_render_suspended = true;
						}
						self.resize_freeze_until =
							Some(Instant::now() + Duration::from_millis(250));
					}
				}
				DashboardEffect::None
			}
			DashboardMsg::ResizeSettled => {
				let Some(until) = self.resize_freeze_until else {
					return DashboardEffect::None;
				};
				if Instant::now() < until {
					return DashboardEffect::None;
				}
				self.resize_freeze_until = None;
				for plot_state in self.content.values_mut() {
					if plot_state.resize_render_suspended {
						plot_state.resize_render_suspended = false;
						plot_state.render_revision = plot_state.render_revision.wrapping_add(1);
					}
				}
				DashboardEffect::None
			}
			DashboardMsg::AddPlot(plot_type) => DashboardEffect::BuildNewPlot {
				composition: PlotComposition::single(plot_type),
			},
			DashboardMsg::ClosePlot(pane) => {
				if let Some(dashboard) = &mut self.panes {
					if dashboard.panes().len() <= 1 {
						self.panes = None;
						self.content.clear();
					} else {
						dashboard.detach_pane(pane);
						self.content.remove(&pane);
					}
				}
				DashboardEffect::None
			}
			DashboardMsg::PlotKernelReady {
				pane,
				composition,
				kernel,
			} => {
				self.apply_ready_plot(pane, composition, kernel);
				DashboardEffect::None
			}
			DashboardMsg::Plot(pane, plot_message) => {
				let Some(plot_state) = self.content.get_mut(&pane) else {
					return DashboardEffect::None;
				};
				match plot_message.clone() {
					PlotPaneMsg::AddLayer(_) => {
						plot_state.update(plot_message);
						if plot_state.live_updates_enabled {
							DashboardEffect::RebuildPlot {
								pane,
								composition: plot_state.composition.clone(),
							}
						} else {
							DashboardEffect::None
						}
					}
					PlotPaneMsg::RefreshData => DashboardEffect::RebuildPlot {
						pane,
						composition: plot_state.composition.clone(),
					},
					PlotPaneMsg::ChangePlotType(_) => {
						plot_state.update(plot_message);
						if plot_state.live_updates_enabled {
							DashboardEffect::RebuildPlot {
								pane,
								composition: plot_state.composition.clone(),
							}
						} else {
							DashboardEffect::None
						}
					}
					PlotPaneMsg::RemoveLayer(_) => {
						plot_state.update(plot_message);
						if plot_state.live_updates_enabled {
							DashboardEffect::RebuildPlot {
								pane,
								composition: plot_state.composition.clone(),
							}
						} else {
							DashboardEffect::None
						}
					}
					PlotPaneMsg::ApplySettings => {
						let needs_kernel_rebuild =
							plot_state.composition != plot_state.kernel_composition;
						plot_state.update(plot_message);
						if needs_kernel_rebuild {
							DashboardEffect::RebuildPlot {
								pane,
								composition: plot_state.composition.clone(),
							}
						} else {
							DashboardEffect::None
						}
					}
					PlotPaneMsg::ToggleLiveUpdates(true) => {
						let needs_kernel_rebuild =
							plot_state.composition != plot_state.kernel_composition;
						plot_state.update(plot_message);
						if needs_kernel_rebuild {
							DashboardEffect::RebuildPlot {
								pane,
								composition: plot_state.composition.clone(),
							}
						} else {
							DashboardEffect::None
						}
					}
					_ => {
						plot_state.update(plot_message);
						DashboardEffect::None
					}
				}
			}
		}
	}

	fn apply_ready_plot(
		&mut self,
		pane: Option<PaneId>,
		composition: PlotComposition,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	) {
		if let Some(pane) = pane {
			// A rebuild for a pane the user closed mid-flight stays a no-op
			// rather than resurrecting the pane.
			if let Some(plot_state) = self.content.get_mut(&pane) {
				plot_state.set_kernel(composition, kernel);
			}
			return;
		}
		let _ = self.apply_plot_replacing(None, composition, kernel);
	}

	/// Deliver a plot to `pane` when it still exists, otherwise to a new pane.
	/// Returns the pane actually used so callers that own a stable plot
	/// surface — the plain Rune buffer's — can keep replacing in place.
	pub fn apply_plot_replacing(
		&mut self,
		pane: Option<PaneId>,
		composition: PlotComposition,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	) -> PaneId {
		if let Some(pane) = pane
			&& let Some(plot_state) = self.content.get_mut(&pane)
		{
			plot_state.set_kernel(composition, kernel);
			return pane;
		}

		let new_plot = PlotPaneState::with_kernel(composition, kernel, 1200, 1200);
		if let Some(dashboard) = &mut self.panes {
			let first_pane = *dashboard
				.panes()
				.first()
				.expect("dashboard panes must exist when dashboard state exists");
			let (_, new_pane) = dashboard
				.split(Axis::Horizontal, first_pane)
				.expect("dashboard split failed");
			self.content.insert(new_pane, new_plot);
			new_pane
		} else {
			let (dashboard, pane) = PaneState::with_initial_pane();
			self.panes = Some(dashboard);
			self.content.insert(pane, new_plot);
			pane
		}
	}
}
