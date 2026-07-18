use crate::gui::pane_grid::{Action, PaneId};
use crate::gui::plot_pane::PlotPaneMsg;
use crate::plot::common::PlotKernel;
use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use std::sync::Arc;

#[derive(Clone)]
pub enum DashboardMsg {
	Pane(Action),
	ResizeSettled,
	AddPlot(PlotType),
	ClosePlot(PaneId),
	Plot(PaneId, PlotPaneMsg),
	PlotKernelReady {
		pane: Option<PaneId>,
		composition: PlotComposition,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	},
}
