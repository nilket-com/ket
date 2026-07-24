use super::menu;
use crate::adapters::{
	common::{DatabaseAdapter, ExecutionResult},
	driver::AdapterSelection,
};
use crate::editor::EditorMsg;
use crate::gui::dashboard::DashboardMsg;
use crate::plot::common::PlotKernel;
use crate::plot::request::ResolvedPlot;
use crate::result_registry::PendingPublication;
use iced::window;
use libaryon::core::BufferId;
use std::sync::Arc;
use tokio::sync::RwLock;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
	SVG,
	PNG,
	AVIF,
}

impl std::fmt::Display for ExportFormat {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ExportFormat::SVG => write!(f, "SVG"),
			ExportFormat::PNG => write!(f, "PNG"),
			ExportFormat::AVIF => write!(f, "AVIF"),
		}
	}
}

#[derive(Clone)]
pub enum Message {
	AdapterConfigurationChanged(String, String),
	AdapterConfigurationSubmitted,
	AdapterConnected(Option<Arc<RwLock<dyn DatabaseAdapter>>>),
	/// The session's in-memory scratch database finished opening at startup.
	ScratchReady(Option<Arc<RwLock<dyn DatabaseAdapter>>>),
	AdapterSelected(AdapterSelection),
	Dashboard(DashboardMsg),
	CloseSettings,
	CloseWindow,
	CodeEditEvent(EditorMsg),
	CodeEditResize(iced::Size),
	Connect,
	ConnectionNameChanged(String),
	DoCloseWindow,
	DragWindow,
	Export(ExportFormat),
	ExportDone(usize, ExportFormat),
	ExportWithWindowSize(ExportFormat, Option<iced::Size>),
	MaximizeWindow,
	Menu(menu::MenuMessage),
	MinimizeWindow,
	OpenSettings,
	ResizeWindow(window::Direction),
	Run,
	RunSnippet,
	RunResult {
		result: ExecutionResult,
		/// A `-- @result name` claim made at dispatch, redeemed against the
		/// session registry when the execution concludes.
		publish: Option<PendingPublication>,
	},
	RunMarkdownCellResult {
		identity: libaryon::uast::projection::MarkdownCellIdentity,
		presentation: libaryon::uast::projection::MarkdownCellPresentation,
		/// One-based document line containing the first byte of the fenced
		/// body. Adapter diagnostics are cell-relative; the host maps them back
		/// through this origin before presenting them.
		source_start_line: usize,
		result: ExecutionResult,
	},
	/// A plain Rune buffer finished: its trailing-expression display plus at
	/// most one planned plot, per the one-buffer-one-plot-pane rule.
	RunRuneBufferResult {
		source_buffer: BufferId,
		revision: u64,
		result: std::result::Result<RuneBufferOutcome, String>,
	},
	RuneBufferPlotReady {
		source_buffer: BufferId,
		revision: u64,
		resolved: ResolvedPlot,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	},
	RunRuneCellResult {
		identity: libaryon::uast::projection::MarkdownCellIdentity,
		presentation: libaryon::uast::projection::MarkdownCellPresentation,
		result: std::result::Result<ResolvedPlot, String>,
	},
	NotebookPlotReady {
		identity: libaryon::uast::projection::MarkdownCellIdentity,
		presentation: libaryon::uast::projection::MarkdownCellPresentation,
		resolved: ResolvedPlot,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	},
	SaveWindowSizeAndClose(iced::Size),
	ShowColumnTypesSaved,
	CloseStatusDetail,
	CopyStatusDetail,
	StatusDetailAction(iced::widget::text_editor::Action),
	ToggleStatusDetail,
	ToggleShowColumnTypes(bool),
}

/// What a plain Rune buffer run delivers back to the update loop.
#[derive(Clone)]
pub struct RuneBufferOutcome {
	pub display: Option<String>,
	pub plot: Option<ResolvedPlot>,
}

pub use crate::gui::plot_pane::PlotPaneMsg as PlotMessage;
