use super::menu;
use crate::adapters::{
	common::{DatabaseAdapter, ExecutionResult},
	driver::AdapterSelection,
};
use crate::editor::EditorMsg;
use crate::gui::dashboard::DashboardMsg;
use crate::notebook_rune::PlotEffect;
use crate::plot::common::PlotKernel;
use iced::window;
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
	RunResult(ExecutionResult),
	RunLanguageResult(std::result::Result<String, String>),
	RunMarkdownCellResult {
		identity: libaryon::uast::projection::MarkdownCellIdentity,
		presentation: libaryon::uast::projection::MarkdownCellPresentation,
		result: ExecutionResult,
	},
	RunRuneCellResult {
		identity: libaryon::uast::projection::MarkdownCellIdentity,
		presentation: libaryon::uast::projection::MarkdownCellPresentation,
		result: std::result::Result<PlotEffect, String>,
	},
	NotebookPlotReady {
		identity: libaryon::uast::projection::MarkdownCellIdentity,
		presentation: libaryon::uast::projection::MarkdownCellPresentation,
		effect: PlotEffect,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	},
	SaveWindowSizeAndClose(iced::Size),
	ShowColumnTypesSaved,
	ToggleShowColumnTypes(bool),
}

pub use crate::gui::plot_pane::PlotPaneMsg as PlotMessage;
