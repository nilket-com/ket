use super::menu;
use crate::adapters::{
	common::{DatabaseAdapter, ExecutionResult},
	driver::AdapterSelection,
};
use crate::editor::EditorMsg;
#[cfg(feature = "toron")]
use crate::gui::table_skin::TableSkin;
use crate::plot::common::PlotKernel;
use crate::plot::request::ResolvedPlot;
#[cfg(feature = "toron")]
use crate::plot::spec::{ColorIdentity, LayerSettings, LayeredPlotSpec, Override};
#[cfg(feature = "toron")]
use crate::plot::theme::PlotSettingsLayer;
#[cfg(feature = "toron")]
use crate::result_artifact::ResultArtifact;
use crate::result_registry::PendingPublication;
#[cfg(feature = "toron")]
use crate::rune_relation_envelope::{
	LocalViewIdentity, RuneEnvelopeError, TransformedRuneView, ViewPresentation,
};
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
	AdapterAdHocChanged(String),
	AdapterConfigurationChanged(String, String),
	AdapterConfigurationSubmitted,
	AdapterConnected(Result<Arc<RwLock<dyn DatabaseAdapter>>, String>),
	#[cfg(feature = "toron")]
	LocalWorkspaceDiscovered(nilket_local::LocalWorkspace),
	#[cfg(feature = "toron")]
	LocalWorkspaceAttached {
		workspace: nilket_local::ReadyWorkspace,
		connection: Result<Arc<RwLock<dyn DatabaseAdapter>>, String>,
	},
	#[cfg(feature = "toron")]
	StartLocalWorkspace,
	#[cfg(feature = "toron")]
	LocalWorkspaceStartupSettled(
		std::result::Result<nilket_local::ReadyWorkspace, (nilket_local::LocalWorkspace, String)>,
	),
	#[cfg(feature = "toron")]
	LocalWorkspaceStopped(String),
	/// The session's in-memory scratch database finished opening at startup.
	ScratchReady(Option<Arc<RwLock<dyn DatabaseAdapter>>>),
	AdapterSelected(AdapterSelection),
	CloseSettings,
	CloseWindow,
	CheckpointSession,
	CopySessionId,
	CloseWithoutCheckpoint,
	CodeEditEvent(EditorMsg),
	CodeEditResize(iced::Size),
	Connect,
	ConnectionNameChanged(String),
	DragWindow,
	Export(ExportFormat),
	ExportDone(usize, ExportFormat),
	MaximizeWindow,
	Menu(menu::MenuMessage),
	MinimizeWindow,
	OpenSettings,
	/// PRODUCT-0134 §8: something needs the profile store. Unlocks (or begins
	/// creating) it lazily; never sent at startup.
	ProfileStoreAccessRequested,
	ProfilePassphraseChanged(String),
	ProfilePassphraseConfirmationChanged(String),
	ProfileUnlockSubmitted,
	ProfileUnlockCancelled,
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
	#[cfg(feature = "toron")]
	RunRunePresentationResult {
		source_buffer: BufferId,
		revision: u64,
		source: String,
		/// `None` means a newer revision won before this blocking task invoked
		/// the local VM; the scheduler still receives the completion.
		result: Option<std::result::Result<TransformedRuneView, RuneEnvelopeError>>,
	},
	#[cfg(feature = "toron")]
	RunRuneViewSetResult {
		source_buffer: BufferId,
		revision: u64,
		origin: String,
		views: Vec<LocalViewIdentity>,
		result: std::result::Result<RuneRelationViewSetOutcome, String>,
	},
	RuneBufferPlotReady {
		source_buffer: BufferId,
		revision: u64,
		panel_id: libaryon::engine::HostPanelId,
		display_label: String,
		resolved: ResolvedPlot,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	},
	#[cfg(feature = "toron")]
	RuneRelationPlotReady {
		source_buffer: BufferId,
		revision: u64,
		panel_id: libaryon::engine::HostPanelId,
		identity: LocalViewIdentity,
		presentation: ViewPresentation,
		authored_settings: PlotSettingsLayer,
		continuation: RuneCategoricalContinuation,
		resolved: ResolvedPlot,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	},
	#[cfg(feature = "toron")]
	RuneRelationLayeredPlotReady {
		source_buffer: BufferId,
		revision: u64,
		panel_id: libaryon::engine::HostPanelId,
		presentation: ViewPresentation,
		authored_settings: PlotSettingsLayer,
		span: std::ops::Range<u32>,
		result: std::result::Result<Arc<dyn PlotKernel + Send + Sync>, String>,
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

#[cfg(feature = "toron")]
#[derive(Clone)]
pub struct RuneRelationViewSetOutcome {
	pub result: Arc<ResultArtifact>,
	pub sinks: Vec<RuneRelationSinkOutcome>,
}

#[cfg(feature = "toron")]
#[derive(Clone)]
pub struct RuneRelationSinkOutcome {
	pub identity: LocalViewIdentity,
	pub presentation: ViewPresentation,
	pub span: std::ops::Range<u32>,
	/// Structural facet intent retained beside the resolved sink output.
	pub facet: Option<String>,
	/// Resolved once from the application default and authored facet setting;
	/// paint, hover, clipping, and extent share this one grid input.
	pub figure_grid: crate::plot::grid::FigureGridLayoutSettings,
	/// Every sink holds the same `Arc` as its containing set. This is the
	/// stamped-result identity proof, not a value-equivalent copy.
	pub source: Arc<ResultArtifact>,
	pub output: std::result::Result<RuneRelationSinkOutput, String>,
}

/// A destination-local continuation proposal. It is head-owned asynchronous
/// state, not database transport or part of the completed plot artifact.
#[cfg(feature = "toron")]
#[derive(Clone)]
pub struct RuneCategoricalContinuation {
	pub palette_len: usize,
	pub identity: ColorIdentity,
}

#[cfg(feature = "toron")]
#[derive(Clone, Debug, PartialEq)]
pub struct RuneTablePresentation {
	pub skin: TableSkin,
	pub authored_show_column_types: Override<bool>,
}

#[cfg(feature = "toron")]
#[derive(Clone)]
pub enum RuneRelationSinkOutput {
	Table(RuneTablePresentation),
	Plot(ResolvedPlot, PlotSettingsLayer, RuneCategoricalContinuation),
	StyledPlot(
		ResolvedPlot,
		LayerSettings,
		PlotSettingsLayer,
		RuneCategoricalContinuation,
	),
	LayeredPlot(LayeredPlotSpec, PlotSettingsLayer),
}

pub use crate::gui::plot_pane::PlotPaneMsg as PlotMessage;
