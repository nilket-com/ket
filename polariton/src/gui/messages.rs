use super::menu;
use crate::adapters::{
	common::{DatabaseAdapter, ExecutionResult},
	driver::AdapterSelection,
};
use crate::editor::EditorMsg;
use crate::gui::dashboard::DashboardMsg;
use crate::persistence::{PrivateDb, SavedConnection, SavedStatement};
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
	CloseSaveStatementDialog,
	CloseSettings,
	CloseWindow,
	CodeEditEvent(EditorMsg),
	CodeEditPoll,
	CodeEditResize(iced::Size),
	Connect,
	ConnectionNameChanged(String),
	ConnectionSaved(Vec<SavedConnection>),
	DeleteConnection(i64),
	DeleteStatement(i64),
	DoCloseWindow,
	DragWindow,
	EditConnection(i64),
	EditStatement(i64),
	Export(ExportFormat),
	ExportDone(usize, ExportFormat),
	ExportWithWindowSize(ExportFormat, Option<iced::Size>),
	LoadSavedConnection(i64),
	LoadSavedStatement(i64),
	MaximizeWindow,
	Menu(menu::MenuMessage),
	MinimizeWindow,
	OpenSaveStatementDialog,
	OpenSettings,
	PasswordDecryptFailed,
	PasswordEntryChanged(String),
	PasswordEntrySubmit,
	PrivateDbError(String),
	PrivateDbReady(PrivateDb),
	PrivateDbRekeyed(PrivateDb),
	ResizeWindow(window::Direction),
	Run,
	RunSnippet,
	RunResult(ExecutionResult),
	RunMarkdownCellResult {
		identity: libaryon::uast::projection::MarkdownCellIdentity,
		result: ExecutionResult,
	},
	SaveConnection,
	SaveStatement,
	SaveStatementNameChanged(String),
	SaveWindowSizeAndClose(iced::Size),
	SavedConnectionsLoaded(Vec<SavedConnection>),
	SavedStatementsLoaded(Vec<SavedStatement>),
	SettingsApplyPassword,
	SettingsConfirmPasswordChanged(String),
	SettingsNewPasswordChanged(String),
	SettingsPasswordSaved,
	SettingsRemovePassword,
	ShowColumnTypesSaved,
	StatementSaved(Vec<SavedStatement>),
	ToggleShowColumnTypes(bool),
}

pub use crate::gui::plot_pane::PlotPaneMsg as PlotMessage;
