use crate::core::{
	CursorPosition, DocByte, DocDisplayCol, DocLine, PaneId, Rgb, ViewId, ViewportId,
};
use crate::engine::settings::SaveDurability;
use crate::engine::ui_colors::UiColorKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditorMode {
	Normal,
	Insert,
	Command,
	Search,
	/// A masked, non-history prompt owned by the engine for session secrets.
	Password,
	Confirm,
	Visual {
		anchor: DocByte,
		kind: VisualKind,
	},
}

#[derive(Clone)]
pub struct SecretInput(pub(crate) zeroize::Zeroizing<Vec<u8>>);

impl SecretInput {
	pub(crate) fn new(bytes: Vec<u8>) -> Self {
		Self(zeroize::Zeroizing::new(bytes))
	}

	pub(crate) fn into_bytes(self) -> zeroize::Zeroizing<Vec<u8>> {
		self.0
	}
}

impl std::fmt::Debug for SecretInput {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str("SecretInput(REDACTED)")
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisualKind {
	Char,
	Line,
	Block,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmAction {
	Yes,
	No,
	All,
	Quit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveDirection {
	Up,
	Down,
	Left,
	Right,
	NextWord,
	PrevWord,
	NextWordEnd,
	Top,
	Bottom,
	FindChar {
		ch: char,
		forward: bool,
		inclusive: bool,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerticalDirection {
	Up,
	Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WordMotion {
	NextStart,
	PrevStart,
	NextEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitDirection {
	Horizontal,
	Vertical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneFocusDirection {
	Up,
	Down,
	Left,
	Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneResizeDirection {
	Increase,
	Decrease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerCursorMovement {
	Left,
	Right,
	Home,
	End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoSearchMode {
	Fuzzy,
	Literal,
	Regex,
}

impl RepoSearchMode {
	pub const fn label(self) -> &'static str {
		match self {
			Self::Fuzzy => "fuzzy",
			Self::Literal => "literal",
			Self::Regex => "regex",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickerAction {
	Insert(String),
	Backspace,
	Delete,
	MoveCursor(PickerCursorMovement),
	MoveSelection(i32),
	Accept,
	Cancel,
}

/// Where a dragged pane lands relative to its drop target.
///
/// `Center` exchanges the two panes; the four edges detach the source pane
/// and insert it as a sibling of the target in the requested direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneDropEdge {
	Center,
	Left,
	Right,
	Top,
	Bottom,
}

#[derive(Debug, Clone)]
pub enum CommandRange {
	WholeFile,
	CurrentLine,
	SingleLine(DocLine),
	LineRange(DocLine, DocLine),
	Visual {
		start_byte: DocByte,
		end_byte: DocByte,
	},
}

impl EditorCommand {
	/// Whether this acts on the workspace rather than a document.
	///
	/// Baryon keeps workspace control while a host panel holds focus — pane and
	/// tab navigation, splits, quit, prompts, resize — and declines everything
	/// document-shaped, which the frontend receives as
	/// [`HostEvent::UnhandledInput`](crate::engine::HostEvent::UnhandledInput).
	/// Classifying commands rather than keys means user mappings and future
	/// bindings are covered automatically, and no frontend maintains a key
	/// allowlist.
	pub(crate) fn is_workspace_scoped(&self) -> bool {
		matches!(
			self,
			Self::Resize(_, _)
				| Self::FocusPane(_)
				| Self::FocusPaneDirection(_)
				| Self::ActivateDocTab { .. }
				| Self::CloseDocTab { .. }
				| Self::SplitPane(_)
				| Self::NewPane { .. }
				| Self::ClosePane
				| Self::ResizePane(_, _)
				| Self::ResizePaneBoundary { .. }
				| Self::MovePane { .. }
				| Self::DocTabNew
				| Self::DocTabEdit(_)
				| Self::DocTabNext
				| Self::DocTabPrev
				| Self::DocTabClose
				| Self::OpenHostPanel { .. }
				| Self::RefreshHostPanel { .. }
				| Self::OpenFileBrowser
				| Self::OpenFileBrowserAt(_)
				| Self::Quit | Self::WriteAndQuit
				| Self::SetTheme(_)
				| Self::SetBackground(_)
				| Self::MouseIntent { .. }
		)
	}
}

#[derive(Debug, Clone, Default)]
pub struct SubstituteFlags {
	pub global: bool,
	pub confirm: bool,
	pub case_insensitive: bool,
}

#[derive(Debug, Clone)]
/// A frontend-to-engine request.
pub enum EditorCommand {
	/// Open libaryon's masked password prompt. The resulting secret never enters
	/// command history or a frontend-visible projection.
	BeginPasswordPrompt {
		label: String,
	},
	/// Internal result of the masked prompt. Debug output is redacted and the
	/// bytes are moved directly into libaryon's secret session.
	SubmitPassword(SecretInput),
	LockSecrets,
	LoadEncryptedFile(String),
	WriteEncryptedFile,
	WriteEncryptedFileAs(String),
	InsertChar(char),
	InsertText(String),
	/// Compile and evaluate a Rune source as a replaceable source generation.
	SourceRune(String),
	/// Retire one Rune source generation and all of its registrations.
	UnloadRune(String),
	ReplaceChar(char),
	ToggleCaseAtCursor,
	/// Resolve and jump to the symbol definition under the primary cursor.
	GotoDefinition,
	/// Open the engine-owned modal repository-content picker.
	RepoSearch {
		mode: RepoSearchMode,
		query: String,
	},
	/// Edit, navigate, accept, or close the active picker.
	Picker(PickerAction),
	/// Return to the newest retained code-navigation origin.
	JumpBack,
	/// Reapply the newest retained forward navigation after jumping back.
	JumpForward,
	OpenLineBelow,
	OpenLineAbove,
	AppendAtLineEnd,
	Backspace,
	Delete,
	DeleteLine {
		register: char,
	},
	Resize(u16, u16),
	FocusPane(PaneId),
	FocusPaneDirection(PaneFocusDirection),
	ActivateDocTab {
		pane_id: PaneId,
		view_id: ViewId,
	},
	CloseDocTab {
		pane_id: PaneId,
		view_id: ViewId,
	},
	SplitPane(SplitDirection),
	NewPane {
		direction: SplitDirection,
		language: Option<String>,
	},
	ClosePane,
	ResizePane(SplitDirection, PaneResizeDirection),
	ResizePaneBoundary {
		before: PaneId,
		after: PaneId,
		axis: SplitDirection,
		target: u32,
	},
	/// Rearrange two existing workspace panes. The engine owns this topology
	/// mutation so GUI hosts never need a parallel split tree.
	MovePane {
		source: PaneId,
		target: PaneId,
		edge: PaneDropEdge,
	},
	Scroll(i32),
	ScrollViewport(i32),
	ScrollViewportToLine(DocLine),
	ToggleFold,
	CloseFold,
	OpenFold,
	CloseAllFolds,
	OpenAllFolds,
	ToggleWrap,
	SetWrap(bool),
	SetMinimap(bool),
	SetLineNumbers(bool),
	SetRelativeLineNumbers(bool),
	SetVisibleWhitespace(bool),
	PersistWrap(bool),
	PersistMinimap(bool),
	PersistLineNumbers(bool),
	PersistRelativeLineNumbers(bool),
	PersistVisibleWhitespace(bool),
	PersistReload(bool),
	PersistSaveDurability(SaveDurability),
	SetGitRefresh(GitRefreshMode),
	/// Atomically export the current persistent settings as `init.rn`.
	WriteInit {
		force: bool,
	},
	/// Open a host-rendered panel in a split beside the focused pane. The
	/// engine owns its identity, geometry, focus, and tab membership; the
	/// frontend draws its contents and owns its backing state. `focus` makes
	/// focus transfer explicit: query results can appear without interrupting
	/// typing in their source pane.
	OpenHostPanel {
		panel_id: crate::engine::HostPanelId,
		kind: String,
		title: String,
		/// Direction of the initial split when this identity is first opened.
		split_direction: SplitDirection,
		focus: bool,
	},
	/// Rebind a live panel to a new revision of its source, so the frontend can
	/// mark or replace stale output.
	RefreshHostPanel {
		panel_id: crate::engine::HostPanelId,
		revision: crate::engine::PanelRevision,
	},
	/// Replace this editor host's init entrypoint with built-in defaults.
	WriteDefaultInit {
		force: bool,
	},
	MoveCursor(MoveDirection),
	MatchDelimiter,
	ClickCursor(CursorPosition),
	MouseIntent {
		viewport_id: ViewportId,
		intent: MouseIntent,
	},
	AddCursorAdjacent(VerticalDirection),
	AddCursorAt(CursorPosition),
	CollapseCursors,
	GotoLine(DocLine),
	LineStart,
	FirstNonWhitespace,
	LineEnd,
	SmartHome,
	PageUp,
	PageDown,
	DeleteInnerWord,
	ChangeInnerWord,
	DeleteWordMotion {
		register: char,
		motion: WordMotion,
	},
	ChangeWordMotion {
		register: char,
		motion: WordMotion,
	},
	DeleteToLineEnd,
	JoinLine,
	SetVisualSelection {
		anchor: DocByte,
		kind: VisualKind,
	},
	ClearVisualSelection,
	VisualYank {
		anchor: DocByte,
		kind: VisualKind,
	},
	VisualDelete {
		anchor: DocByte,
		kind: VisualKind,
	},
	VisualChange {
		anchor: DocByte,
		kind: VisualKind,
	},
	LoadFile(String),
	/// Atomically replace the focused document with in-memory text.
	///
	/// Clears the file association and resets undo history, selection, search
	/// results, folds, caret, and scroll. `language` is a language id or
	/// extension resolved as in [`EditorCommand::SetLanguage`]; `None` selects
	/// plain text. An unknown language rejects the command, leaving the
	/// current document untouched.
	ReplaceDocument {
		text: String,
		language: Option<String>,
		/// File-path-shaped identity for the in-memory document (e.g.
		/// `statement.sql`). Language services that expect a path (semantic
		/// highlighting, diagnostics) receive it; the document still has no
		/// file: writes refuse and no canonical path is claimed. When `None`
		/// and `language` resolves, an identity is derived from the
		/// provider's first extension (`untitled.<ext>`).
		virtual_path: Option<String>,
	},
	/// Request the focused document's full text, delivered once via
	/// `Viewport::document_text` on the next projection.
	QueryDocumentText,
	ReloadFile {
		force: bool,
	},
	CheckFileReload,
	GitStatus,
	/// Refresh an already-open Git pane in its existing repository.
	///
	/// Frontend timers and filesystem observers use this instead of
	/// [`Self::GitStatus`] so focus changes cannot retarget the pane.
	RefreshGitStatus,
	OpenGitStatusEntry(DocLine),
	DiffSplit(String),
	/// Step the focused pair's snapshot partner through the file's commit
	/// history: negative is older, positive is newer.
	DiffStep(i64),
	MarkdownPreview {
		serif: bool,
	},
	/// Open or retarget the live UAST storage inspector for the focused source.
	OpenUastInspector {
		mode: Option<UastInspectorMode>,
	},
	/// Jump from the selected inspector row to its stamped source range.
	ActivateUastInspectorRow,
	/// Collapse or expand the selected inspector node.
	ToggleUastInspectorFold,
	/// Toggle whether inspector selection follows the paired source cursor.
	ToggleUastInspectorFollowCursor,
	/// Jump from a frontend-rendered graph node to its stamped source range.
	ActivateUastGraphNode {
		inspector_view_id: ViewId,
		selection: crate::uast::UastGraphSelection,
	},
	/// Render the focused source's bounded UAST snapshot as Graphviz DOT.
	ExportUastDot {
		path: Option<String>,
	},
	/// Render an in-memory markdown string (e.g. help pages) in a pinned
	/// preview pane that no source document refreshes or scroll-syncs.
	ShowMarkdownDocument {
		title: String,
		markdown: String,
	},
	WriteFile,
	WriteFileAs(String),
	WriteAndQuit,
	OpenFileBrowser,
	OpenFileBrowserAt(String),
	RefreshFileBrowser,
	ActivateBrowserEntry,
	ClickBrowserEntry {
		line: DocLine,
		double_click: bool,
	},
	CollapseBrowserEntryOrParent,
	DocTabNew,
	DocTabEdit(String),
	DocTabNext,
	DocTabPrev,
	DocTabClose,
	SetTheme(String),
	SetBackground(String),
	PersistTheme(String),
	PersistLangTheme {
		language: String,
		theme: String,
	},
	PersistNoLangTheme(String),
	PersistUiColor {
		key: UiColorKey,
		value: Option<Rgb>,
	},
	QueryUiColor(UiColorKey),
	SearchStart(String),
	SearchNext,
	SearchPrev,
	ClearSearch,
	SubstituteAll {
		pattern: String,
		replacement: String,
		range: CommandRange,
		flags: SubstituteFlags,
	},
	SubstituteConfirm {
		pattern: String,
		replacement: String,
		range: CommandRange,
		flags: SubstituteFlags,
	},
	Sort {
		range: CommandRange,
		reverse: bool,
		unique: bool,
		numeric: bool,
		ignore_case: bool,
	},
	Uniq {
		range: CommandRange,
		ignore_case: bool,
	},
	ConfirmResponse(ConfirmAction),
	YankLine {
		register: char,
	},
	YankWordMotion {
		register: char,
		motion: WordMotion,
	},
	Put {
		register: char,
	},
	Undo,
	Redo,
	SelectInnerWord,
	SelectAroundFunction,
	DeleteAroundFunction {
		register: char,
	},
	ChangeAroundFunction,
	NextFunction,
	SearchWordUnderCursor {
		forward: bool,
	},
	SetLanguage(Option<String>),
	Quit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UastInspectorMode {
	Semantic,
	Raw,
	Graph,
	Index(PositionalIndexKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionalIndexKind {
	Treap,
	SumTree,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GitRefreshMode {
	/// No timer and no filesystem observer.
	Disabled,
	/// Filesystem observation without a regular timer. If native observation
	/// is unavailable, the engine falls back to the default polling interval.
	Watch,
	/// Filesystem observation where available, plus polling at this interval.
	Interval(u64),
}

pub const DEFAULT_GIT_REFRESH_SECONDS: u64 = 7;

impl GitRefreshMode {
	pub fn poll_seconds(self, observation_available: bool) -> Option<u64> {
		match self {
			Self::Interval(seconds) if seconds > 0 => Some(seconds),
			Self::Watch if !observation_available => Some(DEFAULT_GIT_REFRESH_SECONDS),
			Self::Disabled | Self::Watch | Self::Interval(_) => None,
		}
	}

	pub fn watch_enabled(self) -> bool {
		match self {
			Self::Disabled | Self::Interval(0) => false,
			Self::Watch | Self::Interval(_) => true,
		}
	}
}

#[cfg(test)]
mod git_refresh_mode_tests {
	use super::{DEFAULT_GIT_REFRESH_SECONDS, GitRefreshMode};

	#[test]
	fn watch_only_falls_back_to_polling_without_an_observer() {
		assert_eq!(GitRefreshMode::Watch.poll_seconds(true), None);
		assert_eq!(
			GitRefreshMode::Watch.poll_seconds(false),
			Some(DEFAULT_GIT_REFRESH_SECONDS)
		);
		assert_eq!(GitRefreshMode::Disabled.poll_seconds(false), None);
		assert_eq!(GitRefreshMode::Interval(11).poll_seconds(true), Some(11));
	}
}

/// A mouse action interpreted from one rendered viewport.
#[derive(Debug, Clone)]
pub enum MouseIntent {
	FocusPane(PaneId),
	ActivateDocTab { pane_id: PaneId, view_id: ViewId },
	CloseDocTab { pane_id: PaneId, view_id: ViewId },
	ScrollViewport {
		delta: i32,
		synchronize_diff: bool,
	},
	ScrollViewportToLine(DocLine),
	ClickBrowserEntry { line: DocLine, double_click: bool },
	OpenGitStatusEntry { line: DocLine },
	ClickCursor { target: CursorPosition, alt: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchMatch {
	pub(crate) line: DocLine,
	pub(crate) col: DocDisplayCol,
	pub(crate) byte_len: usize,
}
