use crate::core::{
	BufferId, CursorPosition, DocByte, DocDisplayCol, DocLine, PaneId, RequestId, Rgb, StateId,
	ViewId, ViewportId,
};
use crate::engine::settings::SaveDurability;
use crate::engine::ui_colors::UiColorKey;

/// Frontend-selected cell reservation between logical pane siblings.
///
/// This is runtime presentation state, not part of the persisted layout. A
/// terminal head reserves visible cells; a graphical head may tile panes and
/// draw its own pixel-valued chrome inside their bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaneDividerReservation {
	pub columns: u32,
	pub rows: u32,
}

impl PaneDividerReservation {
	pub const TERMINAL: Self = Self::new(
		crate::core::SPLIT_DIVIDER_COLS,
		crate::core::SPLIT_DIVIDER_ROWS,
	);
	pub const NONE: Self = Self::new(0, 0);

	pub const fn new(columns: u32, rows: u32) -> Self {
		Self { columns, rows }
	}

	pub(crate) const fn cells(self, direction: SplitDirection) -> u32 {
		match direction {
			SplitDirection::Vertical => self.columns,
			SplitDirection::Horizontal => self.rows,
		}
	}
}

impl Default for PaneDividerReservation {
	fn default() -> Self {
		Self::TERMINAL
	}
}

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

/// Advisory initial placement for a host-rendered panel. The engine remains
/// the sole authority over workspace geometry and tab membership and may fall
/// back to its own placement policy when a request cannot be satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPanelPlacement {
	Split {
		direction: SplitDirection,
	},
	TabWith {
		panel_id: crate::engine::HostPanelId,
	},
}

/// Generic natural-width request for a member of a host-panel group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPanelGroupShape {
	Compact,
	Expansive,
}

/// Engine-owned arrangement requested for a complete host-panel group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPanelGroupLayout {
	RowMajorTile,
}

/// One current member in a complete host-panel group request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPanelGroupMember {
	pub panel_id: crate::engine::HostPanelId,
	pub kind: String,
	pub title: String,
	pub shape: HostPanelGroupShape,
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
				| Self::EnsureHostPanelGroup { .. }
				| Self::ResetHostPanelGroup { .. }
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

/// One UTF-8 source replacement expressed in byte coordinates against the
/// document generation named by [`EditorCommand::ApplyDocumentEdits`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentTextEdit {
	pub start: DocByte,
	pub end: DocByte,
	pub replacement: String,
}

/// Why an atomic document-edit request was not applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentEditRefusal {
	BufferUnavailable,
	GenerationMismatch { actual_generation: StateId },
	InvalidRange,
	OverlappingEdits,
	DocumentIsNotUtf8,
	MutationFailed(String),
}

impl std::fmt::Display for DocumentEditRefusal {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Self::MutationFailed(reason) = self {
			// The inner cause is the only thing that distinguishes one mutation
			// failure from another; an opaque message has cost real diagnosis.
			return write!(
				formatter,
				"the editor could not apply the source edit: {reason}"
			);
		}
		let message = match self {
			Self::BufferUnavailable => "source buffer is unavailable",
			Self::GenerationMismatch { .. } => "source changed before the edit could be applied",
			Self::InvalidRange => "the planned source range is no longer valid",
			Self::OverlappingEdits => "the planned source edits overlap",
			Self::DocumentIsNotUtf8 => "source document is not valid UTF-8",
			Self::MutationFailed(_) => unreachable!("handled above"),
		};
		formatter.write_str(message)
	}
}

/// Exact outcome for one correlated document-edit request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentEditOutcome {
	Applied { new_generation: StateId },
	Refused { reason: DocumentEditRefusal },
}

/// Non-coalescing acknowledgement for [`EditorCommand::ApplyDocumentEdits`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentEditResult {
	pub request_id: RequestId,
	pub buffer_id: BufferId,
	pub outcome: DocumentEditOutcome,
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
	/// Apply one atomic, generation-guarded transaction to a live text buffer.
	///
	/// Coordinates are measured against `expected_generation`; libaryon never
	/// rebases them across document generations. The request may target a
	/// non-focused buffer and produces exactly one [`DocumentEditResult`] on
	/// the ordered host-event stream.
	ApplyDocumentEdits {
		request_id: RequestId,
		buffer_id: BufferId,
		expected_generation: StateId,
		edits: Vec<DocumentTextEdit>,
	},
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
	/// Open a host-rendered panel with an advisory initial placement. The engine
	/// owns its identity, geometry, focus, and tab membership; the frontend
	/// draws its contents and owns its backing state. `focus` makes focus
	/// transfer explicit: query results can appear without interrupting typing
	/// in their source pane.
	OpenHostPanel {
		panel_id: crate::engine::HostPanelId,
		kind: String,
		title: String,
		/// Ignored when this identity already has a live view.
		placement: HostPanelPlacement,
		focus: bool,
	},
	/// Establish or update one producer-scoped host-panel group. Placement is
	/// advisory; the engine keeps every missing member visible on fallback.
	EnsureHostPanelGroup {
		group_id: crate::engine::HostPanelGroupId,
		anchor: Option<PaneId>,
		members: Vec<HostPanelGroupMember>,
		layout: HostPanelGroupLayout,
	},
	/// Reconcile a live group to the automatic layout of its latest current
	/// membership.
	ResetHostPanelGroup {
		group_id: crate::engine::HostPanelGroupId,
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
	ScrollViewport { delta: i32, synchronize_diff: bool },
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

#[cfg(test)]
mod mutation_failure_tests {
	use super::DocumentEditRefusal;

	/// A mutation failure names its cause; the opaque form cost a day of
	/// diagnosis on 2026-09-12.
	#[test]
	fn a_mutation_failure_names_its_cause() {
		let text = DocumentEditRefusal::MutationFailed("registry: root missing".into()).to_string();
		assert_eq!(
			text,
			"the editor could not apply the source edit: registry: root missing"
		);
		assert_eq!(
			DocumentEditRefusal::InvalidRange.to_string(),
			"the planned source range is no longer valid"
		);
	}
}
