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
	Confirm,
	Visual { anchor: DocByte, kind: VisualKind },
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

#[derive(Debug, Clone, Default)]
pub struct SubstituteFlags {
	pub global: bool,
	pub confirm: bool,
	pub case_insensitive: bool,
}

#[derive(Debug, Clone)]
/// A frontend-to-engine request.
pub enum EditorCommand {
	InsertChar(char),
	InsertText(String),
	/// Compile and evaluate a Rune source as a replaceable source generation.
	SourceRune(String),
	/// Retire one Rune source generation and all of its registrations.
	UnloadRune(String),
	ReplaceChar(char),
	ToggleCaseAtCursor,
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
	ClosePane,
	ResizePane(SplitDirection, PaneResizeDirection),
	ResizePaneBoundary {
		before: PaneId,
		after: PaneId,
		axis: SplitDirection,
		target: u32,
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
	SetGitRefresh(u64),
	/// Atomically export the current persistent settings as `init.rn`.
	WriteInit {
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
	OpenGitStatusEntry(DocLine),
	DiffSplit(String),
	MarkdownPreview {
		serif: bool,
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

/// A mouse action interpreted from one rendered viewport.
#[derive(Debug, Clone)]
pub enum MouseIntent {
	FocusPane(PaneId),
	ActivateDocTab { pane_id: PaneId, view_id: ViewId },
	CloseDocTab { pane_id: PaneId, view_id: ViewId },
	ScrollViewport(i32),
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
