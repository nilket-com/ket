use super::coords::{CursorPos, LineIdx};
use super::highlight::SyntaxLanguage;
use super::vim::VimMode;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum EditorCommand {
	// ─── Text Manipulation ──────────────────────────────────────────────────
	Insert(String),
	DeleteBack,
	DeleteForward,
	InsertNewline,
	Indent,
	Outdent,
	ReplaceChar(char),

	// ─── Cursor & Selection ─────────────────────────────────────────────────
	MoveUp(usize, bool),   // (count, extend_selection)
	MoveDown(usize, bool),
	MoveLeft(usize, bool),
	MoveRight(usize, bool),
	MoveWordForward(usize, bool),
	MoveWordBackward(usize, bool),
	MoveToLineStart(bool),
	MoveToLineEnd(bool),
	MoveToDocStart(bool),
	MoveToDocEnd(bool),
	SetCursor(CursorPos, bool),
	AddCursor(CursorPos),
	ClearSecondarySelections,
	SelectWordAt(CursorPos),
	SelectAll,

	// ─── Clipboard ──────────────────────────────────────────────────────────
	Cut,
	Copy,
	Paste(String),
	PasteAfter(String),

	// ─── History ────────────────────────────────────────────────────────────
	Undo,
	Redo,

	// ─── Buffer & View State ────────────────────────────────────────────────
	SetLanguage(SyntaxLanguage),
	ToggleFold(LineIdx),
	SetWrap(bool),
	Scroll(f32, f32), // (dx, dy)
	SetViewport(f32, f32),

	// ─── Search & Replace ───────────────────────────────────────────────────
	SearchOpen,
	SearchClose,
	SearchNext,
	SearchPrev,
	SearchReplaceCurrent,
	SearchReplaceAll,

	// ─── Vim Specific (Transitionary) ───────────────────────────────────────
	VimSetMode(VimMode),
}
