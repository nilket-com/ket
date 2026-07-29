use crate::core::DocByte;
use crate::engine::{
	ConfirmAction, EditorMode, KeyCode, Modifiers, PaneFocusDirection, PaneResizeDirection,
	SplitDirection, VerticalDirection, VisualKind, WordMotion,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InputState {
	Normal(NormalPending),
	Visual {
		anchor: DocByte,
		kind: VisualKind,
		pending: VisualPending,
	},
	Insert,
	Command,
	Search,
	Password,
	Confirm,
}

impl InputState {
	pub(super) const fn normal() -> Self {
		Self::Normal(NormalPending::Ready { register: None })
	}

	pub(super) fn editor_mode(self) -> EditorMode {
		match self {
			Self::Normal(_) => EditorMode::Normal,
			Self::Visual { anchor, kind, .. } => EditorMode::Visual { anchor, kind },
			Self::Insert => EditorMode::Insert,
			Self::Command => EditorMode::Command,
			Self::Search => EditorMode::Search,
			Self::Password => EditorMode::Password,
			Self::Confirm => EditorMode::Confirm,
		}
	}

	pub(super) fn from_editor_mode(mode: EditorMode) -> Self {
		match mode {
			EditorMode::Normal => Self::normal(),
			EditorMode::Visual { anchor, kind } => Self::Visual {
				anchor,
				kind,
				pending: VisualPending::Ready,
			},
			EditorMode::Insert => Self::Insert,
			EditorMode::Command => Self::Command,
			EditorMode::Search => Self::Search,
			EditorMode::Password => Self::Password,
			EditorMode::Confirm => Self::Confirm,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Operator {
	Delete,
	Change,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum CharSearchKind {
	ForwardInclusive,
	BackwardInclusive,
	ForwardTill,
	BackwardTill,
}

impl CharSearchKind {
	pub(super) const fn parts(self) -> (bool, bool) {
		match self {
			Self::ForwardInclusive => (true, true),
			Self::BackwardInclusive => (false, true),
			Self::ForwardTill => (true, false),
			Self::BackwardTill => (false, false),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NormalPending {
	Ready { register: Option<char> },
	RegisterCapture { continuation: RegisterContinuation },
	FindChar(CharSearchKind),
	Operator { op: Operator, register: char },
	OperatorInner { op: Operator, register: char },
	OperatorAround { op: Operator, register: char },
	Replace,
	Yank { register: Option<char> },
	Goto { register: Option<char> },
	Fold,
	Bracket,
	Pane,
	PaneResize,
}

impl NormalPending {
	pub(super) const fn captured_register(self) -> Option<char> {
		match self {
			Self::Ready { register } | Self::Yank { register } | Self::Goto { register } => {
				register
			}
			Self::Operator { register, .. }
			| Self::OperatorInner { register, .. }
			| Self::OperatorAround { register, .. } => Some(register),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegisterContinuation {
	Ready,
	Yank,
	Goto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VisualPending {
	Ready,
	FindChar(CharSearchKind),
	Inner,
	Around,
	Goto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum SpecialKey {
	Esc,
	Enter,
	Tab,
	Backspace,
	Delete,
	Up,
	Down,
	Left,
	Right,
	Home,
	End,
	PageUp,
	PageDown,
}

impl SpecialKey {
	fn matches(self, code: KeyCode) -> bool {
		matches!(
			(self, code),
			(Self::Esc, KeyCode::Esc)
				| (Self::Enter, KeyCode::Enter)
				| (Self::Tab, KeyCode::Tab)
				| (Self::Backspace, KeyCode::Backspace)
				| (Self::Delete, KeyCode::Delete)
				| (Self::Up, KeyCode::Up)
				| (Self::Down, KeyCode::Down)
				| (Self::Left, KeyCode::Left)
				| (Self::Right, KeyCode::Right)
				| (Self::Home, KeyCode::Home)
				| (Self::End, KeyCode::End)
				| (Self::PageUp, KeyCode::PageUp)
				| (Self::PageDown, KeyCode::PageDown)
		)
	}

	fn label(self) -> &'static str {
		match self {
			Self::Esc => "Esc",
			Self::Enter => "Enter",
			Self::Tab => "Tab",
			Self::Backspace => "Backspace",
			Self::Delete => "Delete",
			Self::Up => "Up",
			Self::Down => "Down",
			Self::Left => "Left",
			Self::Right => "Right",
			Self::Home => "Home",
			Self::End => "End",
			Self::PageUp => "PageUp",
			Self::PageDown => "PageDown",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum KeyPattern {
	Ctrl(char),
	Alt(char),
	ModifiedChar {
		ch: char,
		modifiers: Modifiers,
	},
	Special(SpecialKey),
	ModifiedSpecial {
		key: SpecialKey,
		modifiers: Modifiers,
	},
	Char(char),
	AnyChar,
}

impl KeyPattern {
	fn precedence(self) -> u8 {
		match self {
			Self::Ctrl(_)
			| Self::Alt(_)
			| Self::ModifiedChar { .. }
			| Self::Special(_)
			| Self::ModifiedSpecial { .. }
			| Self::Char(_) => 0,
			Self::AnyChar => 1,
		}
	}

	pub(super) fn matches(self, code: KeyCode, modifiers: Modifiers) -> bool {
		let normalized_char_modifiers = || {
			let mut normalized = modifiers;
			normalized.remove(Modifiers::SHIFT);
			normalized
		};
		match self {
			Self::Ctrl(ch) => Self::ModifiedChar {
				ch,
				modifiers: Modifiers::CONTROL,
			}
			.matches(code, modifiers),
			Self::Alt(ch) => Self::ModifiedChar {
				ch,
				modifiers: Modifiers::ALT,
			}
			.matches(code, modifiers),
			Self::ModifiedChar {
				ch: expected,
				modifiers: mut expected_modifiers,
			} => {
				expected_modifiers.remove(Modifiers::SHIFT);
				normalized_char_modifiers() == expected_modifiers
					&& matches!(code, KeyCode::Char(actual) if actual == expected)
			}
			Self::Special(key) => Self::ModifiedSpecial {
				key,
				modifiers: Modifiers::NONE,
			}
			.matches(code, modifiers),
			Self::ModifiedSpecial {
				key: expected,
				modifiers: expected_modifiers,
			} => modifiers == expected_modifiers && expected.matches(code),
			Self::Char(expected) => {
				normalized_char_modifiers().is_empty()
					&& matches!(code, KeyCode::Char(actual) if actual == expected)
			}
			Self::AnyChar => {
				normalized_char_modifiers().is_empty() && matches!(code, KeyCode::Char(_))
			}
		}
	}

	fn label(self) -> String {
		match self {
			Self::Ctrl(ch) => format!("Ctrl-{ch}"),
			Self::Alt(ch) => format!("Alt-{ch}"),
			Self::ModifiedChar { ch, modifiers } => modified_label(modifiers, &ch.to_string()),
			Self::Special(key) => key.label().to_string(),
			Self::ModifiedSpecial { key, modifiers } => modified_label(modifiers, key.label()),
			Self::Char(ch) => ch.to_string(),
			Self::AnyChar => "{c}".to_string(),
		}
	}
}

fn modified_label(modifiers: Modifiers, key: &str) -> String {
	let mut parts = Vec::new();
	for (modifier, label) in [
		(Modifiers::CONTROL, "Ctrl"),
		(Modifiers::ALT, "Alt"),
		(Modifiers::SHIFT, "Shift"),
		(Modifiers::SUPER, "Super"),
		(Modifiers::HYPER, "Hyper"),
		(Modifiers::META, "Meta"),
	] {
		if modifiers.contains(modifier) {
			parts.push(label);
		}
	}
	parts.push(key);
	parts.join("-")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Category {
	Motion,
	Editing,
	YankPut,
	Search,
	ModesTabs,
	Panes,
	FoldsStructure,
	Text,
	Prompt,
	Browser,
	Extensions,
}

impl Category {
	pub(super) const fn label(self) -> &'static str {
		match self {
			Self::Motion => "Motion",
			Self::Editing => "Editing",
			Self::YankPut => "Yank and put",
			Self::Search => "Search",
			Self::ModesTabs => "Modes and tabs",
			Self::Panes => "Panes",
			Self::FoldsStructure => "Folds and structure",
			Self::Text => "Text",
			Self::Prompt => "Prompt",
			Self::Browser => "File browser",
			Self::Extensions => "Extensions",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum KeyMode {
	Normal,
	Visual,
	Insert,
	CommandLine,
	Search,
	Confirm,
	FileBrowser,
}

impl KeyMode {
	pub(super) const fn label(self) -> &'static str {
		match self {
			Self::Normal => "Normal mode",
			Self::Visual => "Visual mode",
			Self::Insert => "Insert mode",
			Self::CommandLine => "Command line",
			Self::Search => "Search",
			Self::Confirm => "Substitute confirm",
			Self::FileBrowser => "File browser",
		}
	}
}

pub(super) const KEY_MODE_ORDER: &[KeyMode] = &[
	KeyMode::Normal,
	KeyMode::Visual,
	KeyMode::Insert,
	KeyMode::CommandLine,
	KeyMode::Search,
	KeyMode::Confirm,
	KeyMode::FileBrowser,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConfirmKeyAction {
	Quit,
	Answer(ConfirmAction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MotionOp {
	Up,
	Down,
	Left,
	Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InsertAction {
	Quit,
	Leave,
	InsertChar,
	Newline,
	Tab,
	Backspace,
	Delete,
	Motion(MotionOp),
	SmartHome,
	LineEnd,
	PageUp,
	PageDown,
	AddCursor(VerticalDirection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum VisualContext {
	Ready,
	FindChar(CharSearchKind),
	Inner,
	Around,
	Goto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VisualMotion {
	Up,
	Down,
	Left,
	Right,
	NextWord,
	PrevWord,
	NextWordEnd,
	LineStart,
	FirstNonBlank,
	LineEnd,
	SmartHome,
	PageUp,
	PageDown,
	Bottom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScrollOp {
	HalfUp,
	HalfDown,
	PageUp,
	PageDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VisualOp {
	Yank,
	Delete,
	Change,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VisualAction {
	Quit,
	Motion(VisualMotion),
	Scroll(ScrollOp),
	Apply(VisualOp),
	Esc,
	CommandPrompt,
	ArmInner,
	ArmAround,
	ArmGoto,
	ArmFind(CharSearchKind),
	SelectInnerWord,
	SelectAroundFunction,
	GotoTop,
	CaptureFindChar,
	RepeatFind,
	RepeatFindReversed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum NormalContext {
	Ready,
	FileBrowser,
	RegisterCapture,
	FindChar(CharSearchKind),
	Replace,
	Operator(Operator),
	OperatorInner(Operator),
	OperatorAround(Operator),
	Yank,
	Goto,
	Fold,
	Bracket,
	Pane,
	PaneResize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NormalMotion {
	Up,
	Down,
	Left,
	Right,
	NextWord,
	PrevWord,
	NextWordEnd,
	Top,
	Bottom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ViewportPos {
	Top,
	Middle,
	Bottom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EditOp {
	LineStart,
	FirstNonBlank,
	LineEnd,
	SmartHome,
	PageUp,
	PageDown,
	MatchDelimiter,
	DeleteToLineEnd,
	JoinLine,
	Undo,
	Redo,
	DeleteChar,
	BackspaceChar,
	ToggleCase,
	OpenBelow,
	OpenAbove,
	AppendAtLineEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SearchOp {
	Next,
	Previous,
	WordForward,
	WordBackward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FoldOp {
	Toggle,
	Close,
	Open,
	CloseAll,
	OpenAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PaneOp {
	Focus(PaneFocusDirection),
	Split(SplitDirection),
	Close,
	Resize(SplitDirection, PaneResizeDirection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TabOp {
	Next,
	Previous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BrowserOp {
	CollapseOrParent,
	Activate,
	Refresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OperatorTarget {
	Line,
	Word(WordMotion),
	InnerWord,
	AroundFunction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum YankTarget {
	Line,
	Word(WordMotion),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TextObject {
	Inner,
	Around,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InsertEntry {
	AtCursor,
	FirstNonBlank,
	AfterCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NormalAction {
	Quit,
	Motion(NormalMotion),
	Scroll(ScrollOp),
	ViewportLine {
		pos: ViewportPos,
		first_non_blank: bool,
	},
	Edit(EditOp),
	Search(SearchOp),
	Fold(FoldOp),
	Pane(PaneOp),
	Tab(TabOp),
	Browser(BrowserOp),
	OperatorApply(OperatorTarget),
	YankApply(YankTarget),
	Put,
	ArmOperator(Operator),
	ArmYank,
	ArmGoto,
	ArmFold,
	ArmBracket,
	ArmPane,
	ArmPaneResize,
	ArmReplace,
	ArmRegisterCapture,
	ArmFind(CharSearchKind),
	ArmOperatorObject(TextObject),
	CaptureRegister,
	CaptureFindChar,
	CaptureReplaceChar,
	RepeatFind,
	RepeatFindReversed,
	EnterInsert(InsertEntry),
	EnterVisual(VisualKind),
	EnterCommandPrompt,
	EnterSearchPrompt,
	Esc,
	ReadOnlyStatus,
	ClearPending,
	GotoDefinition,
	JumpBack,
	NextFunction,
	AddCursor(VerticalDirection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum PromptKind {
	Command,
	Search,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PromptAction {
	Quit,
	InsertChar,
	Enter,
	Cancel,
	HistoryUp,
	HistoryDown,
	Backspace,
	Delete,
	CursorLeft,
	CursorRight,
	CursorHome,
	CursorEnd,
	Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PromptOutcome {
	Stay,
	ToNormal,
	Quit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BindingSpec {
	Confirm {
		pattern: KeyPattern,
		action: ConfirmKeyAction,
	},
	Insert {
		pattern: KeyPattern,
		action: InsertAction,
	},
	Visual {
		context: VisualContext,
		pattern: KeyPattern,
		action: VisualAction,
	},
	Normal {
		context: NormalContext,
		pattern: KeyPattern,
		action: NormalAction,
	},
	Prompt {
		kind: PromptKind,
		pattern: KeyPattern,
		action: PromptAction,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Context {
	Normal(NormalContext),
	Visual(VisualContext),
	Insert,
	Confirm,
	Prompt(PromptKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Unmatched {
	FallThrough,
	SwallowClear,
	SwallowKeep,
}

pub(super) const fn on_unmatched(context: Context) -> Unmatched {
	match context {
		Context::Normal(NormalContext::Yank | NormalContext::Goto) => Unmatched::FallThrough,
		Context::Normal(NormalContext::PaneResize)
		| Context::Visual(VisualContext::Ready)
		| Context::Insert
		| Context::Confirm
		| Context::Prompt(_) => Unmatched::SwallowKeep,
		Context::Normal(_)
		| Context::Visual(
			VisualContext::FindChar(_)
			| VisualContext::Inner
			| VisualContext::Around
			| VisualContext::Goto,
		) => Unmatched::SwallowClear,
	}
}

impl BindingSpec {
	pub(super) const fn pattern(self) -> KeyPattern {
		match self {
			Self::Confirm { pattern, .. } => pattern,
			Self::Insert { pattern, .. } => pattern,
			Self::Visual { pattern, .. } => pattern,
			Self::Normal { pattern, .. } => pattern,
			Self::Prompt { pattern, .. } => pattern,
		}
	}

	pub(super) const fn context(self) -> Context {
		match self {
			Self::Confirm { .. } => Context::Confirm,
			Self::Insert { .. } => Context::Insert,
			Self::Visual { context, .. } => Context::Visual(context),
			Self::Normal { context, .. } => Context::Normal(context),
			Self::Prompt { kind, .. } => Context::Prompt(kind),
		}
	}
}

pub(super) const fn binding_is_applicable(binding: &BindingSpec) -> bool {
	match binding {
		BindingSpec::Confirm { .. } | BindingSpec::Insert { .. } => true,
		BindingSpec::Prompt { kind, action, .. } => {
			!matches!(action, PromptAction::Complete) || matches!(kind, PromptKind::Command)
		}
		BindingSpec::Visual {
			context, action, ..
		} => match context {
			VisualContext::Ready => matches!(
				action,
				VisualAction::Quit
					| VisualAction::Motion(_)
					| VisualAction::Scroll(_)
					| VisualAction::Apply(_)
					| VisualAction::Esc
					| VisualAction::CommandPrompt
					| VisualAction::ArmInner
					| VisualAction::ArmAround
					| VisualAction::ArmGoto
					| VisualAction::ArmFind(_)
					| VisualAction::RepeatFind
					| VisualAction::RepeatFindReversed
			),
			VisualContext::FindChar(_) => matches!(action, VisualAction::CaptureFindChar),
			VisualContext::Inner => matches!(action, VisualAction::SelectInnerWord),
			VisualContext::Around => matches!(action, VisualAction::SelectAroundFunction),
			VisualContext::Goto => matches!(action, VisualAction::GotoTop),
		},
		BindingSpec::Normal {
			context, action, ..
		} => match context {
			NormalContext::Ready => matches!(
				action,
				NormalAction::Quit
					| NormalAction::Motion(_)
					| NormalAction::Scroll(_)
					| NormalAction::ViewportLine { .. }
					| NormalAction::Edit(_)
					| NormalAction::Search(_)
					| NormalAction::Put
					| NormalAction::ArmOperator(_)
					| NormalAction::ArmYank
					| NormalAction::ArmGoto
					| NormalAction::ArmFold
					| NormalAction::ArmBracket
					| NormalAction::ArmPane
					| NormalAction::ArmReplace
					| NormalAction::ArmRegisterCapture
					| NormalAction::ArmFind(_)
					| NormalAction::RepeatFind
					| NormalAction::RepeatFindReversed
					| NormalAction::EnterInsert(_)
					| NormalAction::EnterVisual(_)
					| NormalAction::EnterCommandPrompt
					| NormalAction::EnterSearchPrompt
					| NormalAction::Esc
					| NormalAction::ClearPending
					| NormalAction::JumpBack
					| NormalAction::AddCursor(_)
			),
			NormalContext::FileBrowser => matches!(
				action,
				NormalAction::Quit
					| NormalAction::Motion(_)
					| NormalAction::Scroll(_)
					| NormalAction::ViewportLine { .. }
					| NormalAction::Edit(EditOp::PageUp | EditOp::PageDown)
					| NormalAction::Search(SearchOp::Next | SearchOp::Previous)
					| NormalAction::Browser(_)
					| NormalAction::ArmGoto
					| NormalAction::ArmPane
					| NormalAction::EnterCommandPrompt
					| NormalAction::EnterSearchPrompt
					| NormalAction::Esc
					| NormalAction::ReadOnlyStatus
			),
			NormalContext::RegisterCapture => matches!(action, NormalAction::CaptureRegister),
			NormalContext::FindChar(_) => matches!(action, NormalAction::CaptureFindChar),
			NormalContext::Replace => matches!(action, NormalAction::CaptureReplaceChar),
			NormalContext::Operator(_) => matches!(
				action,
				NormalAction::OperatorApply(OperatorTarget::Line | OperatorTarget::Word(_))
					| NormalAction::ArmOperatorObject(_)
			),
			NormalContext::OperatorInner(_) => matches!(
				action,
				NormalAction::OperatorApply(OperatorTarget::InnerWord)
			),
			NormalContext::OperatorAround(_) => matches!(
				action,
				NormalAction::OperatorApply(OperatorTarget::AroundFunction)
			),
			NormalContext::Yank => matches!(action, NormalAction::YankApply(_)),
			NormalContext::Goto => {
				matches!(
					action,
					NormalAction::Motion(NormalMotion::Top)
						| NormalAction::Tab(_)
						| NormalAction::GotoDefinition
				)
			}
			NormalContext::Fold => matches!(action, NormalAction::Fold(_)),
			NormalContext::Bracket => matches!(action, NormalAction::NextFunction),
			NormalContext::Pane => matches!(
				action,
				NormalAction::Pane(PaneOp::Focus(_) | PaneOp::Split(_) | PaneOp::Close)
					| NormalAction::ArmPaneResize
					| NormalAction::Esc
			),
			NormalContext::PaneResize => matches!(
				action,
				NormalAction::Pane(PaneOp::Resize(_, _)) | NormalAction::Esc
			),
		},
	}
}

pub(super) struct KeyGroup {
	pub(super) category: Category,
	pub(super) summary: &'static str,
	pub(super) bindings: &'static [BindingSpec],
}

macro_rules! n {
	($context:expr, $pattern:expr, $action:expr) => {
		BindingSpec::Normal {
			context: $context,
			pattern: $pattern,
			action: $action,
		}
	};
}

macro_rules! p {
	($kind:expr, $pattern:expr, $action:expr) => {
		BindingSpec::Prompt {
			kind: $kind,
			pattern: $pattern,
			action: $action,
		}
	};
}

pub(super) static KEYMAP: &[KeyGroup] = &[
	KeyGroup {
		category: Category::Motion,
		summary: "Move the cursor",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('h'),
				NormalAction::Motion(NormalMotion::Left)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('j'),
				NormalAction::Motion(NormalMotion::Down)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('k'),
				NormalAction::Motion(NormalMotion::Up)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('l'),
				NormalAction::Motion(NormalMotion::Right)
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Move the cursor (arrow keys)",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::Left),
				NormalAction::Motion(NormalMotion::Left)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::Down),
				NormalAction::Motion(NormalMotion::Down)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::Up),
				NormalAction::Motion(NormalMotion::Up)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::Right),
				NormalAction::Motion(NormalMotion::Right)
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Next word / previous word / word end",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('w'),
				NormalAction::Motion(NormalMotion::NextWord)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('b'),
				NormalAction::Motion(NormalMotion::PrevWord)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('e'),
				NormalAction::Motion(NormalMotion::NextWordEnd)
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Go to top, bottom, definition, or older jump",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('g'),
				NormalAction::ArmGoto
			),
			n!(
				NormalContext::Goto,
				KeyPattern::Char('g'),
				NormalAction::Motion(NormalMotion::Top)
			),
			n!(
				NormalContext::Goto,
				KeyPattern::Char('d'),
				NormalAction::GotoDefinition
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('G'),
				NormalAction::Motion(NormalMotion::Bottom)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Ctrl('o'),
				NormalAction::JumpBack
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Line start / first non-blank / line end",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('0'),
				NormalAction::Edit(EditOp::LineStart)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('^'),
				NormalAction::Edit(EditOp::FirstNonBlank)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('$'),
				NormalAction::Edit(EditOp::LineEnd)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::Home),
				NormalAction::Edit(EditOp::SmartHome)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::End),
				NormalAction::Edit(EditOp::LineEnd)
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Jump to the matching delimiter",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Char('%'),
			NormalAction::Edit(EditOp::MatchDelimiter)
		)],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Top / middle / bottom of the viewport",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('H'),
				NormalAction::ViewportLine {
					pos: ViewportPos::Top,
					first_non_blank: true
				}
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('M'),
				NormalAction::ViewportLine {
					pos: ViewportPos::Middle,
					first_non_blank: true
				}
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('L'),
				NormalAction::ViewportLine {
					pos: ViewportPos::Bottom,
					first_non_blank: true
				}
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Scroll half a page up / down",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Ctrl('u'),
				NormalAction::Scroll(ScrollOp::HalfUp)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Ctrl('d'),
				NormalAction::Scroll(ScrollOp::HalfDown)
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Scroll a full page up / down",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Ctrl('b'),
				NormalAction::Scroll(ScrollOp::PageUp)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Ctrl('f'),
				NormalAction::Scroll(ScrollOp::PageDown)
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Page up / down",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::PageUp),
				NormalAction::Edit(EditOp::PageUp)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::PageDown),
				NormalAction::Edit(EditOp::PageDown)
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Find a character (forward / backward)",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('f'),
				NormalAction::ArmFind(CharSearchKind::ForwardInclusive)
			),
			n!(
				NormalContext::FindChar(CharSearchKind::ForwardInclusive),
				KeyPattern::AnyChar,
				NormalAction::CaptureFindChar
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('F'),
				NormalAction::ArmFind(CharSearchKind::BackwardInclusive)
			),
			n!(
				NormalContext::FindChar(CharSearchKind::BackwardInclusive),
				KeyPattern::AnyChar,
				NormalAction::CaptureFindChar
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Till a character (forward / backward)",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('t'),
				NormalAction::ArmFind(CharSearchKind::ForwardTill)
			),
			n!(
				NormalContext::FindChar(CharSearchKind::ForwardTill),
				KeyPattern::AnyChar,
				NormalAction::CaptureFindChar
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('T'),
				NormalAction::ArmFind(CharSearchKind::BackwardTill)
			),
			n!(
				NormalContext::FindChar(CharSearchKind::BackwardTill),
				KeyPattern::AnyChar,
				NormalAction::CaptureFindChar
			),
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Repeat the character search (same / opposite)",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char(';'),
				NormalAction::RepeatFind
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char(','),
				NormalAction::RepeatFindReversed
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Add a cursor below / above",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Alt('j'),
				NormalAction::AddCursor(VerticalDirection::Down)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Alt('k'),
				NormalAction::AddCursor(VerticalDirection::Up)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Insert at cursor / at first non-blank",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('i'),
				NormalAction::EnterInsert(InsertEntry::AtCursor)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('I'),
				NormalAction::EnterInsert(InsertEntry::FirstNonBlank)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Append after cursor / at line end",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('a'),
				NormalAction::EnterInsert(InsertEntry::AfterCursor)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('A'),
				NormalAction::Edit(EditOp::AppendAtLineEnd)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Open a line below / above",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('o'),
				NormalAction::Edit(EditOp::OpenBelow)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('O'),
				NormalAction::Edit(EditOp::OpenAbove)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Delete the character at / before the cursor",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('x'),
				NormalAction::Edit(EditOp::DeleteChar)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('X'),
				NormalAction::Edit(EditOp::BackspaceChar)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::Delete),
				NormalAction::Edit(EditOp::DeleteChar)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Special(SpecialKey::Backspace),
				NormalAction::Edit(EditOp::BackspaceChar)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Replace the character under the cursor",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('r'),
				NormalAction::ArmReplace
			),
			n!(
				NormalContext::Replace,
				KeyPattern::AnyChar,
				NormalAction::CaptureReplaceChar
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Toggle case at the cursor",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Char('~'),
			NormalAction::Edit(EditOp::ToggleCase)
		)],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Delete the current line",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('d'),
				NormalAction::ArmOperator(Operator::Delete)
			),
			n!(
				NormalContext::Operator(Operator::Delete),
				KeyPattern::Char('d'),
				NormalAction::OperatorApply(OperatorTarget::Line)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Delete by word motion",
		bindings: &[
			n!(
				NormalContext::Operator(Operator::Delete),
				KeyPattern::Char('w'),
				NormalAction::OperatorApply(OperatorTarget::Word(WordMotion::NextStart))
			),
			n!(
				NormalContext::Operator(Operator::Delete),
				KeyPattern::Char('b'),
				NormalAction::OperatorApply(OperatorTarget::Word(WordMotion::PrevStart))
			),
			n!(
				NormalContext::Operator(Operator::Delete),
				KeyPattern::Char('e'),
				NormalAction::OperatorApply(OperatorTarget::Word(WordMotion::NextEnd))
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Change by word motion",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('c'),
				NormalAction::ArmOperator(Operator::Change)
			),
			n!(
				NormalContext::Operator(Operator::Change),
				KeyPattern::Char('w'),
				NormalAction::OperatorApply(OperatorTarget::Word(WordMotion::NextStart))
			),
			n!(
				NormalContext::Operator(Operator::Change),
				KeyPattern::Char('b'),
				NormalAction::OperatorApply(OperatorTarget::Word(WordMotion::PrevStart))
			),
			n!(
				NormalContext::Operator(Operator::Change),
				KeyPattern::Char('e'),
				NormalAction::OperatorApply(OperatorTarget::Word(WordMotion::NextEnd))
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Delete / change the inner word",
		bindings: &[
			n!(
				NormalContext::Operator(Operator::Delete),
				KeyPattern::Char('i'),
				NormalAction::ArmOperatorObject(TextObject::Inner)
			),
			n!(
				NormalContext::Operator(Operator::Change),
				KeyPattern::Char('i'),
				NormalAction::ArmOperatorObject(TextObject::Inner)
			),
			n!(
				NormalContext::OperatorInner(Operator::Delete),
				KeyPattern::Char('w'),
				NormalAction::OperatorApply(OperatorTarget::InnerWord)
			),
			n!(
				NormalContext::OperatorInner(Operator::Change),
				KeyPattern::Char('w'),
				NormalAction::OperatorApply(OperatorTarget::InnerWord)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Delete / change around a function",
		bindings: &[
			n!(
				NormalContext::Operator(Operator::Delete),
				KeyPattern::Char('a'),
				NormalAction::ArmOperatorObject(TextObject::Around)
			),
			n!(
				NormalContext::Operator(Operator::Change),
				KeyPattern::Char('a'),
				NormalAction::ArmOperatorObject(TextObject::Around)
			),
			n!(
				NormalContext::OperatorAround(Operator::Delete),
				KeyPattern::Char('f'),
				NormalAction::OperatorApply(OperatorTarget::AroundFunction)
			),
			n!(
				NormalContext::OperatorAround(Operator::Change),
				KeyPattern::Char('f'),
				NormalAction::OperatorApply(OperatorTarget::AroundFunction)
			),
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Delete to the end of the line",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Char('D'),
			NormalAction::Edit(EditOp::DeleteToLineEnd)
		)],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Join with the next line",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Char('J'),
			NormalAction::Edit(EditOp::JoinLine)
		)],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Undo / redo",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('u'),
				NormalAction::Edit(EditOp::Undo)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Ctrl('r'),
				NormalAction::Edit(EditOp::Redo)
			),
		],
	},
	KeyGroup {
		category: Category::YankPut,
		summary: "Yank the current line",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('y'),
				NormalAction::ArmYank
			),
			n!(
				NormalContext::Yank,
				KeyPattern::Char('y'),
				NormalAction::YankApply(YankTarget::Line)
			),
		],
	},
	KeyGroup {
		category: Category::YankPut,
		summary: "Yank by word motion",
		bindings: &[
			n!(
				NormalContext::Yank,
				KeyPattern::Char('w'),
				NormalAction::YankApply(YankTarget::Word(WordMotion::NextStart))
			),
			n!(
				NormalContext::Yank,
				KeyPattern::Char('b'),
				NormalAction::YankApply(YankTarget::Word(WordMotion::PrevStart))
			),
			n!(
				NormalContext::Yank,
				KeyPattern::Char('e'),
				NormalAction::YankApply(YankTarget::Word(WordMotion::NextEnd))
			),
		],
	},
	KeyGroup {
		category: Category::YankPut,
		summary: "Register prefix, e.g. \"ayy or \"+p (system clipboard)",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('"'),
				NormalAction::ArmRegisterCapture
			),
			n!(
				NormalContext::RegisterCapture,
				KeyPattern::AnyChar,
				NormalAction::CaptureRegister
			),
		],
	},
	KeyGroup {
		category: Category::YankPut,
		summary: "Put below the current line",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Char('p'),
			NormalAction::Put
		)],
	},
	KeyGroup {
		category: Category::Search,
		summary: "Start a search",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Char('/'),
			NormalAction::EnterSearchPrompt
		)],
	},
	KeyGroup {
		category: Category::Search,
		summary: "Next / previous match",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('n'),
				NormalAction::Search(SearchOp::Next)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('N'),
				NormalAction::Search(SearchOp::Previous)
			),
		],
	},
	KeyGroup {
		category: Category::Search,
		summary: "Search the word under the cursor",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('*'),
				NormalAction::Search(SearchOp::WordForward)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('#'),
				NormalAction::Search(SearchOp::WordBackward)
			),
		],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Open the command line",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Char(':'),
			NormalAction::EnterCommandPrompt
		)],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Visual char / line / block selection",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('v'),
				NormalAction::EnterVisual(VisualKind::Char)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Char('V'),
				NormalAction::EnterVisual(VisualKind::Line)
			),
			n!(
				NormalContext::Ready,
				KeyPattern::Ctrl('v'),
				NormalAction::EnterVisual(VisualKind::Block)
			),
		],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Next / previous tab",
		bindings: &[
			n!(
				NormalContext::Goto,
				KeyPattern::Char('t'),
				NormalAction::Tab(TabOp::Next)
			),
			n!(
				NormalContext::Goto,
				KeyPattern::Char('T'),
				NormalAction::Tab(TabOp::Previous)
			),
		],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Quit",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Ctrl('z'),
			NormalAction::Quit
		)],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Clear pending input",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Ctrl('w'),
			NormalAction::ClearPending
		)],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Clear selection/search or secondary cursors",
		bindings: &[n!(
			NormalContext::Ready,
			KeyPattern::Special(SpecialKey::Esc),
			NormalAction::Esc
		)],
	},
	KeyGroup {
		category: Category::Panes,
		summary: "Focus the pane in that direction",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('s'),
				NormalAction::ArmPane
			),
			n!(
				NormalContext::Pane,
				KeyPattern::Char('h'),
				NormalAction::Pane(PaneOp::Focus(PaneFocusDirection::Left))
			),
			n!(
				NormalContext::Pane,
				KeyPattern::Char('j'),
				NormalAction::Pane(PaneOp::Focus(PaneFocusDirection::Down))
			),
			n!(
				NormalContext::Pane,
				KeyPattern::Char('k'),
				NormalAction::Pane(PaneOp::Focus(PaneFocusDirection::Up))
			),
			n!(
				NormalContext::Pane,
				KeyPattern::Char('l'),
				NormalAction::Pane(PaneOp::Focus(PaneFocusDirection::Right))
			),
		],
	},
	KeyGroup {
		category: Category::Panes,
		summary: "Split horizontally / vertically",
		bindings: &[
			n!(
				NormalContext::Pane,
				KeyPattern::Char('s'),
				NormalAction::Pane(PaneOp::Split(SplitDirection::Horizontal))
			),
			n!(
				NormalContext::Pane,
				KeyPattern::Char('v'),
				NormalAction::Pane(PaneOp::Split(SplitDirection::Vertical))
			),
		],
	},
	KeyGroup {
		category: Category::Panes,
		summary: "Close the focused pane",
		bindings: &[n!(
			NormalContext::Pane,
			KeyPattern::Char('c'),
			NormalAction::Pane(PaneOp::Close)
		)],
	},
	KeyGroup {
		category: Category::Panes,
		summary: "Resize panes (sticky until Esc)",
		bindings: &[
			n!(
				NormalContext::Pane,
				KeyPattern::Char('r'),
				NormalAction::ArmPaneResize
			),
			n!(
				NormalContext::PaneResize,
				KeyPattern::Char('h'),
				NormalAction::Pane(PaneOp::Resize(
					SplitDirection::Vertical,
					PaneResizeDirection::Decrease
				))
			),
			n!(
				NormalContext::PaneResize,
				KeyPattern::Char('j'),
				NormalAction::Pane(PaneOp::Resize(
					SplitDirection::Horizontal,
					PaneResizeDirection::Increase
				))
			),
			n!(
				NormalContext::PaneResize,
				KeyPattern::Char('k'),
				NormalAction::Pane(PaneOp::Resize(
					SplitDirection::Horizontal,
					PaneResizeDirection::Decrease
				))
			),
			n!(
				NormalContext::PaneResize,
				KeyPattern::Char('l'),
				NormalAction::Pane(PaneOp::Resize(
					SplitDirection::Vertical,
					PaneResizeDirection::Increase
				))
			),
		],
	},
	KeyGroup {
		category: Category::Panes,
		summary: "Leave the pane prefix or resize mode",
		bindings: &[
			n!(
				NormalContext::Pane,
				KeyPattern::Special(SpecialKey::Esc),
				NormalAction::Esc
			),
			n!(
				NormalContext::PaneResize,
				KeyPattern::Special(SpecialKey::Esc),
				NormalAction::Esc
			),
		],
	},
	KeyGroup {
		category: Category::FoldsStructure,
		summary: "Toggle / close / open the fold",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char('z'),
				NormalAction::ArmFold
			),
			n!(
				NormalContext::Fold,
				KeyPattern::Char('a'),
				NormalAction::Fold(FoldOp::Toggle)
			),
			n!(
				NormalContext::Fold,
				KeyPattern::Char('c'),
				NormalAction::Fold(FoldOp::Close)
			),
			n!(
				NormalContext::Fold,
				KeyPattern::Char('o'),
				NormalAction::Fold(FoldOp::Open)
			),
		],
	},
	KeyGroup {
		category: Category::FoldsStructure,
		summary: "Close / open all folds",
		bindings: &[
			n!(
				NormalContext::Fold,
				KeyPattern::Char('M'),
				NormalAction::Fold(FoldOp::CloseAll)
			),
			n!(
				NormalContext::Fold,
				KeyPattern::Char('R'),
				NormalAction::Fold(FoldOp::OpenAll)
			),
		],
	},
	KeyGroup {
		category: Category::FoldsStructure,
		summary: "Jump to the next function",
		bindings: &[
			n!(
				NormalContext::Ready,
				KeyPattern::Char(']'),
				NormalAction::ArmBracket
			),
			n!(
				NormalContext::Bracket,
				KeyPattern::Char('m'),
				NormalAction::NextFunction
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Move the selection",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('j'),
				NormalAction::Motion(NormalMotion::Down)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::Down),
				NormalAction::Motion(NormalMotion::Down)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('k'),
				NormalAction::Motion(NormalMotion::Up)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::Up),
				NormalAction::Motion(NormalMotion::Up)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Collapse the entry or go to the parent",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('h'),
				NormalAction::Browser(BrowserOp::CollapseOrParent)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::Left),
				NormalAction::Browser(BrowserOp::CollapseOrParent)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Open the entry",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('l'),
				NormalAction::Browser(BrowserOp::Activate)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::Right),
				NormalAction::Browser(BrowserOp::Activate)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::Enter),
				NormalAction::Browser(BrowserOp::Activate)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Refresh the listing",
		bindings: &[n!(
			NormalContext::FileBrowser,
			KeyPattern::Char('r'),
			NormalAction::Browser(BrowserOp::Refresh)
		)],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Run or edit the command line",
		bindings: &[
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Enter),
				PromptAction::Enter
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Tab),
				PromptAction::Complete
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Backspace),
				PromptAction::Backspace
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Delete),
				PromptAction::Delete
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Left),
				PromptAction::CursorLeft
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Right),
				PromptAction::CursorRight
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Home),
				PromptAction::CursorHome
			),
			p!(
				PromptKind::Command,
				KeyPattern::Ctrl('a'),
				PromptAction::CursorHome
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::End),
				PromptAction::CursorEnd
			),
			p!(
				PromptKind::Command,
				KeyPattern::Ctrl('e'),
				PromptAction::CursorEnd
			),
			p!(
				PromptKind::Command,
				KeyPattern::AnyChar,
				PromptAction::InsertChar
			),
		],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Recall command history",
		bindings: &[
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Up),
				PromptAction::HistoryUp
			),
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Down),
				PromptAction::HistoryDown
			),
		],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Cancel the command line or quit",
		bindings: &[
			p!(
				PromptKind::Command,
				KeyPattern::Special(SpecialKey::Esc),
				PromptAction::Cancel
			),
			p!(
				PromptKind::Command,
				KeyPattern::Ctrl('z'),
				PromptAction::Quit
			),
		],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Run or edit the search",
		bindings: &[
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Enter),
				PromptAction::Enter
			),
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Backspace),
				PromptAction::Backspace
			),
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Delete),
				PromptAction::Delete
			),
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Left),
				PromptAction::CursorLeft
			),
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Right),
				PromptAction::CursorRight
			),
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Home),
				PromptAction::CursorHome
			),
			p!(
				PromptKind::Search,
				KeyPattern::Ctrl('a'),
				PromptAction::CursorHome
			),
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::End),
				PromptAction::CursorEnd
			),
			p!(
				PromptKind::Search,
				KeyPattern::Ctrl('e'),
				PromptAction::CursorEnd
			),
			p!(
				PromptKind::Search,
				KeyPattern::AnyChar,
				PromptAction::InsertChar
			),
		],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Recall search history",
		bindings: &[
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Up),
				PromptAction::HistoryUp
			),
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Down),
				PromptAction::HistoryDown
			),
		],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Cancel the search or quit",
		bindings: &[
			p!(
				PromptKind::Search,
				KeyPattern::Special(SpecialKey::Esc),
				PromptAction::Cancel
			),
			p!(
				PromptKind::Search,
				KeyPattern::Ctrl('z'),
				PromptAction::Quit
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Top / bottom of the listing",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('g'),
				NormalAction::ArmGoto
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('G'),
				NormalAction::Motion(NormalMotion::Bottom)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::Home),
				NormalAction::Motion(NormalMotion::Top)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::End),
				NormalAction::Motion(NormalMotion::Bottom)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Top / middle / bottom of the viewport",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('H'),
				NormalAction::ViewportLine {
					pos: ViewportPos::Top,
					first_non_blank: false
				}
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('M'),
				NormalAction::ViewportLine {
					pos: ViewportPos::Middle,
					first_non_blank: false
				}
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('L'),
				NormalAction::ViewportLine {
					pos: ViewportPos::Bottom,
					first_non_blank: false
				}
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Scroll half a page up / down",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Ctrl('u'),
				NormalAction::Scroll(ScrollOp::HalfUp)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Ctrl('d'),
				NormalAction::Scroll(ScrollOp::HalfDown)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Scroll a full page up / down",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Ctrl('b'),
				NormalAction::Scroll(ScrollOp::PageUp)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Ctrl('f'),
				NormalAction::Scroll(ScrollOp::PageDown)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Page up / down",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::PageUp),
				NormalAction::Edit(EditOp::PageUp)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::PageDown),
				NormalAction::Edit(EditOp::PageDown)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Search the entries",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('/'),
				NormalAction::EnterSearchPrompt
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('n'),
				NormalAction::Search(SearchOp::Next)
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('N'),
				NormalAction::Search(SearchOp::Previous)
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "Command line, panes, clear search, quit",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char(':'),
				NormalAction::EnterCommandPrompt
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('s'),
				NormalAction::ArmPane
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Special(SpecialKey::Esc),
				NormalAction::Esc
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Ctrl('z'),
				NormalAction::Quit
			),
		],
	},
	KeyGroup {
		category: Category::Browser,
		summary: "File browser is read-only",
		bindings: &[
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('i'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('I'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('a'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('A'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('o'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('O'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('d'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('c'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('x'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('X'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('p'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('u'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('v'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('V'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('y'),
				NormalAction::ReadOnlyStatus
			),
			n!(
				NormalContext::FileBrowser,
				KeyPattern::Char('~'),
				NormalAction::ReadOnlyStatus
			),
		],
	},
	KeyGroup {
		category: Category::Text,
		summary: "Insert text",
		bindings: &[BindingSpec::Insert {
			pattern: KeyPattern::AnyChar,
			action: InsertAction::InsertChar,
		}],
	},
	KeyGroup {
		category: Category::Text,
		summary: "Insert a line break or tab",
		bindings: &[
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Enter),
				action: InsertAction::Newline,
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Tab),
				action: InsertAction::Tab,
			},
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Add a cursor below / above",
		bindings: &[
			BindingSpec::Insert {
				pattern: KeyPattern::Alt('j'),
				action: InsertAction::AddCursor(VerticalDirection::Down),
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Alt('k'),
				action: InsertAction::AddCursor(VerticalDirection::Up),
			},
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Delete text",
		bindings: &[
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Backspace),
				action: InsertAction::Backspace,
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Delete),
				action: InsertAction::Delete,
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Move while inserting",
		bindings: &[
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Up),
				action: InsertAction::Motion(MotionOp::Up),
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Down),
				action: InsertAction::Motion(MotionOp::Down),
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Left),
				action: InsertAction::Motion(MotionOp::Left),
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Right),
				action: InsertAction::Motion(MotionOp::Right),
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::Home),
				action: InsertAction::SmartHome,
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::End),
				action: InsertAction::LineEnd,
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::PageUp),
				action: InsertAction::PageUp,
			},
			BindingSpec::Insert {
				pattern: KeyPattern::Special(SpecialKey::PageDown),
				action: InsertAction::PageDown,
			},
		],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Back to normal mode",
		bindings: &[BindingSpec::Insert {
			pattern: KeyPattern::Special(SpecialKey::Esc),
			action: InsertAction::Leave,
		}],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Quit",
		bindings: &[BindingSpec::Insert {
			pattern: KeyPattern::Ctrl('z'),
			action: InsertAction::Quit,
		}],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Yank / delete / change the selection",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('y'),
				action: VisualAction::Apply(VisualOp::Yank),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('d'),
				action: VisualAction::Apply(VisualOp::Delete),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('c'),
				action: VisualAction::Apply(VisualOp::Change),
			},
		],
	},
	KeyGroup {
		category: Category::Editing,
		summary: "Select inner word / around function",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('i'),
				action: VisualAction::ArmInner,
			},
			BindingSpec::Visual {
				context: VisualContext::Inner,
				pattern: KeyPattern::Char('w'),
				action: VisualAction::SelectInnerWord,
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('a'),
				action: VisualAction::ArmAround,
			},
			BindingSpec::Visual {
				context: VisualContext::Around,
				pattern: KeyPattern::Char('f'),
				action: VisualAction::SelectAroundFunction,
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Extend the selection",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('h'),
				action: VisualAction::Motion(VisualMotion::Left),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('j'),
				action: VisualAction::Motion(VisualMotion::Down),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('k'),
				action: VisualAction::Motion(VisualMotion::Up),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('l'),
				action: VisualAction::Motion(VisualMotion::Right),
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Extend the selection (arrow keys)",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::Left),
				action: VisualAction::Motion(VisualMotion::Left),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::Down),
				action: VisualAction::Motion(VisualMotion::Down),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::Up),
				action: VisualAction::Motion(VisualMotion::Up),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::Right),
				action: VisualAction::Motion(VisualMotion::Right),
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Extend by word",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('w'),
				action: VisualAction::Motion(VisualMotion::NextWord),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('b'),
				action: VisualAction::Motion(VisualMotion::PrevWord),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('e'),
				action: VisualAction::Motion(VisualMotion::NextWordEnd),
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Extend within the line",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('0'),
				action: VisualAction::Motion(VisualMotion::LineStart),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('^'),
				action: VisualAction::Motion(VisualMotion::FirstNonBlank),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('$'),
				action: VisualAction::Motion(VisualMotion::LineEnd),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::End),
				action: VisualAction::Motion(VisualMotion::LineEnd),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::Home),
				action: VisualAction::Motion(VisualMotion::SmartHome),
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Extend by page",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::PageUp),
				action: VisualAction::Motion(VisualMotion::PageUp),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Special(SpecialKey::PageDown),
				action: VisualAction::Motion(VisualMotion::PageDown),
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Extend to top / bottom of the file",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('g'),
				action: VisualAction::ArmGoto,
			},
			BindingSpec::Visual {
				context: VisualContext::Goto,
				pattern: KeyPattern::Char('g'),
				action: VisualAction::GotoTop,
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('G'),
				action: VisualAction::Motion(VisualMotion::Bottom),
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Find / till a character",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('f'),
				action: VisualAction::ArmFind(CharSearchKind::ForwardInclusive),
			},
			BindingSpec::Visual {
				context: VisualContext::FindChar(CharSearchKind::ForwardInclusive),
				pattern: KeyPattern::AnyChar,
				action: VisualAction::CaptureFindChar,
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('F'),
				action: VisualAction::ArmFind(CharSearchKind::BackwardInclusive),
			},
			BindingSpec::Visual {
				context: VisualContext::FindChar(CharSearchKind::BackwardInclusive),
				pattern: KeyPattern::AnyChar,
				action: VisualAction::CaptureFindChar,
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('t'),
				action: VisualAction::ArmFind(CharSearchKind::ForwardTill),
			},
			BindingSpec::Visual {
				context: VisualContext::FindChar(CharSearchKind::ForwardTill),
				pattern: KeyPattern::AnyChar,
				action: VisualAction::CaptureFindChar,
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char('T'),
				action: VisualAction::ArmFind(CharSearchKind::BackwardTill),
			},
			BindingSpec::Visual {
				context: VisualContext::FindChar(CharSearchKind::BackwardTill),
				pattern: KeyPattern::AnyChar,
				action: VisualAction::CaptureFindChar,
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Repeat the character search (same / opposite)",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char(';'),
				action: VisualAction::RepeatFind,
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Char(','),
				action: VisualAction::RepeatFindReversed,
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Scroll half a page up / down",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Ctrl('u'),
				action: VisualAction::Scroll(ScrollOp::HalfUp),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Ctrl('d'),
				action: VisualAction::Scroll(ScrollOp::HalfDown),
			},
		],
	},
	KeyGroup {
		category: Category::Motion,
		summary: "Scroll a full page up / down",
		bindings: &[
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Ctrl('b'),
				action: VisualAction::Scroll(ScrollOp::PageUp),
			},
			BindingSpec::Visual {
				context: VisualContext::Ready,
				pattern: KeyPattern::Ctrl('f'),
				action: VisualAction::Scroll(ScrollOp::PageDown),
			},
		],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Command line with the visual selection range",
		bindings: &[BindingSpec::Visual {
			context: VisualContext::Ready,
			pattern: KeyPattern::Char(':'),
			action: VisualAction::CommandPrompt,
		}],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Back to normal mode",
		bindings: &[BindingSpec::Visual {
			context: VisualContext::Ready,
			pattern: KeyPattern::Special(SpecialKey::Esc),
			action: VisualAction::Esc,
		}],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Quit",
		bindings: &[BindingSpec::Visual {
			context: VisualContext::Ready,
			pattern: KeyPattern::Ctrl('z'),
			action: VisualAction::Quit,
		}],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Replace this match / skip it",
		bindings: &[
			BindingSpec::Confirm {
				pattern: KeyPattern::Char('y'),
				action: ConfirmKeyAction::Answer(ConfirmAction::Yes),
			},
			BindingSpec::Confirm {
				pattern: KeyPattern::Char('n'),
				action: ConfirmKeyAction::Answer(ConfirmAction::No),
			},
		],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Replace all remaining matches",
		bindings: &[BindingSpec::Confirm {
			pattern: KeyPattern::Char('a'),
			action: ConfirmKeyAction::Answer(ConfirmAction::All),
		}],
	},
	KeyGroup {
		category: Category::Prompt,
		summary: "Stop confirming",
		bindings: &[
			BindingSpec::Confirm {
				pattern: KeyPattern::Char('q'),
				action: ConfirmKeyAction::Answer(ConfirmAction::Quit),
			},
			BindingSpec::Confirm {
				pattern: KeyPattern::Special(SpecialKey::Esc),
				action: ConfirmKeyAction::Answer(ConfirmAction::Quit),
			},
		],
	},
	KeyGroup {
		category: Category::ModesTabs,
		summary: "Quit",
		bindings: &[BindingSpec::Confirm {
			pattern: KeyPattern::Ctrl('z'),
			action: ConfirmKeyAction::Quit,
		}],
	},
];

pub(super) fn resolve_confirm(code: KeyCode, modifiers: Modifiers) -> Option<&'static BindingSpec> {
	KEYMAP
		.iter()
		.flat_map(|group| group.bindings)
		.filter(|binding| matches!(binding, BindingSpec::Confirm { .. }))
		.filter(|binding| binding_is_applicable(binding))
		.filter(|binding| binding.pattern().matches(code, modifiers))
		.min_by_key(|binding| binding.pattern().precedence())
}

pub(super) fn resolve_insert(code: KeyCode, modifiers: Modifiers) -> Option<&'static BindingSpec> {
	KEYMAP
		.iter()
		.flat_map(|group| group.bindings)
		.filter(|binding| matches!(binding, BindingSpec::Insert { .. }))
		.filter(|binding| binding_is_applicable(binding))
		.filter(|binding| binding.pattern().matches(code, modifiers))
		.min_by_key(|binding| binding.pattern().precedence())
}

pub(super) fn visual_context(state: InputState) -> Option<VisualContext> {
	match state {
		InputState::Visual { pending, .. } => Some(match pending {
			VisualPending::Ready => VisualContext::Ready,
			VisualPending::FindChar(kind) => VisualContext::FindChar(kind),
			VisualPending::Inner => VisualContext::Inner,
			VisualPending::Around => VisualContext::Around,
			VisualPending::Goto => VisualContext::Goto,
		}),
		_ => None,
	}
}

pub(super) fn resolve_visual(
	context: VisualContext,
	code: KeyCode,
	modifiers: Modifiers,
) -> Option<&'static BindingSpec> {
	KEYMAP
		.iter()
		.flat_map(|group| group.bindings)
		.filter(|binding| {
			matches!(binding, BindingSpec::Visual { context: candidate, .. } if *candidate == context)
		})
		.filter(|binding| binding_is_applicable(binding))
		.filter(|binding| binding.pattern().matches(code, modifiers))
		.min_by_key(|binding| binding.pattern().precedence())
}

pub(super) fn normal_context(state: InputState, browser: bool) -> Option<NormalContext> {
	let InputState::Normal(pending) = state else {
		return None;
	};
	Some(match pending {
		NormalPending::RegisterCapture { .. } => NormalContext::RegisterCapture,
		NormalPending::FindChar(kind) => NormalContext::FindChar(kind),
		NormalPending::PaneResize => NormalContext::PaneResize,
		NormalPending::Pane => NormalContext::Pane,
		NormalPending::Goto { .. } => NormalContext::Goto,
		_ if browser => NormalContext::FileBrowser,
		NormalPending::Replace => NormalContext::Replace,
		NormalPending::Operator { op, .. } => NormalContext::Operator(op),
		NormalPending::OperatorInner { op, .. } => NormalContext::OperatorInner(op),
		NormalPending::OperatorAround { op, .. } => NormalContext::OperatorAround(op),
		NormalPending::Yank { .. } => NormalContext::Yank,
		NormalPending::Fold => NormalContext::Fold,
		NormalPending::Bracket => NormalContext::Bracket,
		NormalPending::Ready { .. } => NormalContext::Ready,
	})
}

pub(super) fn resolve_normal(
	context: NormalContext,
	code: KeyCode,
	modifiers: Modifiers,
) -> Option<&'static BindingSpec> {
	KEYMAP
		.iter()
		.flat_map(|group| group.bindings)
		.filter(|binding| {
			matches!(binding, BindingSpec::Normal { context: candidate, .. } if *candidate == context)
		})
		.filter(|binding| binding_is_applicable(binding))
		.filter(|binding| binding.pattern().matches(code, modifiers))
		.min_by_key(|binding| binding.pattern().precedence())
}

pub(super) fn prompt_kind(state: InputState) -> Option<PromptKind> {
	match state {
		InputState::Command => Some(PromptKind::Command),
		InputState::Search => Some(PromptKind::Search),
		_ => None,
	}
}

pub(super) fn resolve_prompt(
	kind: PromptKind,
	code: KeyCode,
	modifiers: Modifiers,
) -> Option<&'static BindingSpec> {
	KEYMAP
		.iter()
		.flat_map(|group| group.bindings)
		.filter(
			|binding| matches!(binding, BindingSpec::Prompt { kind: candidate, .. } if *candidate == kind),
		)
		.filter(|binding| binding_is_applicable(binding))
		.filter(|binding| binding.pattern().matches(code, modifiers))
		.min_by_key(|binding| binding.pattern().precedence())
}

pub(super) fn arm_target(binding: &BindingSpec) -> Option<Context> {
	match binding {
		BindingSpec::Normal {
			context, action, ..
		} => Some(Context::Normal(match action {
			NormalAction::ArmOperator(op) => NormalContext::Operator(*op),
			NormalAction::ArmYank => NormalContext::Yank,
			NormalAction::ArmGoto => NormalContext::Goto,
			NormalAction::ArmFold => NormalContext::Fold,
			NormalAction::ArmBracket => NormalContext::Bracket,
			NormalAction::ArmPane => NormalContext::Pane,
			NormalAction::ArmPaneResize => NormalContext::PaneResize,
			NormalAction::ArmReplace => NormalContext::Replace,
			NormalAction::ArmRegisterCapture => NormalContext::RegisterCapture,
			NormalAction::ArmFind(kind) => NormalContext::FindChar(*kind),
			NormalAction::ArmOperatorObject(TextObject::Inner) => {
				let NormalContext::Operator(op) = context else {
					return None;
				};
				NormalContext::OperatorInner(*op)
			}
			NormalAction::ArmOperatorObject(TextObject::Around) => {
				let NormalContext::Operator(op) = context else {
					return None;
				};
				NormalContext::OperatorAround(*op)
			}
			_ => return None,
		})),
		BindingSpec::Visual { action, .. } => Some(Context::Visual(match action {
			VisualAction::ArmInner => VisualContext::Inner,
			VisualAction::ArmAround => VisualContext::Around,
			VisualAction::ArmGoto => VisualContext::Goto,
			VisualAction::ArmFind(kind) => VisualContext::FindChar(*kind),
			_ => return None,
		})),
		BindingSpec::Insert { .. } | BindingSpec::Confirm { .. } | BindingSpec::Prompt { .. } => {
			None
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KeyDoc {
	pub(super) mode: KeyMode,
	pub(super) category: Category,
	pub(super) keys: String,
	pub(super) summary: String,
	#[allow(dead_code)]
	pub(super) group_index: usize,
}

pub(super) fn key_docs() -> Vec<KeyDoc> {
	key_docs_excluding(&std::collections::HashSet::new())
}

pub(super) fn key_docs_excluding(
	excluded: &std::collections::HashSet<(KeyMode, String)>,
) -> Vec<KeyDoc> {
	use std::collections::{HashMap, HashSet};

	fn root(mode: KeyMode) -> Context {
		match mode {
			KeyMode::Normal => Context::Normal(NormalContext::Ready),
			KeyMode::Visual => Context::Visual(VisualContext::Ready),
			KeyMode::Insert => Context::Insert,
			KeyMode::CommandLine => Context::Prompt(PromptKind::Command),
			KeyMode::Search => Context::Prompt(PromptKind::Search),
			KeyMode::Confirm => Context::Confirm,
			KeyMode::FileBrowser => Context::Normal(NormalContext::FileBrowser),
		}
	}

	fn walk(
		mode: KeyMode,
		context: Context,
		prefix: &str,
		stack: &mut HashSet<Context>,
		labels: &mut HashMap<(KeyMode, usize), Vec<String>>,
		excluded: &HashSet<(KeyMode, String)>,
	) {
		if !stack.insert(context) {
			return;
		}
		for (group_index, group) in KEYMAP.iter().enumerate() {
			for binding in group
				.bindings
				.iter()
				.filter(|binding| binding.context() == context)
			{
				let pattern = binding.pattern();
				// Char keys glue onto the prefix (dd, diw, srh); multi-char
				// tokens get a space so "s Esc" doesn't render as "sEsc".
				let separator = if prefix.is_empty()
					|| matches!(pattern, KeyPattern::Char(_) | KeyPattern::AnyChar)
				{
					""
				} else {
					" "
				};
				let path = if matches!(pattern, KeyPattern::AnyChar) {
					format!("{prefix}{{c}}")
				} else {
					format!("{prefix}{separator}{}", pattern.label())
				};
				if let Some(target) = arm_target(binding) {
					walk(mode, target, &path, stack, labels, excluded);
				} else {
					if excluded.contains(&(mode, path.clone())) {
						continue;
					}
					let group_labels = labels.entry((mode, group_index)).or_default();
					if !group_labels.contains(&path) {
						group_labels.push(path);
					}
				}
			}
		}
		stack.remove(&context);
	}

	let mut labels = HashMap::new();
	for mode in KEY_MODE_ORDER {
		walk(
			*mode,
			root(*mode),
			"",
			&mut HashSet::new(),
			&mut labels,
			excluded,
		);
	}

	let mut docs = Vec::new();
	for mode in KEY_MODE_ORDER {
		for (group_index, group) in KEYMAP.iter().enumerate() {
			if let Some(group_labels) = labels.remove(&(*mode, group_index)) {
				let mut chunks: Vec<String> = Vec::new();
				for label in group_labels {
					match chunks.last_mut() {
						Some(chunk) if chunk.len() + 3 + label.len() <= 26 => {
							chunk.push_str(" / ");
							chunk.push_str(&label);
						}
						_ => chunks.push(label),
					}
				}
				for (index, keys) in chunks.into_iter().enumerate() {
					docs.push(KeyDoc {
						mode: *mode,
						category: group.category,
						keys,
						// Overflow rows continue the same group; repeating the
						// summary reads like distinct entries.
						summary: if index == 0 { group.summary } else { "" }.to_string(),
						group_index,
					});
				}
			}
		}
	}
	docs
}

#[cfg(test)]
mod registry_tests {
	use super::*;
	use std::collections::{HashMap, HashSet};

	#[test]
	fn key_patterns_match_exact_normalized_modifier_sets() {
		assert!(KeyPattern::Char('A').matches(KeyCode::Char('A'), Modifiers::SHIFT));
		assert!(!KeyPattern::Char('a').matches(KeyCode::Char('a'), Modifiers::CONTROL));
		assert!(!KeyPattern::AnyChar.matches(KeyCode::Char('c'), Modifiers::CONTROL));

		assert!(KeyPattern::Ctrl('d').matches(KeyCode::Char('d'), Modifiers::CONTROL));
		assert!(
			!KeyPattern::Ctrl('d')
				.matches(KeyCode::Char('d'), Modifiers::CONTROL | Modifiers::ALT,)
		);
		assert!(
			KeyPattern::ModifiedChar {
				ch: 'd',
				modifiers: Modifiers::CONTROL | Modifiers::ALT,
			}
			.matches(KeyCode::Char('d'), Modifiers::CONTROL | Modifiers::ALT,)
		);

		assert!(!KeyPattern::Special(SpecialKey::Tab).matches(KeyCode::Tab, Modifiers::SHIFT,));
		assert!(
			KeyPattern::ModifiedSpecial {
				key: SpecialKey::Tab,
				modifiers: Modifiers::SHIFT,
			}
			.matches(KeyCode::Tab, Modifiers::SHIFT)
		);
	}

	#[test]
	fn confirm_patterns_are_unique_and_groups_are_nonempty() {
		let mut confirm_patterns = HashSet::new();
		let mut insert_patterns = HashSet::new();
		let mut visual_patterns = HashSet::new();
		let mut normal_patterns = HashSet::new();
		let mut prompt_patterns = HashSet::new();
		for group in KEYMAP {
			assert!(!group.bindings.is_empty());
			assert!(!group.summary.is_empty());
			for binding in group.bindings {
				match binding {
					BindingSpec::Confirm { .. } => {
						assert!(confirm_patterns.insert(binding.pattern()));
					}
					BindingSpec::Insert { .. } => {
						assert!(insert_patterns.insert(binding.pattern()));
					}
					BindingSpec::Visual { context, .. } => {
						assert!(visual_patterns.insert((*context, binding.pattern())));
					}
					BindingSpec::Normal { context, .. } => {
						assert!(normal_patterns.insert((*context, binding.pattern())));
					}
					BindingSpec::Prompt { kind, .. } => {
						assert!(prompt_patterns.insert((*kind, binding.pattern())));
					}
				}
			}
		}
	}

	#[test]
	fn search_and_command_prompts_share_editing_bindings() {
		let prompt_bindings = |kind| {
			KEYMAP
				.iter()
				.flat_map(|group| group.bindings)
				.filter_map(move |binding| match binding {
					BindingSpec::Prompt {
						kind: binding_kind,
						pattern,
						action,
					} if *binding_kind == kind && !matches!(action, PromptAction::Complete) => {
						Some((*pattern, *action))
					}
					_ => None,
				})
				.collect::<Vec<_>>()
		};

		let command = prompt_bindings(PromptKind::Command);
		let search = prompt_bindings(PromptKind::Search);
		assert_eq!(command.len(), search.len());
		for binding in command {
			assert!(
				search.contains(&binding),
				"missing search binding: {binding:?}"
			);
		}
	}

	#[test]
	fn confirm_ctrl_z_precedes_plain_char_matching() {
		let binding =
			resolve_confirm(KeyCode::Char('z'), Modifiers::CONTROL).expect("Ctrl-z binding");
		assert!(matches!(
			binding,
			BindingSpec::Confirm {
				action: ConfirmKeyAction::Quit,
				..
			}
		));
	}

	#[test]
	fn wildcard_patterns_only_appear_in_capture_contexts() {
		for group in KEYMAP {
			for binding in group.bindings {
				if !matches!(binding.pattern(), KeyPattern::AnyChar) {
					continue;
				}
				assert!(matches!(
					binding.context(),
					Context::Insert
						| Context::Prompt(_)
						| Context::Normal(NormalContext::RegisterCapture)
						| Context::Normal(NormalContext::FindChar(_))
						| Context::Normal(NormalContext::Replace)
						| Context::Visual(VisualContext::FindChar(_))
				));
			}
		}
	}

	#[test]
	fn every_binding_is_applicable_to_its_context() {
		for group in KEYMAP {
			for binding in group.bindings {
				assert!(
					binding_is_applicable(binding),
					"invalid context/action pairing: {binding:?}"
				);
			}
		}
	}

	#[test]
	fn arm_graph_is_acyclic_and_every_context_is_reachable() {
		let roots = [
			Context::Normal(NormalContext::Ready),
			Context::Normal(NormalContext::FileBrowser),
			Context::Visual(VisualContext::Ready),
			Context::Insert,
			Context::Confirm,
			Context::Prompt(PromptKind::Command),
			Context::Prompt(PromptKind::Search),
		];
		let mut edges: HashMap<Context, Vec<Context>> = HashMap::new();
		let mut all_contexts = HashSet::new();
		for group in KEYMAP {
			for binding in group.bindings {
				all_contexts.insert(binding.context());
				if let Some(target) = arm_target(binding) {
					edges.entry(binding.context()).or_default().push(target);
				}
			}
		}

		fn visit(
			context: Context,
			edges: &HashMap<Context, Vec<Context>>,
			visiting: &mut HashSet<Context>,
			reached: &mut HashSet<Context>,
		) {
			assert!(visiting.insert(context), "cycle at {context:?}");
			reached.insert(context);
			for target in edges.get(&context).into_iter().flatten() {
				assert!(!visiting.contains(target), "cycle at {target:?}");
				if !reached.contains(target) {
					visit(*target, edges, visiting, reached);
				}
			}
			visiting.remove(&context);
		}

		let mut reached = HashSet::new();
		for root in roots {
			visit(root, &edges, &mut HashSet::new(), &mut reached);
		}
		assert_eq!(all_contexts, reached);
	}

	#[test]
	fn every_group_renders_and_core_sequences_are_derived() {
		let docs = key_docs();
		let rendered_groups: HashSet<usize> = docs.iter().map(|doc| doc.group_index).collect();
		assert_eq!(rendered_groups.len(), KEYMAP.len());
		let labels = docs
			.iter()
			.map(|doc| doc.keys.as_str())
			.collect::<Vec<_>>()
			.join(" / ");
		for sequence in [
			"gg", "diw", "ciw", "yy", "za", "]m", "ss", "gt", "Alt-j", "Alt-k",
		] {
			assert!(
				labels.contains(sequence),
				"missing derived sequence {sequence}"
			);
		}
	}

	#[test]
	fn every_base_context_has_quit() {
		let bases = [
			Context::Normal(NormalContext::Ready),
			Context::Normal(NormalContext::FileBrowser),
			Context::Visual(VisualContext::Ready),
			Context::Insert,
			Context::Confirm,
			Context::Prompt(PromptKind::Command),
			Context::Prompt(PromptKind::Search),
		];
		for base in bases {
			assert!(
				KEYMAP
					.iter()
					.flat_map(|group| group.bindings)
					.any(|binding| {
						if binding.context() != base {
							return false;
						}
						matches!(
							binding,
							BindingSpec::Normal {
								action: NormalAction::Quit,
								..
							} | BindingSpec::Visual {
								action: VisualAction::Quit,
								..
							} | BindingSpec::Insert {
								action: InsertAction::Quit,
								..
							} | BindingSpec::Confirm {
								action: ConfirmKeyAction::Quit,
								..
							} | BindingSpec::Prompt {
								action: PromptAction::Quit,
								..
							}
						)
					}),
				"missing quit in {base:?}"
			);
		}
	}

	#[test]
	fn derived_categories_are_contiguous_per_mode() {
		let docs = key_docs();
		for mode in KEY_MODE_ORDER {
			let mut closed = HashSet::new();
			let mut current = None;
			for category in docs
				.iter()
				.filter(|doc| doc.mode == *mode)
				.map(|doc| doc.category)
			{
				if current != Some(category) {
					if let Some(previous) = current {
						closed.insert(previous);
					}
					assert!(!closed.contains(&category));
					current = Some(category);
				}
			}
		}
	}
}
