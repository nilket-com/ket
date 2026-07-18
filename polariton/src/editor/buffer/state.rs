use ropey::Rope;

use crate::editor::analysis::Diagnostic;
use crate::editor::coords::{CharIdx, Selection};
use crate::editor::folding::FoldState;
use crate::editor::highlight::{SyntaxLanguage, SyntaxToken};
use crate::editor::search::SearchState;
use crate::editor::wrap::{VisualLine, WrapConfig};

use super::core::BracketPair;

pub struct DocumentState {
	pub rope: Rope,
	pub diagnostics: Vec<Diagnostic>,
	pub folds: FoldState,
	pub wrap_config: WrapConfig,
	pub visual_lines: Vec<VisualLine>,
	pub(super) language: SyntaxLanguage,
	pub(super) tokens: Vec<SyntaxToken>,
	pub(super) document_version: u64,
	pub(super) analyzed_version: u64,
}

pub struct SessionState {
	pub selection: Selection,
	pub secondary_selections: Vec<Selection>,
	pub matched_bracket: Option<BracketPair>,
	pub search: SearchState,
	pub(super) desired_col: Option<CharIdx>,
	pub clipboard: String,
	pub clipboard_is_line: bool,
}
