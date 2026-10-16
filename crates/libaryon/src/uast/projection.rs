use crate::core::{BufferId, DocumentId, Rgb};
use crate::core::{
	CursorPosition, DisplayRow, DocByte, DocDisplayCol, DocLine, DocLineCount, NodeByteOffset,
	PaneId, PaneLocalCol, PaneLocalRow, PaneRect, TAB_SIZE, ViewId, ViewportId,
};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::UiPalette;
use crate::engine::{
	GutterNumbering, HostPanelGroupId, HostPanelId, MinimapSnapshot, PanelRevision, SplitDirection,
	VisibleRow,
};
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::{CATEGORY_COUNT, HighlightSpan};
use crate::uast::kind::SemanticKind;
pub use crate::uast::root_line_index::RootChildLineIndex;
use crate::uast::root_line_index::{
	KnownLineMetric, RootChildLineIndexEntry, RootChildLineIndexInspectorSnapshot,
};
use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct RenderToken {
	pub node_id: NodeId,
	pub kind: SemanticKind,
	pub text: Vec<u8>,
	pub physical_byte_len: u32,
	#[allow(dead_code)]
	pub absolute_start_line: DocLine,
	pub absolute_start_byte: DocByte,
	pub is_virtual: bool,
	pub is_folded: bool,
}

/// Reassemble contiguous loaded document bytes before publishing them to a
/// frontend. UAST leaves are byte-storage and semantic boundaries, so a valid
/// UTF-8 scalar may straddle two of them. A frontend that decodes each leaf in
/// isolation would therefore fabricate invalid text from a valid document.
///
/// Hydration and pinning consume the original tokens before this projection is
/// applied. Fold placeholders remain separate because their display bytes do
/// not correspond one-for-one with their physical span.
pub(crate) fn coalesce_frontend_text_tokens(tokens: Vec<RenderToken>) -> Vec<RenderToken> {
	let mut coalesced: Vec<RenderToken> = Vec::with_capacity(tokens.len());
	for mut token in tokens {
		let Some(previous) = coalesced.last_mut() else {
			coalesced.push(token);
			continue;
		};
		let previous_end = previous
			.absolute_start_byte
			.saturating_add(previous.text.len() as u64);
		let merged_len = previous.text.len().checked_add(token.text.len());
		let can_merge = !previous.is_folded
			&& !token.is_folded
			&& !previous.text.is_empty()
			&& !token.text.is_empty()
			&& previous_end == token.absolute_start_byte
			&& merged_len.is_some_and(|len| u32::try_from(len).is_ok());
		if can_merge {
			previous.text.append(&mut token.text);
			previous.physical_byte_len = u32::try_from(previous.text.len())
				.expect("frontend text token length was checked before merging");
			previous.is_virtual |= token.is_virtual;
		} else {
			coalesced.push(token);
		}
	}
	coalesced
}

/// How much of each line a render query may discover. A hydrated leaf is
/// published whole and an unloaded one is hydrated on sight, so an unbounded
/// walk of a newline-free line reaches the end of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineDiscovery {
	Unbounded,
	/// Take no leaf that starts this many bytes or more past its line's start.
	/// A byte bound on discovery, not a display-cell bound: one cell may be
	/// longer than any fixed byte count.
	PerLineBytes(u64),
}

/// Display cells kept past an unwrapped pane's right edge. Without horizontal
/// scroll nothing beyond the edge is drawn; one cell is enough to place an
/// end-of-line annotation off-screen. It is not a continuation signal.
pub(crate) const CLIP_LOOKAROUND_COLUMNS: u32 = 1;

/// Drop the bytes of each line past `text_width + CLIP_LOOKAROUND_COLUMNS`
/// display columns, for a pane without wrap. A hydrated leaf is published
/// whole, so a newline-free file would otherwise send its entire 1 MiB extent
/// with every frame. Runs after coalescing, so cuts fall on grapheme
/// boundaries of valid text. Kept bytes keep their absolute positions; a line's
/// newline stays with that line and the bytes after it are untouched.
pub(crate) fn clip_frontend_text_tokens(
	tokens: Vec<RenderToken>,
	text_width: u16,
) -> Vec<RenderToken> {
	let limit = u32::from(text_width).saturating_add(CLIP_LOOKAROUND_COLUMNS);
	let mut clipped = Vec::with_capacity(tokens.len());
	let mut column = 0u32;
	let mut past_limit = false;
	for token in tokens {
		if token.is_folded || token.text.is_empty() {
			if token.is_folded {
				column = crate::core::text::display_width(&token.text, column, TAB_SIZE);
				past_limit = column >= limit;
			}
			clipped.push(token);
			continue;
		}
		// Byte ranges of `token.text` to keep, in order, each with the number
		// of newlines before its start.
		let mut kept: Vec<(Range<usize>, u64)> = Vec::new();
		let mut keep = |range: Range<usize>, lines_before: u64| match kept.last_mut() {
			Some((last, _)) if last.end == range.start => last.end = range.end,
			_ if range.is_empty() => {}
			_ => kept.push((range, lines_before)),
		};
		let mut lines_before = 0u64;
		let mut segment_start = 0usize;
		while segment_start < token.text.len() {
			let newline = token.text[segment_start..]
				.iter()
				.position(|&byte| byte == b'\n')
				.map(|offset| segment_start + offset);
			let segment_end = newline.unwrap_or(token.text.len());
			if !past_limit {
				let (cut, reached) = crate::core::text::byte_offset_for_display_col(
					&token.text[segment_start..segment_end],
					limit,
					column,
					TAB_SIZE,
				);
				keep(segment_start..segment_start + cut, lines_before);
				column = reached;
				past_limit = column >= limit;
			}
			match newline {
				Some(at) => {
					keep(at..at + 1, lines_before);
					lines_before += 1;
					column = 0;
					past_limit = false;
					segment_start = at + 1;
				}
				None => break,
			}
		}
		if kept.len() == 1 && kept[0].0 == (0..token.text.len()) {
			clipped.push(token);
			continue;
		}
		for (range, lines_before) in kept {
			let text = token.text[range.clone()].to_vec();
			clipped.push(RenderToken {
				node_id: token.node_id,
				kind: token.kind,
				physical_byte_len: u32::try_from(text.len())
					.expect("a clipped token is no longer than its source token"),
				text,
				absolute_start_line: token.absolute_start_line.saturating_add(lines_before),
				absolute_start_byte: token.absolute_start_byte.saturating_add(range.start as u64),
				is_virtual: token.is_virtual,
				is_folded: false,
			});
		}
	}
	clipped
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeCursorTarget {
	pub node_id: NodeId,
	pub node_byte: NodeByteOffset,
	pub display_col: DocDisplayCol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeByteTarget {
	pub node_id: NodeId,
	pub node_start_byte: DocByte,
	pub node_end_byte: DocByte,
	pub node_byte: NodeByteOffset,
	/// Absolute line at the start of `node_id`, when every preceding newline
	/// metric encountered by the byte descent is exact.
	pub node_start_line: Option<DocLine>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineLookupState {
	Ready,
	Pending {
		indexed_through_line: DocLine,
		indexed_through_byte: DocByte,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeLineLookup {
	Ready(NodeCursorTarget),
	Pending {
		indexed_through_line: DocLine,
		indexed_through_byte: DocByte,
		/// The cold extent whose bytes are required to finish the projection.
		/// `None` means the background prefix scan has not reached the line yet.
		required_node: Option<NodeId>,
	},
}

impl NodeLineLookup {
	pub fn ready(self) -> Option<NodeCursorTarget> {
		match self {
			Self::Ready(target) => Some(target),
			Self::Pending { .. } => None,
		}
	}

	pub fn required_node(self) -> Option<NodeId> {
		match self {
			Self::Pending { required_node, .. } => required_node,
			Self::Ready(_) => None,
		}
	}
}

#[cfg(test)]
impl std::ops::Deref for NodeLineLookup {
	type Target = NodeCursorTarget;

	fn deref(&self) -> &Self::Target {
		match self {
			Self::Ready(target) => target,
			Self::Pending { .. } => panic!("line lookup should be ready"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiffKind {
	Added,
	Removed,
	Modified,
	Moved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntraLineDiffKind {
	Added,
	Removed,
	Modified,
}

// Sub-line, token-level change produced by Layer 3 of the diff engine. Fields
// describe this pane's ranges; `partner_*` describe the matching ranges on the
// opposing pane so the bridge layer can connect them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IntraLineDiff {
	pub line: DocLine,
	pub col_start: u32,
	pub col_end: u32,
	pub partner_line: DocLine,
	pub partner_col_start: u32,
	pub partner_col_end: u32,
	pub kind: IntraLineDiffKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiffBlock {
	pub range: (DocLine, DocLine),
	pub kind: DiffKind,
	pub partner_range: (DocLine, DocLine),
	pub intra_line_diffs: Option<Vec<IntraLineDiff>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffPair {
	pub partner_pane_id: PaneId,
	pub blocks: Vec<DiffBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MarkdownCellIdentity {
	/// Free, session-local identity for an ordinary fence. Moving text above the
	/// fence changes its range, deliberately: durable references require an
	/// explicit id instead.
	Implicit {
		occurrence: u32,
		body_range: Range<DocByte>,
	},
	/// A user-authored `{#name}` attribute. This is the stable handle Rune and
	/// durable output association will use once those features arrive.
	Explicit { id: String },
}

/// Where a notebook cell presents its execution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MarkdownCellOutputTarget {
	/// Render beneath the fence in Polariton's native Markdown preview.
	#[default]
	Inline,
	/// Open or refresh a Baryon-managed native result pane.
	Pane,
	/// Keep execution feedback in the status line only.
	None,
}

/// Whether the rich preview renders a cell's fenced source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MarkdownCellCodeVisibility {
	#[default]
	Show,
	Hide,
}

/// Bounded presentation metadata authored on a fenced executable cell.
/// Parsing lives with Markdown fence parsing; frontends receive this typed
/// projection rather than interpreting fence strings themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MarkdownCellPresentation {
	pub output: MarkdownCellOutputTarget,
	pub code: MarkdownCellCodeVisibility,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarkdownCellSnapshot {
	pub language_id: String,
	pub body_range: Range<DocByte>,
	pub identity: MarkdownCellIdentity,
	pub presentation: MarkdownCellPresentation,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarkdownPreviewSnapshot {
	pub source_buffer_id: BufferId,
	pub source_view_id: ViewId,
	pub serif: bool,
	pub source_path: Option<String>,
	pub source_text: String,
	/// Unresolved source-coordinate categories. Frontends map these through
	/// `theme_colors`, exactly as they do for text-pane highlighting.
	pub code_highlights: Vec<HighlightSpan>,
	pub theme_colors: [Option<Rgb>; CATEGORY_COUNT],
	/// Fenced executable-shaped blocks, parsed structurally by libaryon. The
	/// frontend receives identities and ranges but never reparses Markdown.
	pub cells: Vec<MarkdownCellSnapshot>,
	pub source_scroll_line: DocLine,
	pub source_viewport_line_count: u32,
}

/// A draggable split boundary, authored by the engine so frontends never infer
/// gap geometry for themselves.
///
/// `visual_rect` is what to paint; `hit_rect` is what to grab, and may be
/// larger. Keeping them separate lets a divider stay visually thin while
/// remaining comfortably clickable — a 1-row horizontal divider is a poor
/// pointer target at its painted size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaneDividerViewport {
	pub before: PaneId,
	pub after: PaneId,
	pub axis: SplitDirection,
	pub visual_rect: PaneRect,
	pub hit_rect: PaneRect,
}

/// Presentation identity of a host-rendered pane. Carries no host data: the
/// frontend maps `panel_id` to whatever it is drawing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPanelViewport {
	pub panel_id: HostPanelId,
	pub kind: String,
	pub title: String,
	pub revision: PanelRevision,
}

pub struct PaneViewport {
	pub pane_id: PaneId,
	/// Stable identity of the active buffer. A buffer can have multiple views;
	/// hosts use this identity when an asynchronous product belongs to the
	/// document rather than to one particular view of it.
	pub buffer_id: BufferId,
	/// Durable identity of an ordinary document in a saved engine.
	pub document_id: Option<crate::core::DocumentId>,
	/// Identity of the active view projected in this pane.
	pub view_id: ViewId,
	pub rect: PaneRect,
	pub tokens: Vec<RenderToken>,
	pub scroll_y: u64,
	pub viewport_start_line: DocLine,
	pub viewport_row_offset: DisplayRow,
	pub viewport_line_count: u32,
	pub wrap_enabled: bool,
	pub visible_whitespace: bool,
	pub gutter_numbering: Option<GutterNumbering>,
	pub gutter_line_numbers: Vec<GutterLineNumber>,
	pub cursor_lines: Vec<DocLine>,
	pub cursor_pane_row: PaneLocalRow,
	pub cursor_abs_pos: CursorPosition,
	pub cursor_pane_col: PaneLocalCol,
	pub cursor_abs_byte: DocByte,
	pub cursor_line_start_byte: DocByte,
	pub secondary_carets: Vec<ProjectedCaret>,
	pub cursor_count: usize,
	pub max_doc_line: DocLine,
	pub file_name: Option<String>,
	pub file_size: u64,
	pub line_count: Option<u64>,
	pub is_dirty: bool,
	pub search_match_info: Option<String>,
	pub global_start_byte: DocByte,
	pub highlights: Vec<HighlightSpan>,
	pub diagnostics: Vec<DiagnosticSpan>,
	pub inline_annotations: Vec<crate::engine::language::InlineAnnotation>,
	pub selection_ranges: Vec<(DocByte, DocByte)>,
	pub yank_flash: Option<(DocByte, DocByte)>,
	pub minimap: Option<MinimapSnapshot>,
	/// Executable fenced cells belonging to this Markdown source document.
	/// This is source metadata, not preview metadata: cells remain runnable
	/// after every paired preview pane is closed.
	pub markdown_cells: Vec<MarkdownCellSnapshot>,
	pub markdown_preview: Option<MarkdownPreviewSnapshot>,
	/// Cursor-scoped UAST graph for frontend-native rendering.
	pub uast_graph: Option<crate::uast::UastGraphSnapshot>,
	/// Present when this pane is host-rendered; the frontend draws its rect.
	pub host_panel: Option<HostPanelViewport>,
	pub theme_colors: [Option<Rgb>; CATEGORY_COUNT],
	pub render_rows: Vec<VisibleRow>,
	pub visible_rows: Vec<VisibleRow>,
	/// Set when the view starts inside a line laid out through the wrap index:
	/// `tokens` then begin at its byte, `render_rows == visible_rows` and
	/// `viewport_row_offset` is 0. Unset, every other field means what it
	/// always has.
	pub row_window: Option<crate::engine::RowWindow>,
	pub visible_line_starts: Vec<DocLine>,
	pub dma_pending: bool,
	/// Stable language provider identifier used by frontends for behavior such
	/// as execution dispatch. Unlike `language_label`, this is not presentation.
	pub language_id: Option<crate::engine::LanguageId>,
	pub language_label: &'static str,
	pub is_file_browser: bool,
	pub is_output_panel: bool,
	pub is_git_status_panel: bool,
	pub background_override: Option<Rgb>,
	pub tabs: Vec<DocTabSummary>,
	pub active_doctab_index: usize,
	pub diff_pair: Option<DiffPair>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GutterLineNumber {
	pub line: DocLine,
	pub number: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectedCaret {
	pub pane_row: PaneLocalRow,
	pub pane_col: PaneLocalCol,
	pub abs_pos: CursorPosition,
	pub abs_byte: DocByte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PickerResultViewport {
	pub path: String,
	pub line: DocLine,
	pub col: DocDisplayCol,
	pub line_text: String,
	pub score: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PickerViewport {
	pub mode: crate::engine::RepoSearchMode,
	pub query: String,
	pub query_cursor: usize,
	/// Absolute selected result within the engine-owned ranked result set.
	pub selected: usize,
	/// Absolute index of `results[0]` within that same set.
	pub first_result: usize,
	pub results: Vec<PickerResultViewport>,
	pub shown: usize,
	pub matched: u64,
	pub truncated: bool,
	pub searching: bool,
	pub files_walked: u64,
	pub error: Option<String>,
}

/// Complete saved-session identity state shared with an opaque frontend owner.
///
/// This projection is intentionally only three sets. It carries no placement,
/// ordering, descriptor, membership, or other relationship fact; PRODUCT-0061's
/// checkpoint witness is sufficient only while those facts remain exclusively
/// owned by libaryon's logical-layout payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharedIdentityCensus {
	pub documents: BTreeSet<DocumentId>,
	pub host_panels: BTreeSet<HostPanelId>,
	pub host_panel_groups: BTreeSet<HostPanelGroupId>,
}

/// Immutable render projection sent from the engine to a frontend.
pub struct Viewport {
	pub viewport_id: ViewportId,
	pub panes: Vec<PaneViewport>,
	pub focused_pane_id: PaneId,
	/// Authoritative shared identities for one saved-engine projection. `None`
	/// means this engine has no saved session and therefore no coupled owner.
	pub shared_identity_census: Option<SharedIdentityCensus>,
	/// Checked-out branch for the focused document's repository, when available.
	pub git_branch: Option<String>,
	/// Repository backing the open Git-status pane, if one has been resolved.
	///
	/// Frontends may observe this path for low-latency refreshes. Interval mode
	/// also polls as a backstop when observation is unavailable or loses events.
	pub git_status_repo_root: Option<std::path::PathBuf>,
	pub status_message: Option<String>,
	/// Draggable split boundaries for this layout, engine-authored so the host
	/// never infers gap geometry (see `plans/wip/notebook-with-panes.md`).
	pub dividers: Vec<PaneDividerViewport>,
	/// One-shot reply to [`EditorCommand::QueryDocumentText`]: the focused
	/// document's full text, present only on the projection that answers the
	/// query.
	///
	/// [`EditorCommand::QueryDocumentText`]: crate::engine::EditorCommand::QueryDocumentText
	pub document_text: Option<String>,
	/// Buffer that [`Self::document_text`] was read from. This is attributed
	/// independently of the viewport's current focus because a queued focus
	/// command may be rendered immediately after the text query.
	pub document_text_buffer_id: Option<BufferId>,
	/// Document state read atomically with [`Self::document_text`].
	pub document_text_state_id: Option<crate::core::StateId>,
	/// Whether foreground I/O or semantic analysis is still in progress.
	///
	/// Frontends can animate this state without asking the engine to render
	/// intermediate frames while it is blocked on synchronous work.
	pub busy: bool,
	pub should_quit: bool,
	pub search_pattern: Option<String>,
	pub search_case_insensitive: bool,
	pub ui_palette: UiPalette,
	pub git_refresh_mode: crate::engine::GitRefreshMode,
	pub confirm_prompt: Option<String>,
	/// Engine-owned modal picker. Frontends render this value but never search.
	pub picker: Option<PickerViewport>,
	pub input: crate::engine::InputProjection,
	pub language_completion_candidates: Vec<String>,
}

impl Viewport {
	pub fn pane(&self, pane_id: PaneId) -> Option<&PaneViewport> {
		self.panes.iter().find(|pane| pane.pane_id == pane_id)
	}

	pub fn pane_mut(&mut self, pane_id: PaneId) -> Option<&mut PaneViewport> {
		self.panes.iter_mut().find(|pane| pane.pane_id == pane_id)
	}

	/// Find a view that is active and therefore present in this projection.
	pub fn view(&self, view_id: ViewId) -> Option<&PaneViewport> {
		self.panes.iter().find(|pane| pane.view_id == view_id)
	}

	/// Find a mutable active-view projection by stable view identity.
	pub fn view_mut(&mut self, view_id: ViewId) -> Option<&mut PaneViewport> {
		self.panes.iter_mut().find(|pane| pane.view_id == view_id)
	}

	pub fn focused_pane(&self) -> Option<&PaneViewport> {
		self.pane(self.focused_pane_id)
	}

	pub fn focused_pane_mut(&mut self) -> Option<&mut PaneViewport> {
		self.pane_mut(self.focused_pane_id)
	}
}

impl std::ops::Deref for Viewport {
	type Target = PaneViewport;

	fn deref(&self) -> &Self::Target {
		self.focused_pane()
			.expect("viewport must include the focused pane")
	}
}

impl std::ops::DerefMut for Viewport {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.focused_pane_mut()
			.expect("viewport must include the focused pane")
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocTabSummary {
	/// Engine-owned file identity, including inactive tabs (PRODUCT-0160).
	pub source_path: Option<String>,
	/// Stable identity of the document behind this tab.
	pub buffer_id: BufferId,
	/// Durable identity of an ordinary document in a saved engine.
	pub document_id: Option<crate::core::DocumentId>,
	/// Stable identity of this tab's view, including inactive tabs.
	pub view_id: ViewId,
	pub title: String,
	pub is_dirty: bool,
	/// Present when this tab shows a snapshot partner (e.g. a `:panediff`
	/// revision pane): the immutable identity it was acquired as.
	pub snapshot: Option<SnapshotIdentity>,
}

/// Immutable identity of a snapshot document, pinned at acquisition. The
/// `revision` is the symbolic label the user asked for; the OIDs are what the
/// snapshot actually is — a ref moving after acquisition changes neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotIdentity {
	pub revision: String,
	pub commit_oid: String,
	pub blob_oid: String,
}

const FOLDED_TOKEN_TEXT: &str = "❯❯❯";

fn line_start_offset(text: &[u8], target_line_in_node: DocLine) -> Option<usize> {
	if target_line_in_node == DocLine::ZERO {
		return Some(0);
	}

	let mut line = DocLine::ZERO;
	for (i, &b) in text.iter().enumerate() {
		if b == b'\n' {
			line = line.saturating_add(1);
			if line == target_line_in_node {
				return Some(i + 1);
			}
		}
	}

	None
}

fn line_starts_at_node_end(text: &[u8], target_line_in_node: DocLine) -> bool {
	matches!(
		line_start_offset(text, target_line_in_node),
		Some(offset) if offset == text.len()
	)
}

fn offset_for_visual_col(
	line_bytes: &[u8],
	target_col: DocDisplayCol,
) -> (NodeByteOffset, DocDisplayCol) {
	let (byte_offset, display_col) =
		crate::core::text::byte_offset_for_display_col(line_bytes, target_col.get(), 0, TAB_SIZE);
	(
		NodeByteOffset::new(byte_offset as u32),
		DocDisplayCol::new(display_col),
	)
}

fn visual_width_for_line_bytes(line_bytes: &[u8]) -> DocDisplayCol {
	DocDisplayCol::new(crate::core::text::display_width(line_bytes, 0, TAB_SIZE))
}

/// Opaque fold membership used by projection and coordinate lookup.
///
/// The hot walks are generic over this policy so a view-owned implementation is
/// monomorphized without exposing its storage representation or paying dynamic
/// dispatch per visited node.
pub trait FoldPolicy {
	fn is_folded(&self, node: NodeId) -> bool;

	fn has_folds(&self) -> bool {
		true
	}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoFolds;

impl FoldPolicy for NoFolds {
	#[inline(always)]
	fn is_folded(&self, _node: NodeId) -> bool {
		false
	}

	#[inline(always)]
	fn has_folds(&self) -> bool {
		false
	}
}

pub trait UastProjection {
	fn query_viewport<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_line: DocLine,
		line_count: u32,
	) -> Vec<RenderToken>;
	fn get_next_node_in_walk(&self, node: NodeId) -> Option<NodeId>;
	fn find_node_at_line_col<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_line: DocLine,
		target_col: DocDisplayCol,
	) -> NodeLineLookup;
	fn find_node_at_doc_byte<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_byte: DocByte,
	) -> NodeByteTarget;
	fn doc_byte_for_node_offset(
		&self,
		root: NodeId,
		node: NodeId,
		node_offset: NodeByteOffset,
	) -> DocByte;
	fn read_loaded_slice(
		&self,
		root: NodeId,
		start: DocByte,
		end: DocByte,
	) -> Result<Vec<u8>, &'static str>;
}

/// An extent's line summary is known only when both its newline count has been
/// inflated and its terminal-newline state has been observed. This is the single
/// place that rule is applied, so no caller can build a half-known entry.
fn known_line_metric(
	newline_known: bool,
	newline_count: u64,
	ends_with_newline: Option<bool>,
) -> Option<KnownLineMetric> {
	if !newline_known {
		return None;
	}
	ends_with_newline.map(|ends_with_newline| KnownLineMetric {
		newline_count,
		ends_with_newline,
	})
}

impl UastRegistry {
	fn indexed_terminal_newline(&self, node: NodeId) -> Option<bool> {
		let mut leaf = node;
		while let Some(child) = self.get_first_child(leaf) {
			let mut last = child;
			while let Some(next) = self.get_next_sibling(last) {
				last = next;
			}
			leaf = last;
		}
		if let Some(bytes) = unsafe { (&*self.virtual_data[leaf.index()].get()).as_deref() } {
			return Some(bytes.last() == Some(&b'\n'));
		}
		self.terminal_newline(leaf)
	}

	pub fn line_lookup_state(&self, root: NodeId, target_line: DocLine) -> LineLookupState {
		let indexes = self.root_child_line_indexes.read().unwrap();
		let Some(index) = indexes.get(&root) else {
			return LineLookupState::Ready;
		};
		if index.known_prefix_len() == index.len() || target_line == DocLine::ZERO {
			return LineLookupState::Ready;
		}
		let (indexed_through_line, indexed_through_byte, indexed_ends_with_newline) =
			if index.known_prefix_len() == 0 {
				(DocLine::ZERO, DocByte::ZERO, true)
			} else {
				let location = index
					.known_prefix_boundary()
					.expect("non-empty known prefix has a boundary");
				(
					location.end_line().expect("known prefix end"),
					location.end_byte(),
					location
						.entry
						.ends_with_newline()
						.expect("known prefix terminal state"),
				)
			};
		if target_line > indexed_through_line
			|| (target_line == indexed_through_line && indexed_ends_with_newline)
		{
			LineLookupState::Pending {
				indexed_through_line,
				indexed_through_byte,
			}
		} else {
			LineLookupState::Ready
		}
	}

	pub(crate) fn build_root_child_line_index(&self, root: NodeId) -> RootChildLineIndex {
		let _resident_read = self.resident_data_guard();
		let mut entries = Vec::new();
		let metrics_pending = self.exact_line_count(root).is_none();
		let mut child = self.get_first_child(root);

		while let Some(node) = child {
			let metrics = unsafe { &*self.metrics[node.index()].get() };
			let is_physical = unsafe { (*self.spans[node.index()].get()).is_some() };
			let newline_known = !metrics_pending
				|| !is_physical
				|| self.metrics_inflated[node.index()].load(Ordering::Acquire);
			entries.push(RootChildLineIndexEntry::new(
				node,
				metrics.byte_length,
				known_line_metric(
					newline_known,
					metrics.newlines,
					self.indexed_terminal_newline(node),
				),
			));
			child = self.get_next_sibling(node);
		}

		RootChildLineIndex::from_entries(entries)
	}

	pub(crate) fn install_root_child_line_index(&self, root: NodeId) {
		let index = self.build_root_child_line_index(root);
		self.root_child_line_index_full_builds
			.fetch_add(1, Ordering::Relaxed);
		self.root_child_line_indexes
			.write()
			.unwrap()
			.insert(root, index);
		self.note_root_child_line_index_change(0);
	}

	pub(crate) fn remove_root_child_line_index(&self, root: NodeId) {
		if self
			.root_child_line_indexes
			.write()
			.unwrap()
			.remove(&root)
			.is_some()
		{
			self.note_root_child_line_index_change(0);
		}
	}

	pub(crate) fn inspect_root_child_line_index(
		&self,
		root: NodeId,
		cursor_byte: Option<DocByte>,
		max_rows: usize,
	) -> Option<RootChildLineIndexInspectorSnapshot> {
		self.root_child_line_indexes
			.read()
			.unwrap()
			.get(&root)
			.map(|index| index.inspector_snapshot(cursor_byte, max_rows))
	}

	pub(crate) fn refresh_installed_root_child_line_index_for_node(&self, node: NodeId) {
		let root = self.document_root_for_node(node);
		let mut root_child = node;
		while let Some(parent) = unsafe { (*self.edges[root_child.index()].get()).parent } {
			if parent == root {
				break;
			}
			root_child = parent;
		}

		let _resident_read = self.resident_data_guard();
		let mut indexes = self.root_child_line_indexes.write().unwrap();
		let Some(index) = indexes.get_mut(&root) else {
			return;
		};
		let changes = if !index.contains(root_child) {
			// New direct children are normally reconciled from their existing
			// predecessor by the mutation path. Keep a bounded fallback for a new
			// tail by anchoring at the last indexed child.
			let mut next_known = Some(root_child);
			while let Some(candidate) = next_known {
				if index.contains(candidate) {
					break;
				}
				next_known = self.get_next_sibling(candidate);
			}
			let anchor = next_known
				.and_then(|next| index.location_for_node(next))
				.and_then(|next| {
					next.rank
						.checked_sub(1)
						.and_then(|rank| index.node_at(rank))
				});
			self.reconcile_root_child_index_after_locked(root, anchor, index)
		} else {
			let mut changes =
				u64::from(index.update_changed(self.root_child_index_entry(root, root_child)));
			if unsafe { (*self.edges[root_child.index()].get()).parent } == Some(root) {
				changes = changes.saturating_add(self.reconcile_root_child_index_after_locked(
					root,
					Some(root_child),
					index,
				));
			}
			changes
		};
		drop(indexes);
		if changes > 0 {
			self.note_root_child_line_index_change(changes);
		}
	}

	pub(crate) fn reconcile_installed_root_child_line_index_after(
		&self,
		root: NodeId,
		anchor: Option<NodeId>,
	) {
		let _resident_read = self.resident_data_guard();
		let mut indexes = self.root_child_line_indexes.write().unwrap();
		let Some(index) = indexes.get_mut(&root) else {
			return;
		};
		let changes = self.reconcile_root_child_index_after_locked(root, anchor, index);
		drop(indexes);
		if changes > 0 {
			self.note_root_child_line_index_change(changes);
		}
	}

	fn reconcile_root_child_index_after_locked(
		&self,
		root: NodeId,
		anchor: Option<NodeId>,
		index: &mut RootChildLineIndex,
	) -> u64 {
		let mut changes = 0u64;
		let mut anchor = anchor.filter(|node| index.contains(*node));
		let mut desired = anchor
			.and_then(|node| self.get_next_sibling(node))
			.or_else(|| {
				anchor
					.is_none()
					.then(|| self.get_first_child(root))
					.flatten()
			});

		loop {
			let indexed_next = match anchor {
				Some(node) => index.successor(node),
				None => index.first().map(|location| location.entry.node_id),
			};
			if desired == indexed_next {
				break;
			}
			match desired {
				Some(node) if index.contains(node) => {
					let Some(stale) = indexed_next else {
						break;
					};
					changes = changes.saturating_add(u64::from(index.remove(stale)));
				}
				Some(node) => {
					index.insert_after(anchor, self.root_child_index_entry(root, node));
					changes = changes.saturating_add(1);
					anchor = Some(node);
					desired = self.get_next_sibling(node);
				}
				None => {
					let Some(stale) = indexed_next else {
						break;
					};
					changes = changes.saturating_add(u64::from(index.remove(stale)));
				}
			}
		}
		changes
	}

	fn root_child_index_entry(&self, root: NodeId, node: NodeId) -> RootChildLineIndexEntry {
		let metrics = unsafe { &*self.metrics[node.index()].get() };
		let is_physical = unsafe { (*self.spans[node.index()].get()).is_some() };
		let newline_known = self.exact_line_count(root).is_some()
			|| !is_physical
			|| self.metrics_inflated[node.index()].load(Ordering::Acquire);
		RootChildLineIndexEntry::new(
			node,
			metrics.byte_length,
			known_line_metric(
				newline_known,
				metrics.newlines,
				self.indexed_terminal_newline(node),
			),
		)
	}

	pub(crate) fn update_root_child_line_index_metric(
		&self,
		node: NodeId,
		_newline_count: u64,
		_ends_with_newline: bool,
	) {
		let root = self.document_root_for_node(node);
		let mut root_child = node;
		while let Some(parent) = unsafe { (*self.edges[root_child.index()].get()).parent } {
			if parent == root {
				break;
			}
			root_child = parent;
		}
		let _resident_read = self.resident_data_guard();
		let mut indexes = self.root_child_line_indexes.write().unwrap();
		let Some(index) = indexes.get_mut(&root) else {
			return;
		};
		let changed = index.update_changed(self.root_child_index_entry(root, root_child));
		drop(indexes);
		if changed {
			self.note_root_child_line_index_change(1);
		}
	}

	pub(crate) fn next_node_skipping_children(&self, node: NodeId) -> Option<NodeId> {
		unsafe {
			let mut curr = node;
			loop {
				if let Some(sib) = (*self.edges[curr.index()].get()).next_sibling {
					return Some(sib);
				}
				curr = (*self.edges[curr.index()].get()).parent?;
			}
		}
	}

	fn find_node_at_line_col_internal<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_line: DocLine,
		target_col: DocDisplayCol,
		line_index: Option<&RootChildLineIndex>,
	) -> NodeLineLookup {
		let _resident_read = self.resident_data_guard();
		let (mut curr, mut line_accumulator, _) =
			seek_root_child_for_line(self, root, target_line, line_index);
		let mut last_effective_leaf: Option<NodeCursorTarget> = None;
		let mut target_line_visual_prefix = DocDisplayCol::ZERO;

		while let Some(node) = curr {
			let idx = node.index();
			let has_children = unsafe { (*self.edges[idx].get()).first_child.is_some() };
			let is_folded = has_children && folds.is_folded(node);

			let metrics = unsafe { &*self.metrics[idx].get() };

			let node_end_line = line_accumulator.saturating_add(metrics.newlines);
			let contains_target_line = target_line < node_end_line
				|| (target_line == node_end_line
					&& (!is_folded || self.next_node_skipping_children(node).is_none()));

			if !contains_target_line {
				// Branch metrics can lag behind their children until leaves inflate and
				// propagate newline deltas upward. When that happens, descend instead of
				// trusting the stale aggregate and skipping the whole subtree.
				if has_children && !is_folded && !self.metrics_inflated[idx].load(Ordering::Acquire)
				{
					curr = self.get_first_child(node);
					continue;
				}

				line_accumulator = node_end_line;
				curr = self.next_node_skipping_children(node);
				continue;
			}

			if has_children && !is_folded {
				curr = self.get_first_child(node);
				continue;
			}

			last_effective_leaf = Some(if is_folded {
				NodeCursorTarget {
					node_id: node,
					node_byte: NodeByteOffset::ZERO,
					display_col: DocDisplayCol::ZERO,
				}
			} else {
				let local_byte_length = u32::try_from(metrics.byte_length)
					.expect("effective leaf length fits NodeByteOffset");
				NodeCursorTarget {
					node_id: node,
					node_byte: NodeByteOffset::new(local_byte_length),
					display_col: DocDisplayCol::ZERO,
				}
			});

			if contains_target_line {
				if is_folded {
					return NodeLineLookup::Ready(NodeCursorTarget {
						node_id: node,
						node_byte: NodeByteOffset::ZERO,
						display_col: DocDisplayCol::ZERO,
					});
				}

				let Some(bytes) = (unsafe { (&*self.virtual_data[idx].get()).as_deref() }) else {
					if target_line == line_accumulator
						&& target_col == DocDisplayCol::ZERO
						&& target_line_visual_prefix == DocDisplayCol::ZERO
					{
						return NodeLineLookup::Ready(NodeCursorTarget {
							node_id: node,
							node_byte: NodeByteOffset::ZERO,
							display_col: DocDisplayCol::ZERO,
						});
					}
					// Interactive coordinate lookup must never read the backing store.
					// The extent is known, but its node-local line offset is not. Publishing
					// offset zero as a ready answer would let an edit land at the start of
					// the extent before hydration completes.
					return NodeLineLookup::Pending {
						indexed_through_line: target_line,
						indexed_through_byte: DocByte::ZERO,
						required_node: Some(node),
					};
				};
				let target_line_in_node = target_line.saturating_sub(line_accumulator.get());
				if line_starts_at_node_end(bytes, target_line_in_node)
					&& self.next_node_skipping_children(node).is_some()
				{
					line_accumulator = node_end_line;
					curr = self.next_node_skipping_children(node);
					continue;
				}
				if let Some(line_start) = line_start_offset(bytes, target_line_in_node) {
					let line_bytes = &bytes[line_start..];
					let segment_width = visual_width_for_line_bytes(line_bytes);
					let local_target_col =
						target_col.saturating_sub(target_line_visual_prefix.get());
					let line_ends_in_node = line_bytes.contains(&b'\n');
					let line_start_line = if target_line_in_node > DocLine::ZERO {
						target_line
					} else {
						line_accumulator
					};

					if !line_ends_in_node && local_target_col > segment_width {
						target_line_visual_prefix = target_line_visual_prefix
							.checked_add(segment_width.get())
							.expect("DocDisplayCol addition overflow");
						line_accumulator = line_start_line;
						curr = self.next_node_skipping_children(node);
						continue;
					}

					let (line_offset, clamped_col) =
						offset_for_visual_col(line_bytes, local_target_col.min(segment_width));
					return NodeLineLookup::Ready(NodeCursorTarget {
						node_id: node,
						node_byte: NodeByteOffset::new(line_start as u32 + line_offset.get()),
						display_col: target_line_visual_prefix
							.checked_add(clamped_col.get())
							.expect("DocDisplayCol addition overflow"),
					});
				}

				return NodeLineLookup::Ready(NodeCursorTarget {
					node_id: node,
					node_byte: NodeByteOffset::new(bytes.len() as u32),
					display_col: DocDisplayCol::ZERO,
				});
			}

			line_accumulator = node_end_line;
			curr = self.next_node_skipping_children(node);
		}

		NodeLineLookup::Ready(last_effective_leaf.unwrap_or(NodeCursorTarget {
			node_id: root,
			node_byte: NodeByteOffset::ZERO,
			display_col: DocDisplayCol::ZERO,
		}))
	}

	fn find_node_at_doc_byte_internal<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_byte: DocByte,
		max_nodes: Option<usize>,
	) -> Result<(NodeByteTarget, usize), ()> {
		let root_len = unsafe { (*self.metrics[root.index()].get()).byte_length };
		let clamped_target = target_byte.get().min(root_len);
		let mut root_metrics_exact = None;
		let indexed_start = {
			let indexes = self.root_child_line_indexes.read().unwrap();
			indexes
				.get(&root)
				.and_then(|index| index.seek_byte(DocByte::new(clamped_target)))
		};

		// The root-child index remains an optional seek accelerator for very wide,
		// flat sparse documents. It seeds the same topology descent rather than
		// returning early, so indexed branches and post-graft nested trees share one
		// byte/newline accumulation path.
		let (mut curr, mut byte_accumulator, mut line_accumulator) = indexed_start
			.map(|entry| {
				(
					Some(entry.entry.node_id),
					entry.start_byte,
					entry.start_line,
				)
			})
			.unwrap_or((Some(root), DocByte::ZERO, Some(DocLine::ZERO)));
		let mut last_effective_leaf: Option<NodeByteTarget> = None;
		let mut visited = 0usize;

		while let Some(node) = curr {
			visited = visited.saturating_add(1);
			if max_nodes.is_some_and(|maximum| visited > maximum) {
				return Err(());
			}
			let idx = node.index();
			let has_children = unsafe { (*self.edges[idx].get()).first_child.is_some() };
			let is_folded = has_children && folds.is_folded(node);
			let metrics = unsafe { &*self.metrics[idx].get() };
			let node_start = byte_accumulator;
			let node_end = node_start.saturating_add(metrics.byte_length);

			if clamped_target < node_end.get()
				|| (clamped_target == root_len && node_end.get() == root_len)
			{
				if has_children && !is_folded {
					curr = self.get_first_child(node);
					continue;
				}

				let local_byte_length = u32::try_from(metrics.byte_length)
					.expect("effective leaf length fits NodeByteOffset");
				let local_offset = if is_folded {
					0
				} else {
					u32::try_from(clamped_target.saturating_sub(node_start.get()))
						.expect("target offset fits effective leaf")
				};
				return Ok((
					NodeByteTarget {
						node_id: node,
						node_start_byte: node_start,
						node_end_byte: node_end,
						node_byte: NodeByteOffset::new(local_offset.min(local_byte_length)),
						node_start_line: line_accumulator,
					},
					visited,
				));
			}

			let local_byte_length = u32::try_from(metrics.byte_length)
				.expect("effective leaf length fits NodeByteOffset");
			last_effective_leaf = Some(NodeByteTarget {
				node_id: node,
				node_start_byte: node_start,
				node_end_byte: node_end,
				node_byte: NodeByteOffset::new(if is_folded { 0 } else { local_byte_length }),
				node_start_line: line_accumulator,
			});

			if let Some(line) = line_accumulator {
				let locally_exact_leaf = !has_children
					&& (unsafe { (*self.spans[idx].get()).is_none() }
						|| self.metrics_inflated[idx].load(Ordering::Acquire));
				let newline_metric_exact = locally_exact_leaf
					|| *root_metrics_exact
						.get_or_insert_with(|| self.exact_line_count(root).is_some());
				line_accumulator =
					newline_metric_exact.then(|| line.saturating_add(metrics.newlines));
			}
			byte_accumulator = node_end;
			curr = self.next_node_skipping_children(node);
		}

		Ok((
			last_effective_leaf.unwrap_or(NodeByteTarget {
				node_id: root,
				node_start_byte: DocByte::ZERO,
				node_end_byte: DocByte::ZERO,
				node_byte: NodeByteOffset::ZERO,
				node_start_line: Some(DocLine::ZERO),
			}),
			visited,
		))
	}

	pub(crate) fn find_node_at_line_col_raw(
		&self,
		root: NodeId,
		target_line: DocLine,
		target_col: DocDisplayCol,
	) -> NodeLineLookup {
		if let LineLookupState::Pending {
			indexed_through_line,
			indexed_through_byte,
		} = self.line_lookup_state(root, target_line)
		{
			return NodeLineLookup::Pending {
				indexed_through_line,
				indexed_through_byte,
				required_node: None,
			};
		}
		self.find_node_at_line_col_internal(root, &NoFolds, target_line, target_col, None)
	}

	#[cfg(test)]
	pub(crate) fn find_node_at_line_col_raw_with_root_line_index(
		&self,
		root: NodeId,
		target_line: DocLine,
		target_col: DocDisplayCol,
		line_index: &RootChildLineIndex,
	) -> NodeLineLookup {
		self.find_node_at_line_col_internal(
			root,
			&NoFolds,
			target_line,
			target_col,
			Some(line_index),
		)
	}

	#[cfg(test)]
	pub(crate) fn find_node_at_line_col_with_root_line_index(
		&self,
		root: NodeId,
		target_line: DocLine,
		target_col: DocDisplayCol,
		line_index: &RootChildLineIndex,
	) -> NodeCursorTarget {
		self.find_node_at_line_col_internal(
			root,
			&NoFolds,
			target_line,
			target_col,
			Some(line_index),
		)
		.ready()
		.expect("test line lookup should be resident")
	}

	pub(crate) fn find_node_at_doc_byte_raw(
		&self,
		root: NodeId,
		target_byte: DocByte,
	) -> NodeByteTarget {
		self.find_node_at_doc_byte_internal(root, &NoFolds, target_byte, None)
			.map(|(target, _)| target)
			.expect("unbounded byte lookup cannot exhaust a node budget")
	}

	#[allow(dead_code)] // Consumed by bounded native buffer operations.
	pub(crate) fn find_node_at_doc_byte_raw_bounded(
		&self,
		root: NodeId,
		target_byte: DocByte,
		max_nodes: usize,
	) -> Result<(NodeByteTarget, usize), ()> {
		self.find_node_at_doc_byte_internal(root, &NoFolds, target_byte, Some(max_nodes))
	}

	pub fn for_each_loaded_slice_fragment<F>(
		&self,
		root: NodeId,
		start: DocByte,
		end: DocByte,
		mut callback: F,
	) -> Result<(), &'static str>
	where
		F: FnMut(&[u8]),
	{
		let _resident_read = self.resident_data_guard();
		let root_len = self.get_total_bytes(root);
		let start = start.get().min(root_len);
		let end = end.get().min(root_len);
		if start >= end {
			return Ok(());
		}

		let start_target = self.find_node_at_doc_byte_raw(root, DocByte::new(start));
		let mut visit = Some(start_target.node_id);
		let mut local_start = start_target.node_byte.get() as usize;
		let mut absolute_start = start_target.node_start_byte;
		let mut remaining = (end - start) as usize;

		while let Some(node) = visit {
			let idx = node.index();
			let has_children = unsafe { (*self.edges[idx].get()).first_child.is_some() };
			if has_children {
				visit = self.get_first_child(node);
				continue;
			}

			let node_len = unsafe { (*self.metrics[idx].get()).byte_length as usize };
			if node_len > 0 && absolute_start.get() < end {
				let bytes = unsafe {
					if let Some(v_data) = &*self.virtual_data[idx].get() {
						v_data.as_slice()
					} else if (*self.spans[idx].get()).is_some() {
						return Err("File still loading, cannot read slice yet");
					} else {
						return Err("Leaf text unavailable");
					}
				};

				let take_len = (node_len.saturating_sub(local_start)).min(remaining);
				if take_len > 0 {
					callback(&bytes[local_start..local_start + take_len]);
					remaining -= take_len;
					if remaining == 0 {
						return Ok(());
					}
				}
			}

			absolute_start = absolute_start.saturating_add(node_len as u64);
			local_start = 0;
			visit = self.get_next_node_in_walk(node);
		}

		if remaining == 0 {
			Ok(())
		} else {
			Err("Slice exceeded loaded document bounds")
		}
	}
}

fn seek_root_child_for_line(
	registry: &UastRegistry,
	root: NodeId,
	target_line: DocLine,
	line_index: Option<&RootChildLineIndex>,
) -> (Option<NodeId>, DocLine, DocByte) {
	fn seek(
		line_index: &RootChildLineIndex,
		target_line: DocLine,
	) -> (Option<NodeId>, DocLine, DocByte) {
		if line_index.is_empty() {
			return (None, DocLine::ZERO, DocByte::ZERO);
		}
		if target_line == DocLine::ZERO {
			let entry = line_index.first().expect("non-empty line index");
			return (Some(entry.entry.node_id), DocLine::ZERO, entry.start_byte);
		}

		if line_index.known_prefix_len() == 0 {
			return (None, DocLine::ZERO, DocByte::ZERO);
		}
		let last_known = line_index
			.known_prefix_boundary()
			.expect("known prefix boundary");
		let last_known_line = last_known.end_line().expect("known prefix end");
		let last_known_ends_with_newline = last_known
			.entry
			.ends_with_newline()
			.expect("known prefix terminal state");
		if line_index.known_prefix_len() < line_index.len()
			&& (target_line > last_known_line
				|| (target_line == last_known_line && last_known_ends_with_newline))
		{
			return (None, last_known_line, last_known.end_byte());
		}

		let entry = line_index
			.seek_line(target_line)
			.expect("ready target line has an indexed extent");
		(
			Some(entry.entry.node_id),
			entry.start_line.expect("known prefix start"),
			entry.start_byte,
		)
	}

	if let Some(line_index) = line_index {
		return seek(line_index, target_line);
	}
	let indexes = registry.root_child_line_indexes.read().unwrap();
	let Some(line_index) = indexes.get(&root) else {
		return (Some(root), DocLine::ZERO, DocByte::ZERO);
	};
	seek(line_index, target_line)
}

impl UastProjection for UastRegistry {
	fn query_viewport<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_line: DocLine,
		line_count: u32,
	) -> Vec<RenderToken> {
		let (start_node, start_line, start_byte) =
			seek_root_child_for_line(self, root, target_line, None);
		self.query_viewport_internal(
			root,
			folds,
			target_line,
			line_count,
			start_node,
			start_line,
			start_byte,
			LineDiscovery::Unbounded,
			None,
		)
		.0
	}

	fn get_next_node_in_walk(&self, node: NodeId) -> Option<NodeId> {
		let index = self.checked_node_index(node)?;
		unsafe {
			if let Some(child) = (*self.edges[index].get())
				.first_child
				.filter(|child| self.is_current_node(*child))
			{
				return Some(child);
			}

			let mut curr = node;
			loop {
				let curr_index = self.checked_node_index(curr)?;
				if let Some(sib) = (*self.edges[curr_index].get())
					.next_sibling
					.filter(|sibling| self.is_current_node(*sibling))
				{
					return Some(sib);
				}
				curr = (*self.edges[curr_index].get())
					.parent
					.filter(|parent| self.is_current_node(*parent))?;
			}
		}
	}

	fn find_node_at_line_col<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_line: DocLine,
		target_col: DocDisplayCol,
	) -> NodeLineLookup {
		if let LineLookupState::Pending {
			indexed_through_line,
			indexed_through_byte,
		} = self.line_lookup_state(root, target_line)
		{
			return NodeLineLookup::Pending {
				indexed_through_line,
				indexed_through_byte,
				required_node: None,
			};
		}
		self.find_node_at_line_col_internal(root, folds, target_line, target_col, None)
	}

	fn doc_byte_for_node_offset(
		&self,
		root: NodeId,
		node: NodeId,
		node_offset: NodeByteOffset,
	) -> DocByte {
		if let Some(start_byte) = {
			let indexes = self.root_child_line_indexes.read().unwrap();
			indexes
				.get(&root)
				.and_then(|index| index.location_for_node(node))
				.map(|location| location.start_byte)
		} {
			return start_byte.saturating_add(u64::from(node_offset.get()));
		}

		let mut absolute = DocByte::new(u64::from(node_offset.get()));
		let mut curr = node;

		while curr != root {
			let Some(parent) = (unsafe { (*self.edges[curr.index()].get()).parent }) else {
				break;
			};

			let mut sibling = unsafe { (*self.edges[parent.index()].get()).first_child };
			while let Some(candidate) = sibling {
				if candidate == curr {
					break;
				}

				let sibling_len = unsafe { (*self.metrics[candidate.index()].get()).byte_length };
				absolute = absolute.saturating_add(sibling_len);
				sibling = unsafe { (*self.edges[candidate.index()].get()).next_sibling };
			}

			curr = parent;
		}

		absolute
	}

	fn read_loaded_slice(
		&self,
		root: NodeId,
		start: DocByte,
		end: DocByte,
	) -> Result<Vec<u8>, &'static str> {
		let root_len = self.get_total_bytes(root);
		let start = start.get().min(root_len);
		let end = end.get().min(root_len);
		let requested = usize::try_from(end.saturating_sub(start))
			.map_err(|_| "Loaded slice does not fit this platform's address space")?;
		if requested > 256 * 1024 * 1024 {
			return Err("Loaded slice exceeds the bounded operation limit");
		}
		let mut result = Vec::new();
		result
			.try_reserve_exact(requested)
			.map_err(|_| "Unable to reserve memory for loaded slice")?;
		self.for_each_loaded_slice_fragment(
			root,
			DocByte::new(start),
			DocByte::new(end),
			|fragment| {
				result.extend_from_slice(fragment);
			},
		)?;
		Ok(result)
	}

	fn find_node_at_doc_byte<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_byte: DocByte,
	) -> NodeByteTarget {
		self.find_node_at_doc_byte_internal(root, folds, target_byte, None)
			.map(|(target, _)| target)
			.expect("unbounded byte lookup cannot exhaust a node budget")
	}
}

impl UastRegistry {
	#[cfg(test)]
	pub(crate) fn query_viewport_with_root_line_index(
		&self,
		root: NodeId,
		target_line: DocLine,
		line_count: u32,
		line_index: Option<&RootChildLineIndex>,
	) -> Vec<RenderToken> {
		let (curr, line_accumulator, byte_accumulator) =
			seek_root_child_for_line(self, root, target_line, line_index);
		self.query_viewport_internal(
			root,
			&NoFolds,
			target_line,
			line_count,
			curr,
			line_accumulator,
			byte_accumulator,
			LineDiscovery::Unbounded,
			None,
		)
		.0
	}

	/// [`UastProjection::query_viewport`] for a renderer, with a bound on how
	/// much of each line it may discover. A capped line resumes at the next
	/// line only when the root's line metrics are exact; otherwise the
	/// projection ends at the capped line.
	pub(crate) fn query_viewport_with_line_discovery<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_line: DocLine,
		line_count: u32,
		discovery: LineDiscovery,
	) -> Vec<RenderToken> {
		let mut tokens = Vec::new();
		let mut next = Some((target_line, line_count));
		while let Some((line, count)) = next {
			let (start_node, start_line, start_byte) =
				seek_root_child_for_line(self, root, line, None);
			let (mut part, resume) = self.query_viewport_internal(
				root, folds, line, count, start_node, start_line, start_byte, discovery, None,
			);
			tokens.append(&mut part);
			next = resume;
		}
		tokens
	}

	/// [`Self::query_viewport_with_line_discovery`] for a view that starts
	/// inside `line`, at `start_byte` (ADR 0009's row window): the first token
	/// begins there, and `line`'s discovery cap counts from it.
	pub(crate) fn query_viewport_from_byte<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		start_byte: DocByte,
		line: DocLine,
		line_count: u32,
		discovery: LineDiscovery,
	) -> Vec<RenderToken> {
		let target = self.find_node_at_doc_byte_raw(root, start_byte);
		let (mut tokens, mut next) = self.query_viewport_internal(
			root,
			folds,
			line,
			line_count,
			Some(target.node_id),
			line,
			target.node_start_byte,
			discovery,
			Some(start_byte),
		);
		while let Some((line, count)) = next {
			let (start_node, start_line, start_byte) =
				seek_root_child_for_line(self, root, line, None);
			let (mut part, resume) = self.query_viewport_internal(
				root, folds, line, count, start_node, start_line, start_byte, discovery, None,
			);
			tokens.append(&mut part);
			next = resume;
		}
		tokens
	}

	/// Returns the tokens, and the line and count to resume at when
	/// `discovery` capped a line whose successor is seekable.
	#[allow(clippy::too_many_arguments)]
	fn query_viewport_internal<F: FoldPolicy + ?Sized>(
		&self,
		root: NodeId,
		folds: &F,
		target_line: DocLine,
		line_count: u32,
		start_node: Option<NodeId>,
		start_line: DocLine,
		start_byte: DocByte,
		discovery: LineDiscovery,
		window_start: Option<DocByte>,
	) -> (Vec<RenderToken>, Option<(DocLine, u32)>) {
		let mut tokens = Vec::new();
		let (mut curr, mut line_accumulator, mut byte_accumulator) =
			(start_node, start_line, start_byte);
		let line_count = line_count.max(1);

		// PHASE 1: DESCENT. Subtree metrics skip whole siblings, but the
		// sibling search itself is linear unless an index supplied start_node.
		let mut descent_node_visits = 0u64;
		while let Some(node) = curr {
			descent_node_visits = descent_node_visits.saturating_add(1);
			let idx = node.index();
			let m = unsafe { &*self.metrics[idx].get() };
			let has_children = unsafe { (*self.edges[idx].get()).first_child.is_some() };
			let is_folded = has_children && folds.is_folded(node);
			let node_end_line = line_accumulator.saturating_add(m.newlines);
			let contains_target_line = target_line < node_end_line
				|| (target_line == node_end_line
					&& (!is_folded || self.next_node_skipping_children(node).is_none()));

			if contains_target_line {
				if !has_children && !is_folded && target_line == node_end_line {
					let text = self.resolve_physical_bytes(node);
					let target_line_in_node = target_line.saturating_sub(line_accumulator.get());
					// A line can begin at this leaf's end only after a newline inside it. Line 0
					// of the leaf began at or before its start, and an unloaded leaf's empty text
					// would otherwise look like that newline, skipping to the line's last leaf.
					if target_line_in_node > DocLine::ZERO
						&& line_starts_at_node_end(&text, target_line_in_node)
						&& self.next_node_skipping_children(node).is_some()
					{
						line_accumulator = node_end_line;
						byte_accumulator = byte_accumulator.saturating_add(m.byte_length);
						curr = self.next_node_skipping_children(node);
						continue;
					}
				}
				if has_children && !is_folded {
					let first_child = unsafe { (*self.edges[idx].get()).first_child };
					if let Some(child) = first_child {
						curr = Some(child);
						continue;
					}
				} else {
					break;
				}
			} else {
				line_accumulator = line_accumulator.saturating_add(m.newlines);
				byte_accumulator = byte_accumulator.saturating_add(m.byte_length);
				let next_sibling = unsafe { (*self.edges[idx].get()).next_sibling };
				curr = next_sibling;
			}
		}
		if descent_node_visits != 0 {
			// Keep instrumentation off the per-node hot path: one relaxed update
			// records the local work total after the seek completes.
			self.viewport_descent_node_visits
				.fetch_add(descent_node_visits, Ordering::Relaxed);
		}

		// PHASE 2: ITERATION
		let mut collected_lines = 0;
		let mut visit = curr;
		let line_cap = match discovery {
			LineDiscovery::Unbounded => None,
			LineDiscovery::PerLineBytes(cap) => Some(cap),
		};
		// Where the line being collected starts. An unloaded leaf hides where
		// its newlines are, so a line beginning inside one is counted from that
		// leaf's end: discovery then takes at most the cap plus that one leaf.
		let mut open_line_start: Option<DocByte> = None;

		while let Some(node) = visit {
			let idx = node.index();
			let has_children = unsafe { (*self.edges[idx].get()).first_child.is_some() };
			let is_folded = has_children && folds.is_folded(node);

			if has_children && !is_folded {
				visit = self.get_first_child(node);
				continue;
			}

			let m = unsafe { &*self.metrics[idx].get() };
			let node_end_line = line_accumulator.saturating_add(m.newlines);

			if m.byte_length == 0 && m.newlines == 0 {
				visit = self.next_node_skipping_children(node);
				continue;
			}

			let text = if is_folded {
				FOLDED_TOKEN_TEXT.as_bytes().to_vec()
			} else {
				self.resolve_physical_bytes(node)
			};

			let is_virtual = is_folded || unsafe { (*self.spans[idx].get()).is_none() };
			let kind = unsafe { *self.kinds[idx].get() };

			let mut display_text = text.clone();
			let mut token_start_line = line_accumulator;
			let mut token_start_byte = byte_accumulator;

			if is_folded && line_accumulator < target_line {
				let skip_fold = node_end_line < target_line
					|| (node_end_line == target_line
						&& self.next_node_skipping_children(node).is_some());
				if skip_fold {
					line_accumulator = node_end_line;
					byte_accumulator = byte_accumulator.saturating_add(m.byte_length);
					visit = self.next_node_skipping_children(node);
					continue;
				}
			}

			if !is_folded && line_accumulator < target_line {
				if line_accumulator + DocLineCount::new(m.newlines) < target_line {
					line_accumulator = node_end_line;
					byte_accumulator = byte_accumulator.saturating_add(m.byte_length);
					visit = self.next_node_skipping_children(node);
					continue;
				}

				if !text.is_empty() {
					let to_skip = target_line.saturating_sub(line_accumulator.get());
					let byte_offset = line_start_offset(&text, to_skip).unwrap_or(text.len());
					if byte_offset == text.len() && self.next_node_skipping_children(node).is_some()
					{
						line_accumulator = node_end_line;
						byte_accumulator = byte_accumulator.saturating_add(m.byte_length);
						visit = self.next_node_skipping_children(node);
						continue;
					}
					display_text = text.get(byte_offset..).unwrap_or(&[]).to_vec();
					token_start_byte = token_start_byte.saturating_add(byte_offset as u64);
				}
				line_accumulator = target_line;
				token_start_line = target_line;
			}

			// A row window starts mid-line: its first token begins at the window,
			// and the line's discovery cap counts from there.
			if tokens.is_empty()
				&& !is_folded
				&& let Some(window) = window_start
				&& window > token_start_byte
			{
				let skip = (window.get() - token_start_byte.get()) as usize;
				if !display_text.is_empty() {
					display_text = display_text.get(skip..).unwrap_or(&[]).to_vec();
				}
				token_start_byte = window;
				open_line_start = Some(window);
			}

			if let (Some(cap), Some(line_start)) = (line_cap, open_line_start)
				&& !is_folded
				&& token_start_byte.get().saturating_sub(line_start.get()) >= cap
			{
				let remaining = u64::from(line_count).saturating_sub(collected_lines + 1);
				let next_line = line_accumulator.saturating_add(1);
				let seekable = remaining > 0
					&& next_line.get() <= self.get_total_newlines(root)
					&& !self.inspector_root_metrics_incomplete(root);
				let remaining = u32::try_from(remaining).expect("remaining lines fit line_count");
				return (tokens, seekable.then_some((next_line, remaining)));
			}

			tokens.push(RenderToken {
				node_id: node,
				kind,
				text: display_text,
				physical_byte_len: u32::try_from(m.byte_length)
					.expect("render leaf length fits physical token"),
				absolute_start_line: token_start_line,
				absolute_start_byte: token_start_byte,
				is_virtual,
				is_folded,
			});
			let pushed = &tokens.last().unwrap().text;
			if is_folded {
				open_line_start = Some(token_start_byte.saturating_add(m.byte_length));
			} else if let Some(newline) = pushed.iter().rposition(|&byte| byte == b'\n') {
				open_line_start = Some(token_start_byte.saturating_add(newline as u64 + 1));
			} else if pushed.is_empty() && m.newlines > 0 {
				open_line_start = Some(token_start_byte.saturating_add(m.byte_length));
			} else if open_line_start.is_none() {
				open_line_start = Some(token_start_byte);
			}

			if is_folded {
				line_accumulator = line_accumulator.saturating_add(m.newlines);
				collected_lines += 1;
			} else if text.is_empty() {
				line_accumulator = line_accumulator.saturating_add(m.newlines);
				collected_lines += m.newlines;
			} else {
				let lines_shown = tokens
					.last()
					.unwrap()
					.text
					.iter()
					.filter(|&&b| b == b'\n')
					.count() as u64;
				line_accumulator = line_accumulator.saturating_add(lines_shown);
				collected_lines += lines_shown;
			}

			byte_accumulator = byte_accumulator.saturating_add(m.byte_length);

			if collected_lines >= u64::from(line_count) {
				break;
			}

			visit = self.next_node_skipping_children(node);
		}

		(tokens, None)
	}
}

#[cfg(test)]
mod tests {
	use super::{FoldPolicy, LineLookupState, NodeLineLookup, UastProjection};
	use crate::core::{DocByte, DocDisplayCol, DocLine, NodeByteOffset, TAB_SIZE};
	use crate::ecs::UastRegistry;
	use crate::engine::MAX_SEMANTIC_BYTES;
	use crate::engine::language::GreenNode;
	use crate::engine::structural_graft::apply_graft;
	use crate::svp::csv_parse::CSV_DELIMITER;
	use crate::svp::csv_semantic::build_csv_topology;
	use crate::svp::pointer::SvpPointer;
	use crate::uast::UastMutation;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;
	use crate::uast::topology::TARGET_TREE_FANOUT;
	use std::collections::HashSet;
	use std::sync::Arc;
	use std::sync::atomic::Ordering;
	use std::time::{SystemTime, UNIX_EPOCH};

	impl FoldPolicy for HashSet<crate::ecs::NodeId> {
		fn is_folded(&self, node: crate::ecs::NodeId) -> bool {
			self.contains(&node)
		}

		fn has_folds(&self) -> bool {
			!self.is_empty()
		}
	}

	fn build_document(text: &str) -> (UastRegistry, crate::ecs::NodeId) {
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(2).expect("OOM");
		let newlines = text.bytes().filter(|&b| b == b'\n').count() as u64;
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: text.len() as u64,
				newlines,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: text.len() as u64,
				newlines,
			},
		);
		chunk.append_local_child(root, leaf);
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some(text.as_bytes().to_vec());
		}
		drop(chunk);
		(registry, root)
	}

	fn build_document_bytes(bytes: &[u8]) -> (UastRegistry, crate::ecs::NodeId) {
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(2).expect("OOM");
		let newlines = bytes.iter().filter(|&&b| b == b'\n').count() as u64;
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: bytes.len() as u64,
				newlines,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: bytes.len() as u64,
				newlines,
			},
		);
		chunk.append_local_child(root, leaf);
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some(bytes.to_vec());
		}
		drop(chunk);
		(registry, root)
	}

	fn build_document_with_byte_leaves(chunks: &[&[u8]]) -> (UastRegistry, crate::ecs::NodeId) {
		let capacity =
			u32::try_from(chunks.len().saturating_add(1)).expect("test capacity fits u32");
		let registry = UastRegistry::new(capacity);
		let mut chunk = registry.reserve_chunk(capacity).expect("OOM");
		let byte_length = chunks.iter().map(|bytes| bytes.len() as u64).sum();
		let newlines = chunks
			.iter()
			.flat_map(|bytes| bytes.iter())
			.filter(|&&byte| byte == b'\n')
			.count() as u64;
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length,
				newlines,
			},
		);
		for bytes in chunks {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				None,
				SpanMetrics {
					byte_length: bytes.len() as u64,
					newlines: bytes.iter().filter(|&&byte| byte == b'\n').count() as u64,
				},
			);
			chunk.append_local_child(root, leaf);
			unsafe {
				*registry.virtual_data[leaf.index()].get() = Some(bytes.to_vec());
			}
		}
		drop(chunk);
		(registry, root)
	}

	fn build_document_with_leaves(chunks: &[&str]) -> (UastRegistry, crate::ecs::NodeId) {
		let registry = UastRegistry::new((chunks.len() + 1) as u32 + 4);
		let mut chunk = registry
			.reserve_chunk((chunks.len() + 1) as u32)
			.expect("OOM");
		let full_text: String = chunks.concat();
		let newlines = full_text.bytes().filter(|&b| b == b'\n').count() as u64;
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: full_text.len() as u64,
				newlines,
			},
		);
		for part in chunks {
			let bytes = part.as_bytes();
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				None,
				SpanMetrics {
					byte_length: bytes.len() as u64,
					newlines: bytes.iter().filter(|&&b| b == b'\n').count() as u64,
				},
			);
			chunk.append_local_child(root, leaf);
			unsafe {
				*registry.virtual_data[leaf.index()].get() = Some(bytes.to_vec());
			}
		}
		drop(chunk);
		(registry, root)
	}

	fn csv_source(rows: usize) -> Vec<u8> {
		const ROW: &[u8] = b"a,b,c\n";
		let mut source = Vec::with_capacity(rows.saturating_mul(ROW.len()));
		for _ in 0..rows {
			source.extend_from_slice(ROW);
		}
		source
	}

	fn csv_source_with_size(rows: usize, total_bytes: usize) -> Vec<u8> {
		assert!(rows > 0 && total_bytes >= rows * 2);
		let payload_bytes = total_bytes - rows;
		let payload_per_row = payload_bytes / rows;
		let extra_payload_rows = payload_bytes % rows;
		let mut source = Vec::with_capacity(total_bytes);
		for row in 0..rows {
			let payload = payload_per_row + usize::from(row < extra_payload_rows);
			source.resize(source.len() + payload, b'a');
			source.push(b'\n');
		}
		assert_eq!(source.len(), total_bytes);
		source
	}

	fn build_indexed_flat_csv_document(rows: usize) -> (UastRegistry, crate::ecs::NodeId, Vec<u8>) {
		const ROW: &[u8] = b"a,b,c\n";
		let capacity = rows
			.checked_mul(3)
			.and_then(|count| count.checked_add(8))
			.and_then(|count| u32::try_from(count).ok())
			.expect("test document fits registry capacity");
		let registry = UastRegistry::new(capacity);
		let node_count = u32::try_from(rows + 1).expect("test row count fits u32");
		let mut chunk = registry.reserve_chunk(node_count).expect("OOM");
		let source = csv_source(rows);
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: source.len() as u64,
				newlines: rows as u64,
			},
		);
		for _ in 0..rows {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				None,
				SpanMetrics {
					byte_length: ROW.len() as u64,
					newlines: 1,
				},
			);
			chunk.append_local_child(root, leaf);
			unsafe {
				*registry.virtual_data[leaf.index()].get() = Some(ROW.to_vec());
			}
		}
		registry.install_root_child_line_index(root);
		drop(chunk);
		(registry, root, source)
	}

	fn viewport_descent_visits(
		registry: &UastRegistry,
		root: crate::ecs::NodeId,
		line: usize,
	) -> u64 {
		let before = registry.stats().viewport_descent_node_visits;
		let tokens =
			registry.query_viewport(root, &crate::uast::NoFolds, DocLine::new(line as u64), 1);
		assert!(
			!tokens.is_empty(),
			"query for line {line} should render a token"
		);
		registry
			.stats()
			.viewport_descent_node_visits
			.saturating_sub(before)
	}

	fn green_node_count(node: &GreenNode) -> usize {
		1 + node.children.iter().map(green_node_count).sum::<usize>()
	}

	fn graft_topology(source: &[u8], green: &GreenNode) -> (UastRegistry, crate::ecs::NodeId) {
		let capacity = green_node_count(green)
			.checked_mul(2)
			.and_then(|count| count.checked_add(4))
			.and_then(|count| u32::try_from(count).ok())
			.expect("test topology fits registry capacity");
		let registry = UastRegistry::new(capacity);
		let root = registry.alloc_test_node();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: source.iter().filter(|&&byte| byte == b'\n').count() as u64,
			};
		}
		apply_graft(&registry, root, green, source).expect("test graft is byte-identical");
		(registry, root)
	}

	fn bounded_descent_visit_limit(item_count: usize) -> u64 {
		let mut sibling_levels = 1usize;
		let mut bounded_capacity = TARGET_TREE_FANOUT;
		while item_count > bounded_capacity {
			sibling_levels += 1;
			bounded_capacity = bounded_capacity.saturating_mul(TARGET_TREE_FANOUT);
		}
		(sibling_levels * TARGET_TREE_FANOUT + 8) as u64
	}

	fn build_foldable_document() -> (UastRegistry, crate::ecs::NodeId, crate::ecs::NodeId) {
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(6).expect("OOM");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 17,
				newlines: 3,
			},
		);
		let head = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 5,
				newlines: 1,
			},
		);
		let folded = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 8,
				newlines: 2,
			},
		);
		let folded_a = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 1,
			},
		);
		let folded_b = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 1,
			},
		);
		let tail = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, head);
		chunk.append_local_child(root, folded);
		chunk.append_local_child(root, tail);
		chunk.append_local_child(folded, folded_a);
		chunk.append_local_child(folded, folded_b);
		unsafe {
			*registry.virtual_data[head.index()].get() = Some(b"head\n".to_vec());
			*registry.virtual_data[folded_a.index()].get() = Some(b"one\n".to_vec());
			*registry.virtual_data[folded_b.index()].get() = Some(b"two\n".to_vec());
			*registry.virtual_data[tail.index()].get() = Some(b"tail".to_vec());
		}
		drop(chunk);
		(registry, root, folded)
	}

	fn temp_test_path(name: &str) -> std::path::PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("time should move forward")
			.as_nanos();
		std::env::temp_dir().join(format!("baryon-{}-{}-{}", name, std::process::id(), nanos))
	}

	fn build_uninflated_physical_document(chunks: &[&str]) -> (UastRegistry, crate::ecs::NodeId) {
		let registry = UastRegistry::new((chunks.len() + 1) as u32 + 4);
		let mut chunk = registry
			.reserve_chunk((chunks.len() + 1) as u32)
			.expect("OOM");
		let total_len = chunks.iter().map(|part| part.len() as u64).sum();
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: total_len,
				newlines: 0,
			},
		);

		let mut byte_offset = 0u64;
		for part in chunks {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba: byte_offset / 512,
					byte_length: part.len() as u32,
					device_id: 77,
					head_trim: (byte_offset % 512) as u16,
				}),
				SpanMetrics {
					byte_length: part.len() as u64,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
			byte_offset += part.len() as u64;
		}

		drop(chunk);
		(registry, root)
	}

	fn build_exact_metric_physical_document(chunks: &[&str]) -> (UastRegistry, crate::ecs::NodeId) {
		let registry = UastRegistry::new((chunks.len() + 1) as u32 + 4);
		let mut chunk = registry
			.reserve_chunk((chunks.len() + 1) as u32)
			.expect("OOM");
		let total_len = chunks.iter().map(|part| part.len() as u64).sum();
		let total_newlines = chunks
			.iter()
			.map(|part| part.bytes().filter(|&b| b == b'\n').count() as u64)
			.sum();
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: total_len,
				newlines: total_newlines,
			},
		);

		let mut byte_offset = 0u64;
		for part in chunks {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba: byte_offset / 512,
					byte_length: part.len() as u32,
					device_id: 77,
					head_trim: (byte_offset % 512) as u16,
				}),
				SpanMetrics {
					byte_length: part.len() as u64,
					newlines: part.bytes().filter(|&b| b == b'\n').count() as u64,
				},
			);
			chunk.append_local_child(root, leaf);
			registry.set_terminal_newline(leaf, part.as_bytes().last() == Some(&b'\n'));
			byte_offset += part.len() as u64;
		}

		drop(chunk);
		(registry, root)
	}

	fn inflated_leaf_count(registry: &UastRegistry, root: crate::ecs::NodeId) -> usize {
		let mut count = 0usize;
		let mut leaf = registry.get_first_child(root);
		while let Some(node) = leaf {
			if registry.metrics_inflated[node.index()].load(Ordering::Acquire) {
				count += 1;
			}
			leaf = registry.get_next_sibling(node);
		}
		count
	}

	fn visual_boundaries_for_line(text: &str) -> Vec<DocDisplayCol> {
		let mut boundaries = vec![DocDisplayCol::ZERO];
		let mut col = DocDisplayCol::ZERO;

		for &b in text.as_bytes() {
			if b == b'\n' {
				break;
			}
			if b == b'\t' {
				col = col
					.checked_add(TAB_SIZE - (col.get() % TAB_SIZE))
					.expect("DocDisplayCol addition overflow");
			} else {
				col = col.checked_add(1).expect("DocDisplayCol addition overflow");
			}
			boundaries.push(col);
		}

		boundaries
	}

	#[test]
	fn find_node_at_line_col_clamps_to_visual_width_with_tabs() {
		let (registry, root) = build_document("\t\tlet x = 1;\n");
		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::ZERO,
			DocDisplayCol::new(30),
		);

		assert_eq!(target.node_byte, crate::core::NodeByteOffset::new(12));
		assert_eq!(target.display_col, DocDisplayCol::new(18));
	}

	#[test]
	fn find_node_at_line_col_only_returns_character_boundaries_for_tabbed_lines() {
		let (registry, root) = build_document("\tfoo\n");
		let valid_boundaries = visual_boundaries_for_line("\tfoo\n");

		for clicked_col in 0..=7 {
			let target = registry.find_node_at_line_col(
				root,
				&crate::uast::NoFolds,
				DocLine::ZERO,
				DocDisplayCol::new(clicked_col),
			);
			assert!(
				valid_boundaries.contains(&target.display_col),
				"clicked visual col {} landed at invalid boundary {:?}",
				clicked_col,
				target.display_col,
			);
		}
	}

	#[test]
	fn find_node_at_line_col_snaps_tab_clicks_to_tab_boundary() {
		let (registry, root) = build_document("\tfoo\n");

		for clicked_col in 1..=3 {
			let target = registry.find_node_at_line_col(
				root,
				&crate::uast::NoFolds,
				DocLine::ZERO,
				DocDisplayCol::new(clicked_col),
			);
			assert_eq!(target.node_byte, crate::core::NodeByteOffset::new(1));
			assert_eq!(target.display_col, DocDisplayCol::new(4));
		}
	}

	#[test]
	fn find_node_at_line_col_only_returns_character_boundaries_for_mixed_indent() {
		let (registry, root) = build_document(" \tfoo\n");
		let valid_boundaries = visual_boundaries_for_line(" \tfoo\n");

		for clicked_col in 0..=7 {
			let target = registry.find_node_at_line_col(
				root,
				&crate::uast::NoFolds,
				DocLine::ZERO,
				DocDisplayCol::new(clicked_col),
			);
			assert!(
				valid_boundaries.contains(&target.display_col),
				"clicked visual col {} landed at invalid boundary {:?}",
				clicked_col,
				target.display_col,
			);
		}
	}

	#[test]
	fn find_node_at_doc_byte_tracks_leaf_start_and_local_offset() {
		let (registry, root) = build_document_with_leaves(&["alpha", "beta", "gamma"]);

		let at_start_of_second =
			registry.find_node_at_doc_byte(root, &crate::uast::NoFolds, DocByte::new(5));
		assert_eq!(at_start_of_second.node_start_byte, DocByte::new(5));
		assert_eq!(at_start_of_second.node_end_byte, DocByte::new(9));
		assert_eq!(
			at_start_of_second.node_byte,
			crate::core::NodeByteOffset::ZERO
		);

		let inside_third =
			registry.find_node_at_doc_byte(root, &crate::uast::NoFolds, DocByte::new(11));
		assert_eq!(inside_third.node_start_byte, DocByte::new(9));
		assert_eq!(inside_third.node_end_byte, DocByte::new(14));
		assert_eq!(inside_third.node_byte, crate::core::NodeByteOffset::new(2),);

		let eof = registry.find_node_at_doc_byte(root, &crate::uast::NoFolds, DocByte::new(14));
		assert_eq!(eof.node_start_byte, DocByte::new(9));
		assert_eq!(eof.node_end_byte, DocByte::new(14));
		assert_eq!(eof.node_byte, crate::core::NodeByteOffset::new(5));
	}

	#[test]
	fn byte_descent_uses_the_right_leaf_and_line_at_a_boundary() {
		let (registry, root) = build_document_with_leaves(&["head\n", "tail"]);
		registry.install_root_child_line_index(root);
		let second = registry
			.get_next_sibling(registry.get_first_child(root).expect("first leaf"))
			.expect("second leaf");

		let target = registry.find_node_at_doc_byte_raw(root, DocByte::new(5));

		assert_eq!(target.node_id, second);
		assert_eq!(target.node_byte, NodeByteOffset::ZERO);
		assert_eq!(target.node_start_line, Some(DocLine::new(1)));
	}

	#[test]
	fn byte_descent_accumulates_lines_through_grouped_graft_topology() {
		const ROWS: usize = 128;
		const TARGET_ROW: usize = 93;
		const ROW: &[u8] = b"a,b,c\n";
		let source = csv_source(ROWS);
		let green = build_csv_topology(&source, CSV_DELIMITER);
		let (registry, root) = graft_topology(&source, &green);
		let byte = TARGET_ROW * ROW.len() + 2;

		let target = registry.find_node_at_doc_byte_raw(root, DocByte::new(byte as u64));

		assert_eq!(
			target.node_start_line,
			Some(DocLine::new(TARGET_ROW as u64))
		);
		assert_eq!(
			registry.doc_byte_for_node_offset(root, target.node_id, target.node_byte),
			DocByte::new(byte as u64)
		);
	}

	#[test]
	fn doc_byte_for_node_offset_reconstructs_absolute_positions() {
		let (registry, root) = build_document_with_leaves(&["alpha", "beta", "gamma"]);

		let second = registry.find_node_at_doc_byte(root, &crate::uast::NoFolds, DocByte::new(6));
		assert_eq!(
			registry.doc_byte_for_node_offset(root, second.node_id, second.node_byte),
			DocByte::new(6)
		);

		let third = registry.find_node_at_doc_byte(root, &crate::uast::NoFolds, DocByte::new(11));
		assert_eq!(
			registry.doc_byte_for_node_offset(root, third.node_id, third.node_byte),
			DocByte::new(11)
		);
	}

	#[test]
	fn find_node_at_line_col_prefers_next_leaf_at_trailing_newline_boundary() {
		let (registry, root) = build_document_with_leaves(&["head\n", "tail"]);
		let second = registry
			.get_next_sibling(registry.get_first_child(root).unwrap())
			.unwrap();

		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(1),
			DocDisplayCol::ZERO,
		);

		assert_eq!(target.node_id, second);
		assert_eq!(target.node_byte, crate::core::NodeByteOffset::ZERO);
	}

	#[test]
	fn find_node_at_line_col_reaches_across_split_leaf_line_fragments() {
		let (registry, root) = build_document_with_leaves(&["ab", "cd", "ef\n"]);

		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::ZERO,
			DocDisplayCol::new(4),
		);

		assert_eq!(target.display_col, DocDisplayCol::new(4));
		assert_eq!(
			registry.doc_byte_for_node_offset(root, target.node_id, target.node_byte),
			DocByte::new(4)
		);
	}

	#[test]
	fn production_index_keeps_a_straddling_line_with_its_starting_extent() {
		let chunks = ["aaa\nbbb", "ccc\n"];
		let (registry, root) = build_exact_metric_physical_document(&chunks);
		registry.install_root_child_line_index(root);
		let first = registry.get_first_child(root).unwrap();
		let second = registry.get_next_sibling(first).unwrap();

		let cold = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(1),
			DocDisplayCol::ZERO,
		);
		assert_eq!(
			cold,
			NodeLineLookup::Pending {
				indexed_through_line: DocLine::new(1),
				indexed_through_byte: DocByte::ZERO,
				required_node: Some(first),
			}
		);
		assert_ne!(Some(second), cold.required_node());

		registry.publish_hydrated_data(first, chunks[0].as_bytes().to_vec());
		let ready = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(1),
			DocDisplayCol::ZERO,
		);
		assert_eq!(ready.node_id, first);
		assert_eq!(ready.node_byte, NodeByteOffset::new(4));
		assert_eq!(
			registry.doc_byte_for_node_offset(root, ready.node_id, ready.node_byte),
			DocByte::new(4)
		);
	}

	#[test]
	fn production_index_moves_a_newline_aligned_line_to_the_next_extent() {
		let chunks = ["aaa\n", "bbb"];
		let (registry, root) = build_exact_metric_physical_document(&chunks);
		registry.install_root_child_line_index(root);
		let first = registry.get_first_child(root).unwrap();
		let second = registry.get_next_sibling(first).unwrap();

		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(1),
			DocDisplayCol::ZERO,
		);
		assert_eq!(target.node_id, second);
		assert_eq!(target.node_byte, NodeByteOffset::ZERO);
	}

	#[test]
	fn query_viewport_starts_with_next_leaf_after_trailing_newline_boundary() {
		let (registry, root) = build_document_with_leaves(&["head\n", "tail"]);
		let second = registry
			.get_next_sibling(registry.get_first_child(root).unwrap())
			.unwrap();

		let tokens = registry.query_viewport(root, &crate::uast::NoFolds, DocLine::new(1), 1);

		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].node_id, second);
		assert_eq!(tokens[0].text, b"tail".to_vec());
	}

	#[test]
	fn for_each_loaded_slice_fragment_yields_disjoint_leaf_slices_in_order() {
		let (registry, root) = build_document_with_leaves(&["alpha", "beta", "gamma"]);
		let mut fragments = Vec::new();

		registry
			.for_each_loaded_slice_fragment(root, DocByte::new(3), DocByte::new(11), |fragment| {
				fragments.push(String::from_utf8_lossy(fragment).into_owned());
			})
			.expect("slice fragments");

		assert_eq!(
			fragments,
			vec!["ha".to_string(), "beta".to_string(), "ga".to_string()]
		);
	}

	#[test]
	fn read_loaded_slice_collects_fragments_from_the_fragment_visitor() {
		let (registry, root) = build_document_with_leaves(&["alpha", "beta", "gamma"]);
		let slice = registry
			.read_loaded_slice(root, DocByte::new(3), DocByte::new(11))
			.expect("loaded slice");

		assert_eq!(String::from_utf8(slice).expect("utf8"), "habetaga");
	}

	#[test]
	fn query_viewport_preserves_invalid_utf8_bytes_when_skipping_lines() {
		let (registry, root) = build_document_bytes(b"aa\n\xffbb\ncc");
		let tokens = registry.query_viewport(root, &crate::uast::NoFolds, DocLine::new(1), 2);

		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].absolute_start_byte, DocByte::new(3));
		assert_eq!(tokens[0].text, b"\xffbb\ncc".to_vec());
	}

	#[test]
	fn valid_utf8_remains_reconstructible_across_render_token_boundaries() {
		let skull = "☠".as_bytes();
		let chunks = [&skull[..1], &skull[1..2], &skull[2..]];
		let (registry, root) = build_document_with_byte_leaves(&chunks);

		let tokens = registry.query_viewport(root, &crate::uast::NoFolds, DocLine::ZERO, 1);
		let reconstructed = tokens
			.iter()
			.flat_map(|token| token.text.iter().copied())
			.collect::<Vec<_>>();

		assert_eq!(
			tokens.len(),
			3,
			"the byte store need not align leaves to text"
		);
		assert_eq!(reconstructed, skull);
		assert_eq!(std::str::from_utf8(&reconstructed), Ok("☠"));
	}

	#[test]
	fn query_viewport_starts_a_line_spanning_unloaded_leaves_at_its_first_leaf() {
		let (registry, root) = build_document_with_byte_leaves(&[b"HEAD", b"middle", b"tail"]);
		let mut leaf = registry.get_first_child(root);
		while let Some(node) = leaf {
			unsafe {
				*registry.virtual_data[node.index()].get() = None;
			}
			leaf = registry.get_next_sibling(node);
		}

		let tokens = registry.query_viewport(root, &crate::uast::NoFolds, DocLine::ZERO, 1);

		assert_eq!(
			tokens.first().map(|token| token.absolute_start_byte),
			Some(DocByte::ZERO),
			"line 0 starts in the first leaf even before it is hydrated: {tokens:?}"
		);
	}

	/// A line whose first two 4-byte leaves are unloaded, then two short lines.
	fn document_with_unloaded_long_line() -> (UastRegistry, crate::ecs::NodeId) {
		let (registry, root) =
			build_document_with_byte_leaves(&[b"aaaa", b"bbbb", b"ccc\n", b"next\n", b"last"]);
		let mut leaf = registry.get_first_child(root);
		for _ in 0..2 {
			let node = leaf.expect("unloaded leaf");
			unsafe {
				*registry.virtual_data[node.index()].get() = None;
			}
			leaf = registry.get_next_sibling(node);
		}
		(registry, root)
	}

	fn starts(tokens: &[super::RenderToken]) -> Vec<(u64, u64)> {
		tokens
			.iter()
			.map(|token| {
				(
					token.absolute_start_byte.get(),
					token.absolute_start_line.get(),
				)
			})
			.collect()
	}

	fn joined(tokens: &[super::RenderToken]) -> Vec<u8> {
		tokens
			.iter()
			.flat_map(|token| token.text.iter().copied())
			.collect()
	}

	#[test]
	fn a_byte_window_starts_mid_line_at_its_byte() {
		// One line across leaves, with a three-byte scalar split over two of them.
		let skull = "☠".as_bytes();
		let mut first = b"abcd".to_vec();
		first.push(skull[0]);
		let mut second = skull[1..].to_vec();
		second.extend_from_slice(b"efgh");
		let (registry, root) =
			build_document_with_byte_leaves(&[&first, &second, b"ijkl\n", b"next"]);
		let line: Vec<u8> = [&first[..], &second[..], b"ijkl"].concat();
		// Mid-leaf, at a leaf boundary, on the split scalar, and past it.
		for start in [2u64, 5, 4, 7, 9] {
			let tokens = registry.query_viewport_from_byte(
				root,
				&crate::uast::NoFolds,
				DocByte::new(start),
				DocLine::ZERO,
				1,
				super::LineDiscovery::Unbounded,
			);
			assert_eq!(tokens[0].absolute_start_byte, DocByte::new(start));
			assert_eq!(tokens[0].absolute_start_line, DocLine::ZERO);
			let text = joined(&tokens);
			assert!(
				text.starts_with(&line[start as usize..]),
				"window from {start}: {:?}",
				String::from_utf8_lossy(&text)
			);
		}
	}

	#[test]
	fn a_byte_window_counts_its_discovery_cap_from_the_window() {
		let (registry, root) = build_document_with_byte_leaves(&[
			b"aaaa", b"bbbb", b"cccc", b"dddd", b"eeee", b"ffff",
		]);
		for start in [0u64, 9, 16] {
			let tokens = registry.query_viewport_from_byte(
				root,
				&crate::uast::NoFolds,
				DocByte::new(start),
				DocLine::ZERO,
				1,
				super::LineDiscovery::PerLineBytes(4),
			);
			let starts: Vec<u64> = tokens
				.iter()
				.map(|token| token.absolute_start_byte.get())
				.collect();
			// Nothing four or more bytes past the window is discovered: a window
			// deep in a line discovers no more than one at its start does.
			assert_eq!(starts.first(), Some(&start), "window {start}");
			assert!(
				starts.iter().all(|&byte| byte < start + 4),
				"window {start} discovered {starts:?}"
			);
		}
	}

	#[test]
	fn unbounded_line_discovery_returns_every_leaf_of_a_long_line() {
		let (registry, root) = document_with_unloaded_long_line();
		let tokens = registry.query_viewport_with_line_discovery(
			root,
			&crate::uast::NoFolds,
			DocLine::ZERO,
			3,
			super::LineDiscovery::Unbounded,
		);
		assert_eq!(
			starts(&tokens),
			starts(&registry.query_viewport(root, &crate::uast::NoFolds, DocLine::ZERO, 3))
		);
		assert_eq!(
			starts(&tokens),
			vec![(0, 0), (4, 0), (8, 0), (12, 1), (17, 2)]
		);
	}

	#[test]
	fn capped_line_discovery_stops_the_line_and_resumes_at_an_exact_next_line() {
		let (registry, root) = document_with_unloaded_long_line();
		let tokens = registry.query_viewport_with_line_discovery(
			root,
			&crate::uast::NoFolds,
			DocLine::ZERO,
			3,
			super::LineDiscovery::PerLineBytes(4),
		);
		// Line 0 stops after its first leaf; nothing of its second or third
		// leaf is discovered, and the following lines keep their true bytes.
		assert_eq!(starts(&tokens), vec![(0, 0), (12, 1), (17, 2)]);
	}

	#[test]
	fn capped_line_discovery_ends_at_the_capped_line_while_metrics_are_incomplete() {
		let (registry, root) = document_with_unloaded_long_line();
		registry.mark_root_metrics_incomplete(root);
		let tokens = registry.query_viewport_with_line_discovery(
			root,
			&crate::uast::NoFolds,
			DocLine::ZERO,
			3,
			super::LineDiscovery::PerLineBytes(4),
		);
		assert_eq!(starts(&tokens), vec![(0, 0)]);
	}

	#[test]
	fn frontend_projection_reassembles_valid_utf8_across_render_tokens() {
		let skull = "☠".as_bytes();
		let chunks = [&skull[..1], &skull[1..2], &skull[2..]];
		let (registry, root) = build_document_with_byte_leaves(&chunks);
		let tokens = registry.query_viewport(root, &crate::uast::NoFolds, DocLine::ZERO, 1);

		let projected = super::coalesce_frontend_text_tokens(tokens);

		assert_eq!(projected.len(), 1);
		assert_eq!(projected[0].text, skull);
		assert_eq!(std::str::from_utf8(&projected[0].text), Ok("☠"));
	}

	#[test]
	fn query_viewport_does_not_truncate_dense_leaf_streams() {
		let mut parts = Vec::new();
		for _ in 0..80 {
			parts.push("a".to_string());
		}
		parts.push("\n".to_string());
		for _ in 0..80 {
			parts.push("b".to_string());
		}
		parts.push("\n".to_string());
		for _ in 0..80 {
			parts.push("c".to_string());
		}
		parts.push("\n".to_string());
		parts.push("tail".to_string());

		let expected = parts.concat().into_bytes();
		let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
		let (registry, root) = build_document_with_leaves(&refs);

		let tokens = registry.query_viewport(root, &crate::uast::NoFolds, DocLine::ZERO, 4);
		let mut rendered = Vec::new();
		for token in tokens {
			rendered.extend_from_slice(&token.text);
		}

		assert_eq!(rendered, expected);
	}

	#[test]
	fn root_line_indexed_query_viewport_matches_plain_query_near_eof() {
		let parts = ["head\n", "alpha\nbeta\n", "gamma\ndelta\n", "tail"];
		let (registry, root) = build_document_with_leaves(&parts);
		let line_index = registry.build_root_child_line_index(root);

		let plain = registry.query_viewport(root, &crate::uast::NoFolds, DocLine::new(4), 2);
		let indexed = registry.query_viewport_with_root_line_index(
			root,
			DocLine::new(4),
			2,
			Some(&line_index),
		);

		assert_eq!(indexed.len(), plain.len());
		for (left, right) in indexed.iter().zip(plain.iter()) {
			assert_eq!(left.node_id, right.node_id);
			assert_eq!(left.text, right.text);
			assert_eq!(left.absolute_start_line, right.absolute_start_line);
			assert_eq!(left.absolute_start_byte, right.absolute_start_byte);
		}
	}

	#[test]
	fn root_index_and_grouped_csv_graft_both_bound_seek_cost() {
		const ROWS: usize = 20_000;
		let (registry, root, source) = build_indexed_flat_csv_document(ROWS);
		let positions = [0, ROWS / 2, ROWS - 1];
		let indexed_visits = positions.map(|line| viewport_descent_visits(&registry, root, line));

		assert!(
			indexed_visits.iter().all(|&visits| visits <= 2),
			"the root index should make flat-tree seek work independent of position: {indexed_visits:?}"
		);

		let green = build_csv_topology(&source, CSV_DELIMITER);
		assert_eq!(green.children.len(), ROWS, "one CSV row per sibling");
		apply_graft(&registry, root, &green, &source).expect("test graft is byte-identical");
		assert!(
			!registry
				.root_child_line_indexes
				.read()
				.unwrap()
				.contains_key(&root),
			"the current graft path removes the flat-root index"
		);

		let grafted_visits = positions.map(|line| viewport_descent_visits(&registry, root, line));
		let visit_limit = bounded_descent_visit_limit(ROWS);
		assert!(
			grafted_visits.iter().all(|&visits| visits <= visit_limit),
			"grafted seek should remain bounded by fan-out at every position: visits={grafted_visits:?}, limit={visit_limit}"
		);
	}

	#[test]
	fn grafted_descent_cost_tracks_fan_out_at_constant_document_size() {
		const ROWS: usize = 4_096;
		let source = csv_source(ROWS);
		let (wide_registry, wide_root, _) = build_indexed_flat_csv_document(ROWS);
		wide_registry.remove_root_child_line_index(wide_root);
		let grouped = build_csv_topology(&source, CSV_DELIMITER);
		let (grouped_registry, grouped_root) = graft_topology(&source, &grouped);

		let wide_visits = viewport_descent_visits(&wide_registry, wide_root, ROWS - 1);
		let grouped_visits = viewport_descent_visits(&grouped_registry, grouped_root, ROWS - 1);

		assert!(
			wide_visits >= ROWS as u64,
			"wide topology should scan its row siblings: {wide_visits}"
		);
		assert!(
			grouped_visits <= bounded_descent_visit_limit(ROWS),
			"two bounded sibling levels should cap descent work: {grouped_visits}"
		);
		assert!(
			wide_visits > grouped_visits.saturating_mul(20),
			"same bytes and lines should be cheaper with bounded fan-out: wide={wide_visits}, grouped={grouped_visits}"
		);
	}

	#[test]
	fn grouped_graft_stays_exact_and_seekable_after_growing_past_semantic_limit() {
		const ROWS: usize = TARGET_TREE_FANOUT * TARGET_TREE_FANOUT;
		let before_len = usize::try_from(MAX_SEMANTIC_BYTES - 1).expect("limit fits usize");
		let source = csv_source_with_size(ROWS, before_len);
		let green = build_csv_topology(&source, CSV_DELIMITER);
		let (registry, root) = graft_topology(&source, &green);
		let module = registry.get_first_child(root).expect("grafted module");
		let first_bucket = registry.get_first_child(module).expect("group bucket");
		assert_eq!(
			unsafe { *registry.kinds[first_bucket.index()].get() },
			SemanticKind::None
		);
		assert!(
			!registry
				.root_child_line_indexes
				.read()
				.unwrap()
				.contains_key(&root),
			"a complete-source graft should not retain the sparse extent treap"
		);

		let eof = registry.find_node_at_doc_byte(
			root,
			&crate::uast::NoFolds,
			DocByte::new(before_len as u64),
		);
		registry.insert_text(eof.node_id, eof.node_byte.get(), b"x\n");

		let after_len = MAX_SEMANTIC_BYTES + 1;
		assert_eq!(registry.get_total_bytes(root), after_len);
		assert_eq!(registry.get_total_bytes(module), after_len);
		assert_eq!(registry.get_total_newlines(root), ROWS as u64 + 1);
		assert_eq!(registry.exact_line_count(root), Some(ROWS as u64 + 2));
		assert!(
			!registry
				.root_child_line_indexes
				.read()
				.unwrap()
				.contains_key(&root),
			"crossing the semantic limit must not require rebuilding a treap"
		);

		let last_content_line = ROWS;
		let visits = viewport_descent_visits(&registry, root, last_content_line);
		assert!(
			visits <= bounded_descent_visit_limit(ROWS),
			"the retained grouping spine should keep post-threshold seeks bounded: {visits}"
		);
		let rendered = registry
			.query_viewport(
				root,
				&crate::uast::NoFolds,
				DocLine::new(last_content_line as u64),
				1,
			)
			.into_iter()
			.flat_map(|token| token.text)
			.collect::<Vec<_>>();
		assert_eq!(rendered, b"x\n");
	}

	#[test]
	#[ignore = "visit-count benchmark; run with --ignored --nocapture"]
	fn benchmark_grouped_csv_graft_descent_visits() {
		for rows in [4_096usize, 20_000, 200_000] {
			let source = csv_source(rows);
			let green = build_csv_topology(&source, CSV_DELIMITER);
			let (registry, root) = graft_topology(&source, &green);
			let positions = [0, rows / 2, rows - 1];
			let visits = positions.map(|line| viewport_descent_visits(&registry, root, line));
			let limit = bounded_descent_visit_limit(rows);
			println!("grouped_csv rows={rows} visits={visits:?} limit={limit}");
			assert!(
				visits.iter().all(|&count| count <= limit),
				"graft exceeded its bounded-fan-out visit budget"
			);
		}
	}

	#[test]
	fn root_line_indexed_cursor_lookup_matches_plain_lookup_near_eof() {
		let parts = ["head\n", "alpha\nbeta\n", "gamma\ndelta\n", "tail"];
		let (registry, root) = build_document_with_leaves(&parts);
		let line_index = registry.build_root_child_line_index(root);

		let plain = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(4),
			DocDisplayCol::new(2),
		);
		let indexed = registry.find_node_at_line_col_with_root_line_index(
			root,
			DocLine::new(4),
			DocDisplayCol::new(2),
			&line_index,
		);

		assert_eq!(indexed.node_id, plain.node_id);
		assert_eq!(indexed.node_byte, plain.node_byte);
		assert_eq!(indexed.display_col, plain.display_col);
	}

	#[test]
	fn query_viewport_emits_fold_placeholder_and_skips_descendants() {
		let (registry, root, folded) = build_foldable_document();
		let folds = HashSet::from([folded]);

		let tokens = registry.query_viewport(root, &folds, DocLine::ZERO, 4);

		assert_eq!(tokens.len(), 3);
		assert_eq!(tokens[0].text, b"head\n".to_vec());
		assert!(tokens[1].is_folded);
		assert_eq!(tokens[1].node_id, folded);
		assert_eq!(tokens[1].text, "❯❯❯".as_bytes().to_vec());
		assert_eq!(tokens[1].absolute_start_line, DocLine::new(1));
		assert_eq!(tokens[1].absolute_start_byte, DocByte::new(5));
		assert_eq!(tokens[1].physical_byte_len, 8);
		assert_eq!(tokens[2].text, b"tail".to_vec());
		assert_eq!(tokens[2].absolute_start_line, DocLine::new(3));
		assert_eq!(tokens[2].absolute_start_byte, DocByte::new(13));
	}

	#[test]
	fn query_viewport_starts_on_fold_placeholder_when_target_line_is_hidden() {
		let (registry, root, folded) = build_foldable_document();
		let folds = HashSet::from([folded]);

		let tokens = registry.query_viewport(root, &folds, DocLine::new(2), 2);

		assert_eq!(tokens.len(), 2);
		assert_eq!(tokens[0].node_id, folded);
		assert!(tokens[0].is_folded);
		assert_eq!(tokens[0].absolute_start_line, DocLine::new(1));
		assert_eq!(tokens[1].text, b"tail".to_vec());
	}

	#[test]
	fn find_node_at_line_col_can_reach_the_line_after_a_folded_block() {
		let (registry, root, folded) = build_foldable_document();
		let folds = HashSet::from([folded]);

		let target =
			registry.find_node_at_line_col(root, &folds, DocLine::new(3), DocDisplayCol::ZERO);

		assert_ne!(target.node_id, folded);
		assert_eq!(target.node_byte, crate::core::NodeByteOffset::ZERO);
	}

	#[test]
	fn find_node_at_line_col_returns_fold_boundary_for_hidden_lines() {
		let (registry, root, folded) = build_foldable_document();
		let folds = HashSet::from([folded]);

		let target =
			registry.find_node_at_line_col(root, &folds, DocLine::new(2), DocDisplayCol::new(3));
		let raw = registry.find_node_at_line_col_raw(root, DocLine::new(2), DocDisplayCol::new(3));

		assert_eq!(target.node_id, folded);
		assert_eq!(target.node_byte, crate::core::NodeByteOffset::ZERO);
		assert_eq!(target.display_col, DocDisplayCol::ZERO);
		assert_ne!(raw.node_id, folded);
	}

	#[test]
	fn find_node_at_doc_byte_returns_fold_boundary_for_hidden_bytes() {
		let (registry, root, folded) = build_foldable_document();
		let folds = HashSet::from([folded]);

		let target = registry.find_node_at_doc_byte(root, &folds, DocByte::new(10));
		let raw = registry.find_node_at_doc_byte_raw(root, DocByte::new(10));

		assert_eq!(target.node_id, folded);
		assert_eq!(target.node_start_byte, DocByte::new(5));
		assert_eq!(target.node_end_byte, DocByte::new(13));
		assert_eq!(target.node_byte, crate::core::NodeByteOffset::ZERO);
		assert_eq!(target.node_start_line, Some(DocLine::new(1)));
		assert_ne!(raw.node_id, folded);
		assert_eq!(raw.node_start_line, Some(DocLine::new(2)));
	}

	#[test]
	fn cold_line_lookup_does_not_inflate_metrics_or_read_synchronously() {
		let path = temp_test_path("projection-uninflated");
		std::fs::write(&path, "aa\nbb\ncc\ndd").expect("write temp file");

		let (registry, root) = build_uninflated_physical_document(&["aa\nbb\n", "cc\ndd"]);
		registry
			.register_device_path(77, &path)
			.expect("register backing");

		let first_leaf = registry.get_first_child(root).expect("first leaf");
		let second_leaf = registry.get_next_sibling(first_leaf).expect("second leaf");
		registry.begin_metric_scan(root, [first_leaf, second_leaf]);
		registry.install_root_child_line_index(root);
		assert_eq!(
			registry.line_lookup_state(root, DocLine::new(3)),
			LineLookupState::Pending {
				indexed_through_line: DocLine::ZERO,
				indexed_through_byte: DocByte::ZERO,
			}
		);
		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(3),
			DocDisplayCol::ZERO,
		);

		assert_eq!(
			target,
			NodeLineLookup::Pending {
				indexed_through_line: DocLine::ZERO,
				indexed_through_byte: DocByte::ZERO,
				required_node: None,
			}
		);
		assert_eq!(
			unsafe { (*registry.metrics[root.index()].get()).newlines },
			0
		);
		assert!(!registry.metrics_inflated[first_leaf.index()].load(Ordering::Acquire));
		assert!(!registry.metrics_inflated[second_leaf.index()].load(Ordering::Acquire));

		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn byte_descent_exposes_only_the_known_newline_prefix() {
		let (registry, root) = build_uninflated_physical_document(&["aa\nbb\n", "cc\ndd"]);
		let first_leaf = registry.get_first_child(root).expect("first leaf");
		let second_leaf = registry.get_next_sibling(first_leaf).expect("second leaf");
		registry.begin_metric_scan(root, [first_leaf, second_leaf]);
		assert!(
			!registry
				.root_child_line_indexes
				.read()
				.unwrap()
				.contains_key(&root)
		);

		let first_target = registry.find_node_at_doc_byte_raw(root, DocByte::new(2));
		assert_eq!(first_target.node_id, first_leaf);
		assert_eq!(first_target.node_start_line, Some(DocLine::ZERO));

		let second_target = registry.find_node_at_doc_byte_raw(root, DocByte::new(7));
		assert_eq!(second_target.node_id, second_leaf);
		assert_eq!(second_target.node_start_line, None);

		assert!(!registry.try_complete_metric_scan_block(root, first_leaf, 2, true));
		let second_target = registry.find_node_at_doc_byte_raw(root, DocByte::new(7));
		assert_eq!(second_target.node_start_line, Some(DocLine::new(2)));
	}

	#[test]
	fn indexed_raw_line_lookup_near_eof_avoids_inflating_prefix_leaves() {
		let path = temp_test_path("projection-indexed-raw");
		let chunk_count = 128usize;
		let mut chunks = vec!["x\n"; chunk_count];
		chunks[chunk_count - 1] = "x";
		let content = chunks.concat();
		std::fs::write(&path, &content).expect("write temp file");

		let (registry, root) = build_exact_metric_physical_document(&chunks);
		registry
			.register_device_path(77, &path)
			.expect("register backing");
		let line_index = registry.build_root_child_line_index(root);
		let target_line = DocLine::new((chunk_count - 2) as u64);

		let target = registry.find_node_at_line_col_raw_with_root_line_index(
			root,
			target_line,
			DocDisplayCol::ZERO,
			&line_index,
		);

		assert_eq!(target.node_id.index(), chunk_count - 1);
		assert!(
			inflated_leaf_count(&registry, root) <= 2,
			"indexed raw lookup should only inflate the target chunk and, at most, one boundary predecessor"
		);

		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn production_index_addresses_a_generated_200_gib_document_sparsely() {
		use crate::svp::backing_store::{BackingStore, GeneratedBackingStore};

		const GIB: u64 = 1024 * 1024 * 1024;
		const EXTENT_BYTES: u64 = 1024 * 1024;
		const EXTENT_COUNT: u64 = 200 * GIB / EXTENT_BYTES;
		let document_bytes = EXTENT_BYTES * EXTENT_COUNT;
		let lines_per_extent = EXTENT_BYTES / 2;
		let document_newlines = lines_per_extent * EXTENT_COUNT;
		let registry = UastRegistry::new(EXTENT_COUNT as u32 + 1);
		let mut chunk = registry
			.reserve_chunk(EXTENT_COUNT as u32 + 1)
			.expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: document_bytes,
				newlines: document_newlines,
			},
		);
		for extent in 0..EXTENT_COUNT {
			let offset = extent * EXTENT_BYTES;
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba: offset / 512,
					byte_length: EXTENT_BYTES as u32,
					device_id: 99,
					head_trim: 0,
				}),
				SpanMetrics {
					byte_length: EXTENT_BYTES,
					newlines: lines_per_extent,
				},
			);
			chunk.append_local_child(root, leaf);
			registry.metrics_inflated[leaf.index()].store(true, Ordering::Release);
			registry.set_terminal_newline(leaf, true);
		}
		registry.install_root_child_line_index(root);

		let generated = Arc::new(GeneratedBackingStore::new(document_bytes, |offset| {
			if offset % 2 == 1 { b'\n' } else { b'x' }
		}));
		registry.register_backing_store(99, generated.clone());

		for offset in [0, 4 * GIB - 1, 4 * GIB, 100 * GIB, document_bytes - 32] {
			let mut marker = [0u8; 32];
			generated
				.read_exact_at(offset, &mut marker)
				.expect("generated marker read");
			assert_eq!(marker[0], if offset % 2 == 1 { b'\n' } else { b'x' });
		}

		let byte_target = document_bytes - 17;
		let selected =
			registry.find_node_at_doc_byte(root, &crate::uast::NoFolds, DocByte::new(byte_target));
		assert_eq!(
			selected.node_start_byte.get(),
			(EXTENT_COUNT - 1) * EXTENT_BYTES
		);
		assert_eq!(
			registry.doc_byte_for_node_offset(root, selected.node_id, selected.node_byte),
			DocByte::new(byte_target)
		);

		let line_target = u64::from(u32::MAX) + 123;
		let pending_line = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(line_target),
			DocDisplayCol::ZERO,
		);
		let expected_extent = line_target / lines_per_extent;
		let required = pending_line
			.required_node()
			.expect("cold line lookup must request its selected extent");
		assert_eq!(required.index() as u64, expected_extent + 1);
		let bytes = registry
			.read_node_bytes_sync(required)
			.expect("hydrate selected generated extent");
		registry.publish_hydrated_data(required, bytes);
		let index_builds = registry
			.root_child_line_index_full_builds
			.load(Ordering::Acquire);
		registry.refresh_installed_root_child_line_index_for_node(required);
		assert_eq!(
			registry
				.root_child_line_index_full_builds
				.load(Ordering::Acquire),
			index_builds,
			"refreshing one extent in a 200 GiB index must not rebuild all siblings"
		);
		let selected_line = registry
			.find_node_at_line_col(
				root,
				&crate::uast::NoFolds,
				DocLine::new(line_target),
				DocDisplayCol::ZERO,
			)
			.ready()
			.expect("hydrated line lookup");
		assert_eq!(selected_line.node_id.index() as u64, expected_extent + 1);
		assert!(generated.bytes_read() <= 16 * 1024 * 1024);
		assert_eq!(registry.stats().allocated_nodes, EXTENT_COUNT + 1);
	}

	#[test]
	fn installed_root_index_counters_are_global_monotonic_and_effective_only() {
		let (registry, root) = build_document_with_leaves(&["ab", "cd"]);
		let child = registry.get_first_child(root).expect("first child");
		assert_eq!(registry.root_child_line_index_counters(), (0, 0));
		assert_eq!(registry.root_child_line_index_revision(), 0);

		registry.install_root_child_line_index(root);
		assert_eq!(registry.root_child_line_index_counters(), (1, 0));
		let installed_revision = registry.root_child_line_index_revision();
		assert!(installed_revision > 0);

		registry.refresh_installed_root_child_line_index_for_node(child);
		assert_eq!(
			registry.root_child_line_index_counters(),
			(1, 0),
			"an identical point refresh is not splice traffic"
		);
		assert_eq!(
			registry.root_child_line_index_revision(),
			installed_revision
		);

		unsafe {
			(*registry.metrics[child.index()].get()).byte_length += 1;
		}
		registry.refresh_installed_root_child_line_index_for_node(child);
		assert_eq!(registry.root_child_line_index_counters(), (1, 1));
		assert!(registry.root_child_line_index_revision() > installed_revision);
	}

	#[test]
	fn root_index_inspection_does_not_hydrate_or_inflate_cold_extents() {
		let (registry, root) =
			build_uninflated_physical_document(&["cold-one", "cold-two", "cold-three"]);
		registry.install_root_child_line_index(root);
		let resident_before = registry.stats().resident_bytes;
		let inspector_revision_before = registry.inspector_revision();
		let snapshot = registry
			.inspect_root_child_line_index(root, Some(DocByte::new(12)), 2)
			.expect("installed root index");

		assert_eq!(snapshot.rows.len(), 2);
		assert_eq!(registry.stats().resident_bytes, resident_before);
		assert_eq!(registry.inspector_revision(), inspector_revision_before);
		let mut child = registry.get_first_child(root);
		while let Some(node) = child {
			assert!(!registry.metrics_inflated[node.index()].load(Ordering::Acquire));
			assert!(unsafe { (&*registry.virtual_data[node.index()].get()).is_none() });
			child = registry.get_next_sibling(node);
		}
	}

	fn clip_token(start_byte: u64, start_line: u64, text: &[u8]) -> super::RenderToken {
		super::RenderToken {
			node_id: crate::ecs::NodeId::from_index(1),
			kind: SemanticKind::default(),
			text: text.to_vec(),
			physical_byte_len: u32::try_from(text.len()).unwrap(),
			absolute_start_line: DocLine::new(start_line),
			absolute_start_byte: DocByte::new(start_byte),
			is_virtual: false,
			is_folded: false,
		}
	}

	/// (absolute start byte, absolute start line, text) for each clipped token.
	fn clip(tokens: Vec<super::RenderToken>, text_width: u16) -> Vec<(u64, u64, Vec<u8>)> {
		super::clip_frontend_text_tokens(tokens, text_width)
			.into_iter()
			.map(|token| {
				assert_eq!(token.physical_byte_len as usize, token.text.len());
				(
					token.absolute_start_byte.get(),
					token.absolute_start_line.get(),
					token.text,
				)
			})
			.collect()
	}

	#[test]
	fn clip_keeps_a_cell_past_the_edge_and_resumes_at_the_newline() {
		let clipped = clip(vec![clip_token(100, 7, b"abcdefghij\nxyz")], 4);
		assert_eq!(
			clipped,
			vec![(100, 7, b"abcde".to_vec()), (110, 7, b"\nxyz".to_vec())]
		);
	}

	#[test]
	fn clip_leaves_lines_within_the_limit_untouched() {
		let clipped = super::clip_frontend_text_tokens(vec![clip_token(3, 1, b"ab\ncd\n")], 4);
		assert_eq!(clipped.len(), 1);
		assert_eq!(clipped[0].text, b"ab\ncd\n");
		assert_eq!(clipped[0].absolute_start_byte, DocByte::new(3));
		assert_eq!(clipped[0].absolute_start_line, DocLine::new(1));
		assert_eq!(clipped[0].physical_byte_len, 6);
	}

	#[test]
	fn clip_counts_display_cells_not_bytes() {
		// A tab advances to the next stop.
		let width = u16::try_from(TAB_SIZE + 1).unwrap();
		assert_eq!(
			clip(vec![clip_token(0, 0, b"\tabcdef")], width)[0].2,
			b"\tab"
		);
		// A wide cell straddling the limit is kept whole, never split.
		assert_eq!(
			clip(vec![clip_token(0, 0, "日本語".as_bytes())], 2)[0].2,
			"日本".as_bytes()
		);
		// A combining mark stays with its base.
		assert_eq!(
			clip(
				vec![clip_token(0, 0, "e\u{301}e\u{301}e\u{301}".as_bytes())],
				1
			)[0]
			.2,
			"e\u{301}e\u{301}".as_bytes()
		);
		// Invalid bytes take one cell each.
		assert_eq!(clip(vec![clip_token(0, 0, &[0xff; 10])], 2)[0].2, [0xff; 3]);
	}

	#[test]
	fn clip_carries_the_column_across_tokens_of_one_line() {
		let clipped = clip(
			vec![
				clip_token(0, 0, b"abc"),
				clip_token(3, 0, b"defgh\nz"),
				clip_token(10, 1, b"wxyz"),
				clip_token(14, 1, b"tail\nok"),
			],
			3,
		);
		assert_eq!(
			clipped,
			vec![
				(0, 0, b"abc".to_vec()),
				(3, 0, b"d".to_vec()),
				(8, 0, b"\nz".to_vec()),
				(10, 1, b"wxy".to_vec()),
				(18, 1, b"\nok".to_vec()),
			]
		);
	}

	#[test]
	fn clip_drops_a_token_wholly_past_the_limit() {
		let clipped = clip(
			vec![
				clip_token(0, 0, b"abcd"),
				clip_token(4, 0, b"efgh"),
				clip_token(8, 0, b"\nx"),
			],
			1,
		);
		assert_eq!(
			clipped,
			vec![(0, 0, b"ab".to_vec()), (8, 0, b"\nx".to_vec())]
		);
	}

	#[test]
	fn clip_keeps_following_lines_at_their_absolute_bytes() {
		let clipped = clip(vec![clip_token(0, 0, b"aaaaaa\nbb\ncccccc\n")], 2);
		assert_eq!(
			clipped,
			vec![
				(0, 0, b"aaa".to_vec()),
				(6, 0, b"\nbb\nccc".to_vec()),
				(16, 2, b"\n".to_vec()),
			]
		);
	}

	#[test]
	fn clip_keeps_one_cell_in_a_zero_width_pane() {
		let clipped = clip(vec![clip_token(0, 0, b"abc\ndef")], 0);
		assert_eq!(
			clipped,
			vec![(0, 0, b"a".to_vec()), (3, 0, b"\nd".to_vec())]
		);
	}

	#[test]
	fn clip_never_touches_folded_tokens() {
		let mut folded = clip_token(0, 0, super::FOLDED_TOKEN_TEXT.as_bytes());
		folded.is_folded = true;
		folded.physical_byte_len = 4096;
		let clipped = super::clip_frontend_text_tokens(vec![folded], 0);
		assert_eq!(clipped.len(), 1);
		assert!(clipped[0].is_folded);
		assert_eq!(clipped[0].text, super::FOLDED_TOKEN_TEXT.as_bytes());
		assert_eq!(clipped[0].physical_byte_len, 4096);
	}
}
