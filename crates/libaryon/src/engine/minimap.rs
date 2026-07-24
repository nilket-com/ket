use super::protocol::SearchMatch;
use crate::core::{BufferId, DocByte, DocLine, DocLineCount, StateId, TAB_SIZE, TokenBin};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::language::LanguageProvider;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(super) const MAX_PREVIEW_MINIMAP_BYTES: u64 = 2 * 1024 * 1024;
pub(super) const MAX_PREVIEW_MINIMAP_LINES: u64 = 8 * 1024;
pub(super) const MINIMAP_BANDS: usize = 256;
pub const PREVIEW_BIN_COLUMNS: usize = 192;

const BYTE_FALLBACK_NO_NEWLINE_AVG_LINE_CAP: usize = 80;
const MAX_PREVIEW_LOGICAL_COLUMNS: u16 = 240;
pub const EMPTY_PREVIEW_BIN: TokenBin = TokenBin::EMPTY;

fn token_bin(category: TokenCategory) -> TokenBin {
	TokenBin::new(category as u8)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinimapOverlay {
	pub doc_line_count: DocLineCount,
	pub viewport_start_line: DocLine,
	pub viewport_end_line: DocLineCount,
	pub viewport_line_count: u32,
	pub cursor_line: DocLine,
	pub search_bands: Vec<u8>,
	pub active_search_band: Option<usize>,
}

impl MinimapOverlay {
	pub fn new(
		viewport_start_line: DocLine,
		viewport_line_count: u32,
		cursor_line: DocLine,
	) -> Self {
		Self {
			doc_line_count: DocLineCount::new(1),
			viewport_start_line,
			viewport_end_line: viewport_start_line
				.boundary_before()
				.saturating_add(u64::from(viewport_line_count)),
			viewport_line_count,
			cursor_line,
			search_bands: vec![0; MINIMAP_BANDS],
			active_search_band: None,
		}
	}
}

/// Convert a minimap row into the viewport start line used by every frontend.
///
/// The hit identifies a document line, then that line is placed halfway down
/// the viewport. With an even viewport height this leaves one more visible row
/// above the hit than below it, preserving a small amount of leading context.
/// The engine performs the final fold-, wrap-, and EOF-aware clamp.
pub fn minimap_click_viewport_start_line(
	local_row: u32,
	max_doc_line: DocLine,
	content_height: u32,
	viewport_line_count: u32,
) -> Option<DocLine> {
	if content_height == 0 {
		return None;
	}
	let denominator = u64::from(content_height.saturating_sub(1)).max(1);
	let clicked_line = u64::from(local_row).saturating_mul(max_doc_line.get()) / denominator;
	Some(
		DocLine::new(clicked_line.min(max_doc_line.get()))
			.saturating_sub(u64::from(viewport_line_count / 2)),
	)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewRow {
	pub logical_width: u16,
	pub bins: Box<[TokenBin]>,
}

/// Identity of the immutable minimap ink for one document revision.
///
/// Buffer identity prevents equal per-buffer state counters from colliding
/// when a pane switches tabs. Semantic revision makes language-server token
/// arrival rebuild and recolor the payload without requiring a text edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MinimapContentRevision {
	pub buffer_id: BufferId,
	pub state_id: StateId,
	pub semantic_revision: u64,
}

impl MinimapContentRevision {
	pub const ZERO: Self = Self {
		buffer_id: BufferId::ZERO,
		state_id: StateId::ZERO,
		semantic_revision: 0,
	};

	pub const fn new(buffer_id: BufferId, state_id: StateId, semantic_revision: u64) -> Self {
		Self {
			buffer_id,
			state_id,
			semantic_revision,
		}
	}
}

#[derive(Debug, PartialEq, Eq)]
pub struct PreviewContent {
	pub revision: MinimapContentRevision,
	pub max_columns: u16,
	pub rows: Vec<PreviewRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewSnapshot {
	pub overlay: MinimapOverlay,
	pub content: Arc<PreviewContent>,
}

impl PreviewSnapshot {
	pub fn new(
		overlay: MinimapOverlay,
		revision: MinimapContentRevision,
		max_columns: u16,
		rows: Vec<PreviewRow>,
	) -> Self {
		Self {
			overlay,
			content: Arc::new(PreviewContent {
				revision,
				max_columns,
				rows,
			}),
		}
	}
}

impl Deref for PreviewSnapshot {
	type Target = PreviewContent;

	fn deref(&self) -> &Self::Target {
		&self.content
	}
}

#[derive(Debug, PartialEq, Eq)]
pub struct OverviewContent {
	pub revision: MinimapContentRevision,
	pub bands: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverviewSnapshot {
	pub overlay: MinimapOverlay,
	pub content: Arc<OverviewContent>,
}

impl OverviewSnapshot {
	pub fn new(overlay: MinimapOverlay, revision: MinimapContentRevision, bands: Vec<u8>) -> Self {
		Self {
			overlay,
			content: Arc::new(OverviewContent { revision, bands }),
		}
	}
}

impl Deref for OverviewSnapshot {
	type Target = OverviewContent;

	fn deref(&self) -> &Self::Target {
		&self.content
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MinimapSnapshot {
	Preview(PreviewSnapshot),
	Overview(OverviewSnapshot),
	ByteFallback(OverviewSnapshot),
}

impl MinimapSnapshot {
	pub fn overlay(&self) -> &MinimapOverlay {
		match self {
			Self::Preview(snapshot) => &snapshot.overlay,
			Self::Overview(snapshot) | Self::ByteFallback(snapshot) => &snapshot.overlay,
		}
	}

	pub fn overlay_mut(&mut self) -> &mut MinimapOverlay {
		match self {
			Self::Preview(snapshot) => &mut snapshot.overlay,
			Self::Overview(snapshot) | Self::ByteFallback(snapshot) => &mut snapshot.overlay,
		}
	}

	pub fn revision(&self) -> MinimapContentRevision {
		match self {
			Self::Preview(snapshot) => snapshot.revision,
			Self::Overview(snapshot) | Self::ByteFallback(snapshot) => snapshot.revision,
		}
	}
}

/// Project a horizontal pixel range of a preview row onto its dominant token
/// category. `range_start..range_end` addresses `total_columns` output columns;
/// the row's fixed bin budget is resampled to that width. Shared by every head
/// so their minimaps agree on what a pixel "means".
pub fn sample_preview_bin_range(
	row: &PreviewRow,
	range_start: usize,
	range_end: usize,
	total_columns: usize,
) -> TokenBin {
	if total_columns == 0 || row.logical_width == 0 {
		return EMPTY_PREVIEW_BIN;
	}

	let start_bin = (range_start * PREVIEW_BIN_COLUMNS) / total_columns;
	let end_bin = (range_end * PREVIEW_BIN_COLUMNS).div_ceil(total_columns);
	let start_bin = start_bin.min(PREVIEW_BIN_COLUMNS - 1);
	let end_bin = end_bin.max(start_bin + 1).min(PREVIEW_BIN_COLUMNS);
	let mut counts = [0u16; crate::svp::rust_highlight::CATEGORY_COUNT];
	let mut saw_unclassified = false;

	for &bin in &row.bins[start_bin..end_bin] {
		if bin == EMPTY_PREVIEW_BIN {
			continue;
		}
		if bin.index() >= crate::svp::rust_highlight::CATEGORY_COUNT {
			continue;
		}
		counts[bin.index()] = counts[bin.index()].saturating_add(1);
		if bin.index() == TokenCategory::Unclassified as usize {
			saw_unclassified = true;
		}
	}

	let mut best: Option<(usize, u16)> = None;
	for (idx, &count) in counts.iter().enumerate() {
		if count == 0 {
			continue;
		}
		match best {
			Some((_, best_count)) if best_count >= count => {}
			_ => best = Some((idx, count)),
		}
	}

	if let Some((idx, _)) = best {
		TokenBin::new(idx as u8)
	} else if saw_unclassified {
		token_bin(TokenCategory::Unclassified)
	} else {
		EMPTY_PREVIEW_BIN
	}
}

/// Dominant token category across a vertical slice of rows for one output
/// column range, plus how many of the rows had ink there (for coverage-based
/// alpha). Returns `None` when every sampled row is empty.
pub fn dominant_preview_bin_for_rows(
	rows: &[PreviewRow],
	x_start: usize,
	x_end: usize,
	total_columns: usize,
) -> Option<(TokenBin, u16, u16)> {
	if rows.is_empty() || total_columns == 0 {
		return None;
	}

	let mut counts = [0u16; crate::svp::rust_highlight::CATEGORY_COUNT];
	let mut non_empty = 0u16;
	for row in rows {
		let bin = sample_preview_bin_range(row, x_start, x_end, total_columns);
		if bin == EMPTY_PREVIEW_BIN {
			continue;
		}
		let idx = bin.index();
		if idx >= crate::svp::rust_highlight::CATEGORY_COUNT {
			continue;
		}
		counts[idx] = counts[idx].saturating_add(1);
		non_empty = non_empty.saturating_add(1);
	}

	if non_empty == 0 {
		return None;
	}

	let mut best_idx = TokenCategory::Unclassified as usize;
	let mut best_count = 0u16;
	for (idx, &count) in counts.iter().enumerate() {
		if count > best_count {
			best_idx = idx;
			best_count = count;
		}
	}

	Some((
		TokenBin::new(best_idx as u8),
		non_empty,
		rows.len().min(u16::MAX as usize) as u16,
	))
}

pub(super) fn build_text_minimap_snapshot(
	bytes: &[u8],
	semantic_highlights: &[HighlightSpan],
	mut overlay: MinimapOverlay,
	provider: Option<&dyn LanguageProvider>,
	revision: MinimapContentRevision,
) -> MinimapSnapshot {
	let doc_line_count = DocLineCount::new(
		u64::try_from(memchr::memchr_iter(b'\n', bytes).count())
			.expect("in-memory newline count fits u64")
			.saturating_add(1),
	);
	overlay.doc_line_count = doc_line_count;

	if bytes.len() as u64 <= MAX_PREVIEW_MINIMAP_BYTES
		&& doc_line_count.get() <= MAX_PREVIEW_MINIMAP_LINES
	{
		MinimapSnapshot::Preview(build_preview_snapshot(
			bytes,
			semantic_highlights,
			overlay,
			provider,
			revision,
		))
	} else {
		MinimapSnapshot::Overview(build_overview_snapshot(bytes, overlay, revision))
	}
}

pub(super) fn build_byte_fallback_minimap_snapshot(
	registry: &UastRegistry,
	root: NodeId,
	mut overlay: MinimapOverlay,
	revision: MinimapContentRevision,
) -> MinimapSnapshot {
	let file_size = registry.get_total_bytes(root).max(1);
	let doc_line_count = DocLineCount::new(registry.get_total_newlines(root).saturating_add(1));
	let mut bands = vec![0u8; MINIMAP_BANDS];
	let fallback_density = normalize_line_density(BYTE_FALLBACK_NO_NEWLINE_AVG_LINE_CAP).max(24);
	// The byte fallback has no per-extent information to add: it represents
	// unknown content as uniform density. Filling fixed bands directly avoids a
	// full registry walk on every minimap rebuild, regardless of file size.
	if file_size > 0 {
		bands.fill(fallback_density);
	}

	overlay.doc_line_count = doc_line_count;
	MinimapSnapshot::ByteFallback(OverviewSnapshot::new(overlay, revision, bands))
}

pub(crate) fn build_search_minimap_bands(
	search_matches: &[SearchMatch],
	active_match_index: Option<usize>,
	doc_line_count: DocLineCount,
) -> (Vec<u8>, Option<usize>) {
	let doc_line_count = doc_line_count.get().max(1);
	let mut bands = vec![0u8; MINIMAP_BANDS];
	if search_matches.is_empty() {
		return (bands, None);
	}

	let stride = (search_matches.len() / 8192).max(1);
	for (idx, m) in search_matches.iter().enumerate().step_by(stride) {
		let band = ((u128::from(m.line.get()) * MINIMAP_BANDS as u128) / u128::from(doc_line_count))
			as usize;
		let band = band.min(MINIMAP_BANDS - 1);
		let intensity = if Some(idx) == active_match_index {
			255
		} else {
			196
		};
		bands[band] = bands[band].max(intensity);
	}

	let active_search_band = active_match_index
		.and_then(|idx| search_matches.get(idx))
		.map(|m| {
			((u128::from(m.line.get()) * MINIMAP_BANDS as u128) / u128::from(doc_line_count))
				.min((MINIMAP_BANDS - 1) as u128) as usize
		});
	(bands, active_search_band)
}

pub(super) fn next_registry_topology_revision(registry: &UastRegistry) -> u32 {
	registry.next_id.load(Ordering::Acquire)
}

fn normalize_line_density(byte_len: usize) -> u8 {
	let capped = byte_len.min(160) as u32;
	((capped * 255) / 160) as u8
}

fn build_overview_snapshot(
	bytes: &[u8],
	overlay: MinimapOverlay,
	revision: MinimapContentRevision,
) -> OverviewSnapshot {
	let doc_line_count = overlay.doc_line_count.get().max(1);
	let mut sums = vec![0u32; MINIMAP_BANDS];
	let mut counts = vec![0u32; MINIMAP_BANDS];
	let mut total_lines = 0u32;
	let mut current_len = 0usize;

	for &b in bytes {
		if b == b'\n' {
			let band = ((total_lines as usize) * MINIMAP_BANDS) / doc_line_count as usize;
			sums[band.min(MINIMAP_BANDS - 1)] += normalize_line_density(current_len) as u32;
			counts[band.min(MINIMAP_BANDS - 1)] += 1;
			total_lines += 1;
			current_len = 0;
		} else {
			current_len += 1;
		}
	}

	let final_band = ((total_lines as usize) * MINIMAP_BANDS) / doc_line_count as usize;
	sums[final_band.min(MINIMAP_BANDS - 1)] += normalize_line_density(current_len) as u32;
	counts[final_band.min(MINIMAP_BANDS - 1)] += 1;

	let bands = sums
		.into_iter()
		.zip(counts)
		.map(|(sum, count)| sum.checked_div(count).unwrap_or(0) as u8)
		.collect();

	OverviewSnapshot::new(overlay, revision, bands)
}

fn build_preview_snapshot(
	bytes: &[u8],
	semantic_highlights: &[HighlightSpan],
	overlay: MinimapOverlay,
	provider: Option<&dyn LanguageProvider>,
	revision: MinimapContentRevision,
) -> PreviewSnapshot {
	let lexical_highlights = match provider {
		Some(p) => p.lexical_baseline(bytes, &[(0, DocByte::ZERO)]),
		None => Vec::new(),
	};
	let merged_highlights = merge_highlights(&lexical_highlights, semantic_highlights);
	let mut rows = Vec::with_capacity(
		usize::try_from(overlay.doc_line_count.get())
			.expect("preview line count is size-gated and fits usize"),
	);
	let mut max_columns = 0u16;
	let mut line_start_byte = 0usize;

	if bytes.is_empty() {
		rows.push(PreviewRow {
			logical_width: 0,
			bins: vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS].into_boxed_slice(),
		});
	} else {
		for line in bytes.split_inclusive(|&b| b == b'\n') {
			let newline_len = usize::from(line.last().copied() == Some(b'\n'));
			let trimmed_end = line.len().saturating_sub(newline_len);
			let line_bytes = &line[..trimmed_end];
			let row = build_preview_row(line_bytes, line_start_byte, &merged_highlights);
			max_columns = max_columns.max(row.logical_width);
			rows.push(row);
			line_start_byte += line.len();
		}
		if bytes.last().copied() == Some(b'\n') {
			rows.push(PreviewRow {
				logical_width: 0,
				bins: vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS].into_boxed_slice(),
			});
		}
	}

	PreviewSnapshot::new(overlay, revision, max_columns.max(1), rows)
}

fn build_preview_row(
	line_bytes: &[u8],
	line_start_byte: usize,
	highlights: &[HighlightSpan],
) -> PreviewRow {
	let trimmed_len = trim_preview_line_len(line_bytes);
	let logical_width = preview_logical_width(&line_bytes[..trimmed_len]);
	let mut bins = vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS].into_boxed_slice();

	if logical_width == 0 {
		return PreviewRow {
			logical_width,
			bins,
		};
	}

	let mut idx = 0usize;
	let mut col = 0u16;
	while idx < trimmed_len && col < logical_width {
		match line_bytes[idx] {
			b' ' => {
				col = col.saturating_add(1);
				idx += 1;
			}
			b'\t' => {
				let tab = TAB_SIZE as u16;
				let next_col = col.saturating_add(tab - (col % tab));
				col = next_col.min(logical_width);
				idx += 1;
			}
			_ => {
				let start_col = col;
				col = col.saturating_add(1);
				let category = token_bin(category_for_byte(
					highlights,
					DocByte::new((line_start_byte + idx) as u64),
				));
				paint_preview_bins(&mut bins, logical_width, start_col, col, category);
				idx += preview_char_len(&line_bytes[idx..trimmed_len]);
			}
		}
	}

	PreviewRow {
		logical_width,
		bins,
	}
}

fn trim_preview_line_len(line_bytes: &[u8]) -> usize {
	let mut end = line_bytes.len();
	while end > 0 && matches!(line_bytes[end - 1], b' ' | b'\t' | b'\r') {
		end -= 1;
	}
	end
}

fn preview_logical_width(bytes: &[u8]) -> u16 {
	let mut width = 0u16;
	let mut idx = 0usize;
	while idx < bytes.len() && width < MAX_PREVIEW_LOGICAL_COLUMNS {
		match bytes[idx] {
			b'\t' => {
				let tab = TAB_SIZE as u16;
				let next = width.saturating_add(tab - (width % tab));
				width = next.min(MAX_PREVIEW_LOGICAL_COLUMNS);
				idx += 1;
			}
			_ => {
				width = width.saturating_add(1);
				idx += preview_char_len(&bytes[idx..]);
			}
		}
	}
	width
}

fn preview_char_len(bytes: &[u8]) -> usize {
	let max_len = bytes.len().min(4);
	for len in 1..=max_len {
		if let Ok(text) = std::str::from_utf8(&bytes[..len])
			&& let Some(ch) = text.chars().next()
			&& ch.len_utf8() == len
		{
			return len;
		}
	}
	1
}

fn category_for_byte(highlights: &[HighlightSpan], byte_offset: DocByte) -> TokenCategory {
	let idx = highlights.partition_point(|span| span.start <= byte_offset);
	if idx > 0 {
		let span = &highlights[idx - 1];
		if byte_offset < span.end {
			return span.category;
		}
	}
	TokenCategory::Unclassified
}

fn paint_preview_bins(
	bins: &mut [TokenBin],
	logical_width: u16,
	start_col: u16,
	end_col: u16,
	category: TokenBin,
) {
	let width = logical_width.max(1) as usize;
	let start_bin = ((start_col as usize) * PREVIEW_BIN_COLUMNS) / width;
	let end_bin = ((end_col as usize) * PREVIEW_BIN_COLUMNS).div_ceil(width);
	for existing in bins
		.iter_mut()
		.take(end_bin.max(start_bin + 1).min(PREVIEW_BIN_COLUMNS))
		.skip(start_bin.min(PREVIEW_BIN_COLUMNS))
	{
		if *existing == EMPTY_PREVIEW_BIN
			|| *existing == token_bin(TokenCategory::Unclassified)
			|| category != token_bin(TokenCategory::Unclassified)
		{
			*existing = category;
		}
	}
}

fn merge_highlights(lexical: &[HighlightSpan], semantic: &[HighlightSpan]) -> Vec<HighlightSpan> {
	if semantic.is_empty() {
		return lexical.to_vec();
	}

	let mut merged = Vec::with_capacity(lexical.len() + semantic.len());
	for &lex_span in lexical {
		let search = semantic.partition_point(|span| span.end <= lex_span.start);
		let mut overwritten = false;
		if let Some(sem_span) = semantic[search..].first()
			&& sem_span.start < lex_span.end
		{
			overwritten = true;
		}
		if !overwritten {
			merged.push(lex_span);
		}
	}
	merged.extend_from_slice(semantic);
	merged.sort_unstable_by_key(|span| span.start);
	merged
}

#[cfg(test)]
mod tests {
	use super::super::protocol::SearchMatch;
	use super::{
		BYTE_FALLBACK_NO_NEWLINE_AVG_LINE_CAP, EMPTY_PREVIEW_BIN, MAX_PREVIEW_MINIMAP_LINES,
		MINIMAP_BANDS, MinimapContentRevision, MinimapOverlay, MinimapSnapshot, OverviewSnapshot,
		PREVIEW_BIN_COLUMNS, PreviewRow, PreviewSnapshot, build_byte_fallback_minimap_snapshot,
		build_search_minimap_bands, build_text_minimap_snapshot, normalize_line_density, token_bin,
	};
	use crate::core::{DocLine, DocLineCount};
	use crate::ecs::UastRegistry;
	use crate::svp::rust_highlight::TokenCategory;
	use crate::svp::rust_provider::RustProvider;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;
	use std::sync::Arc;

	#[test]
	fn small_rust_documents_use_preview_snapshot() {
		let provider = RustProvider::new();
		let overlay = MinimapOverlay::new(DocLine::ZERO, 20, DocLine::ZERO);
		let snapshot = build_text_minimap_snapshot(
			b"fn main() {\n\tlet value = 1;\n}\n",
			&[],
			overlay,
			Some(&provider),
			MinimapContentRevision::ZERO,
		);

		let MinimapSnapshot::Preview(preview) = snapshot else {
			panic!("expected preview minimap");
		};
		assert_eq!(preview.rows.len(), 4);
		assert!(preview.max_columns >= 10);
		assert!(
			preview.rows[0]
				.bins
				.iter()
				.any(|&bin| bin == token_bin(TokenCategory::Keyword))
		);
		assert!(
			preview.rows[1]
				.bins
				.iter()
				.any(|&bin| bin != EMPTY_PREVIEW_BIN)
		);
	}

	#[test]
	fn large_documents_fall_back_to_overview_snapshot() {
		let mut bytes = Vec::new();
		for _ in 0..=MAX_PREVIEW_MINIMAP_LINES {
			bytes.extend_from_slice(b"line\n");
		}

		let overlay = MinimapOverlay::new(DocLine::ZERO, 20, DocLine::ZERO);
		let provider = RustProvider::new();
		let snapshot = build_text_minimap_snapshot(
			&bytes,
			&[],
			overlay,
			Some(&provider),
			MinimapContentRevision::ZERO,
		);

		assert!(matches!(snapshot, MinimapSnapshot::Overview(_)));
	}

	#[test]
	fn search_bands_mark_active_result() {
		let matches = vec![
			SearchMatch {
				line: DocLine::new(10),
				col: crate::core::DocDisplayCol::ZERO,
				byte_len: 1,
			},
			SearchMatch {
				line: DocLine::new(90),
				col: crate::core::DocDisplayCol::ZERO,
				byte_len: 1,
			},
		];
		let (bands, active) = build_search_minimap_bands(&matches, Some(1), DocLineCount::new(100));

		assert!(bands.iter().any(|&band| band > 0));
		assert_eq!(active, Some((90usize * MINIMAP_BANDS) / 100usize));
	}

	#[test]
	fn byte_fallback_minimap_caps_dense_binary_leaf_without_notches() {
		let registry = UastRegistry::new(4);
		let mut chunk = registry.reserve_chunk(2).expect("OOM");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 4096,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 4096,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);

		let overlay = MinimapOverlay::new(DocLine::ZERO, 40, DocLine::ZERO);
		let snapshot = build_byte_fallback_minimap_snapshot(
			&registry,
			root,
			overlay,
			MinimapContentRevision::ZERO,
		);
		let MinimapSnapshot::ByteFallback(snapshot) = snapshot else {
			panic!("expected byte fallback minimap");
		};
		let expected = normalize_line_density(BYTE_FALLBACK_NO_NEWLINE_AVG_LINE_CAP).max(24);

		assert!(expected < 255);
		assert!(snapshot.bands.iter().all(|&band| band == expected));
	}

	#[test]
	fn preview_rows_use_fixed_bin_budget() {
		let overlay = MinimapOverlay::new(DocLine::ZERO, 20, DocLine::ZERO);
		let snapshot =
			build_text_minimap_snapshot(b"abc\n", &[], overlay, None, MinimapContentRevision::ZERO);
		let MinimapSnapshot::Preview(preview) = snapshot else {
			panic!("expected preview minimap");
		};

		assert_eq!(preview.rows[0].bins.len(), PREVIEW_BIN_COLUMNS);
	}

	#[test]
	fn projected_minimap_clones_share_immutable_ink_payloads() {
		let overlay = MinimapOverlay::new(DocLine::ZERO, 20, DocLine::ZERO);
		let preview = PreviewSnapshot::new(
			overlay.clone(),
			MinimapContentRevision::ZERO,
			8,
			vec![PreviewRow {
				logical_width: 8,
				bins: vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS].into_boxed_slice(),
			}],
		);
		let preview_clone = preview.clone();
		assert!(Arc::ptr_eq(&preview.content, &preview_clone.content));

		let overview = OverviewSnapshot::new(
			overlay,
			MinimapContentRevision::ZERO,
			vec![127; MINIMAP_BANDS],
		);
		let overview_clone = overview.clone();
		assert!(Arc::ptr_eq(&overview.content, &overview_clone.content));
	}
}
