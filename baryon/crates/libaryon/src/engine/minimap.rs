use super::protocol::SearchMatch;
use crate::core::{DocByte, DocLine, DocLineCount, TAB_SIZE, TokenBin};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::language::LanguageProvider;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewRow {
	pub logical_width: u16,
	pub bins: Box<[TokenBin]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewSnapshot {
	pub overlay: MinimapOverlay,
	pub max_columns: u16,
	pub rows: Vec<PreviewRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverviewSnapshot {
	pub overlay: MinimapOverlay,
	pub bands: Vec<u8>,
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
}

pub(super) fn build_text_minimap_snapshot(
	bytes: &[u8],
	semantic_highlights: &[HighlightSpan],
	mut overlay: MinimapOverlay,
	provider: Option<&dyn LanguageProvider>,
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
		))
	} else {
		MinimapSnapshot::Overview(build_overview_snapshot(bytes, overlay))
	}
}

pub(super) fn build_byte_fallback_minimap_snapshot(
	registry: &UastRegistry,
	root: NodeId,
	mut overlay: MinimapOverlay,
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
	MinimapSnapshot::ByteFallback(OverviewSnapshot { overlay, bands })
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

fn build_overview_snapshot(bytes: &[u8], overlay: MinimapOverlay) -> OverviewSnapshot {
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

	OverviewSnapshot { overlay, bands }
}

fn build_preview_snapshot(
	bytes: &[u8],
	semantic_highlights: &[HighlightSpan],
	overlay: MinimapOverlay,
	provider: Option<&dyn LanguageProvider>,
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

	PreviewSnapshot {
		overlay,
		max_columns: max_columns.max(1),
		rows,
	}
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
		MINIMAP_BANDS, MinimapOverlay, MinimapSnapshot, PREVIEW_BIN_COLUMNS,
		build_byte_fallback_minimap_snapshot, build_search_minimap_bands,
		build_text_minimap_snapshot, normalize_line_density, token_bin,
	};
	use crate::core::{DocLine, DocLineCount};
	use crate::ecs::UastRegistry;
	use crate::svp::rust_highlight::TokenCategory;
	use crate::svp::rust_provider::RustProvider;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;

	#[test]
	fn small_rust_documents_use_preview_snapshot() {
		let provider = RustProvider::new();
		let overlay = MinimapOverlay::new(DocLine::ZERO, 20, DocLine::ZERO);
		let snapshot = build_text_minimap_snapshot(
			b"fn main() {\n\tlet value = 1;\n}\n",
			&[],
			overlay,
			Some(&provider),
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
		let snapshot = build_text_minimap_snapshot(&bytes, &[], overlay, Some(&provider));

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
		let snapshot = build_byte_fallback_minimap_snapshot(&registry, root, overlay);
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
		let snapshot = build_text_minimap_snapshot(b"abc\n", &[], overlay, None);
		let MinimapSnapshot::Preview(preview) = snapshot else {
			panic!("expected preview minimap");
		};

		assert_eq!(preview.rows[0].bins.len(), PREVIEW_BIN_COLUMNS);
	}
}
