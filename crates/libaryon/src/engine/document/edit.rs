use super::super::folding::{FoldEdit, FoldSet, unfold_ancestor_chain};
#[cfg(test)]
use super::super::support::line_byte_range;
use super::super::support::{line_end_byte_sparse, line_start_byte_sparse, read_line_bytes_sparse};
#[cfg(test)]
use crate::core::DocDisplayCol;
use crate::core::{ByteDelta, DocByte, DocLine, DocLineCount, NodeByteOffset, StateId};
use crate::ecs::{NodeId, UastRegistry};
#[cfg(test)]
use crate::engine::undo::byte_offset_from_line_col;
use crate::engine::undo::{TextDelta, UndoLedger};
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::HighlightSpan;
use crate::uast::{UastMutation, UastProjection};

#[cfg(test)]
fn common_char_prefix_len(left: &str, right: &str) -> usize {
	let mut prefix = 0usize;
	let mut left_chars = left.chars();
	let mut right_chars = right.chars();

	loop {
		match (left_chars.next(), right_chars.next()) {
			(Some(l), Some(r)) if l == r => prefix += l.len_utf8(),
			_ => break,
		}
	}

	prefix
}

#[cfg(test)]
fn common_char_suffix_len(left: &str, right: &str) -> usize {
	let mut suffix = 0usize;
	let mut left_chars = left.chars().rev();
	let mut right_chars = right.chars().rev();

	loop {
		match (left_chars.next(), right_chars.next()) {
			(Some(l), Some(r)) if l == r => suffix += l.len_utf8(),
			_ => break,
		}
	}

	suffix
}

#[cfg(test)]
pub(crate) fn document_rewrite_delta(before: &str, after: &str) -> Option<(u64, String, String)> {
	if before == after {
		return None;
	}

	let prefix = common_char_prefix_len(before, after);
	let suffix = common_char_suffix_len(&before[prefix..], &after[prefix..]);
	let delete_end = before.len().saturating_sub(suffix);
	let insert_end = after.len().saturating_sub(suffix);

	Some((
		prefix as u64,
		before[prefix..delete_end].to_string(),
		after[prefix..insert_end].to_string(),
	))
}

trait RebasableByteSpan {
	fn start(&self) -> DocByte;
	fn end(&self) -> DocByte;
	fn set_start(&mut self, start: DocByte);
	fn set_end(&mut self, end: DocByte);
}

impl RebasableByteSpan for HighlightSpan {
	fn start(&self) -> DocByte {
		self.start
	}

	fn end(&self) -> DocByte {
		self.end
	}

	fn set_start(&mut self, start: DocByte) {
		self.start = start;
	}

	fn set_end(&mut self, end: DocByte) {
		self.end = end;
	}
}

impl RebasableByteSpan for DiagnosticSpan {
	fn start(&self) -> DocByte {
		self.start
	}

	fn end(&self) -> DocByte {
		self.end
	}

	fn set_start(&mut self, start: DocByte) {
		self.start = start;
	}

	fn set_end(&mut self, end: DocByte) {
		self.end = end;
	}
}

fn rebase_byte_spans_after_delta<T: RebasableByteSpan>(spans: &mut Vec<T>, delta: &TextDelta) {
	if spans.is_empty() {
		return;
	}

	let edit_start = delta.global_byte_offset;
	let edit_end_before = edit_start.saturating_add(delta.deleted_bytes.len() as u64);
	let shift =
		ByteDelta::new(delta.inserted_bytes.len() as i64 - delta.deleted_bytes.len() as i64);

	spans.retain_mut(|span| {
		if span.end() <= edit_start {
			return true;
		}

		if span.start() >= edit_end_before {
			span.set_start(span.start().saturating_offset(shift));
			span.set_end(span.end().saturating_offset(shift));
			return true;
		}

		false
	});
}

fn rebase_byte_spans_after_deltas<T: RebasableByteSpan>(spans: &mut Vec<T>, deltas: &[TextDelta]) {
	for delta in deltas {
		rebase_byte_spans_after_delta(spans, delta);
	}
}

#[cfg(test)]
pub(crate) fn rebase_semantic_highlights_after_delta(
	semantic_highlights: &mut Vec<HighlightSpan>,
	delta: &TextDelta,
) {
	rebase_byte_spans_after_delta(semantic_highlights, delta);
}

fn rebase_semantic_highlights_after_deltas(
	semantic_highlights: &mut Vec<HighlightSpan>,
	deltas: &[TextDelta],
) {
	rebase_byte_spans_after_deltas(semantic_highlights, deltas);
}

#[cfg(test)]
pub(crate) fn rebase_diagnostics_after_delta(
	diagnostics: &mut Vec<DiagnosticSpan>,
	delta: &TextDelta,
) {
	rebase_byte_spans_after_delta(diagnostics, delta);
}

fn rebase_diagnostics_after_deltas(diagnostics: &mut Vec<DiagnosticSpan>, deltas: &[TextDelta]) {
	rebase_byte_spans_after_deltas(diagnostics, deltas);
}

#[cfg(test)]
pub(crate) fn linewise_put_insertion(
	doc: &[u8],
	cursor_line: DocLine,
	text: &str,
) -> (usize, String, DocLine) {
	let global_offset = byte_offset_from_line_col(doc, cursor_line, DocDisplayCol::ZERO);
	let off = global_offset.get() as usize;
	let line_end = doc[off..]
		.iter()
		.position(|&b| b == b'\n')
		.map(|p| off + p + 1)
		.unwrap_or(doc.len());

	let needs_line_break = line_end == doc.len() && !doc.is_empty() && !doc.ends_with(b"\n");
	let mut inserted_text = String::with_capacity(text.len() + usize::from(needs_line_break));
	if needs_line_break {
		inserted_text.push('\n');
	}
	inserted_text.push_str(text);

	let target_cursor_line = if doc.is_empty() {
		DocLine::ZERO
	} else {
		cursor_line + DocLineCount::new(1)
	};

	(line_end, inserted_text, target_cursor_line)
}

/// Insert `bytes` at `byte_offset` in the live tree. Infallible by contract:
/// `preflight_delta` has already proven the offset is in bounds, so committing
/// the insertion cannot fail. Called only after every delta in the group has
/// been preflighted.
fn commit_insert(registry: &UastRegistry, root: NodeId, byte_offset: DocByte, bytes: &[u8]) {
	let target = registry.find_node_at_doc_byte_raw(root, byte_offset);
	assert!(
		registry.get_first_child(target.node_id).is_none(),
		"preflighted insertion must resolve to a leaf: root={root:?} offset={byte_offset:?} target={target:?}"
	);
	registry.insert_text(target.node_id, target.node_byte.get(), bytes);
}

fn read_slice_sync_for_edit(
	registry: &UastRegistry,
	root: NodeId,
	start: DocByte,
	end: DocByte,
) -> Result<Vec<u8>, String> {
	if let Ok(bytes) = registry.read_loaded_slice(root, start, end) {
		return Ok(bytes);
	}

	let root_len = registry.get_total_bytes(root);
	let start = start.get().min(root_len);
	let end = end.get().min(root_len);
	if start >= end {
		return Ok(Vec::new());
	}

	let start_target = registry.find_node_at_doc_byte_raw(root, DocByte::new(start));
	let mut visit = Some(start_target.node_id);
	let mut local_start = start_target.node_byte.get() as usize;
	let mut absolute_start = start_target.node_start_byte;
	let mut remaining = (end - start) as usize;
	let mut out = Vec::with_capacity(remaining);

	while let Some(node) = visit {
		let idx = node.index();
		let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };
		if has_children {
			visit = registry.get_first_child(node);
			continue;
		}

		let node_len = unsafe { (*registry.metrics[idx].get()).byte_length as usize };
		if node_len > 0 && absolute_start.get() < end {
			let bytes = registry
				.read_node_bytes_sync(node)
				.map_err(|err| format!("Edit error: {}", err))?;
			let take_len = (node_len.saturating_sub(local_start)).min(remaining);
			if take_len > 0 {
				out.extend_from_slice(&bytes[local_start..local_start + take_len]);
				remaining -= take_len;
				if remaining == 0 {
					return Ok(out);
				}
			}
		}

		absolute_start = absolute_start.saturating_add(node_len as u64);
		local_start = 0;
		visit = registry.get_next_node_in_walk(node);
	}

	if remaining == 0 {
		Ok(out)
	} else {
		Err("Edit range exceeded document bounds".to_string())
	}
}

/// Validate one delta against the pre-mutation document without touching a byte.
///
/// Every delta in a group is preflighted before any is committed, so a failure
/// here aborts the whole group with the document untouched — keeping bytes and
/// `StateId` in lockstep (stage 4 of `plans/caret-anchors.md`). The bounds and
/// live-bytes checks are exactly the guards that `delete_text_sparse` used to run
/// mid-mutation; hoisting them ahead of every write is what makes the commit atomic.
///
/// `current_start` is the delta's span start **in the current (un-mutated) tree**.
/// A delta's own `global_byte_offset` is a live offset against the tree the *prior*
/// deltas would leave behind — the sequential commit resolves it against that moving
/// tree — so the caller maps it back to a current-document position (see
/// `apply_deltas_to_document_internal`). Validating every delta against the one
/// pre-mutation tree is what lets the whole group be checked before any write.
fn preflight_delta(
	registry: &UastRegistry,
	root: NodeId,
	delta: &TextDelta,
	current_start: DocByte,
	total_bytes_before: u64,
) -> Result<(), String> {
	if !delta.deleted_bytes.is_empty() {
		let delete_end = current_start.saturating_add(delta.deleted_bytes.len() as u64);
		if delete_end.get() > total_bytes_before {
			return Err("Edit range exceeded document bounds".to_string());
		}
		let live_bytes = read_slice_sync_for_edit(registry, root, current_start, delete_end)?;
		if live_bytes != delta.deleted_bytes {
			return Err("Edit range no longer matches live document".to_string());
		}
	}
	if !delta.inserted_bytes.is_empty() && current_start.get() > total_bytes_before {
		return Err("Edit range exceeded document bounds".to_string());
	}
	Ok(())
}

/// Map a delta's live `global_byte_offset` to its span start in the current
/// (un-mutated) tree by re-adding the bytes that lower-offset priors remove.
///
/// The commit applies deltas in order, each against the tree the prior deltas leave
/// behind. A group is non-overlapping and monotonic — descending edits in original
/// coordinates (multicursor edits, redo) or ascending re-insertions (undo) — so a
/// prior delta shifts this one only when it sits at a lower offset, by its net byte
/// change. Descending priors sit above and contribute nothing (the offset already is
/// the current position); ascending priors sit below and accumulate.
fn current_span_start(deltas: &[TextDelta], index: usize) -> DocByte {
	let delta = &deltas[index];
	let mut start = delta.global_byte_offset.get();
	for prior in &deltas[..index] {
		if prior.global_byte_offset < delta.global_byte_offset {
			start = start
				.saturating_add(prior.deleted_bytes.len() as u64)
				.saturating_sub(prior.inserted_bytes.len() as u64);
		}
	}
	DocByte::new(start)
}

/// Describe a committed delta group in the coordinate space that existed
/// before the group began. Undo stores live offsets in ascending order, while
/// forward multicursor edits use descending offsets; `current_span_start`
/// normalizes both conventions for view fan-out.
pub(crate) fn fold_edits_for_deltas(deltas: &[TextDelta]) -> Vec<FoldEdit> {
	deltas
		.iter()
		.enumerate()
		.map(|(index, delta)| FoldEdit {
			start: current_span_start(deltas, index),
			deleted_len: delta.deleted_bytes.len() as u64,
			inserted_len: delta.inserted_bytes.len() as u64,
		})
		.collect()
}

/// Delete `deleted_len` bytes at `byte_offset` from the live tree. Infallible by
/// contract: `preflight_delta` has already proven the range is in bounds and matches
/// the live bytes, so failing to walk it back is a torn-tree invariant violation, not
/// a recoverable error. Called only after every delta in the group has been preflighted.
fn commit_delete(registry: &UastRegistry, root: NodeId, byte_offset: DocByte, deleted_len: u64) {
	let mut target = registry.find_node_at_doc_byte_raw(root, byte_offset);
	let mut remaining = deleted_len;
	while remaining > 0 {
		let max_bytes = u32::try_from(remaining.min(u64::from(u32::MAX)))
			.expect("bounded exact delete chunk fits NodeByteOffset");
		let (new_node, new_offset, removed) =
			registry.delete_bytes_forward(target.node_id, target.node_byte.get(), max_bytes);
		target.node_id = new_node;
		target.node_byte = NodeByteOffset::new(new_offset);
		remaining = remaining
			.checked_sub(u64::from(removed))
			.expect("exact delete removed more bytes than requested");
		assert!(
			removed > 0,
			"sparse delete made no progress after preflight"
		);
	}
}

pub(crate) fn linewise_put_insertion_sparse(
	registry: &UastRegistry,
	root: NodeId,
	cursor_line: DocLine,
	text: &str,
) -> Result<(DocByte, String, DocLine), String> {
	let file_size = registry.get_total_bytes(root);
	let insert_offset = line_end_byte_sparse(registry, root, cursor_line, true)?;
	let needs_line_break = if insert_offset.get() == file_size && file_size > 0 {
		let last = read_slice_sync_for_edit(
			registry,
			root,
			DocByte::new(file_size.saturating_sub(1)),
			DocByte::new(file_size),
		)?;
		last.first().copied() != Some(b'\n')
	} else {
		false
	};

	let mut inserted_text = String::with_capacity(text.len() + usize::from(needs_line_break));
	if needs_line_break {
		inserted_text.push('\n');
	}
	inserted_text.push_str(text);

	let target_cursor_line = if file_size == 0 {
		DocLine::ZERO
	} else {
		cursor_line + DocLineCount::new(1)
	};

	Ok((insert_offset, inserted_text, target_cursor_line))
}

pub(crate) fn delete_char_delta_at_cursor(
	registry: &UastRegistry,
	root: NodeId,
	cursor_abs_byte_offset: DocByte,
) -> Result<Option<TextDelta>, String> {
	let file_size = registry.get_total_bytes(root);
	if cursor_abs_byte_offset.get() >= file_size {
		return Ok(None);
	}

	let preview_end = DocByte::new((cursor_abs_byte_offset.get() + 4).min(file_size));
	let preview = read_slice_sync_for_edit(registry, root, cursor_abs_byte_offset, preview_end)?;
	let delete_len = (1..=preview.len())
		.find(|&len| {
			std::str::from_utf8(&preview[..len])
				.map(|text| text.chars().count() == 1)
				.unwrap_or(false)
		})
		.ok_or_else(|| "Delete target is not valid UTF-8".to_string())?;
	let deleted_bytes = preview[..delete_len].to_vec();

	Ok(Some(TextDelta {
		global_byte_offset: cursor_abs_byte_offset,
		deleted_bytes,
		inserted_bytes: Vec::new(),
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	}))
}

pub(crate) fn delete_line_delta_sparse(
	registry: &UastRegistry,
	root: NodeId,
	cursor_line: DocLine,
) -> Result<Option<(TextDelta, String)>, String> {
	let line_start = line_start_byte_sparse(registry, root, cursor_line)?;
	let mut delete_start = line_start;
	let mut deleted_bytes = read_line_bytes_sparse(registry, root, cursor_line, true)?;
	let register_text = if deleted_bytes.is_empty() {
		if line_start == DocByte::ZERO {
			return Ok(None);
		}
		"\n".to_string()
	} else {
		String::from_utf8_lossy(&deleted_bytes).into_owned()
	};

	if deleted_bytes.is_empty() {
		let prev_start = line_start.saturating_sub(1);
		let prev_byte = read_slice_sync_for_edit(registry, root, prev_start, line_start)?;
		if prev_byte.first().copied() != Some(b'\n') {
			return Ok(None);
		}
		delete_start = prev_start;
		deleted_bytes = prev_byte;
	} else if !deleted_bytes.ends_with(b"\n") && line_start > DocByte::ZERO {
		let prev_start = line_start.saturating_sub(1);
		let prev_byte = read_slice_sync_for_edit(registry, root, prev_start, line_start)?;
		if prev_byte.first().copied() == Some(b'\n') {
			let mut prefixed = prev_byte;
			prefixed.extend_from_slice(&deleted_bytes);
			deleted_bytes = prefixed;
			delete_start = prev_start;
		}
	}

	Ok(Some((
		TextDelta {
			global_byte_offset: delete_start,
			deleted_bytes,
			inserted_bytes: Vec::new(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		},
		register_text,
	)))
}

#[cfg(test)]
pub(crate) fn delete_to_line_end_delta(
	doc: &[u8],
	cursor_line: DocLine,
	cursor_col: DocDisplayCol,
) -> Option<TextDelta> {
	let cursor_abs_byte_offset = byte_offset_from_line_col(doc, cursor_line, cursor_col);
	let delete_start = cursor_abs_byte_offset.get() as usize;
	let (line_start, line_end_with_newline) = line_byte_range(doc, cursor_line, cursor_line);
	let line_end = if line_end_with_newline > line_start && doc[line_end_with_newline - 1] == b'\n'
	{
		line_end_with_newline - 1
	} else {
		line_end_with_newline
	};

	if delete_start >= line_end {
		return None;
	}

	Some(TextDelta {
		global_byte_offset: cursor_abs_byte_offset,
		deleted_bytes: doc[delete_start..line_end].to_vec(),
		inserted_bytes: Vec::new(),
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	})
}

pub(crate) fn apply_deltas_to_document_internal(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostics: &mut Vec<DiagnosticSpan>,
	deltas: Vec<TextDelta>,
	record_undo: bool,
) -> Result<(), String> {
	if deltas.is_empty() {
		return Ok(());
	}

	let rid = (*root_id).ok_or_else(|| "No file loaded".to_string())?;
	let total_bytes_before = registry.get_total_bytes(rid);

	// Preflight the whole group before mutating a byte: any failure aborts with the
	// document untouched, so the byte edits below and the `StateId` bump advance
	// together and can never leave bytes moved under an unchanged revision. Each delta
	// is mapped to its span in the current tree so the whole group validates against
	// one pre-mutation snapshot, regardless of application order.
	let planned_edits = fold_edits_for_deltas(&deltas);
	for (delta, edit) in deltas.iter().zip(&planned_edits) {
		preflight_delta(registry, rid, delta, edit.start, total_bytes_before)?;
	}
	commit_preflighted_deltas_to_document(
		registry,
		rid,
		folds,
		ledger,
		semantic_highlights,
		diagnostics,
		deltas,
		planned_edits,
		record_undo,
	);
	*root_id = Some(rid);

	Ok(())
}

#[allow(clippy::too_many_arguments)]
fn commit_preflighted_deltas_to_document(
	registry: &UastRegistry,
	rid: NodeId,
	folds: &mut FoldSet,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostics: &mut Vec<DiagnosticSpan>,
	deltas: Vec<TextDelta>,
	planned_edits: Vec<FoldEdit>,
	record_undo: bool,
) {
	let total_bytes_before = registry.get_total_bytes(rid);

	if total_bytes_before > 0 {
		for edit in &planned_edits {
			let target = registry
				.find_node_at_doc_byte_raw(rid, edit.start.min(DocByte::new(total_bytes_before)));
			unfold_ancestor_chain(registry, folds, target.node_id);
		}
	}
	let fold_snapshot = folds.snapshot(registry, rid);

	for delta in &deltas {
		if !delta.deleted_bytes.is_empty() {
			commit_delete(
				registry,
				rid,
				delta.global_byte_offset,
				delta.deleted_bytes.len() as u64,
			);
		}
		if !delta.inserted_bytes.is_empty() {
			commit_insert(
				registry,
				rid,
				delta.global_byte_offset,
				&delta.inserted_bytes,
			);
		}
	}
	folds.restore_after_edits(registry, rid, fold_snapshot, &planned_edits);

	rebase_semantic_highlights_after_deltas(semantic_highlights, &deltas);
	rebase_diagnostics_after_deltas(diagnostics, &deltas);
	if record_undo {
		ledger.push_group_pending_cursors(deltas);
	}
}

pub(crate) fn apply_preflighted_deltas_to_document(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostics: &mut Vec<DiagnosticSpan>,
	deltas: Vec<TextDelta>,
) {
	if deltas.is_empty() {
		return;
	}
	let rid = root_id.expect("preflighted text edit retains a document root");
	let planned_edits = fold_edits_for_deltas(&deltas);
	commit_preflighted_deltas_to_document(
		registry,
		rid,
		folds,
		ledger,
		semantic_highlights,
		diagnostics,
		deltas,
		planned_edits,
		true,
	);
	*root_id = Some(rid);
}

pub(crate) fn apply_deltas_to_document(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostics: &mut Vec<DiagnosticSpan>,
	deltas: Vec<TextDelta>,
) -> Result<(), String> {
	apply_deltas_to_document_internal(
		registry,
		root_id,
		folds,
		ledger,
		semantic_highlights,
		diagnostics,
		deltas,
		true,
	)
}

#[cfg(test)]
mod tests {
	use super::{
		TextDelta, apply_deltas_to_document, apply_deltas_to_document_internal,
		delete_char_delta_at_cursor, delete_line_delta_sparse, delete_to_line_end_delta,
		document_rewrite_delta, linewise_put_insertion, rebase_diagnostics_after_delta,
		rebase_diagnostics_after_deltas, rebase_semantic_highlights_after_delta,
		rebase_semantic_highlights_after_deltas,
	};
	use crate::core::{DocByte, DocDisplayCol, DocLine, StateId};
	use crate::engine::folding::FoldSet;
	use crate::engine::test_support::{build_document, build_exact_metric_physical_document};
	use crate::engine::undo::UndoLedger;
	use crate::svp::diagnostic::{DiagnosticSeverity, DiagnosticSpan};
	use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
	use crate::uast::UastProjection;

	#[test]
	fn rewrite_delta_tracks_changed_middle_span() {
		let delta =
			document_rewrite_delta("fn main() {}\n", "fn demo() {}\n").expect("expected a delta");
		assert_eq!(delta.0, 3);
		assert_eq!(delta.1, "main");
		assert_eq!(delta.2, "demo");
	}

	#[test]
	fn linewise_put_adds_newline_at_eof_without_trailing_break() {
		let (insert_offset, inserted_text, target_cursor_line) =
			linewise_put_insertion(b"alpha", DocLine::ZERO, "beta");
		assert_eq!(insert_offset, 5);
		assert_eq!(inserted_text, "\nbeta");
		assert_eq!(target_cursor_line, DocLine::new(1));
	}

	#[test]
	fn linewise_put_stays_on_first_line_for_empty_docs() {
		let (insert_offset, inserted_text, target_cursor_line) =
			linewise_put_insertion(b"", DocLine::ZERO, "beta\n");
		assert_eq!(insert_offset, 0);
		assert_eq!(inserted_text, "beta\n");
		assert_eq!(target_cursor_line, DocLine::ZERO);
	}

	#[test]
	fn delete_to_line_end_delta_stops_before_newline() {
		let delta =
			delete_to_line_end_delta(b"alpha beta\nomega\n", DocLine::ZERO, DocDisplayCol::new(6))
				.expect("expected delete-to-eol delta");

		assert_eq!(delta.global_byte_offset, DocByte::new(6));
		assert_eq!(delta.deleted_bytes, b"beta");
		assert_eq!(delta.inserted_bytes, b"");
	}

	#[test]
	fn delete_line_delta_sparse_removes_middle_line_with_newline() {
		let (registry, root) = build_document("alpha\nbeta\ngamma\n");
		let (delta, register_text) = delete_line_delta_sparse(&registry, root, DocLine::new(1))
			.expect("delete line should succeed")
			.expect("middle line should be deletable");

		assert_eq!(delta.global_byte_offset, DocByte::new(6));
		assert_eq!(delta.deleted_bytes, b"beta\n");
		assert_eq!(register_text, "beta\n");
	}

	#[test]
	fn delete_line_delta_sparse_absorbs_preceding_newline_for_eof_line() {
		let (registry, root) = build_document("alpha\nbeta");
		let (delta, register_text) = delete_line_delta_sparse(&registry, root, DocLine::new(1))
			.expect("delete line should succeed")
			.expect("last line should be deletable");

		assert_eq!(delta.global_byte_offset, DocByte::new(5));
		assert_eq!(delta.deleted_bytes, b"\nbeta");
		assert_eq!(register_text, "beta");
	}

	#[test]
	fn delete_char_delta_at_cursor_uses_utf8_width() {
		let (registry, root) = build_document("aéz");
		let delta = delete_char_delta_at_cursor(&registry, root, DocByte::new(1))
			.expect("delete lookup should succeed")
			.expect("expected delete delta");

		assert_eq!(delta.global_byte_offset, DocByte::new(1));
		assert_eq!(delta.deleted_bytes, "é".as_bytes());
		assert_eq!(delta.inserted_bytes, b"");
	}

	#[test]
	fn forward_delete_mutates_only_document_state() {
		let (registry, root) = build_document("abcd");
		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();

		let delta = delete_char_delta_at_cursor(&registry, root, DocByte::new(1))
			.expect("delete lookup should succeed")
			.expect("expected delete delta");
		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![delta],
		)
		.expect("forward delete should apply");

		let root = root_id.expect("mutated root");
		let bytes = registry
			.read_loaded_slice(
				root,
				DocByte::ZERO,
				DocByte::new(registry.get_total_bytes(root)),
			)
			.expect("collect mutated bytes");
		assert_eq!(String::from_utf8(bytes).expect("utf8"), "acd");
	}

	#[test]
	fn grouped_deltas_undo_as_a_single_transaction() {
		let (registry, root) = build_document("abcd\nefgh\nijkl\n");
		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();

		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![
				TextDelta {
					global_byte_offset: DocByte::new(12),
					deleted_bytes: "kl".as_bytes().to_vec(),
					inserted_bytes: Vec::new(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				},
				TextDelta {
					global_byte_offset: DocByte::new(7),
					deleted_bytes: "gh".as_bytes().to_vec(),
					inserted_bytes: Vec::new(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				},
				TextDelta {
					global_byte_offset: DocByte::new(2),
					deleted_bytes: "cd".as_bytes().to_vec(),
					inserted_bytes: Vec::new(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				},
			],
		)
		.expect("grouped delete should apply");

		assert_eq!(ledger.current_state_id, StateId::new(1));
		let bytes = registry
			.read_loaded_slice(
				root_id.expect("mutated root"),
				DocByte::ZERO,
				DocByte::new(registry.get_total_bytes(root_id.expect("mutated root"))),
			)
			.expect("read grouped edit");
		assert_eq!(String::from_utf8_lossy(&bytes), "ab\nef\nij\n");
		let cursors = crate::engine::session::CursorSnapshot::single(DocByte::ZERO);
		ledger.finish_cursor_transaction(StateId::ZERO, cursors.clone(), cursors);

		let crate::engine::undo::HistoryReplay::Text {
			deltas: undo_group,
			cursors: _,
		} = ledger
			.preview(crate::engine::undo::HistoryDirection::Undo)
			.expect("single undo should restore whole group")
		else {
			panic!("grouped sparse undo must replay text deltas");
		};
		assert_eq!(undo_group.len(), 3);
		assert_eq!(ledger.current_state_id, StateId::new(1));
		apply_deltas_to_document_internal(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			undo_group,
			false,
		)
		.expect("undo deltas should apply sparsely");
		ledger.commit(crate::engine::undo::HistoryDirection::Undo);
		assert_eq!(ledger.current_state_id, StateId::ZERO);
		let undo_root = root_id.expect("root after undo");
		let restored = registry
			.read_loaded_slice(
				undo_root,
				DocByte::ZERO,
				DocByte::new(registry.get_total_bytes(undo_root)),
			)
			.expect("collect restored bytes");
		assert_eq!(
			String::from_utf8(restored).expect("utf8"),
			"abcd\nefgh\nijkl\n"
		);

		let crate::engine::undo::HistoryReplay::Text {
			deltas: redo_group,
			cursors: _,
		} = ledger
			.preview(crate::engine::undo::HistoryDirection::Redo)
			.expect("single redo should reapply whole group")
		else {
			panic!("grouped sparse redo must replay text deltas");
		};
		assert_eq!(redo_group.len(), 3);
		assert_eq!(ledger.current_state_id, StateId::ZERO);
		apply_deltas_to_document_internal(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			redo_group,
			false,
		)
		.expect("redo deltas should apply sparsely");
		ledger.commit(crate::engine::undo::HistoryDirection::Redo);
		assert_eq!(ledger.current_state_id, StateId::new(1));
		let redo_root = root_id.expect("root after redo");
		let redone = registry
			.read_loaded_slice(
				redo_root,
				DocByte::ZERO,
				DocByte::new(registry.get_total_bytes(redo_root)),
			)
			.expect("collect redone bytes");
		assert_eq!(String::from_utf8(redone).expect("utf8"), "ab\nef\nij\n");
	}

	#[test]
	fn non_utf8_delta_round_trips_through_undo_and_redo() {
		let (registry, root) = build_document("abz");
		let leaf = registry.get_first_child(root).expect("document leaf");
		let original = b"a\xffz";
		registry.replace_authoritative_data(leaf, original.to_vec());

		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();
		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![TextDelta {
				global_byte_offset: DocByte::new(1),
				deleted_bytes: vec![0xff],
				inserted_bytes: b"X".to_vec(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			}],
		)
		.expect("byte edit should apply");
		let cursors = crate::engine::session::CursorSnapshot::single(DocByte::ZERO);
		ledger.finish_cursor_transaction(StateId::ZERO, cursors.clone(), cursors);

		let read_bytes = |root: crate::ecs::NodeId| {
			registry
				.read_loaded_slice(
					root,
					DocByte::ZERO,
					DocByte::new(registry.get_total_bytes(root)),
				)
				.expect("read document bytes")
		};
		assert_eq!(read_bytes(root), b"aXz");

		let crate::engine::undo::HistoryReplay::Text { deltas, .. } = ledger
			.preview(crate::engine::undo::HistoryDirection::Undo)
			.expect("undo delta")
		else {
			panic!("text edit must undo as text");
		};
		apply_deltas_to_document_internal(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			deltas,
			false,
		)
		.expect("byte undo should apply");
		ledger.commit(crate::engine::undo::HistoryDirection::Undo);
		assert_eq!(read_bytes(root), original);

		let crate::engine::undo::HistoryReplay::Text { deltas, .. } = ledger
			.preview(crate::engine::undo::HistoryDirection::Redo)
			.expect("redo delta")
		else {
			panic!("text edit must redo as text");
		};
		apply_deltas_to_document_internal(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			deltas,
			false,
		)
		.expect("byte redo should apply");
		ledger.commit(crate::engine::undo::HistoryDirection::Redo);
		assert_eq!(read_bytes(root), b"aXz");
	}

	#[test]
	fn byte_delta_can_delete_inside_a_utf8_scalar_without_widening() {
		let (registry, root) = build_document("aéz");
		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();

		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![TextDelta {
				global_byte_offset: DocByte::new(2),
				deleted_bytes: vec![0xa9],
				inserted_bytes: Vec::new(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			}],
		)
		.expect("byte-exact delete should apply");

		let bytes = registry
			.read_loaded_slice(root, DocByte::ZERO, DocByte::new(3))
			.expect("read byte-exact result");
		assert_eq!(bytes, b"a\xc3z");
	}

	#[test]
	fn byte_delta_deletes_exactly_across_physical_leaf_boundaries() {
		let (registry, root, path) = build_exact_metric_physical_document(&["abc", "def"]);
		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();

		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![TextDelta {
				global_byte_offset: DocByte::new(2),
				deleted_bytes: b"cde".to_vec(),
				inserted_bytes: Vec::new(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			}],
		)
		.expect("cross-leaf byte delete should apply");

		let bytes = registry
			.read_loaded_slice(root, DocByte::ZERO, DocByte::new(3))
			.expect("read cross-leaf result");
		assert_eq!(bytes, b"abf");
		registry.unregister_device(78);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn byte_replacement_prunes_a_full_indexed_middle_leaf_without_stranding_it() {
		let (registry, root, path) = build_exact_metric_physical_document(&["abc", "def", "ghi"]);
		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();

		apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![TextDelta {
				global_byte_offset: DocByte::new(2),
				deleted_bytes: b"cdefg".to_vec(),
				inserted_bytes: b"XYZ".to_vec(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			}],
		)
		.expect("cross-extent replacement should apply");

		let bytes = registry
			.read_loaded_slice(root, DocByte::ZERO, DocByte::new(7))
			.expect("read replacement result");
		assert_eq!(bytes, b"abXYZhi");
		registry.unregister_device(78);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn failed_delta_in_group_leaves_bytes_and_state_untouched() {
		// A group whose second (lower-offset) delta no longer matches the live
		// document must abort atomically: preflight rejects it before any byte
		// moves, so both the document bytes and `StateId` stay where they were.
		let (registry, root) = build_document("abcd\nefgh\nijkl\n");
		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();

		let result = apply_deltas_to_document(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![
				// Higher offset first — this one matches and, under the old loop,
				// would have mutated the tree before the mismatch below was hit.
				TextDelta {
					global_byte_offset: DocByte::new(12),
					deleted_bytes: "kl".as_bytes().to_vec(),
					inserted_bytes: Vec::new(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				},
				// Stale expectation: bytes 2..4 are "cd", not "XY".
				TextDelta {
					global_byte_offset: DocByte::new(2),
					deleted_bytes: "XY".as_bytes().to_vec(),
					inserted_bytes: Vec::new(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				},
			],
		);

		assert!(result.is_err(), "mismatched group must be rejected");
		assert_eq!(ledger.current_state_id, StateId::ZERO);
		let bytes = registry
			.read_loaded_slice(
				root_id.expect("root retained"),
				DocByte::ZERO,
				DocByte::new(registry.get_total_bytes(root_id.expect("root retained"))),
			)
			.expect("read untouched doc");
		assert_eq!(String::from_utf8_lossy(&bytes), "abcd\nefgh\nijkl\n");
	}

	#[test]
	fn cold_line_data_cannot_turn_a_committed_group_into_an_error() {
		let (registry, root, path) = build_exact_metric_physical_document(&["abc", "def"]);
		let first = registry.get_first_child(root).expect("first leaf");
		let second = registry.get_next_sibling(first).expect("second leaf");
		registry.publish_hydrated_data(second, b"def".to_vec());

		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostics = Vec::new();
		let mut folds = FoldSet::default();

		apply_deltas_to_document_internal(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			vec![TextDelta {
				global_byte_offset: DocByte::new(4),
				deleted_bytes: Vec::new(),
				inserted_bytes: "X".as_bytes().to_vec(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			}],
			false,
		)
		.expect("derived line projection must not fail the committed edit");

		assert_eq!(registry.get_total_bytes(root), 7);
		assert_eq!(ledger.current_state_id, StateId::ZERO);
		registry.unregister_device(78);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn semantic_cache_rebases_spans_after_backspace() {
		let mut semantic = vec![
			HighlightSpan::new(DocByte::new(0), DocByte::new(3), TokenCategory::Keyword),
			HighlightSpan::new(DocByte::new(10), DocByte::new(13), TokenCategory::Module),
		];

		rebase_semantic_highlights_after_delta(
			&mut semantic,
			&TextDelta {
				global_byte_offset: DocByte::new(5),
				deleted_bytes: "x".as_bytes().to_vec(),
				inserted_bytes: Vec::new(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			},
		);

		assert_eq!(
			semantic,
			vec![
				HighlightSpan::new(DocByte::new(0), DocByte::new(3), TokenCategory::Keyword),
				HighlightSpan::new(DocByte::new(9), DocByte::new(12), TokenCategory::Module),
			]
		);
	}

	#[test]
	fn semantic_cache_drops_spans_crossing_the_edit_boundary() {
		let mut semantic = vec![
			HighlightSpan::new(DocByte::new(4), DocByte::new(7), TokenCategory::Variable),
			HighlightSpan::new(DocByte::new(10), DocByte::new(14), TokenCategory::Type),
		];

		rebase_semantic_highlights_after_delta(
			&mut semantic,
			&TextDelta {
				global_byte_offset: DocByte::new(5),
				deleted_bytes: Vec::new(),
				inserted_bytes: "x".as_bytes().to_vec(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			},
		);

		assert_eq!(
			semantic,
			vec![HighlightSpan::new(
				DocByte::new(11),
				DocByte::new(15),
				TokenCategory::Type,
			)]
		);
	}

	#[test]
	fn diagnostic_cache_rebases_spans_after_backspace() {
		let mut diagnostics = vec![
			DiagnosticSpan::new(DocByte::new(2), DocByte::new(4), DiagnosticSeverity::Error),
			DiagnosticSpan::new(
				DocByte::new(10),
				DocByte::new(14),
				DiagnosticSeverity::Error,
			),
		];

		rebase_diagnostics_after_delta(
			&mut diagnostics,
			&TextDelta {
				global_byte_offset: DocByte::new(5),
				deleted_bytes: "x".as_bytes().to_vec(),
				inserted_bytes: Vec::new(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			},
		);

		assert_eq!(
			diagnostics,
			vec![
				DiagnosticSpan::new(DocByte::new(2), DocByte::new(4), DiagnosticSeverity::Error,),
				DiagnosticSpan::new(DocByte::new(9), DocByte::new(13), DiagnosticSeverity::Error,),
			]
		);
	}

	#[test]
	fn semantic_and_diagnostic_caches_rebase_once_across_a_delta_group() {
		let deltas = vec![
			TextDelta {
				global_byte_offset: DocByte::new(8),
				deleted_bytes: Vec::new(),
				inserted_bytes: "x".as_bytes().to_vec(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			},
			TextDelta {
				global_byte_offset: DocByte::new(1),
				deleted_bytes: Vec::new(),
				inserted_bytes: "y".as_bytes().to_vec(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			},
		];
		let mut semantic = vec![HighlightSpan::new(
			DocByte::new(10),
			DocByte::new(13),
			TokenCategory::Keyword,
		)];
		let mut diagnostics = vec![DiagnosticSpan::new(
			DocByte::new(3),
			DocByte::new(5),
			DiagnosticSeverity::Warning,
		)];

		rebase_semantic_highlights_after_deltas(&mut semantic, &deltas);
		rebase_diagnostics_after_deltas(&mut diagnostics, &deltas);

		assert_eq!(
			semantic,
			vec![HighlightSpan::new(
				DocByte::new(12),
				DocByte::new(15),
				TokenCategory::Keyword,
			)]
		);
		assert_eq!(
			diagnostics,
			vec![DiagnosticSpan::new(
				DocByte::new(4),
				DocByte::new(6),
				DiagnosticSeverity::Warning,
			)]
		);
	}
}
