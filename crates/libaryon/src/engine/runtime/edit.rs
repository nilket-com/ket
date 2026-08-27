use super::super::core::resolve_byte_range;
use super::super::document::cursor::{next_word_end, next_word_start, prev_word_start};
use super::super::document::edit::{
	apply_deltas_to_document, apply_deltas_to_document_internal, delete_char_delta_at_cursor,
	delete_line_delta_sparse, linewise_put_insertion_sparse,
};
use super::super::document::io::read_loaded_document;
use super::super::document::parse::word_object_delta_at_cursor;
use super::super::document::selection::{extract_visual_text_sparse, resolve_visual_ranges_sparse};
use super::super::folding::FoldSet;
use super::super::protocol::{CommandRange, EditorCommand, EditorMode, VisualKind, WordMotion};
use super::super::session::TextEditorMut;
use super::super::session::{
	Affinity, Caret, ClipboardSession, CursorSet, CursorSnapshot, ParkedGenerationRoot,
	RegisterKind, RegisterValue, RootTransition, TextAnchor, UiState,
};
use super::super::shadow::sync_shadow_to_generation;
use super::super::support::{
	line_end_byte_sparse, line_start_byte_sparse, previous_leaf, read_line_bytes_sparse,
	resolve_loaded_node_text,
};
use super::super::uast_query::{UastQuery, is_function_scope};
use super::super::undo::{HistoryDirection, HistoryReplay, TextDelta, UndoLedger};
use super::system::load_root_from_path;
use super::{CommandEffect, RuntimeContext};
use crate::core::{DocByte, DocDisplayCol, DocLine, DocLineCount, NodeByteOffset, StateId};
use crate::ecs::{NodeId, UastRegistry};
use crate::svp::BackingIdentity;
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::HighlightSpan;
use crate::uast::UastProjection;
use std::collections::HashMap;
use std::path::Path;

pub(super) fn apply_document_deltas(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostic_spans: &mut Vec<DiagnosticSpan>,
	deltas: Vec<TextDelta>,
) -> Result<(), String> {
	let target_byte = deltas.last().map(|delta| {
		delta
			.global_byte_offset
			.saturating_add(delta.inserted_bytes.len() as u64)
	});
	apply_deltas_to_document(
		registry,
		root_id,
		folds,
		ledger,
		semantic_highlights,
		diagnostic_spans,
		deltas,
	)?;
	if let (Some(root), Some(byte)) = (*root_id, target_byte) {
		caret.set_byte(
			DocByte::new(byte.get().min(registry.get_total_bytes(root))),
			caret.affinity(),
		);
	}
	Ok(())
}

pub(super) fn apply_document_deltas_with_cursor_override(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostic_spans: &mut Vec<DiagnosticSpan>,
	deltas: Vec<TextDelta>,
	cursor_byte: DocByte,
) -> Result<(), String> {
	apply_deltas_to_document_internal(
		registry,
		root_id,
		folds,
		ledger,
		semantic_highlights,
		diagnostic_spans,
		deltas,
		true,
	)?;
	if let Some(root) = *root_id {
		caret.set_byte(
			DocByte::new(cursor_byte.get().min(registry.get_total_bytes(root))),
			caret.affinity(),
		);
	}
	Ok(())
}

fn apply_single_delta(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostic_spans: &mut Vec<DiagnosticSpan>,
	delta: TextDelta,
) -> Result<(), String> {
	apply_document_deltas(
		registry,
		root_id,
		folds,
		caret,
		ledger,
		semantic_highlights,
		diagnostic_spans,
		vec![delta],
	)
}

#[derive(Clone, Copy)]
enum PointEdit<'a> {
	Insert(&'a str),
	Backspace,
	Delete,
	Replace(char),
	ToggleCase,
}

struct PlannedPointEdit {
	deltas: Vec<TextDelta>,
	before: CursorSnapshot,
	after: CursorSnapshot,
}

fn plan_point_edit(
	registry: &UastRegistry,
	root: NodeId,
	cursors: &CursorSet,
	edit: PointEdit<'_>,
) -> Result<PlannedPointEdit, String> {
	let before = cursors.snapshot();
	let mut deltas = Vec::new();
	let mut targets = Vec::with_capacity(cursors.len());

	for caret in cursors.all() {
		let cursor_byte = caret.byte();
		let (delta, target, affinity) = match edit {
			PointEdit::Insert(text) => (
				(!text.is_empty()).then(|| TextDelta {
					global_byte_offset: cursor_byte,
					deleted_bytes: Vec::new(),
					inserted_bytes: text.as_bytes().to_vec(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				}),
				cursor_byte,
				Affinity::After,
			),
			PointEdit::Backspace => {
				let node = caret
					.node_cache()
					.ok_or_else(|| "Loading cursor extent".to_string())?;
				let delta = backspace_delta_at_cursor(registry, root, node.node, node.offset)?;
				let target = delta
					.as_ref()
					.map(|delta| delta.global_byte_offset)
					.unwrap_or(cursor_byte);
				(delta, target, Affinity::Before)
			}
			PointEdit::Delete => (
				delete_char_delta_at_cursor(registry, root, cursor_byte)?,
				cursor_byte,
				Affinity::Before,
			),
			PointEdit::Replace(ch) => {
				let mut delta = delete_char_delta_at_cursor(registry, root, cursor_byte)?;
				if let Some(delta) = &mut delta {
					let mut buf = [0; 4];
					delta.inserted_bytes = ch.encode_utf8(&mut buf).as_bytes().to_vec();
				}
				(delta, cursor_byte, Affinity::Before)
			}
			PointEdit::ToggleCase => {
				let mut delta = delete_char_delta_at_cursor(registry, root, cursor_byte)?;
				if let Some(delta) = &mut delta {
					delta.inserted_bytes = toggle_case_bytes(&delta.deleted_bytes);
				}
				(delta, cursor_byte, Affinity::Before)
			}
		};
		if let Some(delta) = delta {
			deltas.push(delta);
		}
		targets.push((target, affinity));
	}

	deltas.sort_unstable_by_key(|delta| std::cmp::Reverse(delta.global_byte_offset));
	validate_non_overlapping_deltas(&deltas)?;
	let transformed: Vec<_> = targets
		.into_iter()
		.map(|(byte, affinity)| (transform_cursor_byte(byte, affinity, &deltas), affinity))
		.collect();
	let mut transformed =
		cursors
			.all()
			.copied()
			.zip(transformed)
			.map(|(mut caret, (byte, affinity))| {
				caret.set_anchor(TextAnchor::new(byte, affinity));
				caret
			});
	let primary = transformed.next().unwrap_or_else(|| before.primary());
	let mut seen = std::collections::HashSet::new();
	let secondary = transformed
		.filter(|caret| caret.byte() != primary.byte() && seen.insert(caret.byte()))
		.collect();
	let after = CursorSnapshot::Points { primary, secondary };

	Ok(PlannedPointEdit {
		deltas,
		before,
		after,
	})
}

fn validate_non_overlapping_deltas(deltas: &[TextDelta]) -> Result<(), String> {
	for pair in deltas.windows(2) {
		let higher = &pair[0];
		let lower = &pair[1];
		let lower_end = lower
			.global_byte_offset
			.saturating_add(lower.deleted_bytes.len() as u64);
		if lower_end > higher.global_byte_offset {
			return Err("Overlapping multicursor edits are not supported".to_string());
		}
	}
	Ok(())
}

fn transform_cursor_byte(original: DocByte, affinity: Affinity, deltas: &[TextDelta]) -> DocByte {
	let mut shift = 0i128;
	for delta in deltas.iter().rev() {
		let start = delta.global_byte_offset.get();
		let deleted = delta.deleted_bytes.len() as u64;
		let inserted = delta.inserted_bytes.len() as u64;
		let end = start.saturating_add(deleted);
		if original.get() < start {
			break;
		}
		if deleted == 0 {
			if original.get() > start
				|| (original.get() == start && matches!(affinity, Affinity::After))
			{
				shift += i128::from(inserted);
			}
			continue;
		}
		if original.get() >= end {
			shift += i128::from(inserted) - i128::from(deleted);
			continue;
		}
		let replacement_offset = if matches!(affinity, Affinity::After) {
			inserted
		} else {
			0
		};
		return DocByte::new(((i128::from(start) + shift).max(0) as u64) + replacement_offset);
	}
	DocByte::new((i128::from(original.get()) + shift).max(0) as u64)
}

fn toggle_case_text(text: &str) -> String {
	let mut toggled = String::with_capacity(text.len());
	for ch in text.chars() {
		if ch.is_uppercase() {
			toggled.extend(ch.to_lowercase());
		} else if ch.is_lowercase() {
			toggled.extend(ch.to_uppercase());
		} else {
			toggled.push(ch);
		}
	}
	toggled
}

fn toggle_case_bytes(bytes: &[u8]) -> Vec<u8> {
	std::str::from_utf8(bytes)
		.map(toggle_case_text)
		.map(String::into_bytes)
		.unwrap_or_else(|_| bytes.to_vec())
}

fn split_segment_lines(segment: &str) -> (Vec<&str>, bool) {
	let trailing_newline = segment.ends_with('\n');
	let body = if trailing_newline {
		&segment[..segment.len() - 1]
	} else {
		segment
	};
	let lines: Vec<&str> = if body.is_empty() {
		Vec::new()
	} else {
		body.split('\n').collect()
	};
	(lines, trailing_newline)
}

fn join_lines(lines: &[String], trailing_newline: bool) -> String {
	let mut out = lines.join("\n");
	if trailing_newline && !lines.is_empty() {
		out.push('\n');
	}
	out
}

fn numeric_key(line: &str) -> (bool, i64, String) {
	let trimmed = line.trim_start();
	let mut end = 0;
	let bytes = trimmed.as_bytes();
	if !bytes.is_empty() && (bytes[0] == b'-' || bytes[0] == b'+') {
		end = 1;
	}
	while end < bytes.len() && bytes[end].is_ascii_digit() {
		end += 1;
	}
	let has_number = end > 0 && (end > 1 || bytes[0].is_ascii_digit());
	if has_number {
		let n: i64 = trimmed[..end].parse().unwrap_or(0);
		(true, n, line.to_string())
	} else {
		(false, 0, line.to_string())
	}
}

fn apply_sort_to_segment(
	segment: &str,
	reverse: bool,
	unique: bool,
	numeric: bool,
	ignore_case: bool,
) -> (String, usize) {
	let (lines, trailing_newline) = split_segment_lines(segment);
	let mut owned: Vec<String> = lines.into_iter().map(|s| s.to_string()).collect();

	if numeric {
		owned.sort_by_key(|a| numeric_key(a));
	} else if ignore_case {
		owned.sort_by_key(|a| a.to_lowercase());
	} else {
		owned.sort();
	}

	if reverse {
		owned.reverse();
	}

	if unique {
		if ignore_case {
			let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
			owned.retain(|l| seen.insert(l.to_lowercase()));
		} else {
			let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
			owned.retain(|l| seen.insert(l.clone()));
		}
	}

	let count = owned.len();
	(join_lines(&owned, trailing_newline), count)
}

fn apply_uniq_to_segment(segment: &str, ignore_case: bool) -> (String, usize) {
	let (lines, trailing_newline) = split_segment_lines(segment);
	let eq = |a: &str, b: &str| -> bool {
		if ignore_case {
			a.eq_ignore_ascii_case(b)
		} else {
			a == b
		}
	};

	let mut deduped: Vec<String> = Vec::with_capacity(lines.len());
	for line in lines {
		if deduped.last().is_some_and(|prev| eq(prev, line)) {
			continue;
		}
		deduped.push(line.to_string());
	}

	let count = deduped.len();
	(join_lines(&deduped, trailing_newline), count)
}

pub(super) fn handle_sort_doctab_command(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	range: CommandRange,
	reverse: bool,
	unique: bool,
	numeric: bool,
	ignore_case: bool,
) -> CommandEffect {
	tab.document.bump_topology_rev();
	let state_before = tab.document.ledger.current_state_id;
	let cursors_before = tab.cursor.snapshot();
	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	if !tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		tab.document.ledger.current_state_id,
	) {
		ui.status_message = Some("Loading cursor extent".to_string());
		return CommandEffect::render();
	}
	let effect = {
		let mut root_id = Some(tab.document.root_id);
		handle_sort_command(
			registry,
			&mut root_id,
			&mut tab.view.folds,
			tab.cursor.primary_mut(),
			&mut tab.document.ledger,
			&mut tab.document.semantic.highlights,
			&mut tab.document.semantic.diagnostics,
			&mut ui.status_message,
			range,
			reverse,
			unique,
			numeric,
			ignore_case,
		)
	};
	let caret = tab.cursor.primary_mut();
	caret.set_byte(caret.byte(), Affinity::Before);
	caret.resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		tab.document.ledger.current_state_id,
	);
	tab.cursor.sync_preferred_col();
	tab.document.ledger.finish_cursor_transaction(
		state_before,
		cursors_before,
		tab.cursor.snapshot(),
	);
	effect
}

pub(super) fn handle_sort_command(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostic_spans: &mut Vec<DiagnosticSpan>,
	status_message: &mut Option<String>,
	range: CommandRange,
	reverse: bool,
	unique: bool,
	numeric: bool,
	ignore_case: bool,
) -> CommandEffect {
	let Some(rid) = *root_id else {
		return CommandEffect::default();
	};
	let bytes = match read_loaded_document(registry, rid) {
		Ok(b) => b,
		Err(msg) => {
			*status_message = Some(msg.to_string());
			return CommandEffect::render();
		}
	};
	let (start, end) =
		resolve_byte_range(&range, &bytes, caret.abs_line()).unwrap_or((0, bytes.len()));
	if start >= end {
		*status_message = Some("Empty range".to_string());
		return CommandEffect::render();
	}
	let segment = match std::str::from_utf8(&bytes[start..end]) {
		Ok(segment) => segment,
		Err(_) => {
			*status_message = Some("Sort range is not valid UTF-8".to_string());
			return CommandEffect::render();
		}
	};
	let (new_text, line_count) =
		apply_sort_to_segment(segment, reverse, unique, numeric, ignore_case);
	if new_text.as_bytes() == &bytes[start..end] {
		*status_message = Some(format!("{} lines sorted", line_count));
		return CommandEffect::render();
	}

	let delta = TextDelta {
		global_byte_offset: DocByte::new(start as u64),
		deleted_bytes: bytes[start..end].to_vec(),
		inserted_bytes: new_text.into_bytes(),
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	};
	match apply_document_deltas(
		registry,
		root_id,
		folds,
		caret,
		ledger,
		semantic_highlights,
		diagnostic_spans,
		vec![delta],
	) {
		Ok(()) => {
			*status_message = Some(format!("{} lines sorted", line_count));
		}
		Err(msg) => *status_message = Some(msg),
	}
	CommandEffect::render()
}

pub(super) fn handle_uniq_doctab_command(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	range: CommandRange,
	ignore_case: bool,
) -> CommandEffect {
	tab.document.bump_topology_rev();
	let state_before = tab.document.ledger.current_state_id;
	let cursors_before = tab.cursor.snapshot();
	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	if !tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		tab.document.ledger.current_state_id,
	) {
		ui.status_message = Some("Loading cursor extent".to_string());
		return CommandEffect::render();
	}
	let effect = {
		let mut root_id = Some(tab.document.root_id);
		handle_uniq_command(
			registry,
			&mut root_id,
			&mut tab.view.folds,
			tab.cursor.primary_mut(),
			&mut tab.document.ledger,
			&mut tab.document.semantic.highlights,
			&mut tab.document.semantic.diagnostics,
			&mut ui.status_message,
			range,
			ignore_case,
		)
	};
	let caret = tab.cursor.primary_mut();
	caret.set_byte(caret.byte(), Affinity::Before);
	caret.resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		tab.document.ledger.current_state_id,
	);
	tab.cursor.sync_preferred_col();
	tab.document.ledger.finish_cursor_transaction(
		state_before,
		cursors_before,
		tab.cursor.snapshot(),
	);
	effect
}

pub(super) fn handle_uniq_command(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostic_spans: &mut Vec<DiagnosticSpan>,
	status_message: &mut Option<String>,
	range: CommandRange,
	ignore_case: bool,
) -> CommandEffect {
	let Some(rid) = *root_id else {
		return CommandEffect::default();
	};
	let bytes = match read_loaded_document(registry, rid) {
		Ok(b) => b,
		Err(msg) => {
			*status_message = Some(msg.to_string());
			return CommandEffect::render();
		}
	};
	let (start, end) =
		resolve_byte_range(&range, &bytes, caret.abs_line()).unwrap_or((0, bytes.len()));
	if start >= end {
		*status_message = Some("Empty range".to_string());
		return CommandEffect::render();
	}
	let segment = match std::str::from_utf8(&bytes[start..end]) {
		Ok(segment) => segment,
		Err(_) => {
			*status_message = Some("Uniq range is not valid UTF-8".to_string());
			return CommandEffect::render();
		}
	};
	let (new_text, line_count) = apply_uniq_to_segment(segment, ignore_case);
	if new_text.as_bytes() == &bytes[start..end] {
		*status_message = Some(format!("{} lines", line_count));
		return CommandEffect::render();
	}

	let delta = TextDelta {
		global_byte_offset: DocByte::new(start as u64),
		deleted_bytes: bytes[start..end].to_vec(),
		inserted_bytes: new_text.into_bytes(),
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	};
	match apply_document_deltas(
		registry,
		root_id,
		folds,
		caret,
		ledger,
		semantic_highlights,
		diagnostic_spans,
		vec![delta],
	) {
		Ok(()) => {
			*status_message = Some(format!("{} lines", line_count));
		}
		Err(msg) => *status_message = Some(msg),
	}
	CommandEffect::render()
}

fn visual_delete_deltas_for_ranges(
	registry: &UastRegistry,
	root: NodeId,
	ranges: &[(DocByte, DocByte)],
) -> Result<Vec<TextDelta>, String> {
	let mut deltas = Vec::with_capacity(ranges.len());
	for (start, end) in ranges.iter().rev() {
		let deleted_bytes = registry
			.read_loaded_slice(root, *start, end.saturating_add(1))
			.map_err(|msg| msg.to_string())?;
		if deleted_bytes.is_empty() {
			continue;
		}

		deltas.push(TextDelta {
			global_byte_offset: *start,
			deleted_bytes,
			inserted_bytes: Vec::new(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		});
	}

	Ok(deltas)
}

fn backspace_delta_at_cursor(
	registry: &UastRegistry,
	root: NodeId,
	cursor_node: NodeId,
	cursor_offset: u32,
) -> Result<Option<TextDelta>, String> {
	let (delete_node, delete_offset) = if cursor_offset == 0 {
		match previous_leaf(registry, cursor_node, root) {
			Some(prev) => {
				let prev_len =
					u32::try_from(unsafe { (*registry.metrics[prev.index()].get()).byte_length })
						.expect("leaf byte length fits cursor offset");
				(prev, prev_len)
			}
			None => return Ok(None),
		}
	} else {
		(cursor_node, cursor_offset)
	};

	let text = resolve_loaded_node_text(registry, delete_node)?;
	let bytes = text.as_bytes();
	let delete_end = delete_offset as usize;
	if delete_end == 0 || delete_end > bytes.len() {
		return Ok(None);
	}

	let mut delete_start = delete_end - 1;
	while delete_start > 0 && (bytes[delete_start] & 0xC0) == 0x80 {
		delete_start -= 1;
	}

	let delete_start_byte = registry.doc_byte_for_node_offset(
		root,
		delete_node,
		NodeByteOffset::new(delete_start as u32),
	);

	Ok(Some(TextDelta {
		global_byte_offset: delete_start_byte,
		deleted_bytes: bytes[delete_start..delete_end].to_vec(),
		inserted_bytes: Vec::new(),
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	}))
}

fn store_named_text(
	registers: &mut HashMap<char, RegisterValue>,
	clipboard: &mut super::super::clipboard::ClipboardHandle,
	register: char,
	text: &str,
	kind: RegisterKind,
	sync_clipboard: bool,
) {
	let value = RegisterValue {
		text: text.to_string(),
		kind,
	};
	registers.insert('"', value.clone());
	if register == '+' {
		clipboard.set_text(text);
	} else if register != '"' {
		registers.insert(register, value);
	}
	if sync_clipboard {
		clipboard.set_text(text);
	}
}

fn get_register_value(
	registers: &HashMap<char, RegisterValue>,
	clipboard: &mut super::super::clipboard::ClipboardHandle,
	register: char,
) -> Option<RegisterValue> {
	resolve_register_value(registers, register, || clipboard.get_text())
}

fn resolve_register_value<F: FnOnce() -> Option<String>>(
	registers: &HashMap<char, RegisterValue>,
	register: char,
	read_clipboard: F,
) -> Option<RegisterValue> {
	if register == '+' {
		read_clipboard()
			.map(|text| RegisterValue {
				text,
				kind: RegisterKind::CharWise,
			})
			.or_else(|| registers.get(&'"').cloned())
	} else if register == '"' {
		// The default register is synced with the system clipboard so that a
		// copy performed in another program (another editor, a browser, etc.)
		// can be pasted via plain `p` without needing the explicit `"+` prefix.
		// When the clipboard and the internal register hold the same text we
		// prefer the internal copy to preserve RegisterKind (LineWise etc.)
		// across a within-Baryon yank-then-put sequence; otherwise the
		// external text wins and is treated as CharWise.
		let internal = registers.get(&'"').cloned();
		match read_clipboard() {
			Some(clipboard_text) => match &internal {
				Some(existing) if existing.text == clipboard_text => internal,
				_ => Some(RegisterValue {
					text: clipboard_text,
					kind: RegisterKind::CharWise,
				}),
			},
			None => internal,
		}
	} else {
		registers
			.get(&register)
			.cloned()
			.or_else(|| registers.get(&'"').cloned())
	}
}

fn schedule_clear_flash(tx_internal: &super::super::request::InternalSender) {
	let tx_flash = tx_internal.clone();
	std::thread::spawn(move || {
		std::thread::sleep(std::time::Duration::from_millis(200));
		let _ = tx_flash.send(super::super::request::InternalEvent::ClearYankFlash);
	});
}

fn clamp_utf8_boundary(doc: &str, byte: usize) -> usize {
	let mut byte = byte.min(doc.len());
	while byte > 0 && !doc.is_char_boundary(byte) {
		byte -= 1;
	}
	byte
}

fn next_utf8_boundary(doc: &str, byte: usize) -> usize {
	let byte = clamp_utf8_boundary(doc, byte);
	doc[byte..]
		.chars()
		.next()
		.map(|ch| byte + ch.len_utf8())
		.unwrap_or(byte)
}

fn resolve_word_motion_range(
	doc: &str,
	cursor_abs_byte: DocByte,
	motion: WordMotion,
) -> Option<(DocByte, DocByte)> {
	let cursor_byte = clamp_utf8_boundary(doc, cursor_abs_byte.get() as usize);
	if cursor_byte >= doc.len() {
		return None;
	}

	let (start, end) = match motion {
		WordMotion::NextStart => (cursor_byte, next_word_start(doc, cursor_byte)),
		WordMotion::PrevStart => (prev_word_start(doc, cursor_byte), cursor_byte),
		WordMotion::NextEnd => {
			let word_end = next_word_end(doc, cursor_byte);
			(cursor_byte, next_utf8_boundary(doc, word_end))
		}
	};

	if start >= end {
		None
	} else {
		Some((DocByte::new(start as u64), DocByte::new(end as u64)))
	}
}

fn word_motion_delta_at_cursor(
	registry: &UastRegistry,
	root: NodeId,
	cursor_abs_byte: DocByte,
	motion: WordMotion,
) -> Result<Option<(TextDelta, String)>, String> {
	let bytes = read_loaded_document(registry, root).map_err(|msg| msg.to_string())?;
	let doc = std::str::from_utf8(&bytes)
		.map_err(|_| "Loaded document is not valid UTF-8".to_string())?;
	let Some((start, end)) = resolve_word_motion_range(doc, cursor_abs_byte, motion) else {
		return Ok(None);
	};
	let start_idx = start.get() as usize;
	let end_idx = end.get() as usize;
	let deleted_bytes = doc.as_bytes()[start_idx..end_idx].to_vec();
	let register_text = doc[start_idx..end_idx].to_string();

	Ok(Some((
		TextDelta {
			global_byte_offset: start,
			deleted_bytes,
			inserted_bytes: Vec::new(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		},
		register_text,
	)))
}

fn leading_indentation_len(bytes: &[u8]) -> usize {
	bytes
		.iter()
		.take_while(|&&b| matches!(b, b' ' | b'\t'))
		.count()
}

fn join_line_delta(
	registry: &UastRegistry,
	root: NodeId,
	cursor_line: DocLine,
) -> Result<Option<TextDelta>, String> {
	let last_line = DocLine::new(registry.get_total_newlines(root));
	if cursor_line >= last_line {
		return Ok(None);
	}

	let join_start = line_end_byte_sparse(registry, root, cursor_line, false)?;
	let join_after_newline = line_end_byte_sparse(registry, root, cursor_line, true)?;
	if join_after_newline <= join_start {
		return Ok(None);
	}

	let next_line = cursor_line + DocLineCount::new(1);
	let next_line_bytes = read_line_bytes_sparse(registry, root, next_line, false)?;
	let delete_end =
		join_after_newline.saturating_add(leading_indentation_len(&next_line_bytes) as u64);
	let deleted_bytes = registry
		.read_loaded_slice(root, join_start, delete_end)
		.map_err(|msg| msg.to_string())?;
	if deleted_bytes.is_empty() {
		return Ok(None);
	}

	Ok(Some(TextDelta {
		global_byte_offset: join_start,
		deleted_bytes,
		inserted_bytes: b" ".to_vec(),
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	}))
}

fn charwise_put_delta_and_cursor(
	registry: &UastRegistry,
	root: NodeId,
	cursor_abs_byte: DocByte,
	text: &str,
) -> Result<Option<(TextDelta, DocByte)>, String> {
	if text.is_empty() {
		return Ok(None);
	}

	let bytes = read_loaded_document(registry, root).map_err(|msg| msg.to_string())?;
	let doc = std::str::from_utf8(&bytes)
		.map_err(|_| "Loaded document is not valid UTF-8".to_string())?;
	let file_size = doc.len();
	let cursor_byte = clamp_utf8_boundary(doc, cursor_abs_byte.get() as usize);
	let insert_offset = if file_size == 0 {
		0
	} else {
		next_utf8_boundary(doc, cursor_byte).min(file_size)
	};
	let last_char_start = text.char_indices().last().map(|(idx, _)| idx).unwrap_or(0);
	let cursor_after = DocByte::new((insert_offset + last_char_start) as u64);

	Ok(Some((
		TextDelta {
			global_byte_offset: DocByte::new(insert_offset as u64),
			deleted_bytes: Vec::new(),
			inserted_bytes: text.as_bytes().to_vec(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		},
		cursor_after,
	)))
}

pub(super) fn handle_edit_command(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostic_spans: &mut Vec<DiagnosticSpan>,
	active_visual: &mut Option<(DocByte, VisualKind)>,
	registers: &mut HashMap<char, RegisterValue>,
	clipboard: &mut super::super::clipboard::ClipboardHandle,
	status_message: &mut Option<String>,
	mode_override: &mut Option<EditorMode>,
	command: EditorCommand,
) -> CommandEffect {
	let mut needs_render = false;

	match command {
		EditorCommand::SetVisualSelection { anchor, kind } => {
			*active_visual = Some((anchor, kind));
			needs_render = true;
		}
		EditorCommand::ClearVisualSelection => {
			*active_visual = None;
			needs_render = true;
		}
		EditorCommand::VisualYank { anchor, kind } => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match resolve_visual_ranges_sparse(
					registry,
					rid,
					folds,
					anchor,
					cursor_abs_byte,
					kind,
				) {
					Ok(ranges) => {
						if ranges.is_empty() {
							*status_message = Some("Nothing selected".to_string());
						} else {
							match extract_visual_text_sparse(registry, rid, &ranges) {
								Ok(selected_text) => {
									store_named_text(
										registers,
										clipboard,
										'"',
										&selected_text,
										if matches!(kind, VisualKind::Line) {
											RegisterKind::LineWise
										} else {
											RegisterKind::CharWise
										},
										true,
									);
									*status_message =
										Some(format!("{} bytes yanked", selected_text.len()));
								}
								Err(msg) => *status_message = Some(msg),
							}
						}
						*active_visual = None;
						needs_render = true;
					}
					Err(msg) => {
						*status_message = Some(msg.to_string());
						*active_visual = None;
						needs_render = true;
					}
				}
			}
		}
		visual_delete_cmd @ (EditorCommand::VisualDelete { anchor, kind }
		| EditorCommand::VisualChange { anchor, kind }) => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match resolve_visual_ranges_sparse(
					registry,
					rid,
					folds,
					anchor,
					cursor_abs_byte,
					kind,
				) {
					Ok(ranges) => {
						if ranges.is_empty() {
							*status_message = Some("Nothing selected".to_string());
						} else {
							match extract_visual_text_sparse(registry, rid, &ranges) {
								Ok(selected_text) => {
									store_named_text(
										registers,
										clipboard,
										'"',
										&selected_text,
										if matches!(kind, VisualKind::Line) {
											RegisterKind::LineWise
										} else {
											RegisterKind::CharWise
										},
										false,
									);
								}
								Err(msg) => {
									*status_message = Some(msg);
									*active_visual = None;
									return CommandEffect { needs_render };
								}
							}

							match visual_delete_deltas_for_ranges(registry, rid, &ranges) {
								Ok(deltas) => {
									if !deltas.is_empty() {
										match apply_document_deltas(
											registry,
											root_id,
											folds,
											caret,
											ledger,
											semantic_highlights,
											diagnostic_spans,
											deltas,
										) {
											Ok(()) => {
												if matches!(
													visual_delete_cmd,
													EditorCommand::VisualChange { .. }
												) {
													*mode_override = Some(EditorMode::Insert);
												}
											}
											Err(msg) => *status_message = Some(msg),
										}
									}
								}
								Err(msg) => *status_message = Some(msg),
							}
						}

						*active_visual = None;
						needs_render = true;
					}
					Err(msg) => {
						*status_message = Some(msg.to_string());
						*active_visual = None;
						needs_render = true;
					}
				}
			}
		}
		word_cmd @ (EditorCommand::DeleteInnerWord | EditorCommand::ChangeInnerWord) => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte_offset = caret.byte();
				match word_object_delta_at_cursor(registry, rid, cursor_abs_byte_offset) {
					Ok(Some(delta)) => match apply_single_delta(
						registry,
						root_id,
						folds,
						caret,
						ledger,
						semantic_highlights,
						diagnostic_spans,
						delta,
					) {
						Ok(()) => {
							if matches!(word_cmd, EditorCommand::ChangeInnerWord) {
								*mode_override = Some(EditorMode::Insert);
							}
						}
						Err(msg) => *status_message = Some(msg),
					},
					Ok(None) => {
						*status_message = Some("No structural word at cursor".to_string());
					}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::DeleteWordMotion { register, motion } => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match word_motion_delta_at_cursor(registry, rid, cursor_abs_byte, motion) {
					Ok(Some((delta, deleted_text))) => {
						store_named_text(
							registers,
							clipboard,
							register,
							&deleted_text,
							RegisterKind::CharWise,
							true,
						);
						if let Err(msg) = apply_single_delta(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							delta,
						) {
							*status_message = Some(msg);
						}
					}
					Ok(None) => {
						*status_message = Some("No word motion target".to_string());
					}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::ChangeWordMotion { register, motion } => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match word_motion_delta_at_cursor(registry, rid, cursor_abs_byte, motion) {
					Ok(Some((delta, deleted_text))) => {
						store_named_text(
							registers,
							clipboard,
							register,
							&deleted_text,
							RegisterKind::CharWise,
							true,
						);
						match apply_single_delta(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							delta,
						) {
							Ok(()) => {
								*mode_override = Some(EditorMode::Insert);
							}
							Err(msg) => *status_message = Some(msg),
						}
					}
					Ok(None) => {
						*status_message = Some("No word motion target".to_string());
					}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::YankWordMotion { register, motion } => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match word_motion_delta_at_cursor(registry, rid, cursor_abs_byte, motion) {
					Ok(Some((_delta, yanked_text))) => {
						store_named_text(
							registers,
							clipboard,
							register,
							&yanked_text,
							RegisterKind::CharWise,
							true,
						);
					}
					Ok(None) => {
						*status_message = Some("No word motion target".to_string());
					}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::JoinLine => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match join_line_delta(registry, rid, caret.abs_line()) {
					Ok(Some(delta)) => {
						let cursor_after = cursor_abs_byte.min(delta.global_byte_offset);
						if let Err(msg) = apply_document_deltas_with_cursor_override(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							vec![delta],
							cursor_after,
						) {
							*status_message = Some(msg);
						}
					}
					Ok(None) => {
						*status_message = Some("Already at last line".to_string());
					}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		func_delete_cmd @ (EditorCommand::DeleteAroundFunction { .. }
		| EditorCommand::ChangeAroundFunction) => {
			let register = match &func_delete_cmd {
				EditorCommand::DeleteAroundFunction { register } => *register,
				_ => '"',
			};

			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				let q = UastQuery::new(registry, rid);
				if let Some(leaf) = q.node_at_offset(cursor_abs_byte) {
					if let Some(func) = q.find_ancestor(leaf, is_function_scope) {
						let (fn_start, fn_end) = q.node_bounds(func);
						match registry.read_loaded_slice(rid, fn_start, fn_end) {
							Ok(bytes) => {
								let register_text = String::from_utf8_lossy(&bytes).into_owned();
								store_named_text(
									registers,
									clipboard,
									register,
									&register_text,
									RegisterKind::CharWise,
									true,
								);
								match apply_single_delta(
									registry,
									root_id,
									folds,
									caret,
									ledger,
									semantic_highlights,
									diagnostic_spans,
									TextDelta {
										global_byte_offset: fn_start,
										deleted_bytes: bytes,
										inserted_bytes: Vec::new(),
										state_before: StateId::ZERO,
										state_after: StateId::ZERO,
									},
								) {
									Ok(()) => {
										if matches!(
											func_delete_cmd,
											EditorCommand::ChangeAroundFunction
										) {
											*mode_override = Some(EditorMode::Insert);
										}
									}
									Err(msg) => *status_message = Some(msg),
								}
							}
							Err(msg) => *status_message = Some(msg.to_string()),
						}
					} else {
						*status_message = Some("No enclosing function scope".to_string());
					}
				}
				needs_render = true;
			}
		}
		EditorCommand::DeleteToLineEnd => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match line_end_byte_sparse(registry, rid, caret.abs_line(), false) {
					Ok(line_end) => {
						if cursor_abs_byte < line_end {
							match registry.read_loaded_slice(rid, cursor_abs_byte, line_end) {
								Ok(deleted_bytes) => {
									if let Err(msg) = apply_single_delta(
										registry,
										root_id,
										folds,
										caret,
										ledger,
										semantic_highlights,
										diagnostic_spans,
										TextDelta {
											global_byte_offset: cursor_abs_byte,
											deleted_bytes,
											inserted_bytes: Vec::new(),
											state_before: StateId::ZERO,
											state_after: StateId::ZERO,
										},
									) {
										*status_message = Some(msg);
									}
								}
								Err(msg) => {
									return {
										*status_message = Some(msg.to_string());
										CommandEffect { needs_render }
									};
								}
							}
						} else {
							*status_message = Some("Already at end of line".to_string());
						}
						needs_render = true;
					}
					Err(msg) => {
						*status_message = Some(msg.to_string());
						needs_render = true;
					}
				}
			}
		}
		EditorCommand::DeleteLine { register } => {
			if let Some(rid) = *root_id {
				match delete_line_delta_sparse(registry, rid, caret.abs_line()) {
					Ok(Some((delta, register_text))) => {
						store_named_text(
							registers,
							clipboard,
							register,
							&register_text,
							RegisterKind::LineWise,
							true,
						);
						if let Err(msg) = apply_single_delta(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							delta,
						) {
							*status_message = Some(msg);
						}
					}
					Ok(None) => *status_message = Some("Nothing to delete".to_string()),
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::InsertChar(c) => {
			if root_id.is_some() {
				let global_offset = caret.byte();
				let mut buf = [0; 4];
				let s = c.encode_utf8(&mut buf);
				if let Err(msg) = apply_single_delta(
					registry,
					root_id,
					folds,
					caret,
					ledger,
					semantic_highlights,
					diagnostic_spans,
					TextDelta {
						global_byte_offset: global_offset,
						deleted_bytes: Vec::new(),
						inserted_bytes: s.as_bytes().to_vec(),
						state_before: StateId::ZERO,
						state_after: StateId::ZERO,
					},
				) {
					*status_message = Some(msg);
				}
				needs_render = true;
			}
		}
		EditorCommand::InsertText(text) => {
			if text.is_empty() {
			} else if root_id.is_some() {
				let global_offset = caret.byte();
				if let Err(msg) = apply_single_delta(
					registry,
					root_id,
					folds,
					caret,
					ledger,
					semantic_highlights,
					diagnostic_spans,
					TextDelta {
						global_byte_offset: global_offset,
						deleted_bytes: Vec::new(),
						inserted_bytes: text.into_bytes(),
						state_before: StateId::ZERO,
						state_after: StateId::ZERO,
					},
				) {
					*status_message = Some(msg);
				}
				needs_render = true;
			}
		}
		EditorCommand::Backspace => {
			if let Some(rid) = *root_id {
				let Some(node) = caret.node_cache() else {
					*status_message = Some("Loading cursor extent".to_string());
					return CommandEffect::render();
				};
				match backspace_delta_at_cursor(registry, rid, node.node, node.offset) {
					Ok(Some(delta)) => {
						if let Err(msg) = apply_single_delta(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							delta,
						) {
							*status_message = Some(msg);
						}
						needs_render = true;
					}
					Ok(None) => {}
					Err(msg) => {
						*status_message = Some(msg);
						needs_render = true;
					}
				}
			}
		}
		EditorCommand::Delete => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match delete_char_delta_at_cursor(registry, rid, cursor_abs_byte) {
					Ok(Some(delta)) => {
						if let Err(msg) = apply_single_delta(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							delta,
						) {
							*status_message = Some(msg);
						}
					}
					Ok(None) => {}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::ReplaceChar(new_ch) => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match delete_char_delta_at_cursor(registry, rid, cursor_abs_byte) {
					Ok(Some(mut delta)) => {
						let mut buf = [0; 4];
						delta.inserted_bytes = new_ch.encode_utf8(&mut buf).as_bytes().to_vec();
						// Vim's `r` keeps the cursor on the replaced character,
						// so pin the post-edit cursor to the replacement's start byte.
						if let Err(msg) = apply_document_deltas_with_cursor_override(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							vec![delta],
							cursor_abs_byte,
						) {
							*status_message = Some(msg);
						}
					}
					Ok(None) => {
						*status_message = Some("Nothing to replace".to_string());
					}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::ToggleCaseAtCursor => {
			if let Some(rid) = *root_id {
				let cursor_abs_byte = caret.byte();
				match delete_char_delta_at_cursor(registry, rid, cursor_abs_byte) {
					Ok(Some(mut delta)) => {
						// Unicode case mapping is not 1-to-1: `ß` uppercases
						// to "SS", `İ` lowercases to "i̇". Taking `.next()` on
						// the iterator silently drops the tail bytes and
						// corrupts the buffer, so we walk the full string
						// form for each scalar and concatenate every yielded
						// code point. Scalars with no case (digits, symbols,
						// most CJK) simply round-trip through themselves.
						let Ok(deleted_text) = std::str::from_utf8(&delta.deleted_bytes) else {
							*status_message = Some("Toggle target is not valid UTF-8".to_string());
							return CommandEffect::render();
						};
						let mut toggled = String::with_capacity(delta.deleted_bytes.len());
						for c in deleted_text.chars() {
							if c.is_uppercase() {
								for low in c.to_lowercase() {
									toggled.push(low);
								}
							} else if c.is_lowercase() {
								for up in c.to_uppercase() {
									toggled.push(up);
								}
							} else {
								toggled.push(c);
							}
						}
						delta.inserted_bytes = toggled.into_bytes();
						// Cursor advances past the toggled char (matches Vim `~`).
						if let Err(msg) = apply_single_delta(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							delta,
						) {
							*status_message = Some(msg);
						}
					}
					Ok(None) => {}
					Err(msg) => *status_message = Some(msg),
				}
				needs_render = true;
			}
		}
		EditorCommand::OpenLineBelow => {
			if let Some(rid) = *root_id {
				match line_end_byte_sparse(registry, rid, caret.abs_line(), false) {
					Ok(line_end) => {
						if let Err(msg) = apply_single_delta(
							registry,
							root_id,
							folds,
							caret,
							ledger,
							semantic_highlights,
							diagnostic_spans,
							TextDelta {
								global_byte_offset: line_end,
								deleted_bytes: Vec::new(),
								inserted_bytes: b"\n".to_vec(),
								state_before: StateId::ZERO,
								state_after: StateId::ZERO,
							},
						) {
							*status_message = Some(msg);
						}
					}
					Err(msg) => *status_message = Some(msg),
				}
				*mode_override = Some(EditorMode::Insert);
				needs_render = true;
			}
		}
		EditorCommand::OpenLineAbove => {
			if let Some(rid) = *root_id {
				let Ok(line_start) = line_start_byte_sparse(registry, rid, caret.abs_line()) else {
					*status_message = Some("Line index is still building".to_string());
					return CommandEffect::render();
				};
				if let Err(msg) = apply_document_deltas_with_cursor_override(
					registry,
					root_id,
					folds,
					caret,
					ledger,
					semantic_highlights,
					diagnostic_spans,
					vec![TextDelta {
						global_byte_offset: line_start,
						deleted_bytes: Vec::new(),
						inserted_bytes: b"\n".to_vec(),
						state_before: StateId::ZERO,
						state_after: StateId::ZERO,
					}],
					line_start,
				) {
					*status_message = Some(msg);
				}
				*mode_override = Some(EditorMode::Insert);
				needs_render = true;
			}
		}
		EditorCommand::Put { register } => {
			if let Some(rid) = *root_id {
				if let Some(value) = get_register_value(registers, clipboard, register) {
					if !value.text.is_empty() {
						match value.kind {
							RegisterKind::LineWise => {
								if let Ok((insert_offset, inserted_text, target_cursor_line)) =
									linewise_put_insertion_sparse(
										registry,
										rid,
										caret.abs_line(),
										&value.text,
									) {
									if let Err(msg) = apply_single_delta(
										registry,
										root_id,
										folds,
										caret,
										ledger,
										semantic_highlights,
										diagnostic_spans,
										TextDelta {
											global_byte_offset: insert_offset,
											deleted_bytes: Vec::new(),
											inserted_bytes: inserted_text.into_bytes(),
											state_before: StateId::ZERO,
											state_after: StateId::ZERO,
										},
									) {
										*status_message = Some(msg);
									} else if root_id.is_some() {
										caret.set_pending_target(
											target_cursor_line,
											DocDisplayCol::ZERO,
											Affinity::After,
											true,
											None,
										);
									}
									needs_render = true;
								}
							}
							RegisterKind::CharWise => {
								let cursor_abs_byte = caret.byte();
								match charwise_put_delta_and_cursor(
									registry,
									rid,
									cursor_abs_byte,
									&value.text,
								) {
									Ok(Some((delta, cursor_after))) => {
										if let Err(msg) = apply_document_deltas_with_cursor_override(
											registry,
											root_id,
											folds,
											caret,
											ledger,
											semantic_highlights,
											diagnostic_spans,
											vec![delta],
											cursor_after,
										) {
											*status_message = Some(msg);
										}
										needs_render = true;
									}
									Ok(None) => {}
									Err(msg) => {
										*status_message = Some(msg);
										needs_render = true;
									}
								}
							}
						}
					}
				} else {
					*status_message = Some("Register empty".to_string());
					needs_render = true;
				}
			}
		}
		_ => unreachable!(),
	}

	CommandEffect { needs_render }
}

pub(super) fn handle_edit_doctab_command(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	clipboard_session: &mut ClipboardSession,
	ui: &mut UiState,
	command: EditorCommand,
) -> CommandEffect {
	let preserves_node_map = matches!(
		&command,
		EditorCommand::ClearVisualSelection
			| EditorCommand::SetVisualSelection { .. }
			| EditorCommand::VisualYank { .. }
			| EditorCommand::YankWordMotion { .. }
	);
	if !preserves_node_map {
		tab.document.bump_topology_rev();
	}
	{
		let root = tab.document.root_id;
		let topology_rev = tab.document.topology_rev;
		let state_id = tab.document.ledger.current_state_id;
		let mut pending = false;
		let folds = &mut tab.view.folds;
		tab.cursor.visit_carets_mut(|caret| {
			pending |= !caret.resolve(registry, root, folds, topology_rev, state_id);
		});
		if pending {
			ui.status_message = Some("Loading cursor extent".to_string());
			return CommandEffect::render();
		}
	}

	if tab.cursor.len() > 1 {
		let point_edit = match &command {
			EditorCommand::InsertChar(ch) => {
				let mut buf = [0; 4];
				Some(ch.encode_utf8(&mut buf).to_string())
			}
			EditorCommand::InsertText(text) => Some(text.clone()),
			_ => None,
		};
		let edit = if let Some(text) = point_edit.as_deref() {
			Some(PointEdit::Insert(text))
		} else {
			match &command {
				EditorCommand::Backspace => Some(PointEdit::Backspace),
				EditorCommand::Delete => Some(PointEdit::Delete),
				EditorCommand::ReplaceChar(ch) => Some(PointEdit::Replace(*ch)),
				EditorCommand::ToggleCaseAtCursor => Some(PointEdit::ToggleCase),
				_ => None,
			}
		};
		if let Some(edit) = edit {
			return apply_point_edit(registry, tab, ui, edit);
		}

		if matches!(command, EditorCommand::SetVisualSelection { .. }) {
			tab.cursor.collapse();
		} else if !matches!(command, EditorCommand::ClearVisualSelection) {
			ui.status_message =
				Some("Command is not supported while multiple cursors are active".to_string());
			return CommandEffect::render();
		}
	}

	let mut active_visual = tab.cursor.active_visual();
	let cursors_before = tab.cursor.snapshot();
	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	let state_id_before = tab.document.ledger.current_state_id;
	if !tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id_before,
	) {
		ui.status_message = Some("Loading cursor extent".to_string());
		return CommandEffect::render();
	}
	let moved_affinity = match &command {
		EditorCommand::InsertChar(_)
		| EditorCommand::InsertText(_)
		| EditorCommand::OpenLineAbove
		| EditorCommand::OpenLineBelow
		| EditorCommand::AppendAtLineEnd
		| EditorCommand::Put { .. } => Affinity::After,
		_ => Affinity::Before,
	};
	let effect = {
		let mut root_id = Some(tab.document.root_id);
		handle_edit_command(
			registry,
			&mut root_id,
			&mut tab.view.folds,
			tab.cursor.primary_mut(),
			&mut tab.document.ledger,
			&mut tab.document.semantic.highlights,
			&mut tab.document.semantic.diagnostics,
			&mut active_visual,
			&mut clipboard_session.registers,
			&mut clipboard_session.clipboard,
			&mut ui.status_message,
			&mut ui.mode_override,
			command,
		)
	};
	{
		let root = tab.document.root_id;
		let caret = tab.cursor.primary_mut();
		if caret.pending_target().is_none() {
			caret.set_byte(caret.byte(), moved_affinity);
		}
		caret.resolve(
			registry,
			root,
			&mut tab.view.folds,
			topology_rev,
			tab.document.ledger.current_state_id,
		);
	}
	tab.cursor.set_active_visual(active_visual);
	if tab.cursor.points().is_some() {
		tab.cursor.sync_preferred_col();
	}
	tab.document.ledger.finish_cursor_transaction(
		state_id_before,
		cursors_before,
		tab.cursor.snapshot(),
	);
	effect
}

fn apply_point_edit(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	edit: PointEdit<'_>,
) -> CommandEffect {
	let root = tab.document.root_id;
	let Some(cursors) = tab.cursor.points() else {
		return CommandEffect::default();
	};
	let plan = match plan_point_edit(registry, root, cursors, edit) {
		Ok(plan) => plan,
		Err(msg) => {
			ui.status_message = Some(msg);
			return CommandEffect::render();
		}
	};
	if plan.deltas.is_empty() {
		return CommandEffect::default();
	}
	let history_deltas = plan.deltas.clone();
	let mut root_id = Some(tab.document.root_id);
	let result = apply_deltas_to_document_internal(
		registry,
		&mut root_id,
		&mut tab.view.folds,
		&mut tab.document.ledger,
		&mut tab.document.semantic.highlights,
		&mut tab.document.semantic.diagnostics,
		plan.deltas,
		false,
	);
	if let Err(msg) = result {
		ui.status_message = Some(msg);
		return CommandEffect::render();
	}
	tab.document
		.ledger
		.push_group_with_cursors(history_deltas, plan.before, plan.after.clone());
	tab.cursor.restore_snapshot(plan.after);
	CommandEffect::render()
}

pub(super) fn handle_clipboard_command(
	registry: &UastRegistry,
	tx_internal: &super::super::request::InternalSender,
	root_id: Option<NodeId>,
	cursor_abs_line: DocLine,
	registers: &mut HashMap<char, RegisterValue>,
	clipboard: &mut super::super::clipboard::ClipboardHandle,
	yank_flash: &mut Option<(DocByte, DocByte)>,
	status_message: &mut Option<String>,
	command: EditorCommand,
) -> CommandEffect {
	let mut needs_render = false;

	match command {
		EditorCommand::YankLine { register } => {
			if let Some(rid) = root_id
				&& let (Ok(line_start), Ok(line_bytes)) = (
					line_start_byte_sparse(registry, rid, cursor_abs_line),
					read_line_bytes_sparse(registry, rid, cursor_abs_line, true),
				) {
				let line_text = String::from_utf8_lossy(&line_bytes).into_owned();
				store_named_text(
					registers,
					clipboard,
					register,
					&line_text,
					RegisterKind::LineWise,
					true,
				);
				let line_end = line_start.saturating_add(line_bytes.len() as u64);
				*yank_flash = Some((line_start, line_end));
				schedule_clear_flash(tx_internal);
				*status_message = Some(format!("{} bytes yanked", line_bytes.len()));
				needs_render = true;
			}
		}
		_ => unreachable!(),
	}

	CommandEffect { needs_render }
}

pub(super) fn handle_clipboard_doctab_command(
	registry: &UastRegistry,
	tx_internal: &super::super::request::InternalSender,
	tab: &TextEditorMut<'_>,
	clipboard_session: &mut ClipboardSession,
	ui: &mut UiState,
	command: EditorCommand,
) -> CommandEffect {
	if tab.cursor.len() > 1 && matches!(command, EditorCommand::YankLine { .. }) {
		ui.status_message =
			Some("Command is not supported while multiple cursors are active".to_string());
		return CommandEffect::render();
	}
	handle_clipboard_command(
		registry,
		tx_internal,
		Some(tab.document.root_id),
		tab.cursor.abs_line(),
		&mut clipboard_session.registers,
		&mut clipboard_session.clipboard,
		&mut ui.yank_flash,
		&mut ui.status_message,
		command,
	)
}

fn apply_history_replay(
	ctx: &RuntimeContext<'_>,
	tab: &mut TextEditorMut<'_>,
	replay: HistoryReplay,
	direction: HistoryDirection,
	next_dynamic_device_id: &mut u16,
) -> Result<(), String> {
	tab.document.bump_topology_rev();
	match replay {
		HistoryReplay::Text { deltas, cursors } => {
			let mut root_id = Some(tab.document.root_id);
			apply_deltas_to_document_internal(
				ctx.registry,
				&mut root_id,
				&mut tab.view.folds,
				&mut tab.document.ledger,
				&mut tab.document.semantic.highlights,
				&mut tab.document.semantic.diagnostics,
				deltas,
				false,
			)?;
			tab.document.ledger.commit(direction);
			tab.cursor.restore_snapshot(cursors);
			{
				let root = tab.document.root_id;
				let folds = &mut tab.view.folds;
				tab.cursor.visit_carets_mut(|caret| {
					caret.resolve(
						ctx.registry,
						root,
						folds,
						tab.document.topology_rev,
						tab.document.ledger.current_state_id,
					);
				});
			}
		}
		HistoryReplay::SwapGeneration { target, cursors } => {
			let outgoing_target = tab
				.document
				.shadow
				.as_ref()
				.and_then(|shadow| shadow.current_generation())
				.cloned()
				.ok_or_else(|| "Active shadow generation is unavailable".to_string())?;
			let outgoing_file_identity =
				BackingIdentity::for_path(std::path::Path::new(&outgoing_target.path))
					.map_err(|error| format!("Shadow generation changed: {error}"))?;
			let allocated_before = ctx.registry.allocation_event_count();
			let payload_before = ctx.registry.authoritative_payload_snapshot().retained();
			let mut revived = tab.document.parked_generations.take(&target);
			if revived.as_ref().is_some_and(|entry| {
				entry.target.byte_len != target.byte_len
					|| ctx.registry.get_total_bytes(entry.root) != target.byte_len
					|| match BackingIdentity::for_path(std::path::Path::new(&target.path)) {
						Ok(identity) => identity != entry.file_identity,
						Err(_) => true,
					}
			}) {
				let invalid = revived.take().expect("invalid cache hit");
				super::retire_parked_generations_in_mutation(
					ctx,
					&mut tab.document.pending_parked_retirements,
					&mut tab.document.pending_parked_payload_retirements,
					vec![invalid],
				);
			}

			let mut fresh = None;
			let (rid, new_backing_device_id, revived_live, revived_payload) =
				if let Some(entry) = revived.as_ref() {
					(
						entry.root,
						entry.device_id,
						Some(entry.live_nodes),
						Some(entry.payload_bytes),
					)
				} else {
					let (root, _cursor_leaf, device_id) =
						load_root_from_path(ctx, Path::new(&target.path), next_dynamic_device_id)
							.map_err(|msg| format!("Failed to reload shadow generation: {msg}"))?;
					fresh = Some((root, device_id));
					(root, device_id, None, None)
				};
			if revived_live.is_none() && ctx.registry.get_total_bytes(rid) != target.byte_len {
				ctx.registry.cancel_metric_scan(rid);
				if let Some(device_id) = new_backing_device_id {
					ctx.resolver.unregister_device(device_id);
				}
				let candidate_nodes = ctx
					.registry
					.allocation_event_count()
					.saturating_sub(allocated_before);
				let candidate_payload = ctx
					.registry
					.authoritative_payload_snapshot()
					.retained()
					.checked_sub(payload_before)
					.expect("abandoned candidate payload counter regressed");
				ctx.registry.retire_orphaned_forest(
					[rid],
					candidate_nodes,
					candidate_payload,
					crate::ecs::registry::ReclaimFence::NeverPublished,
				);
				return Err("Shadow generation length no longer matches its manifest".to_string());
			}

			let sync_result = tab
				.document
				.shadow
				.as_mut()
				.ok_or_else(|| "Active shadow session is unavailable".to_string())
				.and_then(|shadow| sync_shadow_to_generation(shadow, &target));
			if let Err(msg) = sync_result {
				if let Some(entry) = revived {
					tab.document.parked_generations.restore(entry);
				}
				if let Some((candidate_root, candidate_device_id)) = fresh {
					ctx.registry.cancel_metric_scan(candidate_root);
					if let Some(device_id) = candidate_device_id {
						ctx.resolver.unregister_device(device_id);
					}
					let candidate_nodes = ctx
						.registry
						.allocation_event_count()
						.saturating_sub(allocated_before);
					let candidate_payload = ctx
						.registry
						.authoritative_payload_snapshot()
						.retained()
						.checked_sub(payload_before)
						.expect("abandoned candidate payload counter regressed");
					ctx.registry.retire_orphaned_forest(
						[candidate_root],
						candidate_nodes,
						candidate_payload,
						crate::ecs::registry::ReclaimFence::NeverPublished,
					);
				}
				return Err(msg);
			}

			let outgoing = ParkedGenerationRoot {
				target: outgoing_target,
				root: tab.document.root_id,
				device_id: tab.document.backing_device_id,
				live_nodes: tab.document.uast_accounting.current_live_nodes,
				payload_bytes: tab.document.uast_accounting.current_live_payload_bytes,
				file_identity: outgoing_file_identity,
			};
			if let Some(evicted) = tab.document.parked_generations.insert(outgoing) {
				super::retire_parked_generations_in_mutation(
					ctx,
					&mut tab.document.pending_parked_retirements,
					&mut tab.document.pending_parked_payload_retirements,
					vec![evicted],
				);
			}
			tab.document.pending_root_transition = Some(RootTransition::Park {
				revived_live,
				revived_payload,
			});
			tab.document.ledger.commit(direction);
			tab.document.root_id = rid;
			tab.view.folds.clear();
			tab.document.backing_path = Some(target.path.clone().into());
			tab.document.backing_device_id = new_backing_device_id;
			tab.reset_pipeline();
			tab.document.search_matches.clear();
			*tab.match_index = None;
			*tab.match_info = None;
			tab.cursor.restore_snapshot(cursors);
			tab.cursor.clamp_anchors(ctx.registry.get_total_bytes(rid));
			let folds = &mut tab.view.folds;
			tab.cursor.visit_carets_mut(|caret| {
				caret.resolve(
					ctx.registry,
					rid,
					folds,
					tab.document.topology_rev,
					tab.document.ledger.current_state_id,
				);
			});
		}
	}
	Ok(())
}

pub(super) fn handle_history_doctab_command(
	ctx: &RuntimeContext<'_>,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	next_dynamic_device_id: &mut u16,
	command: EditorCommand,
) -> CommandEffect {
	let (direction, boundary_message) = match command {
		EditorCommand::Undo => (HistoryDirection::Undo, "Already at oldest change"),
		EditorCommand::Redo => (HistoryDirection::Redo, "Already at newest change"),
		_ => unreachable!(),
	};
	if let Some(replay) = tab.document.ledger.preview(direction) {
		if let Err(msg) = apply_history_replay(ctx, tab, replay, direction, next_dynamic_device_id)
		{
			ui.status_message = Some(msg);
		}
	} else {
		ui.status_message = Some(boundary_message.to_string());
	}
	CommandEffect::render()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::{DocByte, DocDisplayCol, DocLine};
	use crate::engine::clipboard::ClipboardHandle;
	use crate::engine::document::io::read_loaded_document;
	use crate::engine::folding::FoldSet;
	use crate::engine::language::LanguageRegistry;
	use crate::engine::protocol::{EditorCommand, EditorMode, VisualKind, WordMotion};
	use crate::engine::runtime::navigation::handle_navigation_command;
	use crate::engine::session::{
		ClipboardSession, RegisterKind, RegisterValue, SettingsPersistent, TestTextEditor, UiState,
	};
	use crate::engine::settings::SaveDurability;
	use crate::engine::shadow::ShadowSession;
	use crate::engine::support::resolve_loaded_node_text;
	use crate::engine::test_support::temp_test_path;
	use crate::engine::test_support::{build_document, build_grafted_rust_document};
	use crate::engine::uast_query::{UastQuery, is_function_scope};
	use crate::engine::undo::{GenerationTarget, SwapGeneration};
	use crate::svp::SvpResolver;
	use std::collections::{BTreeMap, HashMap};
	use std::sync::{Arc, Mutex, mpsc};

	type GraftedEditResult = (
		String,
		DocByte,
		DocLine,
		DocDisplayCol,
		Option<String>,
		Option<EditorMode>,
		HashMap<char, RegisterValue>,
	);

	/// Serializes tests that touch the real OS clipboard. Yank / delete now
	/// sync to the system clipboard, so parallel `cargo test` threads would
	/// otherwise race on that single shared resource and clobber each
	/// other's register contents mid-test. Any test that constructs a
	/// `ClipboardHandle::new()` must hold this lock for its duration.
	static CLIPBOARD_TEST_LOCK: Mutex<()> = Mutex::new(());

	fn resolved_caret(registry: &UastRegistry, root: NodeId, byte: DocByte) -> Caret {
		let mut caret = Caret::at_start();
		let mut folds = FoldSet::default();
		caret.set_byte(byte, Affinity::After);
		assert!(caret.resolve(
			registry,
			root,
			&mut folds,
			crate::core::TopologyRev::ZERO,
			StateId::ZERO,
		));
		caret.preferred_col = caret.abs_col();
		caret
	}

	fn test_settings() -> SettingsPersistent {
		SettingsPersistent {
			theme_name: "onedark".to_string(),
			minimap_enabled: false,
			wrap_enabled: false,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::new(),
		}
	}

	fn test_ui() -> UiState {
		UiState {
			status_message: None,
			capacity_warning_shown: false,
			document_text_response: None,
			document_text_response_buffer_id: None,
			document_text_response_state_id: None,
			busy: false,
			pending_quit: false,
			mode_override: None,
			yank_flash: None,
		}
	}

	fn test_runtime(registry: &Arc<UastRegistry>) -> (SvpResolver, LanguageRegistry) {
		let (notify, _notifications) = mpsc::channel();
		(
			SvpResolver::new_blocking_for_tests(registry.clone(), notify),
			LanguageRegistry::new(),
		)
	}

	fn assert_walk_parents_are_consistent(registry: &UastRegistry, root: NodeId) {
		let mut visit = Some(root);
		while let Some(node) = visit {
			if node != root {
				let parent = unsafe { (*registry.edges[node.index()].get()).parent }
					.expect("non-root walk node should have a parent");
				let mut child = registry.get_first_child(parent);
				let mut found = false;
				while let Some(candidate) = child {
					if candidate == node {
						found = true;
						break;
					}
					child = registry.get_next_sibling(candidate);
				}
				assert!(found, "parent {:?} is missing child {:?}", parent, node);
			}
			visit = registry.get_next_node_in_walk(node);
		}
	}

	#[test]
	fn edit_attempts_invalidate_topology_before_the_fallible_path() {
		let _clipboard_guard = CLIPBOARD_TEST_LOCK
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let (registry, root) = build_document("");
		let leaf = registry.get_first_child(root).expect("document leaf");
		let settings = test_settings();
		let mut fixture = TestTextEditor::new(&registry, &settings, 80, 24);
		let mut tab = fixture.editor_mut();
		tab.document.root_id = root;
		tab.cursor.primary_mut().reset_to_start(
			leaf,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		);
		let mut clipboard = ClipboardSession {
			registers: HashMap::new(),
			clipboard: ClipboardHandle::new(),
		};
		let mut ui = test_ui();

		for command in [
			EditorCommand::SetVisualSelection {
				anchor: DocByte::ZERO,
				kind: VisualKind::Char,
			},
			EditorCommand::VisualYank {
				anchor: DocByte::ZERO,
				kind: VisualKind::Char,
			},
			EditorCommand::YankWordMotion {
				register: '"',
				motion: WordMotion::NextStart,
			},
			EditorCommand::ClearVisualSelection,
		] {
			handle_edit_doctab_command(&registry, &mut tab, &mut clipboard, &mut ui, command);
			assert_eq!(tab.document.topology_rev, crate::core::TopologyRev::ZERO);
		}

		let state_before = tab.document.ledger.current_state_id;

		handle_edit_doctab_command(
			&registry,
			&mut tab,
			&mut clipboard,
			&mut ui,
			EditorCommand::Backspace,
		);

		assert_eq!(tab.document.topology_rev, crate::core::TopologyRev::new(1));
		assert_eq!(tab.document.ledger.current_state_id, state_before);
		assert_eq!(
			read_loaded_document(&registry, root).expect("read document"),
			b""
		);

		handle_edit_doctab_command(
			&registry,
			&mut tab,
			&mut clipboard,
			&mut ui,
			EditorCommand::InsertChar('x'),
		);

		assert_eq!(tab.document.topology_rev, crate::core::TopologyRev::new(2));
		assert!(tab.document.ledger.current_state_id > state_before);
		assert_eq!(
			read_loaded_document(&registry, root).expect("read document"),
			b"x"
		);
	}

	#[test]
	fn undo_and_redo_restore_lossless_cursor_snapshots() {
		let _clipboard_guard = CLIPBOARD_TEST_LOCK
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let (registry, root) = build_document("abc");
		let registry = Arc::new(registry);
		let (resolver, languages) = test_runtime(&registry);
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
			init_script: None,
			init_entrypoint: None,
		};
		let mut fixture = TestTextEditor::new(&registry, &test_settings(), 80, 24);
		let mut tab = fixture.editor_mut();
		tab.document.root_id = root;
		let caret = tab.cursor.primary_mut();
		caret.set_byte(DocByte::new(1), Affinity::Before);
		assert!(caret.resolve(
			&registry,
			root,
			&mut tab.view.folds,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		));
		caret.preferred_col = DocDisplayCol::new(17);
		let before = tab.cursor.snapshot();
		let mut clipboard = ClipboardSession {
			registers: HashMap::new(),
			clipboard: ClipboardHandle::new(),
		};
		let mut ui = test_ui();

		handle_edit_doctab_command(
			&registry,
			&mut tab,
			&mut clipboard,
			&mut ui,
			EditorCommand::InsertChar('x'),
		);
		let after = tab.cursor.snapshot();
		assert_eq!(read_loaded_document(&registry, root).unwrap(), b"axbc");

		let mut next_dynamic_device_id = crate::engine::core::FILE_DEVICE_ID + 1;
		handle_history_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::Undo,
		);
		assert_eq!(read_loaded_document(&registry, root).unwrap(), b"abc");
		assert_eq!(tab.cursor.snapshot(), before);

		handle_history_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::Redo,
		);
		assert_eq!(read_loaded_document(&registry, root).unwrap(), b"axbc");
		assert_eq!(tab.cursor.snapshot(), after);
	}

	#[test]
	fn failed_text_undo_leaves_bytes_state_and_history_untouched() {
		let (registry, root) = build_document("abc");
		let registry = Arc::new(registry);
		let (resolver, languages) = test_runtime(&registry);
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
			init_script: None,
			init_entrypoint: None,
		};
		let mut fixture = TestTextEditor::new(&registry, &test_settings(), 80, 24);
		let mut tab = fixture.editor_mut();
		tab.document.root_id = root;
		let snapshot = tab.cursor.snapshot();
		tab.document.ledger.push_group_with_cursors(
			vec![TextDelta {
				global_byte_offset: DocByte::new(1),
				deleted_bytes: Vec::new(),
				inserted_bytes: "x".as_bytes().to_vec(),
				state_before: StateId::ZERO,
				state_after: StateId::ZERO,
			}],
			snapshot.clone(),
			snapshot,
		);
		let state_before = tab.document.ledger.current_state_id;
		let mut ui = test_ui();
		let mut next_dynamic_device_id = crate::engine::core::FILE_DEVICE_ID + 1;

		handle_history_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::Undo,
		);

		assert_eq!(read_loaded_document(&registry, root).unwrap(), b"abc");
		assert_eq!(tab.document.ledger.current_state_id, state_before);
		assert!(
			tab.document
				.ledger
				.preview(HistoryDirection::Undo)
				.is_some()
		);
		assert!(
			ui.status_message.as_deref().is_some_and(
				|message| message.contains("Edit range no longer matches live document")
			)
		);
	}

	#[test]
	fn failed_generation_undo_leaves_root_state_and_history_untouched() {
		let (registry, root) = build_document("abc");
		let registry = Arc::new(registry);
		let (resolver, languages) = test_runtime(&registry);
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
			init_script: None,
			init_entrypoint: None,
		};
		let missing = temp_test_path("missing-undo-generation");
		let _ = std::fs::remove_file(&missing);
		let mut fixture = TestTextEditor::new(&registry, &test_settings(), 80, 24);
		let mut tab = fixture.editor_mut();
		tab.document.root_id = root;
		let snapshot = tab.cursor.snapshot();
		tab.document.ledger.push_swap_generation(SwapGeneration {
			before: GenerationTarget {
				generation_id: 1,
				path: missing.to_string_lossy().into_owned(),
				byte_len: 3,
			},
			after: GenerationTarget {
				generation_id: 2,
				path: "current".to_string(),
				byte_len: 3,
			},
			cursors_before: snapshot.clone(),
			cursors_after: snapshot,
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		});
		let state_before = tab.document.ledger.current_state_id;
		let root_before = tab.document.root_id;
		let mut ui = test_ui();
		let mut next_dynamic_device_id = crate::engine::core::FILE_DEVICE_ID + 1;

		handle_history_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::Undo,
		);

		assert_eq!(tab.document.root_id, root_before);
		assert_eq!(read_loaded_document(&registry, root).unwrap(), b"abc");
		assert_eq!(tab.document.ledger.current_state_id, state_before);
		assert!(
			tab.document
				.ledger
				.preview(HistoryDirection::Undo)
				.is_some()
		);
		assert!(
			ui.status_message
				.as_deref()
				.is_some_and(|message| message.contains("shadow generation"))
		);
	}

	#[test]
	fn failed_generation_sync_restores_a_temporarily_removed_cache_hit() {
		let registry = Arc::new(UastRegistry::new(32));
		let (resolver, languages) = test_runtime(&registry);
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
			init_script: None,
			init_entrypoint: None,
		};
		let before_path = temp_test_path("cached-generation-before");
		let after_path = temp_test_path("cached-generation-after");
		std::fs::write(&before_path, b"before").expect("write before generation");
		std::fs::write(&after_path, b"after!").expect("write after generation");
		let before = GenerationTarget {
			generation_id: 1,
			path: before_path.to_string_lossy().into_owned(),
			byte_len: 6,
		};
		let after = GenerationTarget {
			generation_id: 2,
			path: after_path.to_string_lossy().into_owned(),
			byte_len: 6,
		};
		let mut next_dynamic_device_id = crate::engine::core::FILE_DEVICE_ID + 1;
		let (before_root, _, before_device) =
			load_root_from_path(&ctx, Path::new(&before.path), &mut next_dynamic_device_id)
				.expect("load cached before generation");
		let (after_root, after_leaf, after_device) =
			load_root_from_path(&ctx, Path::new(&after.path), &mut next_dynamic_device_id)
				.expect("load active after generation");

		let invalid_dir = temp_test_path("cached-generation-invalid-shadow-dir");
		std::fs::write(&invalid_dir, b"not a directory").expect("create invalid shadow dir");
		let mut fixture = TestTextEditor::new(&registry, &test_settings(), 80, 24);
		let mut tab = fixture.editor_mut();
		tab.document.root_id = after_root;
		tab.document.backing_path = Some(after.path.clone().into());
		tab.document.backing_device_id = after_device;
		tab.document.shadow = Some(ShadowSession {
			dir: invalid_dir.to_string_lossy().into_owned(),
			original_path: after.path.clone(),
			generations: vec![before.clone(), after.clone()],
			current_index: 1,
			next_generation_id: 3,
		});
		assert!(
			tab.document
				.parked_generations
				.insert(ParkedGenerationRoot {
					target: before.clone(),
					root: before_root,
					device_id: before_device,
					live_nodes: 2,
					payload_bytes: 0,
					file_identity: BackingIdentity::for_path(&before_path)
						.expect("before identity"),
				})
				.is_none()
		);
		let snapshot = tab.cursor.snapshot();
		tab.document.ledger.push_swap_generation(SwapGeneration {
			before,
			after,
			cursors_before: snapshot.clone(),
			cursors_after: snapshot,
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		});
		tab.cursor.primary_mut().reset_to_start(
			after_leaf,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		);
		let state_before = tab.document.ledger.current_state_id;
		let allocated_before = registry.stats().allocated_nodes;
		let devices_before = resolver.registered_device_counts();
		let mut ui = test_ui();

		handle_history_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::Undo,
		);

		assert_eq!(tab.document.root_id, after_root);
		assert_eq!(tab.document.backing_device_id, after_device);
		assert_eq!(tab.document.ledger.current_state_id, state_before);
		assert_eq!(tab.document.parked_generations.entries().len(), 1);
		assert_eq!(
			tab.document.parked_generations.entries()[0].root,
			before_root
		);
		assert_eq!(registry.stats().allocated_nodes, allocated_before);
		assert_eq!(resolver.registered_device_counts(), devices_before);
		assert!(
			ui.status_message
				.as_deref()
				.is_some_and(|message| { message.contains("Failed to create shadow dir") })
		);

		if let Some(device_id) = before_device {
			resolver.unregister_device(device_id);
		}
		if let Some(device_id) = after_device {
			resolver.unregister_device(device_id);
		}
		let _ = std::fs::remove_file(before_path);
		let _ = std::fs::remove_file(after_path);
		let _ = std::fs::remove_file(invalid_dir);
	}

	#[test]
	fn default_register_falls_back_to_system_clipboard_for_p_paste() {
		// Regression: plain `p` in normal mode used to resolve only through
		// the in-process `"` register, so text copied in another application
		// was never visible to Baryon. Now the default register transparently
		// falls back to the system clipboard.
		let mut registers = HashMap::new();

		// 1. Empty internal register + external clipboard text -> clipboard wins.
		let value = resolve_register_value(&registers, '"', || Some("external".to_string()))
			.expect("default register should resolve from the clipboard");
		assert_eq!(value.text, "external");
		assert_eq!(value.kind, RegisterKind::CharWise);

		// 2. Internal register matches clipboard -> keep internal kind (so a
		//    LineWise yank done in Baryon still pastes line-wise after the
		//    sync pushed the same text into the clipboard).
		registers.insert(
			'"',
			RegisterValue {
				text: "external".to_string(),
				kind: RegisterKind::LineWise,
			},
		);
		let value = resolve_register_value(&registers, '"', || Some("external".to_string()))
			.expect("matching clipboard should yield the internal register value");
		assert_eq!(value.kind, RegisterKind::LineWise);

		// 3. Internal register is stale relative to clipboard -> clipboard wins.
		registers.insert(
			'"',
			RegisterValue {
				text: "stale internal".to_string(),
				kind: RegisterKind::LineWise,
			},
		);
		let value = resolve_register_value(&registers, '"', || Some("external".to_string()))
			.expect("diverging clipboard should override stale internal register");
		assert_eq!(value.text, "external");
		assert_eq!(value.kind, RegisterKind::CharWise);

		// 4. Clipboard unavailable -> fall through to internal register.
		let value = resolve_register_value(&registers, '"', || None)
			.expect("absent clipboard should leave internal register intact");
		assert_eq!(value.text, "stale internal");
		assert_eq!(value.kind, RegisterKind::LineWise);
	}

	#[test]
	fn sort_segment_sorts_lines_ascending() {
		let (out, count) =
			apply_sort_to_segment("cherry\nbanana\napple\n", false, false, false, false);
		assert_eq!(out, "apple\nbanana\ncherry\n");
		assert_eq!(count, 3);
	}

	#[test]
	fn sort_segment_reverse_sorts_descending() {
		let (out, _) = apply_sort_to_segment("apple\nbanana\ncherry\n", true, false, false, false);
		assert_eq!(out, "cherry\nbanana\napple\n");
	}

	#[test]
	fn sort_segment_numeric_orders_numbers_naturally() {
		let (out, _) = apply_sort_to_segment("10\n2\n1\n20\n", false, false, true, false);
		assert_eq!(out, "1\n2\n10\n20\n");
	}

	#[test]
	fn sort_segment_unique_drops_duplicates() {
		let (out, _) = apply_sort_to_segment("b\na\nb\nc\na\n", false, true, false, false);
		assert_eq!(out, "a\nb\nc\n");
	}

	#[test]
	fn sort_segment_ignore_case_folds_case_for_ordering() {
		let (out, _) = apply_sort_to_segment("Banana\napple\nCherry\n", false, false, false, true);
		assert_eq!(out, "apple\nBanana\nCherry\n");
	}

	#[test]
	fn sort_segment_preserves_missing_trailing_newline() {
		let (out, _) = apply_sort_to_segment("b\na", false, false, false, false);
		assert_eq!(out, "a\nb");
	}

	#[test]
	fn uniq_segment_dedupes_adjacent_lines_only() {
		let (out, count) = apply_uniq_to_segment("a\na\nb\nc\nc\na\n", false);
		assert_eq!(out, "a\nb\nc\na\n");
		assert_eq!(count, 4);
	}

	#[test]
	fn uniq_segment_ignore_case_folds_case_for_equality() {
		let (out, _) = apply_uniq_to_segment("Foo\nfoo\nBar\n", true);
		assert_eq!(out, "Foo\nBar\n");
	}

	#[test]
	fn change_around_function_keeps_grafted_tree_and_cursor_consistent() {
		let source = include_str!("../clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);
		let mut root_id = Some(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostic_spans = Vec::new();
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut registers = HashMap::new();
		let _clipboard_guard = CLIPBOARD_TEST_LOCK
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let mut clipboard = ClipboardHandle::new();
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;

		let get_text_pos = source.find("pub fn get_text").expect("get_text exists");
		let q = UastQuery::new(&registry, root);
		let leaf = q
			.node_at_offset(DocByte::new(get_text_pos as u64))
			.expect("get_text leaf");
		let func = q
			.find_ancestor(leaf, is_function_scope)
			.expect("get_text function");
		let (fn_start, fn_end) = q.node_bounds(func);
		let expected = format!(
			"{}{}",
			&source[..fn_start.get() as usize],
			&source[fn_end.get() as usize..]
		);

		let mut caret = resolved_caret(&registry, root, DocByte::new(get_text_pos as u64));
		let mut folds = FoldSet::default();

		let _ = handle_edit_command(
			&registry,
			&mut root_id,
			&mut folds,
			&mut caret,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostic_spans,
			&mut active_visual,
			&mut registers,
			&mut clipboard,
			&mut status_message,
			&mut mode_override,
			EditorCommand::ChangeAroundFunction,
		);

		assert_eq!(status_message, None, "caf failed: {:?}", status_message);
		assert_eq!(mode_override, Some(EditorMode::Insert));
		assert!(active_visual.is_none());

		let current_text =
			String::from_utf8(read_loaded_document(&registry, root).expect("read doc"))
				.expect("utf8 document");
		assert_eq!(current_text, expected);
		assert_eq!(registry.get_total_bytes(root), expected.len() as u64);
		assert_eq!(
			registry.get_total_newlines(root),
			expected.as_bytes().iter().filter(|&&b| b == b'\n').count() as u64
		);

		let cursor_abs_byte = caret.byte();
		assert_eq!(cursor_abs_byte, fn_start);
		assert!(caret.resolve(
			&registry,
			root,
			&mut folds,
			crate::core::TopologyRev::ZERO,
			ledger.current_state_id,
		));
		let (resynced_line, resynced_col) =
			crate::engine::document::cursor::line_col_from_doc_byte_sparse(
				&registry,
				root,
				cursor_abs_byte,
			)
			.expect("cursor should resolve after caf");
		assert_eq!(caret.abs_line(), resynced_line);
		assert_eq!(caret.abs_col(), resynced_col);
		assert_eq!(caret.abs_line(), DocLine::new(13));
		resolve_loaded_node_text(&registry, caret.node_cache().expect("node cache").node)
			.expect("cursor should land on text");

		let viewport_lines = u32::try_from(registry.get_total_newlines(root) + 1)
			.expect("test document line count fits u32");
		let tokens =
			registry.query_viewport(root, &crate::uast::NoFolds, DocLine::ZERO, viewport_lines);
		assert!(
			!tokens.iter().any(|token| !token.is_folded
				&& token.physical_byte_len > 0
				&& token.text.is_empty()),
			"viewport contains dma-style empty tokens: {:?}",
			tokens
				.iter()
				.filter(|token| !token.is_folded
					&& token.physical_byte_len > 0
					&& token.text.is_empty())
				.map(|token| (token.node_id, token.physical_byte_len))
				.collect::<Vec<_>>()
		);

		assert_walk_parents_are_consistent(&registry, root);
	}

	#[test]
	fn test_structural_delete_full_pipeline() {
		let source = include_str!("../clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);
		let mut root_id = Some(root);

		let get_text_pos = source.find("pub fn get_text").expect("get_text exists");
		let mut caret = resolved_caret(&registry, root, DocByte::new(get_text_pos as u64));
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut scroll_y = 0u64;
		let mut scroll_row_offset = crate::core::DisplayRow::ZERO;
		let mut viewport_width = 80u16;
		let mut viewport_lines = 24u32;
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;
		let mut cached_sum_tree = None;
		let mut folds = FoldSet::default();
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostic_spans = Vec::new();
		let mut registers = HashMap::new();
		let _clipboard_guard = CLIPBOARD_TEST_LOCK
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let mut clipboard = ClipboardHandle::new();

		// 1. Jump to get_text (line 14, index 13)
		// Cursor already starts at the target as if a jump had just occurred.

		// 2. Simulate "vaf" (SelectAroundFunction)
		let _ = handle_navigation_command(
			&registry,
			root_id,
			&mut folds,
			&mut caret,
			&mut active_visual,
			&mut scroll_y,
			&mut scroll_row_offset,
			&mut viewport_width,
			&mut viewport_lines,
			false, // wrap
			false, // minimap
			Some(crate::engine::GutterNumbering::Absolute),
			crate::core::TopologyRev::ZERO,
			ledger.current_state_id,
			&mut cached_sum_tree,
			&mut status_message,
			&mut mode_override,
			EditorCommand::SelectAroundFunction,
		);

		// Assert selection is correct
		let (anchor, kind) = active_visual.expect("Selection should be active");
		assert_eq!(kind, VisualKind::Char);

		let cursor_abs_byte = caret.byte();
		let ranges =
			resolve_visual_ranges_sparse(&registry, root, &folds, anchor, cursor_abs_byte, kind)
				.expect("resolve ranges");
		assert_eq!(ranges.len(), 1);
		let (start, end) = ranges[0];
		let selected_text = String::from_utf8(
			registry
				.read_loaded_slice(root, start, end.saturating_add(1))
				.unwrap(),
		)
		.unwrap();
		assert!(selected_text.contains("pub fn get_text"));
		assert!(selected_text.trim().ends_with('}'));

		// 3. Simulate "d" (VisualDelete)
		let _ = handle_edit_command(
			&registry,
			&mut root_id,
			&mut folds,
			&mut caret,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostic_spans,
			&mut active_visual,
			&mut registers,
			&mut clipboard,
			&mut status_message,
			&mut mode_override,
			EditorCommand::VisualDelete { anchor, kind },
		);

		// Assert selection is cleared
		assert!(active_visual.is_none());

		// Assert file content after delete
		let current_text = read_loaded_document(&registry, root).expect("read doc");
		let current_text_str = String::from_utf8_lossy(&current_text);
		assert!(
			!current_text_str.contains("pub fn get_text"),
			"get_text should be deleted"
		);
		assert!(
			current_text_str.contains("pub fn set_text"),
			"set_text should remain"
		);
		let cursors = CursorSnapshot::single(caret.byte());
		ledger.finish_cursor_transaction(StateId::ZERO, cursors.clone(), cursors);

		// 4. Simulate "u" (Undo)
		let crate::engine::undo::HistoryReplay::Text {
			deltas: undo_group,
			cursors: _,
		} = ledger
			.preview(HistoryDirection::Undo)
			.expect("undo should exist")
		else {
			panic!("visual delete undo must replay text deltas");
		};
		apply_deltas_to_document_internal(
			&registry,
			&mut root_id,
			&mut folds,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostic_spans,
			undo_group,
			false,
		)
		.expect("undo should apply");
		ledger.commit(HistoryDirection::Undo);

		// Assert file content restored
		let restored_text = read_loaded_document(&registry, root).expect("read doc");
		assert_eq!(
			restored_text,
			source.as_bytes(),
			"Undo should restore original source"
		);

		// 5. Simulate "diw" on "arboard" (line 1)
		let arboard_pos = source.find("arboard").unwrap();
		caret = resolved_caret(&registry, root, DocByte::new(arboard_pos as u64));

		let _ = handle_edit_command(
			&registry,
			&mut root_id,
			&mut folds,
			&mut caret,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostic_spans,
			&mut active_visual,
			&mut registers,
			&mut clipboard,
			&mut status_message,
			&mut mode_override,
			EditorCommand::DeleteInnerWord,
		);

		assert_eq!(status_message, None, "diw failed: {:?}", status_message);
		let current_text = read_loaded_document(&registry, root).expect("read doc");
		let current_text_str = String::from_utf8_lossy(&current_text);
		assert!(
			!current_text_str.contains("arboard"),
			"arboard should be deleted"
		);

		// 6. Simulate "diw" on "get_text" (line 14)
		let get_text_pos = source.find("get_text").unwrap();
		caret = resolved_caret(&registry, root, DocByte::new(get_text_pos as u64));

		let _ = handle_edit_command(
			&registry,
			&mut root_id,
			&mut folds,
			&mut caret,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostic_spans,
			&mut active_visual,
			&mut registers,
			&mut clipboard,
			&mut status_message,
			&mut mode_override,
			EditorCommand::DeleteInnerWord,
		);

		assert_eq!(
			status_message, None,
			"diw on get_text failed: {:?}",
			status_message
		);
	}

	/// Helper that threads a single `EditorCommand` through
	/// `handle_edit_command` against a `build_document` fixture. The simple
	/// one-leaf structure is enough for point mutations like `r` and `~`,
	/// keeping the tests terse.
	fn run_single_edit_command(
		source: &str,
		cursor_byte: u64,
		command: EditorCommand,
	) -> (String, DocByte, DocLine, DocDisplayCol, Option<String>) {
		let (registry, root) = build_document(source);
		let mut root_id = Some(root);
		let mut caret = resolved_caret(&registry, root, DocByte::new(cursor_byte));
		let mut folds = FoldSet::default();
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostic_spans = Vec::new();
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut registers = HashMap::new();
		let _clipboard_guard = CLIPBOARD_TEST_LOCK
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let mut clipboard = ClipboardHandle::new();
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;

		let _ = handle_edit_command(
			&registry,
			&mut root_id,
			&mut folds,
			&mut caret,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostic_spans,
			&mut active_visual,
			&mut registers,
			&mut clipboard,
			&mut status_message,
			&mut mode_override,
			command,
		);
		caret.resolve(
			&registry,
			root_id.expect("root after edit"),
			&mut folds,
			crate::core::TopologyRev::ZERO,
			ledger.current_state_id,
		);

		let root = root_id.expect("mutated root");
		let text = String::from_utf8(read_loaded_document(&registry, root).expect("read doc"))
			.expect("utf8 document");
		let cursor_resolved_byte = caret.byte();
		(
			text,
			cursor_resolved_byte,
			caret.abs_line(),
			caret.abs_col(),
			status_message,
		)
	}

	fn run_grafted_edit_command(
		source: &str,
		cursor_byte: u64,
		command: EditorCommand,
	) -> GraftedEditResult {
		run_grafted_edit_commands(source, cursor_byte, &[command])
	}

	fn run_grafted_edit_commands(
		source: &str,
		cursor_byte: u64,
		commands: &[EditorCommand],
	) -> GraftedEditResult {
		let (registry, root) = build_grafted_rust_document(source);
		let mut root_id = Some(root);
		let mut caret = resolved_caret(&registry, root, DocByte::new(cursor_byte));
		let mut folds = FoldSet::default();
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostic_spans = Vec::new();
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut registers = HashMap::new();
		let _clipboard_guard = CLIPBOARD_TEST_LOCK
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let mut clipboard = ClipboardHandle::new();
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;
		let mut yank_flash = None;

		let (_commands, _input, tx_internal, _requests) = crate::engine::request::channel();
		for command in commands.iter().cloned() {
			match command {
				EditorCommand::YankLine { .. } => {
					let _ = handle_clipboard_command(
						&registry,
						&tx_internal,
						root_id,
						caret.abs_line(),
						&mut registers,
						&mut clipboard,
						&mut yank_flash,
						&mut status_message,
						command,
					);
				}
				other => {
					let _ = handle_edit_command(
						&registry,
						&mut root_id,
						&mut folds,
						&mut caret,
						&mut ledger,
						&mut semantic_highlights,
						&mut diagnostic_spans,
						&mut active_visual,
						&mut registers,
						&mut clipboard,
						&mut status_message,
						&mut mode_override,
						other,
					);
					caret.resolve(
						&registry,
						root_id.expect("root after edit"),
						&mut folds,
						crate::core::TopologyRev::ZERO,
						ledger.current_state_id,
					);
				}
			}
		}

		let root = root_id.expect("mutated root");
		let text = String::from_utf8(read_loaded_document(&registry, root).expect("read doc"))
			.expect("utf8 document");
		let cursor_resolved_byte = caret.byte();
		(
			text,
			cursor_resolved_byte,
			caret.abs_line(),
			caret.abs_col(),
			status_message,
			mode_override,
			registers,
		)
	}

	fn register_text(registers: &HashMap<char, RegisterValue>, register: char) -> Option<&str> {
		registers.get(&register).map(|value| value.text.as_str())
	}

	fn find_nth_occurrence(source: &str, needle: &str, nth: usize) -> u64 {
		source
			.match_indices(needle)
			.nth(nth)
			.map(|(idx, _)| idx as u64)
			.unwrap_or_else(|| panic!("missing occurrence {} of {:?}", nth, needle))
	}

	#[test]
	fn replace_char_replaces_and_pins_cursor() {
		// `r3` on the 'e' of "hello" must rewrite the char in place and keep
		// the cursor exactly on the replaced byte — Vim leaves the cursor on
		// the new character after `r`, never to the right of it.
		let (text, cursor_byte, line, col, status) =
			run_single_edit_command("hello world\n", 1, EditorCommand::ReplaceChar('3'));
		assert_eq!(status, None, "replace should not report a status");
		assert_eq!(text, "h3llo world\n");
		assert_eq!(cursor_byte, DocByte::new(1));
		assert_eq!(line, DocLine::ZERO);
		assert_eq!(col, DocDisplayCol::new(1));
	}

	#[test]
	fn toggle_case_expands_sharp_s_without_dropping_data() {
		// `ß`.to_uppercase() yields "SS" — two scalars. The first version of
		// `~` only kept the first scalar from the iterator, so this test
		// would round-trip to "straSe\n" (one S, missing a byte) and the
		// tree metrics would desync. Guard against that regression by
		// asserting the full expansion survives the handler.
		let source = "straße\n";
		let sharp_s_byte = source.find('ß').expect("ß present") as u64;
		let (text, _cursor_byte, _, _, status) =
			run_single_edit_command(source, sharp_s_byte, EditorCommand::ToggleCaseAtCursor);
		assert_eq!(status, None, "toggle case should not report a status");
		assert_eq!(text, "straSSe\n");
	}

	#[test]
	fn toggle_case_ascii_letter_flips_and_advances() {
		// Baseline check: on a plain ASCII 'a' the toggle still flips to
		// 'A'. This keeps the regression coverage honest — the Unicode fix
		// must not break the common case.
		let (text, _, _, _, status) =
			run_single_edit_command("abc\n", 0, EditorCommand::ToggleCaseAtCursor);
		assert_eq!(status, None);
		assert_eq!(text, "Abc\n");
	}

	#[test]
	fn delete_word_motion_commands_use_exact_real_clipboard_ranges() {
		let source = include_str!("../clipboard.rs");
		let struct_decl = "pub struct ClipboardHandle {\n";
		let cursor_byte = find_nth_occurrence(source, "ClipboardHandle", 0);

		let cases = [
			(
				"dw",
				EditorCommand::DeleteWordMotion {
					register: '"',
					motion: WordMotion::NextStart,
				},
				source.replacen(struct_decl, "pub struct {\n", 1),
				Some("ClipboardHandle "),
			),
			(
				"db",
				EditorCommand::DeleteWordMotion {
					register: '"',
					motion: WordMotion::PrevStart,
				},
				source.replacen(struct_decl, "pub ClipboardHandle {\n", 1),
				Some("struct "),
			),
			(
				"de",
				EditorCommand::DeleteWordMotion {
					register: '"',
					motion: WordMotion::NextEnd,
				},
				source.replacen(struct_decl, "pub struct  {\n", 1),
				Some("ClipboardHandle"),
			),
		];

		for (label, command, expected_text, expected_register) in cases {
			let (text, _cursor_byte, _line, _col, status, mode_override, registers) =
				run_grafted_edit_command(source, cursor_byte, command);
			assert_eq!(status, None, "{label} should not report an error");
			assert_eq!(mode_override, None, "{label} must stay in Normal mode");
			assert_eq!(text, expected_text, "{label} changed the wrong range");
			assert_eq!(
				register_text(&registers, '"'),
				expected_register,
				"{label} must store the exact deleted text in the unnamed register",
			);
			assert_eq!(
				registers.get(&'"').map(|value| value.kind),
				Some(RegisterKind::CharWise),
				"{label} must store a charwise register",
			);
		}
	}

	#[test]
	fn yank_word_motion_commands_capture_exact_real_clipboard_ranges_without_mutation() {
		let source = include_str!("../clipboard.rs");
		let cursor_byte = find_nth_occurrence(source, "ClipboardHandle", 0);

		let cases = [
			(
				"yw",
				EditorCommand::YankWordMotion {
					register: '"',
					motion: WordMotion::NextStart,
				},
				Some("ClipboardHandle "),
			),
			(
				"yb",
				EditorCommand::YankWordMotion {
					register: '"',
					motion: WordMotion::PrevStart,
				},
				Some("struct "),
			),
			(
				"ye",
				EditorCommand::YankWordMotion {
					register: '"',
					motion: WordMotion::NextEnd,
				},
				Some("ClipboardHandle"),
			),
		];

		for (label, command, expected_register) in cases {
			let (text, _cursor_byte, _line, _col, status, mode_override, registers) =
				run_grafted_edit_command(source, cursor_byte, command);
			assert_eq!(status, None, "{label} should not report an error");
			assert_eq!(mode_override, None, "{label} must not change mode");
			assert_eq!(text, source, "{label} must not mutate the buffer");
			assert_eq!(
				register_text(&registers, '"'),
				expected_register,
				"{label} must yank the exact motion range",
			);
			assert_eq!(
				registers.get(&'"').map(|value| value.kind),
				Some(RegisterKind::CharWise),
				"{label} must use a charwise register",
			);
		}
	}

	#[test]
	fn change_word_motion_commands_delete_exact_ranges_and_enter_insert_mode_on_real_clipboard_source()
	 {
		let source = include_str!("../clipboard.rs");
		let struct_decl = "pub struct ClipboardHandle {\n";
		let cursor_byte = find_nth_occurrence(source, "ClipboardHandle", 0);

		let cases = [
			(
				"cw",
				EditorCommand::ChangeWordMotion {
					register: '"',
					motion: WordMotion::NextStart,
				},
				source.replacen(struct_decl, "pub struct {\n", 1),
				Some("ClipboardHandle "),
			),
			(
				"cb",
				EditorCommand::ChangeWordMotion {
					register: '"',
					motion: WordMotion::PrevStart,
				},
				source.replacen(struct_decl, "pub ClipboardHandle {\n", 1),
				Some("struct "),
			),
			(
				"ce",
				EditorCommand::ChangeWordMotion {
					register: '"',
					motion: WordMotion::NextEnd,
				},
				source.replacen(struct_decl, "pub struct  {\n", 1),
				Some("ClipboardHandle"),
			),
		];

		for (label, command, expected_text, expected_register) in cases {
			let (text, _cursor_byte, _line, _col, status, mode_override, registers) =
				run_grafted_edit_command(source, cursor_byte, command);
			assert_eq!(status, None, "{label} should not report an error");
			assert_eq!(
				mode_override,
				Some(EditorMode::Insert),
				"{label} must hand control to Insert mode",
			);
			assert_eq!(text, expected_text, "{label} changed the wrong range");
			assert_eq!(
				register_text(&registers, '"'),
				expected_register,
				"{label} must capture the changed text in the unnamed register",
			);
			assert_eq!(
				registers.get(&'"').map(|value| value.kind),
				Some(RegisterKind::CharWise),
				"{label} must use a charwise register",
			);
		}
	}

	#[test]
	fn join_line_collapses_newline_and_indentation_on_real_clipboard_source() {
		let source = include_str!("../clipboard.rs");
		let join_input = concat!(
			"pub struct ClipboardHandle {\n",
			"\tinner: Option<Clipboard>,\n",
		);
		let joined = "pub struct ClipboardHandle { inner: Option<Clipboard>,\n";
		let expected = source.replacen(join_input, joined, 1);
		let cursor_byte = find_nth_occurrence(source, "pub struct ClipboardHandle {", 0);

		let (text, _cursor_byte, _line, _col, status, mode_override, registers) =
			run_grafted_edit_command(source, cursor_byte, EditorCommand::JoinLine);

		assert_eq!(status, None, "J should not report an error");
		assert_eq!(mode_override, None, "J must keep Normal mode");
		assert!(registers.is_empty(), "J must not touch registers");
		assert_eq!(
			text, expected,
			"J must join with one space and strip indent"
		);
		assert_eq!(
			text.lines().count() + 1,
			source.lines().count(),
			"J must remove exactly one newline from the buffer",
		);
	}

	#[test]
	fn linewise_put_after_yank_line_stays_linewise_on_real_clipboard_source() {
		let source = include_str!("../clipboard.rs");
		let cursor_byte = find_nth_occurrence(source, "use arboard::Clipboard;", 0);
		let expected = source.replacen(
			"use arboard::Clipboard;\n\n",
			"use arboard::Clipboard;\nuse arboard::Clipboard;\n\n",
			1,
		);

		let (text, _cursor_byte, cursor_line, cursor_col, status, mode_override, registers) =
			run_grafted_edit_commands(
				source,
				cursor_byte,
				&[
					EditorCommand::YankLine { register: '"' },
					EditorCommand::Put { register: '"' },
				],
			);

		assert_eq!(status.as_deref(), Some("24 bytes yanked"));
		assert_eq!(mode_override, None);
		assert_eq!(
			text, expected,
			"yy followed by p must duplicate the line below"
		);
		assert_eq!(
			cursor_line,
			DocLine::new(1),
			"linewise p should land on inserted line"
		);
		assert_eq!(cursor_col, DocDisplayCol::ZERO);
		assert_eq!(
			register_text(&registers, '"'),
			Some("use arboard::Clipboard;\n")
		);
		assert_eq!(
			registers.get(&'"').map(|value| value.kind),
			Some(RegisterKind::LineWise),
			"yy must mark the unnamed register as linewise",
		);
	}

	#[test]
	fn charwise_put_after_delete_word_end_pastes_inline_on_real_clipboard_source() {
		let source = include_str!("../clipboard.rs");
		let cursor_byte = find_nth_occurrence(source, "ClipboardHandle", 0);
		let expected = source.replacen(
			"pub struct ClipboardHandle {\n",
			"pub struct  ClipboardHandle{\n",
			1,
		);

		let (text, _cursor_byte, _line, _col, status, mode_override, registers) =
			run_grafted_edit_commands(
				source,
				cursor_byte,
				&[
					EditorCommand::DeleteWordMotion {
						register: '"',
						motion: WordMotion::NextEnd,
					},
					EditorCommand::Put { register: '"' },
				],
			);

		assert_eq!(status, None);
		assert_eq!(mode_override, None);
		assert_eq!(
			text, expected,
			"de followed by p must paste inline after the post-delete cursor, not as a new line",
		);
		assert_eq!(register_text(&registers, '"'), Some("ClipboardHandle"));
		assert_eq!(
			registers.get(&'"').map(|value| value.kind),
			Some(RegisterKind::CharWise),
			"de must keep the unnamed register charwise so p restores inline text",
		);
	}

	#[test]
	fn backspace_at_use_line_end_reads_previous_grafted_leaf() {
		let source = include_str!("../../../../baryon/src/app.rs");
		let line = "use std::sync::Once;";
		let byte = source.find(line).expect("Once import should exist") + line.len();
		let expected = source.replacen(line, "use std::sync::Once", 1);

		let (text, cursor_byte, cursor_line, cursor_col, status, _, _) =
			run_grafted_edit_command(source, byte as u64, EditorCommand::Backspace);

		assert_eq!(status, None);
		assert_eq!(text, expected);
		assert_eq!(cursor_byte, DocByte::new((byte - 1) as u64));
		assert_eq!(cursor_line, DocLine::new(11));
		assert_eq!(cursor_col, DocDisplayCol::new((line.len() - 1) as u32));
	}

	/// Exercises OpenLineBelow against a structurally-grafted Rust
	/// document.  In a grafted tree, `line_end_byte_sparse` walks sibling
	/// nodes that may be branch nodes (RelationalTable / RelationalRow)
	/// with no virtual_data.  `resolve_loaded_node_text` returns
	/// "File still loading, cannot resolve structural token" on these,
	/// causing OpenLineBelow to emit an error instead of inserting a line.
	#[test]
	fn open_line_below_grafted_rust_document_inserts_newline() {
		let source = include_str!("../clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);
		let mut root_id = Some(root);

		// Position cursor at "inner:" on line 3 (0-indexed), mid-line.
		let inner_pos = source
			.find("inner:")
			.expect("inner: exists in clipboard.rs");
		let mut caret = resolved_caret(&registry, root, DocByte::new(inner_pos as u64));
		let mut folds = FoldSet::default();

		assert_eq!(
			caret.abs_line(),
			DocLine::new(3),
			"inner: should be on line 3"
		);

		let original_total_newlines = registry.get_total_newlines(root);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights = Vec::new();
		let mut diagnostic_spans = Vec::new();
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut registers = HashMap::new();
		let _clipboard_guard = CLIPBOARD_TEST_LOCK
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let mut clipboard = ClipboardHandle::new();
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;

		let _ = handle_edit_command(
			&registry,
			&mut root_id,
			&mut folds,
			&mut caret,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostic_spans,
			&mut active_visual,
			&mut registers,
			&mut clipboard,
			&mut status_message,
			&mut mode_override,
			EditorCommand::OpenLineBelow,
		);
		assert!(caret.resolve(
			&registry,
			root_id.expect("root after open-line"),
			&mut folds,
			crate::core::TopologyRev::ZERO,
			ledger.current_state_id,
		));

		assert_eq!(
			status_message, None,
			"OpenLineBelow must not produce an error on a grafted document; got: {:?}",
			status_message,
		);
		assert_eq!(
			mode_override,
			Some(EditorMode::Insert),
			"OpenLineBelow must set Insert mode"
		);
		// Cursor should be on the new blank line (line 4, was line 3).
		assert_eq!(
			caret.abs_line(),
			DocLine::new(4),
			"cursor must be on the new line below"
		);
		// Document should have one more newline than before.
		let new_total_newlines = registry.get_total_newlines(root_id.unwrap());
		assert_eq!(
			new_total_newlines,
			original_total_newlines + 1,
			"document must gain exactly one newline"
		);
	}
}
