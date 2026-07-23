use super::super::core::{
	advance_col, build_regex, find_all_matches, resolve_byte_range, substitute_text_delta,
};
use super::super::document::io::read_document_snapshot;
use super::super::document::parse::word_object_delta_at_cursor;
use super::super::folding::FoldSet;
use super::super::protocol::{CommandRange, ConfirmAction, EditorCommand, EditorMode, SearchMatch};
use super::super::session::{
	Affinity, Caret, ConfirmState, CursorSnapshot, TextEditorMut, UiState,
};
use super::super::shadow::{
	LARGE_FILE_GENERATION_THRESHOLD_BYTES, checkpoint_substitute_generations,
	create_shadow_session, discard_shadow_session,
};
use super::super::undo::{
	SwapGeneration, TextDelta, UndoLedger, byte_offset_from_line_col, line_col_from_byte_offset,
};
use super::edit::{apply_document_deltas, apply_document_deltas_with_cursor_override};
use super::system::load_root_from_path;
use super::{CommandEffect, RuntimeContext};
use crate::core::{DocByte, DocDisplayCol, DocLine, StateId, TopologyRev};
use crate::ecs::{NodeId, UastRegistry};
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::HighlightSpan;
use std::path::Path;

fn jump_to_search_match(
	registry: &UastRegistry,
	root: NodeId,
	folds: &mut FoldSet,
	caret: &mut Caret,
	topology_rev: TopologyRev,
	state_id: StateId,
	search_match: SearchMatch,
) {
	caret.place_at_line_col(
		registry,
		root,
		folds,
		topology_rev,
		state_id,
		search_match.line,
		search_match.col,
		true,
		Affinity::After,
	);
}

fn format_search_match_info(idx: usize, total: usize, wrapped: bool) -> String {
	let info = format!("[{}/{}]", idx + 1, total);
	if wrapped {
		format!("{} (wrapped)", info)
	} else {
		info
	}
}

fn format_confirm_prompt(position: u32, total: u32) -> String {
	format!("Replace? [y/n/a/q] ({}/{})", position, total)
}

fn clear_search_state(
	search_pattern: &mut Option<String>,
	search_case_insensitive: &mut bool,
	search_matches: &mut Vec<SearchMatch>,
	search_match_index: &mut Option<usize>,
	search_match_info: &mut Option<String>,
) {
	*search_pattern = None;
	*search_case_insensitive = false;
	search_matches.clear();
	*search_match_index = None;
	*search_match_info = None;
}

fn read_search_document(
	registry: &UastRegistry,
	root: NodeId,
	source_path: Option<&str>,
) -> Result<Vec<u8>, String> {
	read_document_snapshot(registry, root, source_path.map(Path::new))
}

fn should_use_generation_substitute(
	file_len: u64,
	has_shadow: bool,
	visible_path: Option<&str>,
) -> bool {
	visible_path.is_some() && (has_shadow || file_len >= LARGE_FILE_GENERATION_THRESHOLD_BYTES)
}

fn line_col_for_prefix_and_replacement(
	prefix: &[u8],
	replacement: &str,
) -> (DocLine, DocDisplayCol) {
	let mut line = DocLine::ZERO;
	let mut col = DocDisplayCol::ZERO;
	for &byte in prefix {
		advance_col(byte, &mut line, &mut col);
	}
	for &byte in replacement.as_bytes() {
		advance_col(byte, &mut line, &mut col);
	}
	(line, col)
}

fn handle_generation_substitute_all(
	ctx: &RuntimeContext<'_>,
	tab: &mut TextEditorMut<'_>,
	cursors_before: CursorSnapshot,
	status_message: &mut Option<String>,
	pattern: String,
	replacement: String,
	range: CommandRange,
	flags: super::super::protocol::SubstituteFlags,
	next_dynamic_device_id: &mut u16,
) -> Option<CommandEffect> {
	let rid = tab.document.root_id;
	let visible_path = tab.document.file_path.clone()?;
	let backing_path = tab.document.backing_path.clone()?;
	let file_len = ctx.registry.get_total_bytes(rid);
	if !should_use_generation_substitute(
		file_len,
		tab.document.shadow.is_some(),
		tab.document.file_path.as_deref(),
	) {
		return None;
	}

	let re = match build_regex(&pattern, flags.case_insensitive) {
		Ok(re) => re,
		Err(msg) => {
			*status_message = Some(msg);
			return Some(CommandEffect::render());
		}
	};
	let bytes = match read_search_document(ctx.registry, rid, Some(&backing_path)) {
		Ok(bytes) => bytes,
		Err(msg) => {
			*status_message = Some(msg);
			return Some(CommandEffect::render());
		}
	};
	let byte_range = resolve_byte_range(&range, &bytes, tab.cursor.abs_line());
	let had_shadow = tab.document.shadow.is_some();
	let mut shadow = match tab.document.shadow.take() {
		Some(shadow) => shadow,
		None => match create_shadow_session(Path::new(&visible_path)) {
			Ok(shadow) => shadow,
			Err(msg) => {
				*status_message = Some(msg);
				return Some(CommandEffect::render());
			}
		},
	};
	let generation_result =
		match checkpoint_substitute_generations(&mut shadow, &bytes, &re, &replacement, byte_range)
		{
			Ok(result) => result,
			Err(msg) => {
				if had_shadow || !shadow.generations.is_empty() {
					tab.document.shadow = Some(shadow);
				}
				*status_message = Some(msg);
				return Some(CommandEffect::render());
			}
		};
	let Some((before, after, count, cursor_after_byte)) = generation_result else {
		if had_shadow {
			tab.document.shadow = Some(shadow);
		} else {
			let _ = discard_shadow_session(&shadow);
		}
		*status_message = Some("Pattern not found".to_string());
		return Some(CommandEffect::render());
	};
	let (new_rid, _cursor_leaf, new_backing_device_id) =
		match load_root_from_path(ctx, &after.path, next_dynamic_device_id) {
			Ok(loaded) => loaded,
			Err(msg) => {
				tab.document.shadow = Some(shadow);
				*status_message = Some(msg);
				return Some(CommandEffect::render());
			}
		};
	let first_match_start = cursor_after_byte
		.get()
		.saturating_sub(replacement.len() as u64) as usize;
	let (cursor_line, cursor_col) =
		line_col_for_prefix_and_replacement(&bytes[..first_match_start], &replacement);

	ctx.registry.cancel_metric_scan(tab.document.root_id);
	tab.document.root_id = new_rid;
	tab.view.folds.clear();
	tab.document.backing_path = Some(after.path.clone());
	let retired_device_id =
		std::mem::replace(&mut tab.document.backing_device_id, new_backing_device_id);
	if let Some(device_id) = retired_device_id {
		ctx.resolver.unregister_device(device_id);
	}
	tab.document.shadow = Some(shadow);
	tab.reset_pipeline();
	tab.document.search_matches.clear();
	*tab.match_index = None;
	*tab.match_info = None;
	tab.cursor
		.primary_mut()
		.set_byte(cursor_after_byte, super::super::session::Affinity::After);
	tab.cursor.primary_mut().resolve(
		ctx.registry,
		new_rid,
		&mut tab.view.folds,
		tab.document.topology_rev,
		tab.document.ledger.current_state_id,
	);
	if let Some(line) = tab.cursor.primary().line_cache() {
		debug_assert_eq!((line.line, line.col), (cursor_line, cursor_col));
	}
	tab.cursor.sync_preferred_col();
	let cursors_after = tab.cursor.snapshot();
	tab.document.ledger.push_swap_generation(SwapGeneration {
		before,
		after,
		cursors_before,
		cursors_after,
		state_before: StateId::ZERO,
		state_after: StateId::ZERO,
	});
	*status_message = Some(format!("{} substitution(s)", count));
	Some(CommandEffect::render())
}

fn filter_matches_for_range(
	all_matches: Vec<SearchMatch>,
	range: &CommandRange,
	current_line: DocLine,
	doc: &[u8],
) -> Vec<SearchMatch> {
	match range {
		CommandRange::WholeFile => all_matches,
		CommandRange::CurrentLine => all_matches
			.into_iter()
			.filter(|m| m.line == current_line)
			.collect(),
		CommandRange::SingleLine(n) => {
			let n = *n;
			all_matches
				.into_iter()
				.filter(move |m| m.line == n)
				.collect()
		}
		CommandRange::LineRange(a, b) => {
			let (a, b) = (*a, *b);
			all_matches
				.into_iter()
				.filter(move |m| m.line >= a && m.line <= b)
				.collect()
		}
		CommandRange::Visual {
			start_byte,
			end_byte,
		} => {
			let a = start_byte.get() as usize;
			let b = end_byte.get() as usize;
			let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
			let lo = lo.min(doc.len());
			let hi = hi.min(doc.len());
			let line_lo = DocLine::new(doc[..lo].iter().filter(|&&x| x == b'\n').count() as u64);
			let line_hi = DocLine::new(doc[..hi].iter().filter(|&&x| x == b'\n').count() as u64);
			all_matches
				.into_iter()
				.filter(move |m| m.line >= line_lo && m.line <= line_hi)
				.collect()
		}
	}
}

pub(super) fn handle_search_doctab_command(
	ctx: &RuntimeContext<'_>,
	tab: &mut TextEditorMut<'_>,
	search_pattern: &mut Option<String>,
	search_case_insensitive: &mut bool,
	confirm_state: &mut Option<ConfirmState>,
	ui: &mut UiState,
	next_dynamic_device_id: &mut u16,
	command: EditorCommand,
) -> CommandEffect {
	let cursors_before = tab.cursor.snapshot();
	if matches!(
		&command,
		EditorCommand::SubstituteAll { .. }
			| EditorCommand::SubstituteConfirm { .. }
			| EditorCommand::ConfirmResponse(_)
	) {
		tab.document.bump_topology_rev();
	}
	if !matches!(command, EditorCommand::ClearSearch) {
		tab.cursor.collapse();
	}
	if let EditorCommand::SubstituteAll {
		pattern,
		replacement,
		range,
		flags,
	} = command.clone()
		&& let Some(effect) = handle_generation_substitute_all(
			ctx,
			tab,
			cursors_before.clone(),
			&mut ui.status_message,
			pattern,
			replacement,
			range,
			flags,
			next_dynamic_device_id,
		) {
		tab.cursor.sync_preferred_col();
		return effect;
	}

	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	let state_id = tab.document.ledger.current_state_id;
	if !tab.cursor.primary_mut().resolve(
		ctx.registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id,
	) {
		ui.status_message = Some("Loading cursor extent".to_string());
		return CommandEffect::render();
	}
	let effect = {
		let mut root_id = Some(tab.document.root_id);
		handle_search_command(
			ctx.registry,
			&mut root_id,
			&mut tab.view.folds,
			tab.cursor.primary_mut(),
			topology_rev,
			&mut tab.document.ledger,
			&mut tab.document.semantic.highlights,
			&mut tab.document.semantic.diagnostics,
			tab.document.backing_path.as_deref(),
			search_pattern,
			search_case_insensitive,
			&mut tab.document.search_matches,
			tab.match_index,
			tab.match_info,
			confirm_state,
			&mut ui.status_message,
			&mut ui.mode_override,
			command,
		)
	};
	tab.cursor.primary_mut().resolve(
		ctx.registry,
		tab.document.root_id,
		&mut tab.view.folds,
		topology_rev,
		tab.document.ledger.current_state_id,
	);
	tab.cursor.sync_preferred_col();
	tab.document
		.ledger
		.finish_cursor_transaction(state_id, cursors_before, tab.cursor.snapshot());
	effect
}

pub(super) fn handle_search_command(
	registry: &UastRegistry,
	root_id: &mut Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	topology_rev: TopologyRev,
	ledger: &mut UndoLedger,
	semantic_highlights: &mut Vec<HighlightSpan>,
	diagnostic_spans: &mut Vec<DiagnosticSpan>,
	source_path: Option<&str>,
	search_pattern: &mut Option<String>,
	search_case_insensitive: &mut bool,
	search_matches: &mut Vec<SearchMatch>,
	search_match_index: &mut Option<usize>,
	search_match_info: &mut Option<String>,
	confirm_state: &mut Option<ConfirmState>,
	status_message: &mut Option<String>,
	mode_override: &mut Option<EditorMode>,
	command: EditorCommand,
) -> CommandEffect {
	match command {
		EditorCommand::SearchStart(pattern) => {
			if let Some(rid) = *root_id {
				match build_regex(&pattern, false) {
					Ok(re) => match read_search_document(registry, rid, source_path) {
						Ok(bytes) => {
							let matches = find_all_matches(&bytes, &re);
							if matches.is_empty() {
								*status_message = Some("Pattern not found".to_string());
								*search_pattern = Some(pattern);
								*search_case_insensitive = false;
								search_matches.clear();
								*search_match_index = None;
								*search_match_info = None;
							} else {
								let idx = matches
									.iter()
									.position(|m| {
										m.line > caret.abs_line()
											|| (m.line == caret.abs_line()
												&& m.col >= caret.abs_col())
									})
									.unwrap_or(0);
								*search_match_index = Some(idx);
								let current_match = matches[idx];
								jump_to_search_match(
									registry,
									rid,
									folds,
									caret,
									topology_rev,
									ledger.current_state_id,
									current_match,
								);
								*search_pattern = Some(pattern);
								*search_case_insensitive = false;
								*search_match_info =
									Some(format_search_match_info(idx, matches.len(), false));
								*search_matches = matches;
							}
						}
						Err(msg) => *status_message = Some(msg.to_string()),
					},
					Err(msg) => *status_message = Some(msg),
				}
			}
		}
		EditorCommand::ClearSearch => {
			clear_search_state(
				search_pattern,
				search_case_insensitive,
				search_matches,
				search_match_index,
				search_match_info,
			);
		}
		EditorCommand::SearchNext => {
			if let Some(rid) = *root_id {
				if search_matches.is_empty() {
					*status_message = Some("No previous search".to_string());
				} else {
					let idx = match *search_match_index {
						Some(i) if i + 1 < search_matches.len() => i + 1,
						_ => 0,
					};
					let wrapped = search_match_index.is_some_and(|i| i + 1 >= search_matches.len());
					*search_match_index = Some(idx);
					let current_match = search_matches[idx];
					jump_to_search_match(
						registry,
						rid,
						folds,
						caret,
						topology_rev,
						ledger.current_state_id,
						current_match,
					);
					*search_match_info =
						Some(format_search_match_info(idx, search_matches.len(), wrapped));
				}
			}
		}
		EditorCommand::SearchPrev => {
			if let Some(rid) = *root_id {
				if search_matches.is_empty() {
					*status_message = Some("No previous search".to_string());
				} else {
					let idx = match *search_match_index {
						Some(0) | None => search_matches.len() - 1,
						Some(i) => i - 1,
					};
					let wrapped = search_match_index.is_some_and(|i| i == 0);
					*search_match_index = Some(idx);
					let current_match = search_matches[idx];
					jump_to_search_match(
						registry,
						rid,
						folds,
						caret,
						topology_rev,
						ledger.current_state_id,
						current_match,
					);
					*search_match_info =
						Some(format_search_match_info(idx, search_matches.len(), wrapped));
				}
			}
		}
		EditorCommand::SubstituteAll {
			pattern,
			replacement,
			range,
			flags,
		} => {
			if let Some(rid) = *root_id {
				match build_regex(&pattern, flags.case_insensitive) {
					Ok(re) => match read_search_document(registry, rid, source_path) {
						Ok(bytes) => {
							let byte_range = resolve_byte_range(&range, &bytes, caret.abs_line());
							let Some((delta, count)) =
								substitute_text_delta(&bytes, &re, &replacement, byte_range)
							else {
								*status_message = Some("Pattern not found".to_string());
								return CommandEffect::render();
							};
							let cursor_byte = delta
								.global_byte_offset
								.saturating_add(replacement.len() as u64);
							match apply_document_deltas_with_cursor_override(
								registry,
								root_id,
								folds,
								caret,
								ledger,
								semantic_highlights,
								diagnostic_spans,
								vec![delta],
								cursor_byte,
							) {
								Ok(()) => {
									search_matches.clear();
									*search_match_index = None;
									*search_match_info = None;
									*status_message = Some(format!("{} substitution(s)", count));
								}
								Err(msg) => *status_message = Some(msg),
							}
						}
						Err(msg) => *status_message = Some(msg.to_string()),
					},
					Err(msg) => *status_message = Some(msg),
				}
			}
		}
		EditorCommand::SubstituteConfirm {
			pattern,
			replacement,
			range,
			flags,
		} => {
			if let Some(rid) = *root_id {
				match build_regex(&pattern, flags.case_insensitive) {
					Ok(re) => match read_search_document(registry, rid, source_path) {
						Ok(bytes) => {
							let matches = filter_matches_for_range(
								find_all_matches(&bytes, &re),
								&range,
								caret.abs_line(),
								&bytes,
							);
							if matches.is_empty() {
								*status_message = Some("Pattern not found".to_string());
							} else {
								let total = matches.len() as u32;
								*search_pattern = Some(pattern);
								*search_case_insensitive = flags.case_insensitive;
								*search_matches = matches;
								*search_match_index = Some(0);
								*confirm_state = Some(ConfirmState {
									replacement,
									replacements_done: 0,
									total_matches: total,
									flags,
									range,
								});
								let current_match = search_matches[0];
								jump_to_search_match(
									registry,
									rid,
									folds,
									caret,
									topology_rev,
									ledger.current_state_id,
									current_match,
								);
								*mode_override = Some(EditorMode::Confirm);
								*status_message = Some(format_confirm_prompt(1, total));
							}
						}
						Err(msg) => *status_message = Some(msg.to_string()),
					},
					Err(msg) => *status_message = Some(msg),
				}
			}
		}
		EditorCommand::ConfirmResponse(action) => {
			if let (Some(rid), Some(cs)) = (*root_id, confirm_state.as_mut()) {
				let pat = search_pattern.clone().unwrap_or_default();
				match action {
					ConfirmAction::Yes => {
						if let Ok(bytes) = read_search_document(registry, rid, source_path) {
							let idx = search_match_index.unwrap_or(0);
							let current_match = search_matches[idx];
							let byte_off = byte_offset_from_line_col(
								&bytes,
								current_match.line,
								current_match.col,
							)
							.get() as usize;
							let rep = cs.replacement.as_bytes();
							let delta = TextDelta {
								global_byte_offset: DocByte::new(byte_off as u64),
								deleted_bytes: bytes[byte_off..byte_off + current_match.byte_len]
									.to_vec(),
								inserted_bytes: cs.replacement.as_bytes().to_vec(),
								state_before: StateId::ZERO,
								state_after: StateId::ZERO,
							};
							cs.replacements_done += 1;
							let applied_ok = match apply_document_deltas(
								registry,
								root_id,
								folds,
								caret,
								ledger,
								semantic_highlights,
								diagnostic_spans,
								vec![delta],
							) {
								Ok(()) => true,
								Err(msg) => {
									*status_message = Some(msg);
									false
								}
							};

							if applied_ok && let Some(new_rid) = *root_id {
								let Ok(new_bytes) =
									read_search_document(registry, new_rid, source_path)
								else {
									*status_message = Some(
										"Failed to reload document after substitution".to_string(),
									);
									return CommandEffect::render();
								};
								let re = build_regex(&pat, cs.flags.case_insensitive).unwrap();
								let new_matches = filter_matches_for_range(
									find_all_matches(&new_bytes, &re),
									&cs.range,
									current_match.line,
									&new_bytes,
								);
								let (rep_end_line, rep_end_col) = {
									let mut line = current_match.line;
									let mut col = current_match.col;
									for &b in rep {
										advance_col(b, &mut line, &mut col);
									}
									(line, col)
								};
								let next_idx = new_matches.iter().position(|m| {
									m.line > rep_end_line
										|| (m.line == rep_end_line && m.col >= rep_end_col)
								});

								*search_matches = new_matches;

								if let Some(next_idx) = next_idx {
									*search_match_index = Some(next_idx);
									let current_match = search_matches[next_idx];
									jump_to_search_match(
										registry,
										new_rid,
										folds,
										caret,
										topology_rev,
										ledger.current_state_id,
										current_match,
									);
									*status_message = Some(format_confirm_prompt(
										cs.replacements_done + 1,
										cs.total_matches,
									));
								} else {
									*status_message =
										Some(format!("{} substitution(s)", cs.replacements_done));
									*mode_override = Some(EditorMode::Normal);
									*confirm_state = None;
									caret.place_at_line_col(
										registry,
										new_rid,
										folds,
										topology_rev,
										ledger.current_state_id,
										caret.abs_line(),
										caret.abs_col(),
										false,
										Affinity::After,
									);
								}
							}
						}
					}
					ConfirmAction::No => {
						let idx = search_match_index.unwrap_or(0);
						if idx + 1 < search_matches.len() {
							*search_match_index = Some(idx + 1);
							let current_match = search_matches[idx + 1];
							jump_to_search_match(
								registry,
								rid,
								folds,
								caret,
								topology_rev,
								ledger.current_state_id,
								current_match,
							);
							*status_message =
								Some(format_confirm_prompt(idx as u32 + 2, cs.total_matches));
						} else {
							*status_message =
								Some(format!("{} substitution(s)", cs.replacements_done));
							*mode_override = Some(EditorMode::Normal);
							*confirm_state = None;
						}
					}
					ConfirmAction::All => {
						if let Ok(bytes) = read_search_document(registry, rid, source_path) {
							let idx = search_match_index.unwrap_or(0);
							let current_match = search_matches[idx];
							let byte_off = byte_offset_from_line_col(
								&bytes,
								current_match.line,
								current_match.col,
							)
							.get() as usize;
							let re = build_regex(&pat, cs.flags.case_insensitive).unwrap();
							let (_, range_end) =
								resolve_byte_range(&cs.range, &bytes, current_match.line)
									.unwrap_or((0, bytes.len()));
							let byte_range = Some((byte_off.min(range_end), range_end));
							let Some((delta, count)) =
								substitute_text_delta(&bytes, &re, &cs.replacement, byte_range)
							else {
								*status_message =
									Some(format!("{} substitution(s)", cs.replacements_done));
								*mode_override = Some(EditorMode::Normal);
								search_matches.clear();
								*search_match_index = None;
								*search_match_info = None;
								*confirm_state = None;
								return CommandEffect::render();
							};
							cs.replacements_done += count as u32;
							let cursor_byte = delta
								.global_byte_offset
								.saturating_add(cs.replacement.len() as u64);
							match apply_document_deltas_with_cursor_override(
								registry,
								root_id,
								folds,
								caret,
								ledger,
								semantic_highlights,
								diagnostic_spans,
								vec![delta],
								cursor_byte,
							) {
								Ok(()) => {
									*status_message =
										Some(format!("{} substitution(s)", cs.replacements_done));
								}
								Err(msg) => *status_message = Some(msg),
							}
						}
						*mode_override = Some(EditorMode::Normal);
						search_matches.clear();
						*search_match_index = None;
						*search_match_info = None;
						*confirm_state = None;
					}
					ConfirmAction::Quit => {
						*status_message = Some(format!("{} substitution(s)", cs.replacements_done));
						*mode_override = Some(EditorMode::Normal);
						*confirm_state = None;
						search_matches.clear();
						*search_match_index = None;
						*search_match_info = None;
					}
				}
			}
		}
		EditorCommand::SearchWordUnderCursor { forward } => {
			if let Some(rid) = *root_id {
				let cursor_byte = caret.byte();
				match word_object_delta_at_cursor(registry, rid, cursor_byte) {
					Ok(Some(delta)) => {
						let Ok(word) = std::str::from_utf8(&delta.deleted_bytes) else {
							*status_message =
								Some("Word under cursor is not valid UTF-8".to_string());
							return CommandEffect::render();
						};
						let escaped: String = word
							.chars()
							.flat_map(|c| {
								if c.is_alphanumeric() || c == '_' {
									vec![c]
								} else {
									vec!['\\', c]
								}
							})
							.collect();
						let pattern = format!(r"\b{}\b", escaped);
						match build_regex(&pattern, false) {
							Ok(re) => match read_search_document(registry, rid, source_path) {
								Ok(bytes) => {
									let (word_line, word_col) =
										line_col_from_byte_offset(&bytes, delta.global_byte_offset);
									let matches = find_all_matches(&bytes, &re);
									if matches.is_empty() {
										*status_message = Some("Pattern not found".to_string());
									} else {
										let idx = if forward {
											matches
												.iter()
												.position(|m| {
													m.line > word_line
														|| (m.line == word_line && m.col > word_col)
												})
												.unwrap_or(0)
										} else {
											matches
												.iter()
												.rposition(|m| {
													m.line < word_line
														|| (m.line == word_line && m.col < word_col)
												})
												.unwrap_or(matches.len() - 1)
										};
										let wrapped = if forward {
											!matches.iter().any(|m| {
												m.line > word_line
													|| (m.line == word_line && m.col > word_col)
											})
										} else {
											!matches.iter().any(|m| {
												m.line < word_line
													|| (m.line == word_line && m.col < word_col)
											})
										};
										*search_match_index = Some(idx);
										let current_match = matches[idx];
										jump_to_search_match(
											registry,
											rid,
											folds,
											caret,
											topology_rev,
											ledger.current_state_id,
											current_match,
										);
										*search_pattern = Some(pattern);
										*search_case_insensitive = false;
										*search_match_info = Some(format_search_match_info(
											idx,
											matches.len(),
											wrapped,
										));
										*search_matches = matches;
									}
								}
								Err(msg) => *status_message = Some(msg.to_string()),
							},
							Err(msg) => *status_message = Some(msg),
						}
					}
					Ok(None) => {
						*status_message = Some("No word under cursor".to_string());
					}
					Err(msg) => {
						*status_message = Some(msg);
					}
				}
			}
		}
		_ => unreachable!(),
	}

	CommandEffect::render()
}

#[cfg(test)]
mod tests {
	use super::{handle_search_command, handle_search_doctab_command};
	use crate::core::{DocByte, DocDisplayCol, DocLine, StateId, TopologyRev};
	use crate::engine::core::FILE_DEVICE_ID;
	use crate::engine::folding::FoldSet;
	use crate::engine::language::LanguageRegistry;
	use crate::engine::protocol::{CommandRange, EditorCommand, SubstituteFlags};
	use crate::engine::runtime::edit::handle_history_doctab_command;
	use crate::engine::runtime::system::{handle_file_doctab_command, load_root_from_path};
	use crate::engine::session::{
		Caret, CursorSnapshot, SettingsPersistent, TestTextEditor, UiState,
	};
	use crate::engine::settings::SaveDurability;
	use crate::engine::shadow::shadow_dir_for_path;
	use crate::engine::test_support::{build_exact_metric_physical_document, temp_test_path};
	use crate::engine::undo::UndoLedger;
	use crate::svp::SvpResolver;
	use crate::svp::diagnostic::DiagnosticSpan;
	use crate::svp::rust_highlight::HighlightSpan;
	use crate::uast::UastProjection;
	use std::sync::{Arc, mpsc};

	fn test_settings() -> SettingsPersistent {
		SettingsPersistent {
			theme_name: "onedark".to_string(),
			minimap_enabled: false,
			wrap_enabled: false,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::default(),
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: Default::default(),
		}
	}

	fn build_runtime_for_path(
		path: &std::path::Path,
	) -> (
		Arc<crate::ecs::UastRegistry>,
		Arc<SvpResolver>,
		Arc<LanguageRegistry>,
	) {
		let (tx_io_notify, _rx_io_notify) = mpsc::channel();
		let registry = Arc::new(crate::ecs::UastRegistry::new(1_000_000));
		let resolver = Arc::new(SvpResolver::new(registry.clone(), tx_io_notify));
		let lang_registry = Arc::new(LanguageRegistry::new());
		registry
			.register_device_path(FILE_DEVICE_ID, path.to_str().expect("utf8 path"))
			.expect("register backing");
		(registry, resolver, lang_registry)
	}

	#[test]
	fn substitute_all_reads_source_backed_documents_via_snapshot() {
		let owned_chunks = [
			"index,left,middle,right\n00000,left_a,middle_a,right_a\n".to_string(),
			"00001,left_b,middle_b,right_b\n00002,left_c,middle_c,right_c\n".to_string(),
			"00003,left_d,middle_d,right_d\n".to_string(),
		];
		let chunk_refs: Vec<&str> = owned_chunks.iter().map(|chunk| chunk.as_str()).collect();
		let (registry, root, path) = build_exact_metric_physical_document(&chunk_refs);
		let mut root_id = Some(root);
		let mut folds = FoldSet::default();
		let mut caret = Caret::at_start();
		caret.reset_to_start(
			registry
				.find_node_at_line_col(
					root,
					&crate::uast::NoFolds,
					DocLine::ZERO,
					DocDisplayCol::ZERO,
				)
				.node_id,
			TopologyRev::ZERO,
			StateId::ZERO,
		);
		let mut ledger = UndoLedger::new();
		let mut semantic_highlights: Vec<HighlightSpan> = Vec::new();
		let mut diagnostics: Vec<DiagnosticSpan> = Vec::new();
		let mut search_pattern = None;
		let mut search_case_insensitive = false;
		let mut search_matches = Vec::new();
		let mut search_match_index = None;
		let mut search_match_info = None;
		let mut confirm_state = None;
		let mut status_message = None;
		let mut mode_override = None;

		handle_search_command(
			&registry,
			&mut root_id,
			&mut folds,
			&mut caret,
			TopologyRev::ZERO,
			&mut ledger,
			&mut semantic_highlights,
			&mut diagnostics,
			Some(path.to_str().expect("utf8 temp path")),
			&mut search_pattern,
			&mut search_case_insensitive,
			&mut search_matches,
			&mut search_match_index,
			&mut search_match_info,
			&mut confirm_state,
			&mut status_message,
			&mut mode_override,
			EditorCommand::SubstituteAll {
				pattern: ",".to_string(),
				replacement: "\t".to_string(),
				range: CommandRange::WholeFile,
				flags: SubstituteFlags {
					global: true,
					confirm: false,
					case_insensitive: false,
				},
			},
		);

		assert_eq!(status_message.as_deref(), Some("15 substitution(s)"));
		let root = root_id.expect("mutated root should remain available");
		let bytes = registry
			.read_loaded_slice(
				root,
				DocByte::ZERO,
				DocByte::new(registry.get_total_bytes(root)),
			)
			.expect("collect rewritten bytes");
		let text = String::from_utf8(bytes).expect("utf8");
		assert!(text.starts_with("index\tleft\tmiddle\tright\n"));
		assert!(text.contains("00002\tleft_c\tmiddle_c\tright_c\n"));

		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn large_substitute_uses_shadow_generation_and_save_commits_it() {
		let path = temp_test_path("large-substitute.txt");
		let original = "Chase,Account,North\n".repeat(80);
		std::fs::write(&path, &original).expect("write source");

		let (registry, resolver, lang_registry) = build_runtime_for_path(&path);
		let ctx = super::super::RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &lang_registry,
		};
		let fixture_backing_count = resolver.registered_device_counts().0;
		let mut next_dynamic_device_id = FILE_DEVICE_ID + 1;
		let (root, first_leaf, backing_device_id) = load_root_from_path(
			&ctx,
			path.to_str().expect("utf8 path"),
			&mut next_dynamic_device_id,
		)
		.expect("load root");
		let mut fixture = TestTextEditor::new(&registry, &test_settings(), 120, 40);
		let mut tab = fixture.editor_mut();
		tab.document.root_id = root;
		tab.document.file_path = Some(path.to_string_lossy().into_owned());
		tab.document.backing_path = Some(path.to_string_lossy().into_owned());
		tab.document.backing_device_id = backing_device_id;
		assert_eq!(
			resolver.registered_device_counts().0,
			fixture_backing_count + 1
		);
		tab.cursor.primary_mut().reset_to_start(
			first_leaf,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		);
		tab.cursor.restore_snapshot(CursorSnapshot::from_bytes(
			DocByte::ZERO,
			vec![DocByte::new(20)],
		));
		let cursors_before = tab.cursor.snapshot();
		let mut ui = UiState {
			status_message: None,
			document_text_response: None,
			document_text_response_buffer_id: None,
			busy: false,
			pending_quit: false,
			mode_override: None,
			yank_flash: None,
		};
		let mut search_pattern = None;
		let mut search_case_insensitive = false;
		let mut confirm_state = None;

		handle_search_doctab_command(
			&ctx,
			&mut tab,
			&mut search_pattern,
			&mut search_case_insensitive,
			&mut confirm_state,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::SubstituteAll {
				pattern: "Chase".to_string(),
				replacement: "Bank".to_string(),
				range: CommandRange::WholeFile,
				flags: SubstituteFlags::default(),
			},
		);
		assert_eq!(ui.status_message.as_deref(), Some("80 substitution(s)"));
		assert!(
			tab.document.shadow.is_some(),
			"large substitute must create shadow history"
		);
		assert_ne!(
			tab.document.backing_path.as_deref(),
			tab.document.file_path.as_deref(),
			"live backing path must move to the shadow generation",
		);
		assert_eq!(
			resolver.registered_device_counts().0,
			fixture_backing_count + 1,
			"generation replacement must retire the source backing"
		);
		let live_bytes = crate::engine::document::io::read_document_snapshot(
			&registry,
			tab.document.root_id,
			tab.document
				.backing_path
				.as_deref()
				.map(std::path::Path::new),
		)
		.expect("read shadow generation");
		assert!(
			String::from_utf8(live_bytes)
				.expect("utf8")
				.starts_with("Bank,Account,North\n")
		);
		let on_disk = std::fs::read_to_string(&path).expect("read original file");
		assert!(on_disk.starts_with("Chase,Account,North\n"));

		let _ = handle_history_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::Undo,
		);
		let undone_bytes = crate::engine::document::io::read_document_snapshot(
			&registry,
			tab.document.root_id,
			tab.document
				.backing_path
				.as_deref()
				.map(std::path::Path::new),
		)
		.expect("read undo generation");
		assert!(
			String::from_utf8(undone_bytes)
				.expect("utf8")
				.starts_with("Chase,Account,North\n")
		);
		assert_eq!(
			tab.cursor.snapshot(),
			cursors_before,
			"generation undo must restore the invoking view's full cursor set",
		);
		assert_eq!(
			resolver.registered_device_counts().0,
			fixture_backing_count + 1,
			"generation undo must retire the replaced backing"
		);

		let _ = handle_history_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			EditorCommand::Redo,
		);
		let redone_bytes = crate::engine::document::io::read_document_snapshot(
			&registry,
			tab.document.root_id,
			tab.document
				.backing_path
				.as_deref()
				.map(std::path::Path::new),
		)
		.expect("read redo generation");
		assert!(
			String::from_utf8(redone_bytes)
				.expect("utf8")
				.starts_with("Bank,Account,North\n")
		);
		assert_eq!(
			resolver.registered_device_counts().0,
			fixture_backing_count + 1,
			"generation redo must retire the replaced backing"
		);

		let _ = handle_file_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			SaveDurability::default(),
			EditorCommand::WriteFile,
		);
		let saved = std::fs::read_to_string(&path).expect("read saved file");
		assert!(
			saved.starts_with("Bank,Account,North\n"),
			"save status {:?}; saved prefix {:?}",
			ui.status_message,
			&saved[..saved.len().min(64)],
		);
		assert!(
			tab.document.shadow.is_none(),
			"save must clear shadow session"
		);
		assert!(
			!shadow_dir_for_path(&path).expect("shadow path").exists(),
			"save must remove the shadow directory",
		);
		assert_eq!(
			resolver.registered_device_counts().0,
			fixture_backing_count + 1,
			"save must replace rather than accumulate backing generations"
		);
	}

	#[test]
	fn load_file_recovers_shadow_manifest_head_generation() {
		let path = temp_test_path("recover-shadow.txt");
		let original = "Chase,Account,North\n".repeat(80);
		std::fs::write(&path, &original).expect("write source");

		{
			let (registry, resolver, lang_registry) = build_runtime_for_path(&path);
			let ctx = super::super::RuntimeContext {
				registry: &registry,
				resolver: &resolver,
				lang_registry: &lang_registry,
			};
			let mut next_dynamic_device_id = FILE_DEVICE_ID + 1;
			let (root, first_leaf, backing_device_id) = load_root_from_path(
				&ctx,
				path.to_str().expect("utf8 path"),
				&mut next_dynamic_device_id,
			)
			.expect("load root");
			let mut fixture = TestTextEditor::new(&registry, &test_settings(), 120, 40);
			let mut tab = fixture.editor_mut();
			tab.document.root_id = root;
			tab.document.file_path = Some(path.to_string_lossy().into_owned());
			tab.document.backing_path = Some(path.to_string_lossy().into_owned());
			tab.document.backing_device_id = backing_device_id;
			tab.cursor.primary_mut().reset_to_start(
				first_leaf,
				tab.document.topology_rev,
				tab.document.ledger.current_state_id,
			);
			let mut ui = UiState {
				status_message: None,
				document_text_response: None,
				document_text_response_buffer_id: None,
				busy: false,
				pending_quit: false,
				mode_override: None,
				yank_flash: None,
			};
			let mut search_pattern = None;
			let mut search_case_insensitive = false;
			let mut confirm_state = None;

			handle_search_doctab_command(
				&ctx,
				&mut tab,
				&mut search_pattern,
				&mut search_case_insensitive,
				&mut confirm_state,
				&mut ui,
				&mut next_dynamic_device_id,
				EditorCommand::SubstituteAll {
					pattern: "Chase".to_string(),
					replacement: "Bank".to_string(),
					range: CommandRange::WholeFile,
					flags: SubstituteFlags::default(),
				},
			);
			assert!(
				tab.document.shadow.is_some(),
				"shadow state should persist for recovery"
			);
		}

		let (registry, resolver, lang_registry) = build_runtime_for_path(&path);
		let ctx = super::super::RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &lang_registry,
		};
		let mut fixture = TestTextEditor::new(&registry, &test_settings(), 120, 40);
		let mut tab = fixture.editor_mut();
		let mut ui = UiState {
			status_message: None,
			document_text_response: None,
			document_text_response_buffer_id: None,
			busy: false,
			pending_quit: false,
			mode_override: None,
			yank_flash: None,
		};
		let mut next_dynamic_device_id = FILE_DEVICE_ID + 1;
		let _ = handle_file_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_dynamic_device_id,
			SaveDurability::default(),
			EditorCommand::LoadFile(path.to_string_lossy().into_owned()),
		);
		assert_eq!(
			ui.status_message.as_deref(),
			Some("Recovered unsaved large-file session"),
		);
		assert!(
			tab.document.shadow.is_some(),
			"load must recover shadow session"
		);
		let recovered_bytes = crate::engine::document::io::read_document_snapshot(
			&registry,
			tab.document.root_id,
			tab.document
				.backing_path
				.as_deref()
				.map(std::path::Path::new),
		)
		.expect("read recovered generation");
		assert!(
			String::from_utf8(recovered_bytes)
				.expect("utf8")
				.starts_with("Bank,Account,North\n")
		);

		if let Some(shadow) = tab.document.shadow.take() {
			crate::engine::shadow::discard_shadow_session(&shadow)
				.expect("cleanup recovered shadow");
		}
		let _ = std::fs::remove_file(path);
	}
}
