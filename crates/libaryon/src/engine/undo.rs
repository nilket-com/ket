use crate::core::{DocByte, DocDisplayCol, DocLine, DocLineCount, StateId, TAB_SIZE};
use crate::engine::session::CursorSnapshot;

/// One byte-exact document splice recorded for replay.
///
/// The historical `TextDelta` name describes its role in text-buffer history;
/// its payload is deliberately not UTF-8 constrained. Undo and redo must
/// round-trip arbitrary file bytes without lossy conversion.
#[derive(Debug, Clone)]
pub struct TextDelta {
	pub global_byte_offset: DocByte,
	pub deleted_bytes: Vec<u8>,
	pub inserted_bytes: Vec<u8>,
	pub state_before: StateId,
	pub state_after: StateId,
}

fn inverted_deltas(
	deltas: &[TextDelta],
	state_before: StateId,
	state_after: StateId,
) -> Vec<TextDelta> {
	deltas
		.iter()
		.rev()
		.map(|delta| TextDelta {
			global_byte_offset: delta.global_byte_offset,
			deleted_bytes: delta.inserted_bytes.clone(),
			inserted_bytes: delta.deleted_bytes.clone(),
			state_before,
			state_after,
		})
		.collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationTarget {
	pub generation_id: u64,
	pub path: String,
	pub byte_len: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SwapGeneration {
	pub(crate) before: GenerationTarget,
	pub(crate) after: GenerationTarget,
	pub(crate) cursors_before: CursorSnapshot,
	pub(crate) cursors_after: CursorSnapshot,
	pub(crate) state_before: StateId,
	pub(crate) state_after: StateId,
}

#[derive(Debug, Clone)]
enum HistoryEntry {
	TextDeltas {
		deltas: Vec<TextDelta>,
		before: Option<CursorSnapshot>,
		after: Option<CursorSnapshot>,
	},
	SwapGeneration(SwapGeneration),
}

#[derive(Debug, Clone)]
pub(crate) enum HistoryReplay {
	Text {
		deltas: Vec<TextDelta>,
		cursors: CursorSnapshot,
	},
	SwapGeneration {
		target: GenerationTarget,
		cursors: CursorSnapshot,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryDirection {
	Undo,
	Redo,
}

pub struct UndoLedger {
	undo_stack: Vec<HistoryEntry>,
	redo_stack: Vec<HistoryEntry>,
	pub current_state_id: StateId,
	pub saved_state_id: StateId,
	next_state_id: StateId,
}

impl Default for UndoLedger {
	fn default() -> Self {
		Self::new()
	}
}

impl UndoLedger {
	pub fn new() -> Self {
		Self {
			undo_stack: Vec::new(),
			redo_stack: Vec::new(),
			current_state_id: StateId::ZERO,
			saved_state_id: StateId::ZERO,
			next_state_id: StateId::new(1),
		}
	}

	fn allocate_state_id(&mut self) -> StateId {
		let state_id = self.next_state_id;
		self.next_state_id = self.next_state_id.next();
		state_id
	}

	/// Allocate a single new state ID for a forward transaction group.
	/// Sets `state_before`/`state_after` on every delta in the group and advances
	/// `current_state_id` once for the whole transaction.
	pub(crate) fn push_group_pending_cursors(&mut self, mut deltas: Vec<TextDelta>) {
		if deltas.is_empty() {
			return;
		}

		let state_before = self.current_state_id;
		let state_after = self.allocate_state_id();
		for delta in &mut deltas {
			delta.state_before = state_before;
			delta.state_after = state_after;
		}

		self.current_state_id = state_after;
		self.undo_stack.push(HistoryEntry::TextDeltas {
			deltas,
			before: None,
			after: None,
		});
		self.redo_stack.clear();
	}

	pub(crate) fn push_group_with_cursors(
		&mut self,
		mut deltas: Vec<TextDelta>,
		before: CursorSnapshot,
		after: CursorSnapshot,
	) {
		if deltas.is_empty() {
			return;
		}

		let state_before = self.current_state_id;
		let state_after = self.allocate_state_id();
		for delta in &mut deltas {
			delta.state_before = state_before;
			delta.state_after = state_after;
		}

		self.current_state_id = state_after;
		self.undo_stack.push(HistoryEntry::TextDeltas {
			deltas,
			before: Some(before),
			after: Some(after),
		});
		self.redo_stack.clear();
	}

	pub(crate) fn finish_cursor_transaction(
		&mut self,
		state_before: StateId,
		before: CursorSnapshot,
		after: CursorSnapshot,
	) {
		if self.current_state_id == state_before {
			return;
		}
		let Some(HistoryEntry::TextDeltas {
			deltas,
			before: stored_before @ None,
			after: stored_after @ None,
		}) = self.undo_stack.last_mut()
		else {
			panic!("cursor transaction must finalize its pending text history entry");
		};
		assert_eq!(
			deltas
				.last()
				.expect("history delta group must not be empty")
				.state_before,
			state_before,
			"cursor transaction must finish the edit it started from"
		);
		*stored_before = Some(before);
		*stored_after = Some(after);
	}

	pub(crate) fn push_swap_generation(&mut self, mut swap: SwapGeneration) {
		let state_before = self.current_state_id;
		let state_after = self.allocate_state_id();
		swap.state_before = state_before;
		swap.state_after = state_after;

		self.current_state_id = state_after;
		self.undo_stack.push(HistoryEntry::SwapGeneration(swap));
		self.redo_stack.clear();
	}

	pub fn clear(&mut self) {
		self.undo_stack.clear();
		self.redo_stack.clear();
		let baseline = self.allocate_state_id();
		self.current_state_id = baseline;
		self.saved_state_id = baseline;
	}

	/// Mark the current state as the saved-to-disk state.
	pub fn mark_saved(&mut self) {
		self.saved_state_id = self.current_state_id;
	}

	/// True if the document has been modified since the last save.
	pub fn is_dirty(&self) -> bool {
		self.current_state_id != self.saved_state_id
	}

	fn replay_for(entry: &HistoryEntry, direction: HistoryDirection) -> Option<HistoryReplay> {
		match (entry, direction) {
			(
				HistoryEntry::TextDeltas {
					deltas,
					before,
					after: _,
				},
				HistoryDirection::Undo,
			) => {
				deltas.last()?;
				Some(HistoryReplay::Text {
					deltas: inverted_deltas(deltas, StateId::ZERO, StateId::ZERO),
					cursors: before.as_ref()?.clone(),
				})
			}
			(
				HistoryEntry::TextDeltas {
					deltas,
					before: _,
					after,
				},
				HistoryDirection::Redo,
			) => {
				deltas.last()?;
				Some(HistoryReplay::Text {
					deltas: deltas.clone(),
					cursors: after.as_ref()?.clone(),
				})
			}
			(HistoryEntry::SwapGeneration(swap), HistoryDirection::Undo) => {
				Some(HistoryReplay::SwapGeneration {
					target: swap.before.clone(),
					cursors: swap.cursors_before.clone(),
				})
			}
			(HistoryEntry::SwapGeneration(swap), HistoryDirection::Redo) => {
				Some(HistoryReplay::SwapGeneration {
					target: swap.after.clone(),
					cursors: swap.cursors_after.clone(),
				})
			}
		}
	}

	pub(crate) fn preview(&self, direction: HistoryDirection) -> Option<HistoryReplay> {
		let entry = match direction {
			HistoryDirection::Undo => self.undo_stack.last()?,
			HistoryDirection::Redo => self.redo_stack.last()?,
		};
		Self::replay_for(entry, direction)
	}

	/// Return the byte deltas that moved the ledger between two adjacent states.
	///
	/// The mutation coordinator calls this after a command has committed. A
	/// forward edit or redo is then at the top of `undo_stack`; an undo is at the
	/// top of `redo_stack` and must be inverted in replay order.
	pub(crate) fn deltas_for_transition(
		&self,
		before: StateId,
		after: StateId,
	) -> Option<Vec<TextDelta>> {
		if let Some(HistoryEntry::TextDeltas { deltas, .. }) = self.undo_stack.last()
			&& deltas
				.last()
				.is_some_and(|delta| delta.state_before == before && delta.state_after == after)
		{
			return Some(deltas.clone());
		}
		let HistoryEntry::TextDeltas { deltas, .. } = self.redo_stack.last()? else {
			return None;
		};
		if !deltas
			.last()
			.is_some_and(|delta| delta.state_after == before && delta.state_before == after)
		{
			return None;
		}
		Some(inverted_deltas(deltas, before, after))
	}

	/// Commit a replay that has already completed its fallible preparation and
	/// mutation. This operation only moves an existing entry and updates its state;
	/// it cannot fail.
	pub(crate) fn commit(&mut self, direction: HistoryDirection) {
		let entry = match direction {
			HistoryDirection::Undo => self
				.undo_stack
				.pop()
				.expect("previewed undo entry must still exist"),
			HistoryDirection::Redo => self
				.redo_stack
				.pop()
				.expect("previewed redo entry must still exist"),
		};
		let state = match (&entry, direction) {
			(HistoryEntry::TextDeltas { deltas, .. }, HistoryDirection::Undo) => {
				deltas
					.last()
					.expect("history delta group must not be empty")
					.state_before
			}
			(HistoryEntry::TextDeltas { deltas, .. }, HistoryDirection::Redo) => {
				deltas
					.last()
					.expect("history delta group must not be empty")
					.state_after
			}
			(HistoryEntry::SwapGeneration(swap), HistoryDirection::Undo) => swap.state_before,
			(HistoryEntry::SwapGeneration(swap), HistoryDirection::Redo) => swap.state_after,
		};
		self.current_state_id = state;
		match direction {
			HistoryDirection::Undo => self.redo_stack.push(entry),
			HistoryDirection::Redo => self.undo_stack.push(entry),
		}
	}

	pub fn restore_generation_history(
		&mut self,
		generations: &[GenerationTarget],
		current_index: usize,
	) {
		self.undo_stack.clear();
		self.redo_stack.clear();
		if generations.is_empty() {
			let baseline = self.allocate_state_id();
			self.current_state_id = baseline;
			self.saved_state_id = baseline;
			return;
		}

		let generation_states: Vec<_> = (0..generations.len())
			.map(|_| self.allocate_state_id())
			.collect();
		let current_index = current_index.min(generations.len() - 1);

		for (idx, window) in generations.windows(2).take(current_index).enumerate() {
			self.undo_stack
				.push(HistoryEntry::SwapGeneration(SwapGeneration {
					before: window[0].clone(),
					after: window[1].clone(),
					cursors_before: CursorSnapshot::single(DocByte::ZERO),
					cursors_after: CursorSnapshot::single(DocByte::ZERO),
					state_before: generation_states[idx],
					state_after: generation_states[idx + 1],
				}));
		}
		for idx in (current_index..generations.len().saturating_sub(1)).rev() {
			self.redo_stack
				.push(HistoryEntry::SwapGeneration(SwapGeneration {
					before: generations[idx].clone(),
					after: generations[idx + 1].clone(),
					cursors_before: CursorSnapshot::single(DocByte::ZERO),
					cursors_after: CursorSnapshot::single(DocByte::ZERO),
					state_before: generation_states[idx],
					state_after: generation_states[idx + 1],
				}));
		}
		self.current_state_id = generation_states[current_index];
		self.saved_state_id = generation_states[0];
	}
}

/// Compute (line, col) from a global byte offset into raw document bytes.
pub fn line_col_from_byte_offset(doc: &[u8], byte_offset: DocByte) -> (DocLine, DocDisplayCol) {
	let mut line = DocLine::ZERO;
	let mut col = DocDisplayCol::ZERO;
	for (i, &b) in doc.iter().enumerate() {
		if i as u64 >= byte_offset.get() {
			break;
		}
		if b == b'\n' {
			line += DocLineCount::new(1);
			col = DocDisplayCol::ZERO;
		} else if b == b'\t' {
			col = col
				.checked_add(TAB_SIZE - (col.get() % TAB_SIZE))
				.expect("DocDisplayCol addition overflow");
		} else {
			col = col.checked_add(1).expect("DocDisplayCol addition overflow");
		}
	}
	(line, col)
}

/// Compute global byte offset from (line, col) in raw document bytes.
pub fn byte_offset_from_line_col(
	doc: &[u8],
	target_line: DocLine,
	target_col: DocDisplayCol,
) -> DocByte {
	let mut line = DocLine::ZERO;
	let mut col = DocDisplayCol::ZERO;
	for (i, &b) in doc.iter().enumerate() {
		if line == target_line && col >= target_col {
			return DocByte::new(i as u64);
		}
		if b == b'\n' {
			if line == target_line {
				// Cursor is past end of this line; clamp to newline position.
				return DocByte::new(i as u64);
			}
			line += DocLineCount::new(1);
			col = DocDisplayCol::ZERO;
		} else if b == b'\t' {
			col = col
				.checked_add(TAB_SIZE - (col.get() % TAB_SIZE))
				.expect("DocDisplayCol addition overflow");
		} else {
			col = col.checked_add(1).expect("DocDisplayCol addition overflow");
		}
	}
	DocByte::new(doc.len() as u64)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn delta() -> TextDelta {
		TextDelta {
			global_byte_offset: DocByte::ZERO,
			deleted_bytes: Vec::new(),
			inserted_bytes: "x".as_bytes().to_vec(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		}
	}

	fn generation(id: u64) -> GenerationTarget {
		GenerationTarget {
			generation_id: id,
			path: format!("generation-{id}"),
			byte_len: id,
		}
	}

	fn push_delta_group(ledger: &mut UndoLedger) {
		let cursors = CursorSnapshot::single(DocByte::ZERO);
		ledger.push_group_with_cursors(vec![delta()], cursors.clone(), cursors);
	}

	#[test]
	fn undo_replay_and_committed_transition_share_identical_byte_geometry() {
		let mut ledger = UndoLedger::new();
		let cursors = CursorSnapshot::single(DocByte::ZERO);
		ledger.push_group_with_cursors(
			vec![
				TextDelta {
					global_byte_offset: DocByte::new(9),
					deleted_bytes: "old".as_bytes().to_vec(),
					inserted_bytes: "newer".as_bytes().to_vec(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				},
				TextDelta {
					global_byte_offset: DocByte::new(2),
					deleted_bytes: Vec::new(),
					inserted_bytes: "x".as_bytes().to_vec(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				},
			],
			cursors.clone(),
			cursors,
		);
		let state_before_undo = ledger.current_state_id;
		let HistoryReplay::Text {
			deltas: replayed, ..
		} = ledger.preview(HistoryDirection::Undo).expect("undo replay")
		else {
			panic!("text history must replay text deltas");
		};

		ledger.commit(HistoryDirection::Undo);
		let transitioned = ledger
			.deltas_for_transition(state_before_undo, ledger.current_state_id)
			.expect("committed undo transition");
		let geometry = |deltas: &[TextDelta]| {
			deltas
				.iter()
				.map(|delta| {
					(
						delta.global_byte_offset,
						delta.deleted_bytes.clone(),
						delta.inserted_bytes.clone(),
					)
				})
				.collect::<Vec<_>>()
		};
		assert_eq!(geometry(&transitioned), geometry(&replayed));
	}

	#[test]
	fn clear_mints_a_fresh_saved_baseline() {
		let mut ledger = UndoLedger::new();
		push_delta_group(&mut ledger);
		assert_eq!(ledger.current_state_id, StateId::new(1));

		ledger.clear();
		assert_eq!(ledger.current_state_id, StateId::new(2));
		assert_eq!(ledger.saved_state_id, StateId::new(2));
		assert!(!ledger.is_dirty());
		assert!(ledger.preview(HistoryDirection::Undo).is_none());
		assert!(ledger.preview(HistoryDirection::Redo).is_none());

		push_delta_group(&mut ledger);
		assert_eq!(ledger.current_state_id, StateId::new(3));
	}

	#[test]
	fn restored_generations_use_the_monotonic_sequence() {
		let mut ledger = UndoLedger::new();
		push_delta_group(&mut ledger);
		ledger.restore_generation_history(&[generation(10), generation(11), generation(12)], 1);

		assert_eq!(ledger.saved_state_id, StateId::new(2));
		assert_eq!(ledger.current_state_id, StateId::new(3));
		assert!(ledger.is_dirty());

		let Some(HistoryReplay::SwapGeneration { target, .. }) =
			ledger.preview(HistoryDirection::Undo)
		else {
			panic!("restored generation should be undoable");
		};
		assert_eq!(target.generation_id, 10);
		assert_eq!(ledger.current_state_id, StateId::new(3));
		ledger.commit(HistoryDirection::Undo);
		assert_eq!(ledger.current_state_id, StateId::new(2));

		let Some(HistoryReplay::SwapGeneration { target, .. }) =
			ledger.preview(HistoryDirection::Redo)
		else {
			panic!("restored generation should be redoable");
		};
		assert_eq!(target.generation_id, 11);
		assert_eq!(ledger.current_state_id, StateId::new(2));
		ledger.commit(HistoryDirection::Redo);
		assert_eq!(ledger.current_state_id, StateId::new(3));

		assert!(ledger.preview(HistoryDirection::Undo).is_some());
		ledger.commit(HistoryDirection::Undo);
		push_delta_group(&mut ledger);
		assert_eq!(ledger.current_state_id, StateId::new(5));
		assert!(ledger.preview(HistoryDirection::Redo).is_none());
	}

	#[test]
	fn restoring_empty_history_still_mints_a_new_baseline() {
		let mut ledger = UndoLedger::new();
		ledger.clear();
		let before = ledger.current_state_id;

		ledger.restore_generation_history(&[], 0);

		assert!(ledger.current_state_id > before);
		assert_eq!(ledger.current_state_id, ledger.saved_state_id);
		assert!(!ledger.is_dirty());
	}
}
