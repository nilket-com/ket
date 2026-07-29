use super::folding::FoldEdit;
use super::language::{DefinitionResult, LanguageId};
use super::session::{Affinity, TextAnchor, rebase_text_anchor};
use crate::core::{BufferId, DocByte, RequestId, StateId, ViewId};
use crate::ecs::NodeId;
use std::collections::{HashMap, VecDeque};

pub(crate) const JUMP_HISTORY_LIMIT: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NavigationAnchorId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StampedTextAnchor {
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) anchor: TextAnchor,
}

#[derive(Debug, Default)]
pub(crate) struct NavigationAnchorStore {
	next_id: u64,
	anchors: HashMap<NavigationAnchorId, StampedTextAnchor>,
}

impl NavigationAnchorStore {
	pub(crate) fn insert(
		&mut self,
		root: NodeId,
		state_id: StateId,
		byte: DocByte,
	) -> NavigationAnchorId {
		let id = NavigationAnchorId(self.next_id);
		self.next_id = self.next_id.wrapping_add(1);
		self.anchors.insert(
			id,
			StampedTextAnchor {
				root,
				state_id,
				anchor: TextAnchor::new(byte, Affinity::After),
			},
		);
		id
	}

	pub(crate) fn get(&self, id: NavigationAnchorId) -> Option<StampedTextAnchor> {
		self.anchors.get(&id).copied()
	}

	pub(crate) fn remove(&mut self, id: NavigationAnchorId) {
		self.anchors.remove(&id);
	}

	pub(crate) fn rebase_after_edits(
		&mut self,
		root: NodeId,
		state_id: StateId,
		edits: &[FoldEdit],
	) {
		for stamped in self.anchors.values_mut() {
			if stamped.root != root {
				continue;
			}
			stamped.anchor = rebase_text_anchor(stamped.anchor, edits);
			stamped.state_id = state_id;
		}
	}

	pub(crate) fn invalidate_all(&mut self) {
		self.anchors.clear();
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JumpEntry {
	pub(crate) buffer_id: BufferId,
	pub(crate) preferred_view_id: ViewId,
	pub(crate) anchor_id: NavigationAnchorId,
}

#[derive(Debug, Default)]
pub(crate) struct JumpHistory {
	back: VecDeque<JumpEntry>,
	forward: Vec<JumpEntry>,
}

impl JumpHistory {
	pub(crate) fn newest_back(&self) -> Option<JumpEntry> {
		self.back.back().copied()
	}

	pub(crate) fn record_back(&mut self, entry: JumpEntry) -> Vec<JumpEntry> {
		let mut removed = self.clear_forward();
		self.back.push_back(entry);
		while self.back.len() + self.forward.len() > JUMP_HISTORY_LIMIT {
			if let Some(evicted) = self.back.pop_front() {
				removed.push(evicted);
			}
		}
		removed
	}

	pub(crate) fn clear_forward(&mut self) -> Vec<JumpEntry> {
		self.forward.drain(..).collect()
	}

	pub(crate) fn pop_back(&mut self) -> Option<JumpEntry> {
		self.back.pop_back()
	}

	pub(crate) fn push_forward(&mut self, entry: JumpEntry) -> Vec<JumpEntry> {
		self.forward.push(entry);
		let mut removed = Vec::new();
		while self.back.len() + self.forward.len() > JUMP_HISTORY_LIMIT {
			if let Some(evicted) = self.back.pop_front() {
				removed.push(evicted);
			} else if !self.forward.is_empty() {
				removed.push(self.forward.remove(0));
			}
		}
		removed
	}

	pub(crate) fn references(&self, buffer_id: BufferId) -> bool {
		self.back
			.iter()
			.chain(self.forward.iter())
			.any(|entry| entry.buffer_id == buffer_id)
	}

	#[cfg(test)]
	pub(crate) fn back_len(&self) -> usize {
		self.back.len()
	}

	#[cfg(test)]
	pub(crate) fn forward_len(&self) -> usize {
		self.forward.len()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NavigationStamp {
	pub(crate) request_id: RequestId,
	pub(crate) buffer_id: BufferId,
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) view_id: ViewId,
	pub(crate) cursor: DocByte,
}

#[derive(Debug)]
pub(crate) struct GotoDefinitionRequest {
	pub(crate) stamp: NavigationStamp,
	pub(crate) content: Vec<u8>,
	pub(crate) file_path: String,
	pub(crate) language_id: LanguageId,
}

#[derive(Debug)]
pub(crate) struct NavigationResolved {
	pub(crate) stamp: NavigationStamp,
	pub(crate) result: DefinitionResult,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tracked_anchor_rebases_with_cursor_affinity_and_advances_state() {
		let root = NodeId::from_index(0);
		let mut anchors = NavigationAnchorStore::default();
		let id = anchors.insert(root, StateId::new(1), DocByte::new(5));
		anchors.rebase_after_edits(
			root,
			StateId::new(2),
			&[FoldEdit {
				start: DocByte::new(5),
				deleted_len: 0,
				inserted_len: 3,
			}],
		);
		assert_eq!(
			anchors.get(id),
			Some(StampedTextAnchor {
				root,
				state_id: StateId::new(2),
				anchor: TextAnchor::new(DocByte::new(8), Affinity::After),
			})
		);
	}

	#[test]
	fn root_replacement_invalidates_tracked_anchors_instead_of_clamping() {
		let root = NodeId::from_index(0);
		let mut anchors = NavigationAnchorStore::default();
		let id = anchors.insert(root, StateId::new(1), DocByte::new(500));
		anchors.invalidate_all();
		assert_eq!(anchors.get(id), None);
	}

	#[test]
	fn jump_history_bounds_combined_entries_and_clears_forward_on_new_jump() {
		let mut history = JumpHistory::default();
		let buffer_id = BufferId::new(1);
		let view_id = ViewId::new(1);
		for index in 0..JUMP_HISTORY_LIMIT {
			assert!(
				history
					.record_back(JumpEntry {
						buffer_id,
						preferred_view_id: view_id,
						anchor_id: NavigationAnchorId(index as u64),
					})
					.is_empty()
			);
		}
		let consumed = history.pop_back().expect("newest back entry");
		assert!(history.push_forward(consumed).is_empty());
		assert_eq!(history.back_len(), JUMP_HISTORY_LIMIT - 1);
		assert_eq!(history.forward_len(), 1);

		let removed = history.record_back(JumpEntry {
			buffer_id,
			preferred_view_id: view_id,
			anchor_id: NavigationAnchorId(1_000),
		});
		assert_eq!(removed, vec![consumed]);
		assert_eq!(history.back_len(), JUMP_HISTORY_LIMIT);
		assert_eq!(history.forward_len(), 0);

		let evicted = history.record_back(JumpEntry {
			buffer_id,
			preferred_view_id: view_id,
			anchor_id: NavigationAnchorId(1_001),
		});
		assert_eq!(evicted.len(), 1);
		assert_eq!(history.back_len(), JUMP_HISTORY_LIMIT);
		assert_eq!(history.forward_len(), 0);
	}
}
