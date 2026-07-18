use ropey::Rope;

use super::coords::{CharIdx, Selection};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EditKind {
	Insert,
	Delete,
	Newline,
	Paste,
	Other,
}

#[derive(Clone)]
struct TextChange {
	start: CharIdx,
	deleted: String,
	inserted: String,
}

#[derive(Clone)]
struct UndoEntry {
	changes: Vec<TextChange>,
	before_selection: Selection,
	after_selection: Selection,
	kind: EditKind,
	timestamp_ms: u64,
}

/// Configurable undo history.
pub struct UndoConfig {
	/// Max number of undo entries.
	pub max_history: usize,
	/// Consecutive edits of the same kind within this window (ms) are grouped.
	pub group_timeout_ms: u64,
}

impl Default for UndoConfig {
	fn default() -> Self {
		Self {
			max_history: 500,
			group_timeout_ms: 800,
		}
	}
}

pub struct UndoStack {
	history: Vec<UndoEntry>,
	index: usize,
	config: UndoConfig,
	active: Option<usize>,
	merge_blocked: bool,
}

impl UndoStack {
	pub fn new(config: UndoConfig) -> Self {
		Self {
			history: Vec::new(),
			index: 0,
			config,
			active: None,
			merge_blocked: false,
		}
	}

	fn now_ms() -> u64 {
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap_or_default()
			.as_millis() as u64
	}

	pub fn begin_edit(&mut self, selection: Selection, kind: EditKind) {
		if self.active.is_some() {
			return;
		}
		let now = Self::now_ms();

		let can_merge = if !self.merge_blocked && self.index > 0 && !self.history.is_empty() {
			let last = &self.history[self.index - 1];
			last.kind == kind
				&& kind == EditKind::Insert
				&& now - last.timestamp_ms < self.config.group_timeout_ms
		} else {
			false
		};

		self.history.truncate(self.index);
		if can_merge {
			let idx = self.index - 1;
			self.history[idx].timestamp_ms = now;
			self.active = Some(idx);
		} else {
			self.history.push(UndoEntry {
				changes: Vec::new(),
				before_selection: selection,
				after_selection: selection,
				kind,
				timestamp_ms: now,
			});
			if self.history.len() > self.config.max_history {
				self.history.remove(0);
			}
			self.index = self.history.len();
			self.active = self.history.len().checked_sub(1);
		}
		self.merge_blocked = false;
	}

	pub fn record_change(&mut self, start: CharIdx, deleted: String, inserted: String) {
		if deleted == inserted {
			return;
		}
		if let Some(idx) = self.active {
			self.history[idx].changes.push(TextChange {
				start,
				deleted,
				inserted,
			});
		}
	}

	pub fn end_edit(&mut self, selection: Selection) {
		let Some(idx) = self.active.take() else {
			return;
		};
		if self.history[idx].changes.is_empty() {
			if idx + 1 == self.history.len() {
				self.history.pop();
				self.index = self.history.len();
			}
			return;
		}
		self.history[idx].after_selection = selection;
		self.history[idx].timestamp_ms = Self::now_ms();
		self.index = self.history.len();
	}

	pub fn force_boundary(&mut self, selection: Selection) {
		self.begin_edit(selection, EditKind::Other);
		self.merge_blocked = true;
	}

	fn apply_change(rope: &mut Rope, change: &TextChange, forward: bool) {
		let (remove_len, insert_text) = if forward {
			(change.deleted.chars().count(), change.inserted.as_str())
		} else {
			(change.inserted.chars().count(), change.deleted.as_str())
		};
		rope.remove(*change.start..*change.start + remove_len);
		if !insert_text.is_empty() {
			rope.insert(*change.start, insert_text);
		}
	}

	pub fn undo(&mut self, rope: &mut Rope, selection: &mut Selection) -> bool {
		self.end_edit(*selection);
		if self.index == 0 {
			return false;
		}
		self.index -= 1;
		let entry = &self.history[self.index];
		for change in entry.changes.iter().rev() {
			Self::apply_change(rope, change, false);
		}
		*selection = entry.before_selection;
		self.merge_blocked = true;
		true
	}

	pub fn redo(&mut self, rope: &mut Rope, selection: &mut Selection) -> bool {
		self.end_edit(*selection);
		if self.index >= self.history.len() {
			return false;
		}
		let entry = &self.history[self.index];
		for change in &entry.changes {
			Self::apply_change(rope, change, true);
		}
		*selection = entry.after_selection;
		self.index += 1;
		self.merge_blocked = true;
		true
	}
}
