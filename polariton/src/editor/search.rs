use ropey::Rope;
use crate::editor::coords::{CharIdx, LineIdx};

/// A single search match in the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchMatch {
	pub line: LineIdx,
	pub col_start: CharIdx,
	pub col_end: CharIdx,
	/// Char index into the rope for the start of the match.
	pub char_start: usize,
	pub char_end: usize,
}

/// Search state, kept separate from the buffer so the widget can query it.
pub struct SearchState {
	pub query: String,
	pub replacement: String,
	pub matches: Vec<SearchMatch>,
	pub current_match: usize,
	pub case_sensitive: bool,
	pub is_open: bool,
}

impl SearchState {
	pub fn new() -> Self {
		Self {
			query: String::new(),
			replacement: String::new(),
			matches: Vec::new(),
			current_match: 0,
			case_sensitive: false,
			is_open: false,
		}
	}

	/// Recompute all matches against the given rope.
	pub fn find_all(&mut self, rope: &Rope) {
		self.matches.clear();
		if self.query.is_empty() {
			return;
		}

		let query_chars: Vec<char> = self.query.chars().collect();
		let query_cmp: Vec<char> = query_chars
			.iter()
			.map(|&ch| fold_char(ch, self.case_sensitive))
			.collect();
		let query_len = query_chars.len();
		if query_len == 0 {
			return;
		}

		for line_idx in 0..rope.len_lines() {
			let line_char_start = rope.line_to_char(line_idx);
			let line_text: String = rope
				.line(line_idx)
				.chars()
				.filter(|&ch| ch != '\n' && ch != '\r')
				.collect();
			let line_chars: Vec<char> = line_text.chars().collect();
			let line_cmp: Vec<char> = line_chars
				.iter()
				.map(|&ch| fold_char(ch, self.case_sensitive))
				.collect();

			if line_cmp.len() < query_len {
				continue;
			}

			for start in 0..=line_cmp.len() - query_len {
				if line_cmp[start..start + query_len] == query_cmp[..] {
					let char_start = line_char_start + start;
					let char_end = char_start + query_len;
					self.matches.push(SearchMatch {
						line: LineIdx(line_idx),
						col_start: CharIdx(start),
						col_end: CharIdx(start + query_len),
						char_start,
						char_end,
					});
				}
			}
		}

		// Clamp current match
		if !self.matches.is_empty() {
			self.current_match = self.current_match.min(self.matches.len() - 1);
		} else {
			self.current_match = 0;
		}
	}

	pub fn match_count(&self) -> usize {
		self.matches.len()
	}

	pub fn next_match(&mut self) {
		if !self.matches.is_empty() {
			self.current_match = (self.current_match + 1) % self.matches.len();
		}
	}

	pub fn prev_match(&mut self) {
		if !self.matches.is_empty() {
			self.current_match = if self.current_match == 0 {
				self.matches.len() - 1
			} else {
				self.current_match - 1
			};
		}
	}

	/// Find the nearest match at or after the given char index.
	pub fn jump_to_nearest(&mut self, char_idx: usize) {
		if self.matches.is_empty() {
			return;
		}
		for (i, m) in self.matches.iter().enumerate() {
			if m.char_start >= char_idx {
				self.current_match = i;
				return;
			}
		}
		self.current_match = 0; // wrap
	}

	pub fn current(&self) -> Option<&SearchMatch> {
		self.matches.get(self.current_match)
	}

	/// Replace the current match in-place in the rope. Returns the replacement
	/// length delta so the caller can adjust the cursor.
	pub fn replace_current(&mut self, rope: &mut Rope) -> Option<i64> {
		let m = self.matches.get(self.current_match)?.clone();
		rope.remove(m.char_start..m.char_end);
		rope.insert(m.char_start, &self.replacement);
		let delta = self.replacement.chars().count() as i64 - (m.char_end - m.char_start) as i64;
		Some(delta)
	}

	/// Replace all matches. Returns count replaced.
	pub fn replace_all(&mut self, rope: &mut Rope) -> usize {
		let count = self.matches.len();
		// Replace from end to start so byte offsets stay valid
		for m in self.matches.iter().rev() {
			rope.remove(m.char_start..m.char_end);
			rope.insert(m.char_start, &self.replacement);
		}
		self.matches.clear();
		self.current_match = 0;
		count
	}
}

fn fold_char(ch: char, case_sensitive: bool) -> char {
	if case_sensitive {
		ch
	} else {
		ch.to_lowercase().next().unwrap_or(ch)
	}
}
