use std::collections::VecDeque;

pub(crate) const MAX_MESSAGE_LINES: usize = 1_024;
pub(crate) const MAX_MESSAGE_BYTES: usize = 256 * 1024;
pub(crate) const MAX_MESSAGE_LINE_BYTES: usize = 4 * 1024;
const ELISION: &str = "…";

#[derive(Debug, Clone)]
struct MessageLine {
	sequence: u64,
	text: String,
}

#[derive(Debug, Default)]
pub(crate) struct MessageLog {
	lines: VecDeque<MessageLine>,
	bytes: usize,
	partial: String,
	partial_truncated: bool,
	partial_sequence: u64,
	sequence: u64,
}

impl MessageLog {
	pub(crate) fn sequence(&self) -> u64 {
		self.sequence
	}

	pub(crate) fn write(&mut self, text: &str, newline: bool) {
		self.sequence = self.sequence.saturating_add(1);
		let sequence = self.sequence;
		let mut rest = text;
		while let Some(index) = rest.find('\n') {
			self.extend_partial(&rest[..index], sequence);
			self.finish_partial(sequence);
			rest = &rest[index + 1..];
		}
		self.extend_partial(rest, sequence);
		if newline {
			self.finish_partial(sequence);
		}
	}

	pub(crate) fn push_line(&mut self, text: &str) {
		self.write(text, true);
	}

	pub(crate) fn latest_since(&self, sequence: u64) -> Option<String> {
		if self.partial_sequence > sequence && !self.partial.is_empty() {
			return Some(self.partial.clone());
		}
		self.lines
			.iter()
			.rev()
			.find(|line| line.sequence > sequence)
			.map(|line| line.text.clone())
	}

	#[cfg(test)]
	pub(crate) fn line_count(&self) -> usize {
		self.lines.len()
	}

	#[cfg(test)]
	pub(crate) fn total_bytes(&self) -> usize {
		self.bytes + self.partial.len()
	}

	fn extend_partial(&mut self, text: &str, sequence: u64) {
		self.partial_sequence = sequence;
		if self.partial_truncated {
			return;
		}
		let remaining = MAX_MESSAGE_LINE_BYTES.saturating_sub(self.partial.len());
		if remaining == 0 {
			self.partial_truncated |= !text.is_empty();
			if self.partial_truncated {
				install_elision(&mut self.partial, MAX_MESSAGE_LINE_BYTES);
			}
			return;
		}
		let keep = floor_char_boundary(text, remaining);
		self.partial.push_str(&text[..keep]);
		if keep < text.len() {
			self.partial_truncated = true;
			install_elision(&mut self.partial, MAX_MESSAGE_LINE_BYTES);
		}
		while self.bytes.saturating_add(self.partial.len()) > MAX_MESSAGE_BYTES {
			let Some(line) = self.lines.pop_front() else {
				break;
			};
			self.bytes = self.bytes.saturating_sub(line.text.len());
		}
	}

	fn finish_partial(&mut self, sequence: u64) {
		if self.partial_truncated {
			install_elision(&mut self.partial, MAX_MESSAGE_LINE_BYTES);
		}
		let text = std::mem::take(&mut self.partial);
		self.partial_truncated = false;
		self.partial_sequence = 0;
		self.bytes = self.bytes.saturating_add(text.len());
		self.lines.push_back(MessageLine { sequence, text });
		while self.lines.len() > MAX_MESSAGE_LINES || self.bytes > MAX_MESSAGE_BYTES {
			let Some(line) = self.lines.pop_front() else {
				break;
			};
			self.bytes = self.bytes.saturating_sub(line.text.len());
		}
	}
}

fn install_elision(text: &mut String, limit: usize) {
	let target = limit.saturating_sub(ELISION.len());
	let end = floor_char_boundary(text, target);
	text.truncate(end);
	if text.len().saturating_add(ELISION.len()) <= limit {
		text.push_str(ELISION);
	}
}

fn floor_char_boundary(text: &str, max: usize) -> usize {
	let mut index = max.min(text.len());
	while index > 0 && !text.is_char_boundary(index) {
		index -= 1;
	}
	index
}

#[cfg(test)]
mod tests {
	use super::{MAX_MESSAGE_BYTES, MAX_MESSAGE_LINE_BYTES, MAX_MESSAGE_LINES, MessageLog};

	#[test]
	fn line_and_ring_bounds_are_structural() {
		let mut log = MessageLog::default();
		for _ in 0..(MAX_MESSAGE_LINES + 20) {
			log.push_line(&"x".repeat(MAX_MESSAGE_LINE_BYTES * 2));
		}
		assert!(log.line_count() <= MAX_MESSAGE_LINES);
		assert!(log.total_bytes() <= MAX_MESSAGE_BYTES);
		assert!(log.latest_since(0).unwrap().len() <= MAX_MESSAGE_LINE_BYTES);
		assert!(log.latest_since(0).unwrap().ends_with('…'));
	}

	#[test]
	fn latest_since_does_not_resurface_old_output() {
		let mut log = MessageLog::default();
		log.push_line("old");
		let sequence = log.sequence();
		assert_eq!(log.latest_since(sequence), None);
		log.write("new", false);
		assert_eq!(log.latest_since(sequence).as_deref(), Some("new"));
	}
}
