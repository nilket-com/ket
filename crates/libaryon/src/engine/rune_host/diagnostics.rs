use rune::Diagnostics;
use rune::diagnostics::Diagnostic;
use std::fmt::{self, Write};

pub(crate) const MAX_DIAGNOSTICS: usize = 64;
pub(crate) const MAX_DIAGNOSTIC_BYTES: usize = 512;
pub(crate) const MAX_DIAGNOSTIC_TOTAL_BYTES: usize = 32 * 1024;

pub(crate) fn render_diagnostics(diagnostics: &Diagnostics) -> Vec<String> {
	let mut rendered = Vec::new();
	let mut total = 0usize;
	for diagnostic in diagnostics.diagnostics().iter().take(MAX_DIAGNOSTICS) {
		let mut message = BoundedString::new(MAX_DIAGNOSTIC_BYTES);
		let result = match diagnostic {
			Diagnostic::Fatal(value) => write!(message, "Rune error: {value}"),
			Diagnostic::Warning(value) => write!(message, "Rune warning: {value}"),
			Diagnostic::RuntimeWarning(value) => write!(message, "Rune warning: {value}"),
			_ => write!(message, "Rune diagnostic"),
		};
		let _ = result;
		let message = message.finish();
		let separator = usize::from(!rendered.is_empty());
		if total
			.saturating_add(separator)
			.saturating_add(message.len())
			> MAX_DIAGNOSTIC_TOTAL_BYTES
		{
			break;
		}
		total += separator + message.len();
		rendered.push(message);
	}
	rendered
}

pub(crate) fn render_error(label: &str, error: &dyn fmt::Display) -> String {
	let mut message = BoundedString::new(MAX_DIAGNOSTIC_BYTES);
	let _ = write!(message, "{label}: {error}");
	message.finish()
}

struct BoundedString {
	value: String,
	limit: usize,
	truncated: bool,
}

impl BoundedString {
	fn new(limit: usize) -> Self {
		Self {
			value: String::new(),
			limit,
			truncated: false,
		}
	}

	fn finish(mut self) -> String {
		if self.truncated {
			let target = self.limit.saturating_sub('…'.len_utf8());
			let mut end = target.min(self.value.len());
			while end > 0 && !self.value.is_char_boundary(end) {
				end -= 1;
			}
			self.value.truncate(end);
			if self.value.len() + '…'.len_utf8() <= self.limit {
				self.value.push('…');
			}
		}
		self.value
	}
}

impl Write for BoundedString {
	fn write_str(&mut self, value: &str) -> fmt::Result {
		let remaining = self.limit.saturating_sub(self.value.len());
		if remaining == 0 {
			self.truncated |= !value.is_empty();
			return if value.is_empty() {
				Ok(())
			} else {
				Err(fmt::Error)
			};
		}
		let mut keep = remaining.min(value.len());
		while keep > 0 && !value.is_char_boundary(keep) {
			keep -= 1;
		}
		self.value.push_str(&value[..keep]);
		self.truncated |= keep < value.len();
		if keep < value.len() {
			Err(fmt::Error)
		} else {
			Ok(())
		}
	}
}
