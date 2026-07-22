use arboard::Clipboard;

pub struct ClipboardHandle {
	inner: Option<Clipboard>,
}

impl ClipboardHandle {
	pub fn new() -> Self {
		Self {
			inner: Clipboard::new().ok(),
		}
	}

	pub fn get_text(&mut self) -> Option<String> {
		self.inner
			.as_mut()?
			.get_text()
			.ok()
			.filter(|s| !s.is_empty())
	}

	pub fn set_text(&mut self, text: &str) {
		if let Some(cb) = self.inner.as_mut() {
			let _ = cb.set_text(text.to_owned());
		}
	}
}

impl Default for ClipboardHandle {
	fn default() -> Self {
		Self::new()
	}
}
