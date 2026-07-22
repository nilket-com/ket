use bitflags::bitflags;

/// A frontend-neutral input event resolved against engine-owned modal state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputEvent {
	Key { code: KeyCode, modifiers: Modifiers },
	Paste(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InputCompletionStatus {
	#[default]
	Idle,
	Pending,
}

/// Authoritative, frontend-facing projection of the engine's modal input state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputProjection {
	pub mode: crate::engine::EditorMode,
	/// Label supplied by the engine for a masked password prompt. It contains
	/// no secret material.
	pub prompt_label: Option<String>,
	/// The prompt buffer has been redacted to display-safe bullets.
	pub prompt_masked: bool,
	pub prompt_buffer: String,
	pub prompt_cursor: usize,
	pub pending_sequence: Option<String>,
	pub completion: InputCompletionStatus,
}

impl Default for InputProjection {
	fn default() -> Self {
		Self {
			mode: crate::engine::EditorMode::Normal,
			prompt_label: None,
			prompt_masked: false,
			prompt_buffer: String::new(),
			prompt_cursor: 0,
			pending_sequence: None,
			completion: InputCompletionStatus::Idle,
		}
	}
}

/// Portable key codes used by mappings and headless input clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyCode {
	Backspace,
	Enter,
	Left,
	Right,
	Up,
	Down,
	Home,
	End,
	PageUp,
	PageDown,
	Tab,
	Delete,
	Insert,
	Function(u8),
	Char(char),
	Null,
	Esc,
	Unsupported,
}

bitflags! {
	/// Exact modifier set attached to a portable key event.
	#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
	pub struct Modifiers: u8 {
		const NONE = 0;
		const SHIFT = 1 << 0;
		const CONTROL = 1 << 1;
		const ALT = 1 << 2;
		const SUPER = 1 << 3;
		const HYPER = 1 << 4;
		const META = 1 << 5;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn modifier_combinations_remain_exact_and_distinct() {
		assert_ne!(Modifiers::CONTROL, Modifiers::CONTROL | Modifiers::ALT);
		assert!((Modifiers::CONTROL | Modifiers::ALT).contains(Modifiers::CONTROL));
		assert!(!(Modifiers::CONTROL | Modifiers::ALT).contains(Modifiers::SHIFT));
	}
}
