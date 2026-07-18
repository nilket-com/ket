//! Session-effective keymap overlay and builtin-trie compatibility checks.

use super::keymap::{
	Category, Context, KEY_MODE_ORDER, KEYMAP, KeyDoc, KeyMode, KeyPattern, NormalContext,
	PromptKind, SpecialKey, VisualContext, arm_target, key_docs_excluding,
};
use crate::core::RegistryRev;
use crate::engine::runtime_registry::{
	KeyStroke, MappingMode, RegistryError, RegistryOccupant, RuntimeMappingInfo,
	RuntimeMappingRegistration, RuntimeRegistries,
};
use crate::engine::{KeyCode, Modifiers};
use std::collections::{BTreeSet, HashSet};
use std::sync::LazyLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MappingLookup {
	None,
	Prefix,
	Terminal,
}

#[derive(Default)]
pub(super) struct EffectiveKeymap {
	revision: Option<RegistryRev>,
	terminals: BTreeSet<(MappingMode, Vec<KeyStroke>)>,
	prefixes: BTreeSet<(MappingMode, Vec<KeyStroke>)>,
}

impl EffectiveKeymap {
	pub(super) fn refresh(&mut self, registries: &RuntimeRegistries) {
		if self.revision == Some(registries.revision()) {
			return;
		}
		self.terminals.clear();
		self.prefixes.clear();
		for mapping in registries.mapping_infos() {
			for prefix_len in 1..mapping.sequence.len() {
				self.prefixes
					.insert((mapping.mode, mapping.sequence[..prefix_len].to_vec()));
			}
			self.terminals.insert((mapping.mode, mapping.sequence));
		}
		self.revision = Some(registries.revision());
	}

	pub(super) fn lookup(&self, mode: MappingMode, sequence: &[KeyStroke]) -> MappingLookup {
		let key = (mode, sequence.to_vec());
		if self.terminals.contains(&key) {
			MappingLookup::Terminal
		} else if self.prefixes.contains(&key) {
			MappingLookup::Prefix
		} else {
			MappingLookup::None
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveKeymapSnapshot {
	pub(crate) revision: RegistryRev,
	pub(crate) mappings: Vec<RuntimeMappingInfo>,
	pub(super) docs: Vec<KeyDoc>,
}

pub(crate) fn keymap_snapshot(registries: &RuntimeRegistries) -> EffectiveKeymapSnapshot {
	EffectiveKeymapSnapshot {
		revision: registries.revision(),
		mappings: registries.mapping_infos(),
		docs: key_docs_with_runtime(registries),
	}
}

pub(super) fn key_docs_with_runtime(registries: &RuntimeRegistries) -> Vec<KeyDoc> {
	let mappings = registries.mapping_infos();
	let mut excluded = HashSet::new();
	for mapping in &mappings {
		if is_concrete_builtin_terminal(mapping.mode, &mapping.sequence) {
			excluded.insert((key_mode(mapping.mode), format_sequence(&mapping.sequence)));
		}
	}
	let mut docs = key_docs_excluding(&excluded);
	for mode in KEY_MODE_ORDER {
		let mapping_mode = mapping_mode(*mode);
		for mapping in mappings
			.iter()
			.filter(|mapping| mapping.mode == mapping_mode)
		{
			docs.push(KeyDoc {
				mode: *mode,
				category: Category::Extensions,
				keys: format_sequence(&mapping.sequence),
				summary: mapping.summary.clone(),
				group_index: usize::MAX,
			});
		}
	}
	docs
}

pub(crate) fn validate_builtin_compatibility(
	owner: crate::core::RegistryOwnerId,
	mappings: &[RuntimeMappingRegistration],
) -> Result<(), RegistryError> {
	for mapping in mappings {
		for builtin in BUILTIN_TERMINALS
			.iter()
			.filter(|builtin| builtin.mode == mapping.mode)
		{
			let shared = mapping.sequence.len().min(builtin.patterns.len());
			if !(0..shared).all(|index| {
				builtin.patterns[index].matches(
					mapping.sequence[index].code,
					mapping.sequence[index].modifiers,
				)
			}) {
				continue;
			}
			if mapping.sequence.len() < builtin.patterns.len() {
				return Err(RegistryError::MappingPrefixConflict {
					mode: mapping.mode,
					prefix: mapping.sequence.clone(),
					prefix_occupant: RegistryOccupant::Owner(owner),
					sequence: builtin_witness(&builtin.patterns),
					sequence_occupant: RegistryOccupant::Builtin,
				});
			}
			if builtin.patterns.len() < mapping.sequence.len() {
				return Err(RegistryError::MappingPrefixConflict {
					mode: mapping.mode,
					prefix: mapping.sequence[..builtin.patterns.len()].to_vec(),
					prefix_occupant: RegistryOccupant::Builtin,
					sequence: mapping.sequence.clone(),
					sequence_occupant: RegistryOccupant::Owner(owner),
				});
			}
		}
	}
	Ok(())
}

pub(super) fn is_builtin_prefix(mode: MappingMode, sequence: &[KeyStroke]) -> bool {
	BUILTIN_TERMINALS.iter().any(|builtin| {
		builtin.mode == mode
			&& sequence.len() < builtin.patterns.len()
			&& sequence.iter().enumerate().all(|(index, stroke)| {
				builtin.patterns[index].matches(stroke.code, stroke.modifiers)
			})
	})
}

pub(super) fn normalize_stroke(code: KeyCode, mut modifiers: Modifiers) -> KeyStroke {
	if matches!(code, KeyCode::Char(_)) {
		modifiers.remove(Modifiers::SHIFT);
	}
	KeyStroke::new(code, modifiers)
}

pub(super) fn mapping_mode_for(mode: crate::engine::EditorMode, browser: bool) -> MappingMode {
	match mode {
		crate::engine::EditorMode::Normal if browser => MappingMode::FileBrowser,
		crate::engine::EditorMode::Normal => MappingMode::Normal,
		crate::engine::EditorMode::Visual { .. } => MappingMode::Visual,
		crate::engine::EditorMode::Insert => MappingMode::Insert,
		crate::engine::EditorMode::Command => MappingMode::CommandLine,
		crate::engine::EditorMode::Search => MappingMode::Search,
		crate::engine::EditorMode::Confirm => MappingMode::Confirm,
	}
}

pub(super) fn format_sequence(sequence: &[KeyStroke]) -> String {
	let mut out = String::new();
	for stroke in sequence {
		let (label, glues) = stroke_label(*stroke);
		if !out.is_empty() && !glues {
			out.push(' ');
		}
		out.push_str(&label);
	}
	out
}

fn stroke_label(stroke: KeyStroke) -> (String, bool) {
	let key = match stroke.code {
		KeyCode::Backspace => "Backspace".to_string(),
		KeyCode::Enter => "Enter".to_string(),
		KeyCode::Left => "Left".to_string(),
		KeyCode::Right => "Right".to_string(),
		KeyCode::Up => "Up".to_string(),
		KeyCode::Down => "Down".to_string(),
		KeyCode::Home => "Home".to_string(),
		KeyCode::End => "End".to_string(),
		KeyCode::PageUp => "PageUp".to_string(),
		KeyCode::PageDown => "PageDown".to_string(),
		KeyCode::Tab => "Tab".to_string(),
		KeyCode::Delete => "Delete".to_string(),
		KeyCode::Insert => "Insert".to_string(),
		KeyCode::Function(number) => format!("F{number}"),
		KeyCode::Char(ch) => ch.to_string(),
		KeyCode::Null => "Null".to_string(),
		KeyCode::Esc => "Esc".to_string(),
		KeyCode::Unsupported => "Unsupported".to_string(),
	};
	if stroke.modifiers.is_empty() {
		return (key, matches!(stroke.code, KeyCode::Char(_)));
	}
	let mut parts = Vec::new();
	for (modifier, label) in [
		(Modifiers::CONTROL, "Ctrl"),
		(Modifiers::ALT, "Alt"),
		(Modifiers::SHIFT, "Shift"),
		(Modifiers::SUPER, "Super"),
		(Modifiers::HYPER, "Hyper"),
		(Modifiers::META, "Meta"),
	] {
		if stroke.modifiers.contains(modifier) {
			parts.push(label);
		}
	}
	parts.push(&key);
	(parts.join("-"), false)
}

struct BuiltinTerminal {
	mode: MappingMode,
	patterns: Vec<KeyPattern>,
}

static BUILTIN_TERMINALS: LazyLock<Vec<BuiltinTerminal>> = LazyLock::new(|| {
	fn root(mode: MappingMode) -> Context {
		match mode {
			MappingMode::Normal => Context::Normal(NormalContext::Ready),
			MappingMode::Visual => Context::Visual(VisualContext::Ready),
			MappingMode::Insert => Context::Insert,
			MappingMode::CommandLine => Context::Prompt(PromptKind::Command),
			MappingMode::Search => Context::Prompt(PromptKind::Search),
			MappingMode::Confirm => Context::Confirm,
			MappingMode::FileBrowser => Context::Normal(NormalContext::FileBrowser),
		}
	}

	fn walk(
		mode: MappingMode,
		context: Context,
		path: &mut Vec<KeyPattern>,
		stack: &mut HashSet<Context>,
		terminals: &mut Vec<BuiltinTerminal>,
	) {
		if !stack.insert(context) {
			return;
		}
		for binding in KEYMAP
			.iter()
			.flat_map(|group| group.bindings)
			.filter(|binding| binding.context() == context)
		{
			path.push(binding.pattern());
			if let Some(target) = arm_target(binding) {
				walk(mode, target, path, stack, terminals);
			} else {
				terminals.push(BuiltinTerminal {
					mode,
					patterns: path.clone(),
				});
			}
			path.pop();
		}
		stack.remove(&context);
	}

	let mut terminals = Vec::new();
	for mode in [
		MappingMode::Normal,
		MappingMode::Visual,
		MappingMode::Insert,
		MappingMode::CommandLine,
		MappingMode::Search,
		MappingMode::Confirm,
		MappingMode::FileBrowser,
	] {
		walk(
			mode,
			root(mode),
			&mut Vec::new(),
			&mut HashSet::new(),
			&mut terminals,
		);
	}
	terminals
});

fn is_concrete_builtin_terminal(mode: MappingMode, sequence: &[KeyStroke]) -> bool {
	BUILTIN_TERMINALS.iter().any(|builtin| {
		builtin.mode == mode
			&& builtin.patterns.len() == sequence.len()
			&& !builtin
				.patterns
				.iter()
				.any(|pattern| matches!(pattern, KeyPattern::AnyChar))
			&& builtin
				.patterns
				.iter()
				.zip(sequence)
				.all(|(pattern, stroke)| pattern.matches(stroke.code, stroke.modifiers))
	})
}

fn builtin_witness(patterns: &[KeyPattern]) -> Vec<KeyStroke> {
	patterns
		.iter()
		.map(|pattern| match *pattern {
			KeyPattern::Ctrl(ch) => KeyStroke::new(KeyCode::Char(ch), Modifiers::CONTROL),
			KeyPattern::Alt(ch) => KeyStroke::new(KeyCode::Char(ch), Modifiers::ALT),
			KeyPattern::ModifiedChar { ch, modifiers } => {
				KeyStroke::new(KeyCode::Char(ch), modifiers)
			}
			KeyPattern::Special(key) => KeyStroke::new(special_code(key), Modifiers::NONE),
			KeyPattern::ModifiedSpecial { key, modifiers } => {
				KeyStroke::new(special_code(key), modifiers)
			}
			KeyPattern::Char(ch) => KeyStroke::new(KeyCode::Char(ch), Modifiers::NONE),
			KeyPattern::AnyChar => KeyStroke::new(KeyCode::Char('*'), Modifiers::NONE),
		})
		.collect()
}

fn special_code(key: SpecialKey) -> KeyCode {
	match key {
		SpecialKey::Esc => KeyCode::Esc,
		SpecialKey::Enter => KeyCode::Enter,
		SpecialKey::Tab => KeyCode::Tab,
		SpecialKey::Backspace => KeyCode::Backspace,
		SpecialKey::Delete => KeyCode::Delete,
		SpecialKey::Up => KeyCode::Up,
		SpecialKey::Down => KeyCode::Down,
		SpecialKey::Left => KeyCode::Left,
		SpecialKey::Right => KeyCode::Right,
		SpecialKey::Home => KeyCode::Home,
		SpecialKey::End => KeyCode::End,
		SpecialKey::PageUp => KeyCode::PageUp,
		SpecialKey::PageDown => KeyCode::PageDown,
	}
}

fn mapping_mode(mode: KeyMode) -> MappingMode {
	match mode {
		KeyMode::Normal => MappingMode::Normal,
		KeyMode::Visual => MappingMode::Visual,
		KeyMode::Insert => MappingMode::Insert,
		KeyMode::CommandLine => MappingMode::CommandLine,
		KeyMode::Search => MappingMode::Search,
		KeyMode::Confirm => MappingMode::Confirm,
		KeyMode::FileBrowser => MappingMode::FileBrowser,
	}
}

fn key_mode(mode: MappingMode) -> KeyMode {
	match mode {
		MappingMode::Normal => KeyMode::Normal,
		MappingMode::Visual => KeyMode::Visual,
		MappingMode::Insert => KeyMode::Insert,
		MappingMode::CommandLine => KeyMode::CommandLine,
		MappingMode::Search => KeyMode::Search,
		MappingMode::Confirm => KeyMode::Confirm,
		MappingMode::FileBrowser => KeyMode::FileBrowser,
	}
}
