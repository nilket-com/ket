use super::autocmd::RuneAutocmdRef;
use super::facade::{EditorHost, lock_services};
use crate::core::{BufferId, RegistryOwnerId};
use crate::engine::event_bus::EventKindSet;
use crate::engine::input::{KeyCode, Modifiers};
use crate::engine::runtime_registry::{
	ActionLease, CommandArgRule, KeyStroke, MAX_MAPPING_SEQUENCE, MAX_NAME_BYTES,
	MAX_TRANSACTION_ENTRIES, MAX_TRANSACTION_METADATA_BYTES, MappingMode, RegistrationBatch,
	RuntimeCommandRegistration, RuntimeCompletion, RuntimeMappingRegistration,
	RuntimeMappingTarget, RuntimeOptionKind, RuntimeOptionRegistration,
	RuntimeSubscriptionRegistration,
};
use rune::runtime::{Function, VmResult};

const COMMAND_SUMMARY: &str = "Rune command";
const OPTION_SUMMARY: &str = "Rune option";
const MAPPING_SUMMARY: &str = "Rune mapping";
const SOURCE_HELP: &str = "Registered by a Rune source.";
pub(super) const REGISTRATION_PHASE_ERROR: &str =
	"Rune registrations are only valid during source evaluation";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuneCallbackRef {
	pub(crate) owner: RegistryOwnerId,
	pub(crate) callback_id: usize,
}

enum StagedRegistration {
	Command {
		name: String,
		callback_id: usize,
	},
	Option {
		name: String,
		kind: RuntimeOptionKind,
		callback_id: usize,
	},
	Mapping {
		mode: MappingMode,
		sequence: Vec<KeyStroke>,
		callback_id: usize,
	},
	Autocmd {
		kinds: EventKindSet,
		initialize_id: usize,
		event_id: usize,
		buffer_filter: Option<BufferId>,
	},
}

pub(crate) struct SourceStaging {
	owner: RegistryOwnerId,
	entries: Vec<StagedRegistration>,
	callbacks: Vec<Function>,
	metadata_bytes: usize,
}

impl SourceStaging {
	pub(crate) fn new(owner: RegistryOwnerId) -> Self {
		Self {
			owner,
			entries: Vec::new(),
			callbacks: Vec::new(),
			metadata_bytes: 0,
		}
	}

	fn push_command(&mut self, name: &str, callback: Function) -> Result<(), String> {
		self.preflight_name(name)?;
		let metadata_bytes = name
			.len()
			.checked_mul(2)
			.and_then(|bytes| bytes.checked_add(": [ARGS]".len()))
			.and_then(|bytes| bytes.checked_add(COMMAND_SUMMARY.len()))
			.and_then(|bytes| bytes.checked_add(SOURCE_HELP.len()))
			.ok_or_else(|| "Rune command metadata overflowed".to_string())?;
		self.preflight(metadata_bytes)?;
		let callback_id = self.callbacks.len();
		self.callbacks.push(callback);
		self.entries.push(StagedRegistration::Command {
			name: name.to_string(),
			callback_id,
		});
		self.metadata_bytes += metadata_bytes;
		Ok(())
	}

	fn push_option(
		&mut self,
		name: &str,
		kind: RuntimeOptionKind,
		callback: Function,
	) -> Result<(), String> {
		self.preflight_name(name)?;
		let metadata_bytes = name
			.len()
			.checked_mul(2)
			.and_then(|bytes| bytes.checked_add(":set ".len()))
			.and_then(|bytes| bytes.checked_add(OPTION_SUMMARY.len()))
			.and_then(|bytes| bytes.checked_add(SOURCE_HELP.len()))
			.ok_or_else(|| "Rune option metadata overflowed".to_string())?;
		self.preflight(metadata_bytes)?;
		let callback_id = self.callbacks.len();
		self.callbacks.push(callback);
		self.entries.push(StagedRegistration::Option {
			name: name.to_string(),
			kind,
			callback_id,
		});
		self.metadata_bytes += metadata_bytes;
		Ok(())
	}

	fn push_mapping(
		&mut self,
		mode: MappingMode,
		sequence: &str,
		callback: Function,
	) -> Result<(), String> {
		let (_, sequence_bytes) = preflight_mapping_sequence(sequence)?;
		let metadata_bytes = sequence_bytes
			.checked_add(MAPPING_SUMMARY.len())
			.ok_or_else(|| "Rune mapping metadata overflowed".to_string())?;
		self.preflight(metadata_bytes)?;
		let sequence = sequence
			.chars()
			.map(|character| KeyStroke::new(KeyCode::Char(character), Modifiers::NONE))
			.collect();
		let callback_id = self.callbacks.len();
		self.callbacks.push(callback);
		self.entries.push(StagedRegistration::Mapping {
			mode,
			sequence,
			callback_id,
		});
		self.metadata_bytes += metadata_bytes;
		Ok(())
	}

	pub(super) fn push_autocmd(
		&mut self,
		kinds: EventKindSet,
		initialize: Function,
		event: Function,
		buffer_filter: Option<BufferId>,
	) -> Result<(), String> {
		self.preflight(0)?;
		let initialize_id = self.callbacks.len();
		let event_id = initialize_id
			.checked_add(1)
			.ok_or_else(|| "Rune callback table overflowed".to_string())?;
		self.callbacks.push(initialize);
		self.callbacks.push(event);
		self.entries.push(StagedRegistration::Autocmd {
			kinds,
			initialize_id,
			event_id,
			buffer_filter,
		});
		Ok(())
	}

	fn preflight(&self, metadata_bytes: usize) -> Result<(), String> {
		if self.entries.len() >= MAX_TRANSACTION_ENTRIES {
			return Err(format!(
				"Rune source exceeds the {MAX_TRANSACTION_ENTRIES} registration limit"
			));
		}
		let total = self
			.metadata_bytes
			.checked_add(metadata_bytes)
			.ok_or_else(|| "Rune registration metadata overflowed".to_string())?;
		if total > MAX_TRANSACTION_METADATA_BYTES {
			return Err(format!(
				"Rune source exceeds the {MAX_TRANSACTION_METADATA_BYTES} metadata-byte limit"
			));
		}
		Ok(())
	}

	fn preflight_name(&self, name: &str) -> Result<(), String> {
		if name.len() > MAX_NAME_BYTES {
			return Err(format!(
				"Rune registration name has {} bytes; the limit is {MAX_NAME_BYTES}",
				name.len()
			));
		}
		Ok(())
	}

	pub(crate) fn into_batch(self) -> (RegistrationBatch, Vec<Function>) {
		let mut batch = RegistrationBatch::new(self.owner);
		for entry in self.entries {
			match entry {
				StagedRegistration::Command { name, callback_id } => {
					batch.commands.push(RuntimeCommandRegistration {
						usage: format!(":{name} [ARGS]"),
						summary: COMMAND_SUMMARY.to_string(),
						help: SOURCE_HELP.to_string(),
						name,
						aliases: Vec::new(),
						bang: true,
						args: CommandArgRule::Optional,
						completion: RuntimeCompletion::None,
						action: ActionLease::new(
							self.owner,
							RuneCallbackRef {
								owner: self.owner,
								callback_id,
							},
						),
					});
				}
				StagedRegistration::Option {
					name,
					kind,
					callback_id,
				} => batch.options.push(RuntimeOptionRegistration {
					usage: format!(":set {name}"),
					summary: OPTION_SUMMARY.to_string(),
					help: SOURCE_HELP.to_string(),
					name,
					aliases: Vec::new(),
					kind,
					completion_candidates: Vec::new(),
					action: ActionLease::new(
						self.owner,
						RuneCallbackRef {
							owner: self.owner,
							callback_id,
						},
					),
				}),
				StagedRegistration::Mapping {
					mode,
					sequence,
					callback_id,
				} => batch.mappings.push(RuntimeMappingRegistration {
					mode,
					sequence,
					summary: MAPPING_SUMMARY.to_string(),
					target: RuntimeMappingTarget::Action(ActionLease::new(
						self.owner,
						RuneCallbackRef {
							owner: self.owner,
							callback_id,
						},
					)),
				}),
				StagedRegistration::Autocmd {
					kinds,
					initialize_id,
					event_id,
					buffer_filter,
				} => batch.subscriptions.push(RuntimeSubscriptionRegistration {
					kinds,
					action: ActionLease::new(
						self.owner,
						RuneAutocmdRef {
							initialize: RuneCallbackRef {
								owner: self.owner,
								callback_id: initialize_id,
							},
							event: RuneCallbackRef {
								owner: self.owner,
								callback_id: event_id,
							},
							buffer_filter,
						},
					),
				}),
			}
		}
		(batch, self.callbacks)
	}
}

fn preflight_mapping_sequence(sequence: &str) -> Result<(usize, usize), String> {
	let sequence_len = sequence.chars().take(MAX_MAPPING_SEQUENCE + 1).count();
	if sequence_len == 0 {
		return Err("Rune mapping sequence cannot be empty".to_string());
	}
	if sequence_len > MAX_MAPPING_SEQUENCE {
		return Err(format!(
			"Rune mapping has {sequence_len} strokes; the limit is {MAX_MAPPING_SEQUENCE}"
		));
	}
	let bytes = sequence_len
		.checked_mul(std::mem::size_of::<KeyStroke>())
		.ok_or_else(|| "Rune mapping metadata overflowed".to_string())?;
	Ok((sequence_len, bytes))
}

pub(crate) fn register_command(
	editor: &mut EditorHost,
	name: &str,
	callback: Function,
) -> VmResult<()> {
	if let Err(error) = lock_services(&editor.services).budget.charge() {
		return VmResult::panic(error);
	}
	stage(editor, |staging| staging.push_command(name, callback))
}

pub(crate) fn register_option(
	editor: &mut EditorHost,
	name: &str,
	kind: &str,
	callback: Function,
) -> VmResult<()> {
	if let Err(error) = lock_services(&editor.services).budget.charge() {
		return VmResult::panic(error);
	}
	let kind = match kind {
		"bool" => RuntimeOptionKind::Bool,
		"integer" => RuntimeOptionKind::Integer,
		"string" => RuntimeOptionKind::String {
			require_non_empty: false,
		},
		"nonempty-string" => RuntimeOptionKind::String {
			require_non_empty: true,
		},
		_ => {
			return VmResult::panic(
				"Rune option kind must be bool, integer, string, or nonempty-string",
			);
		}
	};
	stage(editor, |staging| staging.push_option(name, kind, callback))
}

pub(crate) fn register_mapping(
	editor: &mut EditorHost,
	mode: &str,
	sequence: &str,
	callback: Function,
) -> VmResult<()> {
	if let Err(error) = lock_services(&editor.services).budget.charge() {
		return VmResult::panic(error);
	}
	let mode = match mode {
		"normal" => MappingMode::Normal,
		"visual" => MappingMode::Visual,
		"insert" => MappingMode::Insert,
		"command" => MappingMode::CommandLine,
		"search" => MappingMode::Search,
		"confirm" => MappingMode::Confirm,
		"browser" => MappingMode::FileBrowser,
		_ => return VmResult::panic("Unknown Rune mapping mode"),
	};
	stage(editor, |staging| {
		staging.push_mapping(mode, sequence, callback)
	})
}

pub(super) fn stage(
	editor: &mut EditorHost,
	operation: impl FnOnce(&mut SourceStaging) -> Result<(), String>,
) -> VmResult<()> {
	let Some(staging) = editor.staging.as_mut() else {
		return VmResult::panic(REGISTRATION_PHASE_ERROR);
	};
	match operation(staging) {
		Ok(()) => VmResult::Ok(()),
		Err(error) => VmResult::panic(error),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn staging_caps_reject_before_owned_metadata_is_built() {
		let mut staging = SourceStaging::new(RegistryOwnerId::new(1));
		assert!(staging.preflight_name(&"n".repeat(MAX_NAME_BYTES)).is_ok());
		assert!(
			staging
				.preflight_name(&"n".repeat(MAX_NAME_BYTES + 1))
				.is_err()
		);

		staging.metadata_bytes = MAX_TRANSACTION_METADATA_BYTES;
		assert!(staging.preflight(0).is_ok());
		assert!(staging.preflight(1).is_err());

		staging.entries = (0..MAX_TRANSACTION_ENTRIES)
			.map(|_| StagedRegistration::Command {
				name: String::new(),
				callback_id: 0,
			})
			.collect();
		assert!(staging.preflight(0).is_err());
	}

	#[test]
	fn mapping_length_preflight_is_bounded_without_collecting_the_input() {
		let exact = "x".repeat(MAX_MAPPING_SEQUENCE);
		let too_many = "x".repeat(MAX_MAPPING_SEQUENCE + 1);
		assert_eq!(
			preflight_mapping_sequence(&exact).unwrap().0,
			MAX_MAPPING_SEQUENCE
		);
		assert!(preflight_mapping_sequence(&too_many).is_err());
		assert!(preflight_mapping_sequence("").is_err());
	}
}
