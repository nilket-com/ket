use super::help::{render_markdown_with_runtime, resolve_topic_with_runtime};
use super::keymap::{Category, KeyMode};
use super::runtime_keymap::keymap_snapshot;
use super::{InputContext, InputRuntime, KeyCode, Modifiers};
use crate::core::{DocByte, RegistryOwnerId, RegistryRev};
use crate::engine::runtime_registry::{
	ActionError, ActionLease, CommandArgRule, DynamicActionExecutor, DynamicInvocation,
	InvocationGate, InvocationSnapshot, KeyStroke, MappingMode, RegistrationBatch, RegistryError,
	RegistryOccupant, RuntimeCommandRegistration, RuntimeCompletion, RuntimeMappingRegistration,
	RuntimeMappingTarget, RuntimeMappingTargetKind, RuntimeRegistries,
};
use crate::engine::{EditorCommand, EditorMode, MoveDirection, VisualKind};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeAction(&'static str);

struct FakeExecutor {
	live: RegistryOwnerId,
}

impl DynamicActionExecutor<()> for FakeExecutor {
	type Output = FakeAction;

	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
		self.live == owner
	}

	fn invoke(
		&mut self,
		_context: &mut (),
		invocation: InvocationSnapshot,
	) -> Result<Self::Output, ActionError> {
		invocation
			.action::<FakeAction>()
			.cloned()
			.ok_or_else(|| ActionError::Failed("unexpected mapping action".to_string()))
	}
}

fn stroke(ch: char) -> KeyStroke {
	KeyStroke::new(KeyCode::Char(ch), Modifiers::NONE)
}

fn action_mapping(
	owner: RegistryOwnerId,
	mode: MappingMode,
	sequence: &[char],
	summary: &str,
	action: &'static str,
) -> RuntimeMappingRegistration {
	RuntimeMappingRegistration {
		mode,
		sequence: sequence.iter().copied().map(stroke).collect(),
		summary: summary.to_string(),
		target: RuntimeMappingTarget::Action(ActionLease::new(owner, FakeAction(action))),
	}
}

fn command_mapping(
	mode: MappingMode,
	sequence: &[char],
	summary: &str,
	command: &str,
) -> RuntimeMappingRegistration {
	RuntimeMappingRegistration {
		mode,
		sequence: sequence.iter().copied().map(stroke).collect(),
		summary: summary.to_string(),
		target: RuntimeMappingTarget::Command(command.to_string()),
	}
}

fn command(owner: RegistryOwnerId, name: &str, action: &'static str) -> RuntimeCommandRegistration {
	RuntimeCommandRegistration {
		name: name.to_string(),
		aliases: Vec::new(),
		bang: false,
		args: CommandArgRule::Required,
		usage: format!(":{name} ARG"),
		summary: format!("Run {name}"),
		help: format!("Help for {name}"),
		completion: RuntimeCompletion::None,
		action: ActionLease::new(owner, FakeAction(action)),
	}
}

fn publish(
	registries: &mut RuntimeRegistries,
	owner: RegistryOwnerId,
	commands: Vec<RuntimeCommandRegistration>,
	mappings: Vec<RuntimeMappingRegistration>,
) -> Result<RegistryRev, RegistryError> {
	registries.commit_owner_batch(RegistrationBatch {
		owner,
		commands,
		mappings,
		options: Vec::new(),
		subscriptions: Vec::new(),
	})
}

fn key(input: &mut InputRuntime, registries: &RuntimeRegistries, ch: char) -> super::InputDispatch {
	input.dispatch_key_with_registries(
		InputContext::default(),
		KeyCode::Char(ch),
		Modifiers::NONE,
		registries,
	)
}

#[test]
fn exact_layers_override_builtin_dispatch_and_docs_then_reveal_the_default() {
	let mut registries = RuntimeRegistries::new();
	let low = registries.allocate_owner().expect("low owner");
	let high = registries.allocate_owner().expect("high owner");
	publish(
		&mut registries,
		low,
		Vec::new(),
		vec![action_mapping(
			low,
			MappingMode::Normal,
			&['j'],
			"Low-priority down override",
			"low",
		)],
	)
	.expect("low layer");
	publish(
		&mut registries,
		high,
		Vec::new(),
		vec![action_mapping(
			high,
			MappingMode::Normal,
			&['j'],
			"High-priority down override",
			"high",
		)],
	)
	.expect("high layer");

	let mut input = InputRuntime::new();
	let mut dispatch = key(&mut input, &registries, 'j');
	assert!(dispatch.commands.is_empty());
	assert_eq!(dispatch.dynamic_invocations.len(), 1);
	assert_eq!(
		dispatch.dynamic_invocations[0].action::<FakeAction>(),
		Some(&FakeAction("high"))
	);
	assert_eq!(
		dispatch.dynamic_invocations[0].invocation,
		DynamicInvocation::Mapping {
			mode: MappingMode::Normal,
			sequence: vec![stroke('j')],
		}
	);
	let invoked = InvocationGate::default()
		.invoke(
			&mut FakeExecutor { live: high },
			&mut (),
			dispatch.dynamic_invocations.remove(0),
		)
		.expect("invoke effective mapping action");
	assert_eq!(invoked, FakeAction("high"));

	let snapshot = keymap_snapshot(&registries);
	assert_eq!(snapshot.revision, RegistryRev::new(2));
	assert_eq!(snapshot.mappings.len(), 1);
	assert_eq!(snapshot.mappings[0].owner, high);
	assert_eq!(
		snapshot.mappings[0].target,
		RuntimeMappingTargetKind::Action
	);
	assert!(snapshot.docs.iter().any(|doc| {
		doc.mode == KeyMode::Normal
			&& doc.category == Category::Extensions
			&& doc.keys == "j"
			&& doc.summary == "High-priority down override"
	}));
	assert!(!snapshot.docs.iter().any(|doc| {
		doc.mode == KeyMode::Normal
			&& doc.category == Category::Motion
			&& doc.keys.split(" / ").any(|keys| keys == "j")
	}));
	let help = resolve_topic_with_runtime("keys", &registries).expect("live keys help");
	let markdown = render_markdown_with_runtime(&help);
	assert!(markdown.contains("### Extensions"));
	assert!(markdown.contains("| `j` | High-priority down override |"));

	publish(&mut registries, high, Vec::new(), Vec::new()).expect("remove high layer");
	let dispatch = key(&mut input, &registries, 'j');
	assert_eq!(
		dispatch.dynamic_invocations[0].action::<FakeAction>(),
		Some(&FakeAction("low"))
	);
	publish(&mut registries, low, Vec::new(), Vec::new()).expect("remove low layer");
	let dispatch = key(&mut input, &registries, 'j');
	assert!(dispatch.dynamic_invocations.is_empty());
	assert!(matches!(
		dispatch.commands.as_slice(),
		[EditorCommand::MoveCursor(MoveDirection::Down)]
	));
	let restored = keymap_snapshot(&registries);
	assert!(restored.docs.iter().any(|doc| {
		doc.mode == KeyMode::Normal
			&& doc.category == Category::Motion
			&& doc.keys.split(" / ").any(|keys| keys == "j")
	}));
}

#[test]
fn vacant_shared_and_wildcard_prefixes_use_one_overlay_pending_machine() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		Vec::new(),
		vec![
			action_mapping(
				owner,
				MappingMode::Normal,
				&['Q', 'x'],
				"Vacant two-key mapping",
				"vacant",
			),
			action_mapping(
				owner,
				MappingMode::Normal,
				&['g', 'x'],
				"Builtin-prefix mapping",
				"shared",
			),
			action_mapping(
				owner,
				MappingMode::Normal,
				&['g', 'q', 'x'],
				"Builtin-prefix then runtime-only mapping",
				"diverged",
			),
			action_mapping(
				owner,
				MappingMode::Normal,
				&['r', 'x'],
				"Wildcard refinement",
				"wildcard",
			),
		],
	)
	.expect("prefix-free mappings");

	let mut input = InputRuntime::new();
	let first = key(&mut input, &registries, 'Q');
	assert!(first.commands.is_empty() && first.dynamic_invocations.is_empty());
	assert_eq!(input.projection().pending_sequence.as_deref(), Some("Q"));
	let terminal = key(&mut input, &registries, 'x');
	assert_eq!(
		terminal.dynamic_invocations[0].action::<FakeAction>(),
		Some(&FakeAction("vacant"))
	);
	assert_eq!(input.projection().pending_sequence, None);

	key(&mut input, &registries, 'Q');
	let mismatch = key(&mut input, &registries, 'z');
	assert!(mismatch.commands.is_empty() && mismatch.dynamic_invocations.is_empty());
	assert_eq!(input.projection().pending_sequence, None);
	assert!(matches!(
		key(&mut input, &registries, 'x').commands.as_slice(),
		[EditorCommand::Delete]
	));

	key(&mut input, &registries, 'g');
	assert_eq!(input.projection().pending_sequence.as_deref(), Some("g"));
	assert_eq!(
		key(&mut input, &registries, 'x').dynamic_invocations[0].action::<FakeAction>(),
		Some(&FakeAction("shared"))
	);
	key(&mut input, &registries, 'g');
	assert!(matches!(
		key(&mut input, &registries, 'g').commands.as_slice(),
		[EditorCommand::MoveCursor(MoveDirection::Top)]
	));
	key(&mut input, &registries, 'g');
	key(&mut input, &registries, 'q');
	assert_eq!(input.projection().pending_sequence.as_deref(), Some("gq"));
	let mismatch = key(&mut input, &registries, 'z');
	assert!(mismatch.commands.is_empty() && mismatch.dynamic_invocations.is_empty());
	assert_eq!(input.projection().pending_sequence, None);

	key(&mut input, &registries, 'r');
	assert_eq!(
		key(&mut input, &registries, 'x').dynamic_invocations[0].action::<FakeAction>(),
		Some(&FakeAction("wildcard"))
	);
	key(&mut input, &registries, 'r');
	assert!(matches!(
		key(&mut input, &registries, 'y').commands.as_slice(),
		[EditorCommand::ReplaceChar('y')]
	));
	publish(&mut registries, owner, Vec::new(), Vec::new()).expect("remove mappings");
	key(&mut input, &registries, 'r');
	assert!(matches!(
		key(&mut input, &registries, 'x').commands.as_slice(),
		[EditorCommand::ReplaceChar('x')]
	));
}

#[test]
fn command_targets_resolve_after_the_prefix_and_report_removed_commands_cleanly() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	let mapping = command_mapping(
		MappingMode::Normal,
		&['Q', 'm'],
		"Invoke the current command definition",
		"Mapped value",
	);
	publish(
		&mut registries,
		owner,
		vec![command(owner, "Mapped", "v1")],
		vec![mapping.clone()],
	)
	.expect("v1");

	let mut input = InputRuntime::new();
	key(&mut input, &registries, 'Q');
	publish(
		&mut registries,
		owner,
		vec![command(owner, "Mapped", "v2")],
		vec![mapping.clone()],
	)
	.expect("replace while prefix pending");
	let dispatch = key(&mut input, &registries, 'm');
	assert_eq!(dispatch.dynamic_invocations.len(), 1);
	assert_eq!(
		dispatch.dynamic_invocations[0].action::<FakeAction>(),
		Some(&FakeAction("v2"))
	);
	assert_eq!(
		dispatch.dynamic_invocations[0].invocation,
		DynamicInvocation::Command {
			canonical_name: "Mapped".to_string(),
			spelling: "Mapped".to_string(),
			args: "value".to_string(),
			bang: false,
		}
	);

	publish(&mut registries, owner, Vec::new(), vec![mapping]).expect("remove command only");
	key(&mut input, &registries, 'Q');
	let removed = key(&mut input, &registries, 'm');
	assert!(removed.dynamic_invocations.is_empty());
	assert_eq!(
		removed.status_message.as_deref(),
		Some("Unknown command: Mapped value")
	);
}

#[test]
fn builtin_structure_and_modifier_failures_are_atomic_and_mode_dispatch_is_portable() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	let before = registries.summary();
	for sequence in [vec!['g'], vec!['j', 'x'], vec!['r', 'x', 'y']] {
		let error = publish(
			&mut registries,
			owner,
			Vec::new(),
			vec![action_mapping(
				owner,
				MappingMode::Normal,
				&sequence,
				"Invalid structural mapping",
				"invalid",
			)],
		)
		.expect_err("builtin terminal/prefix conflict");
		assert!(matches!(
			error,
			RegistryError::MappingPrefixConflict {
				prefix_occupant: RegistryOccupant::Builtin,
				..
			} | RegistryError::MappingPrefixConflict {
				sequence_occupant: RegistryOccupant::Builtin,
				..
			}
		));
		assert_eq!(registries.summary(), before);
	}

	let shifted = RuntimeMappingRegistration {
		mode: MappingMode::Normal,
		sequence: vec![KeyStroke::new(KeyCode::Char('X'), Modifiers::SHIFT)],
		summary: "Non-canonical shift".to_string(),
		target: RuntimeMappingTarget::Action(ActionLease::new(owner, FakeAction("shift"))),
	};
	assert_eq!(
		publish(&mut registries, owner, Vec::new(), vec![shifted]).unwrap_err(),
		RegistryError::NonCanonicalMappingModifiers { index: 0 }
	);
	assert_eq!(registries.summary(), before);

	let low = registries.allocate_owner().expect("low conflicting owner");
	let high = registries.allocate_owner().expect("high conflicting owner");
	publish(
		&mut registries,
		low,
		Vec::new(),
		vec![action_mapping(
			low,
			MappingMode::Normal,
			&['Q', 'x'],
			"Long runtime mapping",
			"long",
		)],
	)
	.expect("long runtime mapping");
	let before_conflict = registries.summary();
	assert_eq!(
		publish(
			&mut registries,
			high,
			Vec::new(),
			vec![action_mapping(
				high,
				MappingMode::Normal,
				&['Q'],
				"Conflicting runtime prefix",
				"prefix",
			)],
		)
		.unwrap_err(),
		RegistryError::MappingPrefixConflict {
			mode: MappingMode::Normal,
			prefix: vec![stroke('Q')],
			prefix_occupant: RegistryOccupant::Owner(high),
			sequence: vec![stroke('Q'), stroke('x')],
			sequence_occupant: RegistryOccupant::Owner(low),
		}
	);
	assert_eq!(registries.summary(), before_conflict);

	let portable_stroke = KeyStroke::new(KeyCode::Char('q'), Modifiers::CONTROL | Modifiers::ALT);
	let modes = [
		MappingMode::Normal,
		MappingMode::Visual,
		MappingMode::Insert,
		MappingMode::CommandLine,
		MappingMode::Search,
		MappingMode::Confirm,
		MappingMode::FileBrowser,
	];
	let portable = modes
		.into_iter()
		.map(|mode| RuntimeMappingRegistration {
			mode,
			sequence: vec![portable_stroke],
			summary: "Portable exact modifiers".to_string(),
			target: RuntimeMappingTarget::Action(ActionLease::new(owner, FakeAction("portable"))),
		})
		.collect();
	publish(&mut registries, owner, Vec::new(), portable).expect("portable mappings");
	let mut tui_path = InputRuntime::new();
	let mut headless_path = InputRuntime::new();
	for input in [&mut tui_path, &mut headless_path] {
		for (mode, editor_mode, context) in [
			(
				MappingMode::Normal,
				EditorMode::Normal,
				InputContext::default(),
			),
			(
				MappingMode::Visual,
				EditorMode::Visual {
					anchor: DocByte::ZERO,
					kind: VisualKind::Char,
				},
				InputContext::default(),
			),
			(
				MappingMode::Insert,
				EditorMode::Insert,
				InputContext::default(),
			),
			(
				MappingMode::CommandLine,
				EditorMode::Command,
				InputContext::default(),
			),
			(
				MappingMode::Search,
				EditorMode::Search,
				InputContext::default(),
			),
			(
				MappingMode::Confirm,
				EditorMode::Confirm,
				InputContext::default(),
			),
			(
				MappingMode::FileBrowser,
				EditorMode::Normal,
				InputContext {
					is_file_browser: true,
					..InputContext::default()
				},
			),
		] {
			input.apply_mode_override(editor_mode);
			let dispatch = input.dispatch_key_with_registries(
				context,
				portable_stroke.code,
				portable_stroke.modifiers,
				&registries,
			);
			assert!(dispatch.commands.is_empty());
			assert_eq!(
				dispatch.dynamic_invocations[0].action::<FakeAction>(),
				Some(&FakeAction("portable"))
			);
			assert_eq!(
				dispatch.dynamic_invocations[0].invocation,
				DynamicInvocation::Mapping {
					mode,
					sequence: vec![portable_stroke],
				}
			);
		}
	}
}
