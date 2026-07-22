//! End-to-end tests for the session-effective runtime option catalog.

use super::command_registry::{CommandCtx, DispatchResult};
use super::help::{render_markdown_with_runtime, resolve_topic_with_runtime};
use super::runtime_catalog::dispatch_ex_command;
use super::runtime_options::{
	EffectiveOptionKind, OptionSource, option_snapshot, parse_set_command,
};
use super::{InputContext, InputRuntime, KeyCode, Modifiers};
use crate::core::{DocLine, RegistryOwnerId};
use crate::engine::EditorMode;
use crate::engine::runtime_registry::{
	ActionError, ActionLease, DynamicActionExecutor, DynamicActionOutcome, DynamicInvocation,
	InvocationGate, InvocationSnapshot, MAX_OPTION_QUERY_DISPLAY_BYTES, OptionInvocation,
	OptionValue, RegistrationBatch, RuntimeOptionKind, RuntimeOptionRegistration,
	RuntimeRegistries, finalize_dynamic_action_outcome,
};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeAction(&'static str);

fn option(
	owner: RegistryOwnerId,
	name: &str,
	aliases: &[&str],
	kind: RuntimeOptionKind,
	usage: &str,
	completion_candidates: &[&str],
	action: &'static str,
) -> RuntimeOptionRegistration {
	RuntimeOptionRegistration {
		name: name.to_string(),
		aliases: aliases.iter().map(|alias| (*alias).to_string()).collect(),
		kind,
		usage: usage.to_string(),
		summary: format!("Configure {name}"),
		help: format!("Long help for the {name} extension option."),
		completion_candidates: completion_candidates
			.iter()
			.map(|candidate| (*candidate).to_string())
			.collect(),
		action: ActionLease::new(owner, FakeAction(action)),
	}
}

fn publish(
	registries: &mut RuntimeRegistries,
	owner: RegistryOwnerId,
	options: Vec<RuntimeOptionRegistration>,
) {
	registries
		.commit_owner_batch(RegistrationBatch {
			owner,
			commands: Vec::new(),
			mappings: Vec::new(),
			options,
			subscriptions: Vec::new(),
		})
		.expect("publish runtime options");
}

fn invocation(registries: &RuntimeRegistries, rest: &str) -> Result<InvocationSnapshot, String> {
	match parse_set_command(registries, rest)? {
		super::runtime_options::SetAction::Runtime(invocation) => Ok(invocation),
		super::runtime_options::SetAction::Builtin(_) => {
			Err("expected a runtime option invocation".to_string())
		}
	}
}

#[test]
fn typed_set_parsing_is_owned_and_runtime_options_never_emit_persistence_commands() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		vec![
			option(
				owner,
				"feature",
				&["feat"],
				RuntimeOptionKind::Bool,
				":set feature [on|off]",
				&[],
				"bool",
			),
			option(
				owner,
				"workers",
				&[],
				RuntimeOptionKind::Integer,
				":set workers COUNT",
				&[],
				"integer",
			),
			option(
				owner,
				"greeting",
				&[],
				RuntimeOptionKind::String {
					require_non_empty: true,
				},
				":set greeting TEXT",
				&[],
				"string",
			),
		],
	);

	for (rest, expected) in [
		("feature", OptionInvocation::Set(OptionValue::Bool(true))),
		("nofeature", OptionInvocation::Set(OptionValue::Bool(false))),
		("nofeat", OptionInvocation::Set(OptionValue::Bool(false))),
		("feature on", OptionInvocation::Set(OptionValue::Bool(true))),
		(
			"feature true",
			OptionInvocation::Set(OptionValue::Bool(true)),
		),
		(
			"feature yes",
			OptionInvocation::Set(OptionValue::Bool(true)),
		),
		("feat off", OptionInvocation::Set(OptionValue::Bool(false))),
		(
			"feat false",
			OptionInvocation::Set(OptionValue::Bool(false)),
		),
		("feat no", OptionInvocation::Set(OptionValue::Bool(false))),
		(
			"workers -9223372036854775808",
			OptionInvocation::Set(OptionValue::Integer(i64::MIN)),
		),
		(
			"greeting hello sparse world",
			OptionInvocation::Set(OptionValue::String("hello sparse world".to_string())),
		),
		("feature?", OptionInvocation::Query),
	] {
		let invocation = invocation(&registries, rest).expect(rest);
		assert_eq!(
			invocation.invocation,
			DynamicInvocation::Option {
				canonical_name: rest
					.starts_with("worker")
					.then_some("workers")
					.or_else(|| rest.starts_with("greeting").then_some("greeting"))
					.unwrap_or("feature")
					.to_string(),
				operation: expected,
			}
		);
	}

	assert_eq!(
		invocation(&registries, "workers 9223372036854775808").unwrap_err(),
		"Usage: :set workers COUNT"
	);
	assert_eq!(
		invocation(&registries, "greeting").unwrap_err(),
		"Usage: :set greeting TEXT"
	);
	assert_eq!(
		invocation(&registries, "nofeature on").unwrap_err(),
		"Usage: :set feature [on|off]"
	);
	assert!(
		parse_set_command(&registries, "featuer")
			.unwrap_err()
			.contains("did you mean 'feature'")
	);

	// Persistence in Baryon is represented exclusively by EditorCommand
	// variants. Runtime :set resolves to a capability invocation and emits no
	// command, so it cannot reach a settings file or extend its schema.
	let mut commands = Vec::new();
	let mut dynamic_invocations = Vec::new();
	let mut status = None;
	let mut ctx = CommandCtx {
		commands: &mut commands,
		dynamic_invocations: &mut dynamic_invocations,
		runtime_registries: Some(&registries),
		status: &mut status,
		cursor_line: DocLine::ZERO,
		visual_range: None,
		quit: false,
	};
	assert_eq!(
		dispatch_ex_command(&mut ctx, &registries, "set feature"),
		DispatchResult::Dispatched
	);
	assert!(commands.is_empty());
	assert_eq!(dynamic_invocations.len(), 1);
	assert_eq!(status, None);
}

struct FakeExecutor {
	live: HashSet<RegistryOwnerId>,
	display: Option<String>,
}

impl DynamicActionExecutor<()> for FakeExecutor {
	type Output = DynamicActionOutcome;

	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
		self.live.contains(&owner)
	}

	fn invoke(
		&mut self,
		_context: &mut (),
		invocation: InvocationSnapshot,
	) -> Result<Self::Output, ActionError> {
		assert_eq!(invocation.action::<FakeAction>(), Some(&FakeAction("bool")));
		Ok(DynamicActionOutcome {
			option_query_display: self.display.take(),
			..DynamicActionOutcome::default()
		})
	}
}

#[test]
fn query_results_are_required_and_bounded_at_the_action_seam() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		vec![option(
			owner,
			"feature",
			&[],
			RuntimeOptionKind::Bool,
			":set feature [on|off]",
			&[],
			"bool",
		)],
	);
	let gate = InvocationGate::default();

	let mut exact = FakeExecutor {
		live: HashSet::from([owner]),
		display: Some("x".repeat(MAX_OPTION_QUERY_DISPLAY_BYTES)),
	};
	let output = gate
		.invoke_dynamic(
			&mut exact,
			&mut (),
			invocation(&registries, "feature?").expect("query"),
		)
		.expect("exact query display limit");
	assert_eq!(
		output.status_message.as_deref().map(str::len),
		Some(MAX_OPTION_QUERY_DISPLAY_BYTES)
	);
	assert!(output.needs_render);

	let mut too_large = FakeExecutor {
		live: HashSet::from([owner]),
		display: Some("x".repeat(MAX_OPTION_QUERY_DISPLAY_BYTES + 1)),
	};
	assert!(matches!(
		gate.invoke_dynamic(
			&mut too_large,
			&mut (),
			invocation(&registries, "feature?").expect("query")
		),
		Err(ActionError::QueryDisplayTooLarge { .. })
	));
	assert_eq!(
		finalize_dynamic_action_outcome(true, DynamicActionOutcome::default()),
		Err(ActionError::MissingQueryDisplay)
	);
	assert_eq!(
		finalize_dynamic_action_outcome(
			false,
			DynamicActionOutcome {
				option_query_display: Some("not a query".to_string()),
				..DynamicActionOutcome::default()
			}
		),
		Err(ActionError::UnexpectedQueryDisplay)
	);
}

#[test]
fn completion_help_and_introspection_share_the_effective_option_catalog() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		vec![
			option(
				owner,
				"extension_palette",
				&["epal"],
				RuntimeOptionKind::String {
					require_non_empty: true,
				},
				":set extension_palette NAME",
				&["black", "blue"],
				"palette",
			),
			option(
				owner,
				"extension_toggle",
				&["etog"],
				RuntimeOptionKind::Bool,
				":set extension_toggle [on|off]",
				&[],
				"toggle",
			),
		],
	);

	let mut input = InputRuntime::new();
	input.apply_mode_override(EditorMode::Command);
	input.command_buffer = "set extension_p".to_string();
	input.command_cursor = input.command_buffer.len();
	let dispatch = input.dispatch_key_with_registries(
		InputContext::default(),
		KeyCode::Tab,
		Modifiers::NONE,
		&registries,
	);
	assert!(dispatch.dynamic_invocations.is_empty());
	assert_eq!(input.projection().prompt_buffer, "set extension_palette");

	input.command_buffer = "set epal blu".to_string();
	input.command_cursor = input.command_buffer.len();
	input.dispatch_key_with_registries(
		InputContext::default(),
		KeyCode::Tab,
		Modifiers::NONE,
		&registries,
	);
	assert_eq!(input.projection().prompt_buffer, "set epal blue");

	let topic = resolve_topic_with_runtime("epal?", &registries).expect("runtime option help");
	let markdown = render_markdown_with_runtime(&topic);
	assert!(markdown.contains("# extension_palette - Configure extension_palette"));
	assert!(markdown.contains("session only; configure it in `init.rn`"));
	assert!(markdown.contains("Long help for the extension_palette extension option."));
	let negated = resolve_topic_with_runtime("noetog", &registries).expect("negated bool help");
	assert!(render_markdown_with_runtime(&negated).contains("# extension_toggle"));
	let set_help = resolve_topic_with_runtime("set", &registries).expect("set help");
	let markdown = render_markdown_with_runtime(&set_help);
	assert!(markdown.contains("## Extensions"));
	assert!(markdown.contains("never written to a settings file"));
	assert!(markdown.contains("`:set extension_palette NAME`"));

	let snapshot = option_snapshot(&registries);
	let runtime = snapshot
		.options
		.iter()
		.filter(|option| matches!(option.source, OptionSource::Runtime(_)))
		.collect::<Vec<_>>();
	assert_eq!(
		runtime
			.iter()
			.map(|option| option.name.as_str())
			.collect::<Vec<_>>(),
		vec!["extension_palette", "extension_toggle"]
	);
	assert_eq!(
		runtime[0].kind,
		EffectiveOptionKind::String {
			require_non_empty: true
		}
	);
	assert!(runtime.iter().all(|option| !option.persistent));
	assert!(snapshot.options.iter().any(|option| {
		matches!(option.source, OptionSource::Builtin) && option.name == "wrap" && option.persistent
	}));
}

#[test]
fn replacement_and_removal_change_the_next_resolution_without_staling_a_snapshot() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	let definition = |action: &'static str| {
		option(
			owner,
			"extension_mode",
			&[],
			RuntimeOptionKind::Bool,
			":set extension_mode [on|off]",
			&[],
			action,
		)
	};
	publish(&mut registries, owner, vec![definition("v1")]);
	let old = invocation(&registries, "extension_mode").expect("v1 snapshot");
	publish(&mut registries, owner, vec![definition("v2")]);
	assert_eq!(old.action::<FakeAction>(), Some(&FakeAction("v1")));
	assert_eq!(
		invocation(&registries, "extension_mode")
			.expect("v2 snapshot")
			.action::<FakeAction>(),
		Some(&FakeAction("v2"))
	);
	publish(&mut registries, owner, Vec::new());
	assert!(parse_set_command(&registries, "extension_mode").is_err());
}
