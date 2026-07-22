use super::command_registry::{ArgCompletion, CommandCtx, DispatchResult};
use super::runtime_catalog::{
	CommandSource, command_snapshot, completion_command_names, dispatch_ex_command, suggest,
};
use super::{InputContext, InputRuntime, KeyCode, Modifiers};
use crate::core::{RegistryOwnerId, RegistryRev};
use crate::engine::EditorMode;
use crate::engine::input::help::{render_markdown_with_runtime, resolve_topic_with_runtime};
use crate::engine::runtime_registry::{
	ActionError, ActionLease, CommandArgRule, DynamicActionExecutor, DynamicInvocation,
	InvocationGate, InvocationSnapshot, RegistrationBatch, RuntimeCommandRegistration,
	RuntimeCompletion, RuntimeRegistries,
};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeAction(&'static str);

fn command(
	owner: RegistryOwnerId,
	name: &str,
	aliases: &[&str],
	args: CommandArgRule,
	completion: RuntimeCompletion,
	action: &'static str,
) -> RuntimeCommandRegistration {
	RuntimeCommandRegistration {
		name: name.to_string(),
		aliases: aliases.iter().map(|alias| (*alias).to_string()).collect(),
		bang: true,
		args,
		usage: format!(":{name} [ARG]"),
		summary: format!("Run {name}"),
		help: format!("Help for the {name} extension command."),
		completion,
		action: ActionLease::new(owner, FakeAction(action)),
	}
}

fn publish(
	registries: &mut RuntimeRegistries,
	owner: RegistryOwnerId,
	commands: Vec<RuntimeCommandRegistration>,
) {
	registries
		.commit_owner_batch(RegistrationBatch {
			owner,
			commands,
			mappings: Vec::new(),
			options: Vec::new(),
			subscriptions: Vec::new(),
		})
		.expect("publish runtime commands");
}

fn dispatch(
	registries: &RuntimeRegistries,
	buffer: &str,
) -> (DispatchResult, Vec<InvocationSnapshot>, Option<String>) {
	let mut commands = Vec::new();
	let mut dynamic_invocations = Vec::new();
	let mut status = None;
	let mut ctx = CommandCtx {
		commands: &mut commands,
		dynamic_invocations: &mut dynamic_invocations,
		runtime_registries: Some(registries),
		status: &mut status,
		cursor_line: crate::core::DocLine::ZERO,
		visual_range: None,
		quit: false,
	};
	let result = dispatch_ex_command(&mut ctx, registries, buffer);
	assert!(
		commands.is_empty(),
		"runtime exact command emitted a builtin action"
	);
	(result, dynamic_invocations, status)
}

struct FakeExecutor {
	live: HashSet<RegistryOwnerId>,
	calls: Vec<(FakeAction, DynamicInvocation)>,
}

impl DynamicActionExecutor<()> for FakeExecutor {
	type Output = (FakeAction, DynamicInvocation);

	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
		self.live.contains(&owner)
	}

	fn invoke(
		&mut self,
		_context: &mut (),
		invocation: InvocationSnapshot,
	) -> Result<Self::Output, ActionError> {
		let action = invocation
			.action::<FakeAction>()
			.cloned()
			.ok_or_else(|| ActionError::Failed("unexpected fake action".to_string()))?;
		let pair = (action, invocation.invocation);
		self.calls.push(pair.clone());
		Ok(pair)
	}
}

#[test]
fn runtime_exact_command_dispatches_owned_args_and_bang_through_fake_executor() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		vec![command(
			owner,
			"FuzzyFind",
			&["FF"],
			CommandArgRule::Required,
			RuntimeCompletion::Path,
			"fuzzy-v1",
		)],
	);

	let mut input = InputRuntime::new();
	input.apply_mode_override(EditorMode::Command);
	input.command_buffer = "FF!  src/main.rs  ".to_string();
	input.command_cursor = input.command_buffer.len();
	let mut dispatch = input.dispatch_key_with_registries(
		InputContext::default(),
		KeyCode::Enter,
		Modifiers::NONE,
		&registries,
	);
	assert_eq!(dispatch.status_message, None);
	assert_eq!(dispatch.dynamic_invocations.len(), 1);
	assert!(matches!(
		dispatch.commands.as_slice(),
		[crate::engine::EditorCommand::ClearVisualSelection]
	));
	assert_eq!(
		dispatch.dynamic_invocations[0].registry_revision,
		RegistryRev::new(1)
	);
	assert_eq!(
		dispatch.dynamic_invocations[0].invocation,
		DynamicInvocation::Command {
			canonical_name: "FuzzyFind".to_string(),
			spelling: "FF".to_string(),
			args: "src/main.rs".to_string(),
			bang: true,
		}
	);

	let gate = InvocationGate::default();
	let mut executor = FakeExecutor {
		live: HashSet::from([owner]),
		calls: Vec::new(),
	};
	let output = gate
		.invoke(
			&mut executor,
			&mut (),
			dispatch.dynamic_invocations.remove(0),
		)
		.expect("invoke fake action");
	assert_eq!(output.0, FakeAction("fuzzy-v1"));
	assert_eq!(executor.calls, vec![output]);
}

#[test]
fn argument_failure_replacement_and_removal_follow_effective_resolution() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		vec![command(
			owner,
			"NeedsArg",
			&[],
			CommandArgRule::Required,
			RuntimeCompletion::None,
			"v1",
		)],
	);
	let (result, invocations, status) = dispatch(&registries, "NeedsArg");
	assert_eq!(result, DispatchResult::Dispatched);
	assert!(invocations.is_empty());
	assert_eq!(status.as_deref(), Some("Usage: :NeedsArg [ARG]"));

	publish(
		&mut registries,
		owner,
		vec![command(
			owner,
			"NeedsArg",
			&[],
			CommandArgRule::Optional,
			RuntimeCompletion::None,
			"v2",
		)],
	);
	let (_, invocations, _) = dispatch(&registries, "NeedsArg");
	assert_eq!(
		invocations[0].action::<FakeAction>(),
		Some(&FakeAction("v2"))
	);

	publish(&mut registries, owner, Vec::new());
	assert_eq!(dispatch(&registries, "NeedsArg").0, DispatchResult::Unknown);

	let mut no_bang = command(
		owner,
		"NoBang",
		&[],
		CommandArgRule::None,
		RuntimeCompletion::None,
		"plain",
	);
	no_bang.bang = false;
	publish(&mut registries, owner, vec![no_bang]);
	assert_eq!(dispatch(&registries, "NoBang").1.len(), 1);
	assert_eq!(dispatch(&registries, "NoBang!").0, DispatchResult::Unknown);
}

#[test]
fn completion_suggestions_help_and_introspection_share_the_effective_catalog() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		vec![
			command(
				owner,
				"ZebraOpen",
				&["ZO"],
				CommandArgRule::Optional,
				RuntimeCompletion::Path,
				"zebra",
			),
			command(
				owner,
				"ChooseColor",
				&[],
				CommandArgRule::Required,
				RuntimeCompletion::Static(vec!["blue".to_string(), "black".to_string()]),
				"color",
			),
		],
	);

	let path_names = completion_command_names(&registries, ArgCompletion::Path);
	assert!(path_names.ends_with(&["ZebraOpen".to_string(), "ZO".to_string()]));
	assert_eq!(
		suggest(&registries, "ZebraOpne").as_deref(),
		Some("ZebraOpen")
	);

	let help = resolve_topic_with_runtime("ZO", &registries).expect("runtime help alias");
	let markdown = render_markdown_with_runtime(&help);
	assert!(markdown.contains("# :ZebraOpen - Run ZebraOpen"));
	assert!(markdown.contains("Help for the ZebraOpen extension command."));
	let commands_help = resolve_topic_with_runtime("commands", &registries).expect("commands help");
	let markdown = render_markdown_with_runtime(&commands_help);
	assert!(markdown.contains("## Extensions"));
	assert!(markdown.contains("`:ChooseColor [ARG]`"));
	assert!(markdown.contains("`:ZebraOpen [ARG]`"));

	let snapshot = command_snapshot(&registries);
	assert_eq!(snapshot.revision, RegistryRev::new(1));
	let runtime = snapshot
		.commands
		.iter()
		.filter(|command| matches!(command.source, CommandSource::Runtime(_)))
		.collect::<Vec<_>>();
	assert_eq!(
		runtime
			.iter()
			.map(|command| command.name.as_str())
			.collect::<Vec<_>>(),
		vec!["ChooseColor", "ZebraOpen"]
	);
	assert!(
		runtime
			.iter()
			.all(|command| command.category == "Extensions")
	);

	let mut commands = Vec::new();
	let mut dynamic_invocations = Vec::new();
	let mut status = None;
	let mut ctx = CommandCtx {
		commands: &mut commands,
		dynamic_invocations: &mut dynamic_invocations,
		runtime_registries: Some(&registries),
		status: &mut status,
		cursor_line: crate::core::DocLine::ZERO,
		visual_range: None,
		quit: false,
	};
	assert_eq!(
		dispatch_ex_command(&mut ctx, &registries, "help ZO"),
		DispatchResult::Dispatched
	);
	assert!(dynamic_invocations.is_empty());
	assert_eq!(status, None);
	assert!(matches!(
		commands.as_slice(),
		[crate::engine::EditorCommand::ShowMarkdownDocument { markdown, .. }]
			if markdown.contains("# :ZebraOpen - Run ZebraOpen")
	));
}

#[test]
fn static_completion_runs_through_the_engine_owned_prompt() {
	let mut registries = RuntimeRegistries::new();
	let owner = registries.allocate_owner().expect("owner");
	publish(
		&mut registries,
		owner,
		vec![command(
			owner,
			"ChooseColor",
			&[],
			CommandArgRule::Required,
			RuntimeCompletion::Static(vec!["blue".to_string(), "black".to_string()]),
			"color",
		)],
	);
	let mut input = InputRuntime::new();
	input.apply_mode_override(EditorMode::Command);
	input.command_buffer = "ChooseColor blu".to_string();
	input.command_cursor = input.command_buffer.len();
	let dispatch = input.dispatch_key_with_registries(
		InputContext::default(),
		KeyCode::Tab,
		Modifiers::NONE,
		&registries,
	);
	assert!(dispatch.dynamic_invocations.is_empty());
	assert_eq!(input.projection().prompt_buffer, "ChooseColor blue");
}
