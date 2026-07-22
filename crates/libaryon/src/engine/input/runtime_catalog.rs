//! Effective command catalog: immutable builtins plus the per-session overlay.

use super::command_registry::{self, ArgCompletion, ArgSpec, COMMANDS, CommandCtx, DispatchResult};
use crate::core::{RegistryOwnerId, RegistryRev};
use crate::engine::runtime_registry::{
	CommandArgRule, RuntimeCommandInfo, RuntimeCompletion, RuntimeRegistries,
};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandSource {
	Builtin,
	Runtime(RegistryOwnerId),
}

/// Owned, action-free catalog entry used by help and future introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveCommandInfo {
	pub(crate) source: CommandSource,
	pub(crate) precedence: Option<u64>,
	pub(crate) name: String,
	pub(crate) aliases: Vec<String>,
	pub(crate) bang: bool,
	pub(crate) args: CommandArgRule,
	pub(crate) usage: String,
	pub(crate) summary: String,
	pub(crate) help: String,
	pub(crate) category: String,
	pub(crate) completion: RuntimeCompletion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveCommandSnapshot {
	pub(crate) revision: RegistryRev,
	pub(crate) commands: Vec<EffectiveCommandInfo>,
}

pub(crate) fn dispatch_ex_command(
	ctx: &mut CommandCtx<'_>,
	registries: &RuntimeRegistries,
	buffer: &str,
) -> DispatchResult {
	if buffer.is_empty() {
		return DispatchResult::Dispatched;
	}
	let (token, args) = match buffer.find(char::is_whitespace) {
		Some(index) => (&buffer[..index], buffer[index..].trim()),
		None => (buffer, ""),
	};
	let (spelling, bang) = match token.strip_suffix('!') {
		Some(stripped) => (stripped, true),
		None => (token, false),
	};
	if let Some(command) = registries.command(spelling)
		&& (!bang || command.bang)
	{
		if !runtime_args_match(command.args, args) {
			ctx.error(format!("Usage: {}", command.usage));
			return DispatchResult::Dispatched;
		}
		let invocation = registries
			.command_invocation(spelling, args, bang)
			.expect("resolved runtime command must retain its action association");
		ctx.dynamic_invocations.push(invocation);
		return DispatchResult::Dispatched;
	}
	command_registry::dispatch_ex_command(ctx, buffer)
}

fn runtime_args_match(rule: CommandArgRule, args: &str) -> bool {
	match rule {
		CommandArgRule::None => args.is_empty(),
		CommandArgRule::Optional => true,
		CommandArgRule::Required => !args.is_empty(),
	}
}

pub(crate) fn completion_command_names(
	registries: &RuntimeRegistries,
	kind: ArgCompletion,
) -> Vec<String> {
	let mut names = command_registry::completion_command_names(kind)
		.into_iter()
		.map(str::to_string)
		.collect::<Vec<_>>();
	let mut runtime_commands = registries
		.commands()
		.map(|(_, _, command)| command)
		.collect::<Vec<_>>();
	runtime_commands.sort_by(|left, right| left.name.cmp(&right.name));
	for command in runtime_commands {
		if runtime_completion_matches(&command.completion, kind) {
			names.push(command.name.clone());
			names.extend(command.aliases.iter().cloned());
		}
	}
	names
}

fn runtime_completion_matches(completion: &RuntimeCompletion, kind: ArgCompletion) -> bool {
	matches!(
		(completion, kind),
		(RuntimeCompletion::Path, ArgCompletion::Path)
			| (RuntimeCompletion::Language, ArgCompletion::Language)
			| (RuntimeCompletion::Theme, ArgCompletion::Theme)
	)
}

pub(crate) fn suggest(registries: &RuntimeRegistries, buffer: &str) -> Option<String> {
	let mut candidates = command_registry::suggestion_candidates();
	let mut runtime_commands = registries
		.commands()
		.map(|(_, _, command)| command)
		.collect::<Vec<_>>();
	runtime_commands.sort_by(|left, right| left.name.cmp(&right.name));
	for command in runtime_commands {
		candidates.push(command.name.clone());
		candidates.extend(command.aliases.iter().cloned());
	}
	let mut seen = HashSet::new();
	candidates.retain(|candidate| seen.insert(candidate.clone()));
	command_registry::suggest_from_candidates(buffer, candidates)
}

pub(crate) fn command_snapshot(registries: &RuntimeRegistries) -> EffectiveCommandSnapshot {
	let mut commands = COMMANDS
		.iter()
		.map(|command| EffectiveCommandInfo {
			source: CommandSource::Builtin,
			precedence: None,
			name: command.name.to_string(),
			aliases: command
				.aliases
				.iter()
				.map(|alias| (*alias).to_string())
				.collect(),
			bang: command.bang,
			args: match command.args {
				ArgSpec::None => CommandArgRule::None,
				ArgSpec::Optional => CommandArgRule::Optional,
				ArgSpec::Required => CommandArgRule::Required,
			},
			usage: command.usage.to_string(),
			summary: command.summary.to_string(),
			help: command.help.to_string(),
			category: command.category.label().to_string(),
			completion: match command.completion {
				ArgCompletion::None => RuntimeCompletion::None,
				ArgCompletion::Path => RuntimeCompletion::Path,
				ArgCompletion::Language => RuntimeCompletion::Language,
				ArgCompletion::Theme => RuntimeCompletion::Theme,
			},
		})
		.collect::<Vec<_>>();
	commands.extend(
		registries
			.command_infos()
			.into_iter()
			.map(runtime_effective_info),
	);
	EffectiveCommandSnapshot {
		revision: registries.revision(),
		commands,
	}
}

fn runtime_effective_info(command: RuntimeCommandInfo) -> EffectiveCommandInfo {
	EffectiveCommandInfo {
		source: CommandSource::Runtime(command.owner),
		precedence: Some(command.precedence),
		name: command.name,
		aliases: command.aliases,
		bang: command.bang,
		args: command.args,
		usage: command.usage,
		summary: command.summary,
		help: command.help,
		category: "Extensions".to_string(),
		completion: command.completion,
	}
}

/// Builtin command spellings protected from runtime exact commands.
#[allow(dead_code)]
pub(crate) fn protected_runtime_command_tokens() -> Vec<String> {
	let mut tokens = command_registry::protected_runtime_tokens();
	tokens.extend(super::option_registry::protected_session_tokens());
	tokens
}

/// Builtin setting spellings protected from runtime `:set` definitions.
#[allow(dead_code)]
pub(crate) fn protected_runtime_option_tokens() -> Vec<String> {
	super::option_registry::protected_setting_tokens()
}
