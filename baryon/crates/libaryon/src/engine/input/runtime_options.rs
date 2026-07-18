//! Effective option catalog: persistent builtins plus session-scoped runtime options.

use super::option_registry::{self, BOOL_OPTIONS, VALUE_OPTIONS, ValueCompletion, parse_bool_like};
use crate::core::{RegistryOwnerId, RegistryRev};
use crate::engine::runtime_registry::{
	InvocationSnapshot, OptionInvocation, OptionValue, RuntimeOptionInfo, RuntimeOptionKind,
	RuntimeRegistries,
};
use crate::engine::{EditorCommand, UI_COLOR_SPECS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OptionSource {
	Builtin,
	Runtime(RegistryOwnerId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectiveOptionKind {
	Bool,
	Integer,
	String { require_non_empty: bool },
	BuiltinValue,
	UiColor,
}

/// Owned, action-free option metadata used by help and future introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveOptionInfo {
	pub(crate) source: OptionSource,
	pub(crate) precedence: Option<u64>,
	pub(crate) name: String,
	pub(crate) aliases: Vec<String>,
	pub(crate) kind: EffectiveOptionKind,
	pub(crate) usage: String,
	pub(crate) summary: String,
	pub(crate) help: String,
	pub(crate) completion_candidates: Vec<String>,
	pub(crate) persistent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveOptionSnapshot {
	pub(crate) revision: RegistryRev,
	pub(crate) options: Vec<EffectiveOptionInfo>,
}

#[derive(Debug)]
pub(crate) enum SetAction {
	Builtin(EditorCommand),
	Runtime(InvocationSnapshot),
}

/// Parse one `:set` argument string against the session-effective catalog.
pub(crate) fn parse_set_command(
	registries: &RuntimeRegistries,
	rest: &str,
) -> Result<SetAction, String> {
	let (key, value) = split_key_value(rest);
	if let Some(query_spelling) = key.strip_suffix('?')
		&& let Some((option, negated)) = registries.option(query_spelling)
	{
		if negated || !value.is_empty() {
			return Err(format!("Usage: set {}?", option.name));
		}
		return registries
			.option_invocation(query_spelling, OptionInvocation::Query)
			.map(SetAction::Runtime)
			.ok_or_else(|| "Runtime option disappeared during dispatch".to_string());
	}
	if let Some((option, negated)) = registries.option(key) {
		let operation = parse_runtime_value(option.kind, negated, value)
			.map_err(|()| format!("Usage: {}", option.usage))?;
		return registries
			.option_invocation(key, OptionInvocation::Set(operation))
			.map(SetAction::Runtime)
			.ok_or_else(|| "Runtime option disappeared during dispatch".to_string());
	}

	match option_registry::parse_set_command(rest) {
		Ok(command) => Ok(SetAction::Builtin(command)),
		Err(error) if error.starts_with("Unknown setting:") => {
			let token = key.trim_end_matches('?');
			let mut message = format!("Unknown setting: {rest}");
			if let Some(suggestion) = suggest(registries, token) {
				message.push_str(&format!(" (did you mean '{suggestion}'?)"));
			}
			Err(message)
		}
		Err(error) => Err(error),
	}
}

fn split_key_value(rest: &str) -> (&str, &str) {
	match rest.find(char::is_whitespace) {
		Some(index) => (&rest[..index], rest[index..].trim()),
		None => (rest, ""),
	}
}

fn parse_runtime_value(
	kind: RuntimeOptionKind,
	negated: bool,
	value: &str,
) -> Result<OptionValue, ()> {
	match kind {
		RuntimeOptionKind::Bool => {
			if negated {
				return value
					.is_empty()
					.then_some(OptionValue::Bool(false))
					.ok_or(());
			}
			if value.is_empty() {
				return Ok(OptionValue::Bool(true));
			}
			parse_bool_like(value).map(OptionValue::Bool).ok_or(())
		}
		RuntimeOptionKind::Integer if !negated => value
			.parse::<i64>()
			.map(OptionValue::Integer)
			.map_err(|_| ()),
		RuntimeOptionKind::String { require_non_empty } if !negated => {
			if require_non_empty && value.is_empty() {
				return Err(());
			}
			Ok(OptionValue::String(value.to_string()))
		}
		RuntimeOptionKind::Integer | RuntimeOptionKind::String { .. } => Err(()),
	}
}

pub(crate) fn setting_key_candidates(registries: &RuntimeRegistries) -> Vec<String> {
	let mut candidates = BOOL_OPTIONS
		.iter()
		.map(|option| option.key.to_string())
		.chain(VALUE_OPTIONS.iter().map(|option| option.key.to_string()))
		.chain(UI_COLOR_SPECS.iter().map(|spec| spec.name.to_string()))
		.collect::<Vec<_>>();
	for option in registries.option_infos() {
		candidates.push(option.name.clone());
		candidates.extend(option.aliases.iter().cloned());
		if option.kind == RuntimeOptionKind::Bool {
			candidates.push(format!("no{}", option.name));
			candidates.extend(option.aliases.iter().map(|alias| format!("no{alias}")));
		}
	}
	candidates.sort();
	candidates.dedup();
	candidates
}

pub(crate) fn static_value_completion<'a>(
	registries: &'a RuntimeRegistries,
	spelling: &str,
) -> Option<&'a [String]> {
	let (option, negated) = registries.option(spelling)?;
	(!negated).then_some(option.completion_candidates.as_slice())
}

pub(crate) fn suggest(registries: &RuntimeRegistries, token: &str) -> Option<String> {
	let mut candidates = option_registry::protected_setting_tokens();
	for option in registries.option_infos() {
		candidates.push(option.name.clone());
		candidates.extend(option.aliases.iter().cloned());
		if option.kind == RuntimeOptionKind::Bool {
			candidates.push(format!("no{}", option.name));
			candidates.extend(option.aliases.iter().map(|alias| format!("no{alias}")));
		}
	}
	option_registry::suggest_setting_from_candidates(token, candidates)
}

pub(crate) fn option_snapshot(registries: &RuntimeRegistries) -> EffectiveOptionSnapshot {
	let mut options = Vec::new();
	for option in BOOL_OPTIONS {
		options.push(EffectiveOptionInfo {
			source: OptionSource::Builtin,
			precedence: None,
			name: option.key.to_string(),
			aliases: option
				.aliases
				.iter()
				.map(|alias| (*alias).to_string())
				.collect(),
			kind: EffectiveOptionKind::Bool,
			usage: format!(":set {} [on|off]", option.key),
			summary: option.summary.to_string(),
			help: option.summary.to_string(),
			completion_candidates: vec!["on".to_string(), "off".to_string()],
			persistent: true,
		});
	}
	for option in VALUE_OPTIONS {
		options.push(EffectiveOptionInfo {
			source: OptionSource::Builtin,
			precedence: None,
			name: option.key.to_string(),
			aliases: option
				.aliases
				.iter()
				.map(|alias| (*alias).to_string())
				.collect(),
			kind: EffectiveOptionKind::BuiltinValue,
			usage: option.usage.to_string(),
			summary: option.summary.to_string(),
			help: option.summary.to_string(),
			completion_candidates: match option.completion {
				ValueCompletion::SaveDurability => {
					vec!["atomic".to_string(), "bounded".to_string()]
				}
				ValueCompletion::None | ValueCompletion::Theme => Vec::new(),
			},
			persistent: true,
		});
	}
	for spec in UI_COLOR_SPECS {
		options.push(EffectiveOptionInfo {
			source: OptionSource::Builtin,
			precedence: None,
			name: spec.name.to_string(),
			aliases: spec
				.aliases
				.iter()
				.map(|alias| (*alias).to_string())
				.collect(),
			kind: EffectiveOptionKind::UiColor,
			usage: format!(":set {} HEX|default", spec.name),
			summary: spec.summary.to_string(),
			help: spec.summary.to_string(),
			completion_candidates: vec!["default".to_string()],
			persistent: true,
		});
	}
	options.extend(
		registries
			.option_infos()
			.into_iter()
			.map(runtime_effective_info),
	);
	EffectiveOptionSnapshot {
		revision: registries.revision(),
		options,
	}
}

fn runtime_effective_info(option: RuntimeOptionInfo) -> EffectiveOptionInfo {
	EffectiveOptionInfo {
		source: OptionSource::Runtime(option.owner),
		precedence: Some(option.precedence),
		name: option.name,
		aliases: option.aliases,
		kind: match option.kind {
			RuntimeOptionKind::Bool => EffectiveOptionKind::Bool,
			RuntimeOptionKind::Integer => EffectiveOptionKind::Integer,
			RuntimeOptionKind::String { require_non_empty } => {
				EffectiveOptionKind::String { require_non_empty }
			}
		},
		usage: option.usage,
		summary: option.summary,
		help: option.help,
		completion_candidates: option.completion_candidates,
		persistent: false,
	}
}
