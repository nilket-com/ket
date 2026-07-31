//! Characterization boundary for the immutable builtin catalogs.
//!
//! The runtime-registry overlay replaces each static table incrementally. This
//! snapshot pins the effective engine-side behavior before that migration so a
//! stage cannot quietly change one consumer while leaving the others behind.

use super::command_registry::{
	ArgCompletion, ArgSpec, COMMANDS, CommandCtx, DispatchKind, DispatchResult,
	completion_command_names, dispatch_ex_command, suggest,
};
use super::help::{GENERAL_HELP_TOPICS, HelpTopic, render_markdown, render_plain, topic_hint};
use super::keymap::{KEYMAP, key_docs};
use super::machine_test_frontend::TestFrontend;
use super::option_registry::{
	BOOL_OPTIONS, VALUE_OPTIONS, ValueCompletion, parse_set_command, session_option_command,
};
use crate::core::{DocByte, DocLine};
use crate::engine::{EditorCommand, EditorMode, KeyCode, Modifiers, UI_COLOR_SPECS};
use std::fmt::Write;

const SNAPSHOT: &str = include_str!("snapshots/builtin-catalog.txt");

fn fnv1a(bytes: &[u8]) -> u64 {
	let mut hash = 0xcbf2_9ce4_8422_2325_u64;
	for byte in bytes {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
	}
	hash
}

fn command_description(command: &EditorCommand) -> String {
	match command {
		EditorCommand::ShowMarkdownDocument { title, markdown } => format!(
			"ShowMarkdownDocument(title={title:?}, bytes={}, fnv1a={:016x})",
			markdown.len(),
			fnv1a(markdown.as_bytes())
		),
		_ => format!("{command:?}"),
	}
}

fn commands_description(commands: &[EditorCommand]) -> String {
	let descriptions = commands.iter().map(command_description).collect::<Vec<_>>();
	format!("[{}]", descriptions.join(", "))
}

fn dispatch(buffer: &str) -> String {
	let mut commands = Vec::new();
	let mut status = None;
	let (result, quit) = {
		let mut ctx = CommandCtx {
			commands: &mut commands,
			dynamic_invocations: &mut Vec::new(),
			runtime_registries: None,
			status: &mut status,
			cursor_line: DocLine::new(6),
			visual_range: Some((DocByte::new(10), DocByte::new(30))),
			quit: false,
		};
		let result = dispatch_ex_command(&mut ctx, buffer);
		(result, ctx.quit)
	};
	format!(
		"result={result:?} commands={} status={status:?} quit={}",
		commands_description(&commands),
		quit
	)
}

fn arg_label(args: ArgSpec) -> &'static str {
	match args {
		ArgSpec::None => "none",
		ArgSpec::Optional => "optional",
		ArgSpec::Required => "required",
	}
}

fn completion_label(completion: ArgCompletion) -> &'static str {
	match completion {
		ArgCompletion::None => "none",
		ArgCompletion::Path => "path",
		ArgCompletion::Language => "language",
		ArgCompletion::Theme => "theme",
	}
}

fn value_completion_label(completion: ValueCompletion) -> &'static str {
	match completion {
		ValueCompletion::None => "none",
		ValueCompletion::Theme => "theme",
		ValueCompletion::SaveDurability => "save-durability",
	}
}

fn exact_invocation(name: &str, args: ArgSpec) -> String {
	match args {
		ArgSpec::Required => match name {
			"set" => "set wrap".to_string(),
			_ => format!("{name} x"),
		},
		ArgSpec::None | ArgSpec::Optional => name.to_string(),
	}
}

fn push_commands(snapshot: &mut String) {
	writeln!(snapshot, "[commands]").unwrap();
	let mut exact_count = 0;
	let mut fallback_names = Vec::new();
	for spec in COMMANDS {
		let dispatch_kind = match spec.dispatch {
			DispatchKind::Exact(_) => {
				exact_count += 1;
				"exact"
			}
			DispatchKind::Fallback(_) => {
				fallback_names.push(spec.name);
				"fallback"
			}
		};
		writeln!(
			snapshot,
			"{} | aliases={} | bang={} | args={} | completion={} | category={} | dispatch={} | usage={:?} | summary={:?} | help={:?}",
			spec.name,
			spec.aliases.join(","),
			spec.bang,
			arg_label(spec.args),
			completion_label(spec.completion),
			spec.category.label(),
			dispatch_kind,
			spec.usage,
			spec.summary,
			spec.help,
		)
		.unwrap();
	}
	writeln!(
		snapshot,
		"counts | total={} exact={} fallback={}",
		COMMANDS.len(),
		exact_count,
		fallback_names.len()
	)
	.unwrap();
	writeln!(snapshot, "fallback-order | {}", fallback_names.join(" > ")).unwrap();

	writeln!(snapshot, "\n[exact-command-dispatch]").unwrap();
	for spec in COMMANDS {
		if !matches!(spec.dispatch, DispatchKind::Exact(_)) {
			continue;
		}
		let invocation = exact_invocation(spec.name, spec.args);
		writeln!(snapshot, "{invocation:?} | {}", dispatch(&invocation)).unwrap();
		for alias in spec.aliases {
			let invocation = exact_invocation(alias, spec.args);
			writeln!(snapshot, "{invocation:?} | {}", dispatch(&invocation)).unwrap();
		}
	}

	writeln!(snapshot, "\n[dispatch-precedence]").unwrap();
	for buffer in [
		"",
		"w",
		"wrap",
		"7",
		"s/a/b/",
		"1,3sort!ni",
		"%uniqi",
		"zzz",
	] {
		writeln!(snapshot, "{buffer:?} | {}", dispatch(buffer)).unwrap();
	}

	writeln!(snapshot, "\n[command-completion-providers]").unwrap();
	for completion in [
		ArgCompletion::Path,
		ArgCompletion::Language,
		ArgCompletion::Theme,
	] {
		writeln!(
			snapshot,
			"{} | {}",
			completion_label(completion),
			completion_command_names(completion).join(",")
		)
		.unwrap();
	}
}

fn push_options(snapshot: &mut String) {
	writeln!(snapshot, "\n[options]").unwrap();
	for option in BOOL_OPTIONS {
		writeln!(
			snapshot,
			"bool {} | aliases={} | off-aliases={} | label={:?} | default={} | session-on={} | session-off={} | summary={:?}",
			option.key,
			option.aliases.join(","),
			option.off_aliases.join(","),
			option.label,
			option.default_on,
			option.session_on.is_some(),
			option.session_off.is_some(),
			option.summary,
		)
		.unwrap();
	}
	for option in VALUE_OPTIONS {
		writeln!(
			snapshot,
			"value {} | aliases={} | completion={} | usage={:?} | summary={:?}",
			option.key,
			option.aliases.join(","),
			value_completion_label(option.completion),
			option.usage,
			option.summary,
		)
		.unwrap();
	}
	writeln!(
		snapshot,
		"counts | bool={} value={} ui-colors={}",
		BOOL_OPTIONS.len(),
		VALUE_OPTIONS.len(),
		UI_COLOR_SPECS.len()
	)
	.unwrap();

	writeln!(snapshot, "\n[boolean-option-forms]").unwrap();
	for option in BOOL_OPTIONS {
		let on_tokens = std::iter::once(option.key)
			.chain(option.aliases.iter().copied())
			.collect::<Vec<_>>();
		for (token, value) in on_tokens.iter().flat_map(|token| {
			["", "on", "true", "yes", "off", "false", "no"]
				.into_iter()
				.map(move |value| (*token, value))
		}) {
			let form = if value.is_empty() {
				token.to_string()
			} else {
				format!("{token} {value}")
			};
			let parsed = parse_set_command(&form)
				.map(|command| command_description(&command))
				.unwrap_or_else(|error| format!("error={error:?}"));
			writeln!(snapshot, "set {form} | {parsed}").unwrap();
		}
		let off_tokens = std::iter::once(format!("no{}", option.key))
			.chain(option.aliases.iter().map(|alias| format!("no{alias}")))
			.chain(option.off_aliases.iter().map(|alias| (*alias).to_string()));
		for token in off_tokens {
			let parsed = parse_set_command(&token)
				.map(|command| command_description(&command))
				.unwrap_or_else(|error| format!("error={error:?}"));
			writeln!(snapshot, "set {token} | {parsed}").unwrap();
		}

		let mut session_tokens = vec![option.key.to_string(), format!("no{}", option.key)];
		session_tokens.extend(option.aliases.iter().map(|alias| (*alias).to_string()));
		session_tokens.extend(option.aliases.iter().map(|alias| format!("no{alias}")));
		session_tokens.extend(option.off_aliases.iter().map(|alias| (*alias).to_string()));
		for token in session_tokens {
			let session = session_option_command(&token)
				.as_ref()
				.map(command_description)
				.unwrap_or_else(|| "none".to_string());
			writeln!(snapshot, ":{token} | {session}").unwrap();
		}
	}

	writeln!(snapshot, "\n[value-option-forms]").unwrap();
	for form in [
		"theme onedark",
		"savedurability atomic",
		"savedurability bounded",
		"language.theme rust glowmax",
		"language.theme rust default",
		"langtheme rune default",
		"nolangtheme rust",
	] {
		let parsed = parse_set_command(form)
			.map(|command| command_description(&command))
			.unwrap_or_else(|error| format!("error={error:?}"));
		writeln!(snapshot, "set {form} | {parsed}").unwrap();
	}
	for form in ["theme", "savedurability true", "language.theme rust", "zzz"] {
		let parsed = parse_set_command(form)
			.map(|command| command_description(&command))
			.unwrap_or_else(|error| format!("error={error:?}"));
		writeln!(snapshot, "set {form} | {parsed}").unwrap();
	}
}

fn push_keys(snapshot: &mut String) {
	writeln!(snapshot, "\n[keymap]").unwrap();
	let binding_count = KEYMAP
		.iter()
		.map(|group| group.bindings.len())
		.sum::<usize>();
	writeln!(
		snapshot,
		"counts | groups={} bindings={} doc-rows={}",
		KEYMAP.len(),
		binding_count,
		key_docs().len()
	)
	.unwrap();
	for (index, group) in KEYMAP.iter().enumerate() {
		writeln!(
			snapshot,
			"group {index} | {} | {:?}",
			group.category.label(),
			group.summary
		)
		.unwrap();
		for binding in group.bindings {
			writeln!(snapshot, "  {binding:?}").unwrap();
		}
	}
	writeln!(snapshot, "\n[effective-key-sequences]").unwrap();
	for doc in key_docs() {
		writeln!(
			snapshot,
			"{} | {} | {} | {}",
			doc.mode.label(),
			doc.category.label(),
			doc.keys,
			doc.summary
		)
		.unwrap();
	}
}

fn push_help(snapshot: &mut String) {
	writeln!(snapshot, "\n[help]").unwrap();
	writeln!(snapshot, "topic-hint | {:?}", topic_hint()).unwrap();

	let mut topics = Vec::new();
	for spec in GENERAL_HELP_TOPICS {
		topics.push((format!("general:{}", spec.name), HelpTopic::General(spec)));
	}
	for spec in COMMANDS {
		topics.push((format!("command:{}", spec.name), HelpTopic::Command(spec)));
	}
	for option in BOOL_OPTIONS {
		topics.push((
			format!("bool-option:{}", option.key),
			HelpTopic::BoolOption(option),
		));
	}
	for option in VALUE_OPTIONS {
		topics.push((
			format!("value-option:{}", option.key),
			HelpTopic::ValueOption(option),
		));
	}
	for spec in &UI_COLOR_SPECS {
		topics.push((format!("ui-color:{}", spec.name), HelpTopic::UiColor(spec)));
	}

	for (name, topic) in topics {
		let plain = render_plain(&topic, "baryon");
		let markdown = render_markdown(&topic);
		writeln!(
			snapshot,
			"{name} | plain-bytes={} plain-fnv1a={:016x} | markdown-bytes={} markdown-fnv1a={:016x}",
			plain.len(),
			fnv1a(plain.as_bytes()),
			markdown.len(),
			fnv1a(markdown.as_bytes())
		)
		.unwrap();
	}
}

fn completion_case(buffer: &str, languages: &[&str]) -> String {
	let (mut frontend, _commands) = TestFrontend::new();
	frontend.set_editor_mode_for_test(EditorMode::Command);
	frontend.command_buffer = buffer.to_string();
	frontend.command_cursor = frontend.command_buffer.len();
	frontend.language_completion_candidates = languages
		.iter()
		.map(|language| (*language).to_string())
		.collect();
	let mut should_quit = false;
	frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit);
	format!(
		"buffer={:?} cursor={} status={:?} pending={} quit={should_quit}",
		frontend.command_buffer,
		frontend.command_cursor,
		frontend.status_message,
		frontend.completion_pending,
	)
}

fn push_completion_and_suggestions(snapshot: &mut String) {
	writeln!(snapshot, "\n[completion]").unwrap();
	for (buffer, languages) in [
		("set colors.minimap.a", &[][..]),
		("set savedurability b", &[][..]),
		("theme gruv", &[][..]),
		("set theme one", &[][..]),
		("lang pla", &[][..]),
		("language r", &["rn", "rs", "rune", "rust"][..]),
		("set rel", &[][..]),
	] {
		writeln!(
			snapshot,
			"{buffer:?} | {}",
			completion_case(buffer, languages)
		)
		.unwrap();
	}

	writeln!(snapshot, "\n[suggestions]").unwrap();
	for typo in [
		"wrp",
		"tabnxet",
		"tabclos",
		"sotr",
		"nowarp",
		"wrap extra",
		"zzzzz",
		"z",
		"",
	] {
		writeln!(snapshot, "{typo:?} | {:?}", suggest(typo)).unwrap();
	}
	for typo in ["colors.minmap.accent f00", "relativnumber", "zzzzz"] {
		writeln!(
			snapshot,
			"set {typo} | {:?}",
			parse_set_command(typo).unwrap_err()
		)
		.unwrap();
	}
}

fn builtin_catalog_snapshot() -> String {
	let mut snapshot = String::new();
	for (name, characterize) in [
		("commands", push_commands as fn(&mut String)),
		("options", push_options),
		("keys", push_keys),
		("help", push_help),
		(
			"completion-and-suggestions",
			push_completion_and_suggestions,
		),
	] {
		let mut detail = String::new();
		characterize(&mut detail);
		writeln!(
			snapshot,
			"{name} | bytes={} lines={} fnv1a={:016x}",
			detail.len(),
			detail.lines().count(),
			fnv1a(detail.as_bytes())
		)
		.unwrap();
	}
	snapshot
}

#[test]
fn builtin_catalogs_match_the_pre_overlay_contract() {
	let actual = builtin_catalog_snapshot();
	assert_eq!(actual, SNAPSHOT, "builtin catalog snapshot changed");
}

#[test]
fn characterized_catalog_sizes_match_the_planning_inventory() {
	assert_eq!(COMMANDS.len(), 44);
	assert_eq!(BOOL_OPTIONS.len(), 6);
	assert_eq!(VALUE_OPTIONS.len(), 5);
	assert_eq!(
		KEYMAP
			.iter()
			.map(|group| group.bindings.len())
			.sum::<usize>(),
		267
	);
	assert_eq!(
		COMMANDS
			.iter()
			.filter_map(|spec| match spec.dispatch {
				DispatchKind::Fallback(_) => Some(spec.name),
				DispatchKind::Exact(_) => None,
			})
			.collect::<Vec<_>>(),
		vec!["{line}", "s", "sort", "uniq"]
	);
}

#[test]
fn dispatch_layers_are_characterized_in_effective_precedence_order() {
	for (buffer, expected) in [
		("w", DispatchResult::Dispatched),
		("wrap", DispatchResult::Dispatched),
		("7", DispatchResult::Dispatched),
		("s/a/b/", DispatchResult::Dispatched),
		("sort", DispatchResult::Dispatched),
		("uniq", DispatchResult::Dispatched),
		("zzz", DispatchResult::Unknown),
	] {
		let mut commands = Vec::new();
		let mut status = None;
		let mut ctx = CommandCtx {
			commands: &mut commands,
			dynamic_invocations: &mut Vec::new(),
			runtime_registries: None,
			status: &mut status,
			cursor_line: DocLine::ZERO,
			visual_range: None,
			quit: false,
		};
		assert_eq!(dispatch_ex_command(&mut ctx, buffer), expected, "{buffer}");
	}
}
