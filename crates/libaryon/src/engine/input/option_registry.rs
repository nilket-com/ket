//! Declarative registry of settings options.
//!
//! Boolean options are described by [`BoolOptionSpec`]: the same table drives
//! the session toggles (`:wrap`, `:nowrap`, ...), the persistent `:set` forms
//! (`:set wrap`, `:set nowrap`, `:set wrap off`, ...), and the help surfaces.
//! `:set`-only options with values (theme, language.theme) are
//! described by [`ValueOptionSpec`].

use crate::engine::settings::SaveDurability;
use crate::engine::{EditorCommand, UI_COLOR_SPECS, UiColorKey, parse_ui_rgb};

#[cfg(test)]
const BOOLEAN_SETTING_KEYS: &[&str] =
	&["wrap", "minimap", "num", "relativenumber", "vis", "reload"];

pub(super) struct BoolOptionSpec {
	pub(super) key: &'static str,
	/// Extra "on" spellings beyond `key`.
	pub(super) aliases: &'static [&'static str],
	/// Extra "off" spellings beyond `no{key}`.
	pub(super) off_aliases: &'static [&'static str],
	/// Human-readable name used in error messages and help.
	pub(super) label: &'static str,
	pub(super) summary: &'static str,
	pub(super) default_on: bool,
	/// Session-only command for the bare `:{key}` form; None when the option
	/// has no session toggle (it can only be changed via `:set`).
	pub(super) session_on: Option<fn() -> EditorCommand>,
	/// Session-only command for the bare `:no{key}` form.
	pub(super) session_off: Option<fn() -> EditorCommand>,
	pub(super) persist: fn(bool) -> EditorCommand,
}

pub(super) struct ValueOptionSpec {
	pub(super) key: &'static str,
	pub(super) aliases: &'static [&'static str],
	pub(super) usage: &'static str,
	pub(super) summary: &'static str,
	pub(super) completion: ValueCompletion,
	pub(super) parse: fn(&str) -> Result<EditorCommand, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ValueCompletion {
	None,
	Theme,
	SaveDurability,
}

pub(super) static BOOL_OPTIONS: &[BoolOptionSpec] = &[
	BoolOptionSpec {
		key: "wrap",
		aliases: &[],
		off_aliases: &[],
		label: "wrap",
		summary: "Soft-wrap lines that are wider than the pane",
		default_on: true,
		session_on: Some(session_wrap_toggle),
		session_off: Some(session_wrap_off),
		persist: EditorCommand::PersistWrap,
	},
	BoolOptionSpec {
		key: "minimap",
		aliases: &[],
		off_aliases: &[],
		label: "minimap",
		summary: "Show the code minimap",
		default_on: true,
		session_on: Some(session_minimap_on),
		session_off: Some(session_minimap_off),
		persist: EditorCommand::PersistMinimap,
	},
	BoolOptionSpec {
		key: "num",
		aliases: &[],
		off_aliases: &[],
		label: "line number",
		summary: "Show the line number gutter",
		default_on: true,
		session_on: Some(session_num_on),
		session_off: Some(session_num_off),
		persist: EditorCommand::PersistLineNumbers,
	},
	BoolOptionSpec {
		key: "relativenumber",
		aliases: &["relnum", "rnum", "rnu"],
		off_aliases: &[],
		label: "relative line number",
		summary: "Relative numbers; with num, cursor line stays absolute",
		default_on: false,
		session_on: Some(session_relnum_on),
		session_off: Some(session_relnum_off),
		persist: EditorCommand::PersistRelativeLineNumbers,
	},
	BoolOptionSpec {
		key: "vis",
		aliases: &[],
		off_aliases: &["noviz"],
		label: "visible whitespace",
		summary: "Render spaces and tabs as visible glyphs",
		default_on: true,
		session_on: Some(session_vis_on),
		session_off: Some(session_vis_off),
		persist: EditorCommand::PersistVisibleWhitespace,
	},
	BoolOptionSpec {
		key: "reload",
		aliases: &[],
		off_aliases: &[],
		label: "reload",
		summary: "Automatically reload files changed on disk",
		default_on: true,
		session_on: None,
		session_off: None,
		persist: EditorCommand::PersistReload,
	},
];

pub(super) static VALUE_OPTIONS: &[ValueOptionSpec] = &[
	ValueOptionSpec {
		key: "theme",
		aliases: &[],
		usage: ":set theme NAME",
		summary: "Syntax color theme",
		completion: ValueCompletion::Theme,
		parse: parse_theme,
	},
	ValueOptionSpec {
		key: "savedurability",
		aliases: &[],
		usage: ":set savedurability MODE",
		summary: "Atomic or bounded save durability",
		completion: ValueCompletion::SaveDurability,
		parse: parse_save_durability,
	},
	ValueOptionSpec {
		key: "gitrefresh",
		aliases: &[],
		usage: ":set gitrefresh SECONDS",
		summary: "Git status refresh (0 disables)",
		completion: ValueCompletion::None,
		parse: parse_git_refresh,
	},
	ValueOptionSpec {
		key: "language.theme",
		aliases: &["langtheme"],
		usage: ":set language.theme LANG THEME|default",
		summary: "Syntax theme override for one language",
		completion: ValueCompletion::None,
		parse: parse_language_theme,
	},
	ValueOptionSpec {
		key: "nolangtheme",
		aliases: &[],
		usage: ":set nolangtheme LANG",
		summary: "Remove one language theme override",
		completion: ValueCompletion::None,
		parse: parse_nolangtheme,
	},
];

/// Resolve a bare session-toggle token like `wrap`, `nowrap`, or `noviz` to
/// its session-only command. Returns None for unknown tokens and for options
/// without a session form.
pub(super) fn session_option_command(token: &str) -> Option<EditorCommand> {
	for option in BOOL_OPTIONS {
		if is_on_key(option, token) {
			return option.session_on.map(|command| command());
		}
		if is_off_key(option, token) {
			return option.session_off.map(|command| command());
		}
	}
	None
}

pub(crate) fn rune_option_key(name: &str) -> Option<&'static str> {
	let legacy_name = match name {
		"line_numbers" => Some("num"),
		"relative_line_numbers" => Some("relativenumber"),
		"visible_whitespace" => Some("vis"),
		_ => None,
	};
	if legacy_name.is_some() {
		return legacy_name;
	}
	BOOL_OPTIONS
		.iter()
		.find(|option| is_on_key(option, name))
		.map(|option| option.key)
		.or_else(|| {
			VALUE_OPTIONS
				.iter()
				.find(|option| option.key == name || option.aliases.contains(&name))
				.map(|option| option.key)
		})
}

/// Builtin boolean spellings that occupy the bare ex-command namespace.
#[allow(dead_code)]
pub(super) fn protected_session_tokens() -> Vec<String> {
	let mut tokens = Vec::new();
	for option in BOOL_OPTIONS {
		if option.session_on.is_some() {
			tokens.push(option.key.to_string());
			tokens.extend(option.aliases.iter().map(|alias| (*alias).to_string()));
		}
		if option.session_off.is_some() {
			tokens.push(format!("no{}", option.key));
			tokens.extend(option.aliases.iter().map(|alias| format!("no{alias}")));
			tokens.extend(option.off_aliases.iter().map(|alias| (*alias).to_string()));
		}
	}
	tokens
}

/// Every builtin spelling protected in the `:set` namespace.
#[allow(dead_code)]
pub(super) fn protected_setting_tokens() -> Vec<String> {
	let mut tokens = Vec::new();
	for option in BOOL_OPTIONS {
		tokens.push(option.key.to_string());
		tokens.push(format!("no{}", option.key));
		for alias in option.aliases {
			tokens.push((*alias).to_string());
			tokens.push(format!("no{alias}"));
		}
		tokens.extend(option.off_aliases.iter().map(|alias| (*alias).to_string()));
	}
	for option in VALUE_OPTIONS {
		tokens.push(option.key.to_string());
		tokens.extend(option.aliases.iter().map(|alias| (*alias).to_string()));
	}
	for spec in UI_COLOR_SPECS {
		tokens.push(spec.name.to_string());
		tokens.extend(spec.aliases.iter().map(|alias| (*alias).to_string()));
	}
	tokens.push("init".to_string());
	tokens
}

/// Parse the arguments of `:set` into a persistent settings command.
pub(super) fn parse_set_command(rest: &str) -> Result<EditorCommand, String> {
	match rest.trim() {
		"init" => return Ok(EditorCommand::WriteInit { force: false }),
		"init!" => return Ok(EditorCommand::WriteInit { force: true }),
		_ => {}
	}
	let (key, value) = match rest.find(char::is_whitespace) {
		Some(idx) => (&rest[..idx], rest[idx..].trim()),
		None => (rest, ""),
	};
	if let Some(query_name) = key.strip_suffix('?')
		&& let Some(color_key) = UiColorKey::from_name(query_name)
	{
		return if value.is_empty() {
			Ok(EditorCommand::QueryUiColor(color_key))
		} else {
			Err(format!("Usage: set {}?", color_key.canonical_name()))
		};
	}
	if let Some(color_key) = UiColorKey::from_name(key) {
		if value.is_empty() {
			return Err(format!("Missing color for {}", color_key.canonical_name()));
		}
		if value == "default" {
			return Ok(EditorCommand::PersistUiColor {
				key: color_key,
				value: None,
			});
		}
		let color = parse_ui_rgb(value).ok_or_else(|| {
			format!(
				"Invalid color for {}: expected 3 or 6 hex digits",
				color_key.canonical_name()
			)
		})?;
		return Ok(EditorCommand::PersistUiColor {
			key: color_key,
			value: Some(color),
		});
	}
	for option in VALUE_OPTIONS {
		if key == option.key || option.aliases.contains(&key) {
			return (option.parse)(value);
		}
	}
	for option in BOOL_OPTIONS {
		if is_on_key(option, key) {
			return if value.is_empty() {
				Ok((option.persist)(true))
			} else {
				match parse_bool_like(value) {
					Some(enabled) => Ok((option.persist)(enabled)),
					None => Err(format!("Invalid {} value: {}", option.label, value)),
				}
			};
		}
		if value.is_empty() && is_off_key(option, key) {
			return Ok((option.persist)(false));
		}
	}
	let mut error = format!("Unknown setting: {rest}");
	if let Some(suggestion) = suggest_setting_key(key.trim_end_matches('?')) {
		error.push_str(&format!(" (did you mean '{suggestion}'?)"));
	}
	Err(error)
}

fn is_on_key(option: &BoolOptionSpec, token: &str) -> bool {
	token == option.key || option.aliases.contains(&token)
}

fn is_off_key(option: &BoolOptionSpec, token: &str) -> bool {
	token
		.strip_prefix("no")
		.is_some_and(|rest| is_on_key(option, rest))
		|| option.off_aliases.contains(&token)
}

pub(super) fn parse_bool_like(value: &str) -> Option<bool> {
	match value {
		"on" | "true" | "yes" => Some(true),
		"off" | "false" | "no" => Some(false),
		_ => None,
	}
}

fn parse_theme(value: &str) -> Result<EditorCommand, String> {
	if value.is_empty() {
		return Err("Missing theme name".to_string());
	}
	Ok(EditorCommand::PersistTheme(value.to_string()))
}

fn parse_save_durability(value: &str) -> Result<EditorCommand, String> {
	match SaveDurability::parse(value) {
		Some(durability) => Ok(EditorCommand::PersistSaveDurability(durability)),
		None => Err("Usage: set savedurability atomic|bounded".to_string()),
	}
}

fn parse_git_refresh(value: &str) -> Result<EditorCommand, String> {
	let seconds = value
		.parse::<u64>()
		.map_err(|_| "Usage: set gitrefresh SECONDS (zero disables)".to_string())?;
	if seconds > 86_400 {
		return Err("Git refresh interval must be between 0 and 86400 seconds".to_string());
	}
	Ok(EditorCommand::SetGitRefresh(seconds))
}

fn parse_language_theme(value: &str) -> Result<EditorCommand, String> {
	let mut parts = value.split_whitespace();
	let Some(language) = parts.next() else {
		return Err("Missing language id".to_string());
	};
	let Some(theme) = parts.next() else {
		return Err("Missing theme name".to_string());
	};
	if parts.next().is_some() {
		return Err("Usage: set language.theme LANG THEME|default".to_string());
	}
	if theme == "default" {
		return Ok(EditorCommand::PersistNoLangTheme(language.to_string()));
	}
	Ok(EditorCommand::PersistLangTheme {
		language: language.to_string(),
		theme: theme.to_string(),
	})
}

fn suggest_setting_key(token: &str) -> Option<String> {
	if token.is_empty() {
		return None;
	}
	let mut candidates = Vec::new();
	for option in BOOL_OPTIONS {
		candidates.push(option.key.to_string());
		candidates.extend(option.aliases.iter().map(|alias| (*alias).to_string()));
	}
	for option in VALUE_OPTIONS {
		candidates.push(option.key.to_string());
		candidates.extend(option.aliases.iter().map(|alias| (*alias).to_string()));
	}
	for spec in UI_COLOR_SPECS {
		candidates.push(spec.name.to_string());
		candidates.extend(spec.aliases.iter().map(|alias| (*alias).to_string()));
	}
	suggest_setting_from_candidates(token, candidates)
}

pub(super) fn suggest_setting_from_candidates(
	token: &str,
	mut candidates: Vec<String>,
) -> Option<String> {
	if token.is_empty() {
		return None;
	}
	candidates.sort();
	candidates.dedup();

	let max_distance = (token.chars().count() / 3).max(1);
	let mut best: Option<(usize, String)> = None;
	let mut tied = false;
	for candidate in candidates {
		if candidate == token {
			continue;
		}
		let distance = setting_edit_distance(token, &candidate);
		if distance > max_distance || distance >= token.chars().count() {
			continue;
		}
		match &best {
			None => {
				best = Some((distance, candidate));
				tied = false;
			}
			Some((best_distance, _)) if distance < *best_distance => {
				best = Some((distance, candidate));
				tied = false;
			}
			Some((best_distance, _)) if distance == *best_distance => tied = true,
			Some(_) => {}
		}
	}
	(!tied)
		.then(|| best.map(|(_, candidate)| candidate))
		.flatten()
}

fn setting_edit_distance(a: &str, b: &str) -> usize {
	let a = a.as_bytes();
	let b = b.as_bytes();
	let mut previous: Vec<usize> = (0..=b.len()).collect();
	let mut current = vec![0; b.len() + 1];
	for (i, a_byte) in a.iter().enumerate() {
		current[0] = i + 1;
		for (j, b_byte) in b.iter().enumerate() {
			current[j + 1] = (previous[j + 1] + 1)
				.min(current[j] + 1)
				.min(previous[j] + usize::from(a_byte != b_byte));
		}
		std::mem::swap(&mut previous, &mut current);
	}
	previous[b.len()]
}

fn parse_nolangtheme(value: &str) -> Result<EditorCommand, String> {
	if value.is_empty() || value.split_whitespace().count() != 1 {
		return Err("Usage: set nolangtheme LANG".to_string());
	}
	Ok(EditorCommand::PersistNoLangTheme(value.to_string()))
}

fn session_wrap_toggle() -> EditorCommand {
	EditorCommand::ToggleWrap
}

fn session_wrap_off() -> EditorCommand {
	EditorCommand::SetWrap(false)
}

fn session_minimap_on() -> EditorCommand {
	EditorCommand::SetMinimap(true)
}

fn session_minimap_off() -> EditorCommand {
	EditorCommand::SetMinimap(false)
}

fn session_num_on() -> EditorCommand {
	EditorCommand::SetLineNumbers(true)
}

fn session_num_off() -> EditorCommand {
	EditorCommand::SetLineNumbers(false)
}

fn session_relnum_on() -> EditorCommand {
	EditorCommand::SetRelativeLineNumbers(true)
}

fn session_relnum_off() -> EditorCommand {
	EditorCommand::SetRelativeLineNumbers(false)
}

fn session_vis_on() -> EditorCommand {
	EditorCommand::SetVisibleWhitespace(true)
}

fn session_vis_off() -> EditorCommand {
	EditorCommand::SetVisibleWhitespace(false)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn debug_eq(actual: &EditorCommand, expected: &EditorCommand) -> bool {
		format!("{actual:?}") == format!("{expected:?}")
	}

	#[test]
	fn set_bool_option_forms_map_to_persist_commands() {
		for option in BOOL_OPTIONS {
			let on = (option.persist)(true);
			let off = (option.persist)(false);

			let bare = parse_set_command(option.key).expect(option.key);
			assert!(debug_eq(&bare, &on), "set {} should persist on", option.key);
			for alias in option.aliases {
				let aliased = parse_set_command(alias).expect(alias);
				assert!(debug_eq(&aliased, &on), "set {alias} should persist on");

				let alias_with_value = format!("{alias} off");
				let aliased_off = parse_set_command(&alias_with_value).expect(&alias_with_value);
				assert!(
					debug_eq(&aliased_off, &off),
					"set {alias_with_value} mismatch"
				);
			}

			let no_form = format!("no{}", option.key);
			let negated = parse_set_command(&no_form).expect(&no_form);
			assert!(debug_eq(&negated, &off), "set {no_form} should persist off");
			for alias in option.aliases {
				let no_alias = format!("no{alias}");
				let negated_alias = parse_set_command(&no_alias).expect(&no_alias);
				assert!(
					debug_eq(&negated_alias, &off),
					"set {no_alias} should persist off"
				);
			}

			for alias in option.off_aliases {
				let aliased = parse_set_command(alias).expect(alias);
				assert!(debug_eq(&aliased, &off), "set {alias} should persist off");
			}

			for (value, expected) in [("on", &on), ("true", &on), ("off", &off), ("no", &off)] {
				let buffer = format!("{} {value}", option.key);
				let parsed = parse_set_command(&buffer).expect(&buffer);
				assert!(debug_eq(&parsed, expected), "set {buffer} mismatch");
			}

			let invalid = parse_set_command(&format!("{} zzz", option.key));
			assert!(
				invalid.as_ref().is_err_and(|e| e.contains("Invalid")),
				"set {} zzz should be invalid, got {invalid:?}",
				option.key
			);
		}
	}

	#[test]
	fn set_value_options_parse_and_validate() {
		assert!(matches!(
			parse_set_command("init"),
			Ok(EditorCommand::WriteInit { force: false })
		));
		assert!(matches!(
			parse_set_command("init!"),
			Ok(EditorCommand::WriteInit { force: true })
		));
		assert!(parse_set_command("initw").is_err());
		assert!(parse_set_command("initwrite").is_err());
		assert!(matches!(
			parse_set_command("theme onedark"),
			Ok(EditorCommand::PersistTheme(name)) if name == "onedark"
		));
		assert_eq!(
			parse_set_command("theme").unwrap_err(),
			"Missing theme name"
		);
		assert!(matches!(
			parse_set_command("background #1a2b3c"),
			Ok(EditorCommand::PersistUiColor {
				key: UiColorKey::EditorBackground,
				value: Some(color),
			}) if color == crate::core::Rgb::new(0x1a, 0x2b, 0x3c)
		));
		assert_eq!(
			parse_set_command("background").unwrap_err(),
			"Missing color for colors.editor.background"
		);
		assert!(matches!(
			parse_set_command("searchhighlight #4cff00"),
			Ok(EditorCommand::PersistUiColor {
				key: UiColorKey::SearchHighlight,
				value: Some(color),
			}) if color == crate::core::Rgb::new(0x4c, 0xff, 0x00)
		));
		assert_eq!(
			parse_set_command("searchhighlight").unwrap_err(),
			"Missing color for colors.search.highlight"
		);
		assert!(matches!(
			parse_set_command("langtheme rust glowmax"),
			Ok(EditorCommand::PersistLangTheme { language, theme })
				if language == "rust" && theme == "glowmax"
		));
		assert_eq!(
			parse_set_command("langtheme rust").unwrap_err(),
			"Missing theme name"
		);
		assert_eq!(
			parse_set_command("langtheme rust glowmax extra").unwrap_err(),
			"Usage: set language.theme LANG THEME|default"
		);
		assert!(matches!(
			parse_set_command("language.theme rust default"),
			Ok(EditorCommand::PersistNoLangTheme(language)) if language == "rust"
		));
		assert!(matches!(
			parse_set_command("savedurability bounded"),
			Ok(EditorCommand::PersistSaveDurability(
				SaveDurability::Bounded
			))
		));
		assert!(matches!(
			parse_set_command("savedurability atomic"),
			Ok(EditorCommand::PersistSaveDurability(SaveDurability::Atomic))
		));
		assert!(matches!(
			parse_set_command("gitrefresh 12"),
			Ok(EditorCommand::SetGitRefresh(12))
		));
		assert!(matches!(
			parse_set_command("gitrefresh 0"),
			Ok(EditorCommand::SetGitRefresh(0))
		));
		assert!(parse_set_command("gitrefresh -1").is_err());
		// The bounded path trades away crash safety, so it is never reachable by a
		// typo or a bare `:set savedurability`.
		for rejected in ["", "on", "true", "boundedish"] {
			assert_eq!(
				parse_set_command(&format!("savedurability {rejected}"))
					.unwrap_err()
					.trim_end(),
				"Usage: set savedurability atomic|bounded",
				"savedurability {rejected:?} must not parse"
			);
		}
		assert!(matches!(
			parse_set_command("nolangtheme rust"),
			Ok(EditorCommand::PersistNoLangTheme(language)) if language == "rust"
		));
		assert_eq!(
			parse_set_command("nolangtheme").unwrap_err(),
			"Usage: set nolangtheme LANG"
		);
	}

	#[test]
	fn setting_suggestions_never_repeat_the_unknown_token() {
		assert_eq!(
			suggest_setting_from_candidates("mystery", vec!["mystery".to_string()]),
			None
		);
	}

	#[test]
	fn every_registered_ui_color_supports_set_reset_and_query() {
		for spec in &UI_COLOR_SPECS {
			let set = format!("{} abc", spec.name);
			assert!(matches!(
				parse_set_command(&set),
				Ok(EditorCommand::PersistUiColor { key, value: Some(color) })
					if key == spec.key && color == crate::core::Rgb::new(0xaa, 0xbb, 0xcc)
			));

			let reset = format!("{} default", spec.name);
			assert!(matches!(
				parse_set_command(&reset),
				Ok(EditorCommand::PersistUiColor { key, value: None }) if key == spec.key
			));

			let query = format!("{}?", spec.name);
			assert!(matches!(
				parse_set_command(&query),
				Ok(EditorCommand::QueryUiColor(key)) if key == spec.key
			));
		}
	}

	#[test]
	fn ui_colors_reject_non_strict_hex_and_suggest_unique_typos() {
		for invalid in ["12", "1234", "12345", "1234567", "ggg", "#abcd"] {
			let error = parse_set_command(&format!("colors.accent {invalid}"))
				.expect_err("invalid color must be rejected");
			assert!(error.contains("expected 3 or 6 hex digits"), "{error}");
		}
		let error = parse_set_command("colors.minmap.accent f00").unwrap_err();
		assert!(error.contains("colors.minimap.accent"), "{error}");
	}

	#[test]
	fn unknown_setting_reports_full_arguments() {
		assert_eq!(
			parse_set_command("foo bar").unwrap_err(),
			"Unknown setting: foo bar"
		);
	}

	#[test]
	fn session_toggles_resolve_from_the_table() {
		assert!(matches!(
			session_option_command("wrap"),
			Some(EditorCommand::ToggleWrap)
		));
		assert!(matches!(
			session_option_command("nowrap"),
			Some(EditorCommand::SetWrap(false))
		));
		assert!(matches!(
			session_option_command("num"),
			Some(EditorCommand::SetLineNumbers(true))
		));
		assert!(matches!(
			session_option_command("noviz"),
			Some(EditorCommand::SetVisibleWhitespace(false))
		));
		assert!(session_option_command("reload").is_none());
		assert!(session_option_command("noreload").is_none());
		assert!(session_option_command("zzz").is_none());
	}

	#[test]
	fn every_settings_key_is_supported_by_the_engine_schema() {
		let registry_keys = BOOL_OPTIONS
			.iter()
			.map(|option| option.key)
			.collect::<Vec<_>>();
		assert_eq!(registry_keys, BOOLEAN_SETTING_KEYS);
	}
}
