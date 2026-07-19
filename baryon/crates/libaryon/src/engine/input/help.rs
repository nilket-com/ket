//! Help content generated from the command and option registries.
//!
//! Topics resolve to registry entries, so the help output can never drift
//! from what actually dispatches. `render_plain` produces the `--help`
//! terminal output (kept within 80 columns).

use super::command_registry::{COMMANDS, CommandCategory, CommandSpec, suggest};
use super::keymap::{Category, KEY_MODE_ORDER, KeyDoc, KeyMode, key_docs};
use super::option_registry::{BOOL_OPTIONS, BoolOptionSpec, VALUE_OPTIONS, ValueOptionSpec};
use super::runtime_catalog::{CommandSource, EffectiveCommandInfo, command_snapshot};
use super::runtime_keymap::keymap_snapshot;
use super::runtime_options::{
	EffectiveOptionInfo, EffectiveOptionKind, OptionSource, option_snapshot,
};
use crate::core::RegistryRev;
use crate::engine::runtime_registry::RuntimeRegistries;
use crate::engine::{
	THEME_SPECS, ThemeFamily, UI_COLOR_SPECS, UiColorOverrides, UiColorSpec, supported_languages,
};

const MAX_WIDTH: usize = 80;

const CATEGORY_ORDER: &[CommandCategory] = &[
	CommandCategory::File,
	CommandCategory::Tabs,
	CommandCategory::Panes,
	CommandCategory::View,
	CommandCategory::Appearance,
	CommandCategory::Language,
	CommandCategory::EditOps,
	CommandCategory::Settings,
	CommandCategory::Help,
	CommandCategory::Session,
];

/// Pane title for in-app help documents. One title means one reused pane:
/// every :help invocation replaces the same split.
pub(super) const HELP_DOCUMENT_TITLE: &str = "Baryon Help";

type PlainHelpRenderer = fn(&str) -> String;
type MarkdownHelpRenderer = fn() -> String;

/// A general help page. Keeping its names and both renderers together makes
/// an addressable-but-undocumented general topic unrepresentable.
pub(super) struct GeneralHelpSpec {
	pub(super) name: &'static str,
	pub(super) aliases: &'static [&'static str],
	pub(super) summary: &'static str,
	render_plain: PlainHelpRenderer,
	render_markdown: MarkdownHelpRenderer,
}

pub(super) static GENERAL_HELP_TOPICS: &[GeneralHelpSpec] = &[
	GeneralHelpSpec {
		name: "overview",
		aliases: &[""],
		summary: "Overview of Baryon commands and help",
		render_plain: render_overview,
		render_markdown: markdown_overview,
	},
	GeneralHelpSpec {
		name: "commands",
		aliases: &[],
		summary: "All ex-commands",
		render_plain: render_commands_topic,
		render_markdown: markdown_commands,
	},
	GeneralHelpSpec {
		name: "keys",
		aliases: &["keybindings", "keymap"],
		summary: "All keybindings by mode",
		render_plain: render_keys_topic,
		render_markdown: markdown_keys,
	},
	GeneralHelpSpec {
		name: "panes",
		aliases: &["splits"],
		summary: "Pane focus, splits, resizing, and mouse controls",
		render_plain: render_panes_topic,
		render_markdown: markdown_panes,
	},
	GeneralHelpSpec {
		name: "vim",
		aliases: &["motions"],
		summary: "Vim-style cursor and selection motions",
		render_plain: render_vim_topic,
		render_markdown: markdown_vim,
	},
	GeneralHelpSpec {
		name: "markdown",
		aliases: &[],
		summary: "Common Markdown syntax and examples",
		render_plain: render_markdown_topic,
		render_markdown: markdown_markdown,
	},
	GeneralHelpSpec {
		name: "themes",
		aliases: &[],
		summary: "Available color themes and modifiers",
		render_plain: render_themes_topic,
		render_markdown: markdown_themes,
	},
	GeneralHelpSpec {
		name: "languages",
		aliases: &[],
		summary: "Supported syntax languages and file extensions",
		render_plain: render_languages_topic,
		render_markdown: markdown_languages,
	},
	GeneralHelpSpec {
		name: "set",
		aliases: &["settings", "options"],
		summary: "Persistent settings and options",
		render_plain: render_set_topic,
		render_markdown: markdown_set,
	},
];

pub(super) enum HelpTopic {
	General(&'static GeneralHelpSpec),
	Command(&'static CommandSpec),
	RuntimeCommand(EffectiveCommandInfo),
	RuntimeOption(EffectiveOptionInfo),
	BoolOption(&'static BoolOptionSpec),
	ValueOption(&'static ValueOptionSpec),
	UiColor(&'static UiColorSpec),
}

pub(super) struct ResolvedHelpTopic {
	pub(super) registry_revision: RegistryRev,
	topic: HelpTopic,
	runtime_commands: Vec<EffectiveCommandInfo>,
	runtime_options: Vec<EffectiveOptionInfo>,
	runtime_key_docs: Vec<KeyDoc>,
}

/// Resolve a topic name: a general topic, a command name or alias, or a
/// settings option (including its `no` forms).
pub(super) fn resolve_topic(name: &str) -> Result<HelpTopic, String> {
	if let Some(spec) = GENERAL_HELP_TOPICS
		.iter()
		.find(|spec| name == spec.name || spec.aliases.contains(&name))
	{
		return Ok(HelpTopic::General(spec));
	}
	let token = name.strip_prefix(':').unwrap_or(name);
	if matches!(token, "initwrite" | "initw" | "initwrite!" | "initw!") {
		let settings = GENERAL_HELP_TOPICS
			.iter()
			.find(|spec| spec.name == "set")
			.expect("the settings help topic must be registered");
		return Ok(HelpTopic::General(settings));
	}
	for spec in COMMANDS {
		if token == spec.name || spec.aliases.contains(&token) {
			return Ok(HelpTopic::Command(spec));
		}
	}
	for option in BOOL_OPTIONS {
		let is_off_form = token
			.strip_prefix("no")
			.is_some_and(|rest| rest == option.key || option.aliases.contains(&rest))
			|| option.off_aliases.contains(&token);
		if token == option.key || option.aliases.contains(&token) || is_off_form {
			return Ok(HelpTopic::BoolOption(option));
		}
	}
	for option in VALUE_OPTIONS {
		if token == option.key || option.aliases.contains(&token) {
			return Ok(HelpTopic::ValueOption(option));
		}
	}
	for spec in &UI_COLOR_SPECS {
		if token == spec.name || spec.aliases.contains(&token) {
			return Ok(HelpTopic::UiColor(spec));
		}
	}
	let mut message = format!("Unknown help topic: {name}");
	if let Some(suggestion) = suggest(token) {
		message.push_str(&format!(" (did you mean '{suggestion}'?)"));
	}
	Err(message)
}

pub(super) fn resolve_topic_with_runtime(
	name: &str,
	registries: &RuntimeRegistries,
) -> Result<ResolvedHelpTopic, String> {
	let snapshot = command_snapshot(registries);
	let option_snapshot = option_snapshot(registries);
	let keymap_snapshot = keymap_snapshot(registries);
	if let Ok(topic) = resolve_topic(name) {
		return Ok(ResolvedHelpTopic {
			registry_revision: snapshot.revision,
			topic,
			runtime_commands: runtime_commands(snapshot.commands),
			runtime_options: runtime_options(option_snapshot.options),
			runtime_key_docs: keymap_snapshot.docs,
		});
	}
	let token = name.strip_prefix(':').unwrap_or(name);
	if let Some(command) = snapshot.commands.iter().find(|command| {
		matches!(command.source, CommandSource::Runtime(_))
			&& (command.name == token || command.aliases.iter().any(|alias| alias == token))
	}) {
		return Ok(ResolvedHelpTopic {
			registry_revision: snapshot.revision,
			topic: HelpTopic::RuntimeCommand(command.clone()),
			runtime_commands: runtime_commands(snapshot.commands),
			runtime_options: runtime_options(option_snapshot.options),
			runtime_key_docs: keymap_snapshot.docs,
		});
	}
	let option_token = token.trim_end_matches('?');
	if let Some(option) = option_snapshot
		.options
		.iter()
		.find(|option| {
			matches!(option.source, OptionSource::Runtime(_))
				&& (option.name == option_token
					|| option.aliases.iter().any(|alias| alias == option_token)
					|| (option.kind == EffectiveOptionKind::Bool
						&& option_token.strip_prefix("no").is_some_and(|positive| {
							option.name == positive
								|| option.aliases.iter().any(|alias| alias == positive)
						})))
		})
		.cloned()
	{
		return Ok(ResolvedHelpTopic {
			registry_revision: option_snapshot.revision,
			topic: HelpTopic::RuntimeOption(option),
			runtime_commands: runtime_commands(snapshot.commands),
			runtime_options: runtime_options(option_snapshot.options),
			runtime_key_docs: keymap_snapshot.docs,
		});
	}
	let mut message = format!("Unknown help topic: {name}");
	if let Some(suggestion) = super::runtime_catalog::suggest(registries, token)
		.or_else(|| super::runtime_options::suggest(registries, option_token))
	{
		message.push_str(&format!(" (did you mean '{suggestion}'?)"));
	}
	Err(message)
}

fn runtime_options(options: Vec<EffectiveOptionInfo>) -> Vec<EffectiveOptionInfo> {
	options
		.into_iter()
		.filter(|option| matches!(option.source, OptionSource::Runtime(_)))
		.collect()
}

fn runtime_commands(commands: Vec<EffectiveCommandInfo>) -> Vec<EffectiveCommandInfo> {
	commands
		.into_iter()
		.filter(|command| matches!(command.source, CommandSource::Runtime(_)))
		.collect()
}

/// Compact list of what `--help TOPIC` accepts, shown after an unknown topic.
pub(super) fn topic_hint() -> String {
	let names = GENERAL_HELP_TOPICS
		.iter()
		.filter(|spec| spec.name != "overview")
		.map(|spec| spec.name)
		.collect::<Vec<_>>()
		.join(", ");
	format!("Topics: {names};\n        or a command/option name (e.g. 'sort')")
}

pub(super) fn render_plain(topic: &HelpTopic, program_name: &str) -> String {
	match topic {
		HelpTopic::General(spec) => (spec.render_plain)(program_name),
		HelpTopic::Command(spec) => render_command(spec),
		HelpTopic::RuntimeCommand(command) => render_runtime_command(command),
		HelpTopic::RuntimeOption(option) => render_runtime_option(option),
		HelpTopic::BoolOption(option) => render_bool_option(option),
		HelpTopic::ValueOption(option) => render_value_option(option),
		HelpTopic::UiColor(spec) => render_ui_color(spec),
	}
}

/// Render a topic as markdown for the in-app `:help` preview pane.
pub(super) fn render_markdown(topic: &HelpTopic) -> String {
	let mut out = match topic {
		HelpTopic::General(spec) => (spec.render_markdown)(),
		HelpTopic::Command(spec) => markdown_command(spec),
		HelpTopic::RuntimeCommand(command) => markdown_runtime_command(command),
		HelpTopic::RuntimeOption(option) => markdown_runtime_option(option),
		HelpTopic::BoolOption(option) => markdown_bool_option(option),
		HelpTopic::ValueOption(option) => markdown_value_option(option),
		HelpTopic::UiColor(spec) => markdown_ui_color(spec),
	};
	push_line(&mut out, "");
	push_line(&mut out, "---");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Close this help pane with `sc` in normal mode or `:paneclose` / `:pc`.",
	);
	out
}

pub(super) fn render_markdown_with_runtime(topic: &ResolvedHelpTopic) -> String {
	// The revision belongs to this owned snapshot; rendering never consults a
	// newer registry state midway through the document.
	let _snapshot_revision = topic.registry_revision;
	let mut out = match &topic.topic {
		HelpTopic::General(spec) if spec.name == "overview" => {
			markdown_overview_with_runtime(&topic.runtime_commands)
		}
		HelpTopic::General(spec) if spec.name == "commands" => {
			markdown_commands_with_runtime(&topic.runtime_commands)
		}
		HelpTopic::General(spec) if spec.name == "set" => {
			markdown_set_with_runtime(&topic.runtime_options)
		}
		HelpTopic::General(spec) if spec.name == "keys" => {
			markdown_keys_with_docs(&topic.runtime_key_docs)
		}
		HelpTopic::General(spec) if spec.name == "panes" => {
			markdown_panes_with_docs(&topic.runtime_key_docs)
		}
		HelpTopic::General(spec) if spec.name == "vim" => {
			markdown_vim_with_docs(&topic.runtime_key_docs)
		}
		other => render_markdown(other),
	};
	if matches!(
		topic.topic,
		HelpTopic::General(spec)
			if spec.name == "overview"
				|| spec.name == "commands"
				|| spec.name == "set"
				|| spec.name == "keys"
				|| spec.name == "panes"
				|| spec.name == "vim"
	) {
		push_line(&mut out, "");
		push_line(&mut out, "---");
		push_line(&mut out, "");
		push_line(
			&mut out,
			"Close this help pane with `sc` in normal mode or `:paneclose` / `:pc`.",
		);
	}
	out
}

fn render_commands_topic(_program_name: &str) -> String {
	render_commands()
}

fn render_keys_topic(_program_name: &str) -> String {
	render_keys()
}

fn render_panes_topic(_program_name: &str) -> String {
	render_panes()
}

fn render_vim_topic(_program_name: &str) -> String {
	render_vim()
}

fn render_markdown_topic(_program_name: &str) -> String {
	render_markdown_guide()
}

fn render_themes_topic(_program_name: &str) -> String {
	render_themes()
}

fn render_languages_topic(_program_name: &str) -> String {
	render_languages()
}

fn render_set_topic(_program_name: &str) -> String {
	render_set()
}

/// Keybinding docs for one mode, chunked into its contiguous categories.
fn key_category_groups(docs: &[KeyDoc], mode: KeyMode) -> Vec<(Category, Vec<&KeyDoc>)> {
	let mut groups: Vec<(Category, Vec<&KeyDoc>)> = Vec::new();
	for doc in docs.iter().filter(|doc| doc.mode == mode) {
		match groups.last_mut() {
			Some((category, docs)) if *category == doc.category => docs.push(doc),
			_ => groups.push((doc.category, vec![doc])),
		}
	}
	groups
}

fn render_keys() -> String {
	let docs = key_docs();
	render_keys_with_docs(&docs)
}

fn render_keys_with_docs(docs: &[KeyDoc]) -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon keybindings");
	for mode in KEY_MODE_ORDER {
		push_line(&mut out, "");
		push_line(&mut out, &format!("{}:", mode.label()));
		for (category, docs) in key_category_groups(docs, *mode) {
			push_line(&mut out, &format!("  {}:", category.label()));
			let rows: Vec<(String, String)> = docs
				.iter()
				.map(|doc| (format!("  {}", doc.keys), doc.summary.to_string()))
				.collect();
			push_table(&mut out, &rows);
		}
	}
	out
}

fn markdown_keys() -> String {
	let docs = key_docs();
	markdown_keys_with_docs(&docs)
}

fn markdown_keys_with_docs(docs: &[KeyDoc]) -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Keybindings");
	for mode in KEY_MODE_ORDER {
		push_line(&mut out, "");
		push_line(&mut out, &format!("## {}", mode.label()));
		for (category, docs) in key_category_groups(docs, *mode) {
			push_line(&mut out, "");
			push_line(&mut out, &format!("### {}", category.label()));
			push_line(&mut out, "");
			push_line(&mut out, "| Keys | Action |");
			push_line(&mut out, "| --- | --- |");
			for doc in docs {
				push_line(&mut out, &format!("| `{}` | {} |", doc.keys, doc.summary));
			}
		}
	}
	out
}

fn pane_key_docs(docs: &[KeyDoc]) -> Vec<&KeyDoc> {
	docs.iter()
		.filter(|doc| doc.mode == KeyMode::Normal && doc.category == Category::Panes)
		.collect()
}

fn render_panes() -> String {
	let docs = key_docs();
	render_panes_with_docs(&docs)
}

fn render_panes_with_docs(docs: &[KeyDoc]) -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon panes");
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"Pane controls work for editor tabs, the file browser, Markdown previews, help, and diff panes. Press s in normal mode to start a pane command.",
	);
	push_line(&mut out, "");
	let rows = pane_key_docs(docs)
		.into_iter()
		.map(|doc| (doc.keys.clone(), doc.summary.to_string()))
		.collect::<Vec<_>>();
	push_table(&mut out, &rows);
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"For repeated resizing, press sr, then use h/j/k/l until Esc. You can also drag the blank divider between two panes with the left mouse button.",
	);
	out
}

fn markdown_panes() -> String {
	let docs = key_docs();
	markdown_panes_with_docs(&docs)
}

fn markdown_panes_with_docs(docs: &[KeyDoc]) -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Panes");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Pane controls work for editor tabs, the file browser, Markdown previews, help, and diff panes. Press `s` in normal mode to start a pane command.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "| Keys | Action |");
	push_line(&mut out, "| --- | --- |");
	for doc in pane_key_docs(docs) {
		push_line(&mut out, &format!("| `{}` | {} |", doc.keys, doc.summary));
	}
	push_line(&mut out, "");
	push_line(
		&mut out,
		"For repeated resizing, press `sr`, then use `h/j/k/l` until `Esc`. You can also drag the blank divider between two panes with the left mouse button.",
	);
	out
}

fn motion_docs(docs: &[KeyDoc], mode: KeyMode) -> Vec<&KeyDoc> {
	docs.iter()
		.filter(|doc| doc.mode == mode && doc.category == Category::Motion)
		.collect()
}

fn render_vim() -> String {
	let docs = key_docs();
	render_vim_with_docs(&docs)
}

fn render_vim_with_docs(docs: &[KeyDoc]) -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon Vim motions");
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"These Vim-style motions move the cursor in normal mode and extend the selection in visual mode.",
	);
	for mode in [KeyMode::Normal, KeyMode::Visual] {
		push_line(&mut out, "");
		push_line(&mut out, &format!("{}:", mode.label()));
		let rows = motion_docs(docs, mode)
			.into_iter()
			.map(|doc| (doc.keys.clone(), doc.summary.to_string()))
			.collect::<Vec<_>>();
		push_table(&mut out, &rows);
	}
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"Word motions w, b, and e also combine with d, c, and y (for example dw, cb, and ye). See ':help keys' for editing operators and every mode.",
	);
	out
}

fn markdown_vim() -> String {
	let docs = key_docs();
	markdown_vim_with_docs(&docs)
}

fn markdown_vim_with_docs(docs: &[KeyDoc]) -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Vim Motions");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"These Vim-style motions move the cursor in normal mode and extend the selection in visual mode. This page is generated from Baryon's keymap.",
	);
	for mode in [KeyMode::Normal, KeyMode::Visual] {
		push_line(&mut out, "");
		push_line(&mut out, &format!("## {}", mode.label()));
		push_line(&mut out, "");
		push_line(&mut out, "| Keys | Motion |");
		push_line(&mut out, "| --- | --- |");
		for doc in motion_docs(docs, mode) {
			push_line(&mut out, &format!("| `{}` | {} |", doc.keys, doc.summary));
		}
	}
	push_line(&mut out, "");
	push_line(&mut out, "## Motions with operators");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Word motions `w`, `b`, and `e` also combine with delete (`d`), change (`c`), and yank (`y`), for example `dw`, `cb`, and `ye`. See `:help keys` for editing operators, text objects, and every mode.",
	);
	out
}

const MARKDOWN_INLINE_EXAMPLES: &[(&str, &str, &str)] = &[
	("Italic", "*italic*", "*italic*"),
	("Bold", "**bold**", "**bold**"),
	("Bold italic", "***bold italic***", "***bold italic***"),
	("Monospace", "`monospace`", "`monospace`"),
	("Strikethrough", "~~removed~~", "~~removed~~"),
	(
		"Link",
		"[Baryon](https://example.com)",
		"[Baryon](https://example.com)",
	),
	(
		"Image",
		"![Alt text](path/to/image.png)",
		"image with fallback alt text",
	),
];

const MARKDOWN_HEADING_EXAMPLES: &[&str] = &["# Heading 1", "## Heading 2", "### Heading 3"];
const MARKDOWN_LIST_EXAMPLES: &[&str] = &[
	"- Bullet item",
	"- Another item",
	"",
	"1. First item",
	"2. Second item",
];
const MARKDOWN_MORE_EXAMPLES: &[&str] = &[
	"> Block quote",
	"",
	"- [ ] Open task",
	"- [x] Completed task",
	"",
	"---",
];

fn markdown_code_span(source: &str) -> String {
	if source.contains('`') {
		format!("`` {source} ``")
	} else {
		format!("`{source}`")
	}
}

fn render_markdown_guide() -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon Markdown quick reference");
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"Use ':mdpreview' (alias ':mp') to open a live rendered preview of the current Markdown document.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "Headings:");
	for source in MARKDOWN_HEADING_EXAMPLES {
		push_line(&mut out, &format!("  {source}"));
	}
	push_line(&mut out, "");
	push_line(&mut out, "Inline formatting, links, and images:");
	let rows = MARKDOWN_INLINE_EXAMPLES
		.iter()
		.map(|(label, source, _)| ((*label).to_string(), (*source).to_string()))
		.collect::<Vec<_>>();
	push_table(&mut out, &rows);
	push_line(&mut out, "");
	push_line(&mut out, "Bullet and numbered lists:");
	for source in MARKDOWN_LIST_EXAMPLES {
		push_line(&mut out, &format!("  {source}"));
	}
	push_line(&mut out, "");
	push_line(&mut out, "Quotes, tasks, and horizontal rules:");
	for source in MARKDOWN_MORE_EXAMPLES {
		push_line(&mut out, &format!("  {source}"));
	}
	push_line(&mut out, "");
	push_line(&mut out, "Fenced code:");
	push_line(&mut out, "  ```rust");
	push_line(&mut out, "  fn main() {}");
	push_line(&mut out, "  ```");
	push_line(&mut out, "");
	push_line(&mut out, "Tables:");
	push_line(&mut out, "  | Name | Value |");
	push_line(&mut out, "  | --- | --- |");
	push_line(&mut out, "  | alpha | 1 |");
	out
}

fn push_markdown_source_block(out: &mut String, lines: &[&str]) {
	push_line(out, "```markdown");
	for line in lines {
		push_line(out, line);
	}
	push_line(out, "```");
}

fn markdown_markdown() -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Markdown Quick Reference");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Use `:mdpreview` (alias `:mp`) to open a live rendered preview of the current Markdown document.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "## Headings");
	push_line(&mut out, "");
	push_markdown_source_block(&mut out, MARKDOWN_HEADING_EXAMPLES);
	push_line(&mut out, "");
	push_line(&mut out, "## Inline formatting, links, and images");
	push_line(&mut out, "");
	push_line(&mut out, "| Element | Write | Result |");
	push_line(&mut out, "| --- | --- | --- |");
	for (label, source, result) in MARKDOWN_INLINE_EXAMPLES {
		push_line(
			&mut out,
			&format!("| {label} | {} | {result} |", markdown_code_span(source)),
		);
	}
	push_line(&mut out, "");
	push_line(&mut out, "## Bullet and numbered lists");
	push_line(&mut out, "");
	push_markdown_source_block(&mut out, MARKDOWN_LIST_EXAMPLES);
	push_line(&mut out, "");
	push_line(&mut out, "## Quotes, tasks, and horizontal rules");
	push_line(&mut out, "");
	push_markdown_source_block(&mut out, MARKDOWN_MORE_EXAMPLES);
	push_line(&mut out, "");
	push_line(&mut out, "## Fenced code");
	push_line(&mut out, "");
	push_line(&mut out, "````markdown");
	push_line(&mut out, "```rust");
	push_line(&mut out, "fn main() {}");
	push_line(&mut out, "```");
	push_line(&mut out, "````");
	push_line(&mut out, "");
	push_line(&mut out, "## Tables");
	push_line(&mut out, "");
	push_markdown_source_block(
		&mut out,
		&["| Name | Value |", "| --- | --- |", "| alpha | 1 |"],
	);
	out
}

fn render_languages() -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon supported languages");
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"Use ':language ID' to override detection for this document. File extensions are also accepted by :language.",
	);
	push_wrapped(
		&mut out,
		"Use ':set language.theme ID THEME' to persist a language-specific syntax theme. The 'langtheme' alias remains accepted; extensions are stored under the canonical ID.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "Language (canonical ID) and accepted extensions:");
	let rows = supported_languages()
		.into_iter()
		.map(|language| {
			(
				format!("{} ({})", language.label, language.id),
				language.extensions.join(", "),
			)
		})
		.collect::<Vec<_>>();
	push_table(&mut out, &rows);
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"Use ':language plain' (also plaintext or txt) to disable syntax highlighting. Remove an override with ':set language.theme ID default' (or ':set nolangtheme ID').",
	);
	out
}

fn markdown_languages() -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Supported Languages");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Baryon detects a language from the file extension. Use `:language ID` to override detection for the focused document; the listed extensions are accepted as aliases.",
	);
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Use `:set language.theme ID THEME` to persist a language-specific syntax theme. The `langtheme` alias remains accepted; extensions are stored under the canonical ID. Remove it with `:set language.theme ID default` or the compatibility form `:set nolangtheme ID`.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "| Language | Canonical ID | Extensions |");
	push_line(&mut out, "| --- | --- | --- |");
	for language in supported_languages() {
		let extensions = language
			.extensions
			.iter()
			.map(|extension| format!("`{extension}`"))
			.collect::<Vec<_>>()
			.join(", ");
		push_line(
			&mut out,
			&format!("| {} | `{}` | {extensions} |", language.label, language.id),
		);
	}
	push_line(&mut out, "");
	push_line(&mut out, "## Plain text");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Use `:language plain` (also `plaintext` or `txt`) to disable syntax highlighting for the focused document.",
	);
	out
}

fn theme_names(family: ThemeFamily) -> String {
	THEME_SPECS
		.iter()
		.filter(|spec| spec.family() == family)
		.map(|spec| spec.name)
		.collect::<Vec<_>>()
		.join(", ")
}

fn render_themes() -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon color themes");
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"Use ':theme NAME' for this session or ':set theme NAME' to persist. Tab completes theme names in either form.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "Curated palettes:");
	push_wrapped(&mut out, &theme_names(ThemeFamily::Curated));
	push_line(&mut out, "");
	push_line(&mut out, "Continuous palettes:");
	push_wrapped(&mut out, &theme_names(ThemeFamily::Continuous));

	let aliases: Vec<(String, String)> = THEME_SPECS
		.iter()
		.flat_map(|spec| {
			spec.aliases
				.iter()
				.map(move |alias| ((*alias).to_string(), spec.name.to_string()))
		})
		.collect();
	if !aliases.is_empty() {
		push_line(&mut out, "");
		push_line(&mut out, "Aliases:");
		push_table(&mut out, &aliases);
	}

	push_line(&mut out, "");
	push_line(
		&mut out,
		"Modifiers (append one suffix; flags may be combined):",
	);
	let modifiers = vec![
		("_r".to_string(), "Reverse the palette".to_string()),
		(
			"_c".to_string(),
			"Clip the low end of a continuous palette".to_string(),
		),
		(
			"_C".to_string(),
			"Clip the high end of a continuous palette".to_string(),
		),
		(
			"_sN".to_string(),
			"Rotate assigned colors by N positions".to_string(),
		),
	];
	push_table(&mut out, &modifiers);
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"Examples: viridis_rC, plasma_c, glowmax_rs1. Clip flags have no effect on curated palettes.",
	);
	out
}

fn markdown_themes() -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Color Themes");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Use `:theme NAME` for this session or `:set theme NAME` to persist. Press Tab after a partial name to complete it.",
	);
	for (family, heading) in [
		(ThemeFamily::Curated, "Curated palettes"),
		(ThemeFamily::Continuous, "Continuous palettes"),
	] {
		push_line(&mut out, "");
		push_line(&mut out, &format!("## {heading}"));
		push_line(&mut out, "");
		for spec in THEME_SPECS.iter().filter(|spec| spec.family() == family) {
			let aliases = if spec.aliases.is_empty() {
				String::new()
			} else {
				format!(" (aliases: `{}`)", spec.aliases.join("`, `"))
			};
			push_line(&mut out, &format!("- `{}`{aliases}", spec.name));
		}
	}
	push_line(&mut out, "");
	push_line(&mut out, "## Modifiers");
	push_line(&mut out, "");
	push_line(&mut out, "- `_r`: reverse the palette");
	push_line(
		&mut out,
		"- `_c` / `_C`: clip the low / high end of a continuous palette",
	);
	push_line(&mut out, "- `_sN`: rotate assigned colors by N positions");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Flags may be combined in one suffix, for example `viridis_rC` or `glowmax_rs1`. Clip flags have no effect on curated palettes.",
	);
	out
}

fn markdown_overview() -> String {
	markdown_overview_with_runtime(&[])
}

fn markdown_overview_with_runtime(runtime_commands: &[EffectiveCommandInfo]) -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Help");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Press `:` in normal mode, type a command, then Enter. `:help TOPIC` opens a general guide or details for one command or option (for example `:help vim`, `:help sort`, or `:help wrap`). Close this pane with `:paneclose` / `:pc`.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "## General help");
	push_line(&mut out, "");
	push_line(&mut out, "| Topic | Description |");
	push_line(&mut out, "| --- | --- |");
	for spec in GENERAL_HELP_TOPICS
		.iter()
		.filter(|spec| spec.name != "overview")
	{
		let aliases = if spec.aliases.is_empty() {
			String::new()
		} else {
			format!(" (aliases: `{}`)", spec.aliases.join("`, `"))
		};
		push_line(
			&mut out,
			&format!("| `:help {}`{aliases} | {} |", spec.name, spec.summary),
		);
	}
	push_markdown_command_tables(&mut out, runtime_commands);
	push_line(&mut out, "");
	push_line(&mut out, "## Session toggles");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"`:wrap`, `:nowrap`, `:num`, `:nonum`, `:relnum`, `:norelnum`, `:vis`, \
		 `:novis`, `:minimap`, and `:nominimap` change the view for this session only. \
		 See `:help set` for the persistent forms.",
	);
	out
}

fn markdown_commands() -> String {
	markdown_commands_with_runtime(&[])
}

fn markdown_commands_with_runtime(runtime_commands: &[EffectiveCommandInfo]) -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Commands");
	push_markdown_command_tables(&mut out, runtime_commands);
	out
}

fn push_markdown_command_tables(out: &mut String, runtime_commands: &[EffectiveCommandInfo]) {
	for category in CATEGORY_ORDER {
		let specs: Vec<&CommandSpec> = COMMANDS
			.iter()
			.filter(|spec| spec.category == *category)
			.collect();
		if specs.is_empty() {
			continue;
		}
		push_line(out, "");
		push_line(out, &format!("## {}", category.label()));
		push_line(out, "");
		push_line(out, "| Command | Description |");
		push_line(out, "| --- | --- |");
		for spec in specs {
			let mut summary = spec.summary.to_string();
			if !spec.aliases.is_empty() {
				let aliases: Vec<String> = spec
					.aliases
					.iter()
					.map(|alias| format!("`:{alias}`"))
					.collect();
				summary.push_str(&format!(" ({})", aliases.join(", ")));
			}
			push_line(out, &format!("| `{}` | {} |", spec.usage, summary));
		}
	}
	if !runtime_commands.is_empty() {
		push_line(out, "");
		push_line(out, "## Extensions");
		push_line(out, "");
		push_line(out, "| Command | Description |");
		push_line(out, "| --- | --- |");
		for command in runtime_commands {
			let mut summary = command.summary.clone();
			if !command.aliases.is_empty() {
				let aliases = command
					.aliases
					.iter()
					.map(|alias| format!("`:{alias}`"))
					.collect::<Vec<_>>();
				summary.push_str(&format!(" ({})", aliases.join(", ")));
			}
			push_line(out, &format!("| `{}` | {} |", command.usage, summary));
		}
	}
}

fn markdown_set() -> String {
	markdown_set_with_runtime(&[])
}

fn markdown_set_with_runtime(runtime_options: &[EffectiveOptionInfo]) -> String {
	let mut out = String::new();
	push_line(&mut out, "# Baryon Settings");
	push_line(&mut out, "");
	if runtime_options.is_empty() {
		push_line(
			&mut out,
			"`:set OPTION [VALUE]` changes an option for this session. Put startup \
			 configuration in `init.rn`; `:set initwrite` (or `:set initw`) atomically \
			 exports the current builtin settings there. \
			 VALUE is on or off (also true/false, yes/no); omitting VALUE means on, and \
			 prefixing the option with `no` turns it off, e.g. `:set nowrap`. \
			 The bare forms (`:wrap`, `:nowrap`, ...) change this session only.",
		);
	} else {
		push_line(
			&mut out,
			"`:set OPTION [VALUE]` changes a named option for this session. Builtin and \
			 extension startup configuration belongs in `init.rn`; `:set initwrite` exports \
			 the current builtin settings there. \
			 Boolean VALUE is on or off (also true/false, yes/no); omitting VALUE means \
			 on, and prefixing the option with `no` turns it off.",
		);
	}
	push_line(&mut out, "");
	push_line(&mut out, "## Boolean options");
	push_line(&mut out, "");
	push_line(&mut out, "| Option | Default | Description |");
	push_line(&mut out, "| --- | --- | --- |");
	for option in BOOL_OPTIONS {
		let default = if option.default_on { "on" } else { "off" };
		let aliases = if option.aliases.is_empty() {
			String::new()
		} else {
			format!(" ({})", option.aliases.join(", "))
		};
		push_line(
			&mut out,
			&format!(
				"| `{}`{aliases} | {default} | {} |",
				option.key, option.summary
			),
		);
	}
	push_line(&mut out, "");
	push_line(&mut out, "## Value options");
	push_line(&mut out, "");
	push_line(&mut out, "| Usage | Description |");
	push_line(&mut out, "| --- | --- |");
	for option in VALUE_OPTIONS {
		push_line(
			&mut out,
			&format!("| `{}` | {} |", option.usage, option.summary),
		);
	}
	if !runtime_options.is_empty() {
		push_line(&mut out, "");
		push_line(&mut out, "## Extensions");
		push_line(&mut out, "");
		push_line(
			&mut out,
			"Extension options are session-scoped. They are configured by `init.rn` and are never written to a settings file.",
		);
		push_line(&mut out, "");
		push_line(&mut out, "| Usage | Type | Description |");
		push_line(&mut out, "| --- | --- | --- |");
		for option in runtime_options {
			push_line(
				&mut out,
				&format!(
					"| `{}` | {} | {} |",
					option.usage,
					option_kind_label(option.kind),
					option.summary
				),
			);
		}
	}
	push_line(&mut out, "");
	push_line(&mut out, "## UI colors");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Set a color with three or six hex digits, use `default` to resume fallback, or append `?` to query the resolved value and provenance.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "| Color | Built-in | Description |");
	push_line(&mut out, "| --- | --- | --- |");
	let defaults = UiColorOverrides::default();
	for spec in &UI_COLOR_SPECS {
		push_line(
			&mut out,
			&format!(
				"| `{}` | `{}` | {} |",
				spec.name,
				defaults.resolve(spec.key).color.to_hex(),
				spec.summary
			),
		);
	}
	out
}

fn markdown_command(spec: &CommandSpec) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!("# :{} - {}", spec.name, spec.summary));
	push_line(&mut out, "");
	push_line(&mut out, &format!("- **Usage:** `{}`", spec.usage));
	if !spec.aliases.is_empty() {
		let aliases: Vec<String> = spec
			.aliases
			.iter()
			.map(|alias| format!("`:{alias}`"))
			.collect();
		push_line(&mut out, &format!("- **Aliases:** {}", aliases.join(", ")));
	}
	push_line(
		&mut out,
		&format!("- **Category:** {}", spec.category.label()),
	);
	push_line(&mut out, "");
	push_line(&mut out, spec.help);
	out
}

fn markdown_runtime_command(command: &EffectiveCommandInfo) -> String {
	let mut out = String::new();
	push_line(
		&mut out,
		&format!("# :{} - {}", command.name, command.summary),
	);
	push_line(&mut out, "");
	push_line(&mut out, &format!("- **Usage:** `{}`", command.usage));
	if !command.aliases.is_empty() {
		let aliases = command
			.aliases
			.iter()
			.map(|alias| format!("`:{alias}`"))
			.collect::<Vec<_>>();
		push_line(&mut out, &format!("- **Aliases:** {}", aliases.join(", ")));
	}
	push_line(&mut out, "- **Category:** Extensions");
	push_line(&mut out, "");
	push_line(&mut out, &command.help);
	out
}

fn markdown_runtime_option(option: &EffectiveOptionInfo) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!("# {} - {}", option.name, option.summary));
	push_line(&mut out, "");
	push_line(&mut out, &format!("- **Usage:** `{}`", option.usage));
	push_line(
		&mut out,
		&format!("- **Type:** {}", option_kind_label(option.kind)),
	);
	if !option.aliases.is_empty() {
		push_line(
			&mut out,
			&format!("- **Aliases:** `{}`", option.aliases.join("`, `")),
		);
	}
	push_line(&mut out, "- **Query:** append `?` to the name");
	push_line(
		&mut out,
		"- **Persistence:** session only; configure it in `init.rn`",
	);
	if !option.completion_candidates.is_empty() {
		push_line(
			&mut out,
			&format!(
				"- **Values:** `{}`",
				option.completion_candidates.join("`, `")
			),
		);
	}
	push_line(&mut out, "");
	push_line(&mut out, &option.help);
	out
}

fn option_kind_label(kind: EffectiveOptionKind) -> &'static str {
	match kind {
		EffectiveOptionKind::Bool => "boolean",
		EffectiveOptionKind::Integer => "integer",
		EffectiveOptionKind::String { .. } => "string",
		EffectiveOptionKind::BuiltinValue => "value",
		EffectiveOptionKind::UiColor => "color",
	}
}

fn markdown_bool_option(option: &BoolOptionSpec) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!("# {} - {}", option.key, option.summary));
	push_line(&mut out, "");
	if !option.aliases.is_empty() {
		push_line(
			&mut out,
			&format!("- **Aliases:** `{}`", option.aliases.join("`, `")),
		);
	}
	if option.session_on.is_some() {
		push_line(
			&mut out,
			&format!("- **Session:** `:{0}`, `:no{0}`", option.key),
		);
	} else {
		push_line(&mut out, "- **Session:** none; use `:set`");
	}
	push_line(
		&mut out,
		&format!(
			"- **Persist:** `:set {0} [on|off]`, `:set no{0}`",
			option.key
		),
	);
	push_line(
		&mut out,
		&format!(
			"- **Default:** {}",
			if option.default_on { "on" } else { "off" }
		),
	);
	push_wrapped(
		&mut out,
		&format!(
			"- **Configured at startup with:** `baryon::set(editor, \"{}\", value)` in `init.rn`",
			option.key
		),
	);
	out
}

fn markdown_value_option(option: &ValueOptionSpec) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!("# {} - {}", option.key, option.summary));
	push_line(&mut out, "");
	push_line(&mut out, &format!("- **Usage:** `{}`", option.usage));
	if !option.aliases.is_empty() {
		push_line(
			&mut out,
			&format!("- **Aliases:** `{}`", option.aliases.join("`, `")),
		);
	}
	push_line(
		&mut out,
		"- **Session-scoped**; use `init.rn` for startup configuration",
	);
	out
}

fn markdown_ui_color(spec: &UiColorSpec) -> String {
	let resolved = UiColorOverrides::default().resolve(spec.key);
	let mut out = String::new();
	push_line(&mut out, &format!("# {} - {}", spec.name, spec.summary));
	push_line(&mut out, "");
	push_line(
		&mut out,
		&format!("- **Set:** `:set {} HEX|default`", spec.name),
	);
	push_line(&mut out, &format!("- **Query:** `:set {}?`", spec.name));
	push_line(
		&mut out,
		&format!(
			"- **Built-in resolved value:** `{}`",
			resolved.color.to_hex()
		),
	);
	if let Some(parent) = spec.parent {
		push_line(
			&mut out,
			&format!("- **Parent override:** `{}`", parent.canonical_name()),
		);
	}
	if !spec.aliases.is_empty() {
		push_line(
			&mut out,
			&format!("- **Aliases:** `{}`", spec.aliases.join("`, `")),
		);
	}
	out
}

fn render_overview(program_name: &str) -> String {
	let mut out = String::new();
	push_line(
		&mut out,
		&format!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
	);
	push_line(&mut out, "");
	push_line(&mut out, &format!("Usage: {program_name} [OPTIONS] [FILE]"));
	push_line(&mut out, "");
	push_line(&mut out, "Options:");
	push_line(&mut out, "  -h, --help [TOPIC]   Print help and exit");
	push_line(&mut out, "  -v, --version        Print version and exit");
	push_line(&mut out, "");
	push_line(&mut out, "General help topics:");
	let topics = GENERAL_HELP_TOPICS
		.iter()
		.filter(|spec| spec.name != "overview")
		.map(|spec| (spec.name.to_string(), spec.summary.to_string()))
		.collect::<Vec<_>>();
	push_table(&mut out, &topics);
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Ex-commands (press ':' in the editor, then the command name):",
	);
	let rows: Vec<(String, String)> = COMMANDS
		.iter()
		.map(|spec| (format!(":{}", spec.name), spec.summary.to_string()))
		.collect();
	push_table(&mut out, &rows);
	push_line(&mut out, "");
	push_line(
		&mut out,
		&format!("'{program_name} --help TOPIC' prints details for one topic;"),
	);
	push_line(&mut out, &topic_hint());
	out
}

fn render_commands() -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon ex-commands");
	push_line(&mut out, "");
	push_line(
		&mut out,
		"Press ':' in the editor, type the command, then Enter.",
	);
	for category in CATEGORY_ORDER {
		let specs: Vec<&CommandSpec> = COMMANDS
			.iter()
			.filter(|spec| spec.category == *category)
			.collect();
		if specs.is_empty() {
			continue;
		}
		push_line(&mut out, "");
		push_line(&mut out, &format!("{}:", category.label()));
		let rows: Vec<(String, String)> = specs
			.iter()
			.map(|spec| {
				let mut summary = spec.summary.to_string();
				if !spec.aliases.is_empty() {
					let aliases: Vec<String> = spec
						.aliases
						.iter()
						.map(|alias| format!(":{alias}"))
						.collect();
					summary.push_str(&format!(" ({})", aliases.join(", ")));
				}
				(spec.usage.to_string(), summary)
			})
			.collect();
		push_table(&mut out, &rows);
	}
	out
}

fn render_set() -> String {
	let mut out = String::new();
	push_line(&mut out, "Baryon settings");
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"':set OPTION [VALUE]' changes an option for this session. Use ':set initwrite' (or \
		 ':set initw') to atomically export the current builtin settings to init.rn; put other startup configuration there. \
		 VALUE is on or off (also true/false, yes/no); omitting VALUE means on, and \
		 prefixing the option with 'no' turns it off, e.g. ':set nowrap'.",
	);
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"The bare forms (':wrap', ':nowrap', ':num', ...) change the option for \
		 this session only, without persisting.",
	);
	push_line(&mut out, "");
	push_line(&mut out, "Boolean options (default in parentheses):");
	let rows: Vec<(String, String)> = BOOL_OPTIONS
		.iter()
		.map(|option| {
			let default = if option.default_on { "on" } else { "off" };
			(
				format!("{} ({default})", option.key),
				option.summary.to_string(),
			)
		})
		.collect();
	push_table(&mut out, &rows);
	push_line(&mut out, "");
	push_line(&mut out, "Value options (:set only):");
	let rows: Vec<(String, String)> = VALUE_OPTIONS
		.iter()
		.map(|option| (option.usage.to_string(), option.summary.to_string()))
		.collect();
	push_table(&mut out, &rows);
	push_line(&mut out, "");
	push_wrapped(
		&mut out,
		"UI colors accept three or six hex digits. Use 'default' to clear an override, or append '?' to query the resolved value and provenance.",
	);
	let defaults = UiColorOverrides::default();
	let rows: Vec<(String, String)> = UI_COLOR_SPECS
		.iter()
		.map(|spec| {
			(
				format!(
					"{} ({})",
					spec.name,
					defaults.resolve(spec.key).color.to_hex()
				),
				spec.summary.to_string(),
			)
		})
		.collect();
	push_table(&mut out, &rows);
	out
}

fn render_command(spec: &CommandSpec) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!(":{} - {}", spec.name, spec.summary));
	push_line(&mut out, "");
	push_line(&mut out, &format!("Usage:    {}", spec.usage));
	if !spec.aliases.is_empty() {
		let aliases: Vec<String> = spec
			.aliases
			.iter()
			.map(|alias| format!(":{alias}"))
			.collect();
		push_line(&mut out, &format!("Aliases:  {}", aliases.join(", ")));
	}
	push_line(&mut out, &format!("Category: {}", spec.category.label()));
	push_line(&mut out, "");
	push_wrapped(&mut out, spec.help);
	out
}

fn render_runtime_command(command: &EffectiveCommandInfo) -> String {
	let mut out = String::new();
	push_line(
		&mut out,
		&format!(":{} - {}", command.name, command.summary),
	);
	push_line(&mut out, "");
	push_line(&mut out, &format!("Usage:    {}", command.usage));
	if !command.aliases.is_empty() {
		let aliases = command
			.aliases
			.iter()
			.map(|alias| format!(":{alias}"))
			.collect::<Vec<_>>();
		push_line(&mut out, &format!("Aliases:  {}", aliases.join(", ")));
	}
	push_line(&mut out, "Category: Extensions");
	push_line(&mut out, "");
	push_wrapped(&mut out, &command.help);
	out
}

fn render_runtime_option(option: &EffectiveOptionInfo) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!("{} - {}", option.name, option.summary));
	push_line(&mut out, "");
	push_line(&mut out, &format!("Usage:       {}", option.usage));
	push_line(
		&mut out,
		&format!("Type:        {}", option_kind_label(option.kind)),
	);
	if !option.aliases.is_empty() {
		push_line(
			&mut out,
			&format!("Aliases:     {}", option.aliases.join(", ")),
		);
	}
	push_line(
		&mut out,
		"Persistence: session only; configure it in init.rn",
	);
	push_line(&mut out, "");
	push_wrapped(&mut out, &option.help);
	out
}

fn render_bool_option(option: &BoolOptionSpec) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!("{} - {}", option.key, option.summary));
	push_line(&mut out, "");
	if !option.aliases.is_empty() {
		push_line(
			&mut out,
			&format!("Aliases:  {}", option.aliases.join(", ")),
		);
	}
	if option.session_on.is_some() {
		push_line(&mut out, &format!("Session:  :{0}, :no{0}", option.key));
	} else {
		push_line(&mut out, "Session:  (no session form; use :set)");
	}
	push_line(
		&mut out,
		&format!("Persist:  :set {0} [on|off], :set no{0}", option.key),
	);
	push_line(
		&mut out,
		&format!("Default:  {}", if option.default_on { "on" } else { "off" }),
	);
	push_wrapped(
		&mut out,
		&format!(
			"Configure at startup with baryon::set(editor, '{}', value) in init.rn.",
			option.key
		),
	);
	out
}

fn render_value_option(option: &ValueOptionSpec) -> String {
	let mut out = String::new();
	push_line(&mut out, &format!("{} - {}", option.key, option.summary));
	push_line(&mut out, "");
	push_line(&mut out, &format!("Usage:    {}", option.usage));
	if !option.aliases.is_empty() {
		push_line(
			&mut out,
			&format!("Aliases:  {}", option.aliases.join(", ")),
		);
	}
	push_line(
		&mut out,
		"Session-scoped; use init.rn for startup configuration.",
	);
	out
}

fn render_ui_color(spec: &UiColorSpec) -> String {
	let resolved = UiColorOverrides::default().resolve(spec.key);
	let mut out = String::new();
	push_line(&mut out, &format!("{} - {}", spec.name, spec.summary));
	push_line(&mut out, "");
	push_line(
		&mut out,
		&format!("Set:      :set {} HEX|default", spec.name),
	);
	push_line(&mut out, &format!("Query:    :set {}?", spec.name));
	push_line(&mut out, &format!("Built-in: {}", resolved.color.to_hex()));
	if let Some(parent) = spec.parent {
		push_line(&mut out, &format!("Parent:   {}", parent.canonical_name()));
	}
	if !spec.aliases.is_empty() {
		push_line(&mut out, &format!("Aliases:  {}", spec.aliases.join(", ")));
	}
	out
}

fn push_line(out: &mut String, line: &str) {
	out.push_str(line);
	out.push('\n');
}

/// Append a two-column table, aligning the second column after the widest
/// entry in the first.
fn push_table(out: &mut String, rows: &[(String, String)]) {
	let width = rows.iter().map(|(left, _)| left.len()).max().unwrap_or(0);
	for (left, right) in rows {
		push_line(out, &format!("  {left:width$}  {right}"));
	}
}

/// Append `text` greedily word-wrapped to the maximum help width.
fn push_wrapped(out: &mut String, text: &str) {
	let mut line = String::new();
	for word in text.split_whitespace() {
		if !line.is_empty() && line.len() + 1 + word.len() > MAX_WIDTH {
			push_line(out, &line);
			line.clear();
		}
		if !line.is_empty() {
			line.push(' ');
		}
		line.push_str(word);
	}
	if !line.is_empty() {
		push_line(out, &line);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashSet;

	fn topic(name: &str) -> HelpTopic {
		resolve_topic(name).unwrap_or_else(|error| panic!("{name}: {error}"))
	}

	fn assert_within_width(rendered: &str, context: &str) {
		for line in rendered.lines() {
			assert!(
				line.chars().count() <= MAX_WIDTH,
				"{context}: line exceeds {MAX_WIDTH} columns: {line:?}"
			);
		}
	}

	#[test]
	fn every_command_and_alias_resolves_to_a_topic() {
		for spec in COMMANDS {
			if spec.name.chars().all(|ch| ch.is_ascii_alphanumeric()) {
				assert!(resolve_topic(spec.name).is_ok(), "{} unresolved", spec.name);
			}
			for alias in spec.aliases {
				assert!(resolve_topic(alias).is_ok(), "{alias} unresolved");
			}
		}
	}

	#[test]
	fn every_general_topic_and_alias_resolves_to_its_registered_page() {
		let mut names = HashSet::new();
		for spec in GENERAL_HELP_TOPICS {
			for name in std::iter::once(spec.name).chain(spec.aliases.iter().copied()) {
				assert!(names.insert(name), "duplicate general help name: {name:?}");
				let HelpTopic::General(resolved) = topic(name) else {
					panic!("{name:?} did not resolve to a general help page");
				};
				assert!(std::ptr::eq(spec, resolved));
			}
		}
	}

	#[test]
	fn initwrite_help_aliases_resolve_to_settings() {
		for name in ["initwrite", "initw", "initwrite!", "initw!"] {
			let HelpTopic::General(spec) = topic(name) else {
				panic!("{name} should resolve to the settings help page");
			};
			assert_eq!(spec.name, "set");
		}
		let rendered = render_markdown(&topic("initw"));
		assert!(rendered.contains(":set initwrite"));
	}

	#[test]
	fn every_option_form_resolves_to_a_topic() {
		for option in BOOL_OPTIONS {
			for form in [option.key.to_string(), format!("no{}", option.key)] {
				assert!(
					matches!(resolve_topic(&form), Ok(HelpTopic::BoolOption(_))),
					"{form} unresolved"
				);
			}
			for alias in option.aliases {
				for form in [alias.to_string(), format!("no{alias}")] {
					assert!(
						matches!(resolve_topic(&form), Ok(HelpTopic::BoolOption(_))),
						"{form} unresolved"
					);
				}
			}
			for alias in option.off_aliases {
				assert!(
					matches!(resolve_topic(alias), Ok(HelpTopic::BoolOption(_))),
					"{alias} unresolved"
				);
			}
		}
		for option in VALUE_OPTIONS {
			assert!(
				resolve_topic(option.key).is_ok(),
				"{} unresolved",
				option.key
			);
			for alias in option.aliases {
				assert!(resolve_topic(alias).is_ok(), "{alias} unresolved");
			}
		}
		for spec in &UI_COLOR_SPECS {
			assert!(
				matches!(resolve_topic(spec.name), Ok(HelpTopic::UiColor(_))),
				"{} unresolved",
				spec.name
			);
			for alias in spec.aliases {
				assert!(resolve_topic(alias).is_ok(), "{alias} unresolved");
			}
		}
	}

	#[test]
	fn overview_names_every_command() {
		let rendered = render_plain(&topic("overview"), "baryon");
		for spec in COMMANDS {
			assert!(
				rendered.contains(&format!(":{}", spec.name)),
				"overview is missing :{}",
				spec.name
			);
		}
	}

	#[test]
	fn overview_names_every_registered_general_topic() {
		let plain = render_plain(&topic("overview"), "baryon");
		let markdown = render_markdown(&topic("overview"));
		for spec in GENERAL_HELP_TOPICS
			.iter()
			.filter(|spec| spec.name != "overview")
		{
			assert!(
				plain.contains(spec.name),
				"plain overview missing {}",
				spec.name
			);
			assert!(
				markdown.contains(spec.name),
				"markdown overview missing {}",
				spec.name
			);
		}
	}

	#[test]
	fn settings_help_is_derived_from_the_ui_color_registry() {
		let topic = topic("set");
		let plain = render_plain(&topic, "baryon");
		let markdown = render_markdown(&topic);
		for spec in &UI_COLOR_SPECS {
			assert!(
				plain.contains(spec.name),
				"plain settings missing {}",
				spec.name
			);
			assert!(
				markdown.contains(spec.name),
				"markdown settings missing {}",
				spec.name
			);
		}
	}

	#[test]
	fn all_topic_pages_fit_in_eighty_columns() {
		for spec in GENERAL_HELP_TOPICS {
			assert_within_width(
				&render_plain(&HelpTopic::General(spec), "baryon"),
				spec.name,
			);
		}
		for spec in COMMANDS {
			assert_within_width(
				&render_plain(&HelpTopic::Command(spec), "baryon"),
				spec.name,
			);
		}
		for option in BOOL_OPTIONS {
			assert_within_width(
				&render_plain(&HelpTopic::BoolOption(option), "baryon"),
				option.key,
			);
		}
		for option in VALUE_OPTIONS {
			assert_within_width(
				&render_plain(&HelpTopic::ValueOption(option), "baryon"),
				option.key,
			);
		}
		for spec in &UI_COLOR_SPECS {
			assert_within_width(
				&render_plain(&HelpTopic::UiColor(spec), "baryon"),
				spec.name,
			);
		}
	}

	#[test]
	fn theme_catalog_is_derived_from_the_registry() {
		let topic = topic("themes");
		let plain = render_plain(&topic, "baryon");
		let markdown = render_markdown(&topic);
		for spec in THEME_SPECS {
			assert!(plain.contains(spec.name), "plain missing {}", spec.name);
			assert!(
				markdown.contains(spec.name),
				"markdown missing {}",
				spec.name
			);
			for alias in spec.aliases {
				assert!(plain.contains(alias), "plain missing alias {alias}");
				assert!(markdown.contains(alias), "markdown missing alias {alias}");
			}
		}
	}

	#[test]
	fn command_pages_include_usage_aliases_and_body() {
		let Ok(HelpTopic::Command(spec)) = resolve_topic("e") else {
			panic!("expected 'e' to resolve to the edit command");
		};
		let rendered = render_plain(&HelpTopic::Command(spec), "baryon");
		assert!(rendered.contains(":e[!] [PATH]"));
		assert!(rendered.contains(":e"));
		assert!(rendered.contains("reloads the current file"));
	}

	#[test]
	fn every_documented_key_mode_renders_in_both_surfaces() {
		let topic = topic("keys");
		let plain = render_plain(&topic, "baryon");
		let markdown = render_markdown(&topic);
		let docs = key_docs();
		for mode in KEY_MODE_ORDER {
			assert!(
				plain.contains(mode.label()),
				"plain missing {}",
				mode.label()
			);
			assert!(
				markdown.contains(mode.label()),
				"markdown missing {}",
				mode.label()
			);
		}
		for doc in docs {
			assert!(plain.contains(&doc.keys), "plain missing keys {}", doc.keys);
			assert!(
				markdown.contains(&doc.keys),
				"markdown missing keys {}",
				doc.keys
			);
		}
	}

	#[test]
	fn vim_page_and_alias_are_projected_from_motion_key_docs() {
		let HelpTopic::General(vim) = topic("vim") else {
			panic!("vim must be a general help page");
		};
		let HelpTopic::General(motions) = topic("motions") else {
			panic!("motions must be a general help page");
		};
		assert!(std::ptr::eq(vim, motions));

		let page = HelpTopic::General(vim);
		let plain = render_plain(&page, "baryon");
		let markdown = render_markdown(&page);
		for doc in key_docs().into_iter().filter(|doc| {
			matches!(doc.mode, KeyMode::Normal | KeyMode::Visual)
				&& doc.category == Category::Motion
		}) {
			assert!(
				plain.contains(&doc.keys),
				"plain Vim page missing {}",
				doc.keys
			);
			assert!(
				markdown.contains(&doc.keys),
				"markdown Vim page missing {}",
				doc.keys
			);
		}
	}

	#[test]
	fn panes_page_surfaces_keyboard_and_mouse_resizing() {
		let HelpTopic::General(panes) = topic("panes") else {
			panic!("panes must be a general help page");
		};
		let plain = render_plain(&HelpTopic::General(panes), "baryon");
		let markdown = render_markdown(&HelpTopic::General(panes));
		for content in [&plain, &markdown] {
			assert!(content.contains("sr"));
			assert!(content.contains("h/j/k/l"));
			assert!(content.contains("drag"));
		}
	}

	#[test]
	fn markdown_reference_surfaces_share_every_inline_example() {
		let topic = topic("markdown");
		let plain = render_plain(&topic, "baryon");
		let markdown = render_markdown(&topic);
		for (_, source, _) in MARKDOWN_INLINE_EXAMPLES {
			assert!(plain.contains(source), "plain page missing {source}");
			assert!(markdown.contains(source), "markdown page missing {source}");
		}
		for source in MARKDOWN_HEADING_EXAMPLES {
			assert!(plain.contains(source), "plain page missing {source}");
			assert!(markdown.contains(source), "markdown page missing {source}");
		}
	}

	#[test]
	fn language_help_is_derived_from_runtime_provider_metadata() {
		let topic = topic("languages");
		let plain = render_plain(&topic, "baryon");
		let markdown = render_markdown(&topic);
		let languages = supported_languages();
		assert!(!languages.is_empty());
		for language in languages {
			for rendered in [&plain, &markdown] {
				assert!(rendered.contains(language.id), "missing id {}", language.id);
				assert!(
					rendered.contains(language.label),
					"missing label {}",
					language.label
				);
				for extension in language.extensions {
					assert!(
						rendered.contains(extension),
						"missing extension {extension}"
					);
				}
			}
		}
	}

	#[test]
	fn markdown_renders_nonempty_pages_for_every_topic() {
		let mut topics = GENERAL_HELP_TOPICS
			.iter()
			.map(HelpTopic::General)
			.collect::<Vec<_>>();
		topics.extend(COMMANDS.iter().map(HelpTopic::Command));
		topics.extend(BOOL_OPTIONS.iter().map(HelpTopic::BoolOption));
		topics.extend(VALUE_OPTIONS.iter().map(HelpTopic::ValueOption));
		topics.extend(UI_COLOR_SPECS.iter().map(HelpTopic::UiColor));
		for topic in &topics {
			let rendered = render_markdown(topic);
			assert!(rendered.starts_with('#'), "page must start with a heading");
			assert!(rendered.len() > 20, "suspiciously short page: {rendered}");
			assert!(
				rendered.contains("`sc` in normal mode or `:paneclose`"),
				"page is missing its close hint"
			);
		}
	}

	#[test]
	fn markdown_overview_names_every_command() {
		let rendered = render_markdown(&topic("overview"));
		for spec in COMMANDS {
			assert!(
				rendered.contains(&spec.usage.to_string()),
				"markdown overview is missing {}",
				spec.usage
			);
		}
	}

	#[test]
	fn unknown_topics_error_with_suggestion() {
		let Err(error) = resolve_topic("wrp") else {
			panic!("expected 'wrp' to be an unknown topic");
		};
		assert!(error.contains("Unknown help topic: wrp"));
		assert!(error.contains("wrap"));
		let Err(error) = resolve_topic("zzzzz") else {
			panic!("expected 'zzzzz' to be an unknown topic");
		};
		assert!(error.contains("zzzzz"));
	}
}
