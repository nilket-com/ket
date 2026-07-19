//! Declarative registry of ex-commands.
//!
//! Every `:command` is described by a [`CommandSpec`]: its name, aliases,
//! argument expectations, category, and help text, plus the handler that
//! executes it. Dispatch ([`dispatch_ex_command`]), tab completion, and the
//! help surfaces all read from [`COMMANDS`], so a command added here is
//! automatically dispatchable, completable, and documented.

use crate::core::{DocByte, DocLine, DocLineCount};
use crate::engine::runtime_registry::{InvocationSnapshot, RuntimeRegistries};
use crate::engine::{CommandRange, EditorCommand, SplitDirection, SubstituteFlags};

/// Everything a command handler may touch. Handlers deliberately do not get
/// `&mut Frontend`: dispatch only ever sends engine commands or reports a
/// status message, which keeps the registry a `static` table of fn pointers.
pub(super) struct CommandCtx<'a> {
	pub(super) commands: &'a mut Vec<EditorCommand>,
	pub(super) dynamic_invocations: &'a mut Vec<InvocationSnapshot>,
	pub(super) runtime_registries: Option<&'a RuntimeRegistries>,
	pub(super) status: &'a mut Option<String>,
	pub(super) cursor_line: DocLine,
	pub(super) visual_range: Option<(DocByte, DocByte)>,
	/// Set by a quit command. The caller must route this through `Frontend::request_quit`
	/// (which sends `EditorCommand::Quit` itself) and skip the post-dispatch
	/// history/mode bookkeeping, preserving the historical early return.
	pub(super) quit: bool,
}

impl CommandCtx<'_> {
	pub(super) fn send(&mut self, command: EditorCommand) {
		self.commands.push(command);
	}

	pub(super) fn error(&mut self, message: impl Into<String>) {
		*self.status = Some(message.into());
	}
}

/// A parsed command invocation: the trimmed text after the command token and
/// whether a trailing `!` was stripped from the token.
pub(super) struct Invocation<'a> {
	pub(super) args: &'a str,
	pub(super) bang: bool,
}

type Handler = fn(&mut CommandCtx<'_>, Invocation<'_>);

/// Fallback parsers see the whole command buffer and return true if they
/// consumed it. They run, in table order, only after no exact name matched.
type FallbackParser = fn(&mut CommandCtx<'_>, &str) -> bool;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ArgSpec {
	None,
	Optional,
	Required,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ArgCompletion {
	None,
	Path,
	Language,
	Theme,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum CommandCategory {
	File,
	Tabs,
	Panes,
	View,
	Appearance,
	Language,
	EditOps,
	Settings,
	Help,
	Session,
}

impl CommandCategory {
	pub(super) fn label(self) -> &'static str {
		match self {
			CommandCategory::File => "File",
			CommandCategory::Tabs => "Tabs",
			CommandCategory::Panes => "Panes",
			CommandCategory::View => "View",
			CommandCategory::Appearance => "Appearance",
			CommandCategory::Language => "Language",
			CommandCategory::EditOps => "Editing",
			CommandCategory::Settings => "Settings",
			CommandCategory::Help => "Help",
			CommandCategory::Session => "Session",
		}
	}
}

pub(super) enum DispatchKind {
	Exact(Handler),
	Fallback(FallbackParser),
}

pub(super) struct CommandSpec {
	pub(super) name: &'static str,
	pub(super) aliases: &'static [&'static str],
	/// Whether the command accepts a trailing `!` on its name.
	pub(super) bang: bool,
	pub(super) args: ArgSpec,
	pub(super) completion: ArgCompletion,
	pub(super) category: CommandCategory,
	pub(super) usage: &'static str,
	pub(super) summary: &'static str,
	pub(super) help: &'static str,
	pub(super) dispatch: DispatchKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DispatchResult {
	Dispatched,
	Unknown,
}

/// The exact-name entries match on the first whitespace-separated token; the
/// `Fallback` entries at the end are tried in table order afterwards, which
/// preserves the historical precedence: line number, substitute, sort, uniq.
pub(super) static COMMANDS: &[CommandSpec] = &[
	CommandSpec {
		name: "w",
		aliases: &[],
		bang: false,
		args: ArgSpec::Optional,
		completion: ArgCompletion::Path,
		category: CommandCategory::File,
		usage: ":w [PATH]",
		summary: "Write the current file",
		help: "Writes the focused document to disk. With PATH, writes a copy to that path instead.",
		dispatch: DispatchKind::Exact(cmd_write),
	},
	CommandSpec {
		name: "wq",
		aliases: &["x"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::File,
		usage: ":wq",
		summary: "Write the current file and quit",
		help: "Writes the focused document to disk, then exits Baryon.",
		dispatch: DispatchKind::Exact(cmd_write_quit),
	},
	CommandSpec {
		name: "edit",
		aliases: &["e"],
		bang: true,
		args: ArgSpec::Optional,
		completion: ArgCompletion::Path,
		category: CommandCategory::File,
		usage: ":e[!] [PATH]",
		summary: "Reload the current file, or open PATH",
		help: "Without arguments, reloads the current file from disk; add ! to discard unsaved changes. With PATH, opens that file in the focused pane.",
		dispatch: DispatchKind::Exact(cmd_edit),
	},
	CommandSpec {
		name: "source",
		aliases: &[],
		bang: true,
		args: ArgSpec::Required,
		completion: ArgCompletion::Path,
		category: CommandCategory::File,
		usage: ":source[!] PATH",
		summary: "Load or unload a Rune source",
		help: "Without !, loads PATH as a bounded Rune source generation; a successful reload atomically replaces its registrations. With !, unloads that source and retires its registrations.",
		dispatch: DispatchKind::Exact(cmd_source),
	},
	CommandSpec {
		name: "q",
		aliases: &["exit", "quit"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Session,
		usage: ":q",
		summary: "Quit Baryon",
		help: "Exits the editor.",
		dispatch: DispatchKind::Exact(cmd_quit),
	},
	CommandSpec {
		name: "tab",
		aliases: &[],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Tabs,
		usage: ":tab",
		summary: "Open a new empty tab",
		help: "Creates a new document tab with an empty buffer.",
		dispatch: DispatchKind::Exact(cmd_tab_new),
	},
	CommandSpec {
		name: "tabedit",
		aliases: &["tabe", "te"],
		bang: false,
		args: ArgSpec::Required,
		completion: ArgCompletion::Path,
		category: CommandCategory::Tabs,
		usage: ":tabedit PATH",
		summary: "Open PATH in a new tab",
		help: "Creates a new document tab and opens the given file in it.",
		dispatch: DispatchKind::Exact(cmd_tab_edit),
	},
	CommandSpec {
		name: "tabnext",
		aliases: &["tabn", "tn"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Tabs,
		usage: ":tabnext",
		summary: "Switch to the next tab",
		help: "Focuses the next document tab, wrapping around at the end.",
		dispatch: DispatchKind::Exact(cmd_tab_next),
	},
	CommandSpec {
		name: "tabprev",
		aliases: &["tabp", "tp"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Tabs,
		usage: ":tabprev",
		summary: "Switch to the previous tab",
		help: "Focuses the previous document tab, wrapping around at the start.",
		dispatch: DispatchKind::Exact(cmd_tab_prev),
	},
	CommandSpec {
		name: "tabclose",
		aliases: &["tabc", "tc"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Tabs,
		usage: ":tabclose",
		summary: "Close the current tab",
		help: "Closes the focused document tab.",
		dispatch: DispatchKind::Exact(cmd_tab_close),
	},
	CommandSpec {
		name: "panehsplit",
		aliases: &["paneh", "ph"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Panes,
		usage: ":panehsplit",
		summary: "Split the focused pane horizontally",
		help: "Splits the focused pane into top and bottom halves showing the same document.",
		dispatch: DispatchKind::Exact(cmd_pane_hsplit),
	},
	CommandSpec {
		name: "panevsplit",
		aliases: &["panev", "pv"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Panes,
		usage: ":panevsplit",
		summary: "Split the focused pane vertically",
		help: "Splits the focused pane into left and right halves showing the same document.",
		dispatch: DispatchKind::Exact(cmd_pane_vsplit),
	},
	CommandSpec {
		name: "paneclose",
		aliases: &["panec", "pc"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Panes,
		usage: ":paneclose",
		summary: "Close the focused pane",
		help: "Closes the focused pane. The last remaining pane cannot be closed.",
		dispatch: DispatchKind::Exact(cmd_pane_close),
	},
	CommandSpec {
		name: "panediff",
		aliases: &["paned", "pd"],
		bang: false,
		args: ArgSpec::Required,
		completion: ArgCompletion::Path,
		category: CommandCategory::Panes,
		usage: ":panediff PATH",
		summary: "Diff the current file against PATH in a split",
		help: "Opens PATH in a vertical split and highlights the differences against the focused document.",
		dispatch: DispatchKind::Exact(cmd_pane_diff),
	},
	CommandSpec {
		name: "mdpreview",
		aliases: &["mp"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::View,
		usage: ":mdpreview",
		summary: "Preview the current markdown document",
		help: "Opens a live rendered preview of the focused markdown document in a split pane while keeping focus in the editable source. The preview follows edits and scrolling in the source.",
		dispatch: DispatchKind::Exact(cmd_markdown_preview),
	},
	CommandSpec {
		name: "mps",
		aliases: &[],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::View,
		usage: ":mps",
		summary: "Markdown preview with a serif font",
		help: "Like :mdpreview, but renders the preview with a serif font.",
		dispatch: DispatchKind::Exact(cmd_markdown_preview_serif),
	},
	CommandSpec {
		name: "tree",
		aliases: &["files"],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::View,
		usage: ":tree",
		summary: "Open the file browser",
		help: "Opens the file browser in the focused pane. Navigate with j/k, open entries with Enter.",
		dispatch: DispatchKind::Exact(cmd_tree),
	},
	CommandSpec {
		name: "git",
		aliases: &[],
		bang: false,
		args: ArgSpec::Optional,
		completion: ArgCompletion::None,
		category: CommandCategory::View,
		usage: ":git [status]",
		summary: "Open or refresh the Git status panel",
		help: "Opens the Git status output panel and refreshes it. The bare command is equivalent to :git status; diff, log, and blame are reserved for future Git views.",
		dispatch: DispatchKind::Exact(cmd_git),
	},
	CommandSpec {
		name: "theme",
		aliases: &[],
		bang: false,
		args: ArgSpec::Optional,
		completion: ArgCompletion::Theme,
		category: CommandCategory::Appearance,
		usage: ":theme [NAME]",
		summary: "Switch the color theme (this session)",
		help: "With NAME, switches the color theme for this session. Without a name, opens the theme catalog. Use :set theme NAME to persist it.",
		dispatch: DispatchKind::Exact(cmd_theme),
	},
	CommandSpec {
		name: "background",
		aliases: &[],
		bang: false,
		args: ArgSpec::Required,
		completion: ArgCompletion::None,
		category: CommandCategory::Appearance,
		usage: ":background HEX",
		summary: "Set the background color (this session)",
		help: "Sets this pane's editor background to a hex color like #1a2b3c or #F80. Use :set colors.editor.background HEX to change the session palette and persist it; :set background remains an alias.",
		dispatch: DispatchKind::Exact(cmd_background),
	},
	CommandSpec {
		name: "language",
		aliases: &["lang"],
		bang: false,
		args: ArgSpec::Required,
		completion: ArgCompletion::Language,
		category: CommandCategory::Language,
		usage: ":language LANG",
		summary: "Set the document's syntax language",
		help: "Overrides syntax highlighting language detection for the focused document. Use txt, plaintext, or plain to disable highlighting.",
		dispatch: DispatchKind::Exact(cmd_language),
	},
	CommandSpec {
		name: "set",
		aliases: &[],
		bang: true,
		args: ArgSpec::Required,
		completion: ArgCompletion::None,
		category: CommandCategory::Settings,
		usage: ":set OPTION [VALUE]",
		summary: "Change a setting for this session",
		help: "Changes a named option for this session. Use :set initwrite (or :set initw) to atomically update parsed config settings in init.rn while preserving other startup behavior; use :set initwrite! to replace the file. Bare option commands like :wrap also affect the current session.",
		dispatch: DispatchKind::Exact(cmd_set),
	},
	CommandSpec {
		name: "init0",
		aliases: &[],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::Settings,
		usage: ":init0",
		summary: "Replace init.rn with built-in defaults",
		help: "Replaces init.rn with Baryon's generated built-in default configuration. This is an explicit hard reset and does not change the current session.",
		dispatch: DispatchKind::Exact(cmd_init0),
	},
	CommandSpec {
		name: "help",
		aliases: &["h"],
		bang: false,
		args: ArgSpec::Optional,
		completion: ArgCompletion::None,
		category: CommandCategory::Help,
		usage: ":help [TOPIC]",
		summary: "Show help in a preview pane",
		help: "Opens rendered help in a split pane. Bare :help lists every command; :help TOPIC shows one command, option, or guide (e.g. :help panes, :help sort, :help wrap), and :help set describes settings. Scroll with j/k or PageUp/PageDown; close with :paneclose / :pc.",
		dispatch: DispatchKind::Exact(cmd_help),
	},
	// Fallback entries: order is semantic (line number, substitute, sort, uniq).
	CommandSpec {
		name: "{line}",
		aliases: &[],
		bang: false,
		args: ArgSpec::None,
		completion: ArgCompletion::None,
		category: CommandCategory::EditOps,
		usage: ":{line}",
		summary: "Jump to a line number",
		help: "A bare line number jumps the cursor to that line, like :42.",
		dispatch: DispatchKind::Fallback(fb_goto_line),
	},
	CommandSpec {
		name: "s",
		aliases: &[],
		bang: false,
		args: ArgSpec::Required,
		completion: ArgCompletion::None,
		category: CommandCategory::EditOps,
		usage: ":[RANGE]s/PAT/REP/[FLAGS]",
		summary: "Search and replace",
		help: "Substitutes PAT with REP. RANGE is % (whole file), N,M (lines), .,+N (relative), or '<,'> (visual selection); default is the current line. Flags: g (all matches on a line), c (confirm each), i/I (case-insensitive/sensitive).",
		dispatch: DispatchKind::Fallback(fb_substitute),
	},
	CommandSpec {
		name: "sort",
		aliases: &[],
		bang: false,
		args: ArgSpec::Optional,
		completion: ArgCompletion::None,
		category: CommandCategory::EditOps,
		usage: ":[RANGE]sort[!] [FLAGS]",
		summary: "Sort lines",
		help: "Sorts the lines in RANGE (default: whole file). ! reverses the order. Flags: u (unique), n (numeric), i (ignore case).",
		dispatch: DispatchKind::Fallback(fb_sort),
	},
	CommandSpec {
		name: "uniq",
		aliases: &[],
		bang: false,
		args: ArgSpec::Optional,
		completion: ArgCompletion::None,
		category: CommandCategory::EditOps,
		usage: ":[RANGE]uniq [FLAGS]",
		summary: "Remove adjacent duplicate lines",
		help: "Removes adjacent duplicate lines in RANGE (default: whole file). Flag: i (ignore case).",
		dispatch: DispatchKind::Fallback(fb_uniq),
	},
];

/// Dispatch an ex-command buffer against the registry. An empty buffer is a
/// no-op that still counts as dispatched, matching the historical behavior of
/// pressing Enter on an empty prompt.
pub(super) fn dispatch_ex_command(ctx: &mut CommandCtx<'_>, buffer: &str) -> DispatchResult {
	if buffer.is_empty() {
		return DispatchResult::Dispatched;
	}
	let (token, args) = match buffer.find(char::is_whitespace) {
		Some(idx) => (&buffer[..idx], buffer[idx..].trim()),
		None => (buffer, ""),
	};
	for spec in COMMANDS {
		let DispatchKind::Exact(handler) = spec.dispatch else {
			continue;
		};
		let (name_token, bang) = match token.strip_suffix('!') {
			Some(stripped) if spec.bang => (stripped, true),
			_ => (token, false),
		};
		if name_token != spec.name && !spec.aliases.contains(&name_token) {
			continue;
		}
		if !args_match(spec.args, args) {
			ctx.error(format!("Usage: {}", spec.usage));
			return DispatchResult::Dispatched;
		}
		handler(ctx, Invocation { args, bang });
		return DispatchResult::Dispatched;
	}
	if args.is_empty()
		&& let Some(command) = super::option_registry::session_option_command(token)
	{
		ctx.send(command);
		return DispatchResult::Dispatched;
	}
	for spec in COMMANDS {
		let DispatchKind::Fallback(parser) = spec.dispatch else {
			continue;
		};
		if parser(ctx, buffer) {
			return DispatchResult::Dispatched;
		}
	}
	DispatchResult::Unknown
}

fn args_match(spec: ArgSpec, args: &str) -> bool {
	match spec {
		ArgSpec::None => args.is_empty(),
		ArgSpec::Optional => true,
		ArgSpec::Required => !args.is_empty(),
	}
}

/// Names and aliases of every exact command whose argument completes as the
/// given kind, for tab completion.
pub(super) fn completion_command_names(kind: ArgCompletion) -> Vec<&'static str> {
	let mut names = Vec::new();
	for spec in COMMANDS {
		if spec.completion == kind && matches!(spec.dispatch, DispatchKind::Exact(_)) {
			names.push(spec.name);
			names.extend_from_slice(spec.aliases);
		}
	}
	names
}

/// Builtin command spellings protected from the runtime overlay. Fallback
/// names are included so an exact runtime command cannot capture their
/// grammar before the fixed parsers run.
#[allow(dead_code)]
pub(super) fn protected_runtime_tokens() -> Vec<String> {
	let mut tokens = Vec::new();
	for spec in COMMANDS {
		tokens.push(spec.name.to_string());
		tokens.extend(spec.aliases.iter().map(|alias| (*alias).to_string()));
	}
	tokens
}

/// Suggest the closest known command for a mistyped buffer, if any candidate
/// is within an edit distance proportional to the token length.
pub(super) fn suggest(buffer: &str) -> Option<String> {
	suggest_from_candidates(buffer, suggestion_candidates())
}

pub(super) fn suggest_from_candidates(
	buffer: &str,
	candidates: impl IntoIterator<Item = String>,
) -> Option<String> {
	let token = match buffer.find(char::is_whitespace) {
		Some(idx) => &buffer[..idx],
		None => buffer,
	};
	if token.is_empty() {
		return None;
	}
	let max_distance = (token.chars().count() / 3).max(1);
	let mut best: Option<(usize, bool, String)> = None;
	for candidate in candidates {
		let distance = edit_distance(token, &candidate);
		if distance > max_distance || distance >= token.chars().count() {
			continue;
		}
		let prefix_related = candidate.starts_with(token) || token.starts_with(candidate.as_str());
		let better = match &best {
			None => true,
			Some((best_distance, best_prefix, best_candidate)) => {
				distance < *best_distance
					|| (distance == *best_distance && prefix_related && !*best_prefix)
					|| (distance == *best_distance
						&& prefix_related == *best_prefix
						&& candidate.len() < best_candidate.len())
			}
		};
		if better {
			best = Some((distance, prefix_related, candidate));
		}
	}
	best.map(|(_, _, candidate)| candidate)
}

/// Every token a user could actually type as a command: exact names and
/// aliases, typeable fallback names, and the bare session-toggle forms.
pub(super) fn suggestion_candidates() -> Vec<String> {
	let mut candidates = Vec::new();
	for spec in COMMANDS {
		if spec.name.chars().all(|ch| ch.is_ascii_alphanumeric()) {
			candidates.push(spec.name.to_string());
		}
		for alias in spec.aliases {
			candidates.push(alias.to_string());
		}
	}
	for option in super::option_registry::BOOL_OPTIONS {
		if option.session_on.is_some() {
			candidates.push(option.key.to_string());
			for alias in option.aliases {
				candidates.push(alias.to_string());
			}
		}
		if option.session_off.is_some() {
			candidates.push(format!("no{}", option.key));
			for alias in option.aliases {
				candidates.push(format!("no{alias}"));
			}
			for alias in option.off_aliases {
				candidates.push(alias.to_string());
			}
		}
	}
	candidates
}

/// Optimal string alignment distance: Levenshtein plus adjacent
/// transpositions, so swapped letters count as one edit.
fn edit_distance(a: &str, b: &str) -> usize {
	let a: Vec<char> = a.chars().collect();
	let b: Vec<char> = b.chars().collect();
	let mut prev2: Vec<usize> = vec![0; b.len() + 1];
	let mut prev: Vec<usize> = (0..=b.len()).collect();
	let mut curr: Vec<usize> = vec![0; b.len() + 1];
	for i in 0..a.len() {
		curr[0] = i + 1;
		for j in 0..b.len() {
			let cost = if a[i] == b[j] { 0 } else { 1 };
			curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
			if i > 0 && j > 0 && a[i] == b[j - 1] && a[i - 1] == b[j] {
				curr[j + 1] = curr[j + 1].min(prev2[j - 1] + 1);
			}
		}
		std::mem::swap(&mut prev2, &mut prev);
		std::mem::swap(&mut prev, &mut curr);
	}
	prev[b.len()]
}

fn expand_path_arg(arg: &str) -> String {
	crate::core::path::expand_path(arg)
		.to_string_lossy()
		.to_string()
}

fn cmd_write(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	if inv.args.is_empty() {
		ctx.send(EditorCommand::WriteFile);
	} else {
		ctx.send(EditorCommand::WriteFileAs(expand_path_arg(inv.args)));
	}
}

fn cmd_write_quit(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::WriteAndQuit);
}

fn cmd_edit(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	if inv.args.is_empty() {
		ctx.send(EditorCommand::ReloadFile { force: inv.bang });
	} else if inv.bang {
		ctx.error("Usage: :e[!] [PATH]");
	} else {
		ctx.send(EditorCommand::LoadFile(expand_path_arg(inv.args)));
	}
}

fn cmd_source(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	let path = expand_path_arg(inv.args);
	if inv.bang {
		ctx.send(EditorCommand::UnloadRune(path));
	} else {
		ctx.send(EditorCommand::SourceRune(path));
	}
}

fn cmd_quit(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.quit = true;
}

fn cmd_tab_new(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::DocTabNew);
}

fn cmd_tab_edit(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	ctx.send(EditorCommand::DocTabEdit(expand_path_arg(inv.args)));
}

fn cmd_tab_next(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::DocTabNext);
}

fn cmd_tab_prev(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::DocTabPrev);
}

fn cmd_tab_close(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::DocTabClose);
}

fn cmd_pane_hsplit(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::SplitPane(SplitDirection::Horizontal));
}

fn cmd_pane_vsplit(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::SplitPane(SplitDirection::Vertical));
}

fn cmd_pane_close(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::ClosePane);
}

fn cmd_pane_diff(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	ctx.send(EditorCommand::DiffSplit(expand_path_arg(inv.args)));
}

fn cmd_markdown_preview(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::MarkdownPreview { serif: false });
}

fn cmd_markdown_preview_serif(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::MarkdownPreview { serif: true });
}

fn cmd_tree(ctx: &mut CommandCtx<'_>, _inv: Invocation<'_>) {
	ctx.send(EditorCommand::OpenFileBrowser);
}

fn cmd_git(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	match inv.args.trim() {
		"" | "status" => ctx.send(EditorCommand::GitStatus),
		"diff" | "log" | "blame" => ctx.error("Git subcommand not yet implemented"),
		other => ctx.error(format!("Unknown Git subcommand: {other}")),
	}
}

fn cmd_theme(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	if inv.args.is_empty() {
		let topic = super::help::resolve_topic("themes")
			.expect("the themes general-help topic must be registered");
		ctx.send(EditorCommand::ShowMarkdownDocument {
			title: super::help::HELP_DOCUMENT_TITLE.to_string(),
			markdown: super::help::render_markdown(&topic),
		});
	} else {
		ctx.send(EditorCommand::SetTheme(inv.args.to_string()));
	}
}

fn cmd_background(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	ctx.send(EditorCommand::SetBackground(inv.args.to_string()));
}

fn cmd_language(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	if is_plaintext_language_alias(inv.args) {
		ctx.send(EditorCommand::SetLanguage(None));
	} else {
		ctx.send(EditorCommand::SetLanguage(Some(inv.args.to_string())));
	}
}

fn cmd_set(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	let trimmed = inv.args.trim();
	if matches!(trimmed, "initwrite" | "initw" | "initwrite!" | "initw!") {
		ctx.send(EditorCommand::WriteInit {
			force: inv.bang || trimmed.ends_with('!'),
		});
		return;
	}
	let Some(registries) = ctx.runtime_registries else {
		match super::option_registry::parse_set_command(inv.args) {
			Ok(command) => ctx.send(command),
			Err(message) => ctx.error(message),
		}
		return;
	};
	match super::runtime_options::parse_set_command(registries, inv.args) {
		Ok(super::runtime_options::SetAction::Builtin(command)) => ctx.send(command),
		Ok(super::runtime_options::SetAction::Runtime(invocation)) => {
			ctx.dynamic_invocations.push(invocation);
		}
		Err(message) => ctx.error(message),
	}
}

fn cmd_init0(ctx: &mut CommandCtx<'_>, _: Invocation<'_>) {
	ctx.send(EditorCommand::WriteDefaultInit);
}

fn cmd_help(ctx: &mut CommandCtx<'_>, inv: Invocation<'_>) {
	if let Some(registries) = ctx.runtime_registries {
		match super::help::resolve_topic_with_runtime(inv.args, registries) {
			Ok(topic) => ctx.send(EditorCommand::ShowMarkdownDocument {
				title: super::help::HELP_DOCUMENT_TITLE.to_string(),
				markdown: super::help::render_markdown_with_runtime(&topic),
			}),
			Err(message) => ctx.error(message),
		}
	} else {
		match super::help::resolve_topic(inv.args) {
			Ok(topic) => ctx.send(EditorCommand::ShowMarkdownDocument {
				title: super::help::HELP_DOCUMENT_TITLE.to_string(),
				markdown: super::help::render_markdown(&topic),
			}),
			Err(message) => ctx.error(message),
		}
	}
}

fn fb_goto_line(ctx: &mut CommandCtx<'_>, buffer: &str) -> bool {
	let Ok(line_num) = buffer.parse::<u64>() else {
		return false;
	};
	ctx.send(EditorCommand::GotoLine(DocLine::new(
		line_num.saturating_sub(1),
	)));
	true
}

fn fb_substitute(ctx: &mut CommandCtx<'_>, buffer: &str) -> bool {
	if !buffer.contains("s/") {
		return false;
	}
	if let Some((pattern, replacement, flags, range)) =
		parse_substitute(buffer, ctx.cursor_line, ctx.visual_range)
	{
		if flags.confirm {
			ctx.send(EditorCommand::SubstituteConfirm {
				pattern,
				replacement,
				range,
				flags,
			});
		} else {
			ctx.send(EditorCommand::SubstituteAll {
				pattern,
				replacement,
				range,
				flags,
			});
		}
	} else {
		ctx.error("Invalid substitution syntax");
	}
	true
}

fn fb_sort(ctx: &mut CommandCtx<'_>, buffer: &str) -> bool {
	let Some(command) = parse_sort_command(buffer, ctx.cursor_line, ctx.visual_range) else {
		return false;
	};
	ctx.send(command);
	true
}

fn fb_uniq(ctx: &mut CommandCtx<'_>, buffer: &str) -> bool {
	let Some(command) = parse_uniq_command(buffer, ctx.cursor_line, ctx.visual_range) else {
		return false;
	};
	ctx.send(command);
	true
}

pub(super) fn is_plaintext_language_alias(arg: &str) -> bool {
	matches!(arg, "txt" | "plaintext" | "plain")
}

fn parse_command_range(
	cmd: &str,
	body_start: usize,
	cursor_line: DocLine,
	visual_range: Option<(DocByte, DocByte)>,
) -> Option<(&str, CommandRange)> {
	let range_str = &cmd[..body_start];
	let rest = &cmd[body_start..];

	let range = if range_str == "%" {
		CommandRange::WholeFile
	} else if range_str.is_empty() || range_str == "." {
		CommandRange::CurrentLine
	} else if range_str == "'<,'>" {
		let (start_byte, end_byte) = visual_range?;
		CommandRange::Visual {
			start_byte,
			end_byte,
		}
	} else if let Some(offset_str) = range_str.strip_prefix(".,+") {
		let n: u64 = offset_str.parse().ok()?;
		CommandRange::LineRange(cursor_line, cursor_line + DocLineCount::new(n))
	} else if let Some((a_str, b_str)) = range_str.split_once(',') {
		let a: u64 = a_str.parse::<u64>().ok()?.saturating_sub(1);
		let b: u64 = b_str.parse::<u64>().ok()?.saturating_sub(1);
		CommandRange::LineRange(DocLine::new(a), DocLine::new(b))
	} else if let Ok(n) = range_str.parse::<u64>() {
		CommandRange::SingleLine(DocLine::new(n.saturating_sub(1)))
	} else {
		return None;
	};

	Some((rest, range))
}

fn parse_sort_command(
	cmd: &str,
	cursor_line: DocLine,
	visual_range: Option<(DocByte, DocByte)>,
) -> Option<EditorCommand> {
	let kw_pos = cmd.find("sort")?;
	let after = &cmd[kw_pos + 4..];
	let (_rest, mut range) = parse_command_range(cmd, kw_pos, cursor_line, visual_range)?;
	if kw_pos == 0 {
		range = CommandRange::WholeFile;
	}

	let (reverse, flags_str) = if let Some(rest) = after.strip_prefix('!') {
		(true, rest)
	} else {
		(false, after)
	};
	let flags_str = flags_str.trim_start();

	let mut unique = false;
	let mut numeric = false;
	let mut ignore_case = false;
	for c in flags_str.chars() {
		match c {
			'u' => unique = true,
			'n' => numeric = true,
			'i' => ignore_case = true,
			_ => return None,
		}
	}

	Some(EditorCommand::Sort {
		range,
		reverse,
		unique,
		numeric,
		ignore_case,
	})
}

fn parse_uniq_command(
	cmd: &str,
	cursor_line: DocLine,
	visual_range: Option<(DocByte, DocByte)>,
) -> Option<EditorCommand> {
	let kw_pos = cmd.find("uniq")?;
	let after = &cmd[kw_pos + 4..];
	let (_rest, mut range) = parse_command_range(cmd, kw_pos, cursor_line, visual_range)?;
	if kw_pos == 0 {
		range = CommandRange::WholeFile;
	}

	let flags_str = after.trim_start();
	let mut ignore_case = false;
	for c in flags_str.chars() {
		match c {
			'i' => ignore_case = true,
			_ => return None,
		}
	}

	Some(EditorCommand::Uniq { range, ignore_case })
}

fn parse_substitute(
	cmd: &str,
	cursor_line: DocLine,
	visual_range: Option<(DocByte, DocByte)>,
) -> Option<(String, String, SubstituteFlags, CommandRange)> {
	let s_pos = cmd.find("s/")?;
	let (rest, range) = parse_command_range(cmd, s_pos, cursor_line, visual_range)?;
	let rest = &rest[2..];

	let (pattern, rest) = parse_substitute_field(rest, false)?;
	let (replacement, flags_str) = parse_substitute_field(rest, true)?;
	if pattern.is_empty() {
		return None;
	}

	let mut flags = SubstituteFlags::default();
	for c in flags_str.chars() {
		match c {
			'g' => flags.global = true,
			'c' => flags.confirm = true,
			'i' => flags.case_insensitive = true,
			'I' => flags.case_insensitive = false,
			_ => {}
		}
	}

	Some((pattern, replacement, flags, range))
}

fn parse_substitute_field(input: &str, decode_replacement_escapes: bool) -> Option<(String, &str)> {
	let mut value = String::new();
	let mut chars = input.char_indices();

	while let Some((idx, ch)) = chars.next() {
		match ch {
			'/' => return Some((value, &input[idx + 1..])),
			'\\' => {
				let Some((_, next)) = chars.next() else {
					value.push('\\');
					continue;
				};
				match next {
					'/' => value.push('/'),
					't' if decode_replacement_escapes => value.push('\t'),
					'n' if decode_replacement_escapes => value.push('\n'),
					'r' if decode_replacement_escapes => value.push('\r'),
					'\\' if decode_replacement_escapes => value.push('\\'),
					_ => {
						value.push('\\');
						value.push(next);
					}
				}
			}
			_ => value.push(ch),
		}
	}

	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::mem::discriminant;

	type CommandMatcher = fn(&EditorCommand) -> bool;

	fn dispatch(
		buffer: &str,
		visual_range: Option<(DocByte, DocByte)>,
	) -> (DispatchResult, Vec<EditorCommand>, Option<String>, bool) {
		let mut commands = Vec::new();
		let mut status = None;
		let mut ctx = CommandCtx {
			commands: &mut commands,
			dynamic_invocations: &mut Vec::new(),
			runtime_registries: None,
			status: &mut status,
			cursor_line: DocLine::ZERO,
			visual_range,
			quit: false,
		};
		let result = dispatch_ex_command(&mut ctx, buffer);
		let quit = ctx.quit;
		(result, commands, status, quit)
	}

	fn exact_buffer(spec: &CommandSpec, name: &str) -> String {
		match spec.args {
			ArgSpec::Required => format!("{name} x"),
			_ => name.to_string(),
		}
	}

	#[test]
	fn set_initwrite_spellings_dispatch_with_the_advertised_force_semantics() {
		// End-to-end over the ex-command layer: the guard's error message
		// tells the user to run `:set initwrite!`, so that exact spelling
		// (and every documented variant) must produce the matching command.
		for (buffer, force) in [
			("set initwrite", false),
			("set initw", false),
			("set initwrite!", true),
			("set initw!", true),
			("set! initwrite", true),
			("set! initw", true),
		] {
			let (result, commands, status, _) = dispatch(buffer, None);
			assert_eq!(result, DispatchResult::Dispatched, "{buffer} not consumed");
			assert_eq!(status, None, "{buffer} reported an error: {status:?}");
			assert!(
				matches!(
					commands.as_slice(),
					[EditorCommand::WriteInit { force: sent }] if *sent == force
				),
				"{buffer} must dispatch WriteInit {{ force: {force} }}, got {commands:?}"
			);
		}
	}

	#[test]
	fn init0_dispatches_the_explicit_default_configuration_reset() {
		let (result, commands, status, _) = dispatch("init0", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert_eq!(status, None);
		assert!(matches!(commands.as_slice(), [EditorCommand::WriteDefaultInit]));
	}

	#[test]
	fn registry_names_and_aliases_are_unique() {
		let mut seen = std::collections::HashSet::new();
		for spec in COMMANDS {
			assert!(
				seen.insert(spec.name.to_string()),
				"duplicate command name {}",
				spec.name
			);
			for alias in spec.aliases {
				assert!(
					seen.insert(alias.to_string()),
					"duplicate command alias {alias}"
				);
			}
		}
		for option in super::super::option_registry::BOOL_OPTIONS {
			assert!(
				seen.insert(option.key.to_string()),
				"option {} collides with a command name",
				option.key
			);
			assert!(
				seen.insert(format!("no{}", option.key)),
				"option no{} collides with a command name",
				option.key
			);
			for alias in option.aliases {
				assert!(
					seen.insert(alias.to_string()),
					"option alias {alias} collides with a command name"
				);
				assert!(
					seen.insert(format!("no{alias}")),
					"option alias no{alias} collides with a command name"
				);
			}
			for alias in option.off_aliases {
				assert!(
					seen.insert(alias.to_string()),
					"option alias {alias} collides with a command name"
				);
			}
		}
	}

	#[test]
	fn session_toggles_dispatch_from_the_option_table() {
		let cases: [(&str, CommandMatcher); 5] = [
			("wrap", |c| matches!(c, EditorCommand::ToggleWrap)),
			("nowrap", |c| matches!(c, EditorCommand::SetWrap(false))),
			("num", |c| matches!(c, EditorCommand::SetLineNumbers(true))),
			("relnum", |c| {
				matches!(c, EditorCommand::SetRelativeLineNumbers(true))
			}),
			("noviz", |c| {
				matches!(c, EditorCommand::SetVisibleWhitespace(false))
			}),
		];
		for (buffer, matches_expected) in cases {
			let (result, commands, _, _) = dispatch(buffer, None);
			assert_eq!(
				result,
				DispatchResult::Dispatched,
				"{buffer} not dispatched"
			);
			assert_eq!(commands.len(), 1, "{buffer} should send one command");
			assert!(
				matches_expected(&commands[0]),
				"{buffer} sent wrong command"
			);
		}
	}

	#[test]
	fn session_toggles_reject_arguments() {
		let (result, commands, _, _) = dispatch("wrap on", None);
		assert_eq!(result, DispatchResult::Unknown);
		assert!(commands.is_empty());
	}

	#[test]
	fn persist_only_options_have_no_session_toggle() {
		let (result, _, _, _) = dispatch("reload", None);
		assert_eq!(result, DispatchResult::Unknown);
	}

	#[test]
	fn argument_mismatch_reports_usage() {
		for (buffer, usage) in [
			("tab extra", "Usage: :tab"),
			("set", "Usage: :set OPTION [VALUE]"),
		] {
			let (result, commands, status, _) = dispatch(buffer, None);
			assert_eq!(result, DispatchResult::Dispatched, "{buffer} not consumed");
			assert!(commands.is_empty(), "{buffer} should not send commands");
			assert_eq!(status.as_deref(), Some(usage), "{buffer} wrong message");
		}
	}

	#[test]
	fn git_command_defaults_to_status_and_rejects_unimplemented_subcommands() {
		for buffer in ["git", "git status"] {
			let (result, commands, status, _) = dispatch(buffer, None);
			assert_eq!(result, DispatchResult::Dispatched);
			assert!(matches!(commands.as_slice(), [EditorCommand::GitStatus]));
			assert_eq!(status, None);
		}
		let (result, commands, status, _) = dispatch("git diff", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert!(commands.is_empty());
		assert_eq!(
			status.as_deref(),
			Some("Git subcommand not yet implemented")
		);
	}

	#[test]
	fn files_is_an_alias_for_tree() {
		let (result, commands, status, _) = dispatch("files", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert!(matches!(
			commands.as_slice(),
			[EditorCommand::OpenFileBrowser]
		));
		assert_eq!(status, None);
	}

	#[test]
	fn registry_entries_have_help_metadata() {
		for spec in COMMANDS {
			assert!(!spec.usage.is_empty(), "{} has no usage", spec.name);
			assert!(!spec.summary.is_empty(), "{} has no summary", spec.name);
			assert!(!spec.help.is_empty(), "{} has no help body", spec.name);
			assert!(
				spec.usage.starts_with(':'),
				"{} usage must start with ':'",
				spec.name
			);
		}
	}

	#[test]
	fn every_exact_command_dispatches() {
		for spec in COMMANDS {
			if !matches!(spec.dispatch, DispatchKind::Exact(_)) {
				continue;
			}
			let buffer = exact_buffer(spec, spec.name);
			let (result, commands, status, quit) = dispatch(&buffer, None);
			assert_eq!(
				result,
				DispatchResult::Dispatched,
				"{buffer:?} was not dispatched"
			);
			assert!(
				!commands.is_empty() || status.is_some() || quit,
				"{buffer:?} dispatched but had no observable effect"
			);
		}
	}

	#[test]
	fn every_alias_matches_its_canonical_command() {
		for spec in COMMANDS {
			if !matches!(spec.dispatch, DispatchKind::Exact(_)) {
				continue;
			}
			let (_, canonical_commands, canonical_status, canonical_quit) =
				dispatch(&exact_buffer(spec, spec.name), None);
			let canonical_commands: Vec<_> = canonical_commands.iter().map(discriminant).collect();
			for alias in spec.aliases {
				let (result, commands, status, quit) = dispatch(&exact_buffer(spec, alias), None);
				assert_eq!(
					result,
					DispatchResult::Dispatched,
					"alias {alias} was not dispatched"
				);
				let sent: Vec<_> = commands.iter().map(discriminant).collect();
				assert_eq!(
					sent, canonical_commands,
					"alias {alias} sent different commands from {}",
					spec.name
				);
				assert_eq!(
					status, canonical_status,
					"alias {alias} reported a different status from {}",
					spec.name
				);
				assert_eq!(
					quit, canonical_quit,
					"alias {alias} had different quit behavior from {}",
					spec.name
				);
			}
		}
	}

	#[test]
	fn write_with_trailing_space_writes_current_file() {
		let (_, commands, _, _) = dispatch("w ", None);
		assert!(matches!(commands.as_slice(), [EditorCommand::WriteFile]));
	}

	#[test]
	fn write_with_path_writes_file_as() {
		let (_, commands, _, _) = dispatch("w notes.md", None);
		assert!(matches!(
			commands.as_slice(),
			[EditorCommand::WriteFileAs(path)] if path.ends_with("notes.md")
		));
	}

	#[test]
	fn edit_bang_force_reloads() {
		for buffer in ["e!", "edit!"] {
			let (_, commands, _, _) = dispatch(buffer, None);
			assert!(matches!(
				commands.as_slice(),
				[EditorCommand::ReloadFile { force: true }]
			));
		}
	}

	#[test]
	fn bang_is_rejected_on_commands_without_bang_support() {
		let (result, commands, _, _) = dispatch("w!", None);
		assert_eq!(result, DispatchResult::Unknown);
		assert!(commands.is_empty());
	}

	#[test]
	fn quit_sets_quit_flag_without_sending_commands() {
		let (result, commands, _, quit) = dispatch("q", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert!(commands.is_empty());
		assert!(quit);
	}

	#[test]
	fn bare_number_jumps_to_line() {
		let (_, commands, _, _) = dispatch("42", None);
		assert!(matches!(
			commands.as_slice(),
			[EditorCommand::GotoLine(line)] if *line == DocLine::new(41)
		));
	}

	#[test]
	fn substitute_dispatches_whole_file_global() {
		let (_, commands, _, _) = dispatch("%s/a/b/g", None);
		match commands.as_slice() {
			[
				EditorCommand::SubstituteAll {
					pattern,
					replacement,
					range,
					flags,
				},
			] => {
				assert_eq!(pattern, "a");
				assert_eq!(replacement, "b");
				assert!(matches!(range, CommandRange::WholeFile));
				assert!(flags.global);
			}
			other => panic!("expected SubstituteAll, got {other:?}"),
		}
	}

	#[test]
	fn broken_substitute_reports_invalid_syntax() {
		let (result, commands, status, _) = dispatch("s/broken", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert!(commands.is_empty());
		assert_eq!(status.as_deref(), Some("Invalid substitution syntax"));
	}

	#[test]
	fn visual_sort_uses_snapshot_range() {
		let snapshot = Some((DocByte::new(5), DocByte::new(42)));
		let (_, commands, _, _) = dispatch("'<,'>sort", snapshot);
		assert!(matches!(
			commands.as_slice(),
			[EditorCommand::Sort {
				range: CommandRange::Visual { .. },
				..
			}]
		));
	}

	#[test]
	fn visual_sort_without_snapshot_is_unknown() {
		let (result, commands, _, _) = dispatch("'<,'>sort", None);
		assert_eq!(result, DispatchResult::Unknown);
		assert!(commands.is_empty());
	}

	#[test]
	fn sort_with_range_bang_and_flags() {
		let (_, commands, _, _) = dispatch("1,5sort!uni", None);
		match commands.as_slice() {
			[
				EditorCommand::Sort {
					range,
					reverse,
					unique,
					numeric,
					ignore_case,
				},
			] => {
				assert!(matches!(range, CommandRange::LineRange(..)));
				assert!(*reverse && *unique && *numeric && *ignore_case);
			}
			other => panic!("expected Sort, got {other:?}"),
		}
	}

	#[test]
	fn uniq_with_inline_flag() {
		let (_, commands, _, _) = dispatch("%uniqi", None);
		assert!(matches!(
			commands.as_slice(),
			[EditorCommand::Uniq {
				range: CommandRange::WholeFile,
				ignore_case: true,
			}]
		));
	}

	#[test]
	fn empty_buffer_is_a_dispatched_noop() {
		let (result, commands, status, quit) = dispatch("", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert!(commands.is_empty());
		assert!(status.is_none());
		assert!(!quit);
	}

	#[test]
	fn unknown_command_sends_nothing() {
		let (result, commands, _, _) = dispatch("zzz", None);
		assert_eq!(result, DispatchResult::Unknown);
		assert!(commands.is_empty());
	}

	#[test]
	fn path_completion_names_come_from_registry() {
		let names = completion_command_names(ArgCompletion::Path);
		for expected in [
			"w", "e", "edit", "panediff", "paned", "pd", "tabedit", "tabe", "te",
		] {
			assert!(names.contains(&expected), "missing {expected}");
		}
		let language = completion_command_names(ArgCompletion::Language);
		assert_eq!(language, vec!["language", "lang"]);
		let theme = completion_command_names(ArgCompletion::Theme);
		assert_eq!(theme, vec!["theme"]);
	}

	#[test]
	fn help_command_sends_a_markdown_document() {
		for buffer in [
			"help",
			"h",
			"help set",
			"help themes",
			"help vim",
			"help motions",
			"help markdown",
			"help languages",
			"help wrap",
			"help tab",
		] {
			let (result, commands, status, _) = dispatch(buffer, None);
			assert_eq!(
				result,
				DispatchResult::Dispatched,
				"{buffer} not dispatched"
			);
			assert!(
				status.is_none(),
				"{buffer} unexpectedly errored: {status:?}"
			);
			match commands.as_slice() {
				[EditorCommand::ShowMarkdownDocument { title, markdown }] => {
					assert_eq!(title, super::super::help::HELP_DOCUMENT_TITLE);
					assert!(!markdown.is_empty(), "{buffer} produced empty help");
				}
				other => panic!("{buffer} sent {other:?}"),
			}
		}
	}

	#[test]
	fn bare_theme_command_opens_the_theme_catalog() {
		let (result, commands, status, _) = dispatch("theme", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert!(status.is_none());
		let [EditorCommand::ShowMarkdownDocument { markdown, .. }] = commands.as_slice() else {
			panic!("expected the theme catalog");
		};
		assert!(markdown.contains("Color Themes"));
		assert!(markdown.contains("gruvbox"));
	}

	#[test]
	fn help_topic_content_matches_the_request() {
		let (_, commands, _, _) = dispatch("help wrap", None);
		let [EditorCommand::ShowMarkdownDocument { markdown, .. }] = commands.as_slice() else {
			panic!("expected a help document");
		};
		assert!(markdown.contains("wrap"));
		assert!(markdown.contains(":set wrap"));
	}

	#[test]
	fn help_with_unknown_topic_errors_without_sending() {
		let (result, commands, status, _) = dispatch("help zzz", None);
		assert_eq!(result, DispatchResult::Dispatched);
		assert!(commands.is_empty());
		assert!(
			status
				.as_deref()
				.is_some_and(|s| s.contains("Unknown help topic: zzz"))
		);
	}

	#[test]
	fn suggestions_catch_common_typos() {
		assert_eq!(suggest("wrp").as_deref(), Some("wrap"));
		assert_eq!(suggest("tabnxet").as_deref(), Some("tabnext"));
		assert!(suggest("tabclos").is_some_and(|s| s.starts_with("tabc")));
		assert_eq!(suggest("sotr").as_deref(), Some("sort"));
		assert_eq!(suggest("nowarp").as_deref(), Some("nowrap"));
		assert_eq!(suggest("wrap extra").as_deref(), Some("wrap"));
	}

	#[test]
	fn suggestions_stay_quiet_when_nothing_is_close() {
		assert_eq!(suggest("zzzzz"), None);
		assert_eq!(suggest("z"), None);
		assert_eq!(suggest(""), None);
	}

	#[test]
	fn suggestions_never_offer_untypeable_names() {
		for candidate in suggestion_candidates() {
			assert!(
				candidate.chars().all(|ch| ch.is_ascii_alphanumeric()),
				"candidate {candidate} is not typeable"
			);
		}
	}

	#[test]
	fn language_command_plaintext_aliases_clear_language() {
		assert!(is_plaintext_language_alias("txt"));
		assert!(is_plaintext_language_alias("plaintext"));
		assert!(is_plaintext_language_alias("plain"));
		assert!(!is_plaintext_language_alias("rust"));
	}

	#[test]
	fn substitute_parser_handles_escaped_delimiters_and_replacement_escapes() {
		let (pattern, replacement, flags, range) =
			parse_substitute("%s/foo\\/\\t/bar\\/\\n\\\\/gc", DocLine::ZERO, None)
				.expect("substitute should parse");

		assert_eq!(pattern, "foo/\\t");
		assert_eq!(replacement, "bar/\n\\");
		assert!(flags.global);
		assert!(flags.confirm);
		assert!(matches!(range, CommandRange::WholeFile));
	}
}
