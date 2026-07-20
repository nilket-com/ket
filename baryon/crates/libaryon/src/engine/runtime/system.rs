use super::super::document::io::{
	SaveMethod, SaveOutcome, read_document_snapshot, rebind_document_spans_to_saved_file,
	save_document,
};
use super::super::folding::{
	FoldSet, resolve_fold_boundary_at_cursor_tracked, set_subtree_fold_state,
	snap_line_to_visible_boundary,
};
use super::super::protocol::{EditorCommand, VisualKind};
use super::super::session::{
	Affinity, BufferStore, Caret, FileStamp, LayoutNode, SettingsPersistent, TextEditorMut,
	UiState, View, resolve_theme_name,
};
use super::super::settings::SaveDurability;
use super::super::settings::default_init_path;
use super::super::shadow::{ShadowSession, discard_shadow_session, load_shadow_session};
use super::super::undo::UndoLedger;
use super::{CommandEffect, RuntimeContext, SemanticRuntime};
use crate::core::atomic_file::{AtomicWriteOptions, atomic_write};
use crate::core::{DisplayRow, DocByte, DocDisplayCol};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::Theme;
use crate::engine::ui_colors::{UiColorSource, parse_ui_rgb};
use crate::svp::ingest_svp_file;
use rune::ast::{self, Spanned};
use rune::SourceId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::ops::Range;
use std::path::Path;

fn detect_language(
	lang_registry: &super::super::language::LanguageRegistry,
	path: &str,
) -> Option<&'static str> {
	Path::new(path)
		.extension()
		.and_then(|ext| ext.to_str())
		.and_then(|ext| lang_registry.provider_for_extension(ext))
		.map(|provider| provider.language_id())
}

fn file_stamp_for_path(path: &str) -> Option<FileStamp> {
	std::fs::metadata(path)
		.ok()
		.map(|metadata| FileStamp::from_metadata(&metadata))
}

pub(super) fn write_init_file(
	settings: &SettingsPersistent,
	language_registry: &super::super::language::LanguageRegistry,
	force: bool,
) -> Result<std::path::PathBuf, String> {
	let path = default_init_path().ok_or_else(|| "No config directory available".to_string())?;
	write_init_file_at(&path, settings, language_registry, force)?;
	Ok(path)
}

pub(super) fn write_default_init_file(
	language_registry: &super::super::language::LanguageRegistry,
	force: bool,
) -> Result<std::path::PathBuf, String> {
	let path = default_init_path().ok_or_else(|| "No config directory available".to_string())?;
	write_default_init_file_at(&path, language_registry, force)?;
	Ok(path)
}

fn write_default_init_file_at(
	path: &std::path::Path,
	language_registry: &super::super::language::LanguageRegistry,
	force: bool,
) -> Result<(), String> {
	if path.exists() && !force {
		let contents = std::fs::read_to_string(path)
			.map_err(|error| format!("Could not inspect init.rn: {error}"))?;
		if !is_generated_init(&contents) {
			return Err("init.rn contains user code; use :init0! to discard it".to_string());
		}
	}
	write_init_file_at(
		path,
		&SettingsPersistent::builtin_defaults(),
		language_registry,
		true,
	)
}

fn write_init_file_at(
	path: &std::path::Path,
	settings: &SettingsPersistent,
	language_registry: &super::super::language::LanguageRegistry,
	force: bool,
) -> Result<(), String> {
	let contents = if path.exists() {
		Some(
			std::fs::read_to_string(path)
				.map_err(|error| format!("Could not inspect init.rn: {error}"))?,
		)
	} else {
		None
	};
	let source = if let Some(contents) = contents.as_deref().filter(|_| !force) {
		if is_generated_init(contents)
		{
			render_init_file(settings)
		} else {
			update_init_file(&contents, settings, language_registry)?
		}
	} else {
		render_init_file(settings)
	};
	validate_init_source(&source)?;
	if force && contents.as_deref().is_some_and(|contents| !is_generated_init(contents)) {
		write_init_backup(path)?;
	}
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)
			.map_err(|error| format!("Failed to create config directory: {error}"))?;
	}
	atomic_write(path, source.as_bytes(), AtomicWriteOptions::config())
		.map_err(|error| format!("Failed to write init.rn: {error}"))?;
	Ok(())
}

fn is_generated_init(source: &str) -> bool {
	let Ok(file) = parse_init_source(source) else {
		return false;
	};
	let [(item, _)] = file.items.as_slice() else {
		return false;
	};
	let ast::Item::Fn(main) = item else {
		return false;
	};
	if span_text(source, main.name.span) != "main" {
		return false;
	}
	let Some(binding) = config_binding(source, main) else {
		return false;
	};
	let Some((first, rest)) = main.body.statements.split_first() else {
		return false;
	};
	if !matches!(first, ast::Stmt::Local(_)) {
		return false;
	}
	let Some((last, operations)) = rest.split_last() else {
		return false;
	};
	if normalize_rune_source(span_text(source, last.span())) != "Ok(())" {
		return false;
	}
	operations.iter().all(|statement| {
		let ast::Stmt::Semi(statement) = statement else {
			return false;
		};
		is_config_operation(source, &statement.expr, &binding)
	})
}

fn write_init_backup(path: &std::path::Path) -> Result<(), String> {
	let backup = path.with_extension("rn.bak");
	std::fs::copy(path, &backup)
		.map_err(|error| format!("Failed to back up init.rn to {}: {error}", backup.display()))?;
	Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum InitConfigKey {
	Set(String),
	UiColor(String),
	LanguageTheme(String),
}

fn update_init_file(
	source: &str,
	settings: &SettingsPersistent,
	language_registry: &super::super::language::LanguageRegistry,
) -> Result<String, String> {
	let file = parse_init_source(source)?;
	let main = file
		.items
		.iter()
		.find_map(|(item, _)| match item {
			ast::Item::Fn(function) if span_text(source, function.name.span) == "main" => {
				Some(function)
			}
			_ => None,
		})
		.ok_or_else(|| "init.rn has no `main` function to update".to_string())?;
	let terminal = main
		.body
		.statements
		.last()
		.filter(|statement| normalize_rune_source(span_text(source, statement.span())) == "Ok(())")
		.ok_or_else(|| {
			"init.rn main must end with `Ok(())` before :set initwrite can update it".to_string()
		})?;
	let insertion = span_range(terminal.span()).start;
	let binding = config_binding(source, main).ok_or_else(|| {
		"init.rn main has no `let config = editor.config();` binding for :set initwrite to update; use :set initwrite! to replace it"
			.to_string()
	})?;
	let desired = init_config_operations(settings, &binding);
	let configured_keys = desired.keys().cloned().collect::<BTreeSet<_>>();
	let mut patches = Vec::new();
	let mut remaining = desired;

	let mut seen = BTreeSet::new();
	for statement in &main.body.statements {
		let ast::Stmt::Semi(statement) = statement else {
			continue;
		};
		let Some(key) = config_call_key(source, &statement.expr, &binding, language_registry)
		else {
			continue;
		};
		let range = span_range(statement.span());
		if configured_keys.contains(&key) {
			if seen.insert(key.clone()) {
				let replacement = remaining
					.remove(&key)
					.expect("key was checked in the remaining config operations");
				patches.push((range, replacement));
			} else {
				patches.push((tombstone_range(source, range), String::new()));
			}
		}
	}

	if !remaining.is_empty() {
		let additions = render_inserted_operations(source, insertion, remaining.into_values());
		patches.push((insertion..insertion, additions));
	}

	patches.sort_by_key(|(range, _)| std::cmp::Reverse(range.start));
	let mut updated = source.to_string();
	for (range, replacement) in patches {
		updated.replace_range(range, &replacement);
	}
	Ok(updated)
}

fn config_binding(source: &str, main: &ast::ItemFn) -> Option<String> {
	main.body.statements.iter().find_map(|statement| {
		let ast::Stmt::Local(local) = statement else {
			return None;
		};
		let name = span_text(source, local.pat.span());
		if !is_identifier(name)
			|| normalize_rune_source(span_text(source, local.expr.span())) != "editor.config()"
		{
			return None;
		}
		Some(name.to_string())
	})
}

fn init_config_operations(
	settings: &SettingsPersistent,
	binding: &str,
) -> BTreeMap<InitConfigKey, String> {
	let mut operations = BTreeMap::new();
	let mut set = |name: &str, value: String| {
		operations.insert(
			InitConfigKey::Set(name.to_string()),
			format!("{binding}.set({}, {value})?;", rune_string(name)),
		);
	};
	set("theme", rune_string(&settings.theme_name));
	set("minimap", settings.minimap_enabled.to_string());
	set("wrap", settings.wrap_enabled.to_string());
	set("num", settings.line_numbers_enabled.to_string());
	set(
		"relativenumber",
		settings.relative_line_numbers_enabled.to_string(),
	);
	set("vis", settings.visible_whitespace.to_string());
	set("reload", settings.reload_enabled.to_string());
	set("savedurability", rune_string(settings.save_durability.as_str()));
	for key in crate::engine::ui_colors::UiColorKey::ALL {
		if let Some(color) = settings.ui_colors.get(key) {
			let name = key.canonical_name().to_string();
			operations.insert(
				InitConfigKey::UiColor(name.clone()),
				format!(
					"{binding}.ui_color({}, {})?;",
					rune_string(&name),
					rune_string(&color.to_hex())
				),
			);
		}
	}
	for (language, theme) in &settings.language_themes {
		operations.insert(
			InitConfigKey::LanguageTheme(language.clone()),
			format!(
				"{binding}.language_theme({}, {})?;",
				rune_string(language),
				rune_string(theme)
			),
		);
	}
	operations
}

fn config_call_key(
	source: &str,
	expression: &ast::Expr,
	binding: &str,
	language_registry: &super::super::language::LanguageRegistry,
) -> Option<InitConfigKey> {
	let ast::Expr::Try(try_expression) = expression else {
		return None;
	};
	let ast::Expr::Call(call) = try_expression.expr.as_ref() else {
		return None;
	};
	let ast::Expr::FieldAccess(access) = call.expr.as_ref() else {
		return None;
	};
	if span_text(source, access.expr.span()) != binding || call.args.len() != 2 {
		return None;
	}
	let ast::ExprField::Path(method) = &access.expr_field else {
		return None;
	};
	let key = parse_rune_string(span_text(source, call.args.first()?.0.span()))?;
	match span_text(source, method.span()) {
		"set" => Some(InitConfigKey::Set(
			crate::engine::input::rune_option_key(&key)
				.map_or(key, str::to_string),
		)),
		"ui_color" => Some(InitConfigKey::UiColor(
			crate::engine::ui_colors::UiColorKey::from_name(&key)
				.or_else(|| crate::engine::ui_colors::UiColorKey::from_canonical_name(&key))
				.or_else(|| crate::engine::ui_colors::UiColorKey::from_name(&format!("colors.{key}")))
				.map_or(key, |color| color.canonical_name().to_string()),
		)),
		"language_theme" => Some(InitConfigKey::LanguageTheme(
			language_registry
				.resolve_supported_language_id(&key)
				.map_or(key, str::to_string),
		)),
		_ => None,
	}
}

fn is_config_operation(source: &str, expression: &ast::Expr, binding: &str) -> bool {
	let ast::Expr::Try(try_expression) = expression else {
		return false;
	};
	let ast::Expr::Call(call) = try_expression.expr.as_ref() else {
		return false;
	};
	let ast::Expr::FieldAccess(access) = call.expr.as_ref() else {
		return false;
	};
	if span_text(source, access.expr.span()) != binding || call.args.len() != 2 {
		return false;
	}
	let ast::ExprField::Path(method) = &access.expr_field else {
		return false;
	};
	matches!(
		span_text(source, method.span()),
		"set" | "ui_color" | "language_theme"
	) && parse_rune_string(span_text(source, call.args.first().expect("length was checked").0.span()))
		.is_some()
}

fn span_text<'a>(source: &'a str, span: ast::Span) -> &'a str {
	&source[span_range(span)]
}

fn span_range(span: ast::Span) -> Range<usize> {
	span.start.into_usize()..span.end.into_usize()
}

fn normalize_rune_source(source: &str) -> String {
	source.chars().filter(|character| !character.is_whitespace()).collect()
}

fn is_identifier(value: &str) -> bool {
	let mut characters = value.chars();
	matches!(characters.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
		&& characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn parse_rune_string(source: &str) -> Option<String> {
	let value = source.trim().strip_prefix('"')?.strip_suffix('"')?;
	let mut decoded = String::with_capacity(value.len());
	let mut characters = value.chars();
	while let Some(character) = characters.next() {
		if character != '\\' {
			decoded.push(character);
			continue;
		}
		match characters.next()? {
			'\\' => decoded.push('\\'),
			'"' => decoded.push('"'),
			'n' => decoded.push('\n'),
			'r' => decoded.push('\r'),
			't' => decoded.push('\t'),
			_ => return None,
		}
	}
	Some(decoded)
}

fn render_inserted_operations(
	source: &str,
	offset: usize,
	operations: impl IntoIterator<Item = String>,
) -> String {
	let (first_prefix, indent) = insertion_layout(source, offset);
	let mut rendered = String::from(first_prefix);
	for (index, operation) in operations.into_iter().enumerate() {
		if index > 0 {
			rendered.push('\n');
			rendered.push_str(indent);
		}
		rendered.push_str(&operation);
	}
	rendered.push('\n');
	rendered.push_str(indent);
	rendered
}

fn insertion_layout(source: &str, offset: usize) -> (&str, &str) {
	let line_start = source[..offset].rfind('\n').map_or(0, |index| index + 1);
	let prefix = &source[line_start..offset];
	if prefix.chars().all(char::is_whitespace) {
		("", prefix)
	} else {
		("\n\t", "\t")
	}
}

fn tombstone_range(source: &str, range: Range<usize>) -> Range<usize> {
	let line_start = source[..range.start]
		.rfind('\n')
		.map_or(0, |index| index + 1);
	let line_end = source[range.end..]
		.find('\n')
		.map_or(source.len(), |index| range.end + index);
	if source[line_start..range.start]
		.chars()
		.all(char::is_whitespace)
		&& source[range.end..line_end]
			.chars()
			.all(char::is_whitespace)
	{
		line_start..line_end.saturating_add(usize::from(line_end < source.len()))
	} else {
		range
	}
}

fn parse_init_source(source: &str) -> Result<ast::File, String> {
	rune::parse::parse_all::<ast::File>(source, SourceId::EMPTY, false)
		.map_err(|error| format!("init.rn is not valid Rune: {error}"))
}

fn validate_init_source(source: &str) -> Result<(), String> {
	parse_init_source(source).map(|_| ())
}

fn render_init_file(settings: &SettingsPersistent) -> String {
	let mut source = "pub fn main(editor) {\n".to_string();
	source.push_str("\tlet config = editor.config();\n");
	write_set(&mut source, "theme", &settings.theme_name);
	write_bool_set(&mut source, "minimap", settings.minimap_enabled);
	write_bool_set(&mut source, "wrap", settings.wrap_enabled);
	write_bool_set(&mut source, "num", settings.line_numbers_enabled);
	write_bool_set(
		&mut source,
		"relativenumber",
		settings.relative_line_numbers_enabled,
	);
	write_bool_set(&mut source, "vis", settings.visible_whitespace);
	write_bool_set(&mut source, "reload", settings.reload_enabled);
	write_set(
		&mut source,
		"savedurability",
		settings.save_durability.as_str(),
	);
	for key in crate::engine::ui_colors::UiColorKey::ALL {
		if let Some(color) = settings.ui_colors.get(key) {
			write_color_set(&mut source, key.canonical_name(), &color.to_hex());
		}
	}
	for (language, theme) in &settings.language_themes {
		writeln!(
			&mut source,
			"\tconfig.language_theme({}, {})?;",
			rune_string(language),
			rune_string(theme)
		)
		.expect("writing to String cannot fail");
	}
	source.push_str("\tOk(())\n}\n");
	source
}

fn write_set(source: &mut String, name: &str, value: &str) {
	writeln!(
		source,
		"\tconfig.set({}, {})?;",
		rune_string(name),
		rune_string(value)
	)
	.expect("writing to String cannot fail");
}

fn write_bool_set(source: &mut String, name: &str, value: bool) {
	writeln!(source, "\tconfig.set({}, {})?;", rune_string(name), value)
		.expect("writing to String cannot fail");
}

fn write_color_set(source: &mut String, name: &str, value: &str) {
	writeln!(
		source,
		"\tconfig.ui_color({}, {})?;",
		rune_string(name),
		rune_string(value)
	)
	.expect("writing to String cannot fail");
}

fn rune_string(value: &str) -> String {
	let mut escaped = String::with_capacity(value.len() + 2);
	escaped.push('"');
	for character in value.chars() {
		match character {
			'\\' => escaped.push_str("\\\\"),
			'"' => escaped.push_str("\\\""),
			'\n' => escaped.push_str("\\n"),
			'\r' => escaped.push_str("\\r"),
			'\t' => escaped.push_str("\\t"),
			character => escaped.push(character),
		}
	}
	escaped.push('"');
	escaped
}

pub(super) fn allocate_dynamic_device_id(next_dynamic_device_id: &mut u16) -> Result<u16, String> {
	let device_id = *next_dynamic_device_id;
	if device_id == u16::MAX {
		return Err("Out of dynamic backing device ids".to_string());
	}
	*next_dynamic_device_id += 1;
	Ok(device_id)
}

#[derive(Debug)]
struct SaveCompletion {
	outcome: SaveOutcome,
	rebound: bool,
	warning: Option<String>,
}

fn save_document_with_fresh_backing(
	ctx: &RuntimeContext<'_>,
	root: NodeId,
	source_path: Option<&Path>,
	target_path: &str,
	durability: SaveDurability,
	next_dynamic_device_id: &mut u16,
	backing_device_id: &mut Option<u16>,
) -> Result<SaveCompletion, String> {
	// A device id names one immutable backing generation. Reusing an id for a
	// later save would redirect every older span carrying that id to the new
	// file, so reserve a fresh id before the save can make any filesystem change.
	let device_id = allocate_dynamic_device_id(next_dynamic_device_id)?;
	let outcome = save_document(
		ctx.registry,
		root,
		source_path,
		Path::new(target_path),
		durability,
	)?;
	match ctx.resolver.register_device(device_id, target_path) {
		Ok(()) => {
			// Only discard resident bytes once both resolver backends can
			// reconstruct every rebound span from the new generation.
			rebind_document_spans_to_saved_file(ctx.registry, root, device_id);
			let retired_device_id = backing_device_id.replace(device_id);
			if let Some(retired_device_id) = retired_device_id {
				ctx.resolver.unregister_device(retired_device_id);
			}
			Ok(SaveCompletion {
				outcome,
				rebound: true,
				warning: None,
			})
		}
		Err(error) => {
			// The disk write succeeded. Keep the current tree and its old backing:
			// virtual leaves still contain every edit, while unchanged physical
			// leaves remain readable from the old generation. The save is clean
			// even though sparsifying it onto the new file was unavailable.
			Ok(SaveCompletion {
				outcome,
				rebound: false,
				warning: Some(format!(
					"Saved \"{target_path}\", but could not register its new backing file: {error}; keeping the current in-memory document"
				)),
			})
		}
	}
}

fn create_empty_document(registry: &UastRegistry) -> Result<(NodeId, NodeId), String> {
	create_document_from_bytes(registry, Vec::new())
}

fn create_document_from_bytes(
	registry: &UastRegistry,
	bytes: Vec<u8>,
) -> Result<(NodeId, NodeId), String> {
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;

	let metrics = SpanMetrics {
		byte_length: bytes.len() as u64,
		newlines: bytes.iter().filter(|byte| **byte == b'\n').count() as u64,
	};
	let mut chunk = registry.reserve_chunk(2).ok_or_else(|| {
		"Not enough document-node capacity to create an empty document".to_string()
	})?;
	let rid = chunk.spawn_node(SemanticKind::RelationalTable, None, metrics);
	let leaf = chunk.spawn_node(SemanticKind::Token, None, metrics);
	chunk.append_local_child(rid, leaf);
	unsafe {
		*registry.virtual_data[leaf.index()].get() = Some(bytes);
	}
	Ok((rid, leaf))
}

pub(super) fn load_root_from_path(
	ctx: &RuntimeContext<'_>,
	path: &str,
	next_dynamic_device_id: &mut u16,
) -> Result<(NodeId, NodeId, Option<u16>), String> {
	let metadata = std::fs::metadata(path).map_err(|e| format!("Open error: {}", e))?;
	if metadata.len() == 0 {
		let (root, leaf) = create_empty_document(ctx.registry)?;
		return Ok((root, leaf, None));
	}

	let device_id = allocate_dynamic_device_id(next_dynamic_device_id)?;
	let rid = ingest_svp_file(
		ctx.resolver,
		ctx.registry,
		metadata.len(),
		device_id,
		path.to_string(),
	)?;
	let cursor_leaf = ctx
		.registry
		.get_first_child(rid)
		.ok_or_else(|| "Failed to load new file root".to_string())?;
	Ok((rid, cursor_leaf, Some(device_id)))
}

#[allow(clippy::too_many_arguments)]
fn initialize_loaded_document(
	ctx: &RuntimeContext<'_>,
	root_id: &mut Option<NodeId>,
	file_path: &mut Option<String>,
	virtual_path: &mut Option<String>,
	backing_path: &mut Option<String>,
	backing_device_id: &mut Option<u16>,
	file_stamp: &mut Option<FileStamp>,
	active_language_id: &mut Option<&'static str>,
	shadow: &mut Option<ShadowSession>,
	ledger: &mut UndoLedger,
	folds: &mut FoldSet,
	caret: &mut Caret,
	active_visual: &mut Option<(DocByte, VisualKind)>,
	scroll_y: &mut u64,
	scroll_row_offset: &mut DisplayRow,
	path: Option<String>,
	new_virtual_path: Option<String>,
	backing: Option<String>,
	language_id: Option<&'static str>,
	shadow_session: Option<ShadowSession>,
	new_root: NodeId,
	_new_cursor_node: NodeId,
	new_backing_device_id: Option<u16>,
	semantic: &mut SemanticRuntime<'_>,
) {
	if let Some(old_root) = *root_id
		&& old_root != new_root
	{
		ctx.registry.cancel_metric_scan(old_root);
	}
	*active_language_id = language_id;
	*file_stamp = path.as_deref().and_then(file_stamp_for_path);
	*file_path = path;
	*virtual_path = new_virtual_path;
	*backing_path = backing;
	*shadow = shadow_session;
	*root_id = Some(new_root);
	folds.clear();
	let retired_device_id = std::mem::replace(backing_device_id, new_backing_device_id);
	if let Some(device_id) = retired_device_id {
		ctx.resolver.unregister_device(device_id);
	}
	caret.set_byte(DocByte::ZERO, Affinity::After);
	caret.preferred_col = DocDisplayCol::ZERO;
	*scroll_y = 0;
	*scroll_row_offset = DisplayRow::ZERO;
	ledger.clear();
	*active_visual = None;
	semantic.reset_semantic_state();
}

#[allow(clippy::too_many_arguments)]
fn reload_loaded_document(
	ctx: &RuntimeContext<'_>,
	root_id: &mut Option<NodeId>,
	file_path: &mut Option<String>,
	virtual_path: &mut Option<String>,
	backing_path: &mut Option<String>,
	backing_device_id: &mut Option<u16>,
	file_stamp: &mut Option<FileStamp>,
	active_language_id: &mut Option<&'static str>,
	shadow: &mut Option<ShadowSession>,
	ledger: &mut UndoLedger,
	folds: &mut FoldSet,
	caret: &mut Caret,
	active_visual: &mut Option<(DocByte, VisualKind)>,
	scroll_y: &mut u64,
	scroll_row_offset: &mut DisplayRow,
	semantic: &mut SemanticRuntime<'_>,
	next_dynamic_device_id: &mut u16,
	path: String,
	preserve_position: bool,
) -> Result<u64, String> {
	let metadata = std::fs::metadata(&path).map_err(|e| format!("Open error: {}", e))?;
	let byte_len = metadata.len();
	let (new_root, new_cursor_node, new_backing_device_id) = if byte_len == 0 {
		let (root, leaf) = create_empty_document(ctx.registry)?;
		(root, leaf, None)
	} else {
		load_root_from_path(ctx, &path, next_dynamic_device_id)?
	};

	let prior_line = caret.abs_line();
	let prior_col = caret.abs_col();
	let prior_scroll_y = *scroll_y;
	let prior_scroll_row_offset = *scroll_row_offset;
	let language_id = detect_language(ctx.lang_registry, &path);

	if let Some(existing_shadow) = shadow.take() {
		let _ = discard_shadow_session(&existing_shadow);
	}

	initialize_loaded_document(
		ctx,
		root_id,
		file_path,
		virtual_path,
		backing_path,
		backing_device_id,
		file_stamp,
		active_language_id,
		shadow,
		ledger,
		folds,
		caret,
		active_visual,
		scroll_y,
		scroll_row_offset,
		Some(path.clone()),
		None,
		Some(path),
		language_id,
		None,
		new_root,
		new_cursor_node,
		new_backing_device_id,
		semantic,
	);

	if preserve_position {
		*scroll_y = prior_scroll_y;
		*scroll_row_offset = prior_scroll_row_offset;
		caret.set_pending_target(prior_line, prior_col, caret.affinity(), false, None);
	}

	Ok(byte_len)
}

pub(crate) fn set_wrap_local(tab: &mut View, enabled: bool) -> String {
	let view = tab.common_mut();
	view.wrap_enabled = enabled;
	if !enabled {
		view.scroll_row_offset = DisplayRow::ZERO;
	}
	if enabled {
		"Wrap enabled".to_string()
	} else {
		"Wrap disabled".to_string()
	}
}

pub(crate) fn set_minimap_local(tab: &mut View, enabled: bool) -> String {
	tab.common_mut().minimap_enabled = enabled;
	tab.reset_minimap_cache();
	if enabled {
		"Minimap enabled".to_string()
	} else {
		"Minimap disabled".to_string()
	}
}

pub(crate) fn set_line_numbers_local(tab: &mut View, enabled: bool) -> String {
	tab.common_mut().line_numbers_enabled = enabled;
	if enabled {
		"Line numbers visible".to_string()
	} else {
		"Line numbers hidden".to_string()
	}
}

pub(crate) fn set_relative_line_numbers_local(tab: &mut View, enabled: bool) -> String {
	tab.common_mut().relative_line_numbers_enabled = enabled;
	if enabled {
		"Relative line numbers visible".to_string()
	} else {
		"Relative line numbers hidden".to_string()
	}
}

pub(crate) fn set_visible_whitespace_local(tab: &mut View, enabled: bool) -> String {
	tab.common_mut().visible_whitespace = enabled;
	if enabled {
		"Whitespace visible".to_string()
	} else {
		"Whitespace hidden".to_string()
	}
}

fn apply_theme_name_to_tab(tab: &mut View, name: String) -> Result<(), String> {
	if tab.common().current_theme_name == name {
		return Ok(());
	}
	let theme = Theme::try_new(&name)?;
	tab.common_mut().current_theme = theme;
	tab.common_mut().current_theme_name = name.clone();
	tab.reset_minimap_cache();
	Ok(())
}

pub(super) fn apply_resolved_theme_to_tab(
	tab: &mut View,
	active_language_id: Option<&str>,
	persistent_settings: &SettingsPersistent,
) -> Result<(), String> {
	let theme_name = resolve_theme_name(
		active_language_id,
		tab.common().theme_override.as_deref(),
		persistent_settings,
	);
	apply_theme_name_to_tab(tab, theme_name)
}

pub(crate) fn re_resolve_auto_themes(
	buffers: &BufferStore,
	layout: &mut LayoutNode,
	persistent_settings: &SettingsPersistent,
) -> Result<(), String> {
	let mut result = Ok(());
	layout.for_each_doctab_mut(&mut |tab| {
		if result.is_err() || tab.common().theme_override.is_some() {
			return;
		}
		let language_id = buffers
			.get(tab.buffer_id)
			.and_then(|buffer| buffer.text())
			.and_then(|buffer| buffer.active_language_id);
		result = apply_resolved_theme_to_tab(tab, language_id, persistent_settings);
	});
	result
}

fn set_theme_local(tab: &mut View, name: String) -> Result<String, String> {
	apply_theme_name_to_tab(tab, name.clone())?;
	tab.common_mut().theme_override = Some(name.clone());
	Ok(format!("Theme set to {}", name))
}

fn set_background_local(tab: &mut View, hex: String) -> Result<String, String> {
	let Some(color) = parse_ui_rgb(&hex) else {
		return Err(format!("Invalid hex color: {}", hex));
	};
	let canonical = color.to_hex();
	tab.common_mut().background_hex = Some(canonical.clone());
	tab.reset_minimap_cache();
	Ok(format!("Background set to {}", canonical))
}

pub(super) fn handle_fold_doctab_command(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	command: EditorCommand,
) -> CommandEffect {
	tab.cursor.collapse();
	let root = tab.document.root_id;
	let state_id = tab.document.ledger.current_state_id;
	let topology_rev_before = tab.document.topology_rev;
	if !tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev_before,
		state_id,
	) {
		return CommandEffect::render();
	}
	let effect = {
		handle_fold_command(
			registry,
			Some(tab.document.root_id),
			&mut tab.document.topology_rev,
			&mut tab.view.folds,
			tab.cursor.primary_mut(),
			command,
		)
	};
	tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		tab.document.topology_rev,
		state_id,
	);
	tab.cursor.sync_preferred_col();
	effect
}

pub(super) fn handle_view_doctab_command(
	tab: &mut View,
	ui: &mut UiState,
	command: EditorCommand,
) -> CommandEffect {
	ui.status_message = Some(match command {
		EditorCommand::ToggleWrap => set_wrap_local(tab, !tab.common().wrap_enabled),
		EditorCommand::SetWrap(enabled) => set_wrap_local(tab, enabled),
		EditorCommand::SetMinimap(enabled) => set_minimap_local(tab, enabled),
		EditorCommand::SetLineNumbers(enabled) => set_line_numbers_local(tab, enabled),
		EditorCommand::SetRelativeLineNumbers(enabled) => {
			set_relative_line_numbers_local(tab, enabled)
		}
		EditorCommand::SetVisibleWhitespace(enabled) => set_visible_whitespace_local(tab, enabled),
		EditorCommand::SetTheme(name) => match set_theme_local(tab, name) {
			Ok(msg) => msg,
			Err(err) => {
				ui.status_message = Some(err);
				return CommandEffect::render();
			}
		},
		EditorCommand::SetBackground(hex) => match set_background_local(tab, hex) {
			Ok(msg) => msg,
			Err(err) => {
				ui.status_message = Some(err);
				return CommandEffect::render();
			}
		},
		_ => unreachable!(),
	});

	CommandEffect::render()
}

pub(super) fn handle_persist_view_doctab_command(
	ctx: &RuntimeContext<'_>,
	tab: &mut View,
	ui: &mut UiState,
	persistent_settings: &mut SettingsPersistent,
	command: EditorCommand,
) -> CommandEffect {
	match command {
		EditorCommand::PersistWrap(enabled) => {
			let success = set_wrap_local(tab, enabled);
			persistent_settings.wrap_enabled = enabled;
			ui.status_message = Some(success);
		}
		EditorCommand::PersistMinimap(enabled) => {
			let success = set_minimap_local(tab, enabled);
			persistent_settings.minimap_enabled = enabled;
			ui.status_message = Some(success);
		}
		EditorCommand::PersistLineNumbers(enabled) => {
			let success = set_line_numbers_local(tab, enabled);
			persistent_settings.line_numbers_enabled = enabled;
			ui.status_message = Some(success);
		}
		EditorCommand::PersistRelativeLineNumbers(enabled) => {
			let success = set_relative_line_numbers_local(tab, enabled);
			persistent_settings.relative_line_numbers_enabled = enabled;
			ui.status_message = Some(success);
		}
		EditorCommand::PersistVisibleWhitespace(enabled) => {
			let success = set_visible_whitespace_local(tab, enabled);
			persistent_settings.visible_whitespace = enabled;
			ui.status_message = Some(success);
		}
		EditorCommand::PersistTheme(name) => match Theme::try_new(&name) {
			Ok(_) => {
				persistent_settings.theme_name = name.clone();
				let success = format!("Theme set to {}", name);
				ui.status_message = Some(success);
			}
			Err(err) => {
				ui.status_message = Some(err);
			}
		},
		EditorCommand::PersistLangTheme { language, theme } => {
			let Some(language_id) = ctx.lang_registry.resolve_supported_language_id(&language)
			else {
				ui.status_message = Some(format!("Unknown language: {language}"));
				return CommandEffect::render();
			};
			match Theme::try_new(&theme) {
				Ok(_) => {
					persistent_settings
						.language_themes
						.insert(language_id.to_string(), theme.clone());
					let success = format!("Theme for {} set to {}", language_id, theme);
					ui.status_message = Some(success);
				}
				Err(err) => {
					ui.status_message = Some(err);
				}
			}
		}
		EditorCommand::PersistNoLangTheme(language) => {
			let Some(language_id) = ctx.lang_registry.resolve_supported_language_id(&language)
			else {
				ui.status_message = Some(format!("Unknown language: {language}"));
				return CommandEffect::render();
			};
			let success = if persistent_settings
				.language_themes
				.remove(language_id)
				.is_some()
			{
				format!("Theme for {} removed", language_id)
			} else {
				format!("No theme was set for {}", language_id)
			};
			ui.status_message = Some(success);
		}
		EditorCommand::PersistUiColor { key, value } => {
			persistent_settings.ui_colors.set(key, value);
			let success = match value {
				Some(color) => format!("{} set to {}", key.canonical_name(), color.to_hex()),
				None => match persistent_settings.ui_colors.resolve(key).source {
					UiColorSource::Inherited(parent) => format!(
						"{} reset to {}",
						key.canonical_name(),
						parent.canonical_name()
					),
					UiColorSource::ScopedBuiltIn | UiColorSource::BuiltInDefault => {
						format!("{} reset to built-in default", key.canonical_name())
					}
					UiColorSource::Explicit => unreachable!("cleared color cannot remain explicit"),
				},
			};
			ui.status_message = Some(success);
		}
		EditorCommand::QueryUiColor(key) => {
			let resolved = persistent_settings.ui_colors.resolve(key);
			let provenance = match resolved.source {
				UiColorSource::Explicit => "explicit".to_string(),
				UiColorSource::Inherited(parent) => {
					format!("inherited from {}", parent.canonical_name())
				}
				UiColorSource::ScopedBuiltIn => "scoped built-in default".to_string(),
				UiColorSource::BuiltInDefault => "built-in default".to_string(),
			};
			ui.status_message = Some(format!(
				"{} = {} ({})",
				key.canonical_name(),
				resolved.color.to_hex(),
				provenance
			));
		}
		EditorCommand::PersistReload(enabled) => {
			persistent_settings.reload_enabled = enabled;
			let success = if enabled {
				"Auto reload enabled".to_string()
			} else {
				"Auto reload disabled".to_string()
			};
			ui.status_message = Some(success);
		}
		EditorCommand::PersistSaveDurability(durability) => {
			persistent_settings.save_durability = durability;
			let success = match durability {
				SaveDurability::Atomic => "Saves are atomic".to_string(),
				SaveDurability::Bounded => {
					"Saves may rewrite a huge file's tail in place (not crash-atomic)".to_string()
				}
			};
			ui.status_message = Some(success);
		}
		_ => unreachable!(),
	}

	CommandEffect::render()
}

/// The status line a completed save reports.
///
/// A bounded save says so. The user opted into reduced durability with
/// `:set savedurability bounded`, but whether any given save actually took that
/// path depends on the document's size and leaf layout, so this message is the
/// only place they can learn that this file now has a tail that a power loss
/// could tear.
fn save_status_message(path: &str, outcome: SaveOutcome) -> String {
	let bytes = outcome.stats.logical_bytes;
	match outcome.method {
		SaveMethod::AtomicReplace => format!("\"{path}\" {bytes}B written"),
		SaveMethod::InPlaceSuffix => {
			format!("\"{path}\" {bytes}B written in place (tail rewrite, not crash-atomic)")
		}
	}
}

pub(super) fn handle_file_doctab_command(
	ctx: &RuntimeContext<'_>,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	next_dynamic_device_id: &mut u16,
	save_durability: SaveDurability,
	command: EditorCommand,
) -> CommandEffect {
	if matches!(&command, EditorCommand::QueryDocumentText) {
		match read_document_snapshot(
			ctx.registry,
			tab.document.root_id,
			tab.document.backing_path.as_deref().map(Path::new),
		) {
			Ok(bytes) => {
				ui.document_text_response = Some(String::from_utf8_lossy(&bytes).into_owned());
			}
			Err(msg) => ui.status_message = Some(msg),
		}
		return CommandEffect::render();
	}
	let resets_search_results = matches!(
		&command,
		EditorCommand::LoadFile(_)
			| EditorCommand::ReloadFile { .. }
			| EditorCommand::ReplaceDocument { .. }
	);
	// A newly installed document always starts at byte zero. Keep that known,
	// exact position instead of immediately asking the sparse resolver to
	// rediscover it: in-memory embedders such as Polariton have no backing I/O
	// to wait for, and a replacement must be editable as soon as it is visible.
	let installs_new_document = matches!(
		&command,
		EditorCommand::LoadFile(_) | EditorCommand::ReplaceDocument { .. }
	);
	let state_before = tab.document.ledger.current_state_id;
	if resets_search_results {
		tab.document.bump_topology_rev();
		tab.cursor.collapse();
	}
	let topology_rev = tab.document.topology_rev;
	let state_id = tab.document.ledger.current_state_id;
	let requires_resolved_caret = matches!(&command, EditorCommand::ReloadFile { .. });
	if requires_resolved_caret
		&& !tab.cursor.primary_mut().resolve(
			ctx.registry,
			tab.document.root_id,
			&mut tab.view.folds,
			topology_rev,
			state_id,
		) {
		ui.status_message = Some("Loading cursor extent".to_string());
		return CommandEffect::render();
	}
	let mut active_visual = tab.cursor.active_visual();
	let mut root_id = Some(tab.document.root_id);
	let (effect, minimap_invalidated) = {
		let mut semantic = SemanticRuntime::from_semantic(&mut tab.document.semantic);
		let effect = {
			let super::super::session::TextViewState { folds, common } = &mut *tab.view;
			handle_file_command(
				ctx,
				&mut root_id,
				&mut tab.document.file_path,
				&mut tab.document.virtual_path,
				&mut tab.document.backing_path,
				&mut tab.document.backing_device_id,
				&mut tab.document.file_stamp,
				&mut tab.document.active_language_id,
				&mut tab.document.shadow,
				&mut tab.document.ledger,
				folds,
				tab.cursor.primary_mut(),
				&mut active_visual,
				&mut common.scroll_y,
				&mut common.scroll_row_offset,
				&mut ui.status_message,
				&mut ui.pending_quit,
				&mut semantic,
				next_dynamic_device_id,
				save_durability,
				command,
			)
		};
		(effect, semantic.minimap_invalidated())
	};
	if minimap_invalidated {
		tab.view.common.reset_minimap_cache();
	}
	tab.document.root_id = root_id.expect("text file command must retain a root");
	if installs_new_document {
		let initial_leaf = ctx
			.registry
			.find_node_at_doc_byte_raw(tab.document.root_id, DocByte::ZERO)
			.node_id;
		tab.cursor.primary_mut().reset_to_start(
			initial_leaf,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		);
	} else {
		tab.cursor.primary_mut().resolve(
			ctx.registry,
			tab.document.root_id,
			&mut tab.view.folds,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		);
	}
	if resets_search_results && tab.document.ledger.current_state_id != state_before {
		tab.document.search_matches.clear();
		*tab.match_index = None;
		*tab.match_info = None;
	}
	tab.cursor.set_active_visual(active_visual);
	if tab.cursor.points().is_some() {
		tab.cursor.sync_preferred_col();
	}
	effect
}

pub(super) fn handle_auto_reload_doctab_command(
	ctx: &RuntimeContext<'_>,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	next_dynamic_device_id: &mut u16,
) -> CommandEffect {
	let Some(path) = tab.document.file_path.clone() else {
		return CommandEffect::default();
	};

	let previous_stamp = tab.document.file_stamp.clone();
	let metadata = match std::fs::metadata(&path) {
		Ok(metadata) => metadata,
		Err(_) => {
			if previous_stamp.is_some() {
				tab.document.file_stamp = None;
				ui.status_message = Some(format!("\"{}\" no longer exists on disk", path));
				return CommandEffect::render();
			}
			return CommandEffect::default();
		}
	};
	let current_stamp = FileStamp::from_metadata(&metadata);
	if previous_stamp.as_ref() == Some(&current_stamp) {
		return CommandEffect::default();
	}

	if tab.document.ledger.is_dirty() {
		tab.document.file_stamp = Some(current_stamp);
		ui.status_message = Some(format!("\"{}\" changed on disk; use :e! to reload", path));
		return CommandEffect::render();
	}

	tab.cursor.collapse();
	tab.document.bump_topology_rev();
	let topology_rev = tab.document.topology_rev;
	let state_id = tab.document.ledger.current_state_id;
	let root_before = tab.document.root_id;
	if !tab.cursor.primary_mut().resolve(
		ctx.registry,
		root_before,
		&mut tab.view.folds,
		topology_rev,
		state_id,
	) {
		return CommandEffect::render();
	}
	let mut active_visual = tab.cursor.active_visual();
	let mut root_id = Some(tab.document.root_id);
	let (result, minimap_invalidated) = {
		let mut semantic = SemanticRuntime::from_semantic(&mut tab.document.semantic);
		let result = {
			let super::super::session::TextViewState { folds, common } = &mut *tab.view;
			reload_loaded_document(
				ctx,
				&mut root_id,
				&mut tab.document.file_path,
				&mut tab.document.virtual_path,
				&mut tab.document.backing_path,
				&mut tab.document.backing_device_id,
				&mut tab.document.file_stamp,
				&mut tab.document.active_language_id,
				&mut tab.document.shadow,
				&mut tab.document.ledger,
				folds,
				tab.cursor.primary_mut(),
				&mut active_visual,
				&mut common.scroll_y,
				&mut common.scroll_row_offset,
				&mut semantic,
				next_dynamic_device_id,
				path.clone(),
				true,
			)
		};
		(result, semantic.minimap_invalidated())
	};
	if minimap_invalidated {
		tab.view.common.reset_minimap_cache();
	}
	if let Some(root) = root_id {
		tab.document.root_id = root;
		tab.cursor.primary_mut().resolve(
			ctx.registry,
			root,
			&mut tab.view.folds,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		);
	}
	tab.cursor.set_active_visual(active_visual);
	tab.cursor.sync_preferred_col();
	match result {
		Ok(len) => {
			tab.document.search_matches.clear();
			*tab.match_index = None;
			*tab.match_info = None;
			ui.status_message = Some(format!("\"{}\" {}B reloaded", path, len));
			CommandEffect::render()
		}
		Err(msg) => {
			ui.status_message = Some(msg);
			CommandEffect::render()
		}
	}
}

pub(super) fn handle_language_doctab_command(
	lang_registry: &super::super::language::LanguageRegistry,
	tab: &mut TextEditorMut<'_>,
	status_message: &mut Option<String>,
	lang_id: Option<String>,
) -> CommandEffect {
	let (effect, minimap_invalidated) = {
		let mut semantic = SemanticRuntime::from_semantic(&mut tab.document.semantic);
		let effect = handle_language_command(
			lang_registry,
			&mut tab.document.active_language_id,
			tab.document.file_path.is_some(),
			&mut tab.document.virtual_path,
			&mut semantic,
			status_message,
			lang_id,
		);
		(effect, semantic.minimap_invalidated())
	};
	if minimap_invalidated {
		tab.view.common.reset_minimap_cache();
	}
	effect
}

pub(super) fn handle_fold_command(
	registry: &UastRegistry,
	root_id: Option<NodeId>,
	topology_rev: &mut crate::core::TopologyRev,
	folds: &mut FoldSet,
	caret: &mut Caret,
	command: EditorCommand,
) -> CommandEffect {
	if let Some(rid) = root_id {
		match command {
			EditorCommand::ToggleFold | EditorCommand::CloseFold | EditorCommand::OpenFold => {
				let Some(node) = caret.node_cache() else {
					return CommandEffect::render();
				};
				let target_node = resolve_fold_boundary_at_cursor_tracked(
					registry,
					rid,
					folds,
					node.node,
					caret.abs_line(),
					caret.abs_col(),
					!matches!(command, EditorCommand::OpenFold),
					topology_rev,
				);

				if let Some(node) = target_node {
					let new_state = match command {
						EditorCommand::ToggleFold => !folds.contains(node),
						EditorCommand::CloseFold => true,
						EditorCommand::OpenFold => false,
						_ => unreachable!(),
					};
					folds.set(node, new_state);
				}
			}
			EditorCommand::CloseAllFolds => {
				set_subtree_fold_state(registry, rid, folds, true);
			}
			EditorCommand::OpenAllFolds => {
				set_subtree_fold_state(registry, rid, folds, false);
			}
			_ => unreachable!(),
		}

		caret.set_pending_target(
			snap_line_to_visible_boundary(registry, rid, folds, caret.abs_line()),
			caret.abs_col(),
			caret.affinity(),
			false,
			None,
		);
	}

	CommandEffect::render()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_file_command(
	ctx: &RuntimeContext<'_>,
	root_id: &mut Option<NodeId>,
	file_path: &mut Option<String>,
	virtual_path: &mut Option<String>,
	backing_path: &mut Option<String>,
	backing_device_id: &mut Option<u16>,
	file_stamp: &mut Option<FileStamp>,
	active_language_id: &mut Option<&'static str>,
	shadow: &mut Option<ShadowSession>,
	ledger: &mut UndoLedger,
	folds: &mut FoldSet,
	caret: &mut Caret,
	active_visual: &mut Option<(DocByte, VisualKind)>,
	scroll_y: &mut u64,
	scroll_row_offset: &mut DisplayRow,
	status_message: &mut Option<String>,
	pending_quit: &mut bool,
	semantic: &mut SemanticRuntime<'_>,
	next_dynamic_device_id: &mut u16,
	save_durability: SaveDurability,
	command: EditorCommand,
) -> CommandEffect {
	match command {
		EditorCommand::LoadFile(path) => {
			if let Some(existing_shadow) = shadow.take() {
				let _ = discard_shadow_session(&existing_shadow);
			}
			let language_id = detect_language(ctx.lang_registry, &path);
			match load_shadow_session(Path::new(&path)) {
				Ok(Some(recovered_shadow)) => {
					if let Some(current_generation) = recovered_shadow.current_generation().cloned()
					{
						match load_root_from_path(
							ctx,
							&current_generation.path,
							next_dynamic_device_id,
						) {
							Ok((rid, cursor_leaf, device_id)) => {
								initialize_loaded_document(
									ctx,
									root_id,
									file_path,
									virtual_path,
									backing_path,
									backing_device_id,
									file_stamp,
									active_language_id,
									shadow,
									ledger,
									folds,
									caret,
									active_visual,
									scroll_y,
									scroll_row_offset,
									Some(path.clone()),
									None,
									Some(current_generation.path.clone()),
									language_id,
									Some(recovered_shadow),
									rid,
									cursor_leaf,
									device_id,
									semantic,
								);
								if let Some(active_shadow) = shadow.as_ref() {
									ledger.restore_generation_history(
										&active_shadow.generations,
										active_shadow.current_index,
									);
								}
								*status_message =
									Some("Recovered unsaved large-file session".to_string());
							}
							Err(msg) => *status_message = Some(msg),
						}
					} else {
						*status_message = Some("Recovered shadow session is empty".to_string());
					}
				}
				Ok(None) => {
					if std::fs::metadata(&path).is_ok() {
						match load_root_from_path(ctx, &path, next_dynamic_device_id) {
							Ok((rid, cursor_leaf, device_id)) => initialize_loaded_document(
								ctx,
								root_id,
								file_path,
								virtual_path,
								backing_path,
								backing_device_id,
								file_stamp,
								active_language_id,
								shadow,
								ledger,
								folds,
								caret,
								active_visual,
								scroll_y,
								scroll_row_offset,
								Some(path.clone()),
								None,
								Some(path.clone()),
								language_id,
								None,
								rid,
								cursor_leaf,
								device_id,
								semantic,
							),
							Err(msg) => *status_message = Some(msg),
						}
					} else {
						match create_empty_document(ctx.registry) {
							Ok((rid, leaf)) => initialize_loaded_document(
								ctx,
								root_id,
								file_path,
								virtual_path,
								backing_path,
								backing_device_id,
								file_stamp,
								active_language_id,
								shadow,
								ledger,
								folds,
								caret,
								active_visual,
								scroll_y,
								scroll_row_offset,
								Some(path),
								None,
								None,
								language_id,
								None,
								rid,
								leaf,
								None,
								semantic,
							),
							Err(msg) => *status_message = Some(msg),
						}
					}
				}
				Err(msg) => *status_message = Some(msg),
			}
		}
		EditorCommand::ReloadFile { force } => {
			if !force && ledger.is_dirty() {
				*status_message = Some("No write since last change; use :e! to reload".to_string());
			} else if let Some(path) = file_path.clone() {
				match reload_loaded_document(
					ctx,
					root_id,
					file_path,
					virtual_path,
					backing_path,
					backing_device_id,
					file_stamp,
					active_language_id,
					shadow,
					ledger,
					folds,
					caret,
					active_visual,
					scroll_y,
					scroll_row_offset,
					semantic,
					next_dynamic_device_id,
					path.clone(),
					true,
				) {
					Ok(len) => *status_message = Some(format!("\"{}\" {}B reloaded", path, len)),
					Err(msg) => *status_message = Some(msg),
				}
			} else {
				*status_message = Some("No file name".to_string());
			}
		}
		EditorCommand::WriteFile => {
			if let Some(rid) = *root_id {
				if let Some(path) = file_path.as_deref() {
					match save_document_with_fresh_backing(
						ctx,
						rid,
						backing_path.as_deref().map(Path::new),
						path,
						save_durability,
						next_dynamic_device_id,
						backing_device_id,
					) {
						Ok(completion) => {
							ledger.mark_saved();
							if completion.rebound {
								*backing_path = Some(path.to_string());
							}
							*file_stamp = file_stamp_for_path(path);
							if let Some(existing_shadow) = shadow.take() {
								let _ = discard_shadow_session(&existing_shadow);
							}
							*status_message = completion
								.warning
								.or_else(|| Some(save_status_message(path, completion.outcome)));
						}
						Err(msg) => *status_message = Some(msg),
					}
				} else {
					*status_message = Some("No file name".to_string());
				}
			} else {
				*status_message = Some("No file to write".to_string());
			}
		}
		EditorCommand::WriteFileAs(path) => {
			if let Some(rid) = *root_id {
				let source_path = backing_path.as_deref().map(Path::new);
				match save_document_with_fresh_backing(
					ctx,
					rid,
					source_path,
					&path,
					save_durability,
					next_dynamic_device_id,
					backing_device_id,
				) {
					Ok(completion) => {
						ledger.mark_saved();
						*status_message = completion
							.warning
							.or_else(|| Some(save_status_message(&path, completion.outcome)));
						*file_stamp = file_stamp_for_path(&path);
						if completion.rebound {
							*backing_path = Some(path.clone());
						}
						*active_language_id = detect_language(ctx.lang_registry, &path);
						semantic.reset_semantic_state();
						*file_path = Some(path);
						// The document now has a real file identity; only a
						// successful write may retire the virtual one.
						*virtual_path = None;
						if let Some(existing_shadow) = shadow.take() {
							let _ = discard_shadow_session(&existing_shadow);
						}
					}
					Err(msg) => *status_message = Some(msg),
				}
			} else {
				*status_message = Some("No file to write".to_string());
			}
		}
		EditorCommand::WriteAndQuit => {
			if let Some(rid) = *root_id {
				if let Some(path) = file_path.as_deref() {
					match save_document_with_fresh_backing(
						ctx,
						rid,
						backing_path.as_deref().map(Path::new),
						path,
						save_durability,
						next_dynamic_device_id,
						backing_device_id,
					) {
						Ok(completion) => {
							ledger.mark_saved();
							if completion.rebound {
								*backing_path = Some(path.to_string());
							}
							*file_stamp = file_stamp_for_path(path);
							if let Some(existing_shadow) = shadow.take() {
								let _ = discard_shadow_session(&existing_shadow);
							}
							*status_message = completion
								.warning
								.or_else(|| Some(save_status_message(path, completion.outcome)));
							*pending_quit = true;
						}
						Err(msg) => *status_message = Some(msg),
					}
				} else {
					*status_message = Some("No file name".to_string());
				}
			} else {
				*status_message = Some("No file to write".to_string());
			}
		}
		EditorCommand::ReplaceDocument {
			text,
			language,
			virtual_path: requested_virtual_path,
		} => {
			let (language_id, derived_virtual_path) = match language.as_deref() {
				Some(name_or_extension) => {
					match ctx
						.lang_registry
						.resolve_supported_language_provider(name_or_extension)
					{
						Some(provider) => (
							Some(provider.language_id()),
							provider
								.extensions()
								.first()
								.map(|extension| format!("untitled.{extension}")),
						),
						None => {
							*status_message =
								Some(format!("Unknown language: {}", name_or_extension));
							return CommandEffect::render();
						}
					}
				}
				None => (None, None),
			};
			match create_document_from_bytes(ctx.registry, text.into_bytes()) {
				Ok((rid, leaf)) => {
					if let Some(existing_shadow) = shadow.take() {
						let _ = discard_shadow_session(&existing_shadow);
					}
					initialize_loaded_document(
						ctx,
						root_id,
						file_path,
						virtual_path,
						backing_path,
						backing_device_id,
						file_stamp,
						active_language_id,
						shadow,
						ledger,
						folds,
						caret,
						active_visual,
						scroll_y,
						scroll_row_offset,
						None,
						requested_virtual_path.or(derived_virtual_path),
						None,
						language_id,
						None,
						rid,
						leaf,
						None,
						semantic,
					);
				}
				Err(msg) => *status_message = Some(msg),
			}
		}
		_ => unreachable!(),
	}

	CommandEffect::render()
}

pub(super) fn handle_language_command(
	lang_registry: &super::super::language::LanguageRegistry,
	active_language_id: &mut Option<&'static str>,
	has_file_path: bool,
	virtual_path: &mut Option<String>,
	semantic: &mut SemanticRuntime<'_>,
	status_message: &mut Option<String>,
	lang_id: Option<String>,
) -> CommandEffect {
	let (language_id, derived_virtual_path) = match lang_id.as_deref() {
		Some(name_or_extension) => {
			match lang_registry.resolve_supported_language_provider(name_or_extension) {
				Some(provider) => {
					*status_message = Some(format!("Language set to {}", provider.label()));
					(
						Some(provider.language_id()),
						provider
							.extensions()
							.first()
							.map(|extension| format!("untitled.{extension}")),
					)
				}
				None => {
					*status_message = Some(format!("Unknown language: {}", name_or_extension));
					return CommandEffect::render();
				}
			}
		}
		None => {
			*status_message = Some("Language set to plaintext".to_string());
			(None, None)
		}
	};
	*active_language_id = language_id;
	// A pathless buffer needs a file-path-shaped identity for the language
	// services the new language enables; a real file keeps its own identity.
	if !has_file_path {
		*virtual_path = derived_virtual_path;
	}
	semantic.reset_semantic_state();
	CommandEffect::render()
}

#[cfg(test)]
mod tests {
	use super::{
		RuntimeContext, create_empty_document, handle_file_doctab_command,
		save_document_with_fresh_backing,
	};
	use crate::core::{CursorPosition, DocByte, DocDisplayCol, DocLine, DocLineCount};
	use crate::ecs::UastRegistry;
	use crate::engine::language::LanguageRegistry;
	use crate::engine::protocol::{
		ConfirmAction, EditorCommand, MoveDirection, SplitDirection, VerticalDirection,
	};
	use crate::engine::session::{SettingsPersistent, TestTextEditor, UiState};
	use crate::engine::settings::SaveDurability;
	use crate::engine::test_support::{
		recv_view, spawn_test_engine, spawn_test_engine_with_csv, spawn_test_engine_with_input,
		spawn_test_engine_with_markdown, spawn_test_engine_with_python_and_rune,
		spawn_test_engine_with_rust, spawn_test_engine_with_rust_and_language_themes,
		spawn_test_engine_with_sql, temp_test_path,
	};
	use crate::engine::undo::TextDelta;
	use crate::engine::{EditorMode, GutterNumbering, MouseIntent, Theme};
	use crate::svp::SvpResolver;
	use crate::svp::projector::HighlightProjector;
	use crate::svp::rust_highlight::TokenCategory;
	use std::collections::BTreeMap;
	use std::fs;
	use std::process::Command;
	use std::sync::{Arc, mpsc};

	fn write_test_init_file_at(
		path: &std::path::Path,
		settings: &SettingsPersistent,
		force: bool,
	) -> Result<(), String> {
		let language_registry = crate::engine::host::build_language_registry();
		super::write_init_file_at(path, settings, &language_registry, force)
	}

	fn write_test_default_init_file_at(path: &std::path::Path, force: bool) -> Result<(), String> {
		let language_registry = crate::engine::host::build_language_registry();
		super::write_default_init_file_at(path, &language_registry, force)
	}

	#[test]
	fn engine_input_owns_modal_state_and_rejects_stale_click_intents() {
		use crate::core::ViewportId;
		use crate::engine::{InputEvent, KeyCode, Modifiers};

		let (tx_cmd, tx_input, rx_view, handle) = spawn_test_engine_with_input(false, false, None);
		let initial = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(80, 24));
		let initial_viewport_id = initial.viewport_id;

		tx_input
			.send(InputEvent::Key {
				code: KeyCode::Char('s'),
				modifiers: Modifiers::NONE,
			})
			.expect("arm pane prefix");
		let pending = recv_view(&rx_view);
		assert_eq!(pending.input.pending_sequence.as_deref(), Some("s"));
		assert_eq!(pending.viewport_id, initial_viewport_id);

		tx_input
			.send(InputEvent::Key {
				code: KeyCode::Esc,
				modifiers: Modifiers::NONE,
			})
			.expect("cancel pane prefix");
		let cancelled = recv_view(&rx_view);
		assert_eq!(cancelled.input.pending_sequence, None);
		assert_eq!(cancelled.viewport_id, initial_viewport_id);

		tx_input
			.send(InputEvent::Key {
				code: KeyCode::Char('i'),
				modifiers: Modifiers::NONE,
			})
			.expect("enter insert mode");
		let inserting = recv_view(&rx_view);
		assert_eq!(inserting.input.mode, EditorMode::Insert);
		assert_eq!(inserting.viewport_id, initial_viewport_id);

		tx_input
			.send(InputEvent::Paste("abcd".to_string()))
			.expect("paste through input stream");
		let edited = recv_view(&rx_view);
		assert_eq!(edited.cursor_abs_pos.col, DocDisplayCol::new(4));
		assert_ne!(edited.viewport_id, initial_viewport_id);
		let current_viewport_id = edited.viewport_id;

		tx_cmd
			.send(EditorCommand::MouseIntent {
				viewport_id: initial_viewport_id,
				intent: MouseIntent::ClickCursor {
					target: CursorPosition::new(DocLine::ZERO, DocDisplayCol::ZERO),
					alt: false,
				},
			})
			.expect("stale click intent");
		tx_cmd
			.refresh_for_test()
			.expect("refresh after stale click");
		let after_stale = recv_view(&rx_view);
		assert_eq!(after_stale.cursor_abs_pos.col, DocDisplayCol::new(4));
		assert_eq!(after_stale.viewport_id, current_viewport_id);

		let clicked = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MouseIntent {
				viewport_id: current_viewport_id,
				intent: MouseIntent::ClickCursor {
					target: CursorPosition::new(DocLine::ZERO, DocDisplayCol::ZERO),
					alt: false,
				},
			},
		);
		assert_eq!(clicked.cursor_abs_pos.col, DocDisplayCol::ZERO);
		assert_eq!(clicked.viewport_id, current_viewport_id);

		let multi = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MouseIntent {
				viewport_id: current_viewport_id,
				intent: MouseIntent::ClickCursor {
					target: CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(2)),
					alt: true,
				},
			},
		);
		assert_eq!(multi.cursor_count, 2);
		assert_eq!(multi.input.mode, EditorMode::Insert);

		assert_ne!(current_viewport_id, ViewportId::ZERO);
		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn init_export_is_tab_indented_and_contains_one_snapshot() {
		let mut settings = SettingsPersistent {
			theme_name: "gruvbox".to_string(),
			minimap_enabled: false,
			wrap_enabled: true,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: false,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::from([("rust".to_string(), "viridis_cs8".to_string())]),
		};
		settings.ui_colors.set(
			crate::engine::UiColorKey::Accent,
			Some(crate::core::Rgb::new(0x12, 0x34, 0x56)),
		);

		let source = super::render_init_file(&settings);
		assert!(source.contains("\tlet config = editor.config();"));
		assert!(source.contains("\tconfig.set(\"theme\", \"gruvbox\")?;"));
		assert!(source.contains("\tconfig.ui_color(\"colors.accent\", \"#123456\")?;"));
		assert!(source.contains("\tconfig.language_theme(\"rust\", \"viridis_cs8\")?;"));
		assert!(
			source
				.lines()
				.skip(3)
				.all(|line| { line.is_empty() || line.starts_with('\t') || line == "}" })
		);
	}

	#[test]
	fn init0_replaces_user_configuration_with_builtin_defaults() {
		let path = temp_test_path("init0");
		std::fs::write(
			&path,
			"pub fn main(editor) { let config = editor.config(); config.set(\"theme\", \"gruvbox\")?; println!(\"custom\"); Ok(()) }\n",
		)
		.expect("write custom init");

		let error = write_test_default_init_file_at(&path, false)
			.expect_err("init0 protects custom init");
		assert!(error.contains("init0!"));
		assert!(std::fs::read_to_string(&path).expect("read protected init").contains("gruvbox"));

		write_test_default_init_file_at(&path, true).expect("init0! replaces custom init");
		let reset = std::fs::read_to_string(&path).expect("read default init");
		assert!(super::is_generated_init(&reset));
		assert!(!reset.contains("Generated by"));
		assert!(reset.contains("config.set(\"theme\", \"onedark\")?;"));
		assert!(!reset.contains("gruvbox"));
		assert!(rune::parse::parse_all::<rune::ast::File>(&reset, rune::SourceId::EMPTY, false).is_ok());
		let backup = std::fs::read_to_string(path.with_extension("rn.bak")).expect("read init backup");
		assert!(backup.contains("gruvbox"));
		let _ = std::fs::remove_file(&path);
		let _ = std::fs::remove_file(path.with_extension("rn.bak"));
	}

	#[test]
	fn init_export_updates_parsed_user_configuration_without_a_marker() {
		let mut settings = SettingsPersistent {
			theme_name: "gruvbox".to_string(),
			minimap_enabled: false,
			wrap_enabled: false,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: false,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::from([("markdown".to_string(), "tab20c_s1".to_string())]),
		};
		settings.ui_colors.set(
			crate::engine::UiColorKey::Accent,
			Some(crate::core::Rgb::new(0x12, 0x34, 0x56)),
		);
		let path = temp_test_path("init-export-update");

		write_test_init_file_at(&path, &settings, false).expect("absent init can be created");
		let generated = std::fs::read_to_string(&path).expect("generated init");
		assert!(super::is_generated_init(&generated));
		assert!(!generated.contains("Generated by"));
		write_test_init_file_at(&path, &settings, false).expect("generated init can be refreshed");

		std::fs::write(
			&path,
			r##"pub fn main(editor) {
	let config = editor.config();
	config.set("theme", "onedark")?;
	config.set("future_option", true)?;
	config.language_theme("markdown", "purples")?;
	println!("keep me");
	Ok(())
}
"##,
		)
			.expect("write handwritten init");
		write_test_init_file_at(&path, &settings, false)
			.expect("user configuration is updated");
		let updated = std::fs::read_to_string(&path).expect("read updated init");
		assert!(updated.contains("println!(\"keep me\");"));
		assert!(updated.contains("config.set(\"theme\", \"gruvbox\")?;"));
		assert!(updated.contains("config.set(\"future_option\", true)?;"));
		assert!(updated.contains("config.language_theme(\"markdown\", \"tab20c_s1\")?;"));
		assert!(updated.contains("config.ui_color(\"colors.accent\", \"#123456\")?;"));
		assert!(updated.contains("config.set(\"wrap\", false)?;"));
		assert!(!updated.contains("\n\t\tconfig."));
		assert!(updated.contains("\n\tOk(())\n}"));
		write_test_init_file_at(&path, &settings, false)
			.expect("a second update is idempotent");
		let updated_twice = std::fs::read_to_string(&path).expect("read second update");
		assert_eq!(updated_twice.matches("config.set(\"theme\"").count(), 1);
		assert_eq!(
			updated_twice
				.matches("config.language_theme(\"markdown\"")
				.count(),
			1
		);
		write_test_init_file_at(&path, &settings, true).expect("force replaces handwritten init");
		assert!(super::is_generated_init(
			&std::fs::read_to_string(&path).expect("read forced init")
		));
		assert!(
			std::fs::read_to_string(path.with_extension("rn.bak"))
				.expect("read forced init backup")
				.contains("println!(\"keep me\");")
		);
		let _ = std::fs::remove_file(&path);
		let _ = std::fs::remove_file(path.with_extension("rn.bak"));
	}

	#[test]
	fn init_export_refuses_to_edit_a_user_main_without_a_result_tail() {
		let settings = SettingsPersistent {
			theme_name: "onedark".to_string(),
			minimap_enabled: true,
			wrap_enabled: true,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::new(),
		};
		let path = temp_test_path("init-export-unsafe");
		std::fs::write(&path, "pub fn main(editor) {\n\tprintln!(\"keep me\");\n}\n")
			.expect("write handwritten init");

		let error = write_test_init_file_at(&path, &settings, false)
			.expect_err("unsafe main is refused");
		assert!(error.contains("must end with `Ok(())`"));
		assert!(
			std::fs::read_to_string(&path)
				.expect("read refused init")
				.contains("keep me")
		);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn init_export_inserts_on_a_new_line_after_same_line_user_code() {
		let settings = SettingsPersistent {
			theme_name: "gruvbox".to_string(),
			minimap_enabled: false,
			wrap_enabled: true,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::new(),
		};
		let path = temp_test_path("init-export-same-line");
		std::fs::write(
			&path,
			"pub fn main(editor) { let config = editor.config(); let x = 1; Ok(()) }\n",
		)
		.expect("write same-line init");

		write_test_init_file_at(&path, &settings, false).expect("same-line init is updated");
		let updated = std::fs::read_to_string(&path).expect("read updated init");
		assert_eq!(updated.matches("let x = 1;").count(), 1);
		assert!(updated.contains("let x = 1; \n\tconfig.set("));
		assert!(!updated.contains("\n\t\tconfig."));
		assert!(updated.contains("\n\tOk(()) }"));
		assert!(
			rune::parse::parse_all::<rune::ast::File>(&updated, rune::SourceId::EMPTY, false)
				.is_ok()
		);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn init_export_normalizes_duplicate_and_aliased_config_keys() {
		let settings = SettingsPersistent {
			theme_name: "gruvbox".to_string(),
			minimap_enabled: true,
			wrap_enabled: true,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::from([("markdown".to_string(), "tab20c_s1".to_string())]),
		};
		let path = temp_test_path("init-export-duplicates");
		std::fs::write(
			&path,
			r##"pub fn main(editor) {
	let config = editor.config();
	config.set("theme", "old")?;
	config.set("theme", "stale")?;
	config.language_theme("md", "old")?;
	config.language_theme("markdown", "stale")?;
	Ok(())
}
"##,
		)
		.expect("write duplicate init");

		write_test_init_file_at(&path, &settings, false).expect("duplicate init is normalized");
		let updated = std::fs::read_to_string(&path).expect("read normalized init");
		assert_eq!(updated.matches("config.set(\"theme\"").count(), 1);
		assert!(updated.contains("config.set(\"theme\", \"gruvbox\")?;"));
		assert_eq!(
			updated
				.matches("config.language_theme(\"markdown\"")
				.count(),
			1
		);
		assert!(updated.contains("config.language_theme(\"markdown\", \"tab20c_s1\")?;"));
		assert!(!updated.contains("\n\t\n"));
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn init_export_refuses_a_legacy_configuration_without_a_config_handle() {
		let settings = SettingsPersistent {
			theme_name: "gruvbox".to_string(),
			minimap_enabled: true,
			wrap_enabled: true,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::new(),
		};
		let path = temp_test_path("init-export-no-config-handle");
		std::fs::write(
			&path,
			"pub fn main(editor) { baryon::set(editor, \"theme\", \"onedark\")?; Ok(()) }\n",
		)
		.expect("write legacy init");

		let error = write_test_init_file_at(&path, &settings, false)
			.expect_err("legacy init without config handle is refused");
		assert!(error.contains("let config = editor.config()"));
		assert!(
			std::fs::read_to_string(&path)
				.expect("read refused legacy init")
				.contains("baryon::set")
		);
		let _ = std::fs::remove_file(path);
	}

	fn viewport_has_any_colored_visible_bytes(view: &crate::uast::Viewport) -> bool {
		let projector = HighlightProjector::new(view.highlights.clone(), view.theme_colors);
		for token in &view.tokens {
			if token.text.is_empty() {
				continue;
			}
			for (idx, &byte) in token.text.iter().enumerate() {
				if byte == b'\n' {
					continue;
				}
				let absolute = token.absolute_start_byte.saturating_add(idx as u64);
				if projector.style_for_byte(absolute).is_some() {
					return true;
				}
			}
		}
		false
	}

	fn assert_focused_theme(view: &crate::uast::Viewport, theme_name: &str) {
		assert_eq!(
			view.focused_pane().expect("focused pane").theme_colors,
			Theme::try_new(theme_name).expect("theme").syntax_colors
		);
	}

	#[test]
	fn exhausted_backing_ids_fail_before_the_save_changes_the_target() {
		let target = temp_test_path("exhausted-save-device-id");
		std::fs::write(&target, b"original bytes").expect("write original target");
		let registry = Arc::new(UastRegistry::new(2));
		let (root, _) = create_empty_document(&registry).expect("create empty document");
		let (notify, _notifications) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), notify);
		let languages = LanguageRegistry::new();
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
		};
		let mut next_device_id = u16::MAX;
		let mut backing_device_id = None;

		let error = save_document_with_fresh_backing(
			&ctx,
			root,
			None,
			target.to_str().expect("utf8 target path"),
			SaveDurability::Atomic,
			&mut next_device_id,
			&mut backing_device_id,
		)
		.expect_err("exhausted device ids must prevent the save");

		assert_eq!(error, "Out of dynamic backing device ids");
		assert_eq!(next_device_id, u16::MAX);
		assert_eq!(
			std::fs::read(&target).expect("read untouched target"),
			b"original bytes"
		);
		let _ = std::fs::remove_file(target);
	}

	#[test]
	fn repeated_saves_retain_only_the_current_backing_generation() {
		let target = temp_test_path("bounded-save-backing-count");
		let registry = Arc::new(UastRegistry::new(2));
		let (root, _) = create_empty_document(&registry).expect("create empty document");
		let (notify, _notifications) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), notify);
		let languages = LanguageRegistry::new();
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
		};
		let mut next_device_id = crate::engine::core::FILE_DEVICE_ID + 1;
		let mut backing_device_id = None;

		for _ in 0..600 {
			let source_path = backing_device_id.map(|_| target.as_path());
			let completion = save_document_with_fresh_backing(
				&ctx,
				root,
				source_path,
				target.to_str().expect("utf8 target path"),
				SaveDurability::Atomic,
				&mut next_device_id,
				&mut backing_device_id,
			)
			.expect("save document");
			assert!(completion.rebound);
			assert_eq!(resolver.registered_device_counts(), (1, 0));
		}

		resolver.unregister_device(backing_device_id.take().expect("current backing"));
		assert_eq!(resolver.registered_device_counts(), (0, 0));
		let _ = std::fs::remove_file(target);
	}

	#[test]
	fn repeated_reloads_retire_each_replaced_backing_generation() {
		let path = temp_test_path("bounded-reload-backing-count");
		std::fs::write(&path, b"reload me\n").expect("write source");
		let registry = Arc::new(UastRegistry::new(2_000));
		let (notify, _notifications) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), notify);
		let languages = LanguageRegistry::new();
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
		};
		let settings = SettingsPersistent {
			theme_name: "onedark".to_string(),
			minimap_enabled: false,
			wrap_enabled: false,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::new(),
		};
		let mut fixture = TestTextEditor::new(&registry, &settings, 80, 24);
		let mut tab = fixture.editor_mut();
		let mut ui = UiState {
			status_message: None,
			document_text_response: None,
			busy: false,
			pending_quit: false,
			mode_override: None,
			yank_flash: None,
		};
		let mut next_device_id = crate::engine::core::FILE_DEVICE_ID + 1;

		handle_file_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_device_id,
			SaveDurability::Atomic,
			EditorCommand::LoadFile(path.to_string_lossy().into_owned()),
		);
		assert_eq!(resolver.registered_device_counts(), (1, 0));
		assert_eq!(tab.document.topology_rev, crate::core::TopologyRev::new(1));

		for reload_index in 0..600 {
			handle_file_doctab_command(
				&ctx,
				&mut tab,
				&mut ui,
				&mut next_device_id,
				SaveDurability::Atomic,
				EditorCommand::ReloadFile { force: true },
			);
			assert_eq!(resolver.registered_device_counts(), (1, 0));
			assert_eq!(
				tab.document.topology_rev,
				crate::core::TopologyRev::new(reload_index + 2)
			);
		}

		resolver.unregister_device(
			tab.document
				.backing_device_id
				.take()
				.expect("current backing"),
		);
		assert_eq!(resolver.registered_device_counts(), (0, 0));
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn post_write_registration_failure_still_completes_the_save_without_rebinding() {
		let target = temp_test_path("registration-failure-save-target");
		let occupied_backing = temp_test_path("registration-failure-occupied-device");
		std::fs::write(&occupied_backing, b"occupied").expect("write occupied backing");
		let registry = Arc::new(UastRegistry::new(2));
		let (root, leaf) = create_empty_document(&registry).expect("create empty document");
		let (notify, _notifications) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), notify);
		resolver
			.register_device(77, occupied_backing.to_str().expect("utf8 occupied path"))
			.expect("occupy next device id");
		let languages = LanguageRegistry::new();
		let ctx = RuntimeContext {
			registry: &registry,
			resolver: &resolver,
			lang_registry: &languages,
		};
		let settings = SettingsPersistent {
			theme_name: "onedark".to_string(),
			minimap_enabled: false,
			wrap_enabled: false,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::Atomic,
			ui_colors: crate::engine::UiColorOverrides::default(),
			language_themes: BTreeMap::new(),
		};
		let mut fixture = TestTextEditor::from_existing_text(root, leaf, &settings, 80, 24);
		let mut tab = fixture.editor_mut();
		tab.document.file_path = Some(target.to_string_lossy().into_owned());
		tab.cursor.primary_mut().reset_to_start(
			leaf,
			tab.document.topology_rev,
			tab.document.ledger.current_state_id,
		);
		let cursors = tab.cursor.snapshot();
		tab.document.ledger.push_group_with_cursors(
			vec![TextDelta {
				global_byte_offset: DocByte::ZERO,
				deleted_bytes: Vec::new(),
				inserted_bytes: "dirty".as_bytes().to_vec(),
				state_before: crate::core::StateId::ZERO,
				state_after: crate::core::StateId::ZERO,
			}],
			cursors.clone(),
			cursors,
		);
		assert!(tab.document.ledger.is_dirty());
		let mut ui = UiState {
			status_message: None,
			document_text_response: None,
			busy: false,
			pending_quit: false,
			mode_override: None,
			yank_flash: None,
		};
		let mut next_device_id = 77;

		handle_file_doctab_command(
			&ctx,
			&mut tab,
			&mut ui,
			&mut next_device_id,
			SaveDurability::Atomic,
			EditorCommand::WriteFile,
		);

		assert_eq!(std::fs::read(&target).expect("read saved target"), b"");
		assert!(!tab.document.ledger.is_dirty());
		assert_eq!(tab.document.backing_device_id, None);
		assert_eq!(tab.document.backing_path, None);
		assert!(
			ui.status_message
				.as_deref()
				.is_some_and(|message| message.contains("keeping the current in-memory document"))
		);
		assert_eq!(resolver.registered_device_counts(), (1, 0));
		let mut occupied = [0; 8];
		registry
			.read_backing_exact_at(77, 0, &mut occupied)
			.expect("read occupied backing");
		assert_eq!(&occupied, b"occupied");

		resolver.unregister_device(77);
		let _ = std::fs::remove_file(target);
		let _ = std::fs::remove_file(occupied_backing);
	}

	#[test]
	fn set_minimap_command_disables_view_minimap_without_persisting_setting() {
		let settings_path = temp_test_path("minimap-settings");
		let scratch_path = temp_test_path("scratch-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let initial_view = recv_view(&rx_view);
		assert_eq!(
			initial_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert!(initial_view.minimap.is_some());

		tx_cmd
			.send(EditorCommand::SetMinimap(false))
			.expect("disable minimap");
		let disabled_view = recv_view(&rx_view);
		assert_eq!(
			disabled_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert!(disabled_view.minimap.is_none());
		assert!(
			!settings_path.exists(),
			"local SetMinimap must not write persistent settings"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn pane_local_background_uses_the_strict_ui_rgb_grammar() {
		let scratch_path = temp_test_path("strict-local-background-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, None);

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name))
			.expect("load scratch buffer");
		let _ = recv_view(&rx_view);

		tx_cmd
			.send(EditorCommand::SetBackground("1a2b3".to_string()))
			.expect("send invalid local background");
		let invalid = recv_view(&rx_view);
		assert_eq!(
			invalid.status_message.as_deref(),
			Some("Invalid hex color: 1a2b3")
		);

		tx_cmd
			.send(EditorCommand::SetBackground("abc".to_string()))
			.expect("send valid local background");
		let valid = recv_view(&rx_view);
		assert_eq!(
			valid.status_message.as_deref(),
			Some("Background set to #AABBCC")
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn persist_minimap_command_is_session_scoped_without_file_write() {
		let settings_path = temp_test_path("persist-minimap-settings");
		let scratch_path = temp_test_path("persist-minimap-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let _ = recv_view(&rx_view);

		tx_cmd
			.send(EditorCommand::PersistMinimap(false))
			.expect("persist minimap off");
		let disabled_view = recv_view(&rx_view);
		assert!(disabled_view.minimap.is_none());

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn set_wrap_command_disables_view_wrap_without_persisting_setting() {
		let settings_path = temp_test_path("wrap-settings");
		let scratch_path = temp_test_path("wrap-scratch-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let initial_view = recv_view(&rx_view);
		assert_eq!(
			initial_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert!(initial_view.wrap_enabled);

		tx_cmd
			.send(EditorCommand::SetWrap(false))
			.expect("disable wrap");
		let disabled_view = recv_view(&rx_view);
		assert_eq!(
			disabled_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert!(!disabled_view.wrap_enabled);
		assert!(
			!settings_path.exists(),
			"local SetWrap must not write persistent settings"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_wrap_command_updates_active_view_without_file_write() {
		let settings_path = temp_test_path("persist-wrap-settings");
		let scratch_path = temp_test_path("persist-wrap-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let _ = recv_view(&rx_view);

		tx_cmd
			.send(EditorCommand::PersistWrap(false))
			.expect("persist wrap off");
		let disabled_view = recv_view(&rx_view);
		assert!(!disabled_view.wrap_enabled);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn set_line_numbers_command_hides_gutter_without_persisting_setting() {
		let settings_path = temp_test_path("line-numbers-settings");
		let scratch_path = temp_test_path("line-numbers-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let initial_view = recv_view(&rx_view);
		assert_eq!(
			initial_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert_eq!(
			initial_view.gutter_numbering,
			Some(GutterNumbering::Absolute)
		);

		tx_cmd
			.send(EditorCommand::SetLineNumbers(false))
			.expect("hide line numbers");
		let hidden_view = recv_view(&rx_view);
		assert_eq!(
			hidden_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert_eq!(hidden_view.gutter_numbering, None);
		assert!(
			!settings_path.exists(),
			"local SetLineNumbers must not write persistent settings"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_line_numbers_command_updates_active_view_without_file_write() {
		let settings_path = temp_test_path("persist-line-numbers-settings");
		let scratch_path = temp_test_path("persist-line-numbers-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let _ = recv_view(&rx_view);

		tx_cmd
			.send(EditorCommand::PersistLineNumbers(false))
			.expect("persist line numbers hidden");
		let hidden_view = recv_view(&rx_view);
		assert_eq!(hidden_view.gutter_numbering, None);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_relative_line_numbers_shows_gutter_without_file_write() {
		let settings_path = temp_test_path("persist-relative-line-numbers-settings");
		let scratch_path = temp_test_path("persist-relative-line-numbers-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name))
			.expect("load scratch buffer");
		let _ = recv_view(&rx_view);

		tx_cmd
			.send(EditorCommand::PersistLineNumbers(false))
			.expect("persist absolute line numbers off");
		let absolute_off = recv_view(&rx_view);
		assert_eq!(absolute_off.gutter_numbering, None);

		tx_cmd
			.send(EditorCommand::PersistRelativeLineNumbers(true))
			.expect("persist relative line numbers on");
		let relative_view = recv_view(&rx_view);
		assert_eq!(
			relative_view.gutter_numbering,
			Some(GutterNumbering::Relative)
		);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn set_visible_whitespace_command_hides_whitespace_without_persisting_setting() {
		let settings_path = temp_test_path("visible-whitespace-settings");
		let scratch_path = temp_test_path("visible-whitespace-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let initial_view = recv_view(&rx_view);
		assert_eq!(
			initial_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert!(initial_view.visible_whitespace);

		tx_cmd
			.send(EditorCommand::SetVisibleWhitespace(false))
			.expect("hide whitespace");
		let hidden_view = recv_view(&rx_view);
		assert_eq!(
			hidden_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert!(!hidden_view.visible_whitespace);
		assert!(
			!settings_path.exists(),
			"local SetVisibleWhitespace must not write persistent settings"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_visible_whitespace_command_updates_active_view_without_file_write() {
		let settings_path = temp_test_path("persist-visible-whitespace-settings");
		let scratch_path = temp_test_path("persist-visible-whitespace-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load scratch buffer");
		let _ = recv_view(&rx_view);

		tx_cmd
			.send(EditorCommand::PersistVisibleWhitespace(false))
			.expect("persist whitespace hidden");
		let hidden_view = recv_view(&rx_view);
		assert!(!hidden_view.visible_whitespace);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_search_highlight_updates_view_without_file_write() {
		let settings_path = temp_test_path("persist-search-highlight-settings");
		let scratch_path = temp_test_path("persist-search-highlight-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name))
			.expect("load scratch buffer");
		let initial = recv_view(&rx_view);
		assert_eq!(
			initial.ui_palette.search_highlight,
			crate::core::SEARCH_MATCH_BG
		);

		tx_cmd
			.send(EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::SearchHighlight,
				value: Some(crate::core::Rgb::new(0x12, 0x34, 0x56)),
			})
			.expect("persist search highlight");
		let updated = recv_view(&rx_view);
		assert_eq!(
			updated.ui_palette.search_highlight,
			crate::core::Rgb::new(0x12, 0x34, 0x56)
		);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn ui_color_fallback_pinning_reset_and_query_update_the_view() {
		let scratch_path = temp_test_path("ui-color-cascade-buffer");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, None);
		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name))
			.expect("load scratch buffer");
		let initial = recv_view(&rx_view);
		let editor_background = initial.ui_palette.editor_background;
		let editor_text = initial.ui_palette.editor_text;
		let search_highlight = initial.ui_palette.search_highlight;

		let accent = crate::core::Rgb::new(0x11, 0x22, 0x33);
		tx_cmd
			.send(EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::Accent,
				value: Some(accent),
			})
			.expect("set accent");
		let accented = recv_view(&rx_view);
		assert_eq!(accented.ui_palette.gutter_accent, accent);
		assert_eq!(accented.ui_palette.minimap_accent, accent);
		assert_eq!(accented.ui_palette.tab_accent, accent);
		assert_eq!(accented.ui_palette.search_highlight, search_highlight);

		let pinned = crate::core::Rgb::new(0xaa, 0xbb, 0xcc);
		tx_cmd
			.send(EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::MinimapAccent,
				value: Some(pinned),
			})
			.expect("pin minimap accent");
		let pinned_view = recv_view(&rx_view);
		assert_eq!(pinned_view.ui_palette.minimap_accent, pinned);

		tx_cmd
			.send(EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::Accent,
				value: Some(crate::core::Rgb::new(0x44, 0x55, 0x66)),
			})
			.expect("change parent accent");
		let parent_changed = recv_view(&rx_view);
		assert_eq!(parent_changed.ui_palette.minimap_accent, pinned);

		tx_cmd
			.send(EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::MinimapAccent,
				value: None,
			})
			.expect("reset minimap accent");
		let reset = recv_view(&rx_view);
		assert_eq!(reset.ui_palette.minimap_accent, reset.ui_palette.accent);

		let chrome = crate::core::Rgb::new(0x20, 0x20, 0x20);
		tx_cmd
			.send(EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::Surface,
				value: Some(chrome),
			})
			.expect("set chrome surface");
		let surfaced = recv_view(&rx_view);
		assert_eq!(surfaced.ui_palette.gutter_surface, chrome);
		assert_eq!(surfaced.ui_palette.minimap_surface, chrome);
		assert_eq!(surfaced.ui_palette.status_surface, chrome);
		assert_eq!(surfaced.ui_palette.tab_surface, chrome);
		assert_eq!(surfaced.ui_palette.preview_surface, chrome);
		assert_eq!(surfaced.ui_palette.editor_background, editor_background);

		let chrome_text = crate::core::Rgb::new(0x11, 0x11, 0x11);
		tx_cmd
			.send(EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::Text,
				value: Some(chrome_text),
			})
			.expect("set chrome text");
		let text_view = recv_view(&rx_view);
		assert_eq!(text_view.ui_palette.gutter_text, chrome_text);
		assert_eq!(text_view.ui_palette.status_text, chrome_text);
		assert_eq!(text_view.ui_palette.tab_text, chrome_text);
		assert_eq!(text_view.ui_palette.preview_text, chrome_text);
		assert_eq!(text_view.ui_palette.editor_text, editor_text);
		let palette_before_theme_change = text_view.ui_palette;
		tx_cmd
			.send(EditorCommand::PersistTheme("gruvbox".to_string()))
			.expect("change syntax theme");
		let theme_changed = recv_view(&rx_view);
		assert_eq!(theme_changed.ui_palette, palette_before_theme_change);

		tx_cmd
			.send(EditorCommand::QueryUiColor(
				crate::engine::UiColorKey::MinimapAccent,
			))
			.expect("query minimap accent");
		let queried = recv_view(&rx_view);
		assert!(
			queried
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("inherited from colors.accent"))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn doctab_commands_open_cycle_and_close_tabs() {
		let first_path = temp_test_path("doctab-first");
		let second_path = temp_test_path("doctab-second");
		std::fs::write(&first_path, "first\n").expect("write first file");
		std::fs::write(&second_path, "second\n").expect("write second file");
		let first_name = first_path.to_string_lossy().to_string();
		let second_name = second_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		tx_cmd
			.send(EditorCommand::LoadFile(first_name.clone()))
			.expect("load first doctab");
		let first_view = recv_view(&rx_view);
		assert_eq!(first_view.file_name.as_deref(), Some(first_name.as_str()));
		assert_eq!(first_view.tabs.len(), 1);
		assert_eq!(first_view.active_doctab_index, 0);

		tx_cmd
			.send(EditorCommand::DocTabEdit(second_name.clone()))
			.expect("open second doctab");
		let second_view = recv_view(&rx_view);
		assert_eq!(second_view.file_name.as_deref(), Some(second_name.as_str()));
		assert_eq!(second_view.tabs.len(), 2);
		assert_eq!(second_view.active_doctab_index, 1);

		tx_cmd
			.send(EditorCommand::DocTabPrev)
			.expect("move to previous doctab");
		let previous_view = recv_view(&rx_view);
		assert_eq!(
			previous_view.file_name.as_deref(),
			Some(first_name.as_str())
		);
		assert_eq!(previous_view.active_doctab_index, 0);

		tx_cmd
			.send(EditorCommand::DocTabClose)
			.expect("close current doctab");
		let closed_view = recv_view(&rx_view);
		assert_eq!(closed_view.file_name.as_deref(), Some(second_name.as_str()));
		assert_eq!(closed_view.tabs.len(), 1);
		assert_eq!(closed_view.active_doctab_index, 0);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(first_path);
		let _ = std::fs::remove_file(second_path);
	}

	#[test]
	fn split_focus_and_close_pane_commands_update_the_layout_tree() {
		let scratch_path = temp_test_path("split-pane");
		std::fs::write(&scratch_path, "alpha\nbeta\n").expect("write split-pane file");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load initial pane");
		let initial_view = recv_view(&rx_view);
		assert_eq!(initial_view.panes.len(), 1);
		assert_eq!(
			initial_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		let original_pane_id = initial_view.focused_pane_id;

		tx_cmd
			.send(EditorCommand::SplitPane(
				crate::engine::SplitDirection::Vertical,
			))
			.expect("split vertical");
		let split_view = recv_view(&rx_view);
		assert_eq!(split_view.panes.len(), 2);
		assert_ne!(split_view.focused_pane_id, original_pane_id);
		assert_eq!(
			split_view.file_name.as_deref(),
			Some(scratch_name.as_str()),
			"a plain split attaches another view to the current buffer"
		);

		tx_cmd
			.send(EditorCommand::FocusPaneDirection(
				crate::engine::PaneFocusDirection::Left,
			))
			.expect("focus original pane");
		let focused_original = recv_view(&rx_view);
		assert_eq!(
			focused_original.file_name.as_deref(),
			Some(scratch_name.as_str())
		);
		assert_eq!(focused_original.focused_pane_id, original_pane_id);

		tx_cmd
			.send(EditorCommand::FocusPaneDirection(
				crate::engine::PaneFocusDirection::Right,
			))
			.expect("focus new pane");
		let focused_split = recv_view(&rx_view);
		assert_eq!(focused_split.panes.len(), 2);
		assert_eq!(
			focused_split.file_name.as_deref(),
			Some(scratch_name.as_str())
		);

		tx_cmd
			.send(EditorCommand::ClosePane)
			.expect("close empty pane");
		let closed_view = recv_view(&rx_view);
		assert_eq!(closed_view.panes.len(), 1);
		assert_eq!(closed_view.focused_pane_id, original_pane_id);
		assert_eq!(
			closed_view.file_name.as_deref(),
			Some(scratch_name.as_str())
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(scratch_path);
	}

	#[test]
	fn closing_the_last_ordinary_tab_in_a_split_closes_its_pane() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let initial = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		assert_eq!(initial.panes.len(), 2);
		let closing_pane_id = initial.focused_pane_id;
		let closing_view_id = initial
			.focused_pane()
			.expect("split pane focused")
			.tabs[0]
			.view_id;

		let closed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::CloseDocTab {
				pane_id: closing_pane_id,
				view_id: closing_view_id,
			},
		);
		assert!(!closed.should_quit);
		assert_eq!(closed.panes.len(), 1);
		assert!(closed.pane(closing_pane_id).is_none());

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn split_views_share_edits_and_undo() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let initial = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("a".to_string()),
		);
		let left = initial.focused_pane_id;
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let right = split.focused_pane_id;
		assert_ne!(left, right);

		let edited = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("b".to_string()),
		);
		assert_eq!(pane_text(&edited, left), "ba");
		assert_eq!(pane_text(&edited, right), "ba");

		let focused_left = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(left));
		assert_eq!(focused_left.focused_pane_id, left);
		let undone = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Undo);
		assert_eq!(pane_text(&undone, left), "a");
		assert_eq!(pane_text(&undone, right), "a");

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn shared_views_clamp_anchors_after_reload_replaces_the_root() {
		let path = temp_test_path("shared-reload");
		std::fs::write(&path, "abc\n").expect("write reload file");
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let loaded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::LoadFile(path.to_string_lossy().into_owned()),
		);
		let left = loaded.focused_pane_id;
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let right = split.focused_pane_id;
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("x".to_string()),
		);
		std::fs::write(&path, "").expect("replace reload file");
		let reloaded =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ReloadFile { force: true });
		assert_eq!(pane_text(&reloaded, left), "");
		assert_eq!(pane_text(&reloaded, right), "");
		assert_eq!(
			reloaded.pane(left).expect("left pane").cursor_abs_byte,
			DocByte::ZERO
		);
		assert_eq!(
			reloaded.pane(right).expect("right pane").cursor_abs_byte,
			DocByte::ZERO
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn canonical_tabedit_reuses_one_buffer() {
		let path = temp_test_path("canonical-tabedit");
		std::fs::write(&path, "shared\n").expect("write shared file");
		let direct = path.to_string_lossy().into_owned();
		let alias = path
			.parent()
			.expect("test path parent")
			.join(".")
			.join(path.file_name().expect("test path name"))
			.to_string_lossy()
			.into_owned();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(direct));
		let opened = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabEdit(alias));
		assert_eq!(opened.tabs.len(), 2);

		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("x".to_string()),
		);
		let first = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabPrev);
		assert!(pane_text(&first, first.focused_pane_id).starts_with("xshared"));

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn search_matches_from_an_older_session_pattern_are_hidden() {
		let first_path = temp_test_path("search-revision-first");
		let second_path = temp_test_path("search-revision-second");
		std::fs::write(&first_path, "alpha\n").expect("write first search file");
		std::fs::write(&second_path, "beta\n").expect("write second search file");
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::LoadFile(first_path.to_string_lossy().into_owned()),
		);
		let first_search = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchStart("alpha".to_string()),
		);
		assert!(first_search.search_match_info.is_some());
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::DocTabEdit(second_path.to_string_lossy().into_owned()),
		);
		let second_search = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchStart("beta".to_string()),
		);
		assert!(second_search.search_match_info.is_some());
		let stale = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabPrev);
		assert_eq!(stale.search_match_info, None);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(first_path);
		let _ = std::fs::remove_file(second_path);
	}

	#[test]
	fn write_file_as_rejects_a_path_owned_by_another_live_buffer_before_writing() {
		let first_path = temp_test_path("save-as-owner-first");
		let second_path = temp_test_path("save-as-owner-second");
		std::fs::write(&first_path, "first\n").expect("write first file");
		std::fs::write(&second_path, "second\n").expect("write second file");
		let first_name = first_path.to_string_lossy().into_owned();
		let second_name = second_path.to_string_lossy().into_owned();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::LoadFile(first_name.clone()),
		);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::DocTabEdit(second_name.clone()),
		);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabPrev);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("x".to_string()),
		);
		let refused = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFileAs(second_name));
		assert_eq!(refused.file_name.as_deref(), Some(first_name.as_str()));
		assert_eq!(
			std::fs::read_to_string(&second_path).expect("read collision target"),
			"second\n"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(first_path);
		let _ = std::fs::remove_file(second_path);
	}

	#[test]
	fn diff_split_rejects_the_current_buffer_through_a_path_alias() {
		let path = temp_test_path("self-diff");
		std::fs::write(&path, "same\n").expect("write diff file");
		let direct = path.to_string_lossy().into_owned();
		let alias = path
			.parent()
			.expect("diff path parent")
			.join(".")
			.join(path.file_name().expect("diff path name"))
			.to_string_lossy()
			.into_owned();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(direct));
		let refused = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DiffSplit(alias));
		assert_eq!(refused.panes.len(), 1);
		assert_eq!(
			refused.status_message.as_deref(),
			Some("Cannot diff a buffer against itself")
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn pane_resize_commands_target_the_nearest_matching_ancestor_split() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let initial_view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 41));
		let left_pane_id = initial_view.focused_pane_id;

		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);

		let nested_view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Horizontal),
		);
		let bottom_right_pane_id = nested_view.focused_pane_id;
		let top_right_pane_id = nested_view
			.panes
			.iter()
			.find(|pane| pane.pane_id != left_pane_id && pane.pane_id != bottom_right_pane_id)
			.expect("top-right pane must exist")
			.pane_id;

		let left_before = nested_view.pane(left_pane_id).expect("left pane").rect;
		let top_right_before = nested_view
			.pane(top_right_pane_id)
			.expect("top-right pane")
			.rect;
		let bottom_right_before = nested_view
			.pane(bottom_right_pane_id)
			.expect("bottom-right pane")
			.rect;

		let width_resized = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Vertical,
				crate::engine::PaneResizeDirection::Increase,
			),
		);

		let left_after_width = width_resized.pane(left_pane_id).expect("left pane").rect;
		let top_right_after_width = width_resized
			.pane(top_right_pane_id)
			.expect("top-right pane")
			.rect;
		let bottom_right_after_width = width_resized
			.pane(bottom_right_pane_id)
			.expect("bottom-right pane")
			.rect;

		assert!(
			left_after_width.width.get() < left_before.width.get(),
			"vertical resize should shrink the left sibling pane"
		);
		assert!(
			top_right_after_width.width.get() > top_right_before.width.get(),
			"vertical resize should widen the focused column"
		);
		assert_eq!(
			top_right_after_width.width.get(),
			bottom_right_after_width.width.get(),
			"both panes in the right column should track the enclosing vertical split width"
		);
		assert_eq!(
			top_right_after_width.height.get(),
			top_right_before.height.get(),
			"vertical resize should not change the nested horizontal split heights"
		);
		assert_eq!(
			bottom_right_after_width.height.get(),
			bottom_right_before.height.get(),
			"vertical resize should not change the focused pane height"
		);

		let height_resized = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Horizontal,
				crate::engine::PaneResizeDirection::Increase,
			),
		);

		let top_right_after_height = height_resized
			.pane(top_right_pane_id)
			.expect("top-right pane")
			.rect;
		let bottom_right_after_height = height_resized
			.pane(bottom_right_pane_id)
			.expect("bottom-right pane")
			.rect;

		assert_eq!(
			top_right_after_height.width.get(),
			top_right_after_width.width.get(),
			"horizontal resize should not change the enclosing column width"
		);
		assert_eq!(
			bottom_right_after_height.width.get(),
			bottom_right_after_width.width.get(),
			"horizontal resize should preserve focused pane width"
		);
		assert!(
			top_right_after_height.height.get() < top_right_after_width.height.get(),
			"horizontal resize should shrink the top sibling in the nested split"
		);
		assert!(
			bottom_right_after_height.height.get() > bottom_right_after_width.height.get(),
			"horizontal resize should grow the focused pane within the nested split"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn pane_resize_reports_missing_axis_and_size_limits() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let no_axis = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Horizontal,
				crate::engine::PaneResizeDirection::Increase,
			),
		);
		assert_eq!(
			no_axis.status_message.as_deref(),
			Some("No horizontal split to resize")
		);

		let split_view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let sibling_pane_id = split_view
			.panes
			.iter()
			.find(|pane| pane.pane_id != split_view.focused_pane_id)
			.expect("split must retain a sibling pane")
			.pane_id;
		let mut last_width = split_view
			.focused_pane()
			.expect("focused pane after split")
			.rect
			.width
			.get();
		let mut at_limit = None;
		for _ in 0..200 {
			let resized = send_cmd_and_recv(
				&tx_cmd,
				&rx_view,
				EditorCommand::ResizePane(
					crate::engine::SplitDirection::Vertical,
					crate::engine::PaneResizeDirection::Increase,
				),
			);
			let resized_width = resized
				.focused_pane()
				.expect("focused pane after resize")
				.rect
				.width
				.get();
			if resized.status_message.as_deref() == Some("Pane cannot be resized further") {
				at_limit = Some(resized);
				break;
			}
			last_width = resized_width;
		}

		let at_limit = at_limit.expect("resize loop should eventually hit a size limit");
		assert_eq!(
			at_limit.status_message.as_deref(),
			Some("Pane cannot be resized further")
		);
		assert_eq!(
			at_limit
				.focused_pane()
				.expect("focused pane at limit")
				.rect
				.width
				.get(),
			last_width
		);
		assert!(
			at_limit
				.pane(sibling_pane_id)
				.expect("sibling pane at limit")
				.rect
				.width
				.get() >= 2,
			"resizing must leave every leaf pane at least two cells wide"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn pane_resize_steps_change_one_rendered_cell_at_a_time() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let initial_view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 41));
		let first_pane_id = initial_view.focused_pane_id;

		let vertical_split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let focused_vertical_before = vertical_split
			.focused_pane()
			.expect("focused pane after vertical split")
			.rect
			.width
			.get();
		let sibling_vertical_before = vertical_split
			.pane(first_pane_id)
			.expect("sibling pane after vertical split")
			.rect
			.width
			.get();

		let vertical_resized = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Vertical,
				crate::engine::PaneResizeDirection::Increase,
			),
		);
		assert_eq!(
			vertical_resized
				.focused_pane()
				.expect("focused pane after vertical resize")
				.rect
				.width
				.get(),
			focused_vertical_before + 1
		);
		assert_eq!(
			vertical_resized
				.pane(first_pane_id)
				.expect("sibling pane after vertical resize")
				.rect
				.width
				.get(),
			sibling_vertical_before - 1
		);

		let horizontal_split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Horizontal),
		);
		let focused_horizontal_before = horizontal_split
			.focused_pane()
			.expect("focused pane after horizontal split")
			.rect
			.height
			.get();
		let sibling_horizontal_pane_id = horizontal_split
			.panes
			.iter()
			.find(|pane| {
				pane.pane_id != first_pane_id && pane.pane_id != horizontal_split.focused_pane_id
			})
			.expect("horizontal sibling pane must exist")
			.pane_id;
		let sibling_horizontal_before = horizontal_split
			.pane(sibling_horizontal_pane_id)
			.expect("sibling pane after horizontal split")
			.rect
			.height
			.get();

		let horizontal_resized = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Horizontal,
				crate::engine::PaneResizeDirection::Increase,
			),
		);
		assert_eq!(
			horizontal_resized
				.focused_pane()
				.expect("focused pane after horizontal resize")
				.rect
				.height
				.get(),
			focused_horizontal_before + 1
		);
		assert_eq!(
			horizontal_resized
				.pane(sibling_horizontal_pane_id)
				.expect("sibling pane after horizontal resize")
				.rect
				.height
				.get(),
			sibling_horizontal_before - 1
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn pane_boundary_resize_targets_the_dragged_nested_divider() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let initial_view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 41));
		let inner_left_id = initial_view.focused_pane_id;
		let outer_split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let outer_right_id = outer_split.focused_pane_id;
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(inner_left_id));
		let nested_split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let inner_right_id = nested_split.focused_pane_id;
		let outer_right_before = nested_split
			.pane(outer_right_id)
			.expect("outer-right pane")
			.rect
			.screen_col
			.get();
		let outer_target_before = nested_split
			.pane(inner_right_id)
			.expect("inner-right pane")
			.rect
			.right()
			.get();

		tx_cmd
			.send(EditorCommand::ResizePaneBoundary {
				before: inner_right_id,
				after: outer_right_id,
				axis: crate::engine::SplitDirection::Vertical,
				target: outer_target_before,
			})
			.expect("send boundary resize to its current target");
		let unchanged = send_refresh_and_recv(&tx_cmd, &rx_view);
		assert_eq!(
			unchanged
				.pane(outer_right_id)
				.expect("outer-right pane after no-op")
				.rect
				.screen_col
				.get(),
			outer_right_before
		);
		assert_eq!(unchanged.status_message, None);

		let outer_resized = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePaneBoundary {
				before: inner_right_id,
				after: outer_right_id,
				axis: crate::engine::SplitDirection::Vertical,
				target: outer_target_before + 3,
			},
		);
		let outer_right_after = outer_resized
			.pane(outer_right_id)
			.expect("outer-right pane after resize")
			.rect
			.screen_col
			.get();
		assert_eq!(outer_right_after, outer_right_before + 3);
		let inner_right_before = outer_resized
			.pane(inner_right_id)
			.expect("inner-right pane before inner resize")
			.rect
			.screen_col
			.get();
		let inner_target_before = outer_resized
			.pane(inner_left_id)
			.expect("inner-left pane before inner resize")
			.rect
			.right()
			.get();

		let inner_resized = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePaneBoundary {
				before: inner_left_id,
				after: inner_right_id,
				axis: crate::engine::SplitDirection::Vertical,
				target: inner_target_before - 2,
			},
		);
		assert_eq!(
			inner_resized
				.pane(outer_right_id)
				.expect("outer-right pane after inner resize")
				.rect
				.screen_col
				.get(),
			outer_right_after,
			"dragging the nested divider must not move its enclosing divider"
		);
		assert_eq!(
			inner_resized
				.pane(inner_right_id)
				.expect("inner-right pane after resize")
				.rect
				.screen_col
				.get(),
			inner_right_before - 2,
			"the nested divider should land on its absolute target"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn queued_pane_boundary_targets_converge_without_viewport_feedback() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 41));
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let mut panes = split.panes.iter().collect::<Vec<_>>();
		panes.sort_by_key(|pane| pane.rect.screen_col);
		let before = panes[0];
		let after = panes[1];
		let initial_target = before.rect.right().get();
		let initial_after_col = after.rect.screen_col.get();

		for target in [
			initial_target + 1,
			initial_target + 2,
			initial_target + 3,
			initial_target + 3,
		] {
			tx_cmd
				.send(EditorCommand::ResizePaneBoundary {
					before: before.pane_id,
					after: after.pane_id,
					axis: crate::engine::SplitDirection::Vertical,
					target,
				})
				.expect("queue absolute boundary target");
		}

		let final_view = send_refresh_and_recv(&tx_cmd, &rx_view);
		assert_eq!(
			final_view
				.pane(after.pane_id)
				.expect("pane after queued boundary targets")
				.rect
				.screen_col
				.get(),
			initial_after_col + 3,
			"queued targets must converge to the final coordinate without cumulative overshoot"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn absolute_pane_boundary_targets_clamp_and_recover() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 41));
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let mut panes = split.panes.iter().collect::<Vec<_>>();
		panes.sort_by_key(|pane| pane.rect.screen_col);
		let before_id = panes[0].pane_id;
		let after_id = panes[1].pane_id;

		let clamped = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePaneBoundary {
				before: before_id,
				after: after_id,
				axis: crate::engine::SplitDirection::Vertical,
				target: u32::MAX,
			},
		);
		let clamped_target = clamped
			.pane(before_id)
			.expect("pane before clamped boundary")
			.rect
			.right()
			.get();
		assert_eq!(
			clamped
				.pane(after_id)
				.expect("pane after clamped boundary")
				.rect
				.width
				.get(),
			2
		);

		tx_cmd
			.send(EditorCommand::ResizePaneBoundary {
				before: before_id,
				after: after_id,
				axis: crate::engine::SplitDirection::Vertical,
				target: u32::MAX,
			})
			.expect("repeat clamped absolute target");
		let still_clamped = send_refresh_and_recv(&tx_cmd, &rx_view);
		assert_eq!(
			still_clamped
				.pane(before_id)
				.expect("pane before repeated clamped boundary")
				.rect
				.right()
				.get(),
			clamped_target
		);

		let recovered = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ResizePaneBoundary {
				before: before_id,
				after: after_id,
				axis: crate::engine::SplitDirection::Vertical,
				target: clamped_target - 3,
			},
		);
		assert_eq!(
			recovered
				.pane(before_id)
				.expect("pane before recovered boundary")
				.rect
				.right()
				.get(),
			clamped_target - 3
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn new_doctab_inherits_active_viewport_dimensions() {
		use crate::core::EDITOR_CHROME_ROWS;

		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		tx_cmd
			.send(EditorCommand::Resize(120, 33))
			.expect("resize editor");
		let resized_view = recv_view(&rx_view);
		assert_eq!(
			resized_view.viewport_line_count,
			33 - EDITOR_CHROME_ROWS as u32
		);

		tx_cmd
			.send(EditorCommand::DocTabNew)
			.expect("create new doctab");
		let new_tab_view = recv_view(&rx_view);
		assert_eq!(
			new_tab_view.viewport_line_count,
			resized_view.viewport_line_count
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn doctab_view_ids_survive_switching_and_are_not_reused_after_close() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let second = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabNew);
		let ids: Vec<_> = second.tabs.iter().map(|tab| tab.view_id).collect();
		assert_eq!(
			ids,
			vec![crate::core::ViewId::new(1), crate::core::ViewId::new(2)]
		);
		assert_eq!(second.view_id, crate::core::ViewId::new(2));

		let first = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabPrev);
		assert_eq!(first.view_id, crate::core::ViewId::new(1));
		assert_eq!(first.tabs, second.tabs);

		let remaining = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabClose);
		assert_eq!(remaining.view_id, crate::core::ViewId::new(2));
		assert_eq!(remaining.tabs.len(), 1);

		let third = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabNew);
		assert_eq!(third.view_id, crate::core::ViewId::new(3));
		assert_eq!(third.tabs[0].view_id, crate::core::ViewId::new(2));
		assert_eq!(third.tabs[1].view_id, crate::core::ViewId::new(3));

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("join engine");
	}

	#[test]
	fn append_at_line_end_moves_cursor_and_sets_insert_mode_integration() {
		use crate::core::DocDisplayCol;
		use crate::engine::protocol::EditorMode;
		use std::time::Duration;

		let scratch_path = temp_test_path("append-at-line-end");
		std::fs::write(&scratch_path, "hello world\n").expect("write scratch file");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load file");

		// Wait for DMA to complete — source-backed nodes need their data
		// inflated from disk before navigation commands can read line bytes.
		let mut view = recv_view(&rx_view);
		for _ in 0..50 {
			if !view.dma_pending {
				break;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after refresh");
		}
		assert!(!view.dma_pending, "DMA must complete before navigation");
		assert_eq!(
			view.cursor_abs_pos.col,
			DocDisplayCol::ZERO,
			"cursor should start at col 0"
		);

		// Baseline: verify LineEnd works.
		tx_cmd.send(EditorCommand::LineEnd).expect("line end");
		let line_end_view = recv_view(&rx_view);
		assert_eq!(
			line_end_view.cursor_abs_pos.col,
			DocDisplayCol::new(11),
			"LineEnd ($) baseline must reach end of 'hello world'; \
			 status={:?}",
			line_end_view.status_message,
		);

		// Move back to col 0.
		tx_cmd.send(EditorCommand::LineStart).expect("line start");
		let _ = recv_view(&rx_view);

		// Now test AppendAtLineEnd.
		tx_cmd
			.send(EditorCommand::AppendAtLineEnd)
			.expect("append at line end");
		let after_view = recv_view(&rx_view);
		assert_eq!(
			after_view.cursor_abs_pos.col,
			DocDisplayCol::new(11),
			"AppendAtLineEnd must move cursor to end of 'hello world'; \
			 status={:?}",
			after_view.status_message,
		);
		assert_eq!(
			after_view.input.mode,
			EditorMode::Insert,
			"must request Insert mode"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	/// Helper: load a .rs file through the full engine, wait for DMA
	/// inflation and semantic grafting to complete, then return the
	/// latest viewport.
	fn load_rs_and_wait_for_graft(
		tx_cmd: &crate::engine::CommandSender,
		rx_view: &std::sync::mpsc::Receiver<crate::uast::Viewport>,
		path: &str,
	) -> crate::uast::Viewport {
		use std::time::Duration;

		tx_cmd
			.send(EditorCommand::LoadFile(path.to_string()))
			.expect("load file");

		// Phase 1: wait for DMA completion.
		let mut view = recv_view(rx_view);
		for _ in 0..100 {
			if !view.dma_pending {
				break;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after refresh");
		}
		assert!(!view.dma_pending, "DMA must complete before testing");

		// Phase 2: give the semantic reactor time to graft.
		// The first render after DMA kicks off the semantic pipeline;
		// subsequent renders pick up the result. Poll until the viewport
		// shows semantic highlights (evidence of grafting) or timeout.
		for _ in 0..100 {
			if !view.highlights.is_empty() {
				break;
			}
			std::thread::sleep(Duration::from_millis(30));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after semantic");
		}
		assert!(
			!view.highlights.is_empty(),
			"semantic highlights must appear (grafting done)"
		);

		view
	}

	/// Send a command and return the resulting viewport, draining any
	/// stale viewports from prior renders.
	fn send_refresh_and_recv(
		tx_cmd: &crate::engine::CommandSender,
		rx_view: &std::sync::mpsc::Receiver<crate::uast::Viewport>,
	) -> crate::uast::Viewport {
		use std::time::Duration;
		while rx_view.try_recv().is_ok() {}
		std::thread::sleep(Duration::from_millis(50));
		while rx_view.try_recv().is_ok() {}
		tx_cmd.refresh_for_test().expect("send refresh");
		loop {
			let view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("expected viewport after refresh");
			if !view.busy {
				return view;
			}
		}
	}

	fn send_cmd_and_recv(
		tx_cmd: &crate::engine::CommandSender,
		rx_view: &std::sync::mpsc::Receiver<crate::uast::Viewport>,
		cmd: EditorCommand,
	) -> crate::uast::Viewport {
		use std::time::Duration;
		// Drain stale viewports.
		while rx_view.try_recv().is_ok() {}
		// Queue the command, then use the refresh as an ordering fence so the
		// returned viewport is produced after the command has been handled.
		tx_cmd.send(cmd).expect("send cmd");
		tx_cmd.refresh_for_test().expect("send refresh");
		let mut last = None;
		let mut status_message = None;
		loop {
			let timeout = if last.is_some() {
				Duration::from_millis(150)
			} else {
				Duration::from_secs(2)
			};
			match rx_view.recv_timeout(timeout) {
				Ok(mut view) => {
					if !view.busy {
						if view.status_message.is_some() {
							status_message = view.status_message.clone();
						} else if status_message.is_some() {
							view.status_message = status_message.clone();
						}
						last = Some(view);
					}
				}
				Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
					return last.expect("expected viewport after command");
				}
				Err(error) => panic!("expected viewport after command: {error}"),
			}
		}
	}

	fn pane_text(view: &crate::uast::Viewport, pane_id: crate::core::PaneId) -> String {
		let pane = view
			.panes
			.iter()
			.find(|pane| pane.pane_id == pane_id)
			.expect("pane must exist");
		String::from_utf8_lossy(
			&pane
				.tokens
				.iter()
				.flat_map(|token| token.text.iter().copied())
				.collect::<Vec<_>>(),
		)
		.into_owned()
	}

	#[test]
	fn git_status_opens_a_reusable_styled_output_panel_and_shows_porcelain_entries() {
		let repo = temp_test_path("git-status-panel");
		fs::create_dir_all(&repo).expect("create git fixture");
		let initialized = Command::new("git")
			.args(["init", "--quiet"])
			.current_dir(&repo)
			.status()
			.expect("run git init");
		assert!(initialized.success(), "git init failed");
		fs::write(repo.join("untracked.txt"), "untracked\n").expect("write fixture");
		let file = repo.join("main.rs");
		fs::write(&file, "fn main() {}\n").expect("write source fixture");

		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);
		let _loaded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::LoadFile(file.to_string_lossy().into_owned()),
		);
		let mut panel = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GitStatus);
		for _ in 0..4 {
			let Some(panel_pane) = panel.panes.iter().find(|pane| pane.is_output_panel) else {
				break;
			};
			if pane_text(&panel, panel_pane.pane_id).contains("?? untracked.txt") {
				break;
			}
			panel = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GitStatus);
		}
		let panel_pane = panel
			.panes
			.iter()
			.find(|pane| pane.is_output_panel)
			.expect("GitStatus must create an output panel");
		assert_eq!(panel_pane.gutter_numbering, None);
		let panel_text = pane_text(&panel, panel_pane.pane_id);
		assert!(
			panel_text.contains("?? untracked.txt"),
			"unexpected Git panel text: {panel_text:?}, status: {:?}",
			panel.status_message
		);
		assert!(
			panel_text
				.lines()
				.next()
				.is_some_and(|line| line.starts_with("On branch ")),
			"Git panel should report its branch: {panel_text:?}"
		);
		let active_tab = &panel_pane.tabs[panel_pane.active_doctab_index];
		assert_eq!(
			active_tab.title, "git",
			"Git panel tab should have a stable title"
		);
		let panel_view_id = active_tab.view_id;
		assert_eq!(
			panel
				.panes
				.iter()
				.filter(|pane| pane.is_output_panel)
				.count(),
			1
		);
		let panel_pane_id = panel_pane.pane_id;
		send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(panel_pane_id));
		let untracked_line = panel_text
			.lines()
			.position(|line| line == "?? untracked.txt")
			.expect("untracked row") as u64;
		let opened_entry = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenGitStatusEntry(DocLine::new(untracked_line)),
		);
		let opened_pane = opened_entry
			.focused_pane()
			.expect("Git status entry should focus an editor pane");
		assert!(!opened_pane.is_output_panel);
		assert_eq!(
			opened_pane.tabs[opened_pane.active_doctab_index].title,
			"untracked.txt"
		);
		send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(panel_pane_id));
		let closed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::CloseDocTab {
				pane_id: panel_pane_id,
				view_id: panel_view_id,
			},
		);
		assert!(
			closed
				.panes
				.iter()
				.all(|pane| pane.pane_id != panel_pane_id)
		);
		let reopened = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GitStatus);
		let reopened_pane = reopened
			.panes
			.iter()
			.find(|pane| pane.is_output_panel)
			.expect("GitStatus must reopen the output panel");
		assert!(pane_text(&reopened, reopened_pane.pane_id).contains("?? untracked.txt"));

		tx_cmd.send(EditorCommand::Quit).expect("quit engine");
		handle.join().expect("engine thread exits");
		let _ = fs::remove_dir_all(repo);
	}

	/// Waits until the background metric scan has published an exact line count.
	///
	/// `dma_pending` only covers viewport reads, so it can be clear while the
	/// low-priority newline scan is still running. Anything that depends on
	/// document-wide line metrics — the wrap-aware scroll clamp above all — must
	/// wait for `line_count` to become `Some`, or it races the scan.
	fn wait_for_exact_line_count(
		tx_cmd: &crate::engine::CommandSender,
		rx_view: &std::sync::mpsc::Receiver<crate::uast::Viewport>,
		mut view: crate::uast::Viewport,
	) -> crate::uast::Viewport {
		use std::time::Duration;

		for _ in 0..100 {
			if !view.dma_pending && view.line_count.is_some() {
				return view;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after refresh");
		}
		panic!("background metric scan never published an exact line count");
	}

	fn wait_for_dma(
		tx_cmd: &crate::engine::CommandSender,
		rx_view: &std::sync::mpsc::Receiver<crate::uast::Viewport>,
		mut view: crate::uast::Viewport,
	) -> crate::uast::Viewport {
		use std::time::Duration;

		for _ in 0..100 {
			if !view.dma_pending {
				return view;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after refresh");
		}
		assert!(!view.dma_pending, "DMA must complete before reading text");
		view
	}

	#[test]
	fn write_publishes_busy_view_before_completion() {
		use std::time::Duration;

		let path = temp_test_path("write-busy-state");
		std::fs::write(&path, "before\n").expect("write save fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let dirty = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("changed ".to_string()),
		);
		assert!(dirty.is_dirty);

		while rx_view.try_recv().is_ok() {}
		tx_cmd.send(EditorCommand::WriteFile).expect("send write");
		let saving = rx_view
			.recv_timeout(Duration::from_secs(2))
			.expect("busy viewport before write");
		assert!(saving.busy);
		assert!(saving.is_dirty);

		let written = rx_view
			.recv_timeout(Duration::from_secs(2))
			.expect("completed write viewport");
		assert!(!written.busy);
		assert!(!written.is_dirty);
		assert!(
			written
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("written"))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn reload_file_refreshes_clean_buffer_from_disk() {
		let path = temp_test_path("reload-clean");
		std::fs::write(&path, "old\n").expect("write initial file");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name.clone()));
		let pane_id = loaded.focused_pane_id;
		std::fs::write(&path, "new contents\n").expect("write updated file");

		let reloaded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReloadFile { force: false },
		);
		let reloaded = wait_for_dma(&tx_cmd, &rx_view, reloaded);

		assert_eq!(pane_text(&reloaded, pane_id), "new contents\n");
		assert!(!reloaded.focused_pane().expect("focused pane").is_dirty);

		let _ = tx_cmd.send(EditorCommand::Quit);
		let _ = handle.join();
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn reload_file_prompts_before_discarding_dirty_buffer() {
		let path = temp_test_path("reload-dirty");
		std::fs::write(&path, "disk old\n").expect("write initial file");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name.clone()));
		let pane_id = loaded.focused_pane_id;
		let dirty = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("local ".to_string()),
		);
		assert!(dirty.focused_pane().expect("focused pane").is_dirty);
		std::fs::write(&path, "disk replacement\n").expect("write replacement file");

		let prompted = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReloadFile { force: false },
		);
		assert_eq!(
			prompted.confirm_prompt.as_deref(),
			Some("Reload and discard changes? [y/n]")
		);
		assert!(pane_text(&prompted, pane_id).starts_with("local "));

		let confirmed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ConfirmResponse(ConfirmAction::Yes),
		);
		let confirmed = wait_for_dma(&tx_cmd, &rx_view, confirmed);
		assert_eq!(pane_text(&confirmed, pane_id), "disk replacement\n");
		assert!(!confirmed.focused_pane().expect("focused pane").is_dirty);

		let _ = tx_cmd.send(EditorCommand::Quit);
		let _ = handle.join();
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn check_file_reload_refreshes_clean_changed_buffer() {
		let path = temp_test_path("autoreload-clean");
		std::fs::write(&path, "alpha\n").expect("write initial file");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name.clone()));
		let pane_id = loaded.focused_pane_id;
		std::fs::write(&path, "alpha beta gamma\n").expect("write updated file");

		let reloaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::CheckFileReload);
		let reloaded = wait_for_dma(&tx_cmd, &rx_view, reloaded);

		assert_eq!(pane_text(&reloaded, pane_id), "alpha beta gamma\n");
		assert!(!reloaded.focused_pane().expect("focused pane").is_dirty);

		let _ = tx_cmd.send(EditorCommand::Quit);
		let _ = handle.join();
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn check_file_reload_does_not_discard_dirty_buffer() {
		let path = temp_test_path("autoreload-dirty");
		std::fs::write(&path, "base\n").expect("write initial file");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name.clone()));
		let pane_id = loaded.focused_pane_id;
		let _dirty = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("local ".to_string()),
		);
		std::fs::write(&path, "external replacement\n").expect("write updated file");

		let checked = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::CheckFileReload);

		assert!(pane_text(&checked, pane_id).starts_with("local "));
		assert!(checked.focused_pane().expect("focused pane").is_dirty);
		assert!(checked.status_message.as_deref().is_some_and(|message| {
			message.contains("changed on disk") && message.contains(":e!")
		}));

		let _ = tx_cmd.send(EditorCommand::Quit);
		let _ = handle.join();
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn markdown_preview_command_opens_rendered_side_pane() {
		let scratch_path = temp_test_path("markdown-preview-open").with_extension("md");
		std::fs::write(
			&scratch_path,
			"# Title\n\n- [x] done\n\n```rust\nfn main() {}\n```\n",
		)
		.expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let source_pane_id = loaded.focused_pane_id;
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);

		assert_eq!(opened.panes.len(), 2);
		let preview = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("markdown preview pane");
		assert_eq!(opened.focused_pane_id, source_pane_id);
		assert_eq!(preview.gutter_numbering, None);
		assert!(!preview.visible_whitespace);
		assert!(preview.file_name.as_deref().is_some_and(|name| {
			name.contains("markdown-preview-open") && name.ends_with(".md Preview")
		}));
		assert!(preview.tokens.is_empty());
		assert!(preview.highlights.is_empty());
		let snapshot = preview
			.markdown_preview
			.as_ref()
			.expect("markdown image preview snapshot");
		assert!(!snapshot.serif);
		assert!(snapshot.source_text.contains("# Title"));
		assert!(snapshot.source_text.contains("- [x] done"));
		assert!(snapshot.source_text.contains("fn main() {}"));
		assert!(opened.view(snapshot.source_view_id).is_some());

		let serif = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: true },
		);
		assert_eq!(serif.panes.len(), 2);
		assert!(
			serif
				.panes
				.iter()
				.find(|pane| pane.language_label == "Markdown Preview")
				.and_then(|pane| pane.markdown_preview.as_ref())
				.is_some_and(|snapshot| snapshot.serif)
		);
		assert_eq!(serif.focused_pane_id, source_pane_id);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn markdown_preview_remains_bound_to_its_source_view_after_tab_switch() {
		let scratch_path = temp_test_path("markdown-preview-view-identity").with_extension("md");
		std::fs::write(
			&scratch_path,
			"# Original source\n\nkept across tab switches\n",
		)
		.expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let source_view_id = loaded.view_id;
		let _opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let switched = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabNew);
		assert_ne!(switched.view_id, source_view_id);

		let snapshot = switched
			.panes
			.iter()
			.find_map(|pane| pane.markdown_preview.as_ref())
			.expect("preview remains open");
		assert_eq!(snapshot.source_view_id, source_view_id);
		assert!(snapshot.source_text.contains("# Original source"));
		assert!(snapshot.source_text.contains("kept across tab switches"));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn show_markdown_document_opens_scrollable_pinned_help_pane() {
		use crate::core::DocLine;
		use crate::engine::protocol::MoveDirection;

		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ShowMarkdownDocument {
				title: "Baryon Help".to_string(),
				markdown: "# Baryon Help\n\nl1\nl2\nl3\nl4\nl5\n".to_string(),
			},
		);
		assert_eq!(opened.panes.len(), 2);
		let preview = opened.focused_pane().expect("help pane focused");
		assert_eq!(preview.file_name.as_deref(), Some("Baryon Help"));
		let snapshot = preview
			.markdown_preview
			.as_ref()
			.expect("help document snapshot");
		assert!(snapshot.source_text.contains("# Baryon Help"));
		assert_eq!(snapshot.source_scroll_line, DocLine::ZERO);

		let scrolled = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(MoveDirection::Down),
		);
		let snapshot = scrolled
			.focused_pane()
			.and_then(|pane| pane.markdown_preview.as_ref())
			.expect("scrolled help snapshot");
		assert_eq!(
			snapshot.source_scroll_line,
			DocLine::new(1),
			"j must scroll the pinned help document"
		);

		let reopened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ShowMarkdownDocument {
				title: "Baryon Help".to_string(),
				markdown: "# Replaced\n".to_string(),
			},
		);
		assert_eq!(reopened.panes.len(), 2, "help pane must be reused");
		let snapshot = reopened
			.focused_pane()
			.and_then(|pane| pane.markdown_preview.as_ref())
			.expect("replaced help snapshot");
		assert!(snapshot.source_text.contains("# Replaced"));
		assert_eq!(snapshot.source_scroll_line, DocLine::ZERO);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn closing_a_help_tab_closes_its_pane_not_the_session() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ShowMarkdownDocument {
				title: "Baryon Help".to_string(),
				markdown: "# Baryon Help\n".to_string(),
			},
		);
		let help_pane = opened.focused_pane().expect("help pane focused");
		let help_pane_id = help_pane.pane_id;
		let help_view_id = help_pane.tabs[help_pane.active_doctab_index].view_id;

		let closed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::CloseDocTab {
				pane_id: help_pane_id,
				view_id: help_view_id,
			},
		);
		assert!(!closed.should_quit);
		assert_eq!(closed.panes.len(), 1);
		assert!(closed.pane(help_pane_id).is_none());

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn theme_command_from_markdown_preview_focus_updates_source_pane() {
		let scratch_path = temp_test_path("markdown-preview-theme").with_extension("md");
		std::fs::write(&scratch_path, "# Title\n\nPlain text\n").expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let _loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let preview_pane = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("preview pane");
		let snapshot = preview_pane
			.markdown_preview
			.as_ref()
			.expect("focused pane should be markdown preview");
		let source_view_id = snapshot.source_view_id;
		let before = opened
			.view(source_view_id)
			.expect("source view")
			.theme_colors[TokenCategory::String as usize];
		let _focused_preview = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::FocusPane(preview_pane.pane_id),
		);

		let themed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetTheme("turbo".to_string()),
		);
		let after = themed
			.view(source_view_id)
			.expect("source view after theme")
			.theme_colors[TokenCategory::String as usize];

		assert_ne!(before, after);
		assert_eq!(themed.status_message.as_deref(), Some("Theme set to turbo"));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn load_file_applies_detected_language_theme() {
		let scratch_path = temp_test_path("language-theme-load").with_extension("rs");
		std::fs::write(&scratch_path, "fn main() {}\n").expect("write rust");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			None,
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		assert_focused_theme(&loaded, "glowmax");

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn split_view_resolves_the_shared_buffers_language_theme() {
		let scratch_path = temp_test_path("language-theme-split").with_extension("rs");
		std::fs::write(&scratch_path, "fn main() {}\n").expect("write rust");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			None,
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		assert_focused_theme(&loaded, "glowmax");
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(SplitDirection::Vertical),
		);
		let expected = Theme::try_new("glowmax").expect("theme").syntax_colors;
		assert_eq!(split.panes.len(), 2);
		assert!(split.panes.iter().all(|pane| pane.theme_colors == expected));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn write_file_as_redetects_language_and_re_resolves_theme() {
		let source_path = temp_test_path("language-theme-write-as-source").with_extension("txt");
		let rust_path = temp_test_path("language-theme-write-as-target").with_extension("rs");
		std::fs::write(&source_path, "fn main() {}\n").expect("write source");
		let source_name = source_path.to_string_lossy().into_owned();
		let rust_name = rust_path.to_string_lossy().into_owned();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			None,
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(source_name));
		assert_focused_theme(&loaded, "onedark");

		let saved = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFileAs(rust_name));
		assert_focused_theme(&saved, "glowmax");
		assert!(
			saved
				.status_message
				.is_some_and(|message| message.contains("written"))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(source_path);
		let _ = std::fs::remove_file(rust_path);
	}

	#[test]
	fn failed_write_file_as_does_not_change_language_theme() {
		let source_path = temp_test_path("language-theme-failed-write-as").with_extension("txt");
		let missing_parent = temp_test_path("language-theme-missing-parent");
		let rust_path = missing_parent.join("target.rs");
		std::fs::write(&source_path, "fn main() {}\n").expect("write source");
		let source_name = source_path.to_string_lossy().into_owned();
		let rust_name = rust_path.to_string_lossy().into_owned();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			None,
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(source_name));
		assert_focused_theme(&loaded, "onedark");

		let failed = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFileAs(rust_name));
		assert_focused_theme(&failed, "onedark");
		assert!(
			failed
				.status_message
				.is_some_and(|message| message.contains("error"))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(source_path);
	}

	/// Send `QueryDocumentText` and return the one-shot reply from whichever
	/// viewport carries it. `send_cmd_and_recv` coalesces to the last viewport,
	/// which can drop the one-shot field, so this scans every projection.
	fn query_document_text(
		tx_cmd: &crate::engine::CommandSender,
		rx_view: &std::sync::mpsc::Receiver<crate::uast::Viewport>,
	) -> String {
		use std::time::Duration;
		while rx_view.try_recv().is_ok() {}
		tx_cmd
			.send(EditorCommand::QueryDocumentText)
			.expect("send query");
		tx_cmd.refresh_for_test().expect("send refresh");
		for _ in 0..100 {
			let view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after query");
			if let Some(text) = view.document_text {
				return text;
			}
		}
		panic!("no viewport carried the document text reply");
	}

	#[test]
	fn replace_document_sets_in_memory_text_language_and_round_trips() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_sql();

		let sql = "select 1;\nselect 2;\n";
		let replaced = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReplaceDocument {
				text: sql.to_string(),
				language: Some("sql".to_string()),
				virtual_path: None,
			},
		);
		let visible = pane_text(&replaced, replaced.focused_pane_id);
		assert!(visible.contains("select 1;"), "visible text: {visible:?}");
		assert!(visible.contains("select 2;"), "visible text: {visible:?}");
		assert_eq!(query_document_text(&tx_cmd, &rx_view), sql);

		// The caret sits at byte zero after a replace, so an insert lands at
		// the document head; read the edited text back through the query.
		send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("-- edited\n".to_string()),
		);
		assert_eq!(
			query_document_text(&tx_cmd, &rx_view),
			format!("-- edited\n{sql}")
		);

		// An unknown language rejects the command and keeps the document.
		let unknown = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReplaceDocument {
				text: "clobbered".to_string(),
				language: Some("doesnotexist".to_string()),
				virtual_path: None,
			},
		);
		assert_eq!(
			unknown.status_message.as_deref(),
			Some("Unknown language: doesnotexist")
		);
		assert_eq!(
			query_document_text(&tx_cmd, &rx_view),
			format!("-- edited\n{sql}")
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	/// Load a file and wait until its source-backed bytes are DMA-inflated, so
	/// `pane_text` can see them.
	fn load_and_wait_for_dma(
		tx_cmd: &crate::engine::CommandSender,
		rx_view: &std::sync::mpsc::Receiver<crate::uast::Viewport>,
		path: &str,
	) -> crate::uast::Viewport {
		use std::time::Duration;
		let mut view = send_cmd_and_recv(tx_cmd, rx_view, EditorCommand::LoadFile(path.to_string()));
		for _ in 0..50 {
			if !view.dma_pending {
				break;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after refresh");
		}
		assert!(!view.dma_pending, "DMA must complete");
		view
	}

	#[test]
	fn replace_document_gives_in_memory_documents_semantic_highlighting() {
		use std::time::Duration;

		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_sql();

		// No explicit identity: one is derived from the language provider, so
		// the semantic pipeline (which requires a file-path-shaped context)
		// still runs for a purely in-memory document.
		let mut view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReplaceDocument {
				text: "select id, name from users where id = 7;\n".to_string(),
				language: Some("sql".to_string()),
				virtual_path: None,
			},
		);
		for _ in 0..100 {
			if !view.highlights.is_empty() {
				break;
			}
			std::thread::sleep(Duration::from_millis(30));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after semantic");
		}
		assert!(
			!view.highlights.is_empty(),
			"in-memory sql document must receive semantic highlights"
		);

		// The virtual identity must not make the document writable.
		let write = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFile);
		assert_eq!(write.status_message.as_deref(), Some("No file name"));

		// Failed file operations must not strip the surviving in-memory
		// document of its identity: highlighting persists after a refused
		// reload (no file name) and a failed write-as (unwritable path).
		let reload = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReloadFile { force: true },
		);
		assert_eq!(reload.status_message.as_deref(), Some("No file name"));
		let after_reload = send_refresh_and_recv(&tx_cmd, &rx_view);
		assert!(
			!after_reload.highlights.is_empty(),
			"refused reload must preserve virtual-identity highlighting"
		);
		let failed_write = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::WriteFileAs("/nonexistent-dir-for-test/out.sql".to_string()),
		);
		assert!(
			failed_write.status_message.is_some(),
			"write into a missing directory must fail"
		);
		let after_write = send_refresh_and_recv(&tx_cmd, &rx_view);
		assert!(
			!after_write.highlights.is_empty(),
			"failed write-as must preserve virtual-identity highlighting"
		);

		// An explicit identity is honored the same way.
		let mut view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReplaceDocument {
				text: "select count(*) as total from orders where total > 10;\n".to_string(),
				language: Some("sql".to_string()),
				virtual_path: Some("statement.sql".to_string()),
			},
		);
		for _ in 0..100 {
			if !view.highlights.is_empty() {
				break;
			}
			std::thread::sleep(Duration::from_millis(30));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after semantic");
		}
		assert!(
			!view.highlights.is_empty(),
			"explicit virtual identity must also produce highlights"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn insert_typing_on_the_last_line_keeps_pane_cursor_at_the_caret() {
		use crate::engine::{InputEvent, KeyCode, Modifiers};
		use std::time::Duration;

		let (tx_cmd, input, rx_view, handle) = spawn_test_engine_with_input(false, true, None);
		let _ = send_refresh_and_recv(&tx_cmd, &rx_view);
		// Type on the document's last line (no trailing newline): the wrap
		// model reads the line's bytes, and dropping the final character made
		// the projected pane cursor clamp one cell short of the caret.
		for ch in ['i', 'a', 'b', 'c'] {
			input
				.send(InputEvent::Key {
					code: KeyCode::Char(ch),
					modifiers: Modifiers::NONE,
				})
				.expect("send key");
		}
		let deadline = std::time::Instant::now() + Duration::from_secs(10);
		let mut view = send_refresh_and_recv(&tx_cmd, &rx_view);
		while (view.cursor_abs_pos.col.get() != 3
			|| view.cursor_pane_col.get() != view.cursor_abs_pos.col.get())
			&& std::time::Instant::now() < deadline
		{
			view = send_refresh_and_recv(&tx_cmd, &rx_view);
		}
		assert_eq!(view.cursor_abs_pos.col.get(), 3, "caret sits after 'abc'");
		assert_eq!(
			view.cursor_pane_col.get(),
			view.cursor_abs_pos.col.get(),
			"pane-projected cursor must sit at the caret, not on the last glyph"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn host_panels_project_identity_dividers_and_refcounted_disposal() {
		use crate::engine::{HostEvent, HostPanelId, PanelRevision};

		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_sql();
		let panel_id = HostPanelId::new(42);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenHostPanel {
				panel_id,
				kind: "result-table".to_string(),
				title: "Trip distribution".to_string(),
			},
		);
		let panel = opened
			.panes
			.iter()
			.find_map(|pane| pane.host_panel.as_ref())
			.expect("host panel is projected");
		assert_eq!(panel.panel_id, panel_id);
		assert_eq!(panel.kind, "result-table");
		assert_eq!(panel.title, "Trip distribution");
		assert_eq!(panel.revision, PanelRevision::new(0));

		// The panel is a pane like any other, so the engine publishes the
		// boundary between it and the editor — the host never infers geometry.
		assert!(
			!opened.dividers.is_empty(),
			"a split must project a draggable divider"
		);
		let divider = opened.dividers.first().expect("divider");
		assert!(
			divider.hit_rect.width.get() >= divider.visual_rect.width.get()
				&& divider.hit_rect.height.get() >= divider.visual_rect.height.get(),
			"the grab area is never smaller than what is painted"
		);

		// Rebinding to a new revision is how stale output is signalled.
		let refreshed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::RefreshHostPanel {
				panel_id,
				revision: PanelRevision::new(3),
			},
		);
		assert_eq!(
			refreshed
				.panes
				.iter()
				.find_map(|pane| pane.host_panel.as_ref())
				.map(|panel| panel.revision),
			Some(PanelRevision::new(3))
		);

		// A host panel is not a document. Text commands are inert on it rather
		// than reaching a document path: no write is attempted and no text
		// status is produced, and typing cannot damage anything because the
		// pane projects no text at all.
		let write = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFile);
		assert_eq!(write.status_message, None, "write is inert on a host panel");
		let typed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("clobber".to_string()),
		);
		let panel_pane = typed
			.panes
			.iter()
			.find(|pane| pane.host_panel.is_some())
			.expect("panel survives document commands");
		assert!(
			panel_pane.tokens.is_empty(),
			"a host panel projects no text"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn closing_a_host_panel_emits_exactly_one_disposal() {
		use crate::engine::{EngineConfig, EngineHandle, HostEvent, HostPanelId};
		use std::time::Duration;

		let mut handle = EngineHandle::spawn(EngineConfig::default());
		let commands = handle.command_sender();
		let events = handle.take_host_events().expect("host event stream");
		let panel_id = HostPanelId::new(7);

		commands
			.send(EditorCommand::OpenHostPanel {
				panel_id,
				kind: "dashboard".to_string(),
				title: "Dashboard".to_string(),
			})
			.expect("open panel");
		commands
			.send(EditorCommand::ClosePane)
			.expect("close the panel pane");
		std::thread::sleep(Duration::from_millis(300));

		let disposals = events
			.try_iter()
			.filter(|event| matches!(event, HostEvent::PanelDisposed { panel_id: id } if *id == panel_id))
			.count();
		assert_eq!(
			disposals, 1,
			"the last workspace reference disappearing disposes the panel exactly once"
		);

		handle.shutdown().expect("engine thread");
	}

	#[test]
	fn set_language_gives_a_pathless_scratch_buffer_semantic_highlighting() {
		use std::time::Duration;

		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_sql();

		// Plain text in a buffer with no file and no virtual identity — the
		// scratch-buffer scenario a terminal user reaches with `[No Name]`.
		let plain = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReplaceDocument {
				text: "select id, name from users where id = 7;\n".to_string(),
				language: None,
				virtual_path: None,
			},
		);
		assert!(plain.highlights.is_empty());

		// Setting the language must derive a file-path-shaped identity so the
		// semantic pipeline runs.
		let mut view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetLanguage(Some("sql".to_string())),
		);
		for _ in 0..100 {
			if !view.highlights.is_empty() {
				break;
			}
			std::thread::sleep(Duration::from_millis(30));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after semantic");
		}
		assert!(
			!view.highlights.is_empty(),
			"set-language on a pathless buffer must produce semantic highlights"
		);

		// The derived identity still does not make the buffer writable.
		let write = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFile);
		assert_eq!(write.status_message.as_deref(), Some("No file name"));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn replace_document_clears_the_file_association() {
		let scratch_path = temp_test_path("replace-document-clears-file").with_extension("txt");
		std::fs::write(&scratch_path, "on disk\n").expect("write fixture");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_sql();

		let loaded = load_and_wait_for_dma(&tx_cmd, &rx_view, &scratch_name);
		let visible = pane_text(&loaded, loaded.focused_pane_id);
		assert!(visible.contains("on disk"), "visible text: {visible:?}");

		let replaced = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ReplaceDocument {
				text: "in memory\n".to_string(),
				language: None,
				virtual_path: None,
			},
		);
		let visible = pane_text(&replaced, replaced.focused_pane_id);
		assert!(visible.contains("in memory"), "visible text: {visible:?}");

		// The replaced document no longer names a file, so a plain write must
		// refuse rather than clobber the previously loaded path.
		let write = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFile);
		assert_eq!(write.status_message.as_deref(), Some("No file name"));
		assert_eq!(
			std::fs::read_to_string(&scratch_path).expect("fixture intact"),
			"on disk\n"
		);

		// The stale canonical-path claim is released: the same file can be
		// loaded again into this session.
		let reloaded = load_and_wait_for_dma(&tx_cmd, &rx_view, &scratch_name);
		let visible = pane_text(&reloaded, reloaded.focused_pane_id);
		assert!(visible.contains("on disk"), "visible text: {visible:?}");

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn set_language_re_resolves_auto_theme() {
		let scratch_path = temp_test_path("language-theme-set-language").with_extension("txt");
		std::fs::write(&scratch_path, "plain text\n").expect("write text");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			None,
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		assert_focused_theme(&loaded, "onedark");

		let rust = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetLanguage(Some("rust".to_string())),
		);
		assert_focused_theme(&rust, "glowmax");
		assert_eq!(rust.status_message.as_deref(), Some("Language set to Rust"));

		let plaintext = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::SetLanguage(None));
		assert_focused_theme(&plaintext, "onedark");
		assert_eq!(
			plaintext.status_message.as_deref(),
			Some("Language set to plaintext")
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn set_language_resolves_registered_extensions_and_rejects_unknown_names() {
		let scratch_path =
			temp_test_path("language-theme-set-language-extension").with_extension("txt");
		std::fs::write(&scratch_path, "plain text\n").expect("write text");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			None,
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		assert_focused_theme(&loaded, "onedark");

		let rust = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetLanguage(Some("rs".to_string())),
		);
		assert_focused_theme(&rust, "glowmax");
		assert_eq!(rust.status_message.as_deref(), Some("Language set to Rust"));

		let unknown = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetLanguage(Some("doesnotexist".to_string())),
		);
		assert_focused_theme(&unknown, "glowmax");
		assert_eq!(
			unknown.status_message.as_deref(),
			Some("Unknown language: doesnotexist")
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn set_language_reports_python_and_rune_labels_for_extension_aliases() {
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_python_and_rune();

		let python = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetLanguage(Some("py".to_string())),
		);
		assert_eq!(
			python.status_message.as_deref(),
			Some("Language set to Python")
		);
		assert!(
			python
				.language_completion_candidates
				.iter()
				.any(|candidate| candidate == "py")
		);

		let rune = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetLanguage(Some("rn".to_string())),
		);
		assert_eq!(rune.status_message.as_deref(), Some("Language set to Rune"));
		assert!(
			rune.language_completion_candidates
				.iter()
				.any(|candidate| candidate == "rn")
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn local_theme_override_survives_global_theme_change_and_tab_switch() {
		let settings_path = temp_test_path("language-theme-global-settings");
		let first_path = temp_test_path("language-theme-first").with_extension("txt");
		let second_path = temp_test_path("language-theme-second").with_extension("txt");
		std::fs::write(&first_path, "first\n").expect("write first");
		std::fs::write(&second_path, "second\n").expect("write second");
		let first_name = first_path.to_string_lossy().to_string();
		let second_name = second_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(true, true, Some(settings_path.clone()));

		let first = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(first_name));
		assert_focused_theme(&first, "onedark");

		let second = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabEdit(second_name));
		assert_focused_theme(&second, "onedark");

		let overridden = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetTheme("turbo".to_string()),
		);
		assert_focused_theme(&overridden, "turbo");

		let persisted = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::PersistTheme("glowmax".to_string()),
		);
		assert_focused_theme(&persisted, "turbo");

		let previous = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabPrev);
		assert_focused_theme(&previous, "glowmax");

		let next = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabNext);
		assert_focused_theme(&next, "turbo");

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(settings_path);
		let _ = std::fs::remove_file(first_path);
		let _ = std::fs::remove_file(second_path);
	}

	#[test]
	fn local_theme_override_survives_language_and_file_changes() {
		let first_path = temp_test_path("language-theme-override-first").with_extension("txt");
		let rust_path = temp_test_path("language-theme-override-rust").with_extension("rs");
		std::fs::write(&first_path, "plain\n").expect("write first");
		std::fs::write(&rust_path, "fn main() {}\n").expect("write rust");
		let first_name = first_path.to_string_lossy().to_string();
		let rust_name = rust_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			None,
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(first_name));
		assert_focused_theme(&loaded, "onedark");

		let overridden = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetTheme("turbo".to_string()),
		);
		assert_focused_theme(&overridden, "turbo");

		let rust_language = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetLanguage(Some("rust".to_string())),
		);
		assert_focused_theme(&rust_language, "turbo");

		let rust_file = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(rust_name));
		assert_focused_theme(&rust_file, "turbo");

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(first_path);
		let _ = std::fs::remove_file(rust_path);
	}

	#[test]
	fn persist_langtheme_re_resolves_matching_auto_tabs_without_file_write() {
		let settings_path = temp_test_path("persist-langtheme-settings");
		let rust_path = temp_test_path("persist-langtheme-rust").with_extension("rs");
		std::fs::write(&rust_path, "fn main() {}\n").expect("write rust");
		let rust_name = rust_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(Some(settings_path.clone()));

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(rust_name));
		assert_focused_theme(&loaded, "onedark");

		let themed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::PersistLangTheme {
				language: "rust".to_string(),
				theme: "glowmax".to_string(),
			},
		);
		assert_focused_theme(&themed, "glowmax");
		assert_eq!(
			themed.status_message.as_deref(),
			Some("Theme for rust set to glowmax")
		);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(settings_path);
		let _ = std::fs::remove_file(rust_path);
	}

	#[test]
	fn persist_langtheme_rejects_invalid_theme_without_file_write() {
		let settings_path = temp_test_path("persist-langtheme-invalid-settings");
		let rust_path = temp_test_path("persist-langtheme-invalid-rust").with_extension("rs");
		std::fs::write(&rust_path, "fn main() {}\n").expect("write rust");
		let rust_name = rust_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			Some(settings_path.clone()),
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(rust_name));
		assert_focused_theme(&loaded, "glowmax");

		let rejected = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::PersistLangTheme {
				language: "rust".to_string(),
				theme: "doesnotexist".to_string(),
			},
		);
		assert_focused_theme(&rejected, "glowmax");
		assert!(
			rejected
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("Unknown theme"))
		);

		assert!(
			!settings_path.exists(),
			"invalid language theme must not persist settings"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(settings_path);
		let _ = std::fs::remove_file(rust_path);
	}

	#[test]
	fn persist_language_theme_commands_reject_unknown_languages_without_file_write() {
		let settings_path = temp_test_path("persist-langtheme-unknown-language");
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(Some(settings_path.clone()));

		for command in [
			EditorCommand::PersistLangTheme {
				language: "doesnotexist".to_string(),
				theme: "glowmax".to_string(),
			},
			EditorCommand::PersistNoLangTheme("doesnotexist".to_string()),
		] {
			let rejected = send_cmd_and_recv(&tx_cmd, &rx_view, command);
			assert_eq!(
				rejected.status_message.as_deref(),
				Some("Unknown language: doesnotexist")
			);
			assert!(!settings_path.exists());
		}

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_langtheme_canonicalizes_extension_aliases_without_file_write() {
		let settings_path = temp_test_path("persist-langtheme-extension-alias");
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(Some(settings_path.clone()));

		let themed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::PersistLangTheme {
				language: "rs".to_string(),
				theme: "glowmax".to_string(),
			},
		);
		assert_eq!(
			themed.status_message.as_deref(),
			Some("Theme for rust set to glowmax")
		);
		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_nolangtheme_reverts_matching_auto_tabs_without_file_write() {
		let settings_path = temp_test_path("persist-nolangtheme-settings");
		let rust_path = temp_test_path("persist-nolangtheme-rust").with_extension("rs");
		std::fs::write(&rust_path, "fn main() {}\n").expect("write rust");
		let rust_name = rust_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust_and_language_themes(
			BTreeMap::from([("rust".to_string(), "glowmax".to_string())]),
			Some(settings_path.clone()),
		);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(rust_name));
		assert_focused_theme(&loaded, "glowmax");

		let removed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::PersistNoLangTheme("rust".to_string()),
		);
		assert_focused_theme(&removed, "onedark");
		assert_eq!(
			removed.status_message.as_deref(),
			Some("Theme for rust removed")
		);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(settings_path);
		let _ = std::fs::remove_file(rust_path);
	}

	#[test]
	fn persist_nolangtheme_reports_when_no_mapping_exists() {
		let settings_path = temp_test_path("persist-nolangtheme-missing-settings");
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(Some(settings_path.clone()));

		let removed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::PersistNoLangTheme("rust".to_string()),
		);
		assert_eq!(
			removed.status_message.as_deref(),
			Some("No theme was set for rust")
		);

		assert!(
			!settings_path.exists(),
			"session commands must not write TOML"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(settings_path);
	}

	#[test]
	fn persist_langtheme_does_not_override_local_theme() {
		let rust_path = temp_test_path("persist-langtheme-override-rust").with_extension("rs");
		std::fs::write(&rust_path, "fn main() {}\n").expect("write rust");
		let rust_name = rust_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(rust_name));
		assert_focused_theme(&loaded, "onedark");

		let overridden = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetTheme("turbo".to_string()),
		);
		assert_focused_theme(&overridden, "turbo");

		let lang_theme = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::PersistLangTheme {
				language: "rust".to_string(),
				theme: "glowmax".to_string(),
			},
		);
		assert_focused_theme(&lang_theme, "turbo");

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(rust_path);
	}

	#[test]
	fn markdown_preview_refreshes_after_source_edits() {
		let scratch_path = temp_test_path("markdown-preview-refresh").with_extension("md");
		std::fs::write(&scratch_path, "# Title\n").expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let _loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let preview_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("markdown preview pane")
			.pane_id;
		let source_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label != "Markdown Preview")
			.expect("source pane")
			.pane_id;

		let _focused_source =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(source_pane_id));
		let refreshed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("## Added\n\n".to_string()),
		);
		let preview = refreshed
			.panes
			.iter()
			.find(|pane| pane.pane_id == preview_pane_id)
			.expect("preview pane");
		let snapshot = preview
			.markdown_preview
			.as_ref()
			.expect("markdown image preview snapshot");
		assert!(snapshot.source_text.contains("## Added"));
		assert!(snapshot.source_text.contains("# Title"));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn scrolling_markdown_preview_pane_scrolls_source_pane() {
		let scratch_path = temp_test_path("markdown-preview-scroll-back").with_extension("md");
		let mut body = String::new();
		for i in 0..200 {
			body.push_str(&format!("Line {i}\n"));
		}
		std::fs::write(&scratch_path, &body).expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let _resized = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 40));
		let _loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let preview_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("markdown preview pane")
			.pane_id;
		let source_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label != "Markdown Preview")
			.expect("source pane")
			.pane_id;
		assert_eq!(opened.focused_pane_id, source_pane_id);
		let _focused_preview =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(preview_pane_id));

		let scrolled = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Scroll(50));
		let preview_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == preview_pane_id)
			.expect("preview pane")
			.markdown_preview
			.as_ref()
			.expect("markdown preview snapshot")
			.source_scroll_line
			.get();
		let source_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == source_pane_id)
			.expect("source pane")
			.scroll_y;
		assert!(preview_scroll > 0, "preview should have scrolled");
		assert_eq!(
			source_scroll, preview_scroll,
			"source should mirror preview scroll"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn scroll_viewport_to_line_positions_viewport_at_target() {
		use crate::core::DocLine;

		let mut content = String::new();
		for i in 0..300 {
			content.push_str(&format!("fn func_{i}() {{}}\n"));
		}
		let scratch_path = temp_test_path("scroll-to-line").with_extension("rs");
		std::fs::write(&scratch_path, &content).expect("write");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &scratch_name);
		let _resized = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(80, 40));

		let mut jumped = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ScrollViewportToLine(DocLine::new(150)),
		);
		if jumped
			.focused_pane()
			.is_some_and(|pane| pane.scroll_y != 150)
		{
			// A semantic refresh may already be queued ahead of the scroll
			// command under a heavily parallel test run. A refresh queued now is
			// ordered after the scroll and observes its durable viewport state.
			jumped = send_refresh_and_recv(&tx_cmd, &rx_view);
		}
		let focused = jumped.focused_pane().expect("focused pane");
		assert_eq!(focused.scroll_y, 150);

		// Clicking near the bottom should reach max_scroll (not undershoot).
		let jumped = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ScrollViewportToLine(DocLine::new(u64::MAX)),
		);
		let focused = jumped.focused_pane().expect("focused pane");
		assert!(focused.scroll_y > 0);
		// New clamp allows one row of past-EOF space so no-trailing-newline files can
		// still reach their last line.
		assert!(
			focused.scroll_y + u64::from(focused.viewport_line_count)
				<= focused.max_doc_line.get() + 1
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn scroll_viewport_to_line_reaches_end_of_wrapped_document() {
		use crate::core::DocLine;

		let long = "wrap".repeat(30);
		let mut content = String::new();
		for i in 0..200 {
			content.push_str(&format!("line {i} {long}\n"));
		}
		let scratch_path = temp_test_path("scroll-to-line-wrap").with_extension("rs");
		std::fs::write(&scratch_path, &content).expect("write");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &scratch_name);
		let _resized = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(40, 30));
		let wrap_on = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::SetWrap(true));
		// The clamp below is wrap-aware and so needs document-wide line metrics.
		// Without this the assertion races the background newline scan.
		let _hydrated = wait_for_exact_line_count(&tx_cmd, &rx_view, wrap_on);

		// Ask to scroll far past the end; wrap-aware clamp must land near the
		// document tail, not undershoot by many lines like the non-wrap clamp
		// would (that would leave scroll_y ~= 200 - 30 = 170).
		let jumped = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ScrollViewportToLine(DocLine::new(u64::MAX)),
		);
		let focused = jumped.focused_pane().expect("focused pane");
		assert!(
			focused.scroll_y >= 190,
			"wrapped scroll_y should reach near end of doc, got {}",
			focused.scroll_y
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn page_down_on_markdown_preview_pane_scrolls_source_pane() {
		let scratch_path = temp_test_path("markdown-preview-page-down").with_extension("md");
		let mut body = String::new();
		for i in 0..200 {
			body.push_str(&format!("Line {i}\n"));
		}
		std::fs::write(&scratch_path, &body).expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let _resized = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 40));
		let _loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let preview_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("markdown preview pane")
			.pane_id;
		let source_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label != "Markdown Preview")
			.expect("source pane")
			.pane_id;
		assert_eq!(opened.focused_pane_id, source_pane_id);
		let _focused_preview =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(preview_pane_id));

		// One PageDown may leave the cursor within the initial viewport; send several to
		// force auto-pan.
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let scrolled = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let source_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == source_pane_id)
			.expect("source pane")
			.scroll_y;
		let preview_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == preview_pane_id)
			.expect("preview pane")
			.markdown_preview
			.as_ref()
			.expect("markdown preview snapshot")
			.source_scroll_line
			.get();
		assert!(
			source_scroll > 0,
			"PageDown on preview should scroll source (got {source_scroll})"
		);
		assert_eq!(
			preview_scroll, source_scroll,
			"preview snapshot must reflect the source's post-pan scroll on the same render",
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn page_down_on_wrapped_markdown_preview_syncs_preview_snapshot() {
		let scratch_path = temp_test_path("markdown-preview-wrap-page").with_extension("md");
		// Long lines that will wrap under a 40-column viewport.
		let long_word = "wrap".repeat(30);
		let mut body = String::new();
		for i in 0..200 {
			body.push_str(&format!("Line {i} {long_word}\n"));
		}
		std::fs::write(&scratch_path, &body).expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let _resized = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(40, 30));
		let _wrap_on = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::SetWrap(true));
		let _loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let preview_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("markdown preview pane")
			.pane_id;
		let source_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label != "Markdown Preview")
			.expect("source pane")
			.pane_id;
		assert_eq!(opened.focused_pane_id, source_pane_id);
		let _focused_preview =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(preview_pane_id));

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let scrolled = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		let source_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == source_pane_id)
			.expect("source pane")
			.scroll_y;
		let preview_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == preview_pane_id)
			.expect("preview pane")
			.markdown_preview
			.as_ref()
			.expect("markdown preview snapshot")
			.source_scroll_line
			.get();
		assert!(source_scroll > 0, "source should have scrolled");
		assert_eq!(
			preview_scroll, source_scroll,
			"wrapped source: preview snapshot must reflect source's post-pan scroll on same render",
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn scrolling_source_pane_still_scrolls_markdown_preview() {
		let scratch_path = temp_test_path("markdown-preview-scroll-forward").with_extension("md");
		let mut body = String::new();
		for i in 0..200 {
			body.push_str(&format!("Line {i}\n"));
		}
		std::fs::write(&scratch_path, &body).expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let _resized = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(120, 40));
		let _loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let preview_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("markdown preview pane")
			.pane_id;
		let source_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label != "Markdown Preview")
			.expect("source pane")
			.pane_id;

		let _focused_source =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(source_pane_id));
		let scrolled = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Scroll(50));
		let source_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == source_pane_id)
			.expect("source pane")
			.scroll_y;
		let preview_scroll = scrolled
			.panes
			.iter()
			.find(|pane| pane.pane_id == preview_pane_id)
			.expect("preview pane")
			.markdown_preview
			.as_ref()
			.expect("markdown preview snapshot")
			.source_scroll_line
			.get();
		assert!(source_scroll > 0);
		assert_eq!(preview_scroll, source_scroll);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn markdown_preview_rejects_edit_commands() {
		let scratch_path = temp_test_path("markdown-preview-read-only").with_extension("md");
		std::fs::write(&scratch_path, "# Title\n").expect("write markdown");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_markdown(None);

		let _loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(scratch_name));
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MarkdownPreview { serif: false },
		);
		let preview_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.language_label == "Markdown Preview")
			.expect("preview pane")
			.pane_id;
		let _focused_preview =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(preview_pane_id));
		let rejected = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::InsertText("x".to_string()),
		);

		assert_eq!(
			rejected.status_message.as_deref(),
			Some("Markdown image preview is read-only")
		);
		assert_eq!(rejected.language_label, "Markdown Preview");
		assert!(rejected.tokens.is_empty());
		let snapshot = rejected
			.markdown_preview
			.as_ref()
			.expect("markdown image preview snapshot");
		assert!(!snapshot.source_text.contains('x'));
		let cursor_rejected = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::ZERO, DocDisplayCol::ZERO)),
		);
		assert_eq!(
			cursor_rejected.status_message.as_deref(),
			Some("Markdown image preview is read-only")
		);
		assert_eq!(cursor_rejected.cursor_count, 0);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn large_csv_scroll_keeps_visible_bytes_highlighted() {
		use std::time::Duration;

		let scratch_path = temp_test_path("large-csv-scroll").with_extension("csv");
		let scratch_name = scratch_path.to_string_lossy().to_string();

		let mut header = String::new();
		for col in 0..512 {
			if col > 0 {
				header.push(',');
			}
			header.push_str(&format!("col_{col:03}"));
		}
		header.push('\n');

		let mut body = String::with_capacity(10 * 1024 * 1024);
		body.push_str(&header);
		for row in 0..160 {
			for col in 0..512 {
				if col > 0 {
					body.push(',');
				}
				body.push_str(&format!("r{row:03}_c{col:03}_"));
				body.push_str(&"x".repeat(120));
			}
			body.push('\n');
		}

		std::fs::write(&scratch_path, body).expect("write scratch csv");
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_csv(None);

		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load csv");

		let mut view = recv_view(&rx_view);
		for _ in 0..100 {
			if !view.dma_pending {
				break;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after refresh");
		}
		assert!(!view.dma_pending, "DMA must complete before scrolling");
		assert!(
			viewport_has_any_colored_visible_bytes(&view),
			"initial CSV viewport should have visible coloring"
		);

		for target in [1u64, 8, 16, 24, 40, 64, 96, 120, 140] {
			let view = send_cmd_and_recv(
				&tx_cmd,
				&rx_view,
				EditorCommand::GotoLine(crate::core::DocLine::new(target)),
			);
			assert!(
				viewport_has_any_colored_visible_bytes(&view),
				"CSV viewport lost visible coloring at line {target}; first token starts at byte {:?}, highlight count {}",
				view.tokens
					.first()
					.map(|token| token.absolute_start_byte)
					.unwrap_or(DocByte::ZERO),
				view.highlights.len(),
			);
		}

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn deep_short_row_csv_keeps_visible_coloring_after_context_gap() {
		use crate::svp::resolver::DMA_CHUNK_SIZE;
		use std::time::Duration;

		let scratch_path = temp_test_path("deep-short-row-csv").with_extension("csv");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let row_template = concat!(
			"00000,AAAAAAAAAA,BBBBBBBBBB,CCCCCCCCCC,DDDDDDDDDD,",
			"EEEEEEEEEE,FFFFFFFFFF,GGGGGGGGGG,HHHHHHHHHH,",
			"IIIIIIIIII,JJJJJJJJJJ\n"
		);
		let lines_per_chunk = DMA_CHUNK_SIZE / row_template.len();
		assert!(
			lines_per_chunk > 120,
			"row template must leave enough lines per DMA chunk to straddle the 60-line lexical context window"
		);

		let mut body = String::with_capacity(10 * 1024 * 1024);
		body.push_str("index,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10\n");
		for row in 0..90_000u32 {
			body.push_str(&format!(
				"{row:05},AAAAAAAAAA,BBBBBBBBBB,CCCCCCCCCC,DDDDDDDDDD,EEEEEEEEEE,FFFFFFFFFF,GGGGGGGGGG,HHHHHHHHHH,IIIIIIIIII,JJJJJJJJJJ\n"
			));
		}

		std::fs::write(&scratch_path, body).expect("write deep short-row csv");
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_csv(None);
		tx_cmd
			.send(EditorCommand::LoadFile(scratch_name.clone()))
			.expect("load csv");

		let mut view = recv_view(&rx_view);
		for _ in 0..100 {
			if !view.dma_pending {
				break;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd.refresh_for_test().expect("refresh");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after refresh");
		}
		assert!(!view.dma_pending, "DMA must complete before deep jump");

		let target_line = 1 + (lines_per_chunk as u64 * 24) + 20;
		let mut view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::GotoLine(crate::core::DocLine::new(target_line)),
		);
		for _ in 0..80 {
			if !view.dma_pending {
				break;
			}
			std::thread::sleep(Duration::from_millis(20));
			tx_cmd
				.refresh_for_test()
				.expect("refresh deep short-row csv");
			view = rx_view
				.recv_timeout(Duration::from_secs(2))
				.expect("viewport after deep short-row refresh");
		}

		assert!(
			viewport_has_any_colored_visible_bytes(&view),
			"deep short-row CSV lost visible coloring at line {target_line}; highlights={}, first_token={:?}",
			view.highlights.len(),
			view.tokens
				.first()
				.map(|t| (t.absolute_start_byte, t.text.len())),
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	/// Full-engine integration test for `A` (AppendAtLineEnd) on a real
	/// Rust source file.  The file is loaded through the SVP pipeline,
	/// DMA-inflated, and then structurally grafted by the semantic
	/// reactor — the exact sequence the user experiences.
	///
	/// The cursor is positioned at a mid-line column, then
	/// AppendAtLineEnd is sent.  The cursor must move to the end of the
	/// line and enter Insert mode.
	#[test]
	fn append_at_line_end_full_engine_grafted_rust_file() {
		use crate::core::{DocDisplayCol, DocLine};
		use crate::engine::protocol::EditorMode;

		// Write a temp .rs file with known content.
		let content = "\
use std::fmt;

pub struct Foo {
    bar: Option<String>,
}
";
		let base = temp_test_path("append-eol-graft");
		let scratch_path = base.with_extension("rs");
		std::fs::write(&scratch_path, content).expect("write scratch .rs file");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &scratch_name);

		// Navigate to line 3 ("    bar: Option<String>,"), col 0.
		let view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GotoLine(DocLine::new(3)));
		assert_eq!(
			view.cursor_abs_pos.line,
			DocLine::new(3),
			"GotoLine must move cursor to line 3"
		);
		assert_eq!(
			view.cursor_abs_pos.col,
			DocDisplayCol::ZERO,
			"GotoLine must land at col 0"
		);

		// Compute expected end-of-line column from the source.
		let line_text = content.lines().nth(3).expect("line 3");
		let expected_eol_col = DocDisplayCol::new(line_text.len() as u32);

		// Send AppendAtLineEnd from col 0.
		let view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::AppendAtLineEnd);

		assert_eq!(
			view.status_message, None,
			"AppendAtLineEnd must not produce an error; got: {:?}",
			view.status_message,
		);
		assert_eq!(
			view.cursor_abs_pos.col,
			expected_eol_col,
			"AppendAtLineEnd must move cursor to end of line (col {}), but got col {}",
			expected_eol_col.get(),
			view.cursor_abs_pos.col.get(),
		);
		assert_eq!(view.cursor_abs_pos.line, DocLine::new(3));
		assert_eq!(
			view.input.mode,
			EditorMode::Insert,
			"must enter Insert mode"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	/// Full-engine integration test for `o` (OpenLineBelow) on a real
	/// Rust source file that has been structurally grafted.
	///
	/// The cursor is positioned at line 3, then OpenLineBelow is sent.
	/// A new blank line must appear below line 3, the cursor must be
	/// on line 4, in Insert mode, and no error must be reported.
	#[test]
	fn open_line_below_full_engine_grafted_rust_file() {
		use crate::core::DocLine;
		use crate::engine::protocol::EditorMode;

		let content = "\
use std::fmt;

pub struct Foo {
    bar: Option<String>,
}
";
		let base = temp_test_path("open-below-graft");
		let scratch_path = base.with_extension("rs");
		std::fs::write(&scratch_path, content).expect("write scratch .rs file");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &scratch_name);
		let original_max_doc_line = view.max_doc_line;

		// Navigate to line 3.
		let view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GotoLine(DocLine::new(3)));
		assert_eq!(view.cursor_abs_pos.line, DocLine::new(3));

		// Send OpenLineBelow.
		let view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::OpenLineBelow);

		assert_eq!(
			view.status_message, None,
			"OpenLineBelow must not produce an error; got: {:?}",
			view.status_message,
		);
		assert_eq!(
			view.input.mode,
			EditorMode::Insert,
			"must enter Insert mode"
		);
		assert_eq!(
			view.cursor_abs_pos.line,
			DocLine::new(4),
			"cursor must be on the new line below (line 4)"
		);
		assert_eq!(
			view.max_doc_line,
			original_max_doc_line + DocLineCount::new(1),
			"document must gain one line"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	/// Full-engine integration test for `Scroll` (Ctrl-d half-page) on
	/// a grafted Rust file.  The cursor starts at line 0 and scrolls
	/// down by 10 lines.  No error must be reported.
	#[test]
	fn scroll_down_full_engine_grafted_rust_file() {
		use crate::core::DocLine;

		let mut content = String::new();
		for i in 0..40 {
			content.push_str(&format!("fn func_{}() {{}}\n", i));
		}
		let base = temp_test_path("scroll-graft");
		let scratch_path = base.with_extension("rs");
		std::fs::write(&scratch_path, &content).expect("write scratch .rs file");
		let scratch_name = scratch_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &scratch_name);

		// Send Scroll(10) — equivalent to Ctrl-d with half-page = 10.
		let view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Scroll(10));

		assert_eq!(
			view.status_message, None,
			"Scroll must not produce an error; got: {:?}",
			view.status_message,
		);
		assert!(
			view.cursor_abs_pos.line >= DocLine::new(10),
			"cursor must have moved down; got line {}",
			view.cursor_abs_pos.line,
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(&scratch_path);
	}

	#[test]
	fn scroll_half_page_moves_viewport_anchor_on_real_engine_file() {
		use crate::core::DocLine;

		let path = format!(
			"{}/src/engine/runtime/navigation.rs",
			env!("CARGO_MANIFEST_DIR")
		);
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &path);
		let resized = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(80, 11));
		assert_eq!(resized.viewport_line_count, 9);
		assert_eq!(resized.viewport_start_line, DocLine::ZERO);

		let view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Scroll(5));

		assert_eq!(view.status_message, None);
		assert_eq!(view.cursor_abs_pos.line, DocLine::new(5));
		assert!(
			view.viewport_start_line > DocLine::ZERO,
			"half-page viewport scroll must advance the viewport anchor; got start={} cursor={}",
			view.viewport_start_line,
			view.cursor_abs_pos.line,
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn scroll_full_page_back_moves_viewport_anchor_on_real_engine_file() {
		use crate::core::DocLine;

		let path = format!(
			"{}/src/engine/runtime/navigation.rs",
			env!("CARGO_MANIFEST_DIR")
		);
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &path);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(80, 11));
		let seeded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::GotoLine(DocLine::new(200)),
		);
		let before_start = seeded.viewport_start_line;
		assert!(before_start > DocLine::ZERO);

		let view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Scroll(-8));

		assert_eq!(view.status_message, None);
		assert_eq!(view.cursor_abs_pos.line, DocLine::new(192));
		assert!(
			view.viewport_start_line < before_start,
			"full-page upward scroll must move the viewport anchor back; before={} after={}",
			before_start,
			view.viewport_start_line,
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn viewport_top_middle_bottom_sequence_on_real_engine_file_lands_on_first_non_whitespace() {
		use crate::core::{CursorPosition, DocLine};

		let path = format!(
			"{}/src/engine/runtime/search.rs",
			env!("CARGO_MANIFEST_DIR")
		);
		let source = std::fs::read_to_string(&path).expect("read search.rs");
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &path);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(80, 11));
		let seeded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::GotoLine(DocLine::new(220)),
		);
		let top = seeded.viewport_start_line;
		let middle = DocLine::new(top.get() + 5);
		let bottom = DocLine::new(top.get() + 9);
		let expected_col = |line: DocLine| {
			let text = source
				.lines()
				.nth(line.get() as usize)
				.expect("expected source line");
			crate::engine::support::first_non_whitespace_visual_col(text.as_bytes(), DocLine::ZERO)
		};

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GotoLine(top));
		let top_view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FirstNonWhitespace);
		assert_eq!(
			top_view.cursor_abs_pos,
			CursorPosition::new(top, expected_col(top))
		);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GotoLine(middle));
		let middle_view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FirstNonWhitespace);
		assert_eq!(
			middle_view.cursor_abs_pos,
			CursorPosition::new(middle, expected_col(middle))
		);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::GotoLine(bottom));
		let bottom_view = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FirstNonWhitespace);
		assert_eq!(
			bottom_view.cursor_abs_pos,
			CursorPosition::new(bottom, expected_col(bottom))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn search_word_under_cursor_forward_uses_whole_word_matches_in_real_engine_file() {
		use crate::core::{CursorPosition, DocByte};

		let path = format!(
			"{}/src/engine/runtime/search.rs",
			env!("CARGO_MANIFEST_DIR")
		);
		let source = std::fs::read_to_string(&path).expect("read search.rs");
		let bytes = source.as_bytes();
		let mut offsets = Vec::new();
		let mut start = 0usize;
		while let Some(rel) = source[start..].find("pattern") {
			let abs = start + rel;
			let before_ok =
				abs == 0 || !(bytes[abs - 1].is_ascii_alphanumeric() || bytes[abs - 1] == b'_');
			let after = abs + "pattern".len();
			let after_ok = after == bytes.len()
				|| !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
			if before_ok && after_ok {
				offsets.push(abs);
			}
			start = abs + 1;
		}
		assert!(
			offsets.len() >= 2,
			"expected multiple whole-word `pattern` matches"
		);

		let (start_line, start_col) = crate::engine::undo::line_col_from_byte_offset(
			bytes,
			DocByte::new((offsets[0] + 2) as u64),
		);
		let (expected_line, expected_col) =
			crate::engine::undo::line_col_from_byte_offset(bytes, DocByte::new(offsets[1] as u64));
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &path);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(start_line, start_col)),
		);
		let view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchWordUnderCursor { forward: true },
		);

		assert_eq!(view.status_message, None);
		assert_eq!(view.search_pattern.as_deref(), Some(r"\bpattern\b"));
		assert_eq!(
			view.cursor_abs_pos,
			CursorPosition::new(expected_line, expected_col),
			"forward search should skip `search_pattern`-style partial matches and land on the next whole-word `pattern`"
		);
		assert_eq!(
			view.search_match_info,
			Some(format!("[2/{}]", offsets.len()))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn search_word_under_cursor_backward_moves_to_previous_occurrence_in_real_engine_file() {
		use crate::core::{CursorPosition, DocByte};

		let path = format!(
			"{}/src/engine/runtime/search.rs",
			env!("CARGO_MANIFEST_DIR")
		);
		let source = std::fs::read_to_string(&path).expect("read search.rs");
		let bytes = source.as_bytes();
		let mut offsets = Vec::new();
		let mut start = 0usize;
		while let Some(rel) = source[start..].find("pattern") {
			let abs = start + rel;
			let before_ok =
				abs == 0 || !(bytes[abs - 1].is_ascii_alphanumeric() || bytes[abs - 1] == b'_');
			let after = abs + "pattern".len();
			let after_ok = after == bytes.len()
				|| !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
			if before_ok && after_ok {
				offsets.push(abs);
			}
			start = abs + 1;
		}
		assert!(
			offsets.len() >= 2,
			"expected multiple whole-word `pattern` matches"
		);

		let (start_line, start_col) = crate::engine::undo::line_col_from_byte_offset(
			bytes,
			DocByte::new((offsets[1] + 2) as u64),
		);
		let (expected_line, expected_col) =
			crate::engine::undo::line_col_from_byte_offset(bytes, DocByte::new(offsets[0] as u64));
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &path);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(start_line, start_col)),
		);
		let view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchWordUnderCursor { forward: false },
		);

		assert_eq!(view.status_message, None);
		assert_eq!(view.search_pattern.as_deref(), Some(r"\bpattern\b"));
		assert_eq!(
			view.cursor_abs_pos,
			CursorPosition::new(expected_line, expected_col),
			"backward search from inside a word should move to the previous whole-word occurrence, not stay on the current match"
		);
		assert_eq!(
			view.search_match_info,
			Some(format!("[1/{}]", offsets.len()))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn search_word_under_cursor_backward_wraps_from_first_occurrence_in_real_engine_file() {
		use crate::core::{CursorPosition, DocByte};

		let path = format!(
			"{}/src/engine/runtime/search.rs",
			env!("CARGO_MANIFEST_DIR")
		);
		let source = std::fs::read_to_string(&path).expect("read search.rs");
		let bytes = source.as_bytes();
		let mut offsets = Vec::new();
		let mut start = 0usize;
		while let Some(rel) = source[start..].find("pattern") {
			let abs = start + rel;
			let before_ok =
				abs == 0 || !(bytes[abs - 1].is_ascii_alphanumeric() || bytes[abs - 1] == b'_');
			let after = abs + "pattern".len();
			let after_ok = after == bytes.len()
				|| !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
			if before_ok && after_ok {
				offsets.push(abs);
			}
			start = abs + 1;
		}
		assert!(
			offsets.len() >= 2,
			"expected multiple whole-word `pattern` matches"
		);

		let (start_line, start_col) = crate::engine::undo::line_col_from_byte_offset(
			bytes,
			DocByte::new((offsets[0] + 2) as u64),
		);
		let last = *offsets.last().expect("last whole-word match");
		let (expected_line, expected_col) =
			crate::engine::undo::line_col_from_byte_offset(bytes, DocByte::new(last as u64));
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &path);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(start_line, start_col)),
		);
		let view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchWordUnderCursor { forward: false },
		);

		assert_eq!(view.status_message, None);
		assert_eq!(view.search_pattern.as_deref(), Some(r"\bpattern\b"));
		assert_eq!(
			view.cursor_abs_pos,
			CursorPosition::new(expected_line, expected_col),
			"backward search from the first whole-word occurrence should wrap to the last match"
		);
		assert_eq!(
			view.search_match_info,
			Some(format!("[{0}/{0}] (wrapped)", offsets.len()))
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn clear_search_clears_active_search_state_on_real_engine_file() {
		use crate::core::{CursorPosition, DocByte};

		let path = format!(
			"{}/src/engine/runtime/search.rs",
			env!("CARGO_MANIFEST_DIR")
		);
		let source = std::fs::read_to_string(&path).expect("read search.rs");
		let bytes = source.as_bytes();
		let mut offsets = Vec::new();
		let mut start = 0usize;
		while let Some(rel) = source[start..].find("pattern") {
			let abs = start + rel;
			let before_ok =
				abs == 0 || !(bytes[abs - 1].is_ascii_alphanumeric() || bytes[abs - 1] == b'_');
			let after = abs + "pattern".len();
			let after_ok = after == bytes.len()
				|| !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
			if before_ok && after_ok {
				offsets.push(abs);
			}
			start = abs + 1;
		}
		assert!(
			offsets.len() >= 2,
			"expected multiple whole-word `pattern` matches"
		);

		let (start_line, start_col) = crate::engine::undo::line_col_from_byte_offset(
			bytes,
			DocByte::new((offsets[0] + 2) as u64),
		);
		let (tx_cmd, rx_view, handle) = spawn_test_engine_with_rust(None);

		let _view = load_rs_and_wait_for_graft(&tx_cmd, &rx_view, &path);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(start_line, start_col)),
		);
		let searched = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchWordUnderCursor { forward: true },
		);
		assert_eq!(searched.search_pattern.as_deref(), Some(r"\bpattern\b"));
		assert!(searched.search_match_info.is_some());

		let cleared = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ClearSearch);
		assert_eq!(cleared.status_message, None);
		assert_eq!(cleared.search_pattern, None);
		assert_eq!(cleared.search_match_info, None);

		let after_next = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::SearchNext);
		assert_eq!(after_next.search_pattern, None);
		assert_eq!(after_next.search_match_info, None);
		assert_eq!(
			after_next.status_message.as_deref(),
			Some("No previous search")
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
	}

	#[test]
	fn successful_load_and_reload_clear_document_search_results() {
		let first_path = temp_test_path("search-results-first");
		let second_path = temp_test_path("search-results-second");
		std::fs::write(&first_path, "needle one\nneedle two\n").expect("write first file");
		std::fs::write(&second_path, "second one\nsecond two\n").expect("write second file");
		let first_name = first_path.to_string_lossy().into_owned();
		let second_name = second_path.to_string_lossy().into_owned();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(first_name));
		let searched = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchStart("needle".to_string()),
		);
		assert!(searched.search_match_info.is_some());

		let loaded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::LoadFile(second_name.clone()),
		);
		assert_eq!(loaded.search_pattern.as_deref(), Some("needle"));
		assert_eq!(loaded.search_match_info, None);

		let searched = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SearchStart("second".to_string()),
		);
		assert!(searched.search_match_info.is_some());

		let reloaded =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ReloadFile { force: true });
		assert_eq!(reloaded.search_pattern.as_deref(), Some("second"));
		assert_eq!(reloaded.search_match_info, None);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(first_path);
		let _ = std::fs::remove_file(second_path);
	}

	#[test]
	fn open_file_browser_command_creates_a_left_sidebar() {
		let root = temp_test_path("file-browser-open");
		std::fs::create_dir_all(root.join("src")).expect("create src dir");
		std::fs::write(root.join("Cargo.toml"), "").expect("write cargo");
		let root_name = root.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let view = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name.clone()),
		);
		assert_eq!(view.panes.len(), 2);
		let browser = view
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane");
		assert_eq!(view.focused_pane_id, browser.pane_id);
		assert_eq!(browser.rect.screen_col.get(), 0);
		assert!(browser.rect.width.get() <= 20);
		assert_eq!(browser.gutter_numbering, None);
		assert!(!browser.visible_whitespace);
		let text = pane_text(&view, browser.pane_id);
		assert!(text.contains("▶ src/"));
		assert!(text.contains("  Cargo.toml"));
		assert!(view.panes.iter().any(|pane| !pane.is_file_browser));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn closing_a_file_browser_tab_closes_its_sidebar_not_the_session() {
		let root = temp_test_path("file-browser-tab-close");
		std::fs::create_dir_all(&root).expect("create root dir");
		let root_name = root.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let browser = opened
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane");
		let browser_pane_id = browser.pane_id;
		let browser_view_id = browser.tabs[browser.active_doctab_index].view_id;

		let closed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::CloseDocTab {
				pane_id: browser_pane_id,
				view_id: browser_view_id,
			},
		);
		assert_eq!(closed.panes.len(), 1);
		assert!(
			closed
				.panes
				.iter()
				.all(|pane| pane.pane_id != browser_pane_id)
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn tree_command_focuses_existing_browser_pane() {
		let root = temp_test_path("file-browser-focus");
		std::fs::create_dir_all(&root).expect("create root dir");
		std::fs::write(root.join("main.rs"), "").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name.clone()),
		);
		let browser_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane")
			.pane_id;

		let focused_editor = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::FocusPaneDirection(crate::engine::PaneFocusDirection::Right),
		);
		assert_eq!(focused_editor.panes.len(), 2);
		assert_ne!(focused_editor.focused_pane_id, browser_pane_id);

		let focused_browser = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		assert_eq!(focused_browser.panes.len(), 2);
		assert_eq!(focused_browser.focused_pane_id, browser_pane_id);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn split_commands_from_focused_browser_target_the_editor_pane() {
		let root = temp_test_path("file-browser-split-target");
		std::fs::create_dir_all(&root).expect("create root dir");
		let file_path = root.join("main.rs");
		std::fs::write(&file_path, "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let file_name = file_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _loaded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::LoadFile(file_name.clone()),
		);
		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let browser = opened
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane");

		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Horizontal),
		);
		assert_eq!(
			split
				.panes
				.iter()
				.filter(|pane| pane.is_file_browser)
				.count(),
			1
		);
		let split_browser = split
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane after split");
		assert_eq!(split_browser.pane_id, browser.pane_id);
		assert_eq!(
			split_browser.rect.screen_col.get(),
			browser.rect.screen_col.get()
		);
		assert_eq!(split_browser.rect.width.get(), browser.rect.width.get());
		assert_eq!(split_browser.rect.height.get(), browser.rect.height.get());
		assert!(
			!split.is_file_browser,
			"new split should focus an editor pane"
		);
		assert_eq!(split.panes.len(), 3);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn close_after_browser_focused_split_keeps_focus_on_existing_pane() {
		let root = temp_test_path("file-browser-close-focus");
		std::fs::create_dir_all(&root).expect("create root dir");
		let file_path = root.join("main.rs");
		std::fs::write(&file_path, "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let file_name = file_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _loaded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::LoadFile(file_name.clone()),
		);
		let _opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Horizontal),
		);
		assert_eq!(split.panes.len(), 3);
		assert!(split.pane(split.focused_pane_id).is_some());

		let closed = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ClosePane);
		assert_eq!(closed.panes.len(), 2);
		assert!(
			closed.pane(closed.focused_pane_id).is_some(),
			"focused pane id must refer to an existing pane after close"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn browser_directory_activation_expands_and_collapses_rows() {
		let root = temp_test_path("file-browser-expand");
		let src = root.join("src");
		std::fs::create_dir_all(&src).expect("create src dir");
		std::fs::write(src.join("main.rs"), "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let browser_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane")
			.pane_id;
		assert_eq!(pane_text(&opened, browser_pane_id), "▶ src/");

		let expanded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ActivateBrowserEntry);
		let expanded_text = pane_text(&expanded, browser_pane_id);
		assert!(expanded_text.contains("▼ src/"));
		assert!(expanded_text.contains("    main.rs"));

		let collapsed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::CollapseBrowserEntryOrParent,
		);
		assert_eq!(pane_text(&collapsed, browser_pane_id), "▶ src/");

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn browser_vertical_cursor_motion_moves_between_visible_rows() {
		let root = temp_test_path("file-browser-vertical-motion");
		std::fs::create_dir_all(root.join("src")).expect("create source directory");
		std::fs::write(root.join("Cargo.toml"), "").expect("write manifest");
		let root_name = root.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let browser_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane")
			.pane_id;
		assert_eq!(
			opened
				.pane(browser_pane_id)
				.expect("browser pane")
				.cursor_abs_pos
				.line,
			DocLine::ZERO
		);

		let moved = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(MoveDirection::Down),
		);
		assert_eq!(
			moved
				.pane(browser_pane_id)
				.expect("browser pane")
				.cursor_abs_pos
				.line,
			DocLine::new(1)
		);
		let restored = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(MoveDirection::Up),
		);
		assert_eq!(
			restored
				.pane(browser_pane_id)
				.expect("browser pane")
				.cursor_abs_pos
				.line,
			DocLine::ZERO
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn browser_single_click_expands_without_moving_the_viewport() {
		let root = temp_test_path("file-browser-click-expand");
		let first = root.join("first");
		let second = root.join("second");
		std::fs::create_dir_all(&first).expect("create first dir");
		std::fs::create_dir_all(&second).expect("create second dir");
		std::fs::write(second.join("main.rs"), "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let browser_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane")
			.pane_id;
		let initial_scroll = opened.pane(browser_pane_id).expect("browser pane").scroll_y;

		let expanded = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickBrowserEntry {
				line: DocLine::new(1),
				double_click: false,
			},
		);
		let browser = expanded.pane(browser_pane_id).expect("browser pane");
		assert_eq!(browser.scroll_y, initial_scroll);
		assert_eq!(browser.cursor_abs_pos.line, DocLine::new(1));
		assert!(pane_text(&expanded, browser_pane_id).contains("▼ second/"));

		let double_clicked = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickBrowserEntry {
				line: DocLine::new(1),
				double_click: true,
			},
		);
		assert!(pane_text(&double_clicked, browser_pane_id).contains("▼ second/"));
		assert_eq!(
			double_clicked
				.pane(browser_pane_id)
				.expect("browser pane")
				.scroll_y,
			initial_scroll
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn browser_file_activation_opens_in_the_sibling_editor_pane() {
		let root = temp_test_path("file-browser-open-file");
		std::fs::create_dir_all(&root).expect("create root dir");
		let file_path = root.join("main.rs");
		std::fs::write(&file_path, "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let file_name = file_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let activated = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ActivateBrowserEntry);
		assert_eq!(activated.panes.len(), 2);
		assert!(!activated.is_file_browser);
		assert_eq!(activated.file_name.as_deref(), Some(file_name.as_str()));
		assert!(activated.panes.iter().any(|pane| pane.is_file_browser));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn browser_file_activation_returns_to_the_most_recently_focused_editor_pane() {
		let root = temp_test_path("file-browser-recent-pane");
		std::fs::create_dir_all(&root).expect("create root dir");
		let file_path = root.join("main.rs");
		std::fs::write(&file_path, "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let file_name = file_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let browser_pane_id = opened
			.panes
			.iter()
			.find(|pane| pane.is_file_browser)
			.expect("browser pane")
			.pane_id;
		let first_editor_id = opened
			.panes
			.iter()
			.find(|pane| !pane.is_file_browser)
			.expect("first editor pane")
			.pane_id;
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(first_editor_id));
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical),
		);
		let recent_editor_id = split.focused_pane_id;
		assert_ne!(recent_editor_id, first_editor_id);

		let focused_browser =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(browser_pane_id));
		assert_eq!(focused_browser.focused_pane_id, browser_pane_id);
		let selected = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickBrowserEntry {
				line: DocLine::ZERO,
				double_click: false,
			},
		);
		assert_eq!(selected.focused_pane_id, browser_pane_id);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FocusPane(browser_pane_id));
		let activated = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickBrowserEntry {
				line: DocLine::ZERO,
				double_click: true,
			},
		);

		assert_eq!(activated.focused_pane_id, recent_editor_id);
		assert_eq!(
			activated
				.pane(recent_editor_id)
				.expect("recent editor pane after file activation")
				.file_name
				.as_deref(),
			Some(file_name.as_str())
		);
		assert_eq!(
			activated
				.pane(first_editor_id)
				.expect("first editor pane after file activation")
				.file_name,
			None,
			"opening from the tree must not replace the first editor pane"
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn browser_file_requires_a_double_click_to_open() {
		let root = temp_test_path("file-browser-double-click-file");
		std::fs::create_dir_all(&root).expect("create root dir");
		let file_path = root.join("main.rs");
		std::fs::write(&file_path, "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let file_name = file_path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let browser_pane_id = opened.focused_pane_id;
		let selected = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickBrowserEntry {
				line: DocLine::ZERO,
				double_click: false,
			},
		);
		assert_eq!(selected.focused_pane_id, browser_pane_id);
		assert!(selected.is_file_browser);

		let activated = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickBrowserEntry {
				line: DocLine::ZERO,
				double_click: true,
			},
		);
		assert!(!activated.is_file_browser);
		assert_eq!(activated.file_name.as_deref(), Some(file_name.as_str()));
		assert!(
			activated
				.pane(browser_pane_id)
				.is_some_and(|pane| pane.is_file_browser)
		);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn browser_rejects_write_commands() {
		let root = temp_test_path("file-browser-write");
		std::fs::create_dir_all(&root).expect("create root dir");
		std::fs::write(root.join("main.rs"), "fn main() {}\n").expect("write main");
		let root_name = root.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _opened = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::OpenFileBrowserAt(root_name),
		);
		let rejected = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFile);
		assert!(rejected.is_file_browser);
		assert_eq!(
			rejected.status_message.as_deref(),
			Some("File browser is read-only")
		);
		let cursor_rejected = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::ZERO, DocDisplayCol::ZERO)),
		);
		assert_eq!(
			cursor_rejected.status_message.as_deref(),
			Some("File browser is read-only")
		);
		assert_eq!(cursor_rejected.cursor_count, 1);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn multicursor_adjacent_insert_undo_redo_and_collapse() {
		let path = temp_test_path("multicursor-adjacent");
		std::fs::write(&path, "aa\nbb\ncc\n").expect("write multicursor fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let pane_id = loaded.focused_pane_id;
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(1))),
		);
		let two = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		assert_eq!(two.cursor_count, 2);
		assert_eq!(two.cursor_abs_pos.line, DocLine::new(1));
		assert_eq!(two.secondary_carets.len(), 1);
		assert_eq!(two.secondary_carets[0].abs_pos.line, DocLine::ZERO);

		let three = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		assert_eq!(three.cursor_count, 3);
		assert_eq!(three.cursor_abs_pos.line, DocLine::new(2));

		let inserted = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::InsertChar('X'));
		let inserted = wait_for_dma(&tx_cmd, &rx_view, inserted);
		assert_eq!(pane_text(&inserted, pane_id), "aXa\nbXb\ncXc\n");
		assert_eq!(inserted.cursor_count, 3);
		assert_eq!(inserted.cursor_abs_pos.col, DocDisplayCol::new(2));

		let undone = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Undo);
		let undone = wait_for_dma(&tx_cmd, &rx_view, undone);
		assert_eq!(pane_text(&undone, pane_id), "aa\nbb\ncc\n");
		assert_eq!(undone.cursor_count, 3);
		assert_eq!(undone.cursor_abs_pos.col, DocDisplayCol::new(1));

		let redone = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Redo);
		let redone = wait_for_dma(&tx_cmd, &rx_view, redone);
		assert_eq!(pane_text(&redone, pane_id), "aXa\nbXb\ncXc\n");
		assert_eq!(redone.cursor_count, 3);

		let collapsed = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::CollapseCursors);
		assert_eq!(collapsed.cursor_count, 1);
		assert!(collapsed.secondary_carets.is_empty());

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn multicursor_arbitrary_add_promotes_and_plain_click_collapses() {
		let path = temp_test_path("multicursor-arbitrary");
		std::fs::write(&path, "alpha\nbeta\n").expect("write multicursor fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let pane_id = loaded.focused_pane_id;
		let added = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::new(1), DocDisplayCol::new(2))),
		);
		assert_eq!(added.cursor_count, 2);
		assert_eq!(
			added.cursor_abs_pos,
			CursorPosition::new(DocLine::new(1), DocDisplayCol::new(2))
		);

		let promoted = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::ZERO, DocDisplayCol::ZERO)),
		);
		assert_eq!(promoted.cursor_count, 2);
		assert_eq!(
			promoted.cursor_abs_pos,
			CursorPosition::new(DocLine::ZERO, DocDisplayCol::ZERO)
		);

		let collapsed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::new(1), DocDisplayCol::new(1))),
		);
		assert_eq!(collapsed.cursor_count, 1);
		assert_eq!(
			collapsed.cursor_abs_pos,
			CursorPosition::new(DocLine::new(1), DocDisplayCol::new(1))
		);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::new(1), DocDisplayCol::new(3))),
		);
		let edited = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::InsertChar('X'));
		let edited = wait_for_dma(&tx_cmd, &rx_view, edited);
		assert_eq!(pane_text(&edited, pane_id), "alpha\nbXetXa\n");
		assert_eq!(edited.cursor_count, 2);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn multicursor_point_edits_and_relative_motions_are_transactional() {
		let path = temp_test_path("multicursor-point-edits");
		let original = "abc\nDEF\nxYz\n";
		std::fs::write(&path, original).expect("write multicursor fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let pane_id = loaded.focused_pane_id;
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(1))),
		);
		for _ in 0..2 {
			let _ = send_cmd_and_recv(
				&tx_cmd,
				&rx_view,
				EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
			);
		}

		let moved = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LineEnd);
		assert_eq!(moved.cursor_count, 3);
		assert_eq!(moved.cursor_abs_pos.col, DocDisplayCol::new(3));
		assert!(
			moved
				.secondary_carets
				.iter()
				.all(|caret| caret.abs_pos.col == DocDisplayCol::new(3))
		);
		let append = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::AppendAtLineEnd);
		assert_eq!(append.cursor_abs_pos.col, DocDisplayCol::new(3));
		assert!(
			append
				.secondary_carets
				.iter()
				.all(|caret| caret.abs_pos.col == DocDisplayCol::new(3))
		);
		assert_eq!(append.input.mode, crate::engine::EditorMode::Insert);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::FirstNonWhitespace);
		let reset = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(crate::engine::MoveDirection::Right),
		);
		assert_eq!(reset.cursor_abs_pos.col, DocDisplayCol::new(1));
		assert!(
			reset
				.secondary_carets
				.iter()
				.all(|caret| caret.abs_pos.col == DocDisplayCol::new(1))
		);

		for (command, expected) in [
			(EditorCommand::ReplaceChar('q'), "aqc\nDqF\nxqz\n"),
			(EditorCommand::ToggleCaseAtCursor, "aBc\nDeF\nxyz\n"),
			(EditorCommand::Backspace, "bc\nEF\nYz\n"),
			(EditorCommand::Delete, "ac\nDF\nxz\n"),
			(
				EditorCommand::InsertText("!!".to_string()),
				"a!!bc\nD!!EF\nx!!Yz\n",
			),
			(EditorCommand::InsertChar('\t'), "a\tbc\nD\tEF\nx\tYz\n"),
			(EditorCommand::InsertChar('\n'), "a\nbc\nD\nEF\nx\nYz\n"),
		] {
			let edited = send_cmd_and_recv(&tx_cmd, &rx_view, command);
			let edited = wait_for_dma(&tx_cmd, &rx_view, edited);
			assert_eq!(pane_text(&edited, pane_id), expected);
			assert_eq!(edited.cursor_count, 3);

			let undone = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Undo);
			let undone = wait_for_dma(&tx_cmd, &rx_view, undone);
			assert_eq!(pane_text(&undone, pane_id), original);
			assert_eq!(undone.cursor_count, 3);
		}

		let rejected = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::DeleteLine { register: '"' },
		);
		assert_eq!(
			rejected.status_message.as_deref(),
			Some("Command is not supported while multiple cursors are active")
		);
		assert_eq!(pane_text(&rejected, pane_id), original);
		assert_eq!(rejected.cursor_count, 3);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn adjacent_cursors_retain_preferred_column_across_short_lines() {
		let path = temp_test_path("multicursor-preferred-column");
		std::fs::write(&path, "abcd\n\n\tx\nwxyz\n").expect("write multicursor fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(3))),
		);
		let short = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		assert_eq!(short.cursor_abs_pos.line, DocLine::new(1));
		assert_eq!(short.cursor_abs_pos.col, DocDisplayCol::ZERO);
		let tabbed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		assert_eq!(tabbed.cursor_abs_pos.line, DocLine::new(2));
		let restored = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		assert_eq!(restored.cursor_abs_pos.line, DocLine::new(3));
		assert_eq!(restored.cursor_abs_pos.col, DocDisplayCol::new(3));
		assert_eq!(restored.cursor_count, 4);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn page_motion_retains_preferred_column_across_short_lines() {
		let path = temp_test_path("page-preferred-column");
		let mut lines = vec!["abcdefghij"; 100];
		lines[1] = "";
		lines[50] = "";
		std::fs::write(&path, format!("{}\n", lines.join("\n"))).expect("write page fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(6))),
		);
		let short = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(MoveDirection::Down),
		);
		assert_eq!(short.cursor_abs_pos.col, DocDisplayCol::ZERO);
		let down = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		assert_eq!(down.cursor_abs_pos.col, DocDisplayCol::new(6));

		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(
				DocLine::new(51),
				DocDisplayCol::new(6),
			)),
		);
		let short = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(MoveDirection::Up),
		);
		assert_eq!(short.cursor_abs_pos.col, DocDisplayCol::ZERO);
		let up = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageUp);
		assert_eq!(up.cursor_abs_pos.col, DocDisplayCol::new(6));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn multicursor_unicode_replacements_preserve_boundaries_and_undo() {
		let path = temp_test_path("multicursor-unicode");
		let original = "aß\nİx\n";
		std::fs::write(&path, original).expect("write unicode fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let pane_id = loaded.focused_pane_id;
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(1))),
		);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::new(1), DocDisplayCol::ZERO)),
		);

		let toggled = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ToggleCaseAtCursor);
		let toggled = wait_for_dma(&tx_cmd, &rx_view, toggled);
		assert_eq!(pane_text(&toggled, pane_id), "aSS\ni̇x\n");
		assert_eq!(toggled.cursor_count, 2);

		let undone = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Undo);
		let undone = wait_for_dma(&tx_cmd, &rx_view, undone);
		assert_eq!(pane_text(&undone, pane_id), original);
		assert_eq!(undone.cursor_count, 2);

		let inserted = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::InsertChar('λ'));
		let inserted = wait_for_dma(&tx_cmd, &rx_view, inserted);
		assert_eq!(pane_text(&inserted, pane_id), "aλß\nλİx\n");
		assert_eq!(inserted.cursor_count, 2);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn multicursor_boundary_noops_do_not_block_other_carets() {
		let path = temp_test_path("multicursor-boundaries");
		std::fs::write(&path, "ab").expect("write boundary fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let loaded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let pane_id = loaded.focused_pane_id;
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(2))),
		);
		let backspaced = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Backspace);
		let backspaced = wait_for_dma(&tx_cmd, &rx_view, backspaced);
		assert_eq!(pane_text(&backspaced, pane_id), "a");
		assert_eq!(backspaced.cursor_count, 2);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Undo);
		let deleted = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Delete);
		let deleted = wait_for_dma(&tx_cmd, &rx_view, deleted);
		assert_eq!(pane_text(&deleted, pane_id), "b");
		assert_eq!(deleted.cursor_count, 2);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn page_motion_moves_every_caret() {
		let path = temp_test_path("multicursor-page-motion");
		let text = (0..100)
			.map(|line| format!("line-{line:03}-abcdefghij\n"))
			.collect::<String>();
		std::fs::write(&path, text).expect("write page fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::new(1), DocDisplayCol::new(2))),
		);
		let initial = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::new(2), DocDisplayCol::new(4))),
		);
		assert_eq!(initial.cursor_count, 2);
		let primary_line = initial.cursor_abs_pos.line;
		let secondary_line = initial.secondary_carets[0].abs_pos.line;

		let down = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		assert_eq!(down.cursor_count, 2);
		assert!(down.cursor_abs_pos.line > primary_line);
		assert_eq!(down.secondary_carets.len(), 1);
		assert!(down.secondary_carets[0].abs_pos.line > secondary_line);

		let up = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageUp);
		assert_eq!(up.cursor_abs_pos.line, primary_line);
		let revealed = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ScrollViewportToLine(DocLine::ZERO),
		);
		assert_eq!(revealed.secondary_carets.len(), 1);
		assert_eq!(revealed.secondary_carets[0].abs_pos.line, secondary_line);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn multicursor_word_find_and_vertical_motions_apply_to_every_caret() {
		let path = temp_test_path("multicursor-motions");
		std::fs::write(&path, "one two\none two\none two\n").expect("write motion fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		let collided = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(crate::engine::MoveDirection::Up),
		);
		assert_eq!(collided.cursor_count, 1);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		let words = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(crate::engine::MoveDirection::NextWord),
		);
		assert_eq!(words.cursor_abs_pos.col, DocDisplayCol::new(4));
		assert!(
			words
				.secondary_carets
				.iter()
				.all(|caret| caret.abs_pos.col == DocDisplayCol::new(4))
		);
		let previous = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(crate::engine::MoveDirection::PrevWord),
		);
		assert!(
			previous.cursor_abs_pos.col == DocDisplayCol::ZERO
				&& previous
					.secondary_carets
					.iter()
					.all(|caret| caret.abs_pos.col == DocDisplayCol::ZERO)
		);
		let found = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(crate::engine::MoveDirection::FindChar {
				ch: 't',
				forward: true,
				inclusive: true,
			}),
		);
		assert_eq!(found.cursor_abs_pos.col, DocDisplayCol::new(4));
		assert!(
			found
				.secondary_carets
				.iter()
				.all(|caret| caret.abs_pos.col == DocDisplayCol::new(4))
		);
		let down = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::MoveCursor(crate::engine::MoveDirection::Down),
		);
		assert_eq!(down.cursor_count, 2);
		assert_eq!(down.cursor_abs_pos.line, DocLine::new(2));
		assert_eq!(down.secondary_carets[0].abs_pos.line, DocLine::new(1));

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn wrapped_multicursors_project_to_distinct_screen_rows() {
		let path = temp_test_path("multicursor-wrap");
		std::fs::write(&path, "abcdefghijklmnopqrstuvwxyz\n").expect("write wrap fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::Resize(14, 10));
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::SetWrap(true));
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::ClickCursor(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(1))),
		);
		let wrapped = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAt(CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(15))),
		);
		assert_eq!(wrapped.cursor_count, 2);
		assert_eq!(wrapped.secondary_carets.len(), 1);
		assert!(wrapped.cursor_pane_row > wrapped.secondary_carets[0].pane_row);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn multicursor_state_obeys_tab_visual_fold_and_reload_boundaries() {
		let path = temp_test_path("multicursor-boundaries-state");
		std::fs::write(&path, "one\ntwo\nthree\n").expect("write state fixture");
		let name = path.to_string_lossy().to_string();
		let (tx_cmd, rx_view, handle) = spawn_test_engine(false, false, None);

		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::LoadFile(name));
		let two = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		assert_eq!(two.cursor_count, 2);
		let original_pane_id = two.focused_pane_id;
		let split = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SplitPane(SplitDirection::Vertical),
		);
		assert_eq!(
			split
				.pane(original_pane_id)
				.expect("original pane")
				.cursor_count,
			2
		);
		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::FocusPane(original_pane_id),
		);

		let new_tab = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabNew);
		assert_eq!(new_tab.cursor_count, 1);
		let original_tab = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::DocTabPrev);
		assert_eq!(original_tab.cursor_count, 2);
		let paged = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::PageDown);
		// Both carets clamp to EOF and normalize to the same position.
		assert_eq!(paged.cursor_count, 1);
		let written = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::WriteFile);
		assert_eq!(written.cursor_count, 1);

		let visual = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::SetVisualSelection {
				anchor: paged.cursor_abs_byte,
				kind: crate::engine::VisualKind::Char,
			},
		);
		assert_eq!(visual.cursor_count, 1);
		let _ = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ClearVisualSelection);

		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Up),
		);
		let folded = send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ToggleFold);
		assert_eq!(folded.cursor_count, 1);

		let _ = send_cmd_and_recv(
			&tx_cmd,
			&rx_view,
			EditorCommand::AddCursorAdjacent(VerticalDirection::Down),
		);
		let reloaded =
			send_cmd_and_recv(&tx_cmd, &rx_view, EditorCommand::ReloadFile { force: true });
		assert_eq!(reloaded.cursor_count, 1);

		tx_cmd.send(EditorCommand::Quit).expect("quit");
		handle.join().expect("engine thread");
		let _ = std::fs::remove_file(path);
	}
}
