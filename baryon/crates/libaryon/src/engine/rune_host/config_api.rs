use super::facade::{EditorHost, lock_services};
use crate::engine::Theme;
use crate::engine::input::rune_option_key;
use crate::engine::runtime::{
	re_resolve_auto_themes, set_line_numbers_local, set_minimap_local,
	set_relative_line_numbers_local, set_visible_whitespace_local, set_wrap_local,
};
use crate::engine::runtime_registry::OptionValue;
use crate::engine::ui_colors::{UiColorKey, parse_ui_rgb};
use rune::runtime::{Value, VmResult};

pub(crate) fn install(module: &mut rune::Module) -> Result<(), rune::ContextError> {
	module.function("set", set).build()?;
	module.function("ui_color", ui_color).build()?;
	module.function("language_theme", language_theme).build()?;
	Ok(())
}

fn set(editor: &mut EditorHost, name: &str, value: Value) -> VmResult<Result<(), String>> {
	if let Err(error) = charge(editor) {
		return VmResult::panic(error);
	}
	let Some(canonical) = rune_option_key(name) else {
		return VmResult::Ok(Err(format!("Unknown setting: {name}")));
	};
	let option = match canonical {
		"wrap" | "minimap" | "num" | "relativenumber" | "vis" | "reload" => match value.as_bool() {
			Ok(value) => OptionValue::Bool(value),
			Err(_) => return VmResult::Ok(Err(expected(name, "boolean"))),
		},
		"theme" | "savedurability" => match string_value(&value, name) {
			Ok(value) => OptionValue::String(value),
			Err(error) => return VmResult::Ok(Err(error)),
		},
		"gitrefresh" => match value.as_integer::<i64>() {
			Ok(value) if (0..=86_400).contains(&value) => OptionValue::Integer(value),
			Ok(_) => return VmResult::Ok(Err(expected(name, "integer from 0 to 86400"))),
			Err(_) => return VmResult::Ok(Err(expected(name, "integer"))),
		},
		_ => return VmResult::Ok(Err(format!("Unknown setting: {name}"))),
	};
	VmResult::Ok(apply_option(editor, canonical, option))
}

fn ui_color(editor: &mut EditorHost, name: &str, hex: &str) -> VmResult<Result<(), String>> {
	if let Err(error) = charge(editor) {
		return VmResult::panic(error);
	}
	let Some(key) = UiColorKey::from_name(name)
		.or_else(|| UiColorKey::from_canonical_name(name))
		.or_else(|| UiColorKey::from_name(&format!("colors.{name}")))
	else {
		return VmResult::Ok(Err(format!("Unknown UI color: {name}")));
	};
	let Some(color) = parse_ui_rgb(hex) else {
		return VmResult::Ok(Err(format!("Invalid hex color: {hex}")));
	};
	editor
		.session
		.persistent_settings
		.ui_colors
		.set(key, Some(color));
	editor.mark_needs_render();
	VmResult::Ok(Ok(()))
}

fn language_theme(
	editor: &mut EditorHost,
	language: &str,
	theme: &str,
) -> VmResult<Result<(), String>> {
	if let Err(error) = charge(editor) {
		return VmResult::panic(error);
	}
	let Some(language) = editor
		.language_registry
		.as_ref()
		.and_then(|registry| registry.resolve_supported_language_id(language))
	else {
		return VmResult::Ok(Err(format!("Unknown language: {language}")));
	};
	if let Err(error) = Theme::try_new(theme) {
		return VmResult::Ok(Err(error.to_string()));
	}
	editor
		.session
		.persistent_settings
		.language_themes
		.insert(language.to_string(), theme.to_string());
	let result = re_resolve_themes(editor);
	if result.is_ok() {
		editor.mark_needs_render();
	}
	VmResult::Ok(result)
}

pub(super) fn apply_option(
	editor: &mut EditorHost,
	name: &str,
	value: OptionValue,
) -> Result<(), String> {
	match (name, value) {
		("wrap", OptionValue::Bool(value)) => {
			editor.session.persistent_settings.wrap_enabled = value;
			set_wrap_local(active_tab(editor)?, value);
		}
		("minimap", OptionValue::Bool(value)) => {
			editor.session.persistent_settings.minimap_enabled = value;
			set_minimap_local(active_tab(editor)?, value);
		}
		("num", OptionValue::Bool(value)) => {
			editor.session.persistent_settings.line_numbers_enabled = value;
			set_line_numbers_local(active_tab(editor)?, value);
		}
		("relativenumber", OptionValue::Bool(value)) => {
			editor
				.session
				.persistent_settings
				.relative_line_numbers_enabled = value;
			set_relative_line_numbers_local(active_tab(editor)?, value);
		}
		("vis", OptionValue::Bool(value)) => {
			editor.session.persistent_settings.visible_whitespace = value;
			set_visible_whitespace_local(active_tab(editor)?, value);
		}
		("reload", OptionValue::Bool(value)) => {
			editor.session.persistent_settings.reload_enabled = value;
		}
		("theme", OptionValue::String(value)) => {
			Theme::try_new(&value).map_err(|error| error.to_string())?;
			editor.session.persistent_settings.theme_name = value;
			re_resolve_themes(editor)?;
		}
		("savedurability", OptionValue::String(value)) => {
			editor.session.persistent_settings.save_durability =
				crate::engine::settings::SaveDurability::parse(&value)
					.ok_or_else(|| "Usage: set savedurability atomic|bounded".to_string())?;
		}
		("gitrefresh", OptionValue::Integer(value)) if (0..=86_400).contains(&value) => {
			editor.session.git_refresh_seconds = value as u64;
			editor.mark_needs_render();
		}
		(_, _) => return Err(format!("Invalid value for setting: {name}")),
	}
	editor.mark_needs_render();
	Ok(())
}

fn re_resolve_themes(editor: &mut EditorHost) -> Result<(), String> {
	let session = &mut editor.session;
	re_resolve_auto_themes(
		&session.buffers,
		&mut session.layout,
		&session.persistent_settings,
	)
}

fn active_tab(editor: &mut EditorHost) -> Result<&mut crate::engine::session::View, String> {
	editor
		.session
		.layout
		.find_pane_mut(editor.session.focused_pane_id)
		.map(|pane| pane.active_tab_mut())
		.ok_or_else(|| "Focused pane is unavailable".to_string())
}

fn string_value(value: &Value, name: &str) -> Result<String, String> {
	value
		.borrow_string_ref()
		.map(|value| value.to_string())
		.map_err(|_| expected(name, "string"))
}

fn expected(name: &str, kind: &str) -> String {
	format!("Setting {name} expects a {kind}")
}

fn charge(editor: &EditorHost) -> Result<(), &'static str> {
	lock_services(&editor.services).budget.charge()
}
