use super::facade::{EditorHost, lock_services};
use crate::engine::Theme;
use crate::engine::input::rune_option_key;
use crate::engine::runtime::{
	re_resolve_auto_themes, set_line_numbers_local, set_minimap_local,
	set_relative_line_numbers_local, set_visible_whitespace_local, set_wrap_local,
};
use crate::engine::runtime_registry::OptionValue;
use crate::engine::ui_colors::{UiColorKey, parse_ui_rgb};
use rune::Any;
use rune::runtime::{Value, VmResult};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(super) enum ConfigOperation {
	Set {
		name: String,
		value: OptionValue,
	},
	UiColor {
		key: UiColorKey,
		color: crate::core::Rgb,
	},
	LanguageTheme {
		language: String,
		theme: String,
	},
}

#[derive(Default)]
pub(super) struct ConfigQueue {
	// `RuneConfig` may outlive the `&mut EditorHost` used to create it. Queue
	// validated changes for the current VM entry instead of retaining that borrow.
	active: bool,
	operations: Vec<ConfigOperation>,
}

impl ConfigQueue {
	fn activate(&mut self) {
		self.active = true;
	}

	fn push(&mut self, operation: ConfigOperation) -> Result<(), String> {
		if !self.active {
			return Err("Config handle expired".to_string());
		}
		self.operations.push(operation);
		Ok(())
	}

	pub(super) fn finish(&mut self) -> Result<Vec<ConfigOperation>, String> {
		if !self.active {
			return Err("Config scope is not active".to_string());
		}
		self.active = false;
		Ok(std::mem::take(&mut self.operations))
	}
}

#[derive(Any, Clone)]
#[rune(item = ::baryon, name = Config)]
struct RuneConfig {
	queue: Arc<Mutex<ConfigQueue>>,
	language_registry: Option<Arc<crate::engine::language::LanguageRegistry>>,
	services: super::facade::SharedHostServices,
}

pub(crate) fn install(module: &mut rune::Module) -> Result<(), rune::ContextError> {
	module.ty::<RuneConfig>()?;
	module.associated_function("config", config)?;
	module.associated_function("set", config_set)?;
	module.associated_function("ui_color", config_ui_color)?;
	module.associated_function("language_theme", config_language_theme)?;
	module.function("set", set).build()?;
	module.function("ui_color", ui_color).build()?;
	module.function("language_theme", language_theme).build()?;
	Ok(())
}

fn config(editor: &mut EditorHost) -> VmResult<RuneConfig> {
	if let Err(error) = charge(editor) {
		return VmResult::panic(error);
	}
	let queue = editor.config_queue();
	if let Err(error) = queue
		.lock()
		.map_err(|_| "Rune config queue lock was poisoned".to_string())
		.map(|mut queue| queue.activate())
	{
		return VmResult::panic(error);
	}
	VmResult::Ok(RuneConfig {
		queue,
		language_registry: editor.language_registry.clone(),
		services: editor.services.clone(),
	})
}

fn config_set(config: &RuneConfig, name: &str, value: Value) -> VmResult<Result<(), String>> {
	if let Err(error) = charge_services(&config.services) {
		return VmResult::panic(error);
	}
	let operation = parse_set_operation(name, value);
	VmResult::Ok(operation.and_then(|operation| config.push(operation)))
}

fn config_ui_color(config: &RuneConfig, name: &str, hex: &str) -> VmResult<Result<(), String>> {
	if let Err(error) = charge_services(&config.services) {
		return VmResult::panic(error);
	}
	let operation = parse_ui_color_operation(name, hex);
	VmResult::Ok(operation.and_then(|operation| config.push(operation)))
}

fn config_language_theme(
	config: &RuneConfig,
	language: &str,
	theme: &str,
) -> VmResult<Result<(), String>> {
	if let Err(error) = charge_services(&config.services) {
		return VmResult::panic(error);
	}
	let operation =
		parse_language_theme_operation(config.language_registry.as_deref(), language, theme);
	VmResult::Ok(operation.and_then(|operation| config.push(operation)))
}

impl RuneConfig {
	fn push(&self, operation: ConfigOperation) -> Result<(), String> {
		self.queue
			.lock()
			.map_err(|_| "Rune config queue lock was poisoned".to_string())?
			.push(operation)
	}
}

fn set(editor: &mut EditorHost, name: &str, value: Value) -> VmResult<Result<(), String>> {
	if let Err(error) = charge(editor) {
		return VmResult::panic(error);
	}
	VmResult::Ok(
		parse_set_operation(name, value).and_then(|operation| apply_operation(editor, operation)),
	)
}

fn parse_set_operation(name: &str, value: Value) -> Result<ConfigOperation, String> {
	let Some(canonical) = rune_option_key(name) else {
		return Err(format!("Unknown setting: {name}"));
	};
	let option = match canonical {
		"wrap" | "minimap" | "num" | "relativenumber" | "vis" | "reload" => match value.as_bool() {
			Ok(value) => OptionValue::Bool(value),
			Err(_) => return Err(expected(name, "boolean")),
		},
		// Value validity is checked here, not only at apply time: a deferred
		// handle promises call-site errors, so `?` in user scripts must be
		// able to observe an unknown theme or durability mode.
		"theme" => match string_value(&value, name) {
			Ok(value) => {
				Theme::try_new(&value).map_err(|error| error.to_string())?;
				OptionValue::String(value)
			}
			Err(error) => return Err(error),
		},
		"savedurability" => match string_value(&value, name) {
			Ok(value) => {
				crate::engine::settings::SaveDurability::parse(&value)
					.ok_or_else(|| "Usage: set savedurability atomic|bounded".to_string())?;
				OptionValue::String(value)
			}
			Err(error) => return Err(error),
		},
		"gitrefresh" => match value.as_integer::<i64>() {
			Ok(value) if (0..=86_400).contains(&value) => OptionValue::Integer(value),
			Ok(_) => return Err(expected(name, "integer from 0 to 86400")),
			Err(_) => return Err(expected(name, "integer")),
		},
		_ => return Err(format!("Unknown setting: {name}")),
	};
	Ok(ConfigOperation::Set {
		name: canonical.to_string(),
		value: option,
	})
}

fn ui_color(editor: &mut EditorHost, name: &str, hex: &str) -> VmResult<Result<(), String>> {
	if let Err(error) = charge(editor) {
		return VmResult::panic(error);
	}
	VmResult::Ok(
		parse_ui_color_operation(name, hex)
			.and_then(|operation| apply_operation(editor, operation)),
	)
}

fn parse_ui_color_operation(name: &str, hex: &str) -> Result<ConfigOperation, String> {
	let Some(key) = UiColorKey::from_name(name)
		.or_else(|| UiColorKey::from_canonical_name(name))
		.or_else(|| UiColorKey::from_name(&format!("colors.{name}")))
	else {
		return Err(format!("Unknown UI color: {name}"));
	};
	let Some(color) = parse_ui_rgb(hex) else {
		return Err(format!("Invalid hex color: {hex}"));
	};
	Ok(ConfigOperation::UiColor { key, color })
}

fn language_theme(
	editor: &mut EditorHost,
	language: &str,
	theme: &str,
) -> VmResult<Result<(), String>> {
	if let Err(error) = charge(editor) {
		return VmResult::panic(error);
	}
	let operation =
		parse_language_theme_operation(editor.language_registry.as_deref(), language, theme);
	VmResult::Ok(operation.and_then(|operation| apply_operation(editor, operation)))
}

fn parse_language_theme_operation(
	language_registry: Option<&crate::engine::language::LanguageRegistry>,
	language: &str,
	theme: &str,
) -> Result<ConfigOperation, String> {
	let Some(language) =
		language_registry.and_then(|registry| registry.resolve_supported_language_id(language))
	else {
		return Err(format!("Unknown language: {language}"));
	};
	if let Err(error) = Theme::try_new(theme) {
		return Err(error.to_string());
	}
	Ok(ConfigOperation::LanguageTheme {
		language: language.to_string(),
		theme: theme.to_string(),
	})
}

pub(super) fn apply_operations(
	editor: &mut EditorHost,
	operations: Vec<ConfigOperation>,
) -> Result<(), String> {
	for operation in operations {
		apply_operation(editor, operation)?;
	}
	Ok(())
}

fn apply_operation(editor: &mut EditorHost, operation: ConfigOperation) -> Result<(), String> {
	match operation {
		ConfigOperation::Set { name, value } => apply_option(editor, &name, value),
		ConfigOperation::UiColor { key, color } => {
			editor
				.session
				.persistent_settings
				.ui_colors
				.set(key, Some(color));
			editor.mark_needs_render();
			Ok(())
		}
		ConfigOperation::LanguageTheme { language, theme } => {
			editor
				.session
				.persistent_settings
				.language_themes
				.insert(language, theme);
			re_resolve_themes(editor)?;
			editor.mark_needs_render();
			Ok(())
		}
	}
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

fn charge_services(services: &super::facade::SharedHostServices) -> Result<(), &'static str> {
	lock_services(services).budget.charge()
}
