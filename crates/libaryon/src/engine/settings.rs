use crate::engine::ui_colors::UiColorOverrides;
use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

const DEFAULT_THEME_NAME: &str = "onedark";
const DEFAULT_MINIMAP_ENABLED: bool = true;
const DEFAULT_WRAP_ENABLED: bool = true;
const DEFAULT_LINE_NUMBERS_ENABLED: bool = true;
const DEFAULT_RELATIVE_LINE_NUMBERS_ENABLED: bool = false;
const DEFAULT_VISIBLE_WHITESPACE: bool = true;
const DEFAULT_RELOAD_ENABLED: bool = true;
const DEFAULT_SAVE_DURABILITY: SaveDurability = SaveDurability::Atomic;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SaveDurability {
	#[default]
	Atomic,
	Bounded,
}

impl SaveDurability {
	pub fn parse(value: &str) -> Option<Self> {
		match value {
			"atomic" => Some(Self::Atomic),
			"bounded" => Some(Self::Bounded),
			_ => None,
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Atomic => "atomic",
			Self::Bounded => "bounded",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineSettings {
	pub theme_name: String,
	pub minimap_enabled: bool,
	pub wrap_enabled: bool,
	pub line_numbers_enabled: bool,
	pub relative_line_numbers_enabled: bool,
	pub visible_whitespace: bool,
	pub reload_enabled: bool,
	pub save_durability: SaveDurability,
	pub ui_colors: UiColorOverrides,
	pub language_themes: BTreeMap<String, String>,
}

impl Default for EngineSettings {
	fn default() -> Self {
		Self {
			theme_name: DEFAULT_THEME_NAME.to_string(),
			minimap_enabled: DEFAULT_MINIMAP_ENABLED,
			wrap_enabled: DEFAULT_WRAP_ENABLED,
			line_numbers_enabled: DEFAULT_LINE_NUMBERS_ENABLED,
			relative_line_numbers_enabled: DEFAULT_RELATIVE_LINE_NUMBERS_ENABLED,
			visible_whitespace: DEFAULT_VISIBLE_WHITESPACE,
			reload_enabled: DEFAULT_RELOAD_ENABLED,
			save_durability: DEFAULT_SAVE_DURABILITY,
			ui_colors: UiColorOverrides::default(),
			language_themes: BTreeMap::new(),
		}
	}
}

/// Resolve the user startup script location without reading or writing any
/// machine-owned settings file.
pub fn default_init_path() -> Option<PathBuf> {
	config_dir().map(|path| path.join("baryon").join("init.rn"))
}

fn config_dir() -> Option<PathBuf> {
	let env = SettingsPathEnv::current();
	if cfg!(target_os = "windows") {
		non_empty_path(env.appdata).or_else(|| {
			non_empty_path(env.userprofile).map(|home| home.join("AppData").join("Roaming"))
		})
	} else if cfg!(target_os = "macos") {
		non_empty_path(env.home).map(|home| home.join("Library").join("Application Support"))
	} else {
		non_empty_path(env.xdg_config_home)
			.or_else(|| non_empty_path(env.home).map(|home| home.join(".config")))
	}
}

#[derive(Debug, Default)]
struct SettingsPathEnv {
	xdg_config_home: Option<OsString>,
	home: Option<OsString>,
	appdata: Option<OsString>,
	userprofile: Option<OsString>,
}

impl SettingsPathEnv {
	fn current() -> Self {
		Self {
			xdg_config_home: env::var_os("XDG_CONFIG_HOME"),
			home: env::var_os("HOME"),
			appdata: env::var_os("APPDATA"),
			userprofile: env::var_os("USERPROFILE"),
		}
	}
}

fn non_empty_path(value: Option<OsString>) -> Option<PathBuf> {
	value.filter(|value| !value.is_empty()).map(PathBuf::from)
}
