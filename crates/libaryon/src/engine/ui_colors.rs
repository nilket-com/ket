use crate::core::Rgb;

pub const UI_COLOR_COUNT: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum UiColorKey {
	Accent,
	Surface,
	Text,
	EditorBackground,
	EditorText,
	SearchHighlight,
	MinimapAccent,
	MinimapSurface,
	GutterAccent,
	GutterSurface,
	GutterText,
	StatusSurface,
	StatusText,
	TabAccent,
	TabSurface,
	TabActiveSurface,
	TabText,
	PreviewSurface,
	PreviewText,
	DividerSurface,
	GitStaged,
	GitUnstaged,
	GitUntracked,
	GitRename,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiColorDefault {
	Value(Rgb),
	Parent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UiColorSpec {
	pub key: UiColorKey,
	pub name: &'static str,
	pub aliases: &'static [&'static str],
	pub parent: Option<UiColorKey>,
	pub default: UiColorDefault,
	pub summary: &'static str,
}

const ACCENT: Rgb = Rgb::new(76, 255, 0);
const SURFACE: Rgb = Rgb::new(18, 18, 18);
const TEXT: Rgb = Rgb::new(218, 218, 218);
const GIT_GREEN: Rgb = Rgb::new(80, 200, 120);
const GIT_RED: Rgb = Rgb::new(235, 90, 90);
const GIT_YELLOW: Rgb = Rgb::new(235, 200, 80);
const GIT_BLUE: Rgb = Rgb::new(100, 160, 235);

pub const UI_COLOR_SPECS: [UiColorSpec; UI_COLOR_COUNT] = [
	UiColorSpec {
		key: UiColorKey::Accent,
		name: "colors.accent",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(ACCENT),
		summary: "Shared UI accent",
	},
	UiColorSpec {
		key: UiColorKey::Surface,
		name: "colors.surface",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(SURFACE),
		summary: "Shared secondary UI surface",
	},
	UiColorSpec {
		key: UiColorKey::Text,
		name: "colors.text",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(TEXT),
		summary: "Shared chrome text",
	},
	UiColorSpec {
		key: UiColorKey::EditorBackground,
		name: "colors.editor.background",
		aliases: &["background"],
		parent: None,
		default: UiColorDefault::Value(SURFACE),
		summary: "Editor content background",
	},
	UiColorSpec {
		key: UiColorKey::EditorText,
		name: "colors.editor.text",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(TEXT),
		summary: "Plain editor text",
	},
	UiColorSpec {
		key: UiColorKey::SearchHighlight,
		name: "colors.search.highlight",
		aliases: &["searchhighlight"],
		parent: None,
		default: UiColorDefault::Value(ACCENT),
		summary: "Search-match highlight",
	},
	UiColorSpec {
		key: UiColorKey::MinimapAccent,
		name: "colors.minimap.accent",
		aliases: &[],
		parent: Some(UiColorKey::Accent),
		default: UiColorDefault::Parent,
		summary: "Minimap viewport and cursor accent",
	},
	UiColorSpec {
		key: UiColorKey::MinimapSurface,
		name: "colors.minimap.surface",
		aliases: &[],
		parent: Some(UiColorKey::Surface),
		default: UiColorDefault::Value(Rgb::new(16, 20, 28)),
		summary: "Minimap background",
	},
	UiColorSpec {
		key: UiColorKey::GutterAccent,
		name: "colors.gutter.accent",
		aliases: &[],
		parent: Some(UiColorKey::Accent),
		default: UiColorDefault::Parent,
		summary: "Current-line gutter accent",
	},
	UiColorSpec {
		key: UiColorKey::GutterSurface,
		name: "colors.gutter.surface",
		aliases: &[],
		parent: Some(UiColorKey::Surface),
		default: UiColorDefault::Parent,
		summary: "Line-number gutter background",
	},
	UiColorSpec {
		key: UiColorKey::GutterText,
		name: "colors.gutter.text",
		aliases: &[],
		parent: Some(UiColorKey::Text),
		default: UiColorDefault::Value(Rgb::new(108, 108, 108)),
		summary: "Line-number gutter text",
	},
	UiColorSpec {
		key: UiColorKey::StatusSurface,
		name: "colors.status.surface",
		aliases: &[],
		parent: Some(UiColorKey::Surface),
		default: UiColorDefault::Parent,
		summary: "Status bar background",
	},
	UiColorSpec {
		key: UiColorKey::StatusText,
		name: "colors.status.text",
		aliases: &[],
		parent: Some(UiColorKey::Text),
		default: UiColorDefault::Value(Rgb::new(255, 255, 255)),
		summary: "Status bar text",
	},
	UiColorSpec {
		key: UiColorKey::TabAccent,
		name: "colors.tab.accent",
		aliases: &[],
		parent: Some(UiColorKey::Accent),
		default: UiColorDefault::Parent,
		summary: "Active document-tab accent",
	},
	UiColorSpec {
		key: UiColorKey::TabSurface,
		name: "colors.tab.surface",
		aliases: &[],
		parent: Some(UiColorKey::Surface),
		default: UiColorDefault::Value(Rgb::new(12, 14, 18)),
		summary: "Document-tab bar background",
	},
	UiColorSpec {
		key: UiColorKey::TabActiveSurface,
		name: "colors.tab.active.surface",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(Rgb::new(32, 38, 46)),
		summary: "Active document-tab contrast surface",
	},
	UiColorSpec {
		key: UiColorKey::TabText,
		name: "colors.tab.text",
		aliases: &[],
		parent: Some(UiColorKey::Text),
		default: UiColorDefault::Value(Rgb::new(148, 148, 148)),
		summary: "Inactive document-tab text",
	},
	UiColorSpec {
		key: UiColorKey::PreviewSurface,
		name: "colors.preview.surface",
		aliases: &[],
		parent: Some(UiColorKey::Surface),
		default: UiColorDefault::Parent,
		summary: "Markdown preview background",
	},
	UiColorSpec {
		key: UiColorKey::PreviewText,
		name: "colors.preview.text",
		aliases: &[],
		parent: Some(UiColorKey::Text),
		default: UiColorDefault::Value(Rgb::new(224, 229, 235)),
		summary: "Markdown preview body text",
	},
	UiColorSpec {
		key: UiColorKey::DividerSurface,
		name: "colors.divider.surface",
		aliases: &[],
		parent: Some(UiColorKey::Surface),
		default: UiColorDefault::Parent,
		summary: "Split-divider background",
	},
	UiColorSpec {
		key: UiColorKey::GitStaged,
		name: "colors.git.staged",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(GIT_GREEN),
		summary: "Git staged changes",
	},
	UiColorSpec {
		key: UiColorKey::GitUnstaged,
		name: "colors.git.unstaged",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(GIT_RED),
		summary: "Git unstaged changes",
	},
	UiColorSpec {
		key: UiColorKey::GitUntracked,
		name: "colors.git.untracked",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(GIT_YELLOW),
		summary: "Git untracked files",
	},
	UiColorSpec {
		key: UiColorKey::GitRename,
		name: "colors.git.rename",
		aliases: &[],
		parent: None,
		default: UiColorDefault::Value(GIT_BLUE),
		summary: "Git renamed files",
	},
];

impl UiColorKey {
	pub const ALL: [Self; UI_COLOR_COUNT] = [
		Self::Accent,
		Self::Surface,
		Self::Text,
		Self::EditorBackground,
		Self::EditorText,
		Self::SearchHighlight,
		Self::MinimapAccent,
		Self::MinimapSurface,
		Self::GutterAccent,
		Self::GutterSurface,
		Self::GutterText,
		Self::StatusSurface,
		Self::StatusText,
		Self::TabAccent,
		Self::TabSurface,
		Self::TabActiveSurface,
		Self::TabText,
		Self::PreviewSurface,
		Self::PreviewText,
		Self::DividerSurface,
		Self::GitStaged,
		Self::GitUnstaged,
		Self::GitUntracked,
		Self::GitRename,
	];

	pub const fn index(self) -> usize {
		self as usize
	}

	pub fn spec(self) -> &'static UiColorSpec {
		&UI_COLOR_SPECS[self.index()]
	}

	pub fn from_name(name: &str) -> Option<Self> {
		UI_COLOR_SPECS
			.iter()
			.find(|spec| spec.name == name || spec.aliases.contains(&name))
			.map(|spec| spec.key)
	}

	pub fn from_canonical_name(name: &str) -> Option<Self> {
		UI_COLOR_SPECS
			.iter()
			.find(|spec| spec.name == name)
			.map(|spec| spec.key)
	}

	pub fn canonical_name(self) -> &'static str {
		self.spec().name
	}

	pub fn toml_section(self) -> &'static str {
		match self {
			Self::Accent | Self::Surface | Self::Text => "ui.colors",
			Self::EditorBackground | Self::EditorText => "ui.colors.editor",
			Self::SearchHighlight => "ui.colors.search",
			Self::MinimapAccent | Self::MinimapSurface => "ui.colors.minimap",
			Self::GutterAccent | Self::GutterSurface | Self::GutterText => "ui.colors.gutter",
			Self::StatusSurface | Self::StatusText => "ui.colors.status",
			Self::TabAccent | Self::TabSurface | Self::TabText => "ui.colors.tab",
			Self::TabActiveSurface => "ui.colors.tab.active",
			Self::PreviewSurface | Self::PreviewText => "ui.colors.preview",
			Self::DividerSurface => "ui.colors.divider",
			Self::GitStaged | Self::GitUnstaged | Self::GitUntracked | Self::GitRename => {
				"ui.colors.git"
			}
		}
	}

	pub fn toml_key(self) -> &'static str {
		self.canonical_name()
			.rsplit_once('.')
			.map_or(self.canonical_name(), |(_, key)| key)
	}

	pub fn toml_prefix(self) -> &'static str {
		self.canonical_name()
			.rsplit_once('.')
			.map_or(self.canonical_name(), |(prefix, _)| prefix)
	}

	pub fn toml_prefix_for_section(section: &str) -> Option<&'static str> {
		Self::ALL
			.into_iter()
			.find(|key| key.toml_section() == section)
			.map(Self::toml_prefix)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiColorSource {
	Explicit,
	Inherited(UiColorKey),
	ScopedBuiltIn,
	BuiltInDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedUiColor {
	pub color: Rgb,
	pub source: UiColorSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UiColorOverrides {
	values: [Option<Rgb>; UI_COLOR_COUNT],
}

impl Default for UiColorOverrides {
	fn default() -> Self {
		Self {
			values: [None; UI_COLOR_COUNT],
		}
	}
}

impl UiColorOverrides {
	pub fn get(&self, key: UiColorKey) -> Option<Rgb> {
		self.values[key.index()]
	}

	pub fn set(&mut self, key: UiColorKey, value: Option<Rgb>) {
		self.values[key.index()] = value;
	}

	pub fn resolve(&self, key: UiColorKey) -> ResolvedUiColor {
		if let Some(color) = self.get(key) {
			return ResolvedUiColor {
				color,
				source: UiColorSource::Explicit,
			};
		}

		let spec = key.spec();
		if let Some(parent) = spec.parent
			&& let Some(color) = self.get(parent)
		{
			return ResolvedUiColor {
				color,
				source: UiColorSource::Inherited(parent),
			};
		}

		match spec.default {
			UiColorDefault::Value(color) => ResolvedUiColor {
				color,
				source: if spec.parent.is_some() {
					UiColorSource::ScopedBuiltIn
				} else {
					UiColorSource::BuiltInDefault
				},
			},
			UiColorDefault::Parent => {
				let parent = spec.parent.expect("parent default requires a parent");
				ResolvedUiColor {
					color: self.resolve(parent).color,
					source: UiColorSource::Inherited(parent),
				}
			}
		}
	}

	pub fn palette(&self) -> UiPalette {
		let color = |key| self.resolve(key).color;
		UiPalette {
			accent: color(UiColorKey::Accent),
			surface: color(UiColorKey::Surface),
			text: color(UiColorKey::Text),
			editor_background: color(UiColorKey::EditorBackground),
			editor_text: color(UiColorKey::EditorText),
			search_highlight: color(UiColorKey::SearchHighlight),
			minimap_accent: color(UiColorKey::MinimapAccent),
			minimap_surface: color(UiColorKey::MinimapSurface),
			gutter_accent: color(UiColorKey::GutterAccent),
			gutter_surface: color(UiColorKey::GutterSurface),
			gutter_text: color(UiColorKey::GutterText),
			status_surface: color(UiColorKey::StatusSurface),
			status_text: color(UiColorKey::StatusText),
			tab_accent: color(UiColorKey::TabAccent),
			tab_surface: color(UiColorKey::TabSurface),
			tab_active_surface: color(UiColorKey::TabActiveSurface),
			tab_text: color(UiColorKey::TabText),
			preview_surface: color(UiColorKey::PreviewSurface),
			preview_text: color(UiColorKey::PreviewText),
			divider_surface: color(UiColorKey::DividerSurface),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UiPalette {
	pub accent: Rgb,
	pub surface: Rgb,
	pub text: Rgb,
	pub editor_background: Rgb,
	pub editor_text: Rgb,
	pub search_highlight: Rgb,
	pub minimap_accent: Rgb,
	pub minimap_surface: Rgb,
	pub gutter_accent: Rgb,
	pub gutter_surface: Rgb,
	pub gutter_text: Rgb,
	pub status_surface: Rgb,
	pub status_text: Rgb,
	pub tab_accent: Rgb,
	pub tab_surface: Rgb,
	pub tab_active_surface: Rgb,
	pub tab_text: Rgb,
	pub preview_surface: Rgb,
	pub preview_text: Rgb,
	pub divider_surface: Rgb,
}

impl Default for UiPalette {
	fn default() -> Self {
		UiColorOverrides::default().palette()
	}
}

pub fn parse_ui_rgb(value: &str) -> Option<Rgb> {
	let hex = value.strip_prefix('#').unwrap_or(value);
	if !hex.is_ascii() || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
		return None;
	}
	match hex.len() {
		3 => {
			let r = u8::from_str_radix(&hex[0..1].repeat(2), 16).ok()?;
			let g = u8::from_str_radix(&hex[1..2].repeat(2), 16).ok()?;
			let b = u8::from_str_radix(&hex[2..3].repeat(2), 16).ok()?;
			Some(Rgb::new(r, g, b))
		}
		6 => {
			let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
			let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
			let b = u8::from_str_radix(&hex[4..6], 16).ok()?;
			Some(Rgb::new(r, g, b))
		}
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashSet;

	#[test]
	fn registry_names_aliases_and_indices_are_unique_and_round_trip() {
		let mut names = HashSet::new();
		for (index, spec) in UI_COLOR_SPECS.iter().enumerate() {
			assert_eq!(spec.key.index(), index);
			assert_eq!(UiColorKey::from_name(spec.name), Some(spec.key));
			assert_eq!(UiColorKey::from_canonical_name(spec.name), Some(spec.key));
			assert!(names.insert(spec.name), "duplicate color key {}", spec.name);
			for alias in spec.aliases {
				assert_eq!(UiColorKey::from_name(alias), Some(spec.key));
				assert!(names.insert(alias), "duplicate color alias {alias}");
			}
			assert_eq!(
				UiColorKey::toml_prefix_for_section(spec.key.toml_section()),
				Some(spec.key.toml_prefix())
			);
			assert_eq!(
				format!("{}.{}", spec.key.toml_prefix(), spec.key.toml_key()),
				spec.name
			);
		}
	}

	#[test]
	fn built_in_palette_preserves_semantic_differences() {
		let palette = UiPalette::default();
		assert_eq!(palette.accent, Rgb::new(76, 255, 0));
		assert_eq!(palette.minimap_accent, palette.accent);
		assert_eq!(palette.surface, Rgb::new(18, 18, 18));
		assert_eq!(palette.minimap_surface, Rgb::new(16, 20, 28));
		assert_eq!(palette.tab_surface, Rgb::new(12, 14, 18));
		assert_eq!(palette.tab_active_surface, Rgb::new(32, 38, 46));
		assert_eq!(palette.gutter_text, Rgb::new(108, 108, 108));
		assert_eq!(palette.status_text, Rgb::new(255, 255, 255));
		assert_eq!(palette.tab_text, Rgb::new(148, 148, 148));
	}

	#[test]
	fn global_overrides_recolor_unpinned_children() {
		let mut overrides = UiColorOverrides::default();
		let surface = Rgb::new(32, 32, 32);
		let text = Rgb::new(17, 17, 17);
		overrides.set(UiColorKey::Surface, Some(surface));
		overrides.set(UiColorKey::Text, Some(text));
		let palette = overrides.palette();
		assert_eq!(palette.gutter_surface, surface);
		assert_eq!(palette.minimap_surface, surface);
		assert_eq!(palette.tab_surface, surface);
		assert_eq!(palette.tab_active_surface, Rgb::new(32, 38, 46));
		assert_eq!(palette.gutter_text, text);
		assert_eq!(palette.preview_text, text);
		assert_eq!(palette.editor_text, TEXT);
		assert_eq!(palette.editor_background, SURFACE);
	}

	#[test]
	fn scoped_override_stays_pinned_and_reset_resumes_parent() {
		let mut overrides = UiColorOverrides::default();
		let first = Rgb::new(1, 2, 3);
		let second = Rgb::new(4, 5, 6);
		let pinned = Rgb::new(7, 8, 9);
		overrides.set(UiColorKey::Accent, Some(first));
		overrides.set(UiColorKey::MinimapAccent, Some(pinned));
		overrides.set(UiColorKey::Accent, Some(second));
		assert_eq!(overrides.palette().minimap_accent, pinned);
		overrides.set(UiColorKey::MinimapAccent, None);
		assert_eq!(overrides.palette().minimap_accent, second);
		assert_eq!(
			overrides.resolve(UiColorKey::MinimapAccent).source,
			UiColorSource::Inherited(UiColorKey::Accent)
		);
	}

	#[test]
	fn search_highlight_and_accent_are_independent() {
		let mut overrides = UiColorOverrides::default();
		overrides.set(UiColorKey::Accent, Some(Rgb::new(1, 2, 3)));
		assert_eq!(overrides.palette().search_highlight, Rgb::new(76, 255, 0));
	}

	#[test]
	fn strict_rgb_parser_accepts_only_three_or_six_digits() {
		assert_eq!(parse_ui_rgb("0f0"), Some(Rgb::new(0, 255, 0)));
		assert_eq!(parse_ui_rgb("#00ff00"), Some(Rgb::new(0, 255, 0)));
		for value in ["", "00", "0000", "00000", "00000000", "ggg", "éx"] {
			assert_eq!(parse_ui_rgb(value), None, "{value}");
		}
	}
}
