//! Plot-specific theme and five-layer settings cascade.
//!
//! Data ramps remain plot vocabulary. `UiPalette` crosses this boundary only
//! through PRODUCT-0007's four UI anchors, which resolve both furniture chrome
//! and PRODUCT-0080's unencoded mark style without changing data meaning.

#![allow(dead_code)] // Public cascade API awaits an accepted plot-editing surface.

use crate::plot::chrome::{PlotChrome, PlotChromeProjection, PlotMarkStyle, ResolvedPlotSkin};
use crate::plot::colors::ColorTheme;
use crate::plot::common::{GridLineStyle, ResolvedPlotSettings, ScatterRenderMode, TickPlacement};
use crate::plot::continuous_color::ContinuousRamps;
use crate::plot::spec::{
	BarWidth, CategoryGap, ColorJob, LayerSettings, LegendPosition, Override, PointSize, Presence,
	ResolvedPresence, StrokeWidth,
};
use iced::Color;
use libaryon::engine::UiPalette;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlotRgb {
	pub red: u8,
	pub green: u8,
	pub blue: u8,
}

impl PlotRgb {
	pub const fn new(red: u8, green: u8, blue: u8) -> Self {
		Self { red, green, blue }
	}

	fn iced(self) -> Color {
		Color::from_rgb8(self.red, self.green, self.blue)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlotChromeSpec {
	pub background: PlotRgb,
	pub axis: PlotRgb,
	pub gridline: PlotRgb,
	pub label: PlotRgb,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlotThemeSpec {
	pub chrome: PlotChromeSpec,
	pub categorical_ramp: ColorTheme,
	pub sequential_ramp: ColorTheme,
	pub diverging_ramp: ColorTheme,
	pub status_ramp: ColorTheme,
}

impl PlotThemeSpec {
	pub const DEFAULT_DARK: Self = Self {
		chrome: PlotChromeSpec {
			background: PlotRgb::new(0, 0, 0),
			axis: PlotRgb::new(196, 196, 196),
			gridline: PlotRgb::new(72, 72, 72),
			label: PlotRgb::new(255, 255, 255),
		},
		categorical_ramp: ColorTheme::Viridis,
		sequential_ramp: ColorTheme::Viridis,
		diverging_ramp: ColorTheme::BlueWhiteRed,
		status_ramp: ColorTheme::RedGreen,
	};

	pub fn ramp(self, job: ColorJob) -> ColorTheme {
		match job {
			ColorJob::Categorical => self.categorical_ramp,
			ColorJob::Sequential => self.sequential_ramp,
			ColorJob::Diverging => self.diverging_ramp,
			ColorJob::Status => self.status_ramp,
		}
	}

	pub fn with_ui_chrome(mut self, palette: UiPalette) -> Self {
		self.chrome = chrome_from_ui_palette(palette);
		self
	}
}

impl Default for PlotThemeSpec {
	fn default() -> Self {
		Self::DEFAULT_DARK
	}
}

/// The sole bridge from interface colors into plot theming. Data ramps are not
/// read from `UiPalette`, so changing editor chrome cannot change data meaning.
pub fn chrome_from_ui_palette(palette: UiPalette) -> PlotChromeSpec {
	let rgb = |value: libaryon::core::Rgb| PlotRgb::new(value.r, value.g, value.b);
	PlotChromeSpec {
		background: rgb(palette.editor_background),
		axis: rgb(palette.editor_text),
		gridline: rgb(palette.divider_surface),
		label: rgb(palette.editor_text),
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlotTypography {
	pub title_size: f32,
	pub subtitle_size: f32,
	pub x_label_size: f32,
	pub y_label_size: f32,
	pub x_tick_size: f32,
	pub y_tick_size: f32,
	pub legend_size: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlotPadding {
	pub top: f32,
	pub bottom: f32,
	pub left: f32,
	pub right: f32,
	pub title_offset: f32,
	pub subtitle_offset: f32,
	pub x_label_padding: f32,
	pub y_label_padding: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlotGuideSettings {
	pub max_legend_rows: u32,
	pub legend_x: f32,
	pub legend_y: f32,
	pub x_tick_placement: TickPlacement,
	pub y_tick_placement: TickPlacement,
	pub x_minor_ticks: u32,
	pub y_minor_ticks: u32,
	pub x_major_grid: bool,
	pub y_major_grid: bool,
	pub x_minor_grid: bool,
	pub y_minor_grid: bool,
	pub x_major_grid_width: f32,
	pub y_major_grid_width: f32,
	pub x_minor_grid_width: f32,
	pub y_minor_grid_width: f32,
	pub x_major_grid_style: GridLineStyle,
	pub y_major_grid_style: GridLineStyle,
	pub x_minor_grid_style: GridLineStyle,
	pub y_minor_grid_style: GridLineStyle,
}

impl Default for PlotGuideSettings {
	fn default() -> Self {
		Self {
			max_legend_rows: 10,
			legend_x: 0.95,
			legend_y: 0.05,
			x_tick_placement: TickPlacement::DEFAULT,
			y_tick_placement: TickPlacement::DEFAULT,
			x_minor_ticks: 4,
			y_minor_ticks: 4,
			x_major_grid: true,
			y_major_grid: true,
			x_minor_grid: true,
			y_minor_grid: true,
			x_major_grid_width: 1.0,
			y_major_grid_width: 1.0,
			x_minor_grid_width: 0.5,
			y_minor_grid_width: 0.5,
			x_major_grid_style: GridLineStyle::Solid,
			y_major_grid_style: GridLineStyle::Solid,
			x_minor_grid_style: GridLineStyle::Solid,
			y_minor_grid_style: GridLineStyle::Solid,
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlotDomainOverrides {
	pub x_min: Option<f64>,
	pub x_max: Option<f64>,
	pub y_min: Option<f64>,
	pub y_max: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScatterPolicy {
	pub render_mode: ScatterRenderMode,
	pub max_vector_points: u32,
	pub downsample_target: u32,
	pub raster_threshold: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlotSettingsLayer {
	pub theme: Override<PlotThemeSpec>,
	/// Authored figure identity and explicit projection intent. The renderer
	/// never resolves this selection.
	pub plot_skin: Override<PlotSkinSelection>,
	/// Prepared complete skin, populated only after fallible identity resolution.
	pub resolved_skin: Override<ResolvedPlotSkin>,
	/// Figure-wide categorical palette identity. Resolution remains libaryon-owned;
	/// this settings layer carries only authored/application/pane intent.
	pub categorical_palette: Override<Arc<String>>,
	pub typography: Override<PlotTypography>,
	pub padding: Override<PlotPadding>,
	pub guides: Override<PlotGuideSettings>,
	pub domains: Override<PlotDomainOverrides>,
	pub scatter: Override<ScatterPolicy>,
	pub point_size: Override<PointSize>,
	pub stroke_width: Override<StrokeWidth>,
	pub bar_width: Override<BarWidth>,
	pub category_gap: Override<CategoryGap>,
	pub title: Presence<Arc<String>>,
	pub subtitle: Presence<Arc<String>>,
	pub x_label: Presence<Arc<String>>,
	pub y_label: Presence<Arc<String>>,
	pub legend: Presence<LegendPosition>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlotSkinSelection {
	pub name: Arc<String>,
	pub projection: PlotChromeProjection,
}

impl PlotSkinSelection {
	pub fn new(name: String, projection: PlotChromeProjection) -> Self {
		Self {
			name: Arc::new(name),
			projection,
		}
	}
}

impl Default for PlotSettingsLayer {
	fn default() -> Self {
		Self {
			theme: Override::Inherit,
			plot_skin: Override::Inherit,
			resolved_skin: Override::Inherit,
			categorical_palette: Override::Inherit,
			typography: Override::Inherit,
			padding: Override::Inherit,
			guides: Override::Inherit,
			domains: Override::Inherit,
			scatter: Override::Inherit,
			point_size: Override::Inherit,
			stroke_width: Override::Inherit,
			bar_width: Override::Inherit,
			category_gap: Override::Inherit,
			title: Presence::Inherit,
			subtitle: Presence::Inherit,
			x_label: Presence::Inherit,
			y_label: Presence::Inherit,
			legend: Presence::Inherit,
		}
	}
}

/// Five precedence layers in the exact order accepted by PRODUCT-0007.
/// `ResolvedPlotSettings` is only the flattened renderer projection of this model.
#[derive(Clone, Debug, Default)]
pub struct PlotSettings {
	pub library_defaults: ResolvedPlotSettings,
	pub application_theme: PlotSettingsLayer,
	pub authored_spec: PlotSettingsLayer,
	pub layer: PlotSettingsLayer,
	pub pane_override: PlotSettingsLayer,
}

impl PlotSettings {
	pub const DEFAULT_CATEGORICAL_PALETTE: &'static str = "viridis";

	/// Resolve the figure-wide categorical palette identity before domain
	/// resolution. The graphical-layer tier is intentionally inapplicable to a
	/// figure-wide property, while the accepted five-tier order remains intact.
	pub fn resolve_categorical_palette_identity(&self) -> Arc<String> {
		let mut identity = Arc::new(Self::DEFAULT_CATEGORICAL_PALETTE.to_owned());
		for layer in [
			&self.application_theme,
			&self.authored_spec,
			&self.pane_override,
		] {
			if let Override::Set(value) = &layer.categorical_palette {
				identity = Arc::clone(value);
			}
		}
		identity
	}

	pub fn resolve_plot_skin(&self) -> Option<PlotSkinSelection> {
		let mut identity = None;
		for layer in [
			&self.application_theme,
			&self.authored_spec,
			&self.pane_override,
		] {
			if let Override::Set(value) = &layer.plot_skin {
				identity = Some(value.clone());
			}
		}
		identity
	}

	/// Preserve an already-resolved legacy/editor setting as the library base.
	/// All later tiers are empty, so resolving is byte-for-byte stable.
	pub fn from_resolved(library_defaults: ResolvedPlotSettings) -> Self {
		Self {
			library_defaults,
			..Self::default()
		}
	}

	pub fn resolve(&self) -> ResolvedPlotSettings {
		let mut settings = self.library_defaults.clone();
		for layer in [
			&self.application_theme,
			&self.authored_spec,
			&self.layer,
			&self.pane_override,
		] {
			apply_layer(&mut settings, layer);
		}
		settings
	}

	/// Resolve with one graphical layer occupying PRODUCT-0007's layer tier.
	/// The pane override is deliberately applied afterward.
	pub fn resolve_for_layer(&self, layer: &LayerSettings) -> ResolvedPlotSettings {
		let mut settings = self.library_defaults.clone();
		apply_layer(&mut settings, &self.application_theme);
		apply_layer(&mut settings, &self.authored_spec);
		apply_layer_settings(&mut settings, layer);
		apply_layer(&mut settings, &self.pane_override);
		settings
	}
}

fn apply_layer(settings: &mut ResolvedPlotSettings, layer: &PlotSettingsLayer) {
	if let Override::Set(theme) = layer.theme {
		settings.continuous_ramps = ContinuousRamps::from_theme(theme);
		settings.color_theme = theme.sequential_ramp;
		let background = theme.chrome.background.iced();
		let decoration = theme.chrome.label.iced();
		settings.chrome = PlotChrome::legacy_from_iced(background, decoration);
		settings.mark_style = PlotMarkStyle::legacy_from_iced(background, decoration);
		settings.surrounding_background_color = background;
		settings.surrounding_decoration_color = decoration;
	}
	if let Override::Set(skin) = layer.resolved_skin {
		settings.chrome = skin.chrome;
		settings.mark_style = skin.mark_style;
	}
	if let Override::Set(value) = &layer.typography {
		settings.title_size = value.title_size;
		settings.subtitle_size = value.subtitle_size;
		settings.x_label_size = value.x_label_size;
		settings.y_label_size = value.y_label_size;
		settings.x_tick_size = value.x_tick_size;
		settings.y_tick_size = value.y_tick_size;
		settings.legend_size = value.legend_size;
	}
	if let Override::Set(value) = &layer.padding {
		settings.plot_padding_top = value.top;
		settings.plot_padding_bottom = value.bottom;
		settings.plot_padding_left = value.left;
		settings.plot_padding_right = value.right;
		settings.title_offset = value.title_offset;
		settings.subtitle_offset = value.subtitle_offset;
		settings.x_label_padding = value.x_label_padding;
		settings.y_label_padding = value.y_label_padding;
	}
	if let Override::Set(value) = &layer.guides {
		settings.max_legend_rows = value.max_legend_rows;
		settings.legend_x = value.legend_x;
		settings.legend_y = value.legend_y;
		settings.x_tick_placement = value.x_tick_placement;
		settings.y_tick_placement = value.y_tick_placement;
		settings.x_minor_ticks = value.x_minor_ticks;
		settings.y_minor_ticks = value.y_minor_ticks;
		settings.show_x_major_grid = value.x_major_grid;
		settings.show_y_major_grid = value.y_major_grid;
		settings.show_x_minor_grid = value.x_minor_grid;
		settings.show_y_minor_grid = value.y_minor_grid;
		settings.x_major_grid_width = value.x_major_grid_width;
		settings.y_major_grid_width = value.y_major_grid_width;
		settings.x_minor_grid_width = value.x_minor_grid_width;
		settings.y_minor_grid_width = value.y_minor_grid_width;
		settings.x_major_grid_style = value.x_major_grid_style;
		settings.y_major_grid_style = value.y_major_grid_style;
		settings.x_minor_grid_style = value.x_minor_grid_style;
		settings.y_minor_grid_style = value.y_minor_grid_style;
	}
	if let Override::Set(value) = &layer.domains {
		settings.x_min = value.x_min;
		settings.x_max = value.x_max;
		settings.y_min = value.y_min;
		settings.y_max = value.y_max;
	}
	if let Override::Set(value) = &layer.scatter {
		settings.scatter_render_mode = value.render_mode;
		settings.scatter_max_vector_points = value.max_vector_points;
		settings.scatter_downsample_target = value.downsample_target;
		settings.scatter_raster_threshold = value.raster_threshold;
	}
	if let Override::Set(value) = layer.point_size {
		settings.point_size = value;
	}
	if let Override::Set(value) = layer.stroke_width {
		settings.stroke_width = value;
	}
	if let Override::Set(value) = layer.bar_width {
		settings.bar_width = value;
	}
	if let Override::Set(value) = layer.category_gap {
		settings.category_gap = value;
	}
	apply_presence(&mut settings.title, &layer.title);
	apply_presence(&mut settings.subtitle, &layer.subtitle);
	apply_presence(&mut settings.x_label, &layer.x_label);
	apply_presence(&mut settings.y_label, &layer.y_label);
	apply_presence(&mut settings.legend, &layer.legend);
}

fn apply_layer_settings(settings: &mut ResolvedPlotSettings, layer: &LayerSettings) {
	if let Override::Set(value) = layer.point_size {
		settings.point_size = value;
	}
	if let Override::Set(value) = layer.stroke_width {
		settings.stroke_width = value;
	}
	if let Override::Set(value) = layer.bar_width {
		settings.bar_width = value;
	}
	if let Override::Set(value) = layer.category_gap {
		settings.category_gap = value;
	}
}

fn apply_presence<T: Clone>(target: &mut ResolvedPresence<T>, presence: &Presence<T>) {
	match presence {
		Presence::Inherit => {}
		Presence::Show(value) => *target = ResolvedPresence::Show(value.clone()),
		Presence::Hide => *target = ResolvedPresence::Hide,
	}
}

const fn brightness(color: PlotRgb) -> u32 {
	(color.red as u32 * 299 + color.green as u32 * 587 + color.blue as u32 * 114) / 1000
}

const fn default_theme_is_valid(theme: PlotThemeSpec) -> bool {
	let background = brightness(theme.chrome.background);
	let label = brightness(theme.chrome.label);
	let axis = brightness(theme.chrome.axis);
	let grid = brightness(theme.chrome.gridline);
	label.abs_diff(background) >= 125 && axis > grid && grid > background
}

const _: () = assert!(default_theme_is_valid(PlotThemeSpec::DEFAULT_DARK));

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default_theme_passes_contrast_ordering_and_keeps_ramps_plot_local() {
		assert!(default_theme_is_valid(PlotThemeSpec::DEFAULT_DARK));
		assert!(!default_theme_is_valid(PlotThemeSpec {
			chrome: PlotChromeSpec {
				background: PlotRgb::new(0, 0, 0),
				axis: PlotRgb::new(20, 20, 20),
				gridline: PlotRgb::new(40, 40, 40),
				label: PlotRgb::new(30, 30, 30),
			},
			..PlotThemeSpec::DEFAULT_DARK
		}));
		let theme = PlotThemeSpec::default();
		assert_eq!(theme.ramp(ColorJob::Sequential), ColorTheme::Viridis);
		assert_eq!(theme.ramp(ColorJob::Diverging), ColorTheme::BlueWhiteRed);
		let luminance = |color: Color| color.r * 0.299 + color.g * 0.587 + color.b * 0.114;
		assert!(
			luminance(theme.sequential_ramp.get_color(0.0))
				< luminance(theme.sequential_ramp.get_color(1.0)),
			"default sequential ramp must preserve low-to-high ordering"
		);
		assert!(
			luminance(theme.diverging_ramp.get_color(0.5))
				> luminance(theme.diverging_ramp.get_color(0.0))
				&& luminance(theme.diverging_ramp.get_color(0.5))
					> luminance(theme.diverging_ramp.get_color(1.0)),
			"default diverging midpoint must remain perceptually distinct"
		);
	}

	#[test]
	fn ui_palette_bridge_changes_only_chrome() {
		let theme = PlotThemeSpec::default();
		let bridged = theme.with_ui_chrome(UiPalette::default());
		assert_ne!(bridged.chrome, theme.chrome);
		assert_eq!(bridged.categorical_ramp, theme.categorical_ramp);
		assert_eq!(bridged.sequential_ramp, theme.sequential_ramp);
		assert_eq!(bridged.diverging_ramp, theme.diverging_ramp);
		assert_eq!(bridged.status_ramp, theme.status_ramp);
	}

	#[test]
	fn five_layer_precedence_is_right_biased_and_hide_is_not_a_value_override() {
		let mut cascade = PlotSettings::default();
		assert!(matches!(
			cascade.resolve().title,
			ResolvedPresence::Fallback
		));
		assert!(matches!(
			cascade.resolve().legend,
			ResolvedPresence::Fallback
		));
		cascade.library_defaults.title = ResolvedPresence::Show(Arc::new("library".to_owned()));
		cascade.application_theme.title = Presence::Show(Arc::new("application".to_owned()));
		cascade.authored_spec.title = Presence::Show(Arc::new("authored".to_owned()));
		cascade.layer.title = Presence::Show(Arc::new("layer".to_owned()));
		cascade.pane_override.title = Presence::Show(Arc::new("pane".to_owned()));
		assert!(matches!(
			cascade.resolve().title,
			ResolvedPresence::Show(ref title) if title.as_str() == "pane"
		));
		cascade.pane_override.title = Presence::Hide;
		assert!(matches!(cascade.resolve().title, ResolvedPresence::Hide));

		let authored_position = LegendPosition::new(0.98, 0.02).unwrap();
		cascade.authored_spec.legend = Presence::Show(authored_position);
		assert_eq!(
			cascade.resolve().legend,
			ResolvedPresence::Show(authored_position)
		);
		cascade.pane_override.legend = Presence::Hide;
		assert!(matches!(cascade.resolve().legend, ResolvedPresence::Hide));

		cascade.application_theme.theme = Override::Set(PlotThemeSpec::default());
		cascade.pane_override.theme = Override::Set(PlotThemeSpec {
			sequential_ramp: ColorTheme::Magma,
			..PlotThemeSpec::default()
		});
		assert_eq!(cascade.resolve().color_theme, ColorTheme::Magma);
	}

	#[test]
	fn product_0089_continuous_ramps_follow_the_same_theme_tiers_atomically() {
		let defaults = PlotThemeSpec::default();
		let resolved = ResolvedPlotSettings::default();
		assert_eq!(
			resolved.continuous_ramps.sequential,
			defaults.sequential_ramp
		);
		assert_eq!(resolved.continuous_ramps.diverging, defaults.diverging_ramp);

		let mut cascade = PlotSettings::default();
		cascade.application_theme.theme = Override::Set(PlotThemeSpec {
			sequential_ramp: ColorTheme::Magma,
			diverging_ramp: ColorTheme::BlueWhiteRed,
			..defaults
		});
		cascade.pane_override.theme = Override::Set(PlotThemeSpec {
			sequential_ramp: ColorTheme::Inferno,
			diverging_ramp: ColorTheme::GreenPurples,
			..defaults
		});
		let resolved = cascade.resolve();
		assert_eq!(resolved.color_theme, ColorTheme::Inferno);
		assert_eq!(resolved.continuous_ramps.sequential, ColorTheme::Inferno);
		assert_eq!(
			resolved.continuous_ramps.diverging,
			ColorTheme::GreenPurples
		);
	}

	#[test]
	fn product_0030_categorical_palette_has_one_explicit_figure_wide_cascade() {
		let identity = |value: &str| Override::Set(Arc::new(value.to_owned()));
		let mut cascade = PlotSettings::default();
		assert_eq!(
			cascade.resolve_categorical_palette_identity().as_str(),
			PlotSettings::DEFAULT_CATEGORICAL_PALETTE
		);
		assert_eq!(PlotSettings::DEFAULT_CATEGORICAL_PALETTE, "viridis");

		cascade.application_theme.categorical_palette = identity("application");
		assert_eq!(
			cascade.resolve_categorical_palette_identity().as_str(),
			"application"
		);
		cascade.authored_spec.categorical_palette = identity("authored");
		assert_eq!(
			cascade.resolve_categorical_palette_identity().as_str(),
			"authored"
		);

		cascade.layer.categorical_palette = identity("wrong-graphical-layer");
		assert_eq!(
			cascade.resolve_categorical_palette_identity().as_str(),
			"authored",
			"graphical-layer settings cannot override figure-wide palette identity"
		);
		cascade.pane_override.categorical_palette = identity("pane");
		assert_eq!(
			cascade.resolve_categorical_palette_identity().as_str(),
			"pane"
		);
	}

	#[test]
	fn product_0080_product_0032_plot_skin_identity_and_prepared_skin_follow_the_cascade() {
		let identity = |value: &str| {
			Override::Set(PlotSkinSelection::new(
				value.to_owned(),
				PlotChromeProjection::Ordered,
			))
		};
		let mut cascade = PlotSettings::default();
		assert!(cascade.resolve_plot_skin().is_none());
		assert_eq!(cascade.resolve().chrome, PlotChrome::DEFAULT);

		let application = PlotThemeSpec {
			chrome: PlotChromeSpec {
				background: PlotRgb::new(12, 34, 56),
				axis: PlotRgb::new(90, 91, 92),
				gridline: PlotRgb::new(70, 71, 72),
				label: PlotRgb::new(210, 220, 230),
			},
			..PlotThemeSpec::DEFAULT_DARK
		};
		let mut application_only = PlotSettings::default();
		application_only.application_theme.theme = Override::Set(application);
		let application_resolved = application_only.resolve();
		assert_eq!(
			application_resolved.chrome.canvas_background.iced(),
			application.chrome.background.iced()
		);
		assert_eq!(
			application_resolved.mark_style.fill.iced(),
			application.chrome.label.iced()
		);
		assert_eq!(
			application_resolved.mark_style.separator.iced(),
			application.chrome.background.iced()
		);

		cascade.application_theme.plot_skin = identity("application");
		cascade.authored_spec.plot_skin = identity("authored");
		cascade.layer.plot_skin = identity("wrong-layer");
		assert_eq!(
			cascade.resolve_plot_skin().unwrap().name.as_str(),
			"authored"
		);

		let nord =
			ResolvedPlotSkin::from_theme_name("nord", PlotChromeProjection::Ordered).unwrap();
		cascade.authored_spec.resolved_skin = Override::Set(nord);
		let resolved = cascade.resolve();
		assert_eq!(resolved.chrome, nord.chrome);
		assert_eq!(resolved.mark_style, nord.mark_style);
		assert_eq!(resolved.surrounding_decoration_color, Color::WHITE);

		let viridis =
			ResolvedPlotSkin::from_theme_name("viridis", PlotChromeProjection::Ordered).unwrap();
		cascade.pane_override.plot_skin = identity("pane");
		cascade.pane_override.resolved_skin = Override::Set(viridis);
		assert_eq!(cascade.resolve_plot_skin().unwrap().name.as_str(), "pane");
		assert_eq!(cascade.resolve().chrome, viridis.chrome);
		assert_eq!(cascade.resolve().mark_style, viridis.mark_style);
	}

	#[test]
	fn product_0017_point_size_selects_each_layer_before_pane_override() {
		let size = |factor| PointSize::new(factor).unwrap();
		let mut cascade = PlotSettings::default();
		cascade.library_defaults.point_size = size(0.25);
		cascade.application_theme.point_size = Override::Set(size(0.5));
		cascade.authored_spec.point_size = Override::Set(size(1.5));
		cascade.layer.point_size = Override::Set(size(2.0));
		cascade.pane_override.point_size = Override::Set(size(4.0));
		assert_eq!(cascade.resolve().point_size, size(4.0));

		cascade.pane_override.point_size = Override::Inherit;
		assert_eq!(cascade.resolve().point_size, size(2.0));

		let small = LayerSettings {
			point_size: Override::Set(size(0.5)),
			..LayerSettings::default()
		};
		let large = LayerSettings {
			point_size: Override::Set(size(2.0)),
			..LayerSettings::default()
		};
		assert_eq!(cascade.resolve_for_layer(&small).point_size, size(0.5));
		assert_eq!(cascade.resolve_for_layer(&large).point_size, size(2.0));

		cascade.pane_override.point_size = Override::Set(size(0.25));
		assert_eq!(cascade.resolve_for_layer(&large).point_size, size(0.25));
		assert_ne!(
			cascade.resolve_for_layer(&large).point_size.factor(),
			2.0 * 0.25,
			"cascade tiers select rather than multiply"
		);
	}

	#[test]
	fn product_0019_stroke_width_selects_each_layer_before_pane_override() {
		let width = |factor| StrokeWidth::new(factor).unwrap();
		let mut cascade = PlotSettings::default();
		cascade.library_defaults.stroke_width = width(0.25);
		cascade.application_theme.stroke_width = Override::Set(width(0.5));
		cascade.authored_spec.stroke_width = Override::Set(width(1.5));
		cascade.layer.stroke_width = Override::Set(width(2.0));
		cascade.pane_override.stroke_width = Override::Set(width(4.0));
		assert_eq!(cascade.resolve().stroke_width, width(4.0));

		cascade.pane_override.stroke_width = Override::Inherit;
		assert_eq!(cascade.resolve().stroke_width, width(2.0));
		let thin = LayerSettings {
			stroke_width: Override::Set(width(0.5)),
			..LayerSettings::default()
		};
		let thick = LayerSettings {
			stroke_width: Override::Set(width(2.0)),
			..LayerSettings::default()
		};
		assert_eq!(cascade.resolve_for_layer(&thin).stroke_width, width(0.5));
		assert_eq!(cascade.resolve_for_layer(&thick).stroke_width, width(2.0));

		cascade.pane_override.stroke_width = Override::Set(width(0.25));
		assert_eq!(cascade.resolve_for_layer(&thick).stroke_width, width(0.25));
		assert_ne!(
			cascade.resolve_for_layer(&thick).stroke_width.factor(),
			2.0 * 0.25,
			"cascade tiers select rather than multiply"
		);
	}

	#[test]
	fn product_0021_bar_width_selects_each_layer_before_pane_override() {
		let width = |fraction| BarWidth::new(fraction).unwrap();
		let mut cascade = PlotSettings::default();
		cascade.library_defaults.bar_width = width(0.2);
		cascade.application_theme.bar_width = Override::Set(width(0.3));
		cascade.authored_spec.bar_width = Override::Set(width(0.4));
		cascade.layer.bar_width = Override::Set(width(0.5));
		cascade.pane_override.bar_width = Override::Set(width(0.6));
		assert_eq!(cascade.resolve().bar_width, width(0.6));

		cascade.pane_override.bar_width = Override::Inherit;
		assert_eq!(cascade.resolve().bar_width, width(0.5));
		let narrow = LayerSettings {
			bar_width: Override::Set(width(0.25)),
			..LayerSettings::default()
		};
		let wide = LayerSettings {
			bar_width: Override::Set(width(0.75)),
			..LayerSettings::default()
		};
		assert_eq!(cascade.resolve_for_layer(&narrow).bar_width, width(0.25));
		assert_eq!(cascade.resolve_for_layer(&wide).bar_width, width(0.75));

		cascade.pane_override.bar_width = Override::Set(width(0.2));
		assert_eq!(cascade.resolve_for_layer(&wide).bar_width, width(0.2));
		assert_ne!(
			cascade.resolve_for_layer(&wide).bar_width.fraction(),
			0.75 * 0.2,
			"cascade tiers select rather than multiply"
		);
	}

	#[test]
	fn product_0053_category_gap_selects_each_layer_before_pane_override() {
		let gap = |fraction| CategoryGap::new(fraction).unwrap();
		let mut cascade = PlotSettings::default();
		cascade.library_defaults.category_gap = gap(0.1);
		cascade.application_theme.category_gap = Override::Set(gap(0.2));
		cascade.authored_spec.category_gap = Override::Set(gap(0.3));
		cascade.layer.category_gap = Override::Set(gap(0.4));
		cascade.pane_override.category_gap = Override::Set(gap(0.5));
		assert_eq!(cascade.resolve().category_gap, gap(0.5));

		cascade.pane_override.category_gap = Override::Inherit;
		assert_eq!(cascade.resolve().category_gap, gap(0.4));
		let close = LayerSettings {
			category_gap: Override::Set(gap(0.15)),
			..LayerSettings::default()
		};
		let separated = LayerSettings {
			category_gap: Override::Set(gap(0.7)),
			..LayerSettings::default()
		};
		assert_eq!(cascade.resolve_for_layer(&close).category_gap, gap(0.15));
		assert_eq!(cascade.resolve_for_layer(&separated).category_gap, gap(0.7));

		cascade.pane_override.category_gap = Override::Set(gap(0.25));
		assert_eq!(
			cascade.resolve_for_layer(&separated).category_gap,
			gap(0.25)
		);
		assert_ne!(
			cascade
				.resolve_for_layer(&separated)
				.category_gap
				.fraction(),
			0.7 * 0.25,
			"cascade tiers select rather than multiply"
		);
	}
}
