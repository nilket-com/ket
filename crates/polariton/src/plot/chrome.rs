//! Complete resolved color treatment for plot chrome and unencoded mark style.
//!
//! Authored data colors deliberately live elsewhere.

#![allow(dead_code)] // Public substrate lives in the application binary crate.

use iced::Color;
use libaryon::core::{Rgb, Srgb};
use libaryon::engine::{ResolvedThemePair, Theme};
use std::fmt;

pub const TEXT_CONTRAST_MINIMUM: f64 = 4.5;
pub const NON_TEXT_CONTRAST_MINIMUM: f64 = 3.0;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlotRgba {
	red: f32,
	green: f32,
	blue: f32,
	alpha: f32,
}

impl PlotRgba {
	pub fn new(red: f32, green: f32, blue: f32, alpha: f32) -> Option<Self> {
		let channel = |value: f32| {
			value
				.is_finite()
				.then_some(value)
				.filter(|value| (0.0..=1.0).contains(value))
				.map(|value| if value == 0.0 { 0.0 } else { value })
		};
		Some(Self {
			red: channel(red)?,
			green: channel(green)?,
			blue: channel(blue)?,
			alpha: channel(alpha)?,
		})
	}

	pub const fn opaque(red: f32, green: f32, blue: f32) -> Self {
		Self {
			red,
			green,
			blue,
			alpha: 1.0,
		}
	}

	pub const fn translucent(red: f32, green: f32, blue: f32, alpha: f32) -> Self {
		Self {
			red,
			green,
			blue,
			alpha,
		}
	}

	pub const fn red(self) -> f32 {
		self.red
	}

	pub const fn green(self) -> f32 {
		self.green
	}

	pub const fn blue(self) -> f32 {
		self.blue
	}

	pub const fn alpha(self) -> f32 {
		self.alpha
	}

	pub(crate) const fn iced(self) -> Color {
		Color::from_rgba(self.red, self.green, self.blue, self.alpha)
	}

	pub(crate) fn from_iced(color: Color) -> Self {
		Self::new(color.r, color.g, color.b, color.a).expect("Iced colors are normalized")
	}

	pub(crate) const fn opaque_rgb(color: Rgb) -> Self {
		Self::opaque(
			color.r as f32 / 255.0,
			color.g as f32 / 255.0,
			color.b as f32 / 255.0,
		)
	}
}

/// Skin-resolved colors for mark properties that have no authored color identity.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlotMarkStyle {
	pub fill: PlotRgba,
	pub decoration: PlotRgba,
	pub label_text: PlotRgba,
	pub separator: PlotRgba,
}

impl PlotMarkStyle {
	pub const DEFAULT: Self = Self::legacy(0.001, 0.001, 0.001);
	pub const APPLICATION_DEFAULT: Self = Self::legacy(0.0, 0.0, 0.0);

	pub(crate) const fn legacy(red: f32, green: f32, blue: f32) -> Self {
		let paper = PlotRgba::opaque(red, green, blue);
		let ink = PlotRgba::opaque(1.0, 1.0, 1.0);
		Self {
			fill: ink,
			decoration: ink,
			label_text: ink,
			separator: paper,
		}
	}

	pub(crate) fn legacy_from_iced(background: Color, decoration: Color) -> Self {
		let paper = PlotRgba::from_iced(background);
		let ink = PlotRgba::from_iced(decoration);
		Self {
			fill: ink,
			decoration: ink,
			label_text: ink,
			separator: paper,
		}
	}

	pub fn visible_contrast_checks(self, chrome: PlotChrome) -> [PlotContrastCheck; 2] {
		let canvas = visible_srgb(chrome.canvas_background);
		let fill = composite_over(self.fill, canvas);
		[
			contrast("mark fill", self.fill, canvas, NON_TEXT_CONTRAST_MINIMUM),
			contrast(
				"mark separator",
				self.separator,
				fill,
				NON_TEXT_CONTRAST_MINIMUM,
			),
		]
	}
}

impl Default for PlotMarkStyle {
	fn default() -> Self {
		Self::DEFAULT
	}
}

/// One fallible skin projection installed atomically into renderer settings.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedPlotSkin {
	pub chrome: PlotChrome,
	pub mark_style: PlotMarkStyle,
}

impl ResolvedPlotSkin {
	pub fn from_theme_name(
		name: &str,
		projection: PlotChromeProjection,
	) -> Result<Self, PlotChromeProjectionError> {
		let anchors = resolve_anchors(name, projection)?;
		Ok(Self {
			chrome: project_anchors(anchors),
			mark_style: project_mark_style(anchors),
		})
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlotChrome {
	pub canvas_background: PlotRgba,
	pub title_text: PlotRgba,
	pub subtitle_text: PlotRgba,
	pub axis_label_text: PlotRgba,
	pub tick_label_text: PlotRgba,
	pub axis_line: PlotRgba,
	pub gridline: PlotRgba,
	pub legend_background: PlotRgba,
	pub legend_text: PlotRgba,
	pub legend_swatch_fill: PlotRgba,
	pub legend_swatch_border: PlotRgba,
}

impl PlotChrome {
	pub const DEFAULT: Self = Self::legacy(0.001, 0.001, 0.001);
	pub const APPLICATION_DEFAULT: Self = Self::legacy(0.0, 0.0, 0.0);

	const fn legacy(red: f32, green: f32, blue: f32) -> Self {
		let background = PlotRgba::opaque(red, green, blue);
		let decoration = PlotRgba::opaque(1.0, 1.0, 1.0);
		Self {
			canvas_background: background,
			title_text: decoration,
			subtitle_text: decoration,
			axis_label_text: decoration,
			tick_label_text: decoration,
			axis_line: decoration,
			gridline: PlotRgba::translucent(1.0, 1.0, 1.0, 0.35),
			legend_background: background,
			legend_text: decoration,
			legend_swatch_fill: PlotRgba::translucent(1.0, 1.0, 1.0, 0.5),
			legend_swatch_border: decoration,
		}
	}

	pub(crate) fn legacy_from_iced(background: Color, decoration: Color) -> Self {
		let background = PlotRgba::from_iced(background);
		let decoration = PlotRgba::from_iced(decoration);
		Self {
			canvas_background: background,
			title_text: decoration,
			subtitle_text: decoration,
			axis_label_text: decoration,
			tick_label_text: decoration,
			axis_line: decoration,
			gridline: PlotRgba {
				alpha: 0.35,
				..decoration
			},
			legend_background: background,
			legend_text: decoration,
			legend_swatch_fill: PlotRgba {
				alpha: 0.5,
				..decoration
			},
			legend_swatch_border: decoration,
		}
	}

	pub fn from_theme_name(
		name: &str,
		projection: PlotChromeProjection,
	) -> Result<Self, PlotChromeProjectionError> {
		ResolvedPlotSkin::from_theme_name(name, projection).map(|skin| skin.chrome)
	}

	pub fn visible_contrast_checks(self) -> [PlotContrastCheck; 8] {
		let canvas = visible_srgb(self.canvas_background);
		let legend = composite_over(self.legend_background, canvas);
		[
			contrast("title text", self.title_text, canvas, TEXT_CONTRAST_MINIMUM),
			contrast(
				"subtitle text",
				self.subtitle_text,
				canvas,
				TEXT_CONTRAST_MINIMUM,
			),
			contrast(
				"axis label text",
				self.axis_label_text,
				canvas,
				TEXT_CONTRAST_MINIMUM,
			),
			contrast(
				"tick label text",
				self.tick_label_text,
				canvas,
				TEXT_CONTRAST_MINIMUM,
			),
			contrast(
				"legend text",
				self.legend_text,
				legend,
				TEXT_CONTRAST_MINIMUM,
			),
			contrast(
				"axis line",
				self.axis_line,
				canvas,
				NON_TEXT_CONTRAST_MINIMUM,
			),
			contrast("gridline", self.gridline, canvas, NON_TEXT_CONTRAST_MINIMUM),
			contrast(
				"legend swatch border",
				self.legend_swatch_border,
				legend,
				NON_TEXT_CONTRAST_MINIMUM,
			),
		]
	}

	pub fn weakest_contrast(self) -> PlotContrastCheck {
		self.visible_contrast_checks()
			.into_iter()
			.min_by(|left, right| {
				(left.ratio / left.minimum).total_cmp(&(right.ratio / right.minimum))
			})
			.expect("plot chrome has eight contrast checks")
	}

	pub fn passes_contrast(self) -> bool {
		self.visible_contrast_checks()
			.into_iter()
			.all(PlotContrastCheck::passes)
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlotChromeProjection {
	#[default]
	Ordered,
	Duotone,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PlotChromeAnchors {
	paper_index: usize,
	paper: Rgb,
	ink_index: usize,
	ink: Rgb,
	contrast_ratio: f64,
}

impl From<ResolvedThemePair> for PlotChromeAnchors {
	fn from(pair: ResolvedThemePair) -> Self {
		Self {
			paper_index: pair.darker_index(),
			paper: pair.darker(),
			ink_index: pair.lighter_index(),
			ink: pair.lighter(),
			contrast_ratio: pair.contrast_ratio(),
		}
	}
}

fn ordered_anchors(colors: &[Rgb; 20]) -> PlotChromeAnchors {
	const BACKGROUND_SLOT: usize = 19;
	let anchor = colors[BACKGROUND_SLOT];
	let mut mate_index = 0;
	let mut contrast_ratio = anchor.contrast_ratio(colors[0]);
	for (index, color) in colors.iter().copied().enumerate().skip(1) {
		if index == BACKGROUND_SLOT {
			continue;
		}
		let ratio = anchor.contrast_ratio(color);
		if ratio > contrast_ratio {
			mate_index = index;
			contrast_ratio = ratio;
		}
	}
	PlotChromeAnchors {
		paper_index: BACKGROUND_SLOT,
		paper: anchor,
		ink_index: mate_index,
		ink: colors[mate_index],
		contrast_ratio,
	}
}

fn resolve_anchors(
	name: &str,
	projection: PlotChromeProjection,
) -> Result<PlotChromeAnchors, PlotChromeProjectionError> {
	let theme = Theme::try_new(name).map_err(|_| PlotChromeProjectionError::UnknownTheme {
		name: name.to_owned(),
	})?;
	Ok(match projection {
		PlotChromeProjection::Ordered => ordered_anchors(theme.palette().colors()),
		PlotChromeProjection::Duotone => {
			PlotChromeAnchors::from(theme.palette().most_contrasting_pair())
		}
	})
}

fn project_anchors(anchors: PlotChromeAnchors) -> PlotChrome {
	let paper = PlotRgba::opaque_rgb(anchors.paper);
	let ink = PlotRgba::opaque_rgb(anchors.ink);
	let mut chrome = PlotChrome {
		canvas_background: paper,
		title_text: ink,
		subtitle_text: ink,
		axis_label_text: ink,
		tick_label_text: ink,
		axis_line: ink,
		gridline: ink,
		legend_background: paper,
		legend_text: ink,
		legend_swatch_fill: ink,
		legend_swatch_border: ink,
	};

	// Choose the quietest alpha that reaches the non-text target when possible.
	// The contrast assay remains observable but never gates the projected skin.
	for alpha in [0.35, 0.4, 0.5, 0.6, 0.75, 1.0] {
		chrome.gridline.alpha = alpha;
		if chrome
			.visible_contrast_checks()
			.into_iter()
			.find(|check| check.role() == "gridline")
			.expect("gridline assay")
			.passes()
		{
			break;
		}
	}
	chrome.legend_swatch_fill.alpha = chrome.gridline.alpha;
	chrome
}

fn project_mark_style(anchors: PlotChromeAnchors) -> PlotMarkStyle {
	let paper = PlotRgba::opaque_rgb(anchors.paper);
	let ink = PlotRgba::opaque_rgb(anchors.ink);
	PlotMarkStyle {
		fill: ink,
		decoration: ink,
		label_text: ink,
		separator: paper,
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlotChromeProjectionError {
	UnknownTheme { name: String },
}

impl PlotChromeProjectionError {
	pub fn name(&self) -> &str {
		match self {
			Self::UnknownTheme { name } => name,
		}
	}
}

impl fmt::Display for PlotChromeProjectionError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::UnknownTheme { name } => write!(formatter, "unknown plot skin theme `{name}`"),
		}
	}
}

impl std::error::Error for PlotChromeProjectionError {}

impl Default for PlotChrome {
	fn default() -> Self {
		Self::DEFAULT
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlotContrastCheck {
	role: &'static str,
	ratio: f64,
	minimum: f64,
}

impl PlotContrastCheck {
	pub const fn role(self) -> &'static str {
		self.role
	}

	pub const fn ratio(self) -> f64 {
		self.ratio
	}

	pub const fn minimum(self) -> f64 {
		self.minimum
	}

	pub fn passes(self) -> bool {
		self.ratio >= self.minimum
	}
}

fn visible_srgb(color: PlotRgba) -> Srgb {
	Srgb::new(color.red as f64, color.green as f64, color.blue as f64)
		.expect("PlotRgba channels are normalized")
}

fn composite_over(foreground: PlotRgba, background: Srgb) -> Srgb {
	let alpha = foreground.alpha as f64;
	Srgb::new(
		foreground.red as f64 * alpha + background.red() * (1.0 - alpha),
		foreground.green as f64 * alpha + background.green() * (1.0 - alpha),
		foreground.blue as f64 * alpha + background.blue() * (1.0 - alpha),
	)
	.expect("validated plot colors composite into normalized sRGB")
}

fn contrast(
	role: &'static str,
	foreground: PlotRgba,
	background: Srgb,
	minimum: f64,
) -> PlotContrastCheck {
	PlotContrastCheck {
		role,
		ratio: composite_over(foreground, background).contrast_ratio(background),
		minimum,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0031_rgba_rejects_invalid_channels_without_clamping() {
		assert!(PlotRgba::new(0.0, 1.0, 0.5, 1.0).is_some());
		for invalid in [f32::NAN, f32::INFINITY, -0.01, 1.01] {
			assert!(PlotRgba::new(invalid, 0.0, 0.0, 1.0).is_none());
			assert!(PlotRgba::new(0.0, invalid, 0.0, 1.0).is_none());
			assert!(PlotRgba::new(0.0, 0.0, invalid, 1.0).is_none());
			assert!(PlotRgba::new(0.0, 0.0, 0.0, invalid).is_none());
		}
		assert_eq!(
			PlotRgba::new(-0.0, 0.0, 0.0, 1.0).unwrap().red().to_bits(),
			0
		);
	}

	#[test]
	fn product_0067_defaults_pass_while_the_retired_grid_remains_a_falsifiable_control() {
		for chrome in [PlotChrome::DEFAULT, PlotChrome::APPLICATION_DEFAULT] {
			assert_eq!(chrome.gridline.alpha(), 0.35);
			let grid = chrome
				.visible_contrast_checks()
				.into_iter()
				.find(|check| check.role() == "gridline")
				.expect("gridline contrast role");
			assert_eq!(grid.minimum(), NON_TEXT_CONTRAST_MINIMUM);
			assert!(grid.passes(), "default gridline: {:.3}", grid.ratio());
			assert!(chrome.passes_contrast());
		}

		let mut retired = PlotChrome::APPLICATION_DEFAULT;
		retired.gridline = PlotRgba::translucent(1.0, 1.0, 1.0, 0.2);
		let grid = retired
			.visible_contrast_checks()
			.into_iter()
			.find(|check| check.role() == "gridline")
			.expect("retired gridline contrast role");
		assert!((grid.ratio() - 1.6620953314177012).abs() < 1e-6);
		assert!(
			!grid.passes(),
			"the historical control must remain below 3:1"
		);
		assert!(!retired.passes_contrast());
	}

	#[test]
	fn product_0031_thresholds_are_assigned_by_semantic_role() {
		let mut chrome = PlotChrome::APPLICATION_DEFAULT;
		chrome.gridline = PlotRgba::translucent(1.0, 1.0, 1.0, 0.35);
		assert!(chrome.passes_contrast());
		let checks = chrome.visible_contrast_checks();
		assert_eq!(
			checks.iter().filter(|check| check.minimum == 4.5).count(),
			5
		);
		assert_eq!(
			checks.iter().filter(|check| check.minimum == 3.0).count(),
			3
		);
	}

	#[test]
	fn product_0067_only_grid_alpha_supersedes_the_omitted_skin_compatibility_bank() {
		let chrome = PlotChrome::DEFAULT;
		let paper = PlotRgba::opaque(0.001, 0.001, 0.001);
		let ink = PlotRgba::opaque(1.0, 1.0, 1.0);
		assert_eq!(chrome.canvas_background, paper);
		assert_eq!(chrome.title_text, ink);
		assert_eq!(chrome.subtitle_text, ink);
		assert_eq!(chrome.axis_label_text, ink);
		assert_eq!(chrome.tick_label_text, ink);
		assert_eq!(chrome.axis_line, ink);
		assert_eq!(chrome.gridline, PlotRgba::translucent(1.0, 1.0, 1.0, 0.35));
		assert_eq!(chrome.legend_background, paper);
		assert_eq!(chrome.legend_text, ink);
		assert_eq!(
			chrome.legend_swatch_fill,
			PlotRgba::translucent(1.0, 1.0, 1.0, 0.5)
		);
		assert_eq!(chrome.legend_swatch_border, ink);

		let reconstructed = PlotChrome::legacy_from_iced(Color::BLACK, Color::WHITE);
		assert_eq!(reconstructed.gridline.alpha(), 0.35);
		assert!(reconstructed.passes_contrast());

		let style = PlotMarkStyle::DEFAULT;
		assert_eq!(style.fill, ink);
		assert_eq!(style.decoration, ink);
		assert_eq!(style.label_text, ink);
		assert_eq!(style.separator, paper);
	}

	#[test]
	fn product_0032_nord_is_complete_and_validated() {
		let chrome = PlotChrome::from_theme_name("nord", PlotChromeProjection::Ordered)
			.expect("Nord plot skin");
		assert_eq!(
			chrome,
			PlotChrome::from_theme_name("nord", PlotChromeProjection::Duotone).unwrap(),
			"the corrective ordered base keeps accepted Nord exact"
		);
		assert!(chrome.passes_contrast());
		assert_eq!(chrome.visible_contrast_checks().len(), 8);
		assert_eq!(chrome.canvas_background, chrome.legend_background);
		assert_eq!(chrome.title_text, chrome.legend_swatch_border);
		assert!(chrome.gridline.alpha() >= 0.35);
		assert!(chrome.gridline.alpha() <= 1.0);
	}

	#[test]
	fn product_0034_unknown_refuses_but_low_range_remains_observable() {
		let unknown =
			PlotChrome::from_theme_name("not-a-theme", PlotChromeProjection::Ordered).unwrap_err();
		assert!(matches!(
			unknown,
			PlotChromeProjectionError::UnknownTheme { .. }
		));
		assert_eq!(unknown.name(), "not-a-theme");

		let anchors = PlotChromeAnchors {
			paper_index: 3,
			paper: Rgb::new(100, 100, 100),
			ink_index: 17,
			ink: Rgb::new(120, 120, 120),
			contrast_ratio: Rgb::new(100, 100, 100).contrast_ratio(Rgb::new(120, 120, 120)),
		};
		let chrome = project_anchors(anchors);
		assert!(!chrome.passes_contrast());
		assert_eq!(chrome.weakest_contrast().minimum(), 4.5);
		assert_eq!(
			chrome.canvas_background,
			PlotRgba::opaque_rgb(anchors.paper)
		);
		assert_eq!(chrome.title_text, PlotRgba::opaque_rgb(anchors.ink));
		let style = project_mark_style(anchors);
		assert_eq!(style.fill, PlotRgba::opaque_rgb(anchors.ink));
		assert_eq!(style.separator, PlotRgba::opaque_rgb(anchors.paper));
	}

	#[test]
	fn product_0080_product_0079_default_mark_pairs_pass_but_authored_low_contrast_is_exact() {
		let checks = PlotMarkStyle::DEFAULT.visible_contrast_checks(PlotChrome::DEFAULT);
		assert_eq!(
			checks.map(PlotContrastCheck::role),
			["mark fill", "mark separator"],
			"decoration and label text have no assay"
		);
		let default = checks
			.into_iter()
			.find(|check| check.role() == "mark fill")
			.expect("mark fill contrast role");
		assert_eq!(default.minimum(), NON_TEXT_CONTRAST_MINIMUM);
		assert!(
			default.passes(),
			"default mark fill: {:.3}",
			default.ratio()
		);
		assert!((default.ratio() - 20.968).abs() < 0.001);
		let separator = checks
			.into_iter()
			.find(|check| check.role() == "mark separator")
			.expect("mark separator contrast role");
		assert_eq!(separator.minimum(), NON_TEXT_CONTRAST_MINIMUM);
		assert!(
			separator.passes(),
			"default mark separator: {:.3}",
			separator.ratio()
		);
		assert!((separator.ratio() - 20.968).abs() < 0.001);

		let anchors = PlotChromeAnchors {
			paper_index: 0,
			paper: Rgb::new(100, 100, 100),
			ink_index: 1,
			ink: Rgb::new(105, 105, 105),
			contrast_ratio: Rgb::new(100, 100, 100).contrast_ratio(Rgb::new(105, 105, 105)),
		};
		let authored_chrome = project_anchors(anchors);
		let authored_style = project_mark_style(anchors);
		assert_eq!(authored_style.fill, PlotRgba::opaque_rgb(anchors.ink));
		assert_eq!(
			authored_style.separator,
			PlotRgba::opaque_rgb(anchors.paper)
		);
		let authored_checks = authored_style.visible_contrast_checks(authored_chrome);
		let mark = authored_checks
			.into_iter()
			.find(|check| check.role() == "mark fill")
			.expect("authored mark fill contrast role");
		assert!(!mark.passes(), "authored low contrast remains observable");
		let separator = authored_checks
			.into_iter()
			.find(|check| check.role() == "mark separator")
			.expect("authored mark separator contrast role");
		assert!(
			!separator.passes(),
			"authored low-contrast separator remains observable"
		);
	}

	#[test]
	fn product_0080_all_mark_roles_map_at_every_entry_point_and_remain_opaque() {
		let paper = PlotRgba::opaque(0.1, 0.2, 0.3);
		let ink = PlotRgba::opaque(0.8, 0.7, 0.6);
		let anchors = PlotChromeAnchors {
			paper_index: 2,
			paper: Rgb::new(26, 51, 77),
			ink_index: 7,
			ink: Rgb::new(204, 179, 153),
			contrast_ratio: Rgb::new(26, 51, 77).contrast_ratio(Rgb::new(204, 179, 153)),
		};
		let projected = project_mark_style(anchors);
		assert_eq!(projected.fill, PlotRgba::opaque_rgb(anchors.ink));
		assert_eq!(projected.decoration, PlotRgba::opaque_rgb(anchors.ink));
		assert_eq!(projected.label_text, PlotRgba::opaque_rgb(anchors.ink));
		assert_eq!(projected.separator, PlotRgba::opaque_rgb(anchors.paper));

		let legacy = PlotMarkStyle::legacy(0.1, 0.2, 0.3);
		assert_eq!(legacy.fill, PlotRgba::opaque(1.0, 1.0, 1.0));
		assert_eq!(legacy.decoration, PlotRgba::opaque(1.0, 1.0, 1.0));
		assert_eq!(legacy.label_text, PlotRgba::opaque(1.0, 1.0, 1.0));
		assert_eq!(legacy.separator, paper);

		let reconstructed = PlotMarkStyle::legacy_from_iced(paper.iced(), ink.iced());
		assert_eq!(reconstructed.fill, ink);
		assert_eq!(reconstructed.decoration, ink);
		assert_eq!(reconstructed.label_text, ink);
		assert_eq!(reconstructed.separator, paper);

		for style in [projected, legacy, reconstructed] {
			for role in [
				style.fill,
				style.decoration,
				style.label_text,
				style.separator,
			] {
				assert_eq!(role.alpha(), 1.0);
			}
		}

		let low_range = PlotChromeAnchors {
			paper_index: 0,
			paper: Rgb::new(100, 100, 100),
			ink_index: 1,
			ink: Rgb::new(120, 120, 120),
			contrast_ratio: Rgb::new(100, 100, 100).contrast_ratio(Rgb::new(120, 120, 120)),
		};
		assert_eq!(project_anchors(low_range).gridline.alpha(), 1.0);
		for role in {
			let style = project_mark_style(low_range);
			[
				style.fill,
				style.decoration,
				style.label_text,
				style.separator,
			]
		} {
			assert_eq!(
				role.alpha(),
				1.0,
				"mark roles never enter the grid alpha search"
			);
		}
	}

	#[test]
	fn product_0034_tab20c_s9_renders_and_reports_its_title_fact() {
		let chrome =
			PlotChrome::from_theme_name("tab20c_s9", PlotChromeProjection::Ordered).unwrap();
		assert_eq!(
			chrome.canvas_background,
			PlotRgba::opaque_rgb(Rgb::new(117, 107, 177))
		);
		assert_eq!(
			chrome.title_text,
			PlotRgba::opaque_rgb(Rgb::new(199, 233, 192))
		);
		let weakest = chrome.weakest_contrast();
		assert_eq!(weakest.role(), "title text");
		assert!((weakest.ratio() - 3.526).abs() < 0.001);
		assert_eq!(weakest.minimum(), 4.5);
		assert!(!chrome.passes_contrast());
	}

	#[test]
	fn product_0033_ordered_modifiers_remain_visible_and_duotone_collapses_explicitly() {
		let tab_s5 =
			PlotChrome::from_theme_name("tab20c_s5", PlotChromeProjection::Ordered).unwrap();
		let tab_s8 =
			PlotChrome::from_theme_name("tab20c_s8", PlotChromeProjection::Ordered).unwrap();
		assert_ne!(tab_s5, tab_s8);
		assert_eq!(
			PlotChrome::from_theme_name("tab20c_s5", PlotChromeProjection::Duotone).unwrap(),
			PlotChrome::from_theme_name("tab20c_s8", PlotChromeProjection::Duotone).unwrap(),
		);

		let reversed =
			PlotChrome::from_theme_name("viridis_r", PlotChromeProjection::Ordered).unwrap();
		let shifted =
			PlotChrome::from_theme_name("viridis_s5", PlotChromeProjection::Ordered).unwrap();
		assert_ne!(reversed, shifted);
		assert_eq!(
			PlotChrome::from_theme_name("viridis_r", PlotChromeProjection::Duotone).unwrap(),
			PlotChrome::from_theme_name("viridis_s5", PlotChromeProjection::Duotone).unwrap(),
		);
		assert_ne!(
			Theme::try_new("viridis_r").unwrap().palette().colors(),
			Theme::try_new("viridis_s5").unwrap().palette().colors()
		);
	}

	#[test]
	fn product_0034_theme_grammar_census_always_completes_and_retains_facts() {
		let mut below_threshold = 0;
		for spec in libaryon::engine::THEME_SPECS {
			for base in std::iter::once(spec.name).chain(spec.aliases.iter().copied()) {
				let expressions = std::iter::once(base.to_owned())
					.chain(["r", "c", "C"].map(|suffix| format!("{base}_{suffix}")))
					.chain((0..20).map(|shift| format!("{base}_s{shift}")));
				for expression in expressions {
					for projection in [PlotChromeProjection::Ordered, PlotChromeProjection::Duotone]
					{
						let chrome = PlotChrome::from_theme_name(&expression, projection)
							.unwrap_or_else(|error| {
								panic!("accepted theme grammar failed: {error}")
							});
						if !chrome.passes_contrast() {
							below_threshold += 1;
						}
					}
				}
			}
		}
		assert!(
			below_threshold > 0,
			"census must exercise nonblocking classifications"
		);
	}
}
