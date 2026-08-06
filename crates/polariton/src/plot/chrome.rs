//! Complete resolved color treatment for plot-artifact chrome.
//!
//! Data colors and mark compatibility ink deliberately live elsewhere.

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

	const fn opaque_rgb(color: Rgb) -> Self {
		Self::opaque(
			color.r as f32 / 255.0,
			color.g as f32 / 255.0,
			color.b as f32 / 255.0,
		)
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
			gridline: PlotRgba::translucent(1.0, 1.0, 1.0, 0.2),
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
				alpha: 0.2,
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

	pub fn from_theme_name(name: &str) -> Result<Self, PlotChromeProjectionError> {
		let theme = Theme::try_new(name).map_err(|_| PlotChromeProjectionError::UnknownTheme {
			name: name.to_owned(),
		})?;
		let anchors = PlotChromeAnchors::from(theme.palette().most_contrasting_pair());
		project_anchors(anchors).map_err(|weakest| PlotChromeProjectionError::UnsuitableTheme {
			name: name.to_owned(),
			darker_index: anchors.darker_index,
			darker: anchors.darker,
			lighter_index: anchors.lighter_index,
			lighter: anchors.lighter,
			anchor_ratio: anchors.contrast_ratio,
			weakest,
		})
	}

	pub fn visible_contrast_checks(self) -> [PlotChromeContrast; 8] {
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

	pub fn weakest_contrast(self) -> PlotChromeContrast {
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
			.all(PlotChromeContrast::passes)
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PlotChromeAnchors {
	darker_index: usize,
	darker: Rgb,
	lighter_index: usize,
	lighter: Rgb,
	contrast_ratio: f64,
}

impl From<ResolvedThemePair> for PlotChromeAnchors {
	fn from(pair: ResolvedThemePair) -> Self {
		Self {
			darker_index: pair.darker_index(),
			darker: pair.darker(),
			lighter_index: pair.lighter_index(),
			lighter: pair.lighter(),
			contrast_ratio: pair.contrast_ratio(),
		}
	}
}

fn project_anchors(anchors: PlotChromeAnchors) -> Result<PlotChrome, PlotChromeContrast> {
	let paper = PlotRgba::opaque_rgb(anchors.darker);
	let ink = PlotRgba::opaque_rgb(anchors.lighter);
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

	// Choose the quietest admitted alpha. The finite list is policy, not a
	// palette sampler, and validation below remains the final authority.
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
	if chrome.passes_contrast() {
		Ok(chrome)
	} else {
		Err(chrome.weakest_contrast())
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlotChromeProjectionError {
	UnknownTheme {
		name: String,
	},
	UnsuitableTheme {
		name: String,
		darker_index: usize,
		darker: Rgb,
		lighter_index: usize,
		lighter: Rgb,
		anchor_ratio: f64,
		weakest: PlotChromeContrast,
	},
}

impl PlotChromeProjectionError {
	pub fn name(&self) -> &str {
		match self {
			Self::UnknownTheme { name } | Self::UnsuitableTheme { name, .. } => name,
		}
	}

	pub const fn darker_index(&self) -> Option<usize> {
		match self {
			Self::UnknownTheme { .. } => None,
			Self::UnsuitableTheme { darker_index, .. } => Some(*darker_index),
		}
	}

	pub const fn lighter_index(&self) -> Option<usize> {
		match self {
			Self::UnknownTheme { .. } => None,
			Self::UnsuitableTheme { lighter_index, .. } => Some(*lighter_index),
		}
	}

	pub const fn darker(&self) -> Option<Rgb> {
		match self {
			Self::UnknownTheme { .. } => None,
			Self::UnsuitableTheme { darker, .. } => Some(*darker),
		}
	}

	pub const fn lighter(&self) -> Option<Rgb> {
		match self {
			Self::UnknownTheme { .. } => None,
			Self::UnsuitableTheme { lighter, .. } => Some(*lighter),
		}
	}

	pub const fn anchor_ratio(&self) -> Option<f64> {
		match self {
			Self::UnknownTheme { .. } => None,
			Self::UnsuitableTheme { anchor_ratio, .. } => Some(*anchor_ratio),
		}
	}

	pub const fn weakest(&self) -> Option<PlotChromeContrast> {
		match self {
			Self::UnknownTheme { .. } => None,
			Self::UnsuitableTheme { weakest, .. } => Some(*weakest),
		}
	}
}

impl fmt::Display for PlotChromeProjectionError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::UnknownTheme { name } => write!(formatter, "unknown plot skin theme `{name}`"),
			Self::UnsuitableTheme {
				name,
				darker_index,
				darker,
				lighter_index,
				lighter,
				anchor_ratio,
				weakest,
			} => write!(
				formatter,
				"plot skin `{name}` is unsuitable: anchors #{} {:?} and #{} {:?} have {:.3}:1 contrast; weakest {} is {:.3}:1, below {:.1}:1",
				darker_index,
				darker,
				lighter_index,
				lighter,
				anchor_ratio,
				weakest.role(),
				weakest.ratio(),
				weakest.minimum(),
			),
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
pub struct PlotChromeContrast {
	role: &'static str,
	ratio: f64,
	minimum: f64,
}

impl PlotChromeContrast {
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
) -> PlotChromeContrast {
	PlotChromeContrast {
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
	fn product_0031_legacy_grid_is_measured_and_not_endorsed() {
		let checks = PlotChrome::APPLICATION_DEFAULT.visible_contrast_checks();
		for check in checks {
			if check.role() == "gridline" {
				assert!((check.ratio() - 1.6620953314177012).abs() < 1e-6);
				assert_eq!(check.minimum(), NON_TEXT_CONTRAST_MINIMUM);
				assert!(!check.passes());
			} else {
				assert!(check.passes(), "{}: {:.3}", check.role(), check.ratio());
			}
		}
		assert!(!PlotChrome::APPLICATION_DEFAULT.passes_contrast());
		assert_eq!(
			PlotChrome::APPLICATION_DEFAULT.weakest_contrast().role(),
			"gridline"
		);
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
	fn product_0032_banks_the_omitted_skin_compatibility_chrome() {
		let chrome = PlotChrome::DEFAULT;
		let paper = PlotRgba::opaque(0.001, 0.001, 0.001);
		let ink = PlotRgba::opaque(1.0, 1.0, 1.0);
		assert_eq!(chrome.canvas_background, paper);
		assert_eq!(chrome.title_text, ink);
		assert_eq!(chrome.subtitle_text, ink);
		assert_eq!(chrome.axis_label_text, ink);
		assert_eq!(chrome.tick_label_text, ink);
		assert_eq!(chrome.axis_line, ink);
		assert_eq!(chrome.gridline, PlotRgba::translucent(1.0, 1.0, 1.0, 0.2));
		assert_eq!(chrome.legend_background, paper);
		assert_eq!(chrome.legend_text, ink);
		assert_eq!(
			chrome.legend_swatch_fill,
			PlotRgba::translucent(1.0, 1.0, 1.0, 0.5)
		);
		assert_eq!(chrome.legend_swatch_border, ink);
	}

	#[test]
	fn product_0032_nord_is_complete_and_validated() {
		let chrome = PlotChrome::from_theme_name("nord").expect("Nord plot skin");
		assert!(chrome.passes_contrast());
		assert_eq!(chrome.visible_contrast_checks().len(), 8);
		assert_eq!(chrome.canvas_background, chrome.legend_background);
		assert_eq!(chrome.title_text, chrome.legend_swatch_border);
		assert!(chrome.gridline.alpha() >= 0.35);
		assert!(chrome.gridline.alpha() <= 1.0);
	}

	#[test]
	fn product_0032_unknown_and_low_range_fail_with_distinct_facts() {
		let unknown = PlotChrome::from_theme_name("not-a-theme").unwrap_err();
		assert!(matches!(
			unknown,
			PlotChromeProjectionError::UnknownTheme { .. }
		));
		assert_eq!(unknown.name(), "not-a-theme");
		assert!(unknown.weakest().is_none());

		let anchors = PlotChromeAnchors {
			darker_index: 3,
			darker: Rgb::new(100, 100, 100),
			lighter_index: 17,
			lighter: Rgb::new(120, 120, 120),
			contrast_ratio: Rgb::new(100, 100, 100).contrast_ratio(Rgb::new(120, 120, 120)),
		};
		let weakest = project_anchors(anchors).unwrap_err();
		let error = PlotChromeProjectionError::UnsuitableTheme {
			name: "synthetic-low-range".to_owned(),
			darker_index: anchors.darker_index,
			darker: anchors.darker,
			lighter_index: anchors.lighter_index,
			lighter: anchors.lighter,
			anchor_ratio: anchors.contrast_ratio,
			weakest,
		};
		assert_eq!(error.darker_index(), Some(3));
		assert_eq!(error.lighter_index(), Some(17));
		assert_eq!(error.darker(), Some(Rgb::new(100, 100, 100)));
		assert_eq!(error.lighter(), Some(Rgb::new(120, 120, 120)));
		assert!(error.anchor_ratio().unwrap() < 4.5);
		assert_eq!(error.weakest().unwrap().minimum(), 4.5);
		assert!(error.to_string().contains("synthetic-low-range"));
	}

	#[test]
	fn product_0032_set_preserving_modifiers_collapse_intentionally() {
		let reversed = PlotChrome::from_theme_name("viridis_r").unwrap();
		let shifted = PlotChrome::from_theme_name("viridis_s5").unwrap();
		assert_eq!(reversed, shifted);
		assert_ne!(
			Theme::try_new("viridis_r").unwrap().palette().colors(),
			Theme::try_new("viridis_s5").unwrap().palette().colors()
		);
	}

	#[test]
	fn product_0032_theme_grammar_census_never_falls_back_or_skips_validation() {
		for spec in libaryon::engine::THEME_SPECS {
			for base in std::iter::once(spec.name).chain(spec.aliases.iter().copied()) {
				let expressions = std::iter::once(base.to_owned())
					.chain(["r", "c", "C"].map(|suffix| format!("{base}_{suffix}")))
					.chain((0..20).map(|shift| format!("{base}_s{shift}")));
				for expression in expressions {
					match PlotChrome::from_theme_name(&expression) {
						Ok(chrome) => assert!(chrome.passes_contrast(), "{expression}"),
						Err(PlotChromeProjectionError::UnsuitableTheme { weakest, .. }) => {
							assert!(!weakest.passes(), "{expression}")
						}
						Err(error) => panic!("accepted theme grammar failed as unknown: {error}"),
					}
				}
			}
		}
	}
}
