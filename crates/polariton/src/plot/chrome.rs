//! Complete resolved color treatment for plot-artifact chrome.
//!
//! Data colors and mark compatibility ink deliberately live elsewhere.

#![allow(dead_code)] // The renderer migration is the next PRODUCT-0031 commit.

use iced::Color;
use libaryon::core::Srgb;

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
}
