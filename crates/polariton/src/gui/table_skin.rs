//! Complete, resolved color treatment for the interior of a table artifact.
//!
//! This model is deliberately independent of editor chrome and plot data ramps.

#[cfg(test)]
use libaryon::core::{Rgb, Srgb};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TableRgba {
	red: u8,
	green: u8,
	blue: u8,
	alpha: f32,
}

impl TableRgba {
	pub const fn opaque(red: u8, green: u8, blue: u8) -> Self {
		Self {
			red,
			green,
			blue,
			alpha: 1.0,
		}
	}

	pub const fn translucent(red: u8, green: u8, blue: u8, alpha: f32) -> Self {
		Self {
			red,
			green,
			blue,
			alpha,
		}
	}

	pub const fn red(self) -> u8 {
		self.red
	}

	pub const fn green(self) -> u8 {
		self.green
	}

	pub const fn blue(self) -> u8 {
		self.blue
	}

	pub const fn alpha(self) -> f32 {
		self.alpha
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TableSkin {
	pub body_background: TableRgba,
	pub header_background: TableRgba,
	pub gutter_background: TableRgba,
	pub row_even_background: TableRgba,
	pub row_odd_background: TableRgba,
	pub gridline: TableRgba,
	pub cell_text: TableRgba,
	pub header_text: TableRgba,
	pub type_text: TableRgba,
	pub gutter_text: TableRgba,
	pub selection_fill: TableRgba,
	pub scrollbar_thumb: TableRgba,
}

impl TableSkin {
	pub const DEFAULT: Self = Self {
		body_background: TableRgba::opaque(1, 1, 1),
		header_background: TableRgba::opaque(4, 4, 4),
		gutter_background: TableRgba::opaque(4, 4, 4),
		row_even_background: TableRgba::opaque(20, 8, 38),
		row_odd_background: TableRgba::opaque(40, 20, 70),
		gridline: TableRgba::translucent(250, 250, 250, 0.1),
		cell_text: TableRgba::opaque(230, 230, 230),
		header_text: TableRgba::opaque(220, 220, 220),
		type_text: TableRgba::opaque(180, 150, 220),
		gutter_text: TableRgba::opaque(220, 220, 220),
		selection_fill: TableRgba::translucent(140, 0, 250, 0.25),
		scrollbar_thumb: TableRgba::translucent(140, 0, 250, 0.5),
	};

	/// Every production skin participates in the contrast assay. PRODUCT-0024
	/// owns stable authored names and may grow this catalog.
	pub const BUILT_INS: [Self; 1] = [Self::DEFAULT];
}

impl Default for TableSkin {
	fn default() -> Self {
		Self::DEFAULT
	}
}

#[cfg(test)]
fn visible_srgb(color: TableRgba) -> Srgb {
	Rgb::new(color.red, color.green, color.blue).as_srgb()
}

#[cfg(test)]
fn composite_over(foreground: TableRgba, background: Srgb) -> Srgb {
	let alpha = foreground.alpha as f64;
	let channel = |foreground: u8, background: f64| {
		foreground as f64 / 255.0 * alpha + background * (1.0 - alpha)
	};
	Srgb::new(
		channel(foreground.red, background.red()),
		channel(foreground.green, background.green()),
		channel(foreground.blue, background.blue()),
	)
	.expect("validated table colors composite into normalized sRGB")
}

#[cfg(test)]
fn text_contrast_ratio(text: TableRgba, background: Srgb) -> f64 {
	composite_over(text, background).contrast_ratio(background)
}

#[cfg(test)]
fn text_contrast_ratios(skin: TableSkin) -> [f64; 7] {
	let even = visible_srgb(skin.row_even_background);
	let odd = visible_srgb(skin.row_odd_background);
	let header = visible_srgb(skin.header_background);
	let gutter = visible_srgb(skin.gutter_background);
	[
		text_contrast_ratio(skin.cell_text, even),
		text_contrast_ratio(skin.cell_text, odd),
		text_contrast_ratio(skin.header_text, header),
		text_contrast_ratio(skin.type_text, header),
		text_contrast_ratio(skin.gutter_text, gutter),
		text_contrast_ratio(skin.cell_text, composite_over(skin.selection_fill, even)),
		text_contrast_ratio(skin.cell_text, composite_over(skin.selection_fill, odd)),
	]
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0023_every_builtin_meets_text_contrast_after_declared_alpha_compositing() {
		for skin in TableSkin::BUILT_INS {
			for ratio in text_contrast_ratios(skin) {
				assert!(
					ratio >= 4.5,
					"table text contrast {ratio:.3}:1 is below 4.5:1"
				);
			}
		}
	}

	#[test]
	fn product_0023_selection_compositing_consumes_the_skin_declared_alpha() {
		let opaque_selection = TableRgba::opaque(0, 0, 0);
		let skin = TableSkin {
			selection_fill: opaque_selection,
			..TableSkin::DEFAULT
		};
		let ratios = text_contrast_ratios(skin);
		let direct = text_contrast_ratio(skin.cell_text, visible_srgb(opaque_selection));
		assert_eq!(ratios[5], direct);
		assert_eq!(ratios[6], direct);

		let translucent = text_contrast_ratios(TableSkin::DEFAULT);
		assert_ne!(translucent[5], direct);
		assert_ne!(translucent[6], direct);
	}

	#[test]
	fn product_0024_fractional_composites_retain_product_0023_ratios_without_quantization() {
		let expected = [
			15.401817251018779,
			13.158388521894944,
			14.950831546071543,
			8.139026492916567,
			14.950831546071543,
			12.81745356023819,
			10.921309527435287,
		];
		for (actual, expected) in text_contrast_ratios(TableSkin::DEFAULT)
			.into_iter()
			.zip(expected)
		{
			assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
		}

		let even = visible_srgb(TableSkin::DEFAULT.row_even_background);
		let selected = composite_over(TableRgba::translucent(1, 2, 3, 0.3), even);
		assert_ne!(selected.red() * 255.0, (selected.red() * 255.0).round());
		assert_ne!(selected.green() * 255.0, (selected.green() * 255.0).round());
		assert_ne!(selected.blue() * 255.0, (selected.blue() * 255.0).round());
	}
}
