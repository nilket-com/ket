//! Complete, resolved color treatment for the interior of a table artifact.
//!
//! This model is deliberately independent of editor chrome and plot data ramps.

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
#[derive(Clone, Copy)]
struct VisibleRgb {
	red: f64,
	green: f64,
	blue: f64,
}

#[cfg(test)]
impl From<TableRgba> for VisibleRgb {
	fn from(color: TableRgba) -> Self {
		Self {
			red: color.red as f64 / 255.0,
			green: color.green as f64 / 255.0,
			blue: color.blue as f64 / 255.0,
		}
	}
}

#[cfg(test)]
fn composite_over(foreground: TableRgba, background: VisibleRgb) -> VisibleRgb {
	let alpha = foreground.alpha as f64;
	let channel = |foreground: u8, background: f64| {
		foreground as f64 / 255.0 * alpha + background * (1.0 - alpha)
	};
	VisibleRgb {
		red: channel(foreground.red, background.red),
		green: channel(foreground.green, background.green),
		blue: channel(foreground.blue, background.blue),
	}
}

#[cfg(test)]
fn relative_luminance(color: VisibleRgb) -> f64 {
	let linear = |value: f64| {
		if value <= 0.04045 {
			value / 12.92
		} else {
			((value + 0.055) / 1.055).powf(2.4)
		}
	};
	0.2126 * linear(color.red) + 0.7152 * linear(color.green) + 0.0722 * linear(color.blue)
}

#[cfg(test)]
fn visible_contrast_ratio(foreground: VisibleRgb, background: VisibleRgb) -> f64 {
	let foreground = relative_luminance(foreground);
	let background = relative_luminance(background);
	let (lighter, darker) = if foreground >= background {
		(foreground, background)
	} else {
		(background, foreground)
	};
	(lighter + 0.05) / (darker + 0.05)
}

#[cfg(test)]
fn text_contrast_ratio(text: TableRgba, background: VisibleRgb) -> f64 {
	visible_contrast_ratio(composite_over(text, background), background)
}

#[cfg(test)]
fn text_contrast_ratios(skin: TableSkin) -> [f64; 7] {
	let even = VisibleRgb::from(skin.row_even_background);
	let odd = VisibleRgb::from(skin.row_odd_background);
	let header = VisibleRgb::from(skin.header_background);
	let gutter = VisibleRgb::from(skin.gutter_background);
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
		let direct = text_contrast_ratio(skin.cell_text, VisibleRgb::from(opaque_selection));
		assert_eq!(ratios[5], direct);
		assert_eq!(ratios[6], direct);

		let translucent = text_contrast_ratios(TableSkin::DEFAULT);
		assert_ne!(translucent[5], direct);
		assert_ne!(translucent[6], direct);
	}
}
