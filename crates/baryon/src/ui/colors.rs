use ratatui::style::Color;

use libaryon::core::{BlendRatio, Rgb};

pub const fn ratatui_color(color: Rgb) -> Color {
	Color::Rgb(color.r, color.g, color.b)
}

fn blend_channel(base: u8, accent: u8, ratio: BlendRatio) -> u8 {
	let base_weight = u32::from(ratio.base());
	let accent_weight = u32::from(ratio.accent());
	let total_weight = u32::from(ratio.total());
	((u32::from(base) * base_weight + u32::from(accent) * accent_weight) / total_weight) as u8
}

pub(crate) fn blend_rgb(base: Rgb, accent: Rgb, ratio: BlendRatio) -> Rgb {
	Rgb::new(
		blend_channel(base.r, accent.r, ratio),
		blend_channel(base.g, accent.g, ratio),
		blend_channel(base.b, accent.b, ratio),
	)
}

pub const SELECTION_BG: Color = Color::Rgb(62, 68, 82);
pub const STATUS_BAR_INFO: Color = Color::Rgb(40, 90, 255);

pub const MODE_NORMAL_BG: Color = Color::Rgb(130, 170, 255);
pub const MODE_INSERT_BG: Color = Color::Rgb(180, 230, 130);
pub const MODE_COMMAND_BG: Color = Color::Rgb(255, 180, 100);
pub const MODE_SEARCH_BG: Color = Color::Rgb(200, 160, 255);
pub const MODE_CONFIRM_BG: Color = Color::Rgb(255, 120, 120);
pub const MODE_VISUAL_BG: Color = Color::Rgb(120, 200, 200);

pub const MODE_TEXT_FG: Color = Color::Rgb(0, 0, 0);

pub const DIRTY_FG: Color = Color::Rgb(255, 200, 120);
pub const DIRTY_BULLET_FG: Color = Color::Rgb(255, 160, 80);

pub const SEARCH_INFO_FG: Color = Color::Rgb(200, 160, 255);
pub const SEARCH_DIM_FG: Color = Color::Indexed(242);

pub const DIAGNOSTIC_ERROR_UNDERLINE: Color = Color::Rgb(255, 92, 92);
pub const FOLDED_PLACEHOLDER_BG: Color = Color::Rgb(4, 2, 8);

pub const TEXT_FG: Color = Color::Rgb(218, 218, 218);

// Non-configurable minimap density colors (RGBA for image crate).
pub const MINIMAP_RGBA_DENSITY: [u8; 4] = [110, 110, 110, 255];
pub const MINIMAP_RGBA_FALLBACK: [u8; 4] = [76, 76, 76, 255];

pub fn contrasting_text_color(background: Rgb) -> Color {
	let luminance = u32::from(background.r) * 299
		+ u32::from(background.g) * 587
		+ u32::from(background.b) * 114;
	if luminance >= 128_000 {
		Color::Rgb(0, 0, 0)
	} else {
		Color::Rgb(255, 255, 255)
	}
}

pub const SYNTAX_WHITESPACE: Color = Color::Rgb(76, 86, 106);

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn contrasting_text_color_keeps_custom_highlights_readable() {
		assert_eq!(
			contrasting_text_color(Rgb::new(76, 255, 0)),
			Color::Rgb(0, 0, 0)
		);
		assert_eq!(
			contrasting_text_color(Rgb::new(12, 24, 36)),
			Color::Rgb(255, 255, 255)
		);
	}

	#[test]
	fn rgb_blending_respects_validated_ratio() {
		assert_eq!(
			blend_rgb(
				Rgb::new(10, 20, 30),
				Rgb::new(110, 120, 130),
				BlendRatio::new(1, 4),
			),
			Rgb::new(35, 45, 55)
		);
	}

	#[test]
	fn diff_surfaces_do_not_reintroduce_literal_semantic_colors() {
		let draw = include_str!("draw.rs");
		let bridge = include_str!("diff.rs");
		let draw_diff = draw
			.split_once("if let Some(diff_pair) = &pane.diff_pair {")
			.and_then(|(_, tail)| tail.split_once("let mut x = text_left;"))
			.map(|(region, _)| region)
			.expect("draw diff region");
		for forbidden in ["Color::Rgb(", "Rgb::new(", "Rgba(["] {
			assert!(
				!draw_diff.contains(forbidden),
				"draw diff colors must come from UiPalette, found {forbidden}"
			);
		}

		let bridge_diff = bridge
			.split_once("for block in &request.blocks {")
			.and_then(|(_, tail)| tail.split_once("let left_y_range ="))
			.map(|(region, _)| region)
			.expect("bridge diff region");
		assert!(!bridge_diff.contains("Rgb::new("));
		assert!(!bridge_diff.contains("Color::Rgb("));
		for line in bridge_diff.lines().filter(|line| line.contains("Rgba([")) {
			assert!(
				line.contains("color.r") && line.contains("color.g") && line.contains("color.b"),
				"bridge RGBA must be constructed from the resolved palette: {line}"
			);
		}
	}
}
