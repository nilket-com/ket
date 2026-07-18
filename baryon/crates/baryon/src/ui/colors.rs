use ratatui::style::Color;

use libaryon::core::Rgb;

pub const fn ratatui_color(color: Rgb) -> Color {
	Color::Rgb(color.r, color.g, color.b)
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
}
