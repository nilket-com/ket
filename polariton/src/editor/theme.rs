use iced::Color;

#[derive(Debug, Clone)]
pub struct EditorTheme {
	pub background: Color,
	pub gutter_bg: Color,
	pub gutter_text: Color,
	pub gutter_active_text: Color,
	pub gutter_border: Color,

	pub cursor: Color,
	pub selection: Color,
	pub current_line_bg: Color,

	// Syntax (shared)
	pub keyword: Color,
	pub type_name: Color,
	pub string: Color,
	pub number: Color,
	pub comment: Color,
	pub operator: Color,
	pub punctuation: Color,
	pub identifier: Color,
	pub function: Color,
	pub plain: Color,

	// Rust-specific
	pub macro_color: Color,
	pub attribute: Color,
	pub lifetime: Color,

	// Bracket match
	pub bracket_match_bg: Color,
	pub bracket_match_border: Color,

	// Indent guides
	pub indent_guide: Color,
	pub indent_guide_active: Color,

	// Diagnostics
	pub error_underline: Color,
	pub error_gutter_marker: Color,

	// Tooltip
	pub tooltip_bg: Color,
	pub tooltip_border: Color,
	pub tooltip_text: Color,

	// Scrollbar
	pub scrollbar_track: Color,
	pub scrollbar_thumb: Color,
	pub scrollbar_thumb_hover: Color,

	// Search highlights
	pub search_match_bg: Color,
	pub search_current_bg: Color,
	pub search_panel_bg: Color,

	// Fold gutter
	pub fold_indicator: Color,
	pub fold_indicator_hover: Color,
	pub fold_collapsed_bg: Color,

	// Minimap
	pub minimap_bg: Color,
	pub minimap_viewport: Color,
	pub minimap_text: Color,

	// Status bar
	pub statusbar_bg: Color,
	pub statusbar_text: Color,
	pub statusbar_sep: Color,

	// Command bar (vim `:` mode)
	pub cmdbar_bg: Color,
	pub cmdbar_text: Color,

	// Tooltip drop shadow
	pub tooltip_shadow: Color,
}

/// Convenience: integer RGB + float alpha.
const fn rgba(r: u8, g: u8, b: u8, a: f32) -> Color {
	Color::from_rgba8(r, g, b, a)
}

impl EditorTheme {
	pub fn dark() -> Self {
		Self {
			background: Color::from_rgb8(2, 2, 2),
			gutter_bg: Color::from_rgb8(4, 4, 4),
			gutter_text: Color::from_rgb8(102, 107, 117),
			gutter_active_text: Color::from_rgb8(191, 199, 209),
			gutter_border: Color::from_rgb8(46, 48, 56),

			cursor: Color::from_rgb8(230, 235, 242),
			selection: rgba(66, 107, 173, 0.45),
			current_line_bg: rgba(255, 255, 255, 0.03),

			keyword: Color::from_rgb8(196, 140, 245),
			type_name: Color::from_rgb8(79, 201, 196),
			string: Color::from_rgb8(204, 230, 122),
			number: Color::from_rgb8(242, 173, 97),
			comment: Color::from_rgb8(107, 117, 128),
			operator: Color::from_rgb8(143, 199, 255),
			punctuation: Color::from_rgb8(153, 158, 168),
			identifier: Color::from_rgb8(230, 235, 242),
			function: Color::from_rgb8(97, 191, 255),
			plain: Color::from_rgb8(209, 214, 224),

			macro_color: Color::from_rgb8(242, 191, 102),
			attribute: Color::from_rgb8(173, 217, 115),
			lifetime: Color::from_rgb8(255, 153, 153),

			bracket_match_bg: rgba(102, 140, 204, 0.25),
			bracket_match_border: rgba(140, 179, 255, 0.60),

			indent_guide: rgba(255, 255, 255, 0.06),
			indent_guide_active: rgba(255, 255, 255, 0.12),

			error_underline: Color::from_rgb8(255, 89, 89),
			error_gutter_marker: Color::from_rgb8(255, 89, 89),

			tooltip_bg: Color::from_rgb8(41, 43, 51),
			tooltip_border: Color::from_rgb8(71, 77, 87),
			tooltip_text: Color::from_rgb8(217, 222, 230),

			scrollbar_track: rgba(255, 255, 255, 0.02),
			scrollbar_thumb: rgba(140, 0, 250, 0.5),
			scrollbar_thumb_hover: rgba(255, 255, 255, 0.18),

			search_match_bg: rgba(230, 191, 51, 0.30),
			search_current_bg: rgba(230, 191, 51, 0.65),
			search_panel_bg: Color::from_rgb8(36, 38, 46),

			fold_indicator: Color::from_rgb8(115, 122, 133),
			fold_indicator_hover: Color::from_rgb8(179, 186, 199),
			fold_collapsed_bg: rgba(102, 140, 204, 0.10),

			minimap_bg: Color::from_rgb8(3, 3, 3),
			minimap_viewport: rgba(255, 255, 255, 0.08),
			minimap_text: rgba(255, 255, 255, 0.25),

			statusbar_bg: Color::from_rgb8(10, 10, 10),
			statusbar_text: Color::from_rgb8(140, 148, 158),
			statusbar_sep: Color::from_rgb8(89, 94, 102),

			cmdbar_bg: Color::from_rgb8(28, 31, 41),
			cmdbar_text: Color::from_rgb8(230, 235, 242),

			tooltip_shadow: rgba(0, 0, 0, 0.25),
		}
	}

	pub fn light() -> Self {
		Self {
			background: Color::from_rgb8(250, 250, 252),
			gutter_bg: Color::from_rgb8(240, 242, 245),
			gutter_text: Color::from_rgb8(153, 158, 168),
			gutter_active_text: Color::from_rgb8(51, 56, 66),
			gutter_border: Color::from_rgb8(219, 224, 230),

			cursor: Color::from_rgb8(13, 13, 26),
			selection: rgba(66, 133, 245, 0.25),
			current_line_bg: rgba(0, 0, 0, 0.03),

			keyword: Color::from_rgb8(140, 38, 209),
			type_name: Color::from_rgb8(0, 140, 140),
			string: Color::from_rgb8(41, 140, 41),
			number: Color::from_rgb8(204, 115, 26),
			comment: Color::from_rgb8(140, 148, 158),
			operator: Color::from_rgb8(26, 89, 179),
			punctuation: Color::from_rgb8(102, 107, 117),
			identifier: Color::from_rgb8(26, 26, 38),
			function: Color::from_rgb8(26, 115, 191),
			plain: Color::from_rgb8(38, 38, 51),

			macro_color: Color::from_rgb8(166, 115, 13),
			attribute: Color::from_rgb8(77, 140, 38),
			lifetime: Color::from_rgb8(204, 77, 77),

			bracket_match_bg: rgba(51, 102, 204, 0.15),
			bracket_match_border: rgba(51, 102, 204, 0.50),

			indent_guide: rgba(0, 0, 0, 0.06),
			indent_guide_active: rgba(0, 0, 0, 0.14),

			error_underline: Color::from_rgb8(230, 38, 38),
			error_gutter_marker: Color::from_rgb8(230, 38, 38),

			tooltip_bg: Color::from_rgb8(245, 245, 247),
			tooltip_border: Color::from_rgb8(209, 214, 219),
			tooltip_text: Color::from_rgb8(38, 38, 51),

			scrollbar_track: rgba(0, 0, 0, 0.02),
			scrollbar_thumb: rgba(140, 0, 250, 0.5),
			scrollbar_thumb_hover: rgba(0, 0, 0, 0.22),

			search_match_bg: rgba(255, 217, 51, 0.30),
			search_current_bg: rgba(255, 217, 51, 0.60),
			search_panel_bg: Color::from_rgb8(235, 237, 240),

			fold_indicator: Color::from_rgb8(128, 133, 143),
			fold_indicator_hover: Color::from_rgb8(64, 71, 82),
			fold_collapsed_bg: rgba(51, 102, 204, 0.06),

			minimap_bg: Color::from_rgb8(240, 242, 245),
			minimap_viewport: rgba(0, 0, 0, 0.06),
			minimap_text: rgba(0, 0, 0, 0.20),

			statusbar_bg: Color::from_rgb8(225, 227, 230),
			statusbar_text: Color::from_rgb8(80, 85, 95),
			statusbar_sep: Color::from_rgb8(160, 165, 175),

			cmdbar_bg: Color::from_rgb8(210, 213, 218),
			cmdbar_text: Color::from_rgb8(20, 20, 30),

			tooltip_shadow: rgba(0, 0, 0, 0.15),
		}
	}
}
