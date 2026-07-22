use iced::Color;

use crate::gui::colors;
use crate::gui::pane_grid::handle;

#[derive(Debug, Clone, Copy)]
pub struct HandleStyle {
	pub dot_top_color: Color,
	pub dot_bottom_color: Color,
	pub background: Color,
	pub border_color: Color,
	pub arrow_color: Color,
	pub arrow_hover_color: Color,
	pub thickness: f32,
}

impl Default for HandleStyle {
	fn default() -> Self {
		Self {
			dot_top_color: colors::HANDLE_DOT_TOP,
			dot_bottom_color: colors::HANDLE_DOT_BOTTOM,
			background: colors::HANDLE_BG,
			border_color: colors::HANDLE_BORDER,
			arrow_color: colors::HANDLE_ARROW,
			arrow_hover_color: colors::HANDLE_ARROW_HOVER,
			thickness: handle::STRIP_THICKNESS,
		}
	}
}

#[derive(Debug, Clone, Copy)]
pub struct PaneStyle {
	pub background: Color,
	pub border_color: Color,
	pub border_width: f32,
}

impl Default for PaneStyle {
	fn default() -> Self {
		Self {
			background: colors::BG_PRIMARY,
			border_color: colors::BORDER_SUBTLE,
			border_width: 1.0,
		}
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChromeVisibility {
	#[default]
	Always,
	OnHover,
}

#[derive(Debug, Clone, Copy)]
pub struct Style {
	pub handle: HandleStyle,
	pub pane: PaneStyle,
	pub drop_overlay_color: Color,
	pub chrome: ChromeVisibility,
}

impl Default for Style {
	fn default() -> Self {
		Self {
			handle: HandleStyle::default(),
			pane: PaneStyle::default(),
			drop_overlay_color: colors::DROP_OVERLAY,
			chrome: ChromeVisibility::default(),
		}
	}
}
