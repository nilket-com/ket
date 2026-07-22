use iced::Color;

pub const BG_PRIMARY: Color = rgb(1, 1, 1);
pub const BG_SECONDARY: Color = rgb(4, 4, 4);
pub const BG_INPUT: Color = rgb(4, 4, 8);
pub const BG_INPUT_HOVER: Color = rgb(10, 8, 16);
pub const BG_INPUT_FOCUS: Color = rgb(8, 4, 10);
pub const BG_BUTTON: Color = rgb(8, 8, 16);
pub const BG_BUTTON_HOVER: Color = rgb(16, 16, 32);
pub const BG_MODAL: Color = rgba(2, 2, 2, 0.96);

pub const BORDER_PRIMARY: Color = rgb(60, 8, 100);
pub const BORDER_SUBTLE: Color = rgb(40, 40, 50);
pub const BORDER_DIM: Color = rgb(24, 2, 32);
pub const BORDER_ACCENT: Color = rgb(110, 10, 240);
pub const BORDER_HOVER: Color = rgb(80, 8, 140);

pub const TEXT_PRIMARY: Color = rgb(230, 230, 230);
pub const TEXT_SECONDARY: Color = rgb(200, 180, 200);
pub const TEXT_PLACEHOLDER: Color = rgb(80, 70, 80);
pub const TEXT_PLACEHOLDER_HOVER: Color = rgb(100, 90, 100);
pub const TEXT_TITLE_BUTTON: Color = rgb(120, 120, 120);
pub const TEXT_TITLE_BUTTON_HOVER: Color = Color::WHITE;
pub const BRAND_PURPLE: Color = rgb(150, 4, 250);

pub const DANGER: Color = rgb(200, 40, 40);
pub const PRIMARY: Color = rgb(110, 10, 240);
pub const SELECTION: Color = rgb(110, 10, 240);
pub const SUCCESS: Color = rgb(40, 200, 40);
pub const WARNING: Color = rgb(200, 80, 80);

pub const TABLE_ROW_EVEN: Color = rgb(20, 8, 38);
pub const TABLE_ROW_ODD: Color = rgb(40, 20, 70);
pub const TABLE_BORDER: Color = rgba(250, 250, 250, 0.1);
pub const TABLE_TEXT_HEADER: Color = rgb(220, 220, 220);
pub const TABLE_TYPE_LABEL: Color = rgb(180, 150, 220);
pub const TABLE_SELECTION: Color = rgba(140, 0, 250, 0.25);
pub const SCROLLBAR_THUMB: Color = rgba(140, 0, 250, 0.5);
pub const STATUS_BAR_RAIL_BACKGROUND: Color = rgb(10, 10, 12);
pub const STATUS_BAR_RAIL_SEPARATOR: Color = BORDER_DIM;
pub const STATUS_BAR_SEGMENT_BACKGROUND: Color = BG_BUTTON;
pub const STATUS_BAR_SEGMENT_BORDER: Color = BORDER_DIM;
pub const STATUS_BAR_TEXT: Color = rgb(200, 190, 210);
pub const STATUS_BAR_TEXT_ACCENT: Color = rgb(200, 190, 210);
pub const STATUS_BAR_TEXT_SUCCESS: Color = SUCCESS;
pub const STATUS_BAR_TEXT_WARNING: Color = WARNING;
pub const STATUS_BAR_TEXT_DANGER: Color = DANGER;
pub const PROGRESS_BAR_TRACK_BACKGROUND: Color = BG_PRIMARY;
pub const PROGRESS_BAR_FILL: Color = BRAND_PURPLE;

pub const DROP_OVERLAY: Color = rgba(60, 8, 100, 0.25);
pub const HANDLE_ARROW: Color = rgb(150, 4, 250);
pub const HANDLE_ARROW_HOVER: Color = rgb(180, 100, 250);
pub const HANDLE_BG: Color = rgb(8, 2, 10);
pub const HANDLE_BORDER: Color = rgba(60, 8, 100, 0.4);
pub const HANDLE_DOT_BOTTOM: Color = rgb(0, 0, 0);
pub const HANDLE_DOT_TOP: Color = rgb(150, 4, 250);

pub const WHITE: Color = Color::WHITE;

const fn rgb(r: u8, g: u8, b: u8) -> Color {
	Color::from_rgb8(r, g, b)
}

const fn rgba(r: u8, g: u8, b: u8, a: f32) -> Color {
	Color::from_rgba8(r, g, b, a)
}
