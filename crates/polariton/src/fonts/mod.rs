mod core;

pub use core::MENU_FONT;

use iced::Font;

/// DejaVu Sans Mono font bytes used for Polariton's application and editor UI.
pub const DEJAVU_SANS_MONO: &[u8] = include_bytes!("../../assets/fonts/DejaVuSansMono.ttf");

/// Bundled, uniquely named proportional face for every plot canvas text
/// operation.
///
/// This is an OFL-1.1 Noto Sans Regular derivative whose name table is changed
/// only to `Polariton Plot Sans`: `Noto Sans` is commonly installed by the
/// host, so its original family name could not prove the bundled bytes won
/// font selection. The accompanying `NotoSans-OFL-1.1.txt` carries the font's
/// SIL Open Font License 1.1 notice. PRODUCT-0117 binds printable-ASCII
/// estimate constants to this exact face; non-ASCII glyphs may still use
/// cosmic-text fallback.
pub const POLARITON_PLOT_SANS_REGULAR: &[u8] =
	include_bytes!("../../assets/fonts/PolaritonPlotSans-Regular.ttf");

/// The explicit font carried by plot text instead of Iced's host-resolved
/// generic sans-serif default.
pub const PLOT_TEXT_FONT: Font = Font::with_name("Polariton Plot Sans");
