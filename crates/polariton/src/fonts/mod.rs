mod core;

pub use core::MENU_FONT;

/// DejaVu Sans Mono font bytes used for Polariton's application and editor UI.
pub const DEJAVU_SANS_MONO: &[u8] = include_bytes!("../../assets/fonts/DejaVuSansMono.ttf");
