/// DejaVu Sans Mono font bytes — pass to `.font()` on your iced app builder
/// so the editor's whitespace glyphs (▸ ␣ ¬) render correctly.
pub const DEJAVU_SANS_MONO: &[u8] = include_bytes!("../../assets/fonts/DejaVuSansMono.ttf");

pub mod analysis;
pub mod buffer;
pub mod command;
pub mod coords;
pub mod folding;
pub mod highlight;
pub mod search;
pub mod theme;
pub mod undo;
pub mod widget;
pub mod wrap;

mod core;
pub mod vim;

#[allow(unused_imports)]
pub use command::EditorCommand;
#[allow(unused_imports)]
pub use core::{CodeEditor, EditorMsg};
#[allow(unused_imports)]
pub use vim::VimMode;
