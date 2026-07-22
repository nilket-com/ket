pub(crate) mod colors;
mod components;
pub mod dashboard;
mod menu;
pub mod messages;
pub mod pane_grid;
pub mod plot_pane;
mod state;
pub mod statusbar;
pub mod table;

pub use state::Result;
pub use state::run;
