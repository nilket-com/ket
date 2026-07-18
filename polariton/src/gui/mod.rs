pub(crate) mod colors;
mod components;
pub mod dashboard;
mod menu;
pub mod messages;
pub mod pane_grid;
pub mod plot_pane;
pub mod statusbar;
mod state;
mod table;

pub use state::run;
pub use state::Result;
