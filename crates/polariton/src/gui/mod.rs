pub(crate) mod colors;
mod components;
mod menu;
pub mod messages;
pub mod plot_pane;
mod state;
pub mod statusbar;
pub mod table;
pub mod table_skin;

pub use state::Result;
pub(crate) use state::StartupTarget;
pub use state::run;
