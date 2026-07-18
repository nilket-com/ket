mod adapters;
mod core;
mod editor;
mod fonts;
mod gui;
mod persistence;
mod plot;

fn main() -> gui::Result {
	let _ = rustls::crypto::ring::default_provider().install_default();
	let startup_data = tokio::runtime::Runtime::new()
		.map(|rt| rt.block_on(persistence::load_startup_data()))
		.unwrap_or_default();
	gui::run(startup_data)
}
