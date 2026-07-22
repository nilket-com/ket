mod app;
mod ui;

fn main() -> std::io::Result<()> {
	ui::startup::run(std::env::args())
}
