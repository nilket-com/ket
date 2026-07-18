use crate::ui::Frontend;
use crossterm::{
	cursor::SetCursorStyle,
	event::{DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture},
	execute,
	terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use libaryon::engine::settings::{EngineSettings, default_init_path};
use libaryon::engine::{EngineConfig, EngineHandle};
use ratatui::backend::CrosstermBackend;
use std::io;
use std::sync::Once;

pub(crate) fn run(settings: EngineSettings, initial_file: Option<String>) -> Result<(), io::Error> {
	install_terminal_panic_hook();

	let mut config: EngineConfig = settings.into();
	config.initial_file = initial_file;
	config.init_script = default_init_path();
	let mut engine = EngineHandle::spawn(config);
	let tx_cmd = engine.command_sender();
	let tx_input = engine.input_sender();
	let rx_view = engine
		.take_viewports()
		.expect("new engine handle owns its viewport receiver");

	// 3. Setup Frontend
	enable_raw_mode()?;
	let mut stdout = io::stdout();
	execute!(
		stdout,
		EnterAlternateScreen,
		EnableMouseCapture,
		EnableBracketedPaste
	)?;
	let backend = CrosstermBackend::new(stdout);
	let terminal = ratatui::Terminal::new(backend)?;

	let mut frontend = Frontend::new(terminal, tx_cmd, tx_input, rx_view);

	let result = frontend.run();

	// 4. Teardown
	disable_raw_mode()?;
	let mut term = frontend.release_terminal();
	execute!(
		term.backend_mut(),
		SetCursorStyle::DefaultUserShape,
		DisableBracketedPaste,
		DisableMouseCapture,
		LeaveAlternateScreen
	)?;
	term.show_cursor()?;
	engine.detach();

	result
}

fn install_terminal_panic_hook() {
	static INSTALL: Once = Once::new();
	INSTALL.call_once(|| {
		std::panic::set_hook(Box::new(|info| {
			restore_terminal_after_panic();

			eprintln!("baryon panicked: {}", panic_payload(info.payload()));
			if let Some(location) = info.location() {
				eprintln!(
					"at {}:{}:{}",
					location.file(),
					location.line(),
					location.column()
				);
			}
		}));
	});
}

fn restore_terminal_after_panic() {
	let _ = disable_raw_mode();
	let mut stdout = io::stdout();
	let _ = execute!(
		stdout,
		SetCursorStyle::DefaultUserShape,
		DisableBracketedPaste,
		DisableMouseCapture,
		LeaveAlternateScreen
	);
}

fn panic_payload(payload: &(dyn std::any::Any + Send)) -> String {
	if let Some(message) = payload.downcast_ref::<&str>() {
		(*message).to_string()
	} else if let Some(message) = payload.downcast_ref::<String>() {
		message.clone()
	} else {
		"<non-string panic payload>".to_string()
	}
}
