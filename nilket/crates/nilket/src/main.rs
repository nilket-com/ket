use std::process::ExitCode;

fn main() -> ExitCode {
	let mut args = std::env::args();
	let program = args.next().unwrap_or_else(|| "nilket".to_string());

	match args.next().as_deref() {
		Some("--version") | Some("-V") => {
			println!("nilket {}", env!("CARGO_PKG_VERSION"));
			ExitCode::SUCCESS
		}
		Some("--help") | Some("-h") => {
			print_root_usage(&program);
			ExitCode::SUCCESS
		}
		Some("wendelon") => wendelon::cli::run("nilket wendelon", args),
		Some("toron") => toron::cli::run("nilket toron", args),
		Some(other) => {
			eprintln!("unknown nilket command: {other}");
			print_root_usage(&program);
			ExitCode::from(2)
		}
		None => {
			print_root_usage(&program);
			ExitCode::from(2)
		}
	}
}

fn print_root_usage(program: &str) {
	eprintln!(
		"Usage: {program} <COMMAND>\n\nCommands:\n  wendelon    Start or manage a storage node\n  toron       Start or manage a compute node"
	);
}
