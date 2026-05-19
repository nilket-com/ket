use std::process::ExitCode;

fn main() -> ExitCode {
	wendelon::cli::run("wendelon", std::env::args().skip(1))
}
