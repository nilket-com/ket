use std::process::ExitCode;

#[cfg(unix)]
fn main() -> ExitCode {
	match ket::run(std::env::args_os()) {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			eprintln!("ket: {error}");
			ExitCode::from(2)
		}
	}
}

#[cfg(not(unix))]
fn main() -> ExitCode {
	eprintln!(
		"ket: PRODUCT-0004 supports transparent process replacement on Unix (Linux and macOS) only in v1"
	);
	ExitCode::from(2)
}
