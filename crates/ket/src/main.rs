use std::process::ExitCode;

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
	match ket::run(std::env::args_os()) {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			eprintln!("ket: {error}");
			ExitCode::from(2)
		}
	}
}

#[cfg(not(target_os = "linux"))]
fn main() -> ExitCode {
	eprintln!("ket: PRODUCT-0004 supports transparent process replacement on Linux only in v1");
	ExitCode::from(2)
}
