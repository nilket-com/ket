use std::process::ExitCode;

use nilket::local::{LocalOptions, run as run_local};

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
		Some("local") => match parse_local_options(args) {
			Ok(options) => run_local(options),
			Err(message) => {
				eprintln!("{message}");
				ExitCode::from(2)
			}
		},
		Some(other) => {
			eprintln!("unknown nilket command: {other}");
			print_root_usage(&program);
			ExitCode::from(2)
		}
		None => run_local(LocalOptions {
			data_dir: None,
			reset: false,
		}),
	}
}

fn parse_local_options<I>(args: I) -> Result<LocalOptions, String>
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<_> = args.into_iter().collect();
	let mut data_dir = None;
	let mut reset = false;
	let mut index = 0;
	while let Some(argument) = args.get(index) {
		match argument.as_str() {
			"--data-dir" => {
				if data_dir.is_some() {
					return Err("nilket local --data-dir may be specified only once".to_owned());
				}
				let value = args
					.get(index + 1)
					.ok_or_else(|| "nilket local --data-dir requires PATH".to_owned())?;
				data_dir = Some(value.into());
				index += 2;
			}
			"--reset" if !reset => {
				reset = true;
				index += 1;
			}
			"--reset" => return Err("nilket local --reset may be specified only once".to_owned()),
			flag if flag.starts_with('-') => {
				return Err(format!("unknown nilket local option: {flag}"));
			}
			other => {
				return Err(format!(
					"nilket local does not accept positional argument: {other}"
				));
			}
		}
	}
	Ok(LocalOptions { data_dir, reset })
}

fn print_root_usage(program: &str) {
	eprintln!(
		"Usage: {program} [COMMAND]\n\nWith no arguments, start the foreground local Nilket deployment.\n\nCommands:\n  local       Start the local Wendelon + Toron deployment\n  wendelon    Start or manage a storage node\n  toron       Start or manage a compute node"
	);
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn local_options_are_strict_and_root_role_names_stay_distinct() {
		assert_eq!(
			parse_local_options([
				"--reset".to_owned(),
				"--data-dir".to_owned(),
				"/tmp/x".to_owned()
			])
			.unwrap(),
			LocalOptions {
				data_dir: Some("/tmp/x".into()),
				reset: true,
			}
		);
		assert!(parse_local_options(["--data-dir".to_owned()]).is_err());
		assert!(parse_local_options(["toron".to_owned()]).is_err());
	}
}
