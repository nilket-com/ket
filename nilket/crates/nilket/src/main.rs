use std::process::ExitCode;

use nilket::local::{
	LocalInitOptions, LocalOptions, RequestedDecisionAuthority, init as init_local,
	run as run_local,
};

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
		Some("local") => {
			let args: Vec<String> = args.collect();
			if args.first().map(String::as_str) == Some("init") {
				match parse_local_init_options(args.into_iter().skip(1)) {
					Ok(options) => init_local(options),
					Err(message) => {
						eprintln!("{message}");
						ExitCode::from(2)
					}
				}
			} else {
				match parse_local_options(args) {
					Ok(options) => run_local(options),
					Err(message) => {
						eprintln!("{message}");
						ExitCode::from(2)
					}
				}
			}
		}
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

fn parse_local_init_options<I>(args: I) -> Result<LocalInitOptions, String>
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<_> = args.into_iter().collect();
	let mut data_dir = None;
	let mut decision_authority = None;
	let mut index = 0;
	while let Some(argument) = args.get(index) {
		match argument.as_str() {
			"--data-dir" => {
				if data_dir.is_some() {
					return Err(
						"nilket local init --data-dir may be specified only once".to_owned()
					);
				}
				let value = args
					.get(index + 1)
					.ok_or_else(|| "nilket local init --data-dir requires PATH".to_owned())?;
				data_dir = Some(value.into());
				index += 2;
			}
			"--decision-authority" => {
				if decision_authority.is_some() {
					return Err(
						"nilket local init --decision-authority may be specified only once"
							.to_owned(),
					);
				}
				let value = args.get(index + 1).ok_or_else(|| {
					"nilket local init --decision-authority requires today or parent-decisions"
						.to_owned()
				})?;
				decision_authority = Some(match value.as_str() {
					"today" => RequestedDecisionAuthority::Today,
					"parent-decisions" => RequestedDecisionAuthority::ParentDecisions,
					other => {
						return Err(format!(
							"nilket local init --decision-authority must be today or parent-decisions, not {other}"
						));
					}
				});
				index += 2;
			}
			flag if flag.starts_with('-') => {
				return Err(format!("unknown nilket local init option: {flag}"));
			}
			other => {
				return Err(format!(
					"nilket local init does not accept positional argument: {other}"
				));
			}
		}
	}
	Ok(LocalInitOptions {
		data_dir,
		decision_authority,
	})
}

fn print_root_usage(program: &str) {
	eprintln!(
		"Usage: {program} [COMMAND]\n\nWith no arguments, start the foreground local Nilket deployment.\n\nCommands:\n  local       Start the local Wendelon + Toron deployment\n  local init  Write the local profile's configs and development PKI, start nothing\n              (--decision-authority parent-decisions: a fresh single-node ParentDecisions profile)\n  wendelon    Start or manage a storage node\n  toron       Start or manage a compute node"
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

	#[test]
	fn local_init_options_take_a_data_dir_and_an_explicit_authority() {
		assert_eq!(
			parse_local_init_options(["--data-dir".to_owned(), "/tmp/x".to_owned()]).unwrap(),
			LocalInitOptions {
				data_dir: Some("/tmp/x".into()),
				decision_authority: None,
			}
		);
		assert_eq!(
			parse_local_init_options(Vec::<String>::new()).unwrap(),
			LocalInitOptions {
				data_dir: None,
				decision_authority: None,
			}
		);
		for (value, authority) in [
			("today", RequestedDecisionAuthority::Today),
			(
				"parent-decisions",
				RequestedDecisionAuthority::ParentDecisions,
			),
		] {
			assert_eq!(
				parse_local_init_options(["--decision-authority".to_owned(), value.to_owned()])
					.unwrap()
					.decision_authority,
				Some(authority)
			);
		}
		assert!(parse_local_init_options(["--decision-authority".to_owned()]).is_err());
		assert!(
			parse_local_init_options([
				"--decision-authority".to_owned(),
				"ParentDecisions".to_owned()
			])
			.is_err()
		);
		assert!(
			parse_local_init_options([
				"--decision-authority".to_owned(),
				"today".to_owned(),
				"--decision-authority".to_owned(),
				"parent-decisions".to_owned()
			])
			.is_err()
		);
		// `nilket local` (run) takes no authority: only init creates one.
		assert!(
			parse_local_options([
				"--decision-authority".to_owned(),
				"parent-decisions".to_owned()
			])
			.is_err()
		);
		// Replacing identities is `nilket local --reset`, never a side effect of init.
		assert!(parse_local_init_options(["--reset".to_owned()]).is_err());
		assert!(parse_local_init_options(["--data-dir".to_owned()]).is_err());
		assert!(
			parse_local_init_options([
				"--data-dir".to_owned(),
				"/a".to_owned(),
				"--data-dir".to_owned(),
				"/b".to_owned()
			])
			.is_err()
		);
		assert!(parse_local_init_options(["extra".to_owned()]).is_err());
	}
}
