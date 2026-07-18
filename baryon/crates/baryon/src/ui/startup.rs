use crate::app;
use libaryon::engine::settings;
use std::ffi::OsStr;
use std::io::{self, Write};

#[derive(Debug, PartialEq, Eq)]
pub enum StartupAction {
	PrintVersion,
	PrintHelp {
		program_name: String,
		topic: Option<String>,
	},
	Run {
		initial_file: Option<String>,
	},
}

pub fn run<I>(args: I) -> io::Result<()>
where
	I: IntoIterator<Item = String>,
{
	match parse_startup_args(args) {
		StartupAction::PrintVersion => {
			print_version(io::stdout())?;
			Ok(())
		}
		StartupAction::PrintHelp {
			program_name,
			topic,
		} => {
			print_help(io::stdout(), &program_name, topic.as_deref())?;
			Ok(())
		}
		StartupAction::Run { initial_file } => {
			app::run(settings::EngineSettings::default(), initial_file)
		}
	}
}

pub fn parse_startup_args<I>(args: I) -> StartupAction
where
	I: IntoIterator<Item = String>,
{
	let mut args = args.into_iter();
	let program_arg = args
		.next()
		.unwrap_or_else(|| env!("CARGO_PKG_NAME").to_string());
	let program_name = executable_name(&program_arg);
	let mut parse_flags = true;

	while let Some(arg) = args.next() {
		if parse_flags {
			match arg.as_str() {
				"--version" | "-v" => return StartupAction::PrintVersion,
				"--help" | "-h" => {
					let topic = args.next().filter(|topic| !topic.starts_with('-'));
					return StartupAction::PrintHelp {
						program_name,
						topic,
					};
				}
				"--" => {
					parse_flags = false;
					continue;
				}
				_ if arg.starts_with('-') => continue,
				_ => {
					return StartupAction::Run {
						initial_file: Some(arg),
					};
				}
			}
		} else {
			return StartupAction::Run {
				initial_file: Some(arg),
			};
		}
	}

	StartupAction::Run { initial_file: None }
}

fn executable_name(program_arg: &str) -> String {
	std::path::Path::new(program_arg)
		.file_name()
		.unwrap_or_else(|| OsStr::new(env!("CARGO_PKG_NAME")))
		.to_string_lossy()
		.into_owned()
}

fn print_version(mut out: impl Write) -> io::Result<()> {
	writeln!(
		out,
		"{} {}",
		env!("CARGO_PKG_NAME"),
		env!("CARGO_PKG_VERSION")
	)
}

fn print_help(mut out: impl Write, program_name: &str, topic: Option<&str>) -> io::Result<()> {
	match libaryon::engine::render_cli_help(topic.unwrap_or(""), program_name) {
		Ok(rendered) => write!(out, "{rendered}"),
		Err(message) => {
			writeln!(out, "{message}")?;
			writeln!(out, "{}", libaryon::engine::help_topic_hint())
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{StartupAction, parse_startup_args};

	#[test]
	fn version_flag_exits_before_app_start() {
		assert_eq!(
			parse_startup_args(["baryon".to_string(), "--version".to_string()]),
			StartupAction::PrintVersion
		);
		assert_eq!(
			parse_startup_args(["baryon".to_string(), "-v".to_string()]),
			StartupAction::PrintVersion
		);
	}

	#[test]
	fn help_flag_exits_before_app_start() {
		assert_eq!(
			parse_startup_args(["baryon".to_string(), "--help".to_string()]),
			StartupAction::PrintHelp {
				program_name: "baryon".to_string(),
				topic: None,
			}
		);
		assert_eq!(
			parse_startup_args(["/usr/bin/vi".to_string(), "-h".to_string()]),
			StartupAction::PrintHelp {
				program_name: "vi".to_string(),
				topic: None,
			}
		);
	}

	#[test]
	fn help_flag_takes_an_optional_topic() {
		assert_eq!(
			parse_startup_args([
				"baryon".to_string(),
				"--help".to_string(),
				"set".to_string(),
			]),
			StartupAction::PrintHelp {
				program_name: "baryon".to_string(),
				topic: Some("set".to_string()),
			}
		);
		assert_eq!(
			parse_startup_args([
				"baryon".to_string(),
				"-h".to_string(),
				"--version".to_string(),
			]),
			StartupAction::PrintHelp {
				program_name: "baryon".to_string(),
				topic: None,
			}
		);
	}

	#[test]
	fn first_non_flag_argument_is_used_as_initial_file() {
		assert_eq!(
			parse_startup_args([
				"baryon".to_string(),
				"--unknown".to_string(),
				"notes.txt".to_string(),
			]),
			StartupAction::Run {
				initial_file: Some("notes.txt".to_string()),
			}
		);
	}

	#[test]
	fn double_dash_allows_dash_prefixed_filenames() {
		assert_eq!(
			parse_startup_args([
				"baryon".to_string(),
				"--".to_string(),
				"-scratch".to_string(),
			]),
			StartupAction::Run {
				initial_file: Some("-scratch".to_string()),
			}
		);
	}
}
