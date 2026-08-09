mod adapters;
mod configuration;
mod coordinate;
mod core;
mod editor;
mod fonts;
mod gui;
#[cfg(feature = "toron")]
mod local_workspace;
mod plot;
mod result_registry;
mod rune_host;
#[cfg(feature = "toron")]
mod rune_relation_envelope;
#[cfg(feature = "toron")]
mod session_persistence;

fn main() -> gui::Result {
	let _ = rustls::crypto::ring::default_provider().install_default();
	let startup_target = startup_target_from_args(std::env::args())
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
	let startup_data = configuration::load_startup_data();
	gui::run(startup_data, startup_target)
}

/// Match Baryon's positional-file behavior without making Polariton's GUI
/// startup depend on the terminal frontend crate. Unknown flags remain
/// available for future Polariton options; `--` permits dash-prefixed paths.
fn startup_target_from_args<I>(args: I) -> Result<gui::StartupTarget, String>
where
	I: IntoIterator<Item = String>,
{
	let mut args = args.into_iter().peekable();
	let _program = args.next();
	let mut parse_flags = true;
	let mut initial_file = None;
	let mut session = None;
	while let Some(arg) = args.next() {
		if parse_flags && arg == "--" {
			parse_flags = false;
		} else if parse_flags && arg == "--session" {
			if session.is_some() {
				return Err("--session may occur at most once".to_owned());
			}
			let value = args
				.next()
				.ok_or_else(|| "--session requires `new` or a session id".to_owned())?;
			session = Some(if value == "new" {
				gui::StartupTarget::CreateSaved
			} else {
				gui::StartupTarget::OpenSaved(
					value
						.parse()
						.map_err(|error| format!("invalid --session value {value:?}: {error}"))?,
				)
			});
		} else if (!parse_flags || !arg.starts_with('-')) && initial_file.is_none() {
			initial_file = Some(arg);
		}
	}
	if session.is_some() && initial_file.is_some() {
		return Err("--session and an initial file are mutually exclusive".to_owned());
	}
	Ok(session.unwrap_or(gui::StartupTarget::Ephemeral { initial_file }))
}

#[cfg(test)]
mod tests {
	use super::{gui::StartupTarget, startup_target_from_args};

	#[test]
	fn first_positional_argument_is_the_initial_file() {
		assert_eq!(
			startup_target_from_args([
				"polariton".to_owned(),
				"--future-option".to_owned(),
				"crates/polariton/src/persistence.rs".to_owned(),
				"ignored.rs".to_owned(),
			])
			.unwrap(),
			StartupTarget::Ephemeral {
				initial_file: Some("crates/polariton/src/persistence.rs".to_owned())
			}
		);
		assert_eq!(
			startup_target_from_args(["polariton".to_owned()]).unwrap(),
			StartupTarget::Ephemeral { initial_file: None }
		);
	}

	#[test]
	fn double_dash_allows_a_dash_prefixed_path() {
		assert_eq!(
			startup_target_from_args([
				"polariton".to_owned(),
				"--".to_owned(),
				"-scratch.sql".to_owned(),
			])
			.unwrap(),
			StartupTarget::Ephemeral {
				initial_file: Some("-scratch.sql".to_owned())
			}
		);
		assert_eq!(
			startup_target_from_args([
				"polariton".to_owned(),
				"--".to_owned(),
				"--session".to_owned(),
				"new".to_owned(),
			])
			.unwrap(),
			StartupTarget::Ephemeral {
				initial_file: Some("--session".to_owned())
			}
		);
	}

	#[test]
	fn saved_session_selector_is_exact_and_exclusive() {
		let id = "1234567890abcdef1234567890abcdef";
		assert_eq!(
			startup_target_from_args([
				"polariton".to_owned(),
				"--session".to_owned(),
				"new".to_owned(),
			])
			.unwrap(),
			StartupTarget::CreateSaved
		);
		assert_eq!(
			startup_target_from_args([
				"polariton".to_owned(),
				"--session".to_owned(),
				id.to_owned(),
			])
			.unwrap(),
			StartupTarget::OpenSaved(id.parse().unwrap())
		);
		for (args, expected) in [
			(
				vec!["polariton", "--session"],
				"--session requires `new` or a session id",
			),
			(
				vec!["polariton", "--session", "1234"],
				"invalid --session value \"1234\"",
			),
			(
				vec!["polariton", "--session", "1234567890ABCDEF1234567890ABCDEF"],
				"invalid --session value \"1234567890ABCDEF1234567890ABCDEF\"",
			),
			(
				vec!["polariton", "--session", "1234567890abcdef1234567890abcdeg"],
				"invalid --session value \"1234567890abcdef1234567890abcdeg\"",
			),
			(
				vec!["polariton", "--session", "00000000000000000000000000000000"],
				"invalid --session value \"00000000000000000000000000000000\"",
			),
			(
				vec!["polariton", "--session", "new", "buffer.rn"],
				"--session and an initial file are mutually exclusive",
			),
			(
				vec!["polariton", "buffer.rn", "--session", "new"],
				"--session and an initial file are mutually exclusive",
			),
			(
				vec!["polariton", "--session", "new", "--session", id],
				"--session may occur at most once",
			),
		] {
			let error = startup_target_from_args(args.into_iter().map(str::to_owned)).unwrap_err();
			assert!(
				error.contains(expected),
				"invalid selector must refuse specifically: {error}"
			);
		}
	}
}
