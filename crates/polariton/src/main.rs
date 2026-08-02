mod adapters;
mod configuration;
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

fn main() -> gui::Result {
	let _ = rustls::crypto::ring::default_provider().install_default();
	let initial_file = initial_file_from_args(std::env::args());
	let startup_data = configuration::load_startup_data();
	gui::run(startup_data, initial_file)
}

/// Match Baryon's positional-file behavior without making Polariton's GUI
/// startup depend on the terminal frontend crate. Unknown flags remain
/// available for future Polariton options; `--` permits dash-prefixed paths.
fn initial_file_from_args<I>(args: I) -> Option<String>
where
	I: IntoIterator<Item = String>,
{
	let mut args = args.into_iter();
	let _program = args.next();
	let mut parse_flags = true;
	for arg in args {
		if parse_flags && arg == "--" {
			parse_flags = false;
		} else if !parse_flags || !arg.starts_with('-') {
			return Some(arg);
		}
	}
	None
}

#[cfg(test)]
mod tests {
	use super::initial_file_from_args;

	#[test]
	fn first_positional_argument_is_the_initial_file() {
		assert_eq!(
			initial_file_from_args([
				"polariton".to_owned(),
				"--future-option".to_owned(),
				"crates/polariton/src/persistence.rs".to_owned(),
				"ignored.rs".to_owned(),
			]),
			Some("crates/polariton/src/persistence.rs".to_owned())
		);
		assert_eq!(initial_file_from_args(["polariton".to_owned()]), None);
	}

	#[test]
	fn double_dash_allows_a_dash_prefixed_path() {
		assert_eq!(
			initial_file_from_args([
				"polariton".to_owned(),
				"--".to_owned(),
				"-scratch.sql".to_owned(),
			]),
			Some("-scratch.sql".to_owned())
		);
	}
}
