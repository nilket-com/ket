mod adapters;
mod configuration;
mod coordinate;
mod core;
mod driver;
mod editor;
mod fonts;
mod gui;
#[cfg(unix)]
mod live;
#[cfg(feature = "toron")]
mod local_workspace;
mod plot;
mod presentation_registry;
mod profile_store;
mod publication;
mod result_artifact;
mod result_registry;
mod rune_host;
#[cfg(feature = "toron")]
mod rune_presentation;
#[cfg(feature = "toron")]
mod rune_relation_envelope;
mod rune_view;
#[cfg(feature = "toron")]
mod rune_view_compiler;
mod selection;
#[cfg(feature = "toron")]
mod session_persistence;
#[cfg(feature = "toron")]
mod settings_writeback;

fn ensure_rustls_provider() {
	if rustls::crypto::CryptoProvider::get_default().is_none() {
		rustls::crypto::ring::default_provider()
			.install_default()
			.expect("install rustls crypto provider before GUI startup");
	}
}

fn main() -> gui::Result {
	let action = action_from_args(std::env::args())
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
	match action {
		ApplicationAction::ListSessions => list_saved_sessions(),
		#[cfg(unix)]
		ApplicationAction::Windows => live::client::windows().map_err(Into::into),
		#[cfg(unix)]
		ApplicationAction::Observe(selector) => live::client::observe(&selector).map_err(Into::into),
		ApplicationAction::GuiAgents(startup_target) => {
			ensure_rustls_provider();
			gui::run(configuration::load_startup_data(), startup_target, true)
		}
		ApplicationAction::Serve { root, bind } => {
			publication::serve(root, bind).map_err(|error| error.into())
		}
		ApplicationAction::Drive {
			source,
			commands,
			receipt,
		} => {
			ensure_rustls_provider();
			driver::drive(&source, &commands, &receipt).map_err(|error| error.into())
		}
		ApplicationAction::Describe { target } => {
			ensure_rustls_provider();
			driver::describe(&target).map_err(|error| error.into())
		}
		ApplicationAction::Gui(startup_target) => {
			ensure_rustls_provider();
			let startup_data = configuration::load_startup_data();
			gui::run(startup_data, startup_target, false)
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ApplicationAction {
	Gui(gui::StartupTarget),
	GuiAgents(gui::StartupTarget),
	#[cfg(unix)]
	Windows,
	#[cfg(unix)]
	Observe(live::discovery::Selector),
	ListSessions,
	/// PRODUCT-0154 §9: `polariton serve <package> [--bind host:port]`.
	Serve {
		root: std::path::PathBuf,
		bind: std::net::SocketAddr,
	},
	/// PRODUCT-0158 §6: `polariton drive <source.rn> --commands <file> --receipt <file>`.
	Drive {
		source: std::path::PathBuf,
		commands: std::path::PathBuf,
		receipt: std::path::PathBuf,
	},
	/// PRODUCT-0158 §2: `polariton describe <source.rn | --session id>`, a
	/// receipt on stdout.
	Describe {
		target: driver::Target,
	},
}

/// Match Baryon's positional-file behavior without making Polariton's GUI
/// startup depend on the terminal frontend crate. Unknown flags remain
/// available for future Polariton options; `--` permits dash-prefixed paths.
fn action_from_args<I>(args: I) -> Result<ApplicationAction, String>
where
	I: IntoIterator<Item = String>,
{
	let mut args = args.into_iter().peekable();
	let _program = args.next();
	#[cfg(unix)]
	if args.peek().is_some_and(|arg| arg == "windows") {
		args.next();
		if args.next().is_some() {
			return Err("windows takes no arguments".into());
		}
		return Ok(ApplicationAction::Windows);
	}
	#[cfg(unix)]
	if args.peek().is_some_and(|arg| arg == "observe") {
		args.next();
		return live::client::selector(args).map(ApplicationAction::Observe);
	}
	if args.peek().is_some_and(|arg| arg == "serve") {
		args.next();
		return serve_action(args);
	}
	if args.peek().is_some_and(|arg| arg == "drive") {
		args.next();
		return drive_action(args);
	}
	if args.peek().is_some_and(|arg| arg == "describe") {
		args.next();
		return describe_action(args);
	}
	let mut accepting_agents = false;
	let mut parse_flags = true;
	let mut initial_file = None;
	let mut session = None;
	while let Some(arg) = args.next() {
		if parse_flags && arg == "--" {
			parse_flags = false;
		} else if parse_flags && arg == "--agents" {
			accepting_agents = true;
		} else if parse_flags && arg == "--session" {
			if session.is_some() {
				return Err("--session may occur at most once".to_owned());
			}
			let value = args
				.next()
				.ok_or_else(|| "--session requires `new`, `list`, or a session id".to_owned())?;
			session = Some(if value == "new" {
				ApplicationAction::Gui(gui::StartupTarget::CreateSaved)
			} else if value == "list" {
				ApplicationAction::ListSessions
			} else {
				ApplicationAction::Gui(gui::StartupTarget::OpenSaved(
					value
						.parse()
						.map_err(|error| format!("invalid --session value {value:?}: {error}"))?,
				))
			});
		} else if (!parse_flags || !arg.starts_with('-')) && initial_file.is_none() {
			initial_file = Some(arg);
		}
	}
	if session.is_some() && initial_file.is_some() {
		return Err("--session and an initial file are mutually exclusive".to_owned());
	}
	let action = session.unwrap_or(ApplicationAction::Gui(gui::StartupTarget::Ephemeral {
		initial_file,
	}));
	if accepting_agents {
		match action {
			ApplicationAction::Gui(target) => Ok(ApplicationAction::GuiAgents(target)),
			_ => Err("--agents requires a window".into()),
		}
	} else {
		Ok(action)
	}
}

#[cfg(feature = "toron")]
fn list_saved_sessions() -> gui::Result {
	use libaryon::core::SessionStore;

	let report = SessionStore::platform_default()?.discover()?;
	print!("{}", render_session_discovery(&report));
	Ok(())
}

#[cfg(not(feature = "toron"))]
fn list_saved_sessions() -> gui::Result {
	Err("saved sessions require Polariton's `toron` feature".into())
}

#[cfg(feature = "toron")]
fn render_session_discovery(report: &libaryon::core::SessionDiscoveryReport) -> String {
	use chrono::{DateTime, SecondsFormat, Utc};
	use libaryon::core::SessionDiscoveryEntry;
	use std::os::unix::ffi::OsStrExt;

	if report.is_empty() {
		return "No saved sessions.\n".to_owned();
	}

	let mut active = Vec::new();
	let mut committed = Vec::new();
	let mut failed = Vec::new();
	for entry in report.entries() {
		match entry {
			SessionDiscoveryEntry::Active { .. } => active.push(entry),
			SessionDiscoveryEntry::Committed { .. } => committed.push(entry),
			SessionDiscoveryEntry::Damaged { .. } | SessionDiscoveryEntry::Unreadable { .. } => {
				failed.push(entry)
			}
		}
	}
	active.sort_by_key(|entry| entry.session_id());
	committed.sort_by(|left, right| compare_committed_entries(left, right));
	failed.sort_by_key(|entry| entry.session_id());

	let mut output = String::from("STATE      UPDATED_UTC           GEN  DOCS  SESSION\n");
	for entry in active.into_iter().chain(committed).chain(failed) {
		match entry {
			SessionDiscoveryEntry::Active { session_id } => {
				output.push_str(&format!(
					"active     -                       -     ?  {session_id}\n"
				));
			}
			SessionDiscoveryEntry::Committed {
				session_id,
				generation,
				modified,
				documents,
			} => {
				let updated = modified
					.map(|value| {
						DateTime::<Utc>::from(value).to_rfc3339_opts(SecondsFormat::Secs, true)
					})
					.unwrap_or_else(|| "unknown".to_owned());
				output.push_str(&format!(
					"committed  {updated:<20}  {generation:>3}  {:>4}  {session_id}\n",
					documents.len()
				));
				for document in documents {
					let path = match document.path() {
						None => "<scratch>".to_owned(),
						Some(path) => {
							let bytes = path.as_os_str().as_bytes();
							match path.to_str() {
								Some(value)
									if value != "<scratch>"
										&& !value.chars().any(char::is_control) =>
								{
									value.to_owned()
								}
								_ => format!("hex:{}", encode_cli_hex(bytes)),
							}
						}
					};
					output.push_str(&format!("  document {}  {path}\n", document.id()));
				}
			}
			SessionDiscoveryEntry::Damaged { session_id, reason } => {
				output.push_str(&format!(
					"damaged    -                       -     ?  {session_id}\n"
				));
				output.push_str(&format!("  corrupt saved session: {reason}\n"));
			}
			SessionDiscoveryEntry::Unreadable { session_id, reason } => {
				output.push_str(&format!(
					"unreadable -                       -     ?  {session_id}\n"
				));
				output.push_str(&format!("  unreadable saved session: {reason}\n"));
			}
		}
	}

	let mut foreign = report.foreign_entries().iter().collect::<Vec<_>>();
	foreign.sort_by(|left, right| left.name_bytes().cmp(right.name_bytes()));
	for entry in foreign {
		let name = match std::str::from_utf8(entry.name_bytes()) {
			Ok(name) => format!("{name:?}"),
			Err(_) => format!("hex:{}", encode_cli_hex(entry.name_bytes())),
		};
		output.push_str(&format!(
			"foreign    -                       -     ?  {name}\n"
		));
		output.push_str("  foreign session-root entry; not traversed\n");
	}
	output
}

#[cfg(feature = "toron")]
fn compare_committed_entries(
	left: &libaryon::core::SessionDiscoveryEntry,
	right: &libaryon::core::SessionDiscoveryEntry,
) -> std::cmp::Ordering {
	use libaryon::core::SessionDiscoveryEntry;

	match (left, right) {
		(
			SessionDiscoveryEntry::Committed {
				modified: left_modified,
				generation: left_generation,
				session_id: left_id,
				..
			},
			SessionDiscoveryEntry::Committed {
				modified: right_modified,
				generation: right_generation,
				session_id: right_id,
				..
			},
		) => right_modified
			.cmp(left_modified)
			.then_with(|| right_generation.cmp(left_generation))
			.then_with(|| left_id.cmp(right_id)),
		_ => std::cmp::Ordering::Equal,
	}
}

#[cfg(feature = "toron")]
fn encode_cli_hex(bytes: &[u8]) -> String {
	const DIGITS: &[u8; 16] = b"0123456789abcdef";
	let mut output = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		output.push(char::from(DIGITS[(byte >> 4) as usize]));
		output.push(char::from(DIGITS[(byte & 0x0f) as usize]));
	}
	output
}

#[cfg(test)]
mod tests {
	use super::{ApplicationAction, action_from_args, gui::StartupTarget};

	fn startup_target_from_args<I>(args: I) -> Result<StartupTarget, String>
	where
		I: IntoIterator<Item = String>,
	{
		match action_from_args(args)? {
			ApplicationAction::Gui(target) => Ok(target),
			_ => Err("expected GUI target".to_owned()),
		}
	}

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
			action_from_args([
				"polariton".to_owned(),
				"--session".to_owned(),
				"new".to_owned(),
			])
			.unwrap(),
			ApplicationAction::Gui(StartupTarget::CreateSaved)
		);
		assert_eq!(
			action_from_args([
				"polariton".to_owned(),
				"--session".to_owned(),
				id.to_owned(),
			])
			.unwrap(),
			ApplicationAction::Gui(StartupTarget::OpenSaved(id.parse().unwrap()))
		);
		assert_eq!(
			action_from_args([
				"polariton".to_owned(),
				"--session".to_owned(),
				"list".to_owned(),
			])
			.unwrap(),
			ApplicationAction::ListSessions
		);
		for (args, expected) in [
			(
				vec!["polariton", "--session"],
				"--session requires `new`, `list`, or a session id",
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
				vec!["polariton", "--session", "list", "buffer.rn"],
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
			let error = action_from_args(args.into_iter().map(str::to_owned)).unwrap_err();
			assert!(
				error.contains(expected),
				"invalid selector must refuse specifically: {error}"
			);
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn committed_session_order_is_newest_then_generation_then_identity() {
		use libaryon::core::SessionDiscoveryEntry;
		use std::time::{Duration, UNIX_EPOCH};

		fn committed(
			id: &str,
			generation: u64,
			modified_seconds: Option<u64>,
		) -> SessionDiscoveryEntry {
			SessionDiscoveryEntry::Committed {
				session_id: id.parse().unwrap(),
				generation,
				modified: modified_seconds.map(|seconds| UNIX_EPOCH + Duration::from_secs(seconds)),
				documents: Vec::new(),
			}
		}

		let newest = committed("11111111111111111111111111111111", 1, Some(200));
		let high_generation = committed("22222222222222222222222222222222", 9, Some(100));
		let low_identity = committed("33333333333333333333333333333333", 8, Some(100));
		let high_identity = committed("44444444444444444444444444444444", 8, Some(100));
		let unknown = committed("55555555555555555555555555555555", 99, None);
		let mut entries = vec![
			&unknown,
			&high_identity,
			&low_identity,
			&high_generation,
			&newest,
		];
		entries.sort_by(|left, right| super::compare_committed_entries(left, right));
		assert_eq!(
			entries
				.into_iter()
				.map(SessionDiscoveryEntry::session_id)
				.map(|id| id.to_string())
				.collect::<Vec<_>>(),
			vec![
				"11111111111111111111111111111111",
				"22222222222222222222222222222222",
				"33333333333333333333333333333333",
				"44444444444444444444444444444444",
				"55555555555555555555555555555555",
			]
		);
	}

	#[cfg(not(feature = "toron"))]
	#[test]
	fn listing_refuses_explicitly_without_saved_session_support() {
		assert_eq!(
			super::list_saved_sessions().unwrap_err().to_string(),
			"saved sessions require Polariton's `toron` feature"
		);
	}

	/// PRODUCT-0158 §6: `drive` takes a source and both files, nothing else.
	#[test]
	fn product_0158_drive_requires_source_commands_and_receipt() {
		let action = action_from_args(
			[
				"polariton",
				"drive",
				"uat/plot_fixtures_showcase.rn",
				"--commands",
				"uat/plot_fixtures_showcase.commands.json",
				"--receipt",
				"/tmp/receipt.json",
			]
			.map(str::to_owned),
		)
		.unwrap();
		assert_eq!(
			action,
			ApplicationAction::Drive {
				source: "uat/plot_fixtures_showcase.rn".into(),
				commands: "uat/plot_fixtures_showcase.commands.json".into(),
				receipt: "/tmp/receipt.json".into(),
			}
		);
		for (arguments, message) in [
			(
				vec!["drive", "--commands", "c.json", "--receipt", "r.json"],
				"drive requires a source file",
			),
			(
				vec!["drive", "a.rn", "--receipt", "r.json"],
				"drive requires --commands <file>",
			),
			(
				vec!["drive", "a.rn", "--commands", "c.json"],
				"drive requires --receipt <file>",
			),
			(
				vec![
					"drive",
					"a.rn",
					"b.rn",
					"--commands",
					"c.json",
					"--receipt",
					"r.json",
				],
				"drive takes exactly one source file",
			),
			(
				vec![
					"drive",
					"a.rn",
					"--window",
					"1",
					"--commands",
					"c.json",
					"--receipt",
					"r.json",
				],
				"unknown drive option \"--window\"",
			),
		] {
			let mut full = vec!["polariton"];
			full.extend(arguments);
			assert_eq!(
				action_from_args(full.into_iter().map(str::to_owned)).unwrap_err(),
				message
			);
		}
	}

	/// PRODUCT-0158 §2: `describe` takes exactly one source.
	#[test]
	fn product_0158_describe_takes_one_source() {
		assert_eq!(
			action_from_args(["polariton", "describe", "a.rn"].map(str::to_owned)).unwrap(),
			ApplicationAction::Describe {
				target: super::driver::Target::Source("a.rn".into())
			}
		);
		assert_eq!(
			action_from_args(["polariton", "describe", "--session", "abc"].map(str::to_owned))
				.unwrap(),
			ApplicationAction::Describe {
				target: super::driver::Target::Session("abc".to_owned())
			}
		);
		assert_eq!(
			action_from_args(["polariton", "describe"].map(str::to_owned)).unwrap_err(),
			"describe requires a source file or --session <id>"
		);
		assert_eq!(
			action_from_args(["polariton", "describe", "--session"].map(str::to_owned))
				.unwrap_err(),
			"--session requires a session id"
		);
		assert_eq!(
			action_from_args(["polariton", "describe", "a.rn", "b.rn"].map(str::to_owned))
				.unwrap_err(),
			"describe takes exactly one source file or one --session id"
		);
	}
}

/// `serve <package-directory> [--bind host:port]`, localhost by default and
/// never anything but a read-only package server.
fn serve_action<I>(mut args: I) -> Result<ApplicationAction, String>
where
	I: Iterator<Item = String>,
{
	let mut root = None;
	let mut bind: std::net::SocketAddr = ([127, 0, 0, 1], 7743).into();
	while let Some(arg) = args.next() {
		if arg == "--bind" {
			let value = args
				.next()
				.ok_or_else(|| "--bind requires host:port".to_owned())?;
			bind = value
				.parse()
				.map_err(|error| format!("invalid --bind value {value:?}: {error}"))?;
		} else if root.is_none() {
			root = Some(std::path::PathBuf::from(arg));
		} else {
			return Err(format!("unexpected argument {arg:?}"));
		}
	}
	let root = root.ok_or_else(|| "serve requires a package directory".to_owned())?;
	Ok(ApplicationAction::Serve { root, bind })
}

/// PRODUCT-0158 §6: `drive <source.rn> --commands <file> --receipt <file>`,
/// every part required, no other option.
fn drive_action<I>(mut args: I) -> Result<ApplicationAction, String>
where
	I: Iterator<Item = String>,
{
	let mut source = None;
	let mut commands = None;
	let mut receipt = None;
	while let Some(arg) = args.next() {
		match arg.as_str() {
			"--commands" => {
				let value = args
					.next()
					.ok_or_else(|| "--commands requires a file path".to_owned())?;
				if commands.replace(std::path::PathBuf::from(value)).is_some() {
					return Err("--commands may occur at most once".to_owned());
				}
			}
			"--receipt" => {
				let value = args
					.next()
					.ok_or_else(|| "--receipt requires a file path".to_owned())?;
				if receipt.replace(std::path::PathBuf::from(value)).is_some() {
					return Err("--receipt may occur at most once".to_owned());
				}
			}
			other if other.starts_with('-') => {
				return Err(format!("unknown drive option {other:?}"));
			}
			other => {
				if source.replace(std::path::PathBuf::from(other)).is_some() {
					return Err("drive takes exactly one source file".to_owned());
				}
			}
		}
	}
	Ok(ApplicationAction::Drive {
		source: source.ok_or_else(|| "drive requires a source file".to_owned())?,
		commands: commands.ok_or_else(|| "drive requires --commands <file>".to_owned())?,
		receipt: receipt.ok_or_else(|| "drive requires --receipt <file>".to_owned())?,
	})
}

/// PRODUCT-0158 §2: `describe <source.rn>` or `describe --session <id>`.
fn describe_action<I>(mut args: I) -> Result<ApplicationAction, String>
where
	I: Iterator<Item = String>,
{
	let first = args
		.next()
		.ok_or_else(|| "describe requires a source file or --session <id>".to_owned())?;
	let target = if first == "--session" {
		let id = args
			.next()
			.ok_or_else(|| "--session requires a session id".to_owned())?;
		driver::Target::Session(id)
	} else if first.starts_with('-') {
		return Err(format!("unknown describe option {first:?}"));
	} else {
		driver::Target::Source(std::path::PathBuf::from(first))
	};
	if args.next().is_some() {
		return Err("describe takes exactly one source file or one --session id".to_owned());
	}
	Ok(ApplicationAction::Describe { target })
}
