//! The subcommands. Each parses its own flags, makes sure passphrase entry is possible before
//! touching anything, and runs inside one locked, logged session.
use crate::args::{self, Args, Spec};
use crate::error::ToolError;
use crate::io::Io;
use std::process::ExitCode;

mod backup;
mod bind;
mod build;
mod describe;
mod generate;
mod merge;
mod retire;
mod show;
mod sign;

const OVERRIDE: &str = "--override-host-checks";

/// Every subcommand with its flags; the parser refuses anything not listed.
const COMMANDS: &[(&str, Spec)] = &[
	(
		"build",
		Spec {
			values: &[
				"--dir",
				"--context",
				"--trusted-manifest",
				"--out",
				"--key",
				"--action",
				"--permission-key",
				"--sidecar",
			],
			twice: &[],
			switches: &[],
		},
	),
	(
		"sign",
		Spec {
			values: &[
				"--dir",
				"--key",
				"--context",
				"--trusted-manifest",
				"--command",
				"--partial",
				"--with",
				"--sidecar",
				"--out",
			],
			twice: &[],
			switches: &[OVERRIDE],
		},
	),
	(
		"sign-statement",
		Spec {
			values: &[
				"--dir",
				"--key",
				"--context",
				"--trusted-manifest",
				"--action",
				"--permission-key",
				"--sidecar",
				"--out",
			],
			twice: &[],
			switches: &[OVERRIDE],
		},
	),
	(
		"merge",
		Spec {
			values: &[
				"--dir",
				"--context",
				"--trusted-manifest",
				"--command",
				"--partial",
				"--out",
			],
			twice: &["--partial"],
			switches: &[],
		},
	),
	(
		"generate",
		Spec {
			values: &[
				"--dir",
				"--role",
				"--intended-epoch",
				"--out",
				"--context",
				"--trusted-manifest",
			],
			twice: &[],
			switches: &[OVERRIDE],
		},
	),
	(
		"bind",
		Spec {
			values: &[
				"--dir",
				"--key",
				"--context",
				"--trusted-manifest",
				"--out",
				"--epoch",
			],
			twice: &["--context"],
			switches: &[OVERRIDE],
		},
	),
	(
		"show-public",
		Spec {
			values: &["--dir", "--key"],
			twice: &[],
			switches: &["--unauthenticated", OVERRIDE],
		},
	),
	(
		"backup",
		Spec {
			values: &["--dir", "--key", "--to", "--out"],
			twice: &[],
			switches: &["--pending", OVERRIDE],
		},
	),
	(
		"restore",
		Spec {
			values: &["--dir", "--from", "--backup", "--out"],
			twice: &[],
			switches: &[OVERRIDE],
		},
	),
	(
		"restore-verify",
		Spec {
			values: &[
				"--dir",
				"--key",
				"--from",
				"--backup",
				"--context",
				"--trusted-manifest",
			],
			twice: &[],
			switches: &[OVERRIDE],
		},
	),
	(
		"retire-epoch",
		Spec {
			values: &["--dir", "--key", "--context", "--trusted-manifest"],
			twice: &[],
			switches: &[],
		},
	),
	(
		"destroy",
		Spec {
			values: &[
				"--dir",
				"--key",
				"--context",
				"--trusted-manifest",
				"--successor",
				"--evidence",
				"--evidence-digest",
			],
			twice: &["--context"],
			switches: &["--retired", "--pending", OVERRIDE],
		},
	),
];

const USAGE: &str = "usage: wendelon-key <generate|bind|show-public|backup|restore|restore-verify|retire-epoch|destroy|build <kind>|sign|sign-statement|merge> --dir <custody dir> [flags]";

/// Runs one command line; prints a refusal on stderr and returns exit code 2 for it.
pub(crate) fn run(argv: &[String], io: &mut dyn Io) -> ExitCode {
	match dispatch(argv, io) {
		Ok(()) => ExitCode::SUCCESS,
		Err(e) => {
			let _ = writeln!(io.err(), "wendelon-key: {e}");
			ExitCode::from(2)
		}
	}
}

fn dispatch(argv: &[String], io: &mut dyn Io) -> Result<(), ToolError> {
	let Some((command, rest)) = argv.split_first() else {
		return Err(ToolError::Usage(USAGE.into()));
	};
	let Some((name, spec)) = COMMANDS.iter().find(|(n, _)| n == command) else {
		return Err(ToolError::Usage(format!(
			"unknown command {command:?}; {USAGE}"
		)));
	};
	// `build` takes its kind as the first argument after the command.
	let (kind, rest) = match (*name, rest.split_first()) {
		("build", Some((kind, rest))) if !kind.starts_with("--") => (Some(kind.as_str()), rest),
		("build", _) => {
			return Err(ToolError::Usage(format!(
				"build needs a kind; {}",
				build::KINDS
			)));
		}
		_ => (None, rest),
	};
	let a = args::parse(rest, spec)?;
	let unauthenticated = *name == "show-public" && a.switch("--unauthenticated");
	let keyless = matches!(*name, "build" | "merge");
	if !unauthenticated && !keyless {
		io.terminal_ready()?;
	}
	match *name {
		"generate" => generate::run(io, &a),
		"bind" => bind::run(io, &a),
		"show-public" => show::run(io, &a),
		"backup" => backup::backup(io, &a),
		"restore" => backup::restore(io, &a),
		"restore-verify" => backup::verify(io, &a),
		"retire-epoch" => retire::retire(io, &a),
		"destroy" => retire::destroy(io, &a),
		"build" => build::run(io, &a, kind.expect("checked above")),
		"sign" => sign::sign(io, &a),
		"sign-statement" => sign::statement(io, &a),
		"merge" => merge::run(io, &a),
		_ => unreachable!("every listed command is dispatched"),
	}
}

pub(crate) fn overridden(a: &Args) -> bool {
	a.switch(OVERRIDE)
}

/// A role flag: 1 admin, 2 governance, 3 recovery (or the names).
pub(crate) fn role(a: &Args) -> Result<u8, ToolError> {
	match a.value("--role")? {
		"1" | "admin" => Ok(1),
		"2" | "governance" => Ok(2),
		"3" | "recovery" => Ok(3),
		other => Err(ToolError::Usage(format!(
			"--role {other:?}: admin, governance or recovery"
		))),
	}
}

/// Asks the operator to type `expected` (a fingerprint or a count); anything else aborts with
/// nothing changed.
pub(crate) fn confirm(io: &mut dyn Io, prompt: &str, expected: &str) -> Result<(), ToolError> {
	if io.confirm(prompt)?.trim() != expected {
		return Err(ToolError::Confirmation);
	}
	Ok(())
}
