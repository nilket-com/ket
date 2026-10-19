//! `wendelon audit root2`: the offline, read-only Root2 compromise audit over a node's data
//! directory. It takes no data-directory owner lock and writes nothing there; the trusted manifest
//! and an optional suspect key are explicit files supplied out of band, never inferred from the
//! log. `--export-context` writes the context export of a complete fold to a file outside the data
//! directory, before any row is printed.
mod publish;

use crate::storage::mutation_visibility::root2::compromise::compromise_audit2;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const USAGE: &str = "usage: audit root2 <data-dir> --trusted-manifest <file> \
	[(--role <admin|governance|recovery> --epoch <n>)... | --key <file>] [--suspect-only] \
	[--export-context <out> --node-id <n>]; suspects are optional only with --export-context";
/// Bounded trust-input reads: a manifest is 241 bytes and a key file one hex line.
const MAX_INPUT_BYTES: u64 = 4096;

#[derive(Debug, PartialEq, Eq)]
struct Options {
	data_dir: PathBuf,
	manifest: PathBuf,
	suspects: Vec<(u8, u64)>,
	key: Option<PathBuf>,
	suspect_only: bool,
	/// The context export's output and the operator-asserted node id it is labelled with.
	export: Option<(PathBuf, u32)>,
}

fn role_code(name: &str) -> Option<u8> {
	match name {
		"admin" => Some(1),
		"governance" => Some(2),
		"recovery" => Some(3),
		_ => None,
	}
}

fn parse(args: &[String]) -> Result<Options, String> {
	let (data_dir, mut rest) = match args.split_first() {
		Some((dir, rest)) if !dir.starts_with("--") => (PathBuf::from(dir), rest),
		_ => return Err(USAGE.into()),
	};
	let (mut manifest, mut key, mut suspects, mut suspect_only) = (None, None, Vec::new(), false);
	let (mut export, mut node_id) = (None, None);
	while let Some((flag, tail)) = rest.split_first() {
		rest = tail;
		match flag.as_str() {
			"--suspect-only" if !suspect_only => suspect_only = true,
			"--trusted-manifest" | "--key" | "--role" | "--export-context" | "--node-id" => {
				let Some((value, tail)) = rest.split_first() else {
					return Err(format!("{flag} requires a value\n{USAGE}"));
				};
				rest = tail;
				match flag.as_str() {
					"--trusted-manifest" if manifest.is_none() => {
						manifest = Some(PathBuf::from(value))
					}
					"--key" if key.is_none() => key = Some(PathBuf::from(value)),
					"--export-context" if export.is_none() => export = Some(PathBuf::from(value)),
					"--node-id" if node_id.is_none() => {
						node_id = Some(
							value
								.parse::<u32>()
								.map_err(|_| format!("bad node id {value}\n{USAGE}"))?,
						)
					}
					"--role" => {
						let role = role_code(value)
							.ok_or_else(|| format!("unknown role {value}\n{USAGE}"))?;
						let epoch = match rest.split_first() {
							Some((flag, tail)) if flag == "--epoch" => match tail.split_first() {
								Some((epoch, tail)) => {
									rest = tail;
									epoch
										.parse::<u64>()
										.map_err(|_| format!("bad epoch {epoch}\n{USAGE}"))?
								}
								None => return Err(format!("--epoch requires a value\n{USAGE}")),
							},
							_ => return Err(format!("--role requires --epoch\n{USAGE}")),
						};
						suspects.push((role, epoch));
					}
					_ => return Err(format!("{flag} given twice\n{USAGE}")),
				}
			}
			other => return Err(format!("unexpected argument {other}\n{USAGE}")),
		}
	}
	let manifest = manifest.ok_or_else(|| format!("--trusted-manifest is required\n{USAGE}"))?;
	let export = match (export, node_id) {
		(Some(out), Some(node)) => Some((out, node)),
		(None, None) => None,
		_ => {
			return Err(format!(
				"--export-context and --node-id go together\n{USAGE}"
			));
		}
	};
	let unnamed = key.is_none() && suspects.is_empty();
	if key.is_some() && !suspects.is_empty() || unnamed && export.is_none() {
		return Err(format!(
			"give either --role/--epoch suspects or one --key\n{USAGE}"
		));
	}
	if unnamed && suspect_only {
		return Err(format!("--suspect-only needs a suspect\n{USAGE}"));
	}
	Ok(Options {
		data_dir,
		manifest,
		suspects,
		key,
		suspect_only,
		export,
	})
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, String> {
	let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
	let mut bytes = Vec::new();
	file.take(MAX_INPUT_BYTES + 1)
		.read_to_end(&mut bytes)
		.map_err(|e| format!("{}: {e}", path.display()))?;
	if bytes.len() as u64 > MAX_INPUT_BYTES {
		return Err(format!(
			"{}: larger than {MAX_INPUT_BYTES} bytes",
			path.display()
		));
	}
	Ok(bytes)
}

/// A suspect key file: one 64-digit hex Ed25519 public key, surrounding whitespace ignored.
fn read_key(path: &Path) -> Result<[u8; 32], String> {
	let bytes = read_bounded(path)?;
	let text = std::str::from_utf8(&bytes)
		.map_err(|_| format!("{}: not a hex key", path.display()))?
		.trim();
	if text.len() != 64 {
		return Err(format!("{}: expected 64 hex digits", path.display()));
	}
	let mut key = [0u8; 32];
	for (i, byte) in key.iter_mut().enumerate() {
		*byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16)
			.map_err(|_| format!("{}: not a hex key", path.display()))?;
	}
	Ok(key)
}

/// Runs the audit and writes its rows to `out` and diagnostics to `err`; returns the exit code.
pub(crate) fn execute(args: &[String], out: &mut dyn Write, err: &mut dyn Write) -> u8 {
	let mut fail = |code: u8, message: String| {
		let _ = writeln!(err, "wendelon audit root2: {message}");
		code
	};
	let options = match parse(args) {
		Ok(options) => options,
		Err(message) => return fail(2, message),
	};
	// The output path rule runs first: a bad path is a usage error before anything is read.
	let target = match &options.export {
		Some((out, _)) => match publish::target(&options.data_dir, out) {
			Ok(target) => Some(target),
			Err(message) => return fail(2, format!("--export-context: {message}")),
		},
		None => None,
	};
	let manifest = match read_bounded(&options.manifest) {
		Ok(bytes) => bytes,
		Err(message) => return fail(2, format!("trusted manifest: {message}")),
	};
	let key = match options.key.as_deref().map(read_key).transpose() {
		Ok(key) => key,
		Err(message) => return fail(2, format!("suspect key: {message}")),
	};
	let runtime = match tokio::runtime::Builder::new_current_thread().build() {
		Ok(runtime) => runtime,
		Err(e) => return fail(2, format!("runtime: {e}")),
	};
	let (stdout, exported, diagnostic, code) = runtime.block_on(compromise_audit2(
		&options.data_dir,
		&manifest,
		&options.suspects,
		key,
		options.suspect_only,
		options.export.as_ref().map(|(_, node)| *node),
	));
	// Only a complete fold is exported, and it is published before any row is printed.
	let mut published = None;
	if let Some(target) = &target
		&& code == 0
	{
		let Some(bytes) = exported else {
			return fail(2, "no context export was produced".into());
		};
		match publish::publish(target, &bytes) {
			Ok(file) => published = Some((target, file)),
			Err(message) => return fail(2, format!("--export-context: {message}")),
		}
	}
	if out
		.write_all(stdout.as_bytes())
		.and_then(|()| out.flush())
		.is_err()
	{
		// An exit 2 leaves no export: the one just published is withdrawn, if it is still there.
		if let Some((target, file)) = published
			&& let Err(message) = publish::withdraw(target, &file)
		{
			fail(2, format!("--export-context: {message}"));
		}
		return fail(2, "stdout write failed".into());
	}
	match diagnostic {
		Some(message) => fail(code, message),
		None => code,
	}
}

/// Test-only: makes the context export's publish fail at `step` on this thread.
#[cfg(test)]
pub(crate) fn fail_publish_at(step: Option<&'static str>) {
	publish::fail_at(step);
}

pub(super) fn run(args: &[String]) -> ExitCode {
	let code = execute(
		args,
		&mut std::io::stdout().lock(),
		&mut std::io::stderr().lock(),
	);
	ExitCode::from(code)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn args(list: &[&str]) -> Vec<String> {
		list.iter().map(|s| s.to_string()).collect()
	}

	#[test]
	fn parses_roles_key_and_flags() {
		let o = parse(&args(&[
			"/d",
			"--trusted-manifest",
			"/m",
			"--role",
			"admin",
			"--epoch",
			"0",
			"--role",
			"recovery",
			"--epoch",
			"7",
			"--suspect-only",
		]))
		.unwrap();
		assert_eq!(
			o,
			Options {
				data_dir: "/d".into(),
				manifest: "/m".into(),
				suspects: vec![(1, 0), (3, 7)],
				key: None,
				suspect_only: true,
				export: None,
			}
		);
		let o = parse(&args(&["/d", "--key", "/k", "--trusted-manifest", "/m"])).unwrap();
		assert_eq!(o.key, Some("/k".into()));
		assert!(o.suspects.is_empty());
		assert_eq!(o.export, None);
		// With the export, the suspect set may be empty.
		let o = parse(&args(&[
			"/d",
			"--trusted-manifest",
			"/m",
			"--export-context",
			"/o",
			"--node-id",
			"4294967295",
		]))
		.unwrap();
		assert_eq!(o.export, Some(("/o".into(), u32::MAX)));
		assert!(o.suspects.is_empty() && o.key.is_none());
	}

	#[test]
	fn export_usage_errors_are_refused() {
		let export = ["/d", "--trusted-manifest", "/m", "--export-context", "/o"];
		for bad in [
			// Each flag without the other.
			&export[..],
			&[
				"/d",
				"--trusted-manifest",
				"/m",
				"--key",
				"/k",
				"--node-id",
				"1",
			][..],
			// Not a u32 node id.
			&[&export[..], &["--node-id", "x"]].concat()[..],
			&[&export[..], &["--node-id", "-1"]].concat()[..],
			&[&export[..], &["--node-id", "4294967296"]].concat()[..],
			// Given twice.
			&[&export[..], &["--node-id", "1", "--export-context", "/p"]].concat()[..],
			&[&export[..], &["--node-id", "1", "--node-id", "2"]].concat()[..],
			// --suspect-only needs a suspect, export or not.
			&[&export[..], &["--node-id", "1", "--suspect-only"]].concat()[..],
		] {
			assert!(parse(&args(bad)).is_err(), "{bad:?}");
		}
	}

	#[test]
	fn usage_errors_are_refused() {
		for bad in [
			&[][..],
			&["--trusted-manifest", "/m"][..],
			&["/d", "--role", "admin", "--epoch", "0"][..],
			&["/d", "--trusted-manifest", "/m"][..],
			&["/d", "--trusted-manifest", "/m", "--role", "admin"][..],
			&[
				"/d",
				"--trusted-manifest",
				"/m",
				"--role",
				"root",
				"--epoch",
				"0",
			][..],
			&[
				"/d",
				"--trusted-manifest",
				"/m",
				"--role",
				"admin",
				"--epoch",
				"x",
			][..],
			&[
				"/d",
				"--trusted-manifest",
				"/m",
				"--key",
				"/k",
				"--role",
				"admin",
				"--epoch",
				"0",
			][..],
			&[
				"/d",
				"--trusted-manifest",
				"/m",
				"--trusted-manifest",
				"/n",
				"--key",
				"/k",
			][..],
			&[
				"/d",
				"--trusted-manifest",
				"/m",
				"--key",
				"/k",
				"--suspect-only",
				"--suspect-only",
			][..],
			&["/d", "--trusted-manifest", "/m", "--key", "/k", "--verbose"][..],
		] {
			assert!(parse(&args(bad)).is_err(), "{bad:?}");
		}
	}

	#[test]
	fn usage_and_trust_input_errors_exit_two_with_no_stdout() {
		let dir = crate::test_support::TestDir::new("root2-audit-cli");
		let key = dir.join("key");
		for (contents, expect_ok) in [
			(format!("{}\n", "ab".repeat(32)), true),
			("ab".repeat(31), false),
			("zz".repeat(32), false),
		] {
			std::fs::write(&key, contents).unwrap();
			assert_eq!(read_key(&key).is_ok(), expect_ok);
		}
		let (mut out, mut err) = (Vec::new(), Vec::new());
		assert_eq!(execute(&args(&["/d"]), &mut out, &mut err), 2);
		assert!(out.is_empty() && !err.is_empty());
		let missing = dir.join("absent-manifest");
		let (mut out, mut err) = (Vec::new(), Vec::new());
		let code = execute(
			&args(&[
				dir.to_str().unwrap(),
				"--trusted-manifest",
				missing.to_str().unwrap(),
				"--role",
				"admin",
				"--epoch",
				"0",
			]),
			&mut out,
			&mut err,
		);
		assert_eq!(code, 2);
		assert!(out.is_empty());
		std::fs::write(&missing, [7u8; 241]).unwrap();
		let (mut out, mut err) = (Vec::new(), Vec::new());
		let code = execute(
			&args(&[
				dir.to_str().unwrap(),
				"--trusted-manifest",
				missing.to_str().unwrap(),
				"--role",
				"admin",
				"--epoch",
				"0",
			]),
			&mut out,
			&mut err,
		);
		assert_eq!(code, 2, "an undecodable manifest is a trust-input error");
		assert!(out.is_empty());
	}
}
