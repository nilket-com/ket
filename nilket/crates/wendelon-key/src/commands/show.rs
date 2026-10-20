//! `show-public`: the authenticated public key of one key file, as exactly one 64-hex line (the
//! form `audit root2 --key` reads), printed only after decryption shows the seed derives it.
//! `--unauthenticated` decrypts nothing and labels every line so; no runbook step takes a key
//! from it.
use super::overridden;
use crate::args::Args;
use crate::envelope::{fingerprint, hex};
use crate::error::ToolError;
use crate::io::Io;
use crate::log::LOG_NAME;
use crate::passphrase;
use crate::session::{self, PENDING, RETIRED, existing_subdir, key_name, peek, read_key};
use key_guard::custody::CustodyDir;

pub(super) fn run(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	if a.switch("--unauthenticated") {
		if a.opt("--key").is_some() {
			return Err(ToolError::Usage(
				"--unauthenticated lists the whole directory; it takes no --key".into(),
			));
		}
		return session::run(
			io,
			a.path("--dir")?,
			"show-public-unauthenticated",
			|io, s| {
				let mut lines = listing(s.dir, "")?;
				for sub in [PENDING, RETIRED] {
					if let Some(d) = existing_subdir(s.dir, sub)? {
						lines.extend(listing(&d, &format!("{sub}/"))?);
					}
				}
				for line in lines {
					writeln!(io.out(), "UNAUTHENTICATED {line}")
						.map_err(ToolError::read("stdout"))?;
				}
				Ok(())
			},
		);
	}
	let name = key_name(a.value("--key")?)?;
	session::run(io, a.path("--dir")?, "show-public", |io, s| {
		let bytes = read_key(s.dir, name)?;
		s.host_checks(peek(name, &bytes)?.role, overridden(a))?;
		let passphrase = passphrase::existing(io, &format!("Passphrase for {name}"))?;
		let (header, _seed) = s.open_key(name, &bytes, &passphrase)?;
		s.identity(&header);
		writeln!(io.out(), "{}", hex(&header.key)).map_err(ToolError::read("stdout"))
	})
}

/// One unauthenticated line per entry of `dir` that is not the log or a subdirectory.
fn listing(dir: &CustodyDir, prefix: &str) -> Result<Vec<String>, ToolError> {
	let mut lines = Vec::new();
	for raw in dir.names()? {
		let Some(name) = raw.to_str() else {
			lines.push(format!(
				"{prefix}{} not a custody name",
				raw.to_string_lossy()
			));
			continue;
		};
		if prefix.is_empty() && [LOG_NAME, PENDING, RETIRED].contains(&name) {
			continue;
		}
		let line = match read_key(dir, name).and_then(|b| peek(name, &b)) {
			Ok(h) => format!(
				"{prefix}{name} state={} role={} epoch={} key={} fingerprint={} backup_of={}",
				h.state.name(),
				h.role,
				h.epoch,
				hex(&h.key),
				fingerprint(&h.key),
				h.backup_of.map_or("none".into(), |b| hex(&b)),
			),
			Err(e) => format!("{prefix}{name} not readable as a key file: {e}"),
		};
		lines.push(line);
	}
	Ok(lines)
}
