//! One operation on a custody directory: admit it, take its lock for the whole operation, verify
//! the log's chain, report leftovers and swap, run the operation, and append exactly one log line
//! with its outcome, success or refusal (KL4).
use crate::envelope::{self, ADMITTED, Header, hex};
use crate::error::ToolError;
use crate::host::HostFacts;
use crate::io::Io;
use crate::log::{LOG_NAME, Line, Log, Output};
use key_guard::custody::{CustodyDir, CustodyError, CustodyLock, SecretBuf};
use std::io::{ErrorKind, Read};
use std::path::Path;

pub(crate) const PENDING: &str = "pending";
pub(crate) const RETIRED: &str = "retired";
const RESERVED: [&str; 3] = [LOG_NAME, PENDING, RETIRED];

pub(crate) struct Session<'d> {
	pub(crate) dir: &'d CustodyDir,
	pub(crate) lock: CustodyLock<'d>,
	pub(crate) host: HostFacts,
	/// The lines before this operation, chain-verified.
	pub(crate) history: Vec<Line>,
	pub(crate) line: Line,
}

fn is_not_found(e: &CustodyError) -> bool {
	matches!(e, CustodyError::Io { source, .. } if source.kind() == ErrorKind::NotFound)
}

/// The subdirectory `name` of `dir` if it exists, admitted by the directory rules.
pub(crate) fn existing_subdir(
	dir: &CustodyDir,
	name: &str,
) -> Result<Option<CustodyDir>, ToolError> {
	match dir.open_subdir(name) {
		Ok(d) => Ok(Some(d)),
		Err(e) if is_not_found(&e) => Ok(None),
		Err(e) => Err(e.into()),
	}
}

/// Every leftover temp file in `dir` and its existing `pending/` and `retired/`, by relative name.
fn leftovers(dir: &CustodyDir, lock: &CustodyLock<'_>) -> Result<Vec<String>, ToolError> {
	let mut found: Vec<String> = lock
		.leftover_temps()?
		.iter()
		.map(|n| n.to_string_lossy().into_owned())
		.collect();
	for sub in [PENDING, RETIRED] {
		if let Some(d) = existing_subdir(dir, sub)? {
			let held = d.lock()?;
			found.extend(
				held.leftover_temps()?
					.iter()
					.map(|n| format!("{sub}/{}", n.to_string_lossy())),
			);
		}
	}
	Ok(found)
}

/// Runs `body` as operation `operation` on the custody directory at `path`.
pub(crate) fn run(
	io: &mut dyn Io,
	path: &Path,
	operation: &str,
	body: impl FnOnce(&mut dyn Io, &mut Session<'_>) -> Result<(), ToolError>,
) -> Result<(), ToolError> {
	let dir = io.open_dir(path)?;
	let lock = dir.lock()?;
	let mut log = Log::open(lock.open_log(LOG_NAME)?)?;
	// From here the lock is held, so even a failed host probe or a busy child lock is logged.
	let prelude = io
		.host()
		.and_then(|host| leftovers(&dir, &lock).map(|found| (host, found)));
	let (host, leftovers) = match prelude {
		Ok(found) => found,
		Err(e) => {
			let line = Line {
				time: io.now(),
				host: "unknown".into(),
				operation: operation.to_string(),
				outcome: format!("refused: {e}"),
				..Line::default()
			};
			let head = log.append(line)?;
			let _ = writeln!(io.err(), "custody.log head {}", hex(&head));
			return Err(e);
		}
	};
	if !leftovers.is_empty() {
		let _ = writeln!(
			io.err(),
			"warning: leftover temp files from an interrupted creation (left in place): {}",
			leftovers.join(", ")
		);
	}
	if host.swap {
		let _ = writeln!(
			io.err(),
			"warning: swap is enabled; the runbook requires it disabled or encrypted on a custody host"
		);
	}
	let line = Line {
		time: io.now(),
		host: host.hostname.clone(),
		operation: operation.to_string(),
		leftovers,
		swap: host.swap,
		..Line::default()
	};
	let mut session = Session {
		dir: &dir,
		lock,
		host,
		history: log.lines.clone(),
		line,
	};
	let result = body(io, &mut session);
	let mut line = std::mem::take(&mut session.line);
	line.outcome = match &result {
		Ok(()) => "ok".into(),
		Err(e) => format!("refused: {e}"),
	};
	let head = match log.append(line) {
		Ok(head) => head,
		Err(append) => {
			// The operation's own refusal still reaches the operator before the log failure.
			if let Err(e) = &result {
				let _ = writeln!(io.err(), "wendelon-key: {e}");
			}
			return Err(append);
		}
	};
	let _ = writeln!(io.err(), "custody.log head {}", hex(&head));
	drop(session);
	result
}

impl Session<'_> {
	/// K4: a recovery-role operation refuses on a host with a node data directory or an up,
	/// non-loopback interface, unless overridden; an override is logged.
	pub(crate) fn host_checks(&mut self, role: u8, overridden: bool) -> Result<(), ToolError> {
		if role != 3 {
			return Ok(());
		}
		match self.host.failures() {
			None => Ok(()),
			Some(found) if overridden => {
				self.line.overrides.push(format!("host-checks: {found}"));
				Ok(())
			}
			Some(found) => Err(ToolError::Host(found)),
		}
	}
	/// Decrypts the key file `name` (KL2). A wrong passphrase or a mismatched seed is logged with
	/// the identity its header claims, labelled unauthenticated.
	pub(crate) fn open_key(
		&mut self,
		name: &str,
		bytes: &[u8],
		passphrase: &SecretBuf,
	) -> Result<(Header, SecretBuf), ToolError> {
		let opened = envelope::open(name, bytes, passphrase);
		if let Err(ToolError::Decrypt { .. } | ToolError::KeyMismatch { .. }) = &opened
			&& let Ok(claimed) = Header::decode(bytes)
		{
			self.line.unauthenticated_identity = Some(hex(&claimed.identity()));
		}
		opened
	}
	pub(crate) fn identity(&mut self, header: &Header) {
		self.line.identity = Some(hex(&header.identity()));
	}
	pub(crate) fn output(&mut self, name: &str, bytes: &[u8]) {
		self.line.outputs.push(Output {
			name: name.to_string(),
			sha256: hex(&envelope::file_digest(bytes)),
		});
	}
}

/// A name for a key file: one plain component and not one of the directory's own names.
pub(crate) fn key_name(name: &str) -> Result<&str, ToolError> {
	if RESERVED.contains(&name) {
		return Err(ToolError::Usage(format!(
			"{name} is reserved in a custody directory"
		)));
	}
	Ok(name)
}

/// The bytes of the key file `name` in `dir`, held to the custody file rules and the version's
/// exact length.
pub(crate) fn read_key(dir: &CustodyDir, name: &str) -> Result<Vec<u8>, ToolError> {
	let mut bytes = Vec::with_capacity(envelope::FILE_BYTES);
	dir.open_file(key_name(name)?, &ADMITTED)?
		.read_to_end(&mut bytes)
		.map_err(ToolError::read(name))?;
	Ok(bytes)
}

/// The unauthenticated header of a key file: for refusals and listings only (KL2).
pub(crate) fn peek(name: &str, bytes: &[u8]) -> Result<Header, ToolError> {
	Header::decode(bytes).map_err(|reason| ToolError::Format {
		name: name.to_string(),
		reason,
	})
}

/// `header` with a fresh salt and nonce, for an encryption (K3 randomness).
pub(crate) fn fresh(io: &mut dyn Io, mut header: Header) -> Result<Header, ToolError> {
	io.fill(&mut header.salt)?;
	io.fill(&mut header.nonce)?;
	Ok(header)
}

/// Refuses unless `name` is free in `dir`, so a multi-step operation fails before its first write.
pub(crate) fn must_be_free(dir: &CustodyDir, name: &str) -> Result<(), ToolError> {
	match dir.open_file(key_name(name)?, &ADMITTED) {
		Err(e) if is_not_found(&e) => Ok(()),
		Err(
			e @ CustodyError::Refused {
				reason: key_guard::custody::Refusal::Name,
				..
			},
		) => Err(e.into()),
		_ => Err(ToolError::refused(format!(
			"{name} already exists; a custody file is never overwritten"
		))),
	}
}
