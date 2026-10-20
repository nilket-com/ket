//! `custody.log` (contract §3 item 4): one JSON line per operation, appended through key-guard's
//! owner-only log open. Each line's `prev` is the SHA-256 of the previous line's bytes (zeros for
//! the first), so an edited or removed middle line breaks the chain. A truncated tail cannot be
//! detected; the runbook records the head digest the tool prints after every append.
use crate::envelope::{file_digest, hex};
use crate::error::ToolError;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};

pub(crate) const LOG_NAME: &str = "custody.log";
/// Lines are short; a log beyond this is refused rather than read without bound.
const MAX_LOG_BYTES: u64 = 64 << 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Output {
	pub(crate) name: String,
	pub(crate) sha256: String,
}

/// One operation. Every value is public: names, digests, public identities and typed outcomes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Line {
	pub(crate) prev: String,
	pub(crate) time: u64,
	pub(crate) host: String,
	pub(crate) operation: String,
	pub(crate) identity: Option<String>,
	/// On a failed decryption: the identity the file's header claims, never authenticated.
	pub(crate) unauthenticated_identity: Option<String>,
	/// The identity an operation started from, when it writes another (bind, restore).
	pub(crate) source: Option<String>,
	pub(crate) command_digest: Option<String>,
	pub(crate) form_slot: Option<String>,
	pub(crate) outputs: Vec<Output>,
	pub(crate) backup_of: Option<String>,
	pub(crate) evidence: Option<String>,
	pub(crate) overrides: Vec<String>,
	pub(crate) leftovers: Vec<String>,
	pub(crate) swap: bool,
	pub(crate) outcome: String,
}

/// The parsed lines of `bytes` and the head digest, or the first line where the chain breaks
/// (1-based).
pub(crate) fn verify_chain(bytes: &[u8]) -> Result<(Vec<Line>, [u8; 32]), ToolError> {
	let mut head = [0_u8; 32];
	let mut lines = Vec::new();
	if bytes.is_empty() {
		return Ok((lines, head));
	}
	let Some(body) = bytes.strip_suffix(b"\n") else {
		return Err(ToolError::LogChain {
			line: bytes.split(|b| *b == b'\n').count(),
		});
	};
	for (index, raw) in body.split(|b| *b == b'\n').enumerate() {
		let broken = || ToolError::LogChain { line: index + 1 };
		let line: Line = serde_json::from_slice(raw).map_err(|_| broken())?;
		if line.prev != hex(&head) {
			return Err(broken());
		}
		head = file_digest(raw);
		lines.push(line);
	}
	Ok((lines, head))
}

/// The log opened for this operation: its verified lines and head.
pub(crate) struct Log {
	file: File,
	pub(crate) lines: Vec<Line>,
	head: [u8; 32],
}

impl Log {
	pub(crate) fn open(mut file: File) -> Result<Self, ToolError> {
		let mut bytes = Vec::new();
		(&mut file)
			.take(MAX_LOG_BYTES + 1)
			.read_to_end(&mut bytes)
			.map_err(ToolError::read(LOG_NAME))?;
		if bytes.len() as u64 > MAX_LOG_BYTES {
			return Err(ToolError::TooLarge {
				what: LOG_NAME.into(),
				max: MAX_LOG_BYTES,
			});
		}
		let (lines, head) = verify_chain(&bytes)?;
		Ok(Self { file, lines, head })
	}

	/// Appends `line` chained to the head, fsyncs, and returns the new head.
	pub(crate) fn append(&mut self, mut line: Line) -> Result<[u8; 32], ToolError> {
		line.prev = hex(&self.head);
		let text = serde_json::to_vec(&line).map_err(|e| ToolError::Log(e.to_string()))?;
		let mut record = text.clone();
		record.push(b'\n');
		self.file
			.write_all(&record)
			.and_then(|()| self.file.sync_data())
			.map_err(ToolError::read(LOG_NAME))?;
		self.head = file_digest(&text);
		self.lines.push(line);
		Ok(self.head)
	}
}
