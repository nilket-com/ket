//! Everything the tool takes from outside its arguments, behind one seam: passphrases and typed
//! confirmations (key-guard's terminal only), randomness (aws-lc-rs), the host probe, the clock
//! and the custody directory open. Production wires the system; tests wire a script under
//! `cfg(test)` only.
use crate::error::ToolError;
use crate::host::HostFacts;
use key_guard::custody::{CustodyDir, SecretBuf, Terminal};
use std::io::Write;
use std::path::Path;

pub(crate) trait Io {
	/// A secret line from the terminal, echo off, at most `max` bytes.
	fn secret(&mut self, prompt: &str, max: usize) -> Result<SecretBuf, ToolError>;
	/// A typed confirmation from the terminal. Not a secret, but read the same way.
	fn confirm(&mut self, prompt: &str) -> Result<String, ToolError> {
		let line = self.secret(prompt, 256)?;
		String::from_utf8(line.as_slice().to_vec()).map_err(|_| ToolError::Confirmation)
	}
	/// Refuses unless passphrase entry is possible; the binary checks it before anything else.
	fn terminal_ready(&mut self) -> Result<(), ToolError>;
	/// A fresh 32-byte Ed25519 seed.
	fn new_seed(&mut self) -> Result<SecretBuf, ToolError>;
	/// Fresh salt and nonce bytes.
	fn fill(&mut self, out: &mut [u8]) -> Result<(), ToolError>;
	fn host(&mut self) -> Result<HostFacts, ToolError>;
	fn now(&self) -> u64;
	fn open_dir(&self, path: &Path) -> Result<CustodyDir, ToolError>;
	fn out(&mut self) -> &mut dyn Write;
	fn err(&mut self) -> &mut dyn Write;
}

/// The production wiring.
pub(crate) struct SystemIo {
	terminal: Option<Terminal>,
	stdout: std::io::Stdout,
	stderr: std::io::Stderr,
}

impl SystemIo {
	pub(crate) fn new() -> Self {
		Self {
			terminal: None,
			stdout: std::io::stdout(),
			stderr: std::io::stderr(),
		}
	}
	fn terminal(&mut self) -> Result<&Terminal, ToolError> {
		if self.terminal.is_none() {
			self.terminal = Some(Terminal::open()?);
		}
		Ok(self.terminal.as_ref().expect("opened above"))
	}
}

impl Io for SystemIo {
	fn secret(&mut self, prompt: &str, max: usize) -> Result<SecretBuf, ToolError> {
		Ok(self.terminal()?.read_secret(prompt, max)?)
	}
	fn terminal_ready(&mut self) -> Result<(), ToolError> {
		self.terminal().map(|_| ())
	}
	fn new_seed(&mut self) -> Result<SecretBuf, ToolError> {
		let mut seed = SecretBuf::zeroed(32);
		aws_lc_rs::rand::fill(seed.as_mut_slice()).map_err(|_| ToolError::Random)?;
		Ok(seed)
	}
	fn fill(&mut self, out: &mut [u8]) -> Result<(), ToolError> {
		aws_lc_rs::rand::fill(out).map_err(|_| ToolError::Random)
	}
	fn host(&mut self) -> Result<HostFacts, ToolError> {
		crate::host::probe()
	}
	fn now(&self) -> u64 {
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map_or(0, |d| d.as_secs())
	}
	fn open_dir(&self, path: &Path) -> Result<CustodyDir, ToolError> {
		Ok(key_guard::custody::open_custody_dir(path)?)
	}
	fn out(&mut self) -> &mut dyn Write {
		&mut self.stdout
	}
	fn err(&mut self) -> &mut dyn Write {
		&mut self.stderr
	}
}
