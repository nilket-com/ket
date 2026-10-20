//! Passphrase entry (K3, G6): only from `/dev/tty`, which must be a terminal, with echo,
//! canonical mode and terminal-generated signals off.
//!
//! The saved terminal settings are restored when the guard drops: on return, on error and on
//! unwind. Because `ISIG` is off, the INTR, QUIT and SUSP characters arrive as bytes; they end
//! the read as [`CustodyError::Interrupted`] after the restore, instead of killing the process
//! with echo off. A fatal signal sent by another process is not caught (stated limit; `stty sane`
//! recovers the terminal). Erase, kill and end-of-file are handled here, since canonical mode is
//! off. Passphrase policy (UTF-8, NFC, length) belongs to the caller.
use crate::custody::memory::SecretBuf;
use crate::custody::{CustodyError, sys};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::OpenOptionsExt;

#[cfg(test)]
mod tests;

/// The largest secret [`Terminal::read_secret`] reads; a larger `max` is capped to this.
pub const MAX_SECRET_LEN: usize = 1024;

/// The controlling terminal, opened for passphrase entry.
#[derive(Debug)]
pub struct Terminal {
	file: File,
}

impl Terminal {
	/// Opens `/dev/tty`. There is no other source: no argv, environment, file or stdin path.
	pub fn open() -> Result<Self, CustodyError> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.custom_flags(libc::O_NOCTTY)
			.open("/dev/tty")
			.map_err(|e| match e.raw_os_error() {
				Some(libc::ENXIO) | Some(libc::ENOENT) => CustodyError::NotATerminal,
				_ => CustodyError::io("/dev/tty")(e),
			})?;
		Self::checked(file)
	}

	fn checked(file: File) -> Result<Self, CustodyError> {
		if !sys::isatty(&file) {
			return Err(CustodyError::NotATerminal);
		}
		Ok(Self { file })
	}

	/// A pty slave in the tests; never reachable outside them.
	#[cfg(test)]
	pub(crate) fn from_file(file: File) -> Result<Self, CustodyError> {
		Self::checked(file)
	}

	/// Prints `prompt`, then reads one line of at most `max` bytes (never more than
	/// [`MAX_SECRET_LEN`]) with echo off.
	pub fn read_secret(&self, prompt: &str, max: usize) -> Result<SecretBuf, CustodyError> {
		let max = max.min(MAX_SECRET_LEN);
		let mut out = SecretBuf::with_capacity(max);
		(&self.file)
			.write_all(prompt.as_bytes())
			.map_err(CustodyError::io("/dev/tty: prompt"))?;
		let guard = EchoOff::engage(&self.file)?;
		let read = read_line(&self.file, &guard.saved, &mut out, max);
		drop(guard);
		let _ = (&self.file).write_all(b"\n");
		read.map(|()| out)
	}
}

/// Echo, canonical mode, `ISIG` and `IEXTEN` off until drop, which restores the saved settings.
pub(crate) struct EchoOff<'a> {
	file: &'a File,
	pub(crate) saved: libc::termios,
}

impl<'a> EchoOff<'a> {
	pub(crate) fn engage(file: &'a File) -> Result<Self, CustodyError> {
		let saved = sys::tcgetattr(file).map_err(CustodyError::io("/dev/tty: tcgetattr"))?;
		let mut quiet = saved;
		quiet.c_lflag &= !(libc::ECHO | libc::ECHONL | libc::ICANON | libc::ISIG | libc::IEXTEN);
		quiet.c_cc[libc::VMIN] = 1;
		quiet.c_cc[libc::VTIME] = 0;
		sys::tcsetattr(file, &quiet).map_err(CustodyError::io("/dev/tty: tcsetattr"))?;
		// From here the guard restores `saved` on every exit, including the refusal below.
		let guard = Self { file, saved };
		let now = sys::tcgetattr(file).map_err(CustodyError::io("/dev/tty: tcgetattr"))?;
		if !is_quiet(&now) {
			return Err(CustodyError::io("/dev/tty: tcsetattr")(
				std::io::Error::other("echo, canonical mode or signals are still on"),
			));
		}
		Ok(guard)
	}
}

impl Drop for EchoOff<'_> {
	fn drop(&mut self) {
		let _ = sys::tcsetattr(self.file, &self.saved);
	}
}

/// Whether the settings read back from the terminal have echo, canonical mode and signals off.
pub(crate) fn is_quiet(termios: &libc::termios) -> bool {
	termios.c_lflag & (libc::ECHO | libc::ICANON | libc::ISIG) == 0
}

/// Whether `byte` is the enabled control character `index` of `saved` (0 disables one on Linux).
fn is_control(saved: &libc::termios, index: usize, byte: u8) -> bool {
	let c = saved.c_cc[index];
	c != 0 && c == byte
}

fn read_line(
	file: &File,
	saved: &libc::termios,
	out: &mut SecretBuf,
	max: usize,
) -> Result<(), CustodyError> {
	loop {
		let mut byte = [0_u8; 1];
		match (&*file).read(&mut byte) {
			Ok(0) => return Err(CustodyError::EndOfInput),
			Ok(_) => {}
			Err(e) if e.kind() == ErrorKind::Interrupted => continue,
			Err(e) => return Err(CustodyError::io("/dev/tty: read")(e)),
		}
		let c = std::mem::take(&mut byte[0]);
		if c == b'\n' || c == b'\r' {
			return Ok(());
		}
		if [libc::VINTR, libc::VQUIT, libc::VSUSP]
			.into_iter()
			.any(|i| is_control(saved, i, c))
		{
			out.clear();
			return Err(CustodyError::Interrupted);
		}
		if is_control(saved, libc::VEOF, c) {
			if out.is_empty() {
				return Err(CustodyError::EndOfInput);
			}
			continue;
		}
		if c == 0x7f || c == 0x08 || is_control(saved, libc::VERASE, c) {
			out.pop_char();
			continue;
		}
		if is_control(saved, libc::VKILL, c) {
			out.clear();
			continue;
		}
		if out.push(c).is_err() {
			out.clear();
			return Err(CustodyError::TooLong { max });
		}
	}
}
