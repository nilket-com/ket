//! Custody of operator role keys (key custody contract r4, K2-K4): the system calls an offline
//! custody tool needs and may not make itself, because it forbids unsafe code.
//!
//! - [`open_custody_dir`] admits an owner-only directory by its handle, walking its ancestry by
//!   `..` from that handle up to `/`. Files are resolved by one name component through it and
//!   created whole or not at all, never overwriting. Every mutation needs the directory's lock.
//! - [`SecretBuf`] is a fixed-capacity heap buffer, best-effort mlocked, zeroed before it is freed.
//! - [`Terminal`] reads a secret from `/dev/tty` with echo off and restores the terminal on every
//!   return, error and unwind; [`install_terminal_restore_hook`] restores it on a panic too, which
//!   matters where panics abort and no destructor runs.
//! - [`CustodyDir::names`] lists a directory through its handle, and [`CustodyLock::leftover_temps`]
//!   reports the temp files a crashed creation left.
//! - [`harden_process`] and [`non_loopback_interfaces_up`] serve the tool's process and host
//!   checks.
//!
//! This module never interprets key, command or log contents: formats, identities and proof forms
//! belong to the tool. It refuses and never repairs. Every `unsafe` lives in `sys.rs`.
mod dir;
mod memory;
mod process;
mod sys;
#[cfg(test)]
mod test_dir;
#[cfg(test)]
mod tests;
mod tty;

#[cfg(any(test, feature = "fixture-anchor"))]
pub use crate::custody::dir::open_custody_dir_anchored;
pub use crate::custody::dir::{CustodyDir, CustodyLock, open_custody_dir};
pub use crate::custody::memory::{SecretBuf, lock_memory, unlock_memory};
pub use crate::custody::process::{
	core_dump_limit, harden_process, is_dumpable, non_loopback_interfaces_up,
};
pub use crate::custody::tty::{MAX_SECRET_LEN, Terminal, install_terminal_restore_hook};

/// Why a custody object was refused. A refusal never changes the object (G1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
	/// Not one plain path component, or a name reserved for temp files.
	Name,
	/// A symlink where the object itself was required.
	Symlink,
	NotDirectory,
	NotRegular,
	/// Owned by someone other than the effective user.
	Owner {
		uid: u32,
	},
	/// A custody directory whose permission bits are not exactly 0700.
	DirectoryMode {
		mode: u32,
	},
	/// A custody file whose permission bits are not exactly 0600.
	FileMode {
		mode: u32,
	},
	LinkCount {
		nlink: u64,
	},
	/// A size outside the caller's admitted lengths.
	Size {
		len: u64,
	},
	/// An ancestor owned by neither root nor the effective user.
	AncestorOwner {
		dev: u64,
		ino: u64,
		uid: u32,
	},
	/// An ancestor that is group- or other-writable.
	AncestorWritable {
		dev: u64,
		ino: u64,
		mode: u32,
	},
	AncestryTooDeep,
	/// A move target that is neither the locked directory nor a direct child of it, so the lock
	/// would not cover the move.
	OutsideLock,
}

impl std::fmt::Display for Refusal {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Name => write!(f, "the name is not a single plain component"),
			Self::Symlink => write!(f, "it is a symlink"),
			Self::NotDirectory => write!(f, "it is not a directory"),
			Self::NotRegular => write!(f, "it is not a regular file"),
			Self::Owner { uid } => write!(f, "it is owned by uid {uid}, not the effective user"),
			Self::DirectoryMode { mode } => write!(f, "its mode is {mode:o}, not 700"),
			Self::FileMode { mode } => write!(f, "its mode is {mode:o}, not 600"),
			Self::LinkCount { nlink } => write!(f, "it has {nlink} links, not 1"),
			Self::Size { len } => write!(f, "its length {len} is not an admitted length"),
			Self::AncestorOwner { dev, ino, uid } => {
				write!(f, "ancestor {dev}:{ino} is owned by uid {uid}")
			}
			Self::AncestorWritable { dev, ino, mode } => {
				write!(
					f,
					"ancestor {dev}:{ino} is group- or other-writable (mode {mode:o})"
				)
			}
			Self::AncestryTooDeep => write!(f, "its ancestry is too deep"),
			Self::OutsideLock => {
				write!(
					f,
					"it is neither the locked directory nor a direct child of it"
				)
			}
		}
	}
}

/// A custody failure. No variant carries secret bytes (G4); names and paths are not secrets.
#[derive(Debug, thiserror::Error)]
pub enum CustodyError {
	#[error("{what} is refused: {reason}")]
	Refused { what: String, reason: Refusal },
	#[error("{what}: {source}")]
	Io {
		what: String,
		#[source]
		source: std::io::Error,
	},
	#[error("{name} already exists; a custody file is never overwritten")]
	Exists { name: String },
	#[error("the custody directory is locked by another operation")]
	Busy,
	#[error("the file system does not support RENAME_NOREPLACE; there is no fallback")]
	NoReplaceUnsupported,
	#[error("the passphrase source is not a terminal")]
	NotATerminal,
	#[error("passphrase entry was interrupted")]
	Interrupted,
	#[error("passphrase entry ended before a newline")]
	EndOfInput,
	#[error("the passphrase exceeds {max} bytes")]
	TooLong { max: usize },
}

impl CustodyError {
	pub(crate) fn io(what: impl Into<String>) -> impl FnOnce(std::io::Error) -> Self {
		let what = what.into();
		move |source| Self::Io { what, source }
	}
}
