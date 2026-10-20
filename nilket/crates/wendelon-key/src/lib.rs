//! The offline custody tool for Root2 role signing keys (key custody contract r4).
//!
//! A role key lives in one encrypted file (K3, [`envelope`]) in an owner-only custody directory
//! (K2, through key-guard). `generate` writes it Pending; `bind` re-encrypts it Bound once a
//! re-verified context export shows its key at its epoch; `backup`, `restore` and
//! `restore-verify` copy it under a second passphrase; `retire-epoch` and `destroy` end it. Every
//! operation that reaches the locked directory appends one line to the hash-chained
//! `custody.log`. Signing arrives in a later slice.
//!
//! Every system call goes through key-guard: this crate has no unsafe code and no network. Every
//! judgement about a context or a key's place in a root's history comes from wendelon's
//! operator proof facade; nothing here reimplements it.
#![forbid(unsafe_code)]

mod args;
mod commands;
mod context;
mod envelope;
mod error;
mod host;
mod io;
mod log;
mod passphrase;
mod session;
#[cfg(test)]
mod tests;

use std::process::ExitCode;

/// The binary's entry: the terminal-restore hook and process hardening come first, then the
/// command. Any refusal prints one line on stderr and exits 2.
pub fn main_entry() -> ExitCode {
	if let Err(e) = startup() {
		eprintln!("wendelon-key: {e}");
		return ExitCode::from(2);
	}
	let args: Vec<String> = match std::env::args_os()
		.skip(1)
		.map(|a| a.into_string())
		.collect()
	{
		Ok(args) => args,
		Err(_) => {
			eprintln!("wendelon-key: an argument is not UTF-8");
			return ExitCode::from(2);
		}
	};
	let mut io = io::SystemIo::new();
	commands::run(&args, &mut io)
}

/// Restores the terminal on a panic (release builds abort on one, so no destructor runs), and
/// makes the process non-dumpable with no core file (K3 process hardening).
pub fn startup() -> Result<(), key_guard::custody::CustodyError> {
	key_guard::custody::install_terminal_restore_hook();
	key_guard::custody::harden_process()
}
