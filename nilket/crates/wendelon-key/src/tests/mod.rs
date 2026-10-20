//! The tool's tests. Every fixture is a fresh directory in `TMPDIR`, admitted through key-guard's
//! `fixture-anchor` open (dev-dependency only); every command runs through the same `run` the
//! binary uses, with passphrases, seeds, randomness and the host probe scripted by `TestIo`.
mod format;
mod host_checks;
mod lifecycle;
mod log_chain;
mod passphrase_policy;
mod secrets;
mod support;
