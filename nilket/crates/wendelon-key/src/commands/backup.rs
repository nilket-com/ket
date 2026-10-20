//! K5: encrypted backups. `backup` writes a second envelope of the same format under a backup
//! passphrase, which must differ from the primary, with `backup_of` naming the primary's
//! identity. `restore` decrypts a backup and re-encrypts it under a new primary passphrase with a
//! fresh salt and nonce. `restore-verify` decrypts in memory only and checks the backup against a
//! Bound file and a verified context.
use super::bind::held_at;
use super::overridden;
use crate::args::Args;
use crate::context;
use crate::envelope::{self, Header, State, fingerprint, hex};
use crate::error::ToolError;
use crate::io::Io;
use crate::passphrase;
use crate::session::{
	self, PENDING, existing_subdir, fresh, key_name, must_be_free, peek, read_key,
};

pub(super) fn backup(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let name = key_name(a.value("--key")?)?;
	let out = key_name(a.value("--out")?)?;
	let to = a.path("--to")?;
	session::run(io, a.path("--dir")?, "backup", |io, s| {
		// Pending originals kept by `bind` are backed up from `pending/` under this directory's lock.
		let held = if a.switch("--pending") {
			Some(
				existing_subdir(s.dir, PENDING)?
					.ok_or_else(|| ToolError::refused("there is no pending/ directory"))?,
			)
		} else {
			None
		};
		let bytes = read_key(held.as_ref().unwrap_or(s.dir), name)?;
		s.host_checks(peek(name, &bytes)?.role, overridden(a))?;
		let primary = passphrase::existing(io, &format!("Passphrase for {name}"))?;
		let (header, seed) = s.open_key(name, &bytes, &primary)?;
		s.identity(&header);
		if header.backup_of.is_some() {
			return Err(ToolError::refused(
				"back up a primary key file, not a backup",
			));
		}
		let target = io.open_dir(to)?;
		let same = target.identity() == s.dir.identity();
		must_be_free(&target, out)?;
		let secondary = passphrase::new(io, "backup passphrase")?;
		if secondary.as_slice() == primary.as_slice() {
			return Err(ToolError::BackupPassphraseReused);
		}
		let id = header.identity();
		let copy = fresh(
			io,
			Header {
				backup_of: Some(id),
				..header
			},
		)?;
		let file = envelope::seal(&copy, &seed, &secondary)?;
		if same {
			s.lock.create_file(out, &file)?;
		} else {
			target.lock()?.create_file(out, &file)?;
		}
		s.line.backup_of = Some(hex(&id));
		s.output(&format!("{}/{out}", to.display()), &file);
		writeln!(
			io.out(),
			"backup of {} written to {}/{out}",
			hex(&id),
			to.display()
		)
		.map_err(ToolError::read("stdout"))
	})
}

/// The backup `name` in the directory `from`, decrypted under the backup passphrase: its
/// authenticated header (which must be a backup consistent with its own identity) and seed.
fn open_backup(
	io: &mut dyn Io,
	s: &mut session::Session<'_>,
	a: &Args,
) -> Result<(Header, key_guard::custody::SecretBuf), ToolError> {
	let from = a.path("--from")?;
	let name = key_name(a.value("--backup")?)?;
	let dir = io.open_dir(from)?;
	let bytes = read_key(&dir, name)?;
	s.host_checks(peek(name, &bytes)?.role, overridden(a))?;
	let passphrase = passphrase::existing(io, &format!("Backup passphrase for {name}"))?;
	let (header, seed) = s.open_key(name, &bytes, &passphrase)?;
	s.line.source = Some(hex(&header.identity()));
	if header.backup_of != Some(header.identity()) {
		return Err(ToolError::refused(format!(
			"{name} is not a backup of its own identity"
		)));
	}
	s.line.backup_of = Some(hex(&header.identity()));
	Ok((header, seed))
}

pub(super) fn restore(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let out = key_name(a.value("--out")?)?;
	session::run(io, a.path("--dir")?, "restore", |io, s| {
		let (backup, seed) = open_backup(io, s, a)?;
		must_be_free(s.dir, out)?;
		let primary = passphrase::new(io, "passphrase")?;
		let restored = fresh(
			io,
			Header {
				backup_of: None,
				..backup
			},
		)?;
		s.identity(&restored);
		let file = envelope::seal(&restored, &seed, &primary)?;
		s.lock.create_file(out, &file)?;
		s.output(out, &file);
		writeln!(
			io.out(),
			"restored {} {} (fingerprint {}) as {out}",
			restored.state.name(),
			hex(&restored.identity()),
			fingerprint(&restored.key)
		)
		.map_err(ToolError::read("stdout"))
	})
}

pub(super) fn verify(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let name = key_name(a.value("--key")?)?;
	let context_path = a.path("--context")?;
	let manifest = a.path("--trusted-manifest")?;
	session::run(io, a.path("--dir")?, "restore-verify", |io, s| {
		let bound = peek(name, &read_key(s.dir, name)?)?;
		// The Bound file is not decrypted: its identity is a claim until the authenticated backup
		// matches it.
		s.line.unauthenticated_identity = Some(hex(&bound.identity()));
		if bound.state != State::Bound {
			return Err(ToolError::refused(
				"restore-verify checks a backup against a Bound file",
			));
		}
		let (backup, _seed) = open_backup(io, s, a)?;
		if backup.identity() != bound.identity() {
			return Err(ToolError::refused(
				"the backup's identity is not the Bound file's identity",
			));
		}
		s.identity(&backup);
		let ctx = context::load(&[context_path], manifest)?.remove(0);
		if backup.context.as_ref() != Some(ctx.context()) {
			return Err(ToolError::refused(
				"the verified context is not the backup's context",
			));
		}
		held_at(&ctx, &backup.key, backup.role, backup.epoch)?;
		writeln!(
			io.out(),
			"backup verified: {} role {} epoch {} (fingerprint {})",
			hex(&backup.identity()),
			backup.role,
			backup.epoch,
			fingerprint(&backup.key)
		)
		.map_err(ToolError::read("stdout"))
	})
}
