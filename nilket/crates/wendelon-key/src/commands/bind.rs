//! `bind`: Pending → Bound (contract §2). The Pending file is decrypted first, so every decision
//! below rests on its authenticated header. The re-verified context must hold the key at the
//! role and epoch; with no successor (genesis) nothing authenticates the context, so a second
//! export from another node with byte-identical context bytes is required. The Bound file is a
//! new file under the same passphrase with a fresh salt and nonce; the Pending original is kept in
//! `pending/`.
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
use wendelon::storage::mutation_visibility::root2::operator_proof::VerifiedContext;

/// The verified context a bind or a check stands on: the first export, plus the second-source
/// rule. With a successor the chain authenticates the context and a second export is optional
/// (but must agree); at genesis it is required.
pub(crate) fn anchored_context(
	verified: &[VerifiedContext],
) -> Result<&VerifiedContext, ToolError> {
	let first = &verified[0];
	match verified.get(1) {
		Some(second) => context::second_source(first, second)?,
		None if !first.context_authenticated() => {
			return Err(ToolError::refused(
				"no successor authenticates this context (genesis); give a second --context export from a different node",
			));
		}
		None => {}
	}
	Ok(first)
}

/// Refuses unless the verified history holds `key` exactly as `role` at `epoch`.
pub(crate) fn held_at(
	ctx: &VerifiedContext,
	key: &[u8; 32],
	role: u8,
	epoch: u64,
) -> Result<(), ToolError> {
	match ctx.identity(key) {
		Some((r, e)) if (r, e) == (role, epoch) => Ok(()),
		Some((r, e)) => Err(ToolError::refused(format!(
			"the verified history holds this key as role {r} epoch {e}, not role {role} epoch {epoch}"
		))),
		None => Err(ToolError::refused(
			"the verified history does not hold this key",
		)),
	}
}

pub(super) fn run(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let name = key_name(a.value("--key")?)?;
	let out = key_name(a.value("--out")?)?;
	let contexts = a.paths("--context");
	if contexts.is_empty() {
		return Err(ToolError::Usage("--context is required".into()));
	}
	let manifest = a.path("--trusted-manifest")?;
	let chosen = a.opt("--epoch").map(|_| a.number("--epoch")).transpose()?;
	session::run(io, a.path("--dir")?, "bind", |io, s| {
		let bytes = read_key(s.dir, name)?;
		let unauthenticated = peek(name, &bytes)?;
		s.host_checks(unauthenticated.role, overridden(a))?;
		let passphrase = passphrase::existing(io, &format!("Passphrase for {name}"))?;
		let (pending, seed) = s.open_key(name, &bytes, &passphrase)?;
		s.line.source = Some(hex(&pending.identity()));
		if pending.state != State::Pending || pending.backup_of.is_some() {
			return Err(ToolError::refused("bind takes a Pending primary key file"));
		}
		let verified = context::load(&contexts, manifest)?;
		let ctx = anchored_context(&verified)?;
		if pending.context.is_some_and(|c| c != *ctx.context()) {
			return Err(ToolError::refused(
				"the Pending file was generated for another context",
			));
		}
		let epoch = chosen.unwrap_or(pending.epoch);
		held_at(ctx, &pending.key, pending.role, epoch)?;
		let pending_dir = match existing_subdir(s.dir, PENDING)? {
			Some(d) => d,
			None => s.lock.create_subdir(PENDING)?,
		};
		must_be_free(&pending_dir, name)?;
		// The destination's lock first: a busy pending/ refuses before the Bound file exists.
		let pending_lock = pending_dir.lock()?;
		must_be_free(s.dir, out)?;
		let bound = fresh(
			io,
			Header {
				state: State::Bound,
				epoch,
				context: Some(*ctx.context()),
				..pending.clone()
			},
		)?;
		s.identity(&bound);
		let file = envelope::seal(&bound, &seed, &passphrase)?;
		s.lock.create_file(out, &file)?;
		s.output(out, &file);
		s.lock.move_file(name, &pending_lock, name)?;
		writeln!(
			io.out(),
			"bound role {} epoch {epoch}: {} (fingerprint {}); the Pending original is in {PENDING}/{name}",
			bound.role,
			hex(&bound.key),
			fingerprint(&bound.key)
		)
		.map_err(ToolError::read("stdout"))
	})
}
