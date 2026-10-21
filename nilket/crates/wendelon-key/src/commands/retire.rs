//! K6: `retire-epoch` and `destroy`. Retired is derived, never stored: an epoch is retired when a
//! verified context shows a later epoch for its role. Destruction overwrites and unlinks on a
//! best-effort basis; it is hygiene, not secure erasure (the runbook states the limits), and a
//! superseded key cannot authorize anyway.
use super::bind::held_at;
use super::{confirm, overridden};
use crate::args::Args;
use crate::context;
use crate::envelope::{ADMITTED, Header, State, fingerprint, hex};
use crate::error::ToolError;
use crate::io::Io;
use crate::passphrase;
use crate::session::{
	self, PENDING, RETIRED, existing_subdir, key_name, must_be_free, peek, read_key,
};
use wendelon::storage::mutation_visibility::root2::operator_proof::VerifiedContext;

/// Refuses unless `header`'s epoch is retired in `ctx`: a later epoch of its role is active.
fn retired_in(ctx: &VerifiedContext, header: &Header) -> Result<u64, ToolError> {
	let active = ctx.active()[usize::from(header.role - 1)].0;
	if active <= header.epoch {
		return Err(ToolError::refused(format!(
			"role {} epoch {} is not retired: the verified context shows no later epoch",
			header.role, header.epoch
		)));
	}
	Ok(active)
}

/// The Bound header's own context must be the verified one, and its key the history's key there.
fn bound_in(ctx: &VerifiedContext, header: &Header) -> Result<(), ToolError> {
	if header.state != State::Bound || header.backup_of.is_some() {
		return Err(ToolError::refused(
			"this operation takes a Bound primary key file",
		));
	}
	if header.context.as_ref() != Some(ctx.context()) {
		return Err(ToolError::refused(
			"the verified context is not the file's context",
		));
	}
	held_at(ctx, &header.key, header.role, header.epoch)
}

pub(super) fn retire(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let name = key_name(a.value("--key")?)?;
	let context_path = a.path("--context")?;
	let manifest = a.path("--trusted-manifest")?;
	session::run(io, a.path("--dir")?, "retire-epoch", |io, s| {
		let header = peek(name, &read_key(s.dir, name)?)?;
		// Retiring decrypts nothing: the identity is the header's claim, logged as such.
		s.line.unauthenticated_identity = Some(hex(&header.identity()));
		let ctx = context::load(&[context_path], manifest)?.remove(0);
		bound_in(&ctx, &header)?;
		retired_in(&ctx, &header)?;
		let id = hex(&header.identity());
		let backups: Vec<String> = s
			.history
			.iter()
			.filter(|l| {
				l.operation == "backup" && l.outcome == "ok" && l.backup_of.as_ref() == Some(&id)
			})
			.flat_map(|l| l.outputs.iter().map(|o| o.name.clone()))
			.collect();
		let count = backups.len().to_string();
		confirm(
			io,
			&format!(
				"{count} backups are recorded for this identity ({}). Type {count} to confirm each is accounted for: ",
				if backups.is_empty() {
					"none".into()
				} else {
					backups.join(", ")
				}
			),
			&count,
		)?;
		let retired = match existing_subdir(s.dir, RETIRED)? {
			Some(d) => d,
			None => s.lock.create_subdir(RETIRED)?,
		};
		// The destination's lock first, so its name check runs under the lock that covers the move.
		let retired_lock = retired.lock()?;
		must_be_free(&retired, name)?;
		s.lock.move_file(name, &retired_lock, name)?;
		writeln!(
			io.out(),
			"retired role {} epoch {}: moved to {RETIRED}/{name}",
			header.role,
			header.epoch
		)
		.map_err(ToolError::read("stdout"))
	})
}

pub(super) fn destroy(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let name = key_name(a.value("--key")?)?;
	let contexts = a.paths("--context");
	if contexts.is_empty() {
		return Err(ToolError::Usage("--context is required".into()));
	}
	let manifest = a.path("--trusted-manifest")?;
	let pending = a.switch("--pending");
	if pending && (a.switch("--retired") || a.opt("--successor").is_some() || contexts.len() > 1) {
		return Err(ToolError::Usage(
			"--pending takes one --context and the --evidence flags only".into(),
		));
	}
	let operation = if pending {
		"destroy-pending"
	} else {
		"destroy"
	};
	session::run(io, a.path("--dir")?, operation, |io, s| {
		let sub = if pending {
			Some(PENDING)
		} else if a.switch("--retired") {
			Some(RETIRED)
		} else {
			None
		};
		let held = match sub {
			Some(sub) => Some(
				existing_subdir(s.dir, sub)?
					.ok_or_else(|| ToolError::refused(format!("there is no {sub}/ directory")))?,
			),
			None => None,
		};
		let dir = held.as_ref().unwrap_or(s.dir);
		let bytes = read_key(dir, name)?;
		s.host_checks(peek(name, &bytes)?.role, overridden(a))?;
		let passphrase = passphrase::existing(io, &format!("Passphrase for {name}"))?;
		let (header, _seed) = s.open_key(name, &bytes, &passphrase)?;
		s.identity(&header);
		let verified = context::load(&contexts, manifest)?;
		let ctx = &verified[0];
		if pending {
			pending_evidence(s, a, ctx, &header)?;
		} else {
			bound_in(ctx, &header)?;
			let later = retired_in(ctx, &header)?;
			second_source(io, s, a, &verified, &header, later)?;
		}
		confirm(
			io,
			&format!(
				"Destroying role {} epoch {} key {}. Type its fingerprint to confirm: ",
				header.role,
				header.epoch,
				hex(&header.key)
			),
			&fingerprint(&header.key),
		)?;
		match &held {
			Some(d) => d.lock()?.destroy_file(name, &ADMITTED)?,
			None => s.lock.destroy_file(name, &ADMITTED)?,
		}
		writeln!(
			io.out(),
			"destroyed {}{name} (overwritten and unlinked; not secure erasure)",
			sub.map_or(String::new(), |d| format!("{d}/"))
		)
		.map_err(ToolError::read("stdout"))
	})
}

/// `destroy --pending`: the Pending original's key must be its role's active key in a verified
/// context, and the operator records the runbook's evidence that the bound identity has been used
/// (an accepted proof, or a `crypto_valid` C-S6 row).
fn pending_evidence(
	s: &mut session::Session<'_>,
	a: &Args,
	ctx: &VerifiedContext,
	header: &Header,
) -> Result<(), ToolError> {
	if header.state != State::Pending || header.backup_of.is_some() {
		return Err(ToolError::refused("--pending takes a Pending original"));
	}
	if header.context.is_some_and(|c| c != *ctx.context()) {
		return Err(ToolError::refused(
			"the verified context is not the Pending file's context",
		));
	}
	let Some((role, epoch)) = ctx.identity(&header.key) else {
		return Err(ToolError::refused(
			"the verified history does not hold this key",
		));
	};
	if role != header.role || ctx.active()[usize::from(role - 1)] != (epoch, header.key) {
		return Err(ToolError::refused(
			"the bound identity's epoch is not active in the verified context",
		));
	}
	let kind = a.value("--evidence")?;
	if !["accepted-proof", "crypto-valid-row"].contains(&kind) {
		return Err(ToolError::Usage(
			"--evidence is accepted-proof or crypto-valid-row".into(),
		));
	}
	let digest = a.value("--evidence-digest")?;
	if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
		return Err(ToolError::Usage(
			"--evidence-digest is 64 hex digits".into(),
		));
	}
	s.line.evidence = Some(format!("{kind} {}", digest.to_ascii_lowercase()));
	Ok(())
}

/// K6: a verified-but-denied successor cannot be told apart offline from an applied one, so
/// destroying needs a second source: another node's export showing the same context and later
/// epoch, or the successor's own key file decrypting here to a later epoch's history key.
fn second_source(
	io: &mut dyn Io,
	s: &mut session::Session<'_>,
	a: &Args,
	verified: &[VerifiedContext],
	header: &Header,
	later: u64,
) -> Result<(), ToolError> {
	let role = usize::from(header.role - 1);
	if let Some(second) = verified.get(1) {
		context::second_source(&verified[0], second)?;
		if second.active()[role] != verified[0].active()[role] {
			return Err(ToolError::refused(
				"the second export shows another active epoch for this role",
			));
		}
		s.line.evidence = Some("second-export".into());
		return Ok(());
	}
	let Some(successor) = a.opt("--successor") else {
		return Err(ToolError::refused(
			"destroy needs second-source evidence: a second --context from another node, or --successor <the successor's key file>",
		));
	};
	let successor = key_name(successor)?;
	let bytes = read_key(s.dir, successor)?;
	s.host_checks(peek(successor, &bytes)?.role, overridden(a))?;
	let passphrase =
		passphrase::existing(io, &format!("Passphrase for the successor {successor}"))?;
	let (next, _seed) = s.open_key(successor, &bytes, &passphrase)?;
	bound_in(&verified[0], &next)?;
	if next.role != header.role || next.epoch <= header.epoch || next.epoch > later {
		return Err(ToolError::refused(
			"the successor file is not a later epoch of this role",
		));
	}
	s.line.evidence = Some(format!("successor {}", hex(&next.identity())));
	Ok(())
}
