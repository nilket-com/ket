//! `sign` and `sign-statement` (K3 refusals, K4 what-you-sign, K7 no reimplementation). The
//! facade builds the unsigned layout and the exact preimage and checks the result; this module
//! decrypts the key, shows the operator what the preimage commits to, takes the 32-hex code,
//! signs the preimage and writes the output only after the production check accepts it.
use super::bind::held_at;
use super::describe::{self, Decoded, MAX_COMMAND, MAX_PROOF};
use super::{confirm, overridden};
use crate::args::Args;
use crate::context::{self, read_bounded};
use crate::envelope::{Header, State, hex};
use crate::error::ToolError;
use crate::io::Io;
use crate::passphrase;
use crate::session::{self, Session, key_name, must_be_free, peek, read_key};
use ed25519_dalek::{Signer, SigningKey};
use key_guard::custody::SecretBuf;
use wendelon::storage::mutation_visibility::root2::Command2;
use wendelon::storage::mutation_visibility::root2::operator_proof::{
	self as facade, Unsigned, VerifiedContext,
};

/// A role flag value: admin, governance, recovery (or 1, 2, 3).
pub(super) fn role_of(flag: &str, value: &str) -> Result<u8, ToolError> {
	match value {
		"1" | "admin" => Ok(1),
		"2" | "governance" => Ok(2),
		"3" | "recovery" => Ok(3),
		other => Err(ToolError::Usage(format!(
			"{flag} {other:?}: admin, governance or recovery"
		))),
	}
}

/// An 8-character form name, uppercased: the facade judges it.
pub(super) fn form(value: &str) -> Result<[u8; 8], ToolError> {
	value
		.to_ascii_uppercase()
		.as_bytes()
		.try_into()
		.map_err(|_| ToolError::Usage(format!("{value:?} is not a form name")))
}

/// `--action grant|revoke` and `--permission-key ID32HEX:REVISION`.
pub(super) fn statement_fields(a: &Args) -> Result<(u8, [u8; 16], u64), ToolError> {
	let action = match a.value("--action")? {
		"grant" => 1,
		"revoke" => 2,
		other => {
			return Err(ToolError::Usage(format!(
				"--action {other:?}: grant or revoke"
			)));
		}
	};
	let key = a.value("--permission-key")?;
	let bad = || ToolError::Usage("--permission-key is <32 hex digits>:<revision>".into());
	let (id, revision) = key.split_once(':').ok_or_else(bad)?;
	let id = unhex::<16>(id).ok_or_else(bad)?;
	let revision = revision.parse().map_err(|_| bad())?;
	Ok((action, id, revision))
}

pub(super) fn unhex<const N: usize>(s: &str) -> Option<[u8; N]> {
	if s.len() != 2 * N || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
		return None;
	}
	let mut out = [0_u8; N];
	for (i, b) in out.iter_mut().enumerate() {
		*b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
	}
	Some(out)
}

/// The decrypted signing key `name` and the context it stands on: K3's file refusals. The
/// facade's `prepare` refuses the rest.
fn signer(
	io: &mut dyn Io,
	s: &mut Session<'_>,
	a: &Args,
) -> Result<(Header, SecretBuf, VerifiedContext), ToolError> {
	let name = key_name(a.value("--key")?)?;
	let bytes = read_key(s.dir, name)?;
	s.host_checks(peek(name, &bytes)?.role, overridden(a))?;
	let passphrase = passphrase::existing(io, &format!("Passphrase for {name}"))?;
	let (header, seed) = s.open_key(name, &bytes, &passphrase)?;
	s.identity(&header);
	if header.state != State::Bound || header.backup_of.is_some() {
		return Err(ToolError::refused("only a Bound primary key file signs"));
	}
	let ctx = context::load(&[a.path("--context")?], a.path("--trusted-manifest")?)?.remove(0);
	if header.context.as_ref() != Some(ctx.context()) {
		return Err(ToolError::refused(
			"the verified context is not the key file's context (its trust anchor)",
		));
	}
	held_at(&ctx, &header.key, header.role, header.epoch)?;
	Ok((header, seed, ctx))
}

fn signature(seed: &SecretBuf, preimage: &[u8]) -> [u8; 64] {
	let seed: &[u8; 32] = seed.as_slice().try_into().expect("a 32-byte seed");
	SigningKey::from_bytes(seed).sign(preimage).to_bytes()
}

fn say(io: &mut dyn Io, lines: &[String]) -> Result<(), ToolError> {
	for line in lines {
		writeln!(io.out(), "{line}").map_err(ToolError::read("stdout"))?;
	}
	Ok(())
}

/// SL2 for a role proof: a complete proof passes `self_check`; a first joint partial is accepted
/// by the facade as the partner's partial, which verifies the signed slot with the production
/// check.
fn checked(
	ctx: &VerifiedContext,
	d: &Decoded,
	command: &[u8],
	output: &[u8],
	role: u8,
	sidecar: Option<&[u8; 8]>,
) -> Result<(), ToolError> {
	let joint = output.starts_with(b"VISROL03") && output[150] != 0;
	let filled = |slot: usize| output[[86, 160][slot]..[150, 224][slot]] != [0; 64];
	if !joint || (filled(0) && filled(1)) {
		return facade::self_check(ctx, command, Some(output)).map_err(ToolError::Proof);
	}
	let partner = if output[76] == role {
		output[150]
	} else {
		output[76]
	};
	let with = matches!(d.command, Command2::RetireRoot | Command2::Unfreeze(_)).then_some(role);
	let key = ctx.active()[usize::from(partner - 1)].1;
	facade::prepare(ctx, command, &key, with, sidecar, Some(output))
		.map(|_| ())
		.map_err(ToolError::Proof)
}

pub(super) fn sign(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let out = key_name(a.value("--out")?)?;
	let (command_out, sidecar_out) = (format!("{out}.command"), format!("{out}.sidecar"));
	session::run(io, a.path("--dir")?, "sign", |io, s| {
		let (header, seed, ctx) = signer(io, s, a)?;
		let command_path = a.path("--command")?;
		let command = read_bounded(command_path, MAX_COMMAND)?;
		let d = describe::decode(&command_path.display().to_string(), &command, &ctx)?;
		s.line.command_digest = Some(hex(&d.digest));
		let partial = a
			.opt("--partial")
			.map(|p| read_bounded(std::path::Path::new(p), MAX_PROOF))
			.transpose()?;
		let with = a.opt("--with").map(|w| role_of("--with", w)).transpose()?;
		let sidecar = a.opt("--sidecar").map(form).transpose()?;
		let unsigned = facade::prepare(
			&ctx,
			&command,
			&header.key,
			with,
			sidecar.as_ref(),
			partial.as_deref(),
		)
		.map_err(ToolError::Proof)?;
		let slot = describe::form_slot(unsigned.layout(), header.role);
		s.line.form_slot = Some(slot.clone());
		must_be_free(s.dir, &command_out)?;
		must_be_free(s.dir, &sidecar_out)?;
		let (kind, fields) = describe::before(&d.command);
		let mut shown = vec![
			format!("command {kind}"),
			format!(
				"signer role {} epoch {} as {slot}",
				header.role, header.epoch
			),
		];
		shown.extend(fields.into_iter().map(|f| format!("  {f}")));
		say(io, &shown)?;
		let (code, source) = describe::code(&d);
		confirm(io, &format!("Type the 32-hex code ({source}): "), &code)?;
		say(io, &describe::after(Some(&d), unsigned.preimage()))?;
		let output = facade::splice(&unsigned, &signature(&seed, unsigned.preimage()))
			.map_err(ToolError::Proof)?;
		checked(&ctx, &d, &command, &output, header.role, sidecar.as_ref())?;
		write(io, s, &[(&command_out, &command), (&sidecar_out, &output)])
	})
}

pub(super) fn statement(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let out = key_name(a.value("--out")?)?;
	let command_out = format!("{out}.command");
	let (action, authority, revision) = statement_fields(a)?;
	let chosen = form(a.opt("--sidecar").unwrap_or("visads03"))?;
	session::run(io, a.path("--dir")?, "sign-statement", |io, s| {
		let (header, seed, ctx) = signer(io, s, a)?;
		let unsigned: Unsigned =
			facade::prepare_statement(&ctx, &header.key, &chosen, action, &authority, revision)
				.map_err(ToolError::Proof)?;
		let slot = describe::form_slot(unsigned.layout(), header.role);
		s.line.form_slot = Some(slot.clone());
		must_be_free(s.dir, &command_out)?;
		say(
			io,
			&[
				format!(
					"statement {} ({slot})",
					if action == 1 { "Grant" } else { "Revoke" }
				),
				format!("signer role {} epoch {}", header.role, header.epoch),
				format!("  authority {} revision {revision}", hex(&authority)),
				"  namespace: the administrator namespace of the verified root".into(),
			],
		)?;
		let code = describe::statement_code(unsigned.preimage());
		confirm(
			io,
			"Type the 32-hex code (the statement preimage hash's first 32 hex digits, from build statement): ",
			&code,
		)?;
		say(io, &describe::after(None, unsigned.preimage()))?;
		let output = facade::splice(&unsigned, &signature(&seed, unsigned.preimage()))
			.map_err(ToolError::Proof)?;
		facade::self_check(&ctx, &output, None).map_err(ToolError::Proof)?;
		let d = describe::decode("the signed statement", &output, &ctx)?;
		s.line.command_digest = Some(hex(&d.digest));
		say(io, &[format!("command digest {}", hex(&d.digest))])?;
		write(io, s, &[(&command_out, &output)])
	})
}

/// Writes each output create-new 0600 in the custody directory and reports its digest.
pub(super) fn write(
	io: &mut dyn Io,
	s: &mut Session<'_>,
	files: &[(&str, &[u8])],
) -> Result<(), ToolError> {
	for (name, bytes) in files {
		s.lock.create_file(name, bytes)?;
		s.output(name, bytes);
		writeln!(
			io.out(),
			"wrote {name} sha256 {}",
			hex(&crate::envelope::file_digest(bytes))
		)
		.map_err(ToolError::read("stdout"))?;
	}
	Ok(())
}
