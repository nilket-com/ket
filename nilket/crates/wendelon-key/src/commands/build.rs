//! `build`: the canonical bytes of a role command, from a verified context, with no key involved.
//! It prints the command digest and the what-you-sign code the signing host will ask for.
//! `build statement` writes nothing and prints a statement's code. The nine governance kinds are
//! not built here: their fields belong to the requesting node's own tooling, and `sign` takes
//! their canonical bytes from any producer.
use super::describe::{self, Decoded, deployment};
use super::sign::{form, statement_fields, unhex, write};
use crate::args::Args;
use crate::context;
use crate::envelope::hex;
use crate::error::ToolError;
use crate::io::Io;
use crate::session::{self, key_name, must_be_free};
use wendelon::storage::mutation_visibility::DecisionLogId;
use wendelon::storage::mutation_visibility::root2::Command2;
use wendelon::storage::mutation_visibility::root2::operator_proof::{
	self as facade, check_new_role_key,
};

pub(super) const KINDS: &str =
	"rotate-admin, rotate-governance, rotate-recovery, freeze, unfreeze, retire-root or statement";

fn binding(raw: [u8; 20]) -> DecisionLogId {
	DecisionLogId {
		term: u64::from_le_bytes(raw[..8].try_into().expect("term width")),
		node: u32::from_le_bytes(raw[8..12].try_into().expect("node width")),
		index: u64::from_le_bytes(raw[12..].try_into().expect("index width")),
	}
}

pub(super) fn run(io: &mut dyn Io, a: &Args, kind: &str) -> Result<(), ToolError> {
	let rotation = match kind {
		"rotate-admin" => Some(1),
		"rotate-governance" => Some(2),
		"rotate-recovery" => Some(3),
		"freeze" | "unfreeze" | "retire-root" | "statement" => None,
		other => return Err(ToolError::Usage(format!("unknown kind {other:?}; {KINDS}"))),
	};
	let out = match kind {
		"statement" => None,
		_ => Some(format!("{}.command", key_name(a.value("--out")?)?)),
	};
	session::run(io, a.path("--dir")?, "build", |io, s| {
		let ctx = context::load(&[a.path("--context")?], a.path("--trusted-manifest")?)?.remove(0);
		if kind == "statement" {
			let (action, authority, revision) = statement_fields(a)?;
			let chosen = form(a.opt("--sidecar").unwrap_or("visads03"))?;
			let admin = ctx.active()[0].1;
			let unsigned =
				facade::prepare_statement(&ctx, &admin, &chosen, action, &authority, revision)
					.map_err(ToolError::Proof)?;
			let code = describe::statement_code(unsigned.preimage());
			s.line.form_slot = Some(describe::form_slot(unsigned.layout(), 1));
			return writeln!(io.out(), "statement code {code}").map_err(ToolError::read("stdout"));
		}
		let command = match (kind, rotation) {
			(_, Some(role)) => {
				let key = unhex::<32>(a.value("--key")?)
					.ok_or_else(|| ToolError::Usage("--key is 64 hex digits".into()))?;
				check_new_role_key(Some(&ctx), role, &key).map_err(ToolError::Key)?;
				match role {
					1 => Command2::RotateAdmin(key),
					2 => Command2::RotateGovernance(key),
					_ => Command2::RotateRecovery(key),
				}
			}
			("freeze", _) => Command2::Freeze(binding(ctx.transition().freeze_binding())),
			("unfreeze", _) => Command2::Unfreeze(binding(ctx.transition().freeze_binding())),
			_ => Command2::RetireRoot,
		};
		let (g, c) = deployment(&ctx);
		let bytes = command.encode(g, c).map_err(|_| ToolError::Command {
			what: kind.to_string(),
		})?;
		let digest = command.digest(g, c).map_err(|_| ToolError::Command {
			what: kind.to_string(),
		})?;
		s.line.command_digest = Some(hex(&digest));
		let out = out.as_deref().expect("a command kind names its output");
		must_be_free(s.dir, out)?;
		write(io, s, &[(out, &bytes)])?;
		let (code, source) = describe::code(&Decoded { command, digest });
		writeln!(
			io.out(),
			"command digest {}\ncode {code} ({source})",
			hex(&digest)
		)
		.map_err(ToolError::read("stdout"))
	})
}
