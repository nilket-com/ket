//! What-you-sign (K4): the decoded display of a command and its 32-hex (128-bit) confirmation code.
//! Everything here reads the one command buffer the signature will be over.
use crate::envelope::{fingerprint, hex};
use crate::error::ToolError;
use aws_lc_rs::digest::{SHA256, digest};
use wendelon::storage::mutation_visibility::DecisionLogId;
use wendelon::storage::mutation_visibility::root2::Command2;
use wendelon::storage::mutation_visibility::root2::operator_proof::VerifiedContext;

/// Commands and partial proofs are bounded; anything larger is refused unread.
pub(super) const MAX_COMMAND: u64 = 1 << 20;
pub(super) const MAX_PROOF: u64 = 4096;
/// The what-you-sign code's length in hex digits: 128 bits, so an attacker who alters a command
/// file in transit cannot search for another command with the same prefix.
pub(super) const CODE_HEX: usize = 32;

/// A command decoded under the context's deployment, with its digest.
pub(super) struct Decoded {
	pub(super) command: Command2,
	pub(super) digest: [u8; 32],
}

/// The context's (generation, cluster): the first 32 bytes of its 188.
pub(super) fn deployment(ctx: &VerifiedContext) -> ([u8; 16], [u8; 16]) {
	let c = ctx.context();
	(
		c[0..16].try_into().expect("generation width"),
		c[16..32].try_into().expect("cluster width"),
	)
}

pub(super) fn decode(
	what: &str,
	bytes: &[u8],
	ctx: &VerifiedContext,
) -> Result<Decoded, ToolError> {
	let bad = || ToolError::Command {
		what: what.to_string(),
	};
	let (g, c, command) = Command2::decode(bytes).map_err(|_| bad())?;
	if (g, c) != deployment(ctx) {
		return Err(ToolError::refused(format!(
			"{what} is for another deployment than the verified context"
		)));
	}
	let digest = command.digest(g, c).map_err(|_| bad())?;
	Ok(Decoded { command, digest })
}

fn log(l: &DecisionLogId) -> String {
	format!("term {} node {} index {}", l.term, l.node, l.index)
}

/// The new role key a rotation enrolls.
pub(super) fn new_key(command: &Command2) -> Option<(&'static str, [u8; 32])> {
	match command {
		Command2::RotateAdmin(k) => Some(("administrator", *k)),
		Command2::RotateGovernance(k) => Some(("governance", *k)),
		Command2::RotateRecovery(k) => Some(("recovery", *k)),
		_ => None,
	}
}

/// The kind and the decoded fields shown before confirmation. A rotation's new key is withheld.
pub(super) fn before(command: &Command2) -> (String, Vec<String>) {
	let (kind, fields): (&str, Vec<String>) = match command {
		Command2::RotateAdmin(_) | Command2::RotateGovernance(_) | Command2::RotateRecovery(_) => {
			let (role, _) = new_key(command).expect("a rotation");
			let kind = match role {
				"administrator" => "RotateAdmin",
				"governance" => "RotateGovernance",
				_ => "RotateRecovery",
			};
			(
				kind,
				vec![format!(
					"new {role} key: withheld until the code is confirmed"
				)],
			)
		}
		Command2::Freeze(b) => ("Freeze", vec![format!("binding (transition): {}", log(b))]),
		Command2::Unfreeze(b) => (
			"Unfreeze",
			vec![format!("binding (transition): {}", log(b))],
		),
		Command2::RetireRoot => (
			"RetireRoot",
			vec![
				"TERMINAL: retiring the root ends every further Root2 action; it cannot be undone"
					.into(),
			],
		),
		Command2::EnrollSelector(b) => (
			"EnrollSelector",
			vec![format!(
				"tenant {} key id {} version {}",
				b.tenant,
				hex(&b.key_id),
				b.key_version
			)],
		),
		Command2::EnrollAuthority {
			authority,
			node,
			key,
		} => (
			"EnrollAuthority",
			vec![
				format!(
					"authority {} revision {}",
					hex(&authority.id),
					authority.revision
				),
				format!("node {} incarnation {}", node.node, hex(&node.incarnation)),
				format!("authority key {}", hex(key)),
			],
		),
		Command2::RevokeAuthority(a) => (
			"RevokeAuthority",
			vec![format!("authority {} revision {}", hex(&a.id), a.revision)],
		),
		Command2::EnrollLeaseIssuer { node, key } => (
			"EnrollLeaseIssuer",
			vec![format!("node {node} issuer key {}", hex(key))],
		),
		Command2::RevokeLeaseIssuer { node } => ("RevokeLeaseIssuer", vec![format!("node {node}")]),
		Command2::Grant { lineage, lease, .. } => (
			"Grant",
			vec![format!(
				"lineage {} epoch {} holder {} range {}..{} issuer {}",
				hex(lineage),
				lease.epoch,
				lease.holder,
				lease.range_start,
				lease.range_end,
				lease.issuer
			)],
		),
		Command2::Release { lineage, latest } => (
			"Release",
			vec![format!(
				"lineage {} latest grant {}",
				hex(lineage),
				log(&latest.log)
			)],
		),
		Command2::Cut {
			expected,
			next,
			configuration,
		} => (
			"Cut",
			vec![format!(
				"epoch {expected} -> {next}, configuration {}",
				hex(configuration)
			)],
		),
		Command2::Resolve { digest, cause, .. } => (
			"Resolve",
			vec![format!("mutation digest {} cause {cause}", hex(digest))],
		),
		Command2::Bootstrap { .. } => ("Bootstrap", vec![]),
		Command2::Cover { .. } => ("Cover", vec![]),
		Command2::Reserve(_) => ("Reserve", vec![]),
		Command2::Finalize { .. } => ("Finalize", vec![]),
		Command2::AdmissionPermission(_) => ("AdmissionPermission", vec![]),
		Command2::EpochAdmissionPermission(_) => ("EpochAdmissionPermission", vec![]),
	};
	(kind.to_string(), fields)
}

/// The 32-hex code the operator types: a rotation's new key prefix (from that key's
/// authenticated `show-public`), otherwise the command digest prefix (from `build`).
pub(super) fn code(d: &Decoded) -> (String, &'static str) {
	match new_key(&d.command) {
		Some((_, key)) => (
			hex(&key)[..CODE_HEX].to_string(),
			"the new key's first 32 hex digits, from its authenticated show-public",
		),
		None => (
			hex(&d.digest)[..CODE_HEX].to_string(),
			"the command digest's first 32 hex digits, from build on the requesting host",
		),
	}
}

/// The code of an administrator statement: SHA-256 of the facade's unsigned preimage.
pub(super) fn statement_code(preimage: &[u8]) -> String {
	hex(&digest(&SHA256, preimage).as_ref()[..CODE_HEX / 2])
}

/// The lines shown after the code matches: the withheld values, then exactly what is signed.
pub(super) fn after(d: Option<&Decoded>, preimage: &[u8]) -> Vec<String> {
	let mut lines = Vec::new();
	if let Some(d) = d {
		if let Some((role, key)) = new_key(&d.command) {
			lines.push(format!(
				"new {role} key {} fingerprint {}",
				hex(&key),
				fingerprint(&key)
			));
		}
		lines.push(format!("command digest {}", hex(&d.digest)));
	}
	lines.push(format!(
		"preimage sha256 {}",
		hex(digest(&SHA256, preimage).as_ref())
	));
	lines
}

/// The form and slot a layout carries: its 8-byte form name and, for `VISROL03`, the role pair
/// and the signer's slot.
pub(super) fn form_slot(layout: &[u8], role: u8) -> String {
	let form = String::from_utf8_lossy(&layout[..8]).into_owned();
	if &layout[..8] == b"VISROL03" {
		let roles = [layout[76], layout[150]];
		let slot = roles
			.iter()
			.position(|r| *r == role)
			.map_or("?".into(), |s| s.to_string());
		format!("{form} [{},{}] slot {slot}", roles[0], roles[1])
	} else {
		form
	}
}
