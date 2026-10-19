//! The Root2 context export: what a complete audit fold established, in the versioned `WRCTXEXP`
//! form the offline custody tool re-verifies. It is evidence, not trust. A consumer holding the
//! trusted manifest re-verifies every successor's committed rotation and proof; every other field
//! (the reducer's freeze, transition and retirement state, the activation coordinates, the stop,
//! the coverage, the node id and the completeness of the list) is audit-derived and usable only to
//! fail safe. Pure bytes: the CLI owns the file, and nothing here reads or writes the data dir.
//!
//! `LogId20 := term u64 LE | node u32 LE | index u64 LE`. Layout: magic, version u16 = 1, reserved
//! u16 = 0, the 188-byte context, the 241-byte manifest, node id u32, stop LogId20, coverage u8 = 1,
//! frozen u8, transition and retired as `present u8 | LogId20` (all-zero when absent), count u32,
//! then per successor in log order `role u8 | epoch u64 | key 32 | activation LogId20 | command_len
//! u32 | command | sidecar_len u32 | sidecar`, and a SHA-256 over every preceding byte (integrity
//! against truncation, not authentication).
use super::trace::Traced2;
use crate::consensus::{ROOT2_COMMAND_ENVELOPE_MAX1, WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::admin_install::admin_context2;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::role_command::{ROLE_PROOF_BYTES3, context_bytes};
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};
use openraft::{Entry, EntryPayload};
use std::collections::BTreeMap;

const MAGIC: &[u8; 8] = b"WRCTXEXP";
const VERSION: u16 = 1;
/// The canonical profile-3 manifest width.
const MANIFEST_BYTES: usize = 241;
/// At most 64 successors per role (the reducer's own bound), so 192 in all.
const ROLE_SUCCESSORS: usize = 64;
const SUCCESSORS: usize = 3 * ROLE_SUCCESSORS;

/// A rotation carrier's position and committed bytes: the command envelope and its sidecar.
pub(super) type Rotation2 = (Log2, Vec<u8>, Option<Vec<u8>>);

/// The rotation's role code (1 admin, 2 governance, 3 recovery) and enrolled key.
fn rotation(command: &Command2) -> Option<(u8, Digest2)> {
	match command {
		Command2::RotateAdmin(key) => Some((1, *key)),
		Command2::RotateGovernance(key) => Some((2, *key)),
		Command2::RotateRecovery(key) => Some((3, *key)),
		_ => None,
	}
}
/// Every rotation carrier in `entries` by index, with its exact committed bytes. Read before the
/// fold consumes the entries; only the ones the fold activated are exported.
pub(super) fn rotations2(
	entries: &BTreeMap<u64, Entry<WendelonTypeConfig>>,
) -> BTreeMap<u64, Rotation2> {
	entries
		.iter()
		.filter_map(|(index, entry)| {
			let EntryPayload::Normal(WendelonRequest::Root2Command(carrier)) = &entry.payload
			else {
				return None;
			};
			let (_, _, command) = Command2::decode(carrier.command()).ok()?;
			rotation(&command)?;
			let at = Log2 {
				term: entry.log_id.leader_id.term,
				node: entry.log_id.leader_id.node_id,
				index: entry.log_id.index,
			};
			Some((
				*index,
				(
					at,
					carrier.command().to_vec(),
					carrier.governance_bytes().map(<[u8]>::to_vec),
				),
			))
		})
		.collect()
}
fn log_id(b: &mut Vec<u8>, l: Log2) {
	b.extend(l.term.to_le_bytes());
	b.extend(l.node.to_le_bytes());
	b.extend(l.index.to_le_bytes());
}
fn optional(b: &mut Vec<u8>, l: Option<Log2>) {
	b.push(u8::from(l.is_some()));
	log_id(
		b,
		l.unwrap_or(Log2 {
			term: 0,
			node: 0,
			index: 0,
		}),
	);
}
fn length(n: usize) -> Result<[u8; 4], &'static str> {
	u32::try_from(n)
		.map(u32::to_le_bytes)
		.map_err(|_| "export length over u32")
}

/// The export of a complete fold over `0..=stop` for the bound `manifest`, labelled with the
/// operator-asserted `node_id`. Deterministic in its inputs. Each successor's bytes must be the
/// activation entry's own rotation of that role to that key; anything else refuses the export.
pub(super) fn export2(
	traced: &Traced2,
	manifest: &[u8],
	node_id: u32,
	stop: Log2,
	rotations: &BTreeMap<u64, Rotation2>,
) -> Result<Vec<u8>, &'static str> {
	let context = admin_context2(&traced.admin);
	if manifest.len() != MANIFEST_BYTES {
		return Err("the bound manifest is not a 241-byte profile-3 manifest");
	}
	let successors = traced.history.successors();
	let mut per_role = [0usize; 3];
	for (role, ..) in &successors {
		per_role[usize::from(role - 1)] += 1;
	}
	if successors.len() > SUCCESSORS || per_role.iter().any(|n| *n > ROLE_SUCCESSORS) {
		return Err("more successors than the export carries");
	}
	let mut b = Vec::new();
	b.extend_from_slice(MAGIC);
	b.extend(VERSION.to_le_bytes());
	b.extend([0, 0]);
	b.extend(context_bytes(&context));
	b.extend_from_slice(manifest);
	b.extend(node_id.to_le_bytes());
	log_id(&mut b, stop);
	// Coverage: only a complete fold is exported.
	b.push(1);
	b.push(u8::from(traced.frozen));
	optional(&mut b, traced.transition);
	optional(&mut b, traced.retired);
	b.extend(length(successors.len())?);
	for (role, epoch, key, activated) in successors {
		let Some((at, command, Some(sidecar))) = rotations.get(&activated.index) else {
			return Err("a successor activation has no committed rotation with a sidecar");
		};
		// The fold verified these bytes; this checks only that they are this activation's own.
		let own = Command2::decode(command).is_ok_and(|(g, c, decoded)| {
			(g, c) == (context.generation, context.cluster)
				&& rotation(&decoded) == Some((role, key))
		});
		if *at != activated
			|| !own || command.len() > ROOT2_COMMAND_ENVELOPE_MAX1
			|| sidecar.len() != ROLE_PROOF_BYTES3
		{
			return Err("a successor's committed bytes are not its activation's rotation");
		}
		b.push(role);
		b.extend(epoch.to_le_bytes());
		b.extend(key);
		log_id(&mut b, activated);
		b.extend(length(command.len())?);
		b.extend_from_slice(command);
		b.extend(length(sidecar.len())?);
		b.extend_from_slice(sidecar);
	}
	let digest = hash(b"", &b);
	b.extend(digest);
	Ok(b)
}
