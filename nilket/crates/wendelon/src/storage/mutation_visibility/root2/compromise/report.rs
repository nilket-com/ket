//! Output rows and the final coverage record: one compact JSON object per line, fixed struct field
//! order and default `serde_json` escaping, rows in ascending log index. Rows are projections of
//! committed log data; nothing is stored.
use super::attribution::{Claim2, Form2, Invalid2, Record2, Unattributable2};
use super::trace::{Effect2, History2, Step2, Traced2};
use crate::storage::mutation_visibility::root2::admin_install::admin_context2;
use crate::storage::mutation_visibility::root2::types::Log2;
use openraft::LogId;
use serde::Serialize;

/// A signer identity `(role, epoch)` within this root; role codes 1 admin, 2 governance,
/// 3 recovery.
pub(super) type Identity2 = (u8, u64);

#[derive(Serialize)]
struct LogJson {
	term: u64,
	node: u32,
	index: u64,
}
fn log(l: Log2) -> LogJson {
	LogJson {
		term: l.term,
		node: l.node,
		index: l.index,
	}
}
pub(super) fn raft_log(l: LogId<u32>) -> Log2 {
	Log2 {
		term: l.leader_id.term,
		node: l.leader_id.node_id,
		index: l.index,
	}
}
fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn role_name(role: u8) -> &'static str {
	match role {
		1 => "admin",
		2 => "governance",
		3 => "recovery",
		_ => "unknown",
	}
}
/// The root key: the full bound administrator context.
#[derive(Serialize)]
struct RootJson {
	generation: String,
	cluster: String,
	installed_at: LogJson,
	bootstrap_fingerprint: String,
	bootstrap_command: String,
	genesis: String,
	initial_epoch: u64,
	manifest_pin: String,
}
#[derive(Serialize)]
struct IdentityJson {
	role: &'static str,
	epoch: u64,
}
fn identity(i: Identity2) -> IdentityJson {
	IdentityJson {
		role: role_name(i.0),
		epoch: i.1,
	}
}
#[derive(Serialize)]
struct CommandJson {
	kind: &'static str,
	digest: String,
}
#[derive(Serialize)]
struct EffectJson {
	outcome: &'static str,
	result: &'static str,
	reason: Option<u16>,
}
#[derive(Serialize)]
#[serde(untagged)]
enum SignatureJson {
	Claim {
		claimed_role: &'static str,
		claimed_epoch: u64,
		form: &'static str,
		slot: u8,
		crypto_valid: bool,
		active_at_entry: bool,
		reason: Option<&'static str>,
	},
	Unattributable {
		unattributable: &'static str,
	},
}
#[derive(Serialize)]
struct PositionJson {
	identity: IdentityJson,
	after_superseded: bool,
	after_retired: bool,
	in_freeze_interval: bool,
}
#[derive(Serialize)]
struct RowJson<'a> {
	kind: &'static str,
	root: &'a RootJson,
	log_id: LogJson,
	fingerprint: String,
	command: CommandJson,
	effect: EffectJson,
	signatures: Vec<SignatureJson>,
	#[serde(rename = "match")]
	class: &'static str,
	positions: Vec<PositionJson>,
	finding: bool,
}
#[derive(Serialize)]
struct ContainmentJson {
	identity: IdentityJson,
	activated_at: Option<LogJson>,
	superseded_at: Option<LogJson>,
	retired_at: Option<LogJson>,
	freeze_intervals: Vec<(LogJson, Option<LogJson>)>,
}
#[derive(Serialize)]
struct CoverageJson {
	kind: &'static str,
	snapshot_boundary: Option<LogJson>,
	first_examined: Option<LogJson>,
	last_examined: Option<LogJson>,
	stop_position: Option<LogJson>,
	stop_scope: &'static str,
	uncommitted_past_stop: u64,
	torn_tail_past_stop: u64,
	containment: Vec<ContainmentJson>,
	status: &'static str,
}

/// The coverage facts the final record states, whatever the outcome.
pub(super) struct Coverage2 {
	pub(super) snapshot_boundary: Option<Log2>,
	pub(super) first_examined: Option<Log2>,
	pub(super) last_examined: Option<Log2>,
	pub(super) stop: Option<Log2>,
	pub(super) uncommitted_past_stop: u64,
	pub(super) torn_tail_bytes: u64,
}
/// One identity's containment, computed from the replayed history alone.
struct Containment2 {
	identity: Identity2,
	activated: Option<Log2>,
	superseded: Option<Log2>,
	retired: Option<Log2>,
}
impl Containment2 {
	fn of(identity: Identity2, history: &History2, retired: Option<Log2>) -> Self {
		let (role, epoch) = identity;
		Self {
			identity,
			activated: history.activated(role, epoch),
			superseded: epoch
				.checked_add(1)
				.and_then(|next| history.activated(role, next)),
			retired,
		}
	}
}
/// Inside a freeze: strictly after the freeze entry and up to and including the unfreeze entry,
/// whose pre-entry state is still frozen.
fn in_freeze(index: u64, freezes: &[(Log2, Option<Log2>)]) -> bool {
	freezes
		.iter()
		.any(|(f, u)| f.index < index && u.is_none_or(|u| index <= u.index))
}
fn signature(record: &Record2) -> SignatureJson {
	match record {
		Record2::Claim(Claim2 {
			role,
			epoch,
			form,
			slot,
			crypto_valid,
			active_at_entry,
			invalid,
		}) => SignatureJson::Claim {
			claimed_role: role_name(*role),
			claimed_epoch: *epoch,
			form: match form {
				Form2::RoleProof => "VISROL03",
				Form2::Governance => "VISADG01",
				Form2::Statement => "VISADS02",
				Form2::EpochStatement => "VISADS03",
			},
			slot: *slot,
			crypto_valid: *crypto_valid,
			active_at_entry: *active_at_entry,
			reason: invalid.map(|i| match i {
				Invalid2::Unsigned => "unsigned",
				Invalid2::Binding => "binding_mismatch",
				Invalid2::UnknownEpoch => "unknown_epoch",
				Invalid2::Domain => "no_signing_domain",
				Invalid2::Signature => "signature",
			}),
		},
		Record2::Unattributable(u) => SignatureJson::Unattributable {
			unattributable: match u {
				Unattributable2::NoSidecar => "no_sidecar",
				Unattributable2::Unparseable => "unparseable",
				Unattributable2::NoRole => "no_role",
				Unattributable2::NoRoleForm => "no_role_form",
			},
		},
	}
}
/// Suspect identities with a valid signature in this row, and those only claimed.
fn matched(step: &Step2, suspects: &[Identity2]) -> (Vec<Identity2>, bool) {
	let mut used = Vec::new();
	let mut claimed = false;
	for record in &step.records {
		let Record2::Claim(c) = record else {
			continue;
		};
		if suspects.contains(&(c.role, c.epoch)) {
			if c.crypto_valid {
				if !used.contains(&(c.role, c.epoch)) {
					used.push((c.role, c.epoch));
				}
			} else {
				claimed = true;
			}
		}
	}
	(used, claimed)
}
fn json(value: &impl Serialize) -> String {
	serde_json::to_string(value).expect("audit rows are plain serde values")
}

/// The audit output for a complete fold: rows (all, or only the two suspect classes), then the
/// final record with status `complete`.
pub(super) fn render_complete(
	traced: &Traced2,
	suspects: &[Identity2],
	suspect_only: bool,
	coverage: &Coverage2,
) -> String {
	let c = admin_context2(&traced.admin);
	let root = RootJson {
		generation: hex(&c.generation),
		cluster: hex(&c.cluster),
		installed_at: log(c.installed_at),
		bootstrap_fingerprint: hex(&c.bootstrap_fingerprint),
		bootstrap_command: hex(&c.bootstrap_command),
		genesis: hex(&c.genesis),
		initial_epoch: c.initial_epoch,
		manifest_pin: hex(&c.manifest_pin),
	};
	let containment: Vec<_> = suspects
		.iter()
		.map(|s| Containment2::of(*s, &traced.history, traced.retired))
		.collect();
	let mut out = String::new();
	for step in &traced.steps {
		let (used, claimed) = matched(step, suspects);
		let class = if !used.is_empty() {
			"suspect_use"
		} else if claimed {
			"suspect_claim_unverified"
		} else {
			"none"
		};
		if suspect_only && class == "none" {
			continue;
		}
		let index = step.at.index;
		let positions: Vec<_> = used
			.iter()
			.filter_map(|u| containment.iter().find(|c| c.identity == *u))
			.map(|c| PositionJson {
				identity: identity(c.identity),
				after_superseded: c.superseded.is_some_and(|s| index > s.index),
				after_retired: c.retired.is_some_and(|r| index > r.index),
				in_freeze_interval: in_freeze(index, &traced.freezes),
			})
			.collect();
		let denied = matches!(step.effect, Effect2::Denied(_));
		let finding = !denied
			&& positions
				.iter()
				.any(|p| p.after_superseded || p.after_retired);
		out.push_str(&json(&RowJson {
			kind: "entry",
			root: &root,
			log_id: log(step.at),
			fingerprint: hex(&step.fingerprint),
			command: CommandJson {
				kind: step.kind,
				digest: hex(&step.digest),
			},
			effect: match step.effect {
				Effect2::Applied(result) => EffectJson {
					outcome: "applied",
					result,
					reason: None,
				},
				Effect2::Denied(reason) => EffectJson {
					outcome: "denied",
					result: "denied",
					reason: Some(reason),
				},
			},
			signatures: step.records.iter().map(signature).collect(),
			class,
			positions,
			finding,
		}));
		out.push('\n');
	}
	let containment = containment
		.iter()
		.map(|c| ContainmentJson {
			identity: identity(c.identity),
			activated_at: c.activated.map(log),
			superseded_at: c.superseded.map(log),
			retired_at: c.retired.map(log),
			freeze_intervals: traced
				.freezes
				.iter()
				.map(|(f, u)| (log(*f), u.map(log)))
				.collect(),
		})
		.collect();
	out.push_str(&coverage_line(coverage, containment, "complete"));
	out
}
/// The final record of a partial audit: the coverage facts and no containment, since no fold ran.
pub(super) fn render_partial(coverage: &Coverage2) -> String {
	coverage_line(coverage, Vec::new(), "partial")
}
fn coverage_line(
	coverage: &Coverage2,
	containment: Vec<ContainmentJson>,
	status: &'static str,
) -> String {
	let mut line = json(&CoverageJson {
		kind: "coverage",
		snapshot_boundary: coverage.snapshot_boundary.map(log),
		first_examined: coverage.first_examined.map(log),
		last_examined: coverage.last_examined.map(log),
		stop_position: coverage.stop.map(log),
		stop_scope: "local_committed_not_quorum_current",
		uncommitted_past_stop: coverage.uncommitted_past_stop,
		torn_tail_past_stop: coverage.torn_tail_bytes,
		containment,
		status,
	});
	line.push('\n');
	line
}
